// Copyright 2026 MinIO Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	. "github.com/honeycombio/avo/build"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

const (
	// simdMaxLen is the longest literal or match length without an escape.
	simdMaxLen = 32
	// simdBatch is how many operations the AVX-512 loops expand at a time.
	simdBatch = 64
)

// genSIMD generates the operation loops of SIMD blocks (SIMD_SPEC.md).
func genSIMD() {
	tab := simdPatternTable()
	otab := NewDataAddr(Symbol{Name: "·simdOffTab"}, 0)
	genSIMDExec("simdExecAVX2", false, tab, otab)
	genSIMDExec("simdExecDeltaAVX2", true, tab, otab)
	genSIMDStats()
	ptab := simdPermTable()
	// VPSHUFB indexes of the low byte of each dword.
	lowb := GLOBL("simdLowByteTab", RODATA|NOPTR)
	DATA(0, U64(0x0404040400000000))
	DATA(8, U64(0x0c0c0c0c08080808))
	// The lane numbers of dwords.
	iota := GLOBL("simdIotaTab", RODATA|NOPTR)
	for i := 0; i < 16; i += 2 {
		DATA(4*i, U64(uint64(i+1)<<32|uint64(i)))
	}
	genSIMDExec512("simdExecAVX512", false, ptab, lowb, iota)
	genSIMDExec512("simdExecDeltaAVX512", true, ptab, lowb, iota)
	genSIMDStats512()
}

// simdPermTable returns 33 rows of 32 VPERMB indexes, indexed by min(offset, 32).
// Byte i of row r is i % r, and i in row 32. Row 0 is row 1, see simdPatternTable.
func simdPermTable() Mem {
	tab := GLOBL("simdPermTab", RODATA|NOPTR)
	for r := 0; r <= simdMaxLen; r++ {
		for j := 0; j < 32; j += 8 {
			var v uint64
			for k := 7; k >= 0; k-- {
				v = v<<8 | uint64((j+k)%max(r, 1))
			}
			DATA(r*32+j, U64(v))
		}
	}
	return tab
}

// simdPatternTable returns 33 rows of 32 bytes, indexed by min(offset, 32).
// Byte i of row r is 0x80 (keep the plain loaded byte) when i < r or r >= 32,
// otherwise i % r, which indexes the first 16 source bytes broadcast to both lanes.
// Row 0 is row 1: a delta reference at position 0 repeats dst[0], which is 0.
func simdPatternTable() Mem {
	tab := GLOBL("simdPatternTab", RODATA|NOPTR)
	for r := 0; r <= simdMaxLen; r++ {
		var row [32]byte
		p := max(r, 1)
		for i := range row {
			row[i] = 0x80
			if p < simdMaxLen && i >= p {
				row[i] = byte(i % p)
			}
		}
		for j := 0; j < 32; j += 8 {
			var v uint64
			for k := 7; k >= 0; k-- {
				v = v<<8 | uint64(row[j+k])
			}
			DATA(r*32+j, U64(v))
		}
	}
	return tab
}

// simdOffset loads the next offset symbol and its raw bits: o = base + bits.
// The symbol and bits are only consumed for new offsets. t is the token; nrep is 1 for a new offset.
func simdOffset(t reg.GPVirtual, ocp, raw reg.Register, pos reg.GPVirtual, otab Mem) (o, nrep reg.GPVirtual) {
	o = GP64()
	MOVBQZX(Mem{Base: ocp}, o)
	ot := GP64()
	LEAQ(otab, ot)
	MOVL(Mem{Base: ot, Index: o, Scale: 4}, o.As32())
	nrep = GP64()
	MOVQ(t, nrep)
	ANDQ(U8(1), nrep)
	XORQ(U8(1), nrep)
	ADDQ(nrep, ocp)
	v, c := GP64(), GP64()
	MOVQ(pos, v)
	SHRQ(U8(3), v)
	MOVQ(Mem{Base: raw, Index: v, Scale: 1}, v)
	MOVQ(pos, c)
	ANDQ(U8(7), c)
	SHRXQ(c, v, v)
	MOVL(o.As32(), c.As32())
	SHRL(U8(24), c.As32())
	BZHIQ(c, v, v)
	ANDL(U32(0xffffff), o.As32())
	ADDQ(v, o)
	NEGQ(nrep)
	ANDQ(nrep, c)
	ADDQ(c, pos)
	return o, nrep
}

// genSIMDExec generates a loop that runs the first n operations of a chunk, see simdExecGo.
// With delta, the first 32 literals of each operation are added to their delta references.
func genSIMDExec(name string, delta bool, tab, otab Mem) {
	TEXT(name, NOSPLIT, "func(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)")
	Doc(name+" runs the first n operations of a chunk. st holds the output position,",
		"the literal position, the repeat offset and an error flag.", "")
	Pragma("noescape")
	dst := Load(Param("dst"), GP64())
	litp := Load(Param("lits"), GP64())
	tok := Load(Param("tok"), GP64())
	llp := Load(Param("ll"), GP64())
	ocp := Load(Param("offc"), GP64())
	raw := Load(Param("raw"), GP64())
	end := Load(Param("n"), GP64())
	ADDQ(tok, end)
	stp := Load(Param("st"), GP64())
	d, last := GP64(), GP64()
	MOVQ(Mem{Base: stp}, d)
	ADDQ(Mem{Base: stp, Disp: 8}, litp)
	MOVQ(Mem{Base: stp, Disp: 16}, last)
	pos := GP64()
	XORQ(pos, pos)
	CMPQ(tok, end)
	JAE(LabelRef(name + "_done"))

	// 64 also aligns the function to 64, so the loop's place in cache lines is the same in every build.
	PCALIGN(Imm(64))
	Label(name + "_loop")
	t := GP64()
	MOVBQZX(Mem{Base: tok}, t)
	lv := YMM()
	VMOVDQU(Mem{Base: litp}, lv)
	var dr, rs reg.GPVirtual
	var rv reg.VecVirtual
	if delta {
		// The references use the repeat offset from before this operation, clamped to the position.
		dr, rs, rv = GP64(), GP64(), YMM()
		MOVQ(last, dr)
		CMPQ(dr, d)
		CMOVQHI(d, dr)
		MOVQ(d, rs)
		SUBQ(dr, rs)
		CMPQ(dr, U8(simdMaxLen))
		JB(LabelRef(name + "_dpattern"))
		VMOVDQU(Mem{Base: dst, Index: rs, Scale: 1}, rv)
		Label(name + "_dadd")
		VPADDB(rv, lv, lv)
	}
	VMOVDQU(lv, Mem{Base: dst, Index: d, Scale: 1})
	llv, f := GP64(), GP64()
	MOVBQZX(Mem{Base: llp}, llv)
	MOVQ(t, f)
	SHRQ(U8(1), f)
	ANDQ(U8(1), f)
	ADDQ(f, llp)
	NEGQ(f)
	ANDQ(f, llv)
	CMPQ(llv, U8(simdMaxLen+1))
	JE(LabelRef(name + "_litlong"))
	Label(name + "_litback")
	ADDQ(llv, d)
	ADDQ(llv, litp)

	o, nrep := simdOffset(t, ocp, raw, pos, otab)
	TESTQ(nrep, nrep)
	CMOVQNE(o, last)
	// Matches copy from the repeat offset clamped to the position.
	r := GP64()
	MOVQ(last, r)
	CMPQ(r, d)
	CMOVQHI(d, r)
	src := GP64()
	MOVQ(d, src)
	SUBQ(r, src)
	SHRQ(U8(2), t)
	srcMem := Mem{Base: dst, Index: src, Scale: 1}
	dstMem := Mem{Base: dst, Index: d, Scale: 1}
	CMPQ(r, t)
	JB(LabelRef(name + "_pattern"))
	mv := YMM()
	VMOVDQU(srcMem, mv)
	VMOVDQU(mv, dstMem)
	Label(name + "_check")
	CMPQ(t, U8(simdMaxLen+1))
	JE(LabelRef(name + "_long"))
	Label(name + "_next")
	ADDQ(t, d)
	INCQ(tok)
	CMPQ(tok, end)
	JB(LabelRef(name + "_loop"))

	Label(name + "_done")
	VZEROUPPER()
	stp = Load(Param("st"), GP64())
	MOVQ(d, Mem{Base: stp})
	lits := Load(Param("lits"), GP64())
	SUBQ(lits, litp)
	MOVQ(litp, Mem{Base: stp, Disp: 8})
	MOVQ(last, Mem{Base: stp, Disp: 16})
	RET()

	pattern := func(row reg.GPVirtual, m Mem, out reg.VecVirtual) {
		tp := GP64()
		LEAQ(tab, tp)
		SHLQ(U8(5), row)
		bv, tv := YMM(), YMM()
		VMOVDQU(m, out)
		VBROADCASTI128(m, bv)
		VMOVDQU(Mem{Base: tp, Index: row, Scale: 1}, tv)
		VPSHUFB(tv, bv, bv)
		VPBLENDVB(tv, out, bv, out)
	}
	// Taken when the clamped offset is below min(ml, 33). Row 32 is a plain copy.
	Label(name + "_pattern")
	pv := YMM()
	pattern(r, srcMem, pv)
	VMOVDQU(pv, dstMem)
	JMP(LabelRef(name + "_check"))

	if delta {
		Label(name + "_dpattern")
		pattern(dr, Mem{Base: dst, Index: rs, Scale: 1}, rv)
		JMP(LabelRef(name + "_dadd"))
	}

	simdEscapes(name, func(a, b Op) { VMOVDQU(a, b) }, dst, litp, d, llv, src, t)
}

// simdEscapes generates the paths of escaped literal and match lengths of an operation loop.
// Escaped literal lengths are 33 plus an escape byte, and the first 32 literals are stored.
// The escape cursor lives in its argument slot: only escapes touch it, and the registers are taken.
// mov moves 32 bytes.
func simdEscapes(name string, mov func(src, dst Op), dst, litp, d, llv, src, t reg.Register) {
	Label(name + "_litlong")
	{
		ep := Load(Param("esc"), GP64())
		cnt := GP64()
		MOVBQZX(Mem{Base: ep}, cnt)
		INCQ(ep)
		Store(ep, Param("esc"))
		LEAQ(Mem{Base: cnt, Disp: simdMaxLen + 1}, llv)
		// 32-byte steps after the first: (ll-32+31)>>5 = (escape+32)>>5.
		ADDQ(U8(simdMaxLen), cnt)
		SHRQ(U8(5), cnt)
		s, o := GP64(), GP64()
		LEAQ(Mem{Base: litp, Disp: 32}, s)
		LEAQ(Mem{Base: d, Disp: 32}, o)
		Label(name + "_litloop")
		y := YMM()
		mov(Mem{Base: s}, y)
		mov(y, Mem{Base: dst, Index: o, Scale: 1})
		ADDQ(U8(32), s)
		ADDQ(U8(32), o)
		DECQ(cnt)
		JNZ(LabelRef(name + "_litloop"))
		JMP(LabelRef(name + "_litback"))
	}

	Label(name + "_long")
	{
		// Matches longer than 32 bytes need an offset of at least 32.
		dist := GP64()
		MOVQ(d, dist)
		SUBQ(src, dist)
		CMPQ(dist, U8(simdMaxLen))
		JAE(LabelRef(name + "_longok"))
		sp := Load(Param("st"), GP64())
		MOVQ(U32(1), Mem{Base: sp, Disp: 24})
		Label(name + "_longok")
		ep := Load(Param("esc"), GP64())
		cnt := GP64()
		MOVBQZX(Mem{Base: ep}, cnt)
		INCQ(ep)
		Store(ep, Param("esc"))
		LEAQ(Mem{Base: cnt, Disp: simdMaxLen + 1}, t)
		ADDQ(U8(simdMaxLen), cnt)
		SHRQ(U8(5), cnt)
		s, o := GP64(), GP64()
		LEAQ(Mem{Base: src, Disp: 32}, s)
		LEAQ(Mem{Base: d, Disp: 32}, o)
		Label(name + "_longloop")
		y := YMM()
		mov(Mem{Base: dst, Index: s, Scale: 1}, y)
		mov(y, Mem{Base: dst, Index: o, Scale: 1})
		ADDQ(U8(32), s)
		ADDQ(U8(32), o)
		DECQ(cnt)
		JNZ(LabelRef(name + "_longloop"))
		JMP(LabelRef(name + "_next"))
	}
}

// simdConst returns a vector with every dword set to c.
func simdConst(c uint32) reg.VecVirtual {
	r, z := GP32(), ZMM()
	MOVL(U32(c), r)
	VPBROADCASTD(r, z)
	return z
}

// genSIMDExec512 generates a loop that runs the first n operations of a chunk, see simdExecGo.
// Each batch of simdBatch operations is first expanded with AVX-512 into the literal length and
// the resolved offset of each operation, so the copy loop does no stream bookkeeping.
// The moves are EVEX encoded, as the constants can push the copy registers above Y15.
func genSIMDExec512(name string, delta bool, tab, lowbTab, iotaTab Mem) {
	TEXT(name, NOSPLIT, "func(dst, lits, tok, ll, offc, raw *byte, nraw int, esc *byte, n int, st *[4]int)")
	Doc(name+" runs the first n operations of a chunk. st holds the output position,",
		"the literal position, the repeat offset and an error flag. nraw is the readable size of raw.", "")
	Pragma("noescape")
	llx, offx := AllocLocal(simdBatch), AllocLocal(4*simdBatch)
	sEnd, sPos := AllocLocal(8), AllocLocal(8)
	at := func(m Mem, i reg.Register, scale uint8) Mem {
		m.Index, m.Scale = i, scale
		return m
	}

	// The copy loop runs at least one operation.
	e := Load(Param("n"), GP64())
	TESTQ(e, e)
	JZ(LabelRef(name + "_ret"))
	dst := Load(Param("dst"), GP64())
	litp := Load(Param("lits"), GP64())
	tok := Load(Param("tok"), GP64())
	stp := Load(Param("st"), GP64())
	d := GP64()
	MOVQ(Mem{Base: stp}, d)
	ADDQ(Mem{Base: stp, Disp: 8}, litp)
	// The offset in effect after the last expanded operation, in every lane.
	carry := ZMM()
	VPBROADCASTD(Mem{Base: stp, Disp: 16}, carry)
	var last reg.GPVirtual
	if delta {
		last = GP64()
		MOVQ(Mem{Base: stp, Disp: 16}, last)
	}
	{
		ADDQ(tok, e)
		MOVQ(e, sEnd)
		p := GP64()
		XORQ(p, p)
		MOVQ(p, sPos)
	}
	zero := ZMM()
	VPXORD(zero, zero, zero)
	ones := ZMM()
	VPTERNLOGD(U8(0xff), ones, ones, ones)
	c1, c2, c1f := simdConst(0x01010101), simdConst(0x02020202), simdConst(0x1f1f1f1f)
	c7, c8, c15 := simdConst(7), simdConst(8), simdConst(15)
	iota := ZMM()
	VMOVDQU32(iotaTab, iota)
	// Dword lanes 0-7 select qword 0, lanes 8-15 qword 1.
	gidx := ZMM()
	{
		r, k := GP32(), K()
		MOVL(U32(0xff00), r)
		KMOVW(r, k)
		VMOVDQA32_Z(c2, k, gidx)
	}
	c0123 := simdConst(0x03020100)
	// Selects the low byte of each dword into all its bytes.
	lowb := ZMM()
	VBROADCASTI32X4(lowbTab, lowb)

	// The lanes of the batch's operations with literals and with new offsets.
	kLit, kNew := K(), K()
	// The offset symbols of the batch with 0 for repeats, their raw bit counts, the bit counts
	// before each within its 8 operations, and the bit position of each 8 operations.
	sym, nb8, exc8, qabs := ZMM(), ZMM(), ZMM(), ZMM()

	// group decodes the offsets of operations 16h to 16h+15 of the batch into o.
	// Symbol 0 decodes as offset 0, which then takes the offset before it.
	group := func(h int, o Mem) {
		// The lane each operation takes its offset from: the last new offset up to it, or -1 for the carry.
		from := ZMM()
		{
			kg := kNew
			if h > 0 {
				kg = K()
				KSHIFTRQ(U8(16*h), kNew, kg)
			}
			VMOVDQA64(ones, from)
			VMOVDQA32(iota, kg, from)
			for i := 1; i < 16; i *= 2 {
				x := ZMM()
				VALIGND(U8(16-i), ones, from, x)
				VPMAXSD(x, from, from)
			}
		}
		s, nb, bp := ZMM(), ZMM(), ZMM()
		qs := qabs
		if h == 0 {
			VPMOVZXBD(sym.AsX(), s)
			VPMOVZXBD(nb8.AsX(), nb)
			VPMOVZXBD(exc8.AsX(), bp)
		} else {
			for _, v := range [][2]reg.VecVirtual{{sym, s}, {nb8, nb}, {exc8, bp}} {
				x := XMM()
				VEXTRACTI32X4(U8(h), v[0], x)
				VPMOVZXBD(x, v[1])
			}
			qs = ZMM()
			VALIGNQ(U8(2*h), qabs, qabs, qs)
		}
		{
			x := ZMM()
			VPERMD(qs, gidx, x)
			VPADDD(x, bp, bp)
		}
		// The 64 raw bytes from the byte holding the first bit position.
		w := ZMM()
		{
			r, wb := GP64(), GP64()
			VMOVQ(qs.AsX(), wb)
			SHRQ(U8(3), wb)
			Load(Param("nraw"), r)
			SUBQ(wb, r)
			// BZHI only reads the low byte of the length.
			c := GP64()
			MOVL(U32(64), c.As32())
			CMPQ(r, c)
			CMOVQGT(c, r)
			MOVQ(I32(-1), c)
			BZHIQ(r, c, c)
			kr := K()
			KMOVQ(c, kr)
			Load(Param("raw"), c)
			VMOVDQU8_Z(at(Mem{Base: c}, wb, 1), kr, w)
			x := ZMM()
			SHLQ(U8(3), wb)
			VPBROADCASTD(wb.As32(), x)
			VPSUBD(x, bp, x)
			// Gather the 4 bytes from each bit position, then shift and mask.
			VPSRLD(U8(3), x, x)
			VPSHUFB(lowb, x, x)
			VPADDB(c0123, x, x)
			VPERMB(w, x, w)
		}
		VPANDD(c7, bp, bp)
		VPSRLVD(bp, w, w)
		hi := ZMM()
		VPSLLVD(nb, ones, hi)
		VPANDND(w, hi, w)
		// The offset is (s&7 | min(s, 8)&8) << nb, plus the bits.
		base := ZMM()
		VPMINUD(c8, s, base)
		VPTERNLOGD(U8(0xd8), c7, s, base)
		VPSLLVD(nb, base, base)
		VPADDD(w, base, w)
		VPERMI2D(carry, w, from)
		VMOVDQU32(from, o)
		VPERMD(from, c15, carry)
	}

	Label(name + "_batch")
	m := GP64()
	{
		c := GP64()
		MOVQ(sEnd, m)
		SUBQ(tok, m)
		MOVL(U32(simdBatch), c.As32())
		CMPQ(m, c)
		CMOVQGT(c, m)
	}
	{
		r := GP64()
		MOVQ(I32(-1), r)
		BZHIQ(m, r, r)
		kv := K()
		KMOVQ(r, kv)
		t := ZMM()
		VMOVDQU8_Z(Mem{Base: tok}, kv, t)
		VPTESTMB(c2, t, kLit)
		VPTESTNMB(c1, t, kv, kNew)
	}
	// expand places the next values of the stream in argument arg at the lanes in k.
	// The memory form only reads the values it places, so it stays inside the stream.
	expand := func(k reg.OpmaskVirtual, arg string, v reg.VecVirtual) {
		n := GP64()
		KMOVQ(k, n)
		POPCNTQ(n, n)
		p := Load(Param(arg), GP64())
		VPEXPANDB_Z(Mem{Base: p}, k, v)
		ADDQ(n, p)
		Store(p, Param(arg))
	}
	{
		l := ZMM()
		expand(kLit, "ll", l)
		VMOVDQU64(l, llx)
		expand(kNew, "offc", sym)
	}
	{
		// nb = max(s>>3 - 1, 0). Its sums within 8 operations fit in bytes.
		x, y := ZMM(), ZMM()
		VPSRLW(U8(3), sym, x)
		VPANDD(c1f, x, x)
		VPSUBUSB(c1, x, nb8)
		VMOVDQA64(nb8, x)
		for _, sh := range []uint8{8, 16, 32} {
			VPSLLQ(U8(sh), x, y)
			VPADDB(y, x, x)
		}
		VPSUBB(nb8, x, exc8)
		t, q := ZMM(), ZMM()
		VPSADBW(zero, nb8, t)
		VMOVDQA64(t, q)
		for i := 1; i < 8; i *= 2 {
			VALIGNQ(U8(8-i), zero, q, y)
			VPADDQ(y, q, q)
		}
		VPSUBQ(t, q, qabs)
		VPBROADCASTQ(sPos, y)
		VPADDQ(y, qabs, qabs)
		r := GP64()
		VALIGNQ(U8(7), q, q, y)
		VMOVQ(y.AsX(), r)
		ADDQ(r, sPos)
	}
	for h := range simdBatch / 16 {
		if h > 0 {
			CMPQ(m, U8(16*h))
			JBE(LabelRef(name + "_exec"))
		}
		group(h, Mem{Base: offx.Base, Disp: offx.Disp + 64*h})
	}
	Label(name + "_exec")
	j := GP64()
	XORQ(j, j)

	// 64 also aligns the function to 64, so the loop's place in cache lines is the same in every build.
	PCALIGN(Imm(64))
	Label(name + "_loop")
	t := GP64()
	MOVBQZX(at(Mem{Base: tok}, j, 1), t)
	lv := YMM()
	VMOVDQU64(Mem{Base: litp}, lv)
	var dr, rs reg.GPVirtual
	var rv reg.VecVirtual
	if delta {
		// The references use the offset from before this operation, clamped to the position.
		dr, rs, rv = GP64(), GP64(), YMM()
		MOVQ(last, dr)
		CMPQ(dr, d)
		CMOVQHI(d, dr)
		MOVQ(d, rs)
		SUBQ(dr, rs)
		CMPQ(dr, U8(simdMaxLen))
		JB(LabelRef(name + "_dpattern"))
		VMOVDQU64(Mem{Base: dst, Index: rs, Scale: 1}, rv)
		Label(name + "_dadd")
		VPADDB(rv, lv, lv)
	}
	VMOVDQU64(lv, Mem{Base: dst, Index: d, Scale: 1})
	llv, o := GP64(), GP64()
	MOVBQZX(at(llx, j, 1), llv)
	MOVL(at(offx, j, 4), o.As32())
	CMPQ(llv, U8(simdMaxLen+1))
	JE(LabelRef(name + "_litlong"))
	Label(name + "_litback")
	ADDQ(llv, d)
	ADDQ(llv, litp)
	if delta {
		MOVQ(o, last)
	}
	// Matches copy from the offset clamped to the position.
	CMPQ(o, d)
	CMOVQHI(d, o)
	src := GP64()
	MOVQ(d, src)
	SUBQ(o, src)
	SHRQ(U8(2), t)
	srcMem := Mem{Base: dst, Index: src, Scale: 1}
	dstMem := Mem{Base: dst, Index: d, Scale: 1}
	CMPQ(o, t)
	JB(LabelRef(name + "_pattern"))
	mv := YMM()
	VMOVDQU64(srcMem, mv)
	VMOVDQU64(mv, dstMem)
	Label(name + "_check")
	CMPQ(t, U8(simdMaxLen+1))
	JE(LabelRef(name + "_long"))
	Label(name + "_next")
	ADDQ(t, d)
	INCQ(j)
	CMPQ(j, m)
	JB(LabelRef(name + "_loop"))
	ADDQ(m, tok)
	CMPQ(tok, sEnd)
	JB(LabelRef(name + "_batch"))

	stp = Load(Param("st"), GP64())
	MOVQ(d, Mem{Base: stp})
	lits := Load(Param("lits"), GP64())
	SUBQ(lits, litp)
	MOVQ(litp, Mem{Base: stp, Disp: 8})
	if delta {
		MOVQ(last, Mem{Base: stp, Disp: 16})
	} else {
		r := GP32()
		VMOVD(carry.AsX(), r)
		MOVQ(r.As64(), Mem{Base: stp, Disp: 16})
	}
	VZEROUPPER()
	Label(name + "_ret")
	RET()

	pattern := func(row reg.GPVirtual, m Mem, out reg.VecVirtual) {
		tp := GP64()
		LEAQ(tab, tp)
		SHLQ(U8(5), row)
		VMOVDQU64(Mem{Base: tp, Index: row, Scale: 1}, out)
		VPERMB(m, out, out)
	}
	// Taken when the clamped offset is below min(ml, 33). Row 32 is a plain copy.
	Label(name + "_pattern")
	pv := YMM()
	pattern(o, srcMem, pv)
	VMOVDQU64(pv, dstMem)
	JMP(LabelRef(name + "_check"))

	if delta {
		Label(name + "_dpattern")
		pattern(dr, Mem{Base: dst, Index: rs, Scale: 1}, rv)
		JMP(LabelRef(name + "_dadd"))
	}

	simdEscapes(name, func(a, b Op) { VMOVDQU64(a, b) }, dst, litp, d, llv, src, t)
}

// genSIMDStats generates the stream checks of chunks, for multiples of 32 bytes.
// See simdTokStats, simdLLStats and simdOffStats.
func genSIMDStats() {
	TEXT("simdTokStatsAVX2", NOSPLIT, "func(b *byte, n int) (out, lit, rep, esc int, ok bool)")
	Doc("simdTokStatsAVX2 returns the token statistics of n bytes, a multiple of 32.",
		"lit and esc are counted 2 and 4 times.", "")
	Pragma("noescape")
	p, end, zero, valid := simdStatsStart()
	acc := simdZeros(4)
	out, lit, rep, esc := acc[0], acc[1], acc[2], acc[3]
	m3f, m4, m2, m1, max := simdBroadcast(0x3f), simdBroadcast(4), simdBroadcast(2), simdBroadcast(1), simdBroadcast(135)
	Label("simdTokStatsAVX2_loop")
	t, x := YMM(), YMM()
	VMOVDQU(Mem{Base: p}, t)
	VPMINUB(max, t, x)
	VPCMPEQB(t, x, x)
	VPAND(x, valid, valid)
	VPSRLW(U8(2), t, x)
	VPAND(m3f, x, x)
	simdSum(x, zero, out)
	VPAND(m2, t, x)
	simdSum(x, zero, lit)
	VPAND(m1, t, x)
	simdSum(x, zero, rep)
	// Match length code 33 has bits 7 and 2.
	VPSRLW(U8(5), t, x)
	VPAND(t, x, x)
	VPAND(m4, x, x)
	simdSum(x, zero, esc)
	simdStatsNext(p, end, "simdTokStatsAVX2_loop")
	simdStatsEnd(valid, []string{"out", "lit", "rep", "esc"}, out, lit, rep, esc)

	TEXT("simdLLStatsAVX2", NOSPLIT, "func(b *byte, n int) (sum, esc int, ok bool)")
	Doc("simdLLStatsAVX2 returns the sum of n bytes, a multiple of 32, and how many are 33.",
		"ok is false if a byte is outside 1-33.", "")
	Pragma("noescape")
	p, end, zero, valid = simdStatsStart()
	acc = simdZeros(2)
	sum, cnt := acc[0], acc[1]
	c1, c33 := simdBroadcast(1), simdBroadcast(33)
	Label("simdLLStatsAVX2_loop")
	t, x = YMM(), YMM()
	VMOVDQU(Mem{Base: p}, t)
	VPMAXUB(c1, t, x)
	VPMINUB(c33, x, x)
	VPCMPEQB(t, x, x)
	VPAND(x, valid, valid)
	VPCMPEQB(c33, t, x)
	VPAND(c1, x, x)
	simdSum(x, zero, cnt)
	simdSum(t, zero, sum)
	simdStatsNext(p, end, "simdLLStatsAVX2_loop")
	simdStatsEnd(valid, []string{"sum", "esc"}, sum, cnt)

	TEXT("simdOffStatsAVX2", NOSPLIT, "func(b *byte, n int) (sum, zeros int, ok bool)")
	Doc("simdOffStatsAVX2 returns the sum of b>>3 over n bytes, a multiple of 32, and how often it is 0.",
		"ok is false if a byte is outside 1-175.", "")
	Pragma("noescape")
	p, end, zero, valid = simdStatsStart()
	acc = simdZeros(2)
	sum, cnt = acc[0], acc[1]
	c1, c175, m1f := simdBroadcast(1), simdBroadcast(175), simdBroadcast(0x1f)
	Label("simdOffStatsAVX2_loop")
	t, x = YMM(), YMM()
	VMOVDQU(Mem{Base: p}, t)
	VPMAXUB(c1, t, x)
	VPMINUB(c175, x, x)
	VPCMPEQB(t, x, x)
	VPAND(x, valid, valid)
	VPSRLW(U8(3), t, t)
	VPAND(m1f, t, t)
	VPCMPEQB(zero, t, x)
	VPAND(c1, x, x)
	simdSum(x, zero, cnt)
	simdSum(t, zero, sum)
	simdStatsNext(p, end, "simdOffStatsAVX2_loop")
	simdStatsEnd(valid, []string{"sum", "zeros"}, sum, cnt)
}

// genSIMDStats512 generates the stream checks of chunks for any length above 0.
// The last 64 bytes are loaded masked, so lanes past the end are 0.
func genSIMDStats512() {
	TEXT("simdTokStatsAVX512", NOSPLIT, "func(b *byte, n int) (out, lit, rep, esc int, ok bool)")
	Doc("simdTokStatsAVX512 returns the token statistics of n bytes, n > 0.",
		"lit and esc are counted 2 and 4 times.", "")
	Pragma("noescape")
	p, rem, zero, bad := simdStats512Start()
	acc := simdZeros512(4)
	out, lit, rep, esc := acc[0], acc[1], acc[2], acc[3]
	m3f, m4, m2, m1, max := simdConst(0x3f3f3f3f), simdConst(0x04040404), simdConst(0x02020202), simdConst(0x01010101), simdConst(0x87878787)
	Label("simdTokStatsAVX512_loop")
	t, _ := simdStats512Load(p, rem)
	k, x := K(), ZMM()
	VPCMPUB(U8(6), max, t, k)
	KORQ(k, bad, bad)
	VPSRLW(U8(2), t, x)
	VPANDD(m3f, x, x)
	simdSum(x, zero, out)
	VPANDD(m2, t, x)
	simdSum(x, zero, lit)
	VPANDD(m1, t, x)
	simdSum(x, zero, rep)
	// Match length code 33 has bits 7 and 2.
	VPSRLW(U8(5), t, x)
	VPANDD(t, x, x)
	VPANDD(m4, x, x)
	simdSum(x, zero, esc)
	simdStats512Next(p, rem, "simdTokStatsAVX512_loop")
	simdStats512End(bad, nil, []string{"out", "lit", "rep", "esc"}, out, lit, rep, esc)

	TEXT("simdLLStatsAVX512", NOSPLIT, "func(b *byte, n int) (sum, esc int, ok bool)")
	Doc("simdLLStatsAVX512 returns the sum of n bytes, n > 0, and how many are 33.",
		"ok is false if a byte is outside 1-33.", "")
	Pragma("noescape")
	p, rem, zero, bad = simdStats512Start()
	sum := simdZeros512(1)[0]
	cnt := GP64()
	XORQ(cnt, cnt)
	c33 := simdConst(0x21212121)
	Label("simdLLStatsAVX512_loop")
	t, kv := simdStats512Load(p, rem)
	k, k2 := K(), K()
	VPCMPUB(U8(6), c33, t, kv, k)
	VPTESTNMB(t, t, kv, k2)
	KORQ(k, bad, bad)
	KORQ(k2, bad, bad)
	VPCMPEQB(c33, t, k)
	simdCount(k, cnt)
	x = ZMM()
	VPSADBW(zero, t, x)
	VPADDQ(x, sum, sum)
	simdStats512Next(p, rem, "simdLLStatsAVX512_loop")
	simdStats512End(bad, cnt, []string{"sum", "esc"}, sum)

	TEXT("simdOffStatsAVX512", NOSPLIT, "func(b *byte, n int) (sum, zeros int, ok bool)")
	Doc("simdOffStatsAVX512 returns the sum of b>>3 over n bytes, n > 0, and how often it is 0.",
		"ok is false if a byte is outside 1-175.", "")
	Pragma("noescape")
	p, rem, zero, bad = simdStats512Start()
	sum = simdZeros512(1)[0]
	cnt = GP64()
	XORQ(cnt, cnt)
	c175, m1f := simdConst(0xafafafaf), simdConst(0x1f1f1f1f)
	Label("simdOffStatsAVX512_loop")
	t, kv = simdStats512Load(p, rem)
	k, k2 = K(), K()
	VPCMPUB(U8(6), c175, t, kv, k)
	VPTESTNMB(t, t, kv, k2)
	KORQ(k, bad, bad)
	KORQ(k2, bad, bad)
	x = ZMM()
	VPSRLW(U8(3), t, x)
	VPANDD(m1f, x, x)
	VPTESTNMB(x, x, kv, k)
	simdCount(k, cnt)
	VPSADBW(zero, x, x)
	VPADDQ(x, sum, sum)
	simdStats512Next(p, rem, "simdOffStatsAVX512_loop")
	simdStats512End(bad, cnt, []string{"sum", "zeros"}, sum)
}

func simdStats512Start() (p, rem reg.GPVirtual, zero reg.VecVirtual, bad reg.OpmaskVirtual) {
	p, rem = GP64(), GP64()
	Load(Param("b"), p)
	Load(Param("n"), rem)
	zero, bad = ZMM(), K()
	VPXORD(zero, zero, zero)
	KXORQ(bad, bad, bad)
	return p, rem, zero, bad
}

func simdZeros512(n int) []reg.VecVirtual {
	v := make([]reg.VecVirtual, n)
	for i := range v {
		v[i] = ZMM()
		VPXORD(v[i], v[i], v[i])
	}
	return v
}

// simdStats512Load loads up to 64 bytes at p, and returns them and their lanes.
func simdStats512Load(p, rem reg.GPVirtual) (reg.VecVirtual, reg.OpmaskVirtual) {
	n, c := GP64(), GP64()
	// BZHI only reads the low byte of the length.
	MOVL(U32(64), n.As32())
	CMPQ(rem, n)
	CMOVQLT(rem, n)
	MOVQ(I32(-1), c)
	BZHIQ(n, c, c)
	k, t := K(), ZMM()
	KMOVQ(c, k)
	VMOVDQU8_Z(Mem{Base: p}, k, t)
	return t, k
}

// simdCount adds the number of lanes in k to cnt.
func simdCount(k reg.OpmaskVirtual, cnt reg.GPVirtual) {
	r := GP64()
	KMOVQ(k, r)
	POPCNTQ(r, r)
	ADDQ(r, cnt)
}

func simdStats512Next(p, rem reg.GPVirtual, loop string) {
	ADDQ(U8(64), p)
	SUBQ(U8(64), rem)
	JG(LabelRef(loop))
}

// simdStats512End stores the sums of the quadwords of the accumulators in their results,
// followed by cnt if not nil, and whether no lane was bad in ok.
func simdStats512End(bad reg.OpmaskVirtual, cnt reg.GPVirtual, names []string, accs ...reg.VecVirtual) {
	KORTESTQ(bad, bad)
	ok := GP8()
	SETEQ(ok)
	Store(ok, Return("ok"))
	for i, v := range accs {
		y, x, x2 := YMM(), XMM(), XMM()
		VEXTRACTI64X4(U8(1), v, y)
		VPADDQ(y, v.AsY(), y)
		VEXTRACTI32X4(U8(1), y, x)
		VPADDQ(x, y.AsX(), x)
		VPSHUFD(U8(0x4e), x, x2)
		VPADDQ(x2, x, x)
		r := GP64()
		VMOVQ(x, r)
		Store(r, Return(names[i]))
	}
	if cnt != nil {
		Store(cnt, Return(names[len(names)-1]))
	}
	VZEROUPPER()
	RET()
}

func simdStatsStart() (p, end reg.GPVirtual, zero, valid reg.VecVirtual) {
	p, end = GP64(), GP64()
	Load(Param("b"), p)
	Load(Param("n"), end)
	ADDQ(p, end)
	zero, valid = YMM(), YMM()
	VPXOR(zero, zero, zero)
	VPCMPEQB(valid, valid, valid)
	return p, end, zero, valid
}

func simdZeros(n int) []reg.VecVirtual {
	v := make([]reg.VecVirtual, n)
	for i := range v {
		v[i] = YMM()
		VPXOR(v[i], v[i], v[i])
	}
	return v
}

func simdBroadcast(c byte) reg.VecVirtual {
	r, x, y := GP32(), XMM(), YMM()
	MOVL(U32(c), r)
	VMOVD(r, x)
	VPBROADCASTB(x, y)
	return y
}

// simdSum adds the sums of each 8 bytes of x to the quadwords of acc. x is overwritten.
func simdSum(x, zero, acc reg.VecVirtual) {
	VPSADBW(zero, x, x)
	VPADDQ(x, acc, acc)
}

func simdStatsNext(p, end reg.GPVirtual, loop string) {
	ADDQ(U8(32), p)
	CMPQ(p, end)
	JB(LabelRef(loop))
}

// simdStatsEnd stores the sums of the quadwords of the accumulators in their results,
// and whether all bytes were valid in ok.
func simdStatsEnd(valid reg.VecVirtual, names []string, accs ...reg.VecVirtual) {
	m := GP32()
	VPMOVMSKB(valid, m)
	CMPL(m, U32(0xffffffff))
	ok := GP8()
	SETEQ(ok)
	Store(ok, Return("ok"))
	for i, v := range accs {
		x, y := XMM(), XMM()
		VEXTRACTI128(U8(1), v, x)
		VPADDQ(x, v.AsX(), x)
		VPSHUFD(U8(0x4e), x, y)
		VPADDQ(y, x, x)
		r := GP64()
		MOVQ(x, r)
		Store(r, Return(names[i]))
	}
	VZEROUPPER()
	RET()
}
