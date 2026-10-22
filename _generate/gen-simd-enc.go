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
	"fmt"
	"go/token"
	"go/types"

	. "github.com/honeycombio/avo/build"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

// The assembly SIMD parsers produce what the Go parsers (simdEncoder methods) do, and the same state.
// They also add the long matches that the Go parsers leave to emitLong, if their operations fit the chunk.
// They return to Go when a chunk is full, for operations whose literals are over simdMaxLong bytes or start
// within simdMaxLen bytes of the end, for long matches that don't fit the chunk, with noOverlap for matches
// that overlap their output, and at the end of the block.
// As they are lowered to arm64, they use no CALL and no BMI2, and of the vector instructions only 128 bit moves.

const (
	// simdDebug adds INT 3 checks, which only amd64 can generate.
	simdDebug    = false
	simdChunkOps = 1 << 14
	simdMaxLong  = simdMaxLen + 1 + 255

	// The return codes, see parseAsm.
	simdAsmDone   = 0
	simdAsmFull   = 1
	simdAsmCopy   = 2
	simdAsmRepeat = 3
)

// The offsets of the simdStreams fields, which TestSIMDAsmLayout checks.
const (
	ssNTok    = 0
	ssNLL     = 8
	ssNOffc   = 16
	ssNEsc    = 24
	ssNRaw    = 32
	ssNLits   = 40
	ssNacc    = 48
	ssAcc     = 56
	ssPos     = 64
	ssRep     = 72
	ssLits    = 80
	ssOverlap = 104
	ssTok     = 105
	ssLL      = ssTok + simdChunkOps
	ssOffc    = ssTok + 2*simdChunkOps
	ssEsc     = ssTok + 3*simdChunkOps
	ssRaw     = ssTok + 5*simdChunkOps
	ssAsmSite = ssRaw + 1<<16 + 7 // raw has 1<<16+3 bytes, padded to 8
	ssAsmBase = ssAsmSite + 8
	ssAsmLen  = ssAsmSite + 16
	ssAsmOff  = ssAsmSite + 24
)

// genSIMDEnc generates the SIMD block parsers.
func genSIMDEnc() {
	fast := simdBlockParams{hashBytes: 8, minMatch: 8, skipLog: 5, skipAdd: 5, noOverlap: true}
	block := simdBlockParams{hashBytes: 6, minMatch: 4, skipLog: 6, skipAdd: 4, backMatch: true}
	genSIMDEncodeBlockAsm("encodeFastBlockSIMDAsm", 13, 4, fast)
	genSIMDEncodeBlockAsm("encodeBlockSIMDAsm", 15, 4, block)
	genSIMDEncodeBetterBlockAsm("encodeBetterBlockSIMDAsm", 17, 14, 4)
	genSIMDEncodeBlockAsm("encodeFastBlockSIMDAsm64K", 13, 2, fast)
	genSIMDEncodeBlockAsm("encodeBlockSIMDAsm64K", 14, 2, block)
	genSIMDEncodeBetterBlockAsm("encodeBetterBlockSIMDAsm64K", 16, 13, 2)
}

func init() {
	// The stubs take a *simdStreams of package minlz. Loading the package would make generating
	// depend on it type checking, so declare a stand-in: the assembly only takes the pointer.
	tn := types.NewTypeName(token.NoPos, nil, "simdStreams", nil)
	types.NewNamed(tn, types.NewStruct(nil, nil), nil)
	types.Universe.Insert(tn)
}

// simdAssert adds a check that goes to ok when it holds, with simdDebug.
func simdAssert(fn func(ok LabelRef)) {
	if !simdDebug {
		return
	}
	assertCounter++
	ok := LabelRef(fmt.Sprintf("simd_assert_%d", assertCounter))
	fn(ok)
	INT(Imm(3))
	Label(string(ok))
}

// simdEmit generates the state handling of the parsers, and their emitters: encode-simd.go's
// fast, oneOp, putOffset, putBits, putLL, putLits and putMatch, and emitLong for matches that fit the chunk,
// which must give the same results.
// The stream positions are in 8 byte slots, as pointers but for nTok, which indexes st.
// Slots are only accessed by 64 bit MOV and ALU instructions, so arm64 can keep them in registers.
// It has 9 for them, and ranks the slots by references, which the out of line code inflates:
// so sLimit is in a register, and the esc position, used the least, stays in st.
type simdEmit struct {
	name                                     string
	sLimit                                   reg.GPVirtual
	litLimit, nextEmit, repSrc               Mem // repSrc is src+1-repeat, where the repeat check loads
	nTok, ll, offc, raw, lits, acc, nacc, ov Mem
	labels                                   int
	exits                                    map[[2]int]LabelRef
	later                                    []func()
	noOverlap                                bool // as simdEncoder.noOverlap
}

func newSIMDEmit(name string) *simdEmit {
	e := &simdEmit{name: name, exits: map[[2]int]LabelRef{}}
	// On ties arm64 promotes the slot allocated first, and acc costs the least in memory.
	for _, m := range []*Mem{&e.litLimit, &e.nextEmit, &e.repSrc, &e.nTok, &e.ll, &e.offc, &e.raw, &e.lits, &e.nacc, &e.ov, &e.acc} {
		*m = AllocLocal(8)
	}
	return e
}

func (e *simdEmit) label(what string) LabelRef {
	e.labels++
	return LabelRef(fmt.Sprintf("%s_%d_%s", what, e.labels, e.name))
}

// simdField is a slot that holds field f of st. Stream pointers have the offset of their stream,
// as st holds counts.
type simdField struct {
	slot      Mem
	f, stream int
}

func (e *simdEmit) fields() []simdField {
	return []simdField{{e.nTok, ssNTok, -1}, {e.ll, ssNLL, ssLL}, {e.offc, ssNOffc, ssOffc},
		{e.raw, ssNRaw, ssRaw}, {e.acc, ssAcc, -1}, {e.nacc, ssNacc, -1}, {e.nextEmit, ssPos, -1}}
}

// load loads the state from st, and returns src and s = st.pos.
func (e *simdEmit) load() (src, s reg.GPVirtual) {
	st := Load(Param("st"), GP64())
	src = GP64()
	Load(Param("src").Base(), src)
	for _, c := range e.fields() {
		v := GP64()
		MOVQ(Mem{Base: st, Disp: c.f}, v)
		if c.stream >= 0 {
			LEAQ(Mem{Base: st, Index: v, Scale: 1, Disp: c.stream}, v)
		}
		MOVQ(v, c.slot)
	}
	v := GP64()
	LEAQ(Mem{Base: src, Disp: 1}, v)
	SUBQ(Mem{Base: st, Disp: ssRep}, v)
	MOVQ(v, e.repSrc)
	v = GP64()
	MOVQ(Mem{Base: st, Disp: ssNLits}, v)
	ADDQ(Mem{Base: st, Disp: ssLits}, v)
	MOVQ(v, e.lits)
	v = GP64()
	MOVBQZX(Mem{Base: st, Disp: ssOverlap}, v)
	MOVQ(v, e.ov)
	n := Load(Param("src").Len(), GP64())
	e.sLimit = GP64()
	LEAQ(Mem{Base: n, Disp: -8}, e.sLimit)
	LEAQ(Mem{Base: n, Disp: -simdMaxLen}, n)
	MOVQ(n, e.litLimit)
	s = GP64()
	MOVQ(Mem{Base: st, Disp: ssPos}, s)
	return src, s
}

// resume goes to sites[st.asmSite-1], or falls through for site 0.
// What the sites read must be defined before.
func (e *simdEmit) resume(sites ...LabelRef) {
	st := Load(Param("st"), GP64())
	for i, l := range sites {
		CMPQ(Mem{Base: st, Disp: ssAsmSite}, U8(i+1))
		JEQ(l)
	}
}

// exit returns code to Go, which resumes at site. An operation for Go stores base, s-base and offset
// (for copies) to asmBase, asmLen and asmOff.
func (e *simdEmit) exit(code, site int, base, s, offset reg.GPVirtual) LabelRef {
	if base == nil {
		if l, ok := e.exits[[2]int{code, site}]; ok {
			return l
		}
	}
	l := e.label(fmt.Sprintf("exit%d_%d", code, site))
	if base == nil {
		e.exits[[2]int{code, site}] = l
	}
	e.later = append(e.later, func() {
		Label(string(l))
		st := Load(Param("st"), GP64())
		if base != nil {
			MOVQ(base, Mem{Base: st, Disp: ssAsmBase})
			SUBQ(base, s)
			MOVQ(s, Mem{Base: st, Disp: ssAsmLen})
		}
		if offset != nil {
			MOVQ(offset, Mem{Base: st, Disp: ssAsmOff})
		}
		MOVQ(U32(site), Mem{Base: st, Disp: ssAsmSite})
		r, err := ReturnIndex(0).Resolve()
		if err != nil {
			panic(err)
		}
		MOVQ(U32(code), r.Addr)
		JMP(LabelRef("return_" + e.name))
	})
	return l
}

// ret generates the out of line code, and the return, which stores the state.
func (e *simdEmit) ret() {
	for i := 0; i < len(e.later); i++ {
		e.later[i]()
	}
	Label("return_" + e.name)
	st := Load(Param("st"), GP64())
	for _, c := range e.fields() {
		v := GP64()
		MOVQ(c.slot, v)
		if c.stream >= 0 {
			SUBQ(st, v)
			SUBQ(U32(c.stream), v)
		}
		MOVQ(v, Mem{Base: st, Disp: c.f})
	}
	v := GP64()
	Load(Param("src").Base(), v)
	INCQ(v)
	SUBQ(e.repSrc, v)
	MOVQ(v, Mem{Base: st, Disp: ssRep})
	v = GP64()
	MOVQ(e.lits, v)
	SUBQ(Mem{Base: st, Disp: ssLits}, v)
	MOVQ(v, Mem{Base: st, Disp: ssNLits})
	v = GP64()
	MOVQ(e.ov, v)
	MOVB(v.As8(), Mem{Base: st, Disp: ssOverlap})
	RET()
}

// fast goes to slow unless the literals from nextEmit to base pass fast in encode-simd.go, and a match from base to s
// at offset fits, and returns their lengths.
func (e *simdEmit) fast(base, s, offset reg.GPVirtual, slow LabelRef) (ll, ml reg.GPVirtual) {
	ll = GP64()
	MOVQ(base, ll)
	SUBQ(e.nextEmit, ll)
	CMPQ(ll, U32(simdMaxLong))
	JA(slow)
	ne := GP64()
	MOVQ(e.nextEmit, ne)
	CMPQ(ne, e.litLimit)
	JG(slow)
	return ll, e.fits(base, s, offset, slow)
}

// fits goes to slow unless a match from base to s at offset fits the chunk: as one operation, which passes oneOp
// in encode-simd.go, or as those that emitLong splits it into. It returns the length.
func (e *simdEmit) fits(base, s, offset reg.GPVirtual, slow LabelRef) (ml reg.GPVirtual) {
	ml = GP64()
	MOVQ(s, ml)
	SUBQ(base, ml)
	ok, split := e.label("one_op"), e.label("split")
	if e.noOverlap {
		// Go splits the matches that overlap their output: those at offsets below min(ml, simdMaxLen).
		long := e.label("long")
		CMPQ(ml, U8(simdMaxLen))
		JA(long)
		CMPQ(offset, ml)
		JB(slow)
		e.later = append(e.later, func() {
			Label(string(long))
			CMPQ(offset, U8(simdMaxLen))
			JB(slow)
			CMPQ(ml, U32(simdMaxLong))
			JA(split)
			JMP(ok)
		})
	} else {
		CMPQ(ml, U8(simdMaxLen))
		JBE(ok)
		CMPQ(ml, U32(simdMaxLong))
		JA(split)
		CMPQ(offset, U8(simdMaxLen))
		JB(split)
	}
	Label(string(ok))
	e.later = append(e.later, func() {
		// The chunk has room for n operations: of 32 bytes, or of 288 at offsets from 32.
		Label(string(split))
		n, short := GP64(), e.label("split_short")
		MOVQ(U32(simdChunkOps), n)
		SUBQ(e.nTok, n)
		if !e.noOverlap {
			CMPQ(offset, U8(simdMaxLen))
			JB(short)
		}
		LEAQ(Mem{Base: n, Index: n, Scale: 8}, n)
		Label(string(short))
		SHLQ(U8(5), n)
		CMPQ(ml, n)
		JA(slow)
		JMP(ok)
	})
	return ml
}

// putOffset adds the symbol and the raw bits of offset.
func (e *simdEmit) putOffset(offset reg.GPVirtual) {
	simdAssert(func(ok LabelRef) {
		CMPQ(e.nacc, U8(32))
		JB(ok)
	})
	// b = bits.Len(offset|8) - 4 = bits.Len(offset>>3|1) - 1 in CX, and the symbol 8*b + offset>>b.
	// The shift first saves an instruction on arm64, which folds it into the copy.
	MOVQ(offset, reg.RCX)
	SHRQ(U8(3), reg.RCX)
	ORQ(U8(1), reg.RCX)
	BSRQ(reg.RCX, reg.RCX)
	hi, sym, p := GP64(), GP64(), GP64()
	MOVQ(offset, hi)
	SHRQ(reg.CL, hi)
	LEAQ(Mem{Base: hi, Index: reg.RCX, Scale: 8}, sym)
	MOVQ(e.offc, p)
	MOVB(sym.As8(), Mem{Base: p})
	INCQ(e.offc)
	// putBits(offset - hi<<b, b), without a branch.
	SHLQ(reg.CL, hi)
	v, n := GP64(), GP64()
	MOVQ(offset, v)
	SUBQ(hi, v)
	MOVQ(e.nacc, n)
	ADDQ(reg.RCX, n)
	MOVQ(e.nacc, reg.RCX)
	SHLQ(reg.CL, v)
	ORQ(e.acc, v)
	p = GP64()
	MOVQ(e.raw, p)
	MOVL(v.As32(), Mem{Base: p})
	// n < 64, so n&32 shifts out the stored bits, and n&32>>3 is the bytes stored.
	MOVQ(n, reg.RCX)
	ANDQ(U8(32), reg.RCX)
	SHRQ(reg.CL, v)
	MOVQ(v, e.acc)
	SHRQ(U8(3), reg.RCX)
	ADDQ(reg.RCX, e.raw)
	ANDQ(U8(31), n)
	MOVQ(n, e.nacc)
	simdAssert(func(ok LabelRef) {
		st, p := Load(Param("st"), GP64()), GP64()
		MOVQ(e.raw, p)
		SUBQ(st, p)
		CMPQ(p, U32(ssRaw+simdChunkOps*20/8-4))
		JBE(ok)
	})
}

// putLits adds the literal length ll and the ll literals at nextEmit in src, as putLL and putLits, where ll is
// at most simdMaxLong and src has simdMaxLen bytes from nextEmit. It returns the literal bit of the token: 1 if ll > 0.
func (e *simdEmit) putLits(src, ll reg.GPVirtual) (lb reg.GPVirtual) {
	c, p := GP64(), GP64()
	MOVQ(U32(simdMaxLen+1), c)
	CMPQ(ll, c)
	CMOVQCS(ll, c)
	MOVQ(e.ll, p)
	MOVB(c.As8(), Mem{Base: p})
	lb = GP64()
	XORL(lb.As32(), lb.As32())
	TESTQ(ll, ll)
	SETNE(lb.As8())
	ADDQ(lb, e.ll)
	sp, dp := GP64(), GP64()
	MOVQ(e.nextEmit, sp)
	simdAssert(func(ok LabelRef) {
		st, n := Load(Param("st"), GP64()), GP64()
		MOVQ(e.lits, n)
		SUBQ(Mem{Base: st, Disp: ssLits}, n)
		CMPQ(n, sp)
		JLE(ok)
	})
	ADDQ(src, sp)
	MOVQ(e.lits, dp)
	move := func() {
		x0, x1 := XMM(), XMM()
		MOVOU(Mem{Base: sp}, x0)
		MOVOU(Mem{Base: sp, Disp: 16}, x1)
		MOVOU(x0, Mem{Base: dp})
		MOVOU(x1, Mem{Base: dp, Disp: 16})
	}
	move()
	long, done := e.label("lits_long"), e.label("lits_done")
	CMPQ(ll, U8(simdMaxLen))
	JA(long)
	Label(string(done))
	ADDQ(ll, e.lits)
	e.later = append(e.later, func() {
		// The escape, then 32 byte moves, the last one ending at ll, so only the literals are read and written.
		Label(string(long))
		c := GP64()
		LEAQ(Mem{Base: ll, Disp: -simdMaxLen - 1}, c)
		e.putEsc(c)
		LEAQ(Mem{Base: ll, Disp: -simdMaxLen}, c)
		loop, last := e.label("lits_loop"), e.label("lits_last")
		Label(string(loop))
		ADDQ(U8(simdMaxLen), sp)
		ADDQ(U8(simdMaxLen), dp)
		CMPQ(c, U8(simdMaxLen))
		JBE(last)
		move()
		SUBQ(U8(simdMaxLen), c)
		JMP(loop)
		Label(string(last))
		LEAQ(Mem{Base: sp, Index: c, Scale: 1, Disp: -simdMaxLen}, sp)
		LEAQ(Mem{Base: dp, Index: c, Scale: 1, Disp: -simdMaxLen}, dp)
		move()
		JMP(done)
	})
	return lb
}

// putEsc adds the escape c.
func (e *simdEmit) putEsc(c reg.GPVirtual) {
	st, n := Load(Param("st"), GP64()), GP64()
	MOVQ(Mem{Base: st, Disp: ssNEsc}, n)
	MOVB(c.As8(), Mem{Base: st, Index: n, Scale: 1, Disp: ssEsc})
	INCQ(n)
	MOVQ(n, Mem{Base: st, Disp: ssNEsc})
}

// setRepeat sets the repeat offset.
func (e *simdEmit) setRepeat(src, offset reg.GPVirtual) {
	r := GP64()
	LEAQ(Mem{Base: src, Disp: 1}, r)
	SUBQ(offset, r)
	MOVQ(r, e.repSrc)
}

// putTok adds the token t, and returns nTok.
func (e *simdEmit) putTok(t reg.GPVirtual) reg.GPVirtual {
	st, n := Load(Param("st"), GP64()), GP64()
	MOVQ(e.nTok, n)
	simdAssert(func(ok LabelRef) {
		CMPQ(n, U32(simdChunkOps))
		JB(ok)
	})
	// arm64 can't add a displacement to an indexed store, so this costs an ADD there.
	// A register holding st+ssTok, or tok at offset 0, would be preferable on arm64.
	MOVB(t.As8(), Mem{Base: st, Index: n, Scale: 1, Disp: ssTok})
	INCQ(n)
	MOVQ(n, e.nTok)
	return n
}

// putMatch adds the token with the bits t and a match of ml bytes at offset, which fits. It sets nextEmit to s,
// and goes to full when the chunk is full. The repeat offset must be set before.
func (e *simdEmit) putMatch(t, offset, ml, s reg.GPVirtual, full LabelRef) {
	long, tok := e.label("match_long"), e.label("match_tok")
	CMPQ(ml, U8(simdMaxLen))
	JA(long)
	LEAQ(Mem{Base: t, Index: ml, Scale: 4}, t)
	if !e.noOverlap {
		o := GP64()
		XORL(o.As32(), o.As32())
		CMPQ(offset, ml)
		SETCS(o.As8())
		ORQ(o, e.ov)
	}
	Label(string(tok))
	n := e.putTok(t)
	MOVQ(s, e.nextEmit)
	CMPQ(n, U32(simdChunkOps))
	JEQ(full)
	e.later = append(e.later, func() {
		// As emitLong: operations of 32 bytes, or of 288 at offsets from 32, then one of the rest, the others at the repeat offset.
		// The loops stay here, as arm64 ranks slots by the loops that enclose their uses.
		Label(string(long))
		esc, short, big, rest := e.label("match_esc"), e.label("match_short"), e.label("match_big"), e.label("match_rest")
		if !e.noOverlap {
			CMPQ(offset, U8(simdMaxLen))
			JB(short)
		}
		CMPQ(ml, U32(simdMaxLong))
		JA(big)
		Label(string(esc))
		c := GP64()
		LEAQ(Mem{Base: ml, Disp: -simdMaxLen - 1}, c)
		e.putEsc(c)
		ORQ(U32((simdMaxLen+1)<<2), t)
		JMP(tok)

		Label(string(big))
		c = GP64()
		MOVQ(U32(simdMaxLong-simdMaxLen-1), c)
		e.putEsc(c)
		ORQ(U32((simdMaxLen+1)<<2), t)
		e.putTok(t)
		MOVQ(U32(1), t)
		SUBQ(U32(simdMaxLong), ml)
		CMPQ(ml, U32(simdMaxLong))
		JA(big)
		CMPQ(ml, U8(simdMaxLen))
		JA(esc)
		if !e.noOverlap {
			JMP(rest)

			// The first operation overlaps.
			Label(string(short))
			ORQ(U8(1), e.ov)
			loop := e.label("match_short_loop")
			Label(string(loop))
			ORQ(U32(simdMaxLen<<2), t)
			e.putTok(t)
			MOVQ(U32(1), t)
			SUBQ(U8(simdMaxLen), ml)
			CMPQ(ml, U8(simdMaxLen))
			JA(loop)
		}
		Label(string(rest))
		LEAQ(Mem{Base: t, Index: ml, Scale: 4}, t)
		JMP(tok)
	})
}

// extend adds 8 to s and cand while the 8 bytes at s and cand match and s <= sLimit,
// then the matching bytes of the last 8: the extension loops of the Go parsers, which have no tail.
// With tail, it then adds the bytes that match up to the end of src, as matchAt.
func (e *simdEmit) extend(src, s, cand reg.GPVirtual, tail bool) {
	loop, check, end := e.label("ext_loop"), e.label("ext_check"), e.label("ext_end")
	over := end
	if tail {
		over = e.label("ext_tail")
		e.later = append(e.later, func() {
			Label(string(over))
			n, a, b := GP64(), GP64(), GP64()
			Load(Param("src").Len(), n)
			next := e.label("ext_tail_loop")
			Label(string(next))
			CMPQ(s, n)
			JGE(end)
			MOVBQZX(Mem{Base: src, Index: s, Scale: 1}, a)
			MOVBQZX(Mem{Base: src, Index: cand, Scale: 1}, b)
			CMPQ(a, b)
			JNE(end)
			INCQ(s)
			INCQ(cand)
			JMP(next)
		})
	}
	x := GP64()
	JMP(check)
	// At 32 bytes the loop stays within a 64 byte line: straddling one made it up to 34% slower on Zen 5.
	PCALIGN(Imm(32))
	Label(string(loop))
	ADDQ(U8(8), s)
	ADDQ(U8(8), cand)
	Label(string(check))
	CMPQ(s, e.sLimit)
	JG(over)
	// The difference has its lowest set bit where the XOR has, and SUB sets the flags on arm64 too.
	MOVQ(Mem{Base: src, Index: s, Scale: 1}, x)
	SUBQ(Mem{Base: src, Index: cand, Scale: 1}, x)
	JZ(loop)
	TZCNTQ(x, x)
	SHRQ(U8(3), x)
	ADDQ(x, s)
	Label(string(end))
	simdAssert(func(ok LabelRef) {
		n := GP64()
		Load(Param("src").Len(), n)
		CMPQ(s, n)
		JBE(ok)
	})
}

// matchBack extends the match at s of cand back, while cand > 0, s > nextEmit and the bytes before them match.
func (e *simdEmit) matchBack(src, s, cand reg.GPVirtual) {
	ne := GP64()
	MOVQ(e.nextEmit, ne)
	back, backEnd := e.label("match_back"), e.label("match_back_end")
	Label(string(back))
	TESTQ(cand, cand)
	JLE(backEnd)
	CMPQ(s, ne)
	JLE(backEnd)
	a, b := GP64(), GP64()
	MOVBQZX(Mem{Base: src, Index: cand, Scale: 1, Disp: -1}, a)
	MOVBQZX(Mem{Base: src, Index: s, Scale: 1, Disp: -1}, b)
	CMPQ(a, b)
	JNE(backEnd)
	DECQ(cand)
	DECQ(s)
	JMP(back)
	Label(string(backEnd))
}

// repeatBack returns the start of the repeat at s+1, extended back as matchBack does, and the repeat offset.
func (e *simdEmit) repeatBack(src, s reg.GPVirtual) (base, rep reg.GPVirtual) {
	base, rep = GP64(), GP64()
	ne, i := GP64(), GP64()
	LEAQ(Mem{Base: s, Disp: 1}, base)
	LEAQ(Mem{Base: src, Disp: 1}, rep)
	SUBQ(e.repSrc, rep)
	MOVQ(e.nextEmit, ne)
	MOVQ(base, i)
	SUBQ(rep, i)
	back, backEnd := e.label("rep_back"), e.label("rep_back_end")
	Label(string(back))
	CMPQ(base, ne)
	JLE(backEnd)
	TESTQ(i, i)
	JLE(backEnd)
	a, b := GP64(), GP64()
	MOVBQZX(Mem{Base: src, Index: i, Scale: 1, Disp: -1}, a)
	MOVBQZX(Mem{Base: src, Index: base, Scale: 1, Disp: -1}, b)
	CMPQ(a, b)
	JNE(backEnd)
	DECQ(i)
	DECQ(base)
	JMP(back)
	Label(string(backEnd))
	return base, rep
}

// simdBlockParams holds what the SuperFast and Fastest Go parsers differ in.
type simdBlockParams struct {
	hashBytes        int  // bytes hashed
	minMatch         int  // 8: 8 byte loads at s, s+1 and s+2 are compared; 4: the low 4 bytes of cv, cv>>8 and cv>>16
	skipLog, skipAdd int  // the search steps to s + (s-nextEmit)>>skipLog + skipAdd
	backMatch        bool // matches are extended back
	noOverlap        bool // as simdEncoder.noOverlap
}

// genSIMDEncodeBlockAsm generates name, the parser of encodeFastBlockGo or encodeBlockGo, or of their 64K versions,
// with 1<<tableBits table entries of entryBytes.
func genSIMDEncodeBlockAsm(name string, tableBits, entryBytes int, p simdBlockParams) {
	TEXT(name, 0, fmt.Sprintf("func(st *simdStreams, src []byte, tab *[%d]byte) int", entryBytes<<tableBits))
	Doc(name+" parses src into st until it returns to Go, see parseAsm.", "")
	Pragma("noescape")

	e := newSIMDEmit(name)
	e.noOverlap = p.noOverlap
	search, post := LabelRef("search_"+name), LabelRef("post_"+name)
	src, s := e.load()
	table := regTable{r: GP64(), scale: uint8(entryBytes)}
	Load(Param("tab"), table.r)
	hasher := hashN(options{}, p.hashBytes, tableBits)
	done := e.exit(simdAsmDone, 0, nil, nil, nil)
	e.resume(post)

	// Site 0, the start of the block or the search after a repeat: s = max(pos, 1).
	one := GP64()
	MOVQ(U32(1), one)
	CMPQ(s, one)
	CMOVQLT(one, s)
	CMPQ(s, e.sLimit)
	JGE(done)
	cv := GP64()
	MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)

	// at returns the bytes at s+i to compare.
	at := func(i int) reg.GPVirtual {
		r := GP64()
		if p.minMatch == 8 {
			MOVQ(Mem{Base: src, Index: s, Scale: 1, Disp: i}, r)
		} else {
			MOVQ(cv, r)
			SHRQ(U8(8*i), r)
		}
		return r
	}
	cmp := func(cand, v reg.GPVirtual) {
		if p.minMatch == 8 {
			CMPQ(Mem{Base: src, Index: cand, Scale: 1}, v)
		} else {
			CMPL(Mem{Base: src, Index: cand, Scale: 1}, v.As32())
		}
	}
	hash := func(v reg.GPVirtual) reg.GPVirtual {
		h := GP64()
		MOVQ(v, h)
		hasher.hash(h)
		return h
	}
	before := func(cand, s reg.GPVirtual) {
		simdAssert(func(ok LabelRef) {
			CMPQ(cand, s)
			JB(ok)
		})
	}

	// 64 also aligns the function to 64, which fixes where its loops fall in cache lines: Go aligns it to 32 else,
	// and the other half line made the same code up to 4% slower on Zen 5, depending on what moved before it.
	PCALIGN(Imm(64))
	Label(string(search))
	cand := GP64()
	{
		nextS := GP64()
		MOVQ(s, nextS)
		SUBQ(e.nextEmit, nextS)
		SHRQ(U8(p.skipLog), nextS)
		LEAQ(Mem{Base: s, Index: nextS, Scale: 1, Disp: p.skipAdd}, nextS)
		CMPQ(nextS, e.sLimit)
		JG(done)
		h0 := hash(cv)
		cv1 := at(1)
		h1 := hash(cv1)
		cand2, s1 := GP64(), GP64()
		table.LoadIdx(h0, cand)
		table.LoadIdx(h1, cand2)
		table.SaveIdx(s, h0)
		LEAQ(Mem{Base: s, Disp: 1}, s1)
		table.SaveIdx(s1, h1)
		cv2 := at(2)
		h2 := hash(cv2)

		// The repeat at s+1.
		r := GP64()
		MOVQ(e.repSrc, r)
		CMPL(Mem{Base: r, Index: s, Scale: 1}, cv1.As32())
		JEQ(LabelRef("repeat_" + name))

		before(cand, s)
		cmp(cand, cv)
		JEQ(LabelRef("match_" + name))
		table.LoadIdx(h2, cand)
		before(cand2, s1)
		cmp(cand2, cv1)
		JEQ(LabelRef("match1_" + name))
		LEAQ(Mem{Base: s, Disp: 2}, s1)
		table.SaveIdx(s1, h2)
		before(cand, s1)
		cmp(cand, cv2)
		JEQ(LabelRef("match2_" + name))
		MOVQ(Mem{Base: src, Index: nextS, Scale: 1}, cv)
		MOVQ(nextS, s)
		JMP(search)

		Label("match1_" + name)
		LEAQ(Mem{Base: s, Disp: 2}, s1)
		table.SaveIdx(s1, h2)
		MOVQ(cand2, cand)
		INCQ(s)
		JMP(LabelRef("match_" + name))
		Label("match2_" + name)
		ADDQ(U8(2), s)
		JMP(LabelRef("match_" + name))
	}

	Label("match_" + name)
	{
		if p.backMatch {
			e.matchBack(src, s, cand)
		}
		base, off := GP64(), GP64()
		MOVQ(s, base)
		MOVQ(s, off)
		SUBQ(cand, off)
		ADDQ(U8(p.minMatch), s)
		ADDQ(U8(p.minMatch), cand)
		e.extend(src, s, cand, false)
		ll, ml := e.fast(base, s, off, e.exit(simdAsmCopy, 1, base, s, off))
		e.putOffset(off)
		lb := e.putLits(src, ll)
		ADDQ(lb, lb)
		e.setRepeat(src, off)
		e.putMatch(lb, off, ml, s, e.exit(simdAsmFull, 1, nil, nil, nil))
	}

	// Site 1, after a copy: matches at s, which is nextEmit.
	Label(string(post))
	{
		CMPQ(s, e.sLimit)
		JGE(done)
		sm2, x0, x := GP64(), GP64(), GP64()
		LEAQ(Mem{Base: s, Disp: -2}, sm2)
		MOVQ(Mem{Base: src, Index: sm2, Scale: 1}, x0)
		hasher.hash(x0)
		MOVQ(Mem{Base: src, Index: s, Scale: 1}, x)
		h := hash(x)
		table.LoadIdx(h, cand)
		table.SaveIdx(sm2, x0)
		table.SaveIdx(s, h)
		before(cand, s)
		cmp(cand, x)
		JNE(LabelRef("post_none_" + name))
		base, off := GP64(), GP64()
		MOVQ(s, base)
		MOVQ(s, off)
		SUBQ(cand, off)
		ADDQ(U8(p.minMatch), s)
		ADDQ(U8(p.minMatch), cand)
		e.extend(src, s, cand, false)
		ml := e.fits(base, s, off, e.exit(simdAsmCopy, 1, base, s, off))
		e.putOffset(off)
		t := GP64()
		XORL(t.As32(), t.As32())
		e.setRepeat(src, off)
		e.putMatch(t, off, ml, s, e.exit(simdAsmFull, 1, nil, nil, nil))
		JMP(post)

		Label("post_none_" + name)
		INCQ(s)
		MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)
		JMP(search)
	}

	// The repeat comes last, which makes arm64 keep the ll position in a register rather than acc.
	Label("repeat_" + name)
	{
		base, rep := e.repeatBack(src, s)
		ADDQ(U8(5), s) // past the 4 bytes that the repeat check compared
		MOVQ(s, cand)
		SUBQ(rep, cand)
		e.extend(src, s, cand, false)
		ll, ml := e.fast(base, s, rep, e.exit(simdAsmRepeat, 0, base, s, nil))
		lb := e.putLits(src, ll)
		t := GP64()
		LEAQ(Mem{Base: lb, Index: lb, Scale: 1, Disp: 1}, t)
		e.putMatch(t, rep, ml, s, e.exit(simdAsmFull, 0, nil, nil, nil))
		CMPQ(s, e.sLimit)
		JGE(done)
		MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)
		JMP(search)
	}
	e.ret()
}

// genSIMDEncodeBetterBlockAsm generates name, the parser of encodeBlockBetterGo or encodeBlockBetterGo64K, with a long table of 1<<lTableBits
// and a short table of 1<<sTableBits entries of entryBytes. It resumes only at the search, as it indexes the interior
// of a match before it adds the match or leaves it to Go. The Go parser indexes after, which ends the same,
// as indexing touches only the tables.
func genSIMDEncodeBetterBlockAsm(name string, lTableBits, sTableBits, entryBytes int) {
	lSize := entryBytes << lTableBits
	TEXT(name, 0, fmt.Sprintf("func(st *simdStreams, src []byte, tab *[%d]byte, maxOff4, maxOff5 int) int", lSize+entryBytes<<sTableBits))
	Doc(name+" parses src into st until it returns to Go, see parseAsm.", "")
	Pragma("noescape")

	e := newSIMDEmit(name)
	search, match, repeat := LabelRef("search_"+name), LabelRef("match_"+name), LabelRef("repeat_"+name)
	src, s := e.load()
	// The short table follows the long one. It has a register of its own, as arm64 folds no offset into an indexed access.
	// next loads both again, so they hold no registers while the emitters run, which need them.
	lTab, sTab := regTable{r: GP64(), scale: uint8(entryBytes)}, regTable{r: GP64(), scale: uint8(entryBytes)}
	tables := func() {
		Load(Param("tab"), lTab.r)
		LEAQ(Mem{Base: lTab.r, Disp: lSize}, sTab.r)
	}
	tables()
	lHash, sHash := hashN(options{}, 7, lTableBits), hashN(options{}, 4, sTableBits)
	done := e.exit(simdAsmDone, 0, nil, nil, nil)

	// The start of the block, or the search after an operation: s = max(pos, 1).
	one := GP64()
	MOVQ(U32(1), one)
	CMPQ(s, one)
	CMOVQLT(one, s)
	CMPQ(s, e.sLimit)
	JGE(done)
	cv := GP64()
	MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)

	before := func(cand, s reg.GPVirtual) {
		simdAssert(func(ok LabelRef) {
			CMPQ(cand, s)
			JB(ok)
		})
	}
	// next searches on from s, unless the block is done.
	next := func() {
		CMPQ(s, e.sLimit)
		JGE(done)
		MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)
		tables()
		JMP(search)
	}
	// cut leaves the last byte of a match of simdMaxLen+1 bytes from base to s as a literal, as the Go parser does.
	cut := func(base, s reg.GPVirtual) {
		l, back := e.label("cut"), e.label("cut_back")
		t := GP64()
		LEAQ(Mem{Base: base, Disp: simdMaxLen + 1}, t)
		CMPQ(s, t)
		JEQ(l)
		Label(string(back))
		e.later = append(e.later, func() {
			Label(string(l))
			DECQ(s)
			JMP(back)
		})
	}
	// put indexes i in the long table, and i+1 in the short one.
	put := func(i reg.GPVirtual) {
		h, h1, i1 := GP64(), GP64(), GP64()
		MOVQ(Mem{Base: src, Index: i, Scale: 1}, h)
		lHash.hash(h)
		lTab.SaveIdx(i, h)
		MOVL(Mem{Base: src, Index: i, Scale: 1, Disp: 1}, h1.As32())
		sHash.hash(h1)
		LEAQ(Mem{Base: i, Disp: 1}, i1)
		sTab.SaveIdx(i1, h1)
	}
	putLong := func(i reg.GPVirtual) {
		h := GP64()
		MOVQ(Mem{Base: src, Index: i, Scale: 1}, h)
		lHash.hash(h)
		lTab.SaveIdx(i, h)
	}

	PCALIGN(Imm(64)) // as in genSIMDEncodeBlockAsm
	Label(string(search))
	cand, nextS := GP64(), GP64()
	{
		// nextS = s + min(100, (s-nextEmit)>>7 + 1)
		c := GP64()
		MOVQ(s, nextS)
		SUBQ(e.nextEmit, nextS)
		SHRQ(U8(7), nextS)
		MOVQ(U32(99), c)
		CMPQ(nextS, c)
		CMOVQGT(c, nextS)
		LEAQ(Mem{Base: s, Index: nextS, Scale: 1, Disp: 1}, nextS)
		CMPQ(nextS, e.sLimit)
		JG(done)
		hl, hs, cs := GP64(), GP64(), GP64()
		MOVQ(cv, hl)
		lHash.hash(hl)
		MOVQ(cv, hs)
		sHash.hash(hs)
		lTab.LoadIdx(hl, cand)
		sTab.LoadIdx(hs, cs)
		lTab.SaveIdx(s, hl)
		sTab.SaveIdx(s, hs)
		before(cand, s)
		before(cs, s)
		vl := GP64()
		MOVQ(Mem{Base: src, Index: cand, Scale: 1}, vl)
		CMPQ(vl, cv)
		JEQ(match)
		// The repeat at s+1.
		cv1, r := GP64(), GP64()
		MOVQ(cv, cv1)
		SHRQ(U8(8), cv1)
		MOVQ(e.repSrc, r)
		CMPL(Mem{Base: r, Index: s, Scale: 1}, cv1.As32())
		JEQ(repeat)
		CMPL(vl.As32(), cv.As32())
		JEQ(match)
		CMPL(Mem{Base: src, Index: cs, Scale: 1}, cv.As32())
		JEQ(LabelRef("short_" + name))
		MOVQ(Mem{Base: src, Index: nextS, Scale: 1}, cv)
		MOVQ(nextS, s)
		JMP(search)

		// The short candidate matches at s, and a long one may at s+1.
		Label("short_" + name)
		h, s1 := GP64(), GP64()
		MOVQ(cv1, h)
		lHash.hash(h)
		lTab.LoadIdx(h, cand)
		LEAQ(Mem{Base: s, Disp: 1}, s1)
		lTab.SaveIdx(s1, h)
		before(cand, s1)
		CMPL(Mem{Base: src, Index: cand, Scale: 1}, cv1.As32())
		JNE(LabelRef("short_only_" + name))
		MOVQ(s1, s)
		JMP(match)
		Label("short_only_" + name)
		MOVQ(cs, cand)
	}

	Label(string(match))
	{
		e.matchBack(src, s, cand)
		base, off := GP64(), GP64()
		MOVQ(s, base)
		MOVQ(s, off)
		SUBQ(cand, off)
		ADDQ(U8(4), s)
		ADDQ(U8(4), cand)
		e.extend(src, s, cand, true)
		// A match of 4 or 5 bytes at an offset over maxOff4 or maxOff5, other than the repeat offset, is passed over:
		// the search goes on at nextS+1. The length goes first, as most matches are longer.
		short, near := e.label("short"), e.label("near")
		t := GP64()
		LEAQ(Mem{Base: base, Disp: 5}, t)
		CMPQ(s, t)
		JBE(short)
		Label(string(near))
		e.later = append(e.later, func() {
			Label(string(short))
			m, five := GP64(), e.label("five")
			Load(Param("maxOff5"), m)
			CMPQ(s, t)
			JEQ(five)
			Load(Param("maxOff4"), m)
			Label(string(five))
			CMPQ(off, m)
			JBE(near)
			LEAQ(Mem{Base: src, Disp: 1}, t)
			SUBQ(e.repSrc, t)
			CMPQ(t, off)
			JEQ(near)
			LEAQ(Mem{Base: nextS, Disp: 1}, s)
			next()
		})
		cut(base, s)
		indexed := e.label("indexed")
		CMPQ(s, e.sLimit)
		JGE(indexed)
		// The two ends, then every other long entry from base+2 and from the middle.
		i0, i1, i2 := GP64(), GP64(), GP64()
		LEAQ(Mem{Base: base, Disp: 1}, i0)
		LEAQ(Mem{Base: s, Disp: -2}, i1)
		put(i0)
		put(i1)
		INCQ(i0)
		DECQ(i1)
		LEAQ(Mem{Base: i0, Index: i1, Scale: 1, Disp: 1}, i2)
		SHRQ(U8(1), i2)
		loop := e.label("index_loop")
		Label(string(loop))
		CMPQ(i2, i1)
		JGE(indexed)
		putLong(i0)
		putLong(i2)
		ADDQ(U8(2), i0)
		ADDQ(U8(2), i2)
		JMP(loop)
		Label(string(indexed))

		ll, ml := e.fast(base, s, off, e.exit(simdAsmCopy, 0, base, s, off))
		e.putOffset(off)
		lb := e.putLits(src, ll)
		ADDQ(lb, lb)
		e.setRepeat(src, off)
		e.putMatch(lb, off, ml, s, e.exit(simdAsmFull, 0, nil, nil, nil))
		next()
	}

	Label(string(repeat))
	{
		base, rep := e.repeatBack(src, s)
		ADDQ(U8(5), s) // past the 4 bytes that the repeat check compared
		MOVQ(s, cand)
		SUBQ(rep, cand)
		e.extend(src, s, cand, true)
		cut(base, s)
		indexed := e.label("indexed")
		CMPQ(s, e.sLimit)
		JGE(indexed)
		// Both ends, every other position, towards the middle.
		i0, i1 := GP64(), GP64()
		LEAQ(Mem{Base: base, Disp: 1}, i0)
		LEAQ(Mem{Base: s, Disp: -2}, i1)
		loop := e.label("index_loop")
		Label(string(loop))
		CMPQ(i0, i1)
		JGE(indexed)
		put(i0)
		put(i1)
		ADDQ(U8(2), i0)
		SUBQ(U8(2), i1)
		JMP(loop)
		Label(string(indexed))

		ll, ml := e.fast(base, s, rep, e.exit(simdAsmRepeat, 0, base, s, nil))
		lb := e.putLits(src, ll)
		t := GP64()
		LEAQ(Mem{Base: lb, Index: lb, Scale: 1, Disp: 1}, t)
		e.putMatch(t, rep, ml, s, e.exit(simdAsmFull, 0, nil, nil, nil))
		next()
	}
	e.ret()
}
