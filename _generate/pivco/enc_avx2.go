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

// AVX2 encoder kernels: gather, the three partitions and the flat pack. They port
// upstream's x86 tier as built with PIVCO_HAS_AVX2 and without AVX-512
// (pivco_huffman_primitives_x86.h, pivco_huffman_x86_vertical.h, pivco_huffman_avx2_pack.h):
// the "2tab" prim_enc_init, part_full_x86 (p16rev), part_core_x86 and prim_enc_pack_dN
// (vert512_pack_ymm_d2..6, vert512_pack_x86_d7, vert_pack_x86_d*, pack_dN_x86).
//
// Requirements: AVX2, BMI2, POPCNT (kernel.hasAVX2). No EVEX encodings, opmasks or
// X16..X31 (avx2_test.go scans the output).
//
// Differences from C:
//   - Reads of src and ranks are exact, and so are the in-place left stores of partFull
//     and every store to the packed region: the tails run the vector step on the last
//     ranks shifted down from a load that ends at n (or on an exact small load when
//     n < 16) and store through exact 8/4/2/1-byte stores. C ends with scalar loops.
//   - The partitions build their masks with VPMAXUB/VPCMPEQB, which also covers thr = 255.
//   - The gather returns the largest rank, from a VPMAXUB pass over the stored ranks.
//   - The natural packs subtract base with VPSUBUSB, so the zero lanes of a short tail
//     stay zero and give zero padding bits.

import (
	"fmt"

	. "github.com/honeycombio/avo/build"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

var et = newBlob("avx2EncTab")

// Offsets of the encoder tables in et.
var etP16A, etP16B, etCtabR, etPreR, etShift, etRev int

func genEncAVX2() {
	encTables()
	genGatherAVX2()
	genPartAVX2("partFullAVX2Asm", partFull)
	genPartAVX2("partRightAVX2Asm", partRight)
	genPartAVX2("partNoneAVX2Asm", partNone)
	genPackAVX2()
	et.emit()
}

// encTables builds the partition tables of x86_build_tabs (primitives_x86.h:907-952).
// x86_pc8 and x86_ctab_l/pre_l are not needed: POPCNT, and no left compaction by table.
func encTables() {
	var tabA, tabB0, ctab []byte
	for m := range 256 {
		// tabA: lefts forward from lane 0, rights backward from 15; tabB0 the same for
		// lanes 8..15 at pc0 = 0 (loaded at +pc0); ctab: rights first, 0x80 fill.
		var a [16]byte
		var b [32]byte
		c := [16]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
		al, ar, bl, br, nr := 0, 15, 8, 15, 0
		for k := range 8 {
			if m>>k&1 != 0 {
				a[ar], b[br], c[nr] = byte(k), byte(8+k), byte(k)
				ar, br, nr = ar-1, br-1, nr+1
			} else {
				a[al], b[bl] = byte(k), byte(8+k)
				al, bl = al+1, bl+1
			}
		}
		tabA, tabB0, ctab = append(tabA, a[:]...), append(tabB0, b[:]...), append(ctab, c[:]...)
	}
	etP16A, etP16B, etCtabR = et.add(tabA, 64), et.add(tabB0, 64), et.add(ctab, 64)
	var pre []byte
	for nlo := range 9 {
		for m := range 256 {
			var r [16]byte
			for i := range r {
				r[i] = 0x80
			}
			p := nlo
			for k := range 8 {
				if m>>k&1 != 0 {
					r[p], p = byte(8+k), p+1
				}
			}
			pre = append(pre, r[:]...)
		}
	}
	etPreR = et.add(pre, 64)
	etShift = et.shiftOff()
	var rev [16]byte
	for i := range rev {
		rev[i] = byte(15 - i)
	}
	etRev = et.add(rev[:], 16)
}

// genGatherAVX2 ports prim_enc_init's 2tab loop (primitives_x86.h:1284-1314): each pair of
// symbols is one u16 store of s2r[a] + hi[b], hi[s] = s2r[s]<<8 in a stack table built per
// call (C keeps it in the table as enc_init_hi). The AH/BH reads need the legacy registers.
func genGatherAVX2() {
	TEXT("gatherAVX2Asm", NOSPLIT, "func(ranks, src *byte, n int, s2r *[256]byte) uint8")
	Pragma("noescape")
	Doc("gatherAVX2Asm sets ranks[i] = s2r[src[i]] for i < n > 0 and returns the largest of them.")
	hi := AllocLocal(512)
	ranks, src, n, s2r, i, lim := reg.R8, reg.R9, reg.R10, reg.R11, reg.R12, reg.R13
	Load(Param("ranks"), ranks)
	Load(Param("src"), src)
	Load(Param("n"), n)
	Load(Param("s2r"), s2r)
	for k := range 16 {
		VPMOVZXBW(Mem{Base: s2r, Disp: 16 * k}, reg.Y0)
		VPSLLW(U8(8), reg.Y0, reg.Y0)
		VMOVDQU(reg.Y0, hi.Offset(32*k))
	}
	XORL(i.As32(), i.As32())
	MOVQ(n, lim)
	SUBQ(U8(16), lim)
	JB(LabelRef("gather_tail"))
	PCALIGN(U8(64))
	Label("gather_loop")
	MOVQ(Mem{Base: src, Index: i, Scale: 1}, reg.RAX)
	MOVQ(Mem{Base: src, Index: i, Scale: 1, Disp: 8}, reg.RBX)
	type chain struct {
		q, lo, hi, t0, t1 reg.GPPhysical
		disp              int
	}
	chains := []chain{{reg.RAX, reg.AL, reg.AH, reg.RCX, reg.RDX, 0}, {reg.RBX, reg.BL, reg.BH, reg.RSI, reg.RDI, 8}}
	for p := range 4 {
		for _, c := range chains {
			MOVBLZX(c.lo, c.t0.As32())
			MOVBLZX(c.hi, c.t1.As32())
			MOVBLZX(Mem{Base: s2r, Index: c.t0, Scale: 1}, c.t0.As32())
			h := hi
			h.Index, h.Scale = c.t1, 2
			ADDW(h, c.t0.As16())
			MOVW(c.t0.As16(), Mem{Base: ranks, Index: i, Scale: 1, Disp: c.disp + 2*p})
			if p < 3 {
				SHRQ(U8(16), c.q)
			}
		}
	}
	ADDQ(U8(16), i)
	CMPQ(i, lim)
	JBE(LabelRef("gather_loop"))
	Label("gather_tail")
	CMPQ(i, n)
	JAE(LabelRef("gather_max"))
	Label("gather_byte")
	MOVBLZX(Mem{Base: src, Index: i, Scale: 1}, reg.EAX)
	MOVBLZX(Mem{Base: s2r, Index: reg.RAX, Scale: 1}, reg.EAX)
	MOVB(reg.AL, Mem{Base: ranks, Index: i, Scale: 1})
	INCQ(i)
	CMPQ(i, n)
	JB(LabelRef("gather_byte"))

	// The maximum of ranks[:n]; the last 32-byte load overlaps the loop's.
	Label("gather_max")
	CMPQ(n, U8(32))
	JB(LabelRef("gather_smax"))
	VPXOR(reg.Y0, reg.Y0, reg.Y0)
	XORL(i.As32(), i.As32())
	MOVQ(n, lim)
	SUBQ(U8(32), lim)
	Label("gather_vmax")
	VPMAXUB(Mem{Base: ranks, Index: i, Scale: 1}, reg.Y0, reg.Y0)
	ADDQ(U8(32), i)
	CMPQ(i, lim)
	JBE(LabelRef("gather_vmax"))
	VPMAXUB(Mem{Base: ranks, Index: n, Scale: 1, Disp: -32}, reg.Y0, reg.Y0)
	VEXTRACTI128(U8(1), reg.Y0, reg.X1)
	VPMAXUB(reg.X1, reg.X0, reg.X0)
	for _, s := range []int{8, 4, 2, 1} {
		VPSRLDQ(U8(s), reg.X0, reg.X1)
		VPMAXUB(reg.X1, reg.X0, reg.X0)
	}
	VMOVD(reg.X0, reg.EAX)
	Store(reg.AL, ReturnIndex(0))
	VZEROUPPER()
	RET()

	Label("gather_smax")
	XORL(reg.EAX, reg.EAX)
	XORL(i.As32(), i.As32())
	Label("gather_sloop")
	MOVBLZX(Mem{Base: ranks, Index: i, Scale: 1}, reg.ECX)
	CMPL(reg.ECX, reg.EAX)
	CMOVLHI(reg.ECX, reg.EAX)
	INCQ(i)
	CMPQ(i, n)
	JB(LabelRef("gather_sloop"))
	Store(reg.AL, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

type partKind int

const (
	partFull partKind = iota
	partRight
	partNone
)

// genPartAVX2 emits a partition by rank > thr, 32 ranks per step with a 16-rank step and a
// tail after it (part_full_x86 and part_core_x86, primitives_x86.h:997-1109).
func genPartAVX2(name string, kind partKind) {
	if kind == partNone {
		TEXT(name, NOSPLIT, "func(ranks, bm *byte, n int, thr uint8) int")
	} else {
		TEXT(name, NOSPLIT, "func(ranks, right, bm *byte, n int, thr uint8) int")
	}
	Pragma("noescape")
	Doc(name + " partitions ranks[:n], n > 0, by rank > thr; see Tier.PartFull.")
	src, start := GP64(), GP64()
	Load(Param("ranks"), src)
	MOVQ(src, start)
	var rb reg.GPVirtual
	if kind != partNone {
		rb = GP64()
		Load(Param("right"), rb)
	}
	bm, n := GP64(), GP64()
	Load(Param("bm"), bm)
	Load(Param("n"), n)
	t := GP32()
	Load(Param("thr"), t)
	thr := YMM()
	VMOVD(t, thr.AsX())
	VPBROADCASTB(thr.AsX(), thr)
	tb := GP64()
	LEAQ(et.mem(), tb)
	nr, nl := GP64(), GP64()
	XORL(nr.As32(), nr.As32())
	XORL(nl.As32(), nl.As32())
	var rev reg.VecVirtual
	if kind == partFull {
		rev = XMM()
		VMOVDQU(Mem{Base: tb, Disp: etRev}, rev)
	}
	// The last 16 ranks, read before any in-place store; the tail shifts them down.
	last := XMM()
	CMPQ(n, U8(16))
	JB(LabelRef(name + "_go"))
	VMOVDQU(Mem{Base: start, Index: n, Scale: 1, Disp: -16}, last)
	Label(name + "_go")

	// mask returns the rank > thr bits of the lanes of v (zero lanes give 0).
	mask := func(v Op, wide bool) reg.GPVirtual {
		c := YMM()
		m := GP32()
		if wide {
			VPMAXUB(v, thr, c)
			VPCMPEQB(thr, c, c)
			VPMOVMSKB(c, m)
			NOTL(m)
		} else {
			VPMAXUB(v, thr.AsX(), c.AsX())
			VPCMPEQB(thr.AsX(), c.AsX(), c.AsX())
			VPMOVMSKB(c.AsX(), m)
			XORL(U32(0xFFFF), m)
		}
		return m
	}
	// rot extracts the byte of m at bit sh, times 1<<sc (RORX + AND).
	rot := func(m reg.GPVirtual, sh, sc int) reg.GPVirtual {
		r := GP32()
		RORXL(U8((sh-sc+32)%32), m, r)
		ANDL(U32(0xFF<<sc), r)
		return r
	}
	// p16 compacts the 16 lanes of v by the mask bits sh..sh+15 of m: lefts in order
	// to lout (or through exactLeft), rights in order to rout (_P16REV).
	p16 := func(v reg.VecVirtual, m reg.GPVirtual, sh int, lout, rout Mem, exactLeft func(comb reg.VecVirtual)) {
		a, b := rot(m, sh, 4), rot(m, sh+8, 5)
		p := GP32()
		POPCNTL(a, p)
		ADDL(p, b)
		ci := XMM()
		VMOVDQU(Mem{Base: tb, Index: a.As64(), Scale: 1, Disp: etP16A}, ci)
		VPOR(Mem{Base: tb, Index: b.As64(), Scale: 1, Disp: etP16B}, ci, ci)
		VPSHUFB(ci, v.AsX(), ci)
		if exactLeft != nil {
			exactLeft(ci)
		} else {
			VMOVDQU(ci, lout)
		}
		VPSHUFB(rev, ci, ci)
		VMOVDQU(ci, rout)
	}
	// core stores the right lanes of v in order to rout (_PC16).
	core := func(v reg.VecVirtual, m reg.GPVirtual, sh int, rout Mem) {
		a, b := rot(m, sh, 4), rot(m, sh+8, 4)
		p := GP32()
		POPCNTL(a, p)
		SHLL(U8(12), p)
		ADDL(p, b)
		ci := XMM()
		VMOVDQU(Mem{Base: tb, Index: a.As64(), Scale: 1, Disp: etCtabR}, ci)
		VPMINUB(Mem{Base: tb, Index: b.As64(), Scale: 1, Disp: etPreR}, ci, ci)
		VPSHUFB(ci, v.AsX(), ci)
		VMOVDQU(ci, rout)
	}
	lpos := func(idx reg.GPVirtual, disp int) Mem { return Mem{Base: start, Index: idx, Scale: 1, Disp: disp} }
	rpos := func(idx reg.GPVirtual) Mem { return Mem{Base: rb, Index: idx, Scale: 1} }

	SUBQ(U8(32), n)
	JB(LabelRef(name + "_16"))
	PCALIGN(U8(64))
	Label(name + "_loop")
	{
		v0, v1 := YMM(), YMM()
		var m reg.GPVirtual
		if kind == partNone {
			m = mask(Mem{Base: src}, true)
		} else {
			VMOVDQU(Mem{Base: src}, v0)
			VMOVDQU(Mem{Base: src, Disp: 16}, v1.AsX())
			m = mask(v0, true)
		}
		MOVL(m, Mem{Base: bm})
		var lo reg.GPVirtual
		if kind != partNone {
			lo = GP32()
			MOVWLZX(m.As16(), lo)
			POPCNTL(lo, lo)
		}
		switch kind {
		case partFull:
			p16(v0, m, 0, lpos(nl, 0), rpos(nr), nil)
			x, y := GP64(), GP64()
			MOVQ(nl, x)
			SUBQ(lo.As64(), x)
			LEAQ(Mem{Base: nr, Index: lo.As64(), Scale: 1}, y)
			p16(v1, m, 16, lpos(x, 16), rpos(y), nil)
		case partRight:
			core(v0, m, 0, rpos(nr))
			y := GP64()
			LEAQ(Mem{Base: nr, Index: lo.As64(), Scale: 1}, y)
			core(v1, m, 16, rpos(y))
		}
		POPCNTL(m, m)
		ADDQ(m.As64(), nr)
		if kind == partFull {
			SUBQ(m.As64(), nl)
			ADDQ(U8(32), nl)
		}
		ADDQ(U8(32), src)
		ADDQ(U8(4), bm)
		SUBQ(U8(32), n)
		JAE(LabelRef(name + "_loop"))
	}
	Label(name + "_16")
	ADDQ(U8(32), n)
	CMPQ(n, U8(16))
	JB(LabelRef(name + "_tail"))
	{
		v := YMM()
		var m reg.GPVirtual
		if kind == partNone {
			m = mask(Mem{Base: src}, false)
		} else {
			VMOVDQU(Mem{Base: src}, v.AsX())
			m = mask(v.AsX(), false)
		}
		MOVW(m.As16(), Mem{Base: bm})
		switch kind {
		case partFull:
			p16(v, m, 0, lpos(nl, 0), rpos(nr), nil)
		case partRight:
			core(v, m, 0, rpos(nr))
		}
		POPCNTL(m, m)
		ADDQ(m.As64(), nr)
		if kind == partFull {
			SUBQ(m.As64(), nl)
			ADDQ(U8(16), nl)
		}
		ADDQ(U8(16), src)
		ADDQ(U8(2), bm)
		SUBQ(U8(16), n)
	}
	Label(name + "_tail")
	TESTQ(n, n)
	JZ(LabelRef(name + "_done"))
	{
		// n = 1..15 ranks remain at src; they become lanes 0..n-1 of v, the rest zero
		// (rank 0 is never right).
		v := YMM()
		CMPQ(src, start)
		JNE(LabelRef(name + "_shift"))
		t0, t1 := GP64(), GP64()
		loadExact(src, n, v.AsX(), XMM(), t0, t1, Mem{Base: tb, Disp: etShift}, name+"_ld")
		JMP(LabelRef(name + "_have"))
		Label(name + "_shift")
		s := GP64()
		MOVQ(n, s)
		NEGQ(s)
		VPSHUFB(Mem{Base: tb, Index: s, Scale: 1, Disp: etShift + 32}, last, v.AsX())
		Label(name + "_have")
		m := mask(v.AsX(), false)
		MOVW(m.As16(), Mem{Base: bm})
		switch kind {
		case partFull:
			p16(v, m, 0, Mem{}, rpos(nr), func(comb reg.VecVirtual) {
				// Exact: the bytes after the lefts belong to the caller.
				k, t0, t1 := GP64(), GP64(), GP64()
				POPCNTL(m, k.As32())
				NEGQ(k)
				ADDQ(n, k)
				p := GP64()
				LEAQ(lpos(nl, 0), p)
				storeExact(p, k, comb, XMM(), t0, t1, Mem{Base: tb, Disp: etShift}, name+"_st")
			})
		case partRight:
			core(v, m, 0, rpos(nr))
		}
		POPCNTL(m, m)
		ADDQ(m.As64(), nr)
	}
	Label(name + "_done")
	Store(nr, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

// genPackAVX2 ports prim_enc_pack_dN: n512 codes in 512-value vertical blocks, the next n128
// in 128-value blocks, the rest natural (pack_dN_x86).
func genPackAVX2() {
	TEXT("packAVX2Asm", NOSPLIT, "func(dst, ranks *byte, n, n512, n128 int, d, base uint8)")
	Pragma("noescape")
	Doc("packAVX2Asm packs the codes ranks[i]-base of a depth-d flat region (2 <= d <= 8, n > 0)",
		"into exactly dst[:(n*d+7)/8]: n512 codes in 512-value vertical blocks, the next n128",
		"in 128-value blocks and the rest in natural order.")
	c := packCtxAVX2{dst: GP64(), src: GP64(), nnat: GP64(), n512: GP64(), n128: GP64(), base: YMM(), tb: GP64()}
	Load(Param("dst"), c.dst)
	Load(Param("ranks"), c.src)
	Load(Param("n"), c.nnat)
	Load(Param("n512"), c.n512)
	Load(Param("n128"), c.n128)
	d := GP64()
	Load(Param("d"), d)
	b := GP32()
	Load(Param("base"), b)
	VMOVD(b, c.base.AsX())
	VPBROADCASTB(c.base.AsX(), c.base)
	LEAQ(et.mem(), c.tb)
	SUBQ(c.n512, c.nnat)
	SUBQ(c.n128, c.nnat)
	for D := 2; D <= 8; D++ {
		CMPQ(d, U8(D))
		JEQ(LabelRef(fmt.Sprintf("pack_d%d", D)))
	}
	JMP(LabelRef("pack_done"))
	for D := 2; D <= 8; D++ {
		Label(fmt.Sprintf("pack_d%d", D))
		if D < 8 {
			c.vertical(D)
		}
		c.natural(D)
		JMP(LabelRef("pack_done"))
	}
	Label("pack_done")
	VZEROUPPER()
	RET()
}

type packCtxAVX2 struct {
	dst, src, nnat, n512, n128 reg.GPVirtual
	base                       reg.VecVirtual // base in every byte
	tb                         reg.GPVirtual  // &avx2EncTab
}

func (c *packCtxAVX2) splat(v byte) Mem { return Mem{Base: c.tb, Disp: et.splat(v)} }

// vertical emits the 512- and 128-value block loops. 512-value blocks run as two 32-lane
// halves (vert512_pack_ymm_d*), except D = 7, which upstream runs as four 16-lane quarters
// on AMD (vert512_pack_x86_d7; 1.12 vs 0.89 on c6a).
func (c *packCtxAVX2) vertical(D int) {
	end := fmt.Sprintf("pack_d%d_vert_end", D)
	for _, blk := range []int{512, 128} {
		cnt, shift := c.n512, 9
		if blk == 128 {
			cnt, shift = c.n128, 7
		}
		name := fmt.Sprintf("pack_d%d_v%d", D, blk)
		SHRQ(U8(shift), cnt)
		JZ(LabelRef(name + "_end"))
		PCALIGN(U8(64))
		Label(name)
		switch {
		case blk == 128:
			c.vertBody(D, 16, 16, 0)
		case D == 7:
			for q := range 4 {
				c.vertBody(D, 16, 64, 16*q)
			}
		default:
			for h := range 2 {
				c.vertBody(D, 32, 64, 32*h)
			}
		}
		ADDQ(U32(uint32(blk)), c.src)
		ADDQ(U32(uint32(blk*D/8)), c.dst)
		DECQ(cnt)
		JNZ(LabelRef(name))
		Label(name + "_end")
	}
	Label(end)
}

// vertBody packs width lanes at lane offset off of a vertical block whose rows (and
// columns) have the given stride: code s*stride+l goes to bits s*D... of lane l.
func (c *packCtxAVX2) vertBody(D, width, stride, off int) {
	vec := func() reg.VecVirtual { return YMM() }
	view := func(v reg.VecVirtual) reg.Register {
		if width == 16 {
			return v.AsX()
		}
		return v
	}
	var cols [8]reg.VecVirtual
	for s := range 8 {
		bit := s * D
		j, o := bit>>3, bit&7
		v := vec()
		VMOVDQU(Mem{Base: c.src, Disp: stride*s + off}, view(v))
		VPSUBUSB(view(c.base), view(v), view(v))
		if o == 0 {
			cols[j] = v
			continue
		}
		t := vec()
		VPSLLW(U8(o), view(v), view(t))
		VPAND(c.splat(byte(0xFF<<o)), view(t), view(t))
		VPOR(view(t), view(cols[j]), view(cols[j]))
		if o+D > 8 {
			VPSRLW(U8(8-o), view(v), view(v))
			VPAND(c.splat(byte(0xFF>>(8-o))), view(v), view(v))
			cols[j+1] = v
		}
	}
	for j := range D {
		VMOVDQU(view(cols[j]), Mem{Base: c.dst, Disp: stride*j + off})
	}
}

// rygRows returns the constants of the multiply-as-shift pack (avx2_pack.h) for D:
// c0 bytes {1, 2^D}, c1 words {1, 2^2D}, c3 = 2^4D-1 per qword, its complement, and the
// compaction of each 128-bit lane's two 8D-bit qwords to 2D bytes (junk lanes zero).
func rygRows(D int) (c0, c1, c3, nc3, cmp int) {
	var r0, r1, r3, rn, rc [32]byte
	for i := 0; i < 32; i += 2 {
		r0[i], r0[i+1] = 1, byte(1<<D)
	}
	for i := 0; i < 32; i += 4 {
		w := uint32(1) | uint32(1)<<(2*D)<<16
		r1[i], r1[i+1], r1[i+2], r1[i+3] = byte(w), byte(w>>8), byte(w>>16), byte(w>>24)
	}
	for i := 0; i < 32; i += 8 {
		m := uint64(1)<<(4*D) - 1
		for k := range 8 {
			r3[i+k], rn[i+k] = byte(m>>(8*k)), ^byte(m>>(8*k))
		}
	}
	for l := 0; l < 32; l += 16 {
		for k := range 16 {
			rc[l+k] = 0x80
			switch {
			case k < D:
				rc[l+k] = byte(k)
			case k < 2*D:
				rc[l+k] = byte(8 + k - D)
			}
		}
	}
	key := fmt.Sprint("ryg", D)
	if off, ok := et.named[key]; ok {
		return off, off + 32, off + 64, off + 96, off + 128
	}
	off := et.add(append(append(append(append(r0[:], r1[:]...), r3[:]...), rn[:]...), rc[:]...), 32)
	et.named[key] = off
	return off, off + 32, off + 64, off + 96, off + 128
}

// natural emits pack_dN_x86's SIMD loops: 32 codes per step while the junk of its 16-byte
// stores stays inside the region, then 16 codes per step with exact stores, then one
// partial step on an exact load.
func (c *packCtxAVX2) natural(D int) {
	name := fmt.Sprintf("pack_d%d_nat", D)
	o, cnt, src := c.dst, c.nnat, c.src
	TESTQ(cnt, cnt)
	JZ(LabelRef(name + "_end"))
	var c0r, c1, c3, nc3, cmp int
	if D != 8 {
		c0r, c1, c3, nc3, cmp = rygRows(D)
	}
	c0 := YMM()
	if D < 8 {
		VMOVDQU(Mem{Base: c.tb, Disp: c0r}, c0)
	}
	// code16 turns the 16 or 32 codes in v (zero in unused lanes) into 2D packed bytes
	// per 128-bit lane, or for D = 4 into 8 bytes per lane, or for D = 8 leaves them.
	code16 := func(v reg.Register) {
		switch D {
		case 8:
		case 4:
			VPMADDUBSW(v, c0view(c0, v), v)
			VPACKUSWB(v, v, v)
		default:
			xs := c0like(v)
			VPMADDUBSW(v, c0view(c0, v), v)
			VPMADDWD(Mem{Base: c.tb, Disp: c1}, v, v)
			VPSRLQ(U8(32-4*D), v, xs)
			VPAND(Mem{Base: c.tb, Disp: c3}, v, v)
			VPAND(Mem{Base: c.tb, Disp: nc3}, xs, xs)
			VPOR(xs, v, v)
			VPSHUFB(Mem{Base: c.tb, Disp: cmp}, v, v)
		}
	}
	per32, per16 := 4*D, 2*D // output bytes per step
	if D == 8 {
		per32, per16 = 32, 16
	}
	CMPQ(cnt, U8(32))
	JB(LabelRef(name + "_16"))
	var lim reg.GPVirtual
	if D != 4 && D != 8 {
		// Last start of a 32-code step whose second store (2D + 16 bytes in) fits.
		lim = GP64()
		IMUL3Q(U8(D), cnt, lim)
		ADDQ(U8(7), lim)
		SHRQ(U8(3), lim)
		ADDQ(o, lim)
		SUBQ(U8(2*D+16), lim)
	}
	PCALIGN(U8(64))
	Label(name + "_32")
	if lim != nil {
		CMPQ(o, lim)
		JA(LabelRef(name + "_16"))
	}
	{
		v := YMM()
		VMOVDQU(Mem{Base: src}, v)
		VPSUBUSB(c.base, v, v)
		code16(v)
		switch D {
		case 8:
			VMOVDQU(v, Mem{Base: o})
		case 4:
			VPERMQ(U8(0x08), v, v)
			VMOVDQU(v.AsX(), Mem{Base: o})
		default:
			VMOVDQU(v.AsX(), Mem{Base: o})
			VEXTRACTI128(U8(1), v, Mem{Base: o, Disp: 2 * D})
		}
		ADDQ(U8(32), src)
		ADDQ(U8(per32), o)
		SUBQ(U8(32), cnt)
		CMPQ(cnt, U8(32))
		JAE(LabelRef(name + "_32"))
	}
	Label(name + "_16")
	CMPQ(cnt, U8(16))
	JB(LabelRef(name + "_part"))
	Label(name + "_16loop")
	{
		v := XMM()
		VMOVDQU(Mem{Base: src}, v)
		VPSUBUSB(c.base.AsX(), v, v)
		code16(v)
		storeConst(o, v, per16)
		ADDQ(U8(16), src)
		ADDQ(U8(per16), o)
		SUBQ(U8(16), cnt)
		CMPQ(cnt, U8(16))
		JAE(LabelRef(name + "_16loop"))
	}
	Label(name + "_part")
	TESTQ(cnt, cnt)
	JZ(LabelRef(name + "_end"))
	{
		v := XMM()
		t0, t1 := GP64(), GP64()
		sh := Mem{Base: c.tb, Disp: et.shiftOff()}
		loadExact(src, cnt, v, XMM(), t0, t1, sh, name+"_ld")
		VPSUBUSB(c.base.AsX(), v, v)
		code16(v)
		nb := GP64()
		if D == 8 {
			MOVQ(cnt, nb)
		} else {
			IMUL3Q(U8(D), cnt, nb)
			ADDQ(U8(7), nb)
			SHRQ(U8(3), nb)
		}
		storeExact(o, nb, v, XMM(), t0, t1, sh, name+"_st")
	}
	Label(name + "_end")
}

// c0view returns c0 at the width of v.
func c0view(c0 reg.VecVirtual, v reg.Register) reg.Register {
	if v.Size() == 16 {
		return c0.AsX()
	}
	return c0
}

// c0like returns a new vector register of v's width.
func c0like(v reg.Register) reg.Register {
	if v.Size() == 16 {
		return XMM()
	}
	return YMM()
}

// storeConst stores the low k (constant, 1..16) bytes of x at o.
func storeConst(o reg.Register, x reg.Register, k int) {
	xx := x
	switch {
	case k == 16:
		VMOVDQU(xx, Mem{Base: o})
	case k == 8:
		VMOVQ(xx, Mem{Base: o})
	case k == 4:
		VMOVD(xx, Mem{Base: o})
	case k > 8:
		VMOVQ(xx, Mem{Base: o})
		t := XMM()
		VPSRLDQ(U8(k-8), xx, t)
		VMOVQ(t, Mem{Base: o, Disp: k - 8})
	case k > 4:
		VMOVD(xx, Mem{Base: o})
		t := XMM()
		VPSRLDQ(U8(k-4), xx, t)
		VMOVD(t, Mem{Base: o, Disp: k - 4})
	default:
		panic(k)
	}
}
