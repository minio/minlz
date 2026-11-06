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

// AVX-512 encoder kernels: gather, the three partitions and the flat pack. They port
// init_avx512, part_{full,right,none}_avx512 and prim_enc_pack_dN (vert512_pack_zmm_d*,
// vert_pack_x86_d*, pack_dN_avx512) from pivco_huffman_primitives_avx512.h,
// pivco_huffman_x86_vertical.h and pivco_huffman_avx512_pack.h.
//
// Differences from C:
//   - Every tail is one masked iteration instead of a scalar loop. Reads of src (gather)
//     and ranks are exact; stores to ranks (gather, in-place left side of partFull) and to
//     the packed region are exact. Right-side and bitmap stores are full width and rely on
//     kernel.RankSlack.
//   - VPCOMPRESSB is always merge-masked into a register that holds the loaded vector:
//     Zen 4/5 give the zero-masking form a dependency on the old destination (upstream
//     issue #11, 46-54% on part_full/part_right), and the memory form is microcoded.
//   - The rank-to-code subtraction is a VPADDB of -base, so the load folds into it.

import (
	"fmt"

	. "github.com/honeycombio/avo/build"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

func genEncAVX512() {
	genGather()
	genPart("partFullAsm", true, true)
	genPart("partRightAsm", true, false)
	genPart("partNoneAsm", false, false)
	genPack()
}

// tailMask sets k to the low rem bits.
func tailMask(rem reg.GPVirtual, k reg.OpmaskVirtual) {
	m := GP64()
	MOVQ(I32(-1), m)
	BZHIQ(rem, m, m)
	KMOVQ(m, k)
}

func genGather() {
	TEXT("gatherAsm", NOSPLIT, "func(ranks, src *byte, n int, s2r *[256]byte) uint8")
	Pragma("noescape")
	Doc("gatherAsm sets ranks[i] = s2r[src[i]] for i < n > 0 and returns the largest of them.")
	ranks, src, n, s2r := ld("ranks"), ld("src"), ld("n"), ld("s2r")

	// Each VPERMI2B covers 128 entries indexed by the low 7 bits; bit 7 picks the half.
	var t [4]reg.VecVirtual
	for i := range t {
		t[i] = ZMM()
		VMOVDQU64(Mem{Base: s2r, Disp: 64 * i}, t[i])
	}
	mx := ZMM()
	VPXORQ(mx, mx, mx)

	lookup := func(c, out reg.VecVirtual) {
		hib := K()
		VPMOVB2M(c, hib)
		VMOVDQA64(c, out)
		VPERMI2B(t[1], t[0], out)
		VPERMI2B(t[3], t[2], c)
		VPBLENDMB(c, out, hib, out)
	}

	// Two vectors per iteration with one VPMAXUB into mx: a VPMAXUB chain through every
	// vector bounds the loop (about 18% at n=32768). The bounds are signed: n < 2^31.
	i := GP64()
	XORQ(i, i)
	SUBQ(U8(128), n)
	JL(LabelRef("gather_one"))
	PCALIGN(U8(64))
	Label("gather_loop")
	{
		c0, r0, c1, r1 := ZMM(), ZMM(), ZMM(), ZMM()
		VMOVDQU64(Mem{Base: src, Index: i, Scale: 1}, c0)
		VMOVDQU64(Mem{Base: src, Index: i, Scale: 1, Disp: 64}, c1)
		lookup(c0, r0)
		lookup(c1, r1)
		VMOVDQU64(r0, Mem{Base: ranks, Index: i, Scale: 1})
		VMOVDQU64(r1, Mem{Base: ranks, Index: i, Scale: 1, Disp: 64})
		VPMAXUB(r0, r1, r0)
		VPMAXUB(r0, mx, mx)
		ADDQ(U8(128), i)
		CMPQ(i, n)
		JLE(LabelRef("gather_loop"))
	}
	Label("gather_one")
	ADDQ(U8(64), n)
	CMPQ(i, n)
	JG(LabelRef("gather_tail"))
	{
		c, r := ZMM(), ZMM()
		VMOVDQU64(Mem{Base: src, Index: i, Scale: 1}, c)
		lookup(c, r)
		VPMAXUB(r, mx, mx)
		VMOVDQU64(r, Mem{Base: ranks, Index: i, Scale: 1})
		ADDQ(U8(64), i)
	}
	Label("gather_tail")
	ADDQ(U8(64), n)
	SUBQ(i, n)
	JZ(LabelRef("gather_done"))
	{
		kv := K()
		tailMask(n, kv)
		c, r := ZMM(), ZMM()
		VMOVDQU8_Z(Mem{Base: src, Index: i, Scale: 1}, kv, c)
		lookup(c, r)
		VPMAXUB(r, mx, kv, mx)
		VMOVDQU8(r, kv, Mem{Base: ranks, Index: i, Scale: 1})
	}
	Label("gather_done")
	y, x, x2 := YMM(), XMM(), XMM()
	VEXTRACTI64X4(U8(1), mx, y)
	VPMAXUB(y, mx.AsY(), y)
	VEXTRACTI32X4(U8(1), y, x)
	VPMAXUB(x, y.AsX(), x)
	for _, s := range []int{8, 4, 2, 1} {
		VPSRLDQ(U8(s), x, x2)
		VPMAXUB(x2, x, x)
	}
	r := GP32()
	VMOVD(x, r)
	Store(r.As8(), ReturnIndex(0))
	VZEROUPPER()
	RET()
}

// genPart emits a partition by rank > thr. right: compact the right ranks into right[];
// left: compact the left ranks in place.
func genPart(name string, right, left bool) {
	if right {
		TEXT(name, NOSPLIT, "func(ranks, right, bm *byte, n int, thr uint8) int")
	} else {
		TEXT(name, NOSPLIT, "func(ranks, bm *byte, n int, thr uint8) int")
	}
	Pragma("noescape")
	Doc(name + " partitions ranks[:n], n > 0, by rank > thr; see Tier.PartFull.")
	src := ld("ranks")
	var rbase, lbase reg.GPVirtual
	if right {
		rbase = ld("right")
	}
	if left {
		lbase = GP64()
		MOVQ(src, lbase)
	}
	bm, n := ld("bm"), ld("n")
	thr := Load(Param("thr"), GP32())
	vt := ZMM()
	VPBROADCASTB(thr, vt)
	nr, nl := GP64(), GP64()
	XORQ(nr, nr)
	if left {
		XORQ(nl, nl)
	}

	// step partitions one vector v whose valid lanes are kv (nil: all 64), rem of them.
	step := func(v reg.VecVirtual, kv reg.OpmaskVirtual, rem reg.GPVirtual) {
		k := K()
		if kv == nil {
			VPCMPUB(U8(6), vt, v, k) // NLE: v > thr
		} else {
			VPCMPUB(U8(6), vt, v, kv, k)
		}
		KMOVQ(k, Mem{Base: bm})
		p := GP64()
		KMOVQ(k, p)
		POPCNTQ(p, p)
		if right {
			r := v
			if left {
				r = ZMM()
				VMOVDQA64(v, r)
			}
			VPCOMPRESSB(v, k, r)
			VMOVDQU64(r, Mem{Base: rbase, Index: nr, Scale: 1})
		}
		if left {
			kl := K()
			if kv == nil {
				KNOTQ(k, kl)
			} else {
				KANDNQ(kv, k, kl)
			}
			VPCOMPRESSB(v, kl, v)
			// In place: the store ends at or before the end of the lanes just loaded.
			if kv == nil {
				VMOVDQU64(v, Mem{Base: lbase, Index: nl, Scale: 1})
				ADDQ(U8(64), nl)
			} else {
				VMOVDQU8(v, kv, Mem{Base: lbase, Index: nl, Scale: 1})
				ADDQ(rem, nl)
			}
			SUBQ(p, nl)
		}
		ADDQ(p, nr)
	}

	SUBQ(U8(64), n)
	JB(LabelRef(name + "_tail"))
	PCALIGN(U8(64))
	Label(name + "_loop")
	{
		v := ZMM()
		VMOVDQU64(Mem{Base: src}, v)
		step(v, nil, nil)
		ADDQ(U8(64), src)
		ADDQ(U8(8), bm)
		SUBQ(U8(64), n)
		JAE(LabelRef(name + "_loop"))
	}
	Label(name + "_tail")
	ADDQ(U8(64), n)
	JZ(LabelRef(name + "_done"))
	{
		kv := K()
		tailMask(n, kv)
		v := ZMM()
		VMOVDQU8_Z(Mem{Base: src}, kv, v)
		step(v, kv, n)
	}
	Label(name + "_done")
	Store(nr, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

// Constants of the pack kernels.
var (
	packMasks    Mem // dword i (1..7): 0xFF<<i per byte; dword 8+i: 0xFF>>i per byte
	packShufD2   Mem // 4 VPERMB index vectors: codes k, k+4, ..., k+60
	packShufD4   Mem // 2 VPERMB index vectors: even, odd codes
	packCompact  Mem // per D in 3,5,6,7: bytes 0..D-1 of each qword, then zero
	packD3       Mem // qwords: masks A..D, multishift controls A..D
	packCompactI = map[int]int{3: 0, 5: 1, 6: 2, 7: 3}
)

func genPackData() {
	packMasks = GLOBL("packMasks", RODATA|NOPTR)
	for i := range 8 {
		DATA(4*i, U32(0x01010101*uint32(0xFF<<i&0xFF)))
	}
	for i := range 8 {
		DATA(32+4*i, U32(0x01010101*uint32(0xFF>>i)))
	}
	packShufD2 = GLOBL("packShufD2", RODATA|NOPTR)
	for k := range 4 {
		for j := range 16 {
			DATA(64*k+j, U8(k+4*j))
		}
		for j := 16; j < 64; j += 8 {
			DATA(64*k+j, U64(0))
		}
	}
	packShufD4 = GLOBL("packShufD4", RODATA|NOPTR)
	for k := range 2 {
		for j := range 32 {
			DATA(64*k+j, U8(k+2*j))
		}
		for j := 32; j < 64; j += 8 {
			DATA(64*k+j, U64(0))
		}
	}
	packCompact = GLOBL("packCompact", RODATA|NOPTR)
	for _, d := range []int{3, 5, 6, 7} {
		o := 64 * packCompactI[d]
		for j := range 64 {
			v := 0
			if j < 8*d {
				v = j/d*8 + j%d
			}
			DATA(o+j, U8(v))
		}
	}
	packD3 = GLOBL("packD3", RODATA|NOPTR)
	for i, v := range []uint64{
		0x0000000700000007, 0x0000070000000700, 0x0007000000070000, 0x0700000007000000,
		0x0000000000081C00, 0x0000000000292105, 0x00000000002E120A, 0x0000000000331700,
	} {
		DATA(8*i, U64(v))
	}
}

// packCtx holds the per-call state shared by the per-D code.
type packCtx struct {
	dst, src, nnat, n512, n128 reg.GPVirtual
	neg                        reg.VecVirtual // -base in every byte
}

func genPack() {
	genPackData()
	TEXT("packAsm", NOSPLIT, "func(dst, ranks *byte, n, n512, n128 int, d, base uint8)")
	Pragma("noescape")
	Doc("packAsm packs the codes ranks[i]-base of a depth-d flat region (2 <= d <= 8, n > 0)",
		"into exactly dst[:(n*d+7)/8]: n512 codes in 512-value vertical blocks, the next n128",
		"in 128-value blocks and the rest in natural order.")
	c := packCtx{
		dst: ld("dst"), src: ld("ranks"), nnat: ld("n"), n512: ld("n512"), n128: ld("n128"),
		neg: ZMM(),
	}
	d := ld("d")
	b := Load(Param("base"), GP32())
	NEGL(b)
	VPBROADCASTB(b, c.neg)
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

// vertical emits the 512- and 128-value block loops (vert512_pack_zmm_d*, vert_pack_x86_d*).
// Code v of a block of 8*L values sits in lane v%L at bits D*(v/L)...; lane l's D bytes are
// stored at a stride of L. Step s adds codes s*L.. to the columns j = s*D/8 (and j+1 when
// the field spans two bytes).
func (c *packCtx) vertical(D int) {
	end := fmt.Sprintf("pack_d%d_vert_end", D)
	t := GP64()
	MOVQ(c.n512, t)
	ORQ(c.n128, t)
	JZ(LabelRef(end))
	hi, lo := map[int]reg.VecVirtual{}, map[int]reg.VecVirtual{}
	for s := 1; s < 8; s++ {
		off := s * D & 7
		if off == 0 {
			continue
		}
		if hi[off] == nil {
			hi[off] = ZMM()
			VPBROADCASTD(packMasks.Offset(4*off), hi[off])
		}
		if off+D > 8 && lo[8-off] == nil {
			lo[8-off] = ZMM()
			VPBROADCASTD(packMasks.Offset(32+4*(8-off)), lo[8-off])
		}
	}
	for _, L := range []int{64, 16} {
		cnt, shift := c.n512, 9
		if L == 16 {
			cnt, shift = c.n128, 7
		}
		name := fmt.Sprintf("pack_d%d_v%d", D, 8*L)
		SHRQ(U8(shift), cnt)
		JZ(LabelRef(name + "_end"))
		PCALIGN(U8(64))
		Label(name)
		vec := func() reg.VecVirtual {
			if L == 16 {
				return XMM()
			}
			return ZMM()
		}
		view := func(v reg.VecVirtual) reg.Register {
			if L == 16 {
				return v.AsX()
			}
			return v
		}
		var cols [8]reg.VecVirtual
		for s := range 8 {
			bit := s * D
			j, off := bit>>3, bit&7
			v := vec()
			VPADDB(Mem{Base: c.src, Disp: s * L}, view(c.neg), v)
			if off == 0 {
				cols[j] = v
				continue
			}
			t := vec()
			VPSLLW(U8(off), v, t)
			VPTERNLOGD(U8(0xF8), view(hi[off]), t, cols[j]) // cols[j] |= t & hi
			if off+D > 8 {
				VPSRLW(U8(8-off), v, v)
				VPANDD(view(lo[8-off]), v, v)
				cols[j+1] = v
			}
		}
		for j := range D {
			VMOVDQU64(cols[j], Mem{Base: c.dst, Disp: j * L})
		}
		ADDQ(U32(8*L), c.src)
		ADDQ(U32(D*L), c.dst)
		DECQ(cnt)
		JNZ(LabelRef(name))
		Label(name + "_end")
	}
	Label(end)
}

// natural emits pack_dN_avx512 for 64 codes per iteration and one masked tail.
func (c *packCtx) natural(D int) {
	name := fmt.Sprintf("pack_d%d_nat", D)
	TESTQ(c.nnat, c.nnat)
	JZ(LabelRef(name + "_end"))
	var (
		shuf   [4]reg.VecVirtual
		m3, c3 [4]reg.VecVirtual
		cmp    reg.VecVirtual
		w0, w1 reg.VecVirtual
		qmask  reg.VecVirtual
	)
	switch D {
	case 2, 4:
		tab := packShufD2
		if D == 4 {
			tab = packShufD4
		}
		for k := range 8 / D {
			shuf[k] = ZMM()
			VMOVDQU64(tab.Offset(64*k), shuf[k])
		}
	case 3:
		for k := range 4 {
			m3[k], c3[k] = ZMM(), ZMM()
			VPBROADCASTQ(packD3.Offset(8*k), m3[k])
			VPBROADCASTQ(packD3.Offset(32+8*k), c3[k])
		}
	case 5, 6, 7:
		// ryg multiply-as-shift pyramid (pack_d5..7_avx512).
		g := GP64()
		w0, w1, qmask = ZMM(), ZMM(), ZMM()
		MOVL(U32(1<<D<<8|1), g.As32())
		VPBROADCASTW(g.As32(), w0)
		MOVL(U32(1<<(2*D)<<16|1), g.As32())
		VPBROADCASTD(g.As32(), w1)
		MOVQ(U64(1<<(4*D)-1), g)
		VPBROADCASTQ(g, qmask)
	}
	if D == 3 || D >= 5 && D <= 7 {
		cmp = ZMM()
		VMOVDQU64(packCompact.Offset(64*packCompactI[D]), cmp)
	}

	// group packs the 64 codes in cb (zero past the valid ones) into the low 8*D bytes.
	group := func(cb reg.VecVirtual) reg.VecVirtual {
		switch D {
		case 2:
			var g [4]reg.VecVirtual
			for k := range 4 {
				g[k] = ZMM()
				VPERMB(cb, shuf[k], g[k])
				if k > 0 {
					VPSLLD(U8(2*k), g[k], g[k])
				}
			}
			VPTERNLOGD(U8(0xFE), g[2], g[1], g[0])
			VPORD(g[3], g[0], g[0])
			return g[0]
		case 4:
			g0, g1 := ZMM(), ZMM()
			VPERMB(cb, shuf[0], g0)
			VPERMB(cb, shuf[1], g1)
			VPSLLD(U8(4), g1, g1)
			VPORD(g1, g0, g0)
			return g0
		case 3:
			var g [4]reg.VecVirtual
			for k := range 4 {
				g[k] = ZMM()
				VPANDQ(m3[k], cb, g[k])
				VPMULTISHIFTQB(g[k], c3[k], g[k])
			}
			VPTERNLOGD(U8(0xFE), g[2], g[1], g[0])
			VPORD(g[3], g[0], g[0])
			VPERMB(g[0], cmp, g[0])
			return g[0]
		case 8:
			return cb
		}
		x, xs := ZMM(), ZMM()
		VPMADDUBSW(cb, w0, x) // word = c[2i] + c[2i+1]<<D
		VPMADDWD(x, w1, x)    // dword = w[2i] + w[2i+1]<<2D
		VPSRLQ(U8(32-4*D), x, xs)
		VPTERNLOGQ(U8(0xE4), qmask, xs, x) // qword = d[2i] | d[2i+1]<<4D
		VPERMB(x, cmp, x)
		return x
	}

	SUBQ(U8(64), c.nnat)
	JB(LabelRef(name + "_tail"))
	var ks reg.OpmaskVirtual
	if D != 2 && D != 4 && D != 8 {
		ks = K()
		g := GP64()
		MOVQ(U64(1<<(8*D)-1), g)
		KMOVQ(g, ks)
	}
	PCALIGN(U8(64))
	Label(name)
	{
		cb := ZMM()
		VPADDB(Mem{Base: c.src}, c.neg, cb)
		p := group(cb)
		switch D {
		case 2:
			VMOVDQU64(p.AsX(), Mem{Base: c.dst})
		case 4:
			VMOVDQU64(p.AsY(), Mem{Base: c.dst})
		case 8:
			VMOVDQU64(p, Mem{Base: c.dst})
		default:
			VMOVDQU8(p, ks, Mem{Base: c.dst})
		}
		ADDQ(U8(64), c.src)
		ADDQ(U8(8*D), c.dst)
		SUBQ(U8(64), c.nnat)
		JAE(LabelRef(name))
	}
	Label(name + "_tail")
	ADDQ(U8(64), c.nnat)
	JZ(LabelRef(name + "_end"))
	{
		kv := K()
		tailMask(c.nnat, kv)
		cb := ZMM()
		VMOVDQU8_Z(Mem{Base: c.src}, kv, cb)
		VPADDB_Z(c.neg, cb, kv, cb) // zero lanes give zero padding bits
		p := group(cb)
		nb := GP64()
		if D == 8 {
			VMOVDQU8(p, kv, Mem{Base: c.dst})
		} else {
			// ceil(rem*D/8) bytes
			IMUL3Q(U8(D), c.nnat, nb)
			ADDQ(U8(7), nb)
			SHRQ(U8(3), nb)
			tailMask(nb, kv)
			VMOVDQU8(p, kv, Mem{Base: c.dst})
		}
	}
	Label(name + "_end")
}
