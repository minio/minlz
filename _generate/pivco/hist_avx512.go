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

// AVX-512 GFNI/BITALG byte histogram: port of histogram_chunk_avx512 and
// hist_consume_bin_avx512 (pivco_huffman_primitives_avx512.h), which port Harold
// Aptroot's histogram (MIT, https://gitlab.com/-/snippets/3745720).
// Copyright (c) 2026 Harold Aptroot; MIT License, see internal/pivco/kernel/LICENSE-histogram.
//
// Bytes are binned by their top two bits into four 16 KiB buffers with VPCOMPRESSB; the
// low six bits, permuted by one GF2P8AFFINEQB, select one of 64 one-bit counters per qword
// (VPSLLVQ of 1). Full-adder trees of VPTERNLOGD promote the counters, a VPERMB +
// GF2P8AFFINEQB 8x8 bit transpose and VPOPCNTB turn the bit planes into byte counts.
//
// Differences from C:
//   - The input is walked in 64-byte aligned rows; a partial first and last row are masked
//     loads counted at the start of the first round (exact reads, no scalar head or tail).
//     Masked-out lanes load as zero and are removed from the low bin with the same mask.
//   - Full rows run in chunks short enough that no bin can fill up, instead of testing
//     the four bin counts after every row.
//   - Each chunk starts in a loop for rows whose bytes are all below 0x40 (one bin, no
//     compress), and moves on to a loop for rows below 0x80 (two bins) and then to the
//     four-bin loop at the first row that does not fit. Skewed and ASCII inputs bin at a
//     fraction of the four-bin cost; other inputs pay two mispredicted tests per chunk.
//   - VPCOMPRESSB merges into a copy of the row instead of zero-masking (Zen 4/5 give the
//     zero-masking form a dependency on the old destination). The lanes past the count
//     are dead: the next store overwrites them, and consume pads the last partial row
//     of a bin with 0xFF, whose shift count >= 64 counts nothing.
//   - The two- and four-bin loops load the next row before they store the current one's
//     bins. C loads it after them; on a Ryzen 9 9950X (Zen 5) the load then often waits
//     for those stores' addresses, which depend on the current row's counts, and the
//     four-bin loop runs at the latency of that chain. Whether it waits depends on where
//     input and scratch sit in memory, so C's histogram is bimodal from run to run. A
//     binning-only C loop over 1.1 MiB of JPEG took 2.24-2.36 ns per row at 48 scratch
//     placements and 1.71-1.85 ns with the load moved up.
//   - The bin masks come from four byte compares of row&0xC0 instead of two VPMOVB2M and
//     mask register logic, whose throughput is low on Zen 5 (masks and counts of a row:
//     3.9 cycles instead of 5.2).

import (
	. "github.com/honeycombio/avo/build"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

const (
	binSize   = 16 << 10
	binLimit  = binSize - 64 // a bin at this count is consumed before the next store
	roundRows = 1023         // 65472 bytes: no u16 counter of a round can overflow

	histOrigin = "Port of Harold Aptroot's AVX-512 histogram (https://gitlab.com/-/snippets/3745720); MIT License, see LICENSE-histogram."
)

type histConsts struct {
	one, gf, tp, lo8 reg.VecVirtual
}

func genHist() {
	tp := GLOBL("histTranspose", RODATA|NOPTR)
	for i := range 64 {
		DATA(i, U8(i%8*8+i/8))
	}

	TEXT("histAsm", 0, "func(h *[256]uint32, src *byte, n int, scratch *byte)")
	Pragma("noescape")
	Doc("histAsm adds the byte counts of src[:n], n > 0, to h. scratch holds kernel.HistScratch bytes.",
		histOrigin)
	h16 := AllocLocal(512) // [256]uint16 counters of the current round
	cnts := AllocLocal(32) // bin counts while flushing
	part := AllocLocal(32) // the partial rows: address, lane mask
	npart := AllocLocal(8) // partial rows not yet counted
	Comment(histOrigin)

	src, n, base := ld("src"), ld("n"), ld("scratch")
	ADDQ(U8(63), base)
	ANDQ(I8(-64), base)

	// The input is split in 64-byte aligned rows from src&^63; at most the first and the
	// last are partial and are counted with masked loads at the start of the first round.
	row, off, rows, t, np := GP64(), GP64(), GP64(), GP64(), GP64()
	MOVQ(src, row)
	ANDQ(I8(-64), row)
	MOVQ(src, off)
	ANDQ(U8(63), off)
	ADDQ(off, n) // span from row
	LEAQ(Mem{Base: n, Disp: 63}, rows)
	SHRQ(U8(6), rows)
	hm, tm := GP64(), GP64()
	MOVQ(I32(-1), hm)
	SHLXQ(off, hm, hm)
	LEAQ(Mem{Base: n, Disp: -1}, t)
	ANDQ(U8(63), t)
	INCQ(t)
	MOVQ(I32(-1), tm)
	BZHIQ(t, tm, tm)
	XORQ(np, np)
	CMPQ(rows, U8(1))
	JNE(LabelRef("hist_multi"))
	ANDQ(tm, hm)
	MOVQ(row, part)
	MOVQ(hm, part.Offset(8))
	INCQ(np)
	XORQ(rows, rows)
	JMP(LabelRef("hist_setup"))
	Label("hist_multi")
	TESTQ(off, off)
	JZ(LabelRef("hist_nohead"))
	MOVQ(row, part)
	MOVQ(hm, part.Offset(8))
	INCQ(np)
	ADDQ(U8(64), row)
	DECQ(rows)
	Label("hist_nohead")
	CMPQ(tm, I8(-1))
	JEQ(LabelRef("hist_setup"))
	DECQ(rows)
	MOVQ(rows, t)
	SHLQ(U8(6), t)
	ADDQ(row, t)
	MOVQ(np, off)
	SHLQ(U8(4), off)
	MOVQ(t, part.Idx(off, 1))
	MOVQ(tm, part.Offset(8).Idx(off, 1))
	INCQ(np)
	Label("hist_setup")
	MOVQ(np, npart)
	last := GP64()
	MOVQ(rows, last)
	SHLQ(U8(6), last)
	LEAQ(Mem{Base: row, Index: last, Scale: 1, Disp: -64}, last)

	aff, ones, top2, b40, b80 := ZMM(), ZMM(), ZMM(), ZMM(), ZMM()
	c := histConsts{one: ZMM(), gf: ZMM(), tp: ZMM(), lo8: ZMM()}
	MOVQ(U64(0x2010010204080000), t)
	VPBROADCASTQ(t, aff)
	MOVQ(U64(0x8040201008040201), t)
	VPBROADCASTQ(t, c.gf)
	MOVQ(U64(1), t)
	VPBROADCASTQ(t, c.one)
	MOVL(U32(0xFF), t.As32())
	VPBROADCASTW(t.As32(), c.lo8)
	VMOVDQU64(tp, c.tp)
	VPTERNLOGD(U8(0xFF), ones, ones, ones)
	MOVL(U32(0xC0C0C0C0), t.As32())
	VPBROADCASTD(t.As32(), top2)
	MOVL(U32(0x40404040), t.As32())
	VPBROADCASTD(t.As32(), b40)
	MOVL(U32(0x80808080), t.As32())
	VPBROADCASTD(t.As32(), b80)

	rrem, thr := GP64(), GP64()
	var cnt [4]reg.GPVirtual
	for i := range cnt {
		cnt[i] = GP64()
	}

	// body bins the row in d, which it clobbers; kv: its valid lanes (nil: all).
	body := func(d reg.VecVirtual, kv reg.OpmaskVirtual) {
		var kc [4]reg.OpmaskVirtual
		for i := range kc {
			kc[i] = K()
		}
		t := ZMM()
		VPANDD(top2, d, t)
		if kv == nil {
			VPTESTNMB(t, t, kc[0])
		} else {
			VPTESTNMB(t, t, kv, kc[0]) // masked-out lanes load as zero
		}
		VPCMPEQB(b40, t, kc[1])
		VPCMPEQB(b80, t, kc[2])
		VPCMPEQB(top2, t, kc[3])
		VGF2P8AFFINEQB(U8(0), aff, d, d) // x = row, A = aff
		for i := range kc {
			z := ZMM()
			VMOVDQA64(d, z)
			VPCOMPRESSB(d, kc[i], z)
			VMOVDQU64(z, Mem{Base: base, Index: cnt[i], Scale: 1, Disp: i * binSize})
			p := GP64()
			KMOVQ(kc[i], p)
			POPCNTQ(p, p)
			ADDQ(p, cnt[i])
		}
	}

	Label("hist_round")
	{
		z := ZMM()
		VPXORQ(z, z, z)
		for i := range 8 {
			VMOVDQU64(z, h16.Offset(64*i))
		}
		for i := range cnt {
			XORQ(cnt[i], cnt[i])
		}
		// The partial rows count against the first round.
		MOVQ(U32(roundRows), rrem)
		SUBQ(npart, rrem)
		CMPQ(rows, rrem)
		CMOVQLT(rows, rrem)
		SUBQ(rrem, rows)
		pi := GP64()
		XORQ(pi, pi)
		Label("hist_part")
		CMPQ(pi, npart)
		JAE(LabelRef("hist_partdone"))
		{
			o, pa := GP64(), GP64()
			MOVQ(pi, o)
			SHLQ(U8(4), o)
			MOVQ(part.Idx(o, 1), pa)
			kv, d := K(), ZMM()
			KMOVQ(part.Offset(8).Idx(o, 1), kv)
			VMOVDQU8_Z(Mem{Base: pa}, kv, d)
			body(d, kv)
			INCQ(pi)
			JMP(LabelRef("hist_part"))
		}
		Label("hist_partdone")
		MOVQ(I32(0), npart)
	}

	// Run full rows in chunks short enough that no bin can overflow: a store at count c
	// needs c <= binSize-64, and each row adds at most 64 to a bin.
	Label("hist_recheck")
	{
		TESTQ(rrem, rrem)
		JZ(LabelRef("hist_roundend"))
		mx := GP64()
		MOVQ(cnt[0], mx)
		for _, c := range cnt[1:] {
			CMPQ(c, mx)
			CMOVQHI(c, mx)
		}
		CMPQ(mx, U32(binLimit))
		JAE(LabelRef("hist_flushmid"))
		left := GP64()
		MOVQ(U32(binLimit), left)
		SUBQ(mx, left)
		SHRQ(U8(6), left)
		INCQ(left)
		CMPQ(rrem, left)
		CMOVQLT(rrem, left)
		SUBQ(left, rrem)
		// The loops keep the row at row in d. The one-bin loop loads it; its store
		// addresses do not depend on the data. ahead loads the next row (the last full
		// one once there is none, for exact reads) before the current row's bins are
		// stored.
		d, nxt := ZMM(), ZMM()
		ahead := func() {
			a := GP64()
			LEAQ(Mem{Base: row, Disp: 64}, a)
			CMPQ(a, last)
			CMOVQHI(last, a)
			VMOVDQA64(Mem{Base: a}, nxt)
		}
		next := func(loop string) {
			ADDQ(U8(64), row)
			DECQ(left)
			JNZ(LabelRef(loop))
			JMP(LabelRef("hist_recheck"))
		}
		// A row that does not fit a loop is redone by the next one.
		PCALIGN(U8(64))
		Label("hist_row1")
		{
			k := K()
			VMOVDQA64(Mem{Base: row}, d)
			VPTESTMB(top2, d, k)
			KORTESTQ(k, k)
			JNZ(LabelRef("hist_row2"))
			VGF2P8AFFINEQB(U8(0), aff, d, d)
			VMOVDQU64(d, Mem{Base: base, Index: cnt[0], Scale: 1})
			ADDQ(U8(64), cnt[0])
			next("hist_row1")
		}
		PCALIGN(U8(64))
		Label("hist_row2")
		{
			z, k7, k6, k0 := ZMM(), K(), K(), K()
			ahead()
			VPMOVB2M(d, k7)
			KORTESTQ(k7, k7)
			JNZ(LabelRef("hist_row4"))
			VPTESTMB(b40, d, k6)
			VPTESTNMB(b40, d, k0)
			VGF2P8AFFINEQB(U8(0), aff, d, d)
			VMOVDQA64(d, z)
			VPCOMPRESSB(d, k0, z)
			VMOVDQU64(z, Mem{Base: base, Index: cnt[0], Scale: 1})
			VPCOMPRESSB(d, k6, d)
			VMOVDQU64(d, Mem{Base: base, Index: cnt[1], Scale: 1, Disp: binSize})
			p := GP64()
			KMOVQ(k6, p)
			POPCNTQ(p, p)
			ADDQ(p, cnt[1])
			SUBQ(p, cnt[0])
			ADDQ(U8(64), cnt[0])
			VMOVDQA64(nxt, d)
			next("hist_row2")
		}
		PCALIGN(U8(64))
		Label("hist_row4")
		ahead()
		body(d, nil)
		VMOVDQA64(nxt, d)
		next("hist_row4")
	}

	Label("hist_roundend")
	MOVQ(U32(1), thr)
	JMP(LabelRef("hist_flush"))
	Label("hist_flushmid")
	MOVQ(U32(binLimit), thr)
	Label("hist_flush")
	{
		for i := range cnt {
			MOVQ(cnt[i], cnts.Offset(8*i))
		}
		b := GP64()
		XORQ(b, b)
		Label("hist_flush_bin")
		nb := GP64()
		MOVQ(cnts.Idx(b, 8), nb)
		CMPQ(nb, thr)
		JB(LabelRef("hist_flush_next"))
		data, hp := GP64(), GP64()
		MOVQ(b, data)
		SHLQ(U8(14), data)
		ADDQ(base, data)
		LEAQ(h16, hp)
		MOVQ(b, t)
		SHLQ(U8(7), t)
		ADDQ(t, hp)
		consume(c, ones, data, nb, hp)
		MOVQ(I32(0), cnts.Idx(b, 8))
		Label("hist_flush_next")
		INCQ(b)
		CMPQ(b, U8(4))
		JB(LabelRef("hist_flush_bin"))
		for i := range cnt {
			MOVQ(cnts.Offset(8*i), cnt[i])
		}
		CMPQ(thr, U8(1))
		JNE(LabelRef("hist_recheck"))
	}

	// Widen: hist16 is in consume's lane order (see histogram_chunk_avx512).
	{
		h := ld("h")
		lo16 := ZMM()
		MOVL(U32(0xFFFF), t.As32())
		VPBROADCASTD(t.As32(), lo16)
		for g := range 4 {
			for half := range 2 {
				v, lo, hi := ZMM(), ZMM(), ZMM()
				VMOVDQU64(h16.Offset(128*g+64*half), v)
				VPANDD(lo16, v, lo)
				VPSRLD(U8(16), v, hi)
				o := 256*g + 128*half
				VPADDD(Mem{Base: h, Disp: o}, lo, lo)
				VMOVDQU64(lo, Mem{Base: h, Disp: o})
				VPADDD(Mem{Base: h, Disp: o + 64}, hi, hi)
				VMOVDQU64(hi, Mem{Base: h, Disp: o + 64})
			}
		}
	}
	TESTQ(rows, rows)
	JNZ(LabelRef("hist_round"))
	VZEROUPPER()
	RET()
}

// consume counts the nb six-bit morsels at data (64-byte aligned) into the 64 u16
// counters at hp (hist_consume_bin_avx512). data, nb are clobbered.
func consume(c histConsts, ones reg.VecVirtual, data, nb, hp reg.GPVirtual) {
	tail := GP64()
	MOVQ(nb, tail)
	ANDQ(U8(63), tail)
	JZ(LabelRef("hist_c_nopad"))
	{
		w, m := GP64(), GP64()
		MOVQ(nb, w)
		ANDQ(I8(-64), w)
		ADDQ(data, w)
		MOVQ(I32(-1), m)
		SHLXQ(tail, m, m)
		k := K()
		KMOVQ(m, k)
		VMOVDQU8(ones, k, Mem{Base: w})
	}
	Label("hist_c_nopad")
	ADDQ(U8(63), nb)
	SHRQ(U8(6), nb)

	zero := func() reg.VecVirtual {
		z := ZMM()
		VPXORQ(z, z, z)
		return z
	}
	h0, h1, w0, w1, w2 := zero(), zero(), zero(), zero(), zero()
	transpose := func(v reg.VecVirtual) {
		VPERMB(v, c.tp, v)
		VGF2P8AFFINEQB(U8(0), v, c.gf, v) // x = gf, A = v: transposes each 8x8 bit block
		VPOPCNTB(v, v)
	}

	Label("hist_c_outer")
	mr := GP64()
	MOVQ(U32(31), mr) // 31 rows of popcounts <= 8 fit a byte
	CMPQ(nb, mr)
	CMOVQLT(nb, mr)
	SUBQ(mr, nb)
	w := zero()
	PCALIGN(U8(64))
	Label("hist_c_inner")
	{
		var x [8]reg.VecVirtual
		for r := range x {
			x[r] = ZMM()
			VPMOVZXBQ(Mem{Base: data, Disp: 8 * r}, x[r])
			VPSLLVQ(x[r], c.one, x[r])
		}
		ADDQ(U8(64), data)
		// fa is HIST_FA: c = a^b^c, b = majority(a, b, c); returns (hh, ll) = (b, c).
		fa := func(a, b, c reg.VecVirtual) (reg.VecVirtual, reg.VecVirtual) {
			VPTERNLOGD(U8(0x96), a, b, c)
			VPTERNLOGD(U8(0xB2), a, c, b) // from a, b and a^b^c
			return b, c
		}
		x[1], x[2] = fa(x[0], x[1], x[2])
		x[4], x[5] = fa(x[3], x[4], x[5])
		x[7], w0 = fa(x[6], x[7], w0)
		x[5], w0 = fa(x[2], x[5], w0)
		x[4], x[7] = fa(x[1], x[4], x[7])
		x[5], w1 = fa(x[7], x[5], w1)
		w3, _ := fa(x[5], x[4], w2)
		transpose(w3)
		VPADDB(w3, w, w)
		DECQ(mr)
		JNZ(LabelRef("hist_c_inner"))
	}
	t := ZMM()
	VPANDQ(c.lo8, w, t)
	VPADDW(t, h0, h0)
	VPSRLW(U8(8), w, t)
	VPADDW(t, h1, h1)
	TESTQ(nb, nb)
	JNZ(LabelRef("hist_c_outer"))

	transpose(w0)
	transpose(w1)
	transpose(w2)
	VPADDB(w1, w0, w0)
	VPADDB(w1, w0, w0)
	VPSLLQ(U8(2), w2, w2)
	VPADDB(w2, w0, w0)
	VPSLLW(U8(3), h0, h0)
	VPSLLW(U8(3), h1, h1)
	VPANDQ(c.lo8, w0, t)
	VPADDW(t, h0, h0)
	VPSRLW(U8(8), w0, t)
	VPADDW(t, h1, h1)
	VPADDW(Mem{Base: hp}, h0, h0)
	VMOVDQU64(h0, Mem{Base: hp})
	VPADDW(Mem{Base: hp, Disp: 64}, h1, h1)
	VMOVDQU64(h1, Mem{Base: hp, Disp: 64})
}
