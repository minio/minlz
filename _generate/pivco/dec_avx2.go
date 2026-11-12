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

// dec_avx2.go generates the AVX2 decoder executor, execAVX2Asm: one call runs a whole
// kernel.Program op list, like execAVX512Asm (dec_avx512.go), whose op checks it repeats.
// The kernels port upstream's x86 tier as built with PIVCO_HAS_AVX2 and without AVX-512
// (pivco_huffman_primitives_x86.h, pivco_huffman_x86_vertical.h): the two-table
// merge_{vec_vec,cst_vec}_x86 (32 per step, a 16 step, then the tail), merge_cst_cst_x86,
// vert512_merge_ymm_d*, vert_merge_x86_d* and a natural unpack.
//
// Requirements: AVX2, BMI2, POPCNT (kernel.hasAVX2). No EVEX encodings, opmasks or
// X16..X31 (avx2_test.go scans the output).
//
// Register map (all physical):
//
//	DI  *Program                BX  current op      R8  &avx2DecTab
//	R13 &Arena[0]               avx2End(SP) end of the ops, avx2ALim(SP) max(len(Arena)-ArenaSlack, 0)
//	SI  bitmap/region pointer   DX  output pointer  CX  K, then a loop count
//	R11 left cursor / D         R12 right cursor / &R2S[P] (the merge loops: pointers)
//	AX, R9, R10, R14 temporaries (R9 = source length and R14 = Bm in the op prologue)
//	Y15 all ones in VV; flats: see flatPlan. R15 is not used (-dynlink).
//
// Geometry: as in dec_avx512.go each op is checked against the slices before it writes.
// Bitmap and region reads are exact: the merges read 4, 2 and then 1 or 2 bitmap bytes,
// the natural unpack reads 16-byte windows only while they end inside the source, then
// exact loads; stores are exact (exact 8/4/2/1-byte tails). The merges load 16 bytes at
// child cursors that stay inside the child, so they read at most 15 bytes past it, and
// the child lies in the op's K bytes at L or R (ArenaSlack).
//
// Differences from C:
//   - The shuf1 table is kept as 16-byte rows with a zero low half, so the 32-lane merge
//     index is two loads and two adds per lane instead of broadcasts and VPBLENDDs.
//   - CV blends the constant by the sign of the merge index (one VPBLENDVB) instead of a
//     second PSHUFB and an OR.
//   - The tails run the 16-lane step on the masked last bitmap bits and store exactly;
//     C ends with scalar loops.
//   - The natural layout uses one unpack for D = 2..7 (PSHUFB pairs, VPMULLW to the top
//     of each word, VPSRLW, VPACKUSWB), 32 codes per step on two 16-byte windows, with
//     the vertical kernels' PSHUFB/VPBLENDVB lookups. C has a kernel per D.

import (
	"fmt"
	"sort"

	. "github.com/honeycombio/avo/build"
	"github.com/honeycombio/avo/gotypes"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

var rTab, rT3 = reg.R8, reg.R10

var ys = []reg.VecPhysical{
	reg.Y0, reg.Y1, reg.Y2, reg.Y3, reg.Y4, reg.Y5, reg.Y6, reg.Y7,
	reg.Y8, reg.Y9, reg.Y10, reg.Y11, reg.Y12, reg.Y13, reg.Y14, reg.Y15,
}

var dt = newBlob("avx2DecTab")

// Offsets in dt.
var dtShuf1, dtCCShuf, dtCCBits int
var dtNat = map[int]int{} // D -> idxA, idxB (+32), mul (+64)

var avx2End, avx2ALim Mem

// decTables builds g_x86_merge_shuf0/shuf1 (primitives_x86.h:120-138), the CC constants
// and the natural unpack constants.
func decTables() {
	var s0, s1 []byte
	for m := range 256 {
		var r0, r1 [16]byte
		rs, rc, pop := 0, 0, 0
		for i := range 8 {
			pop += m >> i & 1
		}
		for i := range 8 {
			if m>>i&1 != 0 {
				r0[i], rs = byte(rs), rs+1
			} else {
				r0[i], rc = byte(255-rc), rc+1
			}
		}
		for i := 8; i < 16; i++ {
			r0[i] = byte(pop)
		}
		rs, rc = 0, 0
		for t := range 8 {
			if m>>t&1 != 0 {
				r1[8+t], rs = byte(rs), rs+1
			} else {
				r1[8+t], rc = byte(247-rc), rc+1
			}
		}
		s0, s1 = append(s0, r0[:]...), append(s1, r1[:]...)
	}
	if dt.add(s0, 64) != 0 {
		panic("shuf0 must be first")
	}
	dtShuf1 = dt.add(s1, 64)
	var cs, cb [32]byte
	for i := range 32 {
		cs[i], cb[i] = byte(i/8), byte(1<<(i%8))
	}
	dtCCShuf, dtCCBits = dt.add(cs[:], 32), dt.add(cb[:], 32)
	dt.shiftOff()
	// Natural unpack: word e of a lane holds the bytes (b, b+1) of code e (idxA) or 8+e
	// (idxB) of its 16-byte window, b = D*e/8; the multiplier moves the code to the top.
	for D := 2; D <= 7; D++ {
		var r [96]byte
		for l := 0; l < 32; l += 16 {
			for e := range 8 {
				pos := D * e
				b := pos >> 3
				r[l+2*e], r[l+2*e+1] = byte(b), byte(b+1)
				r[32+l+2*e], r[32+l+2*e+1] = byte(D+b), byte(D+b+1)
				mul := uint16(1) << (16 - D - pos&7)
				r[64+l+2*e], r[64+l+2*e+1] = byte(mul), byte(mul>>8)
			}
		}
		dtNat[D] = dt.add(r[:], 32)
	}
}

func tab(disp int) Mem { return Mem{Base: rTab, Disp: disp} }
func splat(v byte) Mem { return tab(dt.splat(v)) }
func shiftTab() Mem    { return tab(dt.shiftOff()) }

func genDecAVX2() {
	decTables()
	TEXT("execAVX2Asm", NOSPLIT, "func(p *Program) int")
	Doc("execAVX2Asm runs p.Ops and returns -1, or the index of the first op that fails",
		"its checks.")
	Pragma("noescape")
	loc := AllocLocal(16)
	avx2End, avx2ALim = loc, loc.Offset(8)
	pc := Param("p").Dereference(rP)
	Load(Param("p"), rP)
	LEAQ(dt.mem(), rTab)
	MOVQ(field(pc.Field("Ops").Base()), rOp)
	MOVQ(field(pc.Field("Ops").Len()), rT)
	SHLQ(U8(5), rT)
	ADDQ(rOp, rT)
	MOVQ(rT, avx2End)
	MOVQ(field(pc.Field("Arena").Base()), rArena)
	XORL(rT1.As32(), rT1.As32())
	MOVQ(field(pc.Field("Arena").Len()), rT)
	SUBQ(U32(arenaSlack), rT)
	CMOVQLT(rT1, rT)
	MOVQ(rT, avx2ALim)
	dispatch(pc, avx2End, avx2ALim)

	mergeVVAVX2()
	mergeCVAVX2()
	mergeCCAVX2()
	verticalAVX2(pc, true)
	verticalAVX2(pc, false)
	naturalAVX2(pc)
	copyFlatAVX2()

	exits(pc)
	dt.emit()
}

// tailBits loads the ceil(r/8) bitmap bytes of the last r = K&15 (non-zero, in CX)
// symbols exactly into AX and clears the bits past r.
func tailBits(name string) {
	CMPL(rK.As32(), U8(8))
	JA(LabelRef(name + "_two"))
	MOVBLZX(Mem{Base: rSrc}, rT.As32())
	JMP(LabelRef(name + "_bits"))
	Label(name + "_two")
	MOVWLZX(Mem{Base: rSrc}, rT.As32())
	Label(name + "_bits")
	BZHIL(rK.As32(), rT.As32(), rT.As32())
}

// shuf16 sets x to the merge index of the 16 bitmap bits at bit sh of AX:
// shuf0[m0] + shuf1x[m1] (R lanes: index into R, L lanes: 0x80 | complement of L's).
func shuf16(sh int, x reg.VecPhysical) {
	RORXL(U8((sh+28)%32), rT.As32(), rT3.As32())
	ANDL(U32(0xFF0), rT3.As32())
	RORXL(U8((sh+4)%32), rT.As32(), rT2.As32())
	ANDL(U32(0xFF0), rT2.As32())
	VMOVDQU(Mem{Base: rTab, Index: rT3, Scale: 1}, x.AsX())
	VPADDB(Mem{Base: rTab, Index: rT2, Scale: 1, Disp: dtShuf1}, x.AsX(), x.AsX())
}

// shuf32 sets Y0 to the merge index of the 32 bitmap bits in AX and R10 to the number
// of set bits in the low 16.
func shuf32() {
	shuf16(0, reg.Y0)
	shuf16(16, reg.Y1)
	VINSERTI128(U8(1), reg.X1, reg.Y0, reg.Y0)
	MOVWLZX(rT.As16(), rT3.As32())
	POPCNTL(rT3.As32(), rT3.As32())
}

// mergeLoopAVX2 emits the 32-symbol steps of a merge, two per iteration; step(disp) does
// the symbols at output offset disp. The cursors in R11 and R12 are arena pointers.
func mergeLoopAVX2(name string, hasL bool, step func(disp int)) {
	if hasL {
		ADDQ(rArena, rL)
	}
	ADDQ(rArena, rR)
	MOVQ(rK, rT1)
	SHRQ(U8(6), rT1)
	JZ(LabelRef(name + "_one"))
	PCALIGN(U8(64))
	Label(name + "_loop")
	step(0)
	step(32)
	if hasL {
		ADDQ(U8(64), rL)
	}
	ADDQ(U8(8), rSrc)
	ADDQ(U8(64), rOut)
	DECQ(rT1)
	JNZ(LabelRef(name + "_loop"))
	Label(name + "_one")
	BTL(U8(5), rK.As32())
	JNC(LabelRef(name + "_16"))
	step(0)
	if hasL {
		ADDQ(U8(32), rL)
	}
	ADDQ(U8(4), rSrc)
	ADDQ(U8(32), rOut)
}

// mergeTail emits the 16-symbol step and the exact tail of a merge; step16 merges the
// 16 symbols of the bits in AX into X1 at the cursors.
func mergeTail(name string, hasL bool, step16 func()) {
	Label(name + "_16")
	BTL(U8(4), rK.As32())
	JNC(LabelRef(name + "_tail"))
	MOVWLZX(Mem{Base: rSrc}, rT.As32())
	step16()
	VMOVDQU(reg.X1, Mem{Base: rOut})
	POPCNTL(rT.As32(), rT.As32())
	ADDQ(rT, rR)
	if hasL {
		SUBQ(rT, rL)
		ADDQ(U8(16), rL)
	}
	ADDQ(U8(2), rSrc)
	ADDQ(U8(16), rOut)
	Label(name + "_tail")
	ANDL(U8(15), rK.As32())
	JZ(LabelRef(name + "_check"))
	tailBits(name)
	step16()
	storeExact(rOut, rK, reg.X1, reg.X2, rT1, rT3, shiftTab(), name+"_st")
	POPCNTL(rT.As32(), rT.As32())
	ADDQ(rT, rR)
	Label(name + "_check")
	SUBQ(rArena, rR)
	krCheck()
}

func mergeVVAVX2() {
	Label("vv")
	Comment("out[j] = bit j ? R[r++] : L[l++] (upstream merge_vec_vec_x86)")
	bitmapCheck()
	MOVL(op(opL), rL.As32())
	MOVL(op(opR), rR.As32())
	arenaCheck(rL, avx2ALim)
	arenaCheck(rR, avx2ALim)
	VPCMPEQB(reg.Y15, reg.Y15, reg.Y15)
	// The L cursor moves by 32-pr per step: -pr here, +32 per step in mergeLoopAVX2.
	lDisp := 0
	mergeLoopAVX2("vv", true, func(disp int) {
		if disp == 0 {
			lDisp = 0
		}
		MOVL(Mem{Base: rSrc, Disp: disp / 8}, rT.As32())
		shuf32()
		POPCNTL(rT.As32(), rT.As32())
		VMOVDQU(Mem{Base: rR}, reg.X1)
		VINSERTI128(U8(1), Mem{Base: rR, Index: rT3, Scale: 1}, reg.Y1, reg.Y1)
		NEGQ(rT3)
		VMOVDQU(Mem{Base: rL, Disp: lDisp}, reg.X2)
		VINSERTI128(U8(1), Mem{Base: rL, Index: rT3, Scale: 1, Disp: lDisp + 16}, reg.Y2, reg.Y2)
		VPSHUFB(reg.Y0, reg.Y1, reg.Y1)
		VPXOR(reg.Y15, reg.Y0, reg.Y0)
		VPSHUFB(reg.Y0, reg.Y2, reg.Y2)
		VPOR(reg.Y2, reg.Y1, reg.Y1)
		VMOVDQU(reg.Y1, Mem{Base: rOut, Disp: disp})
		ADDQ(rT, rR)
		SUBQ(rT, rL)
		lDisp += 32
	})
	mergeTail("vv", true, func() {
		shuf16(0, reg.Y0)
		VMOVDQU(Mem{Base: rR}, reg.X1)
		VPSHUFB(reg.X0, reg.X1, reg.X1)
		VPXOR(reg.X15, reg.X0, reg.X0)
		VMOVDQU(Mem{Base: rL}, reg.X2)
		VPSHUFB(reg.X0, reg.X2, reg.X2)
		VPOR(reg.X2, reg.X1, reg.X1)
	})
}

func mergeCVAVX2() {
	Label("cv")
	Comment("out[j] = bit j ? R[r++] : P (upstream merge_cst_vec_x86)")
	bitmapCheck()
	MOVL(op(opR), rR.As32())
	arenaCheck(rR, avx2ALim)
	MOVBLZX(op(opP), rT.As32())
	VMOVD(rT.As32(), reg.X3)
	VPBROADCASTB(reg.X3, reg.Y3)
	mergeLoopAVX2("cv", false, func(disp int) {
		MOVL(Mem{Base: rSrc, Disp: disp / 8}, rT.As32())
		shuf32()
		POPCNTL(rT.As32(), rT.As32())
		VMOVDQU(Mem{Base: rR}, reg.X1)
		VINSERTI128(U8(1), Mem{Base: rR, Index: rT3, Scale: 1}, reg.Y1, reg.Y1)
		VPSHUFB(reg.Y0, reg.Y1, reg.Y1)
		// L lanes have the sign bit of the index set.
		VPBLENDVB(reg.Y0, reg.Y3, reg.Y1, reg.Y1)
		VMOVDQU(reg.Y1, Mem{Base: rOut, Disp: disp})
		ADDQ(rT, rR)
	})
	mergeTail("cv", false, func() {
		shuf16(0, reg.Y0)
		VMOVDQU(Mem{Base: rR}, reg.X1)
		VPSHUFB(reg.X0, reg.X1, reg.X1)
		VPBLENDVB(reg.X0, reg.X3, reg.X1, reg.X1)
	})
}

func mergeCCAVX2() {
	Label("cc")
	Comment("out[j] = bit j ? Q : P (upstream merge_cst_cst_x86)")
	bitmapCheck()
	MOVBLZX(op(opP), rT.As32())
	VMOVD(rT.As32(), reg.X1)
	VPBROADCASTB(reg.X1, reg.Y1)
	MOVBLZX(op(opQ), rT.As32())
	VMOVD(rT.As32(), reg.X2)
	VPBROADCASTB(reg.X2, reg.Y2)
	VMOVDQU(tab(dtCCShuf), reg.Y3)
	VMOVDQU(tab(dtCCBits), reg.Y4)
	// Byte k of the bitmap word to lanes 8k..8k+7, then lane j's bit to all of its bits.
	MOVQ(rK, rT1)
	SHRQ(U8(5), rT1)
	JZ(LabelRef("cc_16"))
	PCALIGN(U8(64))
	Label("cc_loop")
	VPBROADCASTD(Mem{Base: rSrc}, reg.Y0)
	VPSHUFB(reg.Y3, reg.Y0, reg.Y0)
	VPAND(reg.Y4, reg.Y0, reg.Y0)
	VPCMPEQB(reg.Y4, reg.Y0, reg.Y0)
	VPBLENDVB(reg.Y0, reg.Y2, reg.Y1, reg.Y0)
	VMOVDQU(reg.Y0, Mem{Base: rOut})
	ADDQ(U8(4), rSrc)
	ADDQ(U8(32), rOut)
	DECQ(rT1)
	JNZ(LabelRef("cc_loop"))
	Label("cc_16")
	BTL(U8(4), rK.As32())
	JNC(LabelRef("cc_tail"))
	MOVWLZX(Mem{Base: rSrc}, rT.As32())
	ccStep16()
	VMOVDQU(reg.X0, Mem{Base: rOut})
	ADDQ(U8(2), rSrc)
	ADDQ(U8(16), rOut)
	Label("cc_tail")
	ANDL(U8(15), rK.As32())
	JZ(LabelRef("advance"))
	tailBits("cc")
	ccStep16()
	storeExact(rOut, rK, reg.X0, reg.X5, rT1, rT3, shiftTab(), "cc_st")
	JMP(LabelRef("advance"))
}

// ccStep16 selects P or Q into X0 by the 16 bits in AX.
func ccStep16() {
	VMOVD(rT.As32(), reg.X0)
	VPSHUFB(reg.X3, reg.X0, reg.X0)
	VPAND(reg.X4, reg.X0, reg.X0)
	VPCMPEQB(reg.X4, reg.X0, reg.X0)
	VPBLENDVB(reg.X0, reg.X2, reg.X1, reg.X0)
}

// flatPlan is the vector register map of a flat kernel of depth d: temporaries from Y0
// (Y0 the codes, Y1.. the lookup's bit selectors and quarters), the 2^d-entry table as
// 16-byte quarters in the top registers, then the columns and byte masks of a vertical
// block in what is left (else they are memory operands).
type flatPlan struct {
	d, tab int
	cols   []reg.VecPhysical
	masks  map[byte]reg.VecPhysical
	morder []byte // masks in register order
}

func newFlatPlan(d int, vertical bool) *flatPlan {
	tmp := 3 // natural: window and two halves
	if d >= 5 {
		tmp = 2*d - 6
	} else if vertical {
		tmp = 2
	}
	p := &flatPlan{d: d, tab: 16 - max(1, 1<<d/16), masks: map[byte]reg.VecPhysical{}}
	free := ys[tmp:p.tab]
	if !vertical {
		return p
	}
	if len(free) >= d {
		p.cols, free = free[:d], free[d:]
	}
	// Masks by use count, most used first.
	uses := map[byte]int{}
	var order []byte
	for s := range 8 {
		for _, m := range vertMasks(d, s) {
			if uses[m] == 0 {
				order = append(order, m)
			}
			uses[m]++
		}
	}
	sort.SliceStable(order, func(i, j int) bool { return uses[order[i]] > uses[order[j]] })
	for _, m := range order {
		if len(free) == 0 {
			break
		}
		p.masks[m], free = free[0], free[1:]
		p.morder = append(p.morder, m)
	}
	return p
}

// vertMasks returns the byte masks step s of a depth-d vertical block uses.
func vertMasks(d, s int) []byte {
	o := s * d & 7
	if o+d <= 8 {
		return []byte{byte(1<<d - 1)}
	}
	return []byte{byte(0xFF >> o), byte(0xFF<<(8-o)) & byte(1<<d-1)}
}

// mask returns the splat of v as a register if the plan has one, else from memory.
func (p *flatPlan) mask(v byte, w int) Op {
	if r, ok := p.masks[v]; ok {
		return view(r, w)
	}
	return splat(v)
}

// tables loads the lookup table from R12 = &R2S[P], 16-byte quarters broadcast to both lanes.
func (p *flatPlan) tables() {
	for i := range 16 - p.tab {
		VBROADCASTI128(Mem{Base: rR, Disp: 16 * i}, ys[p.tab+i])
	}
	for _, v := range p.morder {
		VMOVDQU(splat(v), p.masks[v])
	}
}

// view returns v at the given width (16 or 32 bytes).
func view(v reg.VecPhysical, width int) reg.VecPhysical {
	if width == 16 {
		return v.AsX().(reg.VecPhysical)
	}
	return v
}

// lookup maps the D-bit codes in Y0 (bits above D clear) to symbols in Y0 with PSHUFB
// on 16-entry quarters and a VPBLENDVB tree on bits 4..6 (the MAP forms of
// x86_vertical.h).
func (p *flatPlan) lookup(w int) {
	d := p.d
	v := view(reg.Y0, w)
	if d <= 4 {
		VPSHUFB(v, view(ys[p.tab], w), v)
		return
	}
	var sel [3]reg.VecPhysical // bit 4, 5, 6 at the sign
	for b := 4; b < d; b++ {
		sel[b-4] = view(ys[b-3], w)
		VPSLLW(U8(7-b), v, sel[b-4])
	}
	// Quarter q merged pairwise as soon as its sibling is there: bit 4 first. The last
	// blend writes v, after the last PSHUFB has read it.
	var free []reg.VecPhysical
	for i := d - 3; i < 2*d-6; i++ {
		free = append(free, view(ys[i], w))
	}
	var stack []reg.VecPhysical
	var lvl []int
	for q := range 1 << (d - 4) {
		r := free[0]
		free = free[1:]
		VPSHUFB(v, view(ys[p.tab+q], w), r)
		stack, lvl = append(stack, r), append(lvl, 0)
		for len(lvl) >= 2 && lvl[len(lvl)-1] == lvl[len(lvl)-2] {
			a, b := stack[len(stack)-2], stack[len(stack)-1]
			l := lvl[len(lvl)-1]
			dst := a
			if l == d-5 {
				dst = v
			}
			VPBLENDVB(sel[l], b, a, dst)
			free = append([]reg.VecPhysical{b}, free...)
			stack, lvl = stack[:len(stack)-1], lvl[:len(lvl)-1]
			stack[len(stack)-1], lvl[len(lvl)-1] = dst, l+1
		}
	}
}

// codes extracts field s of a vertical block's lanes into Y0 (width w): bits s*D... of
// the column bytes at SI+stride*j+off, or of the plan's column registers.
func (p *flatPlan) codes(s, w, stride, off int) {
	d := p.d
	bit := s * d
	j, o := bit>>3, bit&7
	v, t := view(reg.Y0, w), view(reg.Y1, w)
	col := func(j int, dst reg.VecPhysical) reg.VecPhysical {
		if p.cols != nil {
			return view(p.cols[j], w)
		}
		VMOVDQU(Mem{Base: rSrc, Disp: stride*j + off}, dst)
		return dst
	}
	c := col(j, v)
	if o == 0 {
		VPAND(p.mask(byte(1<<d-1), w), c, v)
		return
	}
	VPSRLW(U8(o), c, v)
	if o+d <= 8 {
		VPAND(p.mask(byte(1<<d-1), w), v, v)
		return
	}
	VPAND(p.mask(byte(0xFF>>o), w), v, v)
	c = col(j+1, t)
	VPSLLW(U8(8-o), c, t)
	VPAND(p.mask(byte(0xFF<<(8-o))&byte(1<<d-1), w), t, t)
	VPOR(t, v, v)
}

// verticalAVX2 emits the 512-code (two 32-lane halves of 64 lanes) or 128-code (16
// lanes) vertical kernels (vert512_merge_ymm_d*, vert_merge_x86_d*). Lane l of a block
// holds its 8 codes in D column bytes at stride lanes.
func verticalAVX2(pc gotypes.Component, wide bool) {
	name, lanes, kmask := "v128", 16, 127
	if wide {
		name, lanes, kmask = "v512", 64, 511
	}
	Label(name)
	MOVBLZX(op(opD), rL.As32())
	LEAL(Mem{Base: rL, Disp: -2}, rT.As32())
	CMPL(rT.As32(), U8(5))
	JA(LabelRef("fail"))
	TESTL(U32(uint32(kmask)), rK.As32())
	JNZ(LabelRef("fail"))
	flatChecks(pc, true)
	if wide {
		SHRQ(U8(9), rK)
	} else {
		SHRQ(U8(7), rK)
	}
	JZ(LabelRef("advance"))
	for d := 2; d <= 6; d++ {
		CMPL(rL.As32(), U8(d))
		JE(LabelRef(fmt.Sprintf("%s_d%d", name, d)))
	}
	for d := 7; d >= 2; d-- {
		l := fmt.Sprintf("%s_d%d", name, d)
		Label(l)
		p := newFlatPlan(d, true)
		p.tables()
		PCALIGN(U8(32))
		Label(l + "_loop")
		w, halves := 16, 1
		if wide {
			w, halves = 32, 2
		}
		for h := range halves {
			for j, c := range p.cols {
				VMOVDQU(Mem{Base: rSrc, Disp: lanes*j + w*h}, view(c, w))
			}
			for s := range 8 {
				p.codes(s, w, lanes, w*h)
				p.lookup(w)
				VMOVDQU(view(reg.Y0, w), Mem{Base: rOut, Disp: lanes*s + w*h})
			}
		}
		ADDQ(U32(uint32(lanes*d)), rSrc)
		ADDQ(U32(uint32(8*lanes)), rOut)
		DECQ(rK)
		JNZ(LabelRef(l + "_loop"))
		JMP(LabelRef("advance"))
	}
}

// naturalAVX2 emits the natural-layout kernels for D = 2..8. For D <= 7 a 16-code step
// reads a 16-byte window at 2D*step; the 32-code step does two windows in the two lanes.
func naturalAVX2(pc gotypes.Component) {
	Label("nat")
	MOVBLZX(op(opD), rL.As32())
	LEAL(Mem{Base: rL, Disp: -2}, rT.As32())
	CMPL(rT.As32(), U8(6))
	JA(LabelRef("fail"))
	LEAQ(Mem{Base: rSrc, Index: rT1, Scale: 1}, rT3) // end of the source
	flatChecks(pc, false)
	for d := 2; d <= 7; d++ {
		CMPL(rL.As32(), U8(d))
		JE(LabelRef(fmt.Sprintf("nat_d%d", d)))
	}
	Comment("D = 8: codes are bytes.")
	TESTQ(rK, rK)
	JZ(LabelRef("advance"))
	Label("nat_d8")
	MOVBLZX(Mem{Base: rSrc}, rT.As32())
	MOVBLZX(Mem{Base: rR, Index: rT, Scale: 1}, rT.As32())
	MOVB(rT.As8(), Mem{Base: rOut})
	INCQ(rSrc)
	INCQ(rOut)
	DECQ(rK)
	JNZ(LabelRef("nat_d8"))
	JMP(LabelRef("advance"))

	for d := 7; d >= 2; d-- {
		l := fmt.Sprintf("nat_d%d", d)
		Label(l)
		p := newFlatPlan(d, false)
		p.tables()
		CMPQ(rK, U8(32))
		JB(LabelRef(l + "_16"))
		LEAQ(Mem{Base: rSrc, Disp: 2*d + 16}, rT)
		CMPQ(rT, rT3)
		JA(LabelRef(l + "_16"))
		PCALIGN(U8(32))
		Label(l + "_32")
		VMOVDQU(Mem{Base: rSrc}, reg.X0)
		VINSERTI128(U8(1), Mem{Base: rSrc, Disp: 2 * d}, reg.Y0, reg.Y0)
		unpack(d, 32)
		p.lookup(32)
		VMOVDQU(reg.Y0, Mem{Base: rOut})
		ADDQ(U8(4*d), rSrc)
		ADDQ(U8(32), rOut)
		SUBQ(U8(32), rK)
		CMPQ(rK, U8(32))
		JB(LabelRef(l + "_16"))
		LEAQ(Mem{Base: rSrc, Disp: 2*d + 16}, rT)
		CMPQ(rT, rT3)
		JBE(LabelRef(l + "_32"))
		Label(l + "_16")
		CMPQ(rK, U8(16))
		JB(LabelRef(l + "_exact"))
		LEAQ(Mem{Base: rSrc, Disp: 16}, rT)
		CMPQ(rT, rT3)
		JA(LabelRef(l + "_exact"))
		VMOVDQU(Mem{Base: rSrc}, reg.X0)
		unpack(d, 16)
		p.lookup(16)
		VMOVDQU(reg.X0, Mem{Base: rOut})
		ADDQ(U8(2*d), rSrc)
		ADDQ(U8(16), rOut)
		SUBQ(U8(16), rK)
		JMP(LabelRef(l + "_16"))
		Comment("Near the end of the source: exact loads of min(16, rest) codes.")
		Label(l + "_exact")
		TESTQ(rK, rK)
		JZ(LabelRef("advance"))
		MOVL(U32(16), rT.As32())
		CMPQ(rK, rT)
		CMOVQLT(rK, rT)
		IMUL3Q(U8(d), rT, rT1)
		ADDQ(U8(7), rT1)
		SHRQ(U8(3), rT1)
		loadExact(rSrc, rT1, reg.X0, reg.X1, rT2, rL, shiftTab(), l+"_ld")
		unpack(d, 16)
		p.lookup(16)
		storeExact(rOut, rT, reg.X0, reg.X1, rT1, rT2, shiftTab(), l+"_st")
		ADDQ(U8(2*d), rSrc)
		ADDQ(U8(16), rOut)
		SUBQ(U8(16), rK)
		JG(LabelRef(l + "_exact"))
		JMP(LabelRef("advance"))
	}
}

// unpack turns the window(s) in Y0 into the codes of its 16 (per lane) D-bit fields.
func unpack(d, w int) {
	v, a, b := view(reg.Y0, w), view(reg.Y1, w), view(reg.Y2, w)
	o := dtNat[d]
	VPSHUFB(tab(o), v, a)
	VPSHUFB(tab(o+32), v, b)
	VPMULLW(tab(o+64), a, a)
	VPMULLW(tab(o+64), b, b)
	VPSRLW(U8(16-d), a, a)
	VPSRLW(U8(16-d), b, b)
	VPACKUSWB(b, a, v)
}

func copyFlatAVX2() {
	Label("copy")
	Comment("D = 8 flat: the codes are the symbols (upstream merge_flat_d8_x86).")
	MOVBLZX(op(opD), rT.As32())
	CMPL(rT.As32(), U8(8))
	JNE(LabelRef("fail"))
	LEAQ(Mem{Base: rT2, Index: rK, Scale: 1}, rT)
	CMPQ(rT, rT1)
	JA(LabelRef("fail"))
	ADDQ(rT2, rSrc)
	CMPQ(rK, U8(64))
	JB(LabelRef("copy_16"))
	PCALIGN(U8(32))
	Label("copy_64")
	VMOVDQU(Mem{Base: rSrc}, reg.Y0)
	VMOVDQU(Mem{Base: rSrc, Disp: 32}, reg.Y1)
	VMOVDQU(reg.Y0, Mem{Base: rOut})
	VMOVDQU(reg.Y1, Mem{Base: rOut, Disp: 32})
	ADDQ(U8(64), rSrc)
	ADDQ(U8(64), rOut)
	SUBQ(U8(64), rK)
	CMPQ(rK, U8(64))
	JAE(LabelRef("copy_64"))
	Label("copy_16")
	CMPQ(rK, U8(16))
	JB(LabelRef("copy_tail"))
	VMOVDQU(Mem{Base: rSrc}, reg.X0)
	VMOVDQU(reg.X0, Mem{Base: rOut})
	ADDQ(U8(16), rSrc)
	ADDQ(U8(16), rOut)
	SUBQ(U8(16), rK)
	JMP(LabelRef("copy_16"))
	Label("copy_tail")
	TESTQ(rK, rK)
	JZ(LabelRef("advance"))
	loadExact(rSrc, rK, reg.X0, reg.X1, rT1, rT3, shiftTab(), "copy_ld")
	storeExact(rOut, rK, reg.X0, reg.X1, rT1, rT3, shiftTab(), "copy_st")
	JMP(LabelRef("advance"))
}
