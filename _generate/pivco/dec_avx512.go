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

// dec_avx512.go generates the AVX-512 decoder executor, execAVX512Asm: one call runs a
// whole kernel.Program op list, so the per-op cost is a dispatch, not a Go call.
//
// Requirements: AVX512 F/BW/VL/VBMI/VBMI2 and BMI2 (kernel.hasAVX512).
//
// Register map (all physical; the function has no frame):
//
//	DI  *Program                BX  current op      R8  end of the ops
//	R13 &Arena[0]               R10 max(len(Arena)-ArenaSlack, 0)
//	SI  bitmap/region pointer   DX  output pointer  CX  K, then a loop count
//	R11 left cursor / D         R12 right cursor / &R2S[P]
//	AX, R9, R14 temporaries (R9 = source length and R14 = Bm in the op prologue)
//	Z10..Z13  the flat lookup table, Z14/Z15 natural gather/shift controls,
//	Z16/Z17   merge constants, Z0..Z9 and Z16..Z23 scratch.
//
// R15 is not used: with -dynlink it is clobbered by global accesses.
//
// Geometry: like execGeneric the kernel checks each op against the slices before that
// op writes anything, so a planner bug fails closed (earlier ops have written). Reads of
// In and writes are exact (masked tails). The merges load 64 bytes at each child cursor, which stays
// inside the op's K-byte child, so they read at most 63 bytes past it (ArenaSlack).
//
// Merge loop, A/B on a Ryzen 9 9950X (Zen 5; BenchmarkExec/avx512, K = 32768, WSL,
// one pinned core):
//   - VPEXPANDB from memory, arena base in R9: the 7-byte base+index form that gcc 13
//     and clang 18 emit for upstream's subtree merges (objdump codec_AVX512.o):
//     vv 636 ns, cv 401 ns.
//   - The same with the arena base in R13, which forces a zero disp8 (the 8-byte form
//     upstream's prims-merge.h variants favour on c8a): vv 535, cv 330.
//   - R13 base, VMOVDQU64 loads and register VPEXPANDB: vv 442, cv 297.
//   - That, unrolled twice: vv 433, cv 293; Block/ph/dec went from 0.79-0.97x C to
//     0.92-1.26x. The loads gain from the R13 base too.
//
// Zen 4/5: the expands are merge-masked into a freshly zeroed (VPXORD idiom) or
// broadcast register; the zero-masking form has a false output dependency there.

import (
	"fmt"

	. "github.com/honeycombio/avo/build"
	"github.com/honeycombio/avo/gotypes"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

var rEnd, rALim = reg.R8, reg.R10

var zr = []reg.VecPhysical{
	reg.Z0, reg.Z1, reg.Z2, reg.Z3, reg.Z4, reg.Z5, reg.Z6, reg.Z7,
	reg.Z8, reg.Z9, reg.Z10, reg.Z11, reg.Z12, reg.Z13, reg.Z14, reg.Z15,
	reg.Z16, reg.Z17, reg.Z18, reg.Z19, reg.Z20, reg.Z21, reg.Z22, reg.Z23,
	reg.Z24, reg.Z25, reg.Z26, reg.Z27, reg.Z28, reg.Z29, reg.Z30, reg.Z31,
}

var tmask, perIdx, natShuf, natCtrl Mem

func decConstants() {
	tmask = GLOBL("decTernMask", RODATA|NOPTR)
	for off := 2; off <= 7; off++ {
		DATA(4*(off-2), U32(0x01010101*uint32(0xFF>>off)))
	}
	// perIdx[D-2][i] = i & (2^D-1): VPERMB with it repeats a 2^D-entry table over 64 bytes,
	// so lookups ignore the junk above bit D of an unmasked code.
	perIdx = GLOBL("decPeriodic", RODATA|NOPTR)
	for d := 2; d <= 5; d++ {
		for i := range 64 {
			DATA(64*(d-2)+i, U8(i&(1<<d-1)))
		}
	}
	// Natural layout: qword k of the gather holds bytes D*k..D*k+7 = codes 8k..8k+7 at
	// bit offsets 0, D, ..., 7D, which VPMULTISHIFTQB moves to the bottom of each byte
	// (upstream pivco_huffman_avx512_flat.h flat_dN_unpack64).
	natShuf = GLOBL("decNatShuf", RODATA|NOPTR)
	for d := 2; d <= 7; d++ {
		for k := range 8 {
			for i := range 8 {
				DATA(64*(d-2)+8*k+i, U8(d*k+i))
			}
		}
	}
	natCtrl = GLOBL("decNatCtrl", RODATA|NOPTR)
	for d := 2; d <= 7; d++ {
		for k := range 8 {
			DATA(8*(d-2)+k, U8(d*k))
		}
	}
}

func genDecAVX512() {
	decConstants()
	TEXT("execAVX512Asm", NOSPLIT, "func(p *Program) int")
	Doc("execAVX512Asm runs p.Ops and returns -1, or the index of the first op that fails",
		"its checks.")
	Pragma("noescape")
	pc := Param("p").Dereference(rP)
	Load(Param("p"), rP)
	MOVQ(field(pc.Field("Ops").Base()), rOp)
	MOVQ(field(pc.Field("Ops").Len()), rEnd)
	SHLQ(U8(5), rEnd)
	ADDQ(rOp, rEnd)
	MOVQ(field(pc.Field("Arena").Base()), rArena)
	XORL(rT.As32(), rT.As32())
	MOVQ(field(pc.Field("Arena").Len()), rALim)
	SUBQ(U32(arenaSlack), rALim)
	CMOVQLT(rT, rALim)
	dispatch(pc, rEnd, rALim)

	mergeVV()
	mergeCV()
	mergeCC()
	vertical(true)
	vertical(false)
	natural(pc)
	copyFlat()

	exits(pc)
}

// tailMasks loads the last ceil((K&63)/8) bitmap bytes exactly and sets K1 to its
// K&63 valid bits, K3 to the valid-bit mask and AX to the popcount of K1.
// CX holds K&63 (non-zero) on entry.
func tailMasks() {
	LEAL(Mem{Base: rK, Disp: 7}, rT.As32())
	SHRL(U8(3), rT.As32())
	MOVQ(I32(-1), rT1)
	BZHIQ(rT, rT1, rT)
	KMOVQ(rT, reg.K3)
	VMOVDQU8_Z(Mem{Base: rSrc}, reg.K3, reg.X1)
	VMOVQ(reg.X1, rT)
	BZHIQ(rK, rT1, rT1)
	ANDQ(rT1, rT)
	KMOVQ(rT, reg.K1)
	KMOVQ(rT1, reg.K3)
	POPCNTQ(rT, rT)
}

func mergeVV() {
	Label("vv")
	Comment("out[j] = bit j ? R[r++] : L[l++] (upstream merge_vec_vec_avx512)")
	bitmapCheck()
	MOVL(op(opL), rL.As32())
	MOVL(op(opR), rR.As32())
	arenaCheck(rL, rALim)
	arenaCheck(rR, rALim)
	mergeLoop("vv", vvStep)
	Label("vv_tail")
	ANDL(U8(63), rK.As32())
	JZ(LabelRef("vv_check"))
	tailMasks()
	KANDNQ(reg.K3, reg.K1, reg.K2)
	VPXORD(reg.Z0, reg.Z0, reg.Z0)
	expand(rL, reg.K2, reg.Z0, reg.Z1)
	expand(rR, reg.K1, reg.Z0, reg.Z2)
	VMOVDQU8(reg.Z0, reg.K3, Mem{Base: rOut})
	ADDQ(rT, rR)
	Label("vv_check")
	krCheck()
}

func mergeCV() {
	Label("cv")
	Comment("out[j] = bit j ? R[r++] : P (upstream merge_cst_vec_avx512)")
	bitmapCheck()
	MOVL(op(opR), rR.As32())
	arenaCheck(rR, rALim)
	MOVBLZX(op(opP), rT.As32())
	VPBROADCASTB(rT.As32(), reg.Z16)
	mergeLoop("cv", cvStep)
	Label("cv_tail")
	ANDL(U8(63), rK.As32())
	JZ(LabelRef("cv_check"))
	tailMasks()
	VMOVDQA64(reg.Z16, reg.Z0)
	expand(rR, reg.K1, reg.Z0, reg.Z2)
	VMOVDQU8(reg.Z0, reg.K3, Mem{Base: rOut})
	ADDQ(rT, rR)
	Label("cv_check")
	krCheck()
}

func mergeCC() {
	Label("cc")
	Comment("out[j] = bit j ? Q : P (upstream merge_cst_cst_avx512)")
	bitmapCheck()
	MOVBLZX(op(opP), rT.As32())
	VPBROADCASTB(rT.As32(), reg.Z16)
	MOVBLZX(op(opQ), rT.As32())
	VPBROADCASTB(rT.As32(), reg.Z17)
	MOVQ(rK, rT1)
	SHRQ(U8(6), rT1)
	JZ(LabelRef("cc_tail"))
	PCALIGN(U8(64))
	Label("cc_loop")
	KMOVQ(Mem{Base: rSrc}, reg.K1)
	VPBLENDMB(reg.Z17, reg.Z16, reg.K1, reg.Z0)
	VMOVDQU64(reg.Z0, Mem{Base: rOut})
	ADDQ(U8(8), rSrc)
	ADDQ(U8(64), rOut)
	DECQ(rT1)
	JNZ(LabelRef("cc_loop"))
	Label("cc_tail")
	ANDL(U8(63), rK.As32())
	JZ(LabelRef("advance"))
	tailMasks()
	VPBLENDMB(reg.Z17, reg.Z16, reg.K1, reg.Z0)
	VMOVDQU8(reg.Z0, reg.K3, Mem{Base: rOut})
	JMP(LabelRef("advance"))
}

// table loads the lookup table of depth d from R12 = &R2S[P]: for d <= 6 into Z10,
// repeated every 2^d bytes so that VPERMB ignores code bits >= d; for d = 7 into
// Z10:Z11 (VPERMI2B ignores bit 7); for d = 8 into Z10..Z13. Reads are exact.
func table(d int) {
	switch {
	case d <= 5:
		MOVL(U32(uint32(uint64(1)<<(1<<d)-1)), rT.As32())
		KMOVQ(rT, reg.K1)
		VMOVDQU8_Z(Mem{Base: rR}, reg.K1, reg.Z10)
		VMOVDQU64(perIdx.Offset(64*(d-2)), reg.Z11)
		VPERMB(reg.Z10, reg.Z11, reg.Z10)
	default:
		for i := range 1 << d / 64 {
			VMOVDQU64(Mem{Base: rR, Disp: 64 * i}, zr[10+i])
		}
	}
}

// lookup maps the codes in v (junk above bit d allowed) to symbols in dst; width is
// 16, 32 or 64 bytes of meaningful lanes. For d = 7, VPERMI2B overwrites its index,
// so v is copied unless it is dst.
func lookup(d int, v, dst reg.VecPhysical, width int) {
	switch {
	case d == 7:
		if v != dst {
			VMOVDQA64(v.AsZ(), dst.AsZ())
		}
		VPERMI2B(reg.Z11, reg.Z10, dst.AsZ())
	case d == 6 || width == 64:
		VPERMB(reg.Z10, v.AsZ(), dst.AsZ())
	case d == 5:
		VPERMB(reg.Y10, v.AsY(), dst.AsY())
	default:
		VPERMB(reg.X10, v.AsX(), dst.AsX())
	}
}

// vertical emits the 512-code (64-lane, wide) or 128-code (16-lane) vertical kernels
// (upstream vert512_merge_zmm_d* and vert_merge_x86_d*). Lane l of a block holds its
// 8 codes in D column bytes at stride lanes; step s extracts bit field s*D of every
// lane, which is codes s*lanes .. s*lanes+lanes-1.
func vertical(wide bool) {
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
	flatChecks(Param("p").Dereference(rP), true)
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
		table(d)
		PCALIGN(U8(32))
		Label(l + "_loop")
		vertBlock(d, lanes)
		ADDQ(U32(uint32(lanes*d)), rSrc)
		ADDQ(U32(uint32(8*lanes)), rOut)
		DECQ(rK)
		JNZ(LabelRef(l + "_loop"))
		JMP(LabelRef("advance"))
	}
}

func vec(i, lanes int) reg.VecPhysical {
	if lanes == 16 {
		return zr[i].AsX().(reg.VecPhysical)
	}
	return zr[i]
}

func vertBlock(d, lanes int) {
	for j := range d {
		VMOVDQU64(Mem{Base: rSrc, Disp: j * lanes}, vec(j, lanes))
	}
	for s := range 8 {
		bit := s * d
		j, off := bit>>3, bit&7
		a, b := vec(16+2*(s&3), lanes), vec(17+2*(s&3), lanes)
		v := a
		switch {
		case off == 0:
			v = vec(j, lanes)
		case off+d <= 8:
			VPSRLW(U8(off), vec(j, lanes), a)
		default:
			// Bits off..7 of column j, then bits 0..off+d-9 of column j+1: the 16-bit
			// shifts leave the neighbour byte's bits where the other column's go, so
			// select per bit (0xE4: mask ? a : b) with the mask 0xFF>>off broadcast.
			VPSRLW(U8(off), vec(j, lanes), a)
			VPSLLW(U8(8-off), vec(j+1, lanes), b)
			VPTERNLOGD_BCST(U8(0xE4), tmask.Offset(4*(off-2)), b, a)
		}
		lookup(d, v, a, lanes)
		VMOVDQU64(a, Mem{Base: rOut, Disp: s * lanes})
	}
}

// natural emits the natural-layout kernels for D = 2..8: 64 codes per step from an
// exact masked load of 8*D bytes, the last step loads ceil(rest*D/8) bytes and stores
// rest symbols (upstream merge_flat_avx512 64-wide loops, without the fast/safe split).
func natural(pc gotypes.Component) {
	Label("nat")
	MOVBLZX(op(opD), rL.As32())
	LEAL(Mem{Base: rL, Disp: -2}, rT.As32())
	CMPL(rT.As32(), U8(6))
	JA(LabelRef("fail"))
	flatChecks(pc, false)
	for d := 2; d <= 7; d++ {
		CMPL(rL.As32(), U8(d))
		JE(LabelRef(fmt.Sprintf("nat_d%d", d)))
	}
	for d := 8; d >= 2; d-- {
		l := fmt.Sprintf("nat_d%d", d)
		Label(l)
		table(d)
		if d < 8 {
			VMOVDQU64(natShuf.Offset(64*(d-2)), reg.Z14)
			VPBROADCASTQ(natCtrl.Offset(8*(d-2)), reg.Z15)
			MOVQ(U64(uint64(1)<<(8*d)-1), rT)
			KMOVQ(rT, reg.K1)
		}
		CMPQ(rK, U8(64))
		JB(LabelRef(l + "_tail"))
		PCALIGN(U8(32))
		Label(l + "_loop")
		if d < 8 {
			VMOVDQU8_Z(Mem{Base: rSrc}, reg.K1, reg.Z0)
		} else {
			VMOVDQU64(Mem{Base: rSrc}, reg.Z0)
		}
		natCodes(d)
		VMOVDQU64(reg.Z0, Mem{Base: rOut})
		ADDQ(U8(8*d), rSrc)
		ADDQ(U8(64), rOut)
		SUBQ(U8(64), rK)
		CMPQ(rK, U8(64))
		JAE(LabelRef(l + "_loop"))
		Label(l + "_tail")
		TESTQ(rK, rK)
		JZ(LabelRef("advance"))
		MOVQ(I32(-1), rT1)
		if d < 8 {
			IMUL3Q(U8(d), rK, rT)
			ADDQ(U8(7), rT)
			SHRQ(U8(3), rT)
			BZHIQ(rT, rT1, rT)
			KMOVQ(rT, reg.K2)
		}
		BZHIQ(rK, rT1, rT1)
		KMOVQ(rT1, reg.K3)
		if d < 8 {
			VMOVDQU8_Z(Mem{Base: rSrc}, reg.K2, reg.Z0)
		} else {
			VMOVDQU8_Z(Mem{Base: rSrc}, reg.K3, reg.Z0)
		}
		natCodes(d)
		VMOVDQU8(reg.Z0, reg.K3, Mem{Base: rOut})
		JMP(LabelRef("advance"))
	}
}

// natCodes turns the region bytes in Z0 into 64 symbols.
func natCodes(d int) {
	if d < 8 {
		VPERMB(reg.Z0, reg.Z14, reg.Z0)
		VPMULTISHIFTQB(reg.Z0, reg.Z15, reg.Z0)
		lookup(d, reg.Z0, reg.Z0, 64)
		return
	}
	// 256 entries: two 128-entry VPERMI2B lookups, picked by bit 7.
	VPMOVB2M(reg.Z0, reg.K4)
	VMOVDQA64(reg.Z0, reg.Z1)
	VPERMI2B(reg.Z11, reg.Z10, reg.Z0)
	VPERMI2B(reg.Z13, reg.Z12, reg.Z1)
	VPBLENDMB(reg.Z1, reg.Z0, reg.K4, reg.Z0)
}

func copyFlat() {
	Label("copy")
	Comment("D = 8 flat: the codes are the symbols (upstream merge_flat_d8_avx512).")
	MOVBLZX(op(opD), rT.As32())
	CMPL(rT.As32(), U8(8))
	JNE(LabelRef("fail"))
	LEAQ(Mem{Base: rT2, Index: rK, Scale: 1}, rT)
	CMPQ(rT, rT1)
	JA(LabelRef("fail"))
	ADDQ(rT2, rSrc)
	CMPQ(rK, U32(256))
	JB(LabelRef("copy_64"))
	PCALIGN(U8(32))
	Label("copy_256")
	for i := range 4 {
		VMOVDQU64(Mem{Base: rSrc, Disp: 64 * i}, zr[i])
	}
	for i := range 4 {
		VMOVDQU64(zr[i], Mem{Base: rOut, Disp: 64 * i})
	}
	ADDQ(U32(256), rSrc)
	ADDQ(U32(256), rOut)
	SUBQ(U32(256), rK)
	CMPQ(rK, U32(256))
	JAE(LabelRef("copy_256"))
	Label("copy_64")
	CMPQ(rK, U8(64))
	JB(LabelRef("copy_tail"))
	VMOVDQU64(Mem{Base: rSrc}, reg.Z0)
	VMOVDQU64(reg.Z0, Mem{Base: rOut})
	ADDQ(U8(64), rSrc)
	ADDQ(U8(64), rOut)
	SUBQ(U8(64), rK)
	JMP(LabelRef("copy_64"))
	Label("copy_tail")
	TESTQ(rK, rK)
	JZ(LabelRef("advance"))
	MOVQ(I32(-1), rT)
	BZHIQ(rK, rT, rT)
	KMOVQ(rT, reg.K1)
	VMOVDQU8_Z(Mem{Base: rSrc}, reg.K1, reg.Z0)
	VMOVDQU8(reg.Z0, reg.K1, Mem{Base: rOut})
	JMP(LabelRef("advance"))
}

// expand merges the next popcount(k) bytes at arena offset cur into the k lanes of z.
func expand(cur reg.Register, k reg.Register, z, tmp reg.VecPhysical) {
	VMOVDQU64(Mem{Base: rArena, Index: cur, Scale: 1}, tmp)
	VPEXPANDB(tmp, k, z)
}

// mergeLoop emits the full 64-symbol steps of a merge, two per iteration.
func mergeLoop(name string, step func(i, disp int)) {
	MOVQ(rK, rT1)
	SHRQ(U8(7), rT1)
	JZ(LabelRef(name + "_one"))
	PCALIGN(U8(64))
	Label(name + "_loop")
	step(0, 0)
	step(1, 64)
	ADDQ(U8(16), rSrc)
	SUBQ(I8(-128), rOut)
	DECQ(rT1)
	JNZ(LabelRef(name + "_loop"))
	Label(name + "_one")
	BTL(U8(6), rK.As32())
	JNC(LabelRef(name + "_tail"))
	step(0, 0)
	ADDQ(U8(8), rSrc)
	ADDQ(U8(64), rOut)
}

// vvStep emits one 64-symbol VV step at output offset disp; i picks the registers.
func vvStep(i, disp int) {
	z, k1, k2 := []reg.VecPhysical{reg.Z0, reg.Z3}[i], []reg.Register{reg.K1, reg.K4}[i], []reg.Register{reg.K2, reg.K5}[i]
	MOVQ(Mem{Base: rSrc, Disp: disp / 8}, rT)
	KMOVQ(rT, k1)
	KNOTQ(k1, k2)
	VPXORD(z, z, z)
	expand(rL, k2, z, reg.Z1)
	expand(rR, k1, z, reg.Z2)
	VMOVDQU64(z, Mem{Base: rOut, Disp: disp})
	POPCNTQ(rT, rT)
	MOVL(U32(64), rT2.As32())
	SUBQ(rT, rT2)
	ADDQ(rT, rR)
	ADDQ(rT2, rL)
}

// cvStep emits one 64-symbol CV step at output offset disp.
func cvStep(i, disp int) {
	z, k1 := []reg.VecPhysical{reg.Z0, reg.Z3}[i], []reg.Register{reg.K1, reg.K4}[i]
	MOVQ(Mem{Base: rSrc, Disp: disp / 8}, rT)
	KMOVQ(rT, k1)
	VMOVDQA64(reg.Z16, z)
	expand(rR, k1, z, reg.Z2)
	VMOVDQU64(z, Mem{Base: rOut, Disp: disp})
	POPCNTQ(rT, rT)
	ADDQ(rT, rR)
}
