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

// Shared by the decoder executors (dec_avx512.go, dec_avx2.go).

import (
	. "github.com/honeycombio/avo/build"
	"github.com/honeycombio/avo/gotypes"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

// Mirrors of internal/pivco/kernel (TestExecLayout checks them).
const (
	opKind, opFlags, opD, opP, opQ   = 0, 1, 2, 3, 4
	opK, opKR, opBm, opOut, opL, opR = 8, 12, 16, 20, 24, 28
	opSize                           = 32

	kindVV, kindCV, kindCC, kindV512, kindV128, kindNat, kindCopy = 0, 1, 2, 3, 4, 5, 6

	flagDst    = 1
	arenaSlack = 128
	r2sLen     = 256 + 64
)

var (
	rP, rOp, rArena = reg.RDI, reg.RBX, reg.R13
	rSrc, rOut, rK  = reg.RSI, reg.RDX, reg.RCX
	rL, rR          = reg.R11, reg.R12
	rT, rT1, rT2    = reg.RAX, reg.R9, reg.R14
)

func op(disp int) Mem { return Mem{Base: rOp, Disp: disp} }

// dispatch emits the op loop: it checks the output of the op at BX and sets up its
// source, then jumps to the label of its kind. end is the end of the ops, alim
// max(len(Arena)-ArenaSlack, 0).
func dispatch(pc gotypes.Component, end, alim Op) {
	JMP(LabelRef("next"))
	Label("advance")
	ADDQ(U8(opSize), rOp)
	Label("next")
	CMPQ(rOp, end)
	JAE(LabelRef("ok"))
	Comment("Output: Out+K must fit Dst, or the arena less its slack.")
	MOVBLZX(op(opFlags), rT.As32())
	MOVL(op(opK), rK.As32())
	MOVL(op(opOut), rOut.As32())
	MOVQ(rArena, rT2)
	MOVQ(alim, rT1)
	TESTB(U8(flagDst), rT.As8())
	CMOVQNE(field(pc.Field("Dst").Base()), rT2)
	CMOVQNE(field(pc.Field("Dst").Len()), rT1)
	LEAQ(Mem{Base: rOut, Index: rK, Scale: 1}, rL)
	CMPQ(rL, rT1)
	JA(LabelRef("fail"))
	ADDQ(rT2, rOut)
	Comment("Source: In; R9 = its length, R14 = Bm.")
	MOVQ(field(pc.Field("In").Base()), rSrc)
	MOVQ(field(pc.Field("In").Len()), rT1)
	MOVL(op(opBm), rT2.As32())
	MOVBLZX(op(opKind), rT.As32())
	for _, c := range []struct {
		kind  int
		label string
	}{{kindVV, "vv"}, {kindCV, "cv"}, {kindCC, "cc"}, {kindV512, "v512"}, {kindV128, "v128"}, {kindNat, "nat"}, {kindCopy, "copy"}} {
		CMPL(rT.As32(), U8(c.kind))
		JE(LabelRef(c.label))
	}
	JMP(LabelRef("fail"))
}

// exits emits the returns: the index of the op at BX, or -1 at the end of the ops.
func exits(pc gotypes.Component) {
	Label("fail")
	VZEROUPPER()
	MOVQ(field(pc.Field("Ops").Base()), rT)
	SUBQ(rT, rOp)
	SHRQ(U8(5), rOp)
	Store(rOp, ReturnIndex(0))
	RET()
	Label("ok")
	VZEROUPPER()
	MOVQ(I32(-1), rT)
	Store(rT, ReturnIndex(0))
	RET()
}

// bitmapCheck checks that the ceil(K/8)-byte bitmap at Bm lies in the source and
// points SI at it.
func bitmapCheck() {
	LEAQ(Mem{Base: rK, Disp: 7}, rT)
	SHRQ(U8(3), rT)
	ADDQ(rT2, rT)
	CMPQ(rT, rT1)
	JA(LabelRef("fail"))
	ADDQ(rT2, rSrc)
}

// arenaCheck checks that K bytes at the arena offset in r fit below alim.
func arenaCheck(r reg.Register, alim Op) {
	LEAQ(Mem{Base: r, Index: rK, Scale: 1}, rT)
	CMPQ(rT, alim)
	JA(LabelRef("fail"))
}

// krCheck fails unless the right cursor advanced by exactly KR.
func krCheck() {
	MOVL(op(opR), rT.As32())
	SUBQ(rT, rR)
	MOVL(op(opKR), rT.As32())
	CMPQ(rR, rT)
	JNE(LabelRef("fail"))
	JMP(LabelRef("advance"))
}

// flatChecks validates a flat op whose depth D is in R11 and in range: the region
// of K*D/8 (exact) or ceil(K*D/8) bytes at Bm must lie in the source, R2S must be
// set and R2S[P:P+2^D] inside it. It leaves SI at the region and R12 at &R2S[P].
func flatChecks(pc gotypes.Component, exact bool) {
	MOVQ(rK, rT)
	IMULQ(rL, rT)
	if !exact {
		ADDQ(U8(7), rT)
	}
	SHRQ(U8(3), rT)
	ADDQ(rT2, rT)
	CMPQ(rT, rT1)
	JA(LabelRef("fail"))
	ADDQ(rT2, rSrc)
	MOVQ(field(pc.Field("R2S")), rR)
	TESTQ(rR, rR)
	JZ(LabelRef("fail"))
	MOVBLZX(op(opP), rT.As32())
	ADDQ(rT, rR)
	MOVL(U32(1), rT1.As32())
	SHLXQ(rL, rT1, rT1)
	ADDQ(rT, rT1)
	CMPQ(rT1, U32(r2sLen))
	JA(LabelRef("fail"))
}
