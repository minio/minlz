// Copyright 2025 MinIO Inc.
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

//go:generate go run gen.go -out ../asm_amd64.s -stubs ../asm_amd64.go -pkg=minlz
//go:generate gofmt -w ../asm_amd64.go

import (
	"flag"
	"fmt"
	"math"
	"runtime"
	"strings"

	. "github.com/mmcloughlin/avo/build"
	"github.com/mmcloughlin/avo/buildtags"
	"github.com/mmcloughlin/avo/ir"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

const (
	// insert extra checks here and there.
	debug = false
	// matchOffsetCMOV is true if we should use CMOV to check match offsets.
	matchOffsetCMOV = true
)

func main() {
	flag.Parse()
	Constraint(buildtags.Not("appengine").ToConstraint())
	Constraint(buildtags.Not("noasm").ToConstraint())
	Constraint(buildtags.Term("gc").ToConstraint())
	Constraint(buildtags.Not("purego").ToConstraint())

	o := options{
		bmi1:         false,
		bmi2:         false,
		avx2:         false,
		outputMargin: 17,
		inputMargin:  17,
		skipOutput:   false}

	// 16 bits has too big of a speed impact.
	o.fastOpts = fastOpts{match8: false, fuselits: true, checkRepeats: false, checkBack: true, skipOne: false, incLoop: 4}
	o.genEncodeBlockAsm("encodeBlockAsm", 15, 6, 6, 8<<20)
	o.genEncodeBlockAsm("encodeBlockAsm2MB", 15, 6, 6, 2<<20)
	o.genEncodeBlockAsm("encodeBlockAsm512K", 14, 6, 6, 512<<10)
	o.genEncodeBlockAsm("encodeBlockAsm64K", 13, 5, 6, 64<<10)
	o.genEncodeBlockAsm("encodeBlockAsm16K", 12, 5, 5, 16<<10)
	o.genEncodeBlockAsm("encodeBlockAsm4K", 10, 5, 4, 4<<10)
	o.genEncodeBlockAsm("encodeBlockAsm1K", 9, 4, 4, 1<<10)

	o.fastOpts = fastOpts{match8: true, fuselits: false, checkRepeats: true, checkBack: false, skipOne: false, incLoop: 4}
	const fastHashBytes = 8
	o.genEncodeBlockAsm("encodeFastBlockAsm", 14, 5, fastHashBytes, 8<<20)
	o.genEncodeBlockAsm("encodeFastBlockAsm2MB", 13, 5, fastHashBytes, 2<<20)
	o.genEncodeBlockAsm("encodeFastBlockAsm512K", 13, 5, fastHashBytes, 512<<10)
	o.genEncodeBlockAsm("encodeFastBlockAsm64K", 12, 4, fastHashBytes, 64<<10)
	o.genEncodeBlockAsm("encodeFastBlockAsm16K", 11, 4, fastHashBytes, 16<<10)
	o.genEncodeBlockAsm("encodeFastBlockAsm4K", 10, 4, fastHashBytes, 4<<10)
	o.genEncodeBlockAsm("encodeFastBlockAsm1K", 9, 3, fastHashBytes, 1<<10)

	o.maxSkip = 100 // Blocks can be long, limit max skipping.
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm", 17, 14, 8, 7, 8<<20)
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm2MB", 17, 14, 7, 7, 2<<20)
	o.outputMargin = 11
	o.inputMargin = 8
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm512K", 16, 13, 7, 7, 512<<10)
	o.maxSkip = 0
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm64K", 15, 12, 6, 6, 64<<10)
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm16K", 14, 11, 6, 6, 16<<10)
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm4K", 12, 10, 5, 6, 4<<10)
	o.genEncodeBetterBlockAsm("encodeBetterBlockAsm1K", 11, 8, 4, 6, 1<<10)

	o.maxLen = 8 << 20
	o.maxOffset = 8<<20 - 1
	o.outputMargin = 8
	o.inputMargin = 0
	o.genEmitLiteral()
	o.genEmitRepeat()
	o.genEmitCopy()
	o.genEmitCopyLits2()
	o.genEmitCopyLits3()
	o.genMatchLen()
	o.cvtLZ4BlockAsm()
	o.genDecodeBlockAsm("decodeBlockAsm")

	// This has quite low impact, so we disable it.
	if false {
		o.maxLen = 8 << 20
		o.inputMargin = 16
		o.outputMargin = 20
		o.ignoreMargins = true
		o.genDecodeBlockAsm("decodeBlockAsmMargins")
		o.ignoreMargins = false
	}

	Generate()
}

const (
	tagLiteral = 0x00
	tagCopy1   = 0x01
	tagCopy2   = 0x02
	tagCopy3   = 0x03
	tagRepeat  = tagLiteral | 4

	maxOffset          = 2<<20 + 65535
	copyLitBits        = 2
	maxCopyLits        = 1<<copyLitBits - 1
	copy3LitsMaxOffset = 1<<(23-copyLitBits) - maxCopyLits
	maxCopy1Offset     = 1024

	minCopy2Offset = 64
	maxCopy2Offset = minCopy2Offset + 65535 // 2MiB
	copy2LitMaxLen = 7 + 4                  // max length
	maxCopy2Lits   = 1 << copyLitBits
	minCopy2Length = 64

	maxCopy3Lits   = 1<<copyLitBits - 1
	minCopy3Offset = 65536         // 2MiB
	maxCopy3Offset = 2<<20 + 65535 // 2MiB
	minCopy3Length = 64
)

func debugval(v Op) {
	value := reg.R15
	MOVQ(v, value)
	INT(Imm(3))
	INT(Imm(3))
}

func debugval32(v Op) {
	value := reg.R15L
	MOVL(v, value)
	INT(Imm(3))
}

var assertCounter int

// assert will insert code if debug is enabled.
// The code should jump to 'ok' is assertion is success.
func assert(fn func(ok LabelRef)) {
	assertOffset(0, fn)
}

func assertOffset(off int, fn func(ok LabelRef)) {
	if debug {
		caller := [100]uintptr{0}
		runtime.Callers(3+off, caller[:])
		frame, _ := runtime.CallersFrames(caller[:]).Next()

		ok := fmt.Sprintf("assert_check_%d_ok_srcline_%d", assertCounter, frame.Line)
		fn(LabelRef(ok))
		// Emit several since delve is imprecise.
		INT(Imm(3))
		INT(Imm(3))
		Label(ok)
		assertCounter++
	}
}

type regTable struct {
	r     reg.Register
	disp  int
	scale uint8
}

func (r regTable) Idx(idx reg.GPVirtual) Mem {
	return Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp}
}

func (r regTable) LoadIdx(idx, dst reg.GPVirtual) {
	switch r.scale {
	case 1:
		MOVBQZX(Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp}, dst.As32())
	case 2:
		MOVWLZX(Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp}, dst.As32())
	case 4:
		MOVL(Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp}, dst.As32())
	default:
		panic(r.scale)
	}
}

func (r regTable) SaveIdx(val, idx reg.GPVirtual) {
	switch r.scale {
	case 1:
		MOVB(val.As8(), Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp})
	case 2:
		MOVW(val.As16(), Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp})
	case 4:
		MOVL(val.As32(), Mem{Base: r.r, Index: idx, Scale: r.scale, Disp: r.disp})
	default:
		panic(r.scale)
	}
}
func reloadTables(param string, tables ...*regTable) {
	r := Load(Param(param), GP64())
	for _, t := range tables {
		t.r = r
	}
}

type options struct {
	bmi1          bool
	bmi2          bool
	skipOutput    bool
	avx2          bool
	maxLen        int
	maxOffset     int
	outputMargin  int // Should be at least 5.
	inputMargin   int
	maxSkip       int
	ignoreMargins bool
	fastOpts
}

type fastOpts struct {
	// Do 8 byte minimum match
	match8 bool

	// Do fused literals when emitting
	fuselits bool

	// Check for repeats
	checkRepeats bool

	// Extend matches backwards
	checkBack bool

	// Skip checking s+1 when looking for a match.
	skipOne bool

	// Increment loop by this many bytes when match fails.
	incLoop int
}

func (o options) genEncodeBlockAsm(name string, tableBits, skipLog, hashBytes, maxLen int) {
	dstTxt := "dst, "
	if o.skipOutput {
		dstTxt = ""
	}

	var offsetBytes = uint8(4)
	if maxLen <= 65536 {
		offsetBytes = 2
	}

	var tableSize = int(offsetBytes) * (1 << tableBits)
	// Memzero needs at least 128 bytes.
	if tableSize < 128 {
		panic("tableSize must be at least 128 bytes")
	}

	arrPtr := fmt.Sprintf(",tmp *[%d]byte", tableSize)
	TEXT(name, 0, "func("+dstTxt+"src []byte"+arrPtr+") int")
	Doc(name+" encodes a non-empty src to a guaranteed-large-enough dst.",
		fmt.Sprintf("Maximum input %d bytes.", maxLen),
		"It assumes that the varint-encoded length of the decompressed bytes has already been written.", "")
	Pragma("noescape")

	o.maxLen = maxLen
	o.maxOffset = maxLen - 1
	var literalMaxOverhead = maxLitOverheadFor(maxLen)

	lenSrcBasic, err := Param("src").Len().Resolve()
	if err != nil {
		panic(err)
	}
	lenSrcQ := lenSrcBasic.Addr

	var lenDstQ Mem
	if debug && !o.skipOutput {
		lenDstBasic, err := Param("dst").Len().Resolve()
		if err != nil {
			panic(err)
		}
		lenDstQ = lenDstBasic.Addr
	}

	// Bail if we can't compress to at least this.
	dstLimitPtrQ := AllocLocal(8)

	// sLimitL is when to stop looking for offset/length copies.
	sLimitL := AllocLocal(4)

	// nextEmitL keeps track of the point we have emitted to.
	nextEmitL := AllocLocal(4)

	// Repeat stores the last match offset.
	repeatL := AllocLocal(4)

	// nextSTempL keeps nextS while other functions are being called.
	nextSTempL := AllocLocal(4)

	// Load pointer to temp table
	table := regTable{r: Load(Param("tmp"), GP64()), scale: offsetBytes}

	dst := GP64()
	if !o.skipOutput {
		dstBaseBasic, err := Param("dst").Base().Resolve()
		if err != nil {
			panic(err)
		}
		dstBaseQ := dstBaseBasic.Addr
		MOVQ(dstBaseQ, dst)
	} else {
		// Zero dst address
		XORQ(dst, dst)
	}

	srcBaseBasic, err := Param("src").Base().Resolve()
	if err != nil {
		panic(err)
	}
	srcBaseQ := srcBaseBasic.Addr

	// Zero table
	{
		iReg := GP64()
		MOVQ(U32(tableSize/8/16), iReg)
		tablePtr := GP64()
		MOVQ(table.r, tablePtr)
		zeroXmm := XMM()
		PXOR(zeroXmm, zeroXmm)

		Label("zero_loop_" + name)
		for i := 0; i < 8; i++ {
			MOVOU(zeroXmm, Mem{Base: tablePtr, Disp: i * 16})
		}
		ADDQ(U8(16*8), tablePtr)
		DECQ(iReg)
		JNZ(LabelRef("zero_loop_" + name))
	}

	match8 := o.match8
	fuselits := o.fuselits
	checkRepeats := o.checkRepeats
	checkBack := o.checkBack
	skipOne := o.skipOne

	{
		// nextEmit is offset n src where the next emitLiteral should start from.
		MOVL(U32(0), nextEmitL)
		if o.inputMargin < 8 {
			panic(fmt.Sprintf("input marging must be at least 8, was %d", o.inputMargin))
		}
		tmp, tmp2, tmp3 := GP64(), GP64(), GP64()
		MOVQ(lenSrcQ, tmp)
		LEAQ(Mem{Base: tmp, Disp: -o.outputMargin}, tmp2)
		// sLimitL := len(src) - inputMargin
		LEAQ(Mem{Base: tmp, Disp: -o.inputMargin}, tmp3)

		assert(func(ok LabelRef) {
			CMPQ(tmp3, lenSrcQ)
			JB(ok)
		})

		MOVL(tmp3.As32(), sLimitL)

		// dstLimit := (len(src) - outputMargin ) - len(src)>>5
		SHRQ(U8(5), tmp)
		SUBL(tmp.As32(), tmp2.As32()) // tmp2 = tmp2 - tmp

		assert(func(ok LabelRef) {
			// if len(src) > len(src) - len(src)>>5 - outputMargin: ok
			CMPQ(lenSrcQ, tmp2)
			JAE(ok)
		})

		LEAQ(Mem{Base: dst, Index: tmp2, Scale: 1}, tmp2)
		MOVQ(tmp2, dstLimitPtrQ)
	}

	retIdx := 1
	checkDst := func(code int, lits reg.GPVirtual) {
		ok := fmt.Sprintf("dst_size_check_ok_%d", retIdx)
		retIdx++
		// Add literal count...
		if lits != nil {
			tmp := GP64()
			LEAQ(Mem{Base: dst, Index: lits, Scale: 1, Disp: literalMaxOverhead}, tmp)
			CMPQ(tmp, dstLimitPtrQ)
		} else {
			CMPQ(dst, dstLimitPtrQ)
		}
		JB(LabelRef(ok))
		ri, err := ReturnIndex(0).Resolve()
		if err != nil {
			panic(err)
		}
		MOVQ(U32(0), ri.Addr)
		if o.avx2 {
			VZEROUPPER()
		}
		RET()
		Label(ok)
	}

	assertCand := func(c reg.GPVirtual, fn func(cand reg.Register, ok LabelRef)) {
		assertOffset(0, func(ok LabelRef) {
			fn(c.As32(), ok)
		})
	}
	// s = 1
	s := GP32()
	MOVL(U32(1), s)
	// repeatL = 1
	MOVL(s, repeatL)

	src := GP64()
	Load(Param("src").Base(), src)

	// Load cv
	Label("search_loop_" + name)
	candidate := GP32()
	{
		assert(func(ok LabelRef) {
			// Check if somebody changed src
			tmp := GP64()
			MOVQ(srcBaseQ, tmp)
			CMPQ(tmp, src)
			JEQ(ok)
		})

		cv := GP64()
		nextS := GP32()
		// nextS := s + (s-nextEmit)>>6 + 4
		if o.maxSkip == 0 {
			tmp := GP64()
			MOVL(s, tmp.As32())           // tmp = s
			SUBL(nextEmitL, tmp.As32())   // tmp = s - nextEmit
			SHRL(U8(skipLog), tmp.As32()) // tmp = (s - nextEmit) >> skipLog
			LEAL(Mem{Base: s, Disp: o.incLoop, Index: tmp, Scale: 1}, nextS)
		} else {
			panic("maxskip not implemented")
		}
		// if nextS > sLimit {goto emitRemainder}
		{
			CMPL(nextS.As32(), sLimitL)
			JAE(LabelRef("emit_remainder_" + name))
		}
		MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)

		// Check if offset exceeds max
		var ccCounter int
		var minPos reg.GPVirtual
		if o.maxOffset > maxOffset {
			minPos = GP64()
			LEAL(Mem{Base: s, Disp: -maxOffset + 2}, minPos.As32())
		}
		checkCandidate := func(cand reg.GPVirtual, ifok func()) {
			if o.maxOffset <= maxOffset {
				ifok()
				return
			}
			if matchOffsetCMOV {
				// Use CMOV over JLE to avoid a jump.
				// Intel seems to favor this.
				CMPL(cand.As32(), minPos.As32())
				CMOVLLE(minPos.As32(), cand.As32())
				ifok()
			} else {
				skip := fmt.Sprintf("offset_ok_%d_%s", ccCounter, name)
				ccCounter++
				CMPL(cand.As32(), minPos.As32())
				JLE(LabelRef(skip))
				ifok()
				Label(skip)
			}
		}
		assert(func(ok LabelRef) {
			// Check if s is valid (we should have jumped above if not)
			tmp := GP64()
			MOVQ(lenSrcQ, tmp)
			CMPQ(tmp, s.As64())
			JA(ok)
		})
		// move nextS to stack.
		MOVL(nextS.As32(), nextSTempL)

		candidate2 := GP32()
		hasher := hashN(o, hashBytes, tableBits)
		{
			hash0, hash1 := GP64(), GP64()
			MOVQ(cv, hash0)
			hasher.hash(hash0)
			if !skipOne {
				if hashBytes > 7 {
					MOVQ(Mem{Base: src, Index: s, Disp: 1, Scale: 1}, hash1)
				} else {
					MOVQ(cv, hash1)
					SHRQ(U8(8), hash1)
				}
				hasher.hash(hash1)
				assert(func(ok LabelRef) {
					CMPQ(hash1, U32(tableSize))
					JB(ok)
				})
			}
			table.LoadIdx(hash0, candidate)
			assert(func(ok LabelRef) {
				CMPQ(hash0, U32(tableSize))
				JB(ok)
			})

			table.SaveIdx(s, hash0)
			if !skipOne {
				table.LoadIdx(hash1, candidate2)
				table.SaveIdx(s, hash1)
			}
		}

		// Can be moved up if registers are available.
		hash2 := GP64()
		{
			// hash2 := hash6(cv>>16, tableBits)
			// hasher = hash6(tableBits)
			if hashBytes > 6 {
				MOVQ(Mem{Base: src, Index: s, Disp: 2, Scale: 1}, hash2)
			} else {
				MOVQ(cv, hash2)
				SHRQ(U8(16), hash2)
			}
			hasher.hash(hash2)
			assert(func(ok LabelRef) {
				CMPQ(hash2, U32(tableSize))
				JB(ok)
			})
		}

		// En/disable repeat matching.
		if checkRepeats {
			// Check repeat at offset checkRep
			const checkRep = 1
			{
				// rep = s - repeat
				rep := GP32()
				MOVL(s, rep)
				SUBL(repeatL, rep) // rep = s - repeat

				// if uint32(cv>>(checkRep*8)) == load32(src, s-repeat+checkRep) {
				left, right := GP64(), GP64()
				MOVL(Mem{Base: src, Index: rep, Disp: checkRep, Scale: 1}, right.As32())
				MOVQ(cv, left)
				SHRQ(U8(checkRep*8), left)
				CMPL(left.As32(), right.As32())
				// BAIL, no repeat.
				JNE(LabelRef("no_repeat_found_" + name))
			}
			// base = s + checkRep
			base := GP32()
			LEAL(Mem{Base: s, Disp: checkRep}, base)

			// nextEmit before repeat.
			nextEmit := GP32()
			MOVL(nextEmitL, nextEmit)

			// Extend back
			if checkBack {
				i := GP32()
				MOVL(base, i)
				SUBL(repeatL, i)
				JZ(LabelRef("repeat_extend_back_end_" + name))

				Label("repeat_extend_back_loop_" + name)
				// if base <= nextemit {exit}
				CMPL(base.As32(), nextEmit)
				JBE(LabelRef("repeat_extend_back_end_" + name))
				// if src[i-1] == src[base-1]
				tmp, tmp2 := GP64(), GP64()
				MOVB(Mem{Base: src, Index: i, Scale: 1, Disp: -1}, tmp.As8())
				MOVB(Mem{Base: src, Index: base, Scale: 1, Disp: -1}, tmp2.As8())
				CMPB(tmp.As8(), tmp2.As8())
				JNE(LabelRef("repeat_extend_back_end_" + name))
				LEAL(Mem{Base: base, Disp: -1}, base)
				DECL(i)
				JNZ(LabelRef("repeat_extend_back_loop_" + name))
			}
			Label("repeat_extend_back_end_" + name)

			litLen, nextEmit := GP64(), GP64()
			MOVL(base.As32(), litLen.As32())
			MOVL(nextEmitL, nextEmit.As32())
			SUBL(nextEmit.As32(), litLen.As32())
			checkDst(0, litLen)

			// Base is now at start. Emit until base.
			// d += emitLiteral(dst[d:], src[nextEmit:base])
			litSrc := GP64()
			LEAQ(Mem{Base: src, Index: nextEmit, Scale: 1}, litSrc)
			o.emitLiteral("repeat_emit_lits_"+name, litLen, nil, dst, litSrc, LabelRef("repeat_emit_lits_end_"+name), true)
			Label("repeat_emit_lits_end_" + name)

			// Extend forward
			{
				// s += 4 + checkRep
				ADDL(U8(4+checkRep), s)

				if true {
					// candidate := s - repeat + 4 + checkRep
					MOVL(s, candidate)
					SUBL(repeatL, candidate) // candidate = s - repeat

					// srcLeft = len(src) - s
					srcLeft := GP64()
					MOVQ(lenSrcQ, srcLeft)
					SUBL(s, srcLeft.As32())
					assert(func(ok LabelRef) {
						// if srcleft < maxint32: ok
						CMPQ(srcLeft, U32(0x7fffffff))
						JB(ok)
					})
					// Forward address
					forwardStart := GP64()
					LEAQ(Mem{Base: src, Index: s, Scale: 1}, forwardStart)
					// End address
					backStart := GP64()
					LEAQ(Mem{Base: src, Index: candidate, Scale: 1}, backStart)

					length := o.matchLen("repeat_extend_"+name, forwardStart, backStart, srcLeft, nil, LabelRef("repeat_extend_forward_end_"+name))
					forwardStart, backStart, srcLeft = nil, nil, nil
					Label("repeat_extend_forward_end_" + name)
					// s+= length
					ADDL(length.As32(), s)
				}
			}
			// Emit
			{
				// length = s-base
				length := GP32()
				MOVL(s, length)
				SUBL(base.As32(), length) // length = s - base

				offsetVal := GP32()
				MOVL(repeatL, offsetVal)

				// Emit as repeat...
				o.emitRepeat("match_repeat_"+name, length, nil, dst, LabelRef("repeat_end_emit_"+name))

				Label("repeat_end_emit_" + name)
				// Store new dst and nextEmit
				MOVL(s, nextEmitL)
			}
			if false {
				// if s >= sLimit is picked up on next loop.
				CMPL(s.As32(), sLimitL)
				JAE(LabelRef("emit_remainder_" + name))
			}
			JMP(LabelRef("search_loop_" + name))
		}
		Label("no_repeat_found_" + name)
		{
			// Check candidates are ok. All must be < s and < len(src)
			assertCand(candidate, func(cand reg.Register, ok LabelRef) {
				tmp := GP64()
				MOVQ(lenSrcQ, tmp)
				CMPL(tmp.As32(), cand)
				JA(ok)
			})
			assertCand(candidate, func(cand reg.Register, ok LabelRef) {
				CMPL(s, cand)
				JA(ok)
			})
			if !skipOne {
				assertCand(candidate2, func(cand reg.Register, ok LabelRef) {
					tmp := GP64()
					MOVQ(lenSrcQ, tmp)
					CMPL(tmp.As32(), cand)
					JA(ok)
				})
				assertCand(candidate2, func(cand reg.Register, ok LabelRef) {
					CMPL(s, cand)
					// Candidate2 is at s+1, so s is ok.
					JAE(ok)
				})
			}

			checkCandidate(candidate, func() {
				if match8 {
					CMPQ(Mem{Base: src, Index: candidate, Scale: 1}, cv.As64()) // <<-- Hot
				} else {
					CMPL(Mem{Base: src, Index: candidate, Scale: 1}, cv.As32()) // <<-- Hot
				}
				JEQ(LabelRef("candidate_match_" + name))
			})

			tmp := GP32()
			// cv >>= 8
			if !skipOne {
				if hashBytes > 7 {
					MOVQ(Mem{Base: src, Index: s, Disp: 1, Scale: 1}, cv)
				} else {
					SHRQ(U8(8), cv)
				}
			}

			// candidate = int(table[hash2]) - load early.
			table.LoadIdx(hash2, candidate)
			assert(func(ok LabelRef) {
				tmp := GP64()
				MOVQ(lenSrcQ, tmp)
				CMPL(tmp.As32(), candidate)
				JA(ok)
			})
			assertCand(candidate, func(cand reg.Register, ok LabelRef) {
				// We may get s and s+1
				tmp := GP32()
				LEAL(Mem{Base: s, Disp: 2}, tmp)
				CMPL(tmp, cand)
				JA(ok)
			})

			LEAL(Mem{Base: s, Disp: 2}, tmp)

			//if uint32(cv>>8) == load32(src, candidate2)
			if !skipOne {
				checkCandidate(candidate2, func() {
					if match8 {
						CMPQ(Mem{Base: src, Index: candidate2, Scale: 1}, cv.As64())
					} else {
						CMPL(Mem{Base: src, Index: candidate2, Scale: 1}, cv.As32())
					}
					JEQ(LabelRef("candidate2_match_" + name))
				})
			}

			// table[hash2] = uint32(s + 2)
			table.SaveIdx(tmp, hash2)

			// cv >>= 8 (>> 16 total)
			if hashBytes > 6 {
				MOVQ(Mem{Base: src, Index: s, Disp: 2, Scale: 1}, cv)
			} else {
				if skipOne {
					SHRQ(U8(16), cv)
				} else {
					SHRQ(U8(8), cv)
				}
			}

			// if uint32(cv>>16) == load32(src, candidate)
			checkCandidate(candidate, func() {
				if match8 {
					CMPQ(Mem{Base: src, Index: candidate, Scale: 1}, cv.As64())
				} else {
					CMPL(Mem{Base: src, Index: candidate, Scale: 1}, cv.As32())
				}
				JEQ(LabelRef("candidate3_match_" + name))
			})

			// No match found, next loop
			// s = nextS
			MOVL(nextSTempL, s)
			JMP(LabelRef("search_loop_" + name))

			// Matches candidate at s + 2 (3rd check)
			Label("candidate3_match_" + name)
			ADDL(U8(2), s)
			JMP(LabelRef("candidate_match_" + name))

			// Match at s + 1 (we calculated the hash, lets store it)
			Label("candidate2_match_" + name)
			// table[hash2] = uint32(s + 2)
			table.SaveIdx(tmp, hash2)
			// s++
			INCL(s)
			MOVL(candidate2, candidate)
		}
	}

	Label("candidate_match_" + name)
	// We have a match at 's' with src offset in "candidate" that matches at least 4 bytes.
	// Extend backwards
	if checkBack {
		ne := GP32()
		MOVL(nextEmitL, ne)
		TESTL(candidate, candidate)
		JZ(LabelRef("match_extend_back_end_" + name))

		// candidate is tested when decremented, so we loop back here.
		Label("match_extend_back_loop_" + name)
		// if s <= nextEmit {exit}
		CMPL(s, ne)
		JBE(LabelRef("match_extend_back_end_" + name))
		// if src[candidate-1] == src[s-1]
		tmp, tmp2 := GP64(), GP64()
		MOVB(Mem{Base: src, Index: candidate, Scale: 1, Disp: -1}, tmp.As8())
		MOVB(Mem{Base: src, Index: s, Scale: 1, Disp: -1}, tmp2.As8())
		CMPB(tmp.As8(), tmp2.As8())
		JNE(LabelRef("match_extend_back_end_" + name))
		LEAL(Mem{Base: s, Disp: -1}, s)
		DECL(candidate)
		JZ(LabelRef("match_extend_back_end_" + name))
		JMP(LabelRef("match_extend_back_loop_" + name))
	}
	Label("match_extend_back_end_" + name)

	// Bail if we exceed the maximum size.
	checkDst(0, nil)
	Label("match_dst_size_check_" + name)

	cv := GP64()
	//Label("match_nolit_loop_" + name)
	base := GP32()
	MOVL(s, base.As32())
	// Update repeat
	{
		// repeat = base - candidate
		repeatVal := GP64().As32()
		MOVL(s, repeatVal)
		SUBL(candidate, repeatVal)
		MOVL(repeatVal, repeatL)
	}
	// s+=4, candidate+=4
	if match8 {
		ADDL(U8(8), s)
		ADDL(U8(8), candidate)
	} else {
		ADDL(U8(4), s)
		ADDL(U8(4), candidate)
	}

	// Extend the 4/8-byte match as long as possible and emit copy.
	{
		assert(func(ok LabelRef) {
			// s must be > candidate cannot be equal.
			CMPL(s, candidate)
			JA(ok)
		})
		// srcLeft = len(src) - s
		srcLeft := GP64()
		MOVQ(lenSrcQ, srcLeft)
		SUBL(s, srcLeft.As32())
		assert(func(ok LabelRef) {
			// srcleft should never underflow.
			JNC(ok)
		})

		a, b := GP64(), GP64()
		LEAQ(Mem{Base: src, Index: s, Scale: 1}, a)
		LEAQ(Mem{Base: src, Index: candidate, Scale: 1}, b)
		length := o.matchLen("match_nolit_"+name,
			a, b,
			srcLeft,
			nil,
			LabelRef("match_nolit_end_"+name),
		)
		Label("match_nolit_end_" + name)
		assert(func(ok LabelRef) {
			// Should never exceed max block size...
			CMPL(length.As32(), U32(8<<20))
			JB(ok)
		})
		a, b, srcLeft = nil, nil, nil

		// s += length (length is destroyed, use it now)
		ADDL(length.As32(), s)
		if match8 {
			ADDL(U8(8), length.As32()) // length += 8
		} else {
			ADDL(U8(4), length.As32()) // length += 4
		}

		// Load offset from repeat value.
		offset := GP64()
		MOVL(repeatL, offset.As32())
		// Emit lits
		{
			litLen, nextEmit := GP64(), GP64()
			MOVL(nextEmitL, nextEmit.As32())
			MOVL(base, litLen.As32())
			MOVL(s, nextEmitL) // nextEmit = s
			SUBL(nextEmit.As32(), litLen.As32())
			JZ(LabelRef("match_nolits_copy_" + name))
			litSrc := GP64()
			LEAQ(Mem{Base: src, Index: nextEmit, Scale: 1}, litSrc)
			if fuselits {
				CMPL(litLen.As32(), U8(3))
				JA(LabelRef("match_emit_lits_copy_" + name))
				CMPL(offset.As32(), U8(64))
				JB(LabelRef("match_emit_lits_copy_" + name))

				saveLits := GP64()
				// Save literals.
				// We can always read 4 bytes, since we have a match afterwards.
				if !o.skipOutput {
					MOVL(Mem{Base: litSrc}, saveLits.As32())
				}
				if o.maxOffset > maxCopy2Offset {
					CMPL(offset.As32(), U32(maxCopy2Offset))
					JBE(LabelRef("match_emit_copy2lits_" + name))
					o.emitCopy3("match_emit_lits_"+name, length, offset, nil, dst, litLen, LabelRef("match_emit_copy_lits"+name))
					Label("match_emit_copy_lits" + name)
					if !o.skipOutput {
						MOVL(saveLits.As32(), Mem{Base: dst})
					}
					ADDQ(litLen.As64(), dst) // dst += litLen
					JMP(LabelRef("match_nolit_emitcopy_end_" + name))
				}

				Label("match_emit_copy2lits_" + name)
				remain := o.emitCopy2WithLits("match_emit_lits_copy2_"+name, length, offset, litLen, nil, dst)
				// After copy2+lits, emit lits and repeat if needed.
				Label("match_emit_copy2_lits" + name)
				if !o.skipOutput {
					MOVL(saveLits.As32(), Mem{Base: dst})
				}
				ADDQ(litLen.As64(), dst) // dst += litLen
				TESTL(remain.As32(), remain.As32())
				JZ(LabelRef("match_nolit_emitcopy_end_" + name))
				o.emitRepeat("match_emit_repeat_copy2_"+name, remain, nil, dst, LabelRef("match_nolit_emitcopy_end_"+name))
				// Jumps to copy end...
			}
			Label("match_emit_lits_copy_" + name)
			checkDst(0, litLen)
			o.emitLiteral("match_emit_"+name, litLen, nil, dst, litSrc, LabelRef("match_nolits_copy_"+name), true)
		}

		PCALIGN(16)
		Label("match_nolits_copy_" + name)
		o.emitCopy("match_nolit_"+name, length, offset, nil, dst, LabelRef("match_nolit_emitcopy_end_"+name))
		Label("match_nolit_emitcopy_end_" + name)

		// if s >= sLimit { end }
		{
			CMPL(s.As32(), sLimitL)
			JAE(LabelRef("emit_remainder_" + name))
		}
		// Start load s-2 as early as possible...
		MOVQ(Mem{Base: src, Index: s, Scale: 1, Disp: -2}, cv)
		// Bail if we exceed the maximum size.
		{
			CMPQ(dst, dstLimitPtrQ)
			JB(LabelRef("match_nolit_dst_ok_" + name))
			ri, err := ReturnIndex(0).Resolve()
			if err != nil {
				panic(err)
			}
			MOVQ(U32(0), ri.Addr)
			if o.avx2 {
				VZEROUPPER()
			}
			RET()
			Label("match_nolit_dst_ok_" + name)
		}

		// cv must be set to value at s-2 before arriving here
		// Check for an immediate match, otherwise start search at s+1
		// Index s-2
		hasher := hashN(o, hashBytes, tableBits)
		hash0, hash1 := GP64(), GP64()
		MOVQ(cv, hash0) // src[s-2]
		if hashBytes > 6 {
			MOVQ(Mem{Base: src, Index: s, Disp: 0, Scale: 1}, cv)
		} else {
			SHRQ(U8(16), cv)
		}
		MOVQ(cv, hash1) // src[s]
		hasher.hash(hash0)
		hasher.hash(hash1)

		sm2 := GP32() // s - 2
		LEAL(Mem{Base: s, Disp: -2}, sm2)
		assert(func(ok LabelRef) {
			CMPQ(hash0, U32(tableSize))
			JB(ok)
		})
		assert(func(ok LabelRef) {
			CMPQ(hash1, U32(tableSize))
			JB(ok)
		})

		table.LoadIdx(hash1, candidate)
		table.SaveIdx(sm2, hash0)
		table.SaveIdx(s, hash1)
		MOVL(s, base.As32())
		INCL(s) // Note s = s + 1 for a while

		if o.maxOffset > maxOffset {
			// Check if offset exceeds max
			minPos := GP64()
			LEAL(Mem{Base: base, Disp: -maxOffset}, minPos.As32())
			CMPL(candidate.As32(), minPos.As32())
			JA(LabelRef("match_nolit_len_ok" + name))
			JMP(LabelRef("search_loop_" + name))
			Label("match_nolit_len_ok" + name)
		}
		if match8 {
			CMPQ(Mem{Base: src, Index: candidate, Scale: 1}, cv.As64())
		} else {
			CMPL(Mem{Base: src, Index: candidate, Scale: 1}, cv.As32())
		}
		JNE(LabelRef("search_loop_" + name)) // << -- Hot
		// Prepare for emit
		// Update repeat
		{
			// repeat = base - candidate
			repeatVal := GP64().As32()
			MOVL(base.As32(), repeatVal)
			SUBL(candidate, repeatVal)
			MOVL(repeatVal, repeatL)
		}
		// s+=4, candidate+=4
		checkDst(0, nil)
		if match8 {
			ADDL(U8(7), s)
			ADDL(U8(8), candidate)
		} else {
			ADDL(U8(3), s)
			ADDL(U8(4), candidate)
		}
		{
			// Extend the 4-byte match as long as possible and emit copy.
			assertCand(candidate, func(cand reg.Register, ok LabelRef) {
				// s must be > candidate cannot be equal.
				CMPL(s, cand)
				JA(ok)
			})
			// srcLeft = len(src) - s
			srcLeft := GP64()
			MOVQ(lenSrcQ, srcLeft)
			SUBL(s, srcLeft.As32())
			assert(func(ok LabelRef) {
				// srcleft should never underflow.
				JNC(ok)
			})

			a, b := GP64(), GP64()
			LEAQ(Mem{Base: src, Index: s, Scale: 1}, a)
			LEAQ(Mem{Base: src, Index: candidate, Scale: 1}, b)
			length := o.matchLen("match_nolit2_"+name,
				a, b,
				srcLeft,
				length,
				LabelRef("match_nolit2_end_"+name),
			)
			Label("match_nolit2_end_" + name)
			assert(func(ok LabelRef) {
				// Should never exceed max block size...
				CMPL(length.As32(), U32(8<<20))
				JB(ok)
			})

			// s += length (length is destroyed, use it now)
			ADDL(length.As32(), s)
			if match8 {
				ADDL(U8(8), length.As32()) // length += 4
			} else {
				ADDL(U8(4), length.As32()) // length += 4
			}
			MOVL(s, nextEmitL) // nextEmit = s
		}
		// Load offset from repeat value.
		MOVL(repeatL, offset.As32())
		JMP(LabelRef("match_nolits_copy_" + name))
	}

	Label("emit_remainder_" + name)
	// Bail if we exceed the maximum size.
	// if d+len(src)-nextEmitL > dstLimitPtrQ {	return 0
	{
		// remain = len(src) - nextEmit
		remain := GP64()
		nextEmit := GP64()
		MOVQ(lenSrcQ, remain)
		MOVL(nextEmitL, nextEmit.As32())
		SUBL(nextEmit.As32(), remain.As32())
		JZ(LabelRef("emit_remainder_end_" + name))
		litSrc := GP64()
		LEAQ(Mem{Base: src, Index: nextEmit, Scale: 1}, litSrc)

		checkDst(0, remain)

		// Emit final literals.
		// Since we may be at the end of source,
		// we cannot have input margin.
		x := o.inputMargin
		o.inputMargin = 0
		o.emitLiteral("emit_remainder_"+name, remain, nil, dst, litSrc, LabelRef("emit_remainder_end_"+name), true)
		Label("emit_remainder_end_" + name)
		o.inputMargin = x
		// Assert size is < limit
		assert(func(ok LabelRef) {
			// if dstBaseQ <  dstLimitPtrQ: ok
			CMPQ(dst, dstLimitPtrQ)
			JB(ok)
		})
	}

	// length := start - base (ptr arithmetic)
	length := GP64()
	if !o.skipOutput {
		base := Load(Param("dst").Base(), GP64())
		MOVQ(dst, length)
		SUBQ(base, length)
	} else {
		length = dst
	}

	// Assert size is < len(src)
	assert(func(ok LabelRef) {
		// if len(src) >= length: ok
		CMPQ(lenSrcQ, length)
		JAE(ok)
	})
	// Assert size is < len(dst)
	if !o.skipOutput {
		assert(func(ok LabelRef) {
			// if len(dst) >= length: ok
			CMPQ(lenDstQ, length)
			JAE(ok)
		})
	}
	if o.avx2 {
		VZEROUPPER()
	}
	Store(length, ReturnIndex(0))
	RET()
}

func maxLitOverheadFor(n int) int {
	switch {
	case n == 0:
		return 0
	case n < 30:
		return 1
	case n < 1<<8:
		return 2
	case n < 1<<16:
		return 3
	}
	return 4
}

func (o options) genEncodeBetterBlockAsm(name string, lTableBits, sTableBits, skipLog, lHashBytes, maxLen int) {
	var offsetBytes = uint8(4)
	if maxLen <= 65536 {
		offsetBytes = 2
	}
	var lTableSize = int(offsetBytes) * (1 << lTableBits)
	var sTableSize = int(offsetBytes) * (1 << sTableBits)
	tableSize := lTableSize + sTableSize

	// Memzero needs at least 128 bytes.
	if tableSize < 128 {
		panic("tableSize must be at least 128 bytes")
	}
	arrPtr := fmt.Sprintf(", tmp *[%d]byte", tableSize)

	TEXT(name, 0, "func(dst, src []byte"+arrPtr+") int")
	Doc(name+" encodes a non-empty src to a guaranteed-large-enough dst.",
		fmt.Sprintf("Maximum input %d bytes.", maxLen),
		"It assumes that the varint-encoded length of the decompressed bytes has already been written.", "")
	Pragma("noescape")

	if lHashBytes > 7 || lHashBytes <= 4 {
		panic("lHashBytes must be <= 7 and >4")
	}
	var literalMaxOverhead = maxLitOverheadFor(maxLen)

	const sHashBytes = 4
	o.maxLen = maxLen
	o.maxOffset = maxLen - 1

	lenSrcBasic, err := Param("src").Len().Resolve()
	if err != nil {
		panic(err)
	}
	lenSrcQ := lenSrcBasic.Addr

	lenDstBasic, err := Param("dst").Len().Resolve()
	if err != nil {
		panic(err)
	}
	lenDstQ := lenDstBasic.Addr

	// Bail if we can't compress to at least this.
	dstLimitPtrQ := AllocLocal(8)

	// sLimitL is when to stop looking for offset/length copies.
	sLimitL := AllocLocal(4)

	// nextEmitL keeps track of the point we have emitted to.
	nextEmitL := AllocLocal(4)

	// Repeat stores the last match offset.
	repeatL := AllocLocal(4)

	// nextSTempL keeps nextS while other functions are being called.
	nextSTempL := AllocLocal(4)

	// lTab must be before sTab.
	table := Load(Param("tmp"), GP64())
	lTab := regTable{r: table, scale: offsetBytes}
	sTab := regTable{r: table, disp: lTableSize, scale: offsetBytes}

	dst := GP64()
	{
		dstBaseBasic, err := Param("dst").Base().Resolve()
		if err != nil {
			panic(err)
		}
		dstBaseQ := dstBaseBasic.Addr
		MOVQ(dstBaseQ, dst)
	}

	srcBaseBasic, err := Param("src").Base().Resolve()
	if err != nil {
		panic(err)
	}
	srcBaseQ := srcBaseBasic.Addr

	// Zero table
	{
		iReg := GP64()
		MOVQ(U32((sTableSize+lTableSize)/8/16), iReg)
		tablePtr := GP64()
		MOVQ(table, tablePtr)
		zeroXmm := XMM()
		PXOR(zeroXmm, zeroXmm)

		Label("zero_loop_" + name)
		for i := 0; i < 8; i++ {
			MOVOU(zeroXmm, Mem{Base: tablePtr, Disp: i * 16})
		}
		ADDQ(U8(16*8), tablePtr)
		DECQ(iReg)
		JNZ(LabelRef("zero_loop_" + name))
	}

	{
		// nextEmit is offset n src where the next emitLiteral should start from.
		MOVL(U32(0), nextEmitL)
		if o.inputMargin < 8 {
			panic(fmt.Sprintf("input margin must be at least 8, was %d", o.inputMargin))
		}

		tmp, tmp2, tmp3 := GP64(), GP64(), GP64()
		MOVQ(lenSrcQ, tmp)
		LEAQ(Mem{Base: tmp, Disp: -o.outputMargin}, tmp2)
		// sLimitL := len(src) - inputMargin
		LEAQ(Mem{Base: tmp, Disp: -o.inputMargin}, tmp3)

		assert(func(ok LabelRef) {
			CMPQ(tmp3, lenSrcQ)
			JB(ok)
		})

		MOVL(tmp3.As32(), sLimitL)

		// dstLimit := (len(src) - 5 ) - len(src)>>5
		SHRQ(U8(5), tmp)
		SUBL(tmp.As32(), tmp2.As32()) // tmp2 = tmp2 - tmp

		assert(func(ok LabelRef) {
			// if len(src) > len(src) - len(src)>>5 - 5: ok
			CMPQ(lenSrcQ, tmp2)
			JAE(ok)
		})

		LEAQ(Mem{Base: dst, Index: tmp2, Scale: 1}, tmp2)
		MOVQ(tmp2, dstLimitPtrQ)
	}

	// s = 1
	s := GP32()
	MOVL(U32(1), s)
	// repeatL = 1
	MOVL(s, repeatL)

	src := GP64()
	Load(Param("src").Base(), src)

	// Load cv
	PCALIGN(16)
	Label("search_loop_" + name)
	reloadTables("tmp", &sTab, &lTab)
	candidate := GP32()
	{
		assert(func(ok LabelRef) {
			// Check if somebody changed src
			tmp := GP64()
			MOVQ(srcBaseQ, tmp)
			CMPQ(tmp, src)
			JEQ(ok)
		})

		cv := GP64()
		nextS := GP32()
		// nextS := s + (s-nextEmit)>>skipLog + 1
		if o.maxSkip == 0 {
			tmp := GP64()
			MOVL(s, tmp.As32())           // tmp = s
			SUBL(nextEmitL, tmp.As32())   // tmp = s - nextEmit
			SHRL(U8(skipLog), tmp.As32()) // tmp = (s - nextEmit) >> skipLog
			LEAL(Mem{Base: s, Disp: 1, Index: tmp, Scale: 1}, nextS)
		} else {
			/*
				nextS = (s-nextEmit)>>7 + 1
				if nextS > maxSkip {
					nextS = s + maxSkip
				} else {
					nextS += s
				}
			*/
			tmp := GP64()
			MOVL(s, tmp.As32())           // tmp = s
			SUBL(nextEmitL, tmp.As32())   // tmp = s - nextEmit
			SHRL(U8(skipLog), tmp.As32()) // tmp = (s - nextEmit) >> skipLog
			CMPL(tmp.As32(), U8(o.maxSkip-1))
			JBE(LabelRef("check_maxskip_ok_" + name))
			LEAL(Mem{Base: s, Disp: o.maxSkip, Scale: 1}, nextS)
			JMP(LabelRef("check_maxskip_cont_" + name))

			Label("check_maxskip_ok_" + name)
			LEAL(Mem{Base: s, Disp: 1, Index: tmp, Scale: 1}, nextS)
			Label("check_maxskip_cont_" + name)
		}
		// if nextS > sLimit {goto emitRemainder}
		{
			CMPL(nextS.As32(), sLimitL)
			JAE(LabelRef("emit_remainder_" + name))
		}
		MOVQ(Mem{Base: src, Index: s, Scale: 1}, cv)
		assert(func(ok LabelRef) {
			// Check if s is valid (we should have jumped above if not)
			tmp := GP64()
			MOVQ(lenSrcQ, tmp)
			CMPQ(tmp, s.As64())
			JA(ok)
		})
		// move nextS to stack.
		MOVL(nextS.As32(), nextSTempL)

		candidateS := GP32()
		lHasher := hashN(o, lHashBytes, lTableBits)
		{
			sHasher := hashN(o, sHashBytes, sTableBits)
			hash0, hash1 := GP64(), GP64()
			MOVQ(cv, hash0)
			MOVQ(cv, hash1)
			lHasher.hash(hash0)
			sHasher.hash(hash1)
			lTab.LoadIdx(hash0, candidate)
			sTab.LoadIdx(hash1, candidateS)
			assert(func(ok LabelRef) {
				CMPQ(hash0, U32(lTableSize))
				JB(ok)
			})
			assert(func(ok LabelRef) {
				CMPQ(hash1, U32(sTableSize))
				JB(ok)
			})

			lTab.SaveIdx(s, hash0)
			sTab.SaveIdx(s, hash1)
		}
		// Check if offset exceeds max
		var ccCounter int
		var minPos reg.GPVirtual
		if o.maxOffset > maxOffset {
			minPos = GP64()
			LEAL(Mem{Base: s, Disp: -maxOffset + 2}, minPos.As32())
		}
		checkCandidate := func(cand reg.GPVirtual, ifok func()) {
			if o.maxOffset <= maxOffset {
				ifok()
				return
			}
			if matchOffsetCMOV {
				// Use CMOV over JLE to avoid a jump.
				// Intel seems to favor this.
				CMPL(cand.As32(), minPos.As32())
				CMOVLLE(minPos.As32(), cand.As32())
				ifok()
			} else {
				skip := fmt.Sprintf("offset_ok_%d_%s", ccCounter, name)
				ccCounter++
				CMPL(cand.As32(), minPos.As32())
				JL(LabelRef(skip))
				ifok()
				Label(skip)
			}
		}
		longVal := GP64()
		shortVal := GP64()

		// If we have at least 8 bytes match, choose that first.
		checkCandidate(candidate, func() {
			MOVQ(Mem{Base: src, Index: candidate, Scale: 1}, longVal)
			CMPQ(longVal, cv.As64())
			JEQ(LabelRef("candidate_match_" + name))
		})

		// Load short early...
		checkCandidate(candidateS, func() {
			MOVQ(Mem{Base: src, Index: candidateS, Scale: 1}, shortVal)
			CMPQ(shortVal, cv.As64())
		})

		// En/disable repeat matching.
		if true {
			// Check repeat at offset checkRep
			const checkRep = 1
			const wantRepeatBytes = 4
			const repeatMask = ((1 << (wantRepeatBytes * 8)) - 1) << (8 * checkRep)
			{
				// rep = s - repeat
				rep := GP32()
				MOVL(s, rep)
				SUBL(repeatL, rep) // rep = s - repeat

				// if uint32(cv>>(checkRep*8)) == load32(src, s-repeat+checkRep) {
				tmp := GP64()
				mask := GP64()
				MOVQ(Mem{Base: src, Index: rep, Disp: 0, Scale: 1}, tmp.As64())
				MOVQ(U64(repeatMask), mask)
				XORQ(cv.As64(), tmp.As64())
				TESTQ(mask.As64(), tmp.As64())
				// BAIL, no repeat.
				JNE(LabelRef("no_repeat_found_" + name))
			}
			// base = s + checkRep
			base := GP32()
			LEAL(Mem{Base: s, Disp: checkRep}, base)

			// nextEmit before repeat.
			nextEmit := GP32()
			MOVL(nextEmitL, nextEmit)

			// Extend back
			if true {
				i := GP32()
				MOVL(base, i)
				SUBL(repeatL, i)
				JZ(LabelRef("repeat_extend_back_end_" + name))

				Label("repeat_extend_back_loop_" + name)
				// if base <= nextemit {exit}
				CMPL(base.As32(), nextEmit)
				JBE(LabelRef("repeat_extend_back_end_" + name))
				// if src[i-1] == src[base-1]
				tmp, tmp2 := GP64(), GP64()
				MOVB(Mem{Base: src, Index: i, Scale: 1, Disp: -1}, tmp.As8())
				MOVB(Mem{Base: src, Index: base, Scale: 1, Disp: -1}, tmp2.As8())
				CMPB(tmp.As8(), tmp2.As8())
				JNE(LabelRef("repeat_extend_back_end_" + name))
				LEAL(Mem{Base: base, Disp: -1}, base)
				DECL(i)
				JNZ(LabelRef("repeat_extend_back_loop_" + name))
			}
			Label("repeat_extend_back_end_" + name)

			{
				// tmp = s-nextEmit
				tmp := GP64()
				MOVL(base.As32(), tmp.As32())
				SUBL(nextEmitL, tmp.As32())
				// tmp = &dst + s-nextEmit
				LEAQ(Mem{Base: dst, Index: tmp, Scale: 1, Disp: literalMaxOverhead}, tmp)
				CMPQ(tmp, dstLimitPtrQ)
				JB(LabelRef("repeat_dst_size_check_" + name))
				ri, err := ReturnIndex(0).Resolve()
				if err != nil {
					panic(err)
				}
				MOVQ(U32(0), ri.Addr)
				if o.avx2 {
					VZEROUPPER()
				}
				RET()
			}
			Label("repeat_dst_size_check_" + name)

			// Base is now at start. Emit until base.
			// d += emitLiteral(dst[d:], src[nextEmit:base])
			o.emitLiteralsDstP(nextEmitL, base, src, dst, "repeat_emit_"+name)

			// Extend forward
			{
				// s += 4 + checkRep
				ADDL(U8(wantRepeatBytes+checkRep), s)

				if true {
					// candidate := s - repeat + 4 + checkRep
					MOVL(s, candidate)
					SUBL(repeatL, candidate) // candidate = s - repeat

					// srcLeft = len(src) - s
					srcLeft := GP64()
					MOVQ(lenSrcQ, srcLeft)
					SUBL(s, srcLeft.As32())
					assert(func(ok LabelRef) {
						// if srcleft < maxint32: ok
						CMPQ(srcLeft, U32(0x7fffffff))
						JB(ok)
					})
					// Forward address
					forwardStart := GP64()
					LEAQ(Mem{Base: src, Index: s, Scale: 1}, forwardStart)
					// End address
					backStart := GP64()
					LEAQ(Mem{Base: src, Index: candidate, Scale: 1}, backStart)

					length := o.matchLen("repeat_extend_"+name, forwardStart, backStart, srcLeft, nil, LabelRef("repeat_extend_forward_end_"+name))
					forwardStart, backStart, srcLeft = nil, nil, nil
					Label("repeat_extend_forward_end_" + name)
					// s+= length
					ADDL(length.As32(), s)
				}
			}
			// Emit
			if true {
				// length = s-base
				length := GP32()
				MOVL(s, length)
				SUBL(base.As32(), length) // length = s - base

				offsetVal := GP32()
				MOVL(repeatL, offsetVal)

				// Emit as repeat...
				o.emitRepeat("match_repeat_"+name, length, nil, dst, LabelRef("repeat_end_emit_"+name))

				Label("repeat_end_emit_" + name)
				// Store new dst and nextEmit
				MOVL(s, nextEmitL)
			}
			// if s >= sLimit is picked up on next loop.
			if false {
				CMPL(s.As32(), sLimitL)
				JAE(LabelRef("emit_remainder_" + name))
			}
			JMP(LabelRef("search_loop_" + name))
		}
		PCALIGN(16)
		Label("no_repeat_found_" + name)
		{
			// Check candidates are ok. All must be < s and < len(src)
			assert(func(ok LabelRef) {
				tmp := GP64()
				MOVQ(lenSrcQ, tmp)
				CMPL(tmp.As32(), candidate)
				JA(ok)
			})
			assert(func(ok LabelRef) {
				CMPL(s, candidate)
				JA(ok)
			})
			assert(func(ok LabelRef) {
				tmp := GP64()
				MOVQ(lenSrcQ, tmp)
				CMPL(tmp.As32(), candidateS)
				JA(ok)
			})
			assert(func(ok LabelRef) {
				CMPL(s, candidateS)
				JA(ok)
			})

			checkCandidate(candidate, func() {
				CMPL(longVal.As32(), cv.As32())
				JEQ(LabelRef("candidate_match_" + name))
			})

			//if uint32(cv) == load32(src, candidateS)
			checkCandidate(candidateS, func() {
				CMPL(shortVal.As32(), cv.As32())
				JEQ(LabelRef("candidateS_match_" + name))
			})

			// No match found, next loop
			// s = nextS
			MOVL(nextSTempL, s)
			JMP(LabelRef("search_loop_" + name))

			// Short match at s, try a long candidate at s+1
			Label("candidateS_match_" + name)
			if true {
				hash0 := GP64()
				SHRQ(U8(8), cv)
				MOVQ(cv, hash0)
				lHasher.hash(hash0)
				lTab.LoadIdx(hash0, candidate)
				INCL(s)
				assert(func(ok LabelRef) {
					CMPQ(hash0, U32(lTableSize))
					JB(ok)
				})
				lTab.SaveIdx(s, hash0)
				checkCandidate(candidate, func() {
					CMPL(Mem{Base: src, Index: candidate, Scale: 1}, cv.As32())
					JEQ(LabelRef("candidate_match_" + name))
				})
				// No match, decrement s again and use short match at s...
				DECL(s)
			}
			MOVL(candidateS, candidate)
		}
	}

	PCALIGN(16)
	Label("candidate_match_" + name)
	// We have a match at 's' with src offset in "candidate" that matches at least 4 bytes.
	// Extend backwards
	if true {
		ne := GP32()
		MOVL(nextEmitL, ne)
		// Make sure we don't extend back out of buffer.
		TESTL(candidate, candidate)
		JZ(LabelRef("match_extend_back_end_" + name))

		// candidate is tested when decremented, so we loop back here.
		Label("match_extend_back_loop_" + name)
		// if s <= nextEmit {exit}
		CMPL(s, ne)
		JBE(LabelRef("match_extend_back_end_" + name))
		// if src[candidate-1] == src[s-1]
		tmp, tmp2 := GP64(), GP64()
		MOVB(Mem{Base: src, Index: candidate, Scale: 1, Disp: -1}, tmp.As8())
		MOVB(Mem{Base: src, Index: s, Scale: 1, Disp: -1}, tmp2.As8())
		CMPB(tmp.As8(), tmp2.As8())
		JNE(LabelRef("match_extend_back_end_" + name))
		LEAL(Mem{Base: s, Disp: -1}, s)
		DECL(candidate)
		JZ(LabelRef("match_extend_back_end_" + name))
		JMP(LabelRef("match_extend_back_loop_" + name))
	}
	Label("match_extend_back_end_" + name)

	// Bail if we exceed the maximum size.
	if true {
		// tmp = s-nextEmit
		tmp := GP64()
		MOVL(s, tmp.As32())
		SUBL(nextEmitL, tmp.As32())
		// tmp = &dst + s-nextEmit
		LEAQ(Mem{Base: dst, Index: tmp, Scale: 1, Disp: literalMaxOverhead}, tmp)
		CMPQ(tmp, dstLimitPtrQ)
		JB(LabelRef("match_dst_size_check_" + name))
		ri, err := ReturnIndex(0).Resolve()
		if err != nil {
			panic(err)
		}
		MOVQ(U32(0), ri.Addr)
		if o.avx2 {
			VZEROUPPER()
		}
		RET()
	}
	Label("match_dst_size_check_" + name)

	base := GP32()
	MOVL(s, base.As32())

	// s+=4, candidate+=4
	ADDL(U8(4), s)
	ADDL(U8(4), candidate)
	// Extend the 4-byte match as long as possible and emit copy.
	{
		assert(func(ok LabelRef) {
			// s must be > candidate cannot be equal.
			CMPL(s, candidate)
			JA(ok)
		})
		// srcLeft = len(src) - s
		srcLeft := GP64()
		MOVQ(lenSrcQ, srcLeft)
		SUBL(s, srcLeft.As32())
		assert(func(ok LabelRef) {
			// if srcleft < maxint32: ok
			CMPQ(srcLeft, U32(0x7fffffff))
			JB(ok)
		})

		a, b := GP64(), GP64()
		LEAQ(Mem{Base: src, Index: s, Scale: 1}, a)
		LEAQ(Mem{Base: src, Index: candidate, Scale: 1}, b)
		length := o.matchLen("match_nolit_"+name,
			a, b,
			srcLeft,
			nil,
			LabelRef("match_nolit_end_"+name),
		)
		Label("match_nolit_end_" + name)
		assert(func(ok LabelRef) {
			CMPL(length.As32(), U32(math.MaxInt32))
			JB(ok)
		})
		a, b, srcLeft = nil, nil, nil

		offset := GP64()
		offset32 := offset.As32()
		{
			// offset = base - candidate
			MOVL(s, offset32)
			SUBL(candidate, offset32)

			// NOT REPEAT
			{
				// Bail if the match is equal or worse to the encoding.
				if o.maxOffset > maxCopy2Offset {
					CMPL(length.As32(), U8(1))
					JA(LabelRef("match_length_ok_" + name))
					CMPL(offset32, U32(maxCopy2Offset))
					JBE(LabelRef("match_length_ok_" + name))
					// Match is equal or worse to the encoding.
					MOVL(nextSTempL, s)
					INCL(s)
					JMP(LabelRef("search_loop_" + name))
					Label("match_length_ok_" + name)
				}
				// Store updated repeat
				MOVL(offset32, repeatL)
				Comment("Check if we can combine lit+copy")
				litLen := GP64()
				nextEmit := GP64()
				MOVLQZX(nextEmitL, nextEmit.As64())

				MOVL(base, litLen.As32())
				SUBL(nextEmit.As32(), litLen.As32())
				JZ(LabelRef("match_emit_nolits_" + name)) // If == 0
				CMPL(offset32, U32(minCopy2Offset))
				JL(LabelRef("match_emit_lits_" + name)) // If offset < 64
				// Check if copy 2 or copy 3
				lits := GP32()
				if o.maxOffset > maxCopy2Offset {
					CMPL(offset32, U32(maxCopy2Offset))
					JA(LabelRef("match_emit_copy3_" + name))
				}
				CMPL(litLen.As32(), U8(4))
				JA(LabelRef("match_emit_lits_" + name)) // If > 4
				// Emit as copy2+lits
				// Read literals to store after copy.
				// We are safe to emit combined copy and literals.
				// Read literals to store after copy.
				MOVL(Mem{Base: src, Index: nextEmit, Scale: 1}, lits)
				ADDL(length.As32(), s)     // s += length (length is destroyed, use it now)
				ADDL(U8(4), length.As32()) // length += 4
				MOVL(s, nextEmitL)         // nextEmit = s
				remain := o.emitCopy2WithLits("match_emit_lits_copy2_"+name, length, offset, litLen, nil, dst)
				// After copy2+lits, emit lits and repeat if needed.
				Label("match_emit_copy2_lits" + name)
				MOVL(lits.As32(), Mem{Base: dst})
				ADDQ(litLen.As64(), dst) // dst += litLen
				TESTL(remain.As32(), remain.As32())
				JZ(LabelRef("match_nolit_emitcopy_end_" + name))
				o.emitRepeat("match_emit_repeat_copy2_"+name, remain, nil, dst, LabelRef("match_nolit_emitcopy_end_"+name))

				if o.maxOffset > maxCopy2Offset {
					Label("match_emit_copy3_" + name)
					CMPL(litLen.As32(), U8(3))
					JA(LabelRef("match_emit_lits_" + name)) // If > 3

					//nextEmit = GP64()
					MOVLQZX(nextEmitL, nextEmit.As64())

					// We are safe to emit combined copy and literals.
					// Read literals to store after copy.
					MOVL(Mem{Base: src, Index: nextEmit, Scale: 1}, lits)

					ADDL(length.As32(), s)     // s += length (length is destroyed, use it now)
					ADDL(U8(4), length.As32()) // length += 4
					MOVL(s, nextEmitL)         // nextEmit = s
					o.emitCopy3("match_emit_lits_"+name, length, offset, nil, dst, litLen, LabelRef("match_emit_copy_lits"+name))
					Label("match_emit_copy_lits" + name)
					MOVL(lits.As32(), Mem{Base: dst})
					ADDQ(litLen.As64(), dst) // dst += litLen
					JMP(LabelRef("match_nolit_emitcopy_end_" + name))
				}

				// Emit separate lits + copy....
				Label("match_emit_lits_" + name)
				{
					LEAQ(Mem{Base: src, Index: nextEmit, Scale: 1}, nextEmit)
					o.emitLiteral("match_emit_"+name, litLen, nil, dst, nextEmit, LabelRef("match_emit_nolits_"+name), true)
				}

				Label("match_emit_nolits_" + name)
				ADDL(length.As32(), s)     // s += length (length is destroyed, use it now)
				ADDL(U8(4), length.As32()) // length += 4
				MOVL(s, nextEmitL)         // nextEmit = s
				o.emitCopy("match_nolit_"+name, length, offset, nil, dst, LabelRef("match_nolit_emitcopy_end_"+name))

				// Jumps at end
			}
			// REPEAT
			{
				Label("match_is_repeat_" + name)
				// Emit....
				o.emitLiteralsDstP(nextEmitL, base, src, dst, "match_emit_repeat_"+name)
				// s += length (length is destroyed, use it now)
				ADDL(length.As32(), s)

				// length += 4
				ADDL(U8(4), length.As32())
				MOVL(s, nextEmitL) // nextEmit = s
				o.emitRepeat("match_nolit_repeat_"+name, length, nil, dst, LabelRef("match_nolit_emitcopy_end_"+name))
			}
		}
		Label("match_nolit_emitcopy_end_" + name)

		// if s >= sLimit { end }
		{
			CMPL(s.As32(), sLimitL)
			JAE(LabelRef("emit_remainder_" + name))
		}

		// Bail if we exceed the maximum size.
		{
			CMPQ(dst, dstLimitPtrQ)
			JB(LabelRef("match_nolit_dst_ok_" + name))
			ri, err := ReturnIndex(0).Resolve()
			if err != nil {
				panic(err)
			}
			MOVQ(U32(0), ri.Addr)
			if o.avx2 {
				VZEROUPPER()
			}
			RET()
		}
	}
	Label("match_nolit_dst_ok_" + name)
	reloadTables("tmp", &sTab, &lTab)
	if true {
		lHasher := hashN(o, lHashBytes, lTableBits)
		sHasher := hashN(o, sHashBytes, sTableBits)

		index0, index1 := GP64(), GP64()
		// index0 := base + 1
		LEAQ(Mem{Base: base, Disp: 1}, index0)
		// index1 := s - 2
		LEAQ(Mem{Base: s, Disp: -2}, index1)
		hash0l, hash0s, hash1l, hash1s := GP64(), GP64(), GP64(), GP64()
		MOVQ(Mem{Base: src, Index: index0, Scale: 1, Disp: 0}, hash0l)
		MOVQ(Mem{Base: src, Index: index0, Scale: 1, Disp: 1}, hash0s)
		MOVQ(Mem{Base: src, Index: index1, Scale: 1, Disp: 0}, hash1l)
		MOVQ(Mem{Base: src, Index: index1, Scale: 1, Disp: 1}, hash1s)

		lHasher.hash(hash0l)
		sHasher.hash(hash0s)
		lHasher.hash(hash1l)
		sHasher.hash(hash1s)

		plusone0, plusone1 := GP64(), GP64()
		LEAQ(Mem{Base: index0, Disp: 1}, plusone0)
		LEAQ(Mem{Base: index1, Disp: 1}, plusone1)
		lTab.SaveIdx(index0, hash0l)
		lTab.SaveIdx(index1, hash1l)

		// index2 := (index0 + index1 + 1) >> 1
		index2 := GP64()
		LEAQ(Mem{Base: index1, Disp: 1, Index: index0, Scale: 1}, index2)
		SHRQ(U8(1), index2)

		ADDQ(U8(1), index0)
		SUBQ(U8(1), index1)
		sTab.SaveIdx(plusone0, hash0s)
		sTab.SaveIdx(plusone1, hash1s)

		Label("index_loop_" + name)
		// for index2 < index1
		CMPQ(index2, index1)
		JAE(LabelRef("search_loop_" + name))
		hash0l, hash1l = GP64(), GP64()
		MOVQ(Mem{Base: src, Index: index0, Scale: 1, Disp: 0}, hash0l)
		MOVQ(Mem{Base: src, Index: index2, Scale: 1, Disp: 0}, hash1l)
		lHasher.hash(hash0l)
		lHasher.hash(hash1l)
		lTab.SaveIdx(index0, hash0l)
		lTab.SaveIdx(index1, hash1l)

		ADDQ(U8(2), index0)
		ADDQ(U8(2), index2)
		JMP(LabelRef("index_loop_" + name))
	}

	Label("emit_remainder_" + name)
	// Bail if we exceed the maximum size.
	// if d+len(src)-nextEmitL > dstLimitPtrQ {	return 0
	{
		// remain = len(src) - nextEmit
		remain := GP64()
		MOVQ(lenSrcQ, remain)
		SUBL(nextEmitL, remain.As32())

		dstExpect := GP64()
		// dst := dst + (len(src)-nextEmitL)

		LEAQ(Mem{Base: dst, Index: remain, Scale: 1, Disp: literalMaxOverhead}, dstExpect)
		CMPQ(dstExpect, dstLimitPtrQ)
		JB(LabelRef("emit_remainder_ok_" + name))
		ri, err := ReturnIndex(0).Resolve()
		if err != nil {
			panic(err)
		}
		MOVQ(U32(0), ri.Addr)
		if o.avx2 {
			VZEROUPPER()
		}
		RET()
		Label("emit_remainder_ok_" + name)
	}
	// emitLiteral(dst[d:], src[nextEmitL:])
	emitEnd := GP64()
	MOVQ(lenSrcQ, emitEnd)

	// Emit final literals.
	// Since we may be at the end of source,
	// we cannot have output margin.
	x := o.outputMargin
	o.outputMargin = 0
	o.emitLiteralsDstP(nextEmitL, emitEnd, src, dst, "emit_remainder_"+name)
	o.outputMargin = x

	// Assert size is < limit
	assert(func(ok LabelRef) {
		// if dstBaseQ <  dstLimitPtrQ: ok
		CMPQ(dst, dstLimitPtrQ)
		JB(ok)
	})

	// length := start - base (ptr arithmetic)
	length := GP64()
	dstBase := Load(Param("dst").Base(), GP64())
	MOVQ(dst, length)
	SUBQ(dstBase, length)

	// Assert size is < len(src)
	assert(func(ok LabelRef) {
		// if len(src) >= length: ok
		CMPQ(lenSrcQ, length)
		JAE(ok)
	})
	// Assert size is < len(dst)
	assert(func(ok LabelRef) {
		// if len(dst) >= length: ok
		CMPQ(lenDstQ, length)
		JAE(ok)
	})
	Store(length, ReturnIndex(0))
	if o.avx2 {
		VZEROUPPER()
	}
	RET()
}

// emitLiterals emits literals from nextEmit to base, updates nextEmit, dstBase.
// Checks if base == nextemit.
// src & base are untouched.
func (o options) emitLiteralsDstP(nextEmitL Mem, base reg.GPVirtual, src, dst reg.GPVirtual, name string) {
	Comment("emitLiteralsDstP")
	nextEmit, litLen, litBase := GP32(), GP32(), GP64()
	MOVL(nextEmitL, nextEmit)
	CMPL(nextEmit, base.As32())
	JEQ(LabelRef("emit_literal_done_" + name))
	MOVL(base.As32(), litLen.As32())

	// Base is now next emit.
	MOVL(base.As32(), nextEmitL)

	// litBase = src[nextEmitL:]
	LEAQ(Mem{Base: src, Index: nextEmit, Scale: 1}, litBase)
	SUBL(nextEmit, litLen.As32()) // litlen = base - nextEmit

	// Load (and store when we return)
	o.emitLiteral(name, litLen, nil, dst, litBase, LabelRef("emit_literal_done_"+name), true)
	Label("emit_literal_done_" + name)
}

type hashGen struct {
	bytes     int
	tablebits int
	mulreg    reg.GPVirtual
	clear     reg.GPVirtual
	o         options
}

// hashN uses multiply to get a 'output' hash on the hash of the lowest 'bytes' bytes in value.
func hashN(o options, hashBytes, tablebits int) hashGen {
	h := hashGen{
		bytes:     hashBytes,
		tablebits: tablebits,
		mulreg:    GP64(),
		o:         o,
	}
	if o.bmi2 {
		if hashBytes < 8 {
			h.clear = GP64()
			MOVQ(U8(hashBytes*8), h.clear)
		}
		MOVQ(U8(tablebits), h.mulreg)
		return h
	}
	primebytes := uint64(0)
	switch hashBytes {
	case 3:
		primebytes = 506832829
	case 4:
		primebytes = 2654435761
	case 5:
		primebytes = 889523592379
	case 6:
		primebytes = 227718039650203
	case 7:
		primebytes = 58295818150454627
	case 8:
		primebytes = 0xcf1bbcdcb7a56463
	default:
		panic("invalid hash length")
	}
	MOVQ(Imm(primebytes), h.mulreg)
	return h
}

// hash uses multiply to get hash of the value.
func (h hashGen) hash(val reg.GPVirtual) {
	if h.o.bmi2 {
		if h.bytes < 8 {
			BZHIQ(val, h.clear, val)
		}
		CRC32Q(val, val)
		BZHIQ(val, h.mulreg, val)
	}
	// Move value to top of register.
	if h.bytes < 8 {
		SHLQ(U8(64-8*h.bytes), val)
	}
	//  329 AMD64               :IMUL r64, r64                         L:   0.86ns=  3.0c  T:   0.29ns=  1.00c
	// 2020 BMI2                :MULX r64, r64, r64                    L:   1.14ns=  4.0c  T:   0.29ns=  1.00c
	IMULQ(h.mulreg, val)
	// Move value to bottom
	// 2032 BMI2                :SHRX r64, r64, r64                    L:   0.29ns=  1.0c  T:   0.12ns=  0.42c
	//  236 AMD64               :SHR r64, imm8                         L:   0.29ns=  1.0c  T:   0.13ns=  0.46c
	SHRQ(U8(64-h.tablebits), val)
}

func (o options) genEmitLiteral() {
	TEXT("emitLiteral", NOSPLIT, "func(dst, lit []byte) int")
	Doc("emitLiteral writes a literal chunk and returns the number of bytes written.", "",
		"It assumes that:",
		fmt.Sprintf("  dst is long enough to hold the encoded bytes with margin of %d bytes", o.outputMargin),
		"  0 <= len(lit) && len(lit) <= math.MaxUint32", "")
	Pragma("noescape")

	dstBase, litBase, litLen, retval := GP64(), GP64(), GP64(), GP64()
	Load(Param("lit").Len(), litLen)
	Load(Param("dst").Base(), dstBase)
	Load(Param("lit").Base(), litBase)
	TESTQ(litLen, litLen)
	JZ(LabelRef("emit_literal_end_standalone_skip"))
	o.emitLiteral("standalone", litLen, retval, dstBase, litBase, "emit_literal_end_standalone", false)

	Label("emit_literal_end_standalone_skip")
	XORQ(retval, retval)

	Label("emit_literal_end_standalone")
	Store(retval, ReturnIndex(0))
	RET()

}

// emitLiteral can be used for inlining an emitLiteral call.
// litLen must be > 0.
// stack must have at least 32 bytes.
// retval will contain emitted bytes, but can be nil if this is not interesting.
// dstBase and litBase are updated.
// Uses 2 GP registers.
// If updateDst is true dstBase will have the updated end pointer and an additional register will be used.
func (o options) emitLiteral(name string, litLen, retval, dstBase, litBase reg.GPVirtual, end LabelRef, updateDst bool) {
	Comment("emitLiteral")
	n := GP32()
	n16 := GP32()

	// litLen must be > 0
	assert(func(ok LabelRef) {
		TESTL(litLen.As32(), litLen.As32())
		JNZ(ok)
	})

	assert(func(ok LabelRef) {
		CMPL(litLen.As32(), U32(8<<20))
		JBE(ok)
	})

	// We always add litLen bytes
	if retval != nil {
		MOVL(litLen.As32(), retval.As32())
	}
	// n = litlen - 1
	LEAL(Mem{Base: litLen.As32(), Disp: -1}, n)

	// Find number of bytes to emit for tag.
	CMPL(n.As32(), U8(29))
	JB(LabelRef("one_byte_" + name))
	SUBL(U8(29), n.As32())
	CMPL(n.As32(), U32(1<<8))
	JB(LabelRef("two_bytes_" + name))
	if o.maxLen >= 30+1<<16 {
		CMPL(n.As32(), U32(1<<16))
		JB(LabelRef("three_bytes_" + name))
	} else {
		JB(LabelRef("three_bytes_" + name))
	}

	if o.maxLen >= 1<<16 {
		Label("four_bytes_" + name)
		if !o.skipOutput {
			MOVL(n, n16)
			SHRL(U8(16), n16.As32())
			MOVB(U8(31<<3), Mem{Base: dstBase})
			MOVW(n.As16(), Mem{Base: dstBase, Disp: 1})
			MOVB(n16.As8(), Mem{Base: dstBase, Disp: 3})
		}
		if retval != nil {
			ADDQ(U8(4), retval)
		}
		ADDQ(U8(4), dstBase)
		ADDL(U8(29), n.As32())
		JMP(LabelRef("memmove_long_" + name))
	}
	Label("three_bytes_" + name)
	if !o.skipOutput {
		MOVB(U8(30<<3), Mem{Base: dstBase})
		MOVW(n.As16(), Mem{Base: dstBase, Disp: 1})
	}
	if retval != nil {
		ADDQ(U8(3), retval)
	}
	ADDQ(U8(3), dstBase)
	ADDL(U8(29), n.As32())
	JMP(LabelRef("memmove_long_" + name))

	Label("two_bytes_" + name)
	if !o.skipOutput {
		MOVB(U8(29<<3), Mem{Base: dstBase})
		MOVB(n.As8(), Mem{Base: dstBase, Disp: 1})
	}
	ADDL(U8(29), n.As32())
	if retval != nil {
		ADDQ(U8(2), retval)
	}
	ADDQ(U8(2), dstBase)
	CMPL(n.As32(), U8(64))
	JB(LabelRef("memmove_mid" + name))
	JMP(LabelRef("memmove_long_" + name))

	Label("one_byte_" + name)
	if !o.skipOutput {
		SHLB(U8(3), n.As8())
		MOVB(n.As8(), Mem{Base: dstBase})
	}
	if retval != nil {
		ADDQ(U8(1), retval)
	}
	ADDQ(U8(1), dstBase)
	// Fallthrough

	Label("memmove_" + name)

	// copy(dst[i:], lit)
	dstEnd := GP64()
	copyEnd := end
	if updateDst {
		copyEnd = LabelRef("memmove_end_copy_" + name)
		LEAQ(Mem{Base: dstBase, Index: litLen, Scale: 1}, dstEnd)
	}
	if !o.skipOutput {
		length := GP64()
		MOVL(litLen.As32(), length.As32())

		// We wrote one byte, we have that less in output margin.
		o.outputMargin--
		// updates litBase.
		o.genMemMoveShort("emit_lit_memmove_"+name, dstBase, litBase, length, copyEnd, 1)
		o.outputMargin++
	}
	if updateDst {
		Label("memmove_end_copy_" + name)
		MOVQ(dstEnd, dstBase)
	}
	JMP(end)

	dstEnd = GP64()
	copyEnd = end

	// 30 -> 64 bytes
	Label("memmove_mid" + name)
	// copy(dst[i:], lit)
	if updateDst {
		copyEnd = LabelRef("memmove_mid_end_copy_" + name)
		LEAQ(Mem{Base: dstBase, Index: litLen, Scale: 1}, dstEnd)
	}
	if !o.skipOutput {
		length := GP64()
		MOVL(litLen.As32(), length.As32())

		// We wrote 2 bytes, we have that less in output margin.
		o.outputMargin -= 2
		// updates litBase.
		o.genMemMoveShort("emit_lit_memmove_mid_"+name, dstBase, litBase, length, copyEnd, 30)
		o.outputMargin += 2
	}
	if updateDst {
		Label("memmove_mid_end_copy_" + name)
		MOVQ(dstEnd, dstBase)
	}
	JMP(end)

	// > 64 bytes
	Label("memmove_long_" + name)

	// copy(dst[i:], lit)
	dstEnd = GP64()
	copyEnd = end
	if updateDst {
		copyEnd = LabelRef("memmove_end_copy_long_" + name)
		LEAQ(Mem{Base: dstBase, Index: litLen, Scale: 1}, dstEnd)
	}
	if !o.skipOutput {
		length := GP64()
		MOVL(litLen.As32(), length.As32())

		// updates litBase.
		o.genMemMoveLong("emit_lit_memmove_long_"+name, dstBase, litBase, length, copyEnd)
	}
	if updateDst {
		Label("memmove_end_copy_long_" + name)
		MOVQ(dstEnd, dstBase)
	}
	JMP(end)
	// Should be unreachable
	if debug {
		INT(Imm(3))
	}
	return
}

// genEmitRepeat generates a standlone emitRepeat.
func (o options) genEmitRepeat() {
	TEXT("emitRepeat", NOSPLIT, "func(dst []byte, length int) int")
	Doc("emitRepeat writes a repeat chunk and returns the number of bytes written.",
		"Length must be at least 4 and < 1<<32", "")
	Pragma("noescape")

	dstBase, length, retval := GP64(), GP64(), GP64()

	// retval = 0
	XORQ(retval, retval)

	Load(Param("dst").Base(), dstBase)
	Load(Param("length"), length)
	o.emitRepeat("standalone", length, retval, dstBase, LabelRef("gen_emit_repeat_end"))
	Label("gen_emit_repeat_end")
	Store(retval, ReturnIndex(0))
	RET()
}

// emitRepeat can be used for inlining an emitRepeat call.
// length >= 4 and < 1<<32
// length is not modified. dstBase is updated. retval is added to input.
// retval can be nil.
// Will jump to end label when finished.
// Uses 1 GP register.
func (o options) emitRepeat(name string, length reg.GPVirtual, retval reg.GPVirtual, dstBase reg.GPVirtual, end LabelRef) {
	Comment("emitRepeat")
	Label("emit_repeat_again_" + name)

	// if length <= 28 - one byte
	storeLen := GP64()
	LEAL(Mem{Base: length.As32(), Disp: -1}, storeLen.As32())
	CMPL(length.As32(), U8(29))
	JBE(LabelRef("repeat_one_" + name)) // (subtract one)

	// length < 256 (-1-29)
	LEAL(Mem{Base: length.As32(), Disp: -30}, storeLen.As32())
	CMPL(length.As32(), U32(1+29+256))
	JB(LabelRef("repeat_two_" + name))

	// length < 65536 (-1-29)
	CMPL(length.As32(), U32(1+29+65536))
	JB(LabelRef("repeat_three_" + name))

	// Label("repeat_four_" + name)
	if !o.skipOutput {
		MOVB(U8(31<<3|tagRepeat), Mem{Base: dstBase})      // dst[0] = 31<<3 | tagRepeat
		MOVL(storeLen.As32(), Mem{Base: dstBase, Disp: 1}) // dst[1..3] = uint32(storeLen)
	}
	if retval != nil {
		ADDQ(U8(4), retval) // i += 4
	}
	ADDQ(U8(4), dstBase) // dst += 4
	JMP(end)

	Label("repeat_three_" + name)
	if !o.skipOutput {
		MOVB(U8(30<<3|tagRepeat), Mem{Base: dstBase})      // dst[0] = 30<<3 | tagRepeat
		MOVW(storeLen.As16(), Mem{Base: dstBase, Disp: 1}) // dst[1..2] = uint16(storeLen)
	}
	if retval != nil {
		ADDQ(U8(3), retval) // i += 3
	}
	ADDQ(U8(3), dstBase) // dst += 3
	JMP(end)

	Label("repeat_two_" + name)
	if !o.skipOutput {
		MOVB(U8(29<<3|tagRepeat), Mem{Base: dstBase})     // dst[0] = 29<<3 | tagRepeat
		MOVB(storeLen.As8(), Mem{Base: dstBase, Disp: 1}) // dst[1] = uint16(storeLen)
	}
	if retval != nil {
		ADDQ(U8(2), retval) // i += 2
	}
	ADDQ(U8(2), dstBase) // dst += 2
	JMP(end)

	Label("repeat_one_" + name)
	if !o.skipOutput {
		XORL(storeLen.As32(), storeLen.As32())
		LEAL(Mem{Base: storeLen, Index: length.As32(), Scale: 8, Disp: tagRepeat - (1 * 8)}, storeLen.As32())
		MOVB(storeLen.As8(), Mem{Base: dstBase}) // dst[0] = (length-1)<<3 | tagRepeat
	}
	if retval != nil {
		ADDQ(U8(1), retval) // i += 1
	}
	ADDQ(U8(1), dstBase) // dst += 1
	JMP(end)
}

// emitCopy writes a copy chunk and returns the number of bytes written.
//
// It assumes that:
//	dst is long enough to hold the encoded bytes
//	1 <= offset && offset <= math.MaxUint32
//	4 <= length && length <= 1 << 24

// genEmitCopy generates a standlone emitCopy
func (o options) genEmitCopy() {
	TEXT("emitCopy", NOSPLIT, "func(dst []byte, offset, length int) int")
	Doc("emitCopy writes a copy chunk and returns the number of bytes written.", "",
		"It assumes that:",
		"  dst is long enough to hold the encoded bytes",
		"  1 <= offset && offset <= math.MaxUint32",
		"  4 <= length && length <= 1 << 24", "")
	Pragma("noescape")

	dstBase, offset, length, retval := GP64(), GP64(), GP64(), GP64()

	//	i := 0
	XORQ(retval, retval)
	Load(Param("dst").Base(), dstBase)
	Load(Param("offset"), offset)
	Load(Param("length"), length)
	o.emitCopy("standalone", length, offset, retval, dstBase, LabelRef("gen_emit_copy_end"))
	Label("gen_emit_copy_end")
	Store(retval, ReturnIndex(0))
	RET()
}

func (o options) genEmitCopyLits2() {
	TEXT("emitCopyLits2", NOSPLIT, "func(dst, lits []byte, offset, length int) int")
	Doc("emitCopyLits2 writes a copy chunk and returns the number of bytes written.", "",
		"It assumes that:",
		"  dst is long enough to hold the encoded bytes",
		"  1 <= offset && offset <= 65536",
		"  4 <= length && length <= MaxBlockSize", "")
	Pragma("noescape")

	dstBase, offset, length, retval, nlits := GP64(), GP64(), GP64(), GP64(), GP64()

	//	i := 0
	XORQ(retval, retval)
	Load(Param("dst").Base(), dstBase)
	Load(Param("lits").Len(), nlits)
	Load(Param("offset"), offset)
	Load(Param("length"), length)
	CMPL(length.As32(), U8(11))
	remain := o.emitCopy2WithLits("standalone_lits2", length, offset, nlits, retval, dstBase)
	Label("gen_emit_copy_lits_copylits2")
	litBase := GP64()
	Load(Param("lits").Base(), litBase)
	o.genMemMoveVeryShort("standalone_emitcopy2_lits", dstBase, litBase, nlits, LabelRef("standalone_emitcopy2_lits_end"), 4)
	Label("standalone_emitcopy2_lits_end")

	ADDQ(nlits, retval)
	ADDQ(nlits, dstBase)

	TESTL(remain.As32(), remain.As32())
	JZ(LabelRef("standalone_emitcopy2_lits_done"))
	o.emitRepeat("standalone_emitcopy2_lits", remain, retval, dstBase, LabelRef("standalone_emitcopy2_lits_done"))
	Label("standalone_emitcopy2_lits_done")
	Store(retval, ReturnIndex(0))
	RET()
}

func (o options) genEmitCopyLits3() {
	TEXT("emitCopyLits3", NOSPLIT, "func(dst, lits []byte, offset, length int) int")
	Doc("emitCopyLits3 writes a copy chunk and returns the number of bytes written.", "",
		"It assumes that:",
		"  dst is long enough to hold the encoded bytes",
		"  1 <= offset && offset <= (1<<21)",
		"  4 <= length && length <= MaxBlockSize", "")
	Pragma("noescape")

	dstBase, offset, length, retval, nlits := GP64(), GP64(), GP64(), GP64(), GP64()

	//	i := 0
	XORQ(retval, retval)
	Load(Param("dst").Base(), dstBase)
	Load(Param("lits").Len(), nlits)
	Load(Param("offset"), offset)
	Load(Param("length"), length)
	o.emitCopy3("standalone_lits", length, offset, retval, dstBase, nlits, LabelRef("gen_emit_copy_lits_copylits"))
	Label("gen_emit_copy_lits_copylits")
	litBase := GP64()
	Load(Param("lits").Base(), litBase)
	o.genMemMoveVeryShort("standalone_emitcopy3_lits", dstBase, litBase, nlits, LabelRef("standalone_emitcopy3_lits_end"), 3)
	Label("standalone_emitcopy3_lits_end")

	ADDQ(nlits, retval)
	Store(retval, ReturnIndex(0))
	RET()
}

func (o options) emitCopy2(name string, length, offset, retval, dstBase reg.GPVirtual, end LabelRef) {
	// OPTME: Can be embedded in later ops...
	Comment("emitCopy2")
	LEAL(Mem{Base: offset.As32(), Disp: -64}, offset.As32()) // offset-=64
	LEAL(Mem{Base: length.As32(), Disp: -4}, length.As32())  // length-=4
	if !o.skipOutput {
		MOVW(offset.As16(), Mem{Base: dstBase, Disp: 1})
	}
	CMPL(length.As32(), U8(60))
	JBE(LabelRef("emit_copy2_0_" + name))
	storeval := GP64()
	LEAL(Mem{Base: length.As32(), Disp: -60}, storeval.As32()) // storeval = length-28
	CMPL(length.As32(), U32(60+256))
	JB(LabelRef("emit_copy2_1_" + name))
	CMPL(length.As32(), U32(60+65536))
	JB(LabelRef("emit_copy2_2_" + name))
	// 3 byte length
	if !o.skipOutput {
		MOVB(U8(63<<2|tagCopy2), Mem{Base: dstBase})
		MOVL(storeval.As32(), Mem{Base: dstBase, Disp: 3})
	}
	if retval != nil {
		ADDQ(U8(6), retval) // i+=6
	}
	ADDQ(U8(6), dstBase) // dst+=6
	JMP(end)

	// 2 byte length
	Label("emit_copy2_2_" + name)
	if !o.skipOutput {
		MOVB(U8(62<<2|tagCopy2), Mem{Base: dstBase})
		MOVW(storeval.As16(), Mem{Base: dstBase, Disp: 3})
	}
	if retval != nil {
		ADDQ(U8(5), retval) // i+=5
	}
	ADDQ(U8(5), dstBase) // dst+=5
	JMP(end)

	// 1 byte length
	Label("emit_copy2_1_" + name)
	if !o.skipOutput {
		MOVB(U8(61<<2|tagCopy2), Mem{Base: dstBase})
		MOVB(storeval.As8(), Mem{Base: dstBase, Disp: 3})
	}
	if retval != nil {
		ADDQ(U8(4), retval) // i+=4
	}
	ADDQ(U8(4), dstBase) // dst+=4
	JMP(end)

	// 0 byte length (length in tag)
	Label("emit_copy2_0_" + name)
	if !o.skipOutput {
		MOVL(U32(tagCopy2), storeval.As32())
		LEAL(Mem{Base: storeval, Index: length.As32(), Scale: 4}, storeval.As32())
		MOVB(storeval.As8(), Mem{Base: dstBase, Disp: 0})
	}
	if retval != nil {
		ADDQ(U8(3), retval) // i+=3
	}
	ADDQ(U8(3), dstBase) // dst+=3
	JMP(end)
}

func (o options) emitCopy3(name string, length, offset, retval, dstBase, lits reg.GPVirtual, end LabelRef) {
	Comment("emitCopy3")
	LEAL(Mem{Base: length.As32(), Disp: -4}, length.As32()) // length-=4
	assert(func(ok LabelRef) {
		CMPL(offset.As32(), U32(maxOffset))
		JBE(ok)
	})
	assert(func(ok LabelRef) {
		CMPL(offset.As32(), U32(minCopy3Offset))
		JAE(ok)
	})

	// encoded = uint32(offset-65536)<<11 | tagCopy3 | uint32(lits<<3)
	// Rather long dependency chain, but it takes a little before we use it.
	encoded := GP64().As32()
	if !o.skipOutput {
		LEAL(Mem{Base: offset, Disp: -minCopy3Offset}, encoded)
		SHLL(U8(11), encoded)
		if lits != nil {
			assert(func(ok LabelRef) {
				CMPL(lits.As32(), U8(3))
				JBE(ok)
			})
			// Shift up lits 3 bits
			LEAL(Mem{Base: encoded, Disp: tagCopy3 | 4, Index: lits, Scale: 8}, encoded)
		} else {
			ADDL(U8(tagCopy3|4), encoded)
		}
	}

	CMPL(length.As32(), U8(60))
	JBE(LabelRef("emit_copy3_0_" + name))
	storeval := GP64()
	LEAL(Mem{Base: length.As32(), Disp: -60}, storeval.As32()) // storeval = length-60
	CMPL(length.As32(), U32(60+256))
	JB(LabelRef("emit_copy3_1_" + name))
	CMPL(length.As32(), U32(60+65536))
	JB(LabelRef("emit_copy3_2_" + name))
	// 3 byte length
	if !o.skipOutput {
		ADDL(U32(63<<5), encoded)
		MOVL(encoded, Mem{Base: dstBase})
		MOVL(storeval.As32(), Mem{Base: dstBase, Disp: 4})
	}
	if retval != nil {
		ADDQ(U8(7), retval) // i+=7
	}
	ADDQ(U8(7), dstBase) // dst+=7
	JMP(end)

	// 2 byte length
	Label("emit_copy3_2_" + name)
	if !o.skipOutput {
		ADDL(U32(62<<5), encoded)
		MOVL(encoded, Mem{Base: dstBase})
		MOVW(storeval.As16(), Mem{Base: dstBase, Disp: 4})
	}
	if retval != nil {
		ADDQ(U8(6), retval) // i+=6
	}
	ADDQ(U8(6), dstBase) // dst+=6
	JMP(end)

	// 1 byte length
	Label("emit_copy3_1_" + name)
	if !o.skipOutput {
		ADDL(U32(61<<5), encoded)
		MOVL(encoded, Mem{Base: dstBase})
		MOVB(storeval.As8(), Mem{Base: dstBase, Disp: 4})
	}
	if retval != nil {
		ADDQ(U8(5), retval) // i+=5
	}
	ADDQ(U8(5), dstBase) // dst+=5
	JMP(end)

	// 0 byte length (length in tag)
	Label("emit_copy3_0_" + name)
	SHLL(U8(5), length.As32())
	ORL(length.As32(), encoded)
	if !o.skipOutput {
		MOVL(encoded, Mem{Base: dstBase})
	}
	if retval != nil {
		ADDQ(U8(4), retval) // i+=4
	}
	ADDQ(U8(4), dstBase) // dst+=4
	JMP(end)
}

// emitCopy can be used for inlining an emitCopy call.
// length is modified (and junk). dstBase is updated. retval is added to input.
// retval can be nil.
// lits can be nil - must be added by the caller.
// Will jump to end label when finished.
// Uses 2 GP registers.
func (o options) emitCopy(name string, length, offset, retval, dstBase reg.GPVirtual, end LabelRef) {
	Comment("emitCopy")

	assert(func(ok LabelRef) {
		CMPL(offset.As32(), U32(maxOffset))
		JBE(ok)
	})
	assert(func(ok LabelRef) {
		TESTL(offset.As32(), offset.As32())
		JNZ(ok)
	})
	assert(func(ok LabelRef) {
		CMPL(length.As32(), U8(1))
		JAE(ok)
	})
	if o.maxOffset > maxCopy2Offset {
		//if offset >= 65536 {
		CMPL(offset.As32(), U32(maxCopy2Offset))
		JBE(LabelRef("two_byte_offset_" + name))
		o.emitCopy3(name+"_emit3", length, offset, retval, dstBase, nil, end)
	}

	Label("two_byte_offset_" + name)
	// Offset no more than 2 bytes.

	// if offset > 1024 {
	CMPL(offset.As32(), U32(maxCopy1Offset))
	JA(LabelRef("two_byte_" + name))

	// if offset < 1024 {
	// if length < 15+4 {
	CMPL(length.As32(), U32(15+4))
	JAE(LabelRef("emit_one_longer_" + name))
	{
		// Emit as tag + 1 byte.
		if !o.skipOutput {
			tmp := GP64()
			LEAL(Mem{Base: offset.As32(), Disp: -1}, tmp.As32())
			SHLL(U8(6), tmp.As32())
			LEAL(Mem{Base: tmp.As32(), Index: length.As32(), Scale: 4, Disp: tagCopy1 - (4 << 2)}, tmp.As32())
			MOVW(tmp.As16(), Mem{Base: dstBase})
		}
		if retval != nil {
			ADDQ(U8(2), retval) // i+=2
		}
		ADDQ(U8(2), dstBase) // dst+=2
		JMP(end)
	}
	Label("emit_one_longer_" + name)
	// if length < 18+256 {
	CMPL(length.As32(), U32(18+256))
	JAE(LabelRef("emit_copy1_repeat_" + name))
	{
		// Emit as tag + 1 byte + 1 length.
		if !o.skipOutput {
			tmp := GP64()
			LEAL(Mem{Base: offset.As32(), Disp: -1}, tmp.As32())
			SHLL(U8(6), tmp.As32())
			LEAL(Mem{Base: tmp.As32(), Disp: 15<<2 | tagCopy1}, tmp.As32())
			MOVW(tmp.As16(), Mem{Base: dstBase})
			LEAL(Mem{Base: length.As32(), Disp: -18}, tmp.As32())
			MOVB(tmp.As8(), Mem{Base: dstBase, Disp: 2})
		}
		if retval != nil {
			ADDQ(U8(3), retval) // i+=3
		}
		ADDQ(U8(3), dstBase) // dst+=3
		JMP(end)
	}
	Label("emit_copy1_repeat_" + name)
	{
		// Emit as copy1 tag + repeat.
		// x := uint16(offset<<6) | uint16(14)<<2 | tagCopy1
		// binary.LittleEndian.PutUint16(dst, x)
		// return 2 + emitRepeat(dst[2:], length-18)
		if !o.skipOutput {
			tmp := GP64()
			LEAL(Mem{Base: offset.As32(), Disp: -1}, tmp.As32())
			SHLL(U8(6), tmp.As32())
			LEAL(Mem{Base: tmp.As32(), Disp: 14<<2 | tagCopy1}, tmp.As32())
			MOVW(tmp.As16(), Mem{Base: dstBase})
		}
		if retval != nil {
			ADDQ(U8(2), retval) // i+=2
		}
		ADDQ(U8(2), dstBase) // dst+=2
		SUBL(U8(18), length.As32())
		o.emitRepeat("emit_copy1_do_repeat_"+name, length, retval, dstBase, end)
	}

	// Emit as 2 byte offset.
	Label("two_byte_" + name)
	o.emitCopy2(name+"_emit2", length, offset, retval, dstBase, end)
}

func (o options) emitCopy2WithLits(name string, length, offset, lits, retval, dstBase reg.GPVirtual) (remain reg.GPVirtual) {
	Comment("emitCopy2WithLits")
	if lits != nil {
		assert(func(ok LabelRef) {
			TESTL(lits.As32(), lits.As32())
			JNZ(ok)
		})
		assert(func(ok LabelRef) {
			CMPL(lits.As32(), U8(4))
			JBE(ok)
		})

	}
	assert(func(ok LabelRef) {
		CMPL(offset.As32(), U8(64))
		JAE(ok)
	})

	remain = GP64()
	tmp := GP64()
	XORQ(remain, remain)
	SUBL(U8(64), offset.As32())                             //offset -= 64
	LEAL(Mem{Base: length.As32(), Disp: -11}, tmp.As32())   // tmp = length-11 (to be remain)
	LEAL(Mem{Base: length.As32(), Disp: -4}, length.As32()) // length -= 4
	if !o.skipOutput {
		MOVW(offset.As16(), Mem{Base: dstBase, Disp: 1})
	}
	CMPL(length.As32(), U8(7))
	CMOVLGE(tmp.As32(), remain.As32())
	storeval := GP64()
	MOVQ(U32(7), storeval)
	CMOVLLT(length.As32(), storeval.As32())                                     // storeval = length to store
	LEAL(Mem{Base: lits, Index: storeval, Scale: 4, Disp: -1}, storeval.As32()) // lits-1 at bottom, add storeval above
	tmp = GP64()
	// Tag = 3
	MOVL(U32(3), tmp.As32())
	LEAL(Mem{Base: tmp, Index: storeval, Scale: 8}, storeval.As32()) // Shift up 3 bits
	if !o.skipOutput {
		MOVB(storeval.As8(), Mem{Base: dstBase, Disp: 0})
	}
	if retval != nil {
		ADDQ(U8(3), retval) // i+=3
	}
	ADDQ(U8(3), dstBase) // dst+=3
	return remain
}

// 1-4 bytes...
func (o options) genMemMoveVeryShort(name string, dst, src, length reg.GPVirtual, end LabelRef, max int) {
	Comment("genMemMoveVeryShort")

	if o.inputMargin >= 4 && o.outputMargin >= 4 {
		tmp := GP64()
		MOVL(Mem{Base: src}, tmp.As32())
		MOVL(tmp.As32(), Mem{Base: dst})
		JMP(end)
		return
	}
	name = strings.TrimSuffix(name, "_") + "_"
	AX, CX := GP64(), GP64()
	CMPQ(length, U8(3))
	JE(LabelRef(name + "move_3"))
	if max > 3 {
		JA(LabelRef(name + "move_4"))
	}
	Label(name + "move_1or2")
	MOVB(Mem{Base: src}, AX.As8())
	MOVB(Mem{Base: src, Disp: -1, Index: length, Scale: 1}, CX.As8())
	MOVB(AX.As8(), Mem{Base: dst})
	MOVB(CX.As8(), Mem{Base: dst, Disp: -1, Index: length, Scale: 1})
	JMP(end)

	Label(name + "move_3")
	MOVW(Mem{Base: src}, AX.As16())
	MOVB(Mem{Base: src, Disp: 2}, CX.As8())
	MOVW(AX.As16(), Mem{Base: dst})
	MOVB(CX.As8(), Mem{Base: dst, Disp: 2})
	JMP(end)

	if max > 3 {
		Label(name + "move_4")
		MOVL(Mem{Base: src}, AX.As32())
		MOVL(AX.As32(), Mem{Base: dst})
		JMP(end)
	}
}

// func memmove(to, from unsafe.Pointer, n uintptr)
// src and dst may not overlap.
// No passed registers are updated
// Length must be 1 -> 64 bytes
func (o options) genMemMoveShort(name string, dst, src, length reg.GPVirtual, end LabelRef, minMove int) {
	if o.skipOutput {
		JMP(end)
		return
	}
	Comment("genMemMoveShort")
	AX, CX := GP64(), GP64()
	name += "_memmove_"

	// Only enable if length can be 0.
	if false {
		TESTQ(length, length)
		JEQ(end)
	}
	assert(func(ok LabelRef) {
		CMPQ(length, U8(64))
		JBE(ok)
	})
	assert(func(ok LabelRef) {
		TESTQ(length, length)
		JNZ(ok)
	})
	margin := o.outputMargin
	if o.inputMargin < margin {
		margin = o.inputMargin
	}
	Commentf("margin: %d, min move: %d", margin, minMove)
	if minMove <= 4 {
		if margin <= 3 && minMove < 4 {
			CMPQ(length, U8(3))
			JB(LabelRef(name + "move_1or2"))
			JE(LabelRef(name + "move_3"))
		} else if margin >= 4 && margin < 8 {
			CMPQ(length, U8(4))
			JBE(LabelRef(name + "move_4"))
		}
	}
	if minMove <= 8 {
		if margin <= 15 {
			CMPQ(length, U8(8))
			if margin <= 7 {
				JBE(LabelRef(name + "move_4through8"))
			} else {
				JBE(LabelRef(name + "move_8"))
			}
		}
	}
	if minMove <= 16 {
		CMPQ(length, U8(16))
		JBE(LabelRef(name + "move_8through16"))
	}
	CMPQ(length, U8(32))
	JBE(LabelRef(name + "move_17through32"))
	if debug {
		CMPQ(length, U8(64))
		JBE(LabelRef(name + "move_33through64"))
		INT(U8(3))
	}
	JMP(LabelRef(name + "move_33through64"))

	//genMemMoveLong(name, dst, src, length, end)

	if margin <= 3 && minMove < 3 {
		Label(name + "move_1or2")
		MOVB(Mem{Base: src}, AX.As8())
		MOVB(Mem{Base: src, Disp: -1, Index: length, Scale: 1}, CX.As8())
		MOVB(AX.As8(), Mem{Base: dst})
		MOVB(CX.As8(), Mem{Base: dst, Disp: -1, Index: length, Scale: 1})
		JMP(end)

		Label(name + "move_3")
		MOVW(Mem{Base: src}, AX.As16())
		MOVB(Mem{Base: src, Disp: 2}, CX.As8())
		MOVW(AX.As16(), Mem{Base: dst})
		MOVB(CX.As8(), Mem{Base: dst, Disp: 2})
		JMP(end)
	}

	if margin >= 4 && margin < 8 && minMove <= 8 {
		// Use single move.
		Label(name + "move_4")
		MOVL(Mem{Base: src}, AX.As32())
		MOVL(AX.As32(), Mem{Base: dst})
		JMP(end)
	}

	if minMove <= 8 {
		if margin <= 15 {
			if margin <= 7 {
				Label(name + "move_4through8")
				MOVL(Mem{Base: src}, AX.As32())
				MOVL(Mem{Base: src, Disp: -4, Index: length, Scale: 1}, CX.As32())
				MOVL(AX.As32(), Mem{Base: dst})
				MOVL(CX.As32(), Mem{Base: dst, Disp: -4, Index: length, Scale: 1})
				JMP(end)
			} else {
				// Use single move.
				Label(name + "move_8")
				MOVQ(Mem{Base: src}, AX)
				MOVQ(AX, Mem{Base: dst})
				JMP(end)
			}
		}
	}

	if minMove <= 16 {
		PCALIGN(16)
		Label(name + "move_8through16")
		if margin < 16 {
			MOVQ(Mem{Base: src}, AX)
			MOVQ(Mem{Base: src, Disp: -8, Index: length, Scale: 1}, CX)
			MOVQ(AX, Mem{Base: dst})
			MOVQ(CX, Mem{Base: dst, Disp: -8, Index: length, Scale: 1})
			JMP(end)
		} else {
			// We can always write 16 bytes.
			// 2xGPR is the same speed as 1xXMM
			X0 := XMM()
			MOVOU(Mem{Base: src}, X0)
			MOVOU(X0, Mem{Base: dst})
			JMP(end)
		}
	}

	Label(name + "move_17through32")
	X0, X1, X2, X3 := XMM(), XMM(), XMM(), XMM()
	if margin >= 32 {
		// We always have enough for 2 x 16 bytes.
		MOVOU(Mem{Base: src}, X0)
		MOVOU(Mem{Base: src, Disp: 16}, X1)
		MOVOU(X0, Mem{Base: dst})
		MOVOU(X1, Mem{Base: dst, Disp: 16})
	} else {
		MOVOU(Mem{Base: src}, X0)
		MOVOU(Mem{Base: src, Disp: -16, Index: length, Scale: 1}, X1)
		MOVOU(X0, Mem{Base: dst})
		MOVOU(X1, Mem{Base: dst, Disp: -16, Index: length, Scale: 1})
	}
	JMP(end)

	Label(name + "move_33through64")
	MOVOU(Mem{Base: src}, X0)
	MOVOU(Mem{Base: src, Disp: 16}, X1)
	MOVOU(Mem{Base: src, Disp: -32, Index: length, Scale: 1}, X2)
	MOVOU(Mem{Base: src, Disp: -16, Index: length, Scale: 1}, X3)
	MOVOU(X0, Mem{Base: dst})
	MOVOU(X1, Mem{Base: dst, Disp: 16})
	MOVOU(X2, Mem{Base: dst, Disp: -32, Index: length, Scale: 1})
	MOVOU(X3, Mem{Base: dst, Disp: -16, Index: length, Scale: 1})
	JMP(end)
}

// func genMemMoveLong(to, from unsafe.Pointer, n uintptr)
// src and dst may not overlap.
// length must be >= 64 bytes. Is preserved.
// Non AVX uses 2 GP register, 16 SSE2 registers.
// AVX uses 4 GP registers 16 AVX/SSE registers.
// All passed registers are preserved.
func (o options) genMemMoveLong(name string, dst, src, length reg.GPVirtual, end LabelRef) {
	if o.skipOutput {
		JMP(end)
		return
	}
	Comment("genMemMoveLong")
	name += "large_"

	assert(func(ok LabelRef) {
		CMPQ(length, U8(64))
		JAE(ok)
	})

	// Store start and end for sse_tail
	Label(name + "forward_sse")
	X0, X1, X2, X3, X4, X5 := XMM(), XMM(), XMM(), XMM(), XMM(), XMM()
	// X6, X7 :=  XMM(), XMM()
	//X8, X9, X10, X11 := XMM(), XMM(), XMM(), XMM()

	MOVOU(Mem{Base: src}, X0)
	MOVOU(Mem{Base: src, Disp: 16}, X1)
	MOVOU(Mem{Base: src, Disp: -32, Index: length, Scale: 1}, X2)
	MOVOU(Mem{Base: src, Disp: -16, Index: length, Scale: 1}, X3)

	// forward (only)
	dstAlign := GP64()
	bigLoops := GP64()
	MOVQ(length, bigLoops)
	SHRQ(U8(5), bigLoops) // bigLoops = length / 32

	MOVQ(dst, dstAlign)
	ANDL(U32(31), dstAlign.As32())
	srcOff := GP64()
	MOVQ(U32(64), srcOff)
	SUBQ(dstAlign, srcOff)

	// Move 32 bytes/loop
	DECQ(bigLoops)
	JA(LabelRef(name + "forward_sse_loop_32"))

	// Can be moved inside loop for less regs.
	srcPos := GP64()
	LEAQ(Mem{Disp: -32, Base: src, Scale: 1, Index: srcOff}, srcPos)
	dstPos := GP64()
	LEAQ(Mem{Disp: -32, Base: dst, Scale: 1, Index: srcOff}, dstPos)

	Label(name + "big_loop_back")

	MOVOU(Mem{Disp: 0, Base: srcPos}, X4)
	MOVOU(Mem{Disp: 16, Base: srcPos}, X5)

	MOVOA(X4, Mem{Disp: 0, Base: dstPos})
	MOVOA(X5, Mem{Disp: 16, Base: dstPos})
	ADDQ(U8(32), dstPos)
	ADDQ(U8(32), srcPos)
	ADDQ(U8(32), srcOff) // This could be outside the loop, but we lose a reg if we do.
	DECQ(bigLoops)
	JNA(LabelRef(name + "big_loop_back"))

	Label(name + "forward_sse_loop_32")
	MOVOU(Mem{Disp: -32, Base: src, Scale: 1, Index: srcOff}, X4)
	MOVOU(Mem{Disp: -16, Base: src, Scale: 1, Index: srcOff}, X5)
	MOVOA(X4, Mem{Disp: -32, Base: dst, Scale: 1, Index: srcOff})
	MOVOA(X5, Mem{Disp: -16, Base: dst, Scale: 1, Index: srcOff})
	ADDQ(U8(32), srcOff)
	CMPQ(length, srcOff)
	JAE(LabelRef(name + "forward_sse_loop_32"))

	// sse_tail patches up the beginning and end of the transfer.
	MOVOU(X0, Mem{Base: dst, Disp: 0})
	MOVOU(X1, Mem{Base: dst, Disp: 16})
	MOVOU(X2, Mem{Base: dst, Disp: -32, Index: length, Scale: 1})
	MOVOU(X3, Mem{Base: dst, Disp: -16, Index: length, Scale: 1})

	JMP(end)
	return
}

// genMemMoveLong64 copies regions of at least 64 bytes.
// src and dst may not overlap by less than 64 bytes.
// length must be >= 64 bytes. Is preserved.
// Non AVX uses 2 GP register, 16 SSE2 registers.
// AVX uses 4 GP registers 16 AVX/SSE registers.
// All passed registers are preserved.
func (o options) genMemMoveLong64(name string, dst, src, length reg.GPVirtual, end LabelRef) {
	if o.skipOutput {
		JMP(end)
		return
	}
	Comment("genMemMoveLong")
	name += "large_"

	assert(func(ok LabelRef) {
		CMPQ(length, U8(64))
		JAE(ok)
	})

	// We do purely unaligned copied.
	// Modern processors doesn't seems to care,
	// and one of the addresses will most often be unaligned anyway.
	X0, X1 := XMM(), XMM()

	// forward (only)
	bigLoops := GP64()
	MOVQ(length, bigLoops)
	SHRQ(U8(5), bigLoops) // bigLoops = length / 32

	srcPos, dstPos, remain := GP64(), GP64(), GP64()
	MOVQ(src, srcPos)
	MOVQ(dst, dstPos)
	MOVQ(length, remain)

	Label(name + "big_loop_back")
	MOVOU(Mem{Disp: 0, Base: srcPos}, X0)
	MOVOU(Mem{Disp: 16, Base: srcPos}, X1)
	MOVOU(X0, Mem{Disp: 0, Base: dstPos})
	MOVOU(X1, Mem{Disp: 16, Base: dstPos})
	ADDQ(U8(32), dstPos)
	ADDQ(U8(32), srcPos)
	SUBQ(U8(32), remain)
	DECQ(bigLoops)
	JNZ(LabelRef(name + "big_loop_back"))

	TESTQ(remain, remain)
	JZ(end)

	// We have 1 -> 31 remaining, but we can write in earlier part.
	MOVOU(Mem{Base: srcPos, Disp: -32, Index: remain, Scale: 1}, X0)
	MOVOU(Mem{Base: srcPos, Disp: -16, Index: remain, Scale: 1}, X1)
	MOVOU(X0, Mem{Base: dstPos, Disp: -32, Index: remain, Scale: 1})
	MOVOU(X1, Mem{Base: dstPos, Disp: -16, Index: remain, Scale: 1})

	JMP(end)
	return
}

// genMatchLen generates standalone matchLen.
func (o options) genMatchLen() {
	TEXT("matchLen", NOSPLIT, "func(a, b []byte) int")
	Doc("matchLen returns how many bytes match in a and b", "",
		"It assumes that:",
		"  len(a) <= len(b)", "")
	Pragma("noescape")

	aBase, bBase, length := GP64(), GP64(), GP64()

	Load(Param("a").Base(), aBase)
	Load(Param("b").Base(), bBase)
	Load(Param("a").Len(), length)
	l := o.matchLen("standalone", aBase, bBase, length, nil, LabelRef("gen_match_len_end"))
	Label("gen_match_len_end")
	if o.avx2 {
		VZEROUPPER()
	}
	Store(l.As64(), ReturnIndex(0))
	RET()
}

// matchLen returns the number of matching bytes of a and b.
// len is the maximum number of bytes to match.
// Will jump to end when done and returns the length.
// Uses 2 GP registers.
func (o options) matchLen(name string, a, b, len, dst reg.GPVirtual, end LabelRef) reg.GPVirtual {
	Comment("matchLen")
	tmp := GP64()
	var matched reg.Register
	if dst != nil {
		matched = dst.As32()
	} else {
		dst = GP64()
		matched = dst.As32()
	}
	XORL(matched, matched)
	if o.avx2 {
		// Not faster...
		o.matchLenAVX2(name+"Avx2", a, b, len, LabelRef("avx2_continue_"+name), end, dst)
	}
	Label("avx2_continue_" + name)

	JMP(LabelRef("matchlen_loop_16_entry_" + name))
	Label("matchlen_loopback_16_" + name)
	tmp2 := GP64()
	MOVQ(Mem{Base: a, Index: matched, Scale: 1}, tmp)
	MOVQ(Mem{Base: a, Index: matched, Scale: 1, Disp: 8}, tmp2)
	XORQ(Mem{Base: b, Index: matched, Scale: 1}, tmp)
	JNZ(LabelRef("matchlen_bsf_8_" + name))
	XORQ(Mem{Base: b, Index: matched, Scale: 1, Disp: 8}, tmp2)
	JNZ(LabelRef("matchlen_bsf_16" + name))
	// All 8 byte matched, update and loop.
	LEAL(Mem{Base: len, Disp: -16}, len.As32())
	LEAL(Mem{Base: matched, Disp: 16}, matched)

	Label("matchlen_loop_16_entry_" + name)
	CMPL(len.As32(), U8(16))
	JAE(LabelRef("matchlen_loopback_16_" + name))
	JMP(LabelRef("matchlen_match8_" + name))

	Label("matchlen_bsf_16" + name)
	// Not all match.
	TZCNTQ(tmp2, tmp2)

	SARQ(U8(3), tmp2)
	LEAL(Mem{Base: matched, Index: tmp2, Scale: 1, Disp: 8}, matched)
	JMP(end)

	Label("matchlen_match8_" + name)
	CMPL(len.As32(), U8(8))
	JB(LabelRef("matchlen_match4_" + name))
	MOVQ(Mem{Base: a, Index: matched, Scale: 1}, tmp)
	XORQ(Mem{Base: b, Index: matched, Scale: 1}, tmp)
	JNZ(LabelRef("matchlen_bsf_8_" + name))
	// All 8 byte matched, update and loop.
	LEAL(Mem{Base: len, Disp: -8}, len.As32())
	LEAL(Mem{Base: matched, Disp: 8}, matched)
	JMP(LabelRef("matchlen_match4_" + name))
	Label("matchlen_bsf_8_" + name)

	// Not all match.
	TZCNTQ(tmp, tmp)
	// tmp is the number of bits that matched.
	SARQ(U8(3), tmp)
	LEAL(Mem{Base: matched, Index: tmp, Scale: 1}, matched)
	JMP(end)

	// Less than 8 bytes left.
	// Test 4 bytes...
	Label("matchlen_match4_" + name)
	CMPL(len.As32(), U8(4))
	JB(LabelRef("matchlen_match2_" + name))
	MOVL(Mem{Base: a, Index: matched, Scale: 1}, tmp.As32())
	CMPL(Mem{Base: b, Index: matched, Scale: 1}, tmp.As32())
	JNE(LabelRef("matchlen_match2_" + name))
	LEAL(Mem{Base: len.As32(), Disp: -4}, len.As32())
	LEAL(Mem{Base: matched, Disp: 4}, matched)

	// Test 2 bytes...
	Label("matchlen_match2_" + name)
	CMPL(len.As32(), U8(1))
	// If we don't have 1, branch appropriately
	JE(LabelRef("matchlen_match1_" + name))
	JB(end)
	// 2 or 3
	MOVW(Mem{Base: a, Index: matched, Scale: 1}, tmp.As16())
	CMPW(Mem{Base: b, Index: matched, Scale: 1}, tmp.As16())
	JNE(LabelRef("matchlen_match1_" + name))
	LEAL(Mem{Base: matched, Disp: 2}, matched)
	SUBL(U8(2), len.As32())
	JZ(end)

	// Test 1 byte...
	Label("matchlen_match1_" + name)
	MOVB(Mem{Base: a, Index: matched, Scale: 1}, tmp.As8())
	CMPB(Mem{Base: b, Index: matched, Scale: 1}, tmp.As8())
	JNE(end)
	LEAL(Mem{Base: matched, Disp: 1}, matched)
	JMP(end)
	return dst
}

// matchLen returns the number of matching bytes of a and b.
// len is the maximum number of bytes to match.
// Will jump to end when done and returns the length.
// Uses 3 GP registers.
// It is better on longer matches.
func (o options) matchLenAVX2(name string, a, b, len reg.GPVirtual, cont, end LabelRef, dst reg.GPVirtual) {
	Comment("matchLenAVX2")

	equalMaskBits := GP64()
	Label(name + "loop")
	{
		CMPQ(len, U8(32))
		JB(cont)
		Comment("load 32 bytes into YMM registers")
		adata := YMM()
		bdata := YMM()
		equalMaskBytes := YMM()
		VMOVDQU(Mem{Base: a}, adata)
		VMOVDQU(Mem{Base: b}, bdata)
		Comment("compare bytes in adata and bdata, like 'bytewise XNOR'",
			"if the byte is the same in adata and bdata, VPCMPEQB will store 0xFF in the same position in equalMaskBytes")
		VPCMPEQB(adata, bdata, equalMaskBytes)
		Comment("like convert byte to bit, store equalMaskBytes into general reg")
		VPMOVMSKB(equalMaskBytes, equalMaskBits.As32())
		CMPL(equalMaskBits.As32(), U32(0xffffffff))
		JNE(LabelRef(name + "cal_prefix"))
		ADDQ(U8(32), a)
		ADDQ(U8(32), b)
		ADDL(U8(32), dst)
		SUBQ(U8(32), len)
		JZ(end)
		JMP(LabelRef(name + "loop"))
	}

	Label(name + "cal_prefix")
	{
		NOTQ(equalMaskBits)
		TZCNTQ(equalMaskBits, equalMaskBits)
		ADDL(equalMaskBits.As32(), dst)
	}
	JMP(end)
	return
}

func (o options) cvtLZ4BlockAsm() {
	snap := "Asm"
	name := "lz4_mz_"
	srcAlgo := "LZ4"
	dstAlgo := "MinLZ"

	TEXT("cvt"+srcAlgo+"Block"+snap, NOSPLIT, "func(dst, src []byte) (uncompressed int, dstUsed int)")
	Doc("cvt"+srcAlgo+"Block converts an "+srcAlgo+" block to "+dstAlgo, "")
	Pragma("noescape")
	o.outputMargin = 12
	o.maxOffset = math.MaxUint16

	const (
		errCorrupt     = -1
		errDstTooSmall = -2
	)
	dst, dstLen, src, srcLen, retval := GP64(), GP64(), GP64(), GP64(), GP64()

	// retval = 0
	XORQ(retval, retval)

	Load(Param("dst").Base(), dst)
	Load(Param("dst").Len(), dstLen)
	Load(Param("src").Base(), src)
	Load(Param("src").Len(), srcLen)
	srcEnd, dstEnd := GP64(), GP64()
	LEAQ(Mem{Base: src, Index: srcLen, Scale: 1, Disp: 0}, srcEnd)
	LEAQ(Mem{Base: dst, Index: dstLen, Scale: 1, Disp: -o.outputMargin}, dstEnd)

	checkSrc := func(reg reg.GPVirtual) {
		if debug {
			assert(func(ok LabelRef) {
				CMPQ(reg, srcEnd)
				JB(ok)
			})
		} else {
			CMPQ(reg, srcEnd)
			JAE(LabelRef(name + "corrupt"))
		}
	}
	checkDst := func(reg reg.GPVirtual) {
		CMPQ(reg, dstEnd)
		JAE(LabelRef(name + "dstfull"))
	}

	const lz4MinMatch = 4
	const doRepeat = true

	// We must have last offset on stack.
	var lastOffset Mem
	if doRepeat {
		lastOffset = AllocLocal(8)
		MOVL(U32(1), lastOffset)
	}

	Label(name + "loop")
	checkSrc(src)
	checkDst(dst)
	token := GP64()
	MOVBQZX(Mem{Base: src}, token)
	ll, ml := GP64(), GP64()
	MOVQ(token, ll)
	MOVQ(token, ml)
	ANDQ(U8(0xf), ml)
	fused := GP64()
	XORQ(fused, fused)
	SHRQ(U8(4), ll)
	CMPQ(token, U8(5<<4))
	CMOVQLT(ll, fused)
	JLT(LabelRef(name + "ll_end"))
	// If upper nibble is 15, literal length is extended
	{
		CMPQ(token, U8(0xf0))
		JB(LabelRef(name + "ll_end"))
		Label(name + "ll_loop")
		INCQ(src) // s++
		checkSrc(src)
		val := GP64()
		MOVBQZX(Mem{Base: src}, val)
		ADDQ(val, ll)
		CMPQ(val, U8(255))
		JEQ(LabelRef(name + "ll_loop"))
		Label(name + "ll_end")
	}

	// if s+ll >= len(src)
	endLits := GP64()
	LEAQ(Mem{Base: src, Index: ll, Scale: 1}, endLits)
	ADDQ(U8(lz4MinMatch), ml)
	checkSrc(endLits)
	INCQ(src) // s++
	INCQ(endLits)
	TESTQ(ll, ll)
	JZ(LabelRef(name + "lits_done"))
	TESTQ(fused, fused)
	JNZ(LabelRef(name + "lits_done"))
	{
		Label(name + "lits_emit_do")
		dstEnd := GP64()
		LEAQ(Mem{Base: dst, Index: ll, Scale: 1}, dstEnd)
		checkDst(dstEnd)
		o.emitLiteral(strings.TrimRight(name, "_"), ll, nil, dst, src, LabelRef(name+"lits_emit_done"), true)
		Label(name + "lits_emit_done")
	}
	Label(name + "lits_done")
	ADDQ(ll, retval)
	startLits := endLits
	if false {
		XCHGQ(endLits, src)
	} else {
		// Thanks Intel....
		tmp := GP64()
		MOVQ(endLits, tmp)
		MOVQ(src, startLits)
		MOVQ(tmp, src)
	}

	// if s == len(src) && ml == lz4MinMatch
	CMPQ(src, srcEnd)
	JNE(LabelRef(name + "match"))

	CMPQ(ml, U8(lz4MinMatch))
	JNE(LabelRef(name + "corrupt"))

	TESTQ(fused, fused) // Emit if we owe a fused.
	JNZ(LabelRef(name + "emit_final"))
	JMP(LabelRef(name + "done"))

	Label(name + "match")

	// if s >= len(src)-2 {
	ADDQ(U8(2), src)
	checkSrc(src)
	offset := GP64()
	MOVWQZX(Mem{Base: src, Disp: -2}, offset)

	if debug {
		// if offset == 0 {
		TESTQ(offset, offset)
		JNZ(LabelRef(name + "c1"))
		JMP(LabelRef(name + "corrupt"))

		Label(name + "c1")

		// if int(offset) > uncompressed {
		CMPQ(offset, retval)
		JB(LabelRef(name + "c2"))
		JMP(LabelRef(name + "corrupt"))

		Label(name + "c2")

	} else {
		// if offset == 0 {
		TESTQ(offset, offset)
		JZ(LabelRef(name + "corrupt"))

		// if int(offset) > uncompressed {
		CMPQ(offset, retval)
		JA(LabelRef(name + "corrupt"))
	}

	// if ml == lz4MinMatch+15 {
	{
		CMPQ(ml, U8(lz4MinMatch+15))
		JNE(LabelRef(name + "ml_done"))

		Label(name + "ml_loop")
		val := GP64()
		MOVBQZX(Mem{Base: src}, val)
		INCQ(src)     // s++
		ADDQ(val, ml) // ml += val
		checkSrc(src)
		CMPQ(val, U8(255))
		JEQ(LabelRef(name + "ml_loop"))
	}
	Label(name + "ml_done")

	// uncompressed += ml
	ADDQ(ml, retval)
	TESTQ(fused, fused)
	JNZ(LabelRef(name + "dofuse"))
	if doRepeat {
		CMPQ(lastOffset, offset)
		JNE(LabelRef(name + "docopy"))
		// emitRepeat16(dst[d:], offset, ml)
		o.emitRepeat("lz4_mz", ml, nil, dst, LabelRef(name+"loop"))
	} else {
		JMP(LabelRef(name + "docopy"))
	}
	// Offsets can only be 16 bits
	Label(name + "dofuse")
	{
		if doRepeat {
			MOVQ(offset, lastOffset)
		}
		CMPQ(offset, U8(64))
		JB(LabelRef(name + "doemitcopy"))

		remain := o.emitCopy2WithLits("lz4_mz", ml, offset, fused, nil, dst)
		tmp := GP64()
		MOVL(Mem{Base: startLits}, tmp.As32())
		MOVL(tmp.As32(), Mem{Base: dst})
		ADDQ(fused, dst) // dst += litLen
		TESTL(remain.As32(), remain.As32())
		JZ(LabelRef(name + "loop"))
		o.emitRepeat("fused_emitrep_"+name, remain, nil, dst, LabelRef(name+"loop"))
	}

	Label(name + "doemitcopy")
	{
		o.emitLiteral(name+"emitcopy", fused, nil, dst, startLits, LabelRef(name+"_emit_done"), true)
		Label(name + "_emit_done")
		o.emitCopy(name+"_lz4_mz_short_", ml, offset, nil, dst, LabelRef(name+"loop"))
	}

	Label(name + "docopy")
	{
		// emitCopy16(dst[d:], offset, ml)
		if doRepeat {
			MOVQ(offset, lastOffset)
		}
		o.emitCopy(name+"_lz4_mz", ml, offset, nil, dst, LabelRef(name+"loop"))
	}

	Label(name + "emit_final")
	// We should always have space...
	// dstEnd = GP64()
	// LEAQ(Mem{Base: dst, Index: fused, Scale: 1}, dstEnd)
	// checkDst(dstEnd)
	o.emitLiteral(name+"emit_final", fused, nil, dst, startLits, LabelRef(name+"done"), true)

	Label(name + "done")
	{
		tmp := GP64()
		Load(Param("dst").Base(), tmp)
		SUBQ(tmp, dst)
		Store(retval, ReturnIndex(0))
		Store(dst, ReturnIndex(1))
		RET()
	}
	Label(name + "corrupt")
	{
		tmp := GP64()
		if debug {
			tmp := GP64()
			Load(Param("dst").Base(), tmp)
			SUBQ(tmp, dst)
			Store(dst, ReturnIndex(1))
		}
		XORQ(tmp, tmp)
		LEAQ(Mem{Base: tmp, Disp: errCorrupt}, retval)
		Store(retval, ReturnIndex(0))
		RET()
	}

	Label(name + "dstfull")
	{
		tmp := GP64()
		XORQ(tmp, tmp)
		LEAQ(Mem{Base: tmp, Disp: errDstTooSmall}, retval)
		Store(retval, ReturnIndex(0))
		RET()
	}
}

func (o options) genDecodeBlockAsm(name string) {
	TEXT(name, 0, "func(dst, src []byte) int")
	Doc(name+" encodes a non-empty src to a guaranteed-large-enough dst.",
		"It assumes that the varint-encoded length of the decompressed bytes has already been read.", "")
	Pragma("noescape")
	dstBase := Load(Param("dst").Base(), GP64())
	dstLen := Load(Param("dst").Len(), GP64())
	srcBase := Load(Param("src").Base(), GP64())
	srcLen := Load(Param("src").Len(), GP64())
	dst := GP64()
	MOVQ(dstBase, dst)
	dstPos := GP64()
	XORQ(dstPos, dstPos)

	src := GP64()
	MOVQ(srcBase, src)

	offset := GP64()
	MOVQ(U32(1), offset)

	srcEnd, dstEnd := GP64(), GP64()
	LEAQ(Mem{Base: dstBase, Index: dstLen, Scale: 1, Disp: 0}, dstEnd)
	LEAQ(Mem{Base: srcBase, Index: srcLen, Scale: 1, Disp: 0}, srcEnd)

	if o.ignoreMargins {
		o.genDecodeLoop(name, dstEnd, srcEnd, dst, src, offset, dstPos)
	} else {
		// Do decode with big margins first.
		// When done switch to a version with no margins.
		o.outputMargin = 20
		o.inputMargin = 20
		o.genDecodeLoop(name+"_fast", dstEnd, srcEnd, dst, src, offset, dstPos)

		o.outputMargin = 0
		o.inputMargin = 0
		o.genDecodeLoop(name+"_remain", dstEnd, srcEnd, dst, src, offset, dstPos)
	}

	// END
	Label("end")
	// Validate output length
	// Reload input, so we don't need to have the registers live while decoding.
	srcBase = Load(Param("src").Base(), GP64())
	srcLen = Load(Param("src").Len(), GP64())
	dstBase = Load(Param("dst").Base(), GP64())
	dstLen = Load(Param("dst").Len(), GP64())

	wantDst, wantSrc := GP64(), GP64()
	LEAQ(Mem{Base: dstBase, Index: dstLen, Scale: 1}, wantDst)
	LEAQ(Mem{Base: srcBase, Index: srcLen, Scale: 1}, wantSrc)

	CMPQ(dst, wantDst)
	JNE(LabelRef("corrupt"))
	CMPQ(src, wantSrc)
	JNE(LabelRef("corrupt"))
	ret, _ := ReturnIndex(0).Resolve()
	MOVQ(U32(0), ret.Addr)
	RET()

	Label("corrupt")
	MOVQ(U32(1), ret.Addr)
	RET()
}

func (o options) genDecodeLoop(name string, dstEnd, srcEnd reg.Register, dst, src, offset, dstPos reg.GPVirtual) {
	var dstLimit, srcLimit Op

	// If we need to reduce GP, this can save up to 2 registers with marginal impact.
	const reduceGP = 0
	if o.inputMargin > 0 && !o.ignoreMargins {
		if reduceGP >= 2 {
			srcLimit = AllocLocal(8)
			tmp := GP64()
			LEAQ(Mem{Base: srcEnd, Disp: -o.inputMargin}, tmp)
			MOVQ(tmp, srcLimit)
		} else {
			srcLimit = GP64()
			LEAQ(Mem{Base: srcEnd, Disp: -o.inputMargin}, srcLimit)
		}
	} else {
		srcLimit = srcEnd
	}
	if o.outputMargin > 0 && !o.ignoreMargins {
		if reduceGP >= 1 {
			dstLimit = AllocLocal(8)
			tmp := GP64()
			LEAQ(Mem{Base: dstEnd, Disp: -o.outputMargin}, tmp)
			MOVQ(tmp, dstLimit)
		} else {
			dstLimit = GP64()
			LEAQ(Mem{Base: dstEnd, Disp: -o.outputMargin}, dstLimit)
		}
	} else {
		dstLimit = dstEnd
	}

	// Helpers
	testSrcDst := func(length reg.GP, disp int) {
		tmpSrc := GP64()
		tmpDst := GP64()
		LEAQ(Mem{Base: src, Index: length.As64(), Scale: 1, Disp: disp}, tmpSrc)
		LEAQ(Mem{Base: dst, Index: length.As64(), Scale: 1, Disp: disp}, tmpDst)
		CMPQ(tmpSrc, srcEnd)
		JA(LabelRef("corrupt"))
		CMPQ(tmpDst, dstEnd)
		JA(LabelRef("corrupt"))
	}
	// offset is optional
	testSrc := func(offset reg.GP) {
		if offset == nil {
			CMPQ(src, srcEnd)
			JA(LabelRef("corrupt"))
			return
		}
		tmpSrc := GP64()
		LEAQ(Mem{Base: src, Index: offset.As64(), Scale: 1}, tmpSrc)
		CMPQ(tmpSrc, srcEnd)
		JA(LabelRef("corrupt"))
	}
	testDstRel := func(offset reg.GP) {
		if offset == nil {
			CMPQ(dst, dstEnd)
			JA(LabelRef("corrupt"))
			return
		}
		tmpDst := GP64()
		LEAQ(Mem{Base: dst, Index: offset.As64(), Scale: 1}, tmpDst)
		CMPQ(tmpDst, dstEnd)
		JA(LabelRef("corrupt"))
	}

	_ = testSrc
	_ = testSrcDst

	// preload the copy value.
	// improvement seems quite minimal, but there.
	const preloadCopyVal = true

	// Whether this is a benefit remains undecided.
	alwaysLits := o.inputMargin > 10

	tag := GP64()   // Only tag
	value := GP64() // Only value

	prefetch := o.inputMargin > 11
	loopFinished := func() {
		if prefetch {
			// We already loaded the next input byte.
			MOVQ(tag, value)
			SHRQ(U8(2), value)
			CMPQ(src, srcLimit)
			JB(LabelRef(name + "_loop_nofetch"))
			JMP(LabelRef(name + "_end_copy"))
		} else {
			JMP(LabelRef(name + "_loop"))
		}
	}

	// LOOP
	if !prefetch {
		// Triggers https://github.com/golang/go/issues/74648
		//PCALIGN(16)
	}
	Label(name + "_loop")
	CMPQ(src, srcLimit)
	JAE(LabelRef(name + "_end_copy"))
	MOVBQZX(Mem{Base: src}, tag)
	MOVQ(tag, value)
	SHRQ(U8(2), value)

	if prefetch {
		PCALIGN(16)
		Label(name + "_loop_nofetch")
	}
	// Check destination
	CMPQ(dst, dstLimit)
	JAE(LabelRef(name + "_end_copy"))

	Label(name + "_dst_ok")
	// if input&3 == 0 {literals}
	ANDQ(U8(0x3), tag)
	JNZ(LabelRef(name + "_copy"))
	// TAG 00 Literals
	length := GP64()
	PCALIGN(16)
	Label(name + "_lits")
	{
		MOVL(value.As32(), length.As32())
		SHRL(U8(1), length.As32())
		CMPL(length.As32(), U8(29))
		JB(LabelRef(name + "_lit_0"))
		JEQ(LabelRef(name + "_lit_1"))
		CMPL(length.As32(), U8(30))
		JEQ(LabelRef(name + "_lit_2"))
		JMP(LabelRef(name + "_lit_3")) // Must be 31

		// 1 - > 29 literals
		PCALIGN(16)
		Label(name + "_lit_0")
		{
			INCQ(src)
			INCL(length.As32()) // length++
			testDstRel(length)

			// Check if repeat...
			BTL(U8(0), value.As32())
			JC(LabelRef(name + "_copy_exec_short"))

			testSrc(length)
			// We have to tweak input margin, since we haven't checked it since reading the tag
			o.inputMargin--
			o.genMemMoveShort(name+"_lit_0_copy", dst, src, length, LabelRef(name+"_litcopy_done"), 1)
			o.inputMargin++
		}
		Label(name + "_lit_1")
		{
			// 30 + 1 byte literals
			if o.inputMargin < 2 {
				ADDQ(U8(2), src)
				testSrc(nil)
				MOVBQZX(Mem{Base: src, Disp: -1}, length)
			} else {
				MOVBQZX(Mem{Base: src, Disp: 1}, length)
				ADDQ(U8(2), src)
			}
			JMP(LabelRef(name + "_litcopy_long"))
		}
		// Literal length 2
		Label(name + "_lit_2")
		{
			// 30 + 2 bytes literals
			if o.inputMargin < 3 {
				ADDQ(U8(3), src)
				testSrc(nil)
				MOVWQZX(Mem{Base: src, Disp: -2}, length)
			} else {
				MOVWQZX(Mem{Base: src, Disp: 1}, length)
				ADDQ(U8(3), src)
			}
			JMP(LabelRef(name + "_litcopy_long"))
		}
		// Literal length 3
		// 30 + 3 bytes literals
		Label(name + "_lit_3")
		{
			if o.inputMargin < 4 {
				ADDQ(U8(4), src)
				testSrc(nil)
				MOVL(Mem{Base: src, Disp: -4}, length.As32())
			} else {
				MOVL(Mem{Base: src, Disp: 0}, length.As32())
				ADDQ(U8(4), src)
			}
			SHRL(U8(8), length.As32())
			if debug {
				CMPL(length.As32(), U32((8<<20)-30))
				JA(LabelRef("corrupt"))
			}
			JMP(LabelRef(name + "_litcopy_long"))
		}
		Label(name + "_litcopy_long")
		{
			LEAQ(Mem{Base: length, Disp: 30}, length) // length += 30
			testDstRel(length)
			// Go to repeat
			BTL(U8(0), value.As32())
			JC(LabelRef(name + "_copy_exec"))

			// Literals
			testSrc(length)
			CMPL(length.As32(), U8(64))
			JBE(LabelRef(name + "_litcopy_short_reduced"))
			o.genMemMoveLong(name+"_litcopy_long", dst, src, length, LabelRef(name+"_litcopy_done"))
		}
		Label(name + "_litcopy_short_reduced")
		{
			// We have read up to 4 bytes, so we must reduce the input margin by 5.
			o.inputMargin -= 4
			o.genMemMoveShort(name+"_lit_longer_copy", dst, src, length, LabelRef(name+"_litcopy_done"), 30)
			o.inputMargin += 4
		}

		Label(name + "_litcopy_done")
		{
			ADDQ(length, src)
			ADDQ(length, dst)
			ADDQ(length, dstPos)
			{
				// Inline since branch prediction stats are different due to just emitting lits.
				CMPQ(src, srcLimit)
				JAE(LabelRef(name + "_end_done"))
				MOVBQZX(Mem{Base: src}, tag)
				MOVQ(tag, value)
				SHRQ(U8(2), value)

				// Check destination
				CMPQ(dst, dstLimit)
				JAE(LabelRef(name + "_end_done"))

				// if input&3 == 0 {literals}
				ANDQ(U8(0x3), tag)
				JZ(LabelRef(name + "_lits"))
				// Fall through to copy
			}
		}
	}

	// Copy/Repeat
	// We populate length and offset before jumping to _copy_exec
	Label(name + "_copy")
	var copyIn reg.GPVirtual
	if preloadCopyVal && o.inputMargin > 4 {
		// Read 4 bytes after tag
		copyIn = GP32()
		MOVL(Mem{Base: src, Disp: 0}, copyIn)
	}
	CMPL(tag.As32(), U8(2))
	JB(LabelRef(name + "_copy_1"))
	JEQ(LabelRef(name + "_copy_2"))
	JMP(LabelRef(name + "_copy_3"))

	// TAG 1 - Copy 1
	PCALIGN(16)
	Label(name + "_copy_1")
	{
		if o.inputMargin < 2 {
			ADDQ(U8(2), src)
			testSrc(nil)
			MOVWQZX(Mem{Base: src, Disp: -2}, offset)
		} else {
			if preloadCopyVal {
				MOVWQZX(copyIn.As16(), offset)
			} else {
				MOVWQZX(Mem{Base: src, Disp: 0}, offset)
			}
			ADDQ(U8(2), src)
		}

		MOVQ(value, length)
		ANDL(U8(15), length.As32()) // length = length & 15
		SHRL(U8(6), offset.As32())  // offset = offset >> 6
		INCL(offset.As32())         // offset++
		if o.inputMargin >= 3 {
			// Branchless - slightly faster
			if preloadCopyVal {
				SHRL(U8(16), copyIn.As32())
			}
			tmp, src2 := GP64(), GP64()
			LEAQ(Mem{Base: src, Disp: 1}, src2)
			if preloadCopyVal {
				MOVBLZX(copyIn.As8(), tmp.As32())
			} else {
				MOVBLZX(Mem{Base: src, Disp: 0}, tmp.As32())
			}
			ADDL(U8(4), length.As32()) // length += 4
			LEAL(Mem{Base: tmp.As32(), Disp: 18}, tmp.As32())
			CMPL(length.As32(), U8(15+4))
			CMOVLEQ(tmp.As32(), length.As32())
			CMOVQEQ(src2, src)
			JMP(LabelRef(name + "_copy_exec"))
		} else {
			CMPL(length.As32(), U8(15))
			JNE(LabelRef(name + "_copy_1_short"))
			if o.inputMargin < 3 {
				ADDQ(U8(1), src)
				testSrc(nil)
				MOVBLZX(Mem{Base: src, Disp: -1}, length.As32())
			} else {
				if preloadCopyVal {
					MOVBLZX(copyIn.As8(), length.As32())
				} else {
					MOVBLZX(Mem{Base: src, Disp: 0}, length.As32())
				}
				LEAQ(Mem{Base: src, Disp: 1}, src)
			}
			LEAL(Mem{Base: length.As32(), Disp: 18}, length.As32())
			JMP(LabelRef(name + "_copy_exec"))

			Label(name + "_copy_1_short")
			LEAL(Mem{Base: length.As32(), Disp: 4}, length.As32()) // length += 4
			JMP(LabelRef(name + "_copy_exec_short"))
		}
	}

	// TAG 2 - Copy 2
	PCALIGN(16)
	Label(name + "_copy_2")
	{
		// length = int(src[s-3]) >> 2
		MOVQ(value.As64(), length.As64())
		//  offset = int(uint32(src[s-2]) | uint32(src[s-1])<<8)

		CMPL(value.As32(), U8(61))
		JB(LabelRef(name + "_copy_2_0_extra"))
		JEQ(LabelRef(name + "_copy_2_1_extra"))
		CMPL(length.As32(), U8(63))
		JB(LabelRef(name + "_copy_2_2_extra"))
		// 3 extra length bytes
		{
			if o.inputMargin < 3+3 {
				ADDQ(U8(6), src)
				testSrc(nil)
				MOVWQZX(Mem{Base: src, Disp: -5}, offset.As64())
				MOVL(Mem{Base: src, Disp: -4}, length.As32())
			} else {
				MOVWQZX(Mem{Base: src, Disp: 1}, offset.As64())
				MOVL(Mem{Base: src, Disp: 2}, length.As32())
				ADDQ(U8(6), src)
			}
			SHRL(U8(8), length.As32())
			LEAL(Mem{Base: length.As32(), Disp: 64}, length.As32())
			ADDQ(U8(64), offset)
			JMP(LabelRef(name + "_copy_exec_long_long"))
		}
		// 2 extra length bytes
		Label(name + "_copy_2_2_extra")
		{
			if o.inputMargin < 2+3 {
				ADDQ(U8(5), src)
				testSrc(nil)
				MOVWQZX(Mem{Base: src, Disp: -4}, offset.As64())
				MOVWLZX(Mem{Base: src, Disp: -2}, length.As32())
			} else {
				MOVWQZX(Mem{Base: src, Disp: 1}, offset.As64())
				MOVWLZX(Mem{Base: src, Disp: 3}, length.As32())
				ADDQ(U8(5), src)
			}
			LEAL(Mem{Base: length.As32(), Disp: 64}, length.As32())
			ADDQ(U8(64), offset)
			JMP(LabelRef(name + "_copy_exec_long_long"))
		}

		// 1 extra length bytes
		Label(name + "_copy_2_1_extra")
		{
			if o.inputMargin < 1+3 {
				ADDQ(U8(4), src)
				testSrc(nil)
				MOVWQZX(Mem{Base: src, Disp: -3}, offset.As64())
				MOVBLZX(Mem{Base: src, Disp: -1}, length.As32())
			} else {
				if preloadCopyVal {
					MOVL(copyIn.As32(), length.As32())
					SHRL(U8(8), copyIn)
					SHRL(U8(24), length.As32())
					MOVWQZX(copyIn.As16(), offset.As64())
				} else {
					MOVWQZX(Mem{Base: src, Disp: 1}, offset.As64())
					MOVBLZX(Mem{Base: src, Disp: 3}, length.As32())
				}
				ADDQ(U8(4), src)
			}
			LEAL(Mem{Base: length.As32(), Disp: 64}, length.As32())
			ADDQ(U8(64), offset)
			JMP(LabelRef(name + "_copy_exec_long_long"))
		}
		Label(name + "_copy_2_0_extra")
		{
			if o.inputMargin < 0+3 {
				LEAQ(Mem{Base: src, Disp: 3}, src)
				testSrc(nil)
				MOVWQZX(Mem{Base: src, Disp: -2}, offset.As64())
			} else {
				if preloadCopyVal {
					SHRL(U8(8), copyIn)
					MOVWQZX(copyIn.As16(), offset.As64())
				} else {
					MOVWQZX(Mem{Base: src, Disp: 1}, offset.As64())
				}
				LEAQ(Mem{Base: src, Disp: 3}, src)
			}
			LEAL(Mem{Base: length.As32(), Disp: 4}, length.As32())
			ADDQ(U8(64), offset)
			JMP(LabelRef(name + "_copy_short_no_ol"))
		}
	}
	// TAG 3 - Copy 2/3 fused
	PCALIGN(16)
	Label(name + "_copy_3")
	{
		if o.inputMargin < 4 {
			ADDQ(U8(4), src)
			testSrc(nil)
			MOVL(Mem{Base: src, Disp: -4}, offset.As32())
		} else {
			// Load offset for Copy3
			if preloadCopyVal {
				MOVL(copyIn.As32(), offset.As32())
			} else {
				MOVL(Mem{Base: src, Disp: 0}, offset.As32())
			}

			// Very hot?
			ADDQ(U8(4), src)
		}
		// same on Zen2, so just skip
		const trybmi = false
		lits := GP64()
		if trybmi {
			tmp := GP64()
			MOVL(U32(1|2<<8), tmp.As32())
			BEXTRL(tmp.As32(), value.As32(), lits.As32())
		} else {
			MOVQ(value.As64(), lits.As64())
			SHRQ(U8(1), lits.As64())
			ANDQ(U8(3), lits.As64())
		}

		BTL(U8(0), value.As32())
		JC(LabelRef(name + "_copy3_read"))
		// Copy 2 - fused
		{
			// length = 4 + int(val>>5)&7
			if trybmi {
				tmp := GP64()
				MOVL(U32(3|3<<8), tmp.As32())
				BEXTRL(tmp.As32(), value.As32(), value.As32())
			} else {
				SHRL(U8(3), value.As32()) // (value already shifted down 2)
				ANDL(U8(7), value.As32())
			}
			LEAL(Mem{Base: value, Disp: 4}, length.As32())
			if preloadCopyVal && o.inputMargin > 4 {
				SHRL(U8(8), copyIn.As32())
				MOVWQZX(copyIn.As16(), offset.As64())
			} else {
				// Just load offset instead of shifting.
				MOVWQZX(Mem{Base: src, Disp: -3}, offset.As64())
			}
			DECQ(src)
			INCQ(lits)
			// Add 1-3 lits before copy
			if o.inputMargin < 3+4 || o.outputMargin < 4 {
				testSrcDst(lits, 0)
				o.genMemMoveVeryShort(name+"_copy2_fused_lits", dst, src, lits, LabelRef(name+"_copy2_fused_lits_done"), 4)
			} else {
				tmp := GP64()
				MOVL(Mem{Base: src, Disp: 0}, tmp.As32())
				MOVL(tmp.As32(), Mem{Base: dst, Disp: 0})
			}
			Label(name + "_copy2_fused_lits_done")
			ADDQ(U8(64), offset.As64())
			ADDQ(lits, src)
			ADDQ(lits, dst)
			ADDQ(lits, dstPos)
			JMP(LabelRef(name + "_copy_short_no_ol"))
		}

		Label(name + "_copy3_read")
		{
			// offset is the newly loaded value..
			if trybmi {
				tmp := GP64()
				MOVL(U32(5|6<<8), tmp.As32())
				BEXTRL(tmp.As32(), offset.As32(), length.As32())
				MOVL(U32(11|16<<8), tmp.As32())
				BEXTRL(tmp.As32(), offset.As32(), offset.As32())
			} else {
				MOVL(offset.As32(), length.As32())
				SHRL(U8(5), length.As32())
				ANDL(U8(63), length.As32())
				SHRL(U8(11), offset.As32())
				ADDL(U32(65536), offset.As32())
			}

			CMPL(length.As32(), U8(61))
			JB(LabelRef(name + "_copy_3_0_extra"))
			JEQ(LabelRef(name + "_copy_3_1_extra"))
			CMPL(length.As32(), U8(62))
			JEQ(LabelRef(name + "_copy_3_2_extra"))
			// 3 extra length bytes
			{
				if o.inputMargin < 4+3 {
					ADDQ(U8(3), src)
					testSrc(nil)
					MOVL(Mem{Base: src, Disp: -4}, length.As32())
				} else {
					MOVL(Mem{Base: src, Disp: -1}, length.As32())
					ADDQ(U8(3), src)
				}
				SHRL(U8(8), length.As32())
				LEAL(Mem{Base: length.As32(), Disp: 64}, length.As32())
				JMP(LabelRef(name + "_copy_fused_long"))
			}
			// 2 extra length bytes
			Label(name + "_copy_3_2_extra")
			{
				if o.inputMargin < 4+2 {
					ADDQ(U8(2), src)
					testSrc(nil)
					MOVWLZX(Mem{Base: src, Disp: -2}, length.As32())
				} else {
					MOVWLZX(Mem{Base: src, Disp: 0}, length.As32())
					ADDQ(U8(2), src)
				}
				LEAL(Mem{Base: length.As32(), Disp: 64}, length.As32())
				JMP(LabelRef(name + "_copy_fused_long"))
			}

			// 1 extra length bytes
			Label(name + "_copy_3_1_extra")
			{
				if o.inputMargin < 4+1 {
					ADDQ(U8(1), src)
					testSrc(nil)
					MOVBLZX(Mem{Base: src, Disp: -1}, length.As32())
				} else {
					MOVBLZX(Mem{Base: src, Disp: 0}, length.As32())
					ADDQ(U8(1), src)
				}
				LEAL(Mem{Base: length.As32(), Disp: 64}, length.As32())
				JMP(LabelRef(name + "_copy_fused_long"))
			}
			Label(name + "_copy_3_0_extra")
			{
				LEAL(Mem{Base: length.As32(), Disp: 4}, length.As32())
				if !alwaysLits {
					TESTL(lits.As32(), lits.As32())
					JZ(LabelRef(name + "_copy_short_no_ol"))
				}
				// Add 1-3 lits before copy
				if o.inputMargin < 3+4+4 || o.outputMargin < 4 {
					testSrcDst(lits, 0)
					o.genMemMoveVeryShort(name+"_copy3s_fused_lits", dst, src, lits, LabelRef(name+"_copy3s_fused_lits_done"), 4)
					Label(name + "_copy3s_fused_lits_done")
				} else {
					tmp := GP64()
					MOVL(Mem{Base: src, Disp: 0}, tmp.As32())
					MOVL(tmp.As32(), Mem{Base: dst, Disp: 0})
				}
				ADDQ(lits, src)
				ADDQ(lits, dst)
				ADDQ(lits, dstPos)
				JMP(LabelRef(name + "_copy_short_no_ol"))
			}
		}

		// Long Copy3 with maybe fused lits..
		Label(name + "_copy_fused_long")
		{
			// Branchless slightly faster.
			if !alwaysLits {
				TESTL(lits.As32(), lits.As32())
				JZ(LabelRef(name + "_copy_exec_long_long"))
			}

			// Add 1-3 lits before copy
			if o.inputMargin < 3+4+4 || o.outputMargin < 4 {
				testSrcDst(lits, 0)
				o.genMemMoveVeryShort(name+"_copy3_fused_lits", dst, src, lits, LabelRef(name+"_copy3_fused_lits_done"), 4)
				Label(name + "_copy3_fused_lits_done")
			} else {
				tmp := GP64()
				MOVL(Mem{Base: src, Disp: 0}, tmp.As32())
				MOVL(tmp.As32(), Mem{Base: dst, Disp: 0})
			}
			ADDQ(lits, src)
			ADDQ(lits, dst)
			ADDQ(lits, dstPos)
			JMP(LabelRef(name + "_copy_exec_long_long"))
		}
	}
	// Length always < 64
	copySrc := GP64()
	PCALIGN(16)
	Label(name + "_copy_exec_short")
	{
		CMPL(offset.As32(), dstPos.As32())
		JA(LabelRef("corrupt"))
		testDstRel(length)
		if prefetch {
			Comment("Prefetch next tag")
			MOVBQZX(Mem{Base: src}, tag)
		}
		// Create source pointer with offset
		MOVQ(dst, copySrc)
		SUBQ(offset, copySrc)
		CMPL(offset.As32(), length.As32())
		JB(LabelRef(name + "_copy_overlap"))
		JMP(LabelRef(name + "_copy_short"))
	}
	o.outputMargin -= 4

	// 64 offset, 64 length
	PCALIGN(16)
	Label(name + "_copy_exec_long_long")
	{
		MOVQ(dst, copySrc)
		SUBQ(offset, copySrc)
		CMPL(offset.As32(), dstPos.As32())
		JA(LabelRef("corrupt"))
		testDstRel(length)
		if prefetch {
			Comment("Prefetch next tag")
			MOVBQZX(Mem{Base: src}, tag)
		}
		o.genMemMoveLong64(name+"_copy_long_long", dst, copySrc, length, LabelRef(name+"_copy_done"))
	}

	// length 4 -> 64, no overlap
	// Very hot (16 byte copy mainly)
	PCALIGN(16)
	Label(name + "_copy_short_no_ol")
	{
		// Create source pointer with offset
		MOVQ(dst, copySrc)
		SUBQ(offset, copySrc)
		CMPL(offset.As32(), dstPos.As32())
		JA(LabelRef("corrupt"))
		testDstRel(length)
		if prefetch {
			Comment("Prefetch next tag")
			MOVBQZX(Mem{Base: src}, tag)
		}
		o.genMemMoveShort(name+"_copy_short_no_ol", dst, copySrc, length, LabelRef(name+"_copy_done"), 4)
	}
	// Offset anything, length anything
	PCALIGN(16)
	Label(name + "_copy_exec")
	{
		CMPL(offset.As32(), dstPos.As32())
		JA(LabelRef("corrupt"))
		testDstRel(length)
		// Create source pointer with offset
		MOVQ(dst, copySrc)
		SUBQ(offset, copySrc)
		if prefetch {
			Comment("Prefetch next tag")
			MOVBQZX(Mem{Base: src}, tag)
		}
		CMPL(offset.As32(), length.As32())
		JB(LabelRef(name + "_copy_overlap"))
		CMPL(length.As32(), U8(64))
		JA(LabelRef(name + "_copy_long"))

		Label(name + "_copy_short")
		{
			o.genMemMoveShort(name+"_copy_short", dst, copySrc, length, LabelRef(name+"_copy_done"), 1)
		}
		Label(name + "_copy_long")
		{
			o.genMemMoveLong(name+"_copy_long", dst, copySrc, length, LabelRef(name+"_copy_done"))
		}
		o.outputMargin += 4
		Label(name + "_copy_done")
		{
			ADDQ(length, dst)
			ADDQ(length, dstPos)
			loopFinished()
		}
		Label(name + "_copy_overlap")
		{
			if o.inputMargin < 4 {
				if false {
					// Since overlaps are usually of shorter length,
					// This branch is not worth checking.
					// Use REP MOVSB for longer copies..
					CMPQ(length, U8(16))
					JAE(LabelRef(name + "_copy_overlap_repmovsb"))
					Label(name + "_copy_overlap_repmovsb")
					{
						MOVQ(copySrc, reg.RSI)
						MOVQ(dst, reg.RDI)
						MOVQ(length, reg.RCX)
						ADDQ(length, dstPos)
						Instruction(&ir.Instruction{Opcode: "REP"})
						Instruction(&ir.Instruction{Opcode: "MOVSB"})
						MOVQ(reg.RDI, dst)
						loopFinished()
					}
				}
				{
					tmp := GP64()
					ADDQ(length, dstPos)
					Label(name + "_copy_overlap_simple")
					MOVB(Mem{Base: copySrc}, tmp.As8())
					MOVB(tmp.As8(), Mem{Base: dst})
					INCQ(copySrc)
					INCQ(dst)
					DECQ(length)
					JNZ(LabelRef(name + "_copy_overlap_simple"))
					loopFinished()
				}

			} else {
				// Choose optimized one for offset 1,2,3 and have one for 4+
				CMPL(offset.As32(), U8(3))
				JA(LabelRef(name + "_copy_overlap_4"))
				JE(LabelRef(name + "_copy_overlap_3"))

				// < 3
				CMPL(offset.As32(), U8(2))
				JE(LabelRef(name + "_copy_overlap_2"))
				JMP(LabelRef(name + "_copy_overlap_1"))

				Label(name + "_copy_overlap_1")
				{
					tmp := GP64()
					MOVB(Mem{Base: copySrc}, tmp.As8())
					ADDQ(length.As64(), dstPos)
					Label(name + "_loop_overlap_1")
					MOVB(tmp.As8(), Mem{Base: dst})
					INCQ(dst)
					DECQ(length)
					JNZ(LabelRef(name + "_loop_overlap_1"))
					loopFinished()
				}

				Label(name + "_copy_overlap_2")
				{
					tmp := GP64()
					MOVW(Mem{Base: copySrc}, tmp.As16())
					ADDQ(length.As64(), dstPos)
					BTL(U8(0), length.As32())
					JNC(LabelRef(name + "_loop_overlap_2"))
					MOVB(tmp.As8(), Mem{Base: dst})
					// Load with 1 offset for loop
					MOVW(Mem{Base: copySrc, Disp: 1}, tmp.As16())
					INCQ(dst)
					DECQ(length)

					Label(name + "_loop_overlap_2")
					MOVW(tmp.As16(), Mem{Base: dst})
					ADDQ(U8(2), dst)
					SUBQ(U8(2), length)
					JNZ(LabelRef(name + "_loop_overlap_2"))
					loopFinished()
				}

				Label(name + "_copy_overlap_3")
				{
					tmp := GP64()
					MOVL(Mem{Base: copySrc}, tmp.As32())
					ADDQ(length.As64(), dstPos)
					SUBQ(U8(3), length) // length = length -3
					Label(name + "_loop_overlap_3")
					MOVL(tmp.As32(), Mem{Base: dst})
					ADDQ(U8(3), dst)
					SUBQ(U8(3), length)
					JA(LabelRef(name + "_loop_overlap_3"))

					// First word. We displace the read, otherwise we read before copySrc
					MOVW(Mem{Base: copySrc, Index: length, Scale: 1, Disp: 3}, tmp.As16())
					MOVW(tmp.As16(), Mem{Base: dst, Index: length, Scale: 1})
					// Last byte
					MOVB(Mem{Base: copySrc, Index: length, Disp: 2 + 3, Scale: 1}, tmp.As8())
					MOVB(tmp.As8(), Mem{Base: dst, Index: length, Disp: 2, Scale: 1})

					// Fix up dst
					LEAQ(Mem{Base: dst, Index: length, Disp: 3, Scale: 1}, dst)

					loopFinished()
				}
				// 4 or more...
				{
					Label(name + "_copy_overlap_4")
					ADDQ(length, dstPos)
					SUBQ(U8(4), length) // Length is length - 4
					Label(name + "_loop_overlap_4")
					tmp := GP64()
					MOVL(Mem{Base: copySrc}, tmp.As32())
					ADDQ(U8(4), copySrc)
					MOVL(tmp.As32(), Mem{Base: dst})
					ADDQ(U8(4), dst)
					SUBQ(U8(4), length)
					JA(LabelRef(name + "_loop_overlap_4"))
					// Fix up final longword...
					MOVL(Mem{Base: copySrc, Index: length, Scale: 1}, tmp.As32())
					MOVL(tmp.As32(), Mem{Base: dst, Index: length, Scale: 1})
					// Fix up dst
					LEAQ(Mem{Base: dst, Index: length, Disp: 4, Scale: 1}, dst)
					loopFinished()
				}

				if false {
					// Can be used as fallback for testing.
					Label(name + "_copy_overlap_simple")
					MOVQ(copySrc, reg.RSI)
					MOVQ(dst, reg.RDI)
					MOVQ(length, reg.RCX)
					ADDQ(length, dstPos)
					Instruction(&ir.Instruction{Opcode: "REP"})
					Instruction(&ir.Instruction{Opcode: "MOVSB"})
					MOVQ(reg.RDI, dst)
					loopFinished()
				}
			}
		}
	}

	// DONE - store if last was literal if needed.
	Label(name + "_end_copy")
	Label(name + "_end_done")
}

func PCALIGN(n int) {
	Instruction(&ir.Instruction{
		Opcode:   "PCALIGN",
		Operands: []Op{Imm(uint64(n))},
	})
}
