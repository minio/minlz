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

// Renamed from encode_amd64.go, where the _amd64 filename suffix -- not the
// constraint below -- was what limited it to one architecture. The dispatch
// itself is architecture-neutral: arm64 now supplies the same encodeBlockAsm*
// symbols from asm_arm64.s.

//go:build (amd64 || arm64) && !appengine && !noasm && gc && !purego

package minlz

import (
	"sync"

	"github.com/minio/minlz/internal/race"
)

const hasAsm = true

// encodeBlock encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src)) &&
//	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlockFast(dst, src []byte) (d int) {
	race.ReadSlice(src)
	race.WriteSlice(dst)

	switch {
	case len(src) > 2<<20:
		const sz, pool = 65536, 5
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm(dst, src, tmp)
	case len(src) > 512<<10:
		const sz, pool = 32768, 0
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm2MB(dst, src, tmp)
	case len(src) > 64<<10:
		const sz, pool = 32768, 0
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm512K(dst, src, tmp)
	case len(src) > 16<<10:
		const sz, pool = 8192, 1
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm64K(dst, src, tmp)
	case len(src) > 4<<10:
		const sz, pool = 4096, 2
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm16K(dst, src, tmp)
	case len(src) > 1<<10:
		const sz, pool = 2048, 3
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm4K(dst, src, tmp)
	case len(src) > 32:
		const sz, pool = 1024, 4
		tmp, ok := encFastPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encFastPools[pool].Put(tmp)
		return encodeFastBlockAsm1K(dst, src, tmp)
	}
	return 0
}

// encodeBlock encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src)) &&
//	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlock(dst, src []byte) (d int) {
	race.ReadSlice(src)
	race.WriteSlice(dst)

	switch {
	case len(src) > 2<<20:
		const sz, pool = 131072, 0
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm(dst, src, tmp)
	case len(src) > 512<<10:
		const sz, pool = 131072, 0
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm2MB(dst, src, tmp)
	case len(src) > 64<<10:
		const sz, pool = 65536, 2
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm512K(dst, src, tmp)
	case len(src) > 16<<10:
		const sz, pool = 16384, 3
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm64K(dst, src, tmp)
	case len(src) > 4<<10:
		const sz, pool = 8192, 4
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm16K(dst, src, tmp)
	case len(src) > 1<<10:
		const sz, pool = 2048, 5
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm4K(dst, src, tmp)
	case len(src) > minNonLiteralBlockSize:
		const sz, pool = 1024, 6
		tmp, ok := encPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encPools[pool].Put(tmp)
		return encodeBlockAsm1K(dst, src, tmp)
	}
	return 0
}

// encodeBlockBetter encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src)) &&
//	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlockBetter(dst, src []byte) (d int) {
	race.ReadSlice(src)
	race.WriteSlice(dst)

	switch {
	case len(src) > 2<<20:
		const sz, pool = 589824, 0
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm(dst, src, tmp)
	case len(src) > 512<<10:
		const sz, pool = 589824, 0
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm2MB(dst, src, tmp)
	case len(src) > 64<<10:
		const sz, pool = 294912, 1
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm512K(dst, src, tmp)
	case len(src) > 16<<10:
		const sz, pool = 73728, 2
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm64K(dst, src, tmp)
	case len(src) > 4<<10:
		const sz, pool = 36864, 3
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm16K(dst, src, tmp)
	case len(src) > 1<<10:
		const sz, pool = 10240, 4
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm4K(dst, src, tmp)
	case len(src) > minNonLiteralBlockSize:
		const sz, pool = 4608, 5
		tmp, ok := encBetterPools[pool].Get().(*[sz]byte)
		if !ok {
			tmp = &[sz]byte{}
		}
		race.WriteSlice(tmp[:])
		defer encBetterPools[pool].Put(tmp)
		return encodeBetterBlockAsm1K(dst, src, tmp)
	}
	return 0
}

// The return codes of the assembly parsers.
const (
	simdAsmDone   = iota // the literals from pos end the block
	simdAsmFull          // the chunk is full
	simdAsmCopy          // the operation of asmBase, asmLen and asmOff is for emitCopyLits
	simdAsmRepeat        // the operation of asmBase and asmLen is for emitRepeatLits
)

// simdBetterTables64K holds the tables of encodeBetterBlockSIMDAsm64K. The other assembly parsers
// share the pools of the classic assembly encoders with tables of the same size.
var simdBetterTables64K sync.Pool

// simdAsmHook, when tests set it, sees each return of an assembly parser before Go acts on it.
var simdAsmHook func(e *simdEncoder, r int)

// parseAsm parses src with assembly parser k. It codes the chunks that the parser fills,
// and adds the operations that the parser leaves to the emitters.
func (e *simdEncoder) parseAsm(src []byte, k int) {
	switch k {
	case simdAsmFast:
		t := simdAsmTable[[32 << 10]byte](&encFastPools[0])
		e.runAsm(src, k, t[:])
		encFastPools[0].Put(t)
	case simdAsmBlock:
		t := simdAsmTable[[128 << 10]byte](&encPools[0])
		e.runAsm(src, k, t[:])
		encPools[0].Put(t)
	case simdAsmBetter:
		t := simdAsmTable[[576 << 10]byte](&encBetterPools[0])
		e.runAsm(src, k, t[:])
		encBetterPools[0].Put(t)
	case simdAsmFast64K:
		t := simdAsmTable[[16 << 10]byte](&encPools[3])
		e.runAsm(src, k, t[:])
		encPools[3].Put(t)
	case simdAsmBlock64K:
		t := simdAsmTable[[32 << 10]byte](&encFastPools[0])
		e.runAsm(src, k, t[:])
		encFastPools[0].Put(t)
	case simdAsmBetter64K:
		t := simdAsmTable[[144 << 10]byte](&simdBetterTables64K)
		e.runAsm(src, k, t[:])
		simdBetterTables64K.Put(t)
	}
}

// simdAsmTable returns a table from p, or a new one. runAsm clears it:
// a zero value of the larger tables would not fit on the stack.
func simdAsmTable[T any](p *sync.Pool) *T {
	if t, ok := p.Get().(*T); ok {
		return t
	}
	return new(T)
}

// runAsm runs assembly parser k on src with its table tab.
func (e *simdEncoder) runAsm(src []byte, k int, tab []byte) {
	race.ReadSlice(src)
	race.WriteSlice(tab)
	clear(tab)
	e.asmSite = 0
	for {
		var r int
		switch k {
		case simdAsmFast:
			r = encodeFastBlockSIMDAsm(&e.simdStreams, src, (*[32 << 10]byte)(tab))
		case simdAsmBlock:
			r = encodeBlockSIMDAsm(&e.simdStreams, src, (*[128 << 10]byte)(tab))
		case simdAsmBetter:
			r = encodeBetterBlockSIMDAsm(&e.simdStreams, src, (*[576 << 10]byte)(tab))
		case simdAsmFast64K:
			r = encodeFastBlockSIMDAsm64K(&e.simdStreams, src, (*[16 << 10]byte)(tab))
		case simdAsmBlock64K:
			r = encodeBlockSIMDAsm64K(&e.simdStreams, src, (*[32 << 10]byte)(tab))
		case simdAsmBetter64K:
			r = encodeBetterBlockSIMDAsm64K(&e.simdStreams, src, (*[144 << 10]byte)(tab))
		}
		if simdAsmHook != nil {
			simdAsmHook(e, r)
		}
		switch r {
		case simdAsmFull:
			e.endChunk()
		case simdAsmCopy:
			e.emitCopyLits(src[e.pos:e.asmBase], e.asmOff, e.asmLen)
		case simdAsmRepeat:
			e.emitRepeatLits(src[e.pos:e.asmBase], e.asmLen)
		default:
			e.emitLiterals(src[e.pos:])
			return
		}
	}
}
