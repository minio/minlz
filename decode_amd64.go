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

//go:build amd64 && !(appengine || !gc || noasm || purego)

package minlz

import (
	"github.com/klauspost/cpuid/v2"
	"github.com/minio/minlz/internal/race"
)

// Tests turn these off to check the lower tiers.
var (
	simdAVX2   = cpuid.CPU.Supports(cpuid.AVX2, cpuid.BMI2)
	simdAVX512 = simdAVX2 && cpuid.CPU.Supports(cpuid.AVX512F, cpuid.AVX512BW, cpuid.AVX512VL, cpuid.AVX512VBMI, cpuid.AVX512VBMI2, cpuid.POPCNT)
)

// minLZDecode writes the decoding of src to dst. It assumes that the varint-encoded
// length of the decompressed bytes has already been read, and that len(dst)
// equals that length.
//
// It returns 0 on success or a decodeErrCodeXxx error code on failure.
func minLZDecode(dst, src []byte) int {
	if dst == nil {
		panic("nil dst")
	}

	race.ReadSlice(src)
	race.WriteSlice(dst)
	return decodeBlockAsm(dst, src)
}

// simdExec runs the first n operations of c. See simdExecGo.
func simdExec(dst, lits []byte, c *simdChunk, n int, st *[4]int) {
	switch {
	case simdAVX512 && c.delta:
		simdExecDeltaAVX512(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], len(c.raw), &c.esc[0], n, st)
	case simdAVX512:
		simdExecAVX512(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], len(c.raw), &c.esc[0], n, st)
	case !simdAVX2:
		simdExecGo(dst, lits, c, n, st)
	case c.delta:
		simdExecDeltaAVX2(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	default:
		simdExecAVX2(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	}
}

// simdTokStatsAsm returns the statistics of a prefix of tok, and its length. See simdTokStats.
func simdTokStatsAsm(tok []byte) (out, nLit, nRep, nEsc, n int, ok bool) {
	if simdAVX512 && len(tok) > 0 {
		out, lit, rep, esc, ok := simdTokStatsAVX512(&tok[0], len(tok))
		return out, lit >> 1, rep, esc >> 2, len(tok), ok
	}
	if n = len(tok) &^ 31; !simdAVX2 || n == 0 {
		return 0, 0, 0, 0, 0, true
	}
	out, lit, rep, esc, ok := simdTokStatsAVX2(&tok[0], n)
	return out, lit >> 1, rep, esc >> 2, n, ok
}

// simdLLStatsAsm returns the statistics of a prefix of ll, and its length. See simdLLStats.
func simdLLStatsAsm(ll []byte) (sum, nEsc, n int, ok bool) {
	if simdAVX512 && len(ll) > 0 {
		sum, nEsc, ok = simdLLStatsAVX512(&ll[0], len(ll))
		return sum, nEsc, len(ll), ok
	}
	if n = len(ll) &^ 31; !simdAVX2 || n == 0 {
		return 0, 0, 0, true
	}
	sum, nEsc, ok = simdLLStatsAVX2(&ll[0], n)
	return sum, nEsc, n, ok
}

// simdOffStatsAsm returns the raw bits of a prefix of offc, and its length. See simdOffStats.
func simdOffStatsAsm(offc []byte) (bits, n int, ok bool) {
	if simdAVX512 && len(offc) > 0 {
		sum, zeros, ok := simdOffStatsAVX512(&offc[0], len(offc))
		return sum - len(offc) + zeros, len(offc), ok
	}
	if n = len(offc) &^ 31; !simdAVX2 || n == 0 {
		return 0, 0, true
	}
	sum, zeros, ok := simdOffStatsAVX2(&offc[0], n)
	return sum - n + zeros, n, ok
}
