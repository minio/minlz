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
	"github.com/minio/minlz/internal/cpu"
	"github.com/minio/minlz/internal/race"
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
	case cpu.AVX512 && c.delta:
		simdExecDeltaAVX512(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], len(c.raw), &c.esc[0], n, st)
	case cpu.AVX512:
		simdExecAVX512(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], len(c.raw), &c.esc[0], n, st)
	case !cpu.AVX2:
		simdExecGo(dst, lits, c, n, st)
	case c.delta:
		simdExecDeltaAVX2(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	default:
		simdExecAVX2(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	}
}

// simdExecFlat runs operations k0 to k of c, which has the no-overlap flag, from stream positions cur.
// See simdExecFlatGo.
func simdExecFlat(dst, lits []byte, c *simdChunk, k0, k int, cur [4]int, st *[4]int) {
	tok, ll, offc, esc, n := &c.tok[k0], &c.ll[cur[0]], &c.offc[cur[1]], &c.esc[cur[2]], k-k0
	switch {
	case cpu.AVX512 && c.delta:
		simdExecFlatDeltaAVX512(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], len(c.raw), esc, n, cur[3], st)
	case cpu.AVX512:
		simdExecFlatAVX512(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], len(c.raw), esc, n, cur[3], st)
	case !cpu.AVX2:
		simdExecFlatGo(dst, lits, c, k0, k, cur, st)
	case c.delta:
		simdExecFlatDeltaAVX2(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], esc, n, cur[3], st)
	default:
		simdExecFlatAVX2(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], esc, n, cur[3], st)
	}
}

// simdTokStatsAsm returns the statistics of a prefix of tok, and its length. See simdTokStats.
func simdTokStatsAsm(tok []byte) (out, nLit, nRep, nEsc, n int, ok bool) {
	if cpu.AVX512 && len(tok) > 0 {
		out, lit, rep, esc, ok := simdTokStatsAVX512(&tok[0], len(tok))
		return out, lit >> 1, rep, esc >> 2, len(tok), ok
	}
	if n = len(tok) &^ 31; !cpu.AVX2 || n == 0 {
		return 0, 0, 0, 0, 0, true
	}
	out, lit, rep, esc, ok := simdTokStatsAVX2(&tok[0], n)
	return out, lit >> 1, rep, esc >> 2, n, ok
}

// simdLLStatsAsm returns the statistics of a prefix of ll, and its length. See simdLLStats.
func simdLLStatsAsm(ll []byte) (sum, nEsc, n int, ok bool) {
	if cpu.AVX512 && len(ll) > 0 {
		sum, nEsc, ok = simdLLStatsAVX512(&ll[0], len(ll))
		return sum, nEsc, len(ll), ok
	}
	if n = len(ll) &^ 31; !cpu.AVX2 || n == 0 {
		return 0, 0, 0, true
	}
	sum, nEsc, ok = simdLLStatsAVX2(&ll[0], n)
	return sum, nEsc, n, ok
}

// simdOffStatsAsm returns the raw bits of a prefix of offc, and its length. See simdOffStats.
func simdOffStatsAsm(offc []byte) (bits, n int, ok bool) {
	if cpu.AVX512 && len(offc) > 0 {
		sum, zeros, ok := simdOffStatsAVX512(&offc[0], len(offc))
		return sum - len(offc) + zeros, len(offc), ok
	}
	if n = len(offc) &^ 31; !cpu.AVX2 || n == 0 {
		return 0, 0, true
	}
	sum, zeros, ok := simdOffStatsAVX2(&offc[0], n)
	return sum - n + zeros, n, ok
}
