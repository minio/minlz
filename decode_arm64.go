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

// decodeBlockAsm here is generated: it is lowered to arm64 from the same avo
// program that produces the amd64 decoder, and it replaced a hand-written
// arm64 decoder that it outperformed. See asm_arm64.s.

//go:build arm64 && !appengine && !noasm && gc && !purego

package minlz

import (
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

// Tests turn this off to check the Go code.
var simdNEON = true

// simdExec runs the first n operations of c. See simdExecGo.
func simdExec(dst, lits []byte, c *simdChunk, n int, st *[4]int) {
	switch {
	case !simdNEON:
		simdExecGo(dst, lits, c, n, st)
	case c.delta:
		simdExecDeltaNEON(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	default:
		simdExecNEON(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	}
}

// simdExecNEON runs the first n operations of a chunk. st holds the output position,
// the literal position, the repeat offset and an error flag.
//
//go:noescape
func simdExecNEON(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)

// simdExecDeltaNEON is simdExecNEON for chunks with delta literals.
//
//go:noescape
func simdExecDeltaNEON(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)

// simdTokStatsAsm returns the statistics of a prefix of tok, and its length. See simdTokStats.
func simdTokStatsAsm(tok []byte) (out, nLit, nRep, nEsc, n int, ok bool) {
	if n = len(tok) &^ 15; !simdNEON || n == 0 {
		return 0, 0, 0, 0, 0, true
	}
	out, nLit, nRep, nEsc, ok = simdTokStatsNEON(&tok[0], n)
	return out, nLit, nRep, nEsc, n, ok
}

// simdLLStatsAsm returns the statistics of a prefix of ll, and its length. See simdLLStats.
func simdLLStatsAsm(ll []byte) (sum, nEsc, n int, ok bool) {
	if n = len(ll) &^ 15; !simdNEON || n == 0 {
		return 0, 0, 0, true
	}
	sum, nEsc, ok = simdLLStatsNEON(&ll[0], n)
	return sum, nEsc, n, ok
}

// simdOffStatsAsm returns the raw bits of a prefix of offc, and its length. See simdOffStats.
func simdOffStatsAsm(offc []byte) (bits, n int, ok bool) {
	if n = len(offc) &^ 15; !simdNEON || n == 0 {
		return 0, 0, true
	}
	bits, ok = simdOffStatsNEON(&offc[0], n)
	return bits, n, ok
}

// simdTokStatsNEON returns the token statistics of n bytes, a multiple of 16 above 0.
//
//go:noescape
func simdTokStatsNEON(b *byte, n int) (out, lit, rep, esc int, ok bool)

// simdLLStatsNEON returns the sum of n bytes, a multiple of 16 above 0, and how many are 33.
// ok is false if a byte is outside 1-33.
//
//go:noescape
func simdLLStatsNEON(b *byte, n int) (sum, esc int, ok bool)

// simdOffStatsNEON returns the raw bits of the offset symbols of n bytes, a multiple of 16 above 0.
// ok is false if a byte is outside 1-175.
//
//go:noescape
func simdOffStatsNEON(b *byte, n int) (bits int, ok bool)
