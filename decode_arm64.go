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

// simdOffTabNEON is simdOffTab laid out for simdExecNEON: the mask of the raw bits in the
// low 32 bits, the number of raw bits in bits 32-39 and the base offset in the top 24 bits.
// Symbol 0 decodes to offset 0 with no raw bits: the batched loops give it to repeats,
// which keep the last offset. The other loops ignore the symbol of a repeat.
var simdOffTabNEON = func() (t [256]uint64) {
	for s, v := range simdOffTab[1:] {
		nb := uint64(v >> 24)
		t[s+1] = 1<<nb - 1 | nb<<32 | uint64(v&0xffffff)<<40
	}
	return t
}()

// simdBatchNEON selects the batched operation loops, which expand the literal lengths and
// offset symbols of 8 operations at a time. They read up to 7 tokens past n, and 8 bytes past
// the literal lengths and offset symbols they use.
var simdBatchNEON = true

// simdExec runs the first n operations of c. See simdExecGo.
func simdExec(dst, lits []byte, c *simdChunk, n int, st *[4]int) {
	switch {
	case !cpu.NEON:
		simdExecGo(dst, lits, c, n, st)
	case simdBatchNEON && c.delta:
		simdExecBatchDeltaNEON(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	case simdBatchNEON:
		simdExecBatchNEON(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	case c.delta:
		simdExecDeltaNEON(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	default:
		simdExecNEON(&dst[0], &lits[0], &c.tok[0], &c.ll[0], &c.offc[0], &c.raw[0], &c.esc[0], n, st)
	}
}

// simdExecFlat runs operations k0 to k of c, which has the no-overlap flag, from stream positions cur.
// See simdExecFlatGo.
func simdExecFlat(dst, lits []byte, c *simdChunk, k0, k int, cur [4]int, st *[4]int) {
	tok, ll, offc, esc, n := &c.tok[k0], &c.ll[cur[0]], &c.offc[cur[1]], &c.esc[cur[2]], k-k0
	switch {
	case !cpu.NEON:
		simdExecFlatGo(dst, lits, c, k0, k, cur, st)
	case simdBatchNEON && c.delta:
		simdExecBatchFlatDeltaNEON(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], esc, n, cur[3], st)
	case simdBatchNEON:
		simdExecBatchFlatNEON(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], esc, n, cur[3], st)
	case c.delta:
		simdExecFlatDeltaNEON(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], esc, n, cur[3], st)
	default:
		simdExecFlatNEON(&dst[0], &lits[0], tok, ll, offc, &c.raw[0], esc, n, cur[3], st)
	}
}

// simdExecFlatNEON is simdExecNEON for chunks with the no-overlap flag, from bit pos of raw.
// Their matches must start at position 32 or later.
//
//go:noescape
func simdExecFlatNEON(dst, lits, tok, ll, offc, raw, esc *byte, n, pos int, st *[4]int)

// simdExecFlatDeltaNEON is simdExecFlatNEON for chunks with delta literals.
//
//go:noescape
func simdExecFlatDeltaNEON(dst, lits, tok, ll, offc, raw, esc *byte, n, pos int, st *[4]int)

// simdExecNEON runs the first n operations of a chunk. st holds the output position,
// the literal position, the repeat offset and an error flag.
//
//go:noescape
func simdExecNEON(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)

// simdExecDeltaNEON is simdExecNEON for chunks with delta literals.
//
//go:noescape
func simdExecDeltaNEON(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)

// simdExecBatchNEON is simdExecNEON with the batched loop. See simdBatchNEON.
//
//go:noescape
func simdExecBatchNEON(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)

// simdExecBatchDeltaNEON is simdExecDeltaNEON with the batched loop.
//
//go:noescape
func simdExecBatchDeltaNEON(dst, lits, tok, ll, offc, raw, esc *byte, n int, st *[4]int)

// simdExecBatchFlatNEON is simdExecFlatNEON with the batched loop.
//
//go:noescape
func simdExecBatchFlatNEON(dst, lits, tok, ll, offc, raw, esc *byte, n, pos int, st *[4]int)

// simdExecBatchFlatDeltaNEON is simdExecFlatDeltaNEON with the batched loop.
//
//go:noescape
func simdExecBatchFlatDeltaNEON(dst, lits, tok, ll, offc, raw, esc *byte, n, pos int, st *[4]int)

// simdTokStatsAsm returns the statistics of a prefix of tok, and its length. See simdTokStats.
func simdTokStatsAsm(tok []byte) (out, nLit, nRep, nEsc, n int, ok bool) {
	if n = len(tok) &^ 15; !cpu.NEON || n == 0 {
		return 0, 0, 0, 0, 0, true
	}
	out, nLit, nRep, nEsc, ok = simdTokStatsNEON(&tok[0], n)
	return out, nLit, nRep, nEsc, n, ok
}

// simdLLStatsAsm returns the statistics of a prefix of ll, and its length. See simdLLStats.
func simdLLStatsAsm(ll []byte) (sum, nEsc, n int, ok bool) {
	if n = len(ll) &^ 15; !cpu.NEON || n == 0 {
		return 0, 0, 0, true
	}
	sum, nEsc, ok = simdLLStatsNEON(&ll[0], n)
	return sum, nEsc, n, ok
}

// simdOffStatsAsm returns the raw bits of a prefix of offc, and its length. See simdOffStats.
func simdOffStatsAsm(offc []byte) (bits, n int, ok bool) {
	if n = len(offc) &^ 15; !cpu.NEON || n == 0 {
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
