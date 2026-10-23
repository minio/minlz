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

package pivco

import (
	"math/bits"

	"github.com/minio/minlz/internal/pivco/kernel"
)

// Table holds code lengths and the coding tree derived from them.
// The tree is a pure function of the lengths, so encoder and decoder agree on it.
//
// The zero value is invalid. A built Table may be copied and is safe for concurrent
// use by many Encoders and Decoders; it must not be rebuilt while in use.
type Table struct {
	lens   [256]uint8
	s2r    [256]uint8           // symbol to rank; 0xFF for unused symbols when nranks < 256
	r2s    [kernel.R2SLen]uint8 // rank to symbol, zero padded
	sched  [255]kernel.Rec      // pre-order schedule
	nsched uint16               // 0 for an invalid table
	nranks uint16               // leaves; 2 for a single-symbol table
}

// SetLengths builds t from code lengths, which must be 0..MaxCodeLen and form a
// complete prefix code. A lone used symbol may have any length 1..MaxCodeLen.
// On error t is invalid.
func (t *Table) SetLengths(lens *[256]uint8) error {
	t.nsched = 0
	// build reads lengths as 4 bits.
	var or uint64
	for i := 0; i < len(lens); i += 8 {
		or |= kernel.Load64(lens[:], i)
	}
	if or&0xF0F0F0F0F0F0F0F0 != 0 {
		return errCodeLen
	}
	t.lens = *lens
	return t.build()
}

func (t *Table) valid() bool { return t.nsched != 0 }

// BlockSize returns the encoded size of a block with h, the counts of symbols 0 to len(h)-1,
// at most 256, which must sum to 1..MaxBlockSize.
// It returns -1 if t is not built or a counted symbol has no code.
func (t *Table) BlockSize(h []uint64) int {
	if !t.valid() {
		return -1
	}
	// Every record codes a range of ranks, so prefix sums of the rank counts give the
	// symbol count of each node.
	var cum [257]uint64
	lens, s2r := t.lens[:len(h)], t.s2r[:len(h)]
	for s, f := range h {
		if f != 0 {
			if lens[s] == 0 {
				return -1
			}
			cum[int(s2r[s])+1] += f
		}
	}
	c := cum[:t.nranks+1]
	for r := 1; r < len(c); r++ {
		c[r] += c[r-1]
	}
	return t.nodeSize(&cum, 0, 0, int(t.nranks))
}

// nodeSize returns the size of the regions encWalk.node writes for record rec,
// which codes the ranks lo..hi-1.
func (t *Table) nodeSize(cum *[257]uint64, rec, lo, hi int) int {
	n := int(cum[hi] - cum[lo])
	if n == 0 {
		return 0
	}
	r := t.sched[rec]
	bm := (n + 7) >> 3
	switch r.Kind() {
	case kernel.KindFlat:
		return (n*int(r.FlatDepth()) + 7) >> 3
	case kernel.KindBoth:
		return bm
	case kernel.KindLeafLeft:
		return 2 + bm + t.nodeSize(cum, rec+1, lo+1, hi)
	}
	mid := int(r.Param) + 1
	return 2 + bm + t.nodeSize(cum, rec+1, lo, mid) + t.nodeSize(cum, rec+int(r.Right), mid, hi)
}

// unusedRanks is the initial s2r: no symbol has a rank.
var unusedRanks = func() (r [256]uint8) {
	for i := range r {
		r[i] = 0xFF
	}
	return r
}()

// chunk is 2^bit same-length symbols under one subtree root at depth.
type chunk struct{ depth, bit, idx uint8 }

// build ports upstream pivco_build_decode_core and the tail of pivco_build_codec_table
// (src/huffman_table.c) on t.lens, which must all be below 16.
func (t *Table) build() error {
	// Symbols are counted and placed in 8 interleaved segments of the symbol range (C
	// uses 4 quarters): neighbouring symbols often share a length, and every counter or
	// cursor update waits for the previous one of its segment and length.
	const q = 32
	l := &t.lens
	var h [8][16]uint8
	for i := range q {
		h[0][l[i]&15]++
		h[1][l[q+i]&15]++
		h[2][l[2*q+i]&15]++
		h[3][l[3*q+i]&15]++
		h[4][l[4*q+i]&15]++
		h[5][l[5*q+i]&15]++
		h[6][l[6*q+i]&15]++
		h[7][l[7*q+i]&15]++
	}
	var count [MaxCodeLen + 1]uint16
	var base [16]uint8 // first item of each length
	n, kraft := 0, 0
	for ln := 1; ln <= MaxCodeLen; ln++ {
		c := int(h[0][ln]) + int(h[1][ln]) + int(h[2][ln]) + int(h[3][ln]) +
			int(h[4][ln]) + int(h[5][ln]) + int(h[6][ln]) + int(h[7][ln])
		base[ln] = uint8(n)
		count[ln] = uint16(c)
		n += c
		kraft += c << (MaxCodeLen - ln)
	}
	zeros := int(h[0][0]) + int(h[1][0]) + int(h[2][0]) + int(h[3][0]) +
		int(h[4][0]) + int(h[5][0]) + int(h[6][0]) + int(h[7][0])
	if n+zeros != 256 {
		return errCodeLen // lengths 12..15
	}
	if n == 0 {
		return errNoSymbols
	}
	// Cursors into items: per length in ascending symbol order, unused symbols last.
	// Segment k's cursors are base plus the counts of segments 0..k-1, added bytewise. A
	// cursor only reaches 256, and carries into the next length's, when no symbol from
	// segment k on has its length and none is longer, so only unused cursors go wrong.
	// Lane 0 sums zero counts only (at most 224); the zero cursors are set apart.
	var cur [8][16]uint8
	b0, b1, z := kernel.Load64(base[:], 0), kernel.Load64(base[:], 8), uint8(n)
	for k := range cur {
		kernel.Store64(cur[k][:], 0, b0)
		kernel.Store64(cur[k][:], 8, b1)
		cur[k][0] = z
		z += h[k][0]
		b0 += kernel.Load64(h[k][:], 0)
		b1 += kernel.Load64(h[k][:], 8)
	}
	var items [256]uint8
	for i := range q {
		a, b, c, d := l[i]&15, l[q+i]&15, l[2*q+i]&15, l[3*q+i]&15
		// One load per cursor: the compiler would reload it after the items store.
		pa, pb, pc, pd := cur[0][a], cur[1][b], cur[2][c], cur[3][d]
		items[pa], items[pb], items[pc], items[pd] = uint8(i), uint8(q+i), uint8(2*q+i), uint8(3*q+i)
		cur[0][a], cur[1][b], cur[2][c], cur[3][d] = pa+1, pb+1, pc+1, pd+1
		a, b, c, d = l[4*q+i]&15, l[5*q+i]&15, l[6*q+i]&15, l[7*q+i]&15
		pa, pb, pc, pd = cur[4][a], cur[5][b], cur[6][c], cur[7][d]
		items[pa], items[pb], items[pc], items[pd] = uint8(4*q+i), uint8(5*q+i), uint8(6*q+i), uint8(7*q+i)
		cur[4][a], cur[5][b], cur[6][c], cur[7][d] = pa+1, pb+1, pc+1, pd+1
	}

	t.s2r = unusedRanks
	if n == 1 {
		s := items[0]
		t.sched[0] = kernel.Rec{Op: kernel.KindBoth}
		t.r2s = [kernel.R2SLen]uint8{0: s, 1: s}
		t.s2r[s] = 0
		t.nranks, t.nsched = 2, 1
		return nil
	}
	if kraft != 1<<MaxCodeLen {
		return errKraft
	}

	// Each length splits into chunks by the set bits of its count, largest first,
	// taking that length's symbols in order. A count of at most 256 has at most 8 set bits.
	var chunks [MaxCodeLen * 8]chunk
	nc, idx := 0, 0
	for ln := uint8(1); ln <= MaxCodeLen; ln++ {
		for c := count[ln]; c != 0; {
			b := uint8(bits.Len16(c) - 1)
			d := ln
			if b > 0 {
				d = ln - b
			}
			chunks[nc] = chunk{d, b, uint8(idx)}
			nc++
			idx += 1 << b
			c &^= 1 << b
		}
	}
	// Stable sort by depth; generation order is already nearly sorted.
	for i := 1; i < nc; i++ {
		x := chunks[i]
		j := i
		for ; j > 0 && chunks[j-1].depth > x.depth; j-- {
			chunks[j] = chunks[j-1]
		}
		chunks[j] = x
	}

	// C dc_gen as a loop: a pre-order walk over the depth-sorted chunks, where each
	// open internal node waits on the stack for its right child. A complete code
	// guarantees that the next chunk is never shallower than the current node.
	type open struct {
		self, lrank, rrec, rrank int
		depth                    uint8
		right                    bool
	}
	var stack [MaxCodeLen]open
	sp, nrec, rank, depth := 0, 0, 0, uint8(0)
	for _, c := range chunks[:nc] {
		for ; depth < c.depth; depth++ {
			stack[sp] = open{self: nrec, lrank: rank, depth: depth}
			sp++
			nrec++
		}
		if c.bit == 0 {
			s := items[c.idx] // a leaf has no record
			t.r2s[rank], t.s2r[s] = s, uint8(rank)
			rank++
		} else {
			op := uint8(kernel.KindBoth)
			if c.bit > 1 {
				op = kernel.KindFlat | c.bit<<2
			}
			t.sched[nrec] = kernel.Rec{Op: op, Param: uint8(rank)}
			nrec++
			syms := items[c.idx:][:1<<c.bit]
			copy(t.r2s[rank:], syms)
			for i, s := range syms {
				t.s2r[s] = uint8(rank + i)
			}
			rank += len(syms)
		}
		for sp > 0 {
			f := &stack[sp-1]
			if !f.right {
				f.right, f.rrec, f.rrank = true, nrec, rank
				depth = f.depth + 1
				break
			}
			// Only a left child can be a lone leaf: depth d's only singleton chunk is
			// length d's bit-0 chunk, which sorts first among depth-d chunks.
			op := uint8(kernel.KindFull)
			if f.rrec == f.self+1 && f.rrank == f.lrank+1 {
				op = kernel.KindLeafLeft
			}
			t.sched[f.self] = kernel.Rec{Op: op, Param: uint8(f.rrank - 1), Right: uint8(f.rrec - f.self)}
			sp--
		}
	}
	clear(t.r2s[rank:])
	t.nranks, t.nsched = uint16(rank), uint16(nrec)
	return nil
}
