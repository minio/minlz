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
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/minio/minlz/internal/pivco/kernel"
)

// corruptCase is a valid block and the table it was coded with.
type corruptCase struct {
	name string
	tb   *Table
	src  []byte
	blk  []byte
}

// corruptCases returns about 50 small valid blocks over tables of every record kind.
func corruptCases(t testing.TB) []corruptCase {
	t.Helper()
	c := coders(t)[0]
	var cs []corruptCase
	add := func(name string, lens [256]uint8, src []byte) {
		tb := mustTable(t, &lens)
		blk := make([]byte, blockSize(t, c, src, tb))
		if err := c.enc.Encode(blk, src, tb); err != nil {
			t.Fatal(err)
		}
		cs = append(cs, corruptCase{fmt.Sprintf("%s/n%d", name, len(src)), tb, src, blk})
	}
	for _, name := range []string{"english", "zipfian", "sparse_16", "geometric", "two_sym_skew", "proba80", "single42", "odd_sigma"} {
		h := dist(name)
		for _, n := range []int{1, 9, 100, 700, 3000} {
			add(name, lensOf(t, h), sample(h, n, uint64(n)))
		}
	}
	for i, h := range [][256]uint64{{'a': 63, 'b': 37}, {'a': 50, 'b': 32, 'c': 18}, {'a': 60, 'b': 25, 'c': 15}} {
		add(fmt.Sprint("skew", i), lensOf(t, &h), sample(&h, 3000, 9))
	}
	for _, d := range []int{2, 5, 7} {
		lens := flatLens(d, "sub")
		h := new([256]uint64)
		for s, ln := range lens {
			if ln != 0 {
				h[s] = 1
			}
		}
		add(fmt.Sprint("flat", d), lens, sample(h, 650, uint64(d)))
	}
	return cs
}

// decodeErr decodes n symbols of blk. An error must wrap ErrCorrupt and leave the guard
// bytes around dst intact.
func decodeErr(t testing.TB, c coder, blk []byte, n int, tb *Table) ([]byte, error) {
	t.Helper()
	dst, all := guarded(n, 4)
	err := c.dec.Decode(dst, blk, tb)
	if !intact(all[:4]) || !intact(all[4+n:]) {
		t.Fatalf("%v: decoder wrote outside dst (%v)", c.tier, err)
	}
	if err != nil {
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%v: unexpected error %v", c.tier, err)
		}
		return nil, err
	}
	return dst, nil
}

// canon fails unless blk, which decodes to src, is what Encode writes for src.
func canon(t testing.TB, c coder, tb *Table, blk, src []byte) {
	t.Helper()
	var h [256]uint64
	c.enc.Histogram(&h, src)
	again := make([]byte, tb.BlockSize(&h))
	if len(again) != len(blk) || c.enc.Encode(again, src, tb) != nil || !bytes.Equal(again, blk) {
		t.Fatalf("accepted a block the encoder does not write for its output\nblock %x\n  enc %x", blk, again)
	}
}

func TestCorruptTruncated(t *testing.T) {
	c := coders(t)[0]
	for _, cc := range corruptCases(t) {
		for i := range cc.blk {
			if _, err := decodeErr(t, c, cc.blk[:i], len(cc.src), cc.tb); err == nil {
				t.Fatalf("%s: truncated to %d of %d bytes decodes", cc.name, i, len(cc.blk))
			}
		}
		if _, err := decodeErr(t, c, append(slices.Clip(cc.blk), 0), len(cc.src), cc.tb); err != errTrailing {
			t.Fatalf("%s: trailing byte: %v", cc.name, err)
		}
	}
}

// TestCorruptBitFlips flips every bit of every case: the decoder must reject the result
// or decode it to symbols that encode back to the flipped block.
func TestCorruptBitFlips(t *testing.T) {
	c := coders(t)[0]
	for _, cc := range corruptCases(t) {
		b := slices.Clone(cc.blk)
		for i := range len(b) * 8 {
			b[i/8] ^= 1 << (i % 8)
			if got, err := decodeErr(t, c, b, len(cc.src), cc.tb); err == nil {
				canon(t, c, cc.tb, b, got)
			}
			b[i/8] ^= 1 << (i % 8)
		}
	}
}

// TestCorruptCount decodes blocks with other symbol counts than they hold, which the
// format does not store: the decoder must reject them or decode symbols that encode back
// to the same block.
func TestCorruptCount(t *testing.T) {
	c := coders(t)[0]
	accepted, rejected := 0, 0
	for _, cc := range corruptCases(t) {
		n := len(cc.src)
		for _, m := range []int{n - 8, n - 1, n + 1, n + 7, n + 8, n / 2, 2 * n} {
			if m < 1 || m == n {
				continue
			}
			got, err := decodeErr(t, c, cc.blk, m, cc.tb)
			if err != nil {
				rejected++
				continue
			}
			canon(t, c, cc.tb, cc.blk, got)
			accepted++
		}
	}
	// A block is only also valid for another count when the extra or missing symbols take
	// the leftmost path at every node without changing a region's size.
	if rejected == 0 {
		t.Fatalf("%d other counts accepted, %d rejected", accepted, rejected)
	}
	t.Logf("%d other counts accepted, %d rejected", accepted, rejected)
}

func TestCorruptFields(t *testing.T) {
	c := coders(t)[0]
	for _, cc := range corruptCases(t) {
		for _, f := range walkBlock(t, cc.tb, cc.blk, len(cc.src)) {
			b := slices.Clone(cc.blk)
			mutate := func(what string, want error, edit func()) {
				t.Helper()
				copy(b, cc.blk)
				edit()
				if _, err := decodeErr(t, c, b, len(cc.src), cc.tb); !errors.Is(err, want) {
					t.Fatalf("%s: %s of %c at %d: %v, want %v", cc.name, what, f.kind, f.pos, err, want)
				}
			}
			if f.kind == 'k' {
				mutate("K+1", errSplit, func() { b[f.pos], b[f.pos+1] = byte(f.k+1), byte((f.k+1)>>8) })
				mutate("0xFFFF", errSplit, func() { b[f.pos], b[f.pos+1] = 0xFF, 0xFF })
			}
			if (f.kind == 'b' || f.kind == 'f') && f.bits%8 != 0 {
				mutate("padding", errPadding, func() { b[f.pos+f.size-1] |= 0x80 })
			}
		}
	}
}

func TestCorruptShort(t *testing.T) {
	c := coders(t)[0]
	var lens [256]uint8
	lens['a'], lens['b'], lens['c'] = 1, 2, 2
	tb := mustTable(t, &lens)
	for _, tc := range []struct {
		n int
		b []byte
	}{{5, nil}, {5, []byte{5}}, {5, []byte{0, 0}}, {5, []byte{3, 0, 2}}, {MaxBlockSize, []byte{0xFF, 0xFF, 0}}} {
		if _, err := decodeErr(t, c, tc.b, tc.n, tb); err == nil {
			t.Fatalf("%x decodes", tc.b)
		}
	}
}

// TestCorruptLarge samples bit flips and truncations of maximum size blocks.
func TestCorruptLarge(t *testing.T) {
	c := coders(t)[0]
	x := xorshift64(99)
	for _, name := range []string{"english", "zipfian", "odd_sigma"} {
		h := dist(name)
		lens := lensOf(t, h)
		tb := mustTable(t, &lens)
		src := sample(h, MaxBlockSize, 3)
		blk := roundTrip(t, c, src, tb)
		b := slices.Clone(blk)
		for range 300 {
			i := int(x.next() % uint64(len(b)*8))
			b[i/8] ^= 1 << (i % 8)
			if got, err := decodeErr(t, c, b, len(src), tb); err == nil {
				canon(t, c, tb, b, got)
			}
			b[i/8] ^= 1 << (i % 8)
			if _, err := decodeErr(t, c, b[:x.next()%uint64(len(b))], len(src), tb); err == nil {
				t.Fatalf("%s: truncated block decodes", name)
			}
		}
	}
}

// TestDecodeParseFirst checks that no op runs before the whole block is parsed: a bad
// bitmap closing the first child of the root is a merge error in the full block, but
// truncation when the block is cut right after it.
func TestDecodeParseFirst(t *testing.T) {
	c := coders(t)[0]
	tested := 0
	for _, name := range testDists {
		h := dist(name)
		lens := lensOf(t, h)
		tb := mustTable(t, &lens)
		r := tb.sched[0]
		if r.Kind() != kernel.KindFull {
			continue
		}
		src := sample(h, MaxBlockSize, 5)
		blk := roundTrip(t, c, src, tb)
		kr := int(blk[0]) | int(blk[1])<<8
		first := 1
		if kr > len(src)-kr {
			first = int(r.Right)
		}
		if k := tb.sched[first].Kind(); k != kernel.KindFull && k != kernel.KindLeafLeft {
			continue
		}
		for _, f := range walkBlock(t, tb, blk, len(src)) {
			if f.kind != 'b' || f.rec != first {
				continue
			}
			b := slices.Clone(blk)
			b[f.pos] ^= 1
			if _, err := decodeErr(t, c, b, len(src), tb); err != errMerge {
				t.Fatalf("%s: %v, want %v", name, err, errMerge)
			}
			if _, err := decodeErr(t, c, b[:f.pos+f.size], len(src), tb); err != errTruncated {
				t.Fatalf("%s: cut block: %v, want %v", name, err, errTruncated)
			}
			tested++
		}
	}
	if tested == 0 {
		t.Fatal("no distribution has a merging first child under a FULL root")
	}
}

// TestCorruptSingle: a single-symbol table's bitmap must be all zeros.
func TestCorruptSingle(t *testing.T) {
	c := coders(t)[0]
	var x [256]uint8
	x['x'] = 1
	if _, err := decodeErr(t, c, []byte{0xff, 1}, 9, mustTable(t, &x)); err != errSingle {
		t.Errorf("%v, want %v", err, errSingle)
	}
}
