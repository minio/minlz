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
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/minio/minlz/internal/pivco/kernel"
)

var (
	blockSizes = []int{1, 2, 7, 8, 9, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 640, 1000, 4096, 32768, 65535}
	testDists  = []string{"uniform", "english", "zipfian", "sparse_4", "sparse_16", "geometric",
		"two_sym_eq", "two_sym_skew", "odd_sigma", "proba80", "proba50", "proba14", "proba02",
		"flat_M3", "flat_M5", "flat_M6", "flat_M7", "single0", "single42", "single255",
		"two_equal", "sym0_255"}
)

const guardByte = 0xAA

// coder is an Encoder and Decoder pinned to one kernel tier.
type coder struct {
	tier kernel.Tier
	enc  *Encoder
	dec  *Decoder
}

func coders(t testing.TB) []coder {
	var cs []coder
	for _, tier := range kernel.Tiers() {
		c := coder{tier, new(Encoder), new(Decoder)}
		c.enc.setup()
		c.dec.setup()
		c.enc.tier, c.dec.tier = tier, tier
		cs = append(cs, c)
	}
	return cs
}

// guarded returns n bytes after prefix guard bytes, followed by 64 more.
func guarded(n, prefix int) (b, all []byte) {
	all = bytes.Repeat([]byte{guardByte}, prefix+n+64)
	return all[prefix : prefix+n : prefix+n], all
}

func intact(b []byte) bool {
	for _, c := range b {
		if c != guardByte {
			return false
		}
	}
	return true
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func blockSize(t testing.TB, c coder, src []byte, tb *Table) int {
	t.Helper()
	var h [256]uint64
	c.enc.Histogram(&h, src)
	size := tb.BlockSize(&h)
	if size <= 0 {
		t.Fatalf("n=%d: BlockSize %d", len(src), size)
	}
	return size
}

// roundTrip encodes and decodes src between guard bytes, checks that nothing outside
// the block and the output is written, and returns the block.
func roundTrip(t testing.TB, c coder, src []byte, tb *Table) []byte {
	t.Helper()
	n := len(src)
	size := blockSize(t, c, src, tb)
	blk, all := guarded(size, 3)
	if err := c.enc.Encode(blk, src, tb); err != nil {
		t.Fatalf("%v n=%d: %v", c.tier, n, err)
	}
	if !intact(all[:3]) || !intact(all[3+size:]) {
		t.Fatalf("%v n=%d: encoder wrote outside its output", c.tier, n)
	}
	again := make([]byte, size)
	if err := c.enc.Encode(again, src, tb); err != nil || !bytes.Equal(again, blk) {
		t.Fatalf("%v n=%d: encoding differs between calls", c.tier, n)
	}
	got, all := guarded(n, 5)
	if err := c.dec.Decode(got, blk, tb); err != nil {
		t.Fatalf("%v n=%d: decode: %v", c.tier, n, err)
	}
	if !intact(all[:5]) || !intact(all[5+n:]) {
		t.Fatalf("%v n=%d: decoder wrote outside its output", c.tier, n)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("%v n=%d: decoded output differs", c.tier, n)
	}
	checkFields(t, tb, blk, n)
	return blk
}

// blockField is a field of a block found by walkBlock.
type blockField struct {
	kind            byte // 'k' K_right, 'b' bitmap, 'f' flat codes
	pos, size, bits int
	rec, k          int // record and its symbol count
}

// walkBlock parses a block of n symbols independently of the decoder and lists its
// fields in stream order.
func walkBlock(t testing.TB, tb *Table, b []byte, n int) []blockField {
	t.Helper()
	pos := 0
	var fs []blockField
	add := func(kind byte, size, bits, rec, k int) int {
		if pos+size > len(b) {
			t.Fatalf("field %c of record %d ends past the block", kind, rec)
		}
		fs = append(fs, blockField{kind, pos, size, bits, rec, k})
		pos += size
		return pos - size
	}
	var walk func(rec, k int)
	walk = func(rec, k int) {
		if k == 0 {
			return
		}
		r := tb.sched[rec]
		switch r.Kind() {
		case kernel.KindFlat:
			d := int(r.FlatDepth())
			add('f', (k*d+7)/8, k*d, rec, k)
			return
		case kernel.KindLeafLeft, kernel.KindFull:
			kr := int(binary.LittleEndian.Uint16(b[add('k', 2, 16, rec, k):]))
			switch {
			case kr > k:
				t.Fatalf("record %d: K_right %d > K %d", rec, kr, k)
			case r.Kind() == kernel.KindLeafLeft:
				walk(rec+1, kr)
			case kr > k-kr:
				walk(rec+int(r.Right), kr)
				walk(rec+1, k-kr)
			default:
				walk(rec+1, k-kr)
				walk(rec+int(r.Right), kr)
			}
		}
		add('b', (k+7)/8, k, rec, k)
	}
	walk(0, n)
	if pos != len(b) {
		t.Fatalf("block has %d bytes, fields end at %d", len(b), pos)
	}
	return fs
}

// checkFields checks the canonical form of a block: zero padding bits.
func checkFields(t testing.TB, tb *Table, b []byte, n int) {
	t.Helper()
	for _, f := range walkBlock(t, tb, b, n) {
		last := b[f.pos+f.size-1]
		if f.bits%8 != 0 && last>>(f.bits%8) != 0 {
			t.Fatalf("field %c at %d: bad padding %#x", f.kind, f.pos, last)
		}
	}
}

func lensOf(t testing.TB, h *[256]uint64) [256]uint8 {
	var l [256]uint8
	if !BuildLengths(h, &l) {
		t.Fatal("empty histogram")
	}
	return l
}

func TestBlockRoundTrip(t *testing.T) {
	type named struct {
		name string
		h    *[256]uint64
	}
	var ds []named
	for _, name := range testDists {
		ds = append(ds, named{name, dist(name)})
	}
	for seed := range uint64(6) {
		ds = append(ds, named{fmt.Sprint("rhist", seed), rhist(0x1234 + seed)})
	}
	cs := coders(t)
	for _, d := range ds {
		lens := lensOf(t, d.h)
		tb := mustTable(t, &lens)
		for _, n := range blockSizes {
			if testing.Short() && n > 4096 {
				continue
			}
			src := sample(d.h, n, uint64(n)*0x9E3779B97F4A7C15+1)
			for _, c := range cs {
				roundTrip(t, c, src, tb)
			}
		}
	}
}

// TestBlockSingleSymbol: the lone record's bitmap is all zeros.
func TestBlockSingleSymbol(t *testing.T) {
	cs := coders(t)
	for _, s := range []uint8{0, 42, 255} {
		for _, l := range []uint8{1, 5, 11} {
			var lens [256]uint8
			lens[s] = l
			tb := mustTable(t, &lens)
			for _, n := range []int{1, 7, 8, 9, 255, 256, 32768, 65535} {
				src := bytes.Repeat([]byte{s}, n)
				for _, c := range cs {
					if blk := roundTrip(t, c, src, tb); !bytes.Equal(blk, make([]byte, (n+7)/8)) {
						t.Fatalf("%v sym %d len %d n %d: got %x", c.tier, s, l, n, blk[:min(len(blk), 16)])
					}
				}
			}
		}
	}
}

// TestBlockSpecExamples encodes the examples of PIVCO_SPEC.md appendix A.
func TestBlockSpecExamples(t *testing.T) {
	table := func(lens map[byte]uint8) *Table {
		var l [256]uint8
		for v, n := range lens {
			l[v] = n
		}
		return mustTable(t, &l)
	}
	a1 := table(map[byte]uint8{'a': 3, 'b': 3, 'c': 3, 'd': 3, 'e': 2, 'f': 3, 'g': 3})
	if got := string(a1.r2s[:a1.nranks]); got != "abcdefg" {
		t.Fatalf("A.1 ranks %q", got)
	}
	want := []kernel.Rec{{Op: kernel.KindFull, Param: 3, Right: 2}, {Op: kernel.KindFlat | 2<<2, Param: 0},
		{Op: kernel.KindLeafLeft, Param: 4, Right: 1}, {Op: kernel.KindBoth, Param: 5}}
	if got := a1.recs(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("A.1 records %v", got)
	}
	a5 := table(map[byte]uint8{'a': 3, 'b': 3, 'c': 3, 'd': 3, 'e': 3, 'f': 3, 'g': 3, 'h': 3})
	var a5src []byte
	for v := byte('a'); v <= 'h'; v++ {
		a5src = append(a5src, bytes.Repeat([]byte{v}, 16)...)
	}
	a5blk := bytes.Repeat([]byte{0x88}, 16)
	a5blk = append(append(append(a5blk, bytes.Repeat([]byte{0xc6}, 16)...), bytes.Repeat([]byte{0xfa}, 16)...), 0)
	for _, tc := range []struct {
		name string
		t    *Table
		src  string
		blk  []byte
	}{
		{"A.2", a1, "fedcbafeedbeef", []byte{0x08, 0x00, 0x03, 0x00, 0x00, 0x85, 0x1b, 0x07, 0xc3, 0x39}},
		{"A.3 eee", a1, "eee", []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x07}},
		{"A.3 abba", a1, "abba", []byte{0x00, 0x00, 0x14, 0x00}},
		{"A.4", table(map[byte]uint8{'x': 1}), "xxxxxxxxxxx", []byte{0, 0}},
		{"A.5", a5, string(a5src) + "aa", a5blk},
	} {
		for _, c := range coders(t) {
			if got := roundTrip(t, c, []byte(tc.src), tc.t); !bytes.Equal(got, tc.blk) {
				t.Errorf("%s %v: got %x", tc.name, c.tier, got)
			}
		}
	}
}

// TestBlockDeepChain codes every symbol at the bottom of a chain of LEAF_LEFT nodes, the
// deepest stack of right ranks the encoder's scratch must hold.
func TestBlockDeepChain(t *testing.T) {
	var lens [256]uint8
	for i := range MaxCodeLen {
		lens[i] = uint8(i + 1)
	}
	lens[MaxCodeLen] = MaxCodeLen
	tb := mustTable(t, &lens)
	src := make([]byte, MaxBlockSize)
	for i := range src {
		src[i] = byte(MaxCodeLen - 1 + i%2)
	}
	for _, c := range coders(t) {
		roundTrip(t, c, src, tb)
	}
}

// TestBlockFlatRoot: 256 symbols of length 8 are one depth-8 flat, stored verbatim.
func TestBlockFlatRoot(t *testing.T) {
	var lens [256]uint8
	for i := range lens {
		lens[i] = 8
	}
	cs := coders(t)
	tb := mustTable(t, &lens)
	for _, n := range blockSizes {
		src := sample(dist("sym0_255"), n, uint64(n))
		for _, c := range cs {
			if blk := roundTrip(t, c, src, tb); !bytes.Equal(blk, src) {
				t.Fatalf("%v n %d: not stored verbatim", c.tier, n)
			}
		}
	}
}

// flatLens returns lengths with a depth-d flat subtree of 2^d symbols starting at
// symbol 16: alone at the root, as the right child of the root, or below three
// single leaves.
func flatLens(d int, where string) [256]uint8 {
	var lens [256]uint8
	top := 0
	switch where {
	case "sub":
		lens[0], top = 1, 1
	case "deep":
		lens[0], lens[1], lens[2], top = 1, 2, 3, 3
	}
	first := 16
	if d == 8 {
		first = 0
	}
	for i := range 1 << d {
		lens[first+i] = uint8(top + d)
	}
	return lens
}

func TestBlockFlatDepths(t *testing.T) {
	cs := coders(t)
	for d := 2; d <= 8; d++ {
		for _, where := range []string{"root", "sub", "deep"} {
			if d == 8 && where != "root" {
				continue
			}
			lens := flatLens(d, where)
			tb := mustTable(t, &lens)
			for _, k := range []int{0, 1, 127, 128, 129, 511, 512, 513, 639, 640, 641, 1151} {
				// Exactly k symbols of the flat, shuffled with 300 others.
				x := xorshift64(uint64(k*16+d) | 1)
				var src []byte
				for range k {
					src = append(src, byte(16*b2i(d < 8)+int(x.next()%(1<<d))))
				}
				if top := map[string]uint64{"sub": 1, "deep": 3}[where]; top > 0 {
					for range 300 {
						src = append(src, byte(x.next()%top))
					}
					for i := len(src) - 1; i > 0; i-- {
						j := x.next() % uint64(i+1)
						src[i], src[j] = src[j], src[i]
					}
				}
				if len(src) == 0 {
					continue
				}
				for _, c := range cs {
					blk := roundTrip(t, c, src, tb)
					nf := 0
					for _, f := range walkBlock(t, tb, blk, len(src)) {
						if f.kind == 'f' {
							if f.size != (k*d+7)/8 {
								t.Fatalf("D%d %s k=%d: flat region of %d bytes", d, where, k, f.size)
							}
							nf++
						}
					}
					if nf != b2i(k > 0) {
						t.Fatalf("D%d %s k=%d: %d flat regions", d, where, k, nf)
					}
				}
			}
		}
	}
}

func TestBlockErrors(t *testing.T) {
	var lens [256]uint8
	lens['a'], lens['b'], lens['c'] = 1, 2, 2
	tb := mustTable(t, &lens)
	for _, c := range coders(t) {
		for _, tc := range []struct {
			src []byte
			tb  *Table
			err error
		}{
			{nil, tb, ErrBlockSize},
			{make([]byte, MaxBlockSize+1), tb, ErrBlockSize},
			{[]byte("abc"), new(Table), ErrInvalidTable},
			{[]byte("abcd"), tb, ErrSymbolNotInTable},
			{[]byte("dabc"), tb, ErrSymbolNotInTable},
		} {
			dst, all := guarded(100, 1)
			if err := c.enc.Encode(dst, tc.src, tc.tb); err != tc.err || !intact(all) {
				t.Fatalf("%v %q: err %v, want %v", c.tier, tc.src, err, tc.err)
			}
		}
		for _, tc := range []struct {
			n   int
			tb  *Table
			err error
		}{{3, new(Table), ErrInvalidTable}, {0, tb, ErrBlockSize}, {MaxBlockSize + 1, tb, ErrBlockSize}} {
			if err := c.dec.Decode(make([]byte, tc.n), []byte{1, 0, 0, 0}, tc.tb); err != tc.err {
				t.Fatalf("decode %d symbols: %v, want %v", tc.n, err, tc.err)
			}
		}
		// A dst of the wrong size is a programming error.
		for _, size := range []int{1, 3} {
			func() {
				defer func() {
					if recover() == nil {
						t.Fatalf("dst of %d bytes for a block of 2 accepted", size)
					}
				}()
				c.enc.Encode(make([]byte, size), []byte("abc"), tb)
			}()
		}
	}
	var h [256]uint64
	h['d'] = 1
	if tb.BlockSize(&h) != -1 || new(Table).BlockSize(&h) != -1 {
		t.Fatal("BlockSize of a symbol without a code or an invalid table")
	}
}

func TestHistogram(t *testing.T) {
	src := sample(dist("english"), 100000, 1)
	var want [256]uint64
	for _, c := range src {
		want[c] += 2
	}
	for _, c := range coders(t) {
		for _, off := range []int{0, 1, 63} {
			var h [256]uint64
			for i := range want {
				h[i] = uint64(b2i(i < off))
			}
			c.enc.Histogram(&h, src)
			c.enc.Histogram(&h, src)
			for i := range h {
				if h[i] != want[i]+uint64(b2i(i < off)) {
					t.Fatalf("%v: symbol %d: %d, want %d", c.tier, i, h[i], want[i])
				}
			}
		}
		if (c.enc.hscratch != nil) != c.tier.HistUsesScratch() {
			t.Fatalf("%v: histogram scratch allocated: %v", c.tier, c.enc.hscratch != nil)
		}
	}
}

// TestConcurrentTable shares one Table between goroutines, each with its own coders.
func TestConcurrentTable(t *testing.T) {
	h := dist("english")
	lens := lensOf(t, h)
	tb := mustTable(t, &lens)
	srcs := [][]byte{sample(h, 300, 1), sample(h, 5000, 2), sample(h, 40000, 3)}
	var want [][]byte
	for _, src := range srcs {
		want = append(want, roundTrip(t, coders(t)[0], src, tb))
	}
	var wg sync.WaitGroup
	for g := range 8 {
		c := coders(t)[g%len(kernel.Tiers())]
		wg.Go(func() {
			for range 4 {
				for i, src := range srcs {
					blk := make([]byte, len(want[i]))
					if err := c.enc.Encode(blk, src, tb); err != nil || !bytes.Equal(blk, want[i]) {
						t.Errorf("%v: encode differs (%v)", c.tier, err)
						return
					}
					got := make([]byte, len(src))
					if err := c.dec.Decode(got, blk, tb); err != nil || !bytes.Equal(got, src) {
						t.Errorf("%v: decode differs (%v)", c.tier, err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
}

func BenchmarkBlock(b *testing.B) {
	for _, name := range []string{"english", "proba80", "uniform", "sparse_16"} {
		h := dist(name)
		lens := lensOf(b, h)
		tb := mustTable(b, &lens)
		src := sample(h, 32768, 1)
		for _, c := range coders(b) {
			blk := make([]byte, blockSize(b, c, src, tb))
			c.enc.Encode(blk, src, tb)
			b.Run(fmt.Sprintf("%v/enc/%s", c.tier, name), func(b *testing.B) {
				b.SetBytes(int64(len(src)))
				for b.Loop() {
					c.enc.Encode(blk, src, tb)
				}
			})
			b.Run(fmt.Sprintf("%v/dec/%s", c.tier, name), func(b *testing.B) {
				b.SetBytes(int64(len(src)))
				dst := make([]byte, len(src))
				for b.Loop() {
					if err := c.dec.Decode(dst, blk, tb); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
