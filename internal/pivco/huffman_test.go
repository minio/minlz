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
	"cmp"
	"hash/fnv"
	"slices"
	"testing"
)

// lensRand is a xorshift64 generator; test inputs must not depend on math/rand streams,
// since digests verified against C depend on them.
type lensRand uint64

func (r *lensRand) next() uint64 {
	x := uint64(*r)
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	*r = lensRand(x)
	return x
}

func (r *lensRand) intn(n int) int { return int(r.next() % uint64(n)) }

func (r *lensRand) perm() (p [256]uint8) {
	for i := range p {
		p[i] = uint8(i)
	}
	for i := 255; i > 0; i-- {
		j := r.intn(i + 1)
		p[i], p[j] = p[j], p[i]
	}
	return p
}

func fibHist(r *lensRand, k, extra int) (h [256]uint64) {
	p := r.perm()
	a, b := uint64(1), uint64(1)
	for i := range k {
		h[p[i]] = a
		a, b = b, a+b
	}
	for i := k; i < min(k+extra, 256); i++ {
		h[p[i]] = 1 + uint64(r.intn(3))
	}
	return h
}

// lensTestHists returns histograms whose totals stay below 2^40, so the Huffman depth
// stays below 64 and C (whose limiter has a [64] count array) is well defined on them.
func lensTestHists() [][256]uint64 {
	r := lensRand(0x9E3779B97F4A7C15)
	var out [][256]uint64
	for range 2000 {
		var h [256]uint64
		n := 1 + r.intn(256)
		if r.intn(4) == 0 {
			n = 1 + r.intn(8)
		}
		shift := []int{3, 8, 16, 24, 31}[r.intn(5)]
		p := r.perm()
		for _, s := range p[:n] {
			switch r.intn(3) {
			case 0:
				h[s] = 1 + r.next()%(1<<shift)
			case 1:
				h[s] = 1 + uint64(r.intn(4))
			default:
				h[s] = 1 << r.intn(shift+1)
			}
		}
		out = append(out, h)
	}
	for k := 12; k <= 56; k++ {
		for extra := 0; extra < 200; extra += 25 {
			out = append(out, fibHist(&r, k, extra))
		}
	}
	for k := 2; k <= 38; k++ {
		var h [256]uint64
		for i := range k {
			h[i*3%256] = 1 << i
		}
		out = append(out, h)
	}
	for n := 1; n <= 256; n++ {
		var h, g [256]uint64
		for i := range n {
			h[255-i] = 7
			g[i] = uint64(1 + i%3)
		}
		out = append(out, h, g)
	}
	return out
}

// Digest of BuildLengths over lensTestHists, verified against C pivco_build_table
// (src/huffman_table.c at the commit in doc.go).
const lensTestDigest = 0x9291ff8d45ccc2c1

func kraftSum(lens *[256]uint8) int {
	k := 0
	for _, l := range lens {
		if l != 0 {
			k += 1 << (MaxCodeLen - int(l))
		}
	}
	return k
}

// usedSyms returns the length of h up to its last used symbol.
func usedSyms(h *[256]uint64) int {
	k := len(h)
	for k > 0 && h[k-1] == 0 {
		k--
	}
	return k
}

func checkLengths(t *testing.T, h *[256]uint64, lens *[256]uint8) {
	t.Helper()
	used := 0
	for s, l := range lens {
		if (h[s] != 0) != (l != 0) || l > MaxCodeLen {
			t.Fatalf("sym %d: freq %d len %d", s, h[s], l)
		}
		if l != 0 {
			used++
		}
	}
	if used > 1 && kraftSum(lens) != 1<<MaxCodeLen {
		t.Fatalf("kraft %d, lens %v", kraftSum(lens), lens)
	}
}

func TestBuildLengthsC(t *testing.T) {
	d := fnv.New64a()
	for _, h := range lensTestHists() {
		var lens [256]uint8
		if !BuildLengths(h[:], &lens) {
			t.Fatal("empty")
		}
		checkLengths(t, &h, &lens)
		var short [256]uint8
		if !BuildLengths(h[:usedSyms(&h)], &short) || short != lens {
			t.Fatalf("lengths up to the last used symbol: %v, want %v", short, lens)
		}
		d.Write(lens[:])
	}
	if got := d.Sum64(); got != lensTestDigest {
		t.Fatalf("digest %#x, want %#x", got, uint64(lensTestDigest))
	}
}

func TestBuildLengthsEdges(t *testing.T) {
	var h [256]uint64
	var lens [256]uint8
	lens[3] = 9
	if BuildLengths(h[:], &lens) || lens != ([256]uint8{}) {
		t.Fatal("empty histogram accepted or lens not cleared")
	}
	h[200] = 1 << 63
	if !BuildLengths(h[:], &lens) || lens[200] != 1 || kraftSum(&lens) != 1<<10 {
		t.Fatalf("single symbol: %v", lens)
	}
	h[7] = 3
	BuildLengths(h[:], &lens)
	if lens[7] != 1 || lens[200] != 1 {
		t.Fatal("two symbols")
	}
	for i := range h {
		h[i] = 1000
	}
	BuildLengths(h[:], &lens)
	for _, l := range lens {
		if l != 8 {
			t.Fatal("uniform 256")
		}
	}
	// Equal frequencies: the leaf wins ties, so the shorter codes go to the first symbols.
	h = [256]uint64{'a': 1, 'b': 1, 'c': 1}
	BuildLengths(h[:], &lens)
	if lens['a'] != 2 || lens['b'] != 2 || lens['c'] != 1 {
		t.Fatalf("ties: %d %d %d", lens['a'], lens['b'], lens['c'])
	}
}

// TestBuildLengthsExtreme covers inputs where C is undefined (depth > 63) or the
// frequency sum wraps: BuildLengths must still return a complete code.
func TestBuildLengthsExtreme(t *testing.T) {
	r := lensRand(1)
	var hs [][256]uint64
	for k := 60; k <= 93; k++ {
		hs = append(hs, fibHist(&r, k, 0), fibHist(&r, k, 100))
	}
	for range 300 {
		var h [256]uint64
		p := r.perm()
		for _, s := range p[:2+r.intn(255)] {
			h[s] = 1<<63 + r.next()>>2
			if r.intn(3) == 0 {
				h[s] = ^uint64(0) - uint64(r.intn(5))
			}
			if r.intn(5) == 0 {
				h[s] = 1 + uint64(r.intn(3))
			}
		}
		hs = append(hs, h)
	}
	var all [256]uint64
	for i := range all {
		all[i] = ^uint64(0)
	}
	hs = append(hs, all)
	for _, h := range hs {
		var lens [256]uint8
		BuildLengths(h[:], &lens)
		checkLengths(t, &h, &lens)
	}
}

func TestSortLeaves(t *testing.T) {
	r := lensRand(7)
	for range 500 {
		n := 1 + r.intn(256)
		var bits uint64
		l := make([]leaf, n)
		mask := uint64(1)<<(1+r.intn(63)) - 1
		for i := range l {
			l[i] = leaf{1 + r.next()&mask, uint8(i)}
			if r.intn(2) == 0 {
				l[i].freq = 1 + uint64(r.intn(4))<<r.intn(56)
			}
			bits |= l[i].freq
		}
		want := slices.Clone(l)
		slices.SortStableFunc(want, func(a, b leaf) int { return cmp.Compare(a.freq, b.freq) })
		sortLeaves(l, bits)
		if !slices.Equal(l, want) {
			t.Fatalf("n=%d: sort mismatch", n)
		}
	}
}

func BenchmarkBuildLengths(b *testing.B) {
	hs := lensTestHists()[:2000]
	var lens [256]uint8
	for i := 0; b.Loop(); i++ {
		BuildLengths(hs[i%len(hs)][:], &lens)
	}
}

// FuzzLengths: data[0] picks a frequency scale, then (symbol, u16 count) triples.
func FuzzLengths(f *testing.F) {
	f.Add([]byte{0, 'a', 1, 0, 'b', 1, 0, 'c', 2, 0})
	f.Add([]byte{3, 0, 255, 255, 1, 255, 255, 2, 255, 255, 3, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		shift := []uint{0, 20, 40, 48}[data[0]&3]
		var h [256]uint64
		for p := data[1:]; len(p) >= 3; p = p[3:] {
			h[p[0]] = (uint64(p[1]) | uint64(p[2])<<8) << shift
		}
		var lens, again [256]uint8
		if !BuildLengths(h[:], &lens) {
			if h != ([256]uint64{}) {
				t.Fatal("non-empty histogram rejected")
			}
			return
		}
		checkLengths(t, &h, &lens)
		if BuildLengths(h[:], &again); again != lens {
			t.Fatal("not deterministic")
		}
		mustTable(t, &lens)
	})
}
