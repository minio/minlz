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
	"bufio"
	"compress/gzip"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Deterministic inputs, as upstream's tests and benchmarks generate them
// (bench/bench_distributions.c, test/test_roundtrip.c), and a reader for the golden manifests.

// xorshift64 is upstream's PRNG; next returns the new state.
type xorshift64 uint64

func (x *xorshift64) next() uint64 {
	v := uint64(*x)
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = xorshift64(v)
	return v
}

func fnv1a64(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// dist returns a named histogram of upstream's test suite, or nil if the name is unknown.
func dist(name string) *[256]uint64 {
	var f [256]uint64
	fill := func(f []uint64, v uint64) {
		for i := range f {
			f[i] = v
		}
	}
	switch name {
	case "uniform":
		fill(f[:], 100)
	case "english":
		for _, e := range []struct {
			c byte
			v uint64
		}{{' ', 1830}, {'e', 1270}, {'t', 910}, {'a', 820}, {'o', 750}, {'i', 700},
			{'n', 670}, {'s', 630}, {'h', 610}, {'r', 600}, {'d', 430}, {'l', 400},
			{'c', 280}, {'u', 280}, {'m', 240}, {'w', 240}, {'f', 220}, {'g', 200},
			{'y', 200}, {'p', 190}, {'b', 150}, {'v', 100}, {'k', 80}, {'j', 15},
			{'x', 15}, {'q', 10}, {'z', 7}, {'.', 65}, {',', 61}, {'\n', 50}} {
			f[e.c] = e.v
		}
	case "zipfian":
		for i := range f {
			f[i] = max(uint64(10000.0/float64(i+1)), 1)
		}
	case "sparse_4":
		fill(f[:4], 100)
	case "sparse_16":
		fill(f[:16], 100)
	case "geometric":
		for i := range f {
			f[i] = 1 << max(30-i, 0)
		}
	case "two_sym_eq":
		f[0], f[1] = 500, 500
	case "two_sym_skew":
		f[0], f[1] = 900, 100
	case "odd_sigma":
		for i := range 71 {
			f[13+2*i] = uint64(100000 / (i + 3))
		}
	case "proba80":
		fseProba(&f, 0.80)
	case "proba50":
		fseProba(&f, 0.50)
	case "proba14":
		fseProba(&f, 0.14)
	case "proba02":
		fseProba(&f, 0.02)
	case "flat_M3", "flat_M5", "flat_M6", "flat_M7":
		fill(f[:1<<(name[6]-'0')], 100)
	case "single0":
		f[0] = 1
	case "single42":
		f[42] = 1
	case "single255":
		f[255] = 1
	case "two_equal":
		f[0], f[255] = 1, 1
	case "sym0_255":
		for i := range f {
			f[i] = uint64(i) + 1
		}
	default:
		return nil
	}
	return &f
}

// fseProba is bench_distributions.c make_fse_proba.
func fseProba(f *[256]uint64, p float64) {
	var table [2048]int
	remaining, pos, s := 2048, 0, 0
	if p < 0.01 {
		p = 0.005
	}
	p = min(p, 1)
	for remaining > 0 {
		n := max(int(float64(remaining)*p), 1)
		for end := min(pos+n, 2048); pos < end; pos++ {
			table[pos] = s
		}
		if s++; s == 255 {
			s = 0
		}
		remaining -= n
	}
	for _, v := range table {
		f[v]++
	}
}

// rhist is a random histogram, frequencies capped at 2^35.
func rhist(seed uint64) *[256]uint64 {
	const capf = 1 << 35
	var f [256]uint64
	var perm [256]uint8
	x := xorshift64(seed)
	for i := range perm {
		perm[i] = uint8(i)
	}
	for i := 255; i > 0; i-- {
		j := x.next() % uint64(i+1)
		perm[i], perm[j] = perm[j], perm[i]
	}
	nsym := 1 + int(x.next()%256)
	mode := x.next() % 4
	for i := range nsym {
		var v uint64
		switch mode {
		case 0:
			v = 1 + x.next()%1000
		case 1:
			sh := x.next() % 36
			v = 1 + x.next()%(1<<sh)
		case 2:
			v = 1 + capf>>(i%36)
		default:
			v = 1 << (x.next() % 3)
		}
		f[perm[i]] = min(v, capf)
	}
	return &f
}

// sample draws n IID symbols: r = next % total, first symbol whose cumulative frequency
// exceeds r. Totals must not wrap.
func sample(hist *[256]uint64, n int, seed uint64) []byte {
	var cum [256]uint64
	var total uint64
	for s, v := range hist {
		total += v
		cum[s] = total
	}
	x := xorshift64(seed)
	out := make([]byte, n)
	for i := range out {
		r := x.next() % total
		lo, hi := 0, 255
		for lo < hi {
			if m := (lo + hi) / 2; r < cum[m] {
				hi = m
			} else {
				lo = m + 1
			}
		}
		out[i] = byte(lo)
	}
	return out
}

// sampleLens samples with weight 1<<(11-len) per used symbol.
func sampleLens(lens *[256]uint8, n int, seed uint64) []byte {
	var w [256]uint64
	for s, l := range lens {
		if l > 0 {
			w[s] = 1 << (11 - l)
		}
	}
	return sample(&w, n, seed)
}

// record is one manifest line: its kind (first token) and key=value fields.
type record struct {
	kind string
	line int
	f    map[string]string
}

// readManifest parses a gzipped golden manifest of testdata, skipping '#' lines.
func readManifest(t testing.TB, name string) []record {
	t.Helper()
	f, err := os.Open("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var recs []record
	sc := bufio.NewScanner(zr)
	sc.Buffer(nil, 1<<20)
	for line := 1; sc.Scan(); line++ {
		s := strings.TrimRight(sc.Text(), "\r")
		if s == "" || s[0] == '#' {
			continue
		}
		tok := strings.Fields(s)
		r := record{kind: tok[0], line: line, f: map[string]string{}}
		for _, x := range tok[1:] {
			k, v, _ := strings.Cut(x, "=")
			r.f[k] = v
		}
		recs = append(recs, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return recs
}

func (r record) uint(t testing.TB, key string, base int) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(r.f[key], base, 64)
	if err != nil {
		t.Fatalf("line %d: %s: %v", r.line, key, err)
	}
	return v
}

// lenDigest returns a len:digest field.
func (r record) lenDigest(t testing.TB, key string) (int, uint64) {
	t.Helper()
	l, d, _ := strings.Cut(r.f[key], ":")
	n, err1 := strconv.Atoi(l)
	h, err2 := strconv.ParseUint(d, 16, 64)
	if err1 != nil || err2 != nil {
		t.Fatalf("line %d: %s=%q", r.line, key, r.f[key])
	}
	return n, h
}

// lens returns a lengths field: one hex nibble per symbol 0..255.
func (r record) lens(t testing.TB, key string) *[256]uint8 {
	t.Helper()
	v := r.f[key]
	var l [256]uint8
	if len(v) != len(l) {
		t.Fatalf("line %d: %d length nibbles", r.line, len(v))
	}
	for i := range l {
		d, err := strconv.ParseUint(v[i:i+1], 16, 8)
		if err != nil {
			t.Fatalf("line %d: %v", r.line, err)
		}
		l[i] = uint8(d)
	}
	return &l
}

// hist returns the histogram of a gen=dist:<name> or gen=rhist:<seed> table.
func (r record) hist(t testing.TB) *[256]uint64 {
	t.Helper()
	k, v, _ := strings.Cut(r.f["gen"], ":")
	switch k {
	case "dist":
		if h := dist(v); h != nil {
			return h
		}
	case "rhist":
		if s, err := strconv.ParseUint(v, 0, 64); err == nil {
			return rhist(s)
		}
	}
	t.Fatalf("line %d: bad histogram %q", r.line, r.f["gen"])
	return nil
}
