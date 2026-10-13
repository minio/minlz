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
	"hash/fnv"
	"slices"
	"testing"

	"github.com/minio/minlz/internal/pivco/kernel"
)

func (t *Table) recs() []kernel.Rec { return t.sched[:t.nsched] }

// marks returns the records upstream can mark for FSE coding: all but the flats.
func (t *Table) marks() []uint8 {
	var m []uint8
	for i, r := range t.recs() {
		if r.Kind() != kernel.KindFlat {
			m = append(m, uint8(i))
		}
	}
	return m
}

func mustTable(t testing.TB, lens *[256]uint8) *Table {
	t.Helper()
	var tb Table
	if err := tb.SetLengths(lens); err != nil {
		t.Fatal(err)
	}
	checkTable(t, &tb)
	return &tb
}

// checkTable verifies the schedule semantically: walking it yields a tree whose leaf
// depths are the code lengths, with ranks in order, so the code is prefix-free and
// decodable, and all derived arrays agree.
func checkTable(t testing.TB, tb *Table) {
	t.Helper()
	if !tb.valid() {
		t.Fatal("invalid table")
	}
	recs := tb.recs()
	var leafDepth [256]int
	nleaves := 0
	addLeaf := func(rank, depth int) {
		if rank != nleaves {
			t.Fatalf("rank %d out of order, want %d", rank, nleaves)
		}
		leafDepth[rank] = depth
		nleaves++
	}
	// visit walks the subtree at record i with root depth d and returns the next record.
	var visit func(i, d int) int
	visit = func(i, d int) int {
		if i >= len(recs) || d >= MaxCodeLen {
			t.Fatalf("record %d at depth %d out of range", i, d)
		}
		r := recs[i]
		switch r.Kind() {
		case kernel.KindFlat:
			fd := int(r.FlatDepth())
			if fd < 2 || fd > 8 || r.Right != 0 || int(r.Param) != nleaves {
				t.Fatalf("bad flat %v", r)
			}
			for k := range 1 << fd {
				addLeaf(int(r.Param)+k, d+fd)
			}
			return i + 1
		case kernel.KindBoth:
			if r.Right != 0 || int(r.Param) != nleaves {
				t.Fatalf("bad both %v", r)
			}
			addLeaf(int(r.Param), d+1)
			addLeaf(int(r.Param)+1, d+1)
			return i + 1
		case kernel.KindLeafLeft:
			if r.Right != 1 || int(r.Param) != nleaves {
				t.Fatalf("bad leaf-left %v", r)
			}
			addLeaf(int(r.Param), d+1)
			return visit(i+1, d+1)
		}
		next := visit(i+1, d+1)
		if int(r.Param) != nleaves-1 || int(r.Right) != next-i {
			t.Fatalf("bad full %v at %d", r, i)
		}
		return visit(next, d+1)
	}
	if visit(0, 0) != len(recs) || nleaves != int(tb.nranks) {
		t.Fatalf("schedule covers %d leaves, nranks %d", nleaves, tb.nranks)
	}
	for i, x := range tb.r2s[tb.nranks:] {
		if x != 0 {
			t.Fatalf("r2s[%d] = %d, want zero padding", int(tb.nranks)+i, x)
		}
	}
	if len(recs) == 1 && recs[0].Kind() == kernel.KindBoth && tb.r2s[0] == tb.r2s[1] {
		s := tb.r2s[0]
		for i, l := range tb.lens {
			want := uint8(0xFF)
			if i == int(s) {
				want = 0
			}
			if (l != 0) != (i == int(s)) || tb.s2r[i] != want {
				t.Fatalf("single symbol %d: sym %d len %d s2r %d", s, i, l, tb.s2r[i])
			}
		}
		return
	}
	seen := 0
	for r, s := range tb.r2s[:tb.nranks] {
		if int(tb.lens[s]) != leafDepth[r] || int(tb.s2r[s]) != r {
			t.Fatalf("rank %d sym %d: len %d depth %d s2r %d", r, s, tb.lens[s], leafDepth[r], tb.s2r[s])
		}
		seen++
	}
	for s, l := range tb.lens {
		if l == 0 && tb.s2r[s] != 0xFF {
			t.Fatalf("unused sym %d has rank %d", s, tb.s2r[s])
		}
		if l != 0 {
			seen--
		}
	}
	if seen != 0 {
		t.Fatal("rank count != used symbols")
	}
}

func schedOf(ops ...[3]uint8) []kernel.Rec {
	var r []kernel.Rec
	for _, o := range ops {
		r = append(r, kernel.Rec{Op: o[0], Param: o[1], Right: o[2]})
	}
	return r
}

func TestTableExamples(t *testing.T) {
	const A, B, C, D, E, F, G, H = 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'
	var lens [256]uint8
	lens[A], lens[B] = 1, 3
	for s := C; s <= H; s++ {
		lens[s] = 4
	}
	tb := mustTable(t, &lens)
	if want := schedOf([3]uint8{3, 0, 1}, [3]uint8{0, 4, 2}, [3]uint8{9, 1, 0}, [3]uint8{3, 5, 1}, [3]uint8{2, 6, 0}); !slices.Equal(tb.recs(), want) {
		t.Fatalf("sched %v, want %v", tb.recs(), want)
	}
	if want := []uint8{A, C, D, E, F, B, G, H}; !bytes.Equal(tb.r2s[:tb.nranks], want) {
		t.Fatalf("r2s %v", tb.r2s[:tb.nranks])
	}

	cases := []struct {
		name  string
		lens  map[int]uint8
		sched []kernel.Rec
	}{
		{"abc", map[int]uint8{'a': 1, 'b': 2, 'c': 2}, schedOf([3]uint8{3, 0, 1}, [3]uint8{2, 1, 0})},
		{"two", map[int]uint8{0: 1, 255: 1}, schedOf([3]uint8{2, 0, 0})},
		{"flat2", map[int]uint8{'a': 2, 'b': 2, 'c': 2, 'd': 2}, schedOf([3]uint8{9, 0, 0})},
		{"single", map[int]uint8{'x': 5}, schedOf([3]uint8{2, 0, 0})},
		{"single11", map[int]uint8{0: 11}, schedOf([3]uint8{2, 0, 0})},
	}
	for _, c := range cases {
		var l [256]uint8
		for s, v := range c.lens {
			l[s] = v
		}
		tb := mustTable(t, &l)
		if !slices.Equal(tb.recs(), c.sched) || tb.lens != l {
			t.Errorf("%s: sched %v", c.name, tb.recs())
		}
	}

	for i := range lens {
		lens[i] = 8
	}
	tb = mustTable(t, &lens)
	if !slices.Equal(tb.recs(), schedOf([3]uint8{33, 0, 0})) || tb.nranks != 256 {
		t.Fatalf("256x8: %v", tb.recs())
	}
	for i := range 256 {
		if tb.r2s[i] != uint8(i) || tb.s2r[i] != uint8(i) {
			t.Fatal("256x8: not identity")
		}
	}
}

// tableTestLens returns valid and invalid length sets: lengths of lensTestHists,
// random complete codes and random junk.
func tableTestLens() [][256]uint8 {
	var out [][256]uint8
	for _, h := range lensTestHists() {
		var l [256]uint8
		BuildLengths(&h, &l)
		out = append(out, l)
	}
	r := lensRand(0xC0FFEE)
	for range 3000 {
		var l [256]uint8
		p := r.perm()
		if r.intn(4) == 0 {
			for _, s := range p[:r.intn(256)] {
				l[s] = uint8(r.intn(13))
			}
			out = append(out, l)
			continue
		}
		// Random complete code: split random leaves until there are n.
		depths := []uint8{0}
		for n := 1 + r.intn(256); len(depths) < n; {
			i := r.intn(len(depths))
			if depths[i] == MaxCodeLen {
				if i = slices.IndexFunc(depths, func(d uint8) bool { return d < MaxCodeLen }); i < 0 {
					break
				}
			}
			depths[i]++
			depths = append(depths, depths[i])
		}
		for i, d := range depths {
			l[p[i]] = d
		}
		if len(depths) == 1 {
			l[p[0]] = uint8(1 + r.intn(MaxCodeLen))
		}
		out = append(out, l)
	}
	return out
}

func tableDigest(tb *Table) []byte {
	b := []byte{byte(tb.nsched), byte(tb.nranks >> 8), byte(tb.nranks)}
	for _, r := range tb.recs() {
		b = append(b, r.Op, r.Param, r.Right)
	}
	b = append(b, tb.r2s[:tb.nranks]...)
	for s, l := range tb.lens {
		if l != 0 {
			b = append(b, tb.s2r[s])
		}
	}
	return append(b, tb.marks()...)
}

// Digest of tableDigest over tableTestLens (0xFF for rejected sets), verified against C
// pivco_build_codec_table at the commit in doc.go.
const tableTestDigest = 0x310e97615f352508

func TestTableC(t *testing.T) {
	d := fnv.New64a()
	nvalid := 0
	for _, l := range tableTestLens() {
		var tb Table
		if err := tb.SetLengths(&l); err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			d.Write([]byte{0xFF})
			continue
		}
		nvalid++
		checkTable(t, &tb)
		d.Write(tableDigest(&tb))
	}
	if got := d.Sum64(); got != tableTestDigest {
		t.Fatalf("digest %#x, want %#x (%d valid)", got, uint64(tableTestDigest), nvalid)
	}
}

func TestTableReject(t *testing.T) {
	var good [256]uint8
	good[1], good[2] = 1, 1
	cases := []struct {
		name string
		lens map[int]uint8
		err  error
	}{
		{"empty", nil, errNoSymbols},
		{"len12", map[int]uint8{0: 1, 1: 12}, errCodeLen},
		// build reads 4 bits, which make 16 an unused symbol of a complete code and 31 length 15.
		{"len16", map[int]uint8{0: 1, 1: 1, 2: 16}, errCodeLen},
		{"len31", map[int]uint8{0: 1, 1: 1, 2: 31}, errCodeLen},
		{"len255", map[int]uint8{9: 255}, errCodeLen},
		{"single12", map[int]uint8{9: 12}, errCodeLen},
		{"incomplete", map[int]uint8{0: 1, 1: 2}, errKraft},
		{"overfull", map[int]uint8{0: 1, 1: 1, 2: 1}, errKraft},
		{"overfull11", map[int]uint8{0: 1, 1: 2, 2: 2, 3: 11}, errKraft},
	}
	for _, c := range cases {
		var l [256]uint8
		for s, v := range c.lens {
			l[s] = v
		}
		tb := *mustTable(t, &good)
		if err := tb.SetLengths(&l); err != c.err || !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: SetLengths = %v, want %v", c.name, err, c.err)
		}
		if tb.valid() || tb.BlockSize(&[256]uint64{1: 1}) != -1 {
			t.Errorf("%s: table still valid", c.name)
		}
	}
}

func TestTableRebuild(t *testing.T) {
	// Reusing a Table must not leak state from a larger previous table.
	var all, small [256]uint8
	for i := range all {
		all[i] = 8
	}
	small['a'], small['b'], small['c'] = 1, 2, 2
	var tb Table
	if tb.SetLengths(&all) != nil || tb.SetLengths(&small) != nil {
		t.Fatal("build")
	}
	if fresh := mustTable(t, &small); *fresh != tb {
		t.Fatal("rebuilt table differs from a fresh one")
	}
}

func BenchmarkSetLengths(b *testing.B) {
	var lens [256]uint8
	BuildLengths(&lensTestHists()[1], &lens)
	var tb Table
	for b.Loop() {
		tb.SetLengths(&lens)
	}
}

// FuzzTable: 128 bytes of length nibbles (symbol 2i low, 2i+1 high), then a payload
// mapped onto the used symbols, which must round-trip identically on every tier.
func FuzzTable(f *testing.F) {
	for i, l := range tableTestLens()[:40] {
		var b [128]byte
		for s, v := range l {
			b[s/2] |= v << (4 * (s & 1))
		}
		f.Add(append(b[:], bytes.Repeat([]byte{byte(i)}, i)...))
	}
	cs := coders(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 128 {
			return
		}
		var lens [256]uint8
		for i, b := range data[:128] {
			lens[2*i], lens[2*i+1] = b&15, b>>4
		}
		var tb Table
		if err := tb.SetLengths(&lens); err != nil {
			if !errors.Is(err, ErrCorrupt) || tb.valid() {
				t.Fatalf("err %v, valid %v", err, tb.valid())
			}
			return
		}
		checkTable(t, &tb)
		var used []byte
		for s, l := range lens {
			if l != 0 {
				used = append(used, byte(s))
			}
		}
		src := bytes.Clone(data[128:][:min(len(data)-128, MaxBlockSize)])
		for i, c := range src {
			src[i] = used[int(c)%len(used)]
		}
		if len(src) == 0 {
			return
		}
		var ref []byte
		for i, c := range cs {
			blk := roundTrip(t, c, src, &tb)
			if i == 0 {
				ref = blk
			} else if !bytes.Equal(blk, ref) {
				t.Fatalf("%v differs from %v", c.tier, cs[0].tier)
			}
		}
	})
}
