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
	"fmt"
	"math"
	"testing"

	"github.com/minio/minlz/internal/pivco/kernel"
	"github.com/minio/minlz/internal/race"
)

// blockHists returns histograms of sampled blocks: sigma 2..256 symbols in [lo,hi) with
// skewed weights, all used, totals up to MaxBlockSize.
func blockHists(seed uint64, lo, hi int) [][256]uint64 {
	r := lensRand(seed)
	var out [][256]uint64
	for sigma := 2; sigma <= hi-lo; sigma++ {
		for _, total := range []int{sigma, 1000, 16384, MaxBlockSize} {
			if total < sigma {
				continue
			}
			p := r.perm()
			syms := p[:0]
			for _, s := range p {
				if int(s) >= lo && int(s) < hi && len(syms) < sigma {
					syms = append(syms, s)
				}
			}
			var w [256]uint64
			mode := r.intn(3)
			for i, s := range syms {
				switch mode {
				case 0:
					w[s] = 1 + 1e6/uint64(i+1)
				case 1:
					w[s] = 1 + 1e6/uint64((i+1)*(i+1))
				default:
					w[s] = 1 + 1e9>>(i/4)
				}
			}
			var h [256]uint64
			for _, c := range sample(&w, total-sigma, r.next()) {
				h[c]++
			}
			for _, s := range syms {
				h[s]++
			}
			out = append(out, h)
		}
	}
	return out
}

func shapeTestHists() [][256]uint64 {
	var out [][256]uint64
	for _, name := range testDists {
		out = append(out, *dist(name))
	}
	for seed := range uint64(300) {
		out = append(out, *rhist(seed + 1))
	}
	out = append(out, lensTestHists()...)
	return append(out, blockHists(1, 0, 256)...)
}

// schedTime is shapeCost's decode time read off the schedule Table.build made: a merge
// costs the symbols passing through it, a record jointGamma.
func schedTime(tb *Table, h *[256]uint64) uint64 {
	var cum [257]uint64
	for s, f := range h {
		if tb.lens[s] != 0 {
			cum[int(tb.s2r[s])+1] += min(f, math.MaxUint32)
		}
	}
	for r := range 256 {
		cum[r+1] += cum[r]
	}
	var walk func(rec, lo, hi int) uint64
	walk = func(rec, lo, hi int) uint64 {
		r := tb.sched[rec]
		switch r.Kind() {
		case kernel.KindFlat, kernel.KindBoth:
			return 0
		case kernel.KindLeafLeft:
			return cum[hi] - cum[lo] + walk(rec+1, lo+1, hi)
		}
		mid := int(r.Param) + 1
		return cum[hi] - cum[lo] + walk(rec+1, lo, mid) + walk(rec+int(r.Right), mid, hi)
	}
	return walk(0, 0, int(tb.nranks)) + jointGamma*uint64(tb.nsched)
}

// shapeModel returns the saturated coded bits of lens and its decode time, checking
// shapeCost against the schedule.
func shapeModel(t testing.TB, h *[256]uint64, lens *[256]uint8) (nbits, time uint64) {
	t.Helper()
	var cnt [MaxCodeLen + 1]uint16
	for s, l := range lens {
		cnt[l]++
		nbits += min(h[s], math.MaxUint32) * uint64(l)
	}
	time = schedTime(mustTable(t, lens), h)
	if _, got := shapeCost(h, lens, &cnt); got != time {
		t.Fatalf("shapeCost time %d, schedule %d", got, time)
	}
	return nbits, time
}

// checkShape runs Shape on the BuildLengths lengths of h and checks its contract.
func checkShape(t testing.TB, s *Shaper, h *[256]uint64, lo, hi int) (lens [256]uint8, changed bool) {
	t.Helper()
	var base [256]uint8
	if !BuildLengths(h, &base) {
		return base, false
	}
	lens = base
	changed = s.Shape(h, &lens, lo, hi)
	if changed == (lens == base) {
		t.Fatalf("changed %v, but lens equal %v", changed, lens == base)
	}
	if !changed {
		return lens, false
	}
	ghosts := 0
	for i, l := range lens {
		if h[i] != 0 && l == 0 {
			t.Fatalf("sym %d: count %d lost its code", i, h[i])
		}
		if h[i] == 0 && l != 0 {
			if i < lo || i >= hi {
				t.Fatalf("ghost %d outside [%d,%d)", i, lo, hi)
			}
			ghosts++
		}
	}
	if ghosts > 3 {
		t.Fatalf("%d ghosts", ghosts)
	}
	bb, tb := shapeModel(t, h, &base)
	bc, tc := shapeModel(t, h, &lens)
	if float64(bc) > 1.015*float64(bb) || float64(tc) > 0.9*float64(tb) {
		t.Fatalf("bits %d -> %d, time %d -> %d", bb, bc, tb, tc)
	}
	return lens, true
}

func TestShapeInvariants(t *testing.T) {
	var s Shaper
	hs := shapeTestHists()
	n := 0
	for i := range hs {
		if _, c := checkShape(t, &s, &hs[i], 0, 256); c {
			n++
		}
	}
	t.Logf("%d of %d reshaped", n, len(hs))
}

func TestShapeRanges(t *testing.T) {
	var s Shaper
	for _, rg := range [][2]int{{1, 34}, {1, 176}, {0, 136}} {
		hs := blockHists(uint64(rg[1]), rg[0], rg[1])
		n := 0
		for i := range hs {
			if _, c := checkShape(t, &s, &hs[i], rg[0], rg[1]); c {
				n++
			}
		}
		t.Logf("[%d,%d): %d of %d reshaped", rg[0], rg[1], n, len(hs))
	}
	// 175 symbols need a ghost, and [1,176) has none left.
	n := 0
	for _, h := range blockHists(5, 1, 176) {
		if h[0] != 0 || h[176] != 0 {
			continue
		}
		used := 0
		for _, v := range h {
			used += int(min(v, 1))
		}
		if used != 175 {
			continue
		}
		if _, c := checkShape(t, &s, &h, 1, 176); c {
			t.Fatal("reshaped without a ghost")
		}
		if l, c := checkShape(t, &s, &h, 1, 177); c {
			if l[176] == 0 {
				t.Fatal("no ghost")
			}
			n++
		}
	}
	if n == 0 {
		t.Fatal("no 175-symbol histogram reshaped")
	}
}

func poisonShaper(s *Shaper, r *lensRand) {
	for i := range s.leaves {
		s.leaves[i] = leaf{r.next(), uint8(r.next())}
	}
	for i := range s.p {
		s.p[i] = math.Float64frombits(r.next())
	}
	for i := range s.dpt {
		for j := range s.dpt[i] {
			for k := range s.dpt[i][j] {
				s.dpt[i][j][k] = math.Float32frombits(uint32(r.next()))
			}
		}
	}
	for i := range s.dp0 {
		s.dp0[i] = math.Float32frombits(uint32(r.next()))
	}
	nan := float32(math.NaN())
	for i := range s.cost {
		s.cost[i] = nan
		if i%3 == 0 {
			s.cost[i] = math.Float32frombits(uint32(r.next()))
		}
	}
	for i := range s.arch {
		for j := range s.arch[i] {
			s.arch[i][j] = uint16(r.next())
		}
	}
}

// TestShapeScratch checks that stale scratch never changes a result.
func TestShapeScratch(t *testing.T) {
	var reused, poisoned Shaper
	r := lensRand(3)
	hs := shapeTestHists()
	for i := range hs {
		h := &hs[i]
		var want [256]uint8
		if !BuildLengths(h, &want) {
			continue
		}
		got1, got2 := want, want
		new(Shaper).Shape(h, &want, 0, 256)
		reused.Shape(h, &got1, 0, 256)
		poisonShaper(&poisoned, &r)
		poisoned.Shape(h, &got2, 0, 256)
		if got1 != want || got2 != want {
			t.Fatalf("hist %d: reused %v, poisoned %v", i, got1 == want, got2 == want)
		}
	}
}

func TestShapeAllocs(t *testing.T) {
	if race.Enabled {
		t.Skip("race detector allocates")
	}
	s := new(Shaper)
	var hs [][256]uint64
	var ls [][256]uint8
	for _, name := range []string{"english", "odd_sigma", "zipfian"} {
		hs = append(hs, *dist(name))
		ls = append(ls, lensOf(t, dist(name)))
	}
	checkAllocs(t, "Shape", func() {
		for i := range hs {
			l := ls[i]
			s.Shape(&hs[i], &l, 0, 256)
		}
	})
}

// FuzzShape: data[0] picks the symbol range, then (symbol, count) byte pairs; symbols wrap
// into the range, and a count byte c adds (c&31+1)<<(c>>5).
func FuzzShape(f *testing.F) {
	for i, name := range []string{"english", "zipfian", "odd_sigma", "proba14", "sym0_255"} {
		h := dist(name)
		d := []byte{byte(i)}
		for s, v := range h {
			for ; v > 0; v >>= 2 {
				d = append(d, byte(s), byte(min(v, 31)))
			}
		}
		f.Add(d)
	}
	ranges := [4][2]int{{0, 256}, {1, 34}, {1, 176}, {0, 136}}
	var s Shaper
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		lo, hi := ranges[data[0]&3][0], ranges[data[0]&3][1]
		var h [256]uint64
		for p := data[1:]; len(p) >= 2; p = p[2:] {
			h[lo+int(p[0])%(hi-lo)] += uint64(p[1]&31+1) << (p[1] >> 5)
		}
		lens, _ := checkShape(t, &s, &h, lo, hi)
		if again, _ := checkShape(t, &s, &h, lo, hi); again != lens {
			t.Fatal("not deterministic")
		}
	})
}

func BenchmarkShape(b *testing.B) {
	hs := blockHists(2, 0, 256)
	for _, tier := range [][2]int{{2, 64}, {65, 128}, {129, 256}} {
		var th [][256]uint64
		var tl [][256]uint8
		for _, h := range hs {
			n := 0
			for _, v := range h {
				n += int(min(v, 1))
			}
			if n >= tier[0] && n <= tier[1] {
				th = append(th, h)
				tl = append(tl, lensOf(b, &h))
			}
		}
		b.Run(fmt.Sprintf("sigma=%d-%d", tier[0], tier[1]), func(b *testing.B) {
			var s Shaper
			for i := 0; b.Loop(); i++ {
				l := tl[i%len(tl)]
				s.Shape(&th[i%len(th)], &l, 0, 256)
			}
		})
	}
}
