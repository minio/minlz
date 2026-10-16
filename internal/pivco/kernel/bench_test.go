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

package kernel

import (
	"fmt"
	"testing"

	"github.com/minio/minlz/internal/race"
)

const benchN = 32768

func forTiers(b *testing.B, f func(b *testing.B, t Tier)) {
	for _, t := range Tiers() {
		b.Run(t.String(), func(b *testing.B) { f(b, t) })
	}
}

func BenchmarkGather(b *testing.B) {
	rng := newRNG()
	src := randBytes(rng, make([]byte, benchN))
	ranks := make([]byte, benchN+RankSlack)
	var s2r [256]byte
	randBytes(rng, s2r[:])
	forTiers(b, func(b *testing.B, t Tier) {
		b.SetBytes(benchN)
		b.ReportAllocs()
		for b.Loop() {
			t.Gather(ranks, src, &s2r)
		}
	})
}

func BenchmarkPartition(b *testing.B) {
	rng := newRNG()
	src := randBytes(rng, make([]byte, benchN))
	ranks := make([]byte, benchN+RankSlack)
	right := make([]byte, benchN+RankSlack)
	bm := make([]byte, benchN/8+RankSlack)
	forTiers(b, func(b *testing.B, t Tier) {
		b.Run("full", func(b *testing.B) {
			b.SetBytes(benchN)
			b.ReportAllocs()
			for b.Loop() {
				copy(ranks, src) // PartFull consumes its input; the copy is part of the time
				t.PartFull(ranks, benchN, right, bm, 127)
			}
		})
		copy(ranks, src)
		b.Run("right", func(b *testing.B) {
			b.SetBytes(benchN)
			b.ReportAllocs()
			for b.Loop() {
				t.PartRight(ranks, benchN, right, bm, 127)
			}
		})
		b.Run("none", func(b *testing.B) {
			b.SetBytes(benchN)
			b.ReportAllocs()
			for b.Loop() {
				t.PartNone(ranks, benchN, bm, 127)
			}
		})
	})
}

func BenchmarkPack(b *testing.B) {
	src := randBytes(newRNG(), make([]byte, benchN))
	ranks := make([]byte, benchN+RankSlack)
	dst := make([]byte, benchN)
	forTiers(b, func(b *testing.B, t Tier) {
		for d := uint8(2); d <= 8; d++ {
			b.Run(fmt.Sprintf("d%d", d), func(b *testing.B) {
				for i, c := range src {
					ranks[i] = c & (1<<d - 1)
				}
				b.SetBytes(benchN)
				b.ReportAllocs()
				for b.Loop() {
					t.Pack(dst, ranks, benchN, d, 0)
				}
			})
		}
	})
}

func BenchmarkExec(b *testing.B) {
	rng := newRNG()
	bm, kr := randBits(rng, benchN, 0.5)
	in := append(bm, randBytes(rng, make([]byte, benchN))...)
	arena := randBytes(rng, make([]byte, 3*benchN+ArenaSlack))
	var r2s [R2SLen]byte
	randBytes(rng, r2s[:])
	cases := []struct {
		name string
		op   Op
	}{
		{"vv", Op{Kind: OpVV, KR: uint32(kr), L: benchN, R: 2 * benchN}},
		{"vv/inplace", Op{Kind: OpVV, KR: uint32(kr), L: 2 * benchN, R: benchN - uint32(kr)}},
		{"cv", Op{Kind: OpCV, KR: uint32(kr), P: 7, R: benchN}},
		{"cc", Op{Kind: OpCC, P: 7, Q: 9}},
		{"copy", Op{Kind: OpCopy, D: 8}},
	}
	for d := uint8(2); d <= 7; d++ {
		for _, k := range []uint8{OpV512, OpV128, OpNat} {
			name := [...]string{OpV512: "v512", OpV128: "v128", OpNat: "nat"}[k]
			cases = append(cases, struct {
				name string
				op   Op
			}{fmt.Sprintf("%s/d%d", name, d), Op{Kind: k, D: d}})
		}
	}
	forTiers(b, func(b *testing.B, t Tier) {
		for _, c := range cases {
			b.Run(c.name, func(b *testing.B) {
				ops := []Op{c.op}
				ops[0].K = benchN
				if c.op.Kind >= OpV512 {
					ops[0].Bm = uint32(len(bm))
				}
				p := Program{Ops: ops, In: in, Arena: arena, R2S: &r2s}
				b.SetBytes(benchN)
				b.ReportAllocs()
				for b.Loop() {
					if t.Exec(&p) != -1 {
						b.Fatal("exec failed")
					}
				}
			})
		}
	})
}

func BenchmarkHistogram(b *testing.B) {
	src := randBytes(newRNG(), make([]byte, benchN))
	scratch := make([]byte, HistScratch)
	forTiers(b, func(b *testing.B, t Tier) {
		var h [256]uint32
		b.SetBytes(benchN)
		b.ReportAllocs()
		for b.Loop() {
			t.Histogram(&h, src, scratch)
		}
	})
}

// TestAllocs keeps Program and the outputs as locals inside the measured function: none
// of the kernels may make them escape.
func TestAllocs(t *testing.T) {
	if race.Enabled {
		t.Skip("race detector")
	}
	rng := newRNG()
	const n = 4096
	src := randBytes(rng, make([]byte, n+RankSlack))
	codes := make([]byte, n+RankSlack)
	for i, c := range src {
		codes[i] = c & 7
	}
	bm, kr := randBits(rng, n, 0.5)
	in := append(bm, src...)
	arena := make([]byte, 3*n+ArenaSlack)
	scratch := make([]byte, HistScratch)
	for _, tier := range Tiers() {
		if a := testing.AllocsPerRun(10, func() {
			var s2r [256]byte
			var r2s [R2SLen]byte
			var ranks, right [n + RankSlack]byte
			var bmo [n/8 + RankSlack]byte
			var h [256]uint32
			tier.Gather(ranks[:], src[:n], &s2r)
			tier.PartFull(ranks[:], n, right[:], bmo[:], 100)
			tier.PartRight(ranks[:], n, right[:], bmo[:], 100)
			tier.PartNone(ranks[:], n, bmo[:], 100)
			tier.Pack(right[:], codes, n, 3, 0)
			tier.Histogram(&h, src, scratch)
			var dst [n]byte
			p := Program{
				Ops: []Op{
					{Kind: OpVV, K: n, KR: uint32(kr), L: n, R: 2 * n},
					{Kind: OpNat, D: 2, K: n, Bm: uint32(len(bm)), Flags: FlagDst},
				},
				In: in, Arena: arena, Dst: dst[:], R2S: &r2s,
			}
			if tier.Exec(&p) != -1 {
				panic("exec failed")
			}
		}); a != 0 {
			t.Errorf("%v: %v allocs", tier, a)
		}
	}
}
