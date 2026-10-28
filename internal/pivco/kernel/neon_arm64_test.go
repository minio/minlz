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

//go:build arm64 && !appengine && !noasm && gc && !purego

package kernel

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"unsafe"
)

// The shared tests run TierNEON through Tiers() and vecTiers(); these are NEON specific.

func TestNEONSelected(t *testing.T) {
	if Best() != TierNEON || !slices.Equal(Tiers(), []Tier{TierNEON, TierGeneric}) || TierNEON.HistUsesScratch() {
		t.Fatalf("Best() = %v, Tiers() = %v", Best(), Tiers())
	}
}

// TestNEONFlatSizes decodes every flat size up to 1100 at every depth, split as the planner
// does and as one natural op, from exact regions and with the largest first rank: the
// natural loops stop on the end of In, here the region's end, which every size reaches at
// a different point.
func TestNEONFlatSizes(t *testing.T) {
	rng := newRNG()
	for n := range 1101 {
		for d := uint8(2); d <= 8; d++ {
			region := randBytes(rng, make([]byte, (n*int(d)+7)/8))
			first := uint8(0)
			if d < 8 {
				first = uint8(min(255, R2SLen-1<<d))
			}
			for _, ops := range [][]Op{
				flatOps(n, d, first, FlagDst, 0, 0),
				{{Kind: OpNat, Flags: FlagDst, D: d, P: first, K: uint32(n)}},
			} {
				e := newEnv(rng, 0, n, region)
				e.p.Ops = ops
				if ret := decDiffExec(t, TierNEON, e, "flat"); ret != -1 {
					t.Fatalf("n=%d d=%d kind %d: Exec = %d", n, d, ops[0].Kind, ret)
				}
			}
		}
	}
}

// TestNEONMergeGuard ends the arena's accessible memory 15 bytes past the K bytes of the
// last merge child (the over-read neon_arm64.go asserts ArenaSlack to cover); the rest of
// the slack Arena claims is an inaccessible page. All-left and all-right bitmaps keep a
// cursor at its output position, where its loads reach farthest.
func TestNEONMergeGuard(t *testing.T) {
	rng := newRNG()
	for k := range 200 {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			for _, density := range []float64{0, 0.5, 1} {
				bits, kr := randBits(rng, k, density)
				bm := pageGuarded(t, len(bits), true)
				copy(bm, bits)
				ku, kl := uint32(k), k-kr
				for _, c := range []struct {
					op   Op
					alim int
				}{
					{Op{Kind: OpVV, Flags: FlagDst, K: ku, KR: uint32(kr), L: 0, R: ku}, 2 * k},
					{Op{Kind: OpVV, Flags: FlagDst, K: ku, KR: uint32(kr), L: ku, R: 0}, 2 * k},
					{Op{Kind: OpCV, Flags: FlagDst, P: 7, K: ku, KR: uint32(kr)}, k},
				} {
					mem := randBytes(rng, pageGuarded(t, c.alim+15, true))
					p := &Program{Ops: []Op{c.op}, In: bm, Dst: pageGuarded(t, k, true)}
					p.Arena = unsafe.Slice(unsafe.SliceData(mem), c.alim+ArenaSlack)
					l := p.Arena[c.op.L:][:kl]
					if c.op.Kind == OpCV {
						l = bytes.Repeat([]byte{7}, kl)
					}
					want, _ := refMerge(bm, k, l, p.Arena[c.op.R:][:kr])
					if ret := TierNEON.Exec(p); ret != -1 || !bytes.Equal(p.Dst, want) {
						t.Fatalf("density %v %+v: Exec = %d, out equal %v", density, c.op, ret, bytes.Equal(p.Dst, want))
					}
				}
			}
		})
	}
}
