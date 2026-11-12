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
	"math"
	"math/bits"
	"slices"
)

// Code length shaping: a port of upstream pivco_joint_optimize_lengths (src/joint_lengths.c)
// with PIVCO_EFFORT_FASTER_DECOMPRESS and the scalar DP sweep (PIVCO_JOINT_SCALAR), under
// upstream's fixed model: lambda 0.1, gamma 170, kappa 0, mu_cst 1 and no FSE term.
// kappa 0 makes dp_take_order's take order b descending, and lambda 0.1 always meets its
// exactness condition, so it is folded away.
//
// MinLZ deviations: blocks is 1, since a table codes one block of at most MaxBlockSize
// symbols (upstream counts PIVCO_BLOCK_SIZE blocks); ghost symbols come from [lo,hi) only.
//
// Float rounding is upstream's with -ffp-contract=off: every product feeding an add is
// converted explicitly, which keeps Go from fusing it.

const (
	jointLambda = 0.1 // bits one merge pass is worth
	jointGamma  = 170 // cost of one schedule record, in merged symbols
	// Upstream's auto tier groups larger alphabets by 2 or 4, so it never solves more.
	shapeMax   = 64
	shapeW     = shapeMax/2 + 1
	shapePlane = (shapeMax + 1) * shapeW
)

// Shaper holds the scratch of Shape. It is large, so keep it with other long-lived encoder state.
type Shaper struct {
	leaves [256]leaf
	p      [257]float64                   // prefix sums of leaves[].freq
	dpt    [2][9][shapeW]float32          // [t&1][b][j]: P[k+2^b]-P[k], k = 2j+(t&1)
	dp0    [shapeMax + 1]float32          // P[k]-P[k-1]
	cost   [shapePlane]float32            // [t][j]: k = 2j+(t&1) groups placed, t-k open slots
	arch   [MaxCodeLen][shapePlane]uint16 // per level and cell: the set of flat depths taken
}

// Shape reshapes lens, the code lengths BuildLengths set for h, so that a Table built from
// them decodes faster, for at most 1.5% more coded bits. It reports whether lens changed.
// Up to 3 unused symbols may get codes, the first unused ones in [lo,hi), 0 <= lo <= hi <= 256.
// Without enough of them lens is kept.
func (s *Shaper) Shape(h *[256]uint64, lens *[256]uint8, lo, hi int) bool {
	n := 0
	var or uint64
	for i, f := range h {
		if f != 0 {
			f = min(f, math.MaxUint32)
			s.leaves[n] = leaf{f, uint8(i)}
			n++
			or |= f
		}
	}
	if n < 2 {
		return false
	}
	l := s.leaves[:n]
	sortLeaves(l, or)
	slices.Reverse(l)
	p := &s.p
	p[0] = 0
	for i, x := range l {
		p[i+1] = p[i] + float64(x.freq)
	}

	// 2^glog freq-sorted symbols at length L are a depth-glog flat, so the grouped problem
	// is the same one glog levels shallower. Ghosts pad the alphabet to whole groups.
	glog := 0
	if n > 128 {
		glog = 2
	} else if n > 64 {
		glog = 1
	}
	for c := lo; n&(1<<glog-1) != 0; c++ {
		if c >= hi {
			return false
		}
		if h[c] == 0 {
			s.leaves[n] = leaf{sym: uint8(c)}
			p[n+1] = p[n]
			n++
		}
	}
	var bl [MaxCodeLen + 1]uint16 // symbols per length; bit b set is a chunk of 2^b
	if !s.solve(n>>glog, glog, &bl) {
		return false
	}

	// The most frequent symbols get the cheapest chunks. A chunk costs L+lambda*(L-b) per
	// symbol, which orders them by length for any b <= 8. Ghosts sort last.
	var cand [256]uint8
	k := 0
	for ln := 1; ln <= MaxCodeLen; ln++ {
		for range bl[ln] {
			cand[s.leaves[k].sym] = uint8(ln)
			k++
		}
	}
	var cnt [MaxCodeLen + 1]uint16
	for _, x := range lens {
		cnt[x]++
	}
	bitsB, timeB := shapeCost(h, lens, &cnt)
	bitsC, timeC := shapeCost(h, &cand, &bl)
	if float64(timeC) > 0.90*float64(timeB) || float64(bitsC) > 1.015*float64(bitsB) {
		return false
	}
	*lens = cand
	return true
}

// shapeCost returns the coded bits and the decode time upstream's guard prices
// (realized_chunk_weights, chunk_list_time) for lens, whose per-length counts are cnt.
// Table.build splits each length's symbols, ascending, into chunks of the set bits of its
// count, largest first: symbol i of length L is in the chunk of 2^b, b = Len(cnt^i)-1,
// rooted at depth L-b. A merge costs the symbols passing through it, so each symbol pays
// once per ancestor of its chunk, and each merge and each chunk but a lone leaf is a record.
// The C sums float64s of integers below 2^53, which is exact, so integer sums are equal.
func shapeCost(h *[256]uint64, lens *[256]uint8, cnt *[MaxCodeLen + 1]uint16) (nbits, time uint64) {
	var pos [MaxCodeLen + 1]uint16
	for s, l := range lens {
		if l == 0 {
			continue
		}
		f := min(h[s], math.MaxUint32)
		nbits += f * uint64(l)
		time += f * uint64(int(l)+1-bits.Len16(cnt[l]^pos[l]))
		pos[l]++
	}
	recs := -1 // merges: one less than chunks
	for _, c := range cnt[1:] {
		recs += 2*bits.OnesCount16(c) - int(c&1)
	}
	return nbits, time + jointGamma*uint64(recs)
}

// solve is upstream solve_slot_dp on sigma <= shapeMax groups of 2^glog symbols, with
// prefix sums s.p[k<<glog]. On success it sets bl to the chosen chunks per real length.
//
// A state is k groups placed with s open slots at the current level; takes of 2^b groups
// keep t = k+s, so rows are diagonals t, and only k = t (mod 2) is stored (j = k>>1):
// lone-leaf takes, the only parity flip, are folded into the doubling to the next level.
func (s *Shaper) solve(sigma, glog int, bl *[MaxCodeLen + 1]uint16) bool {
	lmax, bcap := MaxCodeLen-glog, 8-glog
	// lambda*gamma per record: a flat take adds its own and the merge above it, a lone leaf
	// one, but a grouped lone leaf is a flat.
	const tc1 = float32(2 * jointLambda * jointGamma)
	tc0 := float32(jointLambda * jointGamma)
	if glog > 0 {
		tc0 = tc1
	}
	p := &s.p
	for par := range 2 {
		for b := 1; b <= bcap; b++ {
			for k := par; k+1<<b <= sigma; k += 2 {
				s.dpt[par][b][k>>1] = float32(p[(k+1<<b)<<glog] - p[k<<glog])
			}
		}
	}
	for k := 1; k <= sigma; k++ {
		s.dp0[k] = float32(p[k<<glog] - p[(k-1)<<glog])
	}
	// Diagonals at level L: at most 2^L slots, and enough to place the rest below.
	var tlo, thi [MaxCodeLen + 1]int
	for ln := 1; ln <= lmax; ln++ {
		h := lmax - ln
		thi[ln] = min(1<<ln, sigma)
		tlo[ln] = max((sigma+1<<h-1)>>h, 1)
	}

	w := sigma>>1 + 1
	cost := s.cost[:(sigma+1)*w]
	inf := float32(math.Inf(1))
	for i := tlo[1] * w; i < (thi[1]+1)*w; i++ {
		cost[i] = inf
	}
	cost[2*w] = 0 // level 1 entry: k = 0, s = 2
	// Every cell read below was written in this call: rows and cells are cut to the
	// capacity band, which takes and the doubling preserve.
	for ln := 1; ; ln++ {
		h := lmax - ln
		arch := s.arch[ln-1][:len(cost)]
		for t := tlo[ln]; t <= thi[ln]; t++ {
			jcap := dpRowCap(t, h, sigma)
			if jcap < 0 {
				continue
			}
			row := cost[t*w:][:jcap+1]
			prow := arch[t*w:][:jcap+1]
			clear(prow)
			for b := min(ln, bcap); b >= 1; b-- {
				step := 1 << (b - 1)
				m := jcap + 1 - step
				if m <= 0 {
					continue
				}
				a := float32(float64(ln) + float64(jointLambda*float64(ln-b)))
				src, dst := row[:m], row[step:][:m]
				psrc, pdst := prow[:m], prow[step:][:m]
				d := s.dpt[t&1][b][:m]
				bit := uint16(1) << b
				// Descending, so a dest is never a source of the same take. Upstream skips
				// infinite sources, which can never win anyway.
				for j := m - 1; j >= 0; j-- {
					if c := src[j] + float32(a*d[j]) + tc1; c < dst[j] {
						dst[j] = c
						pdst[j] = psrc[j] | bit
					}
				}
			}
		}
		if ln == lmax {
			break
		}
		// Doubling s' = 2s, in place with rows and cells descending: dest (t', k) reads
		// (t, k), t = (t'+k)/2, on rows <= t', or (t, k-1) plus a lone leaf if k and t
		// differ in parity. The one same-row read is the top cell reading itself first.
		a0 := float32(float64(ln) * (1 + jointLambda))
		for tp := thi[ln+1]; tp >= tlo[ln+1]; tp-- {
			jcap := dpRowCap(tp, h-1, sigma)
			if jcap < 0 {
				continue
			}
			pp := tp & 1
			nrow := cost[tp*w:][:jcap+1]
			t0 := (tp + pp) >> 1
			// t0 >= tlo[ln], so only the top of the source band cuts.
			jhi := min(thi[ln]-t0, jcap)
			for j := jhi + 1; j <= jcap; j++ {
				nrow[j] = inf
			}
			for j := jhi; j >= max(jhi-1, 0); j-- {
				t1, k1 := t0+j, 2*j+pp
				d := (t1 ^ k1) & 1
				si := t1*w + (k1-d-t1&1)>>1
				if d == 0 {
					for jj := j; jj >= 0; jj -= 2 {
						nrow[jj] = cost[si]
						si -= 2*w + 2
					}
					continue
				}
				// k = 0 has no lone-leaf source.
				floor := 0
				if pp == 0 && j&1 == 0 {
					floor = 2
				}
				for jj, dk := j, k1; jj >= floor; jj, dk = jj-2, dk-4 {
					nrow[jj] = cost[si] + float32(a0*s.dp0[dk]) + tc0
					si -= 2*w + 2
				}
				if floor == 2 {
					nrow[0] = inf
				}
			}
		}
	}
	if !(cost[sigma*w+sigma>>1] < inf) {
		return false
	}
	// Backtrack: an odd s at the end of a level means it took a lone leaf.
	k, sl := sigma, 0
	for ln := lmax; ln >= 1; ln-- {
		t := k + sl
		odd := sl & 1
		picks := s.arch[ln-1][t*w+(k-odd-t&1)>>1] | uint16(odd)
		bl[ln+glog] = picks << glog
		k -= int(picks)
		sl += int(picks)
		if ln > 1 {
			sl >>= 1
		}
	}
	return true
}

// dpRowCap returns the largest j on diagonal t whose k = 2j+(t&1) groups placed leave
// t-k slots, h levels above the bottom, for the other sigma-k; -1 if there is none.
func dpRowCap(t, h, sigma int) int {
	kcap := t
	if h > 0 {
		num := t<<h - sigma
		if num < 0 {
			return -1
		}
		kcap = min(num/(1<<h-1), t)
	}
	return (kcap - t&1) >> 1 // -1 for kcap 0 on odd t
}
