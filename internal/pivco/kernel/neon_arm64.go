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

import "github.com/minio/minlz/internal/race"

// NEON tier entry points. The kernels in neon_arm64.s are ports of upstream
// src/pivco_huffman_primitives_neon.h, pivco_huffman_neon_flat.h and pivco_huffman_neon_pack.h;
// ASIMD is part of ARMv8, so the tier needs no feature check. The encoder kernels run whole
// vector steps and the generic kernels finish the tails; the decoder runs a whole Program in
// one call. Reads of src, ranks and bitmaps and stores to ranks[:n], outputs and packed
// regions are exact, the partitions store inside right[:n] and the natural unpack reads In
// windows that may pass its region but not In. Merges load up to 15 bytes past the K bytes
// of a child, flats load 64 bytes of R2S from the first rank (128 for D=7, which the op
// checks bound). The histogram is generic, as upstream's.

const (
	_ uint = ArenaSlack - 15
	_ uint = R2SLen - (255 + 64)
)

func hasNEON() bool { return true }

// execNEONAsm runs p.Ops and returns -1, or the index of the first op that fails its checks.
//
//go:noescape
func execNEONAsm(p *Program) int

//go:noescape
func gatherNEONAsm(ranks, src *byte, n int, s2r *[256]byte) uint8

//go:noescape
func partFullNEONAsm(ranks, right, bm *byte, n int, thr uint8) int

//go:noescape
func partRightNEONAsm(ranks, right, bm *byte, n int, thr uint8) int

//go:noescape
func partNoneNEONAsm(ranks, bm *byte, n int, thr uint8) int

//go:noescape
func vertPackNEONAsm(dst, ranks *byte, n, ln int, d, base uint8)

//go:noescape
func natPackNEONAsm(dst, ranks *byte, n, dstLen int, d, base uint8) int

func gatherNEON(ranks, src []byte, s2r *[256]byte) uint8 {
	n := len(src)
	ranks = ranks[:n]
	k, mx := n&^15, uint8(0)
	if k > 0 {
		race.ReadSlice(src)
		race.ReadSlice(s2r[:])
		race.WriteSlice(ranks)
		mx = gatherNEONAsm(&ranks[0], &src[0], k, s2r)
	}
	return max(mx, gatherGeneric(ranks[k:], src[k:], s2r))
}

func partFullNEON(ranks []byte, n int, right, bm []byte, thr uint8) int {
	if n == 0 {
		return 0
	}
	ranks, right, bm = ranks[:n], right[:n+RankSlack], bm[:(n+7)/8+RankSlack]
	j, nr := n&^63, 0
	if j > 0 {
		race.WriteSlice(ranks)
		race.WriteSlice(right)
		race.WriteSlice(bm)
		nr = partFullNEONAsm(&ranks[0], &right[0], &bm[0], j, thr)
	}
	// The tail compacts its left ranks in place; they follow the j-nr vector ones.
	tr := partFullGeneric(ranks[j:], n-j, right[nr:], bm[j/8:], thr)
	copy(ranks[j-nr:], ranks[j:n-tr])
	return nr + tr
}

func partRightNEON(ranks []byte, n int, right, bm []byte, thr uint8) int {
	if n == 0 {
		return 0
	}
	ranks, right, bm = ranks[:n], right[:n+RankSlack], bm[:(n+7)/8+RankSlack]
	j, nr := n&^7, 0
	if j > 0 {
		race.ReadSlice(ranks)
		race.WriteSlice(right)
		race.WriteSlice(bm)
		nr = partRightNEONAsm(&ranks[0], &right[0], &bm[0], j, thr)
	}
	return nr + partRightGeneric(ranks[j:], n-j, right[nr:], bm[j/8:], thr)
}

func partNoneNEON(ranks []byte, n int, bm []byte, thr uint8) int {
	if n == 0 {
		return 0
	}
	ranks, bm = ranks[:n], bm[:(n+7)/8+RankSlack]
	j, nr := n&^7, 0
	if j > 0 {
		race.ReadSlice(ranks)
		race.WriteSlice(bm)
		nr = partNoneNEONAsm(&ranks[0], &bm[0], j, thr)
	}
	return nr + partNoneGeneric(ranks[j:], n-j, bm[j/8:], thr)
}

func packNEON(dst, ranks []byte, n int, d, base uint8) {
	if n == 0 || d < 2 || d > 8 {
		packGeneric(dst, ranks, n, d, base)
		return
	}
	dst, ranks = dst[:(n*int(d)+7)/8], ranks[:n]
	race.ReadSlice(ranks)
	race.WriteSlice(dst)
	n512, n128 := VertSplit(n, d)
	if n512 > 0 {
		vertPackNEONAsm(&dst[0], &ranks[0], n512, 64, d, base)
	}
	dst, ranks = dst[n512*int(d)/8:], ranks[n512:]
	if n128 > 0 {
		vertPackNEONAsm(&dst[0], &ranks[0], n128, 16, d, base)
	}
	dst, ranks = dst[n128*int(d)/8:], ranks[n128:]
	i := 0
	if len(ranks) >= 16 {
		i = natPackNEONAsm(&dst[0], &ranks[0], len(ranks), len(dst), d, base)
	}
	natPack(dst[i*int(d)/8:], ranks[i:], d, base)
}

// execNEON runs the ops in one assembly call. Like execGeneric it checks each op before
// running it, so a malformed op fails closed.
func execNEON(p *Program) int {
	if len(p.Ops) == 0 {
		return -1
	}
	if race.Enabled {
		race.ReadSlice(p.Ops)
		race.ReadSlice(p.In)
		if p.R2S != nil {
			race.ReadSlice(p.R2S[:])
		}
		race.WriteSlice(p.Arena)
		race.WriteSlice(p.Dst)
	}
	return execNEONAsm(p)
}
