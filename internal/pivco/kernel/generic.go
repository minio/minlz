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

// Ports of upstream src/pivco_huffman_primitives_scalar.h (gather, partitions) and
// src/pivco_huffman_hist_scalar.h.

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func gatherGeneric(ranks, src []byte, s2r *[256]byte) uint8 {
	ranks = ranks[:len(src)]
	var mx uint8
	for i, s := range src {
		r := s2r[s]
		ranks[i] = r
		mx = max(mx, r)
	}
	return mx
}

// The partitions store every rank to both sides and advance one cursor, so they are
// branch-free. The left store lands at nl <= j, i.e. on an already read rank, which makes
// the in-place compaction safe and keeps it inside ranks[:n]; the dead right store lands
// in right[nr:n].
func partFullGeneric(ranks []byte, n int, right, bm []byte, thr uint8) int {
	ranks, right, bm = ranks[:n], right[:n], bm[:(n+7)/8]
	nl, nr := 0, 0
	for i := range bm {
		var b byte
		for k, v := range ranks[i*8 : min(i*8+8, n)] {
			g := b2i(v > thr)
			b |= byte(g << k)
			right[nr] = v
			ranks[nl] = v
			nr += g
			nl += g ^ 1
		}
		bm[i] = b
	}
	return nr
}

func partRightGeneric(ranks []byte, n int, right, bm []byte, thr uint8) int {
	ranks, right, bm = ranks[:n], right[:n], bm[:(n+7)/8]
	nr := 0
	for i := range bm {
		var b byte
		for k, v := range ranks[i*8 : min(i*8+8, n)] {
			g := b2i(v > thr)
			b |= byte(g << k)
			right[nr] = v
			nr += g
		}
		bm[i] = b
	}
	return nr
}

func partNoneGeneric(ranks []byte, n int, bm []byte, thr uint8) int {
	ranks, bm = ranks[:n], bm[:(n+7)/8]
	nr := 0
	for i := range bm {
		var b byte
		for k, v := range ranks[i*8 : min(i*8+8, n)] {
			g := b2i(v > thr)
			b |= byte(g << k)
			nr += g
		}
		bm[i] = b
	}
	return nr
}

// histogramGeneric spreads consecutive bytes over 4 sub-histograms so that runs of one
// symbol do not serialize on a single counter. Wrapping uint32 counters are fine: h wraps
// the same way. Byte loads, as in C: taking the bytes out of 64-bit loads costs a shift
// and a zero-extension each.
func histogramGeneric(h *[256]uint32, src []byte) {
	var c [4][256]uint32
	i := 0
	for ; i+16 <= len(src); i += 16 {
		s := src[i : i+16 : i+16]
		c[0][s[0]]++
		c[1][s[1]]++
		c[2][s[2]]++
		c[3][s[3]]++
		c[0][s[4]]++
		c[1][s[5]]++
		c[2][s[6]]++
		c[3][s[7]]++
		c[0][s[8]]++
		c[1][s[9]]++
		c[2][s[10]]++
		c[3][s[11]]++
		c[0][s[12]]++
		c[1][s[13]]++
		c[2][s[14]]++
		c[3][s[15]]++
	}
	for _, b := range src[i:] {
		c[0][b]++
	}
	for i := range h {
		h[i] += c[0][i] + c[1][i] + c[2][i] + c[3][i]
	}
}
