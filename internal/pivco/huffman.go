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

// Code lengths from a histogram: a port of upstream pivco_build_table (src/huffman_table.c)
// with PIVCO_EFFORT_PLAIN. Every tie rule below is load-bearing for upstream's lengths.

type leaf struct {
	freq uint64
	sym  uint8
}

// BuildLengths sets lens to the Huffman code lengths of hist, limited to MaxCodeLen.
// A lone used symbol gets length 1. It returns false, with lens all zero, if hist is all zero.
// The lengths are upstream's wherever upstream is defined; it overruns an array when the
// unlimited Huffman tree is deeper than 63.
func BuildLengths(hist *[256]uint64, lens *[256]uint8) bool {
	*lens = [256]uint8{}
	var leaves [256]leaf
	var used [256]uint8 // used symbols, ascending
	n := 0
	var bits uint64
	for s, f := range hist {
		// Branch-free append: the slot is overwritten unless f != 0.
		leaves[uint8(n)] = leaf{f, uint8(s)}
		used[uint8(n)] = uint8(s)
		n += int((f | -f) >> 63)
		bits |= f
	}
	switch n {
	case 0:
		return false
	case 1:
		lens[used[0]] = 1
		return true
	}
	l := leaves[:n]
	sortLeaves(l, bits)

	// Two-queue Huffman: leaves 0..n-1 in (freq, sym) order, internal nodes appended in
	// creation order. A leaf wins ties, reproducing a heap ordered by (freq, node index).
	var nfreq [511]uint64
	var parent [511]uint16
	for i, x := range l {
		nfreq[i] = x.freq
	}
	li, ih, ni := 0, n, n
	for range n - 1 {
		var a, b int
		if li < n && (ih == ni || nfreq[li] <= nfreq[ih]) {
			a, li = li, li+1
		} else {
			a, ih = ih, ih+1
		}
		if li < n && (ih == ni || nfreq[li] <= nfreq[ih]) {
			b, li = li, li+1
		} else {
			b, ih = ih, ih+1
		}
		// Wraps like C when the frequencies sum to 2^64 or more; the tree stays complete.
		nfreq[ni] = nfreq[a] + nfreq[b]
		parent[a], parent[b] = uint16(ni), uint16(ni)
		ni++
	}
	var depth [511]uint8
	maxLen := uint8(0)
	for i := ni - 2; i >= 0; i-- {
		depth[i] = depth[parent[i]] + 1
	}
	for i, x := range l {
		lens[x.sym] = depth[i]
		maxLen = max(maxLen, depth[i])
	}
	if maxLen > MaxCodeLen {
		limitLengths(lens, used[:n])
	}
	return true
}

// sortLeaves sorts l stably by freq. bits is the OR of all frequencies.
// Any stable sort gives the same order; this mirrors C's choice for speed.
func sortLeaves(l []leaf, bits uint64) {
	if len(l) <= 40 {
		for i := 1; i < len(l); i++ {
			cur := l[i]
			j := i
			for ; j > 0 && l[j-1].freq > cur.freq; j-- {
				l[j] = l[j-1]
			}
			l[j] = cur
		}
		return
	}
	// LSD radix over the bytes present in bits. All planes are counted in one pass so
	// that runs into one bin of one plane overlap the other planes' work.
	npass := 0
	for b := bits; b != 0; b >>= 8 {
		npass++
	}
	// uint8 counts as in C: only a bin holding all 256 leaves wraps (to 0), and then
	// every cursor is 0, which is still the right scatter.
	var cnt [8][256]uint8
	for _, x := range l {
		for p := range npass {
			cnt[p][uint8(x.freq>>(8*p))]++
		}
	}
	var tmp [256]leaf
	src, dst := l, tmp[:len(l)]
	for p := range npass {
		var pos [256]uint16
		sum, dom, domCount := uint16(0), uint8(0), uint8(0)
		for k, c := range cnt[p] {
			pos[k] = sum
			sum += uint16(c)
			if c > domCount {
				dom, domCount = uint8(k), c
			}
		}
		sh := 8 * p
		if int(domCount) >= len(l)/2 {
			// A bin with most leaves keeps its cursor in a register instead of
			// chaining through pos on every leaf.
			d := pos[dom]
			for _, x := range src {
				if k := uint8(x.freq >> sh); k == dom {
					dst[d] = x
					d++
				} else {
					dst[pos[k]] = x
					pos[k]++
				}
			}
		} else {
			for _, x := range src {
				k := uint8(x.freq >> sh)
				dst[pos[k]] = x
				pos[k]++
			}
		}
		src, dst = dst, src
	}
	if &src[0] != &l[0] {
		copy(l, src)
	}
}

// limitLengths is C limit_code_lengths with max_len = MaxCodeLen: DEFLATE-style count
// adjustment, then relabeling by (clamped original length, symbol value).
// used lists the symbols with a length, ascending.
func limitLengths(lens *[256]uint8, used []uint8) {
	const m = MaxCodeLen
	// Lengths above m are counted together; C counts them per length (in an array of
	// 64, which overflows for depths above 63) and then lumps them into m.
	var c4 [4][m + 2]uint8
	i := 0
	for ; i+4 <= len(used); i += 4 {
		c4[0][min(lens[used[i]], m+1)]++
		c4[1][min(lens[used[i+1]], m+1)]++
		c4[2][min(lens[used[i+2]], m+1)]++
		c4[3][min(lens[used[i+3]], m+1)]++
	}
	for _, s := range used[i:] {
		c4[0][min(lens[s], m+1)]++
	}
	var count [m + 2]int
	for l := 1; l <= m+1; l++ {
		count[min(l, m)] += int(c4[0][l]) + int(c4[1][l]) + int(c4[2][l]) + int(c4[3][l])
	}
	// Relabel order: by clamped original length, then symbol. Its bin sizes are the
	// counts before adjustment.
	var start [m + 1]int
	sum := 0
	for l := 1; l <= m; l++ {
		start[l], sum = sum, sum+count[l]
	}

	kraft := 0
	for i := 1; i <= m; i++ {
		kraft += count[i] << (m - i)
	}
	const target = 1 << m
	for kraft > target {
		// Some length below m is in use: otherwise kraft = count[m] <= 256.
		best := m - 1
		for count[best] == 0 {
			best--
		}
		count[best]--
		count[best+1]++
		kraft -= 1 << (m - best - 1)
	}
	// C's search loop here always takes its first candidate (m -> m-1, +1).
	for kraft < target && count[m] > 0 {
		count[m]--
		count[m-1]++
		kraft++
		if kraft < target {
			if count[m] < 2 {
				break
			}
			count[m]--
			count[m-1]++
			kraft++
		}
	}

	var order [256]uint8
	for _, s := range used {
		c := min(lens[s], m)
		order[start[c]] = s
		start[c]++
	}
	si := 0
	for l := 1; l <= m; l++ {
		for range count[l] {
			lens[order[si]] = uint8(l)
			si++
		}
	}
}
