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
	"math/rand/v2"
	"slices"
)

// Obviously-correct references and buffer helpers shared by the kernel tests.

var testSizes = func() []int {
	s := make([]int, 0, 320)
	for n := range 301 {
		s = append(s, n)
	}
	return slices.Clip(append(s, 511, 512, 513, 639, 640, 641, 1023, 1024, 1025, 4095, 4096, 65535))
}()

const (
	guardByte = 0xAA
	guardLen  = 64
)

func newRNG() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func randBytes(rng *rand.Rand, b []byte) []byte {
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

func fill(b []byte, v byte) []byte {
	for i := range b {
		b[i] = v
	}
	return b
}

// guarded returns a copy of b with cap == len, followed in memory by guardLen guard bytes.
func guarded(b []byte) (s, all []byte) {
	all = fill(make([]byte, len(b)+guardLen), guardByte)
	copy(all, b)
	return all[:len(b):len(b)], all
}

func intact(b []byte) bool {
	return !slices.ContainsFunc(b, func(c byte) bool { return c != guardByte })
}

// randBits returns a k-bit bitmap with zero padding bits where each bit is set with
// probability p, and its popcount.
func randBits(rng *rand.Rand, k int, p float64) ([]byte, int) {
	bm := make([]byte, (k+7)/8)
	pc := 0
	for j := range k {
		if rng.Float64() < p {
			bm[j/8] |= 1 << (j % 8)
			pc++
		}
	}
	return bm, pc
}

func bit(bm []byte, j int) bool { return bm[j/8]>>(j%8)&1 != 0 }

func refPart(ranks []byte, thr uint8) (bm, left, right []byte) {
	bm = make([]byte, (len(ranks)+7)/8)
	for j, v := range ranks {
		if v > thr {
			bm[j/8] |= 1 << (j % 8)
			right = append(right, v)
		} else {
			left = append(left, v)
		}
	}
	return bm, left, right
}

// refMerge returns out[j] = bit j ? r[nr++] : l[nl++] for a k-bit bitmap, and nr.
func refMerge(bm []byte, k int, l, r []byte) ([]byte, int) {
	out := make([]byte, k)
	nl, nr := 0, 0
	for j := range out {
		if bit(bm, j) {
			out[j] = r[nr]
			nr++
		} else {
			out[j] = l[nl]
			nl++
		}
	}
	return out, nr
}

// refBitPos returns the byte and bit that hold bit t of code i in an n-code flat region
// of the vertical layout, straight from the layout description in upstream
// src/pivco_huffman_vertical.h.
func refBitPos(n, d, i, t int) (int, int) {
	n512, n128 := 0, 0
	if d >= 2 && d <= 7 {
		if n >= 512 {
			n512 = n / 512 * 512
		}
		if n-n512 >= 128 {
			n128 = (n - n512) / 128 * 128
		}
	}
	base, ln := 0, 64
	switch {
	case i < n512:
	case i < n512+n128:
		base, i, ln = n512*d/8, i-n512, 16
	default:
		pos := i*d + t // the vertical spans end on a byte boundary
		return pos / 8, pos % 8
	}
	blk, v := i/(8*ln), i%(8*ln)
	lane, pos := v%ln, v/ln*d+t
	return base + blk*ln*d + ln*(pos/8) + lane, pos % 8
}

func refPack(codes []byte, d int) []byte {
	out := make([]byte, (len(codes)*d+7)/8)
	for i, c := range codes {
		for t := range d {
			if c>>t&1 != 0 {
				by, bi := refBitPos(len(codes), d, i, t)
				out[by] |= 1 << bi
			}
		}
	}
	return out
}

func refUnpack(region []byte, n, d int) []byte {
	codes := make([]byte, n)
	for i := range codes {
		for t := range d {
			by, bi := refBitPos(n, d, i, t)
			codes[i] |= (region[by] >> bi & 1) << t
		}
	}
	return codes
}

// cPack transliterates the upstream scalar packers (prim_enc_pack_dN in
// src/pivco_huffman_primitives_scalar.h) as an independent check of refBitPos.
func cPack(ranks []byte, d int, base byte) []byte {
	n := len(ranks)
	out := make([]byte, (n*d+7)/8)
	vertN := func(n int) int {
		if d >= 2 && d <= 7 && n >= 128 {
			return n &^ 127
		}
		return 0
	}
	vpack := func(out, ranks []byte, nv, ln int) {
		mask := uint32(1)<<d - 1
		bv := ln * 8
		for b := range nv / bv {
			blk := out[b*ln*d:]
			for v := range bv {
				val := uint32(ranks[b*bv+v]-base) & mask
				l, bit := v%ln, v/ln*d
				j, off := bit>>3, bit&7
				blk[ln*j+l] |= byte(val << off)
				if off+d > 8 {
					blk[ln*(j+1)+l] |= byte(val >> (8 - off))
				}
			}
		}
	}
	natPack := func(out, ranks []byte) {
		var buf uint64
		bits, idx := 0, 0
		for _, r := range ranks {
			buf |= uint64(r-base) << bits
			for bits += d; bits >= 8; bits -= 8 {
				out[idx] = byte(buf)
				idx++
				buf >>= 8
			}
		}
		if bits > 0 {
			out[idx] = byte(buf & (1<<bits - 1))
		}
	}
	n5 := 0
	if d >= 2 && d <= 7 && n >= 512 {
		n5 = n &^ 511
	}
	vpack(out, ranks, n5, 64)
	r := n - n5
	nv := vertN(r)
	o1 := out[n5*d>>3:]
	vpack(o1, ranks[n5:], nv, 16)
	if r > nv {
		natPack(o1[nv*d>>3:], ranks[n5+nv:])
	}
	return out
}
