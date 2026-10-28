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

// Ports of upstream prim_enc_pack_dN (src/pivco_huffman_primitives_scalar.h) and
// vert_pack_scalar, vert512_pack_scalar (src/pivco_huffman_vertical.h).

func packGeneric(dst, ranks []byte, n int, d, base uint8) {
	dst, ranks = dst[:(n*int(d)+7)/8], ranks[:n]
	n512, n128 := VertSplit(n, d)
	vertPack(dst, ranks[:n512], d, base, 64)
	dst, ranks = dst[n512*int(d)/8:], ranks[n512:]
	vertPack(dst, ranks[:n128], d, base, 16)
	natPack(dst[n128*int(d)/8:], ranks[n128:], d, base)
}

// vertPack packs len(ranks), a multiple of 8*ln, codes into ln-lane vertical blocks: code v
// of a block is field v/ln of lane v%ln, whose d bytes sit at a stride of ln.
func vertPack(dst, ranks []byte, d, base uint8, ln int) {
	mask := uint64(1)<<d - 1
	bs := ln * int(d)
	for ; len(ranks) > 0; dst, ranks = dst[bs:], ranks[8*ln:] {
		blk, src := dst[:bs], ranks[:8*ln]
		for l := range ln {
			var w uint64
			for s := range 8 {
				w |= (uint64(src[s*ln+l]-base) & mask) << (s * int(d))
			}
			for j := range int(d) {
				blk[j*ln+l] = byte(w >> (8 * j))
			}
		}
	}
}

// natPack packs codes as one LSB-first bit string; 8 codes fill exactly d bytes.
func natPack(dst, ranks []byte, d, base uint8) {
	if d == 8 {
		for i, r := range ranks {
			dst[i] = r - base
		}
		return
	}
	mask := uint64(1)<<d - 1
	for i := 0; i < len(ranks); i += 8 {
		g := ranks[i:min(i+8, len(ranks))]
		var w uint64
		for k, r := range g {
			w |= (uint64(r-base) & mask) << (k * int(d))
		}
		o := dst[i/8*int(d):]
		if len(o) >= 8 {
			// The upper 8-d bytes are rewritten by the next group.
			Store64(o, 0, w)
			continue
		}
		for j := range (len(g)*int(d) + 7) / 8 {
			o[j] = byte(w >> (8 * j))
		}
	}
}
