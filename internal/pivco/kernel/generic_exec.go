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

// Ports of the upstream merges (merge_*_scalar in src/pivco_huffman_primitives_scalar.h)
// and flat unpacks (src/pivco_huffman_vertical.h).

// execGeneric checks every op's geometry against the slices before running it, so a
// malformed op fails closed instead of panicking.
func execGeneric(p *Program) int {
	for i := range p.Ops {
		if !execOp(p, &p.Ops[i]) {
			return i
		}
	}
	return -1
}

// span returns b[off:off+n] if it is in range.
func span(b []byte, off uint32, n uint64) ([]byte, bool) {
	if uint64(off)+n > uint64(len(b)) {
		return nil, false
	}
	return b[off:][:n], true
}

func execOp(p *Program, op *Op) bool {
	// Ops may address up to ArenaSlack bytes less than the arena, the over-read reserve
	// of the vector tiers.
	arena := p.Arena[:max(len(p.Arena)-ArenaSlack, 0)]
	dst, src := arena, p.In
	if op.Flags&FlagDst != 0 {
		dst = p.Dst
	}
	k, d := uint64(op.K), uint64(op.D)
	out, ok := span(dst, op.Out, k)
	if !ok {
		return false
	}
	switch op.Kind {
	case OpVV:
		bm, ok1 := span(src, op.Bm, (k+7)/8)
		l, ok2 := span(arena, op.L, k)
		r, ok3 := span(arena, op.R, k)
		return ok1 && ok2 && ok3 && mergeVV(out, bm, l, r) == int(op.KR)
	case OpCV:
		bm, ok1 := span(src, op.Bm, (k+7)/8)
		r, ok2 := span(arena, op.R, k)
		return ok1 && ok2 && mergeCV(out, bm, r, op.P) == int(op.KR)
	case OpCC:
		bm, ok := span(src, op.Bm, (k+7)/8)
		if ok {
			mergeCC(out, bm, op.P, op.Q)
		}
		return ok
	case OpV512, OpV128:
		ln := uint64(16)
		if op.Kind == OpV512 {
			ln = 64
		}
		region, ok1 := span(src, op.Bm, k*d/8)
		lut, ok2 := flatLUT(p, op)
		if !ok1 || !ok2 || d > 7 || k%(8*ln) != 0 {
			return false
		}
		vertUnpack(out, region, op.D, int(ln), lut)
	case OpNat:
		region, ok1 := span(src, op.Bm, (k*d+7)/8)
		lut, ok2 := flatLUT(p, op)
		if !ok1 || !ok2 {
			return false
		}
		natUnpack(out, region, op.D, lut)
	case OpCopy:
		region, ok := span(src, op.Bm, k)
		if !ok || d != 8 {
			return false
		}
		copy(out, region)
	default:
		return false
	}
	return true
}

// flatLUT returns the 1<<D symbols of a flat subtree; D must be 2..8.
func flatLUT(p *Program, op *Op) ([]byte, bool) {
	n := 1 << op.D
	if op.D < 2 || op.D > 8 || p.R2S == nil || int(op.P)+n > R2SLen {
		return nil, false
	}
	return p.R2S[op.P:][:n], true
}

// The merges load both sources before storing each output byte (the 8-byte fast paths load
// the whole chunk first). In the in-place layout the larger child ends at out[K) and
// starts K_small bytes in; the write cursor j = l+r then never passes its read cursor, so
// no unread byte is overwritten. Sources are read only inside [0, K): l, r <= j.

// mergeVV writes out[j] = bit j ? r[nr++] : l[nl++] and returns nr.
func mergeVV(out, bm, l, r []byte) int {
	nl, nr := 0, 0
	for i, b := range bm {
		o := out[i*8:]
		if len(o) >= 8 {
			switch b {
			case 0:
				Store64(o, 0, Load64(l, nl))
				nl += 8
				continue
			case 0xff:
				Store64(o, 0, Load64(r, nr))
				nr += 8
				continue
			}
		}
		for k := range o[:min(len(o), 8)] {
			g := int(b>>k) & 1
			x, y := l[nl], r[nr]
			if g != 0 {
				x = y
			}
			o[k] = x
			nr += g
			nl += g ^ 1
		}
	}
	return nr
}

// mergeCV writes out[j] = bit j ? r[nr++] : c and returns nr.
func mergeCV(out, bm, r []byte, c byte) int {
	cc := uint64(c) * 0x0101010101010101
	nr := 0
	for i, b := range bm {
		o := out[i*8:]
		if len(o) >= 8 {
			switch b {
			case 0:
				Store64(o, 0, cc)
				continue
			case 0xff:
				Store64(o, 0, Load64(r, nr))
				nr += 8
				continue
			}
		}
		for k := range o[:min(len(o), 8)] {
			g := int(b>>k) & 1
			x := r[nr]
			if g == 0 {
				x = c
			}
			o[k] = x
			nr += g
		}
	}
	return nr
}

// mergeCC writes out[j] = bit j ? q : p.
func mergeCC(out, bm []byte, p, q byte) {
	pp, qq := uint64(p)*0x0101010101010101, uint64(q)*0x0101010101010101
	for i, b := range bm {
		o := out[i*8:]
		if len(o) >= 8 {
			// Spread bit k of b to all bits of byte k: isolate it in byte k, then carry
			// any non-zero byte into its bit 7.
			t := (uint64(b) * 0x0101010101010101) & 0x8040201008040201
			m := ((t + 0x7f7f7f7f7f7f7f7f) >> 7 & 0x0101010101010101) * 0xff
			Store64(o, 0, pp&^m|qq&m)
			continue
		}
		for k := range o {
			x := p
			if b>>k&1 != 0 {
				x = q
			}
			o[k] = x
		}
	}
}

// vertUnpack is the inverse of vertPack, mapping codes through lut.
func vertUnpack(out, src []byte, d uint8, ln int, lut []byte) {
	mask := uint64(len(lut) - 1)
	bs := ln * int(d)
	for ; len(out) > 0; out, src = out[8*ln:], src[bs:] {
		o, blk := out[:8*ln], src[:bs]
		for l := range ln {
			var w uint64
			for j := range int(d) {
				w |= uint64(blk[j*ln+l]) << (8 * j)
			}
			for s := range 8 {
				o[s*ln+l] = lut[w>>(s*int(d))&mask]
			}
		}
	}
}

// natUnpack is the inverse of natPack, mapping codes through lut.
func natUnpack(out, src []byte, d uint8, lut []byte) {
	mask := uint64(len(lut) - 1)
	for i := 0; i < len(out); i += 8 {
		s := src[i/8*int(d):]
		var w uint64
		if len(s) >= 8 {
			w = Load64(s, 0)
		} else {
			for j, c := range s {
				w |= uint64(c) << (8 * j)
			}
		}
		for k := range out[i:min(i+8, len(out))] {
			out[i+k] = lut[w>>(k*int(d))&mask]
		}
	}
}
