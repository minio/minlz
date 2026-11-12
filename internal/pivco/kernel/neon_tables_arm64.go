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

// Lookup tables of neon_arm64.s. The merge and partition tables are built as upstream's
// init_merge_tables and build_tabs (src/pivco_huffman_primitives_neon.h).
var (
	// mergeTab[0][m0] - mergeTab[1][m1] is the merge index of 16 outputs with bitmap
	// m0|m1<<8: right lanes get their right cursor (0..15), left lanes 240 minus their
	// left cursor, so that one TBL and one TBX pick both sides.
	mergeTab = mergeTabs()
	// p16A[m0] | p16B[m1][pc0:] is the index that puts the left ranks of 16 with mask
	// m0|m1<<8 in order at the front and the right ones reversed at the back;
	// pc0 = popcount(m0).
	p16A, p16B = p16Tabs()
	// ctab[0][m] (ctab[1][m]) moves the right ranks of the low (high) 8 lanes of a vector
	// to the front; 0xFF lanes look up zero.
	ctab = ctabs()
	// natUnpackTab[d] is the TBL gathering the byte pair of each of codes 0..7 and 8..15
	// of a 16-code natural group into a u16 lane, the USHL counts that move the codes
	// down, and the code mask.
	natUnpackTab = natUnpackTabs()
	// natPackTab[d] is the compacting TBL and the USHL counts of upstream's converging
	// pyramid (src/pivco_huffman_neon_pack.h) at the 8, 16, 32 and 64-bit levels.
	natPackTab = natPackTabs()
)

func mergeTabs() (t [2][256][16]uint8) {
	t0, t1 := &t[0], &t[1]
	for i := range 256 {
		pop := uint8(0)
		for j := range 8 {
			if i>>j&1 != 0 {
				t0[i][j], t1[i][j+8] = pop, -pop
				pop++
			} else {
				v := pop - 16 - uint8(j)
				t0[i][j], t1[i][j+8] = v, 8-v
			}
		}
		for j := range 8 {
			t0[i][j+8] = pop
		}
	}
	return t
}

func p16Tabs() (a [256][16]uint8, b [256][32]uint8) {
	for m := range 256 {
		la, ra, lb, rb := 0, 15, 8, 15
		for k := range 8 {
			if m>>k&1 != 0 {
				a[m][ra], b[m][rb] = uint8(k), uint8(8+k)
				ra, rb = ra-1, rb-1
			} else {
				a[m][la], b[m][lb] = uint8(k), uint8(8+k)
				la, lb = la+1, lb+1
			}
		}
	}
	return a, b
}

func ctabs() (t [2][256][8]uint8) {
	for m := range 256 {
		q := 0
		for k := range 8 {
			if m>>k&1 != 0 {
				t[0][m][q], t[1][m][q] = uint8(k), uint8(8+k)
				q++
			}
		}
		for ; q < 8; q++ {
			t[0][m][q], t[1][m][q] = 0xff, 0xff
		}
	}
	return t
}

func natUnpackTabs() (t [8][4][16]uint8) {
	for d := 2; d <= 7; d++ {
		for k := range 16 {
			b, s := k*d/8, k*d%8
			g := &t[d][k/8]
			g[k%8*2], g[k%8*2+1] = uint8(b), uint8(b+1)
			t[d][2][k%8*2] = uint8(-s)
			t[d][3][k] = uint8(1<<d - 1)
		}
	}
	return t
}

func natPackTabs() (t [8][5][16]uint8) {
	for d := 2; d <= 7; d++ {
		// The pyramid leaves the 8d bits of each u64 at bit 32-4d; odd d shift 4 more to
		// start on a byte.
		s, sr := 8-d, 4*(d&1)
		b0 := (32 - 4*d - sr) / 8
		for i := range 16 {
			t[d][0][i] = 0xff
		}
		for i := range d {
			t[d][0][i], t[d][0][d+i] = uint8(b0+i), uint8(8+b0+i)
		}
		for i := 0; i < 16; i += 4 {
			t[d][1][i], t[d][1][i+2] = uint8(s), uint8(s)
			t[d][2][i], t[d][2][i+2] = uint8(s), uint8(-s)
		}
		for i := 0; i < 16; i += 8 {
			t[d][3][i], t[d][3][i+4] = uint8(2*s), uint8(-2*s)
			t[d][4][i] = uint8(-sr)
		}
	}
	return t
}
