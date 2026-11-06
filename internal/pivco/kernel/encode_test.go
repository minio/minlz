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
	"bytes"
	"testing"
)

func TestGather(t *testing.T) {
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, n := range testSizes {
			var s2r [256]byte
			randBytes(rng, s2r[:])
			if n%3 == 0 {
				s2r[0] = 255 // sentinel-like rank must be reported as the max
			}
			src, _ := guarded(randBytes(rng, make([]byte, n)))
			if n%2 == 0 {
				for i := range src {
					src[i] &= 7
				}
			}
			want := make([]byte, n)
			var wmx uint8
			for i, s := range src {
				want[i] = s2r[s]
				wmx = max(wmx, want[i])
			}
			buf := fill(make([]byte, n+RankSlack+guardLen), guardByte)
			mx := tier.Gather(buf[:n+RankSlack], src, &s2r)
			if mx != wmx || !bytes.Equal(buf[:n], want) || !intact(buf[n+RankSlack:]) {
				t.Fatalf("%v n=%d: max %d want %d, ranks ok %v, guard ok %v",
					tier, n, mx, wmx, bytes.Equal(buf[:n], want), intact(buf[n+RankSlack:]))
			}
		}
	}
}

func TestPartition(t *testing.T) {
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, n := range testSizes {
			for _, thr := range []uint8{0, 1, 127, uint8(2 + rng.IntN(251)), 253, 254, 255} {
				ranks := randBytes(rng, make([]byte, n))
				if n%2 == 1 {
					// Ranks around thr, so both sides and long runs occur.
					for i := range ranks {
						ranks[i] = uint8(min(max(int(thr)+int(ranks[i]%7)-3, 0), 255))
					}
				}
				testPartition(t, tier, ranks, thr)
			}
		}
	}
}

// TestPartitionEmpty: every tier returns 0 for n = 0 without touching its buffers.
func TestPartitionEmpty(t *testing.T) {
	for _, tier := range Tiers() {
		if tier.PartFull(nil, 0, nil, nil, 0) != 0 || tier.PartRight(nil, 0, nil, nil, 0) != 0 ||
			tier.PartNone(nil, 0, nil, 0) != 0 {
			t.Errorf("%v: nonzero count", tier)
		}
	}
}

func testPartition(t *testing.T, tier Tier, ranks []byte, thr uint8) {
	t.Helper()
	n, nb := len(ranks), (len(ranks)+7)/8
	wbm, wl, wr := refPart(ranks, thr)
	newBuf := func(n int) []byte { return fill(make([]byte, n+RankSlack+guardLen), guardByte) }
	checkBm := func(name string, bm []byte) {
		if !bytes.Equal(bm[:nb], wbm) || !intact(bm[nb+RankSlack:]) {
			t.Fatalf("%v %s n=%d thr=%d: bitmap ok %v, guard ok %v", tier, name, n, thr,
				bytes.Equal(bm[:nb], wbm), intact(bm[nb+RankSlack:]))
		}
	}

	for _, adjacent := range []bool{false, true} {
		// adjacent: right == ranks[n:], as for a right child in the encoder's walk; left
		// stores must then stay inside ranks[:n].
		var rk, right []byte
		if adjacent {
			rk = newBuf(2 * n)
			right = rk[n:]
		} else {
			rk, right = newBuf(n), newBuf(n)
		}
		copy(rk, ranks)
		bm := newBuf(nb)
		nr := tier.PartFull(rk[:n+RankSlack], n, right[:n+RankSlack], bm[:nb+RankSlack], thr)
		checkBm("PartFull", bm)
		ok := nr == len(wr) && bytes.Equal(rk[:n-nr], wl) && bytes.Equal(right[:nr], wr) &&
			intact(right[n+RankSlack:])
		if !adjacent {
			ok = ok && intact(rk[n:])
		}
		if !ok {
			t.Fatalf("%v PartFull adjacent=%v n=%d thr=%d: nr %d want %d", tier, adjacent, n, thr, nr, len(wr))
		}

		copy(rk, ranks)
		bm = newBuf(nb)
		if adjacent {
			fill(right, guardByte)
		} else {
			right = newBuf(n)
		}
		nr = tier.PartRight(rk[:n+RankSlack], n, right[:n+RankSlack], bm[:nb+RankSlack], thr)
		checkBm("PartRight", bm)
		if nr != len(wr) || !bytes.Equal(rk[:n], ranks) || !bytes.Equal(right[:nr], wr) ||
			!intact(right[n+RankSlack:]) {
			t.Fatalf("%v PartRight adjacent=%v n=%d thr=%d: nr %d want %d", tier, adjacent, n, thr, nr, len(wr))
		}
	}

	rk := newBuf(n)
	copy(rk, ranks)
	bm := newBuf(nb)
	nr := tier.PartNone(rk[:n+RankSlack], n, bm[:nb+RankSlack], thr)
	checkBm("PartNone", bm)
	if nr != len(wr) || !bytes.Equal(rk[:n], ranks) || !intact(rk[n:]) {
		t.Fatalf("%v PartNone n=%d thr=%d: nr %d want %d", tier, n, thr, nr, len(wr))
	}
}

func TestVertSplit(t *testing.T) {
	for _, c := range []struct {
		n          int
		d          uint8
		n512, n128 int
	}{
		{1000, 3, 512, 384},
		{1000, 8, 0, 0},
		{1000, 1, 0, 0},
		{511, 7, 0, 384},
		{639, 2, 512, 0},
		{640, 2, 512, 128},
		{127, 4, 0, 0},
		{65535, 5, 65024, 384},
	} {
		n512, n128 := VertSplit(c.n, c.d)
		if n512 != c.n512 || n128 != c.n128 {
			t.Errorf("VertSplit(%d, %d) = %d, %d; want %d, %d", c.n, c.d, n512, n128, c.n512, c.n128)
		}
	}
}

// TestLayoutExample pins a few addresses of the wire layout by hand.
func TestLayoutExample(t *testing.T) {
	// "abcd" with four 2-bit codes 0..3 packs naturally to 0xE4.
	if got := refPack([]byte{0, 1, 2, 3}, 2); !bytes.Equal(got, []byte{0xe4}) {
		t.Fatalf("natural abcd: %x", got)
	}
	// One 128-value block, D=3: code v lives in lane v%16 at bits 3*(v/16)...
	codes := make([]byte, 128)
	codes[0], codes[1], codes[16], codes[48] = 1, 2, 7, 5
	want := make([]byte, 48)
	want[0] = 1 | 7<<3 // lane 0: v=0 at bit 0, v=16 at bits 3..5
	want[1] = 2        // lane 1: v=1 at bit 0
	want[16] = 5 << 1  // lane 0: v=48 at bits 9..11, column 1
	for _, f := range []func() []byte{
		func() []byte { return refPack(codes, 3) },
		func() []byte { return cPack(codes, 3, 0) },
	} {
		if got := f(); !bytes.Equal(got, want) {
			t.Fatalf("vertical128 block: %x\nwant %x", got, want)
		}
	}
}

func TestPack(t *testing.T) {
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, n := range testSizes {
			for d := 2; d <= 8; d++ {
				base := uint8(rng.IntN(257 - 1<<d))
				codes := randBytes(rng, make([]byte, n))
				for i := range codes {
					codes[i] &= 1<<d - 1
				}
				ranks := fill(make([]byte, n+RankSlack+guardLen), guardByte)
				for i, c := range codes {
					ranks[i] = base + c
				}
				want := refPack(codes, d)
				if n <= 1100 || n == 65535 {
					if c := cPack(ranks[:n], d, base); !bytes.Equal(c, want) {
						t.Fatalf("reference mismatch n=%d d=%d", n, d)
					}
				}
				size := len(want)
				dst := randBytes(rng, make([]byte, size+guardLen)) // junk: every byte must be written
				fill(dst[size:], guardByte)
				tier.Pack(dst[:size], ranks[:n+RankSlack], n, uint8(d), base)
				if !bytes.Equal(dst[:size], want) || !intact(dst[size:]) || !intact(ranks[n:]) {
					t.Fatalf("%v n=%d d=%d: region ok %v, guard ok %v",
						tier, n, d, bytes.Equal(dst[:size], want), intact(dst[size:]))
				}
			}
		}
	}
}

func TestHistogram(t *testing.T) {
	scratch := make([]byte, HistScratch)
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, n := range append(testSizes, 1<<20+13) {
			for kind := range 3 {
				src := randBytes(rng, make([]byte, n))
				switch kind {
				case 1:
					fill(src, 7)
				case 2:
					for i := range src {
						src[i] = 200 + src[i]&1
					}
				}
				src, _ = guarded(src)
				var h, want [256]uint32
				for i := range h {
					h[i] = rng.Uint32()
				}
				want = h
				for _, c := range src {
					want[c]++
				}
				tier.Histogram(&h, src, scratch)
				if h != want {
					t.Fatalf("%v n=%d kind=%d: histogram mismatch", tier, n, kind)
				}
			}
		}
	}
}
