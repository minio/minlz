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
	"testing"

	"github.com/klauspost/cpuid/v2"
)

func needHistAVX512(t testing.TB) {
	if !hasHistAVX512() {
		t.Skip("no AVX-512 GFNI/BITALG")
	}
}

// TestAVX512Histogram uses the sizes of upstream test_histogram at several alignments,
// accumulates into a non-zero histogram twice, and fills 1 MiB with one byte so that a
// bin is consumed in the middle of a round. Inputs below 0x40 and below 0x80, also with
// sparse larger bytes, run the one- and two-bin row loops and their exits.
func TestAVX512Histogram(t *testing.T) {
	needHistAVX512(t)
	rng := newRNG()
	scratch := make([]byte, HistScratch)
	for _, n := range []int{0, 1, 7, 63, 64, 65, 127, 4095, 4096, 65472, 65473, 1 << 20, 5 << 20} {
		for _, off := range []int{0, 1, 63} {
			for kind := range 6 {
				buf := make([]byte, n+off+64)
				src := buf[off : off+n : off+n]
				switch kind {
				case 0:
					randBytes(rng, src)
				case 1:
					fill(src, 0xC3)
				case 2: // one symbol per bin, skewed
					for i := range src {
						src[i] = byte(rng.IntN(4)<<6 | rng.IntN(2))
					}
				case 3: // one bin
					for i := range src {
						src[i] = byte(rng.IntN(64))
					}
				case 4: // two bins
					for i := range src {
						src[i] = byte(rng.IntN(128))
					}
				case 5: // one bin, with sparse bytes of the other bins
					for i := range src {
						src[i] = byte(rng.IntN(64))
						if rng.IntN(3000) == 0 {
							src[i] = byte(rng.IntN(256))
						}
					}
				}
				var h, want [256]uint32
				for i := range h {
					h[i] = rng.Uint32()
				}
				want = h
				for range 2 {
					for _, c := range src {
						want[c]++
					}
					TierAVX512.Histogram(&h, src, scratch)
				}
				if h != want {
					t.Fatalf("n=%d off=%d kind=%d: histogram mismatch", n, off, kind)
				}
			}
		}
	}
}

func TestAVX512HistogramFallback(t *testing.T) {
	needHistAVX512(t)
	saved := cpuid.CPU
	t.Cleanup(func() { cpuid.CPU = saved })
	cpuid.CPU.Disable(cpuid.GFNI)
	if hasHistAVX512() || TierAVX512.HistUsesScratch() {
		t.Fatal("scratch reported without GFNI")
	}
	src := randBytes(newRNG(), make([]byte, 1000))
	var h, want [256]uint32
	histogramGeneric(&want, src)
	TierAVX512.Histogram(&h, src, nil)
	if h != want {
		t.Fatal("fallback histogram mismatch")
	}
}
