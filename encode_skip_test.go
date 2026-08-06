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

package minlz

import (
	"math/rand"
	"testing"
)

// mixedRuns builds total bytes of alternating incompressible and compressible
// runs of runLen each. Long runs are the point: the adaptive skip only grows
// past the clamp after a long stretch with no matches, so shapes with short
// noise runs cannot distinguish a clamped encoder from an unclamped one.
func mixedRuns(rng *rand.Rand, total, runLen int) []byte {
	words := []string{"lorem", "ipsum", "dolor", "sit", "amet", "consectetur"}
	out := make([]byte, 0, total+2*runLen)
	for len(out) < total {
		noise := make([]byte, runLen)
		rng.Read(noise)
		out = append(out, noise...)
		txt := make([]byte, 0, runLen+16)
		for len(txt) < runLen {
			txt = append(txt, words[rng.Intn(len(words))]...)
			txt = append(txt, ' ')
		}
		out = append(out, txt[:runLen]...)
	}
	return out[:total]
}

// TestBetterSkipClampMatchesAsm checks that the Go LevelBalanced encoder
// compresses mixed content as well as the assembly one does.
//
// The two use the same adaptive skip, and above 64KB both clamp it at 100.
// Before the Go side clamped, it lost 0.11%-0.27% of ratio on these shapes,
// because after a long matchless stretch the unclamped skip strides past the
// start of the next compressible region. The bound here is far tighter than
// that gap and far looser than the few bytes the two still differ by, so it
// fails if the clamp is removed and does not fail on incidental drift.
//
// Note the two encoders are not byte-identical and this does not require them
// to be -- they are independent implementations. Only the ratio is compared.
func TestBetterSkipClampMatchesAsm(t *testing.T) {
	if !hasAsm {
		t.Skip("no assembly encoder to compare against")
	}
	// 16KB runs are deliberately excluded: the skip only reaches ~128 there, so
	// the clamp barely binds and unrelated differences between the two
	// implementations dominate the comparison. 64KB and up is where the clamp
	// does real work, and is where the ratio gap closes to a few bytes.
	const tolerance = 0.0005 // 0.05%
	for _, runLen := range []int{64 << 10, 256 << 10} {
		src := mixedRuns(rand.New(rand.NewSource(1)), 4<<20, runLen)
		asm, err := Encode(nil, src, LevelBalanced)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		go_ := encodeGo(nil, src, LevelBalanced)
		excess := float64(len(go_)-len(asm)) / float64(len(asm))
		if excess > tolerance {
			t.Errorf("noise runs of %d bytes: Go output %d bytes vs assembly %d, %+.3f%% worse (tolerance %+.3f%%) -- is the skip clamp still in encodeBlockBetterGo?",
				runLen, len(go_), len(asm), 100*excess, 100*tolerance)
		}
	}
}
