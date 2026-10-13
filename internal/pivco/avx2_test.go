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

//go:build amd64 && !appengine && !noasm && gc && !purego

package pivco

import (
	"slices"
	"testing"

	"github.com/klauspost/cpuid/v2"
	"github.com/minio/minlz/internal/pivco/kernel"
)

// TestZeroCodersAVX2 hides AVX-512 from cpuid (a restored snapshot) and checks that zero
// coders then run the AVX2 tier, without histogram scratch.
func TestZeroCodersAVX2(t *testing.T) {
	if !slices.Contains(kernel.Tiers(), kernel.TierAVX2) {
		t.Skip("no AVX2 tier")
	}
	saved := cpuid.CPU
	t.Cleanup(func() { cpuid.CPU = saved })
	cpuid.CPU.Disable(cpuid.AVX512F)
	var e Encoder
	var d Decoder
	var h [256]uint64
	e.Histogram(&h, []byte("abc"))
	d.setup()
	if e.tier != kernel.TierAVX2 || d.tier != kernel.TierAVX2 || e.hscratch != nil {
		t.Fatalf("zero coders: encoder %v (scratch %v), decoder %v", e.tier, e.hscratch != nil, d.tier)
	}
}
