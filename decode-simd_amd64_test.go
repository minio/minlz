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

//go:build amd64 && !(appengine || !gc || noasm || purego)

package minlz

import "testing"

// simdTiers runs f as a subtest for each SIMD block tier the CPU supports, with that tier forced.
func simdTiers(t *testing.T, f func(t *testing.T)) {
	avx2, avx512 := simdAVX2, simdAVX512
	defer func() { simdAVX2, simdAVX512 = avx2, avx512 }()
	for _, tier := range []struct {
		name         string
		avx2, avx512 bool
	}{{"go", false, false}, {"avx2", true, false}, {"avx512", true, true}} {
		if tier.avx2 && !avx2 || tier.avx512 && !avx512 {
			continue
		}
		simdAVX2, simdAVX512 = tier.avx2, tier.avx512
		t.Run(tier.name, f)
	}
}
