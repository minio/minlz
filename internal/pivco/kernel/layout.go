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

// VertSplit returns how many leading codes of an n-code flat region of depth d are stored
// in 512-value (64-lane) blocks and how many of the following codes in 128-value (16-lane)
// blocks; the remaining n-n512-n128 codes are natural. Mirrors upstream pivco_vert_n512 and
// pivco_vert_n with the FlatVertical layout, as gated in prim_enc_pack_dN.
func VertSplit(n int, d uint8) (n512, n128 int) {
	if d < 2 || d > 7 {
		return 0, 0
	}
	if n >= 512 {
		n512 = n &^ 511
	}
	if r := n - n512; r >= 128 {
		n128 = r &^ 127
	}
	return n512, n128
}
