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

//go:build !arm64 || appengine || noasm || !gc || purego

package kernel

// Without arm64 assembly TierNEON is never selected; these keep the dispatch switch
// compiling on every platform.

func hasNEON() bool { return false }

func gatherNEON(ranks, src []byte, s2r *[256]byte) uint8 { return gatherGeneric(ranks, src, s2r) }

func partFullNEON(ranks []byte, n int, right, bm []byte, thr uint8) int {
	return partFullGeneric(ranks, n, right, bm, thr)
}

func partRightNEON(ranks []byte, n int, right, bm []byte, thr uint8) int {
	return partRightGeneric(ranks, n, right, bm, thr)
}

func partNoneNEON(ranks []byte, n int, bm []byte, thr uint8) int {
	return partNoneGeneric(ranks, n, bm, thr)
}

func packNEON(dst, ranks []byte, n int, d, base uint8) { packGeneric(dst, ranks, n, d, base) }

func execNEON(p *Program) int { return execGeneric(p) }
