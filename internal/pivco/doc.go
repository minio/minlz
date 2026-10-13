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

// Package pivco codes blocks of up to 65535 bytes with PivCo ("pivoted coding") Huffman.
//
// Instead of a sequential stream of codes, a block stores one bitmap per internal
// Huffman tree node, holding the left/right decision of every symbol that passes through it.
// Subtrees whose leaves all share one depth are packed as D-bit codes instead.
// Decoding merges the children of each node bottom-up, which vectorizes well.
//
// This package is a port of the PH block codec of PivCo-Huffman by Marcin Zukowski,
// github.com/MarcinZukowski/pivco-huffman at commit 07d9efc69eef1dfbb0c683d98259319c80135d79
// (2026-09-17), which is licensed under the Apache License, Version 2.0, as is this package.
// Only what MinLZ uses is ported: plain Huffman lengths (upstream effort 0), the
// FlatVertical layout and raw bitmaps (no ANS), without the pivcohuf container.
// Shaper ports the shaping of lengths for faster decoding (effort 2, src/joint_lengths.c),
// which MinLZ doesn't use for now: it costs encode time for a few percent of block decode speed.
// The block format is defined in PIVCO_SPEC.md. It is the upstream PH block without its
// symbol count, its marker prefix and the marker byte before each flat region; otherwise
// the regions are byte for byte those of upstream.
package pivco
