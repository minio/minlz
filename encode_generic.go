// Copyright 2025 MinIO Inc.
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

// Split out of asm_none.go so arm64 can take the assembly block encoders from
// encode_arm64.go while still using the Go helpers (emitLiteral, matchLen and
// friends) that asm_none.go continues to provide there. amd64 replaces those
// helpers with assembly too, which is why it excludes asm_none.go outright.

//go:build (!amd64 && !arm64) || appengine || !gc || noasm || purego

package minlz

const hasAsm = false

// encodeBlockFast encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src))
func encodeBlockFast(dst, src []byte) (d int) {
	if len(src) < minNonLiteralBlockSize {
		return 0
	}
	if len(src) <= 65536 {
		// Only very maginally faster...
		return encodeFastBlockGo64K(dst, src)
	}
	return encodeFastBlockGo(dst, src)
}

// encodeBlock encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src))
func encodeBlock(dst, src []byte) (d int) {
	if len(src) < minNonLiteralBlockSize {
		return 0
	}
	if len(src) <= 65536 {
		return encodeBlockGo64K(dst, src)
	}
	return encodeBlockGo(dst, src)
}

// encodeBlockBetter encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src))
func encodeBlockBetter(dst, src []byte) (d int) {
	if len(src) < minNonLiteralBlockSize {
		return 0
	}
	if len(src) <= 64<<10 {
		return encodeBlockBetterGo64K(dst, src)
	}
	return encodeBlockBetterGo(dst, src)
}
