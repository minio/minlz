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

package reference

import (
	"encoding/binary"
	"fmt"
)

// EncodeBlock is a reference implementation of the MinLZ block decoder.
// This implementation is not optimized for speed nor efficiency,
// but for readability with no dependencies.
// See 'encodeBlockGo' for a more optimal encoder.
func EncodeBlock(src []byte) ([]byte, error) {
	// Create a destination buffer
	n := MaxEncodedLen(len(src))
	dst := make([]byte, 0, n)

	// Return very small blocks uncompressed
	if len(src) <= 16 {
		return encodeUncompressed(dst, src), nil
	}

	// Add initial zero before size to indicate MinLZ block.
	dst = append(dst, 0)

	// Add varint-encoded length of the decompressed bytes.
	dst = binary.AppendUvarint(dst, uint64(len(src)))
	compressed := encodeBlock(dst, src)

	// If compressed, return.
	if compressed != nil {
		return compressed, nil
	}
	// Not compressible, discard and emit as uncompressed.
	return encodeUncompressed(dst[:0], src), nil
}

// encodeUncompressed will append src to dst as uncompressed data and return it.
func encodeUncompressed(dst, src []byte) []byte {
	// This is a valid method to represent a length 0 payload.
	if len(src) == 0 {
		return append(dst, 0)
	}
	// Append 0, 0 and the payload.
	return append(append(dst, 0, 0), src...)
}

// MaxEncodedLen returns the maximum length of a snappy block, given its
// uncompressed length.
//
// It will return a negative value if srcLen is too large to encode.
func MaxEncodedLen(srcLen int) int {
	if srcLen < 0 || srcLen > maxBlockSize {
		return -1
	}
	if srcLen == 0 {
		return 1
	}
	// Maximum overhead is 2 bytes.
	return srcLen + 2
}

// encodeBlockGo encodes a non-empty src to dst.
// It assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//
//	len(dst) >= MaxEncodedLen(len(src)) &&
//	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlock(dst, src []byte) (res []byte) {
	// Initialize the hash table.
	const (
		tableBits    = 16
		maxTableSize = 1 << tableBits
		inputMargin  = 4

		// Print match debug information...
		debug = false
	)

	// Allocate a lookup table on the stack.
	// We use a simple hash function to reduce 4 bytes to 16 bits.
	var table [maxTableSize]uint32

	// sLimit is when to stop looking for offset/length copies. The inputMargin
	// lets us use a fast path for emitLiteral in the main loop, while we are
	// looking for copies.
	sLimit := len(src) - inputMargin

	// Bail if we can't compress to at least this.
	// This also guarantees we can always emit the longest codes.
	dstLimit := len(src) + len(dst) - 11

	// nextEmit is where in src the next emitLiteral should start from.
	// We don't emit literals until we have found a match.
	nextEmit := 0

	// The encoded form must start with a literal, as there are no previous
	// bytes to copy, so we start looking for hash matches at s == 1.
	s := 1

	repeat := 1
	if debug {
		fmt.Println("encodeBlockGo: Starting encode")
	}
	for {
		// Offset in src of a candidate, at least 4 bytes in length.
		candidate := 0

		// This is the maximum offset we can encode...
		minSrcPos := s - (2 << 20) - 65535

		// Search for matches
		for {
			if s > sLimit {
				goto emitRemainder
			}
			// Load 4 bytes.
			cv := binary.LittleEndian.Uint32(src[s:])

			hash := hash4(cv, tableBits)
			candidate = int(table[hash])
			table[hash] = uint32(s)

			if candidate >= minSrcPos && cv == binary.LittleEndian.Uint32(src[candidate:]) {
				break
			}

			// Move to next input byte
			s++
			minSrcPos++
		}

		// A 4-byte match has been found.
		// We'll later see if more than 4 bytes match.

		// Base is where our match starts.
		base := s

		// This is the offset of the match
		offset := s - candidate

		// Extend the 4-byte match as long as possible.
		candidate += 4
		s += 4
		for s < len(src) {
			if src[s] != src[candidate] {
				break
			}
			candidate++
			s++
		}
		length := s - base

		// We can now emit what we have found and any pending literals.
		// The decision tree for final encoding is close to optimal,
		// but there are corner cases where a slightly more optimal encodings,
		// or where encoding isn't better than emitting literals.

		// Check if we have any literals we must emit.
		if nextEmit != base {
			// Grab the literals we must emit before the match
			literals := src[nextEmit:base]

			// Check if we can fuse the literals with the copy
			// Second check can be omitted at a minor compression loss.
			canFuse := (len(literals) <= 3 || (offset <= 65535+64 && len(literals) <= 4)) && offset >= 64
			if canFuse {
				if offset <= 65535+64 {
					dst = emitCopyLits2(dst, literals, offset, length)
					// In cases where offset is <= 1024 and length is between 12 and 18 a literal+copy1 is 1 byte less.
					// The gain of adding this case it typically very small.
				} else {
					dst = emitCopy3(dst, offset, length, literals)
				}
				if debug {
					fmt.Println(base-len(literals), "Fused Copy - literals:", len(literals), "length:", length, "offset:", offset, "d-after:", len(dst))
				}
				// Set to 0, since we emitted the copy.
				length = 0
			} else {
				// Emit literals separately.
				// Bail if we will exceed the maximum size.
				// We will not exceed dstLimit with the other encodings.
				if len(dst)+len(literals) > dstLimit {
					return nil
				}
				dst = emitLiterals(dst, literals)
				if debug {
					fmt.Println(base-len(literals), "Literals:", len(literals), "d-after:", len(dst))
				}
			}
		}
		if length > 0 {
			if offset == repeat {
				dst = emitRepeat(dst, length)
			} else if offset <= 1024 {
				dst = emitCopy1(dst, offset, length)
			} else if offset <= 65535+64 {
				dst = emitCopy2(dst, offset, length)
			} else {
				dst = emitCopy3(dst, offset, length, nil)
			}
			if debug {
				fmt.Println(base, "Copy - length:", length, "offset:", offset, "d-after:", len(dst))
			}
		}
		// Update repeat value and next emit point
		repeat = offset
		nextEmit = s

		// Check our limits...
		if s > sLimit {
			goto emitRemainder
		}
		if len(dst) > dstLimit {
			// Do we have space for more, if not bail.
			return nil
		}

		// Index from base+1 to the end of match.
		// We don't need 'base' any more, so we can use that for indexing
		base++
		for base < s {
			h := hash4(binary.LittleEndian.Uint32(src[base:]), tableBits)
			table[h] = uint32(base)
			base++
		}
	}

emitRemainder:
	if nextEmit < len(src) {
		// Bail if we exceed the maximum size.
		if len(dst)+len(src)-nextEmit > dstLimit {
			return nil
		}
		dst = emitLiterals(dst, src[nextEmit:])
	}
	return dst
}

// hash4 returns the hash of the lowest 4 bytes of u to fit in a hash table with h bits.
// Preferably h should be a constant and should always be <32.
func hash4(u uint32, h uint8) uint32 {
	const prime4bytes = 2654435761
	return (u * prime4bytes) >> ((32 - h) & 31)
}

// emitLiterals will append a number of literals with encoding to dst.
func emitLiterals(dst, lits []byte) []byte {
	// 0-28: Length 1 -> 29
	// 29: Length (Read 1) + 1
	// 30: Length (Read 2) + 1
	// 31: Length (Read 3) + 1
	if len(lits) == 0 {
		return dst
	}

	const tagLiteral = 0

	// Emit length 1 -> 29
	if len(lits) < 30 {
		dst = append(dst, tagLiteral|uint8(len(lits)-1)<<3)
		return append(dst, lits...)
	}

	// For the remaining, we output length minus 30.
	n := uint32(len(lits)) - 30

	// Value 29 - One extra byte
	if n < 256 {
		dst = append(dst, tagLiteral|uint8(29)<<3)
		dst = append(dst, uint8(n))
		return append(dst, lits...)
	}

	// Value 30 - Two extra bytes
	if n < 65536 {
		var tmp [2]byte
		dst = append(dst, tagLiteral|uint8(30)<<3)
		binary.LittleEndian.PutUint16(tmp[:], uint16(n))
		dst = append(dst, tmp[:]...)
		return append(dst, lits...)
	}

	// Value 31, 3 extra bytes
	var tmp [4]byte
	dst = append(dst, tagLiteral|uint8(31)<<3)
	binary.LittleEndian.PutUint32(tmp[:], n)

	// Only add 3 lowest bytes.
	dst = append(dst, tmp[:3]...)
	return append(dst, lits...)
}

// emitRepeat writes a repeat chunk and returns the number of bytes written.
// Length must be at least 1.
func emitRepeat(dst []byte, length int) []byte {
	// Repeat offset, make length cheaper

	// The repeat tag is 0 (literals+repeat), plus bit 3 set.
	const tagRepeat = 0 | 4

	// Length is encoded as length - 1.
	length--
	if length < 29 {
		return append(dst, uint8(length)<<3|tagRepeat)
	}
	// The following subtracts 30 from length.
	length -= 29

	// Value 29 - One extra byte
	if length < 256 {
		dst = append(dst, (uint8(29)<<3)|tagRepeat)
		return append(dst, uint8(length))
	}

	// Value 30 - Two extra bytes
	if length < 65536 {
		var tmp [2]byte
		dst = append(dst, (uint8(30)<<3)|tagRepeat)
		binary.LittleEndian.PutUint16(tmp[:], uint16(length))
		return append(dst, tmp[:]...)
	}

	// Value 31, 3 extra bytes
	var tmp [4]byte
	dst = append(dst, (uint8(31)<<3)|tagRepeat)
	binary.LittleEndian.PutUint32(tmp[:], uint32(length))

	// Only add 3 lowest bytes.
	return append(dst, tmp[:3]...)
}

// emitCopy1 encodes a match with 10 bit offset. Length must be at least 4.
func emitCopy1(dst []byte, offset, length int) []byte {
	const tagCopy1 = 1

	// Minimum offset is 1
	offset--
	// See if we can store the length as-is.
	if length <= 18 {
		var tmp [2]byte
		x := uint16(offset<<6) | uint16(length-4)<<2 | tagCopy1
		binary.LittleEndian.PutUint16(tmp[:], x)
		return append(dst, tmp[:]...)
	}

	// Encode with 1 byte extended length...
	if length <= 273 {
		var tmp [3]byte
		x := uint16(offset<<6) | (uint16(15)<<2 | tagCopy1)
		binary.LittleEndian.PutUint16(tmp[:], x)
		tmp[2] = uint8(length - 18)
		return append(dst, tmp[:]...)
	}

	// Store as length 18 match + repeat the rest.
	var tmp [2]byte
	x := uint16(offset<<6) | uint16(14)<<2 | tagCopy1
	binary.LittleEndian.PutUint16(tmp[:], x)
	dst = append(dst, tmp[:]...)
	return emitRepeat(dst, length-18)
}

// emitCopy2 encodes a match with 16 bit offset. Length must be at least 4.
func emitCopy2(dst []byte, offset, length int) []byte {
	// Remove initial length of 4
	length -= 4
	if length < 0 {
		panic(fmt.Sprintf("invalid length %d", length))
	}

	const tagCopy2 = 2

	// Encode offset. Minimum offset is 64.
	offset -= 64
	var offsetEnc [2]byte
	offsetEnc[1] = uint8(offset >> 8)
	offsetEnc[0] = uint8(offset)

	if length <= 60 {
		dst = append(dst, uint8(length)<<2|tagCopy2)
		return append(dst, offsetEnc[:]...)
	}

	// The following subtracts further 60 from length.
	length -= 60

	// Value 61 - one byte length.
	if length < 256 {
		dst = append(dst, tagCopy2|uint8(61)<<2)
		dst = append(dst, offsetEnc[:]...)
		return append(dst, uint8(length))
	}

	// Value 62 - Two extra bytes length.
	if length < 65536 {
		var tmp [2]byte
		dst = append(dst, tagCopy2|uint8(62)<<2)
		dst = append(dst, offsetEnc[:]...)
		binary.LittleEndian.PutUint16(tmp[:], uint16(length))
		return append(dst, tmp[:]...)
	}

	// Value 63, 3 extra bytes length
	var tmp [4]byte
	dst = append(dst, tagCopy2|uint8(63)<<2)
	dst = append(dst, offsetEnc[:]...)
	binary.LittleEndian.PutUint32(tmp[:], uint32(length))

	// Only add 3 lowest bytes.
	return append(dst, tmp[:3]...)
}

// emitCopy3 encodes a match with 22 bit offset. Length must be at least 4.
func emitCopy3(dst []byte, offset, length int, lits []byte) []byte {
	length -= 4
	if length < 0 {
		panic(fmt.Sprintf("invalid length %d", length))
	}
	// Tag 3 with bit 3 set.
	const tagCopy3 = 3 | 4

	// Encode offset. Minimum offset is 65536.
	offset -= 65536

	// Add tag+copy3 bit at bottom.
	encoded := uint32(tagCopy3)
	// Add literal count
	encoded |= uint32(len(lits)) << 3
	// Add offset.
	encoded |= uint32(offset) << 11

	// Add 6 bit length and write encoded to output.
	var encodedBytes [4]byte
	if length <= 60 {
		encoded |= uint32(length) << 5
		binary.LittleEndian.PutUint32(encodedBytes[:], encoded)
		return append(append(dst, encodedBytes[:]...), lits...)
	}

	// The following subtracts further 60 from length (64 in total)
	length -= 60

	// Value 61 - one byte length.
	if length < 256 {
		encoded |= uint32(61) << 5
		binary.LittleEndian.PutUint32(encodedBytes[:], encoded)
		dst = append(dst, encodedBytes[:]...)
		return append(append(dst, uint8(length)), lits...)
	}

	// Value 62 - Two extra bytes length.
	if length < 65536 {
		encoded |= uint32(62) << 5
		binary.LittleEndian.PutUint32(encodedBytes[:], encoded)
		dst = append(dst, encodedBytes[:]...)
		var tmp [2]byte
		binary.LittleEndian.PutUint16(tmp[:], uint16(length))
		return append(append(dst, tmp[:]...), lits...)
	}

	// Value 63, 3 extra bytes length
	var tmp [4]byte
	encoded |= uint32(63) << 5
	binary.LittleEndian.PutUint32(encodedBytes[:], encoded)
	dst = append(dst, encodedBytes[:]...)
	binary.LittleEndian.PutUint32(tmp[:], uint32(length))

	// Only add 3 lowest bytes.
	return append(append(dst, tmp[:3]...), lits...)
}

// emitCopyLits2 encodes 1 to 4 literals and a match with 16 bit offset.
// Length must be at least 4.
func emitCopyLits2(dst, lits []byte, offset, length int) []byte {
	// Emit as literal + 2 byte offset code.
	// If longer than 11 use repeat for remaining.
	length -= 4

	// Encode offset. Minimum offset is 64.
	offset -= 64

	// Base tag is 3, with bit 3 un-set
	const tagCopyLits2 = 3

	// Longer than 11...
	// Append length 11 match, with the literal count.
	if length > 7 {
		dst = append(dst, tagCopyLits2|uint8(7<<5)|uint8(len(lits)-1)<<3)

		// Append offset
		var tmp [2]byte
		binary.LittleEndian.PutUint16(tmp[:], uint16(offset))
		dst = append(dst, tmp[:]...)

		// Append literals
		dst = append(dst, lits...)

		// Emit remaining as repeats
		return emitRepeat(dst, length-7)
	}

	// Add copy length and literal count.
	dst = append(dst, tagCopyLits2|uint8(length<<5)|uint8(len(lits)-1)<<3)

	// Append offset
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], uint16(offset))
	dst = append(dst, tmp[:]...)

	// Append literals
	return append(dst, lits...)
}
