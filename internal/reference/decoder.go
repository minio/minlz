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
	"errors"
	"fmt"
)

const maxBlockSize = 8 << 20

// DecodeBlock is a reference implementation of the MinLZ block decoder.
// This implementation is not optimized for speed, but for readability with no dependencies.
func DecodeBlock(src []byte) (dst []byte, err error) {
	// Print every operation.
	const debug = false

	if len(src) == 0 {
		return nil, errors.New("src length is zero")
	}
	// Check if first byte is 0.
	if src[0] != 0 {
		return nil, errors.New("first byte is not 0")
	}

	// If 0 is the only byte, this is a size 0 slice.
	if len(src) == 1 && src[0] == 0 {
		return []byte{}, nil
	}

	// Skip first byte
	src = src[1:]

	// Read expected length of uncompressed data.
	// This is uvarint (base 128) encoded.
	var wantSize int
	for i := uint(0); i < 100; i += 7 {
		if i == 7*10 {
			// Value exceeds 64 bits.
			return nil, fmt.Errorf("invalid destination size")
		}
		if len(src) == 0 {
			return nil, errors.New("unable to read length")
		}
		v := src[0]
		wantSize |= int(v&0x7f) << i
		if wantSize > maxBlockSize {
			return nil, fmt.Errorf("invalid destination size")
		}
		src = src[1:]
		if v&0x80 == 0 {
			break
		}
	}

	// Check if the destination size is valid.
	// Can be omitted when we control the uvarint reader as above.
	if wantSize < 0 || wantSize > maxBlockSize {
		return nil, fmt.Errorf("invalid destination size %d", wantSize)
	}

	// If the size is 0, return the remaining bytes as literals
	if wantSize == 0 {
		return src, nil
	}

	// The decompressed size (after removing the header)
	// must same or bigger than compressed size.
	if wantSize < len(src) {
		return nil, fmt.Errorf("decompressed smaller than compressed size %d", wantSize)
	}

	// Create output with capacity of wantSize.
	// We append output data to this slice as we are decoding.
	dst = make([]byte, 0, wantSize)

	// Reader helpers for common cases.
	// Read one byte from src.
	readOne := func() (v uint32, ok bool) {
		if len(src) >= 1 {
			v = uint32(src[0])
			src = src[1:]
			return v, true
		}
		return 0, false
	}

	// Read two bytes (little endian)
	readTwo := func() (v uint32, ok bool) {
		if len(src) >= 2 {
			v = uint32(src[0]) | uint32(src[1])<<8
			src = src[2:]
			return v, true
		}
		return 0, false
	}

	// Read 3 bytes.
	// 4 byte little endian without the top byte.
	readThree := func() (v uint32, ok bool) {
		if len(src) >= 3 {
			v = uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16
			src = src[3:]
			return v, true
		}
		return 0, false
	}

	// Read n bytes - will return false if not enough bytes are available.
	// The returned slice will be a sub-slice of src.
	readN := func(n uint32) (v []byte, ok bool) {
		if uint32(len(src)) >= n {
			v = src[:n]
			src = src[n:]
			return v, true
		}
		return nil, false
	}

	// checkDstSize will return whether n will fit within the destination.
	checkDstSize := func(n uint32) (ok bool) {
		return n < maxBlockSize && len(dst)+int(n) <= wantSize
	}

	// Offset is retained between operations and initialized to 1.
	// This is used for repeat offsets.
	var offset = uint32(1)

	// While we have input left.
	for len(src) > 0 {
		v, ok := readOne()
		if !ok {
			break
		}
		// Separate tag (lower 2 bits) and value (upper 6 bits)
		tag := v & 3
		value := v >> 2
		var length uint32
		switch tag {
		// Literal/repeat tag
		case 0:
			isRepeat := value&1 != 0

			// Decode length
			value = value >> 1
			switch {
			case value < 29:
				// Length is in value
				length = value + 1
			case value == 29:
				// 1 byte length
				length, ok = readOne()
				if !ok {
					return nil, fmt.Errorf("lit tag 29: unable to read length at dst pos %d", len(dst))
				}
				// Add base offset
				length += 30
			case value == 30:
				// 2 byte length
				length, ok = readTwo()
				if !ok {
					return nil, fmt.Errorf("lit tag 30: unable to read length at dst pos %d", len(dst))
				}
				length += 30
			case value == 31:
				// 3 byte length
				length, ok = readThree()
				if !ok {
					return nil, fmt.Errorf("lit tag 31: unable to read length at dst pos %d", len(dst))
				}
				length += 30
			}

			// If repeat, break to copy.
			if isRepeat {
				// Copy with set length from repeat address
				break
			}

			if debug {
				fmt.Println("literals, length:", length, "d-after:", uint32(len(dst))+length)
			}

			// Check if we have enough output space.
			if !checkDstSize(length) {
				return nil, fmt.Errorf("literal length %d exceed destination at dst pos %d", length, len(dst))
			}

			// Get input from source
			var input []byte
			input, ok = readN(length)
			if !ok {
				return nil, fmt.Errorf("literal length %d exceed source at dst pos %d", length, len(dst))
			}
			dst = append(dst, input...)
			continue

		case 1:
			// Copy with 1 byte extra offset
			length = value & 15
			offset, ok = readOne()
			if !ok {
				return nil, fmt.Errorf("copy 1: unable to read offset at dst pos %d", len(dst))
			}
			// Combine offset part of value with 8 bytes read.
			offset = offset<<2 | (value >> 4)
			if length == 15 {
				length, ok = readOne()
				if !ok {
					return nil, fmt.Errorf("copy 1: unable to read length at dst pos %d", len(dst))
				}
				length += 18
			} else {
				length += 4
			}
			// Minimum offset is 1
			offset++

		case 2:
			// Copy with 2 byte offset.

			// Read offset
			offset, ok = readTwo()
			if !ok {
				return nil, fmt.Errorf("copy 2: unable to read offset at dst pos %d", len(dst))
			}

			// Resolve length
			switch {
			case value <= 60:
				length = value + 4
			case value == 61:
				// 1 byte + 64
				length, ok = readOne()
				if !ok {
					return nil, fmt.Errorf("copy 2.61: unable to read length at dst pos %d", len(dst))
				}
				length += 64
			case value == 62:
				// 2 bytes + 64
				length, ok = readTwo()
				if !ok {
					return nil, fmt.Errorf("copy 2.62: unable to read length at dst pos %d", len(dst))
				}
				length += 64
			case value == 63:
				// 3 bytes + 64
				length, ok = readThree()
				if !ok {
					return nil, fmt.Errorf("copy 2.63: unable to read length at dst pos %d", len(dst))
				}
				length += 64
			}

			// Minimum offset is 64
			offset += 64

		case 3:
			// Fused Copy2 or Copy3

			// If bit 3 is set this is a copy3 operation.
			isCopy3 := value&1 == 1

			// Read literal count. Same for both
			litLen := value >> 1 & 3

			if !isCopy3 {
				// Fused copy2, length 4 -> 11.
				offset, ok = readTwo()
				if !ok {
					return nil, fmt.Errorf("copy 2, fused: unable to read offset at dst pos %d", len(dst))
				}

				// Extract length
				length = (value >> 3) + 4

				// literal length is minimum 1.
				litLen++

				// Add minimum offset.
				offset += 64
			} else {
				// Copy3, optionally fused.
				// Read rest of value.
				v2, ok := readThree()
				if !ok {
					return nil, fmt.Errorf("copy 3: unable to read value at dst pos %d", len(dst))
				}
				// Merge top half in, so we have entire value
				value = value | v2<<6

				// Extract offset. Top 22 bits, plus minimum offset.
				offset = (value >> 9) + 65536

				// Value for length.
				value = (value >> 3) & 63

				// Read extended length.
				switch {
				case value < 61:
					length = value + 4
				case value == 61:
					length, ok = readOne()
					if !ok {
						return nil, fmt.Errorf("copy 3.29: unable to read length at dst pos %d", len(dst))
					}
					length += 64
				case value == 62:
					length, ok = readTwo()
					if !ok {
						return nil, fmt.Errorf("copy 3.30: unable to read length at dst pos %d", len(dst))
					}
					length += 64
				case value == 63:
					length, ok = readThree()
					if !ok {
						return nil, fmt.Errorf("copy 3.31: unable to read length at dst pos %d", len(dst))
					}
					length += 64
				}
			}

			if litLen > 0 {
				// Read literals from input.
				input, ok := readN(litLen)
				if !ok {
					return nil, fmt.Errorf("copy 3: unable to read extra literals at dst pos %d", len(dst))
				}
				// Add them before copy.
				if !checkDstSize(litLen) {
					return nil, fmt.Errorf("copy 3: extra literal output size exceeded at dst pos %d", len(dst))
				}
				dst = append(dst, input...)
			}
		}

		if debug {
			fmt.Println("copy, length:", length, "offset:", offset, "d-after:", uint32(len(dst))+length)
		}

		// All paths have filled length & offset - execute copy.
		if !checkDstSize(length) {
			return nil, fmt.Errorf("copy length %d exceeds dst size at dst pos %d", length, len(dst))
		}
		if offset > uint32(len(dst)) {
			return nil, fmt.Errorf("copy offset %d exceeds dst size %d", offset, len(dst))
		}

		// Calculate input position
		inPos := uint32(len(dst) - int(offset))

		// Simple copy that will work with overlaps.
		for i := uint32(0); i < length; i++ {
			dst = append(dst, dst[inPos+i])
		}
	}
	if len(dst) != wantSize {
		return nil, fmt.Errorf("mismatching output size, got %d, want %d", len(dst), wantSize)
	}
	return dst, nil
}
