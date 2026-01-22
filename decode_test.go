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
	"bytes"
	"math/rand"
	"testing"
)

// TestDecodeBlock tests the decoder with various data patterns.
func TestDecodeBlock(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small_literal", []byte("hello world")},
		{"repeated", bytes.Repeat([]byte("abcd"), 1000)},
		{"mixed", append(bytes.Repeat([]byte("x"), 100), bytes.Repeat([]byte("y"), 100)...)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for level := LevelFastest; level <= LevelSmallest; level++ {
				encoded, err := Encode(nil, tc.data, level)
				if err != nil {
					t.Fatalf("Encode failed: %v", err)
				}

				decoded, err := Decode(nil, encoded)
				if err != nil {
					t.Fatalf("Decode failed: %v", err)
				}

				if !bytes.Equal(tc.data, decoded) {
					t.Errorf("level %d: decode mismatch: got %d bytes, want %d bytes",
						level, len(decoded), len(tc.data))
				}
			}
		})
	}
}

// TestDecodeBlockRandom tests with random data of various sizes.
func TestDecodeBlockRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	sizes := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	if testing.Short() {
		sizes = sizes[:5]
	}

	for _, size := range sizes {
		t.Run("size_"+string(rune('0'+size%10)), func(t *testing.T) {
			data := make([]byte, size)
			rng.Read(data)

			for level := LevelFastest; level <= LevelSmallest; level++ {
				encoded, err := Encode(nil, data, level)
				if err != nil {
					t.Fatalf("Encode failed: %v", err)
				}

				decoded, err := Decode(nil, encoded)
				if err != nil {
					t.Fatalf("Decode failed for size %d, level %d: %v", size, level, err)
				}

				if !bytes.Equal(data, decoded) {
					t.Errorf("size %d, level %d: decode mismatch", size, level)
				}
			}
		})
	}
}

// TestDecodeBlockOverlapping tests overlapping copy scenarios.
func TestDecodeBlockOverlapping(t *testing.T) {
	// Test various RLE-like patterns that exercise overlapping copies
	patterns := [][]byte{
		bytes.Repeat([]byte{'a'}, 1000),                              // offset 1 RLE
		bytes.Repeat([]byte{'a', 'b'}, 500),                          // offset 2 pattern
		bytes.Repeat([]byte{'a', 'b', 'c'}, 333),                     // offset 3 pattern
		bytes.Repeat([]byte{'a', 'b', 'c', 'd'}, 250),                // offset 4 pattern
		bytes.Repeat([]byte{'a', 'b', 'c', 'd', 'e', 'f', 'g'}, 143), // offset 7 pattern
	}

	for i, pattern := range patterns {
		t.Run("pattern_"+string(rune('0'+i)), func(t *testing.T) {
			for level := LevelFastest; level <= LevelSmallest; level++ {
				encoded, err := Encode(nil, pattern, level)
				if err != nil {
					t.Fatalf("Encode failed: %v", err)
				}

				decoded, err := Decode(nil, encoded)
				if err != nil {
					t.Fatalf("Decode failed: %v", err)
				}

				if !bytes.Equal(pattern, decoded) {
					t.Errorf("pattern %d, level %d: decode mismatch", i, level)
				}
			}
		})
	}
}

// TestDecodeBlockLongOffsets tests long offset copies (Copy2/Copy3).
func TestDecodeBlockLongOffsets(t *testing.T) {
	// Create data with patterns at various offsets
	size := 200000
	data := make([]byte, size)
	rng := rand.New(rand.NewSource(123))

	// Fill with semi-random data
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}

	// Insert repeated patterns at various offsets to trigger Copy2/Copy3
	pattern := []byte("REPEATED_PATTERN_DATA")
	offsets := []int{100, 1000, 10000, 65000, 100000}
	for _, off := range offsets {
		if off+len(pattern) < size {
			copy(data[off:], pattern)
			// Copy the pattern later to trigger backreference
			if off*2+len(pattern) < size {
				copy(data[off*2:], pattern)
			}
		}
	}

	for level := LevelFastest; level <= LevelSmallest; level++ {
		t.Run("level_"+string(rune('0'+level)), func(t *testing.T) {
			encoded, err := Encode(nil, data, level)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := Decode(nil, encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			if !bytes.Equal(data, decoded) {
				t.Errorf("level %d: decode mismatch", level)
			}
		})
	}
}

// BenchmarkDecodeBlock benchmarks the decoder.
func BenchmarkDecodeBlock(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		data := make([]byte, size)
		rand.Read(data)

		encoded, _ := Encode(nil, data, LevelFastest)
		dst := make([]byte, size)

		b.Run("random_"+string(rune('0'+size%10)), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Decode(dst, encoded)
			}
		})
	}

	// Benchmark highly compressible data
	data := bytes.Repeat([]byte("abcdefghij"), 100000)
	encoded, _ := Encode(nil, data, LevelFastest)
	dst := make([]byte, len(data))

	b.Run("compressible", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Decode(dst, encoded)
		}
	})
}
