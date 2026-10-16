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
	"archive/zip"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/minio/minlz/internal/reference"
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

// decodeBlockBenchInputs returns the blocks BenchmarkDecodeBlock decodes.
// They are encoded with internal/reference so every build, with or without
// assembly, decodes the same bytes.
//
// The random_* inputs are stored blocks, so they time the stored-block copy
// rather than the decode loop. Use text_* and compressible to compare
// decoders.
func decodeBlockBenchInputs(tb testing.TB) (names []string, data, encoded [][]byte) {
	add := func(name string, d []byte) {
		e, err := reference.EncodeBlock(d)
		if err != nil {
			tb.Fatal(err)
		}
		names = append(names, name)
		data = append(data, d)
		encoded = append(encoded, e)
	}
	sizes := []int{1000, 10000, 100000, 1000000}
	rng := rand.New(rand.NewSource(0))
	for _, size := range sizes {
		d := make([]byte, size)
		rng.Read(d)
		add(fmt.Sprintf("random_%d", size), d)
	}
	text := readFile(tb, "testdata/Mark.Twain-Tom.Sawyer.txt")
	for _, size := range sizes {
		add(fmt.Sprintf("text_%d", size), expand(text, size))
	}
	add("compressible", bytes.Repeat([]byte("abcdefghij"), 100000))
	return names, data, encoded
}

// TestDecodeBlockBenchInputs checks that the benchmark inputs decode, and
// that the text and compressible ones are compressed rather than stored.
func TestDecodeBlockBenchInputs(t *testing.T) {
	names, data, encoded := decodeBlockBenchInputs(t)
	for i, name := range names {
		got, err := Decode(nil, encoded[i])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, data[i]) {
			t.Fatalf("%s: decode mismatch", name)
		}
		if strings.HasPrefix(name, "text_") && len(encoded[i]) >= len(data[i]) {
			t.Fatalf("%s: encoded to %d bytes, want a compressed block", name, len(encoded[i]))
		}
		if name == "compressible" && len(encoded[i]) > len(data[i])/100 {
			t.Fatalf("%s: encoded to %d bytes, want a compressed block", name, len(encoded[i]))
		}
	}
}

// BenchmarkDecodeBlock benchmarks the decoder.
func BenchmarkDecodeBlock(b *testing.B) {
	names, data, encoded := decodeBlockBenchInputs(b)
	for i, name := range names {
		dst := make([]byte, len(data[i]))
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(data[i])))
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				Decode(dst, encoded[i])
			}
		})
	}
}

// overlapTailBlock constructs checked tail matches with an exact expected output.
func overlapTailBlock(offset, length, trailing int) (encoded, want []byte) {
	// A preceding match keeps even short final copies smaller than raw input.
	const prefixMatch = 64
	literal := make([]byte, offset)
	for i := range literal {
		literal[i] = byte(i*173 + i/97)
	}
	want = make([]byte, offset+prefixMatch+length+trailing)
	for i := range want[:offset+prefixMatch+length] {
		want[i] = literal[i%offset]
	}
	for i := range want[offset+prefixMatch+length:] {
		want[offset+prefixMatch+length+i] = byte(i*29 + 7)
	}
	payload := make([]byte, MaxEncodedLen(len(want))+16)
	n := emitLiteral(payload, literal)
	n += emitCopy(payload[n:], offset, prefixMatch)
	n += emitCopy(payload[n:], offset, length)
	if trailing != 0 {
		n += emitLiteral(payload[n:], want[offset+prefixMatch+length:])
	}
	encoded = binary.AppendUvarint([]byte{0}, uint64(len(want)))
	return append(encoded, payload[:n]...), want
}

// TestDecodeOverlapTail checks copy-width boundaries and exact-capacity destination guards.
func TestDecodeOverlapTail(t *testing.T) {
	offsets := []int{1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 255, 256, 1023, 1024, 4095, 4096, 65535, 65536, 65537}
	lengths := []int{4, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 1023, 1024, 4096}
	for _, offset := range offsets {
		for _, length := range append(append([]int(nil), lengths...), 7*offset+1, 7*offset+3) {
			for _, trailing := range []int{0, 1, 32} {
				t.Run(fmt.Sprintf("offset%d/length%d/trailing%d", offset, length, trailing), func(t *testing.T) {
					encoded, want := overlapTailBlock(offset, length, trailing)
					storage := bytes.Repeat([]byte{0xa5}, len(want)+64)
					dst := storage[32 : 32+len(want) : 32+len(want)]
					got, err := Decode(dst, encoded)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("decode: %v; equal=%v", err, bytes.Equal(got, want))
					}
					if !bytes.Equal(storage[:32], bytes.Repeat([]byte{0xa5}, 32)) || !bytes.Equal(storage[32+len(want):], bytes.Repeat([]byte{0xa5}, 32)) {
						t.Fatal("decoder wrote outside the destination")
					}
				})
			}
		}
	}
}

// TestDecodeOverlapTailCorrupt checks length mismatches and truncated matches.
func TestDecodeOverlapTailCorrupt(t *testing.T) {
	for _, offset := range []int{1, 3, 15, 16, 17, 63, 64, 65, 1024, 65536} {
		encoded, want := overlapTailBlock(offset, 7*offset+1, 0)
		_, header := binary.Uvarint(encoded[1:])
		for _, delta := range []int{-1, 1} {
			bad := binary.AppendUvarint([]byte{0}, uint64(len(want)+delta))
			bad = append(bad, encoded[1+header:]...)
			if _, err := Decode(nil, bad); err != ErrCorrupt {
				t.Fatalf("offset=%d delta=%d: got %v, want ErrCorrupt", offset, delta, err)
			}
		}
		for cut := 1; cut <= 3; cut++ {
			if _, err := Decode(nil, encoded[:len(encoded)-cut]); err != ErrCorrupt {
				t.Fatalf("offset=%d cut=%d: got %v, want ErrCorrupt", offset, cut, err)
			}
		}
	}
}

// TestDecodeOverlapTailLarge checks overlapping copies at the maximum block size.
func TestDecodeOverlapTailLarge(t *testing.T) {
	for _, offset := range []int{1, 3, 16, 17, 63, 64, 65, 4096, 65536} {
		encoded, want := overlapTailBlock(offset, MaxBlockSize-offset-64, 0)
		got, err := Decode(nil, encoded)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("offset=%d: decode %v; equal=%v", offset, err, bytes.Equal(got, want))
		}
	}
}

// reindexerDecodeBlocks loads actual C-encoded CPROTO replies from Reindexer integration tests.
// The archive includes source URLs, capture settings, checksums, and the source license.
func reindexerDecodeBlocks(tb testing.TB) (names []string, encoded, decoded [][]byte) {
	tb.Helper()
	r, err := zip.OpenReader("testdata/reindexer-cproto.zip")
	if err != nil {
		tb.Fatal(err)
	}
	defer r.Close()
	for _, f := range r.File {
		if !strings.HasSuffix(f.Name, ".mzb") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			tb.Fatal(err)
		}
		block, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			tb.Fatal(err)
		}
		want, err := reference.DecodeBlock(block)
		if err != nil {
			tb.Fatalf("%s: reference decode: %v", f.Name, err)
		}
		if len(want) < 2 || want[0] != 0 || want[1] != 0 {
			tb.Fatalf("%s: captured RPC returned an error", f.Name)
		}
		if strings.HasPrefix(f.Name, "field1m-") {
			payload := bytes.Repeat([]byte("cproto payload-"), ((1<<20)+14)/15)[:1<<20]
			if !bytes.Contains(want, payload) {
				tb.Fatalf("%s: stored field was not restored", f.Name)
			}
		}
		names = append(names, strings.TrimSuffix(f.Name, ".mzb"))
		encoded = append(encoded, block)
		decoded = append(decoded, want)
	}
	if len(names) != 8 {
		tb.Fatalf("captured blocks: got %d, want 8", len(names))
	}
	return names, encoded, decoded
}

// TestDecodeReindexer checks captured CPROTO replies against the reference decoder.
func TestDecodeReindexer(t *testing.T) {
	names, encoded, decoded := reindexerDecodeBlocks(t)
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			want := decoded[i]
			storage := bytes.Repeat([]byte{0xa5}, len(want)+64)
			got, err := Decode(storage[32:32+len(want):32+len(want)], encoded[i])
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("decode: %v; equal=%v", err, bytes.Equal(got, want))
			}
			if !bytes.Equal(storage[:32], bytes.Repeat([]byte{0xa5}, 32)) || !bytes.Equal(storage[32+len(want):], bytes.Repeat([]byte{0xa5}, 32)) {
				t.Fatal("decoder wrote outside the destination")
			}
		})
	}
}
