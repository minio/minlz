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

//go:build (amd64 || arm64) && !appengine && !noasm && gc && !purego

package minlz

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"testing"
)

// TestCompareDecoders compares the Go and ASM decoders
func TestCompareDecoders(t *testing.T) {
	// Test data that triggers specific patterns
	testCases := []struct {
		name string
		data []byte
	}{
		{"small", []byte("hello world")},
		{"repeated_small", bytes.Repeat([]byte("abcd"), 100)},
		{"repeated_large", bytes.Repeat([]byte("abcdefghijklmnop"), 10000)},
		{"random_like", generateTestData(100000)},
		{"very_random", generateTestData(500000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testDecoderComparison(t, tc.data)
		})
	}
}

// TestCompareDecodersWithRegressionData uses actual regression test data
func TestCompareDecodersWithRegressionData(t *testing.T) {
	data, err := os.ReadFile("testdata/enc_regressions.zip")
	if err != nil {
		t.Skip("enc_regressions.zip not found")
		return
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range zr.File {
		t.Run(f.Name, func(t *testing.T) {
			r, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			fileData, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			testDecoderComparison(t, fileData)
		})
	}
}

// testDecoderComparison compares ASM and Go decoder outputs.
func testDecoderComparison(t *testing.T, data []byte) {
	for level := LevelFastest; level <= LevelSmallest; level++ {
		levelName := []string{"fastest", "balanced", "smallest"}[level-1]
		t.Run(levelName, func(t *testing.T) {
			encoded, err := Encode(nil, data, level)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Get decoded length
			_, _, block, dLen, err := isMinLZ(encoded)
			if err != nil {
				t.Fatalf("isMinLZ failed: %v", err)
			}

			// Decode with ASM
			dstAsm := make([]byte, dLen)
			errAsm := decodeBlockAsm(dstAsm, block)

			// Decode with Go
			dstGo := make([]byte, dLen)
			errGo := minLZDecodeGo(dstGo, block)

			if errAsm != errGo {
				t.Errorf("error mismatch: ASM=%d, Go=%d", errAsm, errGo)
				if errAsm != 0 && errGo == 0 {
					t.Logf("ASM decoder returned error, Go decoder succeeded")
					t.Logf("Block size: %d, decoded size: %d", len(block), dLen)
					binarySearchFailure(t, block, dLen)
				} else if errAsm == 0 && errGo == 1 {
					t.Logf("Go decoder returned error, ASM decoder succeeded - unexpected")
				}
				return
			}

			if errAsm == 0 && !bytes.Equal(dstAsm, dstGo) {
				for i := range dstAsm {
					if i >= len(dstGo) || dstAsm[i] != dstGo[i] {
						t.Errorf("content mismatch at byte %d: ASM=%02x, Go=%02x",
							i, dstAsm[i], dstGo[i])

						start := i - 20
						if start < 0 {
							start = 0
						}
						end := i + 20
						if end > len(dstGo) {
							end = len(dstGo)
						}
						t.Logf("Go output around mismatch [%d:%d]: %x", start, end, dstGo[start:end])
						t.Logf("ASM output around mismatch [%d:%d]: %x", start, end, dstAsm[start:end])

						mismatchCount := 0
						firstMismatch := i
						lastMismatch := i
						for j := i; j < len(dstAsm) && j < len(dstGo); j++ {
							if dstAsm[j] != dstGo[j] {
								mismatchCount++
								lastMismatch = j
							}
						}
						t.Logf("Total mismatches from byte %d onwards: %d (last at %d)", i, mismatchCount, lastMismatch)

						if mismatchCount == 4 && lastMismatch-firstMismatch == 3 {
							t.Logf("This looks like a 2-byte pattern swap!")
						}

						t.Logf("Encoded block size: %d bytes", len(block))
						break
					}
				}
			}
		})
	}
}

// binarySearchFailure finds the minimum prefix of the block that causes ASM to fail
func binarySearchFailure(t *testing.T, block []byte, fullDLen int) {
	t.Helper()

	dstAsm := make([]byte, fullDLen)
	if decodeBlockAsm(dstAsm, block) == 0 {
		t.Log("Full block actually passes ASM decoder")
		return
	}

	t.Logf("Searching for failure point in %d byte block...", len(block))

	if len(block) > 40 {
		margin := len(block) - 20
		t.Logf("Bytes near fast/slow boundary (pos %d): ...%x...", margin-10, block[margin-10:margin+10])
	}

	if len(block) > 50 {
		t.Logf("Last 50 bytes: %x", block[len(block)-50:])
	}
}

// TestSpecificTagSequences tests specific tag patterns that might trigger bugs
func TestSpecificTagSequences(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"large_offset_copy2", makeLargeOffsetData(100000, 65000)},
		{"large_offset_copy3", makeLargeOffsetData(200000, 100000)},
		{"fused_lits_small", makeFusedLitData(10000)},
		{"fused_lits_large", makeFusedLitData(100000)},
		{"long_literals", makeLongLiteralData(100000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testDecoderComparison(t, tc.data)
		})
	}
}

func makeLargeOffsetData(size, minOffset int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	pattern := []byte("LARGEPAT")
	if minOffset < size-len(pattern)*2 {
		copy(data[0:], pattern)
		copy(data[minOffset:], pattern)
	}
	return data
}

func makeFusedLitData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 3) % 256)
	}
	pattern := []byte("ABCD")
	for i := 100; i < size-10; i += 500 {
		copy(data[i:], pattern)
		data[i+len(pattern)] = byte(i % 256)
		data[i+len(pattern)+1] = byte((i + 1) % 256)
	}
	return data
}

func makeLongLiteralData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*17 + i*i) % 256)
	}
	return data
}

// TestOffset2Pattern tests the specific pattern swap issue
func TestOffset2Pattern(t *testing.T) {
	sizes := []int{65549, 70000, 60000, 50000}

	for _, size := range sizes {
		t.Run("size_"+string(rune('0'+size/10000)), func(t *testing.T) {
			data := make([]byte, size)
			for i := range data {
				if i%2 == 0 {
					data[i] = 0x35
				} else {
					data[i] = 0x7a
				}
			}
			for i := 1000; i < size-100; i += 3000 {
				copy(data[i:], []byte("UNIQUE_MARKER"))
			}

			testDecoderComparison(t, data)
		})
	}
}

// TestShortRepeats tests short repeat patterns (offset 1-4)
func TestShortRepeats(t *testing.T) {
	testCases := []struct {
		name   string
		offset int
		length int
	}{
		{"offset1_len4", 1, 4},
		{"offset2_len4", 2, 4},
		{"offset2_len10", 2, 10},
		{"offset3_len9", 3, 9},
		{"offset4_len16", 4, 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, 10000)
			for i := range data {
				data[i] = byte((i * 7) % 256)
			}
			pattern := make([]byte, tc.offset)
			for i := range pattern {
				pattern[i] = byte('A' + i)
			}
			copy(data[1000:], pattern)
			for i := 0; i < tc.length; i++ {
				data[1000+tc.offset+i] = pattern[i%tc.offset]
			}

			testDecoderComparison(t, data)
		})
	}
}

// TestLargeDataWith35_7aPattern tests the specific pattern seen in s2fail.txt
func TestLargeDataWith35_7aPattern(t *testing.T) {
	sizes := []int{65549, 66000, 60000}
	for _, size := range sizes {
		t.Run("size_"+string(rune('0'+size/10000)), func(t *testing.T) {
			data := make([]byte, size)

			for i := range data {
				switch i % 10 {
				case 0:
					data[i] = '3'
				case 1:
					data[i] = '5'
				case 2, 3:
					data[i] = 'z'
				case 4:
					data[i] = '1'
				default:
					data[i] = byte('0' + (i % 10))
				}
			}

			for i := 0; i < size-20; i += 3000 {
				copy(data[i:], []byte("UNIQUE_MARKER_"))
			}

			testDecoderComparison(t, data)
		})
	}
}

// TestVeryLargeOffset2 tests offset-2 copies at various large positions
func TestVeryLargeOffset2(t *testing.T) {
	positions := []int{50000, 55000, 56000, 56500, 56600, 56620, 57000}

	for _, pos := range positions {
		t.Run("pos_"+string(rune('0'+pos/10000)), func(t *testing.T) {
			size := pos + 1000
			data := make([]byte, size)

			for i := range data {
				data[i] = byte((i*13 + i/7) % 256)
			}

			data[pos-100] = 0x35
			data[pos-99] = 0x7a
			data[pos-98] = 0x35
			data[pos-97] = 0x7a
			data[pos-96] = 0x35
			data[pos-95] = 0x7a

			testDecoderComparison(t, data)
		})
	}
}

func generateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*7 + i/13) % 256)
	}
	pattern := []byte("PATTERN_DATA_HERE")
	for i := 1000; i < size-len(pattern); i += 5000 {
		copy(data[i:], pattern)
	}
	return data
}

// TestSrcMarginBoundary tests the source buffer margin boundary.
// This is a regression test for a security issue where the fast loop
// could over-read the source buffer when a 4-byte tag header was consumed
// just before a 32-byte NEON load, requiring 36-byte margin instead of 32.
func TestSrcMarginBoundary(t *testing.T) {
	// Test various small sizes that stress the margin boundary
	// The fast loop requires srcLen >= 36 bytes (32 for NEON + 4 for tag header)
	sizes := []int{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 50, 60, 70, 80}

	for _, size := range sizes {
		t.Run("size_"+string(rune('0'+size/10))+string(rune('0'+size%10)), func(t *testing.T) {
			// Test with various data patterns
			patterns := [][]byte{
				bytes.Repeat([]byte{'a'}, size),                          // compressible
				bytes.Repeat([]byte{'a', 'b'}, size/2+1)[:size],          // 2-byte pattern
				bytes.Repeat([]byte{'a', 'b', 'c', 'd'}, size/4+1)[:size], // 4-byte pattern
			}

			// Add random-like pattern
			random := make([]byte, size)
			for i := range random {
				random[i] = byte((i*17 + i*i) % 256)
			}
			patterns = append(patterns, random)

			for i, data := range patterns {
				for level := LevelFastest; level <= LevelSmallest; level++ {
					encoded, err := Encode(nil, data, level)
					if err != nil {
						t.Fatalf("pattern %d, level %d: Encode failed: %v", i, level, err)
					}

					// Get block info
					_, _, block, dLen, err := isMinLZ(encoded)
					if err != nil {
						t.Fatalf("pattern %d, level %d: isMinLZ failed: %v", i, level, err)
					}

					// Decode with ASM
					dstAsm := make([]byte, dLen)
					errAsm := decodeBlockAsm(dstAsm, block)

					// Decode with Go
					dstGo := make([]byte, dLen)
					errGo := minLZDecodeGo(dstGo, block)

					if errAsm != errGo {
						t.Errorf("pattern %d, level %d: error mismatch: ASM=%d, Go=%d", i, level, errAsm, errGo)
						continue
					}

					if errAsm == 0 && !bytes.Equal(dstAsm, dstGo) {
						t.Errorf("pattern %d, level %d: content mismatch", i, level)
					}

					if errAsm == 0 && !bytes.Equal(dstAsm, data) {
						t.Errorf("pattern %d, level %d: decoded data doesn't match original", i, level)
					}
				}
			}
		})
	}
}
