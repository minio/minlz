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

package reference

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"slices"
	"testing"

	"github.com/minio/minlz"
	"github.com/zeebo/xxh3"
)

// streamTestData returns text followed by random bytes, which are stored uncompressed.
func streamTestData() []byte {
	rng := rand.New(rand.NewSource(1))
	b := make([]byte, 0, 300<<10)
	for len(b) < 200<<10 {
		b = fmt.Appendf(b, "line %d: %x\n", len(b), rng.Intn(1<<12))
	}
	b = b[:200<<10]
	tail := make([]byte, 100<<10)
	rng.Read(tail)
	return append(b, tail...)
}

func TestReadStream(t *testing.T) {
	data := streamTestData()
	var all []byte
	for _, level := range []int{
		minlz.LevelUncompressed, minlz.LevelSuperFast, minlz.LevelFastest, minlz.LevelBalanced, minlz.LevelSmallest,
		minlz.LevelSuperFastSIMD, minlz.LevelFastestSIMD, minlz.LevelBalancedSIMD, minlz.LevelSmallestSIMD,
		minlz.LevelSuperFastSIMD | minlz.LevelSIMDDelta, minlz.LevelFastestSIMD | minlz.LevelSIMDDelta,
		minlz.LevelBalancedSIMD | minlz.LevelSIMDDelta,
	} {
		var buf bytes.Buffer
		w := minlz.NewWriter(&buf, minlz.WriterLevel(level), minlz.WriterBlockSize(64<<10),
			minlz.WriterAddIndex(true), minlz.WriterPadding(4096))
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := ReadStream(bytes.NewReader(buf.Bytes()), nil); err != nil {
			t.Errorf("level %d: %v", level, err)
		}
		all = append(all, buf.Bytes()...)
	}
	if err := ReadStream(bytes.NewReader(all), nil); err != nil {
		t.Errorf("concatenated: %v", err)
	}
}

func TestReadStreamReject(t *testing.T) {
	data := streamTestData()[:64<<10]
	chunk := func(typ byte, payload ...[]byte) []byte {
		p := slices.Concat(payload...)
		return append([]byte{typ, byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16)}, p...)
	}
	const info, flag = 6, 1 << 6 // 64 KiB blocks
	id := func(info byte) []byte { return chunk(ChunkStreamID, []byte("MinLz"), []byte{info}) }
	eof := chunk(ChunkEOF, binary.AppendUvarint(nil, uint64(len(data))))
	block := func(level int) []byte {
		b, err := minlz.Encode(nil, data, level)
		if err != nil {
			t.Fatal(err)
		}
		return b[1:]
	}
	lz, simd := block(minlz.LevelBalanced), block(minlz.LevelBalancedSIMD)
	maskedCRC := func(b []byte) []byte {
		c := crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))
		return binary.LittleEndian.AppendUint32(nil, c>>15|c<<17+0xa282ead8)
	}
	crc := maskedCRC(data)
	emptyCRC := binary.LittleEndian.AppendUint32(nil, 0xa282ead8) // masked CRC of no data
	hash := binary.LittleEndian.AppendUint64(nil, xxh3.Hash(data))
	badHash := slices.Clone(hash)
	badHash[0] ^= 1
	lzStream := slices.Concat(id(info), chunk(ChunkMinLZBlock, crc, lz), eof)
	simdStream := slices.Concat(id(info|flag), chunk(ChunkMinLZSIMD, hash, simd), eof)
	// A stream of a literal "x" and a repeat of n bytes, which take 3 bytes after the size header.
	repeat := func(n byte) []byte {
		out := bytes.Repeat([]byte("x"), 1+int(n))
		block := []byte{byte(len(out)), 0, 'x', (n-1)<<3 | 4}
		return slices.Concat(id(info), chunk(ChunkMinLZBlock, maskedCRC(out), block), chunk(ChunkEOF, []byte{byte(len(out))}))
	}

	for _, tc := range []struct {
		name   string
		stream []byte
		ok     bool
	}{
		{"minlz", lzStream, true},
		{"simd", simdStream, true},
		{"concatenated", slices.Concat(lzStream, simdStream, lzStream), true},
		{"empty stream after a stream", slices.Concat(lzStream, chunk(ChunkEOF, []byte{0})), true},
		{"stream after an empty stream", slices.Concat(chunk(ChunkEOF, []byte{0}), lzStream), true},
		{"two empty streams", slices.Concat(chunk(ChunkEOF, []byte{0}), chunk(ChunkEOF, []byte{0})), true},
		{"decoded size equals compressed size", repeat(2), true},
		{"identifier resets the size", slices.Concat(id(info), chunk(ChunkMinLZBlock, crc, lz), lzStream), true},
		{"bits 4-5 ignored", slices.Concat(id(info|flag|0x30), chunk(ChunkMinLZSIMD, hash, simd), eof), true},
		{"skippable chunks", slices.Concat(id(info), chunk(0x41, []byte{1}), chunk(0x80, []byte{2}),
			chunk(ChunkPadding, []byte{3}), chunk(ChunkMinLZBlock, crc, lz), eof), true},
		{"simd without flag", slices.Concat(id(info), chunk(ChunkMinLZSIMD, hash, simd), eof), false},
		{"flag of previous stream", slices.Concat(simdStream, id(info), chunk(ChunkMinLZSIMD, hash, simd), eof), false},
		{"bad hash", slices.Concat(id(info|flag), chunk(ChunkMinLZSIMD, badHash, simd), eof), false},
		{"simd block in 0x02", slices.Concat(id(info|flag), chunk(ChunkMinLZBlock, crc, simd), eof), false},
		{"minlz block in 0x10", slices.Concat(id(info|flag), chunk(ChunkMinLZSIMD, hash, lz), eof), false},
		{"empty 0x02", slices.Concat(id(info), chunk(ChunkMinLZBlock, emptyCRC), chunk(ChunkEOF)), false},
		{"decoded size below compressed size", repeat(1), false},
		{"size 0 with literals", slices.Concat(id(info), chunk(ChunkMinLZBlock, maskedCRC([]byte("xxx")), []byte{0, 'x', 'x', 'x'}), chunk(ChunkEOF, []byte{3})), false},
		{"bit 7", slices.Concat(id(info|0x80), chunk(ChunkMinLZBlock, crc, lz), eof), false},
		{"block size 14", slices.Concat(id(14), chunk(ChunkMinLZBlock, crc, lz), eof), false},
		{"block above max", slices.Concat(id(5|flag), chunk(ChunkMinLZSIMD, hash, simd), eof), false},
		{"bad EOF size", slices.Concat(id(info), chunk(ChunkMinLZBlock, crc, lz), chunk(ChunkEOF, []byte{1})), false},
		{"size of an empty stream", slices.Concat(lzStream, chunk(ChunkEOF, []byte{1})), false},
		{"data after EOF", slices.Concat(id(info), chunk(ChunkEOF), chunk(ChunkMinLZBlock, crc, lz)), false},
		{"legacy chunk", slices.Concat(id(info), chunk(ChunkLegacyCompressed, crc, lz), chunk(ChunkEOF)), false},
		{"user non-skippable", slices.Concat(id(info), chunk(0xc0), chunk(ChunkMinLZBlock, crc, lz), eof), false},
		{"short index", slices.Concat(lzStream, chunk(ChunkIndex, []byte("s2"))), false},
	} {
		if err := ReadStream(bytes.NewReader(tc.stream), nil); (err == nil) != tc.ok {
			t.Errorf("%s: got error %v, want ok %v", tc.name, err, tc.ok)
		}
	}
}
