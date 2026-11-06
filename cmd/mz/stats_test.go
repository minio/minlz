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

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/minio/minlz"
)

// TestStatsBlockTypeMismatch checks that stats rejects a block whose type doesn't match its chunk type,
// as corrupt, and a raw block of a reserved type as unsupported, like the block API.
func TestStatsBlockTypeMismatch(t *testing.T) {
	data := bytes.Repeat([]byte("Hello SIMD chunk, hello SIMD stream. "), 300)
	simd, err := minlz.Encode(nil, data, minlz.LevelBalancedSIMD)
	if err != nil {
		t.Fatal(err)
	}
	minLZ, err := minlz.Encode(nil, data, minlz.LevelBalanced)
	if err != nil {
		t.Fatal(err)
	}
	// Streams store blocks without the indicator byte.
	simd, minLZ = simd[1:], minLZ[1:]
	if v, _ := binary.Uvarint(simd); v>>24 != 1 {
		t.Fatal("not a SIMD block")
	}
	// Stats doesn't check checksums, so they are zero.
	chunk := func(typ byte, block []byte) []byte {
		n := 4 + len(block)
		if typ == chunkMinLZSIMD {
			n += 4
		}
		return slices.Concat([]byte{typ, byte(n), byte(n >> 8), byte(n >> 16)}, make([]byte, n-len(block)), block)
	}
	size := binary.AppendUvarint(nil, uint64(len(data)))
	analyze := func(c []byte) error {
		stream := slices.Concat([]byte("\xff\x06\x00\x00MinLz\x46"), c, []byte{chunkEOF, byte(len(size)), 0, 0}, size)
		return analyzeStream(bufio.NewReader(bytes.NewReader(stream)), &fileReport{}, statsOpts{})
	}
	for _, c := range [][]byte{chunk(chunkMinLZSIMD, simd), chunk(chunkMinLZ, minLZ)} {
		if err := analyze(c); err != nil {
			t.Fatalf("chunk type 0x%02x: %v", c[0], err)
		}
	}
	raw := func(block []byte) error {
		return analyzeRawBlock(append([]byte{0}, block...), &fileReport{}, statsOpts{})
	}
	for _, b := range [][]byte{simd, minLZ} {
		if err := raw(b); err != nil {
			t.Fatal("raw block:", err)
		}
	}

	type mismatch struct {
		name  string
		chunk []byte
	}
	tests := []mismatch{
		{"simd-in-0x02", chunk(chunkMinLZ, simd)},
		{"simd-in-0x03", chunk(chunkMinLZCompCRC, simd)},
		{"minlz-in-0x10", chunk(chunkMinLZSIMD, minLZ)},
	}
	_, h := binary.Uvarint(simd)
	for _, typ := range []uint64{2, 255} {
		reserved := append(binary.AppendUvarint(nil, typ<<24|uint64(len(data))), simd[h:]...)
		for _, ct := range []byte{chunkMinLZ, chunkMinLZCompCRC, chunkMinLZSIMD} {
			tests = append(tests, mismatch{fmt.Sprintf("type-%d-in-0x%02x", typ, ct), chunk(ct, reserved)})
		}
		if err := raw(reserved); !errors.Is(err, minlz.ErrUnsupported) {
			t.Errorf("raw type-%d: got error %v, want %v", typ, err, minlz.ErrUnsupported)
		}
	}
	for _, tt := range tests {
		if err := analyze(tt.chunk); !errors.Is(err, minlz.ErrCorrupt) {
			t.Errorf("%s: got error %v, want %v", tt.name, err, minlz.ErrCorrupt)
		}
	}
}

// TestStatsEmptyStream checks that an EOF chunk directly after an EOF chunk is an empty stream,
// which must have size 0. The Writer writes only an EOF chunk for empty input.
func TestStatsEmptyStream(t *testing.T) {
	var a, empty bytes.Buffer
	w := minlz.NewWriter(&a)
	if _, err := w.Write(bytes.Repeat([]byte("Hello empty stream. "), 1000)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := minlz.NewWriter(&empty).Close(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		stream [][]byte
		ok     bool
	}{
		{"a+empty", [][]byte{a.Bytes(), empty.Bytes()}, true},
		{"empty+a", [][]byte{empty.Bytes(), a.Bytes()}, true},
		{"empty+empty", [][]byte{empty.Bytes(), empty.Bytes()}, true},
		{"a+nonzero-size", [][]byte{a.Bytes(), {chunkEOF, 1, 0, 0, 1}}, false},
	} {
		err := analyzeStream(bufio.NewReader(bytes.NewReader(bytes.Join(tt.stream, nil))), &fileReport{}, statsOpts{})
		if (err == nil) != tt.ok {
			t.Errorf("%s: got error %v, want ok %v", tt.name, err, tt.ok)
		}
	}
}

// TestStatsSIMD checks that the stats of SIMD blocks add up: the output, and the operations against their streams.
func TestStatsSIMD(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	words := strings.Fields("Hello SIMD chunk, hello SIMD stream. The quick brown fox jumps over the lazy dog 0123456789")
	var data []byte
	for len(data) < 300<<10 {
		data = append(data, words[rng.IntN(len(words))]...)
		data = append(data, byte(' '+rng.IntN(3)))
	}
	for _, level := range []int{minlz.LevelSuperFastSIMD, minlz.LevelFastestSIMD | minlz.LevelSIMDDelta, minlz.LevelBalancedSIMD, minlz.LevelSmallestSIMD} {
		block, err := minlz.Encode(nil, data, level)
		if err != nil {
			t.Fatal(err)
		}
		var rep fileReport
		if err := analyzeRawBlock(block, &rep, statsOpts{}); err != nil {
			t.Fatal(level, err)
		}
		ss, ops := &rep.SIMD, &rep.OpTotals
		got := []uint64{rep.TotalOut, rep.TotalEnc, ss.BlockBytes, ss.Streams[5].Values, ss.Operations, ss.Streams[2].Values, ss.Streams[4].Values}
		want := []uint64{uint64(len(data)), uint64(len(block) - 1), uint64(len(block) - 1), ops[opLiteral].OutBytes, ops[opRepeat].Count + ops[opMatch].Count + ss.NoMatch, ops[opMatch].Count, ops[opMatch].Count}
		if !slices.Equal(got, want) || ss.Blocks != 1 || ss.otherBytes() == 0 || ss.otherBytes() > 64 {
			t.Errorf("level %d: got %v, want %v; %d blocks, %d other bytes", level, got, want, ss.Blocks, ss.otherBytes())
		}
		// The literals after the last operation are one more Literal op.
		if n := ops[opLiteral].Count - ss.Streams[1].Values; n > 1 {
			t.Errorf("level %d: %d literal ops for %d literal lengths", level, ops[opLiteral].Count, ss.Streams[1].Values)
		}
	}
}
