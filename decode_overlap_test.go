// Copyright 2026 MinLZ contributors.
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
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

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

func TestDecodeOverlapTailLarge(t *testing.T) {
	for _, offset := range []int{1, 3, 16, 17, 63, 64, 65, 4096, 65536} {
		encoded, want := overlapTailBlock(offset, MaxBlockSize-offset-64, 0)
		got, err := Decode(nil, encoded)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("offset=%d: decode %v; equal=%v", offset, err, bytes.Equal(got, want))
		}
	}
}

func BenchmarkDecodeOverlapTail(b *testing.B) {
	for _, offset := range []int{1, 3, 16, 17, 63, 64, 65, 4096, 65536} {
		for _, length := range []int{64, 128, 256, 4096, 1 << 20} {
			b.Run(fmt.Sprintf("offset%d/length%d", offset, length), func(b *testing.B) {
				encoded, want := overlapTailBlock(offset, length, 0)
				dst := make([]byte, len(want))
				got, err := Decode(dst, encoded)
				if err != nil || !bytes.Equal(got, want) {
					b.Fatal("invalid decode", err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(want)))
				b.ResetTimer()
				for b.Loop() {
					got, err = Decode(dst, encoded)
					if err != nil {
						b.Fatal(err)
					}
				}
				if !bytes.Equal(got, want) {
					b.Fatal("decoded bytes changed")
				}
			})
		}
	}
}

func BenchmarkDecodeSizeMatrix(b *testing.B) {
	sizes := []int{16, 32, 64, 128, 256, 512, 1024, 4096, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 2 << 20, MaxBlockSize, 14 << 20}
	for _, shape := range []string{"repeat1", "repeat16", "document", "random"} {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/size%d", shape, size), func(b *testing.B) {
				input := make([]byte, size)
				pattern := []byte(`{"id":12345,"name":"cproto payload","values":[1,2,3,4],"active":true}`)
				switch shape {
				case "repeat1":
					pattern = []byte("x")
				case "repeat16":
					pattern = []byte("0123456789ABCDEF")
				}
				if shape == "random" {
					rand.New(rand.NewSource(12345)).Read(input)
				} else {
					for i := range input {
						input[i] = pattern[i%len(pattern)]
					}
				}
				var encoded [][]byte
				for start := 0; start < size; start += MaxBlockSize {
					end := min(size, start+MaxBlockSize)
					block, err := Encode(nil, input[start:end], LevelFastest)
					if err != nil {
						b.Fatal(err)
					}
					encoded = append(encoded, block)
				}
				dst := make([]byte, size)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for b.Loop() {
					for i, block := range encoded {
						start := i * MaxBlockSize
						if _, err := Decode(dst[start:min(size, start+MaxBlockSize)], block); err != nil {
							b.Fatal(err)
						}
					}
				}
				if !bytes.Equal(dst, input) {
					b.Fatal("decoded bytes differ")
				}
			})
		}
	}
}
