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

package minlz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestEncodeHuge(t *testing.T) {
	test := func(t *testing.T, data []byte) {
		for i := LevelFastest; i <= LevelSmallest; i++ {
			comp, err := Encode(make([]byte, MaxEncodedLen(len(data))), data, LevelFastest)
			if err != nil {
				t.Error(err)
				return
			}
			decoded, err := Decode(nil, comp)
			if err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(data, decoded) {
				t.Error("block decoder mismatch")
				return
			}
			if mel := MaxEncodedLen(len(data)); len(comp) > mel {
				t.Error(fmt.Errorf("MaxEncodedLen Exceed: input: %d, mel: %d, got %d", len(data), mel, len(comp)))
				return
			}
		}
	}
	test(t, make([]byte, MaxBlockSize))
}

func TestSizes(t *testing.T) {
	// Test emitting all lengths and their corresponding size functions.
	for i := 4; i < MaxBlockSize; i++ {
		var tmp [16]byte
		gotShort := emitCopySize(10, i)
		wantShort := emitCopy(tmp[:], 10, i)
		if gotShort != wantShort {
			t.Errorf("emitCopySize(10, %d): got %d, want %d", i, gotShort, wantShort)
		}
		gotMed := emitCopySize(4000, i)
		wantMed := emitCopy(tmp[:], 4000, i)
		if gotMed != wantMed {
			t.Errorf("emitCopySize(4000, %d): got %d, want %d", i, gotMed, wantMed)
		}
		gotLong := emitCopySize(70000, i)
		wantLong := emitCopy(tmp[:], 70000, i)
		if gotLong != wantLong {
			t.Errorf("emitCopySize(70000, %d): got %d, want %d", i, gotLong, wantLong)
		}
		gotRepeat := emitRepeatSize(i)
		wantRepeat := emitRepeat(tmp[:], i)
		if gotRepeat != wantRepeat {
			t.Errorf("emitCopySize(%d): got %d, want %d", i, gotRepeat, wantRepeat)
		}
	}
}

func TestEmitters(t *testing.T) {
	var tmp [11]byte
	lFactor := 1.01
	if testing.Short() {
		lFactor = 1.5
	}
	t.Run("copy1", func(t *testing.T) {
		for off := 1; off <= 1024; off++ {
			if t.Failed() {
				t.Log("off:", off-1)
				return
			}
			for l := 4; l <= 273; l++ {
				n := emitCopy(tmp[:], off, l)
				in := tmp[:n]
				s := 0
				gotTag, wantTag := in[0]&3, byte(tagCopy1)
				if gotTag != wantTag {
					t.Errorf("tag mismatch: want %d, got %d (off: %d, ml: %d)", wantTag, gotTag, off, l)
					break
				}
				length := int(in[0]) >> 2 & 15
				offset := int(binary.LittleEndian.Uint16(in[:])>>6) + 1
				if length == 15 {
					length = int(in[2]) + 18
					s += 3
				} else {
					length += 4
					s += 2
				}
				if length != l {
					t.Errorf("length mismatch: want %d, got %d", l, length)
				}
				if offset != off {
					t.Errorf("offset mismatch: want %d, got %d", off, offset)
				}
				if s != n {
					t.Errorf("output length mismatch: want %d, got %d", s, n)
				}
			}
		}
	})

	t.Run("copy2", func(t *testing.T) {
		for off := 1025; off <= maxCopy2Offset; off += 100 {
			if t.Failed() {
				t.Log("off:", off-1)
				return
			}
			for ml := 4; ml <= 1<<24; ml = int(float64(ml)*lFactor + 1) {
				//t.Run(fmt.Sprintf("len%d-o%d-lits%d", l, off, len(lits)), func(t *testing.T) {
				var n int
				n = emitCopy(tmp[:], off, ml)
				in := tmp[:n]
				s := 0

				gotTag, wantTag := in[0]&3, byte(tagCopy2)
				if gotTag != wantTag {
					t.Errorf("tag mismatch: want %d, got %d", wantTag, gotTag)
					return
				}
				length := int(in[0]) >> 2
				offset := int(uint32(in[1]) | uint32(in[2])<<8)
				if length <= 60 {
					length += 4
					s += 3
				} else {
					switch length {
					case 61:
						length = int(in[3]) + 64
						s += 4
					case 62:
						length = int(in[3]) | int(in[4])<<8 + 64
						s += 5
					case 63:
						length = int(in[3]) | int(in[4])<<8 | int(in[5])<<16 + 64
						s += 6
					}
				}
				offset += minCopy2Offset

				if length != ml {
					t.Errorf("length mismatch: want %d, got %d", ml, length)
				}
				if offset != off {
					t.Errorf("offset mismatch: want %d, got %d", off, offset)
				}
				if s != n {
					t.Errorf("length mismatch: want %d, got %d", n, s)
				}
			}
		}
	})

	t.Run("copy2-lits", func(t *testing.T) {
		lits := []byte{1}
		for ; len(lits) <= maxCopy2Lits; lits = append(lits, byte(len(lits)+1)) {
			for off := minCopy2Offset; off <= maxCopy2Offset; off += 100 {
				if t.Failed() {
					t.Log("off:", off-1)
					return
				}
				for l := 4; l <= 11; l++ {
					//t.Run(fmt.Sprintf("len%d-o%d-lits%d", l, off, len(lits)), func(t *testing.T) {
					var n int
					n = emitCopyLits2(tmp[:], lits, off, l)
					in := tmp[:n]

					gotTag, wantTag := in[0]&3, byte(tagCopy2Fused)
					if gotTag != wantTag {
						t.Errorf("tag mismatch: want %d, got %d", wantTag, gotTag)
						return
					}
					if in[0]&4 != 0 {
						t.Errorf("copy3 bit was set: 0x%x", in)
						return
					}

					value := in[0] >> 3
					// Read 2 byte offset of copy.
					offset := int(uint32(in[1]) | uint32(in[2])<<8)
					// Add 1 to offset.
					offset += 64

					// Literal length is lowest 2 bits
					litLength := int(value&3) + 1

					// Copy Length is above that + 4
					copyLength := int(value>>2) + 4

					if copyLength != l {
						t.Errorf("copy length mismatch: want %d, got %d", l, copyLength)
					}
					if offset != off {
						t.Errorf("offset mismatch: want %d, got %d", off, offset)
					}
					if litLength != len(lits) {
						t.Errorf("literal length mismatch: want %d, got %d", len(lits), litLength)
					}
					wantN := 3 + len(lits)
					if n != wantN {
						t.Errorf("n mismatch: want %d, got %d", wantN, n)
						return
					}
					for i := range lits {
						if in[3+i] != lits[i] {
							t.Errorf("lit (pos %d) mismatch: want %d, got %d", i, lits[i], in[3+i])
						}
					}
					//})
				}
			}
		}
	})
	t.Run("copy2-lits+repeat", func(t *testing.T) {
		lits := []byte{1}
		for ; len(lits) <= maxCopy2Lits; lits = append(lits, byte(len(lits)+1)) {
			for off := minCopy2Offset; off <= maxCopy2Offset; off += 100 {
				if t.Failed() {
					t.Log("off:", off-1)
					return
				}
				for l := 12; l <= 1<<24; l = int(float64(l)*lFactor + 1) {
					//t.Run(fmt.Sprintf("len%d-o%d-lits%d", l, off, len(lits)), func(t *testing.T) {
					var n int
					n = emitCopyLits2(tmp[:], lits, off, l)
					in := tmp[:n]

					gotTag, wantTag := in[0]&3, byte(tagCopy2Fused)
					if gotTag != wantTag {
						t.Errorf("tag mismatch: want %d, got %d", wantTag, gotTag)
						return
					}
					if in[0]&4 != 0 {
						t.Errorf("copy3 bit was set: 0x%x", in)
						return
					}

					value := in[0] >> 3

					// Read 2 byte offset of copy.
					offset := int(uint32(in[1]) | uint32(in[2])<<8)
					// Add 1 to offset.
					offset += 64

					// Literal length is lowest 2 bits
					litLength := int(value&3) + 1

					// Copy Length is above that + 4
					copyLength := int(value>>2) + 4

					if copyLength != 11 {
						t.Errorf("copy length mismatch: want %d, got %d", 11, copyLength)
					}
					if offset != off {
						t.Errorf("offset mismatch: want %d, got %d", off, offset)
					}
					if litLength != len(lits) {
						t.Errorf("literal length mismatch: want %d, got %d", len(lits), litLength)
					}
					wantN := 3 + len(lits) + emitRepeatSize(l-11)
					if n != wantN {
						t.Errorf("n mismatch: want %d, got %d", wantN, n)
						return
					}
					for i := range lits {
						if in[3+i] != lits[i] {
							t.Errorf("lit (pos %d) mismatch: want %d, got %d", i, lits[i], in[3+i])
						}
					}
					// Check the emitted repeat
					{
						l := l - 11 // 11 already emitted
						in := in[3+len(lits):]
						n -= 3 + len(lits)
						s := 0
						gotTag, wantTag := in[0]&7, byte(tagRepeat)
						if gotTag != wantTag {
							t.Errorf("tag mismatch: want %d, got %d", wantTag, gotTag)
							return
						}
						lengthTmp := int(in[0]) >> 3
						var length int
						switch lengthTmp {
						case 29:
							if n < 2 {
								t.Fatal("len", l, "size", n)
							}
							length = int(in[1]) + 30
							s += 2
						case 30:
							if n < 3 {
								t.Fatal("len", l, "size", n)
							}
							length = int(in[1]) + int(in[2])<<8 + 30
							s += 3
						case 31:
							if n < 4 {
								t.Fatal("len", l, "size", n)
							}
							length = int(in[1]) + int(in[2])<<8 + int(in[3])<<16 + 30
							s += 4
						default:
							length = lengthTmp + 1
							s++
						}
						if length != l {
							t.Errorf("length mismatch: want %d, got %d", l, length)
						}
						if s != n {
							t.Errorf("output length mismatch: want %d, got %d", s, n)
						}
					}
					//})
				}
			}
		}
	})
	t.Run("copy3", func(t *testing.T) {
		var lits []byte
		for ; len(lits) <= maxCopy3Lits; lits = append(lits, byte(len(lits)+1)) {
			for off := maxCopy2Offset + 1; off <= maxCopy3Offset; off *= 2 {
				if t.Failed() {
					t.Log("off:", off-1, "lits:", len(lits))
					return
				}

				for l := 4; l <= 1<<24; l = int(float64(l)*lFactor + 1) {
					var n int
					if len(lits) > 0 {
						n = emitCopyLits3(tmp[:], lits, off, l)
					} else {
						n = emitCopy(tmp[:], off, l)
					}
					in := tmp[:n]
					s := 0
					gotTag, wantTag := in[0]&7, byte(tagCopy3)
					if gotTag != wantTag {
						t.Errorf("tag mismatch: want %d, got %d (off: %d, ml: %d, ll: %d)", wantTag, gotTag, off, l, len(lits))
						break
					}

					length := int(binary.LittleEndian.Uint16(in)>>5) & 63
					offset := int(binary.LittleEndian.Uint32(in)>>11) + minCopy3Offset

					if length <= 60 {
						length += 4
						s += 4
					} else {
						switch length {
						case 61:
							length = int(in[4]) + minCopy3Length
							s += 5
						case 62:
							length = int(in[4]) | int(in[5])<<8 + minCopy3Length
							s += 6
						case 63:
							length = int(in[4]) | int(in[5])<<8 | int(in[6])<<16 + minCopy3Length
							s += 7
						}
					}

					nlits := int(in[0] >> 3 & 3)
					if nlits != len(lits) {
						t.Error("corrupt: lits size want", lits, "got", nlits)
						return
					}
					if nlits > 0 {
						if !bytes.Equal(lits, in[s:s+nlits]) {
							t.Error("corrupt: lits", lits[:nlits], in[s:s+nlits])
						}
						s += nlits
					}

					if length != l {
						t.Errorf("length mismatch: want %d, got %d", l, length)
					}
					if offset != off {
						t.Errorf("offset mismatch: want %d, got %d", off, offset)
					}
					if s != n {
						t.Errorf("length mismatch: want %d, got %d", n, s)
					}
				}
			}
		}
	})
	t.Run("repeat", func(t *testing.T) {
		for l := 1; l <= MaxBlockSize; l++ {
			if t.Failed() {
				return
			}

			n := emitRepeat(tmp[:], l)
			in := tmp[:n]
			s := 0
			gotTag, wantTag := in[0]&7, byte(tagRepeat)
			if gotTag != wantTag {
				t.Errorf("tag mismatch: want %d, got %d", wantTag, gotTag)
				break
			}
			lengthTmp := int(in[0]) >> 3
			var length int
			switch lengthTmp {
			case 29:
				if n < 2 {
					t.Fatal("len", l, "size", n)
				}
				length = int(in[1]) + 30
				s += 2
			case 30:
				if n < 3 {
					t.Fatal("len", l, "size", n)
				}
				length = int(in[1]) + int(in[2])<<8 + 30
				s += 3
			case 31:
				if n < 4 {
					t.Fatal("len", l, "size", n)
				}
				length = int(in[1]) + int(in[2])<<8 + int(in[3])<<16 + 30
				s += 4
			default:
				length = lengthTmp + 1
				s++
			}
			if length != l {
				t.Errorf("length mismatch: want %d, got %d", l, length)
			}
			if s != n {
				t.Errorf("output length mismatch: want %d, got %d", s, n)
			}
		}
	})
	t.Run("literals", func(t *testing.T) {
		in := make([]byte, MaxBlockSize)
		for i := range in {
			in[i] = byte(i)
		}
		dst := make([]byte, MaxEncodedLen(MaxBlockSize))
		for l := 1; l <= MaxBlockSize; l++ {
			for i := range dst {
				dst[i] = 0
			}
			n := emitLiteral(dst, in[:l])
			gotData := dst[n-l : n] // Data
			if !bytes.Equal(gotData, in[:l]) {
				t.Fatalf("data not copied %v != %v", len(gotData), len(in[:l]))
			}
			gotTag := dst[:n-l] // Remove data.
			t.Log(l, "->", gotTag)
			v := uint32(gotTag[0])
			tag := v & 3
			if v&4 == 1 {
				t.Fatal("Got repeat flag set")
			}
			value := v >> 3
			var length uint32
			switch tag {
			case 0:
				// Literal tag
				switch {
				case value < 29:
					// Length is in value
					length = value + 1
				case value == 29:
					// 1 byte length
					length = uint32(gotTag[1])
					// Add base offset
					length += 30
				case value == 30:
					// 2 byte length
					length = uint32(binary.LittleEndian.Uint16(gotTag[1:]))
					// Add base offset
					length += 30
				case value == 31:
					// 3 byte length
					length = uint32(gotTag[1]) + uint32(gotTag[2])<<8 + uint32(gotTag[3])<<16
					// Add base offset
					length += 30
				default:
					t.Errorf("got fused tag %d", value)
				}
			default:
				t.Errorf("got tag %d", tag)
			}
			if uint32(l) != length {
				t.Errorf("length mismatch: want %d, got %d", l, length)
			}
			if l > 50 {
				l *= 2
			}
		}
	})
}
