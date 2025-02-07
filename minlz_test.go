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
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/klauspost/compress/zip"
	"github.com/klauspost/compress/zstd"
)

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)

var (
	download     = flag.Bool("download", false, "If true, download any missing files before running benchmarks")
	testdataDir  = flag.String("testdataDir", "testdata", "Directory containing the test data")
	benchdataDir = flag.String("benchdataDir", "testdata/bench", "Directory containing the benchmark data")
)

func TestMaxEncodedLen(t *testing.T) {
	testSet := []struct {
		in, out int64
	}{
		0: {in: 0, out: 1},
		1: {in: 1 << 5, out: 2},
		2: {in: MaxBlockSize, out: 2},
		3: {in: math.MaxUint32, out: -1},
		4: {in: -1, out: -1},
		5: {in: -2, out: -1},
	}

	// Test all sizes up to maxBlockSize.
	for i := int64(1); i < maxBlockSize; i++ {
		testSet = append(testSet, struct{ in, out int64 }{in: i, out: 2})
	}
	for i := range testSet {
		tt := testSet[i]
		want := tt.out
		if want > 0 {
			want += tt.in
		}
		got := int64(MaxEncodedLen(int(tt.in)))
		if got != want {
			t.Errorf("test %d: input: %d, want: %d, got: %d", i, tt.in, want, got)
		}
	}
}

func encodeGo(dst, src []byte, level int) []byte {
	if n := MaxEncodedLen(len(src)); n < 0 {
		panic(ErrTooLarge)
	} else if len(dst) < n {
		dst = make([]byte, n)
	}
	if len(src) < minNonLiteralBlockSize {
		return encodeUncompressed(dst[:0], src)
	}
	if len(src) > MaxBlockSize {
		panic(ErrTooLarge)
	}

	// The block starts with the varint-encoded length of the decompressed bytes.
	dst[0] = 0
	d := 1
	d += binary.PutUvarint(dst[d:], uint64(len(src)))

	var n int
	switch level {
	case LevelFastest:
		n = encodeBlockGo(dst[d:], src)
	case LevelBalanced:
		n = encodeBlockBetterGo(dst[d:], src)
	case LevelSmallest:
		n = encodeBlockBest(dst[d:], src, nil)
	default:
		panic(ErrInvalidLevel)
	}

	if n > 0 {
		d += n
		return dst[:d]
	}
	// Not compressible.
	return encodeUncompressed(dst[:0], src)
}

func cmp(got, want []byte) error {
	if bytes.Equal(got, want) {
		return nil
	}
	if len(got) != len(want) {
		return fmt.Errorf("got %d bytes, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			return fmt.Errorf("byte #%d: got 0x%02x, want 0x%02x", i, got[i], want[i])
		}
	}
	return nil
}

func roundtrip(b, ebuf, dbuf []byte) error {
	bOrg := make([]byte, len(b))
	copy(bOrg, b)
	for level := LevelFastest; level <= LevelSmallest; level++ {
		asmEnc, err := Encode(nil, b, level)
		if err != nil {
			return err
		}
		if err := cmp(bOrg, b); err != nil {
			return fmt.Errorf("lvl %d: src was changed: %v", level, err)
		}
		d, err := decodeGo(nil, asmEnc)
		if err != nil {
			return fmt.Errorf("lvl %d: decoding error: %v", level, err)
		}
		if err := cmp(d, b); err != nil {
			return fmt.Errorf("lvl %d: roundtrip mismatch: %v", level, err)
		}
		goEnc := encodeGo(nil, b, level)
		if err := cmp(bOrg, b); err != nil {
			return fmt.Errorf("lvl %d: src was changed: %v", level, err)
		}
		dGo, err := Decode(nil, goEnc)
		if err != nil {
			return fmt.Errorf("lvl %d: decoding (asm) error: %v", level, err)
		}

		if err := cmp(dGo, b); err != nil {
			return fmt.Errorf("lvl %d: roundtrip mismatch: %v", level, err)
		}
	}

	// Test concat with some existing data.
	/*
		dst := []byte("existing")
		// Add 3 different encodes and a 0 length block.
		concat, err := ConcatBlocks(dst, Encode(nil, b), EncodeBetter(nil, b), []byte{0}, EncodeSnappy(nil, b))
		if err != nil {
			return fmt.Errorf("concat error: %v", err)
		}
		if err := cmp(concat[:len(dst)], dst); err != nil {
			return fmt.Errorf("concat existing mismatch: %v", err)
		}
		concat = concat[len(dst):]

		d, _ = Decode(nil, concat)
		want := append(make([]byte, 0, len(b)*3), b...)
		want = append(want, b...)
		want = append(want, b...)

		if err := cmp(d, want); err != nil {
			return fmt.Errorf("roundtrip concat mismatch: %v", err)
		}
	*/

	return nil
}

func TestEmpty(t *testing.T) {
	if err := roundtrip(nil, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSmallCopy(t *testing.T) {
	for _, ebuf := range [][]byte{nil, make([]byte, 20), make([]byte, 64)} {
		for _, dbuf := range [][]byte{nil, make([]byte, 20), make([]byte, 64)} {
			for i := 0; i < 32; i++ {
				s := "aaaa" + strings.Repeat("b", i) + "aaaabbbb"
				if err := roundtrip([]byte(s), ebuf, dbuf); err != nil {
					t.Errorf("len(ebuf)=%d, len(dbuf)=%d, i=%d: %v", len(ebuf), len(dbuf), i, err)
				}
			}
		}
	}
}

func TestSmallRand(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for n := 1; n < 20000; n += 23 {
		b := make([]byte, n)
		for i := range b {
			b[i] = uint8(rng.Intn(256))
		}
		if err := roundtrip(b, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSmallRegular(t *testing.T) {
	for n := 1; n < 20000; n += 23 {
		b := make([]byte, n)
		for i := range b {
			b[i] = uint8(i%10 + 'a')
		}
		if err := roundtrip(b, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSmallRepeat(t *testing.T) {
	for n := 1; n < 20000; n += 23 {
		b := make([]byte, n)
		for i := range b[:n/2] {
			b[i] = uint8(i * 255 / n)
		}
		for i := range b[n/2:] {
			b[i+n/2] = uint8(i%10 + 'a')
		}
		if err := roundtrip(b, nil, nil); err != nil {
			t.Fatalf("%d:%v", n, err)
		}
	}
}

func TestInvalidVarint(t *testing.T) {
	testCases := []struct {
		desc  string
		input string
	}{{
		"invalid varint, final byte has continuation bit set",
		"\xff",
	}, {
		"invalid varint, value overflows uint64",
		"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00",
	}, {
		// https://github.com/google/snappy/blob/master/format_description.txt
		// says that "the stream starts with the uncompressed length [as a
		// varint] (up to a maximum of 2^32 - 1)".
		"valid varint (as uint64), but value overflows uint32",
		"\x80\x80\x80\x80\x10",
	}}

	for _, tc := range testCases {
		input := []byte(tc.input)
		if _, err := DecodedLen(input); err != ErrCorrupt {
			t.Errorf("%s: DecodedLen: got %v, want ErrCorrupt", tc.desc, err)
		}
		if _, err := Decode(nil, input); err != ErrCorrupt {
			t.Errorf("%s: Decode: got %v, want ErrCorrupt", tc.desc, err)
		}
	}
}

func TestDecode(t *testing.T) {
	lit40Bytes := make([]byte, 40)
	for i := range lit40Bytes {
		lit40Bytes[i] = byte(i)
	}
	lit40 := string(lit40Bytes)

	testCases := []struct {
		desc    string
		input   string
		want    string
		wantErr error
	}{{
		`decodedLen=0; valid input`,
		"\x00",
		"",
		nil,
	}, {
		`decodedLen=3; tagLiteral, 0-byte length; length=3; valid input`,
		"\x03" + "\x08\xff\xff\xff",
		"\xff\xff\xff",
		nil,
	}, {
		`decodedLen=2; tagLiteral, 0-byte length; length=3; not enough dst bytes`,
		"\x02" + "\x08\xff\xff\xff",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=3; tagLiteral, 0-byte length; length=3; not enough src bytes`,
		"\x03" + "\x08\xff\xff",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=40; tagLiteral, 0-byte length; length=40; valid input`,
		"\x28" + "\x9c" + lit40,
		lit40,
		nil,
	}, {
		`decodedLen=1; tagLiteral, 1-byte length; not enough length bytes`,
		"\x01" + "\xf0",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=3; tagLiteral, 1-byte length; length=3; valid input`,
		"\x03" + "\xf0\x02\xff\xff\xff",
		"\xff\xff\xff",
		nil,
	}, {
		`decodedLen=1; tagLiteral, 2-byte length; not enough length bytes`,
		"\x01" + "\xf4\x00",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=3; tagLiteral, 2-byte length; length=3; valid input`,
		"\x03" + "\xf4\x02\x00\xff\xff\xff",
		"\xff\xff\xff",
		nil,
	}, {
		`decodedLen=1; tagLiteral, 3-byte length; not enough length bytes`,
		"\x01" + "\xf8\x00\x00",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=3; tagLiteral, 3-byte length; length=3; valid input`,
		"\x03" + "\xf8\x02\x00\x00\xff\xff\xff",
		"\xff\xff\xff",
		nil,
	}, {
		`decodedLen=1; tagLiteral, 4-byte length; not enough length bytes`,
		"\x01" + "\xfc\x00\x00\x00",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=1; tagLiteral, 4-byte length; length=3; not enough dst bytes`,
		"\x01" + "\xfc\x02\x00\x00\x00\xff\xff\xff",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=4; tagLiteral, 4-byte length; length=3; not enough src bytes`,
		"\x04" + "\xfc\x02\x00\x00\x00\xff",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=3; tagLiteral, 4-byte length; length=3; valid input`,
		"\x03" + "\xfc\x02\x00\x00\x00\xff\xff\xff",
		"\xff\xff\xff",
		nil,
	}, {
		`decodedLen=4; tagCopy1, 1 extra length|offset byte; not enough extra bytes`,
		"\x04" + "\x01",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=4; tagCopy2, 2 extra length|offset bytes; not enough extra bytes`,
		"\x04" + "\x02\x00",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=4; tagCopy4, 4 extra length|offset bytes; not enough extra bytes`,
		"\x04" + "\x03\x00\x00\x00",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=4; tagLiteral (4 bytes "abcd"); valid input`,
		"\x04" + "\x0cabcd",
		"abcd",
		nil,
	}, {
		`decodedLen=13; tagLiteral (4 bytes "abcd"); tagCopy1; length=9 offset=4; valid input`,
		"\x0d" + "\x0cabcd" + "\x15\x04",
		"abcdabcdabcda",
		nil,
	}, {
		`decodedLen=8; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=4; valid input`,
		"\x08" + "\x0cabcd" + "\x01\x04",
		"abcdabcd",
		nil,
	}, {
		`decodedLen=8; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=2; valid input`,
		"\x08" + "\x0cabcd" + "\x01\x02",
		"abcdcdcd",
		nil,
	}, {
		`decodedLen=8; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=1; valid input`,
		"\x08" + "\x0cabcd" + "\x01\x01",
		"abcddddd",
		nil,
	}, {
		`decodedLen=8; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=0; repeat offset as first match`,
		"\x08" + "\x0cabcd" + "\x01\x00",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=13; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=1; literal: 'z'; tagCopy1; length=4 offset=0; repeat offset as second match`,
		"\x0d" + "\x0cabcd" + "\x01\x01" + "\x00z" + "\x01\x00",
		"abcdddddzzzzz",
		nil,
	}, {
		`decodedLen=9; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=4; inconsistent dLen`,
		"\x09" + "\x0cabcd" + "\x01\x04",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=8; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=5; offset too large`,
		"\x08" + "\x0cabcd" + "\x01\x05",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=7; tagLiteral (4 bytes "abcd"); tagCopy1; length=4 offset=4; length too large`,
		"\x07" + "\x0cabcd" + "\x01\x04",
		"",
		ErrCorrupt,
	}, {
		`decodedLen=6; tagLiteral (4 bytes "abcd"); tagCopy2; length=2 offset=3; valid input`,
		"\x06" + "\x0cabcd" + "\x06\x03\x00",
		"abcdbc",
		nil,
	}, {
		`decodedLen=6; tagLiteral (4 bytes "abcd"); tagCopy4; length=2 offset=3; valid input`,
		"\x06" + "\x0cabcd" + "\x07\x03\x00\x00\x00",
		"abcdbc",
		nil,
	}}

	const (
		// notPresentXxx defines a range of byte values [0xa0, 0xc5) that are
		// not present in either the input or the output. It is written to dBuf
		// to check that Decode does not write bytes past the end of
		// dBuf[:dLen].
		//
		// The magic number 37 was chosen because it is prime. A more 'natural'
		// number like 32 might lead to a false negative if, for example, a
		// byte was incorrectly copied 4*8 bytes later.
		notPresentBase = 0xa0
		notPresentLen  = 37
	)

	var dBuf [100]byte
loop:
	for i, tc := range testCases {
		input := []byte(tc.input)
		for _, x := range input {
			if notPresentBase <= x && x < notPresentBase+notPresentLen {
				t.Errorf("#%d (%s): input shouldn't contain %#02x\ninput: % x", i, tc.desc, x, input)
				continue loop
			}
		}

		dLen, n := binary.Uvarint(input)
		if n <= 0 {
			t.Errorf("#%d (%s): invalid varint-encoded dLen", i, tc.desc)
			continue
		}
		if dLen > uint64(len(dBuf)) {
			t.Errorf("#%d (%s): dLen %d is too large", i, tc.desc, dLen)
			continue
		}

		for j := range dBuf {
			dBuf[j] = byte(notPresentBase + j%notPresentLen)
		}
		g, gotErr := Decode(dBuf[:], input)
		if got := string(g); got != tc.want || gotErr != tc.wantErr {
			t.Errorf("#%d (%s):\ngot  %q, %v\nwant %q, %v",
				i, tc.desc, got, gotErr, tc.want, tc.wantErr)
			continue
		}
		for j, x := range dBuf {
			if uint64(j) < dLen {
				continue
			}
			if w := byte(notPresentBase + j%notPresentLen); x != w {
				t.Errorf("#%d (%s): Decode overrun: dBuf[%d] was modified: got %#02x, want %#02x\ndBuf: % x",
					i, tc.desc, j, x, w, dBuf)
				continue loop
			}
		}
	}
}

func TestDecodeCopy4(t *testing.T) {
	dots := strings.Repeat(".", 65536)

	input := strings.Join([]string{
		"\x89\x80\x04",         // decodedLen = 65545.
		"\x0cpqrs",             // 4-byte literal "pqrs".
		"\xf4\xff\xff" + dots,  // 65536-byte literal dots.
		"\x13\x04\x00\x01\x00", // tagCopy4; length=5 offset=65540.
	}, "")

	gotBytes, err := Decode(nil, []byte(input))
	if err != nil {
		t.Fatal(err)
	}
	got := string(gotBytes)
	want := "pqrs" + dots + "pqrs."
	if len(got) != len(want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	if got != want {
		for i := 0; i < len(got); i++ {
			if g, w := got[i], want[i]; g != w {
				t.Fatalf("byte #%d: got %#02x, want %#02x", i, g, w)
			}
		}
	}
}

// TestDecodeLengthOffset tests decoding an encoding of the form literal +
// copy-length-offset + literal. For example: "abcdefghijkl" + "efghij" + "AB".
func TestDecodeLengthOffset(t *testing.T) {
	const (
		prefix = "abcdefghijklmnopqr"
		suffix = "ABCDEFGHIJKLMNOPQR"

		// notPresentXxx defines a range of byte values [0xa0, 0xc5) that are
		// not present in either the input or the output. It is written to
		// gotBuf to check that Decode does not write bytes past the end of
		// gotBuf[:totalLen].
		//
		// The magic number 37 was chosen because it is prime. A more 'natural'
		// number like 32 might lead to a false negative if, for example, a
		// byte was incorrectly copied 4*8 bytes later.
		notPresentBase = 0xa0
		notPresentLen  = 37
	)
	var gotBuf, wantBuf, inputBuf [128]byte
	for length := 1; length <= 18; length++ {
		for offset := 1; offset <= 18; offset++ {
		loop:
			for suffixLen := 0; suffixLen <= 18; suffixLen++ {
				totalLen := len(prefix) + length + suffixLen

				inputLen := binary.PutUvarint(inputBuf[:], uint64(totalLen))
				inputBuf[inputLen] = tagLiteral + 4*byte(len(prefix)-1)
				inputLen++
				inputLen += copy(inputBuf[inputLen:], prefix)
				inputBuf[inputLen+0] = tagCopy2 + 4*byte(length-1)
				inputBuf[inputLen+1] = byte(offset)
				inputBuf[inputLen+2] = 0x00
				inputLen += 3
				if suffixLen > 0 {
					inputBuf[inputLen] = tagLiteral + 4*byte(suffixLen-1)
					inputLen++
					inputLen += copy(inputBuf[inputLen:], suffix[:suffixLen])
				}
				input := inputBuf[:inputLen]

				for i := range gotBuf {
					gotBuf[i] = byte(notPresentBase + i%notPresentLen)
				}
				got, err := Decode(gotBuf[:], input)
				if err != nil {
					t.Errorf("length=%d, offset=%d; suffixLen=%d: %v", length, offset, suffixLen, err)
					continue
				}

				wantLen := 0
				wantLen += copy(wantBuf[wantLen:], prefix)
				for i := 0; i < length; i++ {
					wantBuf[wantLen] = wantBuf[wantLen-offset]
					wantLen++
				}
				wantLen += copy(wantBuf[wantLen:], suffix[:suffixLen])
				want := wantBuf[:wantLen]

				for _, x := range input {
					if notPresentBase <= x && x < notPresentBase+notPresentLen {
						t.Errorf("length=%d, offset=%d; suffixLen=%d: input shouldn't contain %#02x\ninput: % x",
							length, offset, suffixLen, x, input)
						continue loop
					}
				}
				for i, x := range gotBuf {
					if i < totalLen {
						continue
					}
					if w := byte(notPresentBase + i%notPresentLen); x != w {
						t.Errorf("length=%d, offset=%d; suffixLen=%d; totalLen=%d: "+
							"Decode overrun: gotBuf[%d] was modified: got %#02x, want %#02x\ngotBuf: % x",
							length, offset, suffixLen, totalLen, i, x, w, gotBuf)
						continue loop
					}
				}
				for _, x := range want {
					if notPresentBase <= x && x < notPresentBase+notPresentLen {
						t.Errorf("length=%d, offset=%d; suffixLen=%d: want shouldn't contain %#02x\nwant: % x",
							length, offset, suffixLen, x, want)
						continue loop
					}
				}

				if !bytes.Equal(got, want) {
					t.Errorf("length=%d, offset=%d; suffixLen=%d:\ninput % x\ngot   % x\nwant  % x",
						length, offset, suffixLen, input, got, want)
					continue
				}
			}
		}
	}
}

const (
	goldenText         = "Mark.Twain-Tom.Sawyer.txt"
	goldenCompressed   = goldenText + ".rawsnappy"
	goldenCompressedMz = goldenText + ".mzb"
)

func TestDecodeGoldenInput(t *testing.T) {
	tDir := filepath.FromSlash(*testdataDir)
	src, err := os.ReadFile(filepath.Join(tDir, goldenCompressed))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	got, err := Decode(nil, src)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(tDir, goldenText))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := cmp(got, want); err != nil {
		t.Fatal(err)
	}
	src, err = os.ReadFile(filepath.Join(tDir, goldenCompressedMz))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	got, err = Decode(nil, src)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := cmp(got, want); err != nil {
		t.Fatal(err)
	}
}

// TestSlowForwardCopyOverrun tests the "expand the pattern" algorithm
// described in decode_amd64.s and its claim of a 10 byte overrun worst case.
func TestSlowForwardCopyOverrun(t *testing.T) {
	const base = 100

	for length := 1; length < 18; length++ {
		for offset := 1; offset < 18; offset++ {
			highWaterMark := base
			d := base
			l := length
			o := offset

			// makeOffsetAtLeast8
			for o < 8 {
				if end := d + 8; highWaterMark < end {
					highWaterMark = end
				}
				l -= o
				d += o
				o += o
			}

			// fixUpSlowForwardCopy
			a := d
			d += l

			// finishSlowForwardCopy
			for l > 0 {
				if end := a + 8; highWaterMark < end {
					highWaterMark = end
				}
				a += 8
				l -= 8
			}

			dWant := base + length
			overrun := highWaterMark - dWant
			if d != dWant || overrun < 0 || 10 < overrun {
				t.Errorf("length=%d, offset=%d: d and overrun: got (%d, %d), want (%d, something in [0, 10])",
					length, offset, d, overrun, dWant)
			}
		}
	}
}

// TestEncoderSkip will test skipping various sizes and block types.
func TestEncoderSkip(t *testing.T) {
	for ti, origLen := range []int{10 << 10, 256 << 10, 2 << 20, 8 << 20} {
		if testing.Short() && ti > 1 {
			break
		}
		t.Run(fmt.Sprint(origLen), func(t *testing.T) {
			src := make([]byte, origLen)
			rng := rand.New(rand.NewSource(1))
			firstHalf, secondHalf := src[:origLen/2], src[origLen/2:]
			bonus := secondHalf[len(secondHalf)-origLen/10:]
			for i := range firstHalf {
				// Incompressible.
				firstHalf[i] = uint8(rng.Intn(256))
			}
			for i := range secondHalf {
				// Easy to compress.
				secondHalf[i] = uint8(i & 32)
			}
			for i := range bonus {
				// Incompressible.
				bonus[i] = uint8(rng.Intn(256))
			}
			var dst bytes.Buffer
			enc := NewWriter(&dst, WriterBlockSize(64<<10))
			_, err := io.Copy(enc, bytes.NewBuffer(src))
			if err != nil {
				t.Fatal(err)
			}
			err = enc.Close()
			if err != nil {
				t.Fatal(err)
			}
			compressed := dst.Bytes()
			dec := NewReader(nil)
			for i := 0; i < len(src); i += len(src)/20 - 17 {
				t.Run(fmt.Sprint("skip-", i), func(t *testing.T) {
					want := src[i:]
					dec.Reset(bytes.NewBuffer(compressed))
					// Read some of it first
					read, err := io.CopyN(io.Discard, dec, int64(len(want)/10))
					if err != nil {
						t.Fatal(err)
					}
					// skip what we just read.
					want = want[read:]
					err = dec.Skip(int64(i))
					if err != nil {
						t.Fatal(err)
					}
					got, err := io.ReadAll(dec)
					if err != nil {
						t.Errorf("Skipping %d returned error: %v", i, err)
						return
					}
					if !bytes.Equal(want, got) {
						t.Log("got  len:", len(got))
						t.Log("want len:", len(want))
						t.Errorf("Skipping %d did not return correct data (content mismatch)", i)
						return
					}
				})
				if testing.Short() && i > 0 {
					return
				}
			}
		})
	}
}

// TestEncodeNoiseThenRepeats encodes input for which the first half is very
// incompressible and the second half is very compressible. The encoded form's
// length should be closer to 50% of the original length than 100%.
func TestEncodeNoiseThenRepeats(t *testing.T) {
	for _, origLen := range []int{256 * 1024, 2048 * 1024} {
		src := make([]byte, origLen)
		rng := rand.New(rand.NewSource(1))
		firstHalf, secondHalf := src[:origLen/2], src[origLen/2:]
		for i := range firstHalf {
			firstHalf[i] = uint8(rng.Intn(256))
		}
		for i := range secondHalf {
			secondHalf[i] = uint8(i >> 8)
		}
		dst, _ := Encode(nil, src, LevelFastest)
		if got, want := len(dst), origLen*3/4; got >= want {
			t.Fatalf("origLen=%d: got %d encoded bytes, want less than %d", origLen, got, want)
		}
		t.Log(len(dst))
	}
}

func TestFramingFormat(t *testing.T) {
	// src is comprised of alternating 1e5-sized sequences of random
	// (incompressible) bytes and repeated (compressible) bytes. 1e5 was chosen
	// because it is larger than maxBlockSize (64k).
	src := make([]byte, 1e6)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			for j := 0; j < 1e5; j++ {
				src[1e5*i+j] = uint8(rng.Intn(256))
			}
		} else {
			for j := 0; j < 1e5; j++ {
				src[1e5*i+j] = uint8(i)
			}
		}
	}

	buf := new(bytes.Buffer)
	bw := NewWriter(buf)
	if _, err := bw.Write(src); err != nil {
		t.Fatalf("Write: encoding: %v", err)
	}
	err := bw.Close()
	if err != nil {
		t.Fatal(err)
	}
	dst, err := io.ReadAll(NewReader(buf))
	if err != nil {
		t.Fatalf("ReadAll: decoding: %v", err)
	}
	if err := cmp(dst, src); err != nil {
		t.Fatal(err)
	}
}

func TestFramingFormatBetter(t *testing.T) {
	// src is comprised of alternating 1e5-sized sequences of random
	// (incompressible) bytes and repeated (compressible) bytes. 1e5 was chosen
	// because it is larger than maxBlockSize (64k).
	src := make([]byte, 1e6)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			for j := 0; j < 1e5; j++ {
				src[1e5*i+j] = uint8(rng.Intn(256))
			}
		} else {
			for j := 0; j < 1e5; j++ {
				src[1e5*i+j] = uint8(i)
			}
		}
	}

	buf := new(bytes.Buffer)
	bw := NewWriter(buf, WriterLevel(LevelBalanced))
	if _, err := bw.Write(src); err != nil {
		t.Fatalf("Write: encoding: %v", err)
	}
	err := bw.Close()
	if err != nil {
		t.Fatal(err)
	}
	dst, err := io.ReadAll(NewReader(buf))
	if err != nil {
		t.Fatalf("ReadAll: decoding: %v", err)
	}
	if err := cmp(dst, src); err != nil {
		t.Fatal(err)
	}
}

func TestEmitLiteral(t *testing.T) {
	testCases := []struct {
		length int
		want   string
	}{
		{1, "\x00"},
		{2, "\b"},
		{27, "\xd0"},
		{28, "\xd8"},
		{29, "\xe0"},
		{30, "\xe8\x00"},
		{59, "\xe8\x1d"},
		{60, "\xe8\x1e"},
		{61, "\xe8\x1f"},
		{62, "\xe8 "},
		{254, "\xe8\xe0"},
		{255, "\xe8\xe1"},
		{256, "\xe8\xe2"},
		{257, "\xe8\xe3"},
		{65534, "\xf0\xe0\xff"},
		{65535, "\xf0\xe1\xff"},
		{65536, "\xf0\xe2\xff"},
		{165536, "\xf8\x82\x86\x02"},
	}

	dst := make([]byte, MaxEncodedLen(MaxBlockSize))
	nines := bytes.Repeat([]byte{0x99}, MaxBlockSize)
	for _, tc := range testCases {
		lit := nines[:tc.length]
		n := emitLiteral(dst, lit)
		if !bytes.HasSuffix(dst[:n], lit) {
			t.Errorf("length=%d: did not end with that many literal bytes", tc.length)
			continue
		}
		got := string(dst[:n-tc.length])
		if got != tc.want {
			t.Errorf("length=%d:\ngot  %q\nwant %q", tc.length, got, tc.want)
			continue
		}
	}
}

func TestEmitCopy(t *testing.T) {
	testCases := []struct {
		offset int
		length int
		want   []byte
	}{
		// 10 bit offsets. (copy1)
		{offset: 8, length: 4, want: []uint8{0xc1, 0x1}},
		{offset: 8, length: 11, want: []uint8{0xdd, 0x1}},
		{offset: 8, length: 12, want: []uint8{0xe1, 0x1}},
		{offset: 8, length: 13, want: []uint8{0xe5, 0x1}},
		{offset: 8, length: 17, want: []uint8{0xf5, 0x1}},
		{offset: 8, length: 18, want: []uint8{0xf9, 0x1}},
		{offset: 8, length: 19, want: []uint8{0xfd, 0x1, 0x1}}, // Extra length byte.
		{offset: 8, length: 59, want: []uint8{0xfd, 0x1, 0x29}},
		{offset: 8, length: 60, want: []uint8{0xfd, 0x1, 0x2a}},
		{offset: 8, length: 61, want: []uint8{0xfd, 0x1, 0x2b}},
		{offset: 8, length: 62, want: []uint8{0xfd, 0x1, 0x2c}},
		{offset: 8, length: 63, want: []uint8{0xfd, 0x1, 0x2d}},
		{offset: 8, length: 64, want: []uint8{0xfd, 0x1, 0x2e}},
		{offset: 8, length: 65, want: []uint8{0xfd, 0x1, 0x2f}},
		{offset: 8, length: 66, want: []uint8{0xfd, 0x1, 0x30}},
		{offset: 8, length: 67, want: []uint8{0xfd, 0x1, 0x31}},
		{offset: 8, length: 68, want: []uint8{0xfd, 0x1, 0x32}},
		{offset: 8, length: 69, want: []uint8{0xfd, 0x1, 0x33}},
		{offset: 8, length: 80, want: []uint8{0xfd, 0x1, 0x3e}},
		{offset: 8, length: 800, want: []uint8{0xf9, 0x1, 0xf4, 0xf0, 0x2}}, // (copy+repeat)
		{offset: 8, length: 800000, want: []uint8{0xf9, 0x1, 0xfc, 0xd0, 0x34, 0xc}},

		{offset: 256, length: 4, want: []uint8{0xc1, 0x3f}},
		{offset: 256, length: 11, want: []uint8{0xdd, 0x3f}},
		{offset: 256, length: 12, want: []uint8{0xe1, 0x3f}},
		{offset: 256, length: 13, want: []uint8{0xe5, 0x3f}},
		{offset: 256, length: 18, want: []uint8{0xf9, 0x3f}},
		{offset: 256, length: 19, want: []uint8{0xfd, 0x3f, 0x1}}, // Extra length byte.
		{offset: 256, length: 59, want: []uint8{0xfd, 0x3f, 0x29}},
		{offset: 256, length: 60, want: []uint8{0xfd, 0x3f, 0x2a}},
		{offset: 256, length: 61, want: []uint8{0xfd, 0x3f, 0x2b}},
		{offset: 256, length: 62, want: []uint8{0xfd, 0x3f, 0x2c}},
		{offset: 256, length: 63, want: []uint8{0xfd, 0x3f, 0x2d}},
		{offset: 256, length: 64, want: []uint8{0xfd, 0x3f, 0x2e}},
		{offset: 256, length: 65, want: []uint8{0xfd, 0x3f, 0x2f}},
		{offset: 256, length: 66, want: []uint8{0xfd, 0x3f, 0x30}},
		{offset: 256, length: 67, want: []uint8{0xfd, 0x3f, 0x31}},
		{offset: 256, length: 68, want: []uint8{0xfd, 0x3f, 0x32}},
		{offset: 256, length: 69, want: []uint8{0xfd, 0x3f, 0x33}},
		{offset: 256, length: 80, want: []uint8{0xfd, 0x3f, 0x3e}},
		{offset: 256, length: 800, want: []uint8{0xf9, 0x3f, 0xf4, 0xf0, 0x2}}, // (copy+repeat)
		{offset: 256, length: 80000, want: []uint8{0xf9, 0x3f, 0xfc, 0x50, 0x38, 0x1}},

		// 16 bit offsets. (copy2)
		{offset: 2048, length: 4, want: []uint8{0x2, 0xc0, 0x7}},
		{offset: 2048, length: 11, want: []uint8{0x1e, 0xc0, 0x7}},
		{offset: 2048, length: 12, want: []uint8{0x22, 0xc0, 0x7}},
		{offset: 2048, length: 13, want: []uint8{0x26, 0xc0, 0x7}},
		{offset: 2048, length: 59, want: []uint8{0xde, 0xc0, 0x7}},
		{offset: 2048, length: 60, want: []uint8{0xe2, 0xc0, 0x7}},
		{offset: 2048, length: 61, want: []uint8{0xe6, 0xc0, 0x7}},
		{offset: 2048, length: 62, want: []uint8{0xea, 0xc0, 0x7}},
		{offset: 2048, length: 63, want: []uint8{0xee, 0xc0, 0x7}},
		{offset: 2048, length: 64, want: []uint8{0xf2, 0xc0, 0x7}},
		{offset: 2048, length: 65, want: []uint8{0xf6, 0xc0, 0x7, 0x1}}, // Extra length bytes.
		{offset: 2048, length: 66, want: []uint8{0xf6, 0xc0, 0x7, 0x2}},
		{offset: 2048, length: 67, want: []uint8{0xf6, 0xc0, 0x7, 0x3}},
		{offset: 2048, length: 68, want: []uint8{0xf6, 0xc0, 0x7, 0x4}},
		{offset: 2048, length: 69, want: []uint8{0xf6, 0xc0, 0x7, 0x5}},
		{offset: 2048, length: 80, want: []uint8{0xf6, 0xc0, 0x7, 0x10}},
		{offset: 2048, length: 800, want: []uint8{0xfa, 0xc0, 0x7, 0xe0, 0x2}},
		{offset: 2048, length: 80000, want: []uint8{0xfe, 0xc0, 0x7, 0x40, 0x38, 0x1}},

		// 22 bit offsets. (copy3)
		{offset: 204800, length: 4, want: []uint8{0x7, 0x0, 0x0, 0x11}},
		{offset: 204800, length: 28, want: []uint8{0x7, 0x3, 0x0, 0x11}},
		{offset: 204800, length: 32, want: []uint8{0x87, 0x3, 0x0, 0x11}},
		{offset: 204800, length: 33, want: []uint8{0xa7, 0x3, 0x0, 0x11}},
		{offset: 204800, length: 40, want: []uint8{0x87, 0x4, 0x0, 0x11}},
		{offset: 204800, length: 65, want: []uint8{0xa7, 0x7, 0x0, 0x11, 0x1}}, // add extra length bytes
		{offset: 204800, length: 69, want: []uint8{0xa7, 0x7, 0x0, 0x11, 0x5}},
		{offset: 204800, length: 800, want: []uint8{0xc7, 0x7, 0x0, 0x11, 0xe0, 0x2}},
		{offset: 204800, length: 80000, want: []uint8{0xe7, 0x7, 0x0, 0x11, 0x40, 0x38, 0x1}},
	}

	dst := make([]byte, 1024)
	if false {
		var gotCases []struct {
			offset int
			length int
			want   []byte
		}
		for _, tc := range testCases {
			n := emitCopy(dst, tc.offset, tc.length)
			got := string(dst[:n])
			gotCases = append(gotCases, struct {
				offset int
				length int
				want   []byte
			}{
				offset: tc.offset,
				length: tc.length,
				want:   []byte(got),
			})
		}
		t.Logf("gotCases = %#v", gotCases)
		return
	}

	for _, tc := range testCases {
		n := emitCopy(dst, tc.offset, tc.length)
		got := dst[:n]
		if !bytes.Equal(got, tc.want) {
			t.Errorf("offset=%d, length=%d:\ngot  %#v\nwant %#v", tc.offset, tc.length, got, tc.want)
		}
	}
}

func TestNewWriter(t *testing.T) {
	// Test all 32 possible sub-sequences of these 5 input slices.
	//
	// Their lengths sum to 400,000, which is over 6 times the Writer ibuf
	// capacity: 6 * maxBlockSize is 393,216.
	inputs := [][]byte{
		bytes.Repeat([]byte{'a'}, 40000),
		bytes.Repeat([]byte{'b'}, 150000),
		bytes.Repeat([]byte{'c'}, 60000),
		bytes.Repeat([]byte{'d'}, 120000),
		bytes.Repeat([]byte{'e'}, 30000),
	}
loop:
	for i := 0; i < 1<<uint(len(inputs)); i++ {
		var want []byte
		buf := new(bytes.Buffer)
		w := NewWriter(buf)
		for j, input := range inputs {
			if i&(1<<uint(j)) == 0 {
				continue
			}
			if _, err := w.Write(input); err != nil {
				t.Errorf("i=%#02x: j=%d: Write: %v", i, j, err)
				continue loop
			}
			want = append(want, input...)
		}
		if err := w.Close(); err != nil {
			t.Errorf("i=%#02x: Close: %v", i, err)
			continue
		}
		got, err := io.ReadAll(NewReader(buf))
		if err != nil {
			t.Errorf("i=%#02x: ReadAll: %v", i, err)
			continue
		}
		if err := cmp(got, want); err != nil {
			t.Errorf("i=%#02x: %v", i, err)
			continue
		}
		return
	}
}

func TestFlush(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte{'x'}, 20)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := buf.Len(); n != 0 {
		t.Fatalf("before Flush: %d bytes were written to the underlying io.Writer, want 0", n)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := buf.Len(); n == 0 {
		t.Fatalf("after Flush: %d bytes were written to the underlying io.Writer, want non-0", n)
	}
}

func TestReaderS2UncompressedDataOK(t *testing.T) {
	r := NewReader(strings.NewReader(magicChunkS2+
		"\x01\x08\x00\x00"+ // Uncompressed chunk, 8 bytes long (including 4 byte checksum).
		"\x68\x10\xe6\xb6"+ // Checksum.
		"\x61\x62\x63\x64", // Uncompressed payload: "abcd".
	), ReaderFallback(true))
	g, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(g), "abcd"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReaderSnappyUncompressedDataOK(t *testing.T) {
	r := NewReader(strings.NewReader(magicChunkSnappy+
		"\x01\x08\x00\x00"+ // Uncompressed chunk, 8 bytes long (including 4 byte checksum).
		"\x68\x10\xe6\xb6"+ // Checksum.
		"\x61\x62\x63\x64", // Uncompressed payload: "abcd".
	), ReaderFallback(true))
	g, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(g), "abcd"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReaderMinLZUncompressedDataOK(t *testing.T) {
	r := NewReader(strings.NewReader(string(makeHeader(4096))+
		"\x01\x08\x00\x00"+ // Uncompressed chunk, 8 bytes long (including 4 byte checksum).
		"\x68\x10\xe6\xb6"+ // Checksum.
		"\x61\x62\x63\x64"+ // Uncompressed payload: "abcd".
		"\x20\x00\x00\x00", // EOF
	), ReaderFallback(false))
	g, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(g), "abcd"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReaderUncompressedDataNoPayload(t *testing.T) {
	r := NewReader(strings.NewReader(string(makeHeader(4096)) +
		"\x01\x04\x00\x00" + // Uncompressed chunk, 4 bytes long.
		"", // No payload; corrupt input.
	))
	if _, err := io.ReadAll(r); err != ErrCorrupt {
		t.Fatalf("got %v, want %v", err, ErrCorrupt)
	}
}

func TestReaderUncompressedDataTooLong(t *testing.T) {
	// The maximum legal chunk length... is 8MB + 4 bytes checksum.
	n := maxBlockSize + checksumSize
	n32 := uint32(n)
	r := NewReader(strings.NewReader(string(makeHeader(maxBlockSize)) +
		// Uncompressed chunk, n bytes long.
		string([]byte{chunkTypeUncompressedData, uint8(n32), uint8(n32 >> 8), uint8(n32 >> 16)}) +
		strings.Repeat("\x00", n),
	))
	// CRC is not set, so we should expect that error.
	if _, err := io.ReadAll(r); err != ErrCRC {
		t.Fatalf("got %v, want %v", err, ErrCRC)
	}

	// test first invalid.
	n++
	n32 = uint32(n)
	r = NewReader(strings.NewReader(string(makeHeader(maxBlockSize)) +
		// Uncompressed chunk, n bytes long.
		string([]byte{chunkTypeUncompressedData, uint8(n32), uint8(n32 >> 8), uint8(n32 >> 16)}) +
		strings.Repeat("\x00", n),
	))
	if _, err := io.ReadAll(r); err != ErrTooLarge {
		t.Fatalf("got %v, want %v", err, ErrTooLarge)
	}

	n = 1<<20 + 1 + checksumSize
	n32 = uint32(n)
	r = NewReader(strings.NewReader(string(makeHeader(1<<20)) +
		// Uncompressed chunk, n bytes long.
		string([]byte{chunkTypeUncompressedData, uint8(n32), uint8(n32 >> 8), uint8(n32 >> 16)}) +
		strings.Repeat("\x00", n),
	))
	if _, err := io.ReadAll(r); err != ErrTooLarge {
		t.Fatalf("got %v, want %v", err, ErrTooLarge)
	}
}

func TestReaderReset(t *testing.T) {
	gold := bytes.Repeat([]byte("All that is gold does not glitter,\n"), 10000)
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	_, err := w.Write(gold)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	t.Logf("encoded length: %d", buf.Len())
	encoded, invalid, partial := buf.String(), "invalid", "partial"
	r := NewReader(nil)
	for i, s := range []string{encoded, invalid, partial, encoded, partial, invalid, encoded, encoded} {
		if s == partial {
			r.Reset(strings.NewReader(encoded))
			if _, err := r.Read(make([]byte, 101)); err != nil {
				t.Errorf("#%d: %v", i, err)
				continue
			}
			continue
		}
		r.Reset(strings.NewReader(s))
		got, err := io.ReadAll(r)
		switch s {
		case encoded:
			if err != nil {
				t.Errorf("#%d: %v", i, err)
				continue
			}
			if err := cmp(got, gold); err != nil {
				t.Errorf("#%d: %v", i, err)
				continue
			}
		case invalid:
			if err == nil {
				t.Errorf("#%d: got nil error, want non-nil", i)
				continue
			}
		}
	}
}

func TestWriterReset(t *testing.T) {
	gold := bytes.Repeat([]byte("Not all those who wander are lost;\n"), 10000)
	const n = 20
	w := NewWriter(nil)
	defer w.Close()

	var gots, wants [][]byte
	failed := false
	for i := 0; i <= n; i++ {
		buf := new(bytes.Buffer)
		w.Reset(buf)
		want := gold[:len(gold)*i/n]
		if _, err := w.Write(want); err != nil {
			t.Errorf("#%d: Write: %v", i, err)
			failed = true
			continue
		}
		if err := w.Flush(); err != nil {
			t.Errorf("#%d: Flush: %v", i, err)
			failed = true
			got, err := io.ReadAll(NewReader(buf))
			if err != nil {
				t.Errorf("#%d: ReadAll: %v", i, err)
				failed = true
				continue
			}
			gots = append(gots, got)
			wants = append(wants, want)
		}
		if failed {
			continue
		}
		for i := range gots {
			if err := cmp(gots[i], wants[i]); err != nil {
				t.Errorf("#%d: %v", i, err)
			}
		}
	}
}

func TestWriterResetWithoutFlush(t *testing.T) {
	buf0 := new(bytes.Buffer)
	buf1 := new(bytes.Buffer)
	w := NewWriter(buf0)
	if _, err := w.Write([]byte("xxx")); err != nil {
		t.Fatalf("Write #0: %v", err)
	}
	// Note that we don't Flush the Writer before calling Reset.
	w.Reset(buf1)
	if _, err := w.Write([]byte("yyy")); err != nil {
		t.Fatalf("Write #1: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := io.ReadAll(NewReader(buf1))
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := cmp(got, []byte("yyy")); err != nil {
		t.Fatal(err)
	}
}

type writeCounter int

func (c *writeCounter) Write(p []byte) (int, error) {
	*c++
	return len(p), nil
}

// TestNumUnderlyingWrites tests that each Writer flush only makes one or two
// Write calls on its underlying io.Writer, depending on whether or not the
// flushed buffer was compressible.
func TestNumUnderlyingWrites(t *testing.T) {
	testCases := []struct {
		input []byte
		want  int
	}{
		// Magic header + block
		{bytes.Repeat([]byte{'x'}, 100), 2},
		// One block each:
		{bytes.Repeat([]byte{'y'}, 100), 1},
		{[]byte("ABCDEFGHIJKLMNOPQRST"), 1},
	}

	// If we are doing sync writes, we write uncompressed as two writes.
	if runtime.GOMAXPROCS(0) == 1 {
		testCases[2].want++
	}
	var c writeCounter
	w := NewWriter(&c)
	defer w.Close()
	for i, tc := range testCases {
		c = 0
		if _, err := w.Write(tc.input); err != nil {
			t.Errorf("#%d: Write: %v", i, err)
			continue
		}
		if err := w.Flush(); err != nil {
			t.Errorf("#%d: Flush: %v", i, err)
			continue
		}
		if int(c) != tc.want {
			t.Errorf("#%d: got %d underlying writes, want %d", i, c, tc.want)
			continue
		}
	}
}

func testWriterRoundtrip(t *testing.T, src []byte, opts ...WriterOption) {
	var buf bytes.Buffer
	enc := NewWriter(&buf, opts...)
	n, err := enc.Write(src)
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(src) {
		t.Error(io.ErrShortWrite)
		return
	}
	err = enc.Flush()
	if err != nil {
		t.Error(err)
		return
	}
	// Extra flush and close should be noops.
	err = enc.Flush()
	if err != nil {
		t.Error(err)
		return
	}
	err = enc.Close()
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("encoded to %d -> %d bytes", len(src), buf.Len())
	dec := NewReader(&buf)
	decoded, err := io.ReadAll(dec)
	if err != nil {
		t.Error(err)
		return
	}
	if len(decoded) != len(src) {
		t.Error("decoded len:", len(decoded), "!=", len(src))
		return
	}
	err = cmp(src, decoded)
	if err != nil {
		t.Error(err)
	}
}

func testBlockRoundtrip(t *testing.T, src []byte, level int) {
	dst, _ := Encode(nil, src, level)
	t.Logf("encoded to %d -> %d bytes", len(src), len(dst))
	decoded, err := Decode(nil, dst)
	if err != nil {
		t.Error(err)
		return
	}
	if len(decoded) != len(src) {
		t.Error("decoded len:", len(decoded), "!=", len(src))
		return
	}
	err = cmp(decoded, src)
	if err != nil {
		t.Error(err)
	}
}

/*
func testSnappyDecode(t *testing.T, src []byte) {
	var buf bytes.Buffer
	enc := snapref.NewBufferedWriter(&buf)
	n, err := enc.Write(src)
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(src) {
		t.Error(io.ErrShortWrite)
		return
	}
	enc.Close()
	t.Logf("encoded to %d -> %d bytes", len(src), buf.Len())
	dec := NewReader(&buf)
	decoded, err := io.ReadAll(dec)
	if err != nil {
		t.Error(err)
		return
	}
	if len(decoded) != len(src) {
		t.Error("decoded len:", len(decoded), "!=", len(src))
		return
	}
	err = cmp(src, decoded)
	if err != nil {
		t.Error(err)
	}
}
*/

func testOrBenchmark(b testing.TB) string {
	if _, ok := b.(*testing.B); ok {
		return "benchmark"
	}
	return "test"
}

func readFile(b testing.TB, filename string) []byte {
	src, err := os.ReadFile(filename)
	if err != nil {
		b.Skipf("skipping %s: %v", testOrBenchmark(b), err)
	}
	if len(src) == 0 {
		b.Fatalf("%s has zero length", filename)
	}
	return src
}

// expand returns a slice of length n containing mutated copies of src.
func expand(src []byte, n int) []byte {
	dst := make([]byte, n)
	cnt := uint8(0)
	for x := dst; len(x) > 0; cnt++ {
		idx := copy(x, src)
		for i := range x {
			if i >= len(src) {
				break
			}
			x[i] = src[i] ^ cnt
		}
		x = x[idx:]
	}
	return dst
}

func TestRoundtrips(t *testing.T) {
	testFile(t, 0, 10)
	testFile(t, 1, 10)
	testFile(t, 2, 10)
	testFile(t, 3, 10)
	testFile(t, 4, 10)
	testFile(t, 5, 10)
	testFile(t, 6, 10)
	testFile(t, 7, 10)
	testFile(t, 8, 10)
	testFile(t, 9, 10)
	testFile(t, 10, 10)
	testFile(t, 11, 10)
	testFile(t, 12, 0)
	testFile(t, 13, 0)
	testFile(t, 14, 0)
	testFile(t, 15, 0)
}

func testFile(t *testing.T, i, repeat int) {
	if err := downloadBenchmarkFiles(t, testFiles[i].filename); err != nil {
		t.Skipf("failed to download testdata: %s", err)
	}

	if testing.Short() {
		repeat = 0
	}
	t.Run(fmt.Sprint(i, "-", testFiles[i].label), func(t *testing.T) {
		bDir := filepath.FromSlash(*benchdataDir)
		data := readFile(t, filepath.Join(bDir, testFiles[i].filename))
		if testing.Short() && len(data) > 10000 {
			t.SkipNow()
		}
		oSize := len(data)
		for i := 0; i < repeat; i++ {
			data = append(data, data[:oSize]...)
		}
		t.Run("stream-1", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterLevel(LevelFastest))
		})
		t.Run("stream-2", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterLevel(LevelBalanced))
		})
		t.Run("stream-3", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterLevel(LevelSmallest))
		})
		t.Run("stream-0", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterUncompressed())
		})
		t.Run("block", func(t *testing.T) {
			d := data
			testBlockRoundtrip(t, d, LevelFastest)
		})
		t.Run("block-better", func(t *testing.T) {
			d := data
			testBlockRoundtrip(t, d, LevelBalanced)
		})
		t.Run("block-best", func(t *testing.T) {
			d := data
			testBlockRoundtrip(t, d, LevelSmallest)
		})
	})
}

func TestDataRoundtrips(t *testing.T) {
	test := func(t *testing.T, data []byte) {
		t.Run("stream-1", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterLevel(LevelFastest))
		})
		t.Run("stream-2", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterLevel(LevelBalanced))
		})
		t.Run("stream-3", func(t *testing.T) {
			testWriterRoundtrip(t, data, WriterLevel(LevelSmallest))
		})
		t.Run("block-1", func(t *testing.T) {
			d := data
			testBlockRoundtrip(t, d, LevelFastest)
		})
		t.Run("block-2", func(t *testing.T) {
			d := data
			testBlockRoundtrip(t, d, LevelBalanced)
		})
		t.Run("block-3", func(t *testing.T) {
			d := data
			testBlockRoundtrip(t, d, LevelSmallest)
		})
	}
	t.Run("longblock", func(t *testing.T) {
		data := make([]byte, 8<<20)
		if testing.Short() {
			data = data[:1<<20]
		}
		test(t, data)
	})
	t.Run("4f9e1a0", func(t *testing.T) {
		comp, _ := os.ReadFile("testdata/4f9e1a0da7915a3d69632f5613ed78bc998a8a23.zst")
		dec, _ := zstd.NewReader(bytes.NewBuffer(comp))
		data, _ := io.ReadAll(dec)
		test(t, data)
	})
	data, err := os.ReadFile("testdata/enc_regressions.zip")
	if err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range zr.File {
		if !strings.HasSuffix(t.Name(), "") {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			r, err := tt.Open()
			if err != nil {
				t.Error(err)
				return
			}
			b, err := io.ReadAll(r)
			if err != nil {
				t.Error(err)
				return
			}
			test(t, b[:len(b):len(b)])
		})
	}

}

func TestMatchLen(t *testing.T) {
	// ref is a simple, reference implementation of matchLen.
	ref := func(a, b []byte) int {
		n := 0
		for i := range a {
			if a[i] != b[i] {
				break
			}
			n++
		}
		return n
	}

	// We allow slightly shorter matches at the end of slices
	const maxBelow = 0
	nums := []int{0, 1, 2, 7, 8, 9, 16, 20, 29, 30, 31, 32, 33, 34, 38, 39, 40}
	for yIndex := 40; yIndex > 30; yIndex-- {
		xxx := bytes.Repeat([]byte("x"), 40)
		if yIndex < len(xxx) {
			xxx[yIndex] = 'y'
		}
		for _, i := range nums {
			for _, j := range nums {
				if i >= j {
					continue
				}
				got := matchLen(xxx[j:], xxx[i:])
				want := ref(xxx[j:], xxx[i:])
				if got > want {
					t.Errorf("yIndex=%d, i=%d, j=%d: got %d, want %d", yIndex, i, j, got, want)
					continue
				}
				if got < want-maxBelow {
					t.Errorf("yIndex=%d, i=%d, j=%d: got %d, want %d", yIndex, i, j, got, want)
				}
			}
		}
	}
}

func TestLeadingSkippableBlock(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.AddUserChunk(MinUserSkippableChunk, []byte("skippable block")); err != nil {
		t.Fatalf("w.AddUserChunk: %v", err)
	}
	if _, err := w.Write([]byte("some data")); err != nil {
		t.Fatalf("w.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}
	r := NewReader(&buf)
	var sb []byte
	err := r.UserChunkCB(MinUserSkippableChunk, func(sr io.Reader) error {
		var err error
		sb, err = io.ReadAll(sr)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read([]byte{}); err != nil {
		t.Errorf("empty read failed: %v", err)
	}
	if !bytes.Equal(sb, []byte("skippable block")) {
		t.Errorf("didn't get correct data from skippable block: %q", string(sb))
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("r.Read: %v", err)
	}
	if !bytes.Equal(data, []byte("some data")) {
		t.Errorf("didn't get correct compressed data: %q", string(data))
	}
}

func TestLeadingNonSkippableBlock(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.AddUserChunk(MinUserNonSkippableChunk, []byte("non-skippable block")); err != nil {
		t.Fatalf("w.AddUserChunk: %v", err)
	}
	if _, err := w.Write([]byte("some data")); err != nil {
		t.Fatalf("w.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}
	r := NewReader(&buf)
	var sb []byte
	err := r.UserChunkCB(MinUserNonSkippableChunk, func(sr io.Reader) error {
		var err error
		sb, err = io.ReadAll(sr)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read([]byte{}); err != nil {
		t.Errorf("empty read failed: %v", err)
	}
	if !bytes.Equal(sb, []byte("non-skippable block")) {
		t.Errorf("didn't get correct data from skippable block: %q", string(sb))
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("r.Read: %v", err)
	}
	if !bytes.Equal(data, []byte("some data")) {
		t.Errorf("didn't get correct compressed data: %q", string(data))
	}
}

func ExampleWriter_AddUserChunk() {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	// Add a skippable chunk
	if err := w.AddUserChunk(MinUserSkippableChunk, []byte("Chunk Custom Data")); err != nil {
		log.Fatalf("w.AddUserChunk: %v", err)
	}
	//
	if _, err := w.Write([]byte("some data")); err != nil {
		log.Fatalf("w.Write: %v", err)
	}
	w.Close()

	// Read back what we wrote.
	r := NewReader(&buf)
	err := r.UserChunkCB(MinUserSkippableChunk, func(sr io.Reader) error {
		var err error
		b, err := io.ReadAll(sr)
		fmt.Println("Callback:", string(b), err)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	// Read stream data
	b, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Stream data:", string(b))

	//OUTPUT:
	//Callback: Chunk Custom Data <nil>
	//Stream data: some data
}

func TestLeadingSkippableBlockNonReg(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.AddUserChunk(MinUserSkippableChunk, []byte("skippable block")); err != nil {
		t.Fatalf("w.AddUserChunk: %v", err)
	}
	if _, err := w.Write([]byte("some data")); err != nil {
		t.Fatalf("w.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}
	r := NewReader(&buf)
	if _, err := r.Read([]byte{}); err != nil {
		t.Errorf("empty read failed: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("r.Read: %v", err)
	}
	if !bytes.Equal(data, []byte("some data")) {
		t.Errorf("didn't get correct compressed data: %q", string(data))
	}
}

func TestLeadingNonSkippableBlockNonReg(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.AddUserChunk(MinUserNonSkippableChunk, []byte("non-skippable block")); err != nil {
		t.Fatalf("w.AddUserChunk: %v", err)
	}
	if _, err := w.Write([]byte("some data")); err != nil {
		t.Fatalf("w.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}
	r := NewReader(&buf)
	if _, err := r.Read([]byte{}); err == nil {
		t.Errorf("empty read did not produce error: %v", err)
	} else {
		t.Log("got expected:", err)
	}
}

// decodeGo is the same as Decode, but only using Go for MinLZ.
// Should Only be used for MinLZ blocks.
func decodeGo(dst, src []byte) ([]byte, error) {
	isMLZ, lits, block, dLen, err := isMinLZ(src)
	if err != nil {
		return nil, err
	}
	if !isMLZ {
		return nil, ErrCorrupt
	}
	if dLen > MaxBlockSize {
		return nil, ErrTooLarge
	}
	if lits {
		return append(dst[:0], block...), nil
	}
	if dLen <= cap(dst) {
		dst = dst[:dLen]
	} else {
		dst = make([]byte, dLen, dLen)
	}
	if minLZDecodeGo(dst, block) != 0 {
		return dst, ErrCorrupt
	}
	return dst, nil
}
