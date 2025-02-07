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
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/minio/minlz/internal/fuzz"
	"github.com/minio/minlz/internal/reference"
)

func FuzzEncodingBlocks(f *testing.F) {
	fuzz.AddFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false)
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, testing.Short())
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, testing.Short())

	for i := range testFiles {
		if err := downloadBenchmarkFiles(f, testFiles[i].filename); err != nil {
			f.Fatalf("failed to download testdata: %s", err)
		}
		bDir := filepath.FromSlash(*benchdataDir)
		f.Add(readFile(f, filepath.Join(bDir, testFiles[i].filename)))
	}
	// Fuzzing tweaks:
	const (
		// Max input size:
		maxSize = MaxBlockSize
	)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxSize {
			return
		}
		writeDst := make([]byte, MaxEncodedLen(len(data)), MaxEncodedLen(len(data))+4)
		writeDst = append(writeDst, 1, 2, 3, 4)
		defer func() {
			got := writeDst[MaxEncodedLen(len(data)):]
			want := []byte{1, 2, 3, 4}
			if !bytes.Equal(got, want) {
				t.Fatalf("want %v, got %v - dest modified outside cap", want, got)
			}
		}()
		compDst := writeDst[:MaxEncodedLen(len(data)):MaxEncodedLen(len(data))] // Hard cap

		decDst := make([]byte, len(data), len(data)+4)
		decDst = append(decDst, 1, 2, 3, 4)
		defer func(got []byte) {
			want := []byte{1, 2, 3, 4}
			if !bytes.Equal(got, want) {
				t.Fatalf("want %v, got %v - decode dest modified outside cap", want, got)
			}
		}(decDst[len(decDst)-4:])
		decDst = decDst[:len(data):len(data)]
		const levelReference = LevelSmallest + 1
		for l := LevelFastest; l <= levelReference; l++ {
			for i := range decDst {
				decDst[i] = 0xfe
			}
			for i := range compDst {
				compDst[i] = 0xff
			}
			var comp []byte
			if l < levelReference {
				comp, _ = Encode(nil, data, l)
			} else {
				comp, _ = reference.EncodeBlock(data)
			}
			decoded, err := decodeGo(decDst, comp)
			if err != nil {
				t.Errorf("level %d: Decode error: %v", l, err)
			}
			if !bytes.Equal(data, decoded) {
				t.Errorf("level: %d - block decoder mismatch", l)
				got := decoded
				want := data
				var n int
				if got != nil {
					n = matchLen(want, got)
				}
				x := crc32.ChecksumIEEE(want)
				name := fmt.Sprintf("errs/block-%08x", x)
				fmt.Println(name, "mismatch at pos", n)
				os.WriteFile(name+"input.bin", data, 0644)
				os.WriteFile(name+"encoded.bin", comp, 0644)
				os.WriteFile(name+"decoded.bin", got, 0644)
				t.Errorf("roundtrip Go mismatch at byte %d, output written to %q", n, name)
				return
			}
			if mel := MaxEncodedLen(len(data)); len(comp) > mel {
				t.Error(fmt.Errorf("MaxEncodedLen Exceed: input: %d, mel: %d, got %d", len(data), mel, len(comp)))
				return
			}
			if !bytes.Equal(writeDst[len(writeDst)-4:], []byte{1, 2, 3, 4}) {
				t.Error("overwritten", writeDst[len(writeDst)-4:])
				return
			}
		}
	})
}

func FuzzDecode(f *testing.F) {
	enc := NewWriter(nil, WriterBlockSize(8<<10))
	addCompressed := func(b []byte) {
		if b2, err := Encode(nil, b, LevelBalanced); err == nil {
			f.Add(b2)
		}
		f.Add(s2.EncodeBetter(nil, b))
		var buf bytes.Buffer
		enc.Reset(&buf)
		enc.Write(b)
		enc.Close()
		f.Add(buf.Bytes())
	}
	fuzz.ReturnFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, addCompressed)
	fuzz.AddFromZip(f, "testdata/dec-block-regressions.zip", fuzz.TypeRaw, false)

	dec := NewReader(nil, ReaderIgnoreCRC())
	f.Fuzz(func(t *testing.T, data []byte) {
		if t.Failed() {
			return
		}
		if bytes.HasPrefix(data, []byte(magicChunk)) {
			dec.Reset(bytes.NewReader(data))
			_, err := io.Copy(io.Discard, dec)
			if true {
				dec.Reset(bytes.NewReader(data))
				_, cErr := dec.DecodeConcurrent(io.Discard, 2)
				if (err == nil) != (cErr == nil) {
					t.Error("error mismatch", err, cErr)
				}
			}
			return
		}
		dCopy := append([]byte{}, data...)
		dlen, err := DecodedLen(data)
		base, baseErr := Decode(nil, data)
		if !bytes.Equal(data, dCopy) {
			t.Fatal("data was changed")
		}
		hasErr := baseErr != nil
		dataCapped := make([]byte, 0, len(data)+1024)
		dataCapped = append(dataCapped, data...)
		dataCapped = append(dataCapped, bytes.Repeat([]byte{0xff, 0xff, 0xff, 0xff}, 1024/4)...)
		dataCapped = dataCapped[:len(data):len(data)]
		if dlen > MaxBlockSize {
			dlen = MaxBlockSize
		}
		dst2 := bytes.Repeat([]byte{0xfe}, dlen+1024)
		got, err := Decode(dst2[:dlen:dlen], dataCapped[:len(data)])
		if !bytes.Equal(dataCapped[:len(data)], dCopy) {
			t.Fatal("data was changed")
		}
		if err != nil && !hasErr {
			t.Fatalf("base err: %v, capped: %v", baseErr, err)
		}
		for i, v := range dst2[dlen:] {
			if v != 0xfe {
				t.Errorf("DST overwritten beyond cap! index %d: got 0x%02x, want 0x%02x, err:%v", i, v, 0xfe, err)
				break
			}
		}
		if hasErr {
			if hasAsm {
				isLz, sz, mzErr := IsMinLZ(data)
				// Check against pure Go decoder.
				_, err := decodeGo(nil, data)
				if err == nil {
					t.Errorf("base err: %v, Go: %v, IsMinLZ: %v, sz: %v, IsMinLZ:%v", baseErr, err, isLz, sz, mzErr)
				}
			}
			return
		}
		if !bytes.Equal(base, got) {
			t.Error("base mismatch")
		}
		isLz, sz, mzErr := IsMinLZ(data)
		if isLz {
			gotLen, err := DecodedLen(data)
			if err != nil {
				t.Errorf("DecodedLen returned error: %v", err)
			} else if gotLen != len(got) {
				t.Errorf("DecodedLen mismatch: got %d, want %d", gotLen, len(got))
			}

			if hasAsm {
				// Check against pure Go decoder.
				got, err := decodeGo(nil, data)
				if err != nil {
					t.Errorf("base err: %v, Go: %v, IsMinLZ: %v, sz: %v, IsMinLZ:%v", baseErr, err, isLz, sz, mzErr)
				}
				if !bytes.Equal(base, got) {
					var n int
					if got != nil {
						n = matchLen(base, got)
					}
					x := crc32.ChecksumIEEE(base)
					name := fmt.Sprintf("errs/block-%08x", x)
					fmt.Println(name, "mismatch at pos", n)
					os.WriteFile(name+"input.bin", data, 0644)
					os.WriteFile(name+"decoded-asm.bin", base, 0644)
					os.WriteFile(name+"decoded-go.bin", got, 0644)
					t.Errorf("pure Go mismatch at byte %d, output written to %q", n, name)
				}
				return
			}
			// Check against reference decoder.
			got, err := reference.DecodeBlock(data)
			if err != nil {
				t.Fatalf("base err: %v, Reference: %v, IsMinLZ: %v,%v,%v len(%v)", baseErr, err, isLz, sz, mzErr, len(data))
			}
			if !bytes.Equal(base, got) {
				t.Error("Reference mismatch")
			}
		}
	})
}

func FuzzStreamEncode(f *testing.F) {
	fuzz.AddFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false)
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, false)
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, false)

	var encoders []*Writer
	for l := LevelFastest; l <= LevelSmallest; l++ {
		encoders = append(encoders, NewWriter(nil, WriterLevel(l), WriterConcurrency(1), WriterBlockSize(128<<10)))
		if !testing.Short() && l == LevelFastest {
			// Try some combinations...
			encoders = append(encoders, NewWriter(nil, WriterLevel(l), WriterConcurrency(1), WriterBlockSize(8<<20)))
			encoders = append(encoders, NewWriter(nil, WriterLevel(l), WriterConcurrency(1), WriterBlockSize(4<<10), WriterAddIndex(true)))
			encoders = append(encoders, NewWriter(nil, WriterLevel(l), WriterConcurrency(2), WriterBlockSize(16<<10), WriterPadding(1<<10)))
			encoders = append(encoders, NewWriter(nil, WriterLevel(l), WriterConcurrency(2), WriterBlockSize(128<<10), WriterPadding(1<<10), WriterAddIndex(true)))
		}
		break
	}
	dec := NewReader(nil, ReaderFallback(false))
	f.Fuzz(func(t *testing.T, data []byte) {
		var comp bytes.Buffer
		var decomp bytes.Buffer
		for i, enc := range encoders {
			comp.Reset()
			enc.Reset(&comp)
			var err error
			writeMethod := (len(data) + i) % 3
			switch writeMethod {
			// Pure Write
			case 0:
				_, err = io.Copy(struct{ io.Writer }{Writer: enc}, io.NopCloser(bytes.NewReader(data)))
				// WriteTo
			case 1:
				_, err = io.Copy(struct{ io.Writer }{Writer: enc}, bytes.NewReader(data))
				// ReadFrom
			case 2:
				_, err = enc.ReadFrom(bytes.NewReader(data))
			}
			if err != nil {
				t.Fatalf("encoder %d, write: %d, encode err %v", i, writeMethod, err)
			}
			err = enc.Close()
			if err != nil {
				t.Fatal(err)
			}
			gotUncomp, gotComp := enc.Written()
			if gotUncomp != int64(len(data)) {
				t.Errorf("enc %d, want %d uncomp, got %d", i, len(data), gotUncomp)
			}
			if gotComp != int64(comp.Len()) {
				t.Errorf("enc %d, want %d comp, got %d", i, comp.Len(), gotComp)
			}
			dec.Reset(&comp)
			decomp.Reset()
			readMethod := (i + comp.Len()) % 2
			switch readMethod {
			case 0:
				_, err = io.Copy(&decomp, dec)
			case 1:
				_, err = dec.DecodeConcurrent(&decomp, 2)
			}
			if err != nil {
				t.Errorf("encoder %d, write: %d, read: %d, decode err %v", i, writeMethod, readMethod, err)
				continue
			}
			got := decomp.Bytes()
			if !bytes.Equal(got, data) {
				n := matchLen(got, data)
				t.Errorf("encoder %d, write: %d, read: %d, mismatch at pos %d", i, writeMethod, readMethod, n)
			}
		}
	})
}

func FuzzStreamDecode(f *testing.F) {
	enc := NewWriter(nil, WriterBlockSize(8<<10))
	encu := NewWriter(nil, WriterBlockSize(8<<10), WriterUncompressed(), WriterAddIndex(true))
	addCompressed := func(b []byte) {
		var buf bytes.Buffer
		enc.Reset(&buf)
		enc.EncodeBuffer(b)
		enc.Close()
		f.Add(buf.Bytes())
		buf = bytes.Buffer{}
		encu.Reset(&buf)
		enc.EncodeBuffer(b)
		enc.Close()
		f.Add(buf.Bytes())
	}
	type readerOnly struct {
		io.Reader
	}
	fuzz.ReturnFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, addCompressed)
	dec := NewReader(nil, ReaderIgnoreCRC(), ReaderMaxBlockSize(1<<20))
	f.Fuzz(func(t *testing.T, data []byte) {
		// Using Read
		dec.Reset(bytes.NewReader(data))
		rN, rErr := io.Copy(io.Discard, readerOnly{dec})
		// Using DecodeConcurrent
		dec.Reset(bytes.NewReader(data))
		cN, cErr := dec.DecodeConcurrent(io.Discard, 2)
		if (rErr == nil) != (cErr == nil) {
			t.Error("error mismatch", rErr, cErr)
		}
		if rErr == nil && rN != cN {
			t.Error("length mismatch", rN, cN)
		}

		if testing.Short() {
			return
		}
		dec.Reset(bytes.NewReader(data))
		// Use ByteReader.
		dec.Reset(bytes.NewReader(data))
		var bN int64
		var bErr error
		for {
			_, err := dec.ReadByte()
			if err != nil {
				if err != io.EOF {
					bErr = err
				}
				break
			}
			bN++
		}
		if (rErr == nil) != (bErr == nil) {
			t.Error("error mismatch", rErr, "!=", bErr)
		}
		if rErr == nil && rN != bN {
			t.Error("length mismatch", rN, bN)
		}
	})
}
