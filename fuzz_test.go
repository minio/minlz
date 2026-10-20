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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/minio/minlz/internal/fuzz"
	"github.com/minio/minlz/internal/guard"
	"github.com/minio/minlz/internal/reference"
	"github.com/zeebo/xxh3"
)

func FuzzEncodingBlocks(f *testing.F) {
	fuzz.AddFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false)
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, testing.Short())
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, testing.Short())
	for delta := 1; delta <= 2; delta++ {
		if b := buildMaxOffsetBoundary(0, delta); b != nil {
			f.Add(b)
		}
	}

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

	srcG, compG, decG := fuzzGuard(f, maxSize), fuzzGuard(f, MaxEncodedLen(maxSize)), fuzzGuard(f, maxSize)
	eg, ea := new(simdEncoder), new(simdEncoder)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxSize {
			return
		}
		// Encoders read ahead and extend matches backwards, so put the input against either guard page.
		src := srcG.Slice(len(data), len(data)&1 == 0)
		copy(src, data)
		if hasAsm && len(data) >= minNonLiteralBlockSize {
			for _, p := range simdAsmParsers {
				if len(data) > p.max {
					continue
				}
				if d := simdParseDiff(simdParseRun(eg, src, p, false), simdParseRun(ea, src, p, true)); d != "" {
					t.Errorf("%s: %s", p.name, d)
				}
			}
		}
		compDst := compG.Slice(MaxEncodedLen(len(data)), true)
		decDst := decG.Slice(len(data), true)
		const levelReference = LevelSmallestSIMD + 1
		for _, l := range append(append([]int{LevelSuperFast, LevelUncompressed, LevelFastest, LevelBalanced, LevelSmallest}, simdLevels...), levelReference) {
			for i := range decDst {
				decDst[i] = 0xfe
			}
			for i := range compDst {
				compDst[i] = 0xff
			}
			var comp []byte
			if l != levelReference {
				comp, _ = Encode(compDst, src, l)
			} else {
				comp, _ = reference.EncodeBlock(src)
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
				os.WriteFile(name+"input.bin", data, 0o644)
				os.WriteFile(name+"encoded.bin", comp, 0o644)
				os.WriteFile(name+"decoded.bin", got, 0o644)
				t.Errorf("roundtrip Go mismatch at byte %d, output written to %q", n, name)
				return
			}
			if _, _, simd, _, _, err := isMinLZ(comp); simd && err == nil {
				if ref, err := reference.DecodeSIMDBlockStrict(comp); err != nil || !bytes.Equal(ref, data) {
					t.Errorf("level %d: reference: %v", l, err)
				}
			}
			if mel := MaxEncodedLen(len(data)); len(comp) > mel {
				t.Error(fmt.Errorf("MaxEncodedLen Exceed: input: %d, mel: %d, got %d", len(data), mel, len(comp)))
				return
			}
		}
	})
}

func FuzzDecodeBlock(f *testing.F) {
	for _, offset := range []int{1, 3, 16, 17, 63, 64, 65, 4096, 65536} {
		for _, length := range []int{127, 128, 4096} {
			encoded, _ := overlapTailBlock(offset, length, 0)
			f.Add(encoded)
		}
	}
	// Streams are fuzzed by FuzzStreamDecode.
	addCompressed := func(b []byte) {
		for _, l := range []int{LevelBalanced, LevelBalancedSIMD, LevelSmallestSIMD} {
			if b2, err := Encode(nil, b, l); err == nil {
				f.Add(b2)
			}
		}
		f.Add(s2.EncodeBetter(nil, b))
	}
	fuzz.ReturnFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false, addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, testing.Short(), addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, testing.Short(), addCompressed)
	fuzz.AddFromZip(f, "testdata/dec-block-regressions.zip", fuzz.TypeRaw, false)
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-dec.zip", fuzz.TypeGoFuzz, false)

	// MinLZ blocks are never larger than their output, which is at most MaxBlockSize.
	maxIn := MaxEncodedLen(MaxBlockSize)
	srcG, dstG := fuzzGuard(f, maxIn), fuzzGuard(f, MaxBlockSize)
	f.Fuzz(func(t *testing.T, data []byte) {
		if t.Failed() {
			return
		}
		if len(data) > maxIn {
			return
		}
		dCopy := append([]byte{}, data...)
		isMz, dlen, err := IsMinLZ(data)
		if !isMz && dlen > MaxBlockSize {
			// Don't do s2/snappy fallback if big.
			return
		}
		base, baseErr := Decode(nil, data)
		if !bytes.Equal(data, dCopy) {
			t.Fatal("data was changed")
		}
		hasErr := baseErr != nil
		// Reads past the input fault, and writes past the output when it is against the guard page
		// after it. Otherwise it is against the page before it, where offsets reaching back too far fault.
		src := srcG.Slice(len(data), true)
		copy(src, data)
		dst := dstG.Slice(dlen, len(data)&1 == 0)
		for i := range dst {
			dst[i] = 0xfe
		}
		got, err := Decode(dst, src)
		if !bytes.Equal(src, dCopy) {
			t.Fatal("data was changed")
		}
		if err != nil && !hasErr {
			t.Fatalf("base err: %v, capped: %v", baseErr, err)
		}
		if _, _, simd, _, _, _ := isMinLZ(data); simd {
			ref, refErr := reference.DecodeSIMDBlock(data)
			if hasErr != (refErr != nil) {
				t.Fatalf("base err: %v, reference: %v", baseErr, refErr)
			}
			if !hasErr && (!bytes.Equal(base, ref) || !bytes.Equal(base, got)) {
				t.Fatal("SIMD block mismatch")
			}
			if n, err := DecodedLen(data); !hasErr && (err != nil || n != len(base)) {
				t.Errorf("DecodedLen: %d, %v, want %d", n, err, len(base))
			}
			return
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
					os.WriteFile(name+"input.bin", data, 0o644)
					os.WriteFile(name+"decoded-asm.bin", base, 0o644)
					os.WriteFile(name+"decoded-go.bin", got, 0o644)
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

// FuzzDecodeSIMD decodes SIMD blocks with a valid header and compares with the reference decoder.
// body is the block from its flags byte. The decoded size is len(body) plus the 64 bytes
// the format requires, plus extra.
func FuzzDecodeSIMD(f *testing.F) {
	add := func(b []byte) {
		for _, l := range []int{LevelSuperFastSIMD, LevelBalancedSIMD, LevelSmallestSIMD, LevelFastestSIMD | LevelSIMDDelta} {
			enc, _ := Encode(nil, b, l)
			if _, _, simd, body, size, err := isMinLZ(enc); simd && err == nil {
				f.Add(uint32(size-len(body)-4-simdMinSaving), body)
			}
		}
	}
	fuzz.ReturnFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false, add)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, testing.Short(), add)
	// Larger blocks only cost memory: every check is per chunk or per literal block.
	const maxN = 1 << 20
	srcG, dstG := fuzzGuard(f, maxN+binary.MaxVarintLen64+1), fuzzGuard(f, maxN)
	f.Fuzz(func(t *testing.T, extra uint32, body []byte) {
		n := len(body) + 4 + simdMinSaving + int(extra)
		if len(body) == 0 || n > maxN {
			return
		}
		hdr := binary.AppendUvarint([]byte{0}, simdBlockType<<24|uint64(n))
		src := srcG.Slice(len(hdr)+len(body), true)
		copy(src[copy(src, hdr):], body)
		want, wantErr := reference.DecodeSIMDBlock(src)
		// Output must not depend on what dst held before. A nonzero fill catches reads of
		// unwritten output unless it equals the wanted byte, so vary it between inputs.
		fill := 0xfe ^ byte(extra)
		// Against the guard page after dst, writes past it fault. Against the one before, offsets reaching back too far fault.
		dst := dstG.Slice(n, len(body)&1 == 0)
		simdTiers(t, func(t *testing.T) {
			for i := range dst {
				dst[i] = fill
			}
			got, err := Decode(dst[:0], src)
			if (err == nil) != (wantErr == nil) {
				t.Fatalf("err: %v, reference: %v", err, wantErr)
			}
			if err == nil && !bytes.Equal(got, want) {
				t.Fatal("reference mismatch")
			}
		})
	})
}

func FuzzStreamEncode(f *testing.F) {
	fuzz.AddFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false)
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, testing.Short())
	fuzz.AddFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, testing.Short())
	for delta := 1; delta <= 2; delta++ {
		if b := buildMaxOffsetBoundary(0, delta); b != nil {
			f.Add(b)
		}
	}

	// FuzzEncodingBlocks covers every level, so vary the stream options instead.
	// The small block sizes give several chunks, including uncompressed ones, for typical fuzz inputs.
	encoders := []*Writer{
		NewWriter(nil, WriterLevel(LevelSuperFast), WriterConcurrency(1), WriterBlockSize(128<<10)),
		NewWriter(nil, WriterLevel(LevelFastestSIMD), WriterConcurrency(1), WriterBlockSize(4<<10)),
		NewWriter(nil, WriterLevel(LevelBalancedSIMD|LevelSIMDDelta), WriterConcurrency(2), WriterBlockSize(16<<10), WriterPadding(1<<10), WriterAddIndex(true)),
		NewWriter(nil, WriterLevel(LevelSuperFastSIMD), WriterConcurrency(2), WriterBlockSize(64<<10), WriterAddIndex(true)),
	}
	dec := NewReader(nil, ReaderFallback(false))
	f.Fuzz(func(t *testing.T, data []byte) {
		var comp bytes.Buffer
		var decomp bytes.Buffer
		for i, enc := range encoders {
			comp.Reset()
			enc.Reset(&comp)
			var err error
			writeMethod := (len(data) + i) % 4
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
				// EncodeBuffer after buffered data, and a Flush: short blocks.
			case 3:
				k := len(data) / 3
				_, err = enc.Write(data[:k])
				if err == nil {
					err = enc.EncodeBuffer(data[k : 2*k])
				}
				if err == nil {
					err = enc.Flush()
				}
				if err == nil {
					_, err = enc.Write(data[2*k:])
				}
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
	encSIMD := NewWriter(nil, WriterBlockSize(8<<10), WriterLevel(LevelBalancedSIMD), WriterAddIndex(true))
	addCompressed := func(b []byte) {
		var buf bytes.Buffer
		enc.Reset(&buf)
		enc.EncodeBuffer(b)
		enc.Close()
		f.Add(buf.Bytes())
		buf = bytes.Buffer{}
		encSIMD.Reset(&buf)
		encSIMD.EncodeBuffer(b)
		encSIMD.Close()
		f.Add(buf.Bytes())
		buf = bytes.Buffer{}
		encu.Reset(&buf)
		encu.EncodeBuffer(b)
		encu.Close()
		f.Add(buf.Bytes())
	}
	type readerOnly struct {
		io.Reader
	}
	fuzz.ReturnFromZip(f, "testdata/enc_regressions.zip", fuzz.TypeRaw, false, addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-raw.zip", fuzz.TypeRaw, testing.Short(), addCompressed)
	fuzz.ReturnFromZip(f, "testdata/fuzz/block-corpus-enc.zip", fuzz.TypeGoFuzz, testing.Short(), addCompressed)
	dec := NewReader(nil, ReaderIgnoreCRC(), ReaderMaxBlockSize(1<<20))
	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip offset, sometimes past the end.
		skip := int64(len(data) / 2)
		// Using Read
		read := splitHash{at: skip, all: xxh3.New(), tail: xxh3.New()}
		dec.Reset(bytes.NewReader(data))
		rN, rErr := io.Copy(&read, readerOnly{dec})
		// Using DecodeConcurrent
		conc := xxh3.New()
		dec.Reset(bytes.NewReader(data))
		cN, cErr := dec.DecodeConcurrent(conc, 2)
		if (rErr == nil) != (cErr == nil) {
			t.Error("error mismatch", rErr, cErr)
		}
		if rErr == nil && (rN != cN || read.all.Sum64() != conc.Sum64()) {
			t.Error("output mismatch", rN, cN)
		}

		// Skip doesn't decode skipped blocks, so it only must match Read on valid streams.
		if testing.Short() || rErr != nil {
			return
		}
		dec.Reset(bytes.NewReader(data))
		err := dec.Skip(skip)
		if skip > rN {
			if err == nil {
				t.Error("skip past end", skip, rN)
			}
			return
		}
		if err != nil {
			t.Fatal("skip", skip, rN, err)
		}
		b, err := dec.ReadByte()
		if skip == rN {
			if err != io.EOF {
				t.Error("ReadByte at end", err)
			}
			return
		}
		if err != nil {
			t.Fatal("ReadByte", err)
		}
		tail := xxh3.New()
		tail.Write([]byte{b})
		n, err := io.Copy(tail, readerOnly{dec})
		if err != nil || skip+1+n != rN || tail.Sum64() != read.tail.Sum64() {
			t.Error("skip mismatch", skip, n, rN, err)
		}
	})
}

// splitHash hashes everything written to all and everything after the first at bytes to tail.
type splitHash struct {
	at        int64
	all, tail *xxh3.Hasher
}

func (s *splitHash) Write(p []byte) (int, error) {
	s.all.Write(p)
	if s.at < int64(len(p)) {
		s.tail.Write(p[max(s.at, 0):])
	}
	s.at -= int64(len(p))
	return len(p), nil
}

// fuzzGuard returns a guard.Buffer with room for size bytes, mapped once for all inputs of f.
func fuzzGuard(f *testing.F, size int) *guard.Buffer {
	g, err := guard.New(size)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(g.Free)
	return g
}
