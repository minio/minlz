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
	"io"
	"maps"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/klauspost/compress/zip"
	"github.com/zeebo/xxh3"
)

func testOptions(_ testing.TB) map[string][]WriterOption {
	testOptions := map[string][]WriterOption{
		"default": {WriterAddIndex(true)},
		"fast":    {WriterLevel(LevelFastest)},
		"best":    {WriterLevel(LevelSmallest)},
		"none":    {WriterUncompressed()},
		"simd":    {WriterLevel(LevelBalancedSIMD)},
		"simd-d":  {WriterLevel(LevelFastestSIMD | LevelSIMDDelta)},
	}

	x := make(map[string][]WriterOption)
	cloneAdd := func(org []WriterOption, add ...WriterOption) []WriterOption {
		y := make([]WriterOption, len(org)+len(add))
		copy(y, org)
		copy(y[len(org):], add)
		return y
	}
	for name, opt := range testOptions {
		x[name] = opt
		x[name+"-c1"] = cloneAdd(opt, WriterConcurrency(1))
	}
	testOptions = x
	x = make(map[string][]WriterOption)
	for name, opt := range testOptions {
		x[name] = opt
		if !testing.Short() {
			x[name+"-4k-win"] = cloneAdd(opt, WriterBlockSize(4<<10))
			x[name+"-4M-win"] = cloneAdd(opt, WriterBlockSize(4<<20))
		}
	}
	testOptions = x
	x = make(map[string][]WriterOption)
	for name, opt := range testOptions {
		x[name] = opt
		x[name+"-pad-min"] = cloneAdd(opt, WriterPadding(2), WriterPaddingSrc(zeroReader{}))
		if !testing.Short() {
			x[name+"-pad-8000"] = cloneAdd(opt, WriterPadding(8000), WriterPaddingSrc(zeroReader{}))
			x[name+"-pad-max"] = cloneAdd(opt, WriterPadding(4<<20), WriterPaddingSrc(zeroReader{}))
		}
	}
	maps.Copy(x, testOptions)
	testOptions = x
	return testOptions
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestEncoderRegression(t *testing.T) {
	data, err := os.ReadFile("testdata/enc_regressions.zip")
	if err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	// Same as fuzz test...
	test := func(t *testing.T, data []byte) {
		if testing.Short() && len(data) > 10000 {
			t.SkipNow()
		}
		var blocksTested bool
		for name, opts := range testOptions(t) {
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				dec := NewReader(nil)
				enc := NewWriter(&buf, opts...)

				if !blocksTested {
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
					comp, err = Encode(make([]byte, MaxEncodedLen(len(data))), data, LevelBalanced)
					if err != nil {
						t.Error(err)
						return
					}

					decoded, err = Decode(nil, comp)
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

					comp, err = Encode(make([]byte, MaxEncodedLen(len(data))), data, LevelSmallest)
					if err != nil {
						t.Error(err)
						return
					}
					decoded, err = Decode(nil, comp)
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
					blocksTested = true
				}

				// Test writer.
				n, err := enc.Write(data)
				if err != nil {
					t.Error(err)
					return
				}
				if n != len(data) {
					t.Error(fmt.Errorf("Write: Short write, want %d, got %d", len(data), n))
					return
				}
				err = enc.Close()
				if err != nil {
					t.Error(err)
					return
				}
				// Calling close twice should not affect anything.
				err = enc.Close()
				if err != nil {
					t.Error(err)
					return
				}
				comp := buf.Bytes()
				if enc.pad > 0 && len(comp)%enc.pad != 0 {
					t.Error(fmt.Errorf("wanted size to be mutiple of %d, got size %d with remainder %d", enc.pad, len(comp), len(comp)%enc.pad))
					return
				}
				var got []byte
				dec.Reset(&buf)
				got, err = io.ReadAll(dec)
				if err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(data, got) {
					t.Error("block (reset) decoder mismatch")
					return
				}

				// Test Reset on both and use ReadFrom instead.
				buf.Reset()
				enc.Reset(&buf)
				n2, err := enc.ReadFrom(bytes.NewBuffer(data))
				if err != nil {
					t.Error(err)
					return
				}
				if n2 != int64(len(data)) {
					t.Error(fmt.Errorf("ReadFrom: Short read, want %d, got %d", len(data), n2))
					return
				}
				err = enc.Close()
				if err != nil {
					t.Error(err)
					return
				}
				if enc.pad > 0 && buf.Len()%enc.pad != 0 {
					t.Error(fmt.Errorf("wanted size to be mutiple of %d, got size %d with remainder %d", enc.pad, buf.Len(), buf.Len()%enc.pad))
					return
				}
				dec.Reset(&buf)
				got, err = io.ReadAll(dec)
				if err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(data, got) {
					t.Error("frame (reset) decoder mismatch")
					return
				}
			})
		}
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

func TestIndex(t *testing.T) {
	fatalErr := func(t testing.TB, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create a test corpus
	var input []byte
	if !testing.Short() {
		input = make([]byte, 10<<20)
	} else {
		input = make([]byte, 500<<10)
	}
	rng := rand.New(rand.NewSource(0xabeefcafe))
	rng.Read(input)
	// Make it compressible...
	for i, v := range input {
		input[i] = '0' + v&3
	}
	// Compress it...
	var buf bytes.Buffer
	// We use smaller blocks just for the example...
	enc := NewWriter(&buf, WriterBlockSize(100<<10), WriterAddIndex(true), WriterLevel(LevelBalanced), WriterConcurrency(runtime.GOMAXPROCS(0)))
	todo := input
	for len(todo) > 0 {
		// Write random sized inputs..
		x := todo[:rng.Intn(1+len(todo)&65535)]
		if len(x) == 0 {
			x = todo[:1]
		}
		_, err := enc.Write(x)
		fatalErr(t, err)
		// Flush once in a while
		if rng.Intn(8) == 0 {
			err = enc.Flush()
			fatalErr(t, err)
		}
		todo = todo[len(x):]
	}

	// Close and also get index...
	idxBytes, err := enc.CloseIndex()
	fatalErr(t, err)
	if false {
		// Load the index.
		var index Index
		_, err = index.Load(idxBytes)
		fatalErr(t, err)
		t.Log(string(index.JSON()))
	}
	// This is our compressed stream...
	compressed := buf.Bytes()
	for wantOffset := int64(0); wantOffset < int64(len(input)); wantOffset += 65531 {
		t.Run(fmt.Sprintf("offset-%d", wantOffset), func(t *testing.T) {
			// Let's assume we want to read from uncompressed offset 'i'
			// and we cannot seek in input, but we have the index.
			want := input[wantOffset:]

			// Load the index.
			var index Index
			_, err = index.Load(idxBytes)
			fatalErr(t, err)

			// Find offset in file:
			compressedOffset, uncompressedOffset, err := index.Find(wantOffset)
			fatalErr(t, err)

			// Offset the input to the compressed offset.
			// Notice how we do not provide any bytes before the offset.
			in := io.Reader(bytes.NewBuffer(compressed[compressedOffset:]))

			// When creating the decoder we must specify that it should not
			// expect a stream identifier at the beginning og the frame.
			dec := NewReader(in, ReaderIgnoreStreamIdentifier())

			// We now have a reader, but it will start outputting at uncompressedOffset,
			// and not the actual offset we want, so skip forward to that.
			toSkip := wantOffset - uncompressedOffset
			err = dec.Skip(toSkip)
			fatalErr(t, err)

			// Read the rest of the stream...
			got, err := io.ReadAll(dec)
			fatalErr(t, err)
			if !bytes.Equal(got, want) {
				t.Error("Result mismatch", wantOffset)
			}

			// Test with stream index...
			for i := io.SeekStart; i <= io.SeekEnd; i++ {
				t.Run(fmt.Sprintf("seek-%d", i), func(t *testing.T) {
					// Read it from a seekable stream
					dec = NewReader(bytes.NewReader(compressed))

					rs, err := dec.ReadSeeker(nil)
					fatalErr(t, err)

					// Read a little...
					tmp := make([]byte, len(input)/2)
					_, err = io.ReadFull(rs, tmp[:])
					fatalErr(t, err)

					toSkip := wantOffset
					switch i {
					case io.SeekStart:
					case io.SeekCurrent:
						toSkip = wantOffset - int64(len(input)/2)
					case io.SeekEnd:
						toSkip = -(int64(len(input)) - wantOffset)
					}
					gotOffset, err := rs.Seek(toSkip, i)
					if gotOffset != wantOffset {
						t.Errorf("got offset %d, want %d", gotOffset, wantOffset)
					}
					// Read the rest of the stream...
					got, err := io.ReadAll(dec)
					fatalErr(t, err)
					if !bytes.Equal(got, want) {
						t.Error("Result mismatch", wantOffset)
					}
				})
			}
			t.Run("ReadAt", func(t *testing.T) {
				// Read it from a seekable stream
				dec = NewReader(bytes.NewReader(compressed))

				rs, err := dec.ReadSeeker(nil)
				fatalErr(t, err)

				// Read a little...
				tmp := make([]byte, len(input)/2)
				_, err = io.ReadFull(rs, tmp[:])
				fatalErr(t, err)
				wantLen := len(tmp)
				if wantLen+int(wantOffset) > len(input) {
					wantLen = len(input) - int(wantOffset)
				}
				// Read from wantOffset
				n, err := rs.ReadAt(tmp, wantOffset)
				if n != wantLen {
					t.Errorf("got length %d, want %d", n, wantLen)
				}
				if err != io.EOF {
					fatalErr(t, err)
				}
				want := want[:n]
				got := tmp[:n]

				// Read the rest of the stream...
				if !bytes.Equal(got, want) {
					t.Error("Result mismatch", wantOffset)
				}
			})
		})
	}
}

func TestWriterPadding(t *testing.T) {
	n := 100
	if testing.Short() {
		n = 5
	}
	rng := rand.New(rand.NewSource(0x1337))
	d := NewReader(nil)

	for i := 0; i < n; i++ {
		padding := (rng.Int() & 0xffff) + 1
		src := make([]byte, (rng.Int()&0xfffff)+1)
		for i := range src {
			src[i] = uint8(rng.Uint32()) & 3
		}
		var dst bytes.Buffer
		e := NewWriter(&dst, WriterPadding(padding))
		// Test the added padding is invisible.
		_, err := io.Copy(e, bytes.NewBuffer(src))
		if err != nil {
			t.Fatal(err)
		}
		err = e.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = e.Close()
		if err != nil {
			t.Fatal(err)
		}

		if dst.Len()%padding != 0 {
			t.Fatalf("wanted size to be mutiple of %d, got size %d with remainder %d", padding, dst.Len(), dst.Len()%padding)
		}
		var got bytes.Buffer
		d.Reset(&dst)
		_, err = io.Copy(&got, d)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(src, got.Bytes()) {
			t.Fatal("output mismatch")
		}

		// Try after reset
		dst.Reset()
		e.Reset(&dst)
		_, err = io.Copy(e, bytes.NewBuffer(src))
		if err != nil {
			t.Fatal(err)
		}
		err = e.Close()
		if err != nil {
			t.Fatal(err)
		}
		if dst.Len()%padding != 0 {
			t.Fatalf("wanted size to be mutiple of %d, got size %d with remainder %d", padding, dst.Len(), dst.Len()%padding)
		}

		got.Reset()
		d.Reset(&dst)
		_, err = io.Copy(&got, d)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(src, got.Bytes()) {
			t.Fatal("output mismatch after reset")
		}
	}
}

func TestBigRegularWrites(t *testing.T) {
	var buf [maxBlockSize * 2]byte
	dst := bytes.NewBuffer(nil)
	enc := NewWriter(dst, WriterLevel(LevelSmallest))
	max := uint8(10)
	if testing.Short() {
		max = 4
	}
	for n := uint8(0); n < max; n++ {
		for i := range buf[:] {
			buf[i] = n
		}
		// Writes may not keep a reference to the data beyond the Write call.
		_, err := enc.Write(buf[:])
		if err != nil {
			t.Fatal(err)
		}
	}
	err := enc.Close()
	if err != nil {
		t.Fatal(err)
	}

	dec := NewReader(dst)
	_, err = io.Copy(io.Discard, dec)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBigEncodeBuffer(t *testing.T) {
	const blockSize = 1 << 20
	var buf [blockSize * 2]byte
	dst := bytes.NewBuffer(nil)
	enc := NewWriter(dst, WriterBlockSize(blockSize), WriterLevel(LevelSmallest))
	max := uint8(10)
	if testing.Short() {
		max = 4
	}
	for n := uint8(0); n < max; n++ {
		// Change the buffer to a new value.
		for i := range buf[:] {
			buf[i] = n
		}
		err := enc.EncodeBuffer(buf[:])
		if err != nil {
			t.Fatal(err)
		}
		// We can write it again since we aren't changing it.
		err = enc.EncodeBuffer(buf[:])
		if err != nil {
			t.Fatal(err)
		}
		err = enc.Flush()
		if err != nil {
			t.Fatal(err)
		}
	}
	err := enc.Close()
	if err != nil {
		t.Fatal(err)
	}

	dec := NewReader(dst)
	n, err := io.Copy(io.Discard, dec)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(n)
}

func TestBigEncodeBufferSync(t *testing.T) {
	const blockSize = 1 << 20
	var buf [blockSize * 2]byte
	dst := bytes.NewBuffer(nil)
	enc := NewWriter(dst, WriterBlockSize(blockSize), WriterConcurrency(1), WriterLevel(LevelSmallest))
	max := uint8(10)
	if testing.Short() {
		max = 2
	}
	for n := uint8(0); n < max; n++ {
		// Change the buffer to a new value.
		for i := range buf[:] {
			buf[i] = n
		}
		// When WriterConcurrency == 1 we can encode and reuse the buffer.
		err := enc.EncodeBuffer(buf[:])
		if err != nil {
			t.Fatal(err)
		}
	}
	err := enc.Close()
	if err != nil {
		t.Fatal(err)
	}

	dec := NewReader(dst)
	n, err := io.Copy(io.Discard, dec)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(n)
}

func TestIndexStreamSmall(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Write(bytes.Repeat([]byte("abcdefgh"), 100))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := IndexStream(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
}

// simdTestData returns n bytes of text, with a random part that doesn't compress
// and a table of numbers for delta literals.
func simdTestData(n int) []byte {
	b := makeTestData(n)
	rng := rand.New(rand.NewSource(0x51))
	rng.Read(b[n/3 : n/3+min(n/8, 48<<10)])
	for i := 2 * n / 3; i+4 <= min(n, 2*n/3+64<<10); i += 4 {
		binary.LittleEndian.PutUint32(b[i:], uint32(i*i>>10))
	}
	return b
}

// streamChunks returns the number of chunks of each type in the MinLZ stream b.
func streamChunks(t testing.TB, b []byte) (n [256]int) {
	t.Helper()
	for len(b) > 0 {
		if len(b) < 4 {
			t.Fatal("truncated chunk header")
		}
		l := 4 + (int(b[1]) | int(b[2])<<8 | int(b[3])<<16)
		if l > len(b) {
			t.Fatal("truncated chunk")
		}
		n[b[0]]++
		b = b[l:]
	}
	return n
}

// testSIMDStreamRead checks that comp decodes to want with every reader API.
// comp must have an index.
func testSIMDStreamRead(t *testing.T, comp, want []byte) {
	t.Helper()
	type readerOnly struct{ io.Reader }
	got, err := io.ReadAll(readerOnly{NewReader(bytes.NewReader(comp))})
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Read: err %v, equal %v", err, bytes.Equal(got, want))
	}
	var buf bytes.Buffer
	if _, err := NewReader(bytes.NewReader(comp)).DecodeConcurrent(&buf, 4); err != nil || !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("DecodeConcurrent: err %v, equal %v", err, bytes.Equal(buf.Bytes(), want))
	}
	idx, err := IndexStream(bytes.NewReader(comp))
	if err != nil {
		t.Fatal("IndexStream:", err)
	}
	var index Index
	if _, err := index.Load(idx); err != nil {
		t.Fatal(err)
	}
	rs, err := NewReader(bytes.NewReader(comp)).ReadSeeker(nil)
	if err != nil {
		t.Fatal("ReadSeeker:", err)
	}
	tmp := make([]byte, 1000)
	for _, off := range []int64{0, int64(len(want)/3 + 7), int64(len(want) / 2), int64(len(want) - 100)} {
		wantAt := want[off:min(int(off)+len(tmp), len(want))]
		if _, err := rs.Seek(off, io.SeekStart); err != nil {
			t.Fatal("Seek:", err)
		}
		if n, err := io.ReadFull(rs, tmp[:len(wantAt)]); err != nil || !bytes.Equal(tmp[:n], wantAt) {
			t.Fatalf("Seek %d: err %v", off, err)
		}
		if n, err := rs.ReadAt(tmp[:len(wantAt)], off); (err != nil && err != io.EOF) || !bytes.Equal(tmp[:n], wantAt) {
			t.Fatalf("ReadAt %d: err %v", off, err)
		}
		dec := NewReader(bytes.NewReader(comp))
		if err := dec.Skip(off); err != nil {
			t.Fatal("Skip:", err)
		}
		if got, err := io.ReadAll(dec); err != nil || !bytes.Equal(got, want[off:]) {
			t.Fatalf("Skip %d: err %v", off, err)
		}
		c, u, err := index.Find(off)
		if err != nil {
			t.Fatal(err)
		}
		dec = NewReader(bytes.NewReader(comp[c:]), ReaderIgnoreStreamIdentifier())
		if err := dec.Skip(off - u); err != nil {
			t.Fatal("Skip from index:", err)
		}
		if n, err := io.ReadFull(dec, tmp[:len(wantAt)]); err != nil || !bytes.Equal(tmp[:n], wantAt) {
			t.Fatalf("read from index %d: err %v", off, err)
		}
	}
}

func TestWriterSIMD(t *testing.T) {
	n := 3 << 20
	if testing.Short() {
		n = 600 << 10
	}
	data := simdTestData(n)
	type readerOnly struct{ io.Reader }
	writes := []struct {
		name string
		opts []WriterOption
		fn   func(w *Writer) error
	}{
		{"write", []WriterOption{WriterPadding(1000)}, func(w *Writer) error {
			rng := rand.New(rand.NewSource(1))
			for p := data; len(p) > 0; {
				k := min(len(p), rng.Intn(200<<10)+1)
				if _, err := w.Write(p[:k]); err != nil {
					return err
				}
				p = p[k:]
			}
			return nil
		}},
		{"readfrom-buffer", nil, func(w *Writer) error {
			_, err := w.ReadFrom(bytes.NewBuffer(data))
			return err
		}},
		{"readfrom-reader", nil, func(w *Writer) error {
			_, err := w.ReadFrom(readerOnly{bytes.NewReader(data)})
			return err
		}},
		{"encodebuffer-flush", nil, func(w *Writer) error {
			if err := w.EncodeBuffer(data[:n/2]); err != nil {
				return err
			}
			if err := w.Flush(); err != nil {
				return err
			}
			return w.EncodeBuffer(data[n/2:])
		}},
	}
	for _, level := range simdLevels {
		for _, bs := range []int{4 << 10, 64 << 10, 1 << 20, 8 << 20} {
			for _, conc := range []int{1, 4} {
				for _, wr := range writes {
					t.Run(fmt.Sprintf("level-%d/bs-%d/c-%d/%s", level, bs, conc, wr.name), func(t *testing.T) {
						var buf bytes.Buffer
						w := NewWriter(&buf, append(wr.opts, WriterLevel(level), WriterBlockSize(bs), WriterConcurrency(conc), WriterAddIndex(true))...)
						if err := wr.fn(w); err != nil {
							t.Fatal(err)
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						comp := buf.Bytes()
						if in, out := w.Written(); in != int64(n) || out != int64(len(comp)) {
							t.Fatalf("Written: %d, %d", in, out)
						}
						if comp[len(magicChunk)]&streamFlagSIMD == 0 {
							t.Fatal("stream identifier has no SIMD flag")
						}
						if c := streamChunks(t, comp); c[chunkTypeMinLZSIMD] == 0 || c[chunkTypeMinLZCompressedData] != 0 {
							t.Fatalf("got %d SIMD and %d MinLZ chunks", c[chunkTypeMinLZSIMD], c[chunkTypeMinLZCompressedData])
						}
						testSIMDStreamRead(t, comp, data)
					})
				}
			}
		}
	}
}

// TestWriterSIMDChunks checks the layout of SIMD chunks against the block encoder.
func TestWriterSIMDChunks(t *testing.T) {
	data := simdTestData(300 << 10)
	for _, level := range simdLevels {
		// Concurrent EncodeBuffer gives the encoder the smallest output buffer.
		for _, conc := range []int{1, 2} {
			var buf bytes.Buffer
			w := NewWriter(&buf, WriterLevel(level), WriterBlockSize(16<<10), WriterConcurrency(conc))
			if err := w.EncodeBuffer(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			comp := buf.Bytes()[len(magicChunk)+1:]
			var types [256]int
			for off := 0; len(comp) > 0; {
				l := int(comp[1]) | int(comp[2])<<8 | int(comp[3])<<16
				payload := comp[4 : 4+l]
				types[comp[0]]++
				src := data[off:min(off+16<<10, len(data))]
				switch comp[0] {
				case chunkTypeMinLZSIMD:
					block := TryEncode(nil, src, level)
					if binary.LittleEndian.Uint64(payload) != xxh3.Hash(src) || !bytes.Equal(payload[simdHashSize:], block[1:]) {
						t.Fatalf("level %d: SIMD chunk at %d differs from TryEncode", level, off)
					}
					off += len(src)
				case chunkTypeUncompressedData:
					if TryEncode(nil, src, level) != nil {
						t.Fatalf("level %d: uncompressed chunk at %d compresses with TryEncode", level, off)
					}
					off += l - checksumSize
				}
				comp = comp[4+l:]
			}
			if types[chunkTypeMinLZSIMD] == 0 || types[chunkTypeUncompressedData] == 0 {
				t.Fatalf("level %d: got %d SIMD and %d uncompressed chunks", level, types[chunkTypeMinLZSIMD], types[chunkTypeUncompressedData])
			}
		}
	}
}

// TestWriterSIMDIdentifier checks that the stream identifier and the sidecar identifier
// have the SIMD flag exactly for SIMD levels, and that WriterLevel takes the levels Encode takes.
func TestWriterSIMDIdentifier(t *testing.T) {
	data := makeTestData(10 << 10)
	for level := -2; level <= LevelSIMDDelta|LevelSmallestSIMD+1; level++ {
		_, encErr := Encode(nil, data, level)
		// A user chunk first makes AddUserChunk write the identifier.
		for i := range 4 {
			userFirst, conc := i&1 != 0, 1+i>>1
			var main, side bytes.Buffer
			w := NewWriter(&main, WriterLevel(level), WriterConcurrency(conc), WriterSearchTable(NewSearchTableConfig()), WriterSidecar(&side))
			var err error
			if userFirst {
				err = w.AddUserChunk(MinUserSkippableChunk, []byte("user"))
			}
			if err == nil {
				_, err = w.Write(data)
			}
			if err == nil {
				err = w.Close()
			}
			if (err == nil) != (encErr == nil) {
				t.Fatalf("level %d: writer error %v, Encode error %v", level, err, encErr)
			}
			if err != nil {
				if err != ErrInvalidLevel {
					t.Fatalf("level %d: %v", level, err)
				}
				continue
			}
			simd := level >= LevelSuperFastSIMD
			for _, b := range [][]byte{main.Bytes(), side.Bytes()} {
				if got := b[len(magicChunk)]&streamFlagSIMD != 0; got != simd {
					t.Fatalf("level %d, user chunk %v, concurrency %d: SIMD flag %v", level, userFirst, conc, got)
				}
			}
			if got, err := io.ReadAll(NewReader(&main)); err != nil || !bytes.Equal(got, data) {
				t.Fatalf("level %d: %v", level, err)
			}
		}
	}
}

// TestWriterSIMDHash checks the hash of SIMD chunks against XXH3 64 bit, seed 0, values of the C implementation.
func TestWriterSIMDHash(t *testing.T) {
	for n, want := range map[int]uint64{1000: 0x30e4c5246f300d11, 4095: 0x268198759d7bdf74} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte((i + 1) % 251)
		}
		var buf bytes.Buffer
		w := NewWriter(&buf, WriterLevel(LevelFastestSIMD))
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		chunk := buf.Bytes()[len(magicChunk)+1:]
		if chunk[0] != chunkTypeMinLZSIMD || binary.LittleEndian.Uint64(chunk[4:]) != want {
			t.Fatalf("%d bytes: chunk type %#x, hash %#x, want %#x", n, chunk[0], binary.LittleEndian.Uint64(chunk[4:]), want)
		}
	}
}
