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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"slices"
	"testing"

	"github.com/klauspost/compress/s2"
)

func seekTestData(n int, seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, 0, n+32)
	for len(b) < n {
		b = fmt.Appendf(b, "line %d: %x\n", len(b), rng.Intn(1<<12))
	}
	return b[:n]
}

func seekTestStream(t *testing.T, level int, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterLevel(level), WriterBlockSize(64<<10), WriterConcurrency(1))
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestReadSeekerSIMDConcat seeks into concatenated streams with and without the SIMD flag.
// Index jumps skip the identifier of the target stream.
func TestReadSeekerSIMDConcat(t *testing.T) {
	a, b := seekTestData(400<<10, 1), seekTestData(300<<10, 2)
	want := slices.Concat(a, b)
	const n = 1000
	var offs []int
	for off := 0; off < len(want); off += 16 << 10 {
		offs = append(offs, off)
	}
	offs = append(offs, len(a)-n/2)
	slices.Sort(offs)
	for name, lv := range map[string][2]int{
		"minlz+simd": {LevelBalanced, LevelBalancedSIMD},
		"simd+minlz": {LevelBalancedSIMD, LevelBalanced},
	} {
		t.Run(name, func(t *testing.T) {
			stream := slices.Concat(seekTestStream(t, lv[0], a), seekTestStream(t, lv[1], b))
			idx, err := IndexStream(bytes.NewReader(stream))
			if err != nil {
				t.Fatal(err)
			}
			newRS := func() *ReadSeeker {
				rs, err := NewReader(bytes.NewReader(stream)).ReadSeeker(idx)
				if err != nil {
					t.Fatal(err)
				}
				return rs
			}
			got := make([]byte, n)
			// Each direction on a new reader: seeks go to either stream before and after
			// the read across the boundary has read the second identifier.
			rs := newRS()
			for _, off := range offs {
				m := min(n, len(want)-off)
				if _, err := rs.ReadAt(got[:m], int64(off)); err != nil {
					t.Fatalf("ReadAt(%d): %v", off, err)
				}
				if !bytes.Equal(got[:m], want[off:off+m]) {
					t.Fatalf("ReadAt(%d): wrong data", off)
				}
			}
			rs = newRS()
			for _, off := range slices.Backward(offs) {
				m := min(n, len(want)-off)
				if _, err := rs.Seek(int64(off), io.SeekStart); err != nil {
					t.Fatalf("Seek(%d): %v", off, err)
				}
				if _, err := io.ReadFull(rs, got[:m]); err != nil {
					t.Fatalf("Seek(%d): read: %v", off, err)
				}
				if !bytes.Equal(got[:m], want[off:off+m]) {
					t.Fatalf("Seek(%d): wrong data", off)
				}
			}
		})
	}
}

// TestReaderSIMDFlagStrict checks that reads still require the flag of the last identifier they read.
func TestReaderSIMDFlagStrict(t *testing.T) {
	a := seekTestData(200<<10, 1)
	simd := seekTestStream(t, LevelBalancedSIMD, a)
	noFlag := bytes.Clone(simd)
	noFlag[len(magicChunk)] &^= streamFlagSIMD
	if _, err := io.ReadAll(NewReader(bytes.NewReader(noFlag))); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("sequential read: got %v, want %v", err, ErrCorrupt)
	}

	// IndexStream rejects noFlag, so index the stream with the flag; the offsets are the same.
	idx, err := IndexStream(bytes.NewReader(slices.Concat(simd, simd)))
	if err != nil {
		t.Fatal(err)
	}
	rs, err := NewReader(bytes.NewReader(slices.Concat(simd, noFlag))).ReadSeeker(idx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.ReadAt(make([]byte, 1000), int64(len(a)-500)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read on into a stream without the flag: got %v, want %v", err, ErrCorrupt)
	}
}

// TestReadSeekerConcatOffsets checks that offsets count from the first stream, and that an index jump
// into a later stream accepts its larger blocks and checks its EOF chunk.
func TestReadSeekerConcatOffsets(t *testing.T) {
	a, b := seekTestData(300<<10, 1), seekTestData(1000<<10, 2)
	// An uncompressed chunk larger than a Snappy stream allows.
	rand.New(rand.NewSource(3)).Read(b[768<<10:])
	want := slices.Concat(a, b)
	var large, snappy bytes.Buffer
	for w, data := range map[io.WriteCloser][]byte{
		NewWriter(&large, WriterBlockSize(256<<10), WriterConcurrency(1)):       b,
		s2.NewWriter(&snappy, s2.WriterSnappyCompat(), s2.WriterConcurrency(1)): a,
	} {
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for name, tc := range map[string]struct {
		a, b     []byte
		fallback bool
	}{
		"equal":  {seekTestStream(t, LevelBalanced, a), seekTestStream(t, LevelBalancedSIMD, b), false},
		"larger": {seekTestStream(t, LevelBalanced, a), large.Bytes(), false},
		"snappy": {snappy.Bytes(), large.Bytes(), true},
	} {
		t.Run(name, func(t *testing.T) {
			stream := slices.Concat(tc.a, tc.b)
			idx, err := IndexStream(bytes.NewReader(stream))
			if err != nil {
				t.Fatal(err)
			}
			newRS := func() *ReadSeeker {
				rs, err := NewReader(bytes.NewReader(stream), ReaderFallback(tc.fallback)).ReadSeeker(idx)
				if err != nil {
					t.Fatal(err)
				}
				return rs
			}
			rs := newRS()
			got := make([]byte, 1000)
			off := len(a) - len(got)/2
			if _, err := rs.ReadAt(got, int64(off)); err != nil || !bytes.Equal(got, want[off:off+len(got)]) {
				t.Fatalf("ReadAt(%d): %v", off, err)
			}
			if pos, err := rs.Seek(0, io.SeekCurrent); err != nil || pos != int64(off+len(got)) {
				t.Fatalf("position after ReadAt(%d): got %d, %v; want %d", off, pos, err, off+len(got))
			}
			if _, err := rs.ReadAt(got, 10); err != nil || !bytes.Equal(got, want[10:10+len(got)]) {
				t.Fatalf("ReadAt(10) after the second stream: %v", err)
			}
			// Read and WriteTo from an index jump into the second block of the second stream
			// to its EOF chunk, on readers that have only read the first identifier. Then seek to the end.
			off = len(a) + 256<<10 + 1000
			for _, writeTo := range []bool{false, true} {
				rs := newRS()
				if _, err := rs.Seek(int64(off), io.SeekStart); err != nil {
					t.Fatalf("Seek(%d): %v", off, err)
				}
				var src io.Reader = struct{ io.Reader }{rs}
				if writeTo {
					src = rs
				}
				var out bytes.Buffer
				if _, err := io.Copy(&out, src); err != nil || !bytes.Equal(out.Bytes(), want[off:]) {
					t.Fatalf("from %d, WriteTo %v: %v", off, writeTo, err)
				}
				if pos, err := rs.Seek(0, io.SeekEnd); err != nil || pos != int64(len(want)) {
					t.Fatalf("Seek to end: got %d, %v; want %d", pos, err, len(want))
				}
				if n, err := rs.Read(got); n != 0 || err != io.EOF {
					t.Fatalf("Read at end: got %d, %v", n, err)
				}
			}
		})
	}
}

// TestReadSeekerJumpSingleStream checks that a seek into a single stream still detects a missing block
// at its EOF chunk, and doesn't size the WriteTo buffers for larger blocks than the stream has.
func TestReadSeekerJumpSingleStream(t *testing.T) {
	data := seekTestData(1<<20, 1)
	stream := seekTestStream(t, LevelBalanced, data)
	idx, err := IndexStream(bytes.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	const off = 300 << 10
	rs, err := NewReader(bytes.NewReader(stream)).ReadSeeker(idx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Seek(off, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var out bytes.Buffer
	out.Grow(len(data) - off)
	if _, err := io.Copy(&out, rs); err != nil || !bytes.Equal(out.Bytes(), data[off:]) {
		t.Fatalf("WriteTo: %v", err)
	}
	runtime.ReadMemStats(&after)
	// About 2 MiB for 64 KiB blocks; buffers for the largest blocks allowed would take over 100 MiB.
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 16<<20 {
		t.Errorf("WriteTo after Seek allocated %d bytes", alloc)
	}

	// Drop the last data chunk; the index of the result stays valid up to it.
	last, end := 0, 0
	for pos := 0; stream[pos] != chunkTypeEOF; pos = end {
		last, end = pos, pos+chunkHeaderSize+(int(stream[pos+1])|int(stream[pos+2])<<8|int(stream[pos+3])<<16)
	}
	short := slices.Delete(bytes.Clone(stream), last, end)
	rs, err = NewReader(bytes.NewReader(short)).ReadSeeker(idx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Seek(off, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rs); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing block: got %v, want %v", err, ErrCorrupt)
	}
}
