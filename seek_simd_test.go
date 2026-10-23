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
	"slices"
	"testing"
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
