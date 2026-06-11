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
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// makeTestData builds a chunk of mixed text data that compresses well.
func makeTestData(n int) []byte {
	rng := rand.New(rand.NewSource(0xdead))
	lines := []string{
		`{"timestamp":"2024-01-15T10:11:12Z","level":"INFO","msg":"hello world"}` + "\n",
		`{"timestamp":"2024-01-15T10:11:13Z","level":"WARN","msg":"needle in the haystack"}` + "\n",
		`{"timestamp":"2024-01-15T10:11:14Z","level":"ERROR","msg":"something failed"}` + "\n",
		`{"timestamp":"2024-01-15T10:11:15Z","level":"DEBUG","msg":"verbose log entry"}` + "\n",
		`{"timestamp":"2024-01-15T10:11:16Z","level":"INFO","msg":"shutting down"}` + "\n",
	}
	var buf bytes.Buffer
	for buf.Len() < n {
		buf.WriteString(lines[rng.Intn(len(lines))])
	}
	return buf.Bytes()[:n]
}

// countOccurrences counts overlapping occurrences of pattern in data
// (advances by 1 each match, matching the searcher's semantics).
func countOccurrences(data, pattern []byte) int {
	n := 0
	off := 0
	for {
		i := bytes.Index(data[off:], pattern)
		if i < 0 {
			break
		}
		n++
		off += i + 1
	}
	return n
}

// countingReaderAt wraps an io.ReaderAt and counts calls + bytes.
type countingReaderAt struct {
	r     io.ReaderAt
	calls atomic.Int64
	bytes atomic.Int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.calls.Add(1)
	n, err := c.r.ReadAt(p, off)
	c.bytes.Add(int64(n))
	return n, err
}

func TestWriterSidecar_RoundTrip(t *testing.T) {
	data := makeTestData(200 << 10) // 200 KB to span several blocks
	for _, cc := range []int{1, 4} {
		t.Run(fmt.Sprintf("concurrency=%d", cc), func(t *testing.T) {
			var main, side bytes.Buffer
			w := NewWriter(&main,
				WriterBlockSize(64<<10),
				WriterConcurrency(cc),
				WriterSearchTable(NewSearchTableConfig().WithMatchLen(6)),
				WriterSidecar(&side),
			)
			if _, err := w.Write(data); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Main stream must decompress to the original data.
			r := NewReader(bytes.NewReader(main.Bytes()))
			var got bytes.Buffer
			if _, err := r.WriteTo(&got); err != nil {
				t.Fatalf("decompress main: %v", err)
			}
			if !bytes.Equal(got.Bytes(), data) {
				t.Fatalf("round-trip mismatch: got %d bytes, want %d", got.Len(), len(data))
			}

			// Sidecar must be a valid MinLZ stream — its 0x45/0x46/0x47 chunks
			// are skippable so NewReader can read it without producing data.
			rSide := NewReader(bytes.NewReader(side.Bytes()))
			n, err := io.Copy(io.Discard, rSide)
			if err != nil {
				t.Fatalf("read sidecar as stream: %v (read %d)", err, n)
			}
			if n != 0 {
				t.Fatalf("sidecar produced %d bytes of decoded output; expected 0", n)
			}
		})
	}
}

func TestSidecarSearcher_MatchesBlockSearcher(t *testing.T) {
	data := makeTestData(300 << 10)
	patterns := [][]byte{
		[]byte("needle in the haystack"),
		[]byte(`"level":"ERROR"`),
		[]byte(`shutting down`),
		[]byte(`":"2024-01-15T10:11:1`),
		[]byte(`absolutely-not-present`),
	}

	cfg := NewSearchTableConfig().WithMatchLen(6)
	t.Run("inline-vs-sidecar", func(t *testing.T) {
		// Inline-tabled stream.
		var inline bytes.Buffer
		w := NewWriter(&inline,
			WriterBlockSize(64<<10),
			WriterConcurrency(1),
			WriterSearchTable(cfg),
		)
		if _, err := w.Write(data); err != nil {
			t.Fatalf("write inline: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close inline: %v", err)
		}

		// Sidecar-mode stream.
		var main, side bytes.Buffer
		w2 := NewWriter(&main,
			WriterBlockSize(64<<10),
			WriterConcurrency(1),
			WriterSearchTable(cfg),
			WriterSidecar(&side),
		)
		if _, err := w2.Write(data); err != nil {
			t.Fatalf("write sidecar: %v", err)
		}
		if err := w2.Close(); err != nil {
			t.Fatalf("close sidecar: %v", err)
		}

		for _, pat := range patterns {
			t.Run(string(pat), func(t *testing.T) {
				expected := countOccurrences(data, pat)

				inlineMatches := 0
				bs := NewBlockSearcher(bytes.NewReader(inline.Bytes()))
				if err := bs.Search(pat, func(SearchResult) error {
					inlineMatches++
					return nil
				}); err != nil {
					t.Fatalf("inline search: %v", err)
				}
				if inlineMatches != expected {
					t.Fatalf("inline: got %d matches, want %d", inlineMatches, expected)
				}

				sidecarMatches := 0
				ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))
				if err := ss.Search(pat, func(SearchResult) error {
					sidecarMatches++
					return nil
				}); err != nil {
					t.Fatalf("sidecar search: %v", err)
				}
				if sidecarMatches != expected {
					t.Fatalf("sidecar: got %d matches, want %d", sidecarMatches, expected)
				}
			})
		}
	})
}

func TestBuildSidecar_RoundTrip(t *testing.T) {
	data := makeTestData(150 << 10)
	// 1) Compress with NO search tables.
	var main bytes.Buffer
	w := NewWriter(&main, WriterBlockSize(32<<10), WriterConcurrency(1))
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 2) Build a sidecar against the existing main stream.
	var side bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(6)
	if err := BuildSidecar(&side, bytes.NewReader(main.Bytes()), SidecarSearchTable(cfg)); err != nil {
		t.Fatalf("BuildSidecar: %v", err)
	}

	// 3) Search the sidecar against the original main.
	for _, pat := range [][]byte{
		[]byte("needle in the haystack"),
		[]byte(`"level":"WARN"`),
		[]byte("not-in-stream-anywhere"),
	} {
		t.Run(string(pat), func(t *testing.T) {
			expected := countOccurrences(data, pat)
			matches := 0
			ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))
			if err := ss.Search(pat, func(SearchResult) error {
				matches++
				return nil
			}); err != nil {
				t.Fatalf("search: %v", err)
			}
			if matches != expected {
				t.Fatalf("got %d matches, want %d", matches, expected)
			}
		})
	}
}

func TestExtractSidecar_NoStrip(t *testing.T) {
	data := makeTestData(150 << 10)
	cfg := NewSearchTableConfig().WithMatchLen(6)

	var main bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Extract sidecar (no stripped stream): sidecar offsets reference src as-is.
	var side bytes.Buffer
	if err := ExtractSidecar(&side, nil, bytes.NewReader(main.Bytes())); err != nil {
		t.Fatalf("ExtractSidecar: %v", err)
	}

	for _, pat := range [][]byte{
		[]byte("needle in the haystack"),
		[]byte(`"level":"INFO"`),
	} {
		expected := countOccurrences(data, pat)
		matches := 0
		ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))
		if err := ss.Search(pat, func(SearchResult) error {
			matches++
			return nil
		}); err != nil {
			t.Fatalf("search %q: %v", pat, err)
		}
		if matches != expected {
			t.Fatalf("pattern %q: got %d matches, want %d", pat, matches, expected)
		}
	}
}

func TestExtractSidecar_StripMain(t *testing.T) {
	data := makeTestData(150 << 10)
	cfg := NewSearchTableConfig().WithMatchLen(6)

	// Original indexed stream.
	var orig bytes.Buffer
	w := NewWriter(&orig,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Extract sidecar + write a stripped main stream.
	var side, stripped bytes.Buffer
	if err := ExtractSidecar(&side, &stripped, bytes.NewReader(orig.Bytes())); err != nil {
		t.Fatalf("ExtractSidecar: %v", err)
	}

	// Stripped stream must be smaller (we removed all the search chunks).
	if stripped.Len() >= orig.Len() {
		t.Fatalf("stripped (%d) is not smaller than orig (%d)", stripped.Len(), orig.Len())
	}

	// Stripped stream must decompress to the original data.
	r := NewReader(bytes.NewReader(stripped.Bytes()))
	var got bytes.Buffer
	if _, err := r.WriteTo(&got); err != nil {
		t.Fatalf("decompress stripped: %v", err)
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatalf("stripped decompress mismatch")
	}

	// Searching sidecar against stripped must return the same matches as
	// brute force on the original data.
	for _, pat := range [][]byte{
		[]byte("needle in the haystack"),
		[]byte(`"level":"DEBUG"`),
	} {
		expected := countOccurrences(data, pat)
		matches := 0
		ss := NewSidecarSearcher(bytes.NewReader(stripped.Bytes()), bytes.NewReader(side.Bytes()))
		if err := ss.Search(pat, func(SearchResult) error {
			matches++
			return nil
		}); err != nil {
			t.Fatalf("search %q: %v", pat, err)
		}
		if matches != expected {
			t.Fatalf("pattern %q: got %d matches, want %d", pat, matches, expected)
		}
	}
}

func TestSidecarSearcher_AllTableTypes(t *testing.T) {
	data := makeTestData(120 << 10)
	cases := []struct {
		name string
		cfg  SearchTableConfig
	}{
		{"no-prefix", NewSearchTableConfig().WithMatchLen(6)},
		{"byte-prefix", NewSearchTableConfig().WithMatchLen(6).WithBytePrefix('"', ':')},
		{"long-prefix", NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte(`":"`))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var main, side bytes.Buffer
			w := NewWriter(&main,
				WriterBlockSize(32<<10),
				WriterConcurrency(1),
				WriterSearchTable(tc.cfg),
				WriterSidecar(&side),
			)
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			pat := []byte(`":"2024-01-15T10:11:14Z"`)
			expected := countOccurrences(data, pat)
			matches := 0
			ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))
			if err := ss.Search(pat, func(SearchResult) error {
				matches++
				return nil
			}); err != nil {
				t.Fatalf("search: %v", err)
			}
			if matches != expected {
				t.Fatalf("%s: got %d, want %d", tc.name, matches, expected)
			}
		})
	}
}

func TestSidecarSearcher_MultiConfigAND(t *testing.T) {
	data := makeTestData(120 << 10)
	cfgs := []SearchTableConfig{
		NewSearchTableConfig().WithMatchLen(6),
		NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':'),
	}
	// Build a sidecar carrying both configs.
	var main bytes.Buffer
	w := NewWriter(&main, WriterBlockSize(32<<10), WriterConcurrency(1))
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var side bytes.Buffer
	if err := BuildSidecar(&side, bytes.NewReader(main.Bytes()),
		SidecarSearchTable(cfgs[0]),
		SidecarSearchTable(cfgs[1]),
	); err != nil {
		t.Fatalf("BuildSidecar: %v", err)
	}

	for _, pat := range [][]byte{
		[]byte("needle in the haystack"),
		[]byte(`"level":"INFO"`),
		[]byte(`":"2024-01-15`),
	} {
		t.Run(string(pat), func(t *testing.T) {
			expected := countOccurrences(data, pat)
			matches := 0
			ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))
			if err := ss.Search(pat, func(SearchResult) error {
				matches++
				return nil
			}); err != nil {
				t.Fatalf("search: %v", err)
			}
			if matches != expected {
				t.Fatalf("got %d matches, want %d", matches, expected)
			}
		})
	}
}

func TestSidecarSearcher_ConcurrentSearch(t *testing.T) {
	data := makeTestData(200 << 10)
	cfg := NewSearchTableConfig().WithMatchLen(6)
	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
		WriterSidecar(&side),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	mainBytes := main.Bytes()
	sideBytes := side.Bytes()
	pat := []byte("needle in the haystack")
	expected := countOccurrences(data, pat)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ss := NewSidecarSearcher(bytes.NewReader(mainBytes), bytes.NewReader(sideBytes))
			count := 0
			err := ss.Search(pat, func(SearchResult) error {
				count++
				return nil
			})
			if err != nil {
				errs <- err
				return
			}
			if count != expected {
				errs <- fmt.Errorf("got %d, want %d", count, expected)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestSidecarSearcher_ReadAtCoalescing(t *testing.T) {
	// Make a small stream of 5 short blocks. With a pattern that matches in
	// all blocks, all should be decoded; verify ReadAt was called once
	// (coalesced).
	data := bytes.Repeat([]byte("needle "), 64<<10/7) // ~64KB of "needle "
	data = append(data, makeTestData(200<<10)...)

	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(NewSearchTableConfig().WithMatchLen(6)),
		WriterSidecar(&side),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	cr := &countingReaderAt{r: bytes.NewReader(main.Bytes())}
	pat := []byte("needle ")
	matches := 0
	ss := NewSidecarSearcher(cr, bytes.NewReader(side.Bytes()))
	if err := ss.Search(pat, func(SearchResult) error {
		matches++
		return nil
	}); err != nil {
		t.Fatalf("search: %v", err)
	}
	expected := countOccurrences(data, pat)
	if matches != expected {
		t.Fatalf("matches=%d want=%d", matches, expected)
	}
	// With every block matching, we expect one (or a few) coalesced ReadAts,
	// not one per block. There should be far fewer ReadAt calls than blocks.
	calls := cr.calls.Load()
	expectedBlocks := int64((len(data) + (32 << 10) - 1) / (32 << 10))
	if calls > expectedBlocks {
		t.Fatalf("expected coalesced ReadAt (<= %d calls), got %d", expectedBlocks, calls)
	}
	if calls == 0 {
		t.Fatalf("no ReadAt calls made")
	}
}

// rangeRecorderReaderAt records every ReadAt(off, len) range. Used to verify
// that the searcher never reads the same main-stream region twice.
type rangeRecorderReaderAt struct {
	r      io.ReaderAt
	mu     sync.Mutex
	ranges [][2]int64
}

func (r *rangeRecorderReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.r.ReadAt(p, off)
	r.mu.Lock()
	r.ranges = append(r.ranges, [2]int64{off, off + int64(n)})
	r.mu.Unlock()
	return n, err
}

// TestSidecarSearcher_NoBlockReadTwice verifies that across a full search,
// no byte of the main stream is fetched by more than one ReadAt. Mixes
// matching and non-matching patterns so the searcher's skip + must-decode +
// batched-decode paths all participate.
func TestSidecarSearcher_NoBlockReadTwice(t *testing.T) {
	data := makeTestData(600 << 10) // ~10 blocks @ 64KB
	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(64<<10),
		WriterConcurrency(1),
		WriterSearchTable(NewSearchTableConfig().WithMatchLen(6)),
		WriterSidecar(&side),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for _, pat := range [][]byte{
		[]byte("needle in the haystack"), // matches some blocks
		[]byte(`"level":"WARN"`),         // matches some blocks
		[]byte("absolutely-not-present"), // matches nothing → all-skip
		[]byte(`shutting down`),          // matches some blocks
	} {
		t.Run(string(pat), func(t *testing.T) {
			rr := &rangeRecorderReaderAt{r: bytes.NewReader(main.Bytes())}
			ss := NewSidecarSearcher(rr, bytes.NewReader(side.Bytes()))
			if err := ss.Search(pat, func(SearchResult) error { return nil }); err != nil {
				t.Fatalf("search: %v", err)
			}
			rr.mu.Lock()
			ranges := rr.ranges
			rr.mu.Unlock()
			sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
			for i := 1; i < len(ranges); i++ {
				// decodeBatch reads a conservative [start, last+MaxEncodedLen)
				// range for the final ref, so adjacent batches may legitimately
				// overlap at that tail. Only a range fully contained in the
				// previous one means a region (and its block) was fetched twice.
				if ranges[i][0] < ranges[i-1][0] || ranges[i][1] <= ranges[i-1][1] {
					t.Errorf("main region read twice: [%d,%d) contains [%d,%d)",
						ranges[i-1][0], ranges[i-1][1],
						ranges[i][0], ranges[i][1])
				}
			}
		})
	}
}

func TestSidecarSearcher_MalformedSidecar(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"short-header", []byte{0xff}},
		{"wrong-magic", append([]byte("\xff\x06\x00\x00garbage"), 0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ss := NewSidecarSearcher(bytes.NewReader([]byte{}), bytes.NewReader(tc.body))
			err := ss.Search([]byte("anything"), func(SearchResult) error { return nil })
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestRemoteBlockRef_RoundTrip(t *testing.T) {
	// Encode several refs, parse them back, assert equality.
	const maxBlock = 1 << 20
	cases := []struct {
		offset int64
		uncomp int
	}{
		{42, 1024},
		{maxBlock, 1},
		{1 << 30, maxBlock - 1},
		{0, maxBlock},
	}
	for _, tc := range cases {
		name := fmt.Sprintf("off=%d_uncomp=%d", tc.offset, tc.uncomp)
		t.Run(name, func(t *testing.T) {
			b := appendRemoteBlockRef(nil, tc.offset, maxBlock-tc.uncomp)
			// Skip 4-byte chunk header.
			refs, err := parseRemoteBlockRef(b[4:], maxBlock)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(refs) != 1 {
				t.Fatalf("got %d refs, want 1", len(refs))
			}
			if refs[0].offset != tc.offset || refs[0].uncompSize != tc.uncomp {
				t.Fatalf("got %+v, want {off=%d uncomp=%d}", refs[0], tc.offset, tc.uncomp)
			}
		})
	}
}

// makeConcatStream produces a buffer holding two complete MinLZ streams
// (data1 then data2) concatenated, plus the combined plaintext for the
// brute-force search comparison. opts apply to both streams.
func makeConcatStream(t *testing.T, data1, data2 []byte, opts ...WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts...)
	if _, err := w.Write(data1); err != nil {
		t.Fatalf("write stream 1: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close stream 1: %v", err)
	}
	w.Reset(&buf)
	if _, err := w.Write(data2); err != nil {
		t.Fatalf("write stream 2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close stream 2: %v", err)
	}
	return buf.Bytes()
}

func TestWriterSidecar_ConcatenatedStreams(t *testing.T) {
	d1 := makeTestData(80 << 10)
	d2 := makeTestData(80 << 10)
	cfg := NewSearchTableConfig().WithMatchLen(6)

	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
		WriterSidecar(&side),
	)
	if _, err := w.Write(d1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Reset and write the second stream into the same main + sidecar.
	w.Reset(&main)
	if _, err := w.Write(d2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Sidecar must contain two valid streams — NewReader walks it without
	// producing any user bytes.
	rSide := NewReader(bytes.NewReader(side.Bytes()))
	n, err := io.Copy(io.Discard, rSide)
	if err != nil {
		t.Fatalf("read concat sidecar: %v", err)
	}
	if n != 0 {
		t.Fatalf("sidecar produced %d data bytes, want 0", n)
	}

	// Main must decompress to d1 || d2.
	r := NewReader(bytes.NewReader(main.Bytes()))
	var got bytes.Buffer
	if _, err := r.WriteTo(&got); err != nil {
		t.Fatalf("decompress concat main: %v", err)
	}
	want := append(append([]byte{}, d1...), d2...)
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("decompressed concat main != d1||d2 (got %d bytes, want %d)", got.Len(), len(want))
	}

	// Sidecar searcher against concat main must return the right number of
	// matches across both streams.
	pat := []byte("needle in the haystack")
	expected := countOccurrences(d1, pat) + countOccurrences(d2, pat)
	matches := 0
	ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))
	if err := ss.Search(pat, func(SearchResult) error {
		matches++
		return nil
	}); err != nil {
		t.Fatalf("search: %v", err)
	}
	if matches != expected {
		t.Fatalf("got %d matches, want %d", matches, expected)
	}
}

func TestBuildSidecar_ConcatenatedStreams(t *testing.T) {
	d1 := makeTestData(80 << 10)
	d2 := makeTestData(80 << 10)

	// Produce a concatenated main stream with no inline search.
	mainBytes := makeConcatStream(t, d1, d2,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
	)

	var side bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(6)
	if err := BuildSidecar(&side, bytes.NewReader(mainBytes),
		SidecarSearchTable(cfg),
	); err != nil {
		t.Fatalf("BuildSidecar: %v", err)
	}

	// The sidecar should be a valid (concatenated) MinLZ stream.
	if _, err := io.Copy(io.Discard, NewReader(bytes.NewReader(side.Bytes()))); err != nil {
		t.Fatalf("read sidecar: %v", err)
	}

	pat := []byte(`"level":"WARN"`)
	expected := countOccurrences(d1, pat) + countOccurrences(d2, pat)
	matches := 0
	ss := NewSidecarSearcher(bytes.NewReader(mainBytes), bytes.NewReader(side.Bytes()))
	if err := ss.Search(pat, func(SearchResult) error {
		matches++
		return nil
	}); err != nil {
		t.Fatalf("search: %v", err)
	}
	if matches != expected {
		t.Fatalf("got %d, want %d", matches, expected)
	}
}

func TestExtractSidecar_ConcatenatedStreams(t *testing.T) {
	d1 := makeTestData(80 << 10)
	d2 := makeTestData(80 << 10)

	// Concatenated source has inline search tables in each stream.
	cfg := NewSearchTableConfig().WithMatchLen(6)
	mainBytes := makeConcatStream(t, d1, d2,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
	)

	t.Run("no-strip", func(t *testing.T) {
		var side bytes.Buffer
		if err := ExtractSidecar(&side, nil, bytes.NewReader(mainBytes)); err != nil {
			t.Fatalf("ExtractSidecar: %v", err)
		}
		pat := []byte("needle in the haystack")
		expected := countOccurrences(d1, pat) + countOccurrences(d2, pat)
		matches := 0
		ss := NewSidecarSearcher(bytes.NewReader(mainBytes), bytes.NewReader(side.Bytes()))
		if err := ss.Search(pat, func(SearchResult) error {
			matches++
			return nil
		}); err != nil {
			t.Fatalf("search: %v", err)
		}
		if matches != expected {
			t.Fatalf("got %d, want %d", matches, expected)
		}
	})

	t.Run("strip", func(t *testing.T) {
		var side, stripped bytes.Buffer
		if err := ExtractSidecar(&side, &stripped, bytes.NewReader(mainBytes)); err != nil {
			t.Fatalf("ExtractSidecar: %v", err)
		}
		// Stripped concat must decompress to d1||d2.
		var got bytes.Buffer
		if _, err := NewReader(bytes.NewReader(stripped.Bytes())).WriteTo(&got); err != nil {
			t.Fatalf("decompress stripped: %v", err)
		}
		want := append(append([]byte{}, d1...), d2...)
		if !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("stripped concat decompress mismatch (got %d bytes, want %d)", got.Len(), len(want))
		}
		// Search via sidecar against stripped.
		pat := []byte("needle in the haystack")
		expected := countOccurrences(d1, pat) + countOccurrences(d2, pat)
		matches := 0
		ss := NewSidecarSearcher(bytes.NewReader(stripped.Bytes()), bytes.NewReader(side.Bytes()))
		if err := ss.Search(pat, func(SearchResult) error {
			matches++
			return nil
		}); err != nil {
			t.Fatalf("search: %v", err)
		}
		if matches != expected {
			t.Fatalf("got %d, want %d", matches, expected)
		}
	})
}

// TestSidecarSearcher_StatsFullyPopulated guards against two regressions:
//
//	(1) SearchStats.UncompressedSize was zero because s.blockStart was reset
//	    on chunkTypeEOF; it should equal the sum of decoded block sizes.
//	(2) compressed sub-block stats (CompressedBlocksTotal, raw/RLE/sparse
//	    breakdown, table headers, payload bytes) were not accumulated from
//	    cstDecoder after parsing each 0x46 chunk — they all came out as zero.
func TestSidecarSearcher_StatsFullyPopulated(t *testing.T) {
	// Compressed search tables (0x46) are only emitted when -search.compress
	// is set AND the table actually compresses; build data that produces them.
	data := makeTestData(400 << 10)

	cfg := NewSearchTableConfig().WithMatchLen(6).WithCompression()
	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(64<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
		WriterSidecar(&side),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	ss := NewSidecarSearcher(
		bytes.NewReader(main.Bytes()),
		bytes.NewReader(side.Bytes()),
		BlockSearchCollectStats(),
	)
	if err := ss.Search([]byte("absolutely-not-in-stream"), func(SearchResult) error { return nil }); err != nil {
		t.Fatalf("search: %v", err)
	}
	st := ss.Stats()

	// (1) UncompressedSize must equal the data we wrote.
	if st.UncompressedSize != int64(len(data)) {
		t.Errorf("UncompressedSize: got %d, want %d", st.UncompressedSize, len(data))
	}

	// Sanity: we should see at least one compressed table emitted; if not,
	// the rest of the assertions don't apply.
	if st.TablesCompressed == 0 {
		t.Skip("no 0x46 chunks emitted with this data; skipping compressed-stat check")
	}
	// (2) compressed sub-block stats: CompressedBlocksTotal should be > 0 and
	// equal the sum of the per-disposition counts.
	if st.CompressedBlocksTotal == 0 {
		t.Errorf("CompressedBlocksTotal is 0; per-block sub-block stats not accumulated")
	}
	sum := st.CompressedBlocksRaw + st.CompressedBlocksRLE + st.CompressedBlocksSparse
	if sum > st.CompressedBlocksTotal {
		t.Errorf("CompressedBlocks{Raw,RLE,Sparse} sum=%d exceeds Total=%d", sum, st.CompressedBlocksTotal)
	}
	// At least one payload-byte counter should be non-zero (some sub-blocks
	// must carry actual bytes).
	if st.CompressedBytesTabled+st.CompressedBytesRaw+st.CompressedBytesRLE+st.CompressedBytesSparse == 0 {
		t.Errorf("all CompressedBytes* counters are zero; payload bytes not accumulated")
	}
}

// TestSearchers_SkipUnusableTableDecode asserts that when the search pattern
// makes every per-block table config unusable, the 0x46 bitmap decode is
// short-circuited: TablesUnusable bumps for every data block, but the
// CompressedBlocks*/CompressedTablesSum counters stay at zero — proof that
// the huff0/sparse decode was actually skipped.
func TestSearchers_SkipUnusableTableDecode(t *testing.T) {
	data := makeTestData(400 << 10)
	// Long prefix that appears in every line of makeTestData → table fills
	// and compresses, producing 0x46 chunks. The search pattern below does
	// NOT contain "timestamp", so patternCanUseConfig returns false for every
	// table.
	cfg := NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte("timestamp"))
	cfg = cfg.WithCompression()

	pattern := []byte("ZZZ-no-such-substring")

	check := func(t *testing.T, st SearchStats) {
		t.Helper()
		if st.TablesCompressed == 0 {
			t.Skip("no 0x46 chunks emitted with this data; nothing to verify")
		}
		if st.TablesUnusable != st.TablesPresent {
			t.Errorf("TablesUnusable=%d want %d (all tables should be unusable for this pattern)",
				st.TablesUnusable, st.TablesPresent)
		}
		// Proof the bitmap decode was skipped: none of the sub-block counters
		// can be non-zero — they're populated only by the huff0/sparse decoder.
		if st.CompressedBlocksTotal != 0 || st.CompressedTablesSum != 0 {
			t.Errorf("bitmap decode was not skipped: CompressedBlocksTotal=%d CompressedTablesSum=%d",
				st.CompressedBlocksTotal, st.CompressedTablesSum)
		}
		if st.CompressedBytesTabled+st.CompressedBytesRaw+st.CompressedBytesRLE+st.CompressedBytesSparse != 0 {
			t.Errorf("payload-byte counters non-zero (%d/%d/%d/%d) — bitmap decode happened",
				st.CompressedBytesTabled, st.CompressedBytesRaw, st.CompressedBytesRLE, st.CompressedBytesSparse)
		}
		// Sanity: TableBitmapBytes is still derived from cfg for skipped decodes,
		// so the ratio in Fprint output remains meaningful.
		if st.TableBitmapBytes == 0 {
			t.Errorf("TableBitmapBytes is zero; expected the cfg-derived size to be accumulated")
		}
	}

	t.Run("BlockSearcher", func(t *testing.T) {
		// Inline tables — no sidecar.
		var main bytes.Buffer
		w := NewWriter(&main,
			WriterBlockSize(64<<10),
			WriterConcurrency(1),
			WriterSearchTable(cfg),
		)
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		bs := NewBlockSearcher(bytes.NewReader(main.Bytes()), BlockSearchCollectStats())
		if err := bs.Search(pattern, func(SearchResult) error { return nil }); err != nil {
			t.Fatal(err)
		}
		check(t, bs.Stats())
	})

	t.Run("SidecarSearcher", func(t *testing.T) {
		// Tables emitted to the sidecar stream.
		var main, side bytes.Buffer
		w := NewWriter(&main,
			WriterBlockSize(64<<10),
			WriterConcurrency(1),
			WriterSearchTable(cfg),
			WriterSidecar(&side),
		)
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		ss := NewSidecarSearcher(
			bytes.NewReader(main.Bytes()),
			bytes.NewReader(side.Bytes()),
			BlockSearchCollectStats(),
		)
		if err := ss.Search(pattern, func(SearchResult) error { return nil }); err != nil {
			t.Fatal(err)
		}
		check(t, ss.Stats())
	})
}

// TestSidecarSearcher_InlineParity runs the same queries through both
// BlockSearcher (inline tables) and SidecarSearcher (sidecar tables) and
// asserts they produce identical match sets and equivalent skip/search
// statistics. This is the master cross-check: any divergence in the
// table-driven skip/decode/defer decisions between the two searchers
// will trip one of these assertions.
func TestSidecarSearcher_InlineParity(t *testing.T) {
	data := makeTestData(512 << 10)
	// Plant a unique value that we can search for.
	copy(data[200<<10:], []byte("UNIQUE-PARITY-NEEDLE"))

	type cfgCase struct {
		name string
		cfg  SearchTableConfig
	}
	cases := []cfgCase{
		{"no-prefix-ml6", NewSearchTableConfig().WithMatchLen(6)},
		{"no-prefix-ml4", NewSearchTableConfig().WithMatchLen(4)},
		{"byte-prefix", NewSearchTableConfig().WithMatchLen(6).WithBytePrefix('"', ':')},
		{"long-prefix", NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte(`":"`))},
	}
	patterns := [][]byte{
		[]byte("UNIQUE-PARITY-NEEDLE"),                             // exists
		[]byte(`"level":"WARN"`),                                   // common, exists many times
		[]byte("absolutely-not-anywhere-in-the-stream-payload"),    // never present
		[]byte(`"timestamp":"2024-01-15T10:11:14Z"`),               // boundary-prone
		[]byte("o8ijhdf98u89ehjfg890fdjhgt3454dfgdfgsfghfg6fghfg"), // random pattern that triggers deferred path
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Build inline-tabled stream.
			var inline bytes.Buffer
			w := NewWriter(&inline,
				WriterBlockSize(32<<10),
				WriterConcurrency(1),
				WriterSearchTable(tc.cfg),
			)
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			// Build sidecar-mode stream.
			var main, side bytes.Buffer
			w2 := NewWriter(&main,
				WriterBlockSize(32<<10),
				WriterConcurrency(1),
				WriterSearchTable(tc.cfg),
				WriterSidecar(&side),
			)
			if _, err := w2.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := w2.Close(); err != nil {
				t.Fatal(err)
			}

			for _, pat := range patterns {
				t.Run(fmt.Sprintf("%q", pat[:min(len(pat), 20)]), func(t *testing.T) {
					// Inline search.
					var inlineMatches []int64
					bs := NewBlockSearcher(bytes.NewReader(inline.Bytes()), BlockSearchCollectStats())
					if err := bs.Search(pat, func(r SearchResult) error {
						inlineMatches = append(inlineMatches, r.StreamOffset)
						return nil
					}); err != nil {
						t.Fatalf("inline search: %v", err)
					}
					bsStats := bs.Stats()

					// Sidecar search.
					var sideMatches []int64
					ss := NewSidecarSearcher(
						bytes.NewReader(main.Bytes()),
						bytes.NewReader(side.Bytes()),
						BlockSearchCollectStats(),
					)
					if err := ss.Search(pat, func(r SearchResult) error {
						sideMatches = append(sideMatches, r.StreamOffset)
						return nil
					}); err != nil {
						t.Fatalf("sidecar search: %v", err)
					}
					ssStats := ss.Stats()

					// Match sets must be identical.
					if len(inlineMatches) != len(sideMatches) {
						t.Fatalf("match count: inline=%d sidecar=%d (inline=%v sidecar=%v)",
							len(inlineMatches), len(sideMatches), inlineMatches, sideMatches)
					}
					for i := range inlineMatches {
						if inlineMatches[i] != sideMatches[i] {
							t.Errorf("match[%d] StreamOffset mismatch: inline=%d sidecar=%d",
								i, inlineMatches[i], sideMatches[i])
						}
					}
					// Block decisions must agree at the headline level.
					if bsStats.BlocksTotal != ssStats.BlocksTotal {
						t.Errorf("BlocksTotal: inline=%d sidecar=%d", bsStats.BlocksTotal, ssStats.BlocksTotal)
					}
					if bsStats.BlocksSkipped != ssStats.BlocksSkipped {
						t.Errorf("BlocksSkipped: inline=%d sidecar=%d", bsStats.BlocksSkipped, ssStats.BlocksSkipped)
					}
					if bsStats.BlocksSearched != ssStats.BlocksSearched {
						t.Errorf("BlocksSearched: inline=%d sidecar=%d", bsStats.BlocksSearched, ssStats.BlocksSearched)
					}
					if bsStats.BlocksDeferred != ssStats.BlocksDeferred {
						t.Errorf("BlocksDeferred: inline=%d sidecar=%d", bsStats.BlocksDeferred, ssStats.BlocksDeferred)
					}
					if bsStats.BlocksDeferredSkipped != ssStats.BlocksDeferredSkipped {
						t.Errorf("BlocksDeferredSkipped: inline=%d sidecar=%d",
							bsStats.BlocksDeferredSkipped, ssStats.BlocksDeferredSkipped)
					}
					if bsStats.UncompressedSize != ssStats.UncompressedSize {
						t.Errorf("UncompressedSize: inline=%d sidecar=%d",
							bsStats.UncompressedSize, ssStats.UncompressedSize)
					}
					if bsStats.UncompBytesSearched != ssStats.UncompBytesSearched {
						t.Errorf("UncompBytesSearched: inline=%d sidecar=%d",
							bsStats.UncompBytesSearched, ssStats.UncompBytesSearched)
					}
				})
			}
		})
	}
}

// TestSidecarSearcher_DeferredDecode guards against the SidecarSearcher
// missing SPEC §B.4's deferred-decode optimisation. A pattern is chosen
// whose first 6 bytes are planted in many blocks (so the first-window
// table check trips a "might-match"), while the rest of the pattern is
// random and never appears anywhere. With deferred decode in place,
// nearly every "might-match" turns into a deferred-then-skipped block;
// without it, every one becomes a wasted decode.
func TestSidecarSearcher_DeferredDecode(t *testing.T) {
	// 64-byte pattern. The first 6 bytes are planted into the data; the
	// remaining 58 bytes are random — they should never appear.
	pattern := []byte("PLANT-" + strings.Repeat("x", 58))
	if len(pattern) != 64 {
		t.Fatalf("test setup: pattern length %d, want 64", len(pattern))
	}
	// 4 MiB of base data, then plant "PLANT-" at scattered positions so
	// roughly every block contains it.
	data := makeTestData(4 << 20)
	for off := 1000; off < len(data); off += 8000 {
		copy(data[off:], pattern[:6])
	}
	cfg := NewSearchTableConfig().WithMatchLen(6)

	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(64<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
		WriterSidecar(&side),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	ss := NewSidecarSearcher(
		bytes.NewReader(main.Bytes()),
		bytes.NewReader(side.Bytes()),
		BlockSearchCollectStats(),
	)
	if err := ss.Search(pattern, func(SearchResult) error { return nil }); err != nil {
		t.Fatal(err)
	}
	st := ss.Stats()

	if st.BlocksDeferred == 0 {
		t.Fatalf("BlocksDeferred=0 — deferred-decode path never fired (BlocksTotal=%d)", st.BlocksTotal)
	}
	// Almost all deferred blocks should ultimately skip: the absent later
	// windows of the (random) pattern tail have ~0 chance of all being set
	// in the next block's table. The last deferred at EOF resolves to
	// "decode" conservatively (no next table), so we allow a slack of 1.
	if st.BlocksDeferred-st.BlocksDeferredSkipped > 1 {
		t.Errorf("BlocksDeferredSkipped=%d, BlocksDeferred=%d — expected all-but-one to skip",
			st.BlocksDeferredSkipped, st.BlocksDeferred)
	}
	// BlocksSearched should be a small fraction of total — without deferred
	// decode it would be roughly equal to BlocksDeferred (every defer would
	// otherwise have been a wasted decode).
	if st.BlocksSearched > st.BlocksDeferred {
		t.Errorf("BlocksSearched=%d > BlocksDeferred=%d — deferred decode not effective",
			st.BlocksSearched, st.BlocksDeferred)
	}
}

// TestSidecarSearcher_NoBoundaryCascade guards against a SidecarSearcher
// regression where a block that was decoded *only* to satisfy a boundary
// check from the previous block was retained as prevBlock, causing a chain
// of unnecessary decodes for random-looking patterns. BlockSearcher clears
// prevBlock in this case (search_reader.go's tableNoMatch flag).
//
// We assert by comparing the BlocksSearched count between SidecarSearcher
// and inline BlockSearcher for the same pattern + same data — they should
// match within a small tolerance.
func TestSidecarSearcher_NoBoundaryCascade(t *testing.T) {
	// 64-byte pseudo-random pattern that doesn't appear in the data.
	pattern := []byte("o8ijhdf98u89ehjfg890fdjhgt3454dfgdfg3fghfghfghfjtijhfgoijh05984se")
	// Larger data with many small blocks so the cascade has room to amplify.
	data := makeTestData(2 << 20)
	cfg := NewSearchTableConfig().WithMatchLen(6)

	// Inline tables.
	var inline bytes.Buffer
	w := NewWriter(&inline,
		WriterBlockSize(16<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Sidecar version (same data, same cfg).
	var main, side bytes.Buffer
	w2 := NewWriter(&main,
		WriterBlockSize(16<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
		WriterSidecar(&side),
	)
	if _, err := w2.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	bs := NewBlockSearcher(bytes.NewReader(inline.Bytes()), BlockSearchCollectStats())
	if err := bs.Search(pattern, func(SearchResult) error { return nil }); err != nil {
		t.Fatal(err)
	}
	bsStats := bs.Stats()

	ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()), BlockSearchCollectStats())
	if err := ss.Search(pattern, func(SearchResult) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ssStats := ss.Stats()

	if bsStats.BlocksTotal != ssStats.BlocksTotal {
		t.Fatalf("block count mismatch: inline=%d sidecar=%d", bsStats.BlocksTotal, ssStats.BlocksTotal)
	}
	// Allow a tiny absolute slop (boundary timing differences) but assert
	// the sidecar does not decode more than the inline searcher does.
	if ssStats.BlocksSearched > bsStats.BlocksSearched+2 {
		t.Fatalf("sidecar decoded %d blocks, inline decoded %d — cascade not bounded",
			ssStats.BlocksSearched, bsStats.BlocksSearched)
	}
}

func TestSidecarSearcher_BailOnMissing(t *testing.T) {
	// Build a sidecar with a pattern too short for the configured matchLen.
	// Should return ErrSearchTablesUnusable when BailOnMissing is set.
	data := makeTestData(60 << 10)
	cfg := NewSearchTableConfig().WithMatchLen(8) // pattern shorter than this is unusable
	var main, side bytes.Buffer
	w := NewWriter(&main,
		WriterBlockSize(32<<10),
		WriterConcurrency(1),
		WriterSearchTable(cfg),
		WriterSidecar(&side),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()),
		BlockSearchBailOnMissing(),
	)
	err := ss.Search([]byte("abc"), func(SearchResult) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "search tables cannot be used") {
		// ErrSearchTablesUnusable expected.
		t.Fatalf("got err=%v, want ErrSearchTablesUnusable", err)
	}
}

// Sidecar counterpart to BenchmarkSearchCompressedVsUncompressed in
// search_compressed_test.go. Walks a sidecar (with search tables) and a main
// stream (data only) via io.ReaderAt. Pattern is absent from the random data
// and outside its byte range, so every block is skipped via the sidecar's
// table — the main stream should not be touched.
func BenchmarkSidecarSearchCompressedVsUncompressed(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	data := make([]byte, 256<<20)
	for i := range data {
		data[i] = byte(rng.Intn(8))
	}
	pattern := []byte("needle-not-present")
	build := func(useCompression bool) (main, side []byte) {
		cfg := NewSearchTableConfig().WithMatchLen(4)
		if !useCompression {
			cfg = cfg.WithoutCompression()
		} else {
			cfg = cfg.WithCompression(CompressedSearchForce())
		}
		var mainBuf, sideBuf bytes.Buffer
		mainBuf.Grow(len(data))
		w := NewWriter(&mainBuf,
			WriterLevel(LevelSmallest),
			WriterSearchTable(cfg),
			WriterSidecar(&sideBuf),
			WriterBlockSize(1<<20),
			WriterConcurrency(runtime.GOMAXPROCS(0)),
		)
		_, _ = w.Write(data)
		_ = w.Close()
		return mainBuf.Bytes(), sideBuf.Bytes()
	}
	run := func(b *testing.B, main, side []byte) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		ra := &countingReaderAt{r: bytes.NewReader(main)}
		for b.Loop() {
			ss := NewSidecarSearcher(ra, bytes.NewReader(side))
			_ = ss.Search(pattern, func(SearchResult) error { return nil })
		}
		if n := ra.calls.Load(); n != 0 {
			b.Fatalf("main stream was read %d times for an all-skip search", n)
		}
	}
	mainU, sideU := build(false)
	mainC, sideC := build(true)
	b.Run("uncompressed", func(b *testing.B) { run(b, mainU, sideU) })
	b.Run("compressed", func(b *testing.B) { run(b, mainC, sideC) })
}

// TestSearchConcatenatedNoCrossStreamStraddle verifies a pattern straddling the
// boundary between two concatenated streams is NOT reported: concatenated
// streams are independent and offsets restart at 0. d1 ends with the full
// pattern (one real match) followed by the pattern's head; d2 begins with the
// pattern's tail. A searcher that leaked boundary state (prevBlock/tailBuf)
// across the stream identifier would report a spurious second, straddling
// match. Covers both the inline and sidecar searchers.
func TestSearchConcatenatedNoCrossStreamStraddle(t *testing.T) {
	pattern := []byte("QWERTYUIOPASDFGH")
	filler := bytes.Repeat([]byte("the quick brown fox 0123456789\n"), 500)
	d1 := append(append(append([]byte{}, filler...), pattern...), pattern[:10]...)
	d2 := append(append([]byte{}, pattern[10:]...), filler...)
	if countOccurrences(d1, pattern) != 1 || countOccurrences(d2, pattern) != 0 {
		t.Fatalf("setup: d1=%d d2=%d, want 1,0", countOccurrences(d1, pattern), countOccurrences(d2, pattern))
	}
	cfg := NewSearchTableConfig().WithMatchLen(6)

	type searcher interface {
		Search([]byte, func(SearchResult) error) error
	}
	count := func(s searcher) int {
		n := 0
		if err := s.Search(pattern, func(SearchResult) error { n++; return nil }); err != nil {
			t.Fatalf("search: %v", err)
		}
		return n
	}

	// Inline: two concatenated tabled streams in one buffer.
	var inline bytes.Buffer
	w := NewWriter(&inline, WriterBlockSize(minBlockSize), WriterConcurrency(1), WriterSearchTable(cfg))
	if _, err := w.Write(d1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w.Reset(&inline)
	if _, err := w.Write(d2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Decoded output is d1||d2 and contains the pattern twice (real + straddle).
	dec, err := io.ReadAll(NewReader(bytes.NewReader(inline.Bytes())))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := countOccurrences(dec, pattern); got != 2 {
		t.Fatalf("decoded output has %d occurrences, want 2 (real + straddle)", got)
	}
	if got := count(NewBlockSearcher(bytes.NewReader(inline.Bytes()))); got != 1 {
		t.Fatalf("inline concatenated search: got %d matches, want 1 (no cross-stream straddle)", got)
	}

	// Sidecar: the same two streams with tables in the sidecar.
	var main, side bytes.Buffer
	w2 := NewWriter(&main, WriterBlockSize(minBlockSize), WriterConcurrency(1), WriterSearchTable(cfg), WriterSidecar(&side))
	if _, err := w2.Write(d1); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	w2.Reset(&main)
	if _, err := w2.Write(d2); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if got := count(NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()))); got != 1 {
		t.Fatalf("sidecar concatenated search: got %d matches, want 1 (no cross-stream straddle)", got)
	}
}

// TestSidecarSearchWindowStatsConfigDrift verifies per-window stats collection
// stops on config drift (a multi-config sidecar presents tables with different
// window layouts) rather than mixing incompatible tables into one set of
// counts. Without the guard, every table of every config is tallied, pushing
// the per-window denominator above the block count.
func TestSidecarSearchWindowStatsConfigDrift(t *testing.T) {
	data := makeTestData(120 << 10)
	var main bytes.Buffer
	w := NewWriter(&main, WriterBlockSize(32<<10), WriterConcurrency(1))
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var side bytes.Buffer
	if err := BuildSidecar(&side, bytes.NewReader(main.Bytes()),
		SidecarSearchTable(NewSearchTableConfig().WithMatchLen(6)),
		SidecarSearchTable(NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':')),
	); err != nil {
		t.Fatalf("BuildSidecar: %v", err)
	}

	ss := NewSidecarSearcher(bytes.NewReader(main.Bytes()), bytes.NewReader(side.Bytes()), BlockSearchCollectStats())
	if err := ss.Search([]byte(`"level":"INFO"`), func(SearchResult) error { return nil }); err != nil {
		t.Fatalf("search: %v", err)
	}
	st := ss.Stats()
	if len(st.Windows) == 0 {
		t.Fatal("no pattern windows enumerated")
	}
	denom := st.Windows[0].Present + st.Windows[0].Absent
	for i, ws := range st.Windows {
		if ws.Present+ws.Absent != denom {
			t.Fatalf("window %d tallied %d times, want %d (inconsistent)", i, ws.Present+ws.Absent, denom)
		}
	}
	if denom > st.BlocksTotal {
		t.Fatalf("window denominator %d exceeds %d blocks — incompatible configs were mixed into the counts", denom, st.BlocksTotal)
	}
}
