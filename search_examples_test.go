package minlz_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minlz"
)

func ExampleWriterSearchTable() {
	// Compress data with search tables enabled.
	var buf bytes.Buffer
	cfg := minlz.NewSearchTableConfig() // matchLen defaults to 6
	w := minlz.NewWriter(&buf, minlz.WriterSearchTable(cfg))
	w.Write([]byte("Hello World! This is searchable compressed data."))
	w.Close()

	// The stream is still readable by a normal Reader.
	decoded, _ := io.ReadAll(minlz.NewReader(bytes.NewReader(buf.Bytes())))
	fmt.Println("decoded:", string(decoded))
	// Output:
	// decoded: Hello World! This is searchable compressed data.
}

func ExampleWriterSearchTable_withPrefix() {
	// Prefix bytes dramatically reduce table size for structured data.
	// Only positions after '"' and ':' are indexed, so the table is much sparser.
	blockSize := 4096
	data := make([]byte, blockSize*3)
	// Fill with compressible JSON-like lines.
	line := []byte(`{"name":"Alice","age":30,"id":"xyz"}` + "\n")
	for i := 0; i < len(data); i += len(line) {
		copy(data[i:], line)
	}
	// Place a unique value only in block 1.
	copy(data[blockSize+100:], []byte(`{"name":"FindMe","id":"unique-9876"}`))

	var buf bytes.Buffer
	cfg := minlz.NewSearchTableConfig().WithBytePrefix('"', ':')
	w := minlz.NewWriter(&buf,
		minlz.WriterSearchTable(cfg),
		minlz.WriterBlockSize(blockSize),
		minlz.WriterConcurrency(1),
	)
	w.Write(data)
	w.Close()

	// Search for a pattern that contains prefix bytes internally.
	// The searcher finds '"' at position 2 and ':' at position 3 inside the pattern,
	// and uses those to check the table — no need to start with a prefix byte.
	searcher := minlz.NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	searcher.Search([]byte(`"unique-9876"`), func(r minlz.SearchResult) error {
		fmt.Println("found at stream offset", r.StreamOffset)
		return nil
	})
	stats := searcher.Stats()
	fmt.Printf("blocks: %d total, %d skipped\n", stats.BlocksTotal, stats.BlocksSkipped)
	// Output:
	// found at stream offset 4218
	// blocks: 3 total, 1 skipped
}

func ExampleBlockSearcher() {
	// Create test data: two blocks, needle only in the second.
	data := make([]byte, 8192)
	copy(data[:4096], bytes.Repeat([]byte("aaaa"), 1024))
	copy(data[4096:], bytes.Repeat([]byte("bbbb"), 1024))
	copy(data[4096+100:], []byte("NEEDLE_PATTERN"))

	var buf bytes.Buffer
	cfg := minlz.NewSearchTableConfig().WithMatchLen(4)
	w := minlz.NewWriter(&buf,
		minlz.WriterSearchTable(cfg),
		minlz.WriterBlockSize(4096),
		minlz.WriterConcurrency(1),
	)
	w.Write(data)
	w.Close()

	// Search for the needle.
	searcher := minlz.NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	err := searcher.Search([]byte("NEEDLE_PATTERN"), func(r minlz.SearchResult) error {
		fmt.Printf("found at stream offset %d\n", r.StreamOffset)
		return nil
	})
	if err != nil {
		fmt.Println("error:", err)
	}

	stats := searcher.Stats()
	fmt.Printf("blocks: %d total, %d skipped\n", stats.BlocksTotal, stats.BlocksSkipped)
	// Output:
	// found at stream offset 4196
	// blocks: 2 total, 1 skipped
}

func ExampleBlockSearcher_bail() {
	// Compress WITHOUT search tables.
	var buf bytes.Buffer
	w := minlz.NewWriter(&buf, minlz.WriterBlockSize(4096), minlz.WriterConcurrency(1))
	w.Write(bytes.Repeat([]byte("test data "), 1000))
	w.Close()

	searcher := minlz.NewBlockSearcher(
		bytes.NewReader(buf.Bytes()),
		minlz.BlockSearchBailOnMissing(),
	)
	err := searcher.Search([]byte("pattern"), func(r minlz.SearchResult) error {
		return nil
	})
	fmt.Println(err)
	// Output:
	// minlz: search tables cannot be used for this pattern
}

func ExampleSearchStats_Fprint() {
	// Create and search a stream to show stats output.
	data := make([]byte, 16384)
	copy(data[:8192], bytes.Repeat([]byte("aaaa"), 2048))
	copy(data[8192:], bytes.Repeat([]byte("bbbb"), 2048))
	copy(data[8192+100:], []byte("FINDME"))

	var buf bytes.Buffer
	cfg := minlz.NewSearchTableConfig().WithMatchLen(4)
	w := minlz.NewWriter(&buf,
		minlz.WriterSearchTable(cfg),
		minlz.WriterBlockSize(8192),
		minlz.WriterConcurrency(1),
	)
	w.Write(data)
	w.Close()

	searcher := minlz.NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	searcher.Search([]byte("FINDME"), func(r minlz.SearchResult) error { return nil })

	var out strings.Builder
	searcher.Stats().Fprint(&out)
	s := strings.Replace(out.String(), fmt.Sprintf("skipped: %d compressed", searcher.Stats().CompBytesSkipped), "skipped: <N> compressed", 1)
	fmt.Print(s)
	// Output:
	// Blocks total: 2, skipped: 1 (50.0%), deferred: 0 (0.0%, 0 skipped)
	// Blocks searched: 1 (50.0%), false positive: 0 (0.0%)
	// Bytes skipped: <N> compressed, searched: 8192 uncompressed
	// Tables: 2 present, 0 missing, 0 unusable
	// Table bits/byte: 0.0391, log2: 8.0, avg reductions: 5.0
	// Table total: 80 bytes, avg 40 bytes/table, 0.49% of 16384 uncompressed
	// Table population: avg 2.7%, min 1.6%, max 3.9%
}
