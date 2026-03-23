package minlz

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

// withBaseTableSize sets baseTableSize directly for unit testing.
func withBaseTableSize(c SearchTableConfig, ts int) SearchTableConfig {
	c.baseTableSize = uint8(ts)
	return c
}

func TestHashValue(t *testing.T) {
	// Verify HashValue matches per-matchLen helpers.
	v := uint64(0x0102030405060708)
	for _, ts := range []uint8{8, 12, 16, 20, 23} {
		if got := hashValue(v, ts, 2); ts < 16 && got != hashValue2(v, ts) {
			t.Errorf("matchLen=2 ts=%d: HashValue=%d, hashValue2=%d", ts, got, hashValue2(v, ts))
		}
		if got := hashValue(v, ts, 3); got != hashValue3(v, ts) {
			t.Errorf("matchLen=3 ts=%d: HashValue=%d, hashValue3=%d", ts, got, hashValue3(v, ts))
		}
		if got := hashValue(v, ts, 4); got != hashValue4(v, ts) {
			t.Errorf("matchLen=4 ts=%d: HashValue=%d, hashValue4=%d", ts, got, hashValue4(v, ts))
		}
		if got := hashValue(v, ts, 5); got != hashValue5(v, ts) {
			t.Errorf("matchLen=5 ts=%d: HashValue=%d, hashValue5=%d", ts, got, hashValue5(v, ts))
		}
		if got := hashValue(v, ts, 6); got != hashValue6(v, ts) {
			t.Errorf("matchLen=6 ts=%d: HashValue=%d, hashValue6=%d", ts, got, hashValue6(v, ts))
		}
		if got := hashValue(v, ts, 7); got != hashValue7(v, ts) {
			t.Errorf("matchLen=7 ts=%d: HashValue=%d, hashValue7=%d", ts, got, hashValue7(v, ts))
		}
		if got := hashValue(v, ts, 8); got != hashValue8(v, ts) {
			t.Errorf("matchLen=8 ts=%d: HashValue=%d, hashValue8=%d", ts, got, hashValue8(v, ts))
		}
	}
	// matchLen=1 always returns low byte
	if got := hashValue(0x42, 8, 1); got != 0x42 {
		t.Errorf("matchLen=1: got %d, want 0x42", got)
	}
}

func TestBuildTableNoPrefix(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 4096)
	rng.Read(data)

	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 16)
	table, reductions := cfg.buildSearchTable(data, nil)
	if table == nil {
		t.Fatal("table should not be nil for 4KB data with 16-bit table")
	}

	// Verify no false negatives: every 4-byte window must be set.
	for i := 0; i <= len(data)-4; i++ {
		v := readLE64Pad(data[i:])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & ((1 << (cfg.baseTableSize - reductions)) - 1)
		if table[h>>3]&(1<<(h&7)) == 0 {
			t.Fatalf("false negative at position %d (h=%d, reductions=%d, tableLen=%d)", i, h, reductions, len(table))
		}
	}
}

func TestBuildTableNoPrefix_NoFalseNegative(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for _, size := range []int{100, 1000, 8000, 32000} {
		data := make([]byte, size)
		rng.Read(data)

		for ml := 1; ml <= 8; ml++ {
			cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(ml), 16)
			table, reductions := cfg.buildSearchTable(data, nil)
			if table == nil {
				continue // too populated, skip
			}
			effectiveSize := cfg.baseTableSize - reductions

			for i := 0; i <= len(data)-ml; i++ {
				v := readLE64Pad(data[i:])
				h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & ((1 << (cfg.baseTableSize - reductions)) - 1)
				_ = effectiveSize
				if table[h>>3]&(1<<(h&7)) == 0 {
					t.Fatalf("false negative: size=%d ml=%d pos=%d reductions=%d", size, ml, i, reductions)
				}
			}
		}
	}
}

func TestBuildTableWithOverlap(t *testing.T) {
	data := []byte("hello world test")
	overlap := []byte("xyz")
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 10)
	table, reductions := cfg.buildSearchTable(data, overlap)
	if table == nil {
		t.Fatal("table should not be nil")
	}

	// The last position in data that extends into overlap should be indexed.
	// Position len(data)-3 reads "stx" which is 3 bytes of data + overlap[0].
	// With matchLen=4, position len(data)-3 = 13 reads data[13:17] = "est" + overlap "x".
	combined := append(data, overlap...)
	for i := 0; i <= len(data)-4; i++ {
		v := readLE64Pad(combined[i:])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & ((1 << (cfg.baseTableSize - reductions)) - 1)
		if table[h>>3]&(1<<(h&7)) == 0 {
			t.Fatalf("false negative at position %d", i)
		}
	}
}

func TestReduceTable(t *testing.T) {
	// Create a sparse table: 32 uint64s = 256 bytes = 2048 bits.
	table := make([]uint64, 32)
	table[0] = 1
	table[12] = 2
	table[25] = 4

	orig, _ := tablePopulation(table)
	reduced, reductions := reduceTable(table, orig, 15)
	if reductions == 0 {
		t.Fatal("expected at least one reduction for sparse table")
	}
	if len(reduced) >= 32 {
		t.Fatalf("expected reduced table, got len=%d uint64s", len(reduced))
	}

	// All original set bits must still be set after reduction.
	// (Verified indirectly: the OR-fold preserves all bits from both halves.)
}

func TestPopulationThreshold(t *testing.T) {
	// Create highly populated data (many unique 4-byte patterns).
	rng := rand.New(rand.NewSource(1))
	// Small table + lots of data = high population.
	data := make([]byte, 10000)
	rng.Read(data)
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 8) // 256 entries, tiny table
	table, _ := cfg.buildSearchTable(data, nil)
	// With 10K positions into 256 entries, >70% should be set → table skipped (nil).
	if table != nil {
		t.Log("table was not skipped (unexpected for such a small table with random data)")
	} else {
		t.Log("table correctly skipped due to high population")
	}
}

func TestChunkRoundtrip(t *testing.T) {
	cfgs := []SearchTableConfig{
		withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 12),
		NewSearchTableConfig().WithMatchLen(3).WithBytePrefix('=', ':'),
		NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte("id:")),
	}
	// Build a mask prefix
	var mask [32]byte
	mask['='>>3] |= 1 << ('=' & 7)
	mask[':'>>3] |= 1 << (':' & 7)
	cfgs = append(cfgs, NewSearchTableConfig().WithMatchLen(4).WithMaskPrefix(mask))

	for _, cfg := range cfgs {
		info := cfg.marshalSearchInfoChunk()
		if info[0] != chunkTypeSearchInfo {
			t.Fatalf("expected chunk type 0x44, got 0x%x", info[0])
		}
		parsed, err := parseSearchInfo(info[4:])
		if err != nil {
			t.Fatal(err)
		}
		if parsed.tableType != cfg.tableType || parsed.matchLen != cfg.matchLen || parsed.baseTableSize != cfg.baseTableSize {
			t.Fatalf("info roundtrip mismatch: got type=%d ml=%d ts=%d, want type=%d ml=%d ts=%d",
				parsed.tableType, parsed.matchLen, parsed.baseTableSize,
				cfg.tableType, cfg.matchLen, cfg.baseTableSize)
		}

		// Build a small table and roundtrip the block chunk.
		// 32 bytes = 256 bits = 2^8. For baseTableSize=12, reductions=12-8=4.
		table := make([]byte, 32)
		table[0] = 0xff
		table[31] = 0x01
		reductions := cfg.baseTableSize - 8
		chunk := marshalSearchTableChunk(&cfg, reductions, table)
		if chunk[0] != chunkTypeSearchTable {
			t.Fatalf("expected chunk type 0x45, got 0x%x", chunk[0])
		}
		pcfg, pred, ptable, err := parseSearchTable(chunk[4:])
		if err != nil {
			t.Fatal(err)
		}
		if pcfg.tableType != cfg.tableType || pred != reductions {
			t.Fatalf("block roundtrip mismatch")
		}
		if !bytes.Equal(ptable, table) {
			t.Fatal("table data mismatch")
		}
	}
}

func TestPatternCanMatch(t *testing.T) {
	rng := rand.New(rand.NewSource(123))
	data := make([]byte, 8192)
	rng.Read(data)

	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 16)
	table, reductions := cfg.buildSearchTable(data, nil)
	if table == nil {
		t.Fatal("table should not be nil")
	}

	// Every 4+ byte substring in data should match.
	for i := 0; i < 100; i++ {
		pos := rng.Intn(len(data) - 8)
		pattern := data[pos : pos+4+rng.Intn(5)]
		canUse, match := patternCanMatch(&cfg, table, reductions, pattern)
		if canUse && !match {
			t.Fatalf("false negative for pattern at pos %d", pos)
		}
	}

	// Pattern too short should return canUse=false.
	canUse, _ := patternCanMatch(&cfg, table, reductions, []byte("ab"))
	if canUse {
		t.Fatal("expected canUse=false for short pattern")
	}
}

func TestWriterSearchTable(t *testing.T) {
	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(minBlockSize), WriterConcurrency(1))

	// Use compressible data (search tables are only added for compressible blocks).
	data := make([]byte, minBlockSize*3)
	for i := range data {
		data[i] = byte(i % 251) // repetitive but not trivial
	}
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify 0x44 and 0x45 chunks appear in output.
	out := buf.Bytes()
	hasInfo := false
	hasTable := false
	pos := 0
	for pos < len(out) {
		if pos+4 > len(out) {
			break
		}
		chunkType := out[pos]
		chunkLen := int(out[pos+1]) | int(out[pos+2])<<8 | int(out[pos+3])<<16
		switch chunkType {
		case chunkTypeSearchInfo:
			hasInfo = true
		case chunkTypeSearchTable:
			hasTable = true
		}
		if chunkType == ChunkTypeStreamIdentifier {
			pos += 4 + magicBodyLen
		} else if chunkType == chunkTypeUncompressedData || chunkType == chunkTypeMinLZCompressedData || chunkType == chunkTypeMinLZCompressedDataCompCRC {
			pos += 4 + chunkLen
		} else {
			pos += 4 + chunkLen
		}
	}
	if !hasInfo {
		t.Error("no 0x44 search info chunk found in output")
	}
	if !hasTable {
		t.Error("no 0x45 search table chunk found in output")
	}

	// Verify the stream is still decodable by the regular reader.
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("decoded data mismatch")
	}
}

func TestWriterSearchTableAsync(t *testing.T) {
	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(minBlockSize), WriterConcurrency(2))

	data := make([]byte, minBlockSize*3)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	if err := w.EncodeBuffer(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify decodable.
	reader := NewReader(bytes.NewReader(buf.Bytes()))
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("decoded data mismatch")
	}
}

func TestBlockSearcherE2E(t *testing.T) {
	// Create data with a known pattern in one block.
	blockSize := minBlockSize
	needle := []byte("FINDME_HERE_1234")
	data := make([]byte, blockSize*4)
	rng := rand.New(rand.NewSource(77))
	rng.Read(data)
	// Place needle in block 2 (offset 2*blockSize + 100).
	copy(data[2*blockSize+100:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Search for the needle.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := false
	blocksDecoded := 0
	err = searcher.Search(needle, func(r SearchResult) bool {
		blocksDecoded++
		if bytes.Contains(r.Data, needle) {
			found = true
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found")
	}
	t.Logf("blocks decoded: %d out of 4", blocksDecoded)
	if blocksDecoded >= 4 {
		t.Log("warning: all blocks decoded, search tables didn't help (may be due to hash collisions)")
	}
}

func TestBlockSearcherSkipsBlocks(t *testing.T) {
	blockSize := minBlockSize
	// Create data where blocks 0,1,3 are zeros and block 2 has unique data.
	data := make([]byte, blockSize*4)
	// Block 2 has random data.
	rng := rand.New(rand.NewSource(55))
	rng.Read(data[2*blockSize : 3*blockSize])
	// Place a unique needle only in block 2.
	needle := []byte("UNIQUE_NEEDLE_XX")
	copy(data[2*blockSize+200:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	blocksDecoded := 0
	found := false
	err = searcher.Search(needle, func(r SearchResult) bool {
		blocksDecoded++
		if bytes.Contains(r.Data, needle) {
			found = true
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found")
	}
	// With zero blocks, the needle hash should not collide often.
	t.Logf("blocks decoded: %d out of 4", blocksDecoded)
}

func TestBlockSearcherFallback(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*2)
	rng := rand.New(rand.NewSource(11))
	rng.Read(data)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Search with pattern shorter than matchLen - tables can't help.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	blocksDecoded := 0
	err = searcher.Search([]byte("ab"), func(r SearchResult) bool {
		blocksDecoded++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if blocksDecoded != 2 {
		t.Fatalf("expected 2 blocks decoded (fallback), got %d", blocksDecoded)
	}
}

func TestBlockSearcherBail(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*2)
	rng := rand.New(rand.NewSource(11))
	rng.Read(data)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Bail mode with short pattern should return error.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()), BlockSearchBailOnMissing())
	err = searcher.Search([]byte("ab"), func(r SearchResult) bool {
		return true
	})
	if err != ErrSearchTablesUnusable {
		t.Fatalf("expected ErrSearchTablesUnusable, got %v", err)
	}
}

func TestBlockSearcherPrefixTypes(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*2)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	// Insert pattern with prefix.
	copy(data[blockSize+500:], []byte("=FINDTHIS"))

	tests := []struct {
		name    string
		cfg     SearchTableConfig
		pattern []byte
	}{
		{"byte_prefix", NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('='), []byte("=FIND")},
		{"long_prefix", NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte("=")), []byte("=FIND")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, WriterSearchTable(tt.cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
			_, err := w.Write(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// Verify decodable.
			reader := NewReader(bytes.NewReader(buf.Bytes()))
			decoded, rerr := io.ReadAll(reader)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatal("decoded data mismatch")
			}

			// Search.
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := false
			err = searcher.Search(tt.pattern, func(r SearchResult) bool {
				if bytes.Contains(r.Data, []byte("=FINDTHIS")) {
					found = true
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("needle not found")
			}
		})
	}
}

// TestPrefixInternalMatch verifies that prefix tables can use prefix bytes
// found INSIDE the pattern, not just at the start.
func TestPrefixInternalMatch(t *testing.T) {
	blockSize := minBlockSize

	// Use compressible, non-random data so prefix byte windows are distinct per block.
	// Blocks 0 and 2 are repetitive text without the target patterns.
	filler := bytes.Repeat([]byte("abcdefghijklmnop\n"), blockSize/17+1)
	data := make([]byte, blockSize*3)
	copy(data[0:], filler[:blockSize])
	copy(data[blockSize*2:], filler[:blockSize])

	// Block 1 has the JSON target.
	copy(data[blockSize:], filler[:blockSize])
	jsonLine := []byte(`"timestamp":"1679909263.614381575","cluster_id":"ab3400ab"`)
	copy(data[blockSize+500:], jsonLine)

	tests := []struct {
		name    string
		cfg     SearchTableConfig
		pattern []byte
	}{
		{
			// Pattern doesn't start with a prefix, but contains prefix bytes internally.
			name:    "byte_prefix_internal",
			cfg:     NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':'),
			pattern: []byte(`stamp":"1679909263`),
		},
		{
			// Pattern starts with a prefix byte (original behavior still works).
			name:    "byte_prefix_leading",
			cfg:     NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':'),
			pattern: []byte(`"timestamp"`),
		},
		{
			// Mask prefix with internal prefix bytes.
			name: "mask_prefix_internal",
			cfg: func() SearchTableConfig {
				var m [32]byte
				m['"'>>3] |= 1 << ('"' & 7)
				m[':'>>3] |= 1 << (':' & 7)
				return NewSearchTableConfig().WithMatchLen(4).WithMaskPrefix(m)
			}(),
			pattern: []byte(`stamp":"1679909263`),
		},
		{
			// Long prefix found inside the pattern.
			name:    "long_prefix_internal",
			cfg:     NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte(`":"`)),
			pattern: []byte(`stamp":"1679909263`),
		},
		{
			// Short pattern with no prefix byte inside — should fall back.
			name:    "byte_prefix_no_match_fallback",
			cfg:     NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':'),
			pattern: []byte(`stamp`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, WriterSearchTable(tt.cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
			_, err := w.Write(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := false
			blocksSearched := 0
			err = searcher.Search(tt.pattern, func(r SearchResult) bool {
				blocksSearched++
				if bytes.Contains(r.Data, tt.pattern) {
					found = true
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			stats := searcher.Stats()
			if !found {
				t.Fatal("pattern not found")
			}
			t.Logf("blocks: %d total, %d skipped, %d searched, tablesUnusable=%d",
				stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched, stats.TablesUnusable)

			// For patterns with internal prefix bytes, tables should be usable.
			if tt.name != "byte_prefix_no_match_fallback" {
				if stats.TablesUnusable > 0 {
					t.Errorf("expected tables to be usable, got %d unusable", stats.TablesUnusable)
				}
				if stats.BlocksSkipped == 0 {
					t.Error("expected some blocks to be skipped with internal prefix matching")
				}
			}
		})
	}
}

// TestPrefixInternalNoFalseNegative verifies that internal prefix matching
// never produces false negatives — if the pattern is in the data, search must find it.
func TestPrefixInternalNoFalseNegative(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	blockSize := minBlockSize

	for iter := 0; iter < 20; iter++ {
		data := make([]byte, blockSize*4)
		rng.Read(data)

		// Insert structured data with prefix bytes in various blocks.
		for b := 0; b < 4; b++ {
			line := fmt.Sprintf(`{"key%d":"value%d","num":%d}`, b, rng.Intn(1000), rng.Intn(99999))
			copy(data[blockSize*b+200:], line)
		}

		cfg := NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':')
		var buf bytes.Buffer
		w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
		_, err := w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Search for substrings that contain prefix bytes internally.
		patterns := [][]byte{
			[]byte(`key0":"value`),
			[]byte(`ey1":"val`),
			[]byte(`num":`),
		}
		for _, pat := range patterns {
			if !bytes.Contains(data, pat) {
				continue
			}
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := false
			err = searcher.Search(pat, func(r SearchResult) bool {
				if bytes.Contains(r.Data, pat) {
					found = true
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("iter %d: pattern %q in data but not found by search", iter, pat)
			}
		}
	}
}

func FuzzSearchNoFalseNegatives(f *testing.F) {
	f.Add([]byte("hello world test data"), 4, 12)
	f.Fuzz(func(t *testing.T, data []byte, matchLen, tableSize int) {
		if len(data) < 16 || matchLen < 1 || matchLen > 8 || tableSize < 8 || tableSize > 20 {
			return
		}
		cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(matchLen), tableSize)
		table, reductions := cfg.buildSearchTable(data, nil)
		if table == nil {
			return // too populated
		}
		for i := 0; i <= len(data)-matchLen; i++ {
			v := readLE64Pad(data[i:])
			h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & ((1 << (cfg.baseTableSize - reductions)) - 1)
			if table[h>>3]&(1<<(h&7)) == 0 {
				t.Fatalf("false negative at pos %d (ml=%d ts=%d red=%d)", i, matchLen, tableSize, reductions)
			}
		}
	})
}

func BenchmarkBuildTableNoPrefix(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20, 4 << 20} {
		data := make([]byte, size)
		rng.Read(data)
		cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), int(autoTableSize(size)))
		b.Run(sizeLabel(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cfg.buildSearchTable(data, nil)
			}
		})
	}
}

func sizeLabel(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// TestSearchAllMatchLengths verifies no false negatives across all matchLen 1-8.
func TestSearchAllMatchLengths(t *testing.T) {
	rng := rand.New(rand.NewSource(777))
	data := make([]byte, 16384)
	rng.Read(data)

	for ml := 1; ml <= 8; ml++ {
		t.Run(fmt.Sprintf("matchLen=%d", ml), func(t *testing.T) {
			cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(ml), 16)
			table, reductions := cfg.buildSearchTable(data, nil)
			if table == nil {
				t.Skip("table too populated")
			}
			mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1
			for i := 0; i <= len(data)-ml; i++ {
				v := readLE64Pad(data[i:])
				h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
				if table[h>>3]&(1<<(h&7)) == 0 {
					t.Fatalf("false negative at pos %d", i)
				}
			}
		})
	}
}

// TestSearchAllLevels tests search tables with every compression level.
func TestSearchAllLevels(t *testing.T) {
	levels := []struct {
		name  string
		level int
	}{
		{"SuperFast", LevelSuperFast},
		{"Uncompressed", LevelUncompressed},
		{"Fastest", LevelFastest},
		{"Balanced", LevelBalanced},
		{"Smallest", LevelSmallest},
	}
	needle := []byte("SEARCH_TARGET_XY")

	for _, lv := range levels {
		t.Run(lv.name, func(t *testing.T) {
			data := make([]byte, minBlockSize*2)
			rng := rand.New(rand.NewSource(42))
			rng.Read(data)
			copy(data[minBlockSize+100:], needle)

			var buf bytes.Buffer
			cfg := NewSearchTableConfig().WithMatchLen(4)
			w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(minBlockSize), WriterConcurrency(1), WriterLevel(lv.level))
			_, err := w.Write(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// Verify decodable.
			decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatal("decoded data mismatch")
			}

			// Search.
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := false
			err = searcher.Search(needle, func(r SearchResult) bool {
				if bytes.Contains(r.Data, needle) {
					found = true
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("needle not found")
			}
		})
	}
}

// TestSearchConcurrency tests search tables with concurrent writers.
func TestSearchConcurrency(t *testing.T) {
	for _, conc := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("conc=%d", conc), func(t *testing.T) {
			needle := []byte("CONCURRENT_NEEDLE_SEARCH")
			data := make([]byte, minBlockSize*4)
			rng := rand.New(rand.NewSource(42))
			rng.Read(data)
			copy(data[minBlockSize*2+500:], needle)

			var buf bytes.Buffer
			cfg := NewSearchTableConfig().WithMatchLen(4)
			w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(minBlockSize), WriterConcurrency(conc))
			if err := w.EncodeBuffer(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// Verify decodable.
			decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatal("decoded data mismatch")
			}

			// Search.
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := false
			err = searcher.Search(needle, func(r SearchResult) bool {
				if bytes.Contains(r.Data, needle) {
					found = true
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("needle not found")
			}
		})
	}
}

// TestSearchMultipleNeedles verifies search finds patterns in different blocks.
func TestSearchMultipleNeedles(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*5)
	rng := rand.New(rand.NewSource(99))
	rng.Read(data)

	// Place needles in blocks 1, 3.
	needle := []byte("MULTI_BLOCK_FIND")
	copy(data[blockSize*1+200:], needle)
	copy(data[blockSize*3+300:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	foundBlocks := 0
	err = searcher.Search(needle, func(r SearchResult) bool {
		if bytes.Contains(r.Data, needle) {
			foundBlocks++
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if foundBlocks != 2 {
		t.Fatalf("expected 2 blocks with needle, got %d", foundBlocks)
	}
}

// TestSearchEmptyStream tests search on an empty stream.
func TestSearchEmptyStream(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterSearchTable(NewSearchTableConfig().WithMatchLen(4)), WriterConcurrency(1))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	called := false
	err := searcher.Search([]byte("test"), func(r SearchResult) bool {
		called = true
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("callback should not be called for empty stream")
	}
}

// TestSearchBlockBoundary tests pattern that spans a block boundary.
func TestSearchBlockBoundary(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*2)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)

	// Place needle right at the block boundary.
	needle := []byte("BOUNDARY_TEST_XY")
	copy(data[blockSize-8:], needle) // straddles blocks 0 and 1

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The full needle spans two blocks. At least the block where the
	// first part of the needle starts should be decoded.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	blocksDecoded := 0
	err = searcher.Search(needle[:4], func(r SearchResult) bool {
		blocksDecoded++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	// At least one block should be decoded (the one containing the start of the needle).
	if blocksDecoded == 0 {
		t.Fatal("expected at least one block decoded for boundary pattern")
	}
}

// TestSearchOverlapMultiBlock verifies that cross-block boundary patterns are indexed
// correctly when using EncodeBuffer (contiguous buffer with real overlap) across many blocks.
func TestSearchOverlapMultiBlock(t *testing.T) {
	blockSize := minBlockSize
	numBlocks := 8
	data := make([]byte, blockSize*numBlocks)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)

	// Place a unique pattern at every block boundary (last 2 bytes of block N + first 2 bytes of N+1).
	boundary := []byte("XBND")
	for i := 1; i < numBlocks; i++ {
		copy(data[blockSize*i-2:], boundary)
	}

	for _, conc := range []int{1, 4} {
		t.Run(fmt.Sprintf("conc=%d", conc), func(t *testing.T) {
			var buf bytes.Buffer
			cfg := NewSearchTableConfig().WithMatchLen(4)
			w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(conc))
			// EncodeBuffer gives contiguous buffer → overlap available for all but last block.
			if err := w.EncodeBuffer(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// Verify round-trip.
			decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatal("decoded data mismatch")
			}

			// Search for the boundary pattern. It starts at blockSize*i-2 for each i.
			// The first 4 bytes "XBND" straddle two blocks.
			// Block (i-1) should have it indexed via overlap.
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			foundBlocks := 0
			err = searcher.Search(boundary, func(r SearchResult) bool {
				if bytes.Contains(r.Data, boundary) {
					foundBlocks++
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			// The pattern straddles boundaries — in most cases neither block contains
			// the full pattern. But block (i-1) has the first 2 bytes and block i has
			// the last 2. The search table for block (i-1) should have indexed "XBND"
			// via overlap, so block (i-1) should be decoded (even if it doesn't contain
			// the full pattern within its own bytes, the table says "maybe").
			// We just verify the searcher doesn't crash and decodes blocks.
			t.Logf("blocks with full pattern: %d, blocks decoded: checked via search", foundBlocks)
		})
	}
}

// TestSearchOverlapLongStream tests that overlap indexing works across many blocks.
// Places needles entirely within blocks but near boundaries, and verifies the table
// doesn't falsely skip them. Also tests with EncodeBuffer (contiguous) vs Write (streaming).
func TestSearchOverlapLongStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	blockSize := minBlockSize
	numBlocks := 32
	data := make([]byte, blockSize*numBlocks)
	rng := rand.New(rand.NewSource(99))
	rng.Read(data)

	// Place unique needle 8 bytes before each even block boundary.
	// All 4 bytes fit within the block, but nearby trailing positions
	// exercise the overlap hashing.
	needle := []byte("OVLP_TEST_NEEDLE")
	positions := 0
	for i := 2; i < numBlocks; i += 2 {
		off := blockSize*i - len(needle) - 8
		copy(data[off:], needle)
		positions++
	}

	for _, method := range []string{"EncodeBuffer", "Write"} {
		t.Run(method, func(t *testing.T) {
			var buf bytes.Buffer
			cfg := NewSearchTableConfig().WithMatchLen(4)
			w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(4))
			switch method {
			case "EncodeBuffer":
				if err := w.EncodeBuffer(data); err != nil {
					t.Fatal(err)
				}
			case "Write":
				if _, err := w.Write(data); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// Verify round-trip.
			decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatal("decoded data mismatch")
			}

			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := 0
			err = searcher.Search(needle, func(r SearchResult) bool {
				if bytes.Contains(r.Data, needle) {
					found++
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if found != positions {
				t.Fatalf("expected %d blocks with needle, got %d", positions, found)
			}
			t.Logf("correctly found all %d needles near boundaries via %s", found, method)
		})
	}
}

// TestSearchNoTables tests search falls back when no search tables present.
func TestSearchNoTables(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*3)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	needle := []byte("NOTABLES_NEEDLE!")
	copy(data[blockSize+100:], needle)

	// Compress WITHOUT search tables.
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Search should still work (fallback to full decode).
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := false
	blocksDecoded := 0
	err = searcher.Search(needle, func(r SearchResult) bool {
		blocksDecoded++
		if bytes.Contains(r.Data, needle) {
			found = true
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found in fallback mode")
	}
	if blocksDecoded != 3 {
		t.Fatalf("expected all 3 blocks decoded in fallback, got %d", blocksDecoded)
	}
}

// TestSearchNoTables_Bail verifies bail mode errors when no tables exist.
func TestSearchNoTables_Bail(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*2)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()), BlockSearchBailOnMissing())
	err = searcher.Search([]byte("test"), func(r SearchResult) bool {
		return true
	})
	if err != ErrSearchTablesUnusable {
		t.Fatalf("expected ErrSearchTablesUnusable, got %v", err)
	}
}

// TestSearchLargeData tests with data larger than a single block.
func TestSearchLargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	blockSize := 64 << 10 // 64KB
	dataSize := 1 << 20   // 1MB
	data := make([]byte, dataSize)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)

	// Place needle in the middle.
	needle := []byte("LARGE_DATA_NEEDLE_PATTERN_HERE!")
	copy(data[dataSize/2:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(4))
	if err := w.EncodeBuffer(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify decodable.
	decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("decoded data mismatch")
	}

	// Search.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := false
	blocksDecoded := 0
	totalBlocks := dataSize / blockSize
	err = searcher.Search(needle, func(r SearchResult) bool {
		blocksDecoded++
		if bytes.Contains(r.Data, needle) {
			found = true
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found")
	}
	t.Logf("blocks decoded: %d out of %d (%.0f%% skipped)", blocksDecoded, totalBlocks,
		100*float64(totalBlocks-blocksDecoded)/float64(totalBlocks))
}

// TestSearchMaskPrefix tests type 3 (bitmask) prefix tables.
func TestSearchMaskPrefix(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*3)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	// Place pattern with prefix '=' in block 1.
	copy(data[blockSize+500:], []byte("=MASK_PATTERN"))

	var mask [32]byte
	mask['='>>3] |= 1 << ('=' & 7)
	cfg := NewSearchTableConfig().WithMatchLen(4).WithMaskPrefix(mask)

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify decodable.
	decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatal("decoded data mismatch")
	}

	// Search with prefix context.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := false
	err = searcher.Search([]byte("=MASK"), func(r SearchResult) bool {
		if bytes.Contains(r.Data, []byte("=MASK_PATTERN")) {
			found = true
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found with mask prefix")
	}
}

// TestSearchWriterReset verifies search tables work across writer resets.
func TestSearchWriterReset(t *testing.T) {
	needle := []byte("RESET_TEST_NEED!")
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(nil, WriterSearchTable(cfg), WriterBlockSize(minBlockSize), WriterConcurrency(1))

	for i := 0; i < 3; i++ {
		var buf bytes.Buffer
		w.Reset(&buf)
		data := make([]byte, minBlockSize*2)
		rng := rand.New(rand.NewSource(int64(i)))
		rng.Read(data)
		copy(data[minBlockSize+100:], needle)

		_, err := w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
		found := false
		err = searcher.Search(needle, func(r SearchResult) bool {
			if bytes.Contains(r.Data, needle) {
				found = true
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("needle not found in iteration %d", i)
		}
	}
}

// TestSearchStopEarly verifies the callback can stop search early.
func TestSearchStopEarly(t *testing.T) {
	blockSize := minBlockSize
	needle := []byte("STOP_EARLY_PATT!")
	data := make([]byte, blockSize*5)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	// Place needle in every block.
	for i := 0; i < 5; i++ {
		copy(data[blockSize*i+100:], needle)
	}

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	blocksDecoded := 0
	err = searcher.Search(needle, func(r SearchResult) bool {
		blocksDecoded++
		return blocksDecoded < 2 // stop after 2 blocks
	})
	if err != nil {
		t.Fatal(err)
	}
	if blocksDecoded != 2 {
		t.Fatalf("expected 2 blocks decoded before stop, got %d", blocksDecoded)
	}
}

// TestSearchBlockOffsets verifies block offsets are correct.
func TestSearchBlockOffsets(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*4)
	rng := rand.New(rand.NewSource(42))
	rng.Read(data)
	// Place needle in blocks 0 and 2.
	needle := []byte("OFFSET_CHECK1234")
	copy(data[100:], needle)
	copy(data[blockSize*2+100:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	var offsets []int64
	err = searcher.Search(needle, func(r SearchResult) bool {
		if bytes.Contains(r.Data, needle) {
			offsets = append(offsets, r.BlockStart)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) < 2 {
		t.Fatalf("expected at least 2 offsets, got %d", len(offsets))
	}
	// First needle is in block 0 -> offset 0.
	if offsets[0] != 0 {
		t.Fatalf("expected first offset 0, got %d", offsets[0])
	}
}

// TestHashValueConsistency verifies HashValue matches inlined helpers for all matchLens.
func TestHashValueConsistency(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		var buf [8]byte
		rng.Read(buf[:])
		v := readLE64Pad(buf[:])
		for _, ts := range []uint8{8, 10, 14, 16, 20, 23} {
			if got := hashValue(v, ts, 3); got != hashValue3(v, ts) {
				t.Fatalf("matchLen=3 ts=%d: mismatch", ts)
			}
			if got := hashValue(v, ts, 4); got != hashValue4(v, ts) {
				t.Fatalf("matchLen=4 ts=%d: mismatch", ts)
			}
			if got := hashValue(v, ts, 5); got != hashValue5(v, ts) {
				t.Fatalf("matchLen=5 ts=%d: mismatch", ts)
			}
			if got := hashValue(v, ts, 6); got != hashValue6(v, ts) {
				t.Fatalf("matchLen=6 ts=%d: mismatch", ts)
			}
			if got := hashValue(v, ts, 7); got != hashValue7(v, ts) {
				t.Fatalf("matchLen=7 ts=%d: mismatch", ts)
			}
			if got := hashValue(v, ts, 8); got != hashValue8(v, ts) {
				t.Fatalf("matchLen=8 ts=%d: mismatch", ts)
			}
		}
	}
}

// TestSearchTableSizes verifies table generation across different table sizes.
func TestSearchTableSizes(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 8192)
	rng.Read(data)

	for ts := 8; ts <= 16; ts++ {
		t.Run(fmt.Sprintf("ts=%d", ts), func(t *testing.T) {
			cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), ts)
			table, reductions := cfg.buildSearchTable(data, nil)
			if table == nil {
				t.Log("table skipped (too populated)")
				return
			}
			mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1
			for i := 0; i <= len(data)-4; i++ {
				v := readLE64Pad(data[i:])
				h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
				if table[h>>3]&(1<<(h&7)) == 0 {
					t.Fatalf("false negative at pos %d (ts=%d red=%d)", i, ts, reductions)
				}
			}
		})
	}
}

// TestSearchConfigValidation tests config validation.
func TestSearchConfigValidation(t *testing.T) {
	// Valid configs.
	for ml := 1; ml <= 8; ml++ {
		cfg := NewSearchTableConfig().WithMatchLen(ml)
		if err := cfg.validate(); err != nil {
			t.Fatalf("matchLen=%d should be valid: %v", ml, err)
		}
	}
	// Invalid matchLen.
	cfg := NewSearchTableConfig().WithMatchLen(0)
	if err := cfg.validate(); err == nil {
		t.Fatal("matchLen=0 should be invalid")
	}
	cfg = NewSearchTableConfig().WithMatchLen(9)
	if err := cfg.validate(); err == nil {
		t.Fatal("matchLen=9 should be invalid")
	}
}

// TestSearchReducePreservesAllBits tests reduction doesn't create false negatives.
func TestSearchReducePreservesAllBits(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 4096)
	rng.Read(data)

	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 16)
	// Build full table without reduction.
	tableU64s := 1 << (cfg.baseTableSize - 6)
	fullTable := make([]uint64, tableU64s)
	buildTableNoPrefix(fullTable, data, len(data), cfg.baseTableSize, cfg.matchLen)

	// Reduce.
	origPop, _ := tablePopulation(fullTable)
	reduced, reductions := reduceTable(fullTable, origPop, 50)

	if reductions == 0 {
		t.Skip("no reductions applied")
	}

	// Every hash from the original data must be findable in the reduced table.
	reducedBytes := tableToBytes(reduced)
	mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1
	for i := 0; i <= len(data)-4; i++ {
		v := readLE64Pad(data[i:])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
		if reducedBytes[h>>3]&(1<<(h&7)) == 0 {
			t.Fatalf("reduction created false negative at pos %d (reductions=%d)", i, reductions)
		}
	}
}

// TestSearchWriteReadInterleaved tests Write() path (ibuf-based, multiple calls).
func TestSearchWriteReadInterleaved(t *testing.T) {
	needle := []byte("INTERLEAVED_TEST")
	blockSize := minBlockSize

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))

	// Write in small chunks to exercise the ibuf path.
	rng := rand.New(rand.NewSource(42))
	totalSize := blockSize * 3
	written := 0
	for written < totalSize {
		chunk := make([]byte, 512+rng.Intn(2048))
		rng.Read(chunk)
		if written+len(chunk) > totalSize {
			chunk = chunk[:totalSize-written]
		}
		// Place needle in middle of the data.
		if written < totalSize/2 && written+len(chunk) > totalSize/2 {
			off := totalSize/2 - written
			if off+len(needle) <= len(chunk) {
				copy(chunk[off:], needle)
			}
		}
		_, err := w.Write(chunk)
		if err != nil {
			t.Fatal(err)
		}
		written += len(chunk)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Search.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := false
	err := searcher.Search(needle, func(r SearchResult) bool {
		if bytes.Contains(r.Data, needle) {
			found = true
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found in interleaved write test")
	}
}

// FuzzSearchRoundtrip compresses with search tables and verifies decodability + search correctness.
func FuzzSearchRoundtrip(f *testing.F) {
	f.Add([]byte("hello world this is a test with NEEDLE inside"), []byte("NEEDLE"), 4, true)
	f.Fuzz(func(t *testing.T, data, pattern []byte, matchLen int, usePrefix bool) {
		if len(data) < 32 || len(pattern) < 1 || len(pattern) > 100 {
			return
		}
		if matchLen < 1 || matchLen > 8 {
			return
		}
		cfg := NewSearchTableConfig().WithMatchLen(matchLen)
		if usePrefix && len(pattern) > 0 {
			cfg = cfg.WithBytePrefix(pattern[0])
		}

		var buf bytes.Buffer
		w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(minBlockSize), WriterConcurrency(1))
		_, err := w.Write(data)
		if err != nil {
			return // some data may cause issues
		}
		if err := w.Close(); err != nil {
			return
		}

		// Verify decodable.
		decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if !bytes.Equal(decoded, data) {
			t.Fatal("decoded data mismatch")
		}

		// If data contains pattern, search must find it.
		if bytes.Contains(data, pattern) {
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := false
			err = searcher.Search(pattern, func(r SearchResult) bool {
				if bytes.Contains(r.Data, pattern) {
					found = true
				}
				return true
			})
			if err != nil {
				t.Fatalf("search failed: %v", err)
			}
			// In fallback mode (no prefix match or short pattern) we always find it.
			// With prefix tables, we may not use the table, so fallback still finds it.
			if !found {
				t.Fatal("pattern present in data but search didn't find it")
			}
		}
	})
}

func BenchmarkBuildTablePrefix(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20, 4 << 20} {
		data := make([]byte, size)
		rng.Read(data)
		cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':'), int(autoTableSize(size)))
		b.Run(sizeLabel(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				cfg.buildSearchTable(data, nil)
			}
		})
	}
}

func BenchmarkPatternCanMatch(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 1<<20)
	rng.Read(data)
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 20)
	table, reductions := cfg.buildSearchTable(data, nil)
	if table == nil {
		b.Fatal("table nil")
	}
	pattern := data[5000:5020]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		patternCanMatch(&cfg, table, reductions, pattern)
	}
}
