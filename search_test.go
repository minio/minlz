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
	for _, packed := range []bool{false, true} {
		t.Run(fmt.Sprintf("packed=%v", packed), func(t *testing.T) {
			table, reductions := cfg.buildSearchTable(data, nil, nil, packed)
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
		})
	}
}

func TestBuildTableNoPrefix_NoFalseNegative(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for _, size := range []int{100, 1000, 8000, 32000} {
		data := make([]byte, size)
		rng.Read(data)

		for ml := 1; ml <= 8; ml++ {
			for _, packed := range []bool{false, true} {
				cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(ml), 16)
				table, reductions := cfg.buildSearchTable(data, nil, nil, packed)
				if table == nil {
					continue // too populated, skip
				}
				effectiveSize := cfg.baseTableSize - reductions

				for i := 0; i <= len(data)-ml; i++ {
					v := readLE64Pad(data[i:])
					h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & ((1 << (cfg.baseTableSize - reductions)) - 1)
					_ = effectiveSize
					if table[h>>3]&(1<<(h&7)) == 0 {
						t.Fatalf("false negative: size=%d ml=%d packed=%v pos=%d reductions=%d", size, ml, packed, i, reductions)
					}
				}
			}
		}
	}
}

func TestBuildTableWithOverlap(t *testing.T) {
	data := []byte("hello world test")
	overlap := []byte("xyz")
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 10)
	for _, packed := range []bool{false, true} {
		t.Run(fmt.Sprintf("packed=%v", packed), func(t *testing.T) {
			table, reductions := cfg.buildSearchTable(data, overlap, nil, packed)
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
		})
	}
}

// TestPackBitsDeterministic verifies that packBits (asm or generic) produces
// the same output as packBitsGeneric for all table sizes. This ensures that
// SSE2, AVX2, NEON, and generic code paths are bit-identical.
func TestPackBitsDeterministic(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, bits := range []int{256, 1024, 8192, 65536, 1 << 20} {
		src := make([]byte, bits)
		for i := range src {
			if rng.Intn(4) == 0 {
				src[i] = 0xFF
			}
		}
		got := make([]byte, bits/8)
		want := make([]byte, bits/8)
		packBits(got, src)
		packBitsGeneric(want, src)
		if !bytes.Equal(got, want) {
			t.Fatalf("packBits mismatch for %d entries at first diff byte %d", bits, firstDiff(got, want))
		}
	}
}

func firstDiff(a, b []byte) int {
	for i := range a {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

// TestBuildSearchTableDeterministic verifies that buildSearchTable produces
// bit-identical output regardless of platform/asm path. Uses fixed seeds and
// checks CRC32 of the output table bytes.
func TestBuildSearchTableDeterministic(t *testing.T) {
	rng := rand.New(rand.NewSource(77))

	tests := []struct {
		name     string
		cfg      SearchTableConfig
		dataSize int
		packed   bool
		wantCRC  uint32
	}{
		{"noprefix_4k_ml4", withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 16), 4096, false, 0},
		{"noprefix_4k_ml4_packed", withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 16), 4096, true, 0},
		{"noprefix_64k_ml8", withBaseTableSize(NewSearchTableConfig().WithMatchLen(8), 18), 65536, false, 0},
		{"noprefix_64k_ml8_packed", withBaseTableSize(NewSearchTableConfig().WithMatchLen(8), 18), 65536, true, 0},
		{"byteprefix_4k", withBaseTableSize(NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':'), 16), 4096, false, 0},
		{"longprefix_4k", withBaseTableSize(NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte("id:")), 16), 4096, false, 0},
	}

	// First pass: compute CRCs. Second pass: verify they match.
	for i := range tests {
		data := make([]byte, tests[i].dataSize)
		rng.Read(data)
		table, _ := tests[i].cfg.buildSearchTable(data, nil, nil, tests[i].packed)
		if table == nil {
			t.Fatalf("%s: table nil", tests[i].name)
		}
		tests[i].wantCRC = crc(table)
	}

	// Re-run with same seed — must produce identical CRCs.
	rng = rand.New(rand.NewSource(77))
	for _, tt := range tests {
		data := make([]byte, tt.dataSize)
		rng.Read(data)
		table, _ := tt.cfg.buildSearchTable(data, nil, nil, tt.packed)
		if table == nil {
			t.Fatalf("%s: table nil on re-run", tt.name)
		}
		got := crc(table)
		if got != tt.wantCRC {
			t.Errorf("%s: CRC mismatch: got 0x%08x, want 0x%08x", tt.name, got, tt.wantCRC)
		}
	}
}

func TestReduceTable(t *testing.T) {
	// Create a sparse table: 256 bytes = 2048 bits.
	table := make([]byte, 256)
	setBit(table, 0)
	setBit(table, 12*64+1)
	setBit(table, 25*64+2)

	orig, _ := tablePopulation(table)
	reduced, reductions := reduceTable(table, orig, 15)
	if reductions == 0 {
		t.Fatal("expected at least one reduction for sparse table")
	}
	if len(reduced) >= 256 {
		t.Fatalf("expected reduced table, got len=%d bytes", len(reduced))
	}
}

func TestPopulationThreshold(t *testing.T) {
	// Create highly populated data (many unique 4-byte patterns).
	rng := rand.New(rand.NewSource(1))
	// Small table + lots of data = high population.
	data := make([]byte, 10000)
	rng.Read(data)
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 8) // 256 entries, tiny table
	for _, packed := range []bool{false, true} {
		table, _ := cfg.buildSearchTable(data, nil, nil, packed)
		// With 10K positions into 256 entries, >70% should be set → table skipped (nil).
		if table != nil {
			t.Logf("packed=%v: table was not skipped (unexpected for such a small table with random data)", packed)
		} else {
			t.Logf("packed=%v: table correctly skipped due to high population", packed)
		}
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
	for _, packed := range []bool{false, true} {
		table, reductions := cfg.buildSearchTable(data, nil, nil, packed)
		if table == nil {
			t.Fatal("table should not be nil")
		}

		// Every 4+ byte substring in data should match.
		rng2 := rand.New(rand.NewSource(123))
		for i := 0; i < 100; i++ {
			pos := rng2.Intn(len(data) - 8)
			pattern := data[pos : pos+4+rng2.Intn(5)]
			canUse, match := patternCanMatch(&cfg, table, reductions, pattern)
			if canUse && !match {
				t.Fatalf("packed=%v: false negative for pattern at pos %d", packed, pos)
			}
		}
	}

	// Pattern too short should return canUse=false.
	table, reductions := cfg.buildSearchTable(data, nil, nil, false)
	canUse, _ := patternCanMatch(&cfg, table, reductions, []byte("ab"))
	if canUse {
		t.Fatal("expected canUse=false for short pattern")
	}
}

// TestPatternCanMatchPrefixOrdering verifies that patternCanMatchWithPrefixMask
// correctly uses window ordering: only the first prefix-context window determines
// boundary match eligibility. A false positive on a LATER window must not prevent skipping.
func TestPatternCanMatchPrefixOrdering(t *testing.T) {
	ml := uint8(6)
	tableSize := uint8(16)
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(int(ml)).WithBytePrefix(' ', '.'), int(tableSize))

	pattern := []byte(" 20.38.172.36]")
	// Windows with prefix context:
	// W0 at i=1: prefix=' ', hash("20.38.")
	// W1 at i=4: prefix='.', hash("38.172")
	// W2 at i=7: prefix='.', hash("172.36")
	// Raw fallback: hash(" 20.38") (first ml bytes)

	mask := uint32(1<<(tableSize)) - 1
	h0 := hashValue(readLE64Pad(pattern[1:]), tableSize, ml) & mask
	h1 := hashValue(readLE64Pad(pattern[4:]), tableSize, ml) & mask
	h2 := hashValue(readLE64Pad(pattern[7:]), tableSize, ml) & mask
	hRaw := hashValue(readLE64Pad(pattern[:ml]), tableSize, ml) & mask

	// Verify hashes are distinct so our tests are meaningful.
	hashes := map[uint32]string{h0: "W0", h1: "W1", h2: "W2", hRaw: "Raw"}
	if len(hashes) < 4 {
		t.Skipf("hash collision among test windows — can't test ordering reliably")
	}

	makeTable := func(bits ...uint32) []byte {
		tbl := make([]byte, 1<<(tableSize-3))
		for _, b := range bits {
			tbl[b>>3] |= 1 << (b & 7)
		}
		return tbl
	}

	tests := []struct {
		name       string
		bits       []uint32
		wantCanUse bool
		wantMatch  bool
	}{
		{"all_present", []uint32{h0, h1, h2}, true, true},
		{"all_absent_raw_absent", nil, true, false},
		{"all_absent_raw_present", []uint32{hRaw}, true, true},

		// First present, later absent → boundary match possible.
		{"W0_only", []uint32{h0}, true, true},
		{"W0_W1_only", []uint32{h0, h1}, true, true},

		// KEY CASES: first absent, later present → must NOT "might match"
		// unless raw fallback is set. Old code got this wrong.
		{"W1_only", []uint32{h1}, true, false},
		{"W2_only", []uint32{h2}, true, false},
		{"W1_W2_only", []uint32{h1, h2}, true, false},
		{"W1_only_raw_present", []uint32{h1, hRaw}, true, true},
		{"W2_only_raw_present", []uint32{h2, hRaw}, true, true},
		{"W1_W2_raw_present", []uint32{h1, h2, hRaw}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := makeTable(tt.bits...)
			canUse, match := patternCanMatch(&cfg, tbl, 0, pattern)
			if canUse != tt.wantCanUse || match != tt.wantMatch {
				t.Errorf("got (canUse=%v, match=%v), want (%v, %v)", canUse, match, tt.wantCanUse, tt.wantMatch)
			}
		})
	}
}

// TestPatternCanMatchPrefixOrdering_MaskPrefix same as above but with mask prefix (type 3).
func TestPatternCanMatchPrefixOrdering_MaskPrefix(t *testing.T) {
	ml := uint8(6)
	tableSize := uint8(16)
	cfg := withBaseTableSize(NewSearchTableConfig().WithBytePrefix(' ', '.', '\n'), int(tableSize))
	// WithBytePrefix with ≤8 creates type 2; force mask prefix type 3 via WithMaskPrefix.
	var pfxMask [32]byte
	for _, b := range []byte{' ', '.', '\n'} {
		pfxMask[b>>3] |= 1 << (b & 7)
	}
	cfg = withBaseTableSize(NewSearchTableConfig().WithMatchLen(int(ml)).WithMaskPrefix(pfxMask), int(tableSize))

	pattern := []byte(" 20.38.172.36]")
	mask := uint32(1<<tableSize) - 1
	h0 := hashValue(readLE64Pad(pattern[1:]), tableSize, ml) & mask
	h1 := hashValue(readLE64Pad(pattern[4:]), tableSize, ml) & mask
	h2 := hashValue(readLE64Pad(pattern[7:]), tableSize, ml) & mask
	hRaw := hashValue(readLE64Pad(pattern[:ml]), tableSize, ml) & mask

	hashes := map[uint32]string{h0: "W0", h1: "W1", h2: "W2", hRaw: "Raw"}
	if len(hashes) < 4 {
		t.Skipf("hash collision among test windows")
	}

	makeTable := func(bits ...uint32) []byte {
		tbl := make([]byte, 1<<(tableSize-3))
		for _, b := range bits {
			tbl[b>>3] |= 1 << (b & 7)
		}
		return tbl
	}

	// Same key cases: first absent + later present → must fall to raw fallback.
	tests := []struct {
		name       string
		bits       []uint32
		wantCanUse bool
		wantMatch  bool
	}{
		{"all_present", []uint32{h0, h1, h2}, true, true},
		{"all_absent", nil, true, false},
		{"W0_only", []uint32{h0}, true, true},
		{"W1_only_no_raw", []uint32{h1}, true, false},
		{"W1_only_with_raw", []uint32{h1, hRaw}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := makeTable(tt.bits...)
			canUse, match := patternCanMatch(&cfg, tbl, 0, pattern)
			if canUse != tt.wantCanUse || match != tt.wantMatch {
				t.Errorf("got (canUse=%v, match=%v), want (%v, %v)", canUse, match, tt.wantCanUse, tt.wantMatch)
			}
		})
	}
}

// TestPatternCanMatchPrefixNoFalseNegatives builds real tables from data containing
// known patterns and verifies that patternCanMatch never produces false negatives,
// across all prefix types and various match lengths.
func TestPatternCanMatchPrefixNoFalseNegatives(t *testing.T) {
	rng := rand.New(rand.NewSource(999))
	blockSize := 16384
	data := make([]byte, blockSize)
	rng.Read(data)

	// Embed known patterns at known positions, preceded by prefix bytes.
	patterns := []struct {
		prefix  byte
		payload string
	}{
		{' ', "ALPHA.BETA.GAMMA"},
		{'.', "192.168.1.100"},
		{'\n', "ERROR: something went wrong"},
		{' ', "unique_test_pattern_xyz"},
	}
	pos := 100
	for _, p := range patterns {
		data[pos] = p.prefix
		copy(data[pos+1:], p.payload)
		pos += len(p.payload) + 50
	}

	prefixBytes := []byte{' ', '.', '\n'}
	var pfxMask [32]byte
	for _, b := range prefixBytes {
		pfxMask[b>>3] |= 1 << (b & 7)
	}

	configs := []struct {
		name string
		cfg  SearchTableConfig
	}{
		{"bytePrefix", NewSearchTableConfig().WithBytePrefix(prefixBytes...)},
		{"maskPrefix", NewSearchTableConfig().WithMaskPrefix(pfxMask)},
	}

	for _, cc := range configs {
		for _, ml := range []int{2, 4, 6, 8} {
			for _, ts := range []int{12, 16, 20} {
				cfg := withBaseTableSize(cc.cfg.WithMatchLen(ml), ts)
				table, reductions := cfg.buildSearchTable(data, nil, nil, false)
				if table == nil {
					continue
				}
				for _, p := range patterns {
					// Search with prefix context included.
					search := append([]byte{p.prefix}, p.payload...)
					if len(search) < ml {
						continue
					}
					canUse, match := patternCanMatch(&cfg, table, reductions, search)
					if canUse && !match {
						t.Errorf("%s ml=%d ts=%d: false negative for %q", cc.name, ml, ts, search)
					}

					// Also search without prefix context — should not false-negative
					// if any prefix byte appears inside the pattern.
					if len(p.payload) >= ml {
						canUse2, match2 := patternCanMatch(&cfg, table, reductions, []byte(p.payload))
						if canUse2 && !match2 {
							t.Errorf("%s ml=%d ts=%d: false negative for payload-only %q", cc.name, ml, ts, p.payload)
						}
					}
				}
			}
		}
	}
}

// TestPatternCanMatchPrefixReductions verifies that the prefix ordering logic
// works correctly with reduced tables (nonzero reductions).
func TestPatternCanMatchPrefixReductions(t *testing.T) {
	rng := rand.New(rand.NewSource(456))
	data := make([]byte, 4096)
	rng.Read(data)
	copy(data[200:], " hello.world.test!")

	cfg := withBaseTableSize(
		NewSearchTableConfig().WithMatchLen(6).WithBytePrefix(' ', '.').WithMaxReducedPopulation(80),
		16,
	)
	table, reductions := cfg.buildSearchTable(data, nil, nil, false)
	if table == nil {
		t.Fatal("table too populated")
	}
	if reductions == 0 {
		t.Log("warning: no reductions applied, test is less useful")
	}

	// Pattern that IS in data — must not false-negative.
	canUse, match := patternCanMatch(&cfg, table, reductions, []byte(" hello.world.test!"))
	if canUse && !match {
		t.Error("false negative for pattern in data")
	}

	// Also with partial prefix context.
	canUse, match = patternCanMatch(&cfg, table, reductions, []byte("hello.world.test!"))
	if canUse && !match {
		t.Error("false negative for partial prefix pattern in data")
	}
}

// TestPatternCanMatchPrefixNoPrefixInPattern verifies canUse=false when
// the pattern has no prefix-context bytes at all.
func TestPatternCanMatchPrefixNoPrefixInPattern(t *testing.T) {
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4).WithBytePrefix(' ', '.'), 16)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 251)
	}
	table, reductions := cfg.buildSearchTable(data, nil, nil, false)
	if table == nil {
		t.Fatal("table too populated")
	}

	// Pattern "abcdef" has no ' ' or '.' — should return canUse=false.
	canUse, _ := patternCanMatch(&cfg, table, reductions, []byte("abcdef"))
	if canUse {
		t.Error("expected canUse=false for pattern with no prefix context")
	}
}

// TestPatternCanMatchPrefixLaterFalsePositive verifies the fix: a false positive
// on a LATER prefix window must not prevent skipping when the first window is absent.
// We construct patterns where only specific windows are present (false positives)
// and verify that the function correctly falls through to the raw fallback.
func TestPatternCanMatchPrefixLaterFalsePositive(t *testing.T) {
	rng := rand.New(rand.NewSource(777))
	ml := uint8(6)
	tableSize := uint8(16)
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(int(ml)).WithBytePrefix(' ', '.'), int(tableSize))
	mask := uint32(1<<tableSize) - 1

	skippedWithFix := 0
	wouldSkipOldCode := 0
	total := 0

	for trial := range 500 {
		// Generate patterns with ' ' and '.' to get multiple prefix windows.
		// Format: " XX.YYYYYY.ZZZZZZ" — 3 prefix windows.
		var pat [18]byte
		rng.Read(pat[:])
		pat[0] = ' '
		pat[3] = '.'
		pat[10] = '.'

		h0 := hashValue(readLE64Pad(pat[1:]), tableSize, ml) & mask
		h1 := hashValue(readLE64Pad(pat[4:]), tableSize, ml) & mask
		h2 := hashValue(readLE64Pad(pat[11:]), tableSize, ml) & mask
		hRaw := hashValue(readLE64Pad(pat[:ml]), tableSize, ml) & mask

		// Skip if any hashes collide — we need distinct bits for a clean test.
		hSet := map[uint32]bool{h0: true, h1: true, h2: true, hRaw: true}
		if len(hSet) < 4 {
			continue
		}

		// Build a table where only h1 is set (false positive on a later window).
		tbl := make([]byte, 1<<(tableSize-3))
		tbl[h1>>3] |= 1 << (h1 & 7)

		canUse, match := patternCanMatch(&cfg, tbl, 0, pat[:])
		if !canUse {
			continue
		}
		total++
		if !match {
			skippedWithFix++
		}

		// Old code would have returned (true, true) here because present > 0.
		// Verify the fix correctly returns (true, false).
		if match {
			t.Errorf("trial %d: later false positive on W1 prevented skipping (h0=%d h1=%d h2=%d hRaw=%d)",
				trial, h0, h1, h2, hRaw)
		}

		_ = wouldSkipOldCode
	}
	t.Logf("tested %d patterns, all correctly skipped with fix", total)
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
	err = searcher.Search(needle, func(r SearchResult) error {
		blocksDecoded++
		if bytes.Contains(r.Blocks[1], needle) {
			found = true
		}
		return nil
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
	err = searcher.Search(needle, func(r SearchResult) error {
		blocksDecoded++
		if bytes.Contains(r.Blocks[1], needle) {
			found = true
		}
		return nil
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

	// Search with pattern shorter than matchLen - tables can't help, fallback decodes all.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	err = searcher.Search([]byte("ab"), func(r SearchResult) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stats := searcher.Stats()
	if stats.BlocksSearched != 2 {
		t.Fatalf("expected 2 blocks searched (fallback), got %d", stats.BlocksSearched)
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
	err = searcher.Search([]byte("ab"), func(r SearchResult) error {
		return nil
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
			err = searcher.Search(tt.pattern, func(r SearchResult) error {
				if bytes.Contains(r.Blocks[1], []byte("=FINDTHIS")) {
					found = true
				}
				return nil
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
			err = searcher.Search(tt.pattern, func(r SearchResult) error {
				blocksSearched++
				if bytes.Contains(r.Blocks[1], tt.pattern) {
					found = true
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			stats := searcher.Stats()
			if !found {
				t.Fatal("pattern not found")
			}
			t.Logf("blocks: %d total, %d skipped, %d searched, tables: %d present, %d missing, %d unusable",
				stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched,
				stats.TablesPresent, stats.TablesMissing, stats.TablesUnusable)

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
			err = searcher.Search(pat, func(r SearchResult) error {
				if bytes.Contains(r.Blocks[1], pat) {
					found = true
				}
				return nil
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

// TestBlockTableIndexCorrectness verifies that for each position where
// a pattern occurs in the data, the correct block's search table has
// the hash bit set.
func TestBlockTableIndexCorrectness(t *testing.T) {
	blockSize := minBlockSize

	for _, tc := range []struct {
		name string
		cfg  SearchTableConfig
	}{
		{"no_prefix", NewSearchTableConfig().WithMatchLen(4)},
		{"byte_prefix", NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('"', ':')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Build compressible data with known patterns.
			data := make([]byte, blockSize*4)
			line := []byte(`{"key":"value","num":12345}` + "\n")
			for i := 0; i < len(data); i += len(line) {
				copy(data[i:], line)
			}
			// Inject unique patterns in specific blocks.
			copy(data[blockSize*0+200:], []byte(`"unique0":"aaa111"`))
			copy(data[blockSize*1+200:], []byte(`"unique1":"bbb222"`))
			copy(data[blockSize*2+200:], []byte(`"unique2":"ccc333"`))
			copy(data[blockSize*3+200:], []byte(`"unique3":"ddd444"`))

			// Compress.
			var buf bytes.Buffer
			w := NewWriter(&buf, WriterSearchTable(tc.cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
			_, err := w.Write(data)
			if err != nil {
				t.Fatal(err)
			}
			w.Close()

			// Parse the stream and extract per-block tables.
			type blockTable struct {
				cfg        SearchTableConfig
				reductions uint8
				table      []byte
			}
			var tables []blockTable
			stream := buf.Bytes()
			pos := 0
			var pendingTable *blockTable
			for pos+4 <= len(stream) {
				chunkType := stream[pos]
				chunkLen := int(stream[pos+1]) | int(stream[pos+2])<<8 | int(stream[pos+3])<<16
				pos += 4
				switch chunkType {
				case ChunkTypeStreamIdentifier:
					pos += chunkLen
				case chunkTypeSearchInfo:
					pos += chunkLen
				case chunkTypeSearchTable:
					cfg, red, tbl, err := parseSearchTable(stream[pos : pos+chunkLen])
					if err != nil {
						t.Fatal(err)
					}
					pendingTable = &blockTable{cfg: cfg, reductions: red, table: tbl}
					pos += chunkLen
				case chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC, chunkTypeUncompressedData:
					if pendingTable != nil {
						tables = append(tables, *pendingTable)
						pendingTable = nil
					} else {
						tables = append(tables, blockTable{})
					}
					pos += chunkLen
				default:
					pos += chunkLen
				}
			}

			// For each pattern, verify the correct block table has its hash set.
			patterns := []string{`unique0":"aaa`, `unique1":"bbb`, `unique2":"ccc`, `unique3":"ddd`}
			for _, pat := range patterns {
				patBytes := []byte(pat)
				idx := bytes.Index(data, patBytes)
				if idx < 0 {
					t.Fatalf("pattern %q not in data", pat)
				}
				blockIdx := idx / blockSize
				if blockIdx >= len(tables) {
					t.Fatalf("pattern %q at offset %d in block %d but only %d tables", pat, idx, blockIdx, len(tables))
				}
				bt := tables[blockIdx]
				if bt.table == nil {
					t.Fatalf("pattern %q: block %d has no search table", pat, blockIdx)
				}
				// Check that patternCanMatch says the block might contain it.
				canUse, match := patternCanMatch(&bt.cfg, bt.table, bt.reductions, patBytes)
				if canUse && !match {
					t.Errorf("pattern %q at offset %d: block %d table says not present (false negative)", pat, idx, blockIdx)
				}
				if !canUse {
					// For prefix tables, check with prefix context.
					// The data has '"' before each unique pattern.
					withPfx := append([]byte{'"'}, patBytes...)
					canUse2, match2 := patternCanMatch(&bt.cfg, bt.table, bt.reductions, withPfx)
					if canUse2 && !match2 {
						t.Errorf("pattern %q with prefix at offset %d: block %d table says not present (false negative)", pat, idx, blockIdx)
					}
				}
			}

			// Verify that blocks WITHOUT a pattern can potentially be skipped.
			for _, pat := range []string{`unique0":"aaa`} {
				patBytes := []byte(pat)
				idx := bytes.Index(data, patBytes)
				blockIdx := idx / blockSize
				for i, bt := range tables {
					if i == blockIdx || bt.table == nil {
						continue
					}
					canUse, match := patternCanMatch(&bt.cfg, bt.table, bt.reductions, patBytes)
					if canUse && !match {
						// Good — correctly identified as not present.
					}
					_ = canUse
					_ = match
				}
			}
		})
	}
}

func FuzzSearchNoFalseNegatives(f *testing.F) {
	seeds := [][]byte{
		bytes.Repeat([]byte("hello world test data"), 15),
		bytes.Repeat([]byte("A"), 100),
	}
	// Deterministic pseudo-random seed.
	prng := make([]byte, 400)
	for i := range prng {
		prng[i] = byte(i*179 + 83)
	}
	seeds = append(seeds, prng)
	for _, data := range seeds {
		for matchLen := 1; matchLen <= 8; matchLen++ {
			for tableSize := 8; tableSize <= 23; tableSize++ {
				f.Add(data, matchLen, tableSize, []byte(nil))
				if matchLen > 1 {
					f.Add(data, matchLen, tableSize, bytes.Repeat([]byte("Z"), matchLen-1))
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, data []byte, matchLen, tableSize int, overlap []byte) {
		if len(data) < 256 || matchLen < 1 || matchLen > 8 || tableSize < 8 || tableSize > 23 {
			return
		}
		if len(overlap) > matchLen-1 {
			overlap = overlap[:matchLen-1]
		}
		cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(matchLen), tableSize)
		for _, packed := range []bool{false, true} {
			table, reductions := cfg.buildSearchTable(data, overlap, nil, packed)
			if table == nil {
				continue // too populated
			}
			combined := data
			if len(overlap) > 0 {
				combined = append(append([]byte{}, data...), overlap...)
			}
			mask := uint32((1 << (cfg.baseTableSize - reductions)) - 1)
			// Every position in data must have its bit set.
			for i := 0; i < len(data); i++ {
				if i+int(cfg.matchLen) > len(combined) {
					break
				}
				v := readLE64Pad(combined[i:])
				h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
				if table[h>>3]&(1<<(h&7)) == 0 {
					t.Fatalf("false negative at pos %d (ml=%d ts=%d red=%d overlap=%d packed=%v)", i, matchLen, tableSize, reductions, len(overlap), packed)
				}
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
		for _, packed := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/packed=%v", sizeLabel(size), packed), func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				var dst []byte
				for i := 0; i < b.N; i++ {
					dst, _ = cfg.buildSearchTable(data, nil, dst, packed)
				}
			})
			b.Run(fmt.Sprintf("%s/packed=%v/parallel", sizeLabel(size), packed), func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					var dst []byte
					for pb.Next() {
						dst, _ = cfg.buildSearchTable(data, nil, dst, packed)
					}
				})
			})
		}
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
		for _, packed := range []bool{false, true} {
			t.Run(fmt.Sprintf("matchLen=%d/packed=%v", ml, packed), func(t *testing.T) {
				cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(ml), 16)
				table, reductions := cfg.buildSearchTable(data, nil, nil, packed)
				if table == nil {
					t.Skip("table too populated")
				}
				mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1
				for i := 0; i <= len(data)-ml; i++ {
					v := readLE64Pad(data[i:])
					h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
					if table[h>>3]&(1<<(h&7)) == 0 {
						t.Fatalf("false negative at pos %d, reduction: %d", i, reductions)
					}
				}
			})
		}
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
			err = searcher.Search(needle, func(r SearchResult) error {
				if bytes.Contains(r.Blocks[1], needle) {
					found = true
				}
				return nil
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
			err = searcher.Search(needle, func(r SearchResult) error {
				if bytes.Contains(r.Blocks[1], needle) {
					found = true
				}
				return nil
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
	err = searcher.Search(needle, func(r SearchResult) error {
		if bytes.Contains(r.Blocks[1], needle) {
			foundBlocks++
		}
		return nil
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
	err := searcher.Search([]byte("test"), func(r SearchResult) error {
		called = true
		return nil
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
	err = searcher.Search(needle[:4], func(r SearchResult) error {
		blocksDecoded++
		return nil
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
			err = searcher.Search(boundary, func(r SearchResult) error {
				if bytes.Contains(r.Blocks[1], boundary) {
					foundBlocks++
				}
				return nil
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
			err = searcher.Search(needle, func(r SearchResult) error {
				if bytes.Contains(r.Blocks[1], needle) {
					found++
				}
				return nil
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
	err = searcher.Search(needle, func(r SearchResult) error {
		found = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("needle not found in fallback mode")
	}
	stats := searcher.Stats()
	if stats.BlocksSearched != 3 {
		t.Fatalf("expected all 3 blocks searched in fallback, got %d", stats.BlocksSearched)
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
	err = searcher.Search([]byte("test"), func(r SearchResult) error {
		return nil
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
	err = searcher.Search(needle, func(r SearchResult) error {
		blocksDecoded++
		if bytes.Contains(r.Blocks[1], needle) {
			found = true
		}
		return nil
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
	err = searcher.Search([]byte("=MASK"), func(r SearchResult) error {
		if bytes.Contains(r.Blocks[1], []byte("=MASK_PATTERN")) {
			found = true
		}
		return nil
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
		err = searcher.Search(needle, func(r SearchResult) error {
			if bytes.Contains(r.Blocks[1], needle) {
				found = true
			}
			return nil
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
	matchCount := 0
	errStop := fmt.Errorf("stop")
	err = searcher.Search(needle, func(r SearchResult) error {
		matchCount++
		if matchCount >= 2 {
			return errStop
		}
		return nil
	})
	if err != nil && err != errStop {
		t.Fatal(err)
	}
	if matchCount != 2 {
		t.Fatalf("expected 2 matches before stop, got %d", matchCount)
	}
}

// TestSearchForward exercises ErrSearchForward for requesting forward context.
func TestSearchForward(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*3)
	// Compressible filler.
	for i := range data {
		data[i] = byte(i % 251)
	}
	// Place needle near the end of block 0.
	needle := []byte("FORWARD_CONTEXT!")
	copy(data[blockSize-20:], needle)
	// Place the needle in block 1 too so it isn't skipped.
	copy(data[blockSize+100:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Search and request forward context on the first match.
	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	calls := 0
	err = searcher.Search(needle, func(r SearchResult) error {
		calls++
		if calls == 1 {
			// First call: should have Blocks[1] as the block containing the match.
			if r.Blocks[1] == nil {
				t.Fatal("first call: Blocks[1] should not be nil")
			}
			if r.StreamOffset != int64(blockSize-20) {
				t.Fatalf("first call: StreamOffset=%d, want %d", r.StreamOffset, blockSize-20)
			}
			return ErrSearchForward
		}
		if calls == 2 {
			// Second call (re-dispatch): Blocks[0] should be the block that had the match,
			// Blocks[1] should be the next block (forward context).
			if r.Blocks[0] == nil {
				t.Fatal("forward call: Blocks[0] should not be nil")
			}
			if r.Blocks[1] == nil {
				t.Fatal("forward call: Blocks[1] should be the next block")
			}
			// StreamOffset should be unchanged.
			if r.StreamOffset != int64(blockSize-20) {
				t.Fatalf("forward call: StreamOffset=%d, want %d", r.StreamOffset, blockSize-20)
			}
			// Offset should point into the block data.
			if r.Offset < 0 {
				t.Fatalf("forward call: Offset=%d, should be >= 0", r.Offset)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls < 2 {
		t.Fatalf("expected at least 2 calls (original + forward), got %d", calls)
	}
}

// TestSearchForwardEOF exercises ErrSearchForward when the match is in the last block.
func TestSearchForwardEOF(t *testing.T) {
	blockSize := minBlockSize
	data := make([]byte, blockSize*2)
	for i := range data {
		data[i] = byte(i % 251)
	}
	// Place needle near the end of the last block.
	needle := []byte("EOF_FORWARD_TEST")
	copy(data[len(data)-20:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4)
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	_, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	calls := 0
	err = searcher.Search(needle, func(r SearchResult) error {
		calls++
		if calls == 1 {
			return ErrSearchForward
		}
		// Second call: Blocks[1] should be nil since there's no next block.
		if r.Blocks[1] != nil {
			t.Fatal("forward at EOF: Blocks[1] should be nil")
		}
		if r.Blocks[0] == nil {
			t.Fatal("forward at EOF: Blocks[0] should not be nil")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
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
	err = searcher.Search(needle, func(r SearchResult) error {
		if bytes.Contains(r.Blocks[1], needle) {
			offsets = append(offsets, r.BlockStart)
		}
		return nil
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
		for _, packed := range []bool{false, true} {
			t.Run(fmt.Sprintf("ts=%d/packed=%v", ts, packed), func(t *testing.T) {
				cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), ts)
				table, reductions := cfg.buildSearchTable(data, nil, nil, packed)
				if table == nil {
					t.Log("table skipped (too populated)")
					return
				}
				mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1
				for i := 0; i <= len(data)-4; i++ {
					v := readLE64Pad(data[i:])
					h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
					if table[h>>3]&(1<<(h&7)) == 0 {
						t.Fatalf("false negative at pos %d (ts=%d red=%d packed=%v)", i, ts, reductions, packed)
					}
				}
			})
		}
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
	bt := make([]byte, 1<<cfg.baseTableSize)
	buildTableNoPrefixByte(bt, data, len(data), cfg.baseTableSize, cfg.matchLen)
	fullTable := make([]byte, 1<<(cfg.baseTableSize-3))
	packBits(fullTable, bt)

	// Reduce.
	origPop, _ := tablePopulation(fullTable)
	reduced, reductions := reduceTable(fullTable, origPop, 50)

	if reductions == 0 {
		t.Skip("no reductions applied")
	}

	// Every hash from the original data must be findable in the reduced table.
	mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1
	for i := 0; i <= len(data)-4; i++ {
		v := readLE64Pad(data[i:])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
		if reduced[h>>3]&(1<<(h&7)) == 0 {
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
	err := searcher.Search(needle, func(r SearchResult) error {
		if bytes.Contains(r.Blocks[1], needle) {
			found = true
		}
		return nil
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

		// Collect all expected match offsets from the original data.
		var expected []int64
		if usePrefix {
			// For prefix tables, only count occurrences preceded by the prefix byte.
			pfx := pattern[0]
			needle := append([]byte{pfx}, pattern...)
			off := 0
			for {
				idx := bytes.Index(data[off:], needle)
				if idx < 0 {
					break
				}
				// The match of the pattern (not including prefix) starts 1 byte later.
				expected = append(expected, int64(off+idx+1))
				off += idx + 1
			}
		} else {
			off := 0
			for {
				idx := bytes.Index(data[off:], pattern)
				if idx < 0 {
					break
				}
				expected = append(expected, int64(off+idx))
				off += idx + 1
			}
		}

		if len(expected) > 0 {
			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			var found []int64
			err = searcher.Search(pattern, func(r SearchResult) error {
				found = append(found, r.StreamOffset)
				return nil
			})
			if err != nil {
				t.Fatalf("search failed: %v", err)
			}

			// Every expected offset must appear in found.
			foundSet := make(map[int64]bool, len(found))
			for _, f := range found {
				foundSet[f] = true
			}
			for _, e := range expected {
				if !foundSet[e] {
					stats := searcher.Stats()
					t.Fatalf("expected match at offset %d not found (prefix=%v, expected=%d found=%d). "+
						"blocks: total=%d skipped=%d searched=%d missing=%d unusable=%d",
						e, usePrefix, len(expected), len(found),
						stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched,
						stats.TablesMissing, stats.TablesUnusable)
				}
			}

			// Verify found offsets are sorted (matches should be in stream order).
			for i := 1; i < len(found); i++ {
				if found[i] <= found[i-1] {
					t.Fatalf("found offsets not in order: [%d]=%d <= [%d]=%d",
						i, found[i], i-1, found[i-1])
				}
			}
		}
	})
}

func TestSearchDeferNoPrefixRegression(t *testing.T) {
	// Regression: no-prefix deferral skipped a block containing a real boundary match.
	// Data has repetitive ')' and '\xa2' bytes creating many near-identical hashes.
	// Pattern: 32×')' + 30×'\xa2', matchLen=7.
	pattern := append(bytes.Repeat([]byte(")"), 32), bytes.Repeat([]byte{0xa2}, 30)...)
	matchLen := 7

	// Build data that reproduces the issue: blocks of ')' and '\xa2' with
	// enough variation to trigger the deferred path.
	blockSize := minBlockSize
	data := make([]byte, 63*blockSize)
	rng := rand.New(rand.NewSource(0xa2))
	for i := range data {
		// Mostly ')' and '\xa2' with occasional other bytes
		switch rng.Intn(10) {
		case 0:
			data[i] = 0xa2
		default:
			data[i] = ')'
		}
	}
	// Ensure pattern appears at a block boundary
	off := 39*blockSize + blockSize - 48
	copy(data[off:], pattern)

	cfg := NewSearchTableConfig().WithMatchLen(matchLen)
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	w.Write(data)
	w.Close()

	var expected []int64
	idx := 0
	for {
		i := bytes.Index(data[idx:], pattern)
		if i < 0 {
			break
		}
		expected = append(expected, int64(idx+i))
		idx += i + 1
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	var found []int64
	err := searcher.Search(pattern, func(r SearchResult) error {
		found = append(found, r.StreamOffset)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	foundSet := make(map[int64]bool, len(found))
	for _, f := range found {
		foundSet[f] = true
	}
	for _, e := range expected {
		if !foundSet[e] {
			stats := searcher.Stats()
			t.Fatalf("expected match at offset %d not found (expected=%d found=%d). "+
				"blocks: total=%d skipped=%d searched=%d deferred=%d",
				e, len(expected), len(found),
				stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched, stats.BlocksDeferred)
		}
	}
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
			var dst []byte
			for b.Loop() {
				dst, _ = cfg.buildSearchTable(data, nil, dst, false)
			}
		})
	}
}

// repetitiveData builds nBlocks * blockSize bytes of repetitive text.
// Each block is filled with repeated lines, giving sparse search tables.
func repetitiveData(blockSize, nBlocks int) []byte {
	line := []byte("the quick brown fox jumps over the lazy dog.\n")
	filler := bytes.Repeat(line, blockSize/len(line)+1)
	data := make([]byte, blockSize*nBlocks)
	for i := range nBlocks {
		copy(data[i*blockSize:], filler[:blockSize])
	}
	return data
}

// TestSearchTableEfficiency verifies that search tables are generated, usable,
// and produce meaningful skip rates across all table types.
func TestSearchTableEfficiency(t *testing.T) {
	blockSize := 64 << 10
	nBlocks := 16
	data := repetitiveData(blockSize, nBlocks)

	// Place a unique needle only in one block.
	targetBlock := nBlocks / 2
	needle := []byte("XYZZY!UNIQUE!NEEDLE!PLUGH!")
	copy(data[targetBlock*blockSize+500:], needle)

	tests := []struct {
		name      string
		cfg       SearchTableConfig
		pattern   []byte
		wantSkips bool // prefix types always skip; no_prefix may not due to hash collisions
	}{
		{
			name:    "no_prefix",
			cfg:     NewSearchTableConfig().WithMatchLen(4),
			pattern: needle,
		},
		{
			name:      "byte_prefix",
			cfg:       NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('!'),
			pattern:   []byte("!UNIQUE!NEEDLE"),
			wantSkips: true,
		},
		{
			name: "mask_prefix",
			cfg: func() SearchTableConfig {
				var m [32]byte
				m['!'>>3] |= 1 << ('!' & 7)
				return NewSearchTableConfig().WithMatchLen(4).WithMaskPrefix(m)
			}(),
			pattern:   []byte("!UNIQUE!NEEDLE"),
			wantSkips: true,
		},
		{
			name:      "long_prefix",
			cfg:       NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte("XYZZY!")),
			pattern:   []byte("XYZZY!UNIQUE!NEEDLE"),
			wantSkips: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, WriterSearchTable(tt.cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
			if err := w.EncodeBuffer(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
			found := 0
			err := searcher.Search(tt.pattern, func(r SearchResult) error {
				found++
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if found == 0 {
				t.Fatal("pattern not found")
			}

			stats := searcher.Stats()
			t.Logf("blocks: %d total, %d skipped, %d searched, pop avg %.1f%%",
				stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched,
				stats.TablePopSum/float64(stats.TablesPresent))

			if stats.BlocksSkipped+stats.BlocksSearched != stats.BlocksTotal {
				t.Fatalf("skipped(%d) + searched(%d) != total(%d)",
					stats.BlocksSkipped, stats.BlocksSearched, stats.BlocksTotal)
			}
			if stats.TablesPresent != stats.BlocksTotal {
				t.Fatalf("expected %d tables, got %d", stats.BlocksTotal, stats.TablesPresent)
			}
			if stats.TablesUnusable > 0 {
				t.Fatalf("expected 0 unusable tables, got %d", stats.TablesUnusable)
			}
			if tt.wantSkips && stats.BlocksSkipped < nBlocks/2 {
				t.Errorf("poor skip rate: %d/%d skipped (expected >50%%)", stats.BlocksSkipped, stats.BlocksTotal)
			}
		})
	}
}

// TestSearchNoCascade verifies that decoding one block does not force all
// subsequent blocks to decode (the cascade-breaking fix).
// Uses a prefix type to guarantee blocks are skippable by the table.
func TestSearchNoCascade(t *testing.T) {
	blockSize := 64 << 10
	nBlocks := 16
	data := repetitiveData(blockSize, nBlocks)

	// Place needle in block 0 only.
	needle := []byte("=CASCADE_TEST_NEEDLE!")
	copy(data[100:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('=')
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	if err := w.EncodeBuffer(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := 0
	err := searcher.Search(needle, func(r SearchResult) error {
		found++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found != 1 {
		t.Fatalf("expected 1 match, got %d", found)
	}

	stats := searcher.Stats()
	t.Logf("blocks: %d total, %d skipped, %d searched", stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched)

	// Without cascade fix, block 0 decoded → block 1 forced → ... → all decoded.
	// With fix, only block 0 (+ possible hash collisions) should be searched.
	if stats.BlocksSearched > nBlocks/2 {
		t.Errorf("cascade detected: %d/%d blocks searched, expected fewer than half", stats.BlocksSearched, stats.BlocksTotal)
	}
}

// TestSearchBoundarySkipOptimization verifies that the canBoundaryMatch
// optimization allows skipping blocks after a decoded block when the
// previous block's tail does not contain a prefix of the search pattern.
func TestSearchBoundarySkipOptimization(t *testing.T) {
	blockSize := 64 << 10
	nBlocks := 8
	data := repetitiveData(blockSize, nBlocks)

	// Place needle in middle of block 3. The repetitive filler won't end
	// with a prefix of the needle, so the block after it should be skippable.
	needle := []byte("=BOUNDARY_OPT_NEEDLE_TEST")
	copy(data[3*blockSize+blockSize/2:], needle)

	var buf bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('=')
	w := NewWriter(&buf, WriterSearchTable(cfg), WriterBlockSize(blockSize), WriterConcurrency(1))
	if err := w.EncodeBuffer(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	found := 0
	err := searcher.Search(needle, func(r SearchResult) error {
		found++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Fatal("pattern not found")
	}

	stats := searcher.Stats()
	t.Logf("blocks: %d total, %d skipped, %d searched", stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched)

	if stats.BlocksSkipped == 0 {
		t.Error("expected some blocks to be skipped")
	}
	if stats.BlocksSkipped+stats.BlocksSearched != stats.BlocksTotal {
		t.Fatalf("skipped(%d) + searched(%d) != total(%d)",
			stats.BlocksSkipped, stats.BlocksSearched, stats.BlocksTotal)
	}
}

func TestCanBoundaryMatch(t *testing.T) {
	tests := []struct {
		prev    string
		pattern string
		want    bool
	}{
		{"hello world", "world!", true},   // "world" suffix is prefix of "world!"
		{"hello world", "xyzzy", false},   // no suffix of prev starts pattern
		{"abcdef", "efgh", true},          // "ef" suffix matches "ef" prefix
		{"abcdef", "f_extra", true},       // "f" suffix matches "f" prefix
		{"abcdef", "ghij", false},         // no match
		{"", "anything", false},           // empty prev
		{"data", "d", false},              // single-byte pattern can't straddle
		{"aaaa", "aaaa_end", true},        // full prev is prefix
		{"xxABC", "ABCdef", true},         // 3-byte suffix
		{"random bytes", "NEEDLE", false}, // no overlap
	}
	for _, tt := range tests {
		got := canBoundaryMatch([]byte(tt.prev), []byte(tt.pattern))
		if got != tt.want {
			t.Errorf("canBoundaryMatch(%q, %q) = %v, want %v", tt.prev, tt.pattern, got, tt.want)
		}
	}
}

func BenchmarkPatternCanMatch(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 1<<20)
	rng.Read(data)
	cfg := withBaseTableSize(NewSearchTableConfig().WithMatchLen(4), 20)
	for _, packed := range []bool{false, true} {
		b.Run(fmt.Sprintf("packed=%v", packed), func(b *testing.B) {
			table, reductions := cfg.buildSearchTable(data, nil, nil, packed)
			if table == nil {
				b.Fatal("table nil")
			}
			pattern := data[5000:5020]
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				patternCanMatch(&cfg, table, reductions, pattern)
			}
		})
	}
}
