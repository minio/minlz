package minlz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"strings"
	"testing"
)

// makeBitmap returns a `size`-byte bitmap with the given approximate population
// fraction (0..1). Uses a deterministic RNG seeded with seed.
func makeBitmap(size int, popFrac float64, seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, size)
	totalBits := size * 8
	target := int(float64(totalBits) * popFrac)
	for i := 0; i < target; i++ {
		// Try to set a random unset bit. Retry on collisions; bounded retries
		// keeps this test-only helper cheap even when popFrac is large.
		for tries := 0; tries < 4; tries++ {
			bit := rng.Intn(totalBits)
			byteIdx := bit >> 3
			mask := byte(1 << uint(bit&7))
			if b[byteIdx]&mask == 0 {
				b[byteIdx] |= mask
				break
			}
		}
	}
	return b
}

func TestHuff0BlockSizePolicy(t *testing.T) {
	cases := []struct {
		bitmap   int
		wantLog2 uint8
		wantN    int
	}{
		{32, 5, 1},
		{256, 8, 1},
		{4096, 12, 1},
		{8192, 13, 1},
		{16384, 14, 1},
		{32768, 15, 1},
		{65536, 15, 2},
		{131072, 15, 4},
		{524288, 15, 16},
		{1 << 20, 16, 16},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.bitmap), func(t *testing.T) {
			gotL, gotN := huff0BlockSize(c.bitmap)
			if gotL != c.wantLog2 || gotN != c.wantN {
				t.Fatalf("huff0BlockSize(%d) = (%d, %d), want (%d, %d)", c.bitmap, gotL, gotN, c.wantLog2, c.wantN)
			}
			if gotN*(1<<gotL) != c.bitmap {
				t.Fatalf("nBlocks*blockSize=%d != bitmap=%d", gotN*(1<<gotL), c.bitmap)
			}
		})
	}
}

// chunkBitmapRoundtrip encodes a bitmap as 0x46 and parses it back.
// Compression is forced so we always exercise the codec end-to-end.
func chunkBitmapRoundtrip(t *testing.T, cfg SearchTableConfig, reductions uint8, bitmap []byte) {
	t.Helper()
	cfg = cfg.WithCompression(CompressedSearchForce())
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, reductions, bitmap, e)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if !ok {
		t.Fatalf("append refused (force should always emit)")
	}
	if out[0] != chunkTypeSearchTableCompressed {
		t.Fatalf("expected chunk type 0x46, got 0x%x", out[0])
	}
	dec := newCSTDecoder()
	cfgGot, redGot, tblGot, err := parseSearchTableCompressed(out[4:], dec, false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfgGot.tableType != cfg.tableType || cfgGot.matchLen != cfg.matchLen || cfgGot.baseTableSize != cfg.baseTableSize {
		t.Fatalf("config mismatch: got type=%d ml=%d ts=%d", cfgGot.tableType, cfgGot.matchLen, cfgGot.baseTableSize)
	}
	if redGot != reductions {
		t.Fatalf("reductions: got %d want %d", redGot, reductions)
	}
	if !bytes.Equal(tblGot, bitmap) {
		t.Fatalf("bitmap mismatch: got %d bytes, want %d", len(tblGot), len(bitmap))
	}
}

func TestCompressedChunkRoundtrip(t *testing.T) {
	// Bitmap sizes covering single-block (≤32KiB) and multi-block (>32KiB).
	sizes := []int{256, 4096, 8192, 32 << 10, 64 << 10, 128 << 10}
	pops := []struct {
		name string
		frac float64
	}{
		{"sparse", 0.05},
		{"low", 0.15},
		{"high", 0.85},
		{"dense", 0.95},
	}
	cfgs := []struct {
		name string
		cfg  SearchTableConfig
	}{
		{"noPrefix", NewSearchTableConfig().WithMatchLen(4)},
		{"bytePrefix", NewSearchTableConfig().WithMatchLen(4).WithBytePrefix('=', ':')},
		{"longPrefix", NewSearchTableConfig().WithMatchLen(4).WithLongPrefix([]byte("id:"))},
	}
	for _, cc := range cfgs {
		for _, sz := range sizes {
			for _, p := range pops {
				t.Run(fmt.Sprintf("%s/%d/%s", cc.name, sz, p.name), func(t *testing.T) {
					bitmap := makeBitmap(sz, p.frac, 1)
					// baseTableSize is set so 1 << (baseTableSize-3) == sz, i.e. log2(sz)+3.
					// reductions = 0 to keep the bitmap at the chosen size.
					var bts uint8
					for i := uint8(8); i <= 23; i++ {
						if 1<<(i-3) == sz {
							bts = i
							break
						}
					}
					if bts == 0 {
						t.Fatalf("no baseTableSize matches sz=%d", sz)
					}
					cfg := cc.cfg
					cfg.baseTableSize = bts
					chunkBitmapRoundtrip(t, cfg, 0, bitmap)
				})
			}
		}
	}
}

func TestCompressedPopcountBand(t *testing.T) {
	// 50% population should hit the band and refuse 0x46.
	bitmap := makeBitmap(4096, 0.50, 7)
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchSkipPct(5.0))
	cfg.baseTableSize = 15
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if ok {
		t.Fatalf("expected band rejection, got %d-byte 0x46 chunk", len(out))
	}
}

func TestCompressedTinyBitmapBeats45(t *testing.T) {
	// A 32-byte all-zero bitmap should be emitted as 0x46 via the RLE path
	// (≈16 bytes) instead of the ≈44-byte 0x45 form.
	bitmap := make([]byte, 32)
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression()
	cfg.baseTableSize = 8
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("expected 0x46 emission for tiny sparse bitmap")
	}
	chunk45 := 4 + cfg.searchTablePayloadSize(len(bitmap))
	if len(out) >= chunk45 {
		t.Fatalf("0x46 chunk (%d) not smaller than 0x45 (%d)", len(out), chunk45)
	}
}

func TestCompressedRLEAllZero(t *testing.T) {
	// Multi-block all-zero bitmap → every huff0 block becomes RLE.
	bitmap := make([]byte, 64<<10) // 2 blocks of 32KiB
	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 19 // 1 << (19-3) = 65536
	var seenStats CompressedSearchStats
	cfg = cfg.WithCompression(CompressedSearchForce(), CompressedSearchStatsHook(func(s CompressedSearchStats) { seenStats = s }))
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("append err=%v ok=%v", err, ok)
	}
	// Validate roundtrip.
	dec := newCSTDecoder()
	_, _, got, err := parseSearchTableCompressed(out[4:], dec, false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bytes.Equal(got, bitmap) {
		t.Fatalf("roundtrip mismatch")
	}
	// Every huff0 sub-block should be RLE.
	if seenStats.BlocksRLE != seenStats.Huff0Blocks {
		t.Fatalf("expected all %d blocks RLE, got own=%d global=%d raw=%d rle=%d",
			seenStats.Huff0Blocks,
			seenStats.BlocksOwnTable, seenStats.BlocksGlobalTable,
			seenStats.BlocksRaw, seenStats.BlocksRLE)
	}
}

func TestCompressedRLEAllOne(t *testing.T) {
	bitmap := bytes.Repeat([]byte{0xff}, 32<<10)
	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 18 // 1<<(18-3) = 32768
	cfg = cfg.WithCompression(CompressedSearchForce())
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("append err=%v ok=%v", err, ok)
	}
	dec := newCSTDecoder()
	_, _, got, err := parseSearchTableCompressed(out[4:], dec, false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bytes.Equal(got, bitmap) {
		t.Fatalf("roundtrip mismatch")
	}
}

// TestCompressedGlobalSingleUserWasted constructs a multi-block bitmap where
// only one huff0 block could plausibly use the global table (the rest are
// all-zero → RLE). The encoder should reject the global table because a
// single user is not actually sharing.
func TestCompressedGlobalSingleUserWasted(t *testing.T) {
	const sz = 64 << 10 // 2 huff0 blocks of 32 KiB
	bitmap := make([]byte, sz)
	rng := rand.New(rand.NewSource(11))
	// Block 0: skewed random data — Compress4X will succeed.
	for i := 0; i < 32<<10; i++ {
		bitmap[i] = byte(rng.Intn(16))
	}
	// Block 1: all zeros → forced to RLE.

	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 19 // 1<<(19-3) = 64 KiB
	var seen CompressedSearchStats
	cfg = cfg.WithCompression(CompressedSearchForce(),
		CompressedSearchStatsHook(func(s CompressedSearchStats) { seen = s }))
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("append err=%v ok=%v", err, ok)
	}
	// Block 1 must be RLE; block 0 picks own or raw — never global, since
	// global having a single user is treated as wasted.
	if seen.BlocksGlobalTable != 0 {
		t.Fatalf("expected 0 global users (single-user is wasted), got %d (own=%d raw=%d RLE=%d sparse=%d)",
			seen.BlocksGlobalTable, seen.BlocksOwnTable, seen.BlocksRaw, seen.BlocksRLE, seen.BlocksSparse)
	}
	// Roundtrip sanity.
	_, _, got, err := parseSearchTableCompressed(out[4:], newCSTDecoder(), false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bytes.Equal(got, bitmap) {
		t.Fatalf("roundtrip mismatch")
	}
}

// TestCompressedGlobalShared constructs a multi-block bitmap where every
// block has the exact same byte-skewed distribution. The picker should
// emit one shared global table (rather than four identical own tables) and
// reduce the embedded-table count from N to 1.
func TestCompressedGlobalShared(t *testing.T) {
	const sz = 128 << 10 // 4 huff0 blocks of 32 KiB
	bitmap := make([]byte, sz)
	rng := rand.New(rand.NewSource(22))
	template := make([]byte, 32<<10)
	for i := range template {
		switch rng.Intn(4) {
		case 0:
			template[i] = 10
		case 1:
			template[i] = 20
		case 2:
			template[i] = 30
		default:
			template[i] = byte(40 + rng.Intn(8))
		}
	}
	for blk := 0; blk < 4; blk++ {
		copy(bitmap[blk*32<<10:], template)
	}

	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 20
	var seen CompressedSearchStats
	cfg = cfg.WithCompression(CompressedSearchForce(),
		CompressedSearchStatsHook(func(s CompressedSearchStats) { seen = s }))
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("append err=%v ok=%v", err, ok)
	}
	if seen.BlocksGlobalTable < 2 {
		t.Fatalf("expected ≥2 global users (identical-distribution blocks should share), got %d (own=%d raw=%d RLE=%d sparse=%d)",
			seen.BlocksGlobalTable, seen.BlocksOwnTable, seen.BlocksRaw, seen.BlocksRLE, seen.BlocksSparse)
	}
	if seen.Tables > seen.BlocksGlobalTable {
		t.Fatalf("expected ≤ BlocksGlobalTable tables emitted, got Tables=%d users=%d", seen.Tables, seen.BlocksGlobalTable)
	}
	_, _, got, err := parseSearchTableCompressed(out[4:], newCSTDecoder(), false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bytes.Equal(got, bitmap) {
		t.Fatalf("roundtrip mismatch")
	}
}

// TestCompressedConcurrentDecode hammers the per-sub-block worker goroutines
// by parsing the same multi-block 0x46 chunk repeatedly with fresh decoders.
// Intended to be run under `-race` to catch any cross-goroutine state bug in
// the decompression workers or shared scratch buffers.
func TestCompressedConcurrentDecode(t *testing.T) {
	bitmap := makeBitmap(128<<10, 0.07, 99) // 4 huff0 blocks of 32 KiB
	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 20
	cfg = cfg.WithCompression(CompressedSearchForce())
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("encode: ok=%v err=%v", ok, err)
	}
	payload := out[4:]

	const workers = 8
	const iters = 32
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < iters; i++ {
				dec := newCSTDecoder()
				_, _, got, err := parseSearchTableCompressed(payload, dec, false)
				if err != nil {
					errCh <- err
					return
				}
				if !bytes.Equal(got, bitmap) {
					errCh <- fmt.Errorf("mismatch")
					return
				}
			}
			errCh <- nil
		}()
	}
	for w := 0; w < workers; w++ {
		if err := <-errCh; err != nil {
			t.Fatalf("worker: %v", err)
		}
	}
}

// TestCompressedReductionsInStats verifies the Reductions field is reported.
func TestCompressedReductionsInStats(t *testing.T) {
	bitmap := makeBitmap(8192, 0.10, 33)
	var got CompressedSearchStats
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(
		CompressedSearchForce(),
		CompressedSearchStatsHook(func(s CompressedSearchStats) { got = s }),
	)
	cfg.baseTableSize = 18
	e := newCSTEncoder()
	if _, ok, _ := appendSearchTableCompressedChunk(nil, &cfg, 2, bitmap, e); !ok {
		t.Fatalf("force-emit failed")
	}
	if got.Reductions != 2 {
		t.Fatalf("Reductions=%d, want 2", got.Reductions)
	}
}

func TestCompressedSparseBitTableRoundtrip(t *testing.T) {
	// Direct codec roundtrip for the sparse encoding.
	cases := [][]byte{
		make([]byte, 32),                     // all zero -> 0 bytes
		bytes.Repeat([]byte{0xff}, 32),       // all one  -> 256 set bits, 256 bytes
		{0x01, 0, 0, 0, 0, 0, 0, 0x80},       // 2 set bits, well separated
		bytes.Repeat([]byte{0x00, 0x01}, 16), // every 16th bit
	}
	// One bit at position 8*1024-1.
	{
		b := make([]byte, 1024)
		b[1023] = 0x80
		cases = append(cases, b)
	}
	for i, in := range cases {
		popcount := popcountBytes(in)
		est := sparseBitTableEstimate(len(in), popcount)
		enc := appendSparseBitTable(nil, in)
		// Estimate is a tight upper bound on the actual encoded size.
		if len(enc) > est {
			t.Fatalf("case %d: actual=%d exceeds estimate=%d", i, len(enc), est)
		}
		dec := make([]byte, len(in))
		if err := decodeSparseBitTable(dec, enc); err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if !bytes.Equal(dec, in) {
			t.Fatalf("case %d: roundtrip mismatch", i)
		}
	}
}

func TestCompressedSparseDispositionSelected(t *testing.T) {
	// A bitmap with one set bit per 1024 bits: very sparse, sparse should win.
	const sz = 4096
	bitmap := make([]byte, sz)
	for i := 0; i < sz; i += 128 {
		bitmap[i] = 0x01
	}
	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 15 // 1<<(15-3) = 4096
	var seen CompressedSearchStats
	cfg = cfg.WithCompression(CompressedSearchForce(), CompressedSearchStatsHook(func(s CompressedSearchStats) { seen = s }))
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("append err=%v ok=%v", err, ok)
	}
	if seen.BlocksSparse == 0 {
		t.Fatalf("expected sparse disposition, got own=%d global=%d raw=%d rle=%d sparse=%d",
			seen.BlocksOwnTable, seen.BlocksGlobalTable, seen.BlocksRaw, seen.BlocksRLE, seen.BlocksSparse)
	}
	// Roundtrip.
	dec := newCSTDecoder()
	_, _, got, err := parseSearchTableCompressed(out[4:], dec, false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bytes.Equal(got, bitmap) {
		t.Fatalf("bitmap mismatch")
	}
	if dec.lastBlocksSparse == 0 {
		t.Fatalf("decoder did not record any sparse blocks")
	}
}

func TestCompressedRejectMalformed(t *testing.T) {
	// Build a valid chunk first.
	bitmap := makeBitmap(4096, 0.10, 11)
	cfg := NewSearchTableConfig().WithMatchLen(4)
	cfg.baseTableSize = 15
	cfg2 := cfg.WithCompression(CompressedSearchForce())
	e := newCSTEncoder()
	out, ok, err := appendSearchTableCompressedChunk(nil, &cfg2, 0, bitmap, e)
	if err != nil || !ok {
		t.Fatalf("setup: err=%v ok=%v", err, ok)
	}
	payload := out[4:]

	t.Run("truncated", func(t *testing.T) {
		for i := 0; i < len(payload); i += max(1, len(payload)/32) {
			_, _, _, err := parseSearchTableCompressed(payload[:i], newCSTDecoder(), false)
			if err == nil && i < len(payload) {
				t.Fatalf("expected error for truncated len=%d", i)
			}
		}
	})

	t.Run("badCRC", func(t *testing.T) {
		// CRC is at offset (3 + prefixSize() + 1) within the payload (after type/ml/baseSize/prefix/reductions).
		off := 3 + cfg.prefixSize() + 1
		corrupt := append([]byte(nil), payload...)
		binary.LittleEndian.PutUint32(corrupt[off:], 0xdeadbeef)
		_, _, _, err := parseSearchTableCompressed(corrupt, newCSTDecoder(), false)
		if err == nil {
			t.Fatalf("expected CRC error")
		}
	})

	t.Run("badDisposition", func(t *testing.T) {
		// Disposition for block 0 follows the table section. The simplest invalid
		// is to mutate the byte directly after the table to a reserved value.
		// We don't know the exact offset without parsing — flip every byte after
		// the header looking for an out-of-range disposition that wasn't valid.
		// Cheaper: rewrite the byte at position (len-1 or earlier) — pick a few.
		corrupt := append([]byte(nil), payload...)
		// Force every disposition byte to 200 by scanning common positions.
		// This is a coarse but effective sanity check; we verify no panic.
		for i := len(corrupt) / 2; i < len(corrupt); i++ {
			corrupt[i] = 200
		}
		_, _, _, _ = parseSearchTableCompressed(corrupt, newCSTDecoder(), false)
		// Expect either an error or possible (but rare) success on accidental
		// alignment. The key invariant is no panic — runtime would catch one.
	})

	t.Run("badBaseTableSize", func(t *testing.T) {
		corrupt := append([]byte(nil), payload...)
		corrupt[2] = 0 // baseTableSize byte
		_, _, _, err := parseSearchTableCompressed(corrupt, newCSTDecoder(), false)
		if err == nil {
			t.Fatalf("expected error for invalid baseTableSize")
		}
	})
}

func TestCompressedWriterRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(123))
	// Mix of compressible and skewed regions.
	data := make([]byte, 200<<10)
	for i := range data {
		switch {
		case i%17 == 0:
			data[i] = 'A' + byte(i%26)
		case i%7 == 0:
			data[i] = byte(rng.Intn(8))
		default:
			data[i] = byte(rng.Intn(256))
		}
	}
	// Embed needles for searcher tests.
	copy(data[1024:], []byte("MAGIC_NEEDLE"))
	copy(data[50<<10:], []byte("MAGIC_NEEDLE"))

	for _, conc := range []int{1, 4} {
		t.Run(fmt.Sprintf("conc=%d", conc), func(t *testing.T) {
			cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
			var buf bytes.Buffer
			w := NewWriter(&buf,
				WriterSearchTable(cfg),
				WriterBlockSize(64<<10),
				WriterConcurrency(conc),
			)
			if _, err := w.Write(data); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			// Confirm 0x46 chunks are present in the stream.
			found46 := bytes.IndexByte(buf.Bytes(), chunkTypeSearchTableCompressed)
			if found46 < 0 {
				t.Fatalf("expected at least one 0x46 chunk in stream")
			}
			// Decode the stream.
			decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatalf("data mismatch (len=%d, want=%d)", len(decoded), len(data))
			}
		})
	}
}

// TestCompressedSearcherStats verifies the compressed-table fields in
// SearchStats are populated correctly when 0x46 chunks are present.
func TestCompressedSearcherStatsFields(t *testing.T) {
	rng := rand.New(rand.NewSource(321))
	data := make([]byte, 200<<10)
	for i := range data {
		data[i] = byte(rng.Intn(8)) // skewed so 0x46 is profitable
	}
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
	var buf bytes.Buffer
	w := NewWriter(&buf,
		WriterSearchTable(cfg),
		WriterBlockSize(64<<10),
		WriterConcurrency(1),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s := NewBlockSearcher(bytes.NewReader(buf.Bytes()), BlockSearchCollectStats())
	if err := s.Search([]byte("THIS_PROBABLY_DOES_NOT_EXIST_IN_DATA"), func(r SearchResult) error { return nil }); err != nil {
		t.Fatalf("search: %v", err)
	}
	stats := s.Stats()
	if stats.TablesCompressed == 0 {
		t.Fatalf("expected TablesCompressed > 0, got %d (TablesPresent=%d)", stats.TablesCompressed, stats.TablesPresent)
	}
	if stats.TablesCompressedBytes == 0 {
		t.Fatalf("expected TablesCompressedBytes > 0")
	}
	if stats.TableBitmapBytes == 0 {
		t.Fatalf("expected TableBitmapBytes > 0")
	}
	if stats.TablesCompressed > stats.TablesPresent {
		t.Fatalf("compressed > present: %d > %d", stats.TablesCompressed, stats.TablesPresent)
	}
	if stats.TablesCompressedBytes > stats.TablesBytes {
		t.Fatalf("compressed bytes > total bytes")
	}
}

func TestCompressedSearcherRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(456))
	data := make([]byte, 256<<10)
	for i := range data {
		data[i] = byte(rng.Intn(64))
	}
	needle := []byte("XYZNEEDLEZYX")
	// Insert needles at known offsets.
	offsets := []int{100, 1024, 70 << 10, 150 << 10}
	for _, o := range offsets {
		copy(data[o:], needle)
	}

	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
	var buf bytes.Buffer
	w := NewWriter(&buf,
		WriterSearchTable(cfg),
		WriterBlockSize(64<<10),
		WriterConcurrency(2),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s := NewBlockSearcher(bytes.NewReader(buf.Bytes()))
	var found []int64
	err := s.Search(needle, func(r SearchResult) error {
		found = append(found, r.StreamOffset)
		return nil
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(found) != len(offsets) {
		t.Fatalf("found %d matches, want %d (offsets=%v)", len(found), len(offsets), found)
	}
	for i, o := range offsets {
		if found[i] != int64(o) {
			t.Fatalf("match %d offset: got %d want %d", i, found[i], o)
		}
	}
}

// TestCompressedFallbackTo45 confirms that without CompressedSearchForce, the
// encoder emits 0x45 when 0x46 wouldn't be smaller.
func TestCompressedFallbackTo45(t *testing.T) {
	// Build a bitmap with popcount well outside the band but very high entropy
	// at the byte level — Compress4X may either compress poorly or refuse,
	// either way the 0x46 path should not beat 0x45.
	bitmap := makeBitmap(4096, 0.50, 17) // band rejects this too — both effects combine
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression()
	cfg.baseTableSize = 15
	e := newCSTEncoder()
	out, ok, _ := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if ok {
		t.Fatalf("expected fallback, but 0x46 was emitted (%d bytes)", len(out))
	}
}

// TestCompressedStatsHook validates the stats hook reports plausible values.
func TestCompressedStatsHook(t *testing.T) {
	bitmap := makeBitmap(8192, 0.10, 33)
	var got CompressedSearchStats
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(
		CompressedSearchForce(),
		CompressedSearchStatsHook(func(s CompressedSearchStats) { got = s }),
	)
	cfg.baseTableSize = 16
	e := newCSTEncoder()
	if _, ok, _ := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e); !ok {
		t.Fatalf("expected force-emitted 0x46")
	}
	if got.BitmapBytes != 8192 {
		t.Fatalf("BitmapBytes=%d", got.BitmapBytes)
	}
	if got.Huff0Blocks != 1 || got.Huff0BlockSize != 8192 {
		t.Fatalf("Huff0Blocks=%d size=%d", got.Huff0Blocks, got.Huff0BlockSize)
	}
	totalBlocks := got.BlocksOwnTable + got.BlocksGlobalTable + got.BlocksRaw + got.BlocksRLE
	if totalBlocks != got.Huff0Blocks {
		t.Fatalf("sub-block counts %d do not sum to Huff0Blocks %d", totalBlocks, got.Huff0Blocks)
	}
	if got.Chunk0x45Size == 0 || got.Chunk0x46Size == 0 || !got.Emitted0x46 {
		t.Fatalf("size stats unexpected: %+v", got)
	}
}

// TestCompressedReductionInteraction confirms encode/decode roundtrips when
// the bitmap has been reduced (its size is smaller than 1<<(baseTableSize-3)).
func TestCompressedReductionInteraction(t *testing.T) {
	// baseTableSize=18 → unreduced size = 1<<15 = 32768. With reductions=2,
	// effective size = 1<<13 = 8192.
	bitmap := makeBitmap(8192, 0.10, 99)
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
	cfg.baseTableSize = 18
	chunkBitmapRoundtrip(t, cfg, 2, bitmap)
}

// TestCompressedSmallInputRoundtrip reproduces a fuzz failure: a sub-block-size
// input with compression enabled must still decode cleanly.
func TestCompressedSmallInputRoundtrip(t *testing.T) {
	data := []byte("b9872 fox jumps ove1 the lazy2899C192aC920810070000100011010101129 fox jumps ov1 the lazy000")
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
	var buf bytes.Buffer
	w := NewWriter(&buf,
		WriterSearchTable(cfg),
		WriterBlockSize(minBlockSize),
		WriterConcurrency(1),
	)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	t.Logf("stream length: %d bytes", buf.Len())
	t.Logf("stream hex: %x", buf.Bytes())
	decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(decoded, data) {
		t.Fatalf("mismatch: got %d bytes, want %d", len(decoded), len(data))
	}
}

// FuzzCompressedSearchRoundtrip drives Writer with compression, asserts the
// Reader fully decodes the stream, AND asserts that BlockSearcher finds every
// occurrence of the supplied pattern via the 0x46 chunks.
func FuzzCompressedSearchRoundtrip(f *testing.F) {
	f.Add([]byte("the quick brown fox jumps over the lazy dog. this string is repeated. the quick brown fox jumps over the lazy dog."), []byte("fox"), 4, false)
	f.Add(bytes.Repeat([]byte("abcdefgh"), 1024), []byte("abcd"), 6, false)
	f.Add([]byte("key1=value1&key2=value2&key3=value3"), []byte("value"), 4, true)
	f.Fuzz(func(t *testing.T, data, pattern []byte, matchLen int, usePrefix bool) {
		if len(data) < 64 || len(pattern) < 1 || len(pattern) > 100 {
			return
		}
		if matchLen < 1 || matchLen > 8 {
			return
		}
		cfg := NewSearchTableConfig().WithMatchLen(matchLen).WithCompression(CompressedSearchForce())
		if usePrefix && pattern[0] != 0 {
			cfg = cfg.WithBytePrefix(pattern[0])
		}
		var buf bytes.Buffer
		w := NewWriter(&buf,
			WriterSearchTable(cfg),
			WriterBlockSize(minBlockSize),
			WriterConcurrency(1),
		)
		if _, err := w.Write(data); err != nil {
			return
		}
		if err := w.Close(); err != nil {
			return
		}

		// 1) Reader transparently skips the 0x46 chunks; output must equal input.
		decoded, err := io.ReadAll(NewReader(bytes.NewReader(buf.Bytes())))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !bytes.Equal(decoded, data) {
			t.Fatalf("roundtrip mismatch len=%d want=%d", len(decoded), len(data))
		}

		// 2) Compute reference matches (every occurrence of pattern in data;
		// for prefix tables, only occurrences preceded by the prefix byte).
		var expected []int64
		if usePrefix {
			pfx := pattern[0]
			needle := append([]byte{pfx}, pattern...)
			off := 0
			for {
				idx := bytes.Index(data[off:], needle)
				if idx < 0 {
					break
				}
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
		if len(expected) == 0 {
			return
		}

		// 3) Drive BlockSearcher — it must find every expected offset by
		// parsing the 0x46 chunks emitted above.
		searcher := NewBlockSearcher(bytes.NewReader(buf.Bytes()), BlockSearchCollectStats())
		foundSet := make(map[int64]bool, len(expected))
		if err := searcher.Search(pattern, func(r SearchResult) error {
			foundSet[r.StreamOffset] = true
			return nil
		}); err != nil {
			t.Fatalf("search: %v", err)
		}
		// At least one 0x46 chunk must have been parsed for this test to be
		// meaningful (otherwise we'd be exercising 0x45 fallback only).
		stats := searcher.Stats()
		if stats.TablesCompressed == 0 {
			t.Skipf("no 0x46 chunks emitted (matchLen=%d usePrefix=%v len=%d)", matchLen, usePrefix, len(data))
		}
		for _, e := range expected {
			if !foundSet[e] {
				t.Fatalf("expected match at offset %d not found (prefix=%v, expected=%d found=%d). "+
					"blocks: total=%d skipped=%d searched=%d compressed=%d",
					e, usePrefix, len(expected), len(foundSet),
					stats.BlocksTotal, stats.BlocksSkipped, stats.BlocksSearched, stats.TablesCompressed)
			}
		}
	})
}

// FuzzDecodeCompressedSearchChunk feeds arbitrary bytes to the 0x46 parser.
// The parser must not panic; any output is acceptable.
func FuzzDecodeCompressedSearchChunk(f *testing.F) {
	// Add some valid seeds.
	bitmap := makeBitmap(256, 0.05, 1)
	cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
	cfg.baseTableSize = 11
	e := newCSTEncoder()
	out, ok, _ := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
	if ok {
		f.Add(out[4:])
	}
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _, _ = parseSearchTableCompressed(b, newCSTDecoder(), false)
	})
}

// Benchmarks

func BenchmarkCompressedSearchEncode(b *testing.B) {
	for _, sz := range []int{4096, 8192, 64 << 10, 128 << 10, 512 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			bitmap := makeBitmap(sz, 0.10, 42)
			cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
			// Choose baseTableSize so 1<<(bts-3) == sz.
			for i := uint8(8); i <= 23; i++ {
				if 1<<(i-3) == sz {
					cfg.baseTableSize = i
					break
				}
			}
			e := newCSTEncoder()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			b.ReportAllocs()
			var dst []byte
			for b.Loop() {
				dst, _, _ = appendSearchTableCompressedChunk(dst[:0], &cfg, 0, bitmap, e)
			}
		})
	}
}

func BenchmarkCompressedSearchDecode(b *testing.B) {
	for _, sz := range []int{1 << 20} {
		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			bitmap := makeBitmap(sz, 0.10, 42)
			cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
			for i := uint8(8); i <= 23; i++ {
				if 1<<(i-3) == sz {
					cfg.baseTableSize = i
					break
				}
			}
			e := newCSTEncoder()
			out, ok, _ := appendSearchTableCompressedChunk(nil, &cfg, 0, bitmap, e)
			if !ok {
				b.Fatal("force-emit failed")
			}
			payload := out[4:]
			dec := newCSTDecoder()
			b.SetBytes(int64(sz))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _, _, _ = parseSearchTableCompressed(payload, dec, false)
			}
		})
	}
}

// BenchmarkCompressedSearchEncodeByPath drives the encoder on bitmaps tuned
// to force each disposition family, so regressions in any one path show up
// independently.
func BenchmarkCompressedSearchEncodeByPath(b *testing.B) {
	const sz = 64 << 10 // 2 huff0 blocks of 32 KiB
	rng := rand.New(rand.NewSource(0xBE))

	makeRLE := func() []byte { return make([]byte, sz) }
	makeSparse := func() []byte {
		bm := make([]byte, sz)
		for i := 0; i < sz; i += 1024 {
			bm[i] = 0x01
		}
		return bm
	}
	makeOwn := func() []byte {
		// Skewed per-block (matchable by own table only): each block has its
		// own set of frequent symbols.
		bm := make([]byte, sz)
		for i := 0; i < 32<<10; i++ {
			bm[i] = byte(rng.Intn(4)) // block 0: symbols 0..3
		}
		for i := 32 << 10; i < sz; i++ {
			bm[i] = byte(200 + rng.Intn(4)) // block 1: symbols 200..203
		}
		return bm
	}
	makeGlobal := func() []byte {
		// Same skewed distribution in both blocks: global table wins.
		bm := make([]byte, sz)
		for i := range bm {
			bm[i] = byte(rng.Intn(8))
		}
		return bm
	}

	cases := []struct {
		name   string
		bitmap []byte
	}{
		{"rle", makeRLE()},
		{"sparse", makeSparse()},
		{"own", makeOwn()},
		{"global", makeGlobal()},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			cfg := NewSearchTableConfig().WithMatchLen(4).WithCompression(CompressedSearchForce())
			cfg.baseTableSize = 19
			e := newCSTEncoder()
			b.SetBytes(int64(sz))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _, _ = appendSearchTableCompressedChunk(nil, &cfg, 0, c.bitmap, e)
			}
		})
	}
}

// Quick smoke benchmark covering the Writer→Reader path with compression on
// vs off so the developer can eyeball wire-size differences.
func BenchmarkCompressedSearchVsUncompressed(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	data := make([]byte, 8<<20)
	for i := range data {
		data[i] = byte(rng.Intn(4)) // skewed → good for compression
	}
	run := func(b *testing.B, useCompression bool) {
		cfg := NewSearchTableConfig().WithMatchLen(4)
		if !useCompression {
			cfg = cfg.WithoutCompression()
		} else {
			cfg = cfg.WithCompression(CompressedSearchForce())
		}
		var lastLen int
		var buf bytes.Buffer
		buf.Grow(len(data))
		w := NewWriter(&buf,
			WriterLevel(LevelFastest),
			WriterSearchTable(cfg),
			WriterBlockSize(64<<10),
			WriterConcurrency(runtime.GOMAXPROCS(0)),
		)
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			buf.Reset()
			w.Reset(&buf)
			_ = w.EncodeBuffer(data)
			_ = w.Close()
			lastLen = buf.Len()
		}
		// Use lastLen so the compiler doesn't elide the buffer.
		_ = lastLen
	}
	b.Run("uncompressed", func(b *testing.B) { run(b, false) })
	b.Run("compressed", func(b *testing.B) { run(b, true) })
}

// Quick smoke benchmark covering the Reader→search path with compression on
// vs off so the developer can eyeball search throughput differences. The
// pattern is absent from the random data and lives outside the byte range
// the data uses, so all blocks are skipped via the search table and the
// per-block bitmap decode + lookup dominates the measured cost.
func BenchmarkSearchCompressedVsUncompressed(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	data := make([]byte, 256<<20)
	for i := range data {
		data[i] = byte(rng.Intn(4))
	}
	pattern := []byte("needle-not-present")
	build := func(useCompression bool) []byte {
		cfg := NewSearchTableConfig().WithMatchLen(4)
		if !useCompression {
			cfg = cfg.WithoutCompression()
		} else {
			cfg = cfg.WithCompression(CompressedSearchForce())
		}
		var buf bytes.Buffer
		buf.Grow(len(data))
		w := NewWriter(&buf,
			WriterLevel(LevelFastest),
			WriterSearchTable(cfg),
			WriterBlockSize(64<<10),
			WriterConcurrency(1),
		)
		_, _ = w.Write(data)
		_ = w.Close()
		return buf.Bytes()
	}
	run := func(b *testing.B, stream []byte) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			s := NewBlockSearcher(bytes.NewReader(stream))
			_ = s.Search(pattern, func(SearchResult) error { return nil })
		}
	}
	streamU := build(false)
	streamC := build(true)
	b.Run("uncompressed", func(b *testing.B) { run(b, streamU) })
	b.Run("compressed", func(b *testing.B) { run(b, streamC) })
}

// Suppress unused-import warning when -tags ... drops the strings import.
var _ = strings.HasPrefix
