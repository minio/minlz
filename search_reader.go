package minlz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
)

// ErrSearchTablesUnusable is returned by BlockSearcher.Search when
// BailOnMissing is set and search tables cannot accelerate the query.
var ErrSearchTablesUnusable = errors.New("minlz: search tables cannot be used for this pattern")

// SearchStats contains statistics from a BlockSearcher.Search call.
type SearchStats struct {
	BlocksTotal           int     // Total data blocks encountered
	BlocksSkipped         int     // Blocks skipped via search table (definitely no match)
	BlocksSearched        int     // Blocks decoded and passed to callback
	CompBytesSkipped      int64   // Compressed bytes skipped
	UncompBytesSearched   int64   // Uncompressed bytes decoded and searched
	TablesPresent         int     // Number of 0x45 + 0x46 search table chunks seen
	TablesBytes           int64   // Total wire bytes of search table chunks (0x45 + 0x46)
	TablesMissing         int     // Data blocks without a preceding search table
	TablesUnusable        int     // Tables present but incompatible with query
	TableBitsSum          int     // Sum of effective table bits (for avg: TableBitsSum/TablesPresent)
	TableReductionsSum    int     // Sum of reductions applied (for avg: TableReductionsSum/TablesPresent)
	BlocksDeferred        int     // Blocks that entered the deferred path
	BlocksDeferredSkipped int     // Deferred blocks ultimately skipped (subset of BlocksSkipped)
	BlocksFalsePositive   int     // Blocks decoded due to table match but containing no actual matches
	BlocksBoundaryScanned int     // Blocks the table proved free of the pattern but decoded anyway, to rule out a match straddling in from the previous block (subset of BlocksSearched). A high count means boundary checks are forcing scans that the tables alone would have skipped.
	TablePopMin           float64 // Min population % across tables seen
	TablePopMax           float64 // Max population % across tables seen
	TablePopSum           float64 // Sum of population % (for computing average)
	UncompressedSize      int64   // Total uncompressed bytes in stream

	// Compressed search-table (chunk type 0x46) breakdown. These count a
	// subset of TablesPresent / TablesBytes.
	TablesCompressed      int   // Number of 0x46 chunks seen
	TablesCompressedBytes int64 // Total wire bytes of 0x46 chunks (subset of TablesBytes)
	TableBitmapBytes      int64 // Total uncompressed bitmap bytes across 0x46 chunks

	// Compressed sub-block breakdown for the 0x46 chunks above.
	CompressedBlocksTotal  int // Sum of compressed sub-blocks across all 0x46 chunks
	CompressedBlocksRaw    int // Sub-blocks with disposition = raw
	CompressedBlocksRLE    int // Sub-blocks with disposition = RLE
	CompressedBlocksSparse int // Sub-blocks with disposition = sparse bit table
	CompressedTablesSum    int // Sum of distinct tables emitted across all 0x46 chunks

	// Wire-byte breakdown of 0x46 sub-block payloads (excludes the disposition
	// byte itself; for tabled blocks includes the uvarint length prefix and
	// compressed data, but NOT the table header — that's tracked separately).
	CompressedBytesTabled       int64
	CompressedBytesRaw          int64
	CompressedBytesRLE          int64
	CompressedBytesSparse       int64
	CompressedBytesTableHeaders int64 // bytes consumed by serialized tables

	// Windows holds per-pattern-window table-presence counts, populated when
	// stats collection is enabled. Each entry is one matchLen-window the
	// searcher tests against every block's table; Present/Absent count the
	// tables whose bitmap had that window's hash set/clear. Rendered by
	// FprintExtended — useful for seeing how selective the chosen matchLen and
	// prefix are for a given pattern (the first prefix window is the skip gate).
	Windows []WindowStat
}

// WindowStat reports, across every per-block search table, how often one
// pattern window's hash bit was set ("present", block might contain it) or
// clear ("absent", block definitely lacks it).
type WindowStat struct {
	Pos        int    // start index of the window within the pattern
	Prefix     int    // pattern index of the preceding prefix byte; -1 = raw / no-prefix anchor
	PrefixByte byte   // the prefix byte (0 when Prefix < 0)
	Bytes      []byte // the matchLen bytes that get hashed
	Present    int    // tables with the bit set
	Absent     int    // tables with the bit clear
}

// Fprint writes a human-readable summary of the search stats to w.
func (st SearchStats) Fprint(w io.Writer) {
	total := st.BlocksTotal
	if total == 0 {
		total = 1 // avoid division by zero
	}
	pct := func(n, d int) float64 { return 100 * float64(n) / float64(d) }
	fmt.Fprintf(w, "Blocks total: %d, skipped: %d (%.1f%%), deferred: %d (%.1f%%, %d skipped)\n",
		st.BlocksTotal, st.BlocksSkipped, pct(st.BlocksSkipped, total),
		st.BlocksDeferred, pct(st.BlocksDeferred, total), st.BlocksDeferredSkipped)
	searched := max(st.BlocksSearched, 1)
	fmt.Fprintf(w, "Blocks searched: %d (%.1f%%), false positive: %d (%.1f%%)\n",
		st.BlocksSearched, pct(st.BlocksSearched, total),
		st.BlocksFalsePositive, pct(st.BlocksFalsePositive, searched))
	if st.BlocksBoundaryScanned > 0 {
		fmt.Fprintf(w, "Blocks boundary-scanned: %d (%.1f%% of searched) — table-absent but decoded to rule out a prev-block straddle\n",
			st.BlocksBoundaryScanned, pct(st.BlocksBoundaryScanned, searched))
	}
	fmt.Fprintf(w, "Bytes skipped: %d compressed, searched: %d uncompressed\n", st.CompBytesSkipped, st.UncompBytesSearched)
	fmt.Fprintf(w, "Tables: %d present, %d missing, %d unusable\n", st.TablesPresent, st.TablesMissing, st.TablesUnusable)
	if st.TablesPresent > 0 {
		avgBits := float64(st.TableBitsSum) / float64(st.TablesPresent)
		avgRed := float64(st.TableReductionsSum) / float64(st.TablesPresent)
		bitsPerByte := float64(st.TablesBytes) * 8 / float64(st.UncompressedSize)
		fmt.Fprintf(w, "Table bits/byte: %.4f, log2: %.1f, avg reductions: %.1f\n", bitsPerByte, avgBits, avgRed)
		fmt.Fprintf(w, "Table total: %d bytes, avg %d bytes/table", st.TablesBytes, st.TablesBytes/int64(st.TablesPresent))
		if st.UncompressedSize > 0 {
			fmt.Fprintf(w, ", %.2f%% of %d uncompressed", float64(st.TablesBytes)*100/float64(st.UncompressedSize), st.UncompressedSize)
		}
		fmt.Fprintln(w)
		avg := st.TablePopSum / float64(st.TablesPresent)
		fmt.Fprintf(w, "Table population: avg %.1f%%, min %.1f%%, max %.1f%%\n", avg, st.TablePopMin, st.TablePopMax)
		uncompressed := st.TablesPresent - st.TablesCompressed
		if uncompressed > 0 || st.TablesCompressed > 0 {
			fmt.Fprintf(w, "Table types: %d uncompressed (0x45), %d compressed (0x46)\n",
				uncompressed, st.TablesCompressed)
		}
		if st.TablesCompressed > 0 {
			ratio := 100.0
			if st.TableBitmapBytes > 0 {
				ratio = float64(st.TablesCompressedBytes) * 100 / float64(st.TableBitmapBytes)
			}
			fmt.Fprintf(w, "Compressed tables: %d (%.1f%% of total), %d wire bytes, %d uncompressed bitmap bytes (%.2f%% ratio)\n",
				st.TablesCompressed, pct(st.TablesCompressed, st.TablesPresent),
				st.TablesCompressedBytes, st.TableBitmapBytes, ratio)
			tabled := st.CompressedBlocksTotal - st.CompressedBlocksRaw - st.CompressedBlocksRLE - st.CompressedBlocksSparse
			share := 0.0
			if tabled > 0 {
				share = float64(st.CompressedTablesSum) / float64(tabled)
			}
			fmt.Fprintf(w, " Sub-blocks: %d total (%d tabled, %d raw, %d RLE, %d sparse); %d tables emitted (share=%.2f tables/tabled-block)\n",
				st.CompressedBlocksTotal, tabled, st.CompressedBlocksRaw, st.CompressedBlocksRLE, st.CompressedBlocksSparse, st.CompressedTablesSum, share)
			fmt.Fprintf(w, " Payload bytes: tabled=%d raw=%d RLE=%d sparse=%d; table-header bytes=%d\n",
				st.CompressedBytesTabled, st.CompressedBytesRaw, st.CompressedBytesRLE, st.CompressedBytesSparse, st.CompressedBytesTableHeaders)
		}
	}
}

// FprintExtended writes the standard Fprint summary followed by a per-window
// breakdown of how often each of the pattern's matchLen-windows was present in
// the per-block tables. The first prefix window is the skip "gate"; the raw
// fallback (prefix < 0) anchors a match whose prefix byte sits in the previous
// block. Requires stats collection to have populated Windows.
func (st SearchStats) FprintExtended(w io.Writer) {
	st.Fprint(w)
	if len(st.Windows) == 0 {
		return
	}
	denom := st.Windows[0].Present + st.Windows[0].Absent
	if denom == 0 {
		denom = 1
	}
	// The raw-fallback role only applies to prefix tables (where a separate
	// Prefix<0 window is appended); for no-prefix tables every window has
	// Prefix<0 and is just a sequential position, so don't label those.
	hasPrefixWindows := false
	for _, ws := range st.Windows {
		if ws.Prefix >= 0 {
			hasPrefixWindows = true
			break
		}
	}
	fmt.Fprintf(w, "Pattern windows (per-hash presence across %d tables):\n", st.Windows[0].Present+st.Windows[0].Absent)
	fmt.Fprintf(w, "  win  pat-pos  window           present            role\n")
	for i, ws := range st.Windows {
		role := ""
		switch {
		case i == 0:
			role = "gate"
		case ws.Prefix < 0 && hasPrefixWindows:
			role = "raw fallback"
		}
		fmt.Fprintf(w, "  %-4d %-8d %-16q %7d (%5.1f%%)   %s\n",
			i, ws.Pos, string(ws.Bytes),
			ws.Present, 100*float64(ws.Present)/float64(denom), role)
	}
}

// ErrSearchForward can be returned from the search callback to request
// forward context. The searcher will decode the next block, shift Blocks
// forward, and re-call the callback with the same match but more context.
var ErrSearchForward = errors.New("minlz: forward to next block")

// SearchResult is passed to the search callback for each pattern match.
type SearchResult struct {
	// Blocks contains the block data surrounding the match.
	// Blocks[0] is the previous block (nil if previous was skipped or lazy).
	// Blocks[1] is the current block. The match may start in Blocks[0] (boundary match).
	// Both slices are invalid after the callback returns.
	Blocks [2][]byte

	// Offset is the position of the match relative to PrevBlock()+Blocks[1].
	// When PrevBlock() returns nil, Offset is relative to just Blocks[1].
	Offset int

	// StreamOffset is the absolute uncompressed stream offset of the match.
	StreamOffset int64

	// BlockStart is the stream offset of PrevBlock() data (or Blocks[1] if no prev).
	// Invariant: Offset == int(StreamOffset - BlockStart)
	BlockStart int64

	// PrevBlockLen is the decompressed size of the previous block.
	// This equals len(PrevBlock()) but avoids forcing a lazy decode.
	PrevBlockLen int

	prevLazy *lazyBlock // lazily decompressible previous block; nil if not available
}

// PrevBlock returns the previous block's decompressed data.
// Returns Blocks[0] if non-nil, otherwise lazily decompresses the previous
// block if available. Returns nil if no previous block exists.
func (r SearchResult) PrevBlock() []byte {
	if r.Blocks[0] != nil {
		return r.Blocks[0]
	}
	if r.prevLazy != nil {
		return r.prevLazy.decode()
	}
	return nil
}

// lazyBlock holds compressed block data for on-demand decompression. It
// has two modes:
//   - in-memory: chunkData holds the chunk payload (checksum + body).
//     Used by BlockSearcher for inline streams.
//   - remote: main + mainOffset point at the chunk header in an io.ReaderAt.
//     Used by SidecarSearcher.
type lazyBlock struct {
	chunkData []byte // in-memory: chunk payload: checksum + compressed/uncompressed data
	chunkType byte   // in-memory mode only

	// Remote mode (sidecar): when main is non-nil, decode() fetches and
	// decodes the chunk at mainOffset via ReadAt.
	main       io.ReaderAt
	mainOffset int64
	maxBlock   int

	decompLen int    // decompressed size; avoids decode to get length
	decoded   []byte // cached result; nil until first decode
	ignoreCRC bool
}

func (lb *lazyBlock) decode() []byte {
	if lb.decoded != nil {
		return lb.decoded
	}
	if lb.main != nil {
		d, err := readAndDecodeMainBlock(lb.main, lb.mainOffset, lb.decompLen, lb.maxBlock, lb.ignoreCRC)
		if err != nil {
			return nil
		}
		lb.decoded = d
		return d
	}
	buf := lb.chunkData
	if len(buf) < checksumSize {
		return nil
	}
	checksum := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
	buf = buf[checksumSize:]

	switch lb.chunkType {
	case chunkTypeUncompressedData:
		if !lb.ignoreCRC && crc(buf) != checksum {
			return nil
		}
		lb.decoded = make([]byte, len(buf))
		copy(lb.decoded, buf)
	default:
		n, hdrLen, err := decodedLen(buf)
		if err != nil || n != lb.decompLen {
			return nil
		}
		dst := make([]byte, n)
		if minLZDecode(dst, buf[hdrLen:]) != 0 {
			return nil
		}
		toCRC := dst
		if lb.chunkType == chunkTypeMinLZCompressedDataCompCRC {
			toCRC = buf
		}
		if !lb.ignoreCRC && crc(toCRC) != checksum {
			return nil
		}
		lb.decoded = dst
	}
	return lb.decoded
}

// pendingBlock holds a block whose decode is deferred until the next
// block's search table can confirm or deny a boundary match.
type pendingBlock struct {
	lazy         lazyBlock
	blockStart   int64    // stream offset where this block's decompressed data starts
	deferHashes  []uint32 // absent window hashes at full baseTableSize resolution
	tableNoMatch bool     // table said no match (decoded only for boundary check)
}

type deferredMatch struct {
	streamOff int64  // absolute stream offset of the match
	blockOff  int64  // block start of the block containing the match
	matchOff  int    // offset within that block
	blk       []byte // the block containing the match; becomes Blocks[0] in re-call
}

// BlockSearcher reads a MinLZ stream and searches for patterns using
// per-block search tables to skip blocks that definitely don't contain the pattern.
type BlockSearcher struct {
	r   io.Reader
	err error
	buf []byte
	tmp [16]byte

	streamInfo *SearchTableConfig // from 0x44 chunk
	// Per-block search table state (from most recent 0x45 chunk)
	blockTable      []byte
	blockReductions uint8
	blockInfo       SearchTableConfig
	// blockTableUnusable is set when a 0x46 chunk was parsed but its bitmap
	// was skipped (config alone proved the pattern unanswerable). The next
	// data block bumps TablesUnusable and clears this flag.
	blockTableUnusable bool

	decoded   [2][]byte // alternating decode buffers
	decIdx    int       // which buffer was last used (0 or 1)
	prevBlock []byte    // points into decoded[(decIdx+1)&1], nil if skipped
	// tailBuf holds the last len(pattern)-1 bytes of the current contiguous run
	// of scanned blocks, so a match straddling 3+ blocks (e.g. across a short
	// Flush block whose length is < len(pattern)-1) is still found by the
	// boundary scan. tailOff is its stream offset; bbuf is reused scratch for
	// the boundary region. Reset whenever a block is skipped or deferred
	// (i.e. not decoded), which breaks contiguity with the next block.
	tailBuf []byte
	tailOff int64
	bbuf    []byte
	// prevLazy is *lazyBlock so it can carry a nil sentinel meaning "no
	// lazy prev." When set by bufferSkippedBlock / resolvePending, it
	// always points at &prevLazyStore — the backing storage is reused
	// across iterations to avoid a per-skipped-block heap alloc.
	prevLazy      *lazyBlock
	prevLazyStore lazyBlock
	deferred      *deferredMatch // pending ErrSearchForward re-dispatch
	pending       *pendingBlock  // block deferred pending next table check
	cstDec        *cstDecoder    // lazy decoder state for 0x46 chunks
	// tailRescue is set when the most recently processed block was skipped only
	// because its (prefix) search table was all-zero. That skip is sound for any
	// block with forward overlap, but a stream's final block is tabled without
	// overlap (SPEC §B.4.3), so a prefix occurrence in its last bytes can be
	// unindexed. The block stays buffered in prevLazy; if it turns out to be the
	// stream's last block (end-of-stream reached with this still set) it is
	// decoded and scanned instead of skipped. Cleared when a later block is processed.
	tailRescue    bool
	tailRescueOff int64 // stream offset where the pending block starts
	infoCallback  func(SearchTableConfig)
	maxBlock      int
	readHeader    bool
	ignoreCRC     bool
	bail          bool // return error if tables can't answer query
	collectStats  bool
	blockStart    int64
	blockMatches  int // matches found in current block (for false positive tracking)
	stats         SearchStats
	// searchWindows is the pattern's matchLen-windows, enumerated once when the
	// first usable table is seen; per-table presence is tallied into
	// stats.Windows. Only populated under collectStats. winCfg is the layout
	// they were enumerated for; tallying stops if a later table's config differs
	// (mixing layouts would mislabel the counts).
	searchWindows []windowSpec
	winInit       bool
	winCfg        SearchTableConfig
}

// BlockSearchOption configures a BlockSearcher.
type BlockSearchOption func(*BlockSearcher) error

// BlockSearchBailOnMissing makes Search return ErrSearchTablesUnusable
// if search tables are not available or not compatible with the pattern.
func BlockSearchBailOnMissing() BlockSearchOption {
	return func(s *BlockSearcher) error {
		s.bail = true
		return nil
	}
}

// BlockSearchInfoCallback installs fn to be invoked when a Search Table Info
// chunk (0x44) is parsed from the stream. The callback receives the parsed
// SearchTableConfig and is intended for logging / introspection. The callback
// must not retain references to mutable state inside the config.
func BlockSearchInfoCallback(fn func(SearchTableConfig)) BlockSearchOption {
	return func(s *BlockSearcher) error {
		s.infoCallback = fn
		return nil
	}
}

// BlockSearchIgnoreCRC skips CRC validation during search.
func BlockSearchIgnoreCRC() BlockSearchOption {
	return func(s *BlockSearcher) error {
		s.ignoreCRC = true
		return nil
	}
}

// BlockSearchCollectStats enables collection of search statistics.
// When enabled, Stats() returns detailed metrics after Search completes.
func BlockSearchCollectStats() BlockSearchOption {
	return func(s *BlockSearcher) error {
		s.collectStats = true
		return nil
	}
}

// BlockSearchMaxBlockSize limits the maximum block size the searcher will decode.
func BlockSearchMaxBlockSize(n int) BlockSearchOption {
	return func(s *BlockSearcher) error {
		if n > maxBlockSize || n < minBlockSize {
			return fmt.Errorf("minlz: invalid block size")
		}
		s.maxBlock = n
		return nil
	}
}

// NewBlockSearcher creates a BlockSearcher for the given reader.
func NewBlockSearcher(r io.Reader, opts ...BlockSearchOption) *BlockSearcher {
	s := &BlockSearcher{
		r:        r,
		maxBlock: maxBlockSize,
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			s.err = err
			return s
		}
	}
	return s
}

// Stats returns search statistics accumulated during the last Search call.
func (s *BlockSearcher) Stats() SearchStats {
	return s.stats
}

// Search iterates blocks in the stream, calling fn for each pattern match.
//
// The callback receives a SearchResult with the match offset and surrounding
// block data. Return nil to continue, ErrSearchForward to request the next
// block for forward context (re-calls fn with shifted Blocks), or any other
// error to abort.
func (s *BlockSearcher) Search(pattern []byte, fn func(r SearchResult) error) error {
	if s.err != nil {
		return s.err
	}
	s.stats = SearchStats{}
	s.deferred = nil
	s.pending = nil
	s.prevLazy = nil
	s.tailBuf = s.tailBuf[:0]
	s.tailRescue = false
	s.winInit = false

	if s.collectStats {
		defer func() { s.stats.UncompressedSize = s.blockStart }()
	}

	for {
		if !s.readFull(s.tmp[:4]) {
			if s.err == io.EOF {
				if s.pending != nil {
					if err := s.resolvePending(pattern, fn); err != nil {
						return err
					}
				}
				if err := s.rescueTail(pattern, fn); err != nil {
					return err
				}
				if s.deferred != nil {
					if err := s.flushDeferred(nil, fn); err != nil {
						return err
					}
				}
				return nil
			}
			return s.err
		}
		chunkType := s.tmp[0]
		chunkLen := int(s.tmp[1]) | int(s.tmp[2])<<8 | int(s.tmp[3])<<16

		if !s.readHeader {
			if chunkType == ChunkTypeStreamIdentifier {
				s.readHeader = true
			} else if chunkType <= maxNonSkippableChunk && chunkType != chunkTypeEOF {
				return ErrCorrupt
			}
		}

		switch chunkType {
		case ChunkTypeStreamIdentifier:
			if chunkLen != magicBodyLen {
				return ErrCorrupt
			}
			if !s.readFull(s.tmp[:magicBodyLen]) {
				return s.err
			}
			if string(s.tmp[:len(magicBody)]) != magicBody {
				return ErrUnsupported
			}
			logSize := int(s.tmp[magicBodyLen-1]) + 10
			blockSize := 1 << logSize
			if blockSize > maxBlockSize {
				return ErrCorrupt
			}
			// A concatenated stream starts here. Scan any held-back tail block
			// from the prior stream, flush any pending forward-context dispatch,
			// and drop per-stream boundary state so matches and offsets don't
			// straddle into the new stream (which restarts at offset 0).
			if err := s.rescueTail(pattern, fn); err != nil {
				return err
			}
			if s.deferred != nil {
				if err := s.flushDeferred(nil, fn); err != nil {
					return err
				}
			}
			s.maxBlock = blockSize
			s.blockStart = 0
			s.pending = nil
			s.prevBlock = nil
			s.prevLazy = nil
			s.tailBuf = s.tailBuf[:0]
			s.blockTable = nil
			s.blockTableUnusable = false
			continue

		case chunkTypeSearchInfo:
			s.ensureBuf(chunkLen)
			if !s.readFull(s.buf[:chunkLen]) {
				return s.err
			}
			cfg, err := parseSearchInfo(s.buf[:chunkLen])
			if err != nil {
				return err
			}
			s.streamInfo = &cfg
			if s.infoCallback != nil {
				s.infoCallback(cfg)
			}
			// Preallocate the compressed-table bitmap buffer to the stream's
			// maximum (un-reduced) bitmap size so subsequent 0x46 parses don't
			// grow it.
			if cfg.baseTableSize >= 3 {
				maxBitmap := 1 << (cfg.baseTableSize - 3)
				if s.cstDec == nil {
					s.cstDec = newCSTDecoder()
				}
				if cap(s.cstDec.bitmapBuf) < maxBitmap {
					s.cstDec.bitmapBuf = make([]byte, maxBitmap)
				}
			}
			continue

		case chunkTypeSearchTable:
			s.ensureBuf(chunkLen)
			if !s.readFull(s.buf[:chunkLen]) {
				return s.err
			}
			cfg, reductions, table, err := parseSearchTable(s.buf[:chunkLen], s.ignoreCRC)
			if err != nil {
				return err
			}
			s.blockInfo = cfg
			s.blockReductions = reductions
			s.blockTable = table
			s.blockTableUnusable = false
			// Resolve any pending block against this new table.
			if s.pending != nil {
				if err := s.resolvePending(pattern, fn); err != nil {
					return err
				}
			}
			if s.collectStats {
				if !s.winInit {
					s.winCfg = cfg
					s.searchWindows = s.stats.initWindows(&cfg, pattern)
					s.winInit = true
				} else if s.searchWindows != nil && !sameSearchLayout(&s.winCfg, &cfg) {
					s.searchWindows = nil // config drift: stop, mixed layouts would mislabel
				}
				s.stats.tallyWindows(s.searchWindows, cfg.baseTableSize, reductions, table)
				s.stats.TablesPresent++
				s.stats.TablesBytes += int64(chunkLen + 4) // +4 for chunk header
				s.stats.TableBitsSum += int(cfg.baseTableSize - reductions)
				s.stats.TableReductionsSum += int(reductions)
				setBits := 0
				for _, b := range table {
					setBits += bits.OnesCount8(b)
				}
				pop := float64(setBits) * 100 / float64(len(table)*8)
				s.stats.TablePopSum += pop
				if s.stats.TablesPresent == 1 || pop < s.stats.TablePopMin {
					s.stats.TablePopMin = pop
				}
				if pop > s.stats.TablePopMax {
					s.stats.TablePopMax = pop
				}
			}
			continue

		case chunkTypeSearchTableCompressed:
			s.ensureBuf(chunkLen)
			if !s.readFull(s.buf[:chunkLen]) {
				return s.err
			}
			headCfg, headRed, err := parseSearchTableCompressedHeader(s.buf[:chunkLen])
			if err != nil {
				return err
			}
			if !patternCanUseConfig(&headCfg, pattern) {
				// Bitmap is irrelevant for this pattern — skip the expensive
				// huff0/sparse decode. The next data block bumps TablesUnusable.
				s.blockInfo = headCfg
				s.blockReductions = headRed
				s.blockTable = nil
				s.blockTableUnusable = true
				if s.pending != nil {
					if err := s.resolvePending(pattern, fn); err != nil {
						return err
					}
				}
				if s.collectStats {
					s.stats.TablesPresent++
					s.stats.TablesBytes += int64(chunkLen + 4)
					s.stats.TablesCompressed++
					s.stats.TablesCompressedBytes += int64(chunkLen + 4)
					s.stats.TableBitmapBytes += int64(1 << (headCfg.baseTableSize - headRed - 3))
					s.stats.TableBitsSum += int(headCfg.baseTableSize - headRed)
					s.stats.TableReductionsSum += int(headRed)
				}
				continue
			}
			if s.cstDec == nil {
				s.cstDec = newCSTDecoder()
			}
			cfg, reductions, table, err := parseSearchTableCompressed(s.buf[:chunkLen], s.cstDec, s.ignoreCRC)
			if err != nil {
				return err
			}
			s.blockInfo = cfg
			s.blockReductions = reductions
			s.blockTable = table
			s.blockTableUnusable = false
			if s.pending != nil {
				if err := s.resolvePending(pattern, fn); err != nil {
					return err
				}
			}
			if s.collectStats {
				if !s.winInit {
					s.winCfg = cfg
					s.searchWindows = s.stats.initWindows(&cfg, pattern)
					s.winInit = true
				} else if s.searchWindows != nil && !sameSearchLayout(&s.winCfg, &cfg) {
					s.searchWindows = nil // config drift: stop, mixed layouts would mislabel
				}
				s.stats.tallyWindows(s.searchWindows, cfg.baseTableSize, reductions, table)
				s.stats.TablesPresent++
				s.stats.TablesBytes += int64(chunkLen + 4)
				s.stats.TablesCompressed++
				s.stats.TablesCompressedBytes += int64(chunkLen + 4)
				s.stats.TableBitmapBytes += int64(len(table))
				s.stats.CompressedBlocksTotal += s.cstDec.lastBlocks
				s.stats.CompressedBlocksRaw += s.cstDec.lastBlocksRaw
				s.stats.CompressedBlocksRLE += s.cstDec.lastBlocksRLE
				s.stats.CompressedBlocksSparse += s.cstDec.lastBlocksSparse
				s.stats.CompressedTablesSum += s.cstDec.lastTables
				s.stats.CompressedBytesTabled += int64(s.cstDec.lastBytesTabled)
				s.stats.CompressedBytesRaw += int64(s.cstDec.lastBytesRaw)
				s.stats.CompressedBytesRLE += int64(s.cstDec.lastBytesRLE)
				s.stats.CompressedBytesSparse += int64(s.cstDec.lastBytesSparse)
				s.stats.CompressedBytesTableHeaders += int64(s.cstDec.lastBytesTableHeader)
				s.stats.TableBitsSum += int(cfg.baseTableSize - reductions)
				s.stats.TableReductionsSum += int(reductions)
				setBits := 0
				for _, b := range table {
					setBits += bits.OnesCount8(b)
				}
				pop := float64(setBits) * 100 / float64(len(table)*8)
				s.stats.TablePopSum += pop
				if s.stats.TablesPresent == 1 || pop < s.stats.TablePopMin {
					s.stats.TablePopMin = pop
				}
				if pop > s.stats.TablePopMax {
					s.stats.TablePopMax = pop
				}
			}
			continue

		case chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC:
			if chunkLen < checksumSize {
				return ErrCorrupt
			}
			// Resolve any pending block that wasn't resolved at table arrival.
			if s.pending != nil {
				if err := s.resolvePending(pattern, fn); err != nil {
					return err
				}
			}

			if s.collectStats {
				s.stats.BlocksTotal++
			}
			s.tailRescue = false
			tableNoMatch := false
			deferrable := false
			if s.blockTable != nil {
				savedTable := s.blockTable
				savedReductions := s.blockReductions
				savedInfo := s.blockInfo
				canUse, match := patternCanMatch(&savedInfo, savedTable, savedReductions, pattern)
				s.blockTable = nil
				if canUse && !match {
					if len(s.tailBuf) == 0 || !canBoundaryMatch(s.tailBuf, pattern) {
						// Definite skip — buffer compressed data for lazy PrevBlock.
						decompLen, err := s.bufferSkippedBlock(chunkType, chunkLen)
						if err != nil {
							return err
						}
						if s.collectStats {
							s.stats.BlocksSkipped++
							s.stats.CompBytesSkipped += int64(chunkLen)
						}
						// An all-zero table can't prove absence in a no-overlap
						// final block; remember it so EOF scans this block.
						s.tailRescue = tableAllZero(savedTable)
						s.tailRescueOff = s.blockStart
						s.blockStart += int64(decompLen)
						if s.deferred != nil {
							if err := s.flushDeferred(nil, fn); err != nil {
								return err
							}
						}
						s.prevBlock = nil
						s.tailBuf = s.tailBuf[:0]
						continue
					}
					tableNoMatch = true
				}
				if canUse && match {
					// Don't defer if the previous block could have a boundary
					// match into this block — deferral would skip that check.
					dh := patternDeferHashes(&savedInfo, savedTable, savedReductions, pattern)
					if dh != nil && (len(s.tailBuf) == 0 || !canBoundaryMatch(s.tailBuf, pattern)) {
						deferrable = true
						// Buffer compressed data and defer decode.
						decompLen, err := s.bufferSkippedBlock(chunkType, chunkLen)
						if err != nil {
							return err
						}
						s.pending = &pendingBlock{
							lazy:        *s.prevLazy,
							blockStart:  s.blockStart,
							deferHashes: dh,
						}
						s.prevLazy = nil
						s.blockStart += int64(decompLen)
						if s.collectStats {
							s.stats.BlocksDeferred++
						}
						s.prevBlock = nil
						s.tailBuf = s.tailBuf[:0]
						continue
					}
				}
				if !canUse {
					if s.collectStats {
						s.stats.TablesUnusable++
					}
					if s.bail {
						return ErrSearchTablesUnusable
					}
				}
			} else if s.blockTableUnusable {
				s.blockTableUnusable = false
				if s.collectStats {
					s.stats.TablesUnusable++
				}
				if s.bail {
					return ErrSearchTablesUnusable
				}
			} else {
				if s.collectStats {
					s.stats.TablesMissing++
				}
				if s.bail {
					return ErrSearchTablesUnusable
				}
			}

			_ = deferrable
			s.ensureBuf(chunkLen)
			if !s.readFull(s.buf[:chunkLen]) {
				return s.err
			}
			buf := s.buf[:chunkLen]
			checksum := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
			buf = buf[checksumSize:]

			n, hdrLen, err := decodedLen(buf)
			if err != nil {
				return err
			}
			if n > s.maxBlock {
				return ErrTooLarge
			}
			di := s.decIdx ^ 1
			if n > len(s.decoded[di]) {
				s.decoded[di] = make([]byte, n)
			}
			buf = buf[hdrLen:]
			if ret := minLZDecode(s.decoded[di][:n], buf); ret != 0 {
				return ErrCorrupt
			}
			toCRC := s.decoded[di][:n]
			if chunkType == chunkTypeMinLZCompressedDataCompCRC {
				toCRC = buf
			}
			if !s.ignoreCRC && crc(toCRC) != checksum {
				return ErrCRC
			}

			if s.collectStats {
				s.stats.BlocksSearched++
				s.stats.UncompBytesSearched += int64(n)
			}
			s.blockMatches = 0
			blockOff := s.blockStart
			s.blockStart += int64(n)
			blk := s.decoded[di][:n]
			s.decIdx = di
			if s.deferred != nil {
				if err := s.flushDeferred(blk, fn); err != nil {
					return err
				}
			}
			if err := s.dispatchMatches(blk, blockOff, pattern, fn); err != nil {
				return err
			}
			if s.collectStats && s.blockMatches == 0 {
				s.stats.BlocksFalsePositive++
			}
			if tableNoMatch {
				if s.collectStats {
					s.stats.BlocksBoundaryScanned++
				}
				s.prevBlock = nil
			} else {
				s.prevBlock = blk
			}
			s.prevLazy = nil
			continue

		case chunkTypeUncompressedData:
			if chunkLen < checksumSize {
				return ErrCorrupt
			}
			if s.pending != nil {
				if err := s.resolvePending(pattern, fn); err != nil {
					return err
				}
			}

			if s.collectStats {
				s.stats.BlocksTotal++
			}
			s.tailRescue = false
			tableNoMatch := false
			if s.blockTable != nil {
				savedTable := s.blockTable
				canUse, match := patternCanMatch(&s.blockInfo, savedTable, s.blockReductions, pattern)
				s.blockTable = nil
				if canUse && !match {
					if len(s.tailBuf) == 0 || !canBoundaryMatch(s.tailBuf, pattern) {
						decompLen, err := s.bufferSkippedBlock(chunkType, chunkLen)
						if err != nil {
							return err
						}
						if s.collectStats {
							s.stats.BlocksSkipped++
							s.stats.CompBytesSkipped += int64(chunkLen)
						}
						// An all-zero table can't prove absence in a no-overlap
						// final block; remember it so EOF scans this block.
						s.tailRescue = tableAllZero(savedTable)
						s.tailRescueOff = s.blockStart
						s.blockStart += int64(decompLen)
						if s.deferred != nil {
							if err := s.flushDeferred(nil, fn); err != nil {
								return err
							}
						}
						s.prevBlock = nil
						s.tailBuf = s.tailBuf[:0]
						continue
					}
					tableNoMatch = true
				}
				if !canUse {
					if s.collectStats {
						s.stats.TablesUnusable++
					}
					if s.bail {
						return ErrSearchTablesUnusable
					}
				}
			} else if s.blockTableUnusable {
				s.blockTableUnusable = false
				if s.collectStats {
					s.stats.TablesUnusable++
				}
				if s.bail {
					return ErrSearchTablesUnusable
				}
			} else {
				if s.collectStats {
					s.stats.TablesMissing++
				}
				if s.bail {
					return ErrSearchTablesUnusable
				}
			}

			if !s.readFull(s.tmp[:checksumSize]) {
				return s.err
			}
			checksum := uint32(s.tmp[0]) | uint32(s.tmp[1])<<8 | uint32(s.tmp[2])<<16 | uint32(s.tmp[3])<<24
			n := chunkLen - checksumSize
			if n > s.maxBlock {
				return ErrTooLarge
			}
			di := s.decIdx ^ 1
			if n > len(s.decoded[di]) {
				s.decoded[di] = make([]byte, n)
			}
			if !s.readFull(s.decoded[di][:n]) {
				return s.err
			}
			if !s.ignoreCRC && crc(s.decoded[di][:n]) != checksum {
				return ErrCRC
			}

			if s.collectStats {
				s.stats.BlocksSearched++
				s.stats.UncompBytesSearched += int64(n)
			}
			s.blockMatches = 0
			blockOff := s.blockStart
			s.blockStart += int64(n)
			blk := s.decoded[di][:n]
			s.decIdx = di
			if s.deferred != nil {
				if err := s.flushDeferred(blk, fn); err != nil {
					return err
				}
			}
			if err := s.dispatchMatches(blk, blockOff, pattern, fn); err != nil {
				return err
			}
			if s.collectStats && s.blockMatches == 0 {
				s.stats.BlocksFalsePositive++
			}
			if tableNoMatch {
				if s.collectStats {
					s.stats.BlocksBoundaryScanned++
				}
				s.prevBlock = nil
			} else {
				s.prevBlock = blk
			}
			s.prevLazy = nil
			continue

		case chunkTypeEOF:
			if chunkLen > binary.MaxVarintLen64 {
				return ErrCorrupt
			}
			if chunkLen != 0 {
				if !s.readFull(s.tmp[:chunkLen]) {
					return s.err
				}
			}
			// End of this stream — scan a tail block held back from skipping.
			if err := s.rescueTail(pattern, fn); err != nil {
				return err
			}
			s.readHeader = false
			continue
		}

		if chunkType <= maxNonSkippableChunk {
			return ErrUnsupported
		}
		// Skip unknown skippable chunks
		if !s.skip(chunkLen) {
			return s.err
		}
	}
}

// updateTail records blk (ending at stream offset endOff) as the most recent
// contiguous decoded data, retaining its last keep bytes for the next block's
// boundary scan. Short blocks accumulate so the tail always spans keep bytes
// when that much contiguous data exists.
func (s *BlockSearcher) updateTail(blk []byte, endOff int64, keep int) {
	if keep <= 0 {
		s.tailBuf = s.tailBuf[:0]
		return
	}
	if len(blk) >= keep {
		s.tailBuf = append(s.tailBuf[:0], blk[len(blk)-keep:]...)
	} else {
		if drop := len(s.tailBuf) + len(blk) - keep; drop > 0 {
			s.tailBuf = append(s.tailBuf[:0], s.tailBuf[min(drop, len(s.tailBuf)):]...)
		}
		s.tailBuf = append(s.tailBuf, blk...)
	}
	s.tailOff = endOff - int64(len(s.tailBuf))
}

// dispatchMatches finds all pattern occurrences in blk and calls fn for each.
// If fn returns ErrSearchForward, the match is saved as s.deferred for the main
// loop to re-dispatch after loading the next block. Remaining matches in this
// block are still dispatched.
func (s *BlockSearcher) dispatchMatches(blk []byte, blockOff int64, pattern []byte, fn func(SearchResult) error) error {
	prevBlockStart := blockOff - int64(len(s.prevBlock))
	if s.prevBlock == nil {
		if s.prevLazy != nil {
			prevBlockStart = blockOff - int64(s.prevLazy.decompLen)
		} else {
			prevBlockStart = blockOff
		}
	}

	// Check for matches spanning the boundary into blk. tailBuf holds the last
	// len(pattern)-1 decoded bytes of the contiguous run ending at blk's start,
	// so this also finds matches that straddle 3+ blocks across a short interior
	// block (e.g. a 1-byte Flush block) — prevBlock alone may be too short.
	if len(s.tailBuf) > 0 && len(pattern) > 1 {
		tail := s.tailBuf
		head := blk[:min(len(blk), len(pattern)-1)]
		s.bbuf = append(append(s.bbuf[:0], tail...), head...)
		bOff := 0
		for {
			idx := bytes.Index(s.bbuf[bOff:], pattern)
			if idx < 0 {
				break
			}
			// Only report matches that actually straddle (start in the tail,
			// end in blk).
			absIdx := bOff + idx
			matchInTail := len(tail) - absIdx
			if matchInTail <= 0 || matchInTail >= len(pattern) {
				bOff = absIdx + 1
				continue
			}
			streamOff := s.tailOff + int64(absIdx)
			// Prefer the full previous block for context; fall back to the tail
			// when the match starts before prevBlock (a 3+ block straddle).
			var result SearchResult
			var defOff int
			var defStart int64
			var defBlk []byte
			if s.prevBlock != nil && streamOff >= prevBlockStart {
				off := int(streamOff - prevBlockStart)
				result = SearchResult{
					Blocks:       [2][]byte{s.prevBlock, blk},
					Offset:       off,
					StreamOffset: streamOff,
					BlockStart:   prevBlockStart,
					PrevBlockLen: len(s.prevBlock),
				}
				defOff, defStart, defBlk = off, prevBlockStart, s.prevBlock
			} else {
				result = SearchResult{
					Blocks:       [2][]byte{tail, blk},
					Offset:       absIdx,
					StreamOffset: streamOff,
					BlockStart:   s.tailOff,
					PrevBlockLen: len(tail),
				}
				defOff, defStart = absIdx, s.tailOff
			}
			s.blockMatches++
			err := fn(result)
			if err != nil {
				if errors.Is(err, ErrSearchForward) {
					if defBlk == nil { // tailBuf is reused next block; copy it
						defBlk = append([]byte(nil), tail...)
					}
					s.deferred = &deferredMatch{
						streamOff: streamOff,
						blockOff:  defStart,
						matchOff:  defOff,
						blk:       defBlk,
					}
				} else {
					return err
				}
			}
			bOff = absIdx + 1
		}
	}

	off := 0
	for {
		idx := bytes.Index(blk[off:], pattern)
		if idx < 0 {
			s.updateTail(blk, blockOff+int64(len(blk)), len(pattern)-1)
			return nil
		}
		matchOff := off + idx
		streamOff := blockOff + int64(matchOff)

		var result SearchResult
		if s.prevBlock != nil {
			result = SearchResult{
				Blocks:       [2][]byte{s.prevBlock, blk},
				Offset:       len(s.prevBlock) + matchOff,
				StreamOffset: streamOff,
				BlockStart:   prevBlockStart,
				PrevBlockLen: len(s.prevBlock),
			}
		} else {
			result = SearchResult{
				Blocks:       [2][]byte{nil, blk},
				StreamOffset: streamOff,
				BlockStart:   prevBlockStart,
				prevLazy:     s.prevLazy,
			}
			if s.prevLazy != nil {
				result.Offset = s.prevLazy.decompLen + matchOff
				result.PrevBlockLen = s.prevLazy.decompLen
			} else {
				result.Offset = matchOff
			}
		}

		s.blockMatches++
		err := fn(result)
		if err == nil {
			off = matchOff + 1
			continue
		}
		if !errors.Is(err, ErrSearchForward) {
			return err
		}
		// Save deferred match for the main loop to re-dispatch with the next block.
		s.deferred = &deferredMatch{
			streamOff: streamOff,
			blockOff:  blockOff,
			matchOff:  matchOff,
			blk:       blk,
		}
		off = matchOff + 1
	}
}

// flushDeferred re-dispatches a deferred match with nextBlk as forward context.
// nextBlk may be nil (EOF or skipped block).
func (s *BlockSearcher) flushDeferred(nextBlk []byte, fn func(SearchResult) error) error {
	d := s.deferred
	s.deferred = nil
	result := SearchResult{
		Blocks:       [2][]byte{d.blk, nextBlk},
		Offset:       d.matchOff,
		StreamOffset: d.streamOff,
		BlockStart:   d.blockOff,
		PrevBlockLen: len(d.blk),
	}
	err := fn(result)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrSearchForward) {
		return err
	}
	// Caller wants another forward block. Save again with shifted context.
	if nextBlk != nil {
		s.deferred = &deferredMatch{
			streamOff: d.streamOff,
			blockOff:  d.blockOff,
			matchOff:  d.matchOff,
			blk:       nextBlk,
		}
	}
	return nil
}

func (s *BlockSearcher) readFull(buf []byte) bool {
	_, s.err = io.ReadFull(s.r, buf)
	if s.err != nil {
		if errors.Is(s.err, io.ErrUnexpectedEOF) {
			s.err = ErrCorrupt
		}
		return false
	}
	return true
}

func (s *BlockSearcher) skip(n int) bool {
	if n == 0 {
		return true
	}
	if rs, ok := s.r.(io.Seeker); ok {
		_, s.err = rs.Seek(int64(n), io.SeekCurrent)
		return s.err == nil
	}
	s.ensureBuf(s.maxBlock + obufHeaderLen)
	buf := s.buf
	for n > 0 {
		chunk := buf
		if len(chunk) > n {
			chunk = chunk[:n]
		}
		if !s.readFull(chunk) {
			return false
		}
		n -= len(chunk)
	}
	return true
}

func (s *BlockSearcher) ensureBuf(n int) {
	if cap(s.buf) < n {
		s.buf = make([]byte, 0, n+n/4)
	}
	s.buf = s.buf[:n]
}

// decodePending decompresses a pending block using one of the alternating buffers.
func (s *BlockSearcher) decodePending(p *pendingBlock) ([]byte, error) {
	lb := &p.lazy
	buf := lb.chunkData
	if len(buf) < checksumSize {
		return nil, ErrCorrupt
	}
	checksum := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
	buf = buf[checksumSize:]

	di := s.decIdx ^ 1
	if lb.chunkType == chunkTypeUncompressedData {
		n := len(buf)
		if n > len(s.decoded[di]) {
			s.decoded[di] = make([]byte, n)
		}
		copy(s.decoded[di][:n], buf)
		if !s.ignoreCRC && crc(s.decoded[di][:n]) != checksum {
			return nil, ErrCRC
		}
		s.decIdx = di
		return s.decoded[di][:n], nil
	}

	n, hdrLen, err := decodedLen(buf)
	if err != nil {
		return nil, err
	}
	if n > s.maxBlock {
		return nil, ErrTooLarge
	}
	if n > len(s.decoded[di]) {
		s.decoded[di] = make([]byte, n)
	}
	if ret := minLZDecode(s.decoded[di][:n], buf[hdrLen:]); ret != 0 {
		return nil, ErrCorrupt
	}
	toCRC := s.decoded[di][:n]
	if lb.chunkType == chunkTypeMinLZCompressedDataCompCRC {
		toCRC = buf
	}
	if !s.ignoreCRC && crc(toCRC) != checksum {
		return nil, ErrCRC
	}
	s.decIdx = di
	return s.decoded[di][:n], nil
}

// resolvePending decodes or skips the pending block. If the next block's
// search table is available (s.blockTable != nil), the deferred hashes are
// checked to decide. Otherwise the block is decoded (conservative).
func (s *BlockSearcher) resolvePending(pattern []byte, fn func(SearchResult) error) error {
	p := s.pending
	s.pending = nil

	skip := false
	if p.deferHashes != nil && s.blockTable != nil {
		if !checkDeferredHashes(s.blockTable, s.blockReductions, s.blockInfo.baseTableSize, p.deferHashes) {
			skip = true
		}
	}

	if skip {
		if s.collectStats {
			s.stats.BlocksSkipped++
			s.stats.BlocksDeferredSkipped++
			s.stats.CompBytesSkipped += int64(len(p.lazy.chunkData))
		}
		if s.deferred != nil {
			if err := s.flushDeferred(nil, fn); err != nil {
				return err
			}
		}
		s.prevLazyStore = p.lazy
		s.prevLazy = &s.prevLazyStore
		s.prevBlock = nil
		s.tailBuf = s.tailBuf[:0]
		return nil
	}

	blk, err := s.decodePending(p)
	if err != nil {
		return err
	}
	if s.collectStats {
		s.stats.BlocksSearched++
		s.stats.UncompBytesSearched += int64(len(blk))
	}
	s.blockMatches = 0
	if s.deferred != nil {
		if err := s.flushDeferred(blk, fn); err != nil {
			return err
		}
	}
	if err := s.dispatchMatches(blk, p.blockStart, pattern, fn); err != nil {
		return err
	}
	if s.collectStats && s.blockMatches == 0 {
		s.stats.BlocksFalsePositive++
	}
	if p.tableNoMatch {
		if s.collectStats {
			s.stats.BlocksBoundaryScanned++
		}
		s.prevBlock = nil
	} else {
		s.prevBlock = blk
	}
	s.prevLazy = nil
	return nil
}

// rescueTail scans a block that was skipped only because its (prefix) search
// table was all-zero, once that block proves to be a stream's final block. The
// final block is tabled without forward overlap (SPEC §B.4.3), so an all-zero
// table cannot prove a prefix-only pattern absent from it. The block stays
// buffered in prevLazy; here it is decoded and searched. No-op unless tailRescue
// is set. Safe to call at every end-of-stream point — it clears the flag.
func (s *BlockSearcher) rescueTail(pattern []byte, fn func(SearchResult) error) error {
	if !s.tailRescue {
		return nil
	}
	s.tailRescue = false
	lb := s.prevLazy
	if lb == nil {
		return nil
	}
	blk := lb.decode()
	if blk == nil {
		return ErrCorrupt
	}
	if s.collectStats {
		// Re-classify the block: searched, not skipped.
		s.stats.BlocksSkipped--
		s.stats.CompBytesSkipped -= int64(len(lb.chunkData))
		s.stats.BlocksSearched++
		s.stats.UncompBytesSearched += int64(len(blk))
	}
	s.blockMatches = 0
	// An all-zero table is skipped only when no boundary match from the previous
	// block is possible, so the block is self-contained and needs no prev context.
	s.prevBlock = nil
	s.prevLazy = nil
	s.tailBuf = s.tailBuf[:0]
	if err := s.dispatchMatches(blk, s.tailRescueOff, pattern, fn); err != nil {
		return err
	}
	if s.collectStats && s.blockMatches == 0 {
		s.stats.BlocksFalsePositive++
	}
	return nil
}

// bufferSkippedBlock reads and buffers a compressed block's data for lazy PrevBlock().
func (s *BlockSearcher) bufferSkippedBlock(chunkType byte, chunkLen int) (decompLen int, err error) {
	s.ensureBuf(chunkLen)
	if !s.readFull(s.buf[:chunkLen]) {
		return 0, s.err
	}
	if chunkType == chunkTypeUncompressedData {
		decompLen = chunkLen - checksumSize
	} else {
		n, _, _ := decodedLen(s.buf[checksumSize : checksumSize+min(chunkLen-checksumSize, 10)])
		decompLen = n
	}
	// Hand s.buf to the lazy block and reclaim the previous lazy block's buffer.
	// This avoids an alloc+copy on every skipped block.
	var reclaimed []byte
	if s.prevLazy != nil {
		reclaimed = s.prevLazy.chunkData
	}
	s.prevLazyStore = lazyBlock{
		chunkData: s.buf[:chunkLen],
		chunkType: chunkType,
		decompLen: decompLen,
		ignoreCRC: s.ignoreCRC,
	}
	s.prevLazy = &s.prevLazyStore
	if reclaimed != nil {
		s.buf = reclaimed[:0]
	} else {
		s.buf = nil
	}
	return decompLen, nil
}

// canBoundaryMatch reports whether pattern could start in the tail of prev
// and extend past its end (straddling into the next block).
func canBoundaryMatch(prev, pattern []byte) bool {
	if len(prev) == 0 || len(pattern) <= 1 {
		return false
	}
	tail := prev[max(0, len(prev)-len(pattern)+1):]
	for i := range tail {
		if bytes.HasPrefix(pattern, tail[i:]) {
			return true
		}
	}
	return false
}

// patternCanUseConfig reports whether a search table with this configuration
// can answer queries for the given pattern. It mirrors patternCanMatch's
// canUse return using ONLY the config + pattern — no bitmap needed. Callers
// use this to decide whether to spend the cost of decoding the bitmap.
func patternCanUseConfig(cfg *SearchTableConfig, pattern []byte) bool {
	switch cfg.tableType {
	case searchTableTypeNoPrefix:
		return len(pattern) >= int(cfg.matchLen)
	case searchTableTypeBytePrefix:
		if len(pattern) < int(cfg.matchLen) {
			return false
		}
		var pfxMask [32]byte
		for _, p := range cfg.prefixBytes {
			pfxMask[p>>3] |= 1 << (p & 7)
		}
		return patternHasPrefixContext(&pfxMask, pattern, int(cfg.matchLen))
	case searchTableTypeMaskPrefix:
		if len(pattern) < int(cfg.matchLen) {
			return false
		}
		return patternHasPrefixContext(&cfg.prefixMask, pattern, int(cfg.matchLen))
	case searchTableTypeLongPrefix:
		// Usable whenever the pattern contains the prefix. Occurrences with a
		// full matchLen(+extras) window are answered precisely by patternCanMatch;
		// a prefix-only pattern (the pattern is shorter than the window, e.g. it
		// IS the prefix) is answered by table emptiness. Both need the bitmap
		// decoded, so report usable in either case — even when the pattern is
		// shorter than matchLen.
		return len(cfg.longPrefix) > 0 && bytes.Contains(pattern, cfg.longPrefix)
	}
	return false
}

func patternHasPrefixContext(pfxMask *[32]byte, pattern []byte, ml int) bool {
	for i := 1; i <= len(pattern)-ml; i++ {
		b := pattern[i-1]
		if pfxMask[b>>3]&(1<<(b&7)) != 0 {
			return true
		}
	}
	return false
}

// patternCanMatch checks if pattern could be in a block based on its search table.
// Returns (canUse, mightMatch):
//   - canUse=false means the table can't answer this query (pattern too short, prefix mismatch)
//   - canUse=true, mightMatch=false means the block definitely does NOT contain the pattern
//   - canUse=true, mightMatch=true means the block might contain the pattern
func patternCanMatch(cfg *SearchTableConfig, table []byte, reductions uint8, pattern []byte) (canUse, mightMatch bool) {
	// Reduction folds upper half into lower half, discarding the MSB each time.
	// Lookup: mask off the top `reductions` bits of the hash.
	mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1

	switch cfg.tableType {
	case searchTableTypeNoPrefix:
		if len(pattern) < int(cfg.matchLen) {
			return false, true
		}
		// Check matchLen-windows. If any window is absent, the pattern cannot
		// start at a position where all windows fit within this block.
		// However, the pattern could start near the end of the block with
		// later windows extending into the next block. We check all possible
		// starting positions: the pattern can start at position P in the block
		// if windows 0..K are in the table (where K windows fit in the block).
		// Skip only if NO starting position has its first window present.
		ml := int(cfg.matchLen)
		nWindows := len(pattern) - ml + 1
		// Check if all windows are present (pattern fits entirely in block).
		allPresent := true
		for i := range nWindows {
			v := readLE64Pad(pattern[i:])
			h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
			if table[h>>3]&(1<<(h&7)) == 0 {
				allPresent = false
				// The pattern can't fit entirely starting at a position where
				// window i falls within the block. But it could start later,
				// with fewer windows in this block. Check if the LAST window
				// (first window of a boundary-straddling match) is present.
				break
			}
		}
		if allPresent {
			return true, true
		}
		// Check if the pattern could start near the block end (boundary match).
		// The last matchLen bytes of the pattern's first window must be present.
		v := readLE64Pad(pattern[:ml])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
		if table[h>>3]&(1<<(h&7)) != 0 {
			return true, true // first window present — could be a boundary match
		}
		return true, false

	case searchTableTypeBytePrefix:
		// Build mask from the 8 prefix bytes for fast lookup.
		var pfxMask [32]byte
		for _, p := range cfg.prefixBytes {
			pfxMask[p>>3] |= 1 << (p & 7)
		}
		return patternCanMatchWithPrefixMask(cfg, table, mask, &pfxMask, pattern)

	case searchTableTypeMaskPrefix:
		return patternCanMatchWithPrefixMask(cfg, table, mask, &cfg.prefixMask, pattern)

	case searchTableTypeLongPrefix:
		pl := len(cfg.longPrefix)
		ml := int(cfg.matchLen)
		ex := int(cfg.extras)
		checked := 0
		firstCheckedAllPresent := false
		for i := 0; i <= len(pattern)-pl-ml-ex; i++ {
			if !bytes.Equal(pattern[i:i+pl], cfg.longPrefix) {
				continue
			}
			// All E+1 windows after the prefix must be present for this
			// occurrence to anchor in the block.
			allPresent := true
			for j := 0; j <= ex; j++ {
				v := readLE64Pad(pattern[i+pl+j:])
				h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
				if table[h>>3]&(1<<(h&7)) == 0 {
					allPresent = false
					break
				}
			}
			if checked == 0 {
				firstCheckedAllPresent = allPresent
			}
			if !allPresent {
				if firstCheckedAllPresent {
					return true, true
				}
				return true, false
			}
			checked++
		}
		if checked > 0 {
			return true, true
		}
		// No occurrence has a full matchLen(+extras) window inside the pattern —
		// a prefix-only query (the pattern is too short, e.g. it IS the prefix).
		// The table indexes every position where the prefix starts in the block,
		// so an all-zero table proves the prefix — and any pattern containing it —
		// is absent. A non-empty table can't be narrowed further here. (A stream's
		// final block is tabled without forward overlap, so the caller must still
		// scan it on EOF rather than trust an all-zero table; see the tail-rescue
		// path in the searchers.)
		if pl > 0 && bytes.Contains(pattern, cfg.longPrefix) {
			return true, !tableAllZero(table)
		}
		return false, true
	}

	return false, true
}

// patternCanMatchWithPrefixMask scans pattern for any position where
// a prefix byte appears, then checks the matchLen window that follows.
// All found windows must be set in the table for a possible match.
func patternCanMatchWithPrefixMask(cfg *SearchTableConfig, table []byte, mask uint32, pfxMask *[32]byte, pattern []byte) (canUse, mightMatch bool) {
	ml := int(cfg.matchLen)
	if len(pattern) < ml {
		return false, true
	}

	// Check prefix-context windows in pattern order. For a boundary match
	// (pattern straddling block end), only the first K windows are in the
	// current block. So if the first prefix window is absent, only the raw
	// fallback matters. If the first is present but a later one is absent,
	// it could be a legitimate boundary match.
	checked := 0
	firstCheckedPresent := false
	for i := 1; i <= len(pattern)-ml; i++ {
		b := pattern[i-1]
		if pfxMask[b>>3]&(1<<(b&7)) == 0 {
			continue
		}
		v := readLE64Pad(pattern[i:])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
		present := table[h>>3]&(1<<(h&7)) != 0
		if checked == 0 {
			firstCheckedPresent = present
		}
		checked++
		if !present {
			if firstCheckedPresent {
				return true, true
			}
			break
		}
	}
	if checked == 0 {
		return false, true // no prefix context in pattern
	}
	if firstCheckedPresent {
		return true, true
	}
	// First prefix window absent. Check raw fallback for boundary case
	// where the prefix byte is in the previous block's overlap.
	v := readLE64Pad(pattern[:ml])
	h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
	if table[h>>3]&(1<<(h&7)) != 0 {
		return true, true
	}
	return true, false
}

// patternDeferHashes returns the absent window hashes (at full baseTableSize
// resolution) when patternCanMatch would return (true, true) due to the
// boundary case (first window present, later absent). Returns nil if the
// match is definite (all present), impossible (first absent), or can't be used.
func patternDeferHashes(cfg *SearchTableConfig, table []byte, reductions uint8, pattern []byte) []uint32 {
	mask := uint32(1<<(cfg.baseTableSize-reductions)) - 1

	switch cfg.tableType {
	case searchTableTypeNoPrefix:
		ml := int(cfg.matchLen)
		if len(pattern) < ml {
			return nil
		}
		nWindows := len(pattern) - ml + 1
		if nWindows <= 1 {
			return nil
		}
		// Check first window.
		h0 := hashValue(readLE64Pad(pattern[:ml]), cfg.baseTableSize, cfg.matchLen)
		if table[(h0&mask)>>3]&(1<<((h0&mask)&7)) == 0 {
			return nil // first absent → definite skip
		}
		// Collect absent later windows (store full hash, no mask).
		var absent []uint32
		for i := 1; i < nWindows; i++ {
			h := hashValue(readLE64Pad(pattern[i:]), cfg.baseTableSize, cfg.matchLen)
			if table[(h&mask)>>3]&(1<<((h&mask)&7)) == 0 {
				absent = append(absent, h)
			}
		}
		if len(absent) == 0 {
			return nil // all present → definite match
		}
		return absent

	case searchTableTypeBytePrefix:
		var pfxMask [32]byte
		for _, p := range cfg.prefixBytes {
			pfxMask[p>>3] |= 1 << (p & 7)
		}
		return deferHashesWithPrefixMask(cfg, table, mask, &pfxMask, pattern)

	case searchTableTypeMaskPrefix:
		return deferHashesWithPrefixMask(cfg, table, mask, &cfg.prefixMask, pattern)

	case searchTableTypeLongPrefix:
		ml := int(cfg.matchLen)
		ex := int(cfg.extras)
		pl := len(cfg.longPrefix)
		if len(pattern) < pl+ml+ex {
			return nil
		}
		// Defer every absent window after the first occurrence whose windows are
		// all present. The encoder records an occurrence in the block where its
		// prefix starts — including a prefix that straddles into the next block
		// (SPEC_SEARCH.md 2.1 / B.1) — so a real straddle's absent windows are all
		// present in the next block's table. Handles extras (E+1 windows each).
		first := true
		firstAllPresent := false
		var absent []uint32
		for i := 0; i <= len(pattern)-pl-ml-ex; i++ {
			if !bytes.Equal(pattern[i:i+pl], cfg.longPrefix) {
				continue
			}
			allPresent := true
			var occAbsent []uint32
			for j := 0; j <= ex; j++ {
				h := hashValue(readLE64Pad(pattern[i+pl+j:]), cfg.baseTableSize, cfg.matchLen)
				if table[(h&mask)>>3]&(1<<((h&mask)&7)) == 0 {
					allPresent = false
					occAbsent = append(occAbsent, h)
				}
			}
			if first {
				firstAllPresent = allPresent
				first = false
				continue
			}
			absent = append(absent, occAbsent...)
		}
		if !firstAllPresent || len(absent) == 0 {
			return nil
		}
		return absent
	}
	return nil
}

func deferHashesWithPrefixMask(cfg *SearchTableConfig, table []byte, mask uint32, pfxMask *[32]byte, pattern []byte) []uint32 {
	ml := int(cfg.matchLen)
	if len(pattern) < ml {
		return nil
	}

	// Defer every absent window after the first present one. The encoder emits a
	// table only for a block that had its full forward overlap (a flushed block
	// without it emits no table and is always scanned), so for a real
	// boundary-straddling match every window absent here is present in the next
	// block's table — no window is recorded in neither block.
	first := true
	firstPresent := false
	var absent []uint32
	for i := 1; i <= len(pattern)-ml; i++ {
		b := pattern[i-1]
		if pfxMask[b>>3]&(1<<(b&7)) == 0 {
			continue
		}
		h := hashValue(readLE64Pad(pattern[i:]), cfg.baseTableSize, cfg.matchLen)
		present := table[(h&mask)>>3]&(1<<((h&mask)&7)) != 0
		if first {
			firstPresent = present
			first = false
			continue
		}
		if !present {
			absent = append(absent, h)
		}
	}
	if len(absent) == 0 {
		return nil
	}
	if firstPresent {
		return absent
	}
	// First prefix window absent: a match is only possible anchored via the raw
	// window (the leading prefix byte lies in the previous block).
	hRaw := hashValue(readLE64Pad(pattern[:ml]), cfg.baseTableSize, cfg.matchLen)
	if table[(hRaw&mask)>>3]&(1<<((hRaw&mask)&7)) != 0 {
		return absent
	}
	return nil
}

// checkDeferredHashes checks if ALL deferred hashes are present in a table.
// For a boundary match, every absent window from block N must appear in block N+1.
// Hashes are at full baseTableSize resolution; the per-block reduction mask is applied.
func checkDeferredHashes(table []byte, reductions, baseTableSize uint8, hashes []uint32) bool {
	mask := uint32(1<<(baseTableSize-reductions)) - 1
	for _, h := range hashes {
		h &= mask
		if table[h>>3]&(1<<(h&7)) == 0 {
			return false
		}
	}
	return true
}

// windowSpec is one matchLen-window the searcher tests for a pattern, with its
// full-resolution (un-reduced) hash precomputed once (baseTableSize is constant
// per stream).
type windowSpec struct {
	prefix int // pattern index of the prefix byte; -1 for the raw / no-prefix anchor
	pos    int // pattern index where the window starts
	hash   uint32
}

// enumeratePatternWindows returns, in pattern order, every matchLen-window the
// searcher tests for pattern under cfg, mirroring patternCanMatch /
// patternDeferHashes. For byte/mask prefix tables the raw fallback window (the
// leading matchLen bytes, anchoring a match whose prefix byte sits in the
// previous block) is appended last.
func enumeratePatternWindows(cfg *SearchTableConfig, pattern []byte) []windowSpec {
	ml := int(cfg.matchLen)
	if ml == 0 || len(pattern) < ml {
		return nil
	}
	var out []windowSpec
	add := func(prefix, pos int) {
		h := hashValue(readLE64Pad(pattern[pos:]), cfg.baseTableSize, cfg.matchLen)
		out = append(out, windowSpec{prefix: prefix, pos: pos, hash: h})
	}
	switch cfg.tableType {
	case searchTableTypeNoPrefix:
		for i := 0; i <= len(pattern)-ml; i++ {
			add(-1, i)
		}
	case searchTableTypeBytePrefix:
		var pfx [256]bool
		for _, p := range cfg.prefixBytes {
			pfx[p] = true
		}
		for i := 1; i <= len(pattern)-ml; i++ {
			if pfx[pattern[i-1]] {
				add(i-1, i)
			}
		}
		add(-1, 0)
	case searchTableTypeMaskPrefix:
		for i := 1; i <= len(pattern)-ml; i++ {
			b := pattern[i-1]
			if cfg.prefixMask[b>>3]&(1<<(b&7)) != 0 {
				add(i-1, i)
			}
		}
		add(-1, 0)
	case searchTableTypeLongPrefix:
		pl := len(cfg.longPrefix)
		ex := int(cfg.extras)
		for i := 0; i <= len(pattern)-pl-ml-ex; i++ {
			if bytes.Equal(pattern[i:i+pl], cfg.longPrefix) {
				for j := 0; j <= ex; j++ {
					add(i, i+pl+j)
				}
			}
		}
	}
	return out
}

// initWindows enumerates pattern windows under cfg and seeds st.Windows with
// their metadata, returning the specs for per-table tallying via tallyWindows.
func (st *SearchStats) initWindows(cfg *SearchTableConfig, pattern []byte) []windowSpec {
	windows := enumeratePatternWindows(cfg, pattern)
	ml := int(cfg.matchLen)
	st.Windows = make([]WindowStat, len(windows))
	for i, wd := range windows {
		ws := WindowStat{Pos: wd.pos, Prefix: wd.prefix}
		ws.Bytes = append(ws.Bytes, pattern[wd.pos:min(wd.pos+ml, len(pattern))]...)
		if wd.prefix >= 0 {
			ws.PrefixByte = pattern[wd.prefix]
		}
		st.Windows[i] = ws
	}
	return windows
}

// tallyWindows updates per-window present/absent counts against one block's
// table. baseTableSize must match the value used to compute windows' hashes.
func (st *SearchStats) tallyWindows(windows []windowSpec, baseTableSize, reductions uint8, table []byte) {
	if len(windows) == 0 || len(table) == 0 {
		return
	}
	mask := uint32(1<<(baseTableSize-reductions)) - 1
	for i := range windows {
		h := windows[i].hash & mask
		if table[h>>3]&(1<<(h&7)) != 0 {
			st.Windows[i].Present++
		} else {
			st.Windows[i].Absent++
		}
	}
}

// sameSearchLayout reports whether two configs produce the same per-window
// layout (and hashes), i.e. whether their tables can be tallied into the same
// SearchStats.Windows. matchLen, baseTableSize and the prefix all affect the
// enumerated windows; per-block reductions do not (they only mask at lookup).
func sameSearchLayout(a, b *SearchTableConfig) bool {
	if a.tableType != b.tableType || a.matchLen != b.matchLen ||
		a.baseTableSize != b.baseTableSize || a.extras != b.extras {
		return false
	}
	switch a.tableType {
	case searchTableTypeBytePrefix:
		return a.prefixBytes == b.prefixBytes
	case searchTableTypeMaskPrefix:
		return a.prefixMask == b.prefixMask
	case searchTableTypeLongPrefix:
		return bytes.Equal(a.longPrefix, b.longPrefix)
	}
	return true
}
