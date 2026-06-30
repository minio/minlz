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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
)

// blockTableEntry is one search table associated with a block.
type blockTableEntry struct {
	cfg        SearchTableConfig
	reductions uint8
	table      []byte
}

// pendingDecode is one batched decode-and-search entry.
type pendingDecode struct {
	ref    remoteRef
	tables []blockTableEntry
	// tableNoMatch is true when the table proved the pattern absent from
	// this block but we still decode it to satisfy a boundary check from
	// the previous block. In that case prevBlock is cleared after this
	// block so a single false positive doesn't cascade into the next.
	tableNoMatch bool
}

// SidecarSearcher searches a main MinLZ data stream (via io.ReaderAt) using
// search indexes from a sidecar stream (via io.Reader). Adjacent must-read
// blocks are coalesced into a single ReadAt call to minimise I/O.
type SidecarSearcher struct {
	main    io.ReaderAt
	sidecar io.Reader

	// Per-stream config(s); multiple configs are supported.
	streamInfos []SearchTableConfig
	infoCB      func(SearchTableConfig)
	cstDec      *cstDecoder

	// Tables accumulated between 0x45/0x46 chunks and the next 0x47.
	pending []blockTableEntry
	// tableBufs[i] is the reusable backing storage for pending[i].table.
	// Reused across blocks so each new table can be copied into existing
	// scratch instead of allocating a fresh slice.
	tableBufs [][]byte

	// At most one block can be deferred at a time (per SPEC_SEARCH B.4).
	// When set, sideDeferred holds the block reference whose decision is
	// postponed until the next block's table arrives; sideDeferredHashes
	// is the set of absent later-window hashes from the deferred block's
	// own table that the next-block table must contain for a boundary
	// match to remain possible.
	sideDeferred       *remoteRef
	sideDeferredHashes []uint32
	sideDeferredBase   uint8 // baseTableSize of the deferred block's table

	// Search state.
	prevBlock []byte // decoded previous block (nil if not yet decoded / skipped)
	prevLazy  *lazyMainBlock
	deferred  *deferredMatch
	// tailBuf holds the last len(pattern)-1 bytes of the current contiguous run
	// of decoded blocks (mirrors BlockSearcher.tailBuf). It lets the boundary
	// scan and the skip/defer guards see a match that straddles 3+ blocks — e.g.
	// across a short Flush block shorter than len(pattern)-1 — where prevBlock
	// alone is too short. tailOff is its stream offset; bbuf is reused scratch.
	// Reset whenever a block is skipped or deferred (contiguity break).
	tailBuf []byte
	tailOff int64
	bbuf    []byte

	// tailRescue is set when the most recently processed block was skipped only
	// because its (prefix) search table was all-zero. Sound for blocks with
	// forward overlap, but a stream's final block is tabled without it (SPEC
	// §B.4.3), so a prefix in its last bytes may be unindexed. If that block
	// turns out to be the stream's last (end-of-stream reached with this set) it
	// is fetched and scanned instead of skipped. Cleared as each block is processed.
	tailRescue    bool
	tailRescueRef remoteRef // location of the held-back block in the main stream
	tailRescueOff int64     // stream offset where it starts

	// Options.
	bail         bool
	ignoreCRC    bool
	collectStats bool
	maxBlock     int // per-stream effective cap = min(maxBlockCfg, stream's declared max)
	maxBlockCfg  int // configured ceiling, set once at construction

	// Cumulative state during a single Search call.
	blockStart   int64 // uncompressed offset where the next block begins
	blockMatches int
	stats        SearchStats
	// searchWindows is the pattern's matchLen-windows, enumerated once when the
	// first usable table is seen; per-table presence is tallied into
	// stats.Windows. Only populated under collectStats. winCfg is the layout
	// they were enumerated for; tallying stops if a later table's config differs
	// (mixing layouts — e.g. a multi-config sidecar — would mislabel the counts).
	searchWindows []windowSpec
	winInit       bool
	winCfg        SearchTableConfig

	// Sidecar reader scratch + buffers used between blocks.
	scratch    []byte
	tmp        [16]byte
	sideMaxBlk int

	err error
}

// lazyMainBlock holds the location of a skipped block in the main stream.
// The actual fetch+decode is deferred to lazyBlock.decode() once the user
// calls SearchResult.PrevBlock().
type lazyMainBlock struct {
	main       io.ReaderAt
	offset     int64
	uncompSize int
	ignoreCRC  bool
	maxBlock   int
}

// NewSidecarSearcher creates a searcher reading the sidecar sequentially
// from sidecar and the main data stream via main. main must support
// concurrent ReadAt calls (per io.ReaderAt's contract).
func NewSidecarSearcher(main io.ReaderAt, sidecar io.Reader, opts ...BlockSearchOption) *SidecarSearcher {
	s := &SidecarSearcher{
		main:     main,
		sidecar:  sidecar,
		maxBlock: maxBlockSize,
	}
	// Re-use BlockSearchOption by adapting a temporary BlockSearcher.
	tmp := &BlockSearcher{maxBlock: maxBlockSize}
	for _, opt := range opts {
		if err := opt(tmp); err != nil {
			s.err = err
			return s
		}
	}
	s.bail = tmp.bail
	s.ignoreCRC = tmp.ignoreCRC
	s.collectStats = tmp.collectStats
	s.infoCB = tmp.infoCallback
	s.maxBlock = tmp.maxBlock
	s.maxBlockCfg = s.maxBlock
	return s
}

// copyPendingTable copies table into s.tableBufs[len(s.pending)] (growing on
// demand) and returns the copy. Reusing per-position buffers across blocks
// avoids the per-block allocation that "append([]byte(nil), table...)" would
// incur. Safe because pending tables only need to live until the next 0x47
// chunk is consumed — by the time the next batch of tables starts arriving,
// the prior batch's slot in tableBufs is no longer referenced.
func (s *SidecarSearcher) copyPendingTable(table []byte) []byte {
	i := len(s.pending)
	for i >= len(s.tableBufs) {
		s.tableBufs = append(s.tableBufs, nil)
	}
	buf := s.tableBufs[i]
	if cap(buf) < len(table) {
		buf = make([]byte, len(table))
	} else {
		buf = buf[:len(table)]
	}
	copy(buf, table)
	s.tableBufs[i] = buf
	return buf
}

// Stats returns search statistics accumulated during the last Search call.
func (s *SidecarSearcher) Stats() SearchStats {
	return s.stats
}

// Search iterates blocks referenced by the sidecar, decoding only those
// whose search tables do not prove the pattern absent, and calls fn for
// each pattern occurrence.
func (s *SidecarSearcher) Search(pattern []byte, fn func(SearchResult) error) error {
	if s.err != nil {
		return s.err
	}
	if len(pattern) == 0 {
		return errors.New("minlz: empty search pattern")
	}
	// Reset state for a fresh search.
	s.stats = SearchStats{}
	s.deferred = nil
	s.prevBlock = nil
	s.prevLazy = nil
	s.tailBuf = s.tailBuf[:0]
	s.tailRescue = false
	s.winInit = false
	s.blockStart = 0
	s.pending = s.pending[:0]
	s.streamInfos = s.streamInfos[:0]

	// Read sidecar stream header.
	if _, err := s.readFull(s.tmp[:4]); err != nil {
		return fmt.Errorf("minlz: sidecar header: %w", err)
	}
	if s.tmp[0] != ChunkTypeStreamIdentifier {
		return ErrSidecarInvalid
	}
	chunkLen := int(s.tmp[1]) | int(s.tmp[2])<<8 | int(s.tmp[3])<<16
	if chunkLen != magicBodyLen {
		return ErrSidecarInvalid
	}
	var body [magicBodyLen]byte
	if _, err := s.readFull(body[:]); err != nil {
		return err
	}
	if string(body[:len(magicBody)]) != magicBody {
		return ErrUnsupported
	}
	mb, err := streamBlockSizeFromHeaderByte(body[magicBodyLen-1])
	if err != nil {
		return err
	}
	s.sideMaxBlk = mb
	// Effective cap = min(configured ceiling, this stream's declared max).
	s.maxBlock = min(s.maxBlockCfg, mb)

	// Pending decode batch — list of refs whose pending tables don't prove
	// absence, accumulated so the I/O can be coalesced.
	var batch []pendingDecode

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := s.decodeBatch(batch, pattern, fn)
		batch = batch[:0]
		return err
	}

	// Walk sidecar.
	for {
		ct, cl, err := s.readChunkHeader()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return s.finalize(pattern, fn, flushBatch)
			}
			return err
		}
		switch ct {
		case chunkTypeSearchInfo:
			payload, err := s.readPayload(cl)
			if err != nil {
				return err
			}
			cfg, err := parseSearchInfo(payload)
			if err != nil {
				return err
			}
			// Pre-size the per-position table scratch to the config's max
			// bitmap (reductions=0 → 1<<(baseTableSize-3) bytes), so the
			// per-block copyPendingTable copies into existing storage.
			i := len(s.streamInfos)
			for i >= len(s.tableBufs) {
				s.tableBufs = append(s.tableBufs, nil)
			}
			if maxBmp := 1 << (cfg.baseTableSize - 3); cap(s.tableBufs[i]) < maxBmp {
				s.tableBufs[i] = make([]byte, 0, maxBmp)
			}
			s.streamInfos = append(s.streamInfos, cfg)
			if s.infoCB != nil {
				s.infoCB(cfg)
			}

		case chunkTypeSearchTable:
			payload, err := s.readPayload(cl)
			if err != nil {
				return err
			}
			cfg, reductions, table, err := parseSearchTable(payload, s.ignoreCRC)
			if err != nil {
				return err
			}
			tcopy := s.copyPendingTable(table)
			s.pending = append(s.pending, blockTableEntry{cfg: cfg, reductions: reductions, table: tcopy})
			if s.collectStats {
				if !s.winInit {
					s.winCfg = cfg
					s.searchWindows = s.stats.initWindows(&cfg, pattern)
					s.winInit = true
				} else if s.searchWindows != nil && !sameSearchLayout(&s.winCfg, &cfg) {
					s.searchWindows = nil // config drift: stop, mixed layouts would mislabel
				}
				s.stats.tallyWindows(s.searchWindows, cfg.baseTableSize, reductions, tcopy)
				s.statsAccumTable(cfg.baseTableSize, reductions, tcopy, cl, false)
			}

		case chunkTypeSearchTableCompressed:
			payload, err := s.readPayload(cl)
			if err != nil {
				return err
			}
			headCfg, headRed, err := parseSearchTableCompressedHeader(payload)
			if err != nil {
				return err
			}
			if !patternCanUseConfig(&headCfg, pattern) {
				// Bitmap is irrelevant for this pattern — skip the expensive
				// huff0/sparse decode. Recorded with table=nil so blockDecision
				// counts it correctly against TablesUnusable.
				s.pending = append(s.pending, blockTableEntry{cfg: headCfg, reductions: headRed, table: nil})
				if s.collectStats {
					s.stats.TablesPresent++
					s.stats.TablesBytes += int64(cl + 4)
					s.stats.TablesCompressed++
					s.stats.TablesCompressedBytes += int64(cl + 4)
					s.stats.TableBitmapBytes += int64(1 << (headCfg.baseTableSize - headRed - 3))
					s.stats.TableBitsSum += int(headCfg.baseTableSize - headRed)
					s.stats.TableReductionsSum += int(headRed)
				}
				continue
			}
			if s.cstDec == nil {
				s.cstDec = newCSTDecoder()
			}
			cfg, reductions, table, err := parseSearchTableCompressed(payload, s.cstDec, s.ignoreCRC)
			if err != nil {
				return err
			}
			tcopy := s.copyPendingTable(table)
			s.pending = append(s.pending, blockTableEntry{cfg: cfg, reductions: reductions, table: tcopy})
			if s.collectStats {
				if !s.winInit {
					s.winCfg = cfg
					s.searchWindows = s.stats.initWindows(&cfg, pattern)
					s.winInit = true
				} else if s.searchWindows != nil && !sameSearchLayout(&s.winCfg, &cfg) {
					s.searchWindows = nil // config drift: stop, mixed layouts would mislabel
				}
				s.stats.tallyWindows(s.searchWindows, cfg.baseTableSize, reductions, tcopy)
				s.statsAccumTable(cfg.baseTableSize, reductions, tcopy, cl, true)
				// Compressed sub-block stats live on the cstDecoder after parse.
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
			}

		case chunkTypeRemoteBlockRef:
			payload, err := s.readPayload(cl)
			if err != nil {
				return err
			}
			refs, err := parseRemoteBlockRef(payload, s.sideMaxBlk)
			if err != nil {
				return err
			}
			// Tables apply to the FIRST ref only (per spec — multi-block
			// 0x47 implies "no indexes between"). Subsequent refs in the
			// same chunk have no tables.
			tablesForFirst := s.pending
			s.pending = nil
			// Resolve any deferred block using the NEW tables (this 0x47
			// belongs to the next block, whose tables are tablesForFirst).
			// If all absent hashes from the deferred block are present in
			// the new table, decode the deferred; otherwise skip it.
			if s.sideDeferred != nil {
				if err := s.resolveSideDeferred(tablesForFirst, &batch, flushBatch); err != nil {
					return err
				}
			}
			for i, ref := range refs {
				var tables []blockTableEntry
				if i == 0 {
					tables = tablesForFirst
				}
				if s.collectStats {
					s.stats.BlocksTotal++
				}
				s.tailRescue = false
				skip, anyUsable, emptySkip := s.blockDecision(tables, pattern)
				if s.bail && !anyUsable {
					return ErrSearchTablesUnusable
				}
				if skip {
					// Decode the coalesced batch now so tailBuf reflects the run
					// of blocks immediately preceding this one. The boundary guard
					// needs that real tail; batched decoding would otherwise leave
					// it stale from an earlier flush, and if that stale tail
					// happened to be a pattern prefix every skip would be vetoed.
					if err := flushBatch(); err != nil {
						return err
					}
				}
				if skip && (len(s.tailBuf) == 0 || !canBoundaryMatch(s.tailBuf, pattern)) {
					if s.collectStats {
						s.stats.BlocksSkipped++
					}
					// The skipped block's compressed data stays in main for
					// lazy retrieval via SearchResult.PrevBlock().
					s.prevLazy = &lazyMainBlock{
						main:       s.main,
						offset:     ref.offset,
						uncompSize: ref.uncompSize,
						ignoreCRC:  s.ignoreCRC,
						maxBlock:   s.maxBlock,
					}
					s.prevBlock = nil
					s.tailBuf = s.tailBuf[:0]
					// An all-zero table can't prove absence in a no-overlap final
					// block; remember it so end-of-stream scans this block.
					s.tailRescue = emptySkip
					s.tailRescueRef = ref
					s.tailRescueOff = s.blockStart
					if s.deferred != nil {
						if err := s.flushDeferred(nil, fn); err != nil {
							return err
						}
					}
					s.blockStart += int64(ref.uncompSize)
					continue
				}
				// Deferred-decode optimisation (SPEC §B.4): when the table
				// says "might match" but the only way that could be true is
				// via a straddling match into the next block, postpone the
				// decision until the next block's table arrives. Restricted
				// to single-config tables and no boundary risk from prev.
				if !skip && i == 0 && len(tables) == 1 && len(s.streamInfos) == 1 && tables[0].table != nil && s.sideDeferred == nil {
					t := &tables[0]
					if hashes := patternDeferHashes(&t.cfg, t.table, t.reductions, pattern); hashes != nil {
						// Flush first so tailBuf reflects the immediately-preceding
						// block before the boundary guard (same staleness reason as
						// the skip path above).
						if err := flushBatch(); err != nil {
							return err
						}
						if len(s.tailBuf) == 0 || !canBoundaryMatch(s.tailBuf, pattern) {
							refCopy := ref
							s.sideDeferred = &refCopy
							s.sideDeferredHashes = hashes
							s.sideDeferredBase = t.cfg.baseTableSize
							if s.collectStats {
								s.stats.BlocksDeferred++
							}
							s.prevBlock = nil
							s.tailBuf = s.tailBuf[:0]
							s.prevLazy = &lazyMainBlock{
								main:       s.main,
								offset:     ref.offset,
								uncompSize: ref.uncompSize,
								ignoreCRC:  s.ignoreCRC,
								maxBlock:   s.maxBlock,
							}
							// Do NOT advance blockStart here — the deferred
							// resolution path advances it: processBlock advances
							// on decode; resolveSideDeferred advances on skip.
							continue
						}
						// Boundary risk from the real previous block — fall
						// through and decode this block now.
					}
				}
				// Must decode. tableNoMatch is true when the table proved
				// absence but we still decode for a boundary check — used
				// post-decode to prevent the cascade into the next block.
				batch = append(batch, pendingDecode{ref: ref, tables: tables, tableNoMatch: skip})
			}

		case chunkTypeEOF:
			if cl > binary.MaxVarintLen64 {
				return ErrSidecarInvalid
			}
			if cl > 0 {
				if _, err := s.readFull(s.tmp[:cl]); err != nil {
					return err
				}
			}
			// End of one sidecar stream. Conservatively resolve any deferred
			// (no next table available → decode), drain any pending decode
			// batch and deferred match. Continue the outer loop — a
			// concatenated stream may follow. If io.EOF is returned by the
			// next chunk read, we finalize cleanly.
			if s.sideDeferred != nil {
				batch = append(batch, pendingDecode{
					ref:          *s.sideDeferred,
					tableNoMatch: false,
				})
				s.sideDeferred = nil
				s.sideDeferredHashes = nil
			}
			if err := flushBatch(); err != nil {
				return err
			}
			// Scan a tail block held back from skipping (final block of this stream).
			if err := s.rescueTail(pattern, fn); err != nil {
				return err
			}
			if s.deferred != nil {
				if err := s.flushDeferred(nil, fn); err != nil {
					return err
				}
			}
			// Per-stream block-context state (prev/pending) is cleared here;
			// blockStart is preserved so SearchStats.UncompressedSize reflects
			// the total when the loop exits without another stream identifier.
			// It is reset on the next ChunkTypeStreamIdentifier (mirrors
			// BlockSearcher's behaviour).
			s.pending = nil
			s.prevBlock = nil
			s.prevLazy = nil
			s.tailBuf = s.tailBuf[:0]
			s.streamInfos = s.streamInfos[:0]

		case ChunkTypeStreamIdentifier:
			// Concatenated sidecar stream starts here. Conservatively resolve
			// any deferred from the prior stream first.
			if s.sideDeferred != nil {
				batch = append(batch, pendingDecode{
					ref:          *s.sideDeferred,
					tableNoMatch: false,
				})
				s.sideDeferred = nil
				s.sideDeferredHashes = nil
				if err := flushBatch(); err != nil {
					return err
				}
			}
			// Scan a tail block held back from the prior stream (defensive: a
			// well-formed stream ends with chunkTypeEOF, which already rescues).
			if err := s.rescueTail(pattern, fn); err != nil {
				return err
			}
			if cl != magicBodyLen {
				return ErrSidecarInvalid
			}
			var body [magicBodyLen]byte
			if _, err := s.readFull(body[:]); err != nil {
				return err
			}
			if string(body[:len(magicBody)]) != magicBody {
				return ErrUnsupported
			}
			mb, err := streamBlockSizeFromHeaderByte(body[magicBodyLen-1])
			if err != nil {
				return err
			}
			s.sideMaxBlk = mb
			s.maxBlock = min(s.maxBlockCfg, mb)
			// New stream — offsets/context don't cross stream boundaries.
			s.blockStart = 0
			s.prevBlock = nil
			s.prevLazy = nil
			s.tailBuf = s.tailBuf[:0]

		default:
			if ct <= maxNonSkippableChunk {
				return ErrUnsupported
			}
			// Skip unknown skippable chunk.
			if err := s.discard(cl); err != nil {
				return err
			}
		}
	}
}

// finalize drains pending state at end-of-sidecar.
func (s *SidecarSearcher) finalize(pattern []byte, fn func(SearchResult) error, flush func() error) error {
	if s.collectStats {
		defer func() { s.stats.UncompressedSize = s.blockStart }()
	}
	if err := flush(); err != nil {
		return err
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

// rescueTail fetches and scans a block that was skipped only because its
// (prefix) table was all-zero, once that block proves to be a stream's final
// block (its table has no forward overlap, so an all-zero table cannot prove a
// prefix-only pattern absent). No-op unless tailRescue is set; clears the flag.
func (s *SidecarSearcher) rescueTail(pattern []byte, fn func(SearchResult) error) error {
	if !s.tailRescue {
		return nil
	}
	s.tailRescue = false
	blk, err := readAndDecodeMainBlock(s.main, s.tailRescueRef.offset, s.tailRescueRef.uncompSize, s.maxBlock, s.ignoreCRC)
	if err != nil {
		return err
	}
	if s.collectStats {
		// Re-classify the block: searched, not skipped.
		s.stats.BlocksSkipped--
		s.stats.BlocksSearched++
		s.stats.UncompBytesSearched += int64(len(blk))
	}
	s.blockMatches = 0
	// An all-zero table is skipped only when no boundary match from the previous
	// block is possible, so the block is self-contained and needs no prev context.
	s.prevBlock = nil
	s.prevLazy = nil
	s.tailBuf = s.tailBuf[:0]
	if err := s.dispatchSidecarMatches(blk, s.tailRescueOff, pattern, fn); err != nil {
		return err
	}
	if s.collectStats && s.blockMatches == 0 {
		s.stats.BlocksFalsePositive++
	}
	return nil
}

// resolveSideDeferred resolves the pending deferred block using the new
// block's tables. When all absent hashes from the deferred block are also
// present in nextTables, a boundary-straddling match remains possible —
// the deferred block is decoded (added to batch and flushed so prevBlock
// is updated). When any hash is missing, the boundary match is impossible
// and the deferred block is skipped without decoding.
//
// If nextTables is empty the resolution falls back to "decode" (conservative).
func (s *SidecarSearcher) resolveSideDeferred(nextTables []blockTableEntry, batch *[]pendingDecode, flushBatch func() error) error {
	if s.sideDeferred == nil {
		return nil
	}
	defer func() {
		s.sideDeferred = nil
		s.sideDeferredHashes = nil
	}()

	// Decide. Without a usable next-block table, decode conservatively.
	skip := false
	if len(nextTables) > 0 && nextTables[0].table != nil {
		t := &nextTables[0]
		// Per SPEC §B.4: ALL absent hashes must be present in the next
		// block's table for the boundary match to remain possible.
		if !checkDeferredHashes(t.table, t.reductions, t.cfg.baseTableSize, s.sideDeferredHashes) {
			skip = true
		}
	}
	if skip {
		if err := flushBatch(); err != nil {
			return err
		}
		if s.collectStats {
			s.stats.BlocksSkipped++
			s.stats.BlocksDeferredSkipped++
		}
		// The deferred block stays available lazily via main; prev was
		// already set to its lazyMainBlock at defer time, leave it as-is.
		s.prevBlock = nil
		// Advance past the skipped deferred block.
		s.blockStart += int64(s.sideDeferred.uncompSize)
		return nil
	}
	// Decode the deferred block — flush so prevBlock is updated to its
	// decoded data before the next ref is processed.
	*batch = append(*batch, pendingDecode{
		ref:          *s.sideDeferred,
		tableNoMatch: false,
	})
	return flushBatch()
}

// blockDecision applies the multi-table policy:
//   - If any usable table proves the pattern absent, the block can be skipped.
//   - If at least one table is usable and none prove absence, the block must
//     be decoded.
//   - If no tables are usable (or none are present at all), the caller falls
//     back to decode (or bails).
func (s *SidecarSearcher) blockDecision(tables []blockTableEntry, pattern []byte) (skip, anyUsable, emptySkip bool) {
	if len(tables) == 0 {
		if s.collectStats {
			s.stats.TablesMissing++
		}
		return false, false, false
	}
	for i := range tables {
		t := &tables[i]
		if t.table == nil {
			// Bitmap decode was skipped because the config alone proved the
			// pattern was unanswerable — same as canUse=false.
			continue
		}
		canUse, mightMatch := patternCanMatch(&t.cfg, t.table, t.reductions, pattern)
		if !canUse {
			continue
		}
		anyUsable = true
		if !mightMatch {
			// emptySkip: an all-zero table can't prove absence in a no-overlap
			// final block, so the caller must scan that block at end-of-stream.
			return true, true, tableAllZero(t.table)
		}
	}
	if !anyUsable {
		if s.collectStats {
			s.stats.TablesUnusable++
		}
	}
	return false, anyUsable, false
}

// decodeBatch issues one (or, in pathological cases, more) ReadAt(s) to fetch
// the compressed chunks for the batched refs, then decodes and searches each.
func (s *SidecarSearcher) decodeBatch(batch []pendingDecode, pattern []byte, fn func(SearchResult) error) error {
	if len(batch) == 0 {
		return nil
	}
	first := batch[0].ref
	last := batch[len(batch)-1].ref
	// Conservative upper bound for the last block's chunk size: header (4) +
	// checksum (4) + uvarint length (<=5) + max-encoded data.
	lastMaxLen := MaxEncodedLen(last.uncompSize) + obufHeaderLen + binary.MaxVarintLen64
	rangeEnd := last.offset + int64(lastMaxLen)
	rangeStart := first.offset
	rangeLen := rangeEnd - rangeStart
	if rangeLen <= 0 || rangeLen > math.MaxInt {
		return errors.New("minlz: sidecar: invalid batch range")
	}
	rangeLenI := int(rangeLen)
	// Make the buffer.
	if cap(s.scratch) < rangeLenI {
		s.scratch = make([]byte, rangeLenI)
	}
	buf := s.scratch[:rangeLenI]
	n, err := s.main.ReadAt(buf, rangeStart)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	buf = buf[:n]

	for i := range batch {
		pd := batch[i]
		// Locate this block's chunk in buf.
		off := int(pd.ref.offset - rangeStart)
		if off < 0 || off+4 > len(buf) {
			return fmt.Errorf("%w: ReadAt did not return enough bytes for block at offset %d", ErrSidecarInvalid, pd.ref.offset)
		}
		hdr := buf[off : off+4]
		ct := hdr[0]
		cl := int(hdr[1]) | int(hdr[2])<<8 | int(hdr[3])<<16
		end := off + 4 + cl
		if end > len(buf) {
			// Fall back to a per-block ReadAt for this oversize block.
			full := make([]byte, 4+cl)
			n, e := s.main.ReadAt(full, pd.ref.offset)
			if e != nil && !errors.Is(e, io.EOF) {
				return e
			}
			if n != len(full) {
				return io.ErrUnexpectedEOF
			}
			if err := s.processBlock(ct, cl, full[4:], pd, pattern, fn); err != nil {
				return err
			}
			continue
		}
		payload := buf[off+4 : end]
		if err := s.processBlock(ct, cl, payload, pd, pattern, fn); err != nil {
			return err
		}
	}
	return nil
}

// processBlock decodes a single block's payload and runs the search dispatch.
func (s *SidecarSearcher) processBlock(chunkType byte, chunkLen int, payload []byte, pd pendingDecode, pattern []byte, fn func(SearchResult) error) error {
	if !isDataChunk(chunkType) {
		return fmt.Errorf("%w: expected data chunk at offset %d, got 0x%02x", ErrSidecarInvalid, pd.ref.offset, chunkType)
	}
	if chunkLen < checksumSize {
		return ErrCorrupt
	}
	decoded, err := decodeDataChunk(chunkType, payload, pd.ref.uncompSize, s.maxBlock, s.ignoreCRC)
	if err != nil {
		return err
	}
	if s.collectStats {
		s.stats.BlocksSearched++
		s.stats.UncompBytesSearched += int64(len(decoded))
	}
	s.blockMatches = 0
	blockOff := s.blockStart
	s.blockStart += int64(len(decoded))
	if s.deferred != nil {
		if err := s.flushDeferred(decoded, fn); err != nil {
			return err
		}
	}
	if err := s.dispatchSidecarMatches(decoded, blockOff, pattern, fn); err != nil {
		return err
	}
	if s.collectStats && s.blockMatches == 0 {
		s.stats.BlocksFalsePositive++
	}
	if pd.tableNoMatch {
		// Block was decoded only because the prev block forced a boundary
		// check. The table already proved no match inside this block — do
		// not keep it as prevBlock or the cascade continues unboundedly.
		if s.collectStats {
			s.stats.BlocksBoundaryScanned++
		}
		s.prevBlock = nil
	} else {
		s.prevBlock = decoded
	}
	s.prevLazy = nil
	return nil
}

// updateTail records blk (ending at stream offset endOff) as the most recent
// contiguous decoded data, retaining its last keep bytes for the next block's
// boundary scan. Mirrors BlockSearcher.updateTail; short blocks accumulate so
// the tail spans keep bytes whenever that much contiguous data exists.
func (s *SidecarSearcher) updateTail(blk []byte, endOff int64, keep int) {
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

// dispatchSidecarMatches mirrors BlockSearcher.dispatchMatches but uses
// lazyMainBlock for prev-block lazy access.
func (s *SidecarSearcher) dispatchSidecarMatches(blk []byte, blockOff int64, pattern []byte, fn func(SearchResult) error) error {
	prevBlockStart := blockOff
	if s.prevBlock != nil {
		prevBlockStart = blockOff - int64(len(s.prevBlock))
	} else if s.prevLazy != nil {
		prevBlockStart = blockOff - int64(s.prevLazy.uncompSize)
	}
	// Boundary scan over the rolling tail of the contiguous decoded run, so a
	// match straddling 3+ blocks (e.g. across a short Flush block shorter than
	// len(pattern)-1) is found even when prevBlock alone is too short.
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
			if err := fn(result); err != nil {
				if !errors.Is(err, ErrSearchForward) {
					return err
				}
				if defBlk == nil { // tailBuf is reused next block; copy it
					defBlk = append([]byte(nil), tail...)
				}
				s.deferred = &deferredMatch{
					streamOff: streamOff,
					blockOff:  defStart,
					matchOff:  defOff,
					blk:       defBlk,
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
		} else if s.prevLazy != nil {
			// Build a SearchResult whose PrevBlock() decodes lazily.
			lb := s.prevLazy
			result = SearchResult{
				Blocks:       [2][]byte{nil, blk},
				StreamOffset: streamOff,
				BlockStart:   prevBlockStart,
				PrevBlockLen: lb.uncompSize,
				Offset:       lb.uncompSize + matchOff,
				prevLazy:     wrapLazyMainBlock(lb),
			}
		} else {
			result = SearchResult{
				Blocks:       [2][]byte{nil, blk},
				StreamOffset: streamOff,
				BlockStart:   prevBlockStart,
				Offset:       matchOff,
			}
		}
		s.blockMatches++
		if err := fn(result); err != nil {
			if !errors.Is(err, ErrSearchForward) {
				return err
			}
			s.deferred = &deferredMatch{
				streamOff: streamOff,
				blockOff:  blockOff,
				matchOff:  matchOff,
				blk:       blk,
			}
		}
		off = matchOff + 1
	}
}

// flushDeferred re-dispatches a deferred ErrSearchForward match with nextBlk
// as forward context.
func (s *SidecarSearcher) flushDeferred(nextBlk []byte, fn func(SearchResult) error) error {
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

// statsAccumTable records per-table stats consistent with BlockSearcher.
func (s *SidecarSearcher) statsAccumTable(baseSize, reductions uint8, table []byte, chunkLen int, compressed bool) {
	s.stats.TablesPresent++
	s.stats.TablesBytes += int64(chunkLen + 4)
	if compressed {
		s.stats.TablesCompressed++
		s.stats.TablesCompressedBytes += int64(chunkLen + 4)
		s.stats.TableBitmapBytes += int64(len(table))
	}
	s.stats.TableBitsSum += int(baseSize - reductions)
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

// --- internal helpers --------------------------------------------------------

func (s *SidecarSearcher) readFull(buf []byte) (int, error) {
	n, err := io.ReadFull(s.sidecar, buf)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

func (s *SidecarSearcher) readChunkHeader() (chunkType byte, chunkLen int, err error) {
	_, err = io.ReadFull(s.sidecar, s.tmp[:4])
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		return 0, 0, err
	}
	return s.tmp[0], int(s.tmp[1]) | int(s.tmp[2])<<8 | int(s.tmp[3])<<16, nil
}

func (s *SidecarSearcher) readPayload(n int) ([]byte, error) {
	if cap(s.scratch) < n {
		s.scratch = make([]byte, n)
	}
	buf := s.scratch[:n]
	if _, err := io.ReadFull(s.sidecar, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *SidecarSearcher) discard(n int) error {
	if n == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, s.sidecar, int64(n))
	return err
}

// readAndDecodeMainBlock fetches and decodes a block from the main stream
// via a single ReadAt. Returns the decoded bytes (newly allocated).
func readAndDecodeMainBlock(main io.ReaderAt, offset int64, uncomp, maxBlock int, ignoreCRC bool) ([]byte, error) {
	maxLen := MaxEncodedLen(uncomp) + obufHeaderLen + binary.MaxVarintLen64
	buf := make([]byte, maxLen)
	n, err := main.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:n]
	if len(buf) < 4 {
		return nil, ErrCorrupt
	}
	ct := buf[0]
	cl := int(buf[1]) | int(buf[2])<<8 | int(buf[3])<<16
	if 4+cl > len(buf) {
		// Read more.
		extra := make([]byte, 4+cl)
		n, err := main.ReadAt(extra, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != len(extra) {
			return nil, io.ErrUnexpectedEOF
		}
		buf = extra
	}
	return decodeDataChunk(ct, buf[4:4+cl], uncomp, maxBlock, ignoreCRC)
}

// decodeDataChunk decodes the payload of a data chunk into a fresh slice.
func decodeDataChunk(chunkType byte, payload []byte, uncompHint, maxBlock int, ignoreCRC bool) ([]byte, error) {
	if len(payload) < checksumSize {
		return nil, ErrCorrupt
	}
	checksum := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
	body := payload[checksumSize:]
	switch chunkType {
	case chunkTypeUncompressedData:
		if !ignoreCRC && crc(body) != checksum {
			return nil, ErrCRC
		}
		if uncompHint != 0 && len(body) != uncompHint {
			return nil, fmt.Errorf("%w: uncompressed size mismatch: %d != %d", ErrSidecarInvalid, len(body), uncompHint)
		}
		out := make([]byte, len(body))
		copy(out, body)
		return out, nil
	case chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC:
		n, hdrSize, err := decodedLen(body)
		if err != nil {
			return nil, err
		}
		if n > maxBlock {
			return nil, ErrTooLarge
		}
		if uncompHint != 0 && n != uncompHint {
			return nil, fmt.Errorf("%w: decompressed size %d != hint %d", ErrSidecarInvalid, n, uncompHint)
		}
		out := make([]byte, n)
		if ret := minLZDecode(out, body[hdrSize:]); ret != 0 {
			return nil, ErrCorrupt
		}
		toCRC := out
		if chunkType == chunkTypeMinLZCompressedDataCompCRC {
			toCRC = body[hdrSize:]
		}
		if !ignoreCRC && crc(toCRC) != checksum {
			return nil, ErrCRC
		}
		return out, nil
	}
	return nil, ErrUnsupported
}

// wrapLazyMainBlock adapts lazyMainBlock to the lazyBlock structure that
// SearchResult.PrevBlock() expects. decode() is deferred until PrevBlock()
// is actually called.
func wrapLazyMainBlock(lb *lazyMainBlock) *lazyBlock {
	return &lazyBlock{
		main:       lb.main,
		mainOffset: lb.offset,
		decompLen:  lb.uncompSize,
		maxBlock:   lb.maxBlock,
		ignoreCRC:  lb.ignoreCRC,
	}
}
