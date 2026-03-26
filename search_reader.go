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
	BlocksTotal         int     // Total data blocks encountered
	BlocksSkipped       int     // Blocks skipped via search table (definitely no match)
	BlocksSearched      int     // Blocks decoded and passed to callback
	CompBytesSkipped    int64   // Compressed bytes skipped
	UncompBytesSearched int64   // Uncompressed bytes decoded and searched
	TablesPresent       int     // Number of 0x45 search table chunks seen
	TablesBytes         int64   // Total bytes of search table chunks
	TablesMissing       int     // Data blocks without a preceding search table
	TablesUnusable      int     // Tables present but incompatible with query
	TableBitsSum        int     // Sum of effective table bits (for avg: TableBitsSum/TablesPresent)
	TableReductionsSum  int     // Sum of reductions applied (for avg: TableReductionsSum/TablesPresent)
	TablePopMin         float64 // Min population % across tables seen
	TablePopMax         float64 // Max population % across tables seen
	TablePopSum         float64 // Sum of population % (for computing average)
	UncompressedSize    int64   // Total uncompressed bytes in stream
}

// Fprint writes a human-readable summary of the search stats to w.
func (st SearchStats) Fprint(w io.Writer) {
	fmt.Fprintf(w, "Blocks total: %d, skipped: %d, searched: %d\n", st.BlocksTotal, st.BlocksSkipped, st.BlocksSearched)
	if st.BlocksTotal > 0 {
		fmt.Fprintf(w, "Skip rate: %.1f%%\n", 100*float64(st.BlocksSkipped)/float64(st.BlocksTotal))
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
	}
}

// ErrSearchForward can be returned from the search callback to request
// forward context. The searcher will decode the next block, shift Blocks
// forward, and re-call the callback with the same match but more context.
var ErrSearchForward = errors.New("minlz: forward to next block")

// SearchResult is passed to the search callback for each pattern match.
type SearchResult struct {
	// Blocks contains the block data surrounding the match.
	// Blocks[0] is the previous block (nil if first or previous was skipped).
	// Blocks[1] is the current block. The match may start in Blocks[0] (boundary match).
	// Both slices are invalid after the callback returns.
	Blocks [2][]byte

	// Offset is the position of the match within the Blocks.
	// Relative to Blocks[0] if non-nil, otherwise relative to Blocks[1].
	Offset int

	// StreamOffset is the absolute uncompressed stream offset of the match.
	StreamOffset int64

	// BlockStart is the stream offset of Blocks[0] (or Blocks[1] if Blocks[0]==nil).
	// Invariant: Offset == int(StreamOffset - BlockStart)
	BlockStart int64
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

	decoded    [2][]byte      // alternating decode buffers
	decIdx     int            // which buffer was last used (0 or 1)
	prevBlock  []byte         // points into decoded[(decIdx+1)&1], nil if skipped
	deferred   *deferredMatch // pending ErrSearchForward re-dispatch
	maxBlock   int
	readHeader bool
	ignoreCRC  bool
	bail       bool // return error if tables can't answer query
	blockStart int64
	stats      SearchStats
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

// BlockSearchIgnoreCRC skips CRC validation during search.
func BlockSearchIgnoreCRC() BlockSearchOption {
	return func(s *BlockSearcher) error {
		s.ignoreCRC = true
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

	defer func() { s.stats.UncompressedSize = s.blockStart }()

	for {
		if !s.readFull(s.tmp[:4]) {
			if s.err == io.EOF {
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
			s.maxBlock = blockSize
			s.blockStart = 0
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
			continue

		case chunkTypeSearchTable:
			s.ensureBuf(chunkLen)
			if !s.readFull(s.buf[:chunkLen]) {
				return s.err
			}
			cfg, reductions, table, err := parseSearchTable(s.buf[:chunkLen])
			if err != nil {
				return err
			}
			s.blockInfo = cfg
			s.blockReductions = reductions
			s.blockTable = table
			s.stats.TablesPresent++
			s.stats.TablesBytes += int64(chunkLen + 4) // +4 for chunk header
			s.stats.TableBitsSum += int(cfg.baseTableSize - reductions)
			s.stats.TableReductionsSum += int(reductions)
			// Compute population %.
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
			continue

		case chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC:
			if chunkLen < checksumSize {
				return ErrCorrupt
			}
			s.stats.BlocksTotal++
			tableNoMatch := false
			if s.blockTable != nil {
				canUse, match := patternCanMatch(&s.blockInfo, s.blockTable, s.blockReductions, pattern)
				s.blockTable = nil
				if canUse && !match {
					if s.prevBlock == nil || !canBoundaryMatch(s.prevBlock, pattern) {
						s.stats.BlocksSkipped++
						s.stats.CompBytesSkipped += int64(chunkLen)
						// Read checksum + varint to get exact decompressed size before skipping.
						if !s.readFull(s.tmp[:checksumSize]) {
							return s.err
						}
						remain := chunkLen - checksumSize
						// Read enough for the varint (max 10 bytes).
						peek := min(remain, 10)
						if !s.readFull(s.tmp[checksumSize : checksumSize+peek]) {
							return s.err
						}
						n, _, _ := decodedLen(s.tmp[checksumSize : checksumSize+peek])
						if n > 0 {
							s.blockStart += int64(n)
						}
						if !s.skip(remain - peek) {
							return s.err
						}
						if s.deferred != nil {
							if err := s.flushDeferred(nil, fn); err != nil {
								return err
							}
						}
						s.prevBlock = nil
						continue
					}
					// Table says no match, but prevBlock tail has a pattern prefix — must decode.
					tableNoMatch = true
				}
				if !canUse {
					s.stats.TablesUnusable++
					if s.bail {
						return ErrSearchTablesUnusable
					}
				}
			} else {
				s.stats.TablesMissing++
				if s.bail {
					return ErrSearchTablesUnusable
				}
			}

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
			di := s.decIdx ^ 1 // alternate buffer
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

			s.stats.BlocksSearched++
			s.stats.UncompBytesSearched += int64(n)
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
			if tableNoMatch {
				// Table confirmed pattern can't start in this block,
				// so no future block needs a boundary check against it.
				s.prevBlock = nil
			} else {
				s.prevBlock = blk
			}
			continue

		case chunkTypeUncompressedData:
			if chunkLen < checksumSize {
				return ErrCorrupt
			}
			s.stats.BlocksTotal++
			tableNoMatch := false
			if s.blockTable != nil {
				canUse, match := patternCanMatch(&s.blockInfo, s.blockTable, s.blockReductions, pattern)
				s.blockTable = nil
				if canUse && !match {
					if s.prevBlock == nil || !canBoundaryMatch(s.prevBlock, pattern) {
						s.stats.BlocksSkipped++
						s.stats.CompBytesSkipped += int64(chunkLen)
						if !s.skip(chunkLen) {
							return s.err
						}
						s.blockStart += int64(chunkLen - checksumSize)
						if s.deferred != nil {
							if err := s.flushDeferred(nil, fn); err != nil {
								return err
							}
						}
						s.prevBlock = nil
						continue
					}
					tableNoMatch = true
				}
				if !canUse {
					s.stats.TablesUnusable++
					if s.bail {
						return ErrSearchTablesUnusable
					}
				}
			} else {
				s.stats.TablesMissing++
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

			s.stats.BlocksSearched++
			s.stats.UncompBytesSearched += int64(n)
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
			if tableNoMatch {
				s.prevBlock = nil
			} else {
				s.prevBlock = blk
			}
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

// dispatchMatches finds all pattern occurrences in blk and calls fn for each.
// If fn returns ErrSearchForward, the match is saved as s.deferred for the main
// loop to re-dispatch after loading the next block. Remaining matches in this
// block are still dispatched.
func (s *BlockSearcher) dispatchMatches(blk []byte, blockOff int64, pattern []byte, fn func(SearchResult) error) error {
	prevBlockStart := blockOff - int64(len(s.prevBlock))
	if s.prevBlock == nil {
		prevBlockStart = blockOff
	}

	// Check for matches spanning the boundary between prevBlock and blk.
	if s.prevBlock != nil && len(pattern) > 1 {
		tail := s.prevBlock[max(0, len(s.prevBlock)-len(pattern)+1):]
		head := blk[:min(len(blk), len(pattern)-1)]
		boundary := append(tail, head...)
		bOff := 0
		for {
			idx := bytes.Index(boundary[bOff:], pattern)
			if idx < 0 {
				break
			}
			// Only report matches that actually straddle the boundary
			// (start in prevBlock, end in blk).
			absIdx := bOff + idx
			matchInPrev := len(tail) - absIdx
			if matchInPrev <= 0 || matchInPrev >= len(pattern) {
				bOff = absIdx + 1
				continue
			}
			streamOff := blockOff - int64(matchInPrev)
			result := SearchResult{
				Blocks:       [2][]byte{s.prevBlock, blk},
				Offset:       len(s.prevBlock) - matchInPrev,
				StreamOffset: streamOff,
				BlockStart:   prevBlockStart,
			}
			err := fn(result)
			if err != nil {
				if errors.Is(err, ErrSearchForward) {
					s.deferred = &deferredMatch{
						streamOff: streamOff,
						blockOff:  blockOff - int64(len(s.prevBlock)),
						matchOff:  len(s.prevBlock) - matchInPrev,
						blk:       s.prevBlock,
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
			}
		} else {
			result = SearchResult{
				Blocks:       [2][]byte{nil, blk},
				Offset:       matchOff,
				StreamOffset: streamOff,
				BlockStart:   blockOff,
			}
		}

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
		if len(pattern) < int(cfg.matchLen) {
			return false, true
		}
		checked := 0
		firstCheckedPresent := false
		for i := 0; i <= len(pattern)-pl-int(cfg.matchLen); i++ {
			if !bytes.Equal(pattern[i:i+pl], cfg.longPrefix) {
				continue
			}
			v := readLE64Pad(pattern[i+pl:])
			h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
			present := table[h>>3]&(1<<(h&7)) != 0
			if checked == 0 {
				firstCheckedPresent = present
			}
			if !present {
				if firstCheckedPresent {
					return true, true
				}
				return true, false
			}
			checked++
		}
		if checked == 0 {
			return false, true
		}
		return true, true
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
