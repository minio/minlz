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

// SearchResult is passed to the search callback for each candidate block.
type SearchResult struct {
	// Data is the decoded block content.
	Data []byte
	// BlockStart is the uncompressed byte offset of this block in the stream.
	BlockStart int64
	// prevBlock holds the previous block's decoded data, or nil if the
	// previous block was skipped or this is the first block.
	prevBlock []byte
}

// PrevBlock returns the previous block's decoded data.
// Returns nil if this is the first block or the previous block was skipped.
func (r *SearchResult) PrevBlock() []byte {
	return r.prevBlock
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

	decoded    []byte
	prevBlock  []byte // previous block's decoded data (nil if skipped)
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

// Search iterates blocks in the stream. For each block that might contain
// pattern, it decodes the block and calls fn with a SearchResult.
// fn returns false to stop the search.
//
// SearchResult.PrevBlock() returns the previous decoded block (nil if skipped or first block),
// which is useful for matching patterns that span block boundaries.
//
// For type 1 (no prefix) tables, all matchLen-sized windows of pattern
// are checked. For prefix tables, the pattern must begin with the prefix
// for tables to be used.
func (s *BlockSearcher) Search(pattern []byte, fn func(r SearchResult) bool) error {
	if s.err != nil {
		return s.err
	}
	s.stats = SearchStats{}

	defer func() { s.stats.UncompressedSize = s.blockStart }()

	for {
		if !s.readFull(s.tmp[:4]) {
			if s.err == io.EOF {
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
			if s.blockTable != nil {
				canUse, match := patternCanMatch(&s.blockInfo, s.blockTable, s.blockReductions, pattern)
				s.blockTable = nil
				if canUse && !match {
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
					s.prevBlock = nil
					continue
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
			if n > len(s.decoded) {
				s.decoded = make([]byte, n)
			}
			buf = buf[hdrLen:]
			if ret := minLZDecode(s.decoded[:n], buf); ret != 0 {
				return ErrCorrupt
			}
			toCRC := s.decoded[:n]
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
			result := SearchResult{Data: s.decoded[:n], BlockStart: blockOff, prevBlock: s.prevBlock}
			s.prevBlock = append(s.prevBlock[:0], s.decoded[:n]...)
			if !fn(result) {
				return nil
			}
			continue

		case chunkTypeUncompressedData:
			if chunkLen < checksumSize {
				return ErrCorrupt
			}
			s.stats.BlocksTotal++
			if s.blockTable != nil {
				canUse, match := patternCanMatch(&s.blockInfo, s.blockTable, s.blockReductions, pattern)
				s.blockTable = nil
				if canUse && !match {
					s.stats.BlocksSkipped++
					s.stats.CompBytesSkipped += int64(chunkLen)
					if !s.skip(chunkLen) {
						return s.err
					}
					s.blockStart += int64(chunkLen - checksumSize)
					s.prevBlock = nil
					continue
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
			if n > len(s.decoded) {
				s.decoded = make([]byte, n)
			}
			if !s.readFull(s.decoded[:n]) {
				return s.err
			}
			if !s.ignoreCRC && crc(s.decoded[:n]) != checksum {
				return ErrCRC
			}

			s.stats.BlocksSearched++
			s.stats.UncompBytesSearched += int64(n)
			blockOff := s.blockStart
			s.blockStart += int64(n)
			result := SearchResult{Data: s.decoded[:n], BlockStart: blockOff, prevBlock: s.prevBlock}
			s.prevBlock = append(s.prevBlock[:0], s.decoded[:n]...)
			if !fn(result) {
				return nil
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
		// Check ALL matchLen-windows. All must be present.
		if len(pattern) < int(cfg.matchLen) {
			return false, true
		}
		for i := 0; i <= len(pattern)-int(cfg.matchLen); i++ {
			v := readLE64Pad(pattern[i:])
			h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
			if table[h>>3]&(1<<(h&7)) == 0 {
				return true, false
			}
		}
		return true, true

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
		// Scan pattern for any occurrence of the long prefix, check the window after it.
		checked := 0
		for i := 0; i <= len(pattern)-pl-int(cfg.matchLen); i++ {
			if !bytes.Equal(pattern[i:i+pl], cfg.longPrefix) {
				continue
			}
			v := readLE64Pad(pattern[i+pl:])
			h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
			if table[h>>3]&(1<<(h&7)) == 0 {
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
	checked := 0
	for i := 0; i <= len(pattern)-ml; i++ {
		if i > 0 {
			b := pattern[i-1]
			if pfxMask[b>>3]&(1<<(b&7)) == 0 {
				continue
			}
		} else {
			// Position 0: we don't know what precedes the pattern in the data.
			// Skip — we can't check this window.
			continue
		}
		v := readLE64Pad(pattern[i:])
		h := hashValue(v, cfg.baseTableSize, cfg.matchLen) & mask
		if table[h>>3]&(1<<(h&7)) == 0 {
			return true, false
		}
		checked++
	}
	if checked == 0 {
		return false, true
	}
	return true, true
}
