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

package reference

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/klauspost/compress/huff0"
)

// Chunk types from SPEC_SEARCH.md section 2.
const (
	ChunkSearchInfo            = 0x44
	ChunkSearchTable           = 0x45
	ChunkSearchTableCompressed = 0x46
	ChunkRemoteBlockRef        = 0x47
)

// Table types from SPEC_SEARCH.md section 3.0.
const (
	TableTypeNoPrefix   = 1
	TableTypeBytePrefix = 2
	TableTypeMaskPrefix = 3
	TableTypeLongPrefix = 4
)

// Hash primes from SPEC_SEARCH.md section 3.1.
const (
	prime2bytes uint32 = 40503
	prime3bytes uint32 = 506832829
	prime4bytes uint32 = 2654435761
	prime5bytes uint64 = 889523592379
	prime6bytes uint64 = 227718039650203
	prime7bytes uint64 = 58295818150454627
	prime8bytes uint64 = 0xcf1bbcdcb7a56463
)

// Tunable thresholds. Edit to change behaviour; not exposed via the API.
const (
	maxReducedPopPct      = 25 // stop reducing while reduced density would exceed this
	huff0BlockSizeLog2Max = 17 // largest huff0 sub-block per SPEC_SEARCH.md section 2.2 (128 KiB)
	huff0BlockSizeLog2Min = 5  // smallest huff0 sub-block (32 B)
	searchTableMinBytes   = 32 // minimum bitmap = 256 entries = 32 bytes
)

// Dispositions for 0x46 sub-blocks (SPEC_SEARCH.md section 2.2).
const (
	dispRaw    = 16
	dispRLE    = 17
	dispSparse = 18
)

// SearchConfig is the per-block search-table wire descriptor.
//
// Wire fields only — tunable thresholds are package constants, not config.
type SearchConfig struct {
	MatchLen      uint8    // 1..8
	BaseTableSize uint8    // 8..23 (log2 of bit count)
	TableType     uint8    // 1..4
	Extras        uint8    // type 4 only: E+1 hashes per prefix occurrence; MatchLen+Extras ≤ 16
	PrefixBytes   []byte   // type 2: 1..8 bytes (will be padded to 8 on the wire)
	PrefixMask    [32]byte // type 3: 256-bit set
	LongPrefix    []byte   // type 4: 1..256 bytes
}

// RemoteBlockRef references one remote data block (chunk 0x47).
type RemoteBlockRef struct {
	Offset            uint64 // stream byte offset of the referenced block
	MaxMinusActualLen uint64 // maxBlockSize - actualUncompressedSize
}

// HashValue returns a table index for the lowest matchLen bytes of val.
// tableSize is the number of output bits (8..23). matchLen must be 1..8.
// Direct translation of SPEC_SEARCH.md section 3.1.
func HashValue(val uint64, tableSize, matchLen uint8) uint32 {
	switch matchLen {
	case 1:
		return uint32(val & 0xff)
	case 2:
		if tableSize >= 16 {
			return uint32(val & 0xffff)
		}
		return (uint32(val<<16) * prime2bytes) >> (32 - tableSize)
	case 3:
		return (uint32(val<<8) * prime3bytes) >> (32 - tableSize)
	case 4:
		return (uint32(val) * prime4bytes) >> (32 - tableSize)
	case 5:
		return uint32(((val << 24) * prime5bytes) >> (64 - tableSize))
	case 6:
		return uint32(((val << 16) * prime6bytes) >> (64 - tableSize))
	case 7:
		return uint32(((val << 8) * prime7bytes) >> (64 - tableSize))
	case 8:
		return uint32((val * prime8bytes) >> (64 - tableSize))
	}
	return 0
}

// BuildSearchTable produces the packed bitmap and the chosen reductions count
// for blockData. overlap is up to MatchLen+Extras bytes from the start of
// the next block (empty for the last block in a stream).
//
// The returned table length is 1 << (BaseTableSize - reductions - 3) bytes,
// rounded up to the 32-byte minimum.
func BuildSearchTable(cfg SearchConfig, blockData, overlap []byte) (table []byte, reductions uint8) {
	tableBytes := 1 << (cfg.BaseTableSize - 3)
	if tableBytes < searchTableMinBytes {
		tableBytes = searchTableMinBytes
	}
	table = make([]byte, tableBytes)

	// Index every starting position in blockData. The window read at each
	// position can cross into overlap. With Extras=E the encoder writes E+1
	// hashes per indexed position (offsets 0..E), so the last byte read is
	// matchLen+E-1 past the position.
	ml := int(cfg.MatchLen)
	ex := int(cfg.Extras)
	last := len(blockData) - ml - ex + 1 // last position where all windows fit fully inside blockData
	if last < 0 {
		last = 0
	}

	indexAt := func(pos int) {
		// For prefix table types, the preceding byte must be a valid prefix.
		// Position 0 has no preceding byte, so it's skipped for prefix tables.
		if cfg.TableType != TableTypeNoPrefix {
			if pos == 0 {
				return
			}
			if !prefixOK(cfg, blockData, pos) {
				return
			}
		}
		var scratch [16]byte
		n := copy(scratch[:], blockData[pos:])
		copy(scratch[n:], overlap)
		for j := 0; j <= ex; j++ {
			h := HashValue(readLE64Pad(scratch[j:j+ml]), cfg.BaseTableSize, cfg.MatchLen)
			setBit(table, h)
		}
	}

	// Positions fully inside blockData.
	for pos := 0; pos < last; pos++ {
		indexAt(pos)
	}
	// Overlap tail: positions whose windows extend into the next block.
	// Only emitted when overlap data is available — the last block of a stream
	// has no overlap, so its tail positions are not indexed.
	//
	// Prefix tables also index position len(blockData): the window whose prefix
	// byte is the block's last byte begins in the next block and cannot be
	// indexed there (it would be position 0), so block N records it (3.3.1).
	if len(overlap) > 0 {
		end := len(blockData)
		if cfg.TableType != TableTypeNoPrefix {
			end = len(blockData) + 1
		}
		for pos := last; pos < end; pos++ {
			indexAt(pos)
		}
	}

	// Long prefix: index occurrences whose prefix starts in this block but
	// straddles into the next (window-start past the block end). The spec puts
	// an occurrence in the block where its prefix starts; block N+1 can't index
	// it (its prefix is partly here). Read the prefix tail and window(s) from
	// the overlap. Single-byte prefixes can't straddle.
	if len(overlap) > 0 && cfg.TableType == TableTypeLongPrefix {
		pl := len(cfg.LongPrefix)
		s := len(blockData)
		for q := max(0, s-pl+1); q < s; q++ {
			k := s - q // prefix bytes inside this block (1..pl-1)
			if pl-k > len(overlap) {
				continue
			}
			ok := true
			for i := 0; i < k && ok; i++ {
				ok = blockData[q+i] == cfg.LongPrefix[i]
			}
			for i := 0; i < pl-k && ok; i++ {
				ok = overlap[i] == cfg.LongPrefix[k+i]
			}
			if !ok {
				continue
			}
			woff := q + pl - s // window start within overlap
			for j := 0; j <= ex; j++ {
				off := woff + j
				if off > len(overlap) {
					off = len(overlap)
				}
				setBit(table, HashValue(readLE64Pad(overlap[off:]), cfg.BaseTableSize, cfg.MatchLen))
			}
		}
	}

	return reduceTable(table)
}

// prefixOK reports whether the byte preceding pos satisfies the prefix
// predicate for cfg.TableType (assumed != TableTypeNoPrefix).
func prefixOK(cfg SearchConfig, blockData []byte, pos int) bool {
	prev := blockData[pos-1]
	switch cfg.TableType {
	case TableTypeBytePrefix:
		for _, p := range cfg.PrefixBytes {
			if prev == p {
				return true
			}
		}
		return false
	case TableTypeMaskPrefix:
		return cfg.PrefixMask[prev>>3]&(1<<(prev&7)) != 0
	case TableTypeLongPrefix:
		k := len(cfg.LongPrefix)
		if pos < k {
			return false
		}
		for i := 0; i < k; i++ {
			if blockData[pos-k+i] != cfg.LongPrefix[i] {
				return false
			}
		}
		return true
	}
	return false
}

// setBit sets bit h in the packed bitmap.
func setBit(table []byte, h uint32) {
	table[h>>3] |= 1 << (h & 7)
}

// readLE64Pad reads up to 8 bytes from b as a little-endian uint64,
// zero-padding when b is shorter than 8 bytes.
func readLE64Pad(b []byte) uint64 {
	if len(b) >= 8 {
		return binary.LittleEndian.Uint64(b)
	}
	var v uint64
	for i := range b {
		v |= uint64(b[i]) << (i * 8)
	}
	return v
}

// reduceTable halves the bitmap while the reduced density would stay at or
// below maxReducedPopPct. Stops at searchTableMinBytes (256 entries).
// The halving rule combines `lower[i] |= upper[i]` per SPEC_SEARCH.md 3.2.
func reduceTable(table []byte) ([]byte, uint8) {
	reductions := uint8(0)
	for len(table) > searchTableMinBytes {
		half := len(table) / 2
		lower := table[:half]
		upper := table[half:]
		var setBits int
		for i := range lower {
			merged := lower[i] | upper[i]
			setBits += popcount8(merged)
		}
		// Compute density of the proposed reduced table.
		totalBits := half * 8
		if setBits*100 > maxReducedPopPct*totalBits {
			break
		}
		// Commit the merge.
		for i := range lower {
			lower[i] |= upper[i]
		}
		table = lower
		reductions++
	}
	return table, reductions
}

func popcount8(b byte) int {
	b = (b & 0x55) + ((b >> 1) & 0x55)
	b = (b & 0x33) + ((b >> 2) & 0x33)
	return int((b & 0x0f) + (b >> 4))
}

// crc32cTable is the Castagnoli table used for both block-data and
// search-table checksums.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// crc32c implements the MinLZ checksum from section 3 of the snappy framing
// format spec. The transform is `c>>15 | c<<17 + magic`; Go's `+` binds
// tighter than `|`, so this parses as `c>>15 | (c<<17 + 0xa282ead8)`.
func crc32c(b []byte) uint32 {
	c := crc32.Update(0, crc32cTable, b)
	return c>>15 | c<<17 + 0xa282ead8
}

// appendChunkHeader writes the 4-byte MinLZ chunk header (chunk type + 3-byte
// little-endian payload size).
func appendChunkHeader(dst []byte, chunkType byte, payloadSize int) []byte {
	return append(dst, chunkType, byte(payloadSize), byte(payloadSize>>8), byte(payloadSize>>16))
}

// prefixSize returns the on-wire size of the prefix data for cfg.TableType.
func prefixSize(cfg SearchConfig) int {
	switch cfg.TableType {
	case TableTypeBytePrefix:
		return 8
	case TableTypeMaskPrefix:
		return 32
	case TableTypeLongPrefix:
		// length byte + extras byte + prefix bytes
		return 2 + len(cfg.LongPrefix)
	}
	return 0
}

// appendConfig writes the 3-byte type/matchLen/baseTableSize header followed
// by the prefix data (varies by table type).
func appendConfig(dst []byte, cfg SearchConfig) []byte {
	dst = append(dst, cfg.TableType, cfg.MatchLen, cfg.BaseTableSize)
	switch cfg.TableType {
	case TableTypeBytePrefix:
		// Pad to 8 bytes by repeating the last entry, per SPEC_SEARCH.md 3.3.2.
		var pad [8]byte
		n := copy(pad[:], cfg.PrefixBytes)
		if n == 0 {
			panic("minlz: TableTypeBytePrefix requires at least one prefix byte")
		}
		for i := n; i < 8; i++ {
			pad[i] = pad[n-1]
		}
		return append(dst, pad[:]...)
	case TableTypeMaskPrefix:
		return append(dst, cfg.PrefixMask[:]...)
	case TableTypeLongPrefix:
		dst = append(dst, byte(len(cfg.LongPrefix)-1), cfg.Extras)
		return append(dst, cfg.LongPrefix...)
	}
	return dst
}

// AppendSearchInfoChunk appends a complete 0x44 chunk (header + payload).
func AppendSearchInfoChunk(dst []byte, cfg SearchConfig) []byte {
	dst = appendChunkHeader(dst, ChunkSearchInfo, 3+prefixSize(cfg))
	return appendConfig(dst, cfg)
}

// AppendSearchTableChunk appends a complete 0x45 chunk: header, config,
// reductions byte, CRC32 of the bitmap, then the bitmap bytes.
func AppendSearchTableChunk(dst []byte, cfg SearchConfig, reductions uint8, table []byte) []byte {
	payloadSize := 3 + prefixSize(cfg) + 1 + 4 + len(table)
	dst = appendChunkHeader(dst, ChunkSearchTable, payloadSize)
	dst = appendConfig(dst, cfg)
	dst = append(dst, reductions)
	dst = binary.LittleEndian.AppendUint32(dst, crc32c(table))
	return append(dst, table...)
}

// AppendSearchTableCompressedChunk appends a complete 0x46 chunk. The bitmap
// is split into one or more huff0 sub-blocks; each sub-block carries its own
// huff0 table when compression helps, otherwise disposition 16 (raw) is used.
// RLE and sparse dispositions are not produced by this reference, but the
// decoder accepts them.
func AppendSearchTableCompressedChunk(dst []byte, cfg SearchConfig, reductions uint8, table []byte) []byte {
	log2bs, nBlocks := pickHuff0BlockSize(len(table))
	blockSize := 1 << log2bs

	// Per-sub-block compression. ti < 16 means the block carries its own
	// huff0 table at position tableIdx in the chunk's table list.
	type sub struct {
		ti       uint8
		tableHdr []byte // serialized huff0 table header (empty for raw)
		payload  []byte // compressed bytes for ti<16, raw bytes for ti=16
	}
	subs := make([]sub, nBlocks)
	tableIdx := uint8(0)
	for i := range nBlocks {
		slice := table[i*blockSize : (i+1)*blockSize]
		var sc huff0.Scratch
		sc.Reuse = huff0.ReusePolicyNone
		_, _, err := huff0.Compress4X(slice, &sc)
		if err == nil && len(sc.OutTable)+len(sc.OutData) < blockSize {
			subs[i] = sub{
				ti:       tableIdx,
				tableHdr: append([]byte(nil), sc.OutTable...),
				payload:  append([]byte(nil), sc.OutData...),
			}
			tableIdx++
			continue
		}
		subs[i] = sub{ti: dispRaw, payload: slice}
	}
	tc := tableIdx

	// Compute payload size.
	payloadSize := 3 + prefixSize(cfg) + 1 + 4 + 2
	for _, s := range subs {
		payloadSize += len(s.tableHdr)
		switch s.ti {
		case dispRaw:
			payloadSize += 1 + blockSize
		default:
			payloadSize += 1 + uvarintLen(uint64(len(s.payload))) + len(s.payload)
		}
	}

	dst = appendChunkHeader(dst, ChunkSearchTableCompressed, payloadSize)
	dst = appendConfig(dst, cfg)
	dst = append(dst, reductions)
	dst = binary.LittleEndian.AppendUint32(dst, crc32c(table))
	dst = append(dst, log2bs, tc)
	for _, s := range subs {
		dst = append(dst, s.tableHdr...)
	}
	for _, s := range subs {
		dst = append(dst, s.ti)
		switch s.ti {
		case dispRaw:
			dst = append(dst, s.payload...)
		default:
			dst = binary.AppendUvarint(dst, uint64(len(s.payload)))
			dst = append(dst, s.payload...)
		}
	}
	return dst
}

// pickHuff0BlockSize chooses the log2(blockSize) and block count for a bitmap
// of bitmapLen bytes: a single block when the bitmap fits in the maximum huff0
// block size, otherwise split into max-sized blocks. bitmapLen is always a
// power of two ≥ 32, so the result divides exactly.
func pickHuff0BlockSize(bitmapLen int) (log2bs uint8, nBlocks int) {
	log2bs = uint8(log2Pow2(bitmapLen))
	if log2bs > huff0BlockSizeLog2Max {
		log2bs = huff0BlockSizeLog2Max
	}
	if log2bs < huff0BlockSizeLog2Min {
		panic(fmt.Sprintf("minlz: bitmap of %d bytes is below the 32-byte minimum", bitmapLen))
	}
	return log2bs, bitmapLen >> log2bs
}

// log2Pow2 returns log2(n) for n that is a positive power of two.
func log2Pow2(n int) int {
	r := 0
	for n > 1 {
		n >>= 1
		r++
	}
	return r
}

// uvarintLen returns the number of bytes binary.AppendUvarint would write for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// AppendRemoteBlockRefChunk emits a complete 0x47 chunk containing one or more
// remote block references. refs[i].Offset is the absolute stream offset; the
// wire encoding stores the first offset absolutely and subsequent offsets as
// positive deltas (SPEC_SEARCH.md section 2.3). Offsets must be strictly
// ascending.
func AppendRemoteBlockRefChunk(dst []byte, refs []RemoteBlockRef) []byte {
	var payload []byte
	for i, r := range refs {
		var wireOff uint64
		if i == 0 {
			wireOff = r.Offset
		} else {
			if r.Offset <= refs[i-1].Offset {
				panic(fmt.Sprintf("minlz: remote block ref offsets must be strictly ascending (refs[%d].Offset=%d <= refs[%d].Offset=%d)", i, r.Offset, i-1, refs[i-1].Offset))
			}
			wireOff = r.Offset - refs[i-1].Offset
		}
		payload = binary.AppendUvarint(payload, wireOff)
		payload = binary.AppendUvarint(payload, r.MaxMinusActualLen)
	}
	dst = appendChunkHeader(dst, ChunkRemoteBlockRef, len(payload))
	return append(dst, payload...)
}
