package minlz

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
)

const (
	searchTableTypeNoPrefix   = 1
	searchTableTypeBytePrefix = 2
	searchTableTypeMaskPrefix = 3
	searchTableTypeLongPrefix = 4

	searchTableMinLog2 = 8  // 256 entries = 32 bytes
	searchTableMaxLog2 = 23 // matches maxBlockLog

	defaultMaxPopPct      = 70
	defaultMaxConflictPct = 25
)

// Hash primes from SPEC_SEARCH.md
const (
	prime2bytes uint32 = 40503
	prime3bytes uint32 = 506832829
	prime4bytes uint32 = 2654435761
	prime5bytes uint64 = 889523592379
	prime6bytes uint64 = 227718039650203
	prime7bytes uint64 = 58295818150454627
	prime8bytes uint64 = 0xcf1bbcdcb7a56463
)

// SearchTableConfig configures search table generation for a Writer.
// Use NewSearchTableConfig to create, then chain With* methods to customize.
type SearchTableConfig struct {
	matchLen      uint8
	tableType     uint8
	baseTableSize uint8 // log2, computed at writer init from block size
	prefixBytes   [8]byte
	prefixMask         [32]byte
	longPrefix         []byte
	maxPopPct          int
	maxConflictPct     int
}

// NewSearchTableConfig creates a search table config.
// Defaults: matchLen=6, no prefix (type 1), auto table size, 70% max population, 25% max conflicts.
func NewSearchTableConfig() SearchTableConfig {
	return SearchTableConfig{
		matchLen:       6,
		tableType:      searchTableTypeNoPrefix,
		maxPopPct:      defaultMaxPopPct,
		maxConflictPct: defaultMaxConflictPct,
	}
}

// WithMatchLen sets the match length (1-8).
// Shorter values use less of the search pattern but are more likely to collide.
// Default is 6.
func (c SearchTableConfig) WithMatchLen(n int) SearchTableConfig {
	c.matchLen = uint8(n)
	return c
}

// WithBytePrefix sets prefix byte values. With 1-8 unique values, table type 2
// is used. With more than 8, it automatically switches to a bitmask (type 3).
func (c SearchTableConfig) WithBytePrefix(prefixes ...byte) SearchTableConfig {
	if len(prefixes) > 8 {
		var mask [32]byte
		for _, p := range prefixes {
			mask[p>>3] |= 1 << (p & 7)
		}
		return c.WithMaskPrefix(mask)
	}
	c.tableType = searchTableTypeBytePrefix
	for i := range c.prefixBytes {
		if i < len(prefixes) {
			c.prefixBytes[i] = prefixes[i]
		} else if len(prefixes) > 0 {
			c.prefixBytes[i] = prefixes[len(prefixes)-1]
		}
	}
	return c
}

// WithMaskPrefix sets a 256-bit bitmask of prefix bytes (table type 3).
func (c SearchTableConfig) WithMaskPrefix(mask [32]byte) SearchTableConfig {
	c.tableType = searchTableTypeMaskPrefix
	c.prefixMask = mask
	return c
}

// WithLongPrefix sets a long prefix (1-256 bytes, table type 4).
func (c SearchTableConfig) WithLongPrefix(prefix []byte) SearchTableConfig {
	c.tableType = searchTableTypeLongPrefix
	c.longPrefix = slices.Clone(prefix)
	return c
}


// WithMaxPopulation sets the max population percentage (0-100).
// Tables with more bits set are skipped entirely.
func (c SearchTableConfig) WithMaxPopulation(pct int) SearchTableConfig {
	c.maxPopPct = pct
	return c
}

// WithMaxConflicts sets the max accumulated conflict percentage (0-100) for reductions.
func (c SearchTableConfig) WithMaxConflicts(pct int) SearchTableConfig {
	c.maxConflictPct = pct
	return c
}

func (c *SearchTableConfig) validate() error {
	if c.matchLen < 1 || c.matchLen > 8 {
		return fmt.Errorf("minlz: search table matchLen must be 1-8, got %d", c.matchLen)
	}
	switch c.tableType {
	case searchTableTypeNoPrefix, searchTableTypeBytePrefix, searchTableTypeMaskPrefix, searchTableTypeLongPrefix:
	default:
		return fmt.Errorf("minlz: unknown search table type %d", c.tableType)
	}
	if c.tableType == searchTableTypeLongPrefix && (len(c.longPrefix) < 1 || len(c.longPrefix) > 256) {
		return fmt.Errorf("minlz: long prefix length must be 1-256, got %d", len(c.longPrefix))
	}
	return nil
}

// maxChunkSize returns the maximum 0x45 chunk size (header + payload) for this config.
func (c *SearchTableConfig) maxChunkSize() int {
	// 4 (chunk header) + 3 (type+matchLen+baseSize) + prefixSize + 1 (reductions) + max table bytes
	// Max table = 2^(baseTableSize-3) bytes (0 reductions).
	return 4 + 3 + c.prefixSize() + 1 + (1 << (c.baseTableSize - 3))
}

func (c *SearchTableConfig) prefixSize() int {
	switch c.tableType {
	case searchTableTypeBytePrefix:
		return 8
	case searchTableTypeMaskPrefix:
		return 32
	case searchTableTypeLongPrefix:
		return 1 + len(c.longPrefix)
	}
	return 0
}

// HashValue returns a table index for the lowest matchLen bytes of val.
// tableSize is the number of output bits (8-23). matchLen must be 1-8.
func hashValue(val uint64, tableSize, matchLen uint8) uint32 {
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
		return uint32(((val << (64 - 40)) * prime5bytes) >> (64 - uint64(tableSize)))
	case 6:
		return uint32(((val << (64 - 48)) * prime6bytes) >> (64 - uint64(tableSize)))
	case 7:
		return uint32(((val << (64 - 56)) * prime7bytes) >> (64 - uint64(tableSize)))
	case 8:
		return uint32((val * prime8bytes) >> (64 - uint64(tableSize)))
	}
	return 0
}

// Per-matchLen hash helpers for branchless inner loops.
func hashValue1(v uint64) uint32           { return uint32(v & 0xff) }
func hashValue2(v uint64, ts uint8) uint32 { return (uint32(v<<16) * prime2bytes) >> (32 - ts) }
func hashValue3(v uint64, ts uint8) uint32 { return (uint32(v<<8) * prime3bytes) >> (32 - ts) }
func hashValue4(v uint64, ts uint8) uint32 { return (uint32(v) * prime4bytes) >> (32 - ts) }
func hashValue5(v uint64, ts uint8) uint32 {
	return uint32(((v << 24) * prime5bytes) >> (64 - uint64(ts)))
}
func hashValue6(v uint64, ts uint8) uint32 {
	return uint32(((v << 16) * prime6bytes) >> (64 - uint64(ts)))
}
func hashValue7(v uint64, ts uint8) uint32 {
	return uint32(((v << 8) * prime7bytes) >> (64 - uint64(ts)))
}
func hashValue8(v uint64, ts uint8) uint32 { return uint32((v * prime8bytes) >> (64 - uint64(ts))) }

// hashValue2Full handles the special case where tableSize >= 16 for matchLen 2.
func hashValue2Full(v uint64) uint32 { return uint32(v & 0xffff) }

// tablePopulation returns the number of set bits and total bits.
func tablePopulation(table []uint64) (setBits, totalBits int) {
	for _, v := range table {
		setBits += bits.OnesCount64(v)
	}
	return setBits, len(table) * 64
}

// reduceTable OR-folds table halves. Stops when accumulated conflicts
// exceed maxConflictPct% of the original set bits, or table reaches min 32 bytes (4 uint64s).
// Returns the reduced table and number of reductions applied.
func reduceTable(table []uint64, origPopcount, maxConflictPct int) ([]uint64, uint8) {
	if origPopcount == 0 {
		// Empty table — reduce to minimum size (all zeros is still valid).
		reductions := uint8(0)
		for len(table)/2 >= 4 {
			table = table[:len(table)/2]
			reductions++
		}
		return table, reductions
	}
	threshold := origPopcount * maxConflictPct / 100
	totalConflicts := 0
	reductions := uint8(0)

	// Minimum table: 4 uint64s = 32 bytes = 256 bits.
	for len(table)/2 >= 4 {
		half := len(table) / 2
		lower := table[:half]
		upper := table[half:]
		upper = upper[:len(lower)]
		conflicts := 0
		for i := range lower {
			conflicts += bits.OnesCount64(lower[i] & upper[i])
		}
		if totalConflicts+conflicts > threshold {
			break
		}
		totalConflicts += conflicts
		for i := range lower {
			lower[i] |= upper[i]
		}
		table = lower
		reductions++
	}
	return table, reductions
}

// tableToBytes converts the internal uint64 table to bytes for wire serialization.
func tableToBytes(table []uint64) []byte {
	b := make([]byte, len(table)*8)
	for i, v := range table {
		binary.LittleEndian.PutUint64(b[i*8:], v)
	}
	return b
}

// marshalSearchInfoChunk produces a complete 0x44 chunk.
func (c *SearchTableConfig) marshalSearchInfoChunk() []byte {
	payloadSize := 3 + c.prefixSize()
	chunk := make([]byte, 4+payloadSize)
	chunk[0] = chunkTypeSearchInfo
	chunk[1] = uint8(payloadSize)
	chunk[2] = uint8(payloadSize >> 8)
	chunk[3] = uint8(payloadSize >> 16)
	chunk[4] = c.tableType
	chunk[5] = c.matchLen
	chunk[6] = c.baseTableSize
	c.marshalPrefix(chunk[7:])
	return chunk
}

// marshalSearchTableChunk produces a complete 0x45 chunk.
func marshalSearchTableChunk(cfg *SearchTableConfig, reductions uint8, table []byte) []byte {
	prefixSize := cfg.prefixSize()
	payloadSize := 3 + prefixSize + 1 + len(table)
	chunk := make([]byte, 4+payloadSize)
	chunk[0] = chunkTypeSearchTable
	chunk[1] = uint8(payloadSize)
	chunk[2] = uint8(payloadSize >> 8)
	chunk[3] = uint8(payloadSize >> 16)
	chunk[4] = cfg.tableType
	chunk[5] = cfg.matchLen
	chunk[6] = cfg.baseTableSize
	off := 7 + cfg.marshalPrefix(chunk[7:])
	chunk[off] = reductions
	copy(chunk[off+1:], table)
	return chunk
}

// marshalSearchTableChunkTo writes a 0x45 chunk into dst, returning bytes written.
// dst must be large enough (use maxChunkSize).
func marshalSearchTableChunkTo(dst []byte, cfg *SearchTableConfig, reductions uint8, table []byte) int {
	prefixSize := cfg.prefixSize()
	payloadSize := 3 + prefixSize + 1 + len(table)
	dst[0] = chunkTypeSearchTable
	dst[1] = uint8(payloadSize)
	dst[2] = uint8(payloadSize >> 8)
	dst[3] = uint8(payloadSize >> 16)
	dst[4] = cfg.tableType
	dst[5] = cfg.matchLen
	dst[6] = cfg.baseTableSize
	off := 7 + cfg.marshalPrefix(dst[7:])
	dst[off] = reductions
	copy(dst[off+1:], table)
	return 4 + payloadSize
}

func (c *SearchTableConfig) marshalPrefix(dst []byte) int {
	switch c.tableType {
	case searchTableTypeBytePrefix:
		copy(dst, c.prefixBytes[:])
		return 8
	case searchTableTypeMaskPrefix:
		copy(dst, c.prefixMask[:])
		return 32
	case searchTableTypeLongPrefix:
		dst[0] = uint8(len(c.longPrefix) - 1)
		copy(dst[1:], c.longPrefix)
		return 1 + len(c.longPrefix)
	}
	return 0
}

// parseSearchInfo parses the payload (after chunk header) of a 0x44 chunk.
func parseSearchInfo(payload []byte) (SearchTableConfig, error) {
	if len(payload) < 3 {
		return SearchTableConfig{}, fmt.Errorf("minlz: search info chunk too short")
	}
	cfg := SearchTableConfig{
		tableType:      payload[0],
		matchLen:       payload[1],
		baseTableSize:  payload[2],
		maxPopPct:      defaultMaxPopPct,
		maxConflictPct: defaultMaxConflictPct,
	}
	payload = payload[3:]
	switch cfg.tableType {
	case searchTableTypeNoPrefix:
	case searchTableTypeBytePrefix:
		if len(payload) < 8 {
			return cfg, fmt.Errorf("minlz: search info byte prefix too short")
		}
		copy(cfg.prefixBytes[:], payload[:8])
	case searchTableTypeMaskPrefix:
		if len(payload) < 32 {
			return cfg, fmt.Errorf("minlz: search info mask prefix too short")
		}
		copy(cfg.prefixMask[:], payload[:32])
	case searchTableTypeLongPrefix:
		if len(payload) < 1 {
			return cfg, fmt.Errorf("minlz: search info long prefix too short")
		}
		pLen := int(payload[0]) + 1
		if len(payload) < 1+pLen {
			return cfg, fmt.Errorf("minlz: search info long prefix data too short")
		}
		cfg.longPrefix = slices.Clone(payload[1 : 1+pLen])
	default:
		return cfg, fmt.Errorf("minlz: unknown search table type %d", cfg.tableType)
	}
	return cfg, nil
}

// parseSearchTable parses the payload (after chunk header) of a 0x45 chunk.
func parseSearchTable(payload []byte) (cfg SearchTableConfig, reductions uint8, table []byte, err error) {
	cfg, err = parseSearchInfo(payload)
	if err != nil {
		return
	}
	off := 3 + cfg.prefixSize()
	if off >= len(payload) {
		err = fmt.Errorf("minlz: search table chunk too short for reductions")
		return
	}
	reductions = payload[off]
	table = payload[off+1:]
	expectedSize := 1 << (cfg.baseTableSize - reductions - 3)
	if len(table) < expectedSize {
		err = fmt.Errorf("minlz: search table data too short: got %d, want %d", len(table), expectedSize)
		return
	}
	table = table[:expectedSize]
	return
}

// readLE64Pad reads up to 8 bytes from b as a little-endian uint64, zero-padding if short.
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
