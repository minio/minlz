package minlz

import (
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	searchTableTypeNoPrefix   = 1
	searchTableTypeBytePrefix = 2
	searchTableTypeMaskPrefix = 3
	searchTableTypeLongPrefix = 4

	searchTableMinLog2 = 8  // 256 entries = 32 bytes
	searchTableMaxLog2 = 23 // matches maxBlockLog

	defaultMaxPopPct        = 70
	defaultMaxReducedPopPct = 25
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
	matchLen            uint8
	tableType           uint8
	baseTableSize       uint8 // log2, computed at writer init from block size
	prefixBytes         [8]byte
	prefixMask          [32]byte
	longPrefix          []byte
	maxPopPct           int
	maxReducedPopPct    int
	maxReducedPopPctSet bool            // true once WithMaxReducedPopulation has been called
	compression         *compressedOpts // nil = emit 0x45 only
}

// String returns a one-line, human-readable summary of the search table
// configuration. Useful for logging the contents of an 0x44 chunk.
func (c SearchTableConfig) String() string {
	var prefix string
	switch c.tableType {
	case searchTableTypeNoPrefix:
		prefix = "no-prefix"
	case searchTableTypeBytePrefix:
		// Compact the prefixBytes array — duplicates are encoder-internal padding.
		seen := make(map[byte]struct{}, 8)
		var unique []byte
		for _, b := range c.prefixBytes {
			if _, ok := seen[b]; ok {
				continue
			}
			seen[b] = struct{}{}
			unique = append(unique, b)
		}
		prefix = fmt.Sprintf("byte-prefix=%q", string(unique))
	case searchTableTypeMaskPrefix:
		n := 0
		for i := range 256 {
			if c.prefixMask[i>>3]&(1<<(i&7)) != 0 {
				n++
			}
		}
		prefix = fmt.Sprintf("mask-prefix (%d bytes)", n)
	case searchTableTypeLongPrefix:
		prefix = fmt.Sprintf("long-prefix=%q", string(c.longPrefix))
	default:
		prefix = fmt.Sprintf("type=%d", c.tableType)
	}
	return fmt.Sprintf("matchLen=%d baseTableSize=%d (%d entries) %s",
		c.matchLen, c.baseTableSize, 1<<c.baseTableSize, prefix)
}

// NewSearchTableConfig creates a search table config.
// Defaults: matchLen=6, no prefix (type 1), auto table size, 70% max
// population, 25% max reduced population (auto-tightened to 10% when a prefix
// is configured), compression on.
func NewSearchTableConfig() SearchTableConfig {
	return SearchTableConfig{
		matchLen:         6,
		tableType:        searchTableTypeNoPrefix,
		maxPopPct:        defaultMaxPopPct,
		maxReducedPopPct: defaultMaxReducedPopPct,
		compression: &compressedOpts{
			enabled:         true,
			skipPctTimes100: cstDefaultSkipPctTimes100,
		},
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

// WithMaxReducedPopulation sets the max population percentage (0-100) for the
// reduced table. Reductions stop before exceeding this threshold.
//
// Default is 25%, automatically tightened to 10% when a prefix is configured
// (byte/mask/long). Calling this method disables that auto-tightening — the
// supplied value is used regardless of prefix.
func (c SearchTableConfig) WithMaxReducedPopulation(pct int) SearchTableConfig {
	c.maxReducedPopPct = pct
	c.maxReducedPopPctSet = true
	return c
}

// WithCompression enables huff0 compression of per-block search tables and
// optionally tunes it. Compression is on by default — call this only to pass
// non-default options.
//
// Search indexes that can only be marginally compressed are stored uncompressed.
//
// With no options, defaults are: 10.0% popcount band, no stats hook,
// non-forced (emit compressed only when smaller).
func (c SearchTableConfig) WithCompression(opts ...CompressedSearchOption) SearchTableConfig {
	co := &compressedOpts{
		enabled:         true,
		skipPctTimes100: cstDefaultSkipPctTimes100,
	}
	for _, o := range opts {
		o(co)
	}
	c.compression = co
	return c
}

// WithoutCompression disables the per-block search-table compression that is
// on by default. The stream will emit only 0x45 chunks; readers that don't
// implement 0x46 can read it.
func (c SearchTableConfig) WithoutCompression() SearchTableConfig {
	c.compression = nil
	return c
}

// resolveDefaults applies context-dependent defaults that were not set
// explicitly by the caller. Currently: tightens maxReducedPopPct to 10 when a
// prefix is configured and WithMaxReducedPopulation was not called.
// Must be called once the tableType is final.
func (c *SearchTableConfig) resolveDefaults() {
	if !c.maxReducedPopPctSet && c.tableType != searchTableTypeNoPrefix {
		c.maxReducedPopPct = 10
	}
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

// searchTablePayloadSize returns the 0x45 payload size for the given table length.
func (c *SearchTableConfig) searchTablePayloadSize(tableLen int) int {
	// 3 (type+matchLen+baseSize) + prefixSize + 1 (reductions) + 4 (crc) + table
	return 3 + c.prefixSize() + 1 + 4 + tableLen
}

// maxChunkSize returns the maximum 0x45 chunk size (header + payload) for this config.
// Max table = 2^(baseTableSize-3) bytes (0 reductions).
func (c *SearchTableConfig) maxChunkSize() int {
	return 4 + c.searchTablePayloadSize(1<<(c.baseTableSize-3))
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

func (c *SearchTableConfig) appendPrefix(dst []byte) []byte {
	switch c.tableType {
	case searchTableTypeBytePrefix:
		return append(dst, c.prefixBytes[:]...)
	case searchTableTypeMaskPrefix:
		return append(dst, c.prefixMask[:]...)
	case searchTableTypeLongPrefix:
		dst = append(dst, uint8(len(c.longPrefix)-1))
		return append(dst, c.longPrefix...)
	}
	return dst
}

func (c *SearchTableConfig) appendConfig(dst []byte) []byte {
	dst = append(dst, c.tableType, c.matchLen, c.baseTableSize)
	return c.appendPrefix(dst)
}

func appendChunkHeader(dst []byte, chunkType byte, payloadSize int) []byte {
	return append(dst, chunkType, uint8(payloadSize), uint8(payloadSize>>8), uint8(payloadSize>>16))
}

// marshalSearchInfoChunk produces a complete 0x44 chunk.
func (c *SearchTableConfig) marshalSearchInfoChunk() []byte {
	dst := appendChunkHeader(nil, chunkTypeSearchInfo, 3+c.prefixSize())
	return c.appendConfig(dst)
}

// appendSearchTableChunk appends a complete 0x45 chunk to dst.
func appendSearchTableChunk(dst []byte, cfg *SearchTableConfig, reductions uint8, table []byte) []byte {
	dst = appendSearchTableHeader(dst, cfg, reductions, table)
	return append(dst, table...)
}

// appendSearchTableHeader appends everything in the 0x45 chunk before the table bytes:
// chunk header, config, reductions, and CRC of table.
func appendSearchTableHeader(dst []byte, cfg *SearchTableConfig, reductions uint8, table []byte) []byte {
	dst = appendChunkHeader(dst, chunkTypeSearchTable, cfg.searchTablePayloadSize(len(table)))
	dst = cfg.appendConfig(dst)
	dst = append(dst, reductions)
	return binary.LittleEndian.AppendUint32(dst, crc(table))
}

// parseSearchInfo parses the payload (after chunk header) of a 0x44 chunk.
func parseSearchInfo(payload []byte) (SearchTableConfig, error) {
	if len(payload) < 3 {
		return SearchTableConfig{}, fmt.Errorf("minlz: search info chunk too short")
	}
	cfg := SearchTableConfig{
		tableType:        payload[0],
		matchLen:         payload[1],
		baseTableSize:    payload[2],
		maxPopPct:        defaultMaxPopPct,
		maxReducedPopPct: defaultMaxReducedPopPct,
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
func parseSearchTable(payload []byte, ignoreCRC bool) (cfg SearchTableConfig, reductions uint8, table []byte, err error) {
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
	if len(table) < expectedSize+4 {
		err = fmt.Errorf("minlz: search table data too short: got %d, want %d", len(table), expectedSize+4)
		return
	}
	// Read stored CRC
	crc32 := binary.LittleEndian.Uint32(table)
	table = table[4:]
	table = table[:expectedSize]
	if !ignoreCRC && crc32 != crc(table) {
		err = fmt.Errorf("minlz: search table CRC mismatch")
	}
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
