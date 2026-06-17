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

	"github.com/klauspost/compress/huff0"
)

// ParsedSearchTable is the result of decoding a 0x45 or 0x46 chunk.
//
// Table holds the reduced packed bitmap (length = 1 << (BaseTableSize -
// Reductions - 3)). Look up a candidate value with the Contains method.
type ParsedSearchTable struct {
	Cfg        SearchConfig
	Reductions uint8
	Table      []byte
}

// ParseSearchInfoChunk parses a 0x44 chunk payload (the bytes after the
// 4-byte chunk header).
func ParseSearchInfoChunk(payload []byte) (SearchConfig, error) {
	cfg, _, err := parseConfig(payload)
	return cfg, err
}

// ParseSearchTableChunk parses a 0x45 chunk payload (the bytes after the
// 4-byte chunk header). The bitmap CRC is verified.
func ParseSearchTableChunk(payload []byte) (ParsedSearchTable, error) {
	cfg, off, err := parseConfig(payload)
	if err != nil {
		return ParsedSearchTable{}, err
	}
	if off >= len(payload) {
		return ParsedSearchTable{}, fmt.Errorf("minlz: 0x45 chunk too short for reductions")
	}
	reductions := payload[off]
	off++
	if cfg.BaseTableSize <= reductions+3 {
		return ParsedSearchTable{}, fmt.Errorf("minlz: invalid reductions %d for baseTableSize %d", reductions, cfg.BaseTableSize)
	}
	expectedSize := 1 << (cfg.BaseTableSize - reductions - 3)
	if off+4+expectedSize > len(payload) {
		return ParsedSearchTable{}, fmt.Errorf("minlz: 0x45 chunk too short: have %d, need %d", len(payload), off+4+expectedSize)
	}
	storedCRC := binary.LittleEndian.Uint32(payload[off:])
	off += 4
	table := payload[off : off+expectedSize]
	if crc32c(table) != storedCRC {
		return ParsedSearchTable{}, fmt.Errorf("minlz: 0x45 CRC mismatch")
	}
	// Copy the table so callers can hold onto it independently of payload.
	out := make([]byte, len(table))
	copy(out, table)
	return ParsedSearchTable{Cfg: cfg, Reductions: reductions, Table: out}, nil
}

// ParseSearchTableCompressedChunk parses a 0x46 chunk payload. All four
// dispositions (huff0 table 0..15, raw 16, RLE 17, sparse 18) are accepted.
// The bitmap CRC is verified after reconstruction.
func ParseSearchTableCompressedChunk(payload []byte) (ParsedSearchTable, error) {
	cfg, off, err := parseConfig(payload)
	if err != nil {
		return ParsedSearchTable{}, err
	}
	if off+1+4+2 > len(payload) {
		return ParsedSearchTable{}, fmt.Errorf("minlz: 0x46 chunk too short for header")
	}
	reductions := payload[off]
	off++
	if cfg.BaseTableSize <= reductions+3 {
		return ParsedSearchTable{}, fmt.Errorf("minlz: invalid reductions %d for baseTableSize %d", reductions, cfg.BaseTableSize)
	}
	storedCRC := binary.LittleEndian.Uint32(payload[off:])
	off += 4
	log2bs := payload[off]
	off++
	tc := payload[off]
	off++

	if log2bs < huff0BlockSizeLog2Min || log2bs > huff0BlockSizeLog2Max {
		return ParsedSearchTable{}, fmt.Errorf("minlz: invalid huff0 block log2 %d", log2bs)
	}
	if tc > 16 {
		return ParsedSearchTable{}, fmt.Errorf("minlz: huff0 table count %d exceeds 16", tc)
	}
	expectedSize := 1 << (cfg.BaseTableSize - reductions - 3)
	blockSize := 1 << log2bs
	if expectedSize%blockSize != 0 {
		return ParsedSearchTable{}, fmt.Errorf("minlz: bitmap size %d not divisible by huff0 block size %d", expectedSize, blockSize)
	}
	nBlocks := expectedSize / blockSize
	if nBlocks > 16 {
		return ParsedSearchTable{}, fmt.Errorf("minlz: huff0 block count %d exceeds 16", nBlocks)
	}

	// Parse huff0 tables.
	scratches := make([]*huff0.Scratch, tc)
	rem := payload[off:]
	for i := range tc {
		sc, after, herr := huff0.ReadTable(rem, nil)
		if herr != nil {
			return ParsedSearchTable{}, fmt.Errorf("minlz: huff0 ReadTable[%d]: %w", i, herr)
		}
		scratches[i] = sc
		rem = after
	}

	// Decode each sub-block.
	bitmap := make([]byte, expectedSize)
	for i := range nBlocks {
		if len(rem) < 1 {
			return ParsedSearchTable{}, fmt.Errorf("minlz: 0x46 sub-block %d disposition truncated", i)
		}
		ti := rem[0]
		rem = rem[1:]
		dst := bitmap[i*blockSize : (i+1)*blockSize]
		switch {
		case ti <= 15:
			if int(ti) >= int(tc) {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d references missing table %d", i, ti)
			}
			n, src, nerr := readUvarintBlob(rem)
			if nerr != nil {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d: %w", i, nerr)
			}
			rem = rem[n:]
			out, derr := scratches[ti].Decompress4X(src, blockSize)
			if derr != nil {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d decompress: %w", i, derr)
			}
			if len(out) != blockSize {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d decompressed length %d != %d", i, len(out), blockSize)
			}
			copy(dst, out)
		case ti == dispRaw:
			if len(rem) < blockSize {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d raw payload truncated", i)
			}
			copy(dst, rem[:blockSize])
			rem = rem[blockSize:]
		case ti == dispRLE:
			if len(rem) < 1 {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d RLE payload truncated", i)
			}
			fill := rem[0]
			rem = rem[1:]
			for k := range dst {
				dst[k] = fill
			}
		case ti == dispSparse:
			n, src, nerr := readUvarintBlob(rem)
			if nerr != nil {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d: %w", i, nerr)
			}
			rem = rem[n:]
			// dst is already zero (bitmap was just allocated). Decoder OR-s in set bits.
			if derr := decodeSparseBitTable(dst, src); derr != nil {
				return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d sparse: %w", i, derr)
			}
		default:
			return ParsedSearchTable{}, fmt.Errorf("minlz: sub-block %d invalid disposition %d", i, ti)
		}
	}
	if len(rem) != 0 {
		return ParsedSearchTable{}, fmt.Errorf("minlz: 0x46 chunk has %d trailing bytes", len(rem))
	}
	if crc32c(bitmap) != storedCRC {
		return ParsedSearchTable{}, fmt.Errorf("minlz: 0x46 CRC mismatch")
	}
	return ParsedSearchTable{Cfg: cfg, Reductions: reductions, Table: bitmap}, nil
}

// ParseRemoteBlockRefChunk parses a 0x47 chunk payload (the bytes after the
// 4-byte chunk header). The first wire offset is absolute, subsequent ones
// are positive deltas (SPEC_SEARCH.md section 2.3). Returned Offset values
// are absolute.
func ParseRemoteBlockRefChunk(payload []byte) ([]RemoteBlockRef, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("minlz: 0x47 empty payload")
	}
	var refs []RemoteBlockRef
	var prev uint64
	first := true
	for len(payload) > 0 {
		wireOff, no := binary.Uvarint(payload)
		if no <= 0 {
			return nil, fmt.Errorf("minlz: 0x47 offset uvarint truncated")
		}
		payload = payload[no:]
		miss, nm := binary.Uvarint(payload)
		if nm <= 0 {
			return nil, fmt.Errorf("minlz: 0x47 max-actual uvarint truncated")
		}
		payload = payload[nm:]
		var abs uint64
		if first {
			abs = wireOff
			first = false
		} else {
			if wireOff == 0 {
				return nil, fmt.Errorf("minlz: 0x47 zero delta breaks ascending order")
			}
			abs = prev + wireOff
		}
		refs = append(refs, RemoteBlockRef{Offset: abs, MaxMinusActualLen: miss})
		prev = abs
	}
	return refs, nil
}

// Contains reports whether the MatchLen-byte prefix of needle could be present
// in the indexed block. Returns false if needle is shorter than MatchLen, or
// when the bit is definitely absent. A true result allows for false positives
// (the table is a Bloom-style summary, not a membership proof).
func (p ParsedSearchTable) Contains(needle []byte) bool {
	if len(needle) < int(p.Cfg.MatchLen) {
		return false
	}
	val := readLE64Pad(needle[:p.Cfg.MatchLen])
	h := HashValue(val, p.Cfg.BaseTableSize, p.Cfg.MatchLen)
	mask := uint32(1)<<(p.Cfg.BaseTableSize-p.Reductions) - 1
	h &= mask
	return p.Table[h>>3]&(1<<(h&7)) != 0
}

// parseConfig parses the leading 3+prefixSize bytes of a 0x44/0x45/0x46 chunk
// payload and returns the parsed config plus the number of bytes consumed.
func parseConfig(payload []byte) (SearchConfig, int, error) {
	if len(payload) < 3 {
		return SearchConfig{}, 0, fmt.Errorf("minlz: search chunk payload too short for config")
	}
	cfg := SearchConfig{
		TableType:     payload[0],
		MatchLen:      payload[1],
		BaseTableSize: payload[2],
	}
	if cfg.MatchLen < 1 || cfg.MatchLen > 8 {
		return cfg, 0, fmt.Errorf("minlz: matchLen %d out of range 1..8", cfg.MatchLen)
	}
	if cfg.BaseTableSize < 8 || cfg.BaseTableSize > 23 {
		return cfg, 0, fmt.Errorf("minlz: baseTableSize %d out of range 8..23", cfg.BaseTableSize)
	}
	off := 3
	switch cfg.TableType {
	case TableTypeNoPrefix:
	case TableTypeBytePrefix:
		if off+8 > len(payload) {
			return cfg, 0, fmt.Errorf("minlz: byte-prefix data truncated")
		}
		cfg.PrefixBytes = append([]byte(nil), payload[off:off+8]...)
		off += 8
	case TableTypeMaskPrefix:
		if off+32 > len(payload) {
			return cfg, 0, fmt.Errorf("minlz: mask-prefix data truncated")
		}
		copy(cfg.PrefixMask[:], payload[off:off+32])
		off += 32
	case TableTypeLongPrefix:
		if off+2 > len(payload) {
			return cfg, 0, fmt.Errorf("minlz: long-prefix header truncated")
		}
		k := int(payload[off]) + 1
		off++
		cfg.Extras = payload[off]
		off++
		if int(cfg.MatchLen)+int(cfg.Extras) > 16 {
			return cfg, 0, fmt.Errorf("minlz: matchLen+extras must be <= 16, got matchLen=%d extras=%d", cfg.MatchLen, cfg.Extras)
		}
		if off+k > len(payload) {
			return cfg, 0, fmt.Errorf("minlz: long-prefix data truncated (need %d)", k)
		}
		cfg.LongPrefix = append([]byte(nil), payload[off:off+k]...)
		off += k
	default:
		return cfg, 0, fmt.Errorf("minlz: unknown table type %d", cfg.TableType)
	}
	return cfg, off, nil
}

// readUvarintBlob reads a uvarint length L from src, then a slice of L bytes.
// Returns the total bytes consumed (length-prefix + payload) and the payload.
func readUvarintBlob(src []byte) (int, []byte, error) {
	n, nl := binary.Uvarint(src)
	if nl <= 0 {
		return 0, nil, fmt.Errorf("uvarint length truncated")
	}
	if n > uint64(len(src)-nl) {
		return 0, nil, fmt.Errorf("uvarint length %d exceeds remaining %d", n, len(src)-nl)
	}
	return nl + int(n), src[nl : nl+int(n)], nil
}

// decodeSparseBitTable populates dst (assumed pre-zeroed) from src using the
// sparse-bit-table format of SPEC_SEARCH.md 2.2.1.
func decodeSparseBitTable(dst, src []byte) error {
	totalBits := len(dst) * 8
	pos := 0
	gap := 0
	for _, b := range src {
		gap += int(b)
		if b == 255 {
			continue
		}
		pos += gap
		if pos >= totalBits {
			return fmt.Errorf("sparse position %d out of range (max %d)", pos, totalBits-1)
		}
		dst[pos>>3] |= 1 << (pos & 7)
		pos++
		gap = 0
	}
	if gap != 0 {
		return fmt.Errorf("sparse trailing gap %d without terminator", gap)
	}
	return nil
}
