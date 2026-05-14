package minlz

import (
	"bytes"
	"math/bits"
	"sync"
)

var (
	byteTablePool   sync.Pool
	searchTablePool sync.Pool
)

// buildSearchTable generates a search table for blockData.
// overlap contains up to matchLen-1 bytes from the next block (for boundary patterns).
// dst is an optional reusable buffer; if nil or too small a new one is allocated.
// Returns (table, reductions) or (nil, 0) if the table is too populated.
// Base table size is fixed per c.baseTableSize; reductions vary per block.
// To reduce L3 cache pressure the "packed" can be used with less L3 cache pressure.
func (c *SearchTableConfig) buildSearchTable(blockData, overlap, dst []byte, packed bool) ([]byte, uint8) {
	tableSize := max(32, 1<<(c.baseTableSize-3))
	table := dst
	if cap(table) < tableSize {
		table = make([]byte, tableSize)
	} else {
		table = table[:tableSize]
		clear(table)
	}

	// Index the main block data.
	nPositions := len(blockData)
	switch c.tableType {
	case searchTableTypeNoPrefix:
		if packed {
			buildTableNoPrefix(table, blockData, nPositions, c.baseTableSize, c.matchLen)
			break
		}
		btSize := 1 << c.baseTableSize
		var bt []byte
		if v := byteTablePool.Get(); v != nil {
			bt = v.([]byte)
			if cap(bt) >= btSize {
				bt = bt[:btSize]
			} else {
				bt = make([]byte, btSize)
			}
		} else {
			bt = make([]byte, btSize)
		}
		clear(bt)
		buildTableNoPrefixByte(bt, blockData, nPositions, c.baseTableSize, c.matchLen)
		// Handle overlap tail positions (few entries, use byte table).
		if len(overlap) > 0 {
			ml := int(c.matchLen)
			var tmp [16]byte
			start := max(0, len(blockData)-ml+1)
			for pos := start; pos < len(blockData); pos++ {
				n := copy(tmp[:], blockData[pos:])
				copy(tmp[n:], overlap)
				h := hashValue(readLE64Pad(tmp[:ml]), c.baseTableSize, c.matchLen)
				store8(bt, int(h), 0xFF)
			}
		}
		packBits(table, bt)
		byteTablePool.Put(bt)
		goto packed
	case searchTableTypeBytePrefix:
		var lookup [256]bool
		for _, p := range c.prefixBytes {
			lookup[p] = true
		}
		buildTablePrefixLookup(table, blockData, nPositions, c.baseTableSize, c.matchLen, &lookup)
	case searchTableTypeMaskPrefix:
		var lookup [256]bool
		for i := range 256 {
			if c.prefixMask[i>>3]&(1<<(i&7)) != 0 {
				lookup[i] = true
			}
		}
		buildTablePrefixLookup(table, blockData, nPositions, c.baseTableSize, c.matchLen, &lookup)
	case searchTableTypeLongPrefix:
		buildTablePrefixLong(table, blockData, nPositions, c.baseTableSize, c.matchLen, c.longPrefix)
	}

	// Index the tail positions that span into the overlap.
	if len(overlap) > 0 {
		ml := int(c.matchLen)
		var tmp [16]byte
		start := max(0, len(blockData)-ml+1)
		for pos := start; pos < len(blockData); pos++ {
			// For prefix tables, only index if the preceding byte is a prefix.
			if pos > 0 {
				switch c.tableType {
				case searchTableTypeBytePrefix:
					isPfx := false
					for _, p := range c.prefixBytes {
						if blockData[pos-1] == p {
							isPfx = true
							break
						}
					}
					if !isPfx {
						continue
					}
				case searchTableTypeMaskPrefix:
					b := blockData[pos-1]
					if c.prefixMask[b>>3]&(1<<(b&7)) == 0 {
						continue
					}
				case searchTableTypeLongPrefix:
					pl := len(c.longPrefix)
					if pos < pl || !matchPrefix(blockData[pos-pl:pos], c.longPrefix) {
						continue
					}
				}
			} else if c.tableType != searchTableTypeNoPrefix {
				continue // pos 0 with prefix type: no preceding byte
			}
			n := copy(tmp[:], blockData[pos:])
			copy(tmp[n:], overlap)
			setBit(table, hashValue(readLE64Pad(tmp[:ml]), c.baseTableSize, c.matchLen))
		}
	}

packed:
	setBits, totalBits := tablePopulation(table)
	if totalBits > 0 && setBits*100/totalBits > c.maxPopPct {
		return nil, 0
	}

	table, reductions := reduceTable(table, setBits, c.maxReducedPopPct)
	return table, reductions
}

// shouldPack returns true if the expected L3 cache usage is above 8MB.
// Typically at this point performance starts degrading.
func (s *SearchTableConfig) shouldPack(concurrency int) bool {
	return concurrency*(1<<s.baseTableSize) > 8<<20
}

func setBit(table []byte, h uint32) {
	table[h>>3] |= 1 << (h & 7)
}

// packBitsGeneric converts a byte-per-entry table (0x00 or 0xFF) into a bit-packed table.
// dst must be len(src)/8 bytes.
func packBitsGeneric(dst, src []byte) {
	base := 0
	for i := range dst {
		dst[i] = (load8(src, base+0) & 0x01) |
			(load8(src, base+1) & 0x02) |
			(load8(src, base+2) & 0x04) |
			(load8(src, base+3) & 0x08) |
			(load8(src, base+4) & 0x10) |
			(load8(src, base+5) & 0x20) |
			(load8(src, base+6) & 0x40) |
			(load8(src, base+7) & 0x80)
		base += 8
	}
}

// buildTableNoPrefix indexes all positions. Branchless inner loop.
// nPositions is the number of starting positions to index (len of original block).
// data may be longer (includes overlap for boundary reads).
func buildTableNoPrefix(table []byte, data []byte, nPositions int, tableSize, matchLen uint8) {
	n := nPositions - int(matchLen) + 1
	if n <= 0 {
		return
	}
	safeEnd := max(0, len(data)-7)
	mainEnd := min(n, safeEnd)

	switch matchLen {
	case 1:
		for i := range n {
			setBit(table, hashValue1(uint64(data[i])))
		}
	case 2:
		if tableSize >= 16 {
			for i := range mainEnd {
				setBit(table, hashValue2Full(load64(data, i)))
			}
		} else {
			for i := range mainEnd {
				setBit(table, hashValue2(load64(data, i), tableSize))
			}
		}
	case 3:
		for i := range mainEnd {
			setBit(table, hashValue3(load64(data, i), tableSize))
		}
	case 4:
		i := 0
		for ; i < mainEnd-4; i += 4 {
			v := load64(data, i)
			setBit(table, hashValue4(v, tableSize))
			setBit(table, hashValue4(v>>8, tableSize))
			setBit(table, hashValue4(v>>16, tableSize))
			setBit(table, hashValue4(v>>24, tableSize))
		}
		for ; i < mainEnd; i++ {
			setBit(table, hashValue4(load64(data, i), tableSize))
		}
	case 5:
		i := 0
		for ; i < mainEnd-4; i += 4 {
			v := load64(data, i)
			setBit(table, hashValue5(v, tableSize))
			setBit(table, hashValue5(v>>8, tableSize))
			setBit(table, hashValue5(v>>16, tableSize))
			setBit(table, hashValue5(v>>24, tableSize))
		}
		for ; i < mainEnd; i++ {
			setBit(table, hashValue5(load64(data, i), tableSize))
		}
	case 6:
		i := 0
		for ; i < mainEnd-3; i += 3 {
			v := load64(data, i)
			setBit(table, hashValue6(v, tableSize))
			setBit(table, hashValue6(v>>8, tableSize))
			setBit(table, hashValue6(v>>16, tableSize))
		}
		for ; i < mainEnd; i++ {
			setBit(table, hashValue6(load64(data, i), tableSize))
		}
	case 7:
		i := 0
		for ; i < mainEnd-2; i += 2 {
			v := load64(data, i)
			setBit(table, hashValue7(v, tableSize))
			setBit(table, hashValue7(v>>8, tableSize))
		}
		for ; i < mainEnd; i++ {
			setBit(table, hashValue7(load64(data, i), tableSize))
		}
	case 8:
		for i := range mainEnd {
			setBit(table, hashValue8(load64(data, i), tableSize))
		}
	}

	// Tail: positions where 8-byte read isn't safe.
	for i := mainEnd; i < n; i++ {
		v := readLE64Pad(data[i:])
		setBit(table, hashValue(v, tableSize, matchLen))
	}
}

// buildTableNoPrefixByte indexes all positions into a byte-per-entry table.
// Each entry is set to 0xFF when present. The table must be 1<<tableSize bytes.
func buildTableNoPrefixByte(table []byte, data []byte, nPositions int, tableSize, matchLen uint8) {
	n := nPositions - int(matchLen) + 1
	if n <= 0 {
		return
	}
	safeEnd := max(0, len(data)-7)
	mainEnd := min(n, safeEnd)

	switch matchLen {
	case 1:
		for i := range n {
			store8(table, int(hashValue1(uint64(data[i]))), 0xFF)
		}
	case 2:
		if tableSize >= 16 {
			for i := range mainEnd {
				store8(table, int(hashValue2Full(load64(data, i))), 0xFF)
			}
		} else {
			buildML2Byte(table, data, mainEnd, tableSize)
		}
	case 3:
		buildML3Byte(table, data, mainEnd, tableSize)
	case 4:
		buildML4Byte(table, data, mainEnd, tableSize)
	case 5:
		buildML5Byte(table, data, mainEnd, tableSize)
	case 6:
		buildML6Byte(table, data, mainEnd, tableSize)
	case 7:
		buildML7Byte(table, data, mainEnd, tableSize)
	case 8:
		buildML8Byte(table, data, mainEnd, tableSize)
	}

	// Tail: positions where 8-byte read isn't safe.
	for i := mainEnd; i < n; i++ {
		v := readLE64Pad(data[i:])
		store8(table, int(hashValue(v, tableSize, matchLen)), 0xFF)
	}
}

func buildML2Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), 15)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue2(load64(data, i), tableSize)), 0xFF)
		}
	}
}

func buildML3Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 15)), 0xFF)
		}
	case 16:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 16)), 0xFF)
		}
	case 17:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 17)), 0xFF)
		}
	case 18:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 18)), 0xFF)
		}
	case 19:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 19)), 0xFF)
		}
	case 20:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 20)), 0xFF)
		}
	case 21:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 21)), 0xFF)
		}
	case 22:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 22)), 0xFF)
		}
	case 23:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), 23)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue3(load64(data, i), tableSize)), 0xFF)
		}
	}
}

func buildML4Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 15)), 0xFF)
		}
	case 16:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 16)), 0xFF)
		}
	case 17:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 17)), 0xFF)
		}
	case 18:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 18)), 0xFF)
		}
	case 19:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 19)), 0xFF)
		}
	case 20:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 20)), 0xFF)
		}
	case 21:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 21)), 0xFF)
		}
	case 22:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 22)), 0xFF)
		}
	case 23:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), 23)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue4(load64(data, i), tableSize)), 0xFF)
		}
	}
}

func buildML5Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 15)), 0xFF)
		}
	case 16:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 16)), 0xFF)
		}
	case 17:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 17)), 0xFF)
		}
	case 18:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 18)), 0xFF)
		}
	case 19:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 19)), 0xFF)
		}
	case 20:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 20)), 0xFF)
		}
	case 21:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 21)), 0xFF)
		}
	case 22:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 22)), 0xFF)
		}
	case 23:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), 23)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue5(load64(data, i), tableSize)), 0xFF)
		}
	}
}

func buildML6Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 15)), 0xFF)
		}
	case 16:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 16)), 0xFF)
		}
	case 17:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 17)), 0xFF)
		}
	case 18:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 18)), 0xFF)
		}
	case 19:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 19)), 0xFF)
		}
	case 20:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 20)), 0xFF)
		}
	case 21:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 21)), 0xFF)
		}
	case 22:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 22)), 0xFF)
		}
	case 23:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), 23)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue6(load64(data, i), tableSize)), 0xFF)
		}
	}
}

func buildML7Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 15)), 0xFF)
		}
	case 16:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 16)), 0xFF)
		}
	case 17:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 17)), 0xFF)
		}
	case 18:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 18)), 0xFF)
		}
	case 19:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 19)), 0xFF)
		}
	case 20:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 20)), 0xFF)
		}
	case 21:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 21)), 0xFF)
		}
	case 22:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 22)), 0xFF)
		}
	case 23:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), 23)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue7(load64(data, i), tableSize)), 0xFF)
		}
	}
}

func buildML8Byte(table, data []byte, n int, tableSize uint8) {
	switch tableSize {
	case 8:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 8)), 0xFF)
		}
	case 9:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 9)), 0xFF)
		}
	case 10:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 10)), 0xFF)
		}
	case 11:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 11)), 0xFF)
		}
	case 12:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 12)), 0xFF)
		}
	case 13:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 13)), 0xFF)
		}
	case 14:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 14)), 0xFF)
		}
	case 15:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 15)), 0xFF)
		}
	case 16:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 16)), 0xFF)
		}
	case 17:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 17)), 0xFF)
		}
	case 18:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 18)), 0xFF)
		}
	case 19:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 19)), 0xFF)
		}
	case 20:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 20)), 0xFF)
		}
	case 21:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 21)), 0xFF)
		}
	case 22:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 22)), 0xFF)
		}
	case 23:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), 23)), 0xFF)
		}
	default:
		for i := range n {
			store8(table, int(hashValue8(load64(data, i), tableSize)), 0xFF)
		}
	}
}

// buildTablePrefixLookup indexes positions following a prefix byte.
// lookup[b] == true means b is a prefix byte.
func buildTablePrefixLookup(table []byte, data []byte, nPositions int, tableSize, matchLen uint8, lookup *[256]bool) {
	n := nPositions - int(matchLen) + 1
	if n <= 1 {
		return
	}
	safeEnd := max(0, len(data)-7)
	mainEnd := min(n, safeEnd)

	switch matchLen {
	case 1:
		for i := 1; i < n; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue1(uint64(data[i])))
			}
		}
	case 2:
		if tableSize >= 16 {
			for i := 1; i < mainEnd; i++ {
				if lookup[data[i-1]] {
					setBit(table, hashValue2Full(load64(data, i)))
				}
			}
		} else {
			for i := 1; i < mainEnd; i++ {
				if lookup[data[i-1]] {
					setBit(table, hashValue2(load64(data, i), tableSize))
				}
			}
		}
	case 3:
		for i := 1; i < mainEnd; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue3(load64(data, i), tableSize))
			}
		}
	case 4:
		for i := 1; i < mainEnd; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue4(load64(data, i), tableSize))
			}
		}
	case 5:
		for i := 1; i < mainEnd; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue5(load64(data, i), tableSize))
			}
		}
	case 6:
		for i := 1; i < mainEnd; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue6(load64(data, i), tableSize))
			}
		}
	case 7:
		for i := 1; i < mainEnd; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue7(load64(data, i), tableSize))
			}
		}
	case 8:
		for i := 1; i < mainEnd; i++ {
			if lookup[data[i-1]] {
				setBit(table, hashValue8(load64(data, i), tableSize))
			}
		}
	}

	for i := max(1, mainEnd); i < n; i++ {
		if lookup[data[i-1]] {
			setBit(table, hashValue(readLE64Pad(data[i:]), tableSize, matchLen))
		}
	}
}

// buildTablePrefixLong indexes positions following a long prefix match.
func buildTablePrefixLong(table []byte, data []byte, nPositions int, tableSize, matchLen uint8, prefix []byte) {
	n := nPositions - int(matchLen) + 1
	if n <= 0 {
		return
	}
	pl := len(prefix)
	if pl > n {
		return
	}
	safeEnd := max(0, len(data)-7)
	mainEnd := min(n, safeEnd)

	switch {
	case pl == 1:
		// Single byte prefix - similar to mask but with one value.
		p := prefix[0]
		switch matchLen {
		case 1:
			for i := 1; i < n; i++ {
				if data[i-1] == p {
					setBit(table, hashValue1(uint64(data[i])))
				}
			}
		case 2:
			for i := 1; i < mainEnd; i++ {
				if data[i-1] == p {
					setBit(table, hashValue2(load64(data, i), tableSize))
				}
			}
		case 3:
			for i := 1; i < mainEnd; i++ {
				if data[i-1] == p {
					setBit(table, hashValue3(load64(data, i), tableSize))
				}
			}
		case 4:
			for i := 1; i < mainEnd; i++ {
				if data[i-1] == p {
					setBit(table, hashValue4(load64(data, i), tableSize))
				}
			}
		default:
			for i := 1; i < mainEnd; i++ {
				if data[i-1] == p {
					setBit(table, hashValue(load64(data, i), tableSize, matchLen))
				}
			}
		}
		for i := max(1, mainEnd); i < n; i++ {
			if data[i-1] == p {
				setBit(table, hashValue(readLE64Pad(data[i:]), tableSize, matchLen))
			}
		}
	default:
		// Multi-byte prefix. Use simple comparison.
		for i := pl; i < mainEnd; i++ {
			if matchPrefix(data[i-pl:i], prefix) {
				setBit(table, hashValue(load64(data, i), tableSize, matchLen))
			}
		}
		for i := max(pl, mainEnd); i < n; i++ {
			if matchPrefix(data[i-pl:i], prefix) {
				setBit(table, hashValue(readLE64Pad(data[i:]), tableSize, matchLen))
			}
		}
	}
}

func matchPrefix(data, prefix []byte) bool {
	// Manual comparison for common small sizes.
	switch len(prefix) {
	case 1:
		return data[0] == prefix[0]
	case 2:
		return data[0] == prefix[0] && data[1] == prefix[1]
	case 3:
		return data[0] == prefix[0] && data[1] == prefix[1] && data[2] == prefix[2]
	case 4:
		return load32(data, 0) == load32(prefix, 0)
	default:
		return bytes.Equal(prefix, data[:len(prefix)])
	}
}

func autoTableSize(blockSize int) uint8 {
	s := min(searchTableMaxLog2, max(searchTableMinLog2, uint8(bits.Len(uint(blockSize-1)))))
	return s
}
