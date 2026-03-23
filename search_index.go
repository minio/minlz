package minlz

import (
	"math/bits"
	"unsafe"
)

// buildSearchTable generates a search table for blockData.
// overlap contains up to matchLen-1 bytes from the next block (for boundary patterns).
// Returns (table, reductions) or (nil, 0) if the table is too populated.
// Base table size is fixed per c.baseTableSize; reductions vary per block.
func (c *SearchTableConfig) buildSearchTable(blockData, overlap []byte) ([]byte, uint8) {
	tableU64s := max(4, 1<<(c.baseTableSize-6))
	tableBytes := make([]byte, tableU64s*8)
	table := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(tableBytes))), tableU64s)

	var data []byte
	if len(overlap) > 0 {
		data = make([]byte, len(blockData)+len(overlap))
		copy(data, blockData)
		copy(data[len(blockData):], overlap)
	} else {
		data = blockData
	}

	nPositions := len(blockData)

	switch c.tableType {
	case searchTableTypeNoPrefix:
		buildTableNoPrefix(table, data, nPositions, c.baseTableSize, c.matchLen)
	case searchTableTypeBytePrefix:
		var lookup [256]bool
		for _, p := range c.prefixBytes {
			lookup[p] = true
		}
		buildTablePrefixLookup(table, data, nPositions, c.baseTableSize, c.matchLen, &lookup)
	case searchTableTypeMaskPrefix:
		var lookup [256]bool
		for i := range 256 {
			if c.prefixMask[i>>3]&(1<<(i&7)) != 0 {
				lookup[i] = true
			}
		}
		buildTablePrefixLookup(table, data, nPositions, c.baseTableSize, c.matchLen, &lookup)
	case searchTableTypeLongPrefix:
		buildTablePrefixLong(table, data, nPositions, c.baseTableSize, c.matchLen, c.longPrefix)
	}

	setBits, totalBits := tablePopulation(table)
	if totalBits > 0 && setBits*100/totalBits > c.maxPopPct {
		return nil, 0
	}

	table, reductions := reduceTable(table, setBits, c.maxConflictPct)
	return tableBytes[:len(table)*8], reductions
}

func setBit(table []uint64, h uint32) {
	table[h>>6] |= 1 << (h & 63)
}

// buildTableNoPrefix indexes all positions. Branchless inner loop.
// nPositions is the number of starting positions to index (len of original block).
// data may be longer (includes overlap for boundary reads).
func buildTableNoPrefix(table []uint64, data []byte, nPositions int, tableSize, matchLen uint8) {
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
		for i := range mainEnd {
			setBit(table, hashValue4(load64(data, i), tableSize))
		}
	case 5:
		for i := range mainEnd {
			setBit(table, hashValue5(load64(data, i), tableSize))
		}
	case 6:
		for i := range mainEnd {
			setBit(table, hashValue6(load64(data, i), tableSize))
		}
	case 7:
		for i := range mainEnd {
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

// buildTablePrefixLookup indexes positions following a prefix byte.
// lookup[b] == true means b is a prefix byte.
func buildTablePrefixLookup(table []uint64, data []byte, nPositions int, tableSize, matchLen uint8, lookup *[256]bool) {
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
func buildTablePrefixLong(table []uint64, data []byte, nPositions int, tableSize, matchLen uint8, prefix []byte) {
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
		for i := range prefix {
			if data[i] != prefix[i] {
				return false
			}
		}
		return true
	}
}

func autoTableSize(blockSize int) uint8 {
	s := min(searchTableMaxLog2, max(searchTableMinLog2, uint8(bits.Len(uint(blockSize-1)))))
	return s
}
