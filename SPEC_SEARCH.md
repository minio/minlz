# 1 MinLZ Block Search Specification

This extends the base MinLZ spec in [SPEC.md](SPEC.md).

This specification contains an optional search feature for MinLZ streams.

Before each block an optional search information chunk is written.
Conceptually, it is like a bloom table for the block that allows quickly 
checking if a pattern is present in the block.

This can be used to determine if a block may, or definitely does *not* contain a specific pattern.
With this information, blocks can be skipped if searching for specific patterns.

## 2 New Chunks

### 2.0 Search Table Information (chunk type 0x44, skippable)

This chunk is purely informational and should follow the stream identifier
and be before the first block of the stream or its block search table (see below).

The payload of this chunk contains the following information:

| Length | Description             |
|--------|-------------------------|
| 1      | Table Type              |
| 1      | Search pattern length   |
| 1      | Base Table Size in log2 |
| 0/8/32 | Prefix values           |

The Table Type must be `1` if no prefix is present and `2/3` if prefix values are present.

The hash table size is the number of entries in the hash table, which is 2^tableSize.

* The smallest tables are 256 entries (size 8).
* The largest tables are the same as the maximum block size of the stream - ie 8,388,608 entries (size 23).

The search length must be at least `1` and at most `8`.

See section "3.2 Prefix Values" for how to determine the prefix values.

### 2.1 Block Search Table (chunk type 0x45, skippable)

The Block Search Table is an optional chunk that will come before a block that represents the contents.

| Length | Description                   |
|--------|-------------------------------|
| 1      | Table Type                    |
| 1      | Search pattern length         |
| 1      | Base Table Size in log2       |
| 0/8/32 | Prefix values                 |
| 1      | Reductions from Base Table    |
| n      | Table entries. n = 2^(BT-R-3) |

Search pattern length, base table size and prefix values *should* match the values given in chunk type 0x44.
If no chunk type 0x44 has been seen or the values are different, the decoder *may* choose not to use the table.

`n` must be at least 32 bytes – meaning 256 entries.

The table entries are bits, where the search pattern resolves to 'x' 
can be resolved by looking up `table[x>>3] & (1<<(x&7))`.

Note that encoders can decide to omit the Search Table for any block if it is deemed not worth the space, 
for example, if there are too many collisions for the table to provide any benefit.

The block table *must* include patterns that start in the upcoming block and continue into the next block 
- including if only the prefix is in the block.

It is allowed to have multiple block tables before a block.
All tables are assumed to be for the same block.
It is purely up to the decoder to decide which table(s) to use.

If creating an index - as per spec [section 4.12](SPEC.md#412-index-chunk-type-0x40--optional),
it is recommended to include this chunk as the indexed offset.

## 3 Table Definition

Each entry will only contain a single bit to indicate if a pattern matching the hash value is within the current block.

### 3.0 Table Types

There are 3 table types. They all use the same hashing algorithm.

The only difference is how many prefix values, if any, were used for the table.

The decoder must ignore unknown table types.

### 3.1 Table Hashing

The tables are generated using unsigned 32/64-bit multiplications. 

Input values are all values read as little-endian 4 or 8-byte integers.

If there are prefix limitations, only entries following one of the prefix values will be added to the table.

```go
const (
	prime2bytes = 40503
	prime3bytes = 506832829
	prime4bytes = 2654435761
	prime5bytes = 889523592379
	prime6bytes = 227718039650203
	prime7bytes = 58295818150454627
	prime8bytes = 0xcf1bbcdcb7a56463
)

// HashValue returns a table index of the lowest matchLen bytes,
// with tableSize output bits.
// matchLen must be >= 1 and <= 8.
// tableSize should always be 8 - 23.
func HashValue(val uint64, tableSize, matchLen uint8) uint32 {
	switch matchLen {
	case 1:
		return uint32(val&0xff)
	case 2:
		if tableSize >= 16 {
			return uint32(val&0xffff)
		}
		return (uint32(val<<16) * prime2bytes) >> (32 - tableSize)
	case 3:
		return (uint32(val<<8) * prime3bytes) >> (32 - tableSize)
	case 4:
		return (uint32(val) * prime4bytes) >> (32 - tableSize)
	case 5:
		return uint32(((val << (64 - 40)) * prime5bytes) >> (64 - tableSize))
	case 6:
		return uint32(((val << (64 - 48)) * prime6bytes) >> (64 - tableSize))
	case 7:
		return uint32(((val << (64 - 56)) * prime7bytes) >> (64 - tableSize))
	case 8:
		return uint32((val * prime8bytes) >> (64 - tableSize))
	default:
	}
}
```

### 3.2 Reductions

Each table can be reduced in size by combining adjacent entries if the table is deemed sparse enough.

The reductions are stored in the table header for each.

For example, if the base table size is 20 bits, it can be reduced to half the
size by combining entries bit-wise between the lower and upper half of the table.

The tables are combined like this:

```go
func reduce(b []byte) []byte {
	lower := b[:len(b)/2]
	upper := b[len(b)/2:]
	for i := range lower {
		lower[i] |= upper[i]
	}
	return lower
}
```

In that case the reduction would be `1`. The reduction is purely decided by the compressor.

When searching, this means that for each reduction, the search pattern index should have 
the highest active bit discarded to get the correct index.

### 3.3 Prefix Values

Prefixes allow only selectively indexing values in a block.
If there is no prefix, all values will be indexed.

When a prefix value is set, only entries following one of the prefix bytes will be added to the table,
but the prefix value itself will not be added, unless it follows itself or another prefix value.

For example, setting a prefix value to `=` means that only values following a `=` will be added to the table.
This can significantly reduce the size and improve the quality of the table.

The table type will indicate how many prefix values are present.

| Table Type | Prefix bytes | Prefix values | Description                |
|------------|--------------|---------------|----------------------------|
| 1          | 0            | 0             | No prefix values           |
| 2          | 8            | 1-8           | 1-8 prefix values          |
| 3          | 32           | 0-256         | Bit mask for prefix values |

#### 3.3.1 Table Type 2

With table type 2 up to 8 individual prefix values can be defined.

If less than 8 values are needed, the rest can be filled with duplicates of previous ones.

#### 3.3.2 Table Type 3

Each bit indicates if a byte value at that position is a prefix of the search pattern.