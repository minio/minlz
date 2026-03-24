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
and be before the first data block of the stream and before any block search table (type 0x45, see below).

The payload of this chunk contains the following information:

| Length | Description             |
|--------|-------------------------|
| 1      | Table Type              |
| 1      | Search pattern length   |
| 1      | Base Table Size in log2 |
| 0/8/32 | Prefix values           |

The Table Type must be `1` if no prefix is present and `2/3/4` if prefix values are present.

The hash table size is the number of entries in the hash table, which is 2^tableSize bits.

* The smallest table is 256 entries (size 8, 32 bytes).
* The largest table is the same as the maximum block size of the stream - ie 8,388,608 entries (size 23, 1MiB).

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
| 4          | 1+n          | 1 (256 B max) | Long Prefix                |


#### 3.3.1 Prefix Indexing

When indexing overlap positions near block boundaries, the prefix context must still
be satisfied. An overlap position is only indexed if its preceding byte in the block
data is a valid prefix value. This means:

- A prefix byte at the end of block N followed by a value starting in block N+1
  WILL be indexed in block N's table (the prefix is in block N's data).
- A value at the start of block N where the prefix byte is the last byte of block N-1
  will NOT be indexed in block N's table.

An empty prefix table (all zero bits) indicates that no prefix bytes exist
in the block, allowing the searcher to skip it entirely.

#### 3.3.2 Table Type 2

With table type 2 up to 8 individual prefix values can be defined.

If less than 8 values are needed, the rest can be filled with duplicates of previous ones.

#### 3.3.3 Table Type 3

Each bit indicates if a byte value at that position is a prefix of the search pattern.

#### 3.3.4 Table Type 4

The first byte defines the prefix length. One must be added to the length after being read.

The prefix (1 to 256 bytes) is stored after the length indication.

A type 4 table with prefix `"id":` will therefore only contain entries following that prefix.


## Appendix A - Using Search Tables

This appendix describes how a searcher can apply the different table types
to determine if a block may contain a given byte pattern.

This is a guideline for implementers to get the most of search tables, not a specification.

### A.1 General Lookup

Given a search table with `baseTableSize` and `reductions`, the effective lookup is:

```
mask = (1 << (baseTableSize - reductions)) - 1
h = HashValue(window, baseTableSize, matchLen) & mask
present = table[h >> 3] & (1 << (h & 7)) != 0
```

If `present` is false for any checked window, the block definitely does not contain the
pattern and can be skipped. If all checked windows are present, the block may contain
the pattern and must be decoded to verify.

### A.2 Type 1 - No Prefix

Every `matchLen`-byte window of the search pattern is checked.
All must be present for a possible match.

For pattern `P` of length `L` with matchLen  `M`:

```
for i = 0 to L - M:
    if not present(P[i : i+M]):
        skip block
```

This is the most powerful mode for arbitrary searches. Longer patterns produce
more window checks, giving exponentially better filtering.

### A.3 Types 2 and 3 - Byte Prefix / Mask Prefix

The table only contains entries for positions in the data that immediately follow
a prefix byte. When searching, the pattern is scanned for any position where a
prefix byte appears. The `matchLen` bytes following that position are checked.

For pattern `P` of length `L`, prefix set `S`, and matchLen `M`:

```
checked = 0
for i = 1 to L - M:
    if P[i-1] is in S:
        if not present(P[i : i+M]):
            skip block
        checked++
if checked == 0:
    cannot use table (fall back to full decode)
```

This means the pattern does **not** need to start with a prefix byte.
Any prefix byte found inside the pattern produces a checkable window.

For example, with prefix bytes `"` and `:`, searching for `stamp":"1679909263`
finds `"` at position 5 and `:` at position 6. The windows `:"1679`,  `"16799`
as well as `167990` are all checked. If either is absent, the block is skipped.

If the pattern contains no prefix bytes at all (e.g. `stamp`), the table cannot
help and the searcher must fall back to decoding the block.

### A.4 Type 4 - Long Prefix

The table contains entries for positions following a multi-byte prefix sequence.
The searcher scans the pattern for any occurrence of the prefix substring and
checks the `matchLen` bytes that follow it.

For pattern `P` of length `L`, prefix `pfx` of length `K`, and matchLen = `M`:

```
checked = 0
for i = 0 to L - K - M:
    if P[i : i+K] == pfx:
        if not present(P[i+K : i+K+M]):
            skip block
        checked++
if checked == 0:
    cannot use table (fall back to full decode)
```

For example, with prefix `":"` and matchLen 4, searching for `stamp":"1679909263`
finds `":"` at position 5. The window `1679` is checked.

### A.5 Fallback Behavior

When the search table cannot be applied to a given pattern (pattern too short,
no prefix bytes found inside), a searcher has two options:

1. **Fall back**: Decode the block and search it directly. This is the safe default.
2. **Bail**: Return an error indicating search tables are unusable for this query.
   This is useful when the caller only wants table-accelerated searches.

## Appendix B - Handling Block Overlaps

### B.1 Encoder: Overlap Indexing

When generating a block's search table, the encoder must hash positions near the end
of the block where the `matchLen`-byte window extends into the next block. The spec
requires this (section 3: "The block table must include patterns that start in the
upcoming block and continue into the next block").

For block N of size S with matchLen M, the positions that need overlap are
`S-M+1` through `S-1`. Each of these positions reads bytes from both block N
and block N+1. The encoder should provide the first `M-1` bytes of block N+1
as overlap when building block N's table.

For contiguous buffers (`EncodeBuffer`), the overlap is directly available.
For streaming writes, the overlap comes from the remaining data in the write buffer.
The last block in a stream has no overlap — its tail positions cannot be indexed.

**Prefix tables and overlap:** For prefix-filtered tables (types 2, 3, 4), the overlap
tail positions must still respect the prefix context. An overlap position is only indexed
if its preceding byte in the block data is a valid prefix byte. This ensures that prefix
tables remain accurate — an empty prefix table correctly indicates "no prefix bytes exist
in this block" and the block can be skipped for any prefix-aware search.

Implementation note: the overlap positions are few (at most `M-1 = 7` for matchLen 8)
and can be handled with a small stack buffer rather than concatenating the full block
with the overlap bytes.

### B.2 Searcher: Boundary Pattern Matching

#### B.2.1 Type 1 (No Prefix)

When a search pattern is longer than `matchLen`, the pattern produces multiple
hash windows. For a pattern that straddles a block boundary, some windows fall
in block N and others in block N+1. A naive check that requires ALL windows to
be present in a single block's table will incorrectly skip blocks that contain
the start of a boundary-straddling pattern.

The correct approach: a block can be skipped only if the **first** matchLen-window
of the pattern is absent from the table. If the first window IS present, the pattern
could start near the end of the block with later windows extending into the next block.

```
firstWindow = hash(pattern[0 : matchLen])
if not present(firstWindow):
    skip block  -- pattern cannot start in this block
else:
    decode block  -- pattern might start here
```

When all windows are checked and all are present, the block definitely might contain
the full pattern. When a later window is absent but the first is present, the block
might contain a boundary-straddling occurrence.

This means longer patterns provide less filtering power for boundary matches
(only the first window is checked rather than all windows). For patterns that
fit entirely within a block, all windows are still checked.

#### B.2.2 Prefix Tables (Types 2, 3, 4)

For prefix-filtered tables, the searcher scans the pattern for internal prefix bytes
and checks the windows that follow them (see Appendix A). All checked windows must
be present for a possible match. If any is absent, the block is skipped.

Unlike type 1, there is no "first window" boundary fallback for prefix tables.
The overlap tail in the encoder respects prefix context, so boundary positions are
only indexed when the preceding byte in the block data is a valid prefix byte. This
means the table accurately reflects what the block contains after prefix bytes.

Consequence: with prefix tables, the searcher only guarantees finding patterns at
data positions where the prefix context is satisfied. A pattern that straddles a block
boundary where the prefix byte is NOT in the pattern itself may not be found via the
table. In such cases, the searcher falls back to full decode (when `canUse=false`
because no prefix bytes were found in the pattern).

### B.3 Searcher: Previous Block Access

When a block is decoded, the searcher should retain its data so the next decoded
block can check the boundary region. If a block is skipped, the previous block
reference should be cleared.

For each decoded block, the boundary check examines:
- The last `len(pattern)-1` bytes of the previous block
- The first `len(pattern)-1` bytes of the current block

If the concatenation of these regions contains the pattern, it is a boundary match.
This requires the previous block to have been decoded (not skipped). For type 1 tables,
the overlap indexing in B.1 ensures that if a pattern starts in block N, block N's table
has the first window set, so block N is decoded and available as PrevBlock for block N+1.
For prefix tables, boundary detection depends on the prefix context being present in both
the block data and the search pattern.
