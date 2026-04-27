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

See section "3.3 Prefix Values" for how to determine the prefix values.

### 2.1 Block Search Table (chunk type 0x45, skippable)

The Block Search Table is an optional chunk that will come before a block that represents the contents.

| Length     | Description                   |
|------------|-------------------------------|
| 1          | Table Type                    |
| 1          | Search pattern length         |
| 1          | Base Table Size in log2       |
| 0/8/32/1+n | Prefix values                 |
| 1          | Reductions from Base Table    |
| n          | Table entries. n = 2^(BT-R-3) |

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

There are 4 table types. They all use the same hashing algorithm.

The only difference is how many prefix values, if any, were used for the table.

The decoder must ignore unknown table types.

### 3.1 Table Hashing

The tables are generated using unsigned 64-bit multiplications, using the upper bits of the result. 

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
	lower := b[:len(b)/2] // lower half
	upper := b[len(b)/2:] // upper half
	for i := range lower {
		lower[i] |= upper[i]
	}
	return lower
}
```

In that case the reduction would be `1`. The reduction is purely decided by the compressor.

When searching, this means that for each reduction, the search pattern index should have 
the highest active bit discarded to get the correct index.

The effective index is `HashValue(...) & ((1 << (baseTableSize - reductions)) - 1)`.

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
  will NOT be indexed in block N's table, but only in block N-1's table.

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

See Appendix B.2.1 for boundary handling when a later window is absent but the first window is present.

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

See Appendix B.2.2 for boundary handling when prefix windows are absent but the raw first window is present.

For example, with prefix bytes `"` and `:` and matchLen 6, searching for `stamp":"1679909263`
finds `"` at positions 5 and 7, and `:` at position 6. The windows `:"1679`, `"16799`,
and `167990` are all checked. If any is absent, the block is skipped.

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
and checks the windows that follow them (see Appendix A).

If all checked prefix windows are present, the block might contain the pattern.
If some are present and some absent, the block might contain a boundary match.
If ALL checked prefix windows are absent, the block can still contain the pattern
at a boundary position where the prefix byte is in the block data but not in the
search pattern's context. To handle this, the searcher should also check the first
`matchLen` bytes of the pattern as a raw (non-prefix) lookup:

```
// After prefix-context scan finds all windows absent:
firstWindow = hash(pattern[0 : matchLen])
if present(firstWindow):
    decode block  -- pattern could start at overlap boundary
else:
    skip block
```

This works because the overlap tail in the encoder indexes boundary positions
where the preceding byte in the block data is a valid prefix byte. The hash of
the first `matchLen` bytes of the pattern matches exactly what the overlap tail
hashed for that boundary position.

If no prefix bytes appear in the pattern at all (`canUse=false`), the searcher
falls back to full decode — all blocks are decoded and searched.

### B.3 Searcher: Previous Block Access

When a block is decoded, the searcher should retain its data so the next decoded
block can check the boundary region. If a block is skipped, the previous block
reference should be cleared.

For each decoded block, the boundary check examines:
- The last `len(pattern)-1` bytes of the previous block
- The first `len(pattern)-1` bytes of the current block

If the concatenation of these regions contains the pattern, it is a boundary match.
This requires the previous block to have been decoded (not skipped). The overlap
indexing in B.1 ensures that if a pattern starts in block N, block N's table has
the first window set (for type 1) or the raw first window set (for prefix types,
via the overlap tail), so block N is decoded and available as PrevBlock for block N+1.

A searcher should not skip a block if the previous block was decoded and a
boundary match is possible. A boundary match is possible only when some suffix
of the previous block's tail (`last len(pattern)-1 bytes`) is a prefix of the
search pattern. If no such overlap exists, the block can safely be skipped and
the previous block reference cleared.

When a block is decoded solely for a boundary check (the table indicates no
match within the block), the previous block reference should be cleared
afterward. This prevents a cascade where every decoded block forces the next
to decode as well.

### B.4 Deferred Block Decode

The boundary handling in B.2 is conservative: when the first window is present
but later windows are absent, the block is decoded because the pattern *might*
start near the block end. In practice, the first window is often a false positive
in the table and no match exists. The deferred decode optimization avoids this
unnecessary decode.

#### B.4.1 Principle

When a block N has the first window present but later windows W₁, W₂, ..., Wₖ
absent, these absent windows represent the continuation of the pattern into
block N+1. If block N+1's search table also does not contain ALL of
W₁, W₂, ..., Wₖ, then the boundary match is impossible and block N can be
skipped.

The hash values for the absent windows are computed at full `baseTableSize`
resolution (before reduction masking). When checking against block N+1's table,
the per-block reduction mask for block N+1 is applied:

```
mask_N1 = (1 << (baseTableSize - reductions_N1)) - 1
for each absent hash h:
    h_masked = h & mask_N1
    if table_N1[h_masked >> 3] & (1 << (h_masked & 7)) == 0:
        skip block N  -- continuation not in block N+1
```

All absent windows must be present in block N+1's table for the boundary
match to remain possible. If even one is absent, the match cannot exist.

#### B.4.2 Flow

1. Block N's table says "might match" due to the boundary case.
2. Read block N's compressed data into a buffer but do NOT decompress.
3. Record the absent window hashes.
4. When block N+1's search table (0x45 chunk) arrives, check the absent
   hashes against it.
5. If all absent hashes are present in block N+1 → decompress and search
   block N (the match might be real).
6. If any absent hash is missing in block N+1 → skip block N (the boundary
   match is impossible).

The savings come from avoiding decompression of false-positive blocks. The
compressed data must still be read (or seeked past) to advance the stream.

#### B.4.3 Prefix Tables: Safe Deferral Conditions

For prefix tables (types 2, 3, 4), not all absent windows can be safely
deferred. The issue: block N+1's table builder cannot see prefix bytes that
are in block N (section 3.3.1). For a boundary match, continuation windows
whose prefix bytes fall within block N will NOT be indexed in block N+1's
table, producing false negatives if checked.

Two conditions must both be met for safe deferral:

**Condition 1 — Position threshold.** Let `iFirst` be the pattern position
of the first prefix-context window. For a boundary match via the overlap
region, at most `matchLen - 1 + iFirst` pattern bytes can be in block N.
A continuation window at pattern position `j` has its prefix byte at
`pattern[j-1]`, which is in block N+1 only when `j >= matchLen + iFirst`.
Only absent windows above this threshold may be deferred.

For type 4 (long prefix) with prefix length `K`, the threshold is
`matchLen + K + iFirst`.

**Condition 2 — Near window confirmation.** The position threshold is only
valid when the match is within the overlap range (K ≤ matchLen - 1 + iFirst).
If the first prefix window's hash was set by a normal (non-overlap) position
deeper in the block, K can exceed the overlap range and the threshold is
insufficient.

To confirm the overlap range: check whether any prefix window between
`iFirst` and the threshold ("near" windows) is absent from block N's table.
If a near window is absent, K cannot exceed the overlap range — otherwise
the near window would be at a normal position within block N and would be
present. If ALL near windows are present, K might exceed the overlap range
and deferral must be skipped (decode conservatively).

**Raw fallback.** When the first prefix window is absent but the raw hash
is present, K ≤ iFirst (the first prefix window would be within block N for
larger K and would be present). The near-window condition is inherently
satisfied (the first prefix window itself is absent). The position threshold
still applies.

If no windows meet the threshold, or the near-window condition is not met,
deferral is not possible and the block must be decoded.

#### B.4.4 Limitations

- Deferral requires the next block to have a search table. If the next block
  has no table (missing 0x45 chunk), or the stream ends, the deferred block
  must be decoded conservatively.
- Only one block can be deferred at a time. If block N is deferred and block
  N+1 would also be deferred, block N must be resolved first (decoded).
- The optimization provides no benefit when the pattern is shorter than
  `2 × matchLen` (only one window, nothing to defer).

### B.5 Lazy Previous Block Access

When a block is skipped, the searcher may retain its compressed data so that
it can be decompressed on demand later. This is useful for providing context
around matches — for example, extracting the full line containing a match
when the line starts in a previous (skipped) block.

The `SearchResult.PrevBlock()` method returns the previous block's data:
- If the previous block was decoded normally, returns the decoded data directly.
- If the previous block was skipped (or deferred-then-skipped), decompresses
  the buffered compressed data on first access and caches the result.
- Returns nil if no previous block exists (first block or stream start).

When a lazy previous block is available, `Offset` and `BlockStart` in the
search result are set as if the previous block were present:

```
Offset    = prevBlockDecompSize + matchOffsetInCurrentBlock
BlockStart = currentBlockStreamOffset - prevBlockDecompSize
```

This allows callers to uniformly concatenate `PrevBlock()` with `Blocks[1]`
and use `Offset` directly, regardless of whether the previous block was
decoded or lazy:

```
data = append(result.PrevBlock(), result.Blocks[1]...)
matchPos = result.Offset  // always valid within data
```
