# 1 MinLZ Block Search Specification

This extends the base MinLZ spec in [SPEC.md](SPEC.md).

This specification contains an optional search feature for MinLZ streams.

Before each block an optional search information chunk is written.
Conceptually, it is like a bloom table for the block that allows quickly 
checking if a pattern is present in the block.

This can be used to determine if a block may, or definitely does *not* contain a specific pattern.
With this information, blocks can be skipped if searching for specific patterns.

## 1.1 Sidecar Search Index Streams

A stream can contain search indexes only. This means that blocks are referenced into another stream.

The stream should be valid, but instead of the block data, Remote Block Reference (chunk type 0x47)
is inserted for each data block.

A sidecar stream carries the stream header, the search-related chunks (0x44 / 0x45 / 0x46),
and a 0x47 Remote Block Reference per data block, followed by the stream's EOF chunk.
Other chunk types from the original stream (raw / compressed data, seek index, padding, etc.)
are NOT carried over — the sidecar references the data via the 0x47 offsets instead.

This allows the index to be stored separately from the data.

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
| 4          | CRC32 of table entries        |
| n          | Table entries. n = 2^(BT-R-3) |

Search pattern length, base table size and prefix values *should* match the values given in chunk type 0x44.
If no chunk type 0x44 has been seen or the values are different, the decoder *may* choose not to use the table.

`n` must be at least 32 bytes – meaning 256 entries.

The table entries are bits, where the search pattern resolves to 'x' 
can be resolved by looking up `table[x>>3] & (1<<(x&7))`.

A CRC32 is calculated of the table entries, similar to the CRC32 of the block data. 
See section [3. Checksum format](SPEC.md#3-checksum-format).

Note that encoders can decide to omit the Search Table for any block if it is deemed not worth the space, 
for example, if there are too many collisions for the table to provide any benefit.

The block table *must* include patterns that start in the upcoming block and continue into the next block 
- including if only the prefix is in the block.

It is allowed to have multiple block tables before a block.
All tables are assumed to be for the same block.
It is purely up to the decoder to decide which table(s) to use.

If creating an index - as per spec [section 4.12](SPEC.md#412-index-chunk-type-0x40--optional),
it is recommended to include this chunk as the indexed offset.

### 2.2 Compressed Block Search Table (chunk type 0x46, skippable)

The Compressed Block Search Table is an optional chunk that will come before a block that represents the contents.

| Length     | Code  | Description                    |
|------------|-------|--------------------------------|
| 1          |       | Table Type                     |
| 1          |       | Search pattern length          |
| 1          |       | Base Table Size in log2        |
| 0/8/32/1+n |       | Prefix values                  |
| 1          |       | Reductions from Base Table     |
| 4          |       | CRC32 of table entries         |
| 1          | h0_bs | log2 huff0 block size          |
| 1          | h0_tc | huff0 table count              |
| ...        |       | huff0 tables                   |
| 1          | h0_ti | huff0 table index              |
| 1-3        | bd    | compressed length (uvarint)    |
| bd         |       | huff0 4X compressed block data |
| ...        |       | Additional huff0 blocks        |

See Section 2.1 on how to decode table information until the huff0 section.

The index bits are split into huff0 blocks.

The first value (h0_bs) is the log2 of the uncompressed huff0 block size in bytes.
The bitmap size (n) must be divisible by the huff0 block size.
The maximum huff0 block size is 128KiB (h0_bs = 17).
The minimum size is 32 bytes (h0_bs = 5).

Tables for huff0 blocks are stored separately. There can be up to 16 tables per block.
The encoder is allowed to reuse tables for different huff0 blocks.

The encoded huff0 tables follow. This is similar to zstd, [rfc 8878](https://datatracker.ietf.org/doc/html/rfc8878#section-4.2).
Table sizes are self-contained, but h0_tc must be read.

The number of blocks can be calculated as `n / (1 << h0_bs)`.

For each block the table index is specified by the h0_ti value and decoded as follows:

| h0_ti value | Meaning                                   |
|-------------|-------------------------------------------|
| 0 -> 15     | huff0 table index                         |
| 16          | Uncompressed block                        |
| 17          | RLE - Read 1 byte, repeat value for block |
| 18          | Sparse Bit Table                          |
| 19 -> 255   | Reserved [invalid]                        |  

If h0_ti is <= 15, the compressed size follows as a uvarint - and the compressed data itself.
Note the compressed streams are always [4 streams interleaved](https://datatracker.ietf.org/doc/html/rfc8878#name-jump_table),
also named 4X in zstd.

An uncompressed block (h0_ti = 16) and RLE (h0_ti = 17) will not have any size,
since that can be inferred.

RLE in this context means "single value repeated for the entire block" and can only be used for that.

#### 2.2.1 Sparse Bit Table

A sparse bit table can be used for very sparsely populated blocks.

Like huff0 block, this starts with a uvarint encoded length in bytes.

The block contains byte-encoded distances between bits.

To decode, read single bytes. Each byte is the distance to the next set bit.
Bits are counted from the least significant bit.

If the byte is 255, add 255 to the distance and read the following byte.

When the final byte has been read, there are no more bits. 
The distance is not expected to reach the end of the block. 

### 2.3 Remote Block Reference (chunk type 0x47, skippable)

When generating search indexes from an existing stream or you, for
another reason, want to separate the search index from the data,
you can use the Remote Block Reference chunk type to indicate a remote block.

| Length  | Description                          |
|---------|--------------------------------------|
| 1       | Chunk ID                             |
| 3       | Chunk Size                           |
| UVarInt | Block Offset                         |
| UVarInt | Max Uncompressed - Actual Block Size |
| ...     | <Additional blocks...>               |

This block replaces a block that and indicates the offset of the block in the stream.

The Chunk ID and Block Size are the same as specified on the [stream chunks](SPEC.md#1-general-structure).
This means Chunk Size does also not include the 4-byte header. 

The uncompressed size is the [maximum block size](SPEC.md#411-max-block-size) as
indicated by the header minus the actual block size.

If more values are present, it indicates additional blocks without indexes between.
These are stored as relative offsets from the current block.

The block offsets must be in strictly ascending order.

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
  WILL be indexed in block N's table (the prefix is in block N's data). This
  includes the case where the prefix byte is block N's very last byte: the
  encoder reads the value from the overlap (the first `matchLen` bytes of block
  N+1) and records it in block N, because block N+1 cannot index it — its
  position 0 has no preceding prefix byte. See Appendix B.1 and B.4.3.
- For a multi-byte (long) prefix that STARTS in block N but straddles into block
  N+1, the occurrence is likewise indexed in block N — the block where the prefix
  starts — reading the remainder of the prefix and the value from the overlap.
  Block N+1 cannot index it (its prefix is only partly in block N+1's data).

An empty prefix table (all zero bits) indicates that no prefix bytes exist
in the block, allowing the searcher to skip it entirely.

#### 3.3.2 Table Type 2

With table type 2 up to 8 individual prefix values can be defined.

If less than 8 values are needed, the rest can be filled with duplicates of previous ones.

#### 3.3.3 Table Type 3

Each bit indicates if a byte value at that position is a prefix of the search pattern.

#### 3.3.4 Table Type 4

| Length | Description           |
|--------|-----------------------|
| 1      | Table Type (always 4) |
| 1      | Prefix Length         |
| 1      | Extra Matches         |
| n      | Prefix                |

The first byte defines the prefix length. One must be added to the length after being read.

The second byte defines the number of extra matches following the prefix.
0 means one match, 15 means 16 matches. `matchLen + Extra Matches` MUST NOT exceed 16.
Tables where `matchLen + Extra Matches > 16` are malformed and MUST be rejected.

For each indexed position `P+pl` (the byte immediately following the prefix in the data),
hashes are written for `Extra Matches + 1` consecutive overlapping windows at positions
`P+pl, P+pl+1, ..., P+pl+Extra Matches`. Each window hashes `matchLen` bytes, so the
windows collectively span bytes `[P+pl, P+pl+matchLen+Extra Matches)` of the data.

All `Extra Matches + 1` hash entries for one prefix occurrence are stored in the same
block's search table — the block where the prefix STARTS. The encoder's tail-overlap
into the next block must therefore cover `len(prefix) - 1 + matchLen + Extra Matches`
bytes (rather than `matchLen`), so that every occurrence whose prefix starts in this
block can read its windows from the overlap — including a prefix that itself straddles
the boundary, and the position whose last prefix byte is the block's last byte.

The prefix (1 to 256 bytes) is stored after the length indication and the extras byte.

A type 4 table with prefix `"id":` will only contain entries following that prefix.

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
With `Extra Matches = E`, the encoder writes `E+1` overlapping windows of `matchLen`
bytes per prefix occurrence. The searcher mirrors this: for every prefix occurrence
found inside the pattern, it checks all `E+1` windows. Every checked window must be
present for that occurrence to remain a candidate.

For pattern `P` of length `L`, prefix `pfx` of length `K`, matchLen = `M`, extras = `E`:

```text
checked = 0
for i = 0 to L - K - M - E:
    if P[i : i+K] == pfx:
        for j = 0 to E:
            if not present(P[i+K+j : i+K+j+M]):
                skip block
        checked++
if checked == 0:
    cannot use table (fall back to full decode)
```

A prefix occurrence is only usable when the pattern has at least `K + M + E` bytes
after it (so all `E+1` windows fit). Occurrences that fall short of the trailing-byte
requirement are silently ignored; if no occurrence is usable, the table cannot help
and the searcher falls back.

For example, with prefix `":"`, matchLen 4, and extras 3, searching for
`stamp":"1679909263` finds `":"` at position 5. The windows `1679`, `6799`, `7990`,
`9909` are all checked.

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
`S-M+1` through `S`. Each reads bytes from both block N and block N+1. Position
`S` is the window whose prefix byte is block N's last byte; prefix tables index
it (block N+1 cannot — see 3.3.1), while no-prefix tables stop at `S-1` because
block N+1 indexes that window at its own position 0. The encoder should provide
the first `M` bytes of block N+1 as overlap when building block N's table (`M-1`
suffices for no-prefix tables).

For type 4 (long prefix) tables the prefix is `pl` bytes and may itself straddle the
boundary, and with `Extra Matches = E` every indexed occurrence contributes `E+1`
consecutive windows. An occurrence is indexed in the block where its prefix STARTS
(see 3.3.1). The encoder must therefore read up to `(pl-1) + M + E` bytes past the
block end, so the overlap from block N+1 must extend to `pl - 1 + M + E` bytes for
type 4 tables. (Types 2 and 3 have `pl = 1`, giving the `M + E` requirement above.)

For contiguous buffers (`EncodeBuffer`), the overlap is directly available.
For streaming writes the encoder **defers** a block until the next block's bytes
are buffered, so every non-final block is tabled with its full overlap. A block
forced out without that overlap — a mid-stream `Flush` — is emitted with **no
search table** (it is always scanned at search time; see B.4.3). The final block
in a stream has no successor, so its missing forward overlap is harmless and it
keeps its table.

**Prefix tables and overlap:** For prefix-filtered tables (types 2, 3, 4), the overlap
tail positions must still respect the prefix context. An overlap position is only indexed
if its preceding byte in the block data is a valid prefix byte. This ensures that prefix
tables remain accurate — an empty prefix table correctly indicates "no prefix bytes exist
in this block" and the block can be skipped for any prefix-aware search.

Implementation note: the overlap positions are few (at most `M = 8` for matchLen 8)
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
- The last `len(pattern)-1` bytes of the contiguously decoded data preceding the block
- The first `len(pattern)-1` bytes of the current block

If the concatenation of these regions contains the pattern, it is a boundary match.
The preceding region is a rolling tail of the most recent contiguous run of decoded
blocks — **not** just the single previous block. A short interior block (e.g. one
emitted by a mid-stream `Flush`) may be shorter than `len(pattern)-1`, so a single
match can straddle three or more blocks; the tail must therefore span as many prior
blocks as needed to cover `len(pattern)-1` bytes. The tail is reset whenever a block
is skipped or deferred (its bytes are not decoded, breaking contiguity). The overlap
indexing in B.1 ensures that if a pattern starts in block N, block N's table has
the first window set (for type 1) or the raw first window set (for prefix types,
via the overlap tail), so block N is decoded and contributes to the tail for block N+1.

A searcher should not skip a block if the boundary tail could start a match reaching
into it. A boundary match is possible only when some suffix of that tail
(`last len(pattern)-1 bytes` of the preceding contiguous decoded data) is a prefix of
the search pattern. If no such overlap exists, the block can safely be skipped and the
tail cleared.

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

#### B.4.3 Prefix Tables: Tabled Blocks Carry Full Overlap

The deferred check (B.4.1) verifies absent windows against block N+1's table.
For prefix tables (types 2, 3, 4) this is sound because of an encoder invariant:
a block's search table is emitted **only when the encoder had the block's full
forward overlap** (`len(prefix) - 1 + matchLen + Extra Matches` bytes of the next
block; see B.1). The encoder records, for every prefix occurrence that STARTS in
block N — including one whose prefix byte is its last byte, or whose multi-byte
prefix straddles into block N+1 — all `Extra Matches + 1` windows that follow it
(section 3.3.1, B.1). So a tabled block holds every prefix-context window of every
occurrence starting in it; there is no boundary "blind spot".

When the overlap is unavailable — a mid-stream `Flush`, where the next block's
bytes don't exist yet — the block is emitted with **no search table** (its `0x47`
ref, in sidecar mode, is still written). A tableless block is always scanned, so
a match straddling that boundary is still found via the decoded-block boundary
check (B.2/B.3); it simply can't be skipped. The stream's final block has no
successor (hence no forward straddle), so it keeps its table.

**Deferral rule.** Because every tabled block carries full overlap, for a real
match straddling N→N+1 every window absent from block N's table is present in
block N+1's table. The searcher therefore defers **all** absent windows after the
first present one and skips block N if any is missing from N+1 — the same rule as
the no-prefix path (B.4.1), with no special boundary handling.

For type 4 (long prefix) with `Extra Matches = E` the same defer-all rule applies:
each pattern occurrence contributes `E+1` windows, all stored together in the
prefix-start block, so the absent windows of every occurrence after the first
present one are deferred and checked against block N+1's table.

**Raw fallback.** When the first prefix window is absent but the raw first window
is present (the pattern's leading prefix byte lies in the previous block), the
absent windows are deferred only when that raw window is set in block N's table.

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
