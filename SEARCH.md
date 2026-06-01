# Stream Searching

## Introduction

MinLZ streams can include optional per-block hash tables that allow searching
compressed data without decompressing every block. When a block's table indicates the
search pattern is definitely absent, the block is skipped entirely — via `io.Seeker`
if available (single syscall), or by buffering past the compressed data.

Streams with search tables remain fully backward-compatible: readers that don't
understand the table chunks silently skip them, and the compressed data is unchanged.

Search tables are generated during compression with zero impact on the compressed data
itself — they are stored as additional skippable chunks interleaved between blocks.

For format specification see [SPEC_SEARCH.md](SPEC_SEARCH.md).

### How It Works

Each block's search table is a bit array where each position in the uncompressed data
is hashed and the corresponding bit is set. When searching, the pattern's byte windows
are hashed and checked against the table:

- If **any** window hash is not set: the block **definitely** doesn't contain the pattern — skip it.
- If **all** window hashes are set: the block **might** contain the pattern — decode and search.

#### Example

Say you are indexing a block with hashes of 4 bytes. This means that a block witj `abcdefgh` will be
hashed as `abcd`, `bcde`, `cdef`, and `defg`.

So if we are looking for `bcdef` in a block, we will check if `bcde` and `cdef` are set
in the table. If not, we skip the block.

We do not know anything about the position or the order of the values, so a block with `bcdeXYZbcde` 
will also appear to match the search. There is a chance that collisions will occur, 
so it is possible that `1234` will collide with `bcde` and therefore produce a false positive. 

#### Sizes and Reductions

Tables are reduced (halved) by OR-folding the upper and lower halves, trading accuracy
for size. Reductions are applied per-block based on population density.

Longer search patterns produce more window checks, giving exponentially better
filtering. For example, a 19-byte pattern with matchLen=8 produces 12 window
checks — all 12 must match for a false positive, which is extremely unlikely
with typical table populations of 10–30%.

There is a limit to table sizes at 1 bit per byte. This means that at most the tables will b 1/8th of the
uncompressed stream size - and for these tables a maximum population count - default at 70%.
If the 8:1 table is filled more than this they will not be saved to the stream.

This means that blocks with near-random data will not have any tables
and searching will have to fall back to decompression.

MinLZ will not attempt to generate tables for incompressible blocks.

## Parameters

### Match Length

Controls how many bytes of each position are hashed into the table. Range: 1–8,
default: 6.

- **Lower values** (e.g. 4) hash fewer bytes per position, making the table useful for
  shorter search patterns. However, shorter hashes collide more, increasing table
  population.
- **Higher values** (e.g. 8) hash more bytes per position, giving fewer collisions.
  However, a search pattern of length N only produces `N - matchLen + 1` hash windows
  to check. Fewer windows means fewer independent chances to prove a block doesn't
  contain the pattern, which can *reduce* skip rates. Higher match lengths also produce
  lower base population, which means fewer reductions and larger tables on disk.

The match length must be less than or equal to the search pattern length. Patterns
shorter than the match length cannot use the table (the searcher falls back to full
decode).

A good default is 6; it balances table density against the number of check windows.
Use 4 for short patterns (e.g. short IDs), but be aware that short windows from common
character classes (digits, hex, lowercase) will appear in nearly every block, collapsing
skip rates. For example, searching numeric data with matchLen=4 can drop skip rates to
single digits because 4-byte digit sequences are ubiquitous.

```go
cfg := minlz.NewSearchTableConfig().WithMatchLen(5)
```

### Table Reduction Limit

Once a base table is built, it can be reduced — folded in half by OR-ing the
upper half into the lower half. Each reduction halves the size of the table
on the wire and doubles the population density (because two slots' worth of
1-bits are merged into one). Reductions keep going until the projected
population exceeds this limit; whatever is left is what the stream
carries.

The trade-off is direct:

- **Smaller table** = fewer bytes per block stored alongside the data, but
  more **false positives** at search time (two distinct hash values can now
  share a slot, so a "maybe" answer is more often wrong → more blocks
  decoded unnecessarily).
- **Larger table** = more bytes on the wire, but **fewer collisions** —
  blocks that don't contain the pattern are skipped more often.

A useful intuition: every reduction roughly doubles the collision rate.
A 25 % populated reduced table will spuriously match ~25 % of the time
*per window check*. With a long pattern this multiplies down (each window
check is independent), so even a moderately populated table filters
aggressively for patterns longer than `matchLen`. Short patterns (close to
`matchLen` bytes) get all their filtering from a single window and benefit
most from a sparser table.

Defaults:

- Library default: 25 %.
- `mz` CLI default: 50 %, automatically tightened (halved with prefix or
  compression, divided by 3 with both) since [prefixed tables](#prefixes)
  and [compressed tables](#compressed-search-tables-opt-in) both want
  sparser bitmaps.

```go
cfg := minlz.NewSearchTableConfig().WithMaxReducedPopulation(30)
```

When compression is on, lower limits (<20 %) has much less of an impact on
stream size than the default because the extra bytes spent on a larger bitmap
are recovered by the huff0 / sparse encoding.

### Table Max Population Size

Maximum percentage of bits that may be set in the base table before it is discarded
entirely. Default: 70%.

When a block's data is highly random or the match length is short, most hash slots get
filled and the table loses its ability to prove absence. Tables exceeding this threshold
are dropped — the block will always be decoded during search.

Lowering this value makes the compressor more aggressive about discarding noisy tables,
reducing overhead at the cost of fewer indexed blocks. Raising it keeps more tables but
with higher false-positive rates.

```go
cfg := minlz.NewSearchTableConfig().WithMaxPopulation(50)
```

### Compressed Search Tables (opt-in)

Search-table bitmaps can optionally be stored compressed. This is a stream-
level opt-in: the encoder shrinks each per-block bitmap when doing so saves
bytes, and the decoder unpacks it on the fly during search.

When it helps most:

- **High-Quality tables**. This will reduce the impact of having a low
  reduction limit and therefore less collisions.
- **Sparse search tables** (small prefix matchLen, structured data with a
  selective prefix). Often shrinks the table by 5×–50× on the wire.
- **Very dense tables** where the same byte value repeats (especially
  all-zero or all-one bitmaps that compress to a few bytes regardless of
  size).

When it makes little difference:

- Tables with ~50% population: byte entropy is already maximal, so there's
  nothing to compress. The encoder detects this and skips the compressed
  form by default.

Enable it via `WithCompression` on the search table config:

```go
// Defaults: 10 % popcount band, no stats hook.
cfg := minlz.NewSearchTableConfig().WithCompression()
w := minlz.NewWriter(out, minlz.WriterSearchTable(cfg))
```

Tuning options (all optional):

| Option                                   | Description                                                                                       |
|------------------------------------------|---------------------------------------------------------------------------------------------------|
| `CompressedSearchSkipPct(pct float64)`   | Skip compression when popcount % is within ±pct of 50 (default 10 %).                             |
| `CompressedSearchStatsHook(fn)`          | Per-bitmap callback with disposition counts and on-wire sizes — useful for tuning.                |
| `CompressedSearchForce()`                | Emit the compressed chunk even when larger than the uncompressed form. Benchmarking only.         |

When compression is enabled, the recommended `MaxReducedPopulation` is
lower than the default — leaving tables sparser pays off because the bytes
shrink so much. The `mz` CLI does this automatically; for library use,
call `.WithMaxReducedPopulation(15)` (or pass `-search.lim` to `mz`).

Decoding is transparent: a `BlockSearcher` over a stream that mixes
compressed and uncompressed search tables handles both with no caller
opt-in. The per-bitmap unpack runs in parallel for large bitmaps, so
search throughput is unchanged in practice.

For format details see `SPEC_SEARCH.md` §2.2 / §2.2.1.

Some decoders may not support compressed search tables.

### Prefixes

Prefix filtering dramatically reduces table size for structured data by only indexing
positions that follow specific bytes. For example, in JSON data, values always follow
`"` or `:`, so most byte positions can be skipped during indexing.

Since fewer positions are indexed, the base table population is much lower, allowing
more reductions and producing significantly smaller tables on disk. The downside is that
search patterns must contain at least one prefix byte (or match the long prefix) for
the table to be usable. Patterns without any prefix bytes fall back to full block decode.

There are two prefix modes:

#### Single byte

Single-byte prefixes only indexes positions preceded by one of these bytes.
`WithBytePrefix` accepts up to 8 bytes directly; for more than 8, use `WithMaskPrefix`
with a 256-bit bitmask. Both produce the same result.
```go
cfg := minlz.NewSearchTableConfig().WithBytePrefix('"', ':')
```

#### Long prefix

Long prefix only indexes positions preceded by an exact multi-byte sequence (1–256 bytes):
```go
cfg := minlz.NewSearchTableConfig().WithLongPrefix([]byte(`id:"`))
```

The searcher scans the search pattern for prefix bytes *anywhere inside it*. For example,
searching for `"unique-9876"` with byte prefix `"` works because `"` appears at position 0
in the pattern. The table is consulted for the hash window that follows each prefix
occurrence in the pattern.

When no prefix bytes appear in the search pattern, the table cannot be used and the
searcher falls back to full block decode.

#### Choosing good prefix bytes

Pick bytes that immediately precede the values you'll search for:

- **JSON data:** `"` and `:` — values always follow `":` or `:[`.
- **CSV data:** `,` or `\t` — field separators.
- **Key=value formats:** `=` precedes values.
- **Log lines with fields:** space, tab, `=`, or `:`.

## Sidecar Streams

Search tables normally live *inside* the compressed stream — one per block,
right in front of the corresponding data. A **sidecar stream** instead puts
every search table into a *separate* file, leaving the main `.mz` stream as
plain compressed data. Searching then takes two inputs: the sidecar (read
once, top-to-bottom) and the main stream (random-access via `io.ReaderAt`,
typically a regular file).

### Why?

- **You can't (or don't want to) re-encode the data.** Maybe the `.mz` was
  written long ago, or it lives on read-only storage. `BuildSidecar` decodes
  each block, builds fresh tables, and writes the sidecar — the original
  `.mz` is never touched.
- **You want to store the index separately.** Indexes can be regenerated;
  data often can't. Putting them on different storage tiers (or even
  different hosts) is a clean way to express that.
- **You want to drop the indexes from an existing indexed stream.** If you
  compressed with `-search` but later want to slim the main file without
  losing search, extract the index to a sidecar and strip the main.
- **You want multiple search configurations for the same data.** The inline
  writer only carries one config; a sidecar can carry many (for example one
  tuned for short patterns and one tuned for JSON keys).

The main stream and the sidecar are **both valid MinLZ streams on their
own**. A regular `NewReader` over a sidecar produces zero data bytes (only
skippable chunks); over a stripped main stream it produces the original
payload.

Sidecar files conventionally use the `.mzs` extension (sits next to `.mz`),
but they are ordinary MinLZ streams and can have any filename.

### Three ways to produce a sidecar

#### 1. While compressing — `WriterSidecar`

The main writer behaves normally; the search-table chunks are diverted to a
second `io.Writer`:

```go
mainF, _ := os.Create("foo.mz")
sideF, _ := os.Create("foo.mz.mzs")
cfg := minlz.NewSearchTableConfig().WithMatchLen(6)
w := minlz.NewWriter(mainF,
    minlz.WriterSearchTable(cfg),
    minlz.WriterSidecar(sideF),
)
w.Write(data)
w.Close()
```

#### 2. From an existing stream that has no indexes — `BuildSidecar`

Reads the source once, decompresses each block, builds fresh tables, and
writes the sidecar. The source is not modified:

```go
src, _ := os.Open("foo.mz")
side, _ := os.Create("foo.mz.mzs")
err := minlz.BuildSidecar(side, src,
    minlz.SidecarSearchTable(minlz.NewSearchTableConfig().WithMatchLen(6)),
)
```

Pass `SidecarSearchTable` multiple times to embed several configs in one
sidecar. At search time a block is skipped if **any** embedded table proves
the pattern absent; tables that don't apply to the query are ignored.

#### 3. From an existing indexed stream — `ExtractSidecar`

Copies the existing `0x44`/`0x45`/`0x46` chunks verbatim into a sidecar. No
decoding required. Two modes:

```go
// (a) No stripping. Sidecar offsets reference src's original layout, so
//     the original file must be kept and searched against.
minlz.ExtractSidecar(sideOut, nil, src)

// (b) With stripping. Re-emits src to newMainOut without the search chunks;
//     sidecar offsets reference the new layout. A fresh seek index is
//     appended to newMainOut so it remains seekable on its own.
minlz.ExtractSidecar(sideOut, newMainOut, src)
```

### Searching with a sidecar

```go
mainF, _ := os.Open("foo.mz")        // io.ReaderAt
sideF, _ := os.Open("foo.mz.mzs")    // io.Reader

searcher := minlz.NewSidecarSearcher(mainF, sideF)
searcher.Search([]byte("pattern"), func(r minlz.SearchResult) error {
    fmt.Printf("match at stream offset %d\n", r.StreamOffset)
    return nil
})
```

The searcher walks the sidecar once and only touches the main stream for
blocks that aren't proven absent. Adjacent must-decode blocks are coalesced
into a single `ReadAt`; skipped blocks issue no I/O against the main stream
until the caller invokes `result.PrevBlock()` for boundary context (then
they are fetched and decoded on demand).

`SidecarSearcher` accepts the same `BlockSearchOption`s as `BlockSearcher`
(`BlockSearchBailOnMissing`, `BlockSearchCollectStats`,
`BlockSearchIgnoreCRC`, …). Multiple goroutines may run independent
`SidecarSearcher`s against the same main file concurrently — each owns its
own sidecar `io.Reader` and the underlying `ReadAt` is shared safely.

### Commandline

```
mz sidecar build   [-search.lens=4,6] [-search.compress] foo.mz   # -> foo.mz.mzs
mz sidecar extract [-newstream stripped.mz]              foo.mz   # -> foo.mz.mzs (+ stripped.mz)
mz search --sidecar foo.mz.mzs  "pattern"  foo.mz                 # search via sidecar
```

`mz sidecar build` accepts most of the same `-search.*` flags as
`mz c -search` (match length, prefixes, compression, population limits).
The `-search.lens` flag is comma-separated to request multiple configs in
one sidecar.

### Sidecar format at a glance

A sidecar stream contains, in order:

1. Stream identifier (`0xff`) — same maximum block size as the main stream.
2. One or more **Search Table Info** chunks (`0x44`) — one per embedded config.
3. For each data block in the main stream:
   - Zero or more **Search Table** chunks (`0x45`/`0x46`) — one per config
     that produced a usable table for that block.
   - One **Remote Block Reference** chunk (`0x47`) carrying the block's
     offset in the main stream and its uncompressed size.
4. EOF chunk (`0x20`).

For format details see [SPEC_SEARCH.md §1.1 / §2.3](SPEC_SEARCH.md).

## Commandline

The `mz` tool supports search table generation during compression and pattern search
on compressed files.

**Compression with search tables:**
```
mz c -search file.log                              # default matchLen=6, no prefix
mz c -search -search.len=4 file.log                # matchLen=4
mz c -search -search.prefixes='":'  file.log       # byte prefixes " and :
mz c -search -search.prefix='id:"' file.log        # long prefix
mz c -search -search.max=50 -search.lim=30 file.log # custom population limits
mz c -bs=1MB -search file.log                       # 1MB blocks (more granular skipping)
mz c -search -search.compress file.log              # also compress the search tables
mz c -search -search.compress -search.compress.skip=5 file.log # tighter popcount band
```

When `-search.compress` is set, `-search.lim` is automatically tightened
(divided by 2 with a prefix or compression, by 3 with both) so the encoder
keeps larger, sparser bitmaps that compress better. Override `-search.lim`
to taste — lower values produce sparser, more compressible tables.

**Searching compressed files:**
```
mz search "pattern" file.log.mz                     # print matching lines
mz search -c "pattern" file.log.mz                  # count matching lines
mz search -n "pattern" file.log.mz                  # print with line numbers
mz search -v "pattern" file.log.mz                  # verbose: print stats after search
mz search -q "pattern" file.log.mz                  # quiet: exit code only (0=found, 1=not)
mz search -bail "pattern" file.log.mz               # error if tables are missing/unusable
```

When tables are absent, the searcher decodes every block (equivalent to decompress + grep).
With tables present, only blocks that might contain the pattern are decoded.

## API reference

### Compression

Enable search tables by passing `WriterSearchTable` as a writer option:

```go
// Default configuration (matchLen=6, no prefix).
cfg := minlz.NewSearchTableConfig()
w := minlz.NewWriter(output, minlz.WriterSearchTable(cfg))
```

```go
// With byte prefixes for structured data.
cfg := minlz.NewSearchTableConfig().
    WithMatchLen(4).
    WithBytePrefix('"', ':')
w := minlz.NewWriter(output, minlz.WriterSearchTable(cfg))
```

```go
// With long prefix for field-specific indexing.
cfg := minlz.NewSearchTableConfig().
    WithLongPrefix([]byte(`"id":"`))
w := minlz.NewWriter(output, minlz.WriterSearchTable(cfg))
```

Tables are generated concurrently alongside compression with no extra goroutine
synchronization overhead. The `Writer` handles all table generation and chunk
serialization automatically.

Configuration methods:

| Method                          | Description                                         |
|---------------------------------|-----------------------------------------------------|
| `NewSearchTableConfig()`        | Create config with defaults (matchLen=6, no prefix) |
| `WithMatchLen(n)`               | Set match length 1–8                                |
| `WithBytePrefix(b...)`          | Set 1–8 prefix bytes (>8 auto-promotes to bitmask)  |
| `WithMaskPrefix(mask)`          | Set a 256-bit prefix bitmask                        |
| `WithLongPrefix(p)`             | Set a multi-byte prefix (1–256 bytes)               |
| `WithMaxPopulation(pct)`        | Discard tables above this population % (default 70) |
| `WithMaxReducedPopulation(pct)` | Stop reducing above this population % (default 25)  |

Decompressing the stream will ignore the search tables.

### Searching

Search compressed streams using `BlockSearcher`:

```go
searcher := minlz.NewBlockSearcher(input)
err := searcher.Search([]byte("pattern"), func(r minlz.SearchResult) error {
    fmt.Printf("match at stream offset %d\n", r.StreamOffset)
    return nil
})
```

The callback receives a `SearchResult` for each match:

| Field          | Type        | Description                                                         |
|----------------|-------------|---------------------------------------------------------------------|
| `Blocks`       | `[2][]byte` | `[0]` = previous block (nil if skipped/lazy), `[1]` = current block |
| `Offset`       | `int`       | Match position relative to `PrevBlock()` + `Blocks[1]`              |
| `StreamOffset` | `int64`     | Absolute byte offset in the uncompressed stream                     |
| `BlockStart`   | `int64`     | Stream offset of `PrevBlock()` data                                 |
| `PrevBlockLen` | `int`       | Decompressed size of previous block (avoids lazy decode)            |

Methods on `SearchResult`:

- `PrevBlock() []byte` — Returns the previous block's data. Lazily decompresses if the
  previous block was skipped by the index. Returns nil if no previous block exists.

Return values from the callback:

- `nil` — continue searching
- `ErrSearchForward` — request the next block for forward context; the searcher will
  re-call the callback with the same match but `Blocks[1]` replaced by the next block
- any other error — abort the search

Searcher options:

| Option                              | Description                                                                                                  |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `BlockSearchBailOnMissing()`        | Return `ErrSearchTablesUnusable` if tables are absent or incompatible                                        |
| `BlockSearchIgnoreCRC()`            | Skip CRC validation during search                                                                            |
| `BlockSearchMaxBlockSize(n)`        | Limit maximum decoded block size                                                                             |
| `BlockSearchCollectStats()`         | Populate `SearchStats` during the search                                                                     |
| `BlockSearchInfoCallback(fn)`       | Invoke `fn(SearchTableConfig)` when the stream's Search Info chunk is parsed — useful for logging/inspection |

After `Search` returns, call `Stats()` for a `SearchStats` struct with block counts,
skip rates, table population metrics, and byte-level statistics. When the
stream uses compressed search tables, the stats also report:

- counts of 0x45 (uncompressed) vs 0x46 (compressed) search-table chunks,
- a per-disposition breakdown of huff0 sub-blocks (raw / RLE / sparse / tabled),
- on-wire payload bytes for each disposition.

Use `stats.Fprint(os.Stderr)` for a human-readable summary. The `mz search -v`
command also prints the parsed Search Info config the first time it's seen
in the stream.

Note that the maximum backreference on matches is limited by the block size. 
So a match right after a block boundary will only have the previous block's data available.
