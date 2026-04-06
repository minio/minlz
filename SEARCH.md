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

### Table Reduction Limit

Maximum population percentage of the *reduced* table. Library default: 25%.
The `mz` CLI defaults to 50% without prefix and 25% with prefix.

After the base table is built, it is iteratively halved by folding (OR-ing) the upper
half into the lower half. Each reduction halves the table size but increases the
population density. Reductions stop before this threshold is exceeded.

Lower values produce smaller tables (fewer bytes per block in the compressed stream) but
with more false positives. Higher values keep larger, sparser tables that skip more blocks.

```go
cfg := minlz.NewSearchTableConfig().WithMaxReducedPopulation(30)
```

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
```

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

| Option                       | Description                                                           |
|------------------------------|-----------------------------------------------------------------------|
| `BlockSearchBailOnMissing()` | Return `ErrSearchTablesUnusable` if tables are absent or incompatible |
| `BlockSearchIgnoreCRC()`     | Skip CRC validation during search                                     |
| `BlockSearchMaxBlockSize(n)` | Limit maximum decoded block size                                      |

After `Search` returns, call `Stats()` for a `SearchStats` struct with block counts,
skip rates, table population metrics, and byte-level statistics. Use
`stats.Fprint(os.Stderr)` for a human-readable summary.

Note that the maximum backreference on matches is limited by the block size. 
So a match right after a block boundary will only have the previous block's data available. 