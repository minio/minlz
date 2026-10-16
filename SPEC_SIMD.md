# MINLZ SIMD BLOCKS SPECIFICATION

This document adds SIMD blocks to the [MinLZ specification](SPEC.md): entropy-coded blocks that decode with SIMD.
Anything not described here is as defined there.

Entropy coding uses pivco blocks, defined in [SPEC_PIVCO.md](SPEC_PIVCO.md): a subset of [pivco-huffman](https://github.com/MarcinZukowski/pivco-huffman).

# BLOCK FORMAT

A SIMD block stores its literals and its LZ operations in separate sections.

## 1.0 Block Header

A SIMD block starts like a MinLZ block (SPEC.md 1.0 and 1.1):
the MinLZ indicator byte 0, followed by an unsigned varint.
As for MinLZ blocks, the indicator is omitted in streams.

The varint holds the block type and the decoded size:

| Bits  | Meaning      |
|-------|--------------|
| 0-23  | Decoded size |
| 24-31 | Block type   |

| Type  | Block                        |
|-------|------------------------------|
| 0     | MinLZ block (SPEC.md)        |
| 1     | SIMD block, this document    |
| 2-255 | Reserved                     |

Decoders *must* reject reserved types. The varint must fit in 32 bits.

A flags byte follows the varint:

| Bits | Meaning                                                             |
|------|---------------------------------------------------------------------|
| 0-2  | Chunk limit (4.2): chunks have at most `1024 << value` operations   |
| 3-4  | Reserved hints, must be ignored                                     |
| 5-7  | Reserved, must be 0                                                 |

Chunk limit values 6 and 7 are invalid. Encoders set the reserved hints to 0.

## 1.1 Size Limits

The decoded size is at most 8 MiB = 8,388,608 bytes.

A block has at most 128 chunks (4).
With 16384 operations per chunk, that is an average of 4 bytes per operation in an 8 MiB block.

The encoded block, everything after the indicator byte, must be at least 64 bytes smaller than the decoded size.

## 2 Block Layout

Sizes are stored *after* the data they describe, as reverse varints (2.1),
so a block is parsed from its end.

    | 0 | varint | flags | LZ section | literal section | L |

The LZ section starts after the flags byte.
`L` is the size of the LZ section, and ends the block.
The literal section is everything between the LZ section and `L`.
It has no stored size.

Both sections have the same form: their data, then *records* that describe the data, then `R`, the size of the records:

    | data | records | R |

`L` and `R` must not be larger than the bytes before them.
The records end exactly at `R`, and the sizes in their entries add up exactly to the data.

### 2.1 Reverse Varints

A reverse varint is an unsigned varint with its bytes in reverse order.

It is read backwards, starting with its last byte.
Each byte adds its lower 7 bits, least significant bits first.
The first byte with the top bit clear is the last byte read.

| Value  | Varint     | Reverse varint |
|--------|------------|----------------|
| 5      | `05`       | `05`           |
| 300    | `AC 02`    | `02 AC`        |
| 100000 | `A0 8D 06` | `06 8D A0`     |

Values written one after another are read back in reverse order.

Varints and reverse varints are at most 10 bytes, and are not strictly needed to be minimal.

### 2.2 Entries

Records describe the stored streams with entries: one entry per literal block (3), and one per stream of a chunk (4).
An entry starts with an unsigned varint: the stored size of the stream's data in bytes times 8, plus its mode:

| Mode | Stored data                                                                                    |
|------|------------------------------------------------------------------------------------------------|
| 0    | The values, uncompressed                                                                       |
| 1    | A pivco block coded with the stream's current table                                            |
| 2    | A pivco block coded with a new Huffman table, given as code lengths (2.3)                      |
| 3    | A pivco block coded with a new Huffman table, given as class lengths (2.3)                     |
| 4    | A pivco block coded with a new Huffman table, given as class lengths of the delta layout (A.6) |
| 5-7  | Reserved, invalid modes                                                                        |

With modes 2-4, the new Huffman table follows the varint, inside the entry.
It becomes the stream's current table, also for the following chunks or literal blocks.
Modes 0 and 1 do not change the current table.
Each stream type has its own current table, and so do the literals. Mode 1 needs a current table.
A block starts without current tables; they never carry over from another block.
Mode 4 is only used for literals, in any literal block.

With modes 1-4, the stored data holds a pivco block (PIVCO_SPEC.md 3) that codes exactly the stream's values.
A pivco block does not store how many values it holds.
Tokens and literal blocks store their number of values as 2 bytes little endian, followed by the pivco block.
The other streams store only the pivco block: their number of values follows from the chunk's tokens and literal lengths (4.1).
Either way, the data fills the stored size of the entry.
A pivco block holds 1 to 65535 values (PIVCO_SPEC.md 1.1), so a stream without values, or with more than 65535, uses mode 0.

### 2.3 Huffman Tables

A Huffman table gives each value a code length of 0 to 11 bits; 0 means the value has no code.
Pivco blocks are coded with these code lengths (PIVCO_SPEC.md 1.2).
They must form a complete code, or give a single value length 1,
and must not have codes for values outside the stream's range.

Both table forms store 4 bits per length: byte `i` holds length `2i` in the low nibble, and `2i + 1` in the high nibble.
An odd number of lengths ends with a 0 nibble.

*Code lengths* store the length of every value, from 0 up to the stream's highest value.
Higher values have length 0.

*Class lengths* store one length per class of the stream's class layout, in class order.
A value's code length is its class length plus its in-class length; class length 0 means the class has no codes.
Appendix A, which defines the classes and in-class lengths of each layout, is part of this specification.

Token class lengths are 14 match length group lengths, then 4 flag lengths for match length codes 0-31, then 4 for codes 32-33 (A.4).
A token's code length is its group length, plus its in-group length, plus its flag length.
A token has no code if its group length or its flag length is 0.
The group lengths and both flag codes must each be complete codes.

The sizes of both forms, per stream, are in 4.2.

## 3 Literal Section

The literal section holds all literals of the block, in output order, in literal blocks.

    | literal block 1 | ... | literal block n | literal records | R |

`R`, a reverse varint, is the size of the literal records. It ends the literal section, just before `L`.
There is one literal record per literal block, in order. A literal record is an entry (2.2):

    literal record:  | varint: stored size * 8 + mode | table (modes 2-4 only) |

The mode and the table are in the record; the literal block holds only the stored data:

| Mode | Table in the record           | Literal block                                       |
|------|-------------------------------|-----------------------------------------------------|
| 0    | None                          | The literals                                        |
| 1    | None, the current table       | Number of literals (2 bytes LE), then a pivco block |
| 2    | Code lengths, 128 bytes       | Number of literals (2 bytes LE), then a pivco block |
| 3    | Class lengths (A.5), 7 bytes  | Number of literals (2 bytes LE), then a pivco block |
| 4    | Class lengths (A.6), 5 bytes  | Number of literals (2 bytes LE), then a pivco block |

The stored size is the size of the literal block, without the table.
A table set by a literal record is the literals' current table for the following literal records (2.2).

A decoder reads `R`, then the literal records forwards from their start.
The literal blocks follow each other from the start of the section, with the sizes their records give.

For example, 1000 literals in one literal block, coded with a new class-lengths table into a 600-byte pivco block:
the literal block is the 2 bytes `E8 03` and the pivco block, 602 bytes,
and the literal record is `D3 25` (`602 * 8 + 3` = 4819) followed by the 7 bytes of the table.

There is at least one literal block, and a literal block holds 1 to 65535 literals.

The number of literals must not exceed the decoded size of the block.

## 4 LZ Section

The LZ section holds the lz stream operations, in chunks.

A block may have no operations: then the LZ section is empty (`L` = 0) or has no chunks, and the output is the literals.

    | chunk 1 | ... | chunk n | chunk records | R |

`R`, a reverse varint, is the size of the chunk records, and ends the LZ section.
There is one chunk record per chunk, in order.

    chunk:                 | tokens | literal lengths | offset symbols | escapes | offset bits |
    chunk record:  | flags | entry  | entry           | entry          | entry   | entry       |

A decoder reads `R`, then the chunk records forwards from their start.
The chunks follow each other from the start of the section, and so do the streams of a chunk, with the sizes their entries give.
The number of chunks follows from the chunk records, and is at most 128 (1.1).

### 4.1 Chunk Records

A chunk record describes one chunk: its flags, and the stored size and coding of each of its five streams.
The chunk records are the records of the LZ section (4), one per chunk, in chunk order.
A chunk record has no size of its own: it ends after its last entry, and the next chunk record starts there.

    chunk record:  | flags  | tokens | literal lengths | offset symbols | escapes | offset bits |
                     1 byte   entry    entry             entry            entry     entry

The flags byte:

| Bits | Meaning                                                                                              |
|------|------------------------------------------------------------------------------------------------------|
| 0    | Delta literals: the first 32 literals of each operation of the chunk are stored as differences (4.5) |
| 1    | No overlap: no match of up to 32 bytes in the chunk is longer than its offset, after clamping (4.3)  |
| 2-3  | Reserved hints: encoders set them to 0, decoders ignore them                                         |
| 4-7  | Reserved, must be 0                                                                                  |

The no-overlap flag lets a decoder copy short matches without handling overlap. Decoders need not check it (4.3).

Each entry is an unsigned varint: the stored size of the stream's data in bytes times 8, plus its mode (2.2).

| Mode | Stored data                                          | After the varint        |
|------|------------------------------------------------------|-------------------------|
| 0    | The values, one byte each                            | Nothing                 |
| 1    | A pivco block, coded with the stream's current table | Nothing                 |
| 2    | A pivco block, coded with a new table                | The code lengths (2.3)  |
| 3    | A pivco block, coded with a new table                | The class lengths (2.3) |

Modes 4-7 are invalid in chunk records. The offset bits are raw bits, so their entry always has mode 0:
its varint is the size of the offset bits times 8.

A table has a fixed size, so it has no size of its own:

| Stream          | Code lengths (mode 2) | Class lengths (mode 3) |
|-----------------|-----------------------|------------------------|
| Tokens          | 68 bytes              | 11 bytes               |
| Literal lengths | 17 bytes              | 6 bytes                |
| Offset symbols  | 88 bytes              | 12 bytes               |
| Escapes         | 128 bytes             | 8 bytes                |

A new table becomes the stream's current table, for this chunk and the following ones, until the stream's next new table.
Mode 1 uses the current table of the same stream, so it needs a mode 2 or 3 entry for that stream earlier in the block.
The literals have their own current table (2.2).

The data of a chunk holds its streams in the order of its entries, each taking exactly the stored size of its entry:

    chunk:  | tokens | literal lengths | offset symbols | escapes | offset bits |

The first chunk starts at the start of the LZ section, and each chunk starts where the one before it ends.
So a stream's data starts after the stored sizes of all entries before it, in its own and earlier chunk records.

An entry does not store the number of values of its stream.
The tokens give the number of operations: the stored size with mode 0, otherwise the number stored before the pivco block (2.2).
The other streams hold exactly the values that the operations read (4.2):

| Stream          | Number of values                                                       |
|-----------------|------------------------------------------------------------------------|
| Tokens          | One per operation, 1 up to the chunk limit (1.0)                       |
| Literal lengths | One per token with the literals bit                                    |
| Offset symbols  | One per token without the repeat bit                                   |
| Escapes         | One per literal length 33, and one per token with match length code 33 |
| Offset bits     | The raw bits of the offset symbols (4.4), rounded up to whole bytes    |

So a decoder can check every stream of a chunk before it runs its operations.
It also knows how many values the pivco block of a stream holds before it decodes it (2.2):
the tokens give the number of literal lengths and offset symbols, and the tokens and literal lengths the number of escapes.

Chunks only divide the operations. The output position, the repeat offset, the next literal
and the current tables all continue from one chunk to the next.

For example, a chunk with three operations:

| Operation | Literals | Match length | Offset        |
|-----------|----------|--------------|---------------|
| 1         | 5        | 4            | New offset 20 |
| 2         | 2        | 6            | Repeat, so 20 |
| 3         | 0        | 8            | New offset 3  |

With all streams stored uncompressed (mode 0), the chunk data is:

    tokens           12 1B 20    literals + match 4, repeat + literals + match 6, match 8
    literal lengths  05 02
    offset symbols   12 03       20 is symbol 18 with 1 raw bit (0); 3 is symbol 3
    escapes                      no values
    offset bits      00          the 1 raw bit, rounded up to a byte

Operation 3 copies 8 bytes from offset 3, so the no-overlap flag is not set. The chunk record is:

    00 18 10 10 00 08

`00` is the flags, `18` is 3 bytes in mode 0 (`3 * 8 + 0`), each `10` is 2 bytes, the next `00` is an empty stream,
and `08` is the 1 byte of offset bits.

In a larger chunk, tokens coded with a new table in class lengths, stored as their 2-byte number and a 38-byte pivco block,
have the entry `C3 02` (`40 * 8 + 3` = 323), followed by the 11 bytes of the table.
A later chunk can code its tokens with that table, as mode 1 with no table after the varint.

### 4.2 LZ Streams

| Stream          | Values                                | Range | Code lengths | Class lengths                 |
|-----------------|---------------------------------------|-------|--------------|-------------------------------|
| Tokens          | One per operation                     | 0-135 | 68 bytes     | 11 bytes                      |
| Literal lengths | One per operation with literals       | 1-33  | 17 bytes     | 6 bytes                       |
| Offset symbols  | One per operation with a new offset   | 1-175 | 88 bytes     | 12 bytes                      |
| Escapes         | One per length code 33                | 0-255 | 128 bytes    | 8 bytes                       |
| Offset bits     | The raw bits of the new offsets (4.4) |       | None         | None                          |
| Literals (3)    | One per literal                       | 0-255 | 128 bytes    | 7 bytes, delta layout 5 bytes |

Each lz stream of a chunk holds exactly the values its operations read.
Values outside a stream's range are invalid, also when stored uncompressed.

A chunk has at least one operation, and at most the chunk limit (1.0).
It outputs at least as many bytes as it has operations.

### 4.3 Operations

| Token bits | Meaning                 |
|------------|-------------------------|
| 0          | Repeat                  |
| 1          | Literals                |
| 2-7        | Match length code, 0-33 |

Operations run in order, also across chunks. An operation:

1. With literals: copies as many literals as the next literal length.
2. When NOT repeat: reads a new offset (4.4), which becomes the repeat offset.
3. Copies as many bytes as the match length, from the repeat offset back.

The initial repeat offset is 1. If a block has operations, the first one must have literals.
Operations must not read more literals than the literal section holds.

Literal lengths and match length codes below 33 are the length.
33 is a length of 33 plus the next escape byte (33-288).
An operation's literal length escape comes before its match length escape.

As in SPEC.md, a match may be longer than its offset.

An offset larger than the current position is clamped to the current position, so the match copies from position 0.
Clamping does not change the repeat offset: each use clamps it to the position at that time.

Matches longer than 32 bytes must have an offset of at least 32, after clamping.
In a chunk with the no-overlap flag, matches of up to 32 bytes are not longer than their offset, after clamping.
Decoders need not check this. A block that breaks it may be rejected; otherwise its output is undefined.

### 4.4 Offsets

A new offset is an offset symbol plus raw bits:

| Symbol | Offset                                  | Raw bits |
|--------|-----------------------------------------|----------|
| 1-15   | The symbol                              | 0        |
| 16-175 | `(8 + m) << (b - 3)`, plus the raw bits | `b - 3`  |

Here `b = (symbol + 16) >> 3` and `m = (symbol + 16) & 7`.
So symbol 16 is offsets 16-17, symbol 24 is offsets 32-35, and symbol 175 is offsets 15,728,640-16,777,215.

Raw bits are read least significant bit first: bit `i` of the offset bits is bit `i & 7` of byte `i >> 3`.
Each chunk starts at bit 0. Its offset bits are exactly the bits it reads, rounded up to whole bytes;
unused bits in the last byte are ignored.

### 4.5 Delta Literals

In a chunk with the delta flag, the first 32 literals of each operation are stored as differences.

Literal `j` (0-31) of an operation that starts at position `p` decodes to the stored byte plus the output byte at `p - r + (j mod r)`, modulo 256.
`r` is the repeat offset before the operation, clamped to `p` (4.3); with `r` = 0 all references are 0.

When `r` is 32 or more, the reference is simply the output byte `r` positions before the literal.
When `r` is smaller, the references repeat the last `r` output bytes.

For example, the output ends with `41 42 43 [07] 00 41 42 43`, and the repeat offset is 5 pointing to `[07]`.
The references for the next operation's literals are then `07 00 41 42 43 07 00 41 ...`,
so the literals `07 01 41 42 44 07` are stored as `00 01 00 00 01 00`.

The first operation of a block starts at position 0 with repeat offset 1, so its references are 0.

Other literals are stored as they are: those after the first 32 of an operation, those in chunks without the flag,
and those appended after the last operation.

## 5 Decoding

1. Read the header.
2. Read `L` from the end of the block, and locate the two sections.
3. Decode the literals.
4. Execute the operations of the LZ section in order. Operations take literals in order.
5. Append the literals that are left after the last operation.

The output must be exactly the decoded size.

A decoder must report an error for a block that breaks a rule of this document, except the no-overlap flag (4.3).
Appendix B lists the checks.
After an error, the output is undefined.

# STREAM FORMAT

SIMD blocks are stored in MinLZ SIMD block chunks (chunk type 0x10, SPEC.md 4.5B), without the indicator byte,
after an XXH3 64-bit hash (seed 0) of the decoded data. The stream identifier must have bit 6 set (SPEC.md 4.1).

Blocks carry no checksum of their own; the chunk's hash is the only integrity check.

# APPENDIX A: CLASS LAYOUTS

Class lengths (2.3) are stored in class order.

Inside a class, values are in ascending order.
A class of `n` values uses a complete code of `k` or `k + 1` bits, with `k = floor(log2 n)`:
the first `2^(k+1) - n` values get `k` bits, the rest `k + 1` bits. A class of one value adds no bits.

A class length plus the longest in-class length of the class must not exceed 11.

## A.1 Literal Lengths

11 classes: `1`, `2`, `3`, `4`, `5-6`, `7-8`, `9-12`, `13-16`, `17-24`, `25-32`, `33`.

```
Class, values 1-33:
 0,  1,  2,  3,  4,  4,  5,  5,  6,  6,  6,  6,  7,  7,  7,  7,
 8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,
10

In-class length, values 1-33:
 0,  0,  0,  0,  1,  1,  1,  1,  2,  2,  2,  2,  2,  2,  2,  2,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 0
```

## A.2 Escapes

16 classes: `0`, `1`, `2`, `3-4`, `5-6`, `7-10`, `11-14`, `15-22`, `23-30`, `31-46`, `47-62`,
`63-94`, `95-126`, `127-190`, `191-254`, `255`.

```
Class, values 0-255:
 0,  1,  2,  3,  3,  4,  4,  5,  5,  5,  5,  6,  6,  6,  6,  7,
 7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  9,
 9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9, 10,
10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11,
11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12,
12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13,
13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 14,
14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,
14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15

In-class length, values 0-255:
 0,  0,  0,  1,  1,  1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  4,
 4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
 4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  5,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  0
```

## A.3 Offset Symbols

24 classes, one per offset octave (4.4): class 0 is symbol 1, class 1 is symbols 2-3, class 2 is 4-7 and class 3 is 8-15.
Class `c` from 4 to 23 is symbols `8c - 16` to `8c - 9`, the 8 symbols of its octave.

```
Class, symbols 1-175:
 0,  1,  1,  2,  2,  2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  4,
 4,  4,  4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  6,
 6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  8,
 8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9, 10,
10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 12,
12, 12, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13, 13, 13, 13, 14,
14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 16,
16, 16, 16, 16, 16, 16, 16, 17, 17, 17, 17, 17, 17, 17, 17, 18,
18, 18, 18, 18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 20,
20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, 22,
22, 22, 22, 22, 22, 22, 22, 23, 23, 23, 23, 23, 23, 23, 23

In-class length, symbols 1-175:
 0,  1,  1,  2,  2,  2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3
```

## A.4 Tokens

Tokens use 22 lengths. A token's code length is the length of its match length group, plus its in-group length,
plus the length of its flags (token bits 0-1) in the flag code for its match length code:

| Lengths | Codes for                                                                                                            |
|---------|----------------------------------------------------------------------------------------------------------------------|
| 0-13    | Match length code groups `0`, `1-3`, `4`, `5`, `6`, `7`, `8`, `9-10`, `11-12`, `13-16`, `17-24`, `25-31`, `32`, `33` |
| 14-17   | Flags 0-3, for match length codes 0-31                                                                               |
| 18-21   | Flags 0-3, for match length codes 32-33                                                                              |

A token has no code if its group length or its flag length is 0.
The group lengths and each flag code must be complete codes. A token's code length must not exceed 11.

```
Class, match length codes 0-33:
 0,  1,  1,  1,  2,  3,  4,  5,  6,  7,  7,  8,  8,  9,  9,  9,
 9, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11,
12, 13

In-class length, match length codes 0-33:
 0,  1,  2,  2,  0,  0,  0,  0,  0,  1,  1,  1,  1,  2,  2,  2,
 2,  3,  3,  3,  3,  3,  3,  3,  3,  2,  3,  3,  3,  3,  3,  3,
 0,  0
```

## A.5 Literals

| Class | Bytes                              |
|-------|------------------------------------|
| 0     | `00`                               |
| 1     | `09`, `0A`, `0D`                   |
| 2     | Other bytes below `20`, and `7F`   |
| 3     | `20` (space)                       |
| 4     | `30`-`39` (digits)                 |
| 5     | `a e i n o r s t`                  |
| 6     | Other lowercase letters            |
| 7     | `41`-`5A` (uppercase letters)      |
| 8     | `" , - . / : _`                    |
| 9     | Other bytes `21`-`7E`              |
| 10    | `80`-`BF`                          |
| 11    | `C2`-`F4`                          |
| 12    | `C0`, `C1`, `F5`-`FF`              |

```
Class, bytes 0-255:
 0,  2,  2,  2,  2,  2,  2,  2,  2,  1,  1,  2,  2,  1,  2,  2,
 2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
 3,  9,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  8,  8,  8,  8,
 4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  8,  9,  9,  9,  9,  9,
 9,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  9,  9,  9,  9,  8,
 9,  5,  6,  6,  6,  5,  6,  6,  6,  5,  6,  6,  6,  6,  5,  5,
 6,  6,  5,  5,  5,  6,  6,  6,  6,  6,  6,  9,  9,  9,  9,  2,
10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
12, 12, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12

In-class length, bytes 0-255:
 0,  4,  4,  4,  5,  5,  5,  5,  5,  1,  2,  5,  5,  2,  5,  5,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 0,  4,  2,  4,  4,  4,  4,  4,  4,  5,  5,  5,  3,  3,  3,  3,
 3,  3,  3,  3,  3,  3,  4,  4,  4,  4,  3,  5,  5,  5,  5,  5,
 5,  4,  4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  3,
 5,  3,  4,  4,  4,  3,  4,  4,  4,  3,  4,  4,  4,  4,  3,  3,
 4,  4,  3,  3,  3,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 3,  3,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4
```

## A.6 Delta Literals

9 classes by the distance `d = min(b, 256 - b)` of the byte `b` from 0:
class 0 is `d = 0`, class `c` from 1 to 7 is `d` from `2^(c-1)` to `2^c - 1` (both signs), and class 8 is `d = 128`.
For example, class 1 is `01` and `FF`, and class 2 is `02`, `03`, `FD` and `FE`.

```
Class, bytes 0-255:
 0,  1,  2,  2,  3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 8,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 5,  4,  4,  4,  4,  4,  4,  4,  4,  3,  3,  3,  3,  2,  2,  1

In-class length, bytes 0-255:
 0,  1,  2,  2,  3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,
 5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 0,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
 7,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
 6,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
 5,  4,  4,  4,  4,  4,  4,  4,  4,  3,  3,  3,  3,  2,  2,  1
```

# APPENDIX B: VALIDATION

A decoder must report an error for every block listed below. After an error, the output is undefined.
Whatever the input, a decoder must not read or write outside its buffers.

Offsets larger than the current position are not errors; they are clamped (4.3).
The no-overlap flag (4.1) need not be checked (4.3).

## B.1 Header

* A reserved block type, or a header varint above 32 bits.
* A decoded size above 8 MiB.
* An encoded block that is not at least 64 bytes smaller than the decoded size.
* Header flag bits that must be 0 are set, or a chunk limit value of 6 or 7.

## B.2 Framing

* A varint or reverse varint that runs past its bytes.
* `L` larger than the bytes before it, or an `R` larger than the rest of its section.
* Records that do not end exactly at their `R`.
* Entry sizes that do not add up exactly to the data before the records.
* A literal section without literal blocks, a literal block without literals, or one with more than 65535.
* A chunk without operations, or with more than the chunk limit.
* More than 128 chunks.
* Chunk flag bits that must be 0 are set.

## B.3 Entries and Huffman Tables

* Modes 5-7, mode 4 outside the literals, or offset bits in a mode other than 0.
* Mode 1 without a current table.
* A table with a code length above 11, with an incomplete code (other than a single value of length 1),
  or with codes for values outside the stream's range.
* Class lengths whose padding nibble (2.3) is not 0.
* Token class lengths whose group lengths or flag codes are not complete codes (A.4).
* A number of values of tokens or of a literal block (2.2) that runs past the stored size.
* A pivco block that does not decode to exactly the expected number of values using exactly the rest of the stored size (PIVCO_SPEC.md 6),
  including one for a stream without values or with more than 65535.
* Uncompressed values outside the stream's range.

## B.4 Chunks

Checked at the end of each chunk:

* Literal lengths, offset symbols or escapes that do not hold exactly the values the chunk's tokens read (4.2).
* Offset bits whose size is not the number of bits the chunk reads, rounded up to whole bytes.
  Unused bits in the last byte are ignored.
* A match longer than 32 bytes with an offset below 32, after clamping.
* A chunk that outputs fewer bytes than it has operations.

## B.5 Block

* A block whose first operation has no literals.
* Operations that read more literals than the literal section holds.
* Output that does not end exactly at the decoded size.

## B.6 Implementation Note

A decoder can avoid checks per operation by checking each chunk before running it:

* Count what the chunk's tokens read: literal lengths, offset symbols and escapes, and the raw bits of the offset symbols.
  Streams that do not hold exactly that are errors (B.4).
* Add up the match length codes, literal lengths and escapes. This is the chunk's exact output.
  A total past the decoded size, or below the number of operations, is an error (B.4, B.5).

Every operation then stays inside these buffers, whatever the block holds:

| Buffer                                  | Size                                                     |
|-----------------------------------------|----------------------------------------------------------|
| Tokens, literal lengths, offset symbols | The chunk limit                                          |
| Escapes                                 | Twice the chunk limit: an operation can have two         |
| Literals                                | The decoded size, which the literals may not fill        |
| Output                                  | The decoded size                                         |

A decoder that copies 32 bytes at a time needs 32 readable bytes after each buffer it reads,
and 32 bytes after each operation it writes. The last operations of a block can copy exactly instead.

* An operation never reads more literals than it outputs, so literal reads stay below the decoded size plus 32 bytes.
  Whether they stay inside the literals the section holds is checked once, at the end of the block (B.5).
* Offsets only change what a match copies, not how much. Offset bits can be read in place:
  at least 8 bytes of the block follow the last chunk's offset bits.
* Clamping (4.3) costs nothing extra when the clamped offset, `min(offset, position)`, is used for the source,
  the overlap test and the pattern.
* The first operation's delta references are 0 (4.5). Setting the first output byte to 0, and treating offset 0 like offset 1,
  gives that without a special case.
