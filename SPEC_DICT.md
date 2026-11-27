# MINLZ DICTIONARY SPECIFICATION

This document adds dictionaries to the [MinLZ specification](SPEC.md) and the [SIMD block specification](SPEC_SIMD.md),
in version 1.2 of the format. Anything not described here is as defined there.

A dictionary is data that blocks start from:

* *Content*: up to 128 KiB that blocks copy from as if it had been decoded just before them.
* *Dictionary tables*: Huffman tables that SIMD blocks select instead of coding their own.
* A *phrase book*: up to 65535 *entries* of up to 64 bytes each, which blocks copy whole by number.

Each of them may be empty, and the tables and the phrase book may be absent.

Blocks that use a dictionary have block types of their own (2).
A stream embeds its dictionary, or names it by its ID, right after the stream identifier (3.1, 3.2).

# DICTIONARY FORMAT

## 1 Layout

    dictionary:   | ID | flags | tables | phrase book | content |

    tables:       | uvarint size | counts | table | table | ... |    if flags bit 0 is set
    phrase book:  | uvarint size | block |                          if flags bit 1 is set
    content:      | block |                                         always

| Flags bits | Meaning                         |
|------------|---------------------------------|
| 0          | Dictionary tables follow (1.2)  |
| 1          | A phrase book follows (1.3)     |
| 2-7        | Reserved, must be 0             |

The ID is 8 bytes, and the flags 1 byte.
The size before the tables and before the phrase book is the number of bytes of the section after the uvarint.
Uvarints are as in SPEC_SIMD.md 2.1: at most 10 bytes, and not necessarily minimal.
The data of a section must end exactly at its size.

The content is the rest of the dictionary, and is always present (1.4).
So the smallest dictionary is 10 bytes: the ID, a flags byte of 0, and an empty content block.

The tables, the phrase book and the content are the sections of the dictionary.
A dictionary is valid only if all its sections are (A.1).
A block of type 2-5 uses the content, a block of type 4 or 5 the phrase book, and a block that selects a dictionary table (2.3) the tables.

A decoder may decode a section when a block first uses it: it must report an invalid section before a block uses it,
and may leave a section that no block uses unchecked. A reader can skip the tables and the phrase book by their size, without decoding them.

### 1.1 ID

The ID is the XXH3 64-bit hash (seed 0) of everything after it, as stored, in 8 bytes little endian.
A reader checks it before it decodes anything else of the dictionary.

The ID names the dictionary in streams (3.2). Dictionaries with the same ID are taken to be the same dictionary.
As it hashes the stored bytes, the same dictionary stored differently has another ID.

### 1.2 Dictionary Tables

    tables:  | uvarint size | counts | table | table | ... |

The counts are 5 bytes: the number of dictionary tables of the tokens, literal lengths, offset symbols, escapes and literals,
in this order. Each count is 0-4, and 0-3 for the literals.

The tables follow in the same order, stream by stream. The n-th table of a stream, counting from 0, is its dictionary table n (2.3).
A table is a mode byte, followed by the table as a chunk or literal record stores it ([SPEC_SIMD.md](SPEC_SIMD.md) 2.3, 3, 4.1):

| Mode | Table                                                 | Streams       |
|------|-------------------------------------------------------|---------------|
| 2    | Code lengths                                          | All           |
| 3    | Class lengths                                         | All           |
| 4    | Class lengths of the delta layout (SPEC_SIMD.md A.6)  | Literals only |

Token tables have the range of type 4 blocks (2.4): 100 bytes of code lengths, or 15 bytes of class lengths.
The tables of the other streams have the sizes of SPEC_SIMD.md 3 and 4.2.

Non SIMD MinLZ blocks ignore the dictionary tables.

### 1.3 Phrase Book

    phrase book:  | uvarint size | block |

The block decodes to:

    | count | lengths | bytes |

| Part    | Size               | Meaning                                      |
|---------|--------------------|----------------------------------------------|
| count   | 2 bytes            | Number of entries, little endian (0-65535)   |
| lengths | 1 byte per entry   | Each entry's length, in entry order (0-64)   |
| bytes   | Sum of the lengths | The entries' bytes, in entry order           |

The block decodes to exactly `2 + count + the sum of the lengths` bytes, at most 4,259,777.

Entries are numbered from 0. Entry `i` is the `lengths[i]` bytes that start after the sum of the lengths before it.
An entry of length 0 copies nothing, and so does an entry number of `count` or above.

Blocks of type 4 and 5 copy entries (2.4, 2.5). A dictionary with flags bit 1 set has a phrase book, also with a count of 0.

### 1.4 Content

The content is a block that decodes to at most 128 KiB (131072 bytes), or to nothing.
An empty content is a block that decodes to nothing, such as the single byte 0, an empty MinLZ block (SPEC.md 1.0).

### 1.5 Stored Blocks

The phrase book and the content are each stored as a MinLZ block (type 0, SPEC.md) or a SIMD block (type 1, SPEC_SIMD.md),
starting with its indicator byte, which must be 0. They must not use a dictionary.
The SIMD flag of the stream identifier (SPEC.md 4.1) does not apply to them.

A MinLZ block with decoded size 0 holds its bytes as they are (SPEC.md 1.1).

# BLOCKS WITH A DICTIONARY

## 2 Block Types

The block type in the header varint (SPEC.md 1.1, SPEC_SIMD.md 1.0) says whether a block uses a dictionary:

| Type  | Block                                                   |
|-------|---------------------------------------------------------|
| 0     | MinLZ block (SPEC.md)                                   |
| 1     | SIMD block (SPEC_SIMD.md)                               |
| 2     | SIMD block that uses a dictionary                       |
| 3     | MinLZ block that uses a dictionary                      |
| 4     | SIMD block that uses a dictionary and its phrase book   |
| 5     | MinLZ block that uses a dictionary and its phrase book  |
| 6-255 | Reserved                                                |

A decoder must reject a block of type 2-5 if it has no dictionary,
and a block of type 4 or 5 if the dictionary has no phrase book.

Blocks do not identify their dictionary: the caller passes the one they were encoded with.
A block decoded with a different dictionary is not detected. An application can store the dictionary's ID (1.1) with its blocks to check it.

### 2.1 Decoding

A block of type 2 or 4 decodes as a SIMD block, and a block of type 3 or 5 as a MinLZ block, with these changes:

* Before the block, the output holds the content of the dictionary. Its first byte is position 0,
  and the first byte of the block follows its last byte. All rules that use positions count from there:
  * Copies may reach into the content. A MinLZ copy or repeat must not reach before position 0 (SPEC.md 2.2).
    A SIMD offset larger than the position is clamped to it (SPEC_SIMD.md 4.3).
  * Delta references (SPEC_SIMD.md 4.5) and the no-overlap rules (SPEC_SIMD.md 4.1, 4.3) use these positions.
    So the delta references of the first operation are bytes of the content; with an empty content, they are 0.
* The repeat offset starts at 1.
* SIMD blocks start without current tables, and their records may select dictionary tables (2.3).
* The first operation of a SIMD block may have no literals.
  A match of 1 byte or more must not start at position 0, which an empty content and a first operation without literals allow:
  there every offset clamps to 0. Entry operations (2.4) are not matches, and may start at position 0.
* A SIMD block may have no literal blocks, if its operations output all of it. Its literal section is then only its `R`, 0.

The output of the block is what follows the content.

### 2.2 Size Limits

The decoded size in the header is the size of the block alone. It is at least 1:
the form of SPEC.md 1.1 with decoded size 0, which holds the bytes as they are, is only for type 0 blocks.
The content plus the decoded size must not exceed 8 MiB.

The other limits count only the block: the chunk and literal block limits (SPEC_SIMD.md 1.1),
the rule that a SIMD block is at least 64 bytes smaller than its decoded size (SPEC_SIMD.md 1.1),
and the rule that a MinLZ block is not bigger than its decoded size (SPEC.md 1.1).

### 2.3 Dictionary Tables

In blocks of type 2 and 4, the modes of record entries (SPEC_SIMD.md 2.2) that are invalid in type 1 blocks select a dictionary table:

| Mode | Chunk records (SPEC_SIMD.md 4.1)     | Literal records (SPEC_SIMD.md 3)               |
|------|--------------------------------------|------------------------------------------------|
| 4    | Dictionary table 0 of the stream     | Delta class lengths, as in type 1 blocks       |
| 5-7  | Dictionary tables 1-3 of the stream  | Dictionary tables 0-2 of the literals          |

* No table follows the varint, and the stored data is as for mode 1.
* The dictionary must have the table selected.
* The record entry of the offset bits keeps mode 0.
* A dictionary table does not become the current table. Mode 1 uses the last table that the block set itself,
  with modes 2-4 in literal records and 2-3 in chunk records.
* A block of type 2 must not select a token table with codes for tokens 136-199.

### 2.4 Entries in SIMD Blocks

In blocks of type 4, match length codes 34-49 are *entry codes*: the operation copies a whole entry of the phrase book (1.3)
instead of a match. Tokens range 0-199.

| Token bits | Meaning, for entry codes                                                      |
|------------|-------------------------------------------------------------------------------|
| 0          | Reserved, must be 0                                                           |
| 1          | Literals                                                                      |
| 2-7        | Entry code 34 + n: the entry number is `2^n - 1` plus n raw bits (n = 0-15)   |

A token with an entry code and bit 0 set is invalid. Entry numbers range 0-65534.

An operation with an entry code:

1. With literals: copies as many literals as the next literal length.
2. Reads the n raw bits of its entry number from the offset bits, in operation order, as an offset reads them (SPEC_SIMD.md 4.4).
   It reads no offset symbol and no match length escape.
3. Copies the bytes of the entry: nothing for an entry of length 0, or an entry number of the count or above (1.3).

The repeat offset does not change.
In a chunk with the delta flag, the literals of an entry operation are stored as differences like those of other operations,
with the repeat offset before the operation (SPEC_SIMD.md 4.5).

The streams of a chunk (SPEC_SIMD.md 4.1) hold, in type 4 blocks:

| Stream          | Number of values                                                                 |
|-----------------|----------------------------------------------------------------------------------|
| Tokens          | One per operation                                                                |
| Literal lengths | One per token with the literals bit                                              |
| Offset symbols  | One per token without the repeat bit, other than tokens with entry codes         |
| Escapes         | One per literal length 33, and one per token with match length code 33           |
| Offset bits     | The raw bits of the offset symbols and of the entry numbers, rounded up to whole bytes |

An operation with an entry code and no literals may output nothing. A chunk must still output at least as many bytes
as it has operations, counting every operation (SPEC_SIMD.md 4.2). As the tokens don't give the lengths of entries,
a decoder can only check this after it runs the chunk.

Token tables of type 4 blocks have the range 0-199: code lengths take 100 bytes, and class lengths 15 bytes, 30 lengths:

| Lengths | Codes for                                                                                    |
|---------|----------------------------------------------------------------------------------------------|
| 0-13    | Match length code groups, as in SPEC_SIMD.md A.4                                             |
| 14-17   | Entry code groups `34-37`, `38-41`, `42-45`, `46-49`, each with an in-group length of 2      |
| 18-21   | Flags 0-3, for match length codes 0-31                                                       |
| 22-25   | Flags 0-3, for match length codes 32-33                                                      |
| 26-29   | Flags 0-3, for entry codes                                                                   |

A token's code length is its group length, plus its in-group length, plus its flag length.
A token has no code if its group length or its flag length is 0.
The 18 group lengths and each of the three flag codes must be complete codes.
Tokens with an entry code and bit 0 set have no code: in code lengths their lengths are 0, and in class lengths
lengths 27 and 29 (flags 1 and 3 for entry codes) are 0.

Blocks of type 2 keep the token range 0-135 and the table sizes of SPEC_SIMD.md 4.2.

### 2.5 Entries in MinLZ Blocks

A block of type 5 decodes as a block of type 3, except for repeats: tag 00 with bit 2 set (SPEC.md 2.1).
Literals are unchanged. The length value of a repeat means:

| Value | Type 5                                                                                              |
|-------|-----------------------------------------------------------------------------------------------------|
| 0-24  | Repeat of 1 + value bytes                                                                           |
| 25    | Repeat of 26 + read 1 byte                                                                          |
| 26    | Repeat of 26 + read 2 bytes                                                                         |
| 27    | Repeat of 26 + read 3 bytes                                                                         |
| 28    | Entry with literals: read 1 byte. Bits 0-5 are the entry number (0-63), bits 6-7 literals - 1 (1-4) |
| 29    | Entry: read 1 byte, the entry number (0-255)                                                        |
| 30    | Entry with literals: read 2 bytes. Bits 0-13 are the entry number (0-16383), bits 14-15 literals - 1 (1-4) |
| 31    | Entry: read 2 bytes, the entry number (0-65535)                                                     |

* Values that read 2 or 3 bytes read them little endian.
* The literals of values 28 and 30 follow the bytes read, and are output before the entry, as fused literals (SPEC.md 2.2.1).
* An entry operation copies the bytes of the entry (1.3), and does not change the repeat offset.

# STREAM FORMAT

## 3.1 Stream Identifier

The dictionary flag is bit 0 of the first extension byte of the stream identifier (SPEC.md 4.1.2).
It is set if and only if a dictionary chunk (3.2) directly follows the identifier.
A sidecar has the stream identifiers of its main stream (SPEC_SEARCH.md 1.1), and references the dictionary chunk instead (3.5).

## 3.2 Dictionary Chunks (chunk types 0x21 and 0x22)

| Chunk                     | Data                                      |
|---------------------------|-------------------------------------------|
| 0x21, dictionary          | The dictionary (1)                        |
| 0x22, dictionary ID       | The ID of the dictionary (1.1), 8 bytes   |

Both are non-skippable. A dictionary chunk directly follows a stream identifier with the dictionary flag, and appears nowhere else.
Other chunks that SPEC_SEARCH.md 2.0 places after the identifier, such as 0x44, follow the dictionary chunk.

The dictionary applies to the blocks of the stream, up to its EOF chunk.
Blocks after the EOF chunk and before the next stream identifier have no dictionary.
A stream identifier after the EOF chunk starts a new stream, with its own dictionary or none.
Dictionary chunks output nothing, and the size in the EOF chunk (SPEC.md 4.6) does not count the content.

A decoder keeps a list of up to 8 dictionaries embedded in 0x21 chunks, by ID, most recently used first.
The list starts empty where the decoder starts reading, and carries over from one stream to the next:

* A 0x21 chunk puts its dictionary's ID first, and removes it from a later place in the list.
* A 0x22 chunk whose ID is in the list moves it to the first place.
* When the list holds 9 IDs, the last one is removed.

A 0x22 chunk resolves to the dictionary in the list with its ID, or, if the list has none, to the dictionary with that ID
that the application gives the decoder.
A writer must not name a dictionary in a 0x22 chunk that a 0x21 chunk embedded earlier in the file, but that is no longer in the list.

If neither the list nor the application has the ID, the dictionary is missing: the decoder stops at the 0x22 chunk,
and reports the dictionary as missing, not the stream as corrupt.

A decoder that starts at an index entry resolves 0x22 chunks as 3.4 says.

## 3.3 Blocks

Blocks of type 2 and 4 are stored in chunk 0x10 (SPEC.md 4.5B).
Blocks of type 3 and 5 are stored in chunks 0x02 and 0x03 (SPEC.md 4.4, 4.5).
Only streams with a dictionary may hold blocks of type 2-5. They may also hold blocks of type 0 and 1.
As with type 1 blocks, a stream with blocks of type 2 or 4 must have the SIMD flag set (SPEC.md 4.5B).

The checksum of chunk 0x02 and the hash of chunk 0x10 are of the block's output, without the content.
Chunk 0x03 keeps its checksum of the compressed block (SPEC.md 4.5).

The content plus the decoded size of a block of type 2-5 must not exceed the maximum block size of the stream identifier (SPEC.md 4.1.1),
and decoders must reject a block that does. So no block can use a content as large as the maximum block size.
The maximum block size does not limit the phrase book, nor the dictionary chunk.

## 3.4 Index (chunk type 0x41)

An index (SPEC.md 4.12) that covers a stream with a dictionary is stored in chunk 0x41, which is skippable and adds
the dictionary of each entry. Other indexes stay 0x40. A 0x40 index must not cover a stream with a dictionary.

| Content                                        | Format                                                                                     |
|------------------------------------------------|--------------------------------------------------------------------------------------------|
| ID, `[1]byte`                                  | Always 0x41                                                                                |
| Data Length, `[3]byte`                         | As in 0x40                                                                                 |
| Header `[6]byte`                               | "mZIDx\x00"                                                                                |
| UncompressedSize ... CompressedOffsets         | As in 0x40                                                                                 |
| Dictionaries, Varint                           | The number of dictionaries, 0-65535: every dictionary that a stream before the index uses   |
| Per dictionary: ID `[8]byte`, Position Varint  | Its ID (1.1), and the compressed offset of the first byte of a 0x21 chunk that holds it, counted as the compressed offsets of the entries are, or 0 if none does |
| DictionaryNumbers, `[Entries]Varint`           | For each entry, the dictionary of the stream that holds the first block at or after it: 0 for none, or n (1-Dictionaries) for the n-th dictionary of the list |
| Block Size, `[4]byte`                          | As in 0x40                                                                                 |
| Trailer `[6]byte`                              | "\x00xDIZm"                                                                                |

Varints are signed zigzag varints, as in 0x40 (SPEC.md 4.12): a count of 1 dictionary is stored as `02`.
None of the new values is negative.
An entry has the dictionary of its stream also when the first block at or after it is of type 0 or 1.
Entries point to blocks as in 0x40, never to a dictionary chunk.

To decode from an entry, a decoder reads the 0x21 chunk at the position of the entry's dictionary, if it has one,
and checks that it holds a dictionary with that ID; otherwise it takes the dictionary with that ID from the application.
If neither has it, the decoder reports the dictionary as missing, and does not decode from the entry.
It then decodes as SPEC.md 4.12 says. When it reads a stream identifier, the dictionary chunk after it applies from there on.

It keeps the list of 3.2 for the 0x21 chunks it reads, starting empty at the entry.
A 0x22 chunk resolves to the dictionary in the list, then to the dictionary of the index with a position, then to the application's.
Such a decoder can't check the writer rule of 3.2 for dictionaries embedded before the entry.

A decoder that starts at an entry of a 0x40 index has no dictionary until it reads a stream identifier, so it rejects blocks of type 2-5 before that.

## 3.5 Sidecar Search Streams

A sidecar (SPEC_SEARCH.md 1.1) of a stream with a dictionary references the main stream's dictionary chunk (0x21 or 0x22)
as it references blocks, with a Remote Block Reference (chunk 0x47, SPEC_SEARCH.md 2.3):

* The first 0x47 chunk after a stream identifier with the dictionary flag starts with the reference to the dictionary chunk.
  Only a 0x44 chunk may come between the identifier and that 0x47 chunk.
  The 0x47 chunk may go on with references to the blocks after the dictionary chunk.
* A dictionary chunk outputs nothing, so the size field of its reference (the maximum block size minus the block size)
  is the maximum block size.
* A searcher resolves a 0x22 chunk as a decoder does (3.2), with the 0x21 chunks that the sidecar references as the embedded dictionaries.

Search tables (chunks 0x44-0x46) cover the output of a block, which doesn't include the content.

# APPENDIX A: VALIDATION

A decoder must report an error for each case listed below, in addition to those of SPEC.md and SPEC_SIMD.md Appendix B.

## A.1 Dictionary

A decoder checks the ID, the flags and the section sizes when it reads a dictionary,
and each section at the latest before a block uses it (1).

* A dictionary that ends before its content block.
* An ID that is not the hash of the rest of the dictionary (1.1).
* Reserved flag bits that are set.
* A section size longer than 10 bytes, above 64 bits, or past the end of the dictionary, or a section whose data does not end exactly at its size.
* A count above 4, or above 3 for the literals.
* A table mode other than 2 or 3, or 2-4 for the literals.
* A table that breaks the table rules of SPEC_SIMD.md B.3, with the token layout of 2.4.
* A phrase book or content that is not a valid block of type 0 or 1 (1.5).
* A phrase book that doesn't decode to exactly its count, lengths and bytes, or with a length above 64.
* A content that decodes to more than 128 KiB.

## A.2 Blocks

* A block of type 2-5 without a dictionary, or a block of type 4 or 5 whose dictionary has no phrase book.
* A block of type 2-5 with a decoded size of 0.
* The content plus the decoded size above 8 MiB.
* In types 2 and 4: a mode that selects a dictionary table the dictionary doesn't have, or a mode other than 0 for the offset bits.
* In type 2: a selected token table with codes for tokens 136-199.
* In types 2 and 4: a match of 1 byte or more that starts at position 0.
* In type 4: a token above 199, or a token with an entry code and bit 0 set; a token table that gives such a token a code.
* In types 3 and 5: a copy or repeat whose offset is larger than the position, counted from the start of the content.

These checks of SPEC_SIMD.md Appendix B do not apply with a dictionary:

* Modes 5-7, and mode 4 outside the literals (B.3), in types 2 and 4 when they select a table the dictionary has.
* Codes for tokens above 135, and tokens above 135 (B.3), in type 4, where tokens range 0-199.
* A literal section without literal blocks (B.2), in types 2 and 4.
* A block whose first operation has no literals (B.5), in types 2 and 4.

In type 4, B.4's check that a chunk outputs at least as many bytes as it has operations still applies,
but can only be made after the chunk runs (2.4).

## A.3 Streams

* In a stream that is not a sidecar: an identifier with the dictionary flag that is not directly followed by a dictionary chunk,
  or a dictionary chunk anywhere else, including at an index entry.
* A 0x21 chunk that does not hold a valid dictionary, checked as A.1 says, or a 0x22 chunk whose length is not 8.
* A block of type 2-5 in a stream without a dictionary, a block of type 2 or 4 in a chunk other than 0x10,
  or a block of type 3 or 5 in a chunk other than 0x02 or 0x03.
* The content plus the decoded size of a block above the maximum block size of the stream identifier.
* In a 0x41 index: a wrong header or trailer, more than 65535 dictionaries, a negative value, or a dictionary number above
  the number of dictionaries. When a decoder uses a position: no 0x21 chunk there, or one whose dictionary has another ID.
* In a sidecar with the dictionary flag: a first 0x47 chunk that doesn't start with the reference to the dictionary chunk,
  or a chunk other than 0x44 before it.

A 0x22 chunk that resolves to no dictionary is reported as a missing dictionary (3.2), not as an error in the stream.

# APPENDIX B: IMPLEMENTATION NOTES

These notes are not part of the format.

* A decoder can keep the content at the start of its output buffer, and decode each block after it.
  The content is never written by blocks, so it is copied there once.
* The bytes of the phrase book can be used in place. One pass over the lengths gives each entry's position.
  With 64 readable bytes after the entries, an entry copies as two 32-byte copies.
  Its output then needs 64 bytes of slack, or an exact copy near the end of the block.
* An entry operation outputs its literals plus 0-64 bytes, so the tokens of a chunk bound its output from above,
  at 64 bytes per entry operation; the checks of SPEC_SIMD.md B.6 can use that bound.
  Whether the chunk output at least one byte per operation is known after it runs.
* The trick of SPEC_SIMD.md B.6 for the delta references of the first operation, a 0 at position 0,
  only works with an empty content (2.1).
* Blocks of type 5 differ from type 3 only in the repeat path of tag 00, so the block type selects the decode loop before decoding.
* The safe fused literals of SPEC.md (Implementation Notes) rely on a match of at least 4 bytes after the literals.
  An entry can be shorter or empty, so the literals of type 5 values 28 and 30 need an exact copy, or slack after the output.
