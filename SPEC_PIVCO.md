# PIVCO BLOCKS SPECIFICATION

A pivco block codes 1 to 65535 byte values with a Huffman code, in a form that decodes with SIMD.
MinLZ SIMD blocks use pivco blocks for their entropy coded streams ([SPEC_SIMD.md](SPEC_SIMD.md) 2.2).

A pivco block does not store a code per value.
It stores a *bitmap* per node of the Huffman tree, with one bit per value that passes through the node:
whether the value continues to the left or to the right child.
Subtrees whose leaves all have the same depth store fixed size codes instead.
A decoder rebuilds the values bottom-up: each node merges the values of its two children, as its bitmap tells.

## Origin

Pivco blocks are the PH blocks of [pivco-huffman](https://github.com/MarcinZukowski/pivco-huffman) by Marcin Zukowski,
at commit `07d9efc69eef1dfbb0c683d98259319c80135d79`.
This document specifies the subset that MinLZ uses: tables given as code lengths, uncoded bitmaps,
and flat regions in upstream's `FlatVertical` layout. Upstream's entropy coded bitmaps and its container are not part of it.

A pivco block differs from an upstream PH block in three ways:

1. It has no marker prefix: upstream's list of entropy coded bitmaps, which this subset does not have.
2. Its flat codes (3.3) have no marker byte. Upstream stores a byte before them that selects their entropy coding; this subset only has uncoded flat codes, which upstream marks `00`.
3. It does not store its number of values. The container provides it.

Otherwise, the regions of a pivco block (3) are byte for byte those of the upstream block.

# BLOCK FORMAT

## 1 Inputs

### 1.1 Values

A block codes `n` values, each a byte (0-255), with `n` from 1 to 65535.

The block stores neither `n` nor its own size: the container provides both.
MinLZ stores `n` for some streams and derives it for others (SPEC_SIMD.md 2.2).
A decoder must know both before it decodes the block. A block can be valid for more than one `n`.

### 1.2 Huffman Table

A block is coded with a Huffman table: a code length of 0 to 11 for each of the 256 values.
Length 0 means the value has no code. Values with a code are *coded*.

The table must either:

* Form a complete code: the sum of `2^(11 - length)` over the coded values is exactly `2^11`.
* Or code exactly one value. Its length does not change the blocks. MinLZ requires length 1 (SPEC_SIMD.md 2.3).

Every value of the input must be coded.
How an encoder chooses the code lengths is not part of this format.

## 2 Coding Tree

The table defines a binary tree, the same for encoder and decoder.
Each coded value is a leaf, at the depth of its code length.
This section builds the tree of a complete code. A table that codes one value is in 2.5.

### 2.1 Groups

For each length `L` from 1 to 11, list the values with length `L` in ascending order. Let `c` be their number.
The list is split into *groups* by the set bits of `c`, highest bit first: for each set bit `b`, the next `2^b` values form a group.

A group has `2^b` values of length `L`, and *depth* `L - b`.
It is a node at that depth, with its values as the leaves of the complete subtree of height `b` below it.

For example, 6 values of length 3 form a group of 4 values at depth 1, and a group of 2 values at depth 2.

### 2.2 Ranks and Paths

The groups are sorted by depth, then by length, both ascending. No two groups have the same depth and length.

In this order, each group gets a *path* of as many bits as its depth.
The first group's path is all zeros.
Each next path is `(p + 1) << (depth - p_depth)`, with `p` the path of the previous group and `p_depth` its depth.
Read from its most significant bit, a path leads from the root to the group's node: 0 is the left child, 1 is the right child.
The path of a group at depth 0 is empty: the group is the whole tree.

A group of `2^b` values of length `L` weighs as much as one code of length `L - b`,
so the depths of the groups form a complete code too, and the paths are its canonical codes.
Hence no depth is negative, every path fits in as many bits as its depth, and the group nodes are the leaves of a full binary tree.

The *rank* of a value is its position when the groups are concatenated in this order, each group's values in ascending order.
Rank 0 is the first value of the first group.
Ranks number the leaves from left to right, so the values below any node have consecutive ranks.

Ranks follow the groups, not the code lengths: a value can have a shorter code than a value of a lower rank (A.1).

### 2.3 Node Kinds

The nodes on the paths to the group nodes, other than the group nodes, are *internal* nodes. Each has two children (2.2).

| Kind      | Node                                                    | Stores (3)             |
|-----------|---------------------------------------------------------|------------------------|
| FULL      | An internal node whose left child is not a Leaf node    | Right count and bitmap |
| LEAF_LEFT | An internal node whose left child is a Leaf node        | Right count and bitmap |
| BOTH      | A group of 2 values                                     | Bitmap                 |
| FLAT      | A group of `2^D` values, `D` from 2 to 8                | Flat codes             |
| Leaf      | A group of 1 value: the leaf of the value itself        | Nothing                |

The *flat depth* `D` of a FLAT node is the `b` of its group (2.1): the height of its subtree, not its depth.

The right child of an internal node is never a Leaf node: a group of 1 value of length `L` has depth `L`,
so it is the first group of its depth, and its path ends with a 0 bit (2.2).

A FLAT node with `D` = 8 is a group of all 256 values, which all have length 8. It is the root.

The *split* of an internal node is the highest rank of its left subtree. The split of a BOTH node is its first rank.
A value *goes right* at an internal or BOTH node when its rank is above the split. Otherwise it goes left.

### 2.4 Records

The nodes other than Leaf nodes are *records*. A tree has at most 255 records.

Records are numbered in pre-order: a record, then the records of its left subtree, then those of its right subtree.
The root is record 0. The left child of a FULL record is the next record, and so is the right child of a LEAF_LEFT record.

Record numbers do not appear in blocks. This document uses them to name nodes.

### 2.5 One Coded Value

When the table codes a single value `v`, the tree is one BOTH node whose two leaves are both `v`.
`v` has rank 0, and ranks 0 and 1 both decode to `v`.
Every value goes left, so a block of `n` values is `ceil(n / 8)` zero bytes.

## 3 Block Layout

### 3.1 Regions

A node *holds* the values that pass through it: the input values whose leaves are in its subtree, in input order.
The root holds all `n` values. `K` is the number of values a node holds.

A node that holds no values stores nothing, and the nodes below it store nothing either.
A node with `K` from 1 to 65535 stores its subtree as:

| Kind      | Subtree                                                                           |
|-----------|-----------------------------------------------------------------------------------|
| FULL      | Right count, subtree of the first child, subtree of the second child, bitmap      |
| LEAF_LEFT | Right count, subtree of the right child, bitmap                                   |
| BOTH      | Bitmap                                                                            |
| FLAT      | Flat codes                                                                        |

The block is the subtree of the root. It has no header, and nothing between or after its regions.

The *right count* (K_right) is the number of the node's values that go right, stored as 2 bytes little endian. It is at most `K`.

The *first child* of a FULL node is the child that holds more values. When both hold the same number, it is the left child.

The left child of a LEAF_LEFT node is a Leaf node, which stores nothing: the `K - K_right` values that go left are all its value.

So the right counts are in pre-order and the bitmaps in post-order, of a walk that visits the first child first.
The regions of each subtree are contiguous.

### 3.2 Bitmaps

A bitmap holds one bit per value of its node, in order.
Bit `j` is 1 when value `j` goes right (2.3).
In a BOTH node, it is 1 when value `j` is the group's second value, the higher of its two values.

Bit `j` is bit `j & 7` of byte `j >> 3`.
A bitmap takes `ceil(K / 8)` bytes. The unused high bits of its last byte must be 0.

### 3.3 Flat Codes

A FLAT node of flat depth `D` stores a `D`-bit code per value it holds:
the value's index in its group, which is its rank minus the group's first rank.

The `K` codes take `ceil(K * D / 8)` bytes. With `D` from 2 to 7, they are in 3 parts that follow each other:

| Part | Codes                                            | Layout                              |
|------|--------------------------------------------------|-------------------------------------|
| 1    | The first `N512 = 512 * floor(K / 512)`          | 64-lane blocks of 512 codes         |
| 2    | The next `N128 = 128 * floor((K - N512) / 128)`  | 16-lane blocks of 128 codes         |
| 3    | The rest, `K - N512 - N128` (0 to 127)           | Natural                             |

This is upstream's `FlatVertical` layout.
With `D` = 8, all `K` codes are natural. The 3 parts would give the same bytes: a W-lane block of 8-bit codes holds its codes in natural order.

A *W-lane block* holds `8 * W` codes in `W * D` bytes. Number the codes of the block `c[0]` to `c[8W - 1]`.
Lane `l`, from 0 to `W - 1`, holds 8 of them as one number of `8 * D` bits:

    lane(l) = c[l] | c[W + l] << D | c[2W + l] << 2D | ... | c[7W + l] << 7D

Byte `i` of the block, from 0 to `W * D - 1`, is bits `8 * floor(i / W)` to `8 * floor(i / W) + 7` of lane `i mod W`.
So the first `W` bytes hold the low 8 bits of every lane, the next `W` bytes the next 8 bits, and so on.

The natural codes are one bit string, least significant bit first.
Bit `m` of code `i` of the part is bit `i * D + m` of the string, and bit `k` of the string is bit `k & 7` of byte `k >> 3`.
The unused high bits of its last byte must be 0.

Parts 1 and 2 are whole bytes, so only the last byte of a flat codes region can have unused bits.

With `D` = 8, the group is all 256 values in order (2.3), so the codes are the values themselves.

## 4 Block Size

The size of a block depends only on how many values of each rank it holds, not on their order.
With `K` the number of values a node holds, the size of its subtree is 0 when `K` is 0, and otherwise:

| Kind      | Size of the subtree                                               |
|-----------|-------------------------------------------------------------------|
| FULL      | `2 + ceil(K / 8)` + size of the left + size of the right subtree  |
| LEAF_LEFT | `2 + ceil(K / 8)` + size of the right subtree                     |
| BOTH      | `ceil(K / 8)`                                                     |
| FLAT      | `ceil(K * D / 8)`                                                 |

The size of a block is the size of the subtree of the root.
An encoder can compute it from a histogram of the values, before it codes them.

## 5 Decoding

A decoder reads the regions in the order they are stored (3.1), starting with the root holding `n` values.
For a node holding `K` values:

* FLAT: read the `ceil(K * D / 8)` bytes of codes.
  Value `j` is the value of rank `first + x`, with `x` the code of value `j` and `first` the group's first rank.
* BOTH: read the bitmap. Value `j` is the value of rank `first + 1` if bit `j` is set, of rank `first` otherwise.
* LEAF_LEFT: read the right count, decode the right child holding `K_right` values, then read the bitmap.
  Value `j` is the next value of the right child if bit `j` is set, the value of the left Leaf node otherwise.
* FULL: read the right count. The left child holds `K - K_right` values.
  Decode the first child (3.1), then the second child, then read the bitmap.
  Value `j` is the next value of the right child if bit `j` is set, the next value of the left child otherwise.
* A node holding no values reads nothing.

Each node knows the number of values of its children before it reads them,
so a decoder can find all regions first, then merge the nodes bottom-up.

## 6 Validation

A decoder must report an error when:

* `n` is 0 or above 65535.
* The table has a length above 11, codes no value, or codes two or more values that are not a complete code (1.2).
* A region runs past the end of the block.
* A right count is above the number of values of its node.
* The bitmap of a FULL or LEAF_LEFT node does not have exactly `K_right` bits set.
* A bitmap or flat codes region has an unused bit set in its last byte (3.2, 3.3).
* The table codes one value (2.5) and a bitmap has a bit set.
* The block does not end exactly after the regions of the root.

A node that holds no values has no regions: a decoder must not read any for it.
The bitmap of a BOTH node whose two values differ is not checked further: all its bits are valid.
A container can restrict tables further: MinLZ requires length 1 for a single coded value (SPEC_SIMD.md 2.3).

With these checks, a decoder accepts exactly the blocks an encoder writes:
a valid block is the encoding of the values it decodes to, and every input of `n` coded values has one encoding.

# APPENDIX A: EXAMPLES

## A.1 Tree

A table with `e` at length 2, and `a b c d f g` at length 3. The code is complete: `2^9 + 6 * 2^8 = 2^11`.

| Length | Values        | Groups                                     |
|--------|---------------|--------------------------------------------|
| 2      | `e`           | `e` at depth 2                             |
| 3      | `a b c d f g` | `a b c d` at depth 1, `f g` at depth 2     |

Sorted by depth, then length, the groups are `a b c d` (depth 1, length 3), `e` (depth 2, length 2) and `f g` (depth 2, length 3).
Their paths are `0`, `10` (`(0 + 1) << 1`) and `11`.
So the ranks are `a b c d e f g`: `e` has the shortest code, but rank 4.

| Record | Path   | Kind      | Ranks | Values    | Split |
|--------|--------|-----------|-------|-----------|-------|
| 0      | (root) | FULL      | 0-6   | `a` - `g` | 3     |
| 1      | `0`    | FLAT, D=2 | 0-3   | `a b c d` |       |
| 2      | `1`    | LEAF_LEFT | 4-6   | `e f g`   | 4     |
|        | `10`   | Leaf      | 4     | `e`       |       |
| 3      | `11`   | BOTH      | 5-6   | `f g`     | 5     |

## A.2 Block

The 14 values `fedcbafeedbeef`, coded with the table of A.1, are this 10-byte block:

    08 00 03 00 00 85 1B 07 C3 39

| Bytes   | Region                                                                       |
|---------|------------------------------------------------------------------------------|
| `08 00` | Record 0 right count: 8 of its 14 values are `e`, `f` or `g`                 |
| `03 00` | Record 2 right count: 3 of its 8 values `fefeeeef` are `f` or `g`            |
| `00`    | Record 3 bitmap: `fff`, all the group's first value                          |
| `85`    | Record 2 bitmap: `fefeeeef` is `1 0 1 0 0 0 0 1`, bit 0 first                |
| `1B 07` | Record 1 codes: `dcbadb` is `3 2 1 0 3 1`, 2 bits each, 12 bits              |
| `C3 39` | Record 0 bitmap: `fedcbafeedbeef` is `1 1 0 0 0 0 1 1 1 0 0 1 1 1`, 14 bits  |

Record 0 is FULL, and its right child holds more values (8 to 6), so record 2 comes before record 1.
Record 2 is LEAF_LEFT: only its right child, record 3, is stored.

The size from section 4 is `2 + 2` for record 0, `2 + 1` for record 2, `1` for record 3, and `2` for record 1: 10 bytes.

## A.3 Empty Subtrees

With the same table, `eee` is `03 00 00 00 00 07`: record 0's right count is 3, record 2's is 0,
record 2's bitmap is `00`, and record 0's bitmap is `07`.
Records 1 and 3 hold no values and store nothing.

`abba` is `00 00 14 00`: record 0's right count is 0, so its left child, record 1, comes first.
Its codes `0 1 1 0` (`14`) follow, then record 0's bitmap `00`. Records 2 and 3 store nothing.

## A.4 One Coded Value

With a table that only codes `x`, 11 values `x` are `00 00`.

## A.5 Flat Codes

A table with `a` to `h` at length 3 is one group of 8 values at depth 0: the root is a FLAT node with `D` = 3.

The 130 values `a` x 16, `b` x 16, ..., `h` x 16, `a a` have the codes `c[i] = floor(i / 16)` for `i` below 128, then `0 0`.
`K` = 130, so part 1 is empty, part 2 is one 16-lane block of 128 codes, and part 3 is the last 2 codes.

Lane `l` holds the codes `0 1 2 3 4 5 6 7`, so `lane(l) = 0 | 1 << 3 | 2 << 6 | ... | 7 << 21 = 0xFAC688`.
The block is 49 bytes: 16 times `88`, 16 times `C6`, 16 times `FA`, and `00` for the 2 natural codes.
