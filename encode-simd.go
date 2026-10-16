// Copyright 2026 MinIO Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package minlz

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"sync"

	"github.com/minio/minlz/internal/pivco"
)

const (
	// simdChunkLimit is the chunk limit value of the header flags.
	simdChunkLimit = 4
	simdChunkOps   = 1024 << simdChunkLimit
	// simdMaxLong is the longest literal run or match of an operation, with an escape.
	simdMaxLong = simdMaxLen + 1 + 255
	// simdBlockBits estimates the overhead of a pivco block.
	simdBlockBits = 32 * 8
	// simdFastBlockBits is the overhead SuperFast and Fastest assume. They store more small streams,
	// which encodes ~5% faster at 4 KB and costs at most 0.2% size.
	simdFastBlockBits = 40 * 8
	// simdMinCoded is the fewest values that, at one bit each, code smaller than stored.
	simdMinCoded = simdBlockBits/7 + 1
	// simdRecordMax is the size of the largest chunk record: flags, 5 sizes and 4 tables.
	simdRecordMax = 1 + 5*3 + 68 + 17 + 88 + 128
	// simdTableBits is added to the cost of a new table, since every table costs decode time.
	simdTableBits = 64 * 8
	// simdLitSeg is the step of the literal block splits of LevelSmallestSIMD.
	simdLitSeg = 8192
)

// simdTable is a table with its code lengths and its serialized form.
type simdTable struct {
	t    pivco.Table
	lens [256]uint8
	mode int // simdCodeLens, simdClassLens or simdDeltaLens
	n    int
	b    [128]byte
}

// simdClassBytes is the size of the class lengths of each layout.
var simdClassBytes = [...]int{simdTok: 11, simdLL: 6, simdOffc: 12, simdEsc: 8, simdLits: 7, simdDelta: 5}

// simdStreams holds the streams of the current chunk and the parser state that the assembly parsers share.
// _generate/gen-simd-enc.go mirrors its layout, see TestSIMDAsmLayout.
type simdStreams struct {
	nTok, nLL, nOffc, nEsc, nRaw, nLits, nacc int
	acc                                       uint64
	pos, rep                                  int
	// lits holds the literals of the block.
	lits    []byte
	overlap bool // a match of the chunk overlaps its output

	// The arrays follow the fields of every operation, so those and tok have offsets below 128,
	// which amd64 encodes in a byte.
	// An offset has at most 20 raw bits.
	// The fast paths mask their indexes, so the compiler drops the bounds checks.
	tok, ll, offc [simdChunkOps]byte
	esc           [2 * simdChunkOps]byte
	raw           [1<<16 + 3]byte // room for a 4 byte store at any uint16 index

	// An assembly parser resumes at asmSite. It leaves operations to Go as the literals src[pos:asmBase],
	// then a match of asmLen bytes at offset asmOff, or at the repeat offset.
	asmSite, asmBase, asmLen, asmOff int
}

// simdEncoder builds SIMD blocks. Parsers emit operations, which are coded a chunk at a time,
// then finish codes the literals and writes the block.
type simdEncoder struct {
	// First, so the parsers' fields have short offsets.
	simdStreams

	pv     pivco.Encoder
	src    []byte
	chunks int

	// buf holds lits and dlits, the literals of the block as delta literals. With delta literals only, dlits is lits.
	buf, dlits []byte
	// opLits ends the literals of the operations, and chunkLits starts those of the current chunk.
	// switches holds the literal positions where plain and delta literals take turns, starting with plain.
	opLits, chunkLits int
	switches          []int
	// splitLits chooses the literal block splits by size, using cuts, segs and dp.
	splitLits bool
	cuts, dp  []int
	segs      [][256]uint32
	// fastCode, when set, makes put build one table at most: code lengths for at least fastCode values,
	// else class lengths. Building tables dominates the encoding of small blocks.
	fastCode int

	deltaDiv int // delta literals must save 1/deltaDiv of a chunk; 0 disables them, -1 uses only them
	limit    int // largest size of the encoded block

	out, litRecs, chunkRecs []byte
	cur                     [simdLits + 1]simdTable
	have                    [simdLits + 1]bool
	tmp                     [2]simdTable
	hist                    [256]uint64
}

var simdEncoders sync.Pool

// encodeBlockSIMD encodes src to dst as a SIMD block without its indicator byte, and returns its size.
// It returns 0 if the block would not save the level's minimum, and at least simdMinSaving bytes.
func encodeBlockSIMD(dst, src []byte, level int) int {
	e, _ := simdEncoders.Get().(*simdEncoder)
	if e == nil {
		e = new(simdEncoder)
	}
	e.out = dst[:0:len(dst)]
	e.parse(src, level)
	n := e.finish()
	// Don't keep the buffers alive in the pool.
	e.src, e.out = nil, nil
	simdEncoders.Put(e)
	return n
}

// simdEncAsm makes blocks without delta literals use the assembly parsers, which parse as the Go parsers.
var simdEncAsm = hasAsm

// The assembly parsers, see parseAsm.
const (
	simdAsmFast      = iota // encodeFastBlockGo
	simdAsmBlock            // encodeBlockGo
	simdAsmBetter           // encodeBlockBetterGo
	simdAsmFast64K          // encodeFastBlockGo64K
	simdAsmBlock64K         // encodeBlockGo64K
	simdAsmBetter64K        // encodeBlockBetterGo64K
)

// parse codes the operations of src.
func (e *simdEncoder) parse(src []byte, level int) {
	delta := 0
	if level&LevelSIMDDelta != 0 {
		delta = -1
	}
	asm := simdEncAsm && delta == 0
	e.splitLits = level == LevelSmallestSIMD
	e.fastCode = 0
	switch level &^ LevelSIMDDelta {
	case LevelSuperFastSIMD:
		e.fastCode = 1
		e.reset(src, delta, len(src)>>3)
		switch {
		case asm && len(src) <= 64<<10:
			e.parseAsm(src, simdAsmFast64K)
		case len(src) <= 64<<10:
			e.encodeFastBlockGo64K(src)
		case asm:
			e.parseAsm(src, simdAsmFast)
		default:
			e.encodeFastBlockGo(src)
		}
	case LevelFastestSIMD:
		e.fastCode = 4096
		e.reset(src, delta, len(src)>>5)
		switch {
		case asm && len(src) <= 64<<10:
			e.parseAsm(src, simdAsmBlock64K)
		case len(src) <= 64<<10:
			e.encodeBlockGo64K(src)
		case asm:
			e.parseAsm(src, simdAsmBlock)
		default:
			e.encodeBlockGo(src)
		}
	case LevelBalancedSIMD:
		e.reset(src, delta, len(src)>>5)
		switch {
		case asm && len(src) <= 64<<10:
			e.parseAsm(src, simdAsmBetter64K)
		case len(src) <= 64<<10:
			e.encodeBlockBetterGo64K(src)
		case asm:
			e.parseAsm(src, simdAsmBetter)
		default:
			e.encodeBlockBetterGo(src)
		}
	default:
		e.reset(src, 256, 0)
		e.encodeBlockBest(src)
	}
	if e.nTok > 0 {
		e.endChunk()
	}
	if debugEncode && e.pos != len(e.src) {
		panic("operations do not cover the block")
	}
}

// reset starts a block for src, which must save at least save bytes. deltaDiv is as e.deltaDiv.
func (e *simdEncoder) reset(src []byte, deltaDiv, save int) {
	n := len(src)
	size := n
	if deltaDiv > 0 {
		size += n
	}
	if cap(e.buf) < size {
		e.buf = make([]byte, size)
	}
	e.lits, e.dlits = e.buf[:n], e.buf[n:size]
	if deltaDiv < 0 {
		e.dlits = e.lits
	}
	e.out = binary.AppendUvarint(e.out[:0], simdBlockType<<24|uint64(n))
	e.out = append(e.out, simdChunkLimit)
	// Operations are fewer than n/2, so this bounds the chunks and literal blocks.
	// Each chunk can switch between plain and delta literals, which starts a literal block.
	chunks := n/2/simdChunkOps + 2
	parts := n/pivco.MaxBlockSize + chunks + 2
	if cap(e.chunkRecs) < chunks*simdRecordMax {
		e.chunkRecs, e.switches = make([]byte, 0, chunks*simdRecordMax), make([]int, 0, chunks+2)
	}
	if cap(e.litRecs) < parts*(3+128) {
		e.litRecs = make([]byte, 0, parts*(3+128))
	}
	e.chunkRecs, e.switches = e.chunkRecs[:0], e.switches[:0]
	e.nTok, e.nLL, e.nOffc, e.nEsc, e.nRaw, e.nLits, e.chunks = 0, 0, 0, 0, 0, 0, 0
	e.opLits, e.chunkLits = 0, 0
	e.acc, e.nacc, e.overlap = 0, 0, false
	e.src, e.pos, e.rep, e.deltaDiv, e.limit = src, 0, 1, deltaDiv, n-max(simdMinSaving, save)
	e.have = [len(e.have)]bool{}
}

// emitLiterals adds the literals after the last operation.
func (e *simdEncoder) emitLiterals(lits []byte) {
	e.nLits += copy(e.lits[e.nLits:], lits)
	e.pos += len(lits)
}

// The parsers add the common operations with fast, putOffset, putBits, putLits and putMatch,
// which must stay inlinable to save the calls, and leave the others to the emitters.

// fast reports whether the plain literals src[from:base], and a match at offset that ends at s, fit putLits and putMatch.
// Delta literals are rare, so they are left to the emitters.
func (e *simdEncoder) fast(src []byte, from, base, s, offset int) bool {
	return e.deltaDiv == 0 && base-from <= simdMaxLong && len(src)-from >= simdMaxLen && oneOp(offset, s-base)
}

// oneOp reports whether putMatch can add a match of length bytes at offset.
func oneOp(offset, length int) bool {
	return length <= simdMaxLen || length <= simdMaxLong && offset >= simdMaxLen
}

// matchAt returns how many bytes match at src[a:] and src[b:], with b < a, like matchLen.
// It inlines, where matchLen is an assembly call on amd64.
func matchAt(src []byte, a, b int) (n int) {
	for ; a+n <= len(src)-8; n += 8 {
		if diff := load64(src, a+n) ^ load64(src, b+n); diff != 0 {
			return n + bits.TrailingZeros64(diff)>>3
		}
	}
	for a+n < len(src) && src[a+n] == src[b+n] {
		n++
	}
	return n
}

// putOffset adds the symbol of a match offset, and returns its raw bits and their count for putBits.
func (e *simdEncoder) putOffset(offset int) (uint64, int) {
	// With the 8, offsets below 16 code as themselves without raw bits, and need no branch.
	b := bits.Len(uint(offset)|8) - 4
	e.offc[e.nOffc&(simdChunkOps-1)] = byte(8*b + offset>>b)
	e.nOffc++
	return uint64(offset & (1<<b - 1)), b
}

// putBits adds the n raw bits of v.
func (e *simdEncoder) putBits(v uint64, n int) {
	// Store and advance without a branch, since the stores come at an irregular pace.
	// e.raw has room, as the offsets of a chunk have at most 20 raw bits each.
	acc := e.acc | v<<(e.nacc&63)
	n += e.nacc
	binary.LittleEndian.PutUint32(e.raw[uint16(e.nRaw):], uint32(acc))
	k := n >> 5
	e.nRaw += k << 2
	e.acc, e.nacc = acc>>(k<<5&63), n&31
}

// putLL adds the literal length ll, at most simdMaxLong, and returns the literal bit of the operation.
func (e *simdEncoder) putLL(ll int) byte {
	// Operations with and without literals alternate unpredictably, so don't branch on it.
	n := min(ll, 1)
	e.ll[e.nLL&(simdChunkOps-1)] = byte(min(ll, simdMaxLen+1))
	e.nLL += n
	if ll > simdMaxLen {
		e.esc[e.nEsc&(2*simdChunkOps-1)] = byte(ll - simdMaxLen - 1)
		e.nEsc++
	}
	return byte(n) << 1
}

// putLits adds the ll plain literals at src[from:], where ll is at most simdMaxLong.
// src has at least simdMaxLen bytes from from, so e.lits, as long as src, has as much room after its literals.
func (e *simdEncoder) putLits(src []byte, from, ll int) {
	if ll > simdMaxLen {
		copy(e.lits[e.nLits:], src[from:from+ll])
	} else {
		store256(e.lits, e.nLits, load256(src, from))
	}
	e.nLits += ll
	e.pos += ll
}

// putMatch adds an operation with the literal and repeat bits t, and a match at offset that satisfies oneOp.
// The offset becomes the repeat offset. It returns whether the chunk is full.
func (e *simdEncoder) putMatch(t byte, offset, length int) bool {
	e.rep = offset
	if length > simdMaxLen {
		e.esc[e.nEsc&(2*simdChunkOps-1)] = byte(length - simdMaxLen - 1)
		e.nEsc++
		t |= (simdMaxLen + 1) << 2
	} else {
		t |= byte(length) << 2
		e.overlap = e.overlap || offset < length
	}
	e.tok[e.nTok&(simdChunkOps-1)] = t
	e.nTok++
	e.pos += length
	return e.nTok == simdChunkOps
}

// emitCopyLits adds lits, then a match of length bytes at offset.
func (e *simdEncoder) emitCopyLits(lits []byte, offset, length int) {
	for len(lits) > simdMaxLong {
		e.emitLong(lits[:simdMaxLong], e.rep, 1, 0)
		lits = lits[simdMaxLong:]
	}
	prev := e.rep
	e.rep = offset
	e.putBits(e.putOffset(offset))
	if len(lits) > simdMaxLen || length > simdMaxLen {
		e.emitLong(lits, prev, 0, length)
		return
	}
	var t byte
	if len(lits) > 0 {
		t = 2
		e.ll[e.nLL] = byte(len(lits))
		e.nLL++
		if e.deltaDiv != 0 {
			e.deltaLits(lits, prev)
		} else {
			e.nLits += copy(e.lits[e.nLits:], lits)
		}
		e.pos += len(lits)
	}
	e.overlap = e.overlap || offset < length
	e.tok[e.nTok] = t | byte(length)<<2
	e.nTok++
	e.pos += length
	if e.nTok == simdChunkOps {
		e.endChunk()
	}
}

// emitRepeatLits adds lits, then a match of length bytes at the repeat offset.
func (e *simdEncoder) emitRepeatLits(lits []byte, length int) {
	for len(lits) > simdMaxLong {
		e.emitLong(lits[:simdMaxLong], e.rep, 1, 0)
		lits = lits[simdMaxLong:]
	}
	if len(lits) > simdMaxLen || length > simdMaxLen {
		e.emitLong(lits, e.rep, 1, length)
		return
	}
	t := byte(1)
	if len(lits) > 0 {
		t |= 2
		e.ll[e.nLL] = byte(len(lits))
		e.nLL++
		if e.deltaDiv != 0 {
			e.deltaLits(lits, e.rep)
		} else {
			e.nLits += copy(e.lits[e.nLits:], lits)
		}
		e.pos += len(lits)
	}
	e.overlap = e.overlap || e.rep < length
	e.tok[e.nTok] = t | byte(length)<<2
	e.nTok++
	e.pos += length
	if e.nTok == simdChunkOps {
		e.endChunk()
	}
}

// emitCopy adds a match of length bytes at offset, without literals.
func (e *simdEncoder) emitCopy(offset, length int) {
	if debugEncode && e.pos == 0 {
		panic("first operation without literals")
	}
	e.rep = offset
	e.putBits(e.putOffset(offset))
	e.emitLong(nil, 0, 0, length)
}

// emitLong adds an operation with lits, at most simdMaxLong of them, and a match of length bytes at e.rep,
// split into more operations if needed. t is the repeat bit of the operation, and prev the repeat offset before it.
// The offset of the match must already be added.
func (e *simdEncoder) emitLong(lits []byte, prev int, t byte, length int) {
	if n := len(lits); n > 0 {
		t |= 2
		if n > simdMaxLen {
			e.esc[e.nEsc] = byte(n - simdMaxLen - 1)
			e.nEsc++
			n = simdMaxLen + 1
		}
		e.ll[e.nLL] = byte(n)
		e.nLL++
		if e.deltaDiv != 0 {
			e.deltaLits(lits, prev)
		} else {
			e.nLits += copy(e.lits[e.nLits:], lits)
		}
		e.pos += len(lits)
	}
	step := simdMaxLen
	if e.rep >= simdMaxLen {
		step = simdMaxLong
	}
	for {
		n := min(length, step)
		if n > simdMaxLen {
			e.esc[e.nEsc] = byte(n - simdMaxLen - 1)
			e.nEsc++
			t |= (simdMaxLen + 1) << 2
		} else {
			t |= byte(n) << 2
			e.overlap = e.overlap || e.rep < n
		}
		e.tok[e.nTok] = t
		e.nTok++
		e.pos += n
		length -= n
		if e.nTok == simdChunkOps {
			e.endChunk()
		}
		if length == 0 {
			return
		}
		t = 1
	}
}

// deltaLits adds lits, and their delta literals (SPEC_SIMD.md 4.5), for an operation at e.pos with repeat offset rep.
func (e *simdEncoder) deltaLits(lits []byte, rep int) {
	n := e.nLits
	e.nLits += copy(e.lits[n:], lits)
	e.opLits = e.nLits
	dl := e.dlits[n:e.nLits]
	if e.deltaDiv > 0 {
		copy(dl, lits)
	}
	r := min(rep, e.pos)
	if r == 0 {
		return
	}
	dl, ref := dl[:min(len(lits), simdMaxLen)], e.src[e.pos-r:]
	if r >= simdMaxLen {
		for j := range dl {
			dl[j] -= ref[j]
		}
		return
	}
	for j := range dl {
		dl[j] -= ref[j%r]
	}
}

// endChunk codes the streams of the current chunk to e.out and its record to e.chunkRecs.
func (e *simdEncoder) endChunk() {
	for ; e.nacc > 0; e.nacc -= 8 {
		e.raw[e.nRaw] = byte(e.acc)
		e.nRaw++
		e.acc >>= 8
	}
	e.acc, e.nacc = 0, 0
	// The parsers' operations average at least 4 bytes, so 8 MiB fits in simdMaxChunks chunks. Fail the block if not.
	if e.chunks++; e.chunks > simdMaxChunks {
		e.limit = -1
	}
	// A block that is already too large fails, so don't code more.
	if len(e.out)+len(e.chunkRecs) <= e.limit {
		var flags byte
		if !e.overlap {
			flags = 2
		}
		if e.deltaDiv < 0 {
			flags |= 1
		}
		rec, size := len(e.chunkRecs), len(e.out)+len(e.chunkRecs)
		e.chunkRecs = append(e.chunkRecs, flags)
		e.chunkRecs = e.put(e.chunkRecs, simdTok, e.tok[:e.nTok])
		e.chunkRecs = e.put(e.chunkRecs, simdLL, e.ll[:e.nLL])
		e.chunkRecs = e.put(e.chunkRecs, simdOffc, e.offc[:e.nOffc])
		e.chunkRecs = e.put(e.chunkRecs, simdEsc, e.esc[:e.nEsc])
		if cap(e.out)-len(e.out) < e.nRaw {
			e.limit = -1
		} else {
			e.out = append(e.out, e.raw[:e.nRaw]...)
			e.chunkRecs = binary.AppendUvarint(e.chunkRecs, uint64(e.nRaw)<<3)
		}
		if e.deltaDiv > 0 && e.deltaChunk(len(e.out)+len(e.chunkRecs)-size) {
			e.chunkRecs[rec] |= 1
		}
	}
	e.nTok, e.nLL, e.nOffc, e.nEsc, e.nRaw, e.overlap = 0, 0, 0, 0, 0, false
}

// finish codes the literals and completes the block. It returns its size, or 0 if it is too large.
func (e *simdEncoder) finish() int {
	if len(e.out)+len(e.chunkRecs) > e.limit {
		return 0
	}
	if len(e.chunkRecs) > 0 {
		e.out = appendRvarint(append(e.out, e.chunkRecs...), uint64(len(e.chunkRecs)))
	}
	// The LZ section starts after the 4 byte size and the flags.
	lz := len(e.out) - 5
	e.litRecs = e.litRecs[:0]
	// Plain and delta literals go in separate literal blocks. The literals after the operations are plain.
	if len(e.switches)&1 == 1 {
		e.switches = append(e.switches, e.opLits)
	}
	start := 0
	for i, end := range append(e.switches, e.nLits) {
		k, lits := simdLits, e.lits[start:end]
		if i&1 == 1 || e.deltaDiv < 0 {
			k, lits = simdDelta, e.dlits[start:end]
		}
		start = end
		if len(lits) == 0 {
			continue
		}
		cuts := e.litBlocks(lits)
		for j := 1; j < len(cuts); j++ {
			e.litRecs = e.put(e.litRecs, k, lits[cuts[j-1]:cuts[j]])
			if len(e.out)+len(e.litRecs) > e.limit {
				return 0
			}
		}
	}
	e.out = appendRvarint(append(e.out, e.litRecs...), uint64(len(e.litRecs)))
	e.out = appendRvarint(e.out, uint64(lz))
	if len(e.out) > e.limit {
		return 0
	}
	return len(e.out)
}

// litBlocks returns the boundaries of the literal blocks of lits.
// With splitLits, they are on simdLitSeg boundaries, chosen for the smallest estimated size.
// Otherwise the blocks are equal.
func (e *simdEncoder) litBlocks(lits []byte) []int {
	cuts := e.cuts[:0]
	if !e.splitLits {
		parts := (len(lits) + pivco.MaxBlockSize - 1) / pivco.MaxBlockSize
		size := (len(lits) + parts - 1) / parts
		for j := 0; j < len(lits); j += size {
			cuts = append(cuts, j)
		}
		e.cuts = append(cuts, len(lits))
		return e.cuts
	}
	n := (len(lits) + simdLitSeg - 1) / simdLitSeg
	e.segs = slices.Grow(e.segs[:0], n)[:n]
	for i := range e.segs {
		e.segs[i] = [256]uint32{}
		for _, b := range lits[i*simdLitSeg : min((i+1)*simdLitSeg, len(lits))] {
			e.segs[i][b]++
		}
	}
	// cost[i] is the smallest size of the first i segments, and from[i] where its last block starts.
	e.dp = slices.Grow(e.dp[:0], 2*n+2)[:2*n+2]
	cost, from := e.dp[:n+1], e.dp[n+1:]
	cost[0] = 0
	for i := 1; i <= n; i++ {
		cost[i] = math.MaxInt
		var h [256]uint64
		for j := i - 1; j >= max(0, i-pivco.MaxBlockSize/simdLitSeg); j-- {
			for v, f := range &e.segs[j] {
				h[v] += uint64(f)
			}
			// An entry is about 3 bytes.
			if c := cost[j] + 3*8 + simdLitBits(&h, min(i*simdLitSeg, len(lits))-j*simdLitSeg); c < cost[i] {
				cost[i], from[i] = c, j
			}
		}
	}
	for i := n; i > 0; i = from[i] {
		cuts = append(cuts, min(i*simdLitSeg, len(lits)))
	}
	e.cuts = append(cuts, 0)
	slices.Reverse(e.cuts)
	return e.cuts
}

// simdLitBits estimates the size in bits of a literal block of n values with histogram h:
// stored, or its entropy plus the overhead of a block with a new table of about 16 bytes.
// The entropy is cheaper than building the tables, which would dominate the encoding of literal-heavy data.
func simdLitBits(h *[256]uint64, n int) int {
	if n < simdMinCoded {
		return 8 * n
	}
	// n*log2(n) - sum(f*log2(f)), in 1/256 bits.
	e := n * simdLog2(uint64(n))
	for _, f := range h {
		if f > 1 {
			e -= int(f) * simdLog2(f)
		}
	}
	return min(8*n, max(e>>8, 0)+simdBlockBits+simdTableBits+16*8)
}

// simdEntropyBits returns a lower bound of the entropy in bits of the counts h of n values.
// simdLog2 is within -1.88 and +0.5 of 256*log2, so n*log2(n) - sum(f*log2(f)) in 1/256 bits, as simdLitBits
// computes it, is at most 2.38n above 256 times the entropy.
func simdEntropyBits(h []uint64, n int) int {
	e := n*simdLog2(uint64(n)) - (5*n+1)/2
	for _, f := range h {
		if f > 1 {
			e -= int(f) * simdLog2(f)
		}
	}
	return max(e, 0) >> 8
}

// simdMaxEntropy is log2 of the alphabet size of each stream, in 1/256 bits, rounded up.
var simdMaxEntropy = func() (m [simdLits + 1]int) {
	for k, n := range simdCodeBytes {
		m[k] = int(math.Ceil(256 * math.Log2(float64(2*n))))
	}
	return m
}()

// simdLog2Frac holds log2(1+i/256) in 1/256 units.
var simdLog2Frac = func() (t [256]uint8) {
	for i := range t {
		t[i] = uint8(math.Round(256 * math.Log2(1+float64(i)/256)))
	}
	return t
}()

// simdLog2 returns log2(x) for x > 0 in 1/256 units, from the 9 top bits of x.
func simdLog2(x uint64) int {
	b := bits.Len64(x) - 1
	return b<<8 + int(simdLog2Frac[x<<(63-b)>>55&0xff])
}

// deltaChunk reports whether the current chunk, with lz bytes of operations, takes delta literals:
// when they are estimated to save 1/e.deltaDiv of the chunk. It records where the literals switch.
func (e *simdEncoder) deltaChunk(lz int) bool {
	start := e.chunkLits
	e.chunkLits = e.opLits
	if start == e.opLits {
		return false
	}
	raw, delta := e.litBits(e.lits[start:e.opLits]), e.litBits(e.dlits[start:e.opLits])
	d := raw-delta > (raw+8*lz)/e.deltaDiv
	if d != (len(e.switches)&1 == 1) {
		e.switches = append(e.switches, start)
	}
	return d
}

// litBits returns the estimated size of lits in bits.
func (e *simdEncoder) litBits(lits []byte) int {
	e.histogram(lits, 256)
	e.tmp[0].setCode(simdLits, &e.hist)
	n, _ := e.tmp[0].cost(&e.hist, 256)
	return n
}

// histogram sets e.hist[:syms] to the counts of v, whose values are below syms.
func (e *simdEncoder) histogram(v []byte, syms int) {
	clear(e.hist[:syms])
	e.pv.Histogram(&e.hist, v)
}

// put appends the values v of stream k to e.out, and their entry to recs. k is simdDelta for delta literals.
// It takes the smallest of storing v, the current table and new tables for v.
// With e.fastCode, it takes the current table when that is smaller than storing,
// and otherwise only tries one new table.
// If v doesn't fit, the block fails.
func (e *simdEncoder) put(recs []byte, k int, v []byte) []byte {
	layout := k
	k = min(k, simdLits)
	// The values of the stream are below syms.
	syms := 2 * simdCodeBytes[k]
	var best *simdTable
	bestBits := 8 * len(v)
	blockBits := simdBlockBits
	if e.fastCode != 0 {
		blockBits = simdFastBlockBits
	}
	try := func(t *simdTable, extra int) {
		if n, ok := t.cost(&e.hist, syms); ok && n+extra+blockBits < bestBits {
			best, bestBits = t, n+extra+blockBits
		}
	}
	// As simdMinCoded, for blockBits.
	if len(v) > blockBits/7 {
		e.histogram(v, syms)
		if e.have[k] {
			try(&e.cur[k], 0)
		}
		if e.fastCode == 0 || best == nil {
			// No prefix code codes v in fewer bits than its entropy, so new tables that can't win with that aren't built.
			// The entropy is at most simdMaxEntropy per value, so it is only computed when it can stop a table.
			low := simdTableBits + blockBits
			if low < bestBits && bestBits <= low+(len(v)*simdMaxEntropy[k]+255)>>8+8*simdCodeBytes[k] {
				low += simdEntropyBits(e.hist[:syms], len(v))
			}
			if len(v) >= e.fastCode && low+8*simdCodeBytes[k] < bestBits {
				e.tmp[0].setCode(k, &e.hist)
				try(&e.tmp[0], simdTableBits+8*e.tmp[0].n)
			}
			if (e.fastCode == 0 || len(v) < e.fastCode) && low+8*simdClassBytes[layout] < bestBits {
				e.tmp[1].setClass(layout, &e.hist, syms)
				try(&e.tmp[1], simdTableBits+8*e.tmp[1].n)
			}
		}
	}
	room := cap(e.out) - len(e.out)
	if best != nil {
		mode, n := simdCurrent, 0
		if best != &e.cur[k] {
			mode, n = best.mode, best.n
			// Tables are only built when used.
			if best.t.SetLengths(&best.lens) != nil {
				panic("invalid table")
			}
		}
		// Only tokens and literal blocks store their number of values (SPEC_SIMD.md 2.2).
		hdr := 0
		if k == simdTok || k == simdLits {
			hdr = 2
		}
		if size := hdr + best.t.BlockSize(e.hist[:syms]); size <= room && size+n < len(v) {
			out := e.out[len(e.out):][:size]
			if hdr > 0 {
				binary.LittleEndian.PutUint16(out, uint16(len(v)))
			}
			if e.pv.Encode(out[hdr:], v, &best.t) == nil {
				recs = binary.AppendUvarint(recs, uint64(size)<<3|uint64(mode))
				e.out = e.out[:len(e.out)+size]
				if mode != simdCurrent {
					recs = append(recs, best.b[:n]...)
					e.cur[k], e.have[k] = *best, true
				}
				return recs
			}
		}
	}
	if room < len(v) {
		e.limit = -1
		return recs
	}
	e.out = append(e.out, v...)
	return binary.AppendUvarint(recs, uint64(len(v))<<3)
}

// cost returns the size in bits of histogram h[:syms] coded with t, and false if a value has no code.
func (t *simdTable) cost(h *[256]uint64, syms int) (int, bool) {
	var n uint64
	ok := true
	for v, f := range h[:syms] {
		n += f * uint64(t.lens[v])
		ok = ok && (f == 0 || t.lens[v] != 0)
	}
	return int(n), ok
}

// setCode sets t to the Huffman code of h, as code lengths of stream k. h must not be empty.
func (t *simdTable) setCode(k int, h *[256]uint64) {
	syms := 2 * simdCodeBytes[k]
	if !pivco.BuildLengths(h[:syms], &t.lens) {
		panic("empty histogram")
	}
	t.mode = simdCodeLens
	t.setNibbles(t.lens[:syms])
}

// setClass sets t to the class lengths of h for layout k (SPEC_SIMD.md 2.3).
// The values of the stream are below syms.
func (t *simdTable) setClass(k int, h *[256]uint64, syms int) {
	var cl [24]uint8
	n := 0
	if k == simdTok {
		n = simdTokClassLens(h, &cl, &t.lens)
	} else {
		lay := &simdLayouts[k]
		var w [24]uint64
		var lim [24]uint8
		for v, c := range lay.class[:syms] {
			if c != 0xff {
				w[c] += h[v]
				lim[c] = max(lim[c], lay.in[v])
			}
		}
		n = lay.n
		for c := range lim[:n] {
			lim[c] = pivco.MaxCodeLen - lim[c]
		}
		simdLimitedLens(w[:n], lim[:n], cl[:n])
		t.lens = [256]uint8{}
		for v, c := range lay.class[:syms] {
			if c != 0xff && cl[c] != 0 {
				t.lens[v] = cl[c] + lay.in[v]
			}
		}
	}
	t.mode = simdClassLens
	if k == simdDelta {
		t.mode = simdDeltaLens
	}
	t.setNibbles(cl[:n])
}

// simdTokClassLens sets cl to the token class lengths of h (SPEC_SIMD.md A.4),
// and lens to the code lengths. It returns the number of class lengths.
func simdTokClassLens(h *[256]uint64, cl *[24]uint8, lens *[256]uint8) int {
	const groups = 14
	grp := &simdLayouts[simdTok]
	var w [groups]uint64
	var fw [2][4]uint64
	for v := range 136 {
		w[grp.class[v>>2]] += h[v]
		fw[v>>7][v&3] += h[v]
	}
	// Flag codes for match length codes 0-31 and 32-33.
	f := [2][]uint8{cl[groups : groups+4], cl[groups+4 : groups+8]}
	var maxF [2]uint8
	for i := range f {
		simdLimitedLens(fw[i][:], []uint8{3, 3, 3, 3}, f[i])
		maxF[i] = slices.Max(f[i])
	}
	var lim [groups]uint8
	for c := range lim {
		lim[c] = pivco.MaxCodeLen
	}
	for ml := range 34 {
		c := grp.class[ml]
		lim[c] = min(lim[c], pivco.MaxCodeLen-grp.in[ml]-maxF[ml>>5])
	}
	simdLimitedLens(w[:], lim[:], cl[:groups])
	*lens = [256]uint8{}
	for v := range 136 {
		ml := v >> 2
		if g, fl := cl[grp.class[ml]], f[ml>>5][v&3]; g != 0 && fl != 0 {
			lens[v] = g + grp.in[ml] + fl
		}
	}
	return groups + 8
}

func (t *simdTable) setNibbles(l []uint8) {
	t.n = (len(l) + 1) / 2
	clear(t.b[:t.n])
	for i, v := range l {
		t.b[i/2] |= v << (4 * (i & 1))
	}
}

// simdLimitedLens sets l to the lengths of an optimal complete prefix code for the weights w,
// with l[i] <= lim[i], using package-merge. Weights of 0 get no code,
// except that a code always has at least 2 lengths. len(w) must be at most 24.
func simdLimitedLens(w []uint64, lim, l []uint8) {
	clear(l)
	var sym [24]uint8
	n, maxLim := 0, uint8(0)
	for i, x := range w {
		if x > 0 {
			sym[n] = uint8(i)
			n++
			maxLim = max(maxLim, lim[i])
		}
	}
	if n < 2 {
		for i := range l {
			if w[i] > 0 || n < 2 {
				if w[i] == 0 {
					n++
				}
				l[i] = 1
			}
		}
		return
	}
	s := sym[:n]
	for i := 1; i < n; i++ {
		for j := i; j > 0 && w[s[j]] < w[s[j-1]]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	// items holds the symbol of each item of a level, or -1 for a package of two items of the level below.
	// The selected items of a level are a prefix, and so are the items their packages select below.
	var items [pivco.MaxCodeLen + 1][48]int8
	var a, b [48]uint64
	prev, cur := a[:0], b[:0]
	for lev := maxLim; lev > 0; lev-- {
		cur = cur[:0]
		it := &items[lev]
		for i, j := 0, 0; ; {
			for i < n && lim[s[i]] < lev {
				i++
			}
			if i < n && (j == len(prev) || w[s[i]] <= prev[j]) {
				it[len(cur)] = int8(s[i])
				cur = append(cur, w[s[i]])
				i++
			} else if j < len(prev) {
				it[len(cur)] = -1
				cur = append(cur, prev[j])
				j++
			} else {
				break
			}
		}
		prev = prev[:0]
		for i := 0; i+1 < len(cur); i += 2 {
			prev = append(prev, cur[i]+cur[i+1])
		}
	}
	for lev, k := 1, 2*n-2; k > 0; lev++ {
		p := 0
		for _, x := range items[lev][:k] {
			if x < 0 {
				p++
			} else {
				l[x]++
			}
		}
		k = 2 * p
	}
}

func appendRvarint(b []byte, v uint64) []byte {
	n := len(b)
	b = binary.AppendUvarint(b, v)
	slices.Reverse(b[n:])
	return b
}
