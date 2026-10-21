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

package reference

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// Streams of a chunk, in order, then the literals.
const (
	streamTokens = iota
	streamLitLens
	streamOffsets
	streamEscapes
	streamOffsetBits
	streamLiterals
)

// Entry modes.
const (
	modeStored = iota
	modeCurrent
	modeCodeLengths
	modeClassLengths
	modeDeltaLengths
)

const maxCodeLen = 11

// maxChunks is the most chunks of a block.
const maxChunks = 128

// classLayout holds the values of each class, in ascending order.
type classLayout [][]int

type simdStream struct {
	name    string
	lo, hi  int
	classes classLayout
}

// For tokens, the classes are the match length code groups.
var simdStreams = [...]simdStream{
	streamTokens:     {"tokens", 0, 135, lastValues(0, 0, 3, 4, 5, 6, 7, 8, 10, 12, 16, 24, 31, 32, 33)},
	streamLitLens:    {"literal lengths", 1, 33, lastValues(1, 1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 33)},
	streamOffsets:    {"offset symbols", 1, 175, offsetClasses()},
	streamEscapes:    {"escapes", 0, 255, lastValues(0, 0, 1, 2, 4, 6, 10, 14, 22, 30, 46, 62, 94, 126, 190, 254, 255)},
	streamOffsetBits: {"offset bits", 0, 255, nil},
	streamLiterals:   {"literals", 0, 255, classesOf(0, 255, literalClass)},
}

var deltaClasses = classesOf(0, 255, func(b int) int { return bits.Len(uint(min(b, 256-b))) })

// lastValues returns the classes of the values from first on, given the last value of each class.
func lastValues(first int, last ...int) (l classLayout) {
	for _, end := range last {
		var c []int
		for ; first <= end; first++ {
			c = append(c, first)
		}
		l = append(l, c)
	}
	return l
}

// classesOf returns the classes of the values lo to hi, given the class of each value.
func classesOf(lo, hi int, class func(v int) int) (l classLayout) {
	for v := lo; v <= hi; v++ {
		c := class(v)
		for len(l) <= c {
			l = append(l, nil)
		}
		l[c] = append(l[c], v)
	}
	return l
}

func offsetClasses() classLayout {
	last := []int{1, 3, 7, 15}
	for c := 4; c <= 23; c++ {
		last = append(last, 8*c-9)
	}
	return lastValues(1, last...)
}

func literalClass(b int) int {
	switch {
	case b == 0x00:
		return 0
	case b == 0x09 || b == 0x0a || b == 0x0d:
		return 1
	case b < 0x20 || b == 0x7f:
		return 2
	case b == ' ':
		return 3
	case b >= '0' && b <= '9':
		return 4
	case b == 'a' || b == 'e' || b == 'i' || b == 'n' || b == 'o' || b == 'r' || b == 's' || b == 't':
		return 5
	case b >= 'a' && b <= 'z':
		return 6
	case b >= 'A' && b <= 'Z':
		return 7
	case b == '"' || b == ',' || b == '-' || b == '.' || b == '/' || b == ':' || b == '_':
		return 8
	case b <= 0x7e:
		return 9
	case b <= 0xbf:
		return 10
	case b >= 0xc2 && b <= 0xf4:
		return 11
	}
	return 12
}

// inClassLengths returns the code lengths inside a class of n values.
func inClassLengths(n int) []int {
	k := bits.Len(uint(n)) - 1
	l := make([]int, n)
	for i := range l {
		l[i] = k
		if i >= 1<<(k+1)-n {
			l[i]++
		}
	}
	return l
}

// lengths returns the code length of each value, given the length of each class.
func (l classLayout) lengths(cl []int) (lens [256]int) {
	for c, vals := range l {
		if cl[c] == 0 {
			continue
		}
		for i, in := range inClassLengths(len(vals)) {
			lens[vals[i]] = cl[c] + in
		}
	}
	return lens
}

// rvarint reads the reverse varint at the end of b, and returns it and the bytes before it.
func rvarint(b []byte) (uint64, []byte, error) {
	var rev []byte
	for i := len(b) - 1; i >= 0 && len(rev) < binary.MaxVarintLen64; i-- {
		rev = append(rev, b[i])
	}
	v, n := binary.Uvarint(rev)
	if n <= 0 {
		return 0, nil, errors.New("invalid reverse varint")
	}
	return v, b[:len(b)-n], nil
}

// nibbles reads n lengths of 4 bits from b, and returns them and their size in bytes.
func nibbles(b []byte, n int) ([]int, int, error) {
	size := (n + 1) / 2
	if len(b) < size {
		return nil, 0, errors.New("table runs past its records")
	}
	var l []int
	for _, c := range b[:size] {
		l = append(l, int(c&15), int(c>>4))
	}
	if len(l) > n && l[n] != 0 {
		return nil, 0, errors.New("table pad nibble is not 0")
	}
	return l[:n], size, nil
}

// complete reports whether the lengths l form a complete code.
func complete(l []int) bool {
	kraft := 0
	for _, v := range l {
		if v != 0 {
			kraft += 1 << (15 - v)
		}
	}
	return kraft == 1<<15
}

// readTable reads a table of stream k from tab, and returns it and its size.
func readTable(k, mode int, tab []byte) (*pivcoTable, int, error) {
	s := &simdStreams[k]
	var lens [256]int
	var l []int
	var n int
	var err error
	switch {
	case mode == modeCodeLengths:
		l, n, err = nibbles(tab, s.hi+1)
		copy(lens[:], l)
	case k == streamTokens:
		if l, n, err = nibbles(tab, 22); err != nil {
			break
		}
		if !complete(l[:14]) || !complete(l[14:18]) || !complete(l[18:]) {
			err = errors.New("token group lengths or flag codes are not complete codes")
			break
		}
		ml := s.classes.lengths(l[:14])
		for t := range 136 {
			f := l[14+t&3]
			if t>>2 >= 32 {
				f = l[18+t&3]
			}
			if ml[t>>2] != 0 && f != 0 {
				lens[t] = ml[t>>2] + f
			}
		}
	default:
		classes := s.classes
		if mode == modeDeltaLengths {
			classes = deltaClasses
		}
		if l, n, err = nibbles(tab, len(classes)); err == nil {
			lens = classes.lengths(l)
		}
	}
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", s.name, err)
	}
	used, kraft := 0, 0
	var lens8 [256]uint8
	for v, ln := range lens {
		if ln == 0 {
			continue
		}
		if ln > maxCodeLen {
			return nil, 0, fmt.Errorf("%s: code length %d of value %d above %d", s.name, ln, v, maxCodeLen)
		}
		if v < s.lo || v > s.hi {
			return nil, 0, fmt.Errorf("%s: code for value %d outside the stream's range", s.name, v)
		}
		used++
		kraft += 1 << (maxCodeLen - ln)
		lens8[v] = uint8(ln)
	}
	if kraft != 1<<maxCodeLen && (used != 1 || kraft != 1<<(maxCodeLen-1)) {
		return nil, 0, fmt.Errorf("%s: table is not a complete code", s.name)
	}
	t, err := newPivcoTable(&lens8)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", s.name, err)
	}
	return t, n, nil
}

// entryReader reads entries from records, and their data from the data before the records.
type entryReader struct {
	recs, data []byte
}

type simdDecoder struct {
	size   int
	tables [streamLiterals + 1]*pivcoTable
	lits   []byte
	l      int // literals used
	out    []byte
	repeat int
	ops    int
	strict bool
	w      *SIMDWalker
}

// SIMDWalker receives the contents of a SIMD block from WalkSIMDBlock. Nil functions are not called.
type SIMDWalker struct {
	// Entry gets each stream entry in block order: the stream (0 tokens, 1 literal lengths, 2 offset symbols,
	// 3 escapes, 4 offset bits, 5 literals), its mode, its number of values, and the bytes of its data and new table.
	Entry func(stream, mode, values, data, table int)
	// Chunk gets the flags and number of operations of each chunk, after its entries.
	Chunk func(flags byte, ops int)
	// Op gets each operation: its number of literals, match length and offset, and whether the offset is repeated.
	Op func(lits, ml, offset int, repeat bool)
}

// DecodeSIMDBlock is a reference decoder of SIMD blocks, as specified in SPEC_SIMD.md.
// src is the block, including its indicator byte.
// This implementation is not optimized for speed, but for readability.
func DecodeSIMDBlock(src []byte) ([]byte, error) {
	return decodeSIMDBlock(src, false, &SIMDWalker{})
}

// DecodeSIMDBlockStrict is DecodeSIMDBlock, but also rejects matches in chunks with the no-overlap flag
// that rely on their offset being raised, which SPEC_SIMD.md allows. It checks encoders.
func DecodeSIMDBlockStrict(src []byte) ([]byte, error) {
	return decodeSIMDBlock(src, true, &SIMDWalker{})
}

// WalkSIMDBlock is DecodeSIMDBlock, and reports the contents of the block to w.
func WalkSIMDBlock(src []byte, w *SIMDWalker) ([]byte, error) {
	return decodeSIMDBlock(src, false, w)
}

func decodeSIMDBlock(src []byte, strict bool, w *SIMDWalker) ([]byte, error) {
	if len(src) == 0 || src[0] != 0 {
		return nil, errors.New("indicator byte is not 0")
	}
	src = src[1:]
	v, n := binary.Uvarint(src)
	switch {
	case n <= 0:
		return nil, errors.New("invalid header varint")
	case v >= 1<<32:
		return nil, errors.New("header varint above 32 bits")
	case v>>24 != 1:
		return nil, fmt.Errorf("block type %d is not a SIMD block", v>>24)
	}
	size := int(v & (1<<24 - 1))
	switch {
	case size > maxBlockSize:
		return nil, fmt.Errorf("decoded size %d above 8 MiB", size)
	case len(src)+64 > size:
		return nil, fmt.Errorf("block of %d bytes is not 64 bytes smaller than its decoded size %d", len(src), size)
	case n == len(src):
		return nil, errors.New("no flags byte")
	}
	flags := src[n]
	if flags&0xe0 != 0 || flags&7 > 5 {
		return nil, fmt.Errorf("invalid block flags %#x", flags)
	}
	lzSize, body, err := rvarint(src[n+1:])
	if err != nil {
		return nil, fmt.Errorf("LZ section size: %w", err)
	}
	if lzSize > uint64(len(body)) {
		return nil, fmt.Errorf("LZ section size %d larger than the block", lzSize)
	}
	lz := int(lzSize)
	d := simdDecoder{size: size, repeat: 1, out: make([]byte, 0, size), strict: strict, w: w}
	if err := d.literals(body[lz:]); err != nil {
		return nil, err
	}
	if lz > 0 {
		if err := d.chunks(body[:lz], 1024<<(flags&7)); err != nil {
			return nil, err
		}
	}
	d.out = append(d.out, d.lits[d.l:]...)
	if len(d.out) != size {
		return nil, fmt.Errorf("decoded %d bytes, want %d", len(d.out), size)
	}
	return d.out, nil
}

// literals decodes the literal section.
func (d *simdDecoder) literals(sec []byte) error {
	t, sec, err := rvarint(sec)
	if err != nil {
		return fmt.Errorf("literal records size: %w", err)
	}
	if t > uint64(len(sec)) {
		return fmt.Errorf("literal records size %d larger than its section", t)
	}
	r := entryReader{data: sec[:len(sec)-int(t)], recs: sec[len(sec)-int(t):]}
	for len(r.recs) > 0 {
		v, err := d.stream(&r, streamLiterals, -1)
		if err != nil {
			return err
		}
		if len(v) == 0 || len(v) > 65535 {
			return fmt.Errorf("literal block of %d literals", len(v))
		}
		if len(d.lits)+len(v) > d.size {
			return errors.New("more literals than the decoded size")
		}
		d.lits = append(d.lits, v...)
	}
	if len(d.lits) == 0 {
		return errors.New("no literal blocks")
	}
	if len(r.data) != 0 {
		return fmt.Errorf("%d bytes of literal data without an entry", len(r.data))
	}
	return nil
}

// chunks runs the operations of the LZ section.
func (d *simdDecoder) chunks(sec []byte, limit int) error {
	t, sec, err := rvarint(sec)
	if err != nil {
		return fmt.Errorf("chunk records size: %w", err)
	}
	if t > uint64(len(sec)) {
		return fmt.Errorf("chunk records size %d larger than its section", t)
	}
	r := entryReader{data: sec[:len(sec)-int(t)], recs: sec[len(sec)-int(t):]}
	for i := 0; len(r.recs) > 0; i++ {
		flags := r.recs[0]
		r.recs = r.recs[1:]
		if flags&0xf0 != 0 {
			return fmt.Errorf("invalid chunk flags %#x", flags)
		}
		if i == maxChunks {
			return fmt.Errorf("more than %d chunks", maxChunks)
		}
		// The tokens give the number of values of the next streams, and the literal lengths
		// add escapes (SPEC_SIMD.md 4.1). -1 is a number stored with the values.
		var c [streamOffsetBits + 1][]byte
		counts := [streamOffsetBits + 1]int{streamTokens: -1, streamOffsetBits: -1}
		for k := range c {
			if c[k], err = d.stream(&r, k, counts[k]); err != nil {
				return err
			}
			for _, v := range c[k] {
				switch k {
				case streamTokens:
					counts[streamLitLens] += int(v>>1) & 1
					counts[streamOffsets] += int(v&1) ^ 1
					if v>>2 == 33 {
						counts[streamEscapes]++
					}
				case streamLitLens:
					if v == 33 {
						counts[streamEscapes]++
					}
				}
			}
		}
		if err := d.chunk(c, flags, limit); err != nil {
			return err
		}
	}
	if len(r.data) != 0 {
		return fmt.Errorf("%d bytes of chunk data without an entry", len(r.data))
	}
	return nil
}

// stream reads the next entry of r for stream k, and returns the stream's values.
// A pivco block holds count values, or stores their number if count is -1.
func (d *simdDecoder) stream(r *entryReader, k, count int) ([]byte, error) {
	s := &simdStreams[k]
	e, n := binary.Uvarint(r.recs)
	if n <= 0 {
		return nil, fmt.Errorf("%s: invalid entry", s.name)
	}
	r.recs = r.recs[n:]
	size, mode := e>>3, int(e&7)
	if size > uint64(len(r.data)) {
		return nil, fmt.Errorf("%s: stored size %d past the data", s.name, size)
	}
	vals := r.data[:size]
	r.data = r.data[size:]
	tab := 0
	switch {
	case mode == modeStored:
	case mode > modeDeltaLengths:
		return nil, fmt.Errorf("%s: reserved mode %d", s.name, mode)
	case k == streamOffsetBits:
		return nil, fmt.Errorf("offset bits in mode %d", mode)
	case mode == modeDeltaLengths && k != streamLiterals:
		return nil, fmt.Errorf("%s: delta lengths outside the literals", s.name)
	case mode == modeCurrent:
		if d.tables[k] == nil {
			return nil, fmt.Errorf("%s: no current table", s.name)
		}
	default:
		t, tn, err := readTable(k, mode, r.recs)
		if err != nil {
			return nil, err
		}
		r.recs = r.recs[tn:]
		d.tables[k] = t
		tab = tn
	}
	if mode != modeStored {
		if count < 0 {
			if len(vals) < 2 {
				return nil, fmt.Errorf("%s: number of values past the data", s.name)
			}
			count, vals = int(binary.LittleEndian.Uint16(vals)), vals[2:]
		}
		var err error
		if vals, err = d.tables[k].decode(vals, count); err != nil {
			return nil, fmt.Errorf("%s: %w", s.name, err)
		}
	}
	for _, v := range vals {
		if int(v) < s.lo || int(v) > s.hi {
			return nil, fmt.Errorf("%s: value %d outside the stream's range", s.name, v)
		}
	}
	if d.w.Entry != nil {
		d.w.Entry(k, mode, len(vals), int(size), tab)
	}
	return vals, nil
}

// chunk runs the operations of a chunk, given its streams.
func (d *simdDecoder) chunk(c [streamOffsetBits + 1][]byte, flags byte, limit int) error {
	delta := flags&1 != 0
	tok, ll, offc, esc, raw := c[streamTokens], c[streamLitLens], c[streamOffsets], c[streamEscapes], c[streamOffsetBits]
	if len(tok) == 0 || len(tok) > limit {
		return fmt.Errorf("chunk of %d operations, limit %d", len(tok), limit)
	}
	if d.w.Chunk != nil {
		d.w.Chunk(flags, len(tok))
	}
	next := func(s *[]byte, name string) (int, error) {
		if len(*s) == 0 {
			return 0, fmt.Errorf("operation %d: %s missing", d.ops, name)
		}
		v := (*s)[0]
		*s = (*s)[1:]
		return int(v), nil
	}
	// length reads a length code and its escape.
	length := func(code int) (int, error) {
		if code < 33 {
			return code, nil
		}
		e, err := next(&esc, "escape")
		return code + e, err
	}
	nbits, start := 0, len(d.out)
	for _, t := range tok {
		if d.ops == 0 && t&2 == 0 {
			return errors.New("first operation without literals")
		}
		lits := 0
		if t&2 != 0 {
			n, err := next(&ll, "literal length")
			if err == nil {
				n, err = length(n)
			}
			if err != nil {
				return err
			}
			if d.l+n > len(d.lits) {
				return fmt.Errorf("operation %d: reads more literals than the literal section holds", d.ops)
			}
			if len(d.out)+n > d.size {
				return fmt.Errorf("operation %d: output past the decoded size", d.ops)
			}
			p, r := len(d.out), min(d.repeat, len(d.out))
			if flags&2 != 0 {
				// The no-overlap flag raises delta references to 32 first.
				r = min(max(d.repeat, 32), p)
			}
			for j, b := range d.lits[d.l : d.l+n] {
				if delta && j < 32 && r > 0 {
					b += d.out[p-r+j%r]
				}
				d.out = append(d.out, b)
			}
			d.l += n
			lits = n
		}
		if t&1 == 0 {
			sym, err := next(&offc, "offset symbol")
			if err != nil {
				return err
			}
			off, nb := sym, 0
			if sym >= 16 {
				b, m := (sym+16)>>3, (sym+16)&7
				off, nb = (8+m)<<(b-3), b-3
			}
			for i := range nb {
				if nbits >= 8*len(raw) {
					return fmt.Errorf("operation %d: offset bits missing", d.ops)
				}
				off += int(raw[nbits>>3]>>(nbits&7)&1) << i
				nbits++
			}
			d.repeat = off
		}
		ml, err := length(int(t >> 2))
		if err != nil {
			return err
		}
		r := min(d.repeat, len(d.out))
		if flags&2 != 0 {
			if d.strict && r < min(ml, 32) {
				return fmt.Errorf("operation %d: match of %d bytes at offset %d in a chunk without overlaps", d.ops, ml, r)
			}
			r = min(max(d.repeat, min(ml, 32)), len(d.out))
		}
		if ml > 32 && r < 32 {
			return fmt.Errorf("operation %d: match of %d bytes at offset %d", d.ops, ml, r)
		}
		if len(d.out)+ml > d.size {
			return fmt.Errorf("operation %d: output past the decoded size", d.ops)
		}
		for range ml {
			d.out = append(d.out, d.out[len(d.out)-r])
		}
		if d.w.Op != nil {
			d.w.Op(lits, ml, r, t&1 != 0)
		}
		d.ops++
	}
	switch {
	case len(d.out)-start < len(tok):
		return fmt.Errorf("chunk outputs %d bytes with %d operations", len(d.out)-start, len(tok))
	case len(ll) != 0 || len(offc) != 0 || len(esc) != 0:
		return fmt.Errorf("chunk with %d literal lengths, %d offset symbols and %d escapes unused", len(ll), len(offc), len(esc))
	case len(raw) != (nbits+7)/8:
		return fmt.Errorf("chunk with %d bytes of offset bits, reads %d bits", len(raw), nbits)
	}
	return nil
}
