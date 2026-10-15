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
	"sync"

	"github.com/minio/minlz/internal/pivco"
	"github.com/minio/minlz/internal/race"
)

// SIMD blocks are described in SIMD_SPEC.md.

const (
	simdBlockType = 1
	simdMinSaving = 64
	simdMaxChunk  = 1024 << 5
	// simdMaxChunks is the most chunks of a block.
	simdMaxChunks = 128
	simdMaxLen    = 32
	// simdSlack is how far the fast loops write past the end of an operation.
	simdSlack = 32
	// simdPad is the number of readable bytes after decoded streams and literals.
	simdPad = 64
)

// Streams, in chunk order, then the literals.
// simdDelta is only a class layout, used by literals.
const (
	simdTok = iota
	simdLL
	simdOffc
	simdEsc
	simdLits
	simdDelta
)

// Entry modes.
const (
	simdStored = iota
	simdCurrent
	simdCodeLens
	simdClassLens
	simdDeltaLens
)

var simdCodeBytes = [...]int{simdTok: 68, simdLL: 17, simdOffc: 88, simdEsc: 128, simdLits: 128}

// simdOffTab holds the base offset of each offset symbol, and the number of raw bits in the top byte.
// Symbol 0 and symbols above 175 decode as offset 1.
var simdOffTab = func() (t [256]uint32) {
	for s := range t {
		switch {
		case s == 0 || s > 175:
			t[s] = 1
		case s < 16:
			t[s] = uint32(s)
		default:
			b, m := (s+16)>>3, (s+16)&7
			t[s] = uint32(8+m)<<(b-3) | uint32(b-3)<<24
		}
	}
	return t
}()

// simdLayout is a class layout. Values outside the stream's range have class 0xff.
type simdLayout struct {
	class, in [256]uint8
	n         int
}

// simdLayouts are indexed by stream. For tokens, the classes are the match length code groups.
var simdLayouts = [...]simdLayout{
	simdTok: newSIMDLayout(0, 33, simdClassBounds(0, 3, 4, 5, 6, 7, 8, 10, 12, 16, 24, 31, 32, 33)),
	simdLL:  newSIMDLayout(1, 33, simdClassBounds(1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 33)),
	simdOffc: newSIMDLayout(1, 175, func(s int) int {
		if s < 16 {
			return bits.Len(uint(s)) - 1
		}
		return (s + 16) >> 3
	}),
	simdEsc:  newSIMDLayout(0, 255, simdClassBounds(0, 1, 2, 4, 6, 10, 14, 22, 30, 46, 62, 94, 126, 190, 254, 255)),
	simdLits: newSIMDLayout(0, 255, simdLitClass),
	simdDelta: newSIMDLayout(0, 255, func(b int) int {
		if b == 128 {
			return 8
		}
		return bits.Len(uint(min(b, 256-b)))
	}),
}

// newSIMDLayout returns the layout of values lo to hi. Inside a class, values are in ascending order
// and use a complete code of k or k+1 bits.
func newSIMDLayout(lo, hi int, class func(v int) int) (l simdLayout) {
	var cnt, idx [24]int
	for v := range l.class {
		l.class[v] = 0xff
	}
	for v := lo; v <= hi; v++ {
		c := class(v)
		l.class[v] = uint8(c)
		cnt[c]++
		l.n = max(l.n, c+1)
	}
	for v := lo; v <= hi; v++ {
		c := l.class[v]
		k := bits.Len(uint(cnt[c])) - 1
		l.in[v] = uint8(k)
		if idx[c] >= 1<<(k+1)-cnt[c] {
			l.in[v]++
		}
		idx[c]++
	}
	return l
}

// simdClassBounds returns the class of a value, given the last value of each class.
func simdClassBounds(last ...int) func(v int) int {
	return func(v int) int {
		c := 0
		for last[c] < v {
			c++
		}
		return c
	}
}

func simdLitClass(b int) int {
	switch {
	case b == 0:
		return 0
	case b == '\t' || b == '\n' || b == '\r':
		return 1
	case b < ' ' || b == 0x7f:
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
	case b < 0x7f:
		return 9
	case b < 0xc0:
		return 10
	case b >= 0xc2 && b <= 0xf4:
		return 11
	}
	return 12
}

// rvarint reads the reverse varint that ends at the end of b.
// It returns the value and its size, or a size of 0 if it is invalid.
func rvarint(b []byte) (uint64, int) {
	var x uint64
	var s uint
	for i := range min(len(b), binary.MaxVarintLen64) {
		c := b[len(b)-1-i]
		if c < 0x80 {
			if i == binary.MaxVarintLen64-1 && c > 1 {
				return 0, 0
			}
			return x | uint64(c)<<s, i + 1
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0
}

// simdHeader parses the header of a SIMD block without its indicator byte.
// It returns the decoded size and the block from its flags byte.
func simdHeader(src []byte) (size int, block []byte, err error) {
	v, n := binary.Uvarint(src)
	switch {
	case n <= 0 || n >= len(src) || v > math.MaxUint32:
		return 0, nil, ErrCorrupt
	case v>>24 != simdBlockType:
		return 0, nil, ErrUnsupported
	case v&(1<<24-1) > MaxBlockSize:
		return 0, nil, ErrTooLarge
	}
	size = int(v & (1<<24 - 1))
	if f := src[n]; len(src)+simdMinSaving > size || f&0xe0 != 0 || f&7 > 5 {
		return 0, nil, ErrCorrupt
	}
	return size, src[n:], nil
}

type simdDecoder struct {
	pv   pivco.Decoder
	tab  [simdLits + 1]pivco.Table
	have [simdLits + 1]bool
	// lits holds the literals decoded so far. Its capacity holds every literal of the block and simdPad.
	lits []byte

	tok, ll, offc [simdMaxChunk + simdPad]byte
	esc           [2*simdMaxChunk + simdPad]byte
}

// simdChunk holds the streams of a chunk. ll, offc, esc and raw stay readable past their values.
type simdChunk struct {
	tok, ll, offc, esc, raw []byte
	nLL, nOff, nEsc, bits   int
	delta                   bool
}

// simdReader reads stream entries from recs, and their data from body[pos:end].
type simdReader struct {
	recs     []byte
	body     []byte
	pos, end int
}

var simdDecoders sync.Pool

// decodeSIMD decodes a SIMD block, from its flags byte, into dst, which must have the decoded size.
func decodeSIMD(dst, block []byte) error {
	s, _ := simdDecoders.Get().(*simdDecoder)
	if s == nil {
		s = new(simdDecoder)
	}
	race.ReadSlice(block)
	race.WriteSlice(dst)
	ok := s.decode(dst, block)
	simdDecoders.Put(s)
	if !ok {
		// Literals of earlier blocks may have been copied.
		clear(dst)
		return ErrCorrupt
	}
	return nil
}

func (s *simdDecoder) decode(dst, block []byte) bool {
	limit := 1024 << (block[0] & 7)
	body := block[1:]
	lz, n := rvarint(body)
	if n == 0 || lz > uint64(len(body)-n) {
		return false
	}
	lr, ok := s.literals(body[lz:len(body)-n], len(dst))
	if !ok {
		return false
	}
	// The first operation has literals, so offset 0 only occurs as a delta reference at position 0.
	dst[0] = 0
	st := [4]int{0, 0, 1, 0}
	if lz > 0 && !s.ops(dst, body, int(lz), limit, &st, &lr) {
		return false
	}
	if len(lr.recs) > 0 && !s.fill(&lr, 0, math.MaxInt, len(dst)) {
		return false
	}
	d, l, nLits := st[0], st[1], len(s.lits)
	if nLits == 0 || lr.pos != lr.end || d+nLits-l != len(dst) {
		return false
	}
	copy(dst[d:], s.lits[l:nLits])
	return true
}

// literals starts the literal section sec of a block of size bytes, and returns the reader of its literal blocks.
func (s *simdDecoder) literals(sec []byte, size int) (simdReader, bool) {
	s.have = [len(s.have)]bool{}
	t, n := rvarint(sec)
	if n == 0 || t > uint64(len(sec)-n) {
		return simdReader{}, false
	}
	end := len(sec) - n - int(t)
	if cap(s.lits) < size+simdPad {
		s.lits = make([]byte, 0, size+simdPad)
	}
	s.lits = s.lits[:0]
	return simdReader{recs: sec[end : len(sec)-n], body: sec, end: end}, true
}

// fill decodes literal blocks of r until need literals follow position l, or none are left.
// Decoding them just before the chunk that reads them keeps them in cache.
func (s *simdDecoder) fill(r *simdReader, l, need, size int) bool {
	lits := s.lits
	for len(r.recs) > 0 && len(lits)-l < need {
		v, nv, stored, ok := s.stream(r, simdLits, 1, min(pivco.MaxBlockSize, size-len(lits)), lits[len(lits):])
		if !ok {
			return false
		}
		if stored {
			lits = append(lits, v[:nv]...)
		} else {
			lits = lits[:len(lits)+nv]
		}
	}
	// The executors read up to 32 bytes past the literals of a chunk.
	clear(lits[len(lits) : len(lits)+simdPad])
	s.lits = lits
	return true
}

// ops runs the chunks of the LZ section body[:end], with the literal blocks of lr.
func (s *simdDecoder) ops(dst, body []byte, end, limit int, st *[4]int, lr *simdReader) bool {
	t, n := rvarint(body[:end])
	if n == 0 || t > uint64(end-n) {
		return false
	}
	r := simdReader{recs: body[end-n-int(t) : end-n], body: body, end: end - n - int(t)}
	lits := s.lits[:cap(s.lits)]
	for i := 0; len(r.recs) > 0; i++ {
		flags := r.recs[0]
		r.recs = r.recs[1:]
		if flags&0xf0 != 0 || i == simdMaxChunks {
			return false
		}
		c := simdChunk{delta: flags&1 != 0}
		tok, nTok, _, ok := s.stream(&r, simdTok, 1, limit, s.tok[:])
		if !ok {
			return false
		}
		c.tok = tok[:nTok]
		out, nLit, nOff, nEsc, ok := simdTokStats(c.tok)
		if !ok || i == 0 && c.tok[0]&2 == 0 {
			return false
		}
		if c.ll, c.nLL, _, ok = s.stream(&r, simdLL, nLit, nLit, s.ll[:]); !ok {
			return false
		}
		sumLL, nLLEsc, ok := simdLLStats(c.ll[:nLit])
		if !ok {
			return false
		}
		if c.offc, c.nOff, _, ok = s.stream(&r, simdOffc, nOff, nOff, s.offc[:]); !ok {
			return false
		}
		if c.bits, ok = simdOffStats(c.offc[:nOff]); !ok {
			return false
		}
		nEsc += nLLEsc
		if c.esc, c.nEsc, _, ok = s.stream(&r, simdEsc, nEsc, nEsc, s.esc[:]); !ok {
			return false
		}
		// Escapes may be any value, so only their sum is used.
		sumEsc, _, _ := simdLLStats(c.esc[:nEsc])
		out += sumLL + sumEsc
		e, m := binary.Uvarint(r.recs)
		nb := (c.bits + 7) / 8
		if m <= 0 || e != uint64(nb)<<3 || nb > r.end-r.pos || st[0]+out > len(dst) || out < len(c.tok) {
			return false
		}
		r.recs = r.recs[m:]
		c.raw = body[r.pos:]
		r.pos += nb
		// Each literal escape adds at most 255; the escapes of long matches can add far more.
		if need := sumLL + min(sumEsc, 255*nLLEsc); st[1]+need > len(s.lits) && !s.fill(lr, st[1], need, len(dst)) {
			return false
		}
		c.run(dst, lits, out, st)
		// Chunks only use more literals than are decoded once the literal blocks run out.
		if st[3] != 0 || st[1] > len(s.lits) {
			return false
		}
	}
	return r.pos == r.end
}

// stream reads the entry of stream k, which must have lo to hi values, and returns the values.
// Coded values are decoded into buf.
func (s *simdDecoder) stream(r *simdReader, k, lo, hi int, buf []byte) (v []byte, n int, stored, ok bool) {
	e, m := binary.Uvarint(r.recs)
	if m <= 0 || e>>3 > uint64(r.end-r.pos) {
		return nil, 0, false, false
	}
	r.recs = r.recs[m:]
	size, mode := int(e>>3), int(e&7)
	data := r.body[r.pos:]
	r.pos += size
	switch mode {
	case simdStored:
		return data, size, true, size >= lo && size <= hi
	case simdCurrent:
		if !s.have[k] {
			return nil, 0, false, false
		}
	case simdCodeLens, simdClassLens, simdDeltaLens:
		if mode == simdDeltaLens && k != simdLits {
			return nil, 0, false, false
		}
		t, ok := s.setTable(k, mode, r.recs)
		if !ok {
			return nil, 0, false, false
		}
		r.recs = r.recs[t:]
	default:
		return nil, 0, false, false
	}
	// Only tokens and literal blocks store their number of values; lo == hi for the others.
	n, data = lo, data[:size]
	if k == simdTok || k == simdLits {
		if size < 2 {
			return nil, 0, false, false
		}
		n, data = int(binary.LittleEndian.Uint16(data)), data[2:]
		if n < lo || n > hi {
			return nil, 0, false, false
		}
	}
	if s.pv.Decode(buf[:n], data, &s.tab[k]) != nil {
		return nil, 0, false, false
	}
	return buf, n, false, true
}

// setTable reads a new table for stream k from b and returns its size.
func (s *simdDecoder) setTable(k, mode int, b []byte) (int, bool) {
	var lens [256]uint8
	var n int
	switch {
	case mode == simdCodeLens:
		n = simdCodeBytes[k]
		if len(b) < n {
			return 0, false
		}
		for i, v := range b[:n] {
			lens[2*i], lens[2*i+1] = v&15, v>>4
		}
		if (k == simdLL || k == simdOffc) && lens[0] != 0 {
			return 0, false
		}
	case k == simdTok:
		var g [22]uint8
		n = len(g) / 2
		if len(b) < n {
			return 0, false
		}
		for i, v := range b[:n] {
			g[2*i], g[2*i+1] = v&15, v>>4
		}
		if !simdComplete(g[:14]) || !simdComplete(g[14:18]) || !simdComplete(g[18:]) {
			return 0, false
		}
		grp := &simdLayouts[simdTok]
		for v := range 136 {
			ml := v >> 2
			gl, f := g[grp.class[ml]], g[14+4*(v>>7)+v&3]
			lens[v] = (gl + grp.in[ml] + f) & -((gl + 15) >> 4) & -((f + 15) >> 4)
		}
	default:
		lay := &simdLayouts[k]
		if mode == simdDeltaLens {
			lay = &simdLayouts[simdDelta]
		}
		// Class 0xff, outside the stream's range, has length 0.
		var cl [256]uint8
		n = (lay.n + 1) / 2
		if len(b) < n || lay.n&1 != 0 && b[n-1]>>4 != 0 {
			return 0, false
		}
		for i, v := range b[:n] {
			cl[2*i], cl[2*i+1] = v&15, v>>4
		}
		// Branch-free: which classes have codes is data. x+15 has bit 4 set unless x is 0.
		for v, c := range lay.class[:2*simdCodeBytes[min(k, simdLits)]] {
			x := cl[c]
			lens[v] = (x + lay.in[v]) & -((x + 15) >> 4)
		}
	}
	// Lengths are at most 15, so adding 15 to a byte doesn't carry out of it,
	// and the sum of 8 lengths fits a byte.
	used, sum := 0, 0
	for i := 0; i < 2*simdCodeBytes[min(k, simdLits)]; i += 8 {
		x := load64(lens[:], i)
		used += bits.OnesCount64((x + 0x0f0f0f0f0f0f0f0f) & 0x1010101010101010)
		sum += int(x * 0x0101010101010101 >> 56)
	}
	// pivco accepts any length for a lone value.
	if used == 1 && sum != 1 || s.tab[k].SetLengths(&lens) != nil {
		return 0, false
	}
	s.have[k] = true
	return n, true
}

// simdComplete reports whether the lengths l, up to 15, form a complete code.
func simdComplete(l []uint8) bool {
	k := 0
	for _, v := range l {
		if v != 0 {
			k += 1 << (15 - v)
		}
	}
	return k == 1<<15
}

// simdTokStats returns the summed match length codes of tok, and the number of operations with
// literals, with new offsets and with escaped matches. ok is false for tokens above 135.
func simdTokStats(tok []byte) (out, nLit, nOff, nEsc int, ok bool) {
	const lo, hi = 0x0101010101010101, 0x8080808080808080
	out, nLit, nRep, nEsc, i, ok := simdTokStatsAsm(tok)
	var bad uint64
	if !ok {
		bad = hi
	}
	for ; i+8 <= len(tok); i += 8 {
		x := load64(tok, i)
		nLit += bits.OnesCount64(x & (lo << 1))
		nRep += bits.OnesCount64(x & lo)
		// Match length code 33 is 132-135: bits 7 and 2.
		nEsc += bits.OnesCount64(x & (x << 5) & hi)
		bad |= x & ((x & 0x7878787878787878) + 0x7878787878787878)
		y := x >> 2 & 0x3f3f3f3f3f3f3f3f
		y = y&0x00ff00ff00ff00ff + y>>8&0x00ff00ff00ff00ff
		out += int(y * 0x0001000100010001 >> 48)
	}
	for _, t := range tok[i:] {
		out += int(t >> 2)
		nLit += int(t>>1) & 1
		nRep += int(t) & 1
		nEsc += int(t>>7) & int(t>>2) & 1
		bad |= uint64(t) & ((uint64(t) & 0x78) + 0x78)
	}
	return out, nLit, len(tok) - nRep, nEsc, bad&hi == 0
}

// simdLLStats returns the sum of the literal lengths ll, and the number of escapes.
// ok is false for lengths outside 1-33.
func simdLLStats(ll []byte) (sum, nEsc int, ok bool) {
	const lo7, hi = 0x7f7f7f7f7f7f7f7f, 0x8080808080808080
	sum, nEsc, i, ok := simdLLStatsAsm(ll)
	var bad uint64
	if !ok {
		bad = hi
	}
	for ; i+8 <= len(ll); i += 8 {
		x := load64(ll, i)
		z := x ^ 0x2121212121212121
		nEsc += bits.OnesCount64(^(z&lo7 + lo7 | z) & hi)
		bad |= ^(x&lo7 + lo7 | x) | (x&lo7 + 0x5e5e5e5e5e5e5e5e) | x
		y := x&0x00ff00ff00ff00ff + x>>8&0x00ff00ff00ff00ff
		sum += int(y * 0x0001000100010001 >> 48)
	}
	for _, v := range ll[i:] {
		sum += int(v)
		if v == simdMaxLen+1 {
			nEsc++
		}
		if v-1 > simdMaxLen {
			bad = hi
		}
	}
	return sum, nEsc, bad&hi == 0
}

// simdOffStats returns the number of raw bits of the offset symbols offc.
// ok is false for symbols outside 1-175.
func simdOffStats(offc []byte) (n int, ok bool) {
	const lo7, hi = 0x7f7f7f7f7f7f7f7f, 0x8080808080808080
	n, i, ok := simdOffStatsAsm(offc)
	var bad uint64
	if !ok {
		bad = hi
	}
	for ; i+8 <= len(offc); i += 8 {
		x := load64(offc, i)
		// Zero bytes, and bytes of 176 and above.
		bad |= ^(x&lo7 + lo7 | x) | x&(x&lo7+0x5050505050505050)
		// The raw bits of symbol s are max(s>>3 - 1, 0).
		y := x >> 3 & 0x1f1f1f1f1f1f1f1f
		n += int(y*0x0101010101010101>>56) - bits.OnesCount64((y&lo7+lo7|y)&hi)
	}
	for _, s := range offc[i:] {
		n += int(simdOffTab[s] >> 24)
		if s-1 >= 175 {
			bad = hi
		}
	}
	return n, bad&hi == 0
}

// run executes the operations of c, which output out bytes.
func (c *simdChunk) run(dst, lits []byte, out int, st *[4]int) {
	k, cur := len(c.tok), [4]int{c.nLL, c.nOff, c.nEsc, c.bits}
	if end := st[0] + out; end+simdSlack > len(dst) {
		k, cur = c.split(end, len(dst))
	}
	if k > 0 {
		simdExec(dst, lits, c, k, st)
	}
	if k < len(c.tok) {
		simdExecExact(dst, lits, c, k, cur, st)
	}
}

// split returns how many operations of c end at least simdSlack bytes before limit,
// and the stream positions after them. end is the output position after c.
func (c *simdChunk) split(end, limit int) (k int, cur [4]int) {
	li, oi, gi, pos := c.nLL, c.nOff, c.nEsc, c.bits
	for k = len(c.tok); k > 0 && end+simdSlack > limit; {
		k--
		t := c.tok[k]
		n := int(t >> 2)
		// The match escape follows the literal length escape.
		if n > simdMaxLen {
			gi--
			n += int(c.esc[gi])
		}
		if t&2 != 0 {
			li--
			ll := int(c.ll[li])
			if ll > simdMaxLen {
				gi--
				ll += int(c.esc[gi])
			}
			n += ll
		}
		if t&1 == 0 {
			oi--
			pos -= int(simdOffTab[c.offc[oi]] >> 24)
		}
		end -= n
	}
	return k, [4]int{li, oi, gi, pos}
}

// simdExecGo runs the first n operations of c.
// They must end at least simdSlack bytes before the end of dst.
func simdExecGo(dst, lits []byte, c *simdChunk, n int, st *[4]int) {
	d, l, last := st[0], st[1], st[2]
	li, oi, gi, pos := 0, 0, 0, 0
	for _, t := range c.tok[:n] {
		lit := int(t>>1) & 1
		nrep := int(t&1) ^ 1
		ll := int(load8(c.ll, li)) & -lit
		li += lit
		ot := simdOffTab[load8(c.offc, oi)]
		oi += nrep
		nb := int(ot >> 24)
		o := int(ot&0xffffff) + int(load64(c.raw, pos>>3)>>(pos&7)&(1<<nb-1))
		pos += nb & -nrep
		lv := load256(lits, l)
		if c.delta {
			simdDeltaRef(&lv, dst, d, min(last, d))
		}
		store256(dst, d, lv)
		if ll > simdMaxLen {
			ll += int(c.esc[gi])
			gi++
			for j := 32; j < ll; j += 32 {
				store256(dst, d+j, load256(lits, l+j))
			}
		}
		d += ll
		l += ll
		if nrep != 0 {
			last = o
		}
		r := min(last, d)
		ml := int(t >> 2)
		if ml > simdMaxLen {
			ml += int(c.esc[gi])
			gi++
			if r < simdMaxLen {
				st[3] = 1
			}
		}
		if r < min(ml, simdMaxLen) {
			for j := range ml {
				store8(dst, d+j, load8(dst, d-r+j))
			}
		} else {
			for j := 0; j == 0 || j < ml; j += 32 {
				store256(dst, d+j, load256(dst, d-r+j))
			}
		}
		d += ml
	}
	st[0], st[1], st[2] = d, l, last
}

// simdDeltaRef adds the delta references of literals at d to lv, for repeat offset r.
func simdDeltaRef(lv *[32]byte, dst []byte, d, r int) {
	if r >= 32 {
		ref := load256(dst, d-r)
		for j := range lv {
			lv[j] += ref[j]
		}
		return
	}
	// r is only 0 at position 0, where dst[0] is 0.
	rs, r := d-r, max(r, 1)
	for j, k := 0, 0; j < len(lv); j++ {
		lv[j] += load8(dst, rs+k)
		if k++; k == r {
			k = 0
		}
	}
}

// simdExecExact runs the operations of c from k on, from stream positions cur.
// It writes nothing past the operations.
func simdExecExact(dst, lits []byte, c *simdChunk, k int, cur [4]int, st *[4]int) {
	d, l, last := st[0], st[1], st[2]
	li, oi, gi, pos := cur[0], cur[1], cur[2], cur[3]
	for _, t := range c.tok[k:] {
		if t&2 != 0 {
			ll := int(c.ll[li])
			li++
			if ll > simdMaxLen {
				ll += int(c.esc[gi])
				gi++
			}
			copy(dst[d:d+ll], lits[l:l+ll])
			if r := min(last, d); c.delta && r > 0 {
				for j := range min(ll, 32) {
					dst[d+j] += dst[d-r+j%r]
				}
			}
			d += ll
			l += ll
		}
		if t&1 == 0 {
			ot := simdOffTab[c.offc[oi]]
			oi++
			nb := int(ot >> 24)
			last = int(ot&0xffffff) + int(load64(c.raw, pos>>3)>>(pos&7)&(1<<nb-1))
			pos += nb
		}
		r := min(last, d)
		ml := int(t >> 2)
		if ml > simdMaxLen {
			ml += int(c.esc[gi])
			gi++
			if r < simdMaxLen {
				st[3] = 1
			}
		}
		if r >= ml {
			copy(dst[d:d+ml], dst[d-r:])
		} else {
			for j := range ml {
				dst[d+j] = dst[d-r+j]
			}
		}
		d += ml
	}
	st[0], st[1], st[2] = d, l, last
}
