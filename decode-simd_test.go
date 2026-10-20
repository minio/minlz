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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"math/rand"
	"os"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/minio/minlz/internal/guard"
	"github.com/minio/minlz/internal/pivco"
	"github.com/minio/minlz/internal/race"
	"github.com/minio/minlz/internal/reference"
	"github.com/zeebo/xxh3"
)

// The test block builder below is written from SPEC_SIMD.md, independently of the encoder.

// simdTestOp copies ll literals, then ml bytes from offset off back. off 0 repeats the offset.
type simdTestOp struct{ ll, ml, off int }

// simdTestRun executes ops as specified, and appends the literals left.
func simdTestRun(ops []simdTestOp, lits []byte) []byte {
	var out []byte
	l, last := 0, 1
	for _, o := range ops {
		out = append(out, lits[l:l+o.ll]...)
		l += o.ll
		if o.off != 0 {
			last = o.off
		}
		r := min(last, len(out))
		for range o.ml {
			// Only invalid blocks match at position 0.
			if r == 0 {
				out = append(out, 0)
				continue
			}
			out = append(out, out[len(out)-r])
		}
	}
	return append(out, lits[l:]...)
}

func simdTestRvarint(b []byte, v uint64) []byte {
	t := binary.AppendUvarint(nil, v)
	slices.Reverse(t)
	return append(b, t...)
}

func simdTestNibbles(l []uint8) []byte {
	b := make([]byte, (len(l)+1)/2)
	for i, v := range l {
		b[i/2] |= v << (4 * (i & 1))
	}
	return b
}

type simdTestStream struct {
	mode  int
	table []byte
	data  []byte
}

func (s *simdTestStream) appendEntry(b []byte) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s.data))<<3|uint64(s.mode)), s.table...)
}

type simdTestChunk struct {
	flags byte
	s     [5]simdTestStream
	ops   []simdTestOp
	vals  [5][]byte
}

// simdTestEnc is a block in parts, so tests can change them.
type simdTestEnc struct {
	typ, size     int
	flags         byte
	lits          []simdTestStream
	chunks        []simdTestChunk
	noLZ          bool
	litExtra      []byte // literal data without an entry
	lzExtra       []byte
	lzCut         int // chunk data bytes dropped from the end, entries unchanged
	litRecExtra   []byte
	chunkRecExtra []byte
	stored        []byte // literals as stored
}

func (e *simdTestEnc) bytes() []byte {
	out := binary.AppendUvarint([]byte{0}, uint64(e.typ)<<24|uint64(e.size))
	out = append(out, e.flags)
	start := len(out)
	var recs []byte
	if !e.noLZ {
		for i := range e.chunks {
			c := &e.chunks[i]
			recs = append(recs, c.flags)
			for k := range c.s {
				out = append(out, c.s[k].data...)
				recs = c.s[k].appendEntry(recs)
			}
		}
		out = append(out[:len(out)-e.lzCut], e.lzExtra...)
		recs = append(recs, e.chunkRecExtra...)
		out = simdTestRvarint(append(out, recs...), uint64(len(recs)))
	}
	lz := len(out) - start
	recs = recs[:0]
	for i := range e.lits {
		out = append(out, e.lits[i].data...)
		recs = e.lits[i].appendEntry(recs)
	}
	out = append(out, e.litExtra...)
	recs = append(recs, e.litRecExtra...)
	out = simdTestRvarint(append(out, recs...), uint64(len(recs)))
	return simdTestRvarint(out, uint64(lz))
}

// simdTestBlock describes a block for simdTestBuilder.
type simdTestBlock struct {
	ops      []simdTestOp
	lits     []byte // output literal values
	chunk    int    // operations per chunk
	limit    int    // chunk limit value
	delta    uint64 // chunk i has delta literals if bit i%64 is set
	litBlock int    // literals per literal block
	modes    []int  // entry modes, cycled
	litModes []int
}

type simdTestBuilder struct {
	t   testing.TB
	enc pivco.Encoder
	cur [simdLits + 1]*pivco.Table
}

func newSIMDTestBuilder(t testing.TB) *simdTestBuilder {
	return &simdTestBuilder{t: t}
}

// code returns v coded with t as stored in an entry of stream k: a pivco block,
// after the number of values for tokens and literals (SPEC_SIMD.md 2.2).
func (b *simdTestBuilder) code(k int, v []byte, t *pivco.Table) ([]byte, error) {
	var h [256]uint64
	for _, c := range v {
		h[c]++
	}
	size := t.BlockSize(h[:])
	if size < 0 {
		return nil, pivco.ErrSymbolNotInTable
	}
	var d []byte
	if k == simdTok || k == simdLits {
		d = binary.LittleEndian.AppendUint16(d, uint16(len(v)))
	}
	n := len(d)
	d = append(d, make([]byte, size)...)
	return d, b.enc.Encode(d[n:], v, t)
}

// simdTestClassLens returns class lengths that give every value of stream k a code.
func simdTestClassLens(k, mode int) (cl []uint8, lens [256]uint8) {
	trunc := func(n int) []uint8 {
		k := bits.Len(uint(n)) - 1
		l := make([]uint8, n)
		for i := range l {
			l[i] = uint8(k)
			if i >= 1<<(k+1)-n {
				l[i]++
			}
		}
		return l
	}
	lay := &simdLayouts[k]
	if mode == simdDeltaLens {
		lay = &simdLayouts[simdDelta]
	}
	cl = trunc(lay.n)
	if k == simdTok {
		cl = append(cl, 2, 2, 2, 2, 2, 2, 2, 2)
		for v := range 136 {
			lens[v] = cl[lay.class[v>>2]] + lay.in[v>>2] + 2
		}
		return cl, lens
	}
	for v, c := range lay.class {
		if c != 0xff {
			lens[v] = cl[c] + lay.in[v]
		}
	}
	return cl, lens
}

// stream codes the values of stream k with mode, or stores them if that isn't possible.
func (b *simdTestBuilder) stream(k, mode int, v []byte) simdTestStream {
	s := simdTestStream{mode: mode}
	if len(v) == 0 || mode == simdStored || mode == simdDeltaLens && k != simdLits {
		s.mode, s.data = simdStored, v
		return s
	}
	t := b.cur[k]
	if mode == simdCurrent && t != nil {
		if d, err := b.code(k, v, t); err == nil {
			s.data = d
			return s
		}
	}
	var lens [256]uint8
	switch mode {
	case simdClassLens, simdDeltaLens:
		var cl []uint8
		cl, lens = simdTestClassLens(k, mode)
		s.table = simdTestNibbles(cl)
	default:
		s.mode = simdCodeLens
		var h [256]uint64
		for _, c := range v {
			h[c]++
		}
		if !pivco.BuildLengths(h[:], &lens) {
			b.t.Fatal("no values")
		}
		s.table = simdTestNibbles(lens[:2*simdCodeBytes[k]])
	}
	t = new(pivco.Table)
	if err := t.SetLengths(&lens); err != nil {
		b.t.Fatal(err)
	}
	b.cur[k] = t
	d, err := b.code(k, v, t)
	if err != nil {
		b.t.Fatal(err)
	}
	s.data = d
	return s
}

// simdTestOffset returns the offset symbol of off, its raw bits and their number.
func simdTestOffset(off int) (sym byte, raw uint64, n int) {
	if off < 16 {
		return byte(off), 0, 0
	}
	b := bits.Len(uint(off)) - 1
	return byte(8*b - 16 + off>>(b-3)&7), uint64(off) & (1<<(b-3) - 1), b - 3
}

// simdTestValues returns the values of the streams of ops.
func simdTestValues(ops []simdTestOp) (s [5][]byte) {
	var acc uint64
	var nacc int
	for _, o := range ops {
		ll, ml := o.ll, o.ml
		var t byte
		if ll > 0 {
			t |= 2
			if ll > simdMaxLen {
				s[simdEsc] = append(s[simdEsc], byte(ll-simdMaxLen-1))
				ll = simdMaxLen + 1
			}
			s[simdLL] = append(s[simdLL], byte(ll))
		}
		if ml > simdMaxLen {
			s[simdEsc] = append(s[simdEsc], byte(ml-simdMaxLen-1))
			ml = simdMaxLen + 1
		}
		t |= byte(ml << 2)
		if o.off == 0 {
			t |= 1
		} else {
			sym, raw, n := simdTestOffset(o.off)
			s[simdOffc] = append(s[simdOffc], sym)
			acc |= raw << nacc
			for nacc += n; nacc >= 8; nacc -= 8 {
				s[4] = append(s[4], byte(acc))
				acc >>= 8
			}
		}
		s[simdTok] = append(s[simdTok], t)
	}
	if nacc > 0 {
		s[4] = append(s[4], byte(acc))
	}
	return s
}

// build returns the block parts and the decoded output.
func (b *simdTestBuilder) build(tb simdTestBlock) (*simdTestEnc, []byte) {
	b.cur = [len(b.cur)]*pivco.Table{}
	out := simdTestRun(tb.ops, tb.lits)
	if tb.chunk == 0 {
		tb.chunk = 1024 << tb.limit
	}
	if tb.litBlock == 0 {
		tb.litBlock = pivco.MaxBlockSize
	}
	modes, litModes := tb.modes, tb.litModes
	if len(modes) == 0 {
		modes = []int{simdStored}
	}
	if len(litModes) == 0 {
		litModes = []int{simdStored}
	}
	e := &simdTestEnc{typ: simdBlockType, size: len(out), flags: byte(tb.limit), stored: bytes.Clone(tb.lits)}
	p, l, last := 0, 0, 1
	for i, o := range tb.ops {
		ci := i / tb.chunk
		if tb.delta>>(ci&63)&1 != 0 {
			r := min(last, p)
			for j := range min(o.ll, 32) {
				if r > 0 {
					e.stored[l+j] -= out[p-r+j%r]
				}
			}
		}
		p += o.ll + o.ml
		l += o.ll
		if o.off != 0 {
			last = o.off
		}
	}
	for i := 0; i < len(e.stored); i += tb.litBlock {
		v := e.stored[i:min(i+tb.litBlock, len(e.stored))]
		e.lits = append(e.lits, b.stream(simdLits, litModes[len(e.lits)%len(litModes)], v))
	}
	for i := 0; i < len(tb.ops); i += tb.chunk {
		c := simdTestChunk{ops: tb.ops[i:min(i+tb.chunk, len(tb.ops))]}
		ci := len(e.chunks)
		if tb.delta>>(ci&63)&1 != 0 {
			c.flags = 1
		}
		c.vals = simdTestValues(c.ops)
		for k := range 4 {
			c.s[k] = b.stream(k, modes[(ci+k)%len(modes)], c.vals[k])
		}
		c.s[4] = simdTestStream{data: c.vals[4]}
		e.chunks = append(e.chunks, c)
	}
	return e, out
}

// simdTestOps returns random operations and their literals.
// Literals are drawn from alphabet, matches are at most maxML and offsets at most maxOff.
func simdTestOps(rng *rand.Rand, n int, alphabet string, maxML, maxOff int) ([]simdTestOp, []byte) {
	var ops []simdTestOp
	var lits []byte
	pos, last := 0, 1
	for i := range n {
		var o simdTestOp
		if i == 0 || rng.Intn(3) > 0 {
			o.ll = 1 + rng.Intn(8)
			if rng.Intn(20) == 0 {
				o.ll = 1 + rng.Intn(simdMaxLen+256)
			}
		}
		for range o.ll {
			lits = append(lits, alphabet[rng.Intn(len(alphabet))])
		}
		pos += o.ll
		o.ml = rng.Intn(min(maxML, simdMaxLen) + 1)
		if rng.Intn(10) == 0 {
			o.ml = rng.Intn(maxML + 1)
		}
		switch {
		case rng.Intn(4) == 0:
			// Repeat.
		case rng.Intn(20) == 0:
			// Clamped to the position.
			o.off = 1 + rng.Intn(min(maxOff, 1<<24-1))
		default:
			o.off = 1 + rng.Intn(max(1, min(pos, maxOff)))
		}
		r := last
		if o.off != 0 {
			r = o.off
		}
		if o.ml > simdMaxLen && min(r, pos) < simdMaxLen {
			o.ml = simdMaxLen
		}
		if o.off != 0 {
			last = o.off
		}
		pos += o.ml
		ops = append(ops, o)
	}
	for range rng.Intn(50) {
		lits = append(lits, alphabet[rng.Intn(len(alphabet))])
	}
	return ops, lits
}

// simdTestPad appends repeats until the block is 64 bytes smaller than its output.
func simdTestPad(b *simdTestBuilder, tb *simdTestBlock) (*simdTestEnc, []byte) {
	for {
		e, out := b.build(*tb)
		if len(e.bytes())-1+simdMinSaving <= len(out) {
			return e, out
		}
		for range 32 {
			tb.ops = append(tb.ops, simdTestOp{ml: simdMaxLen})
		}
	}
}

func simdTestDecode(t testing.TB, block, want []byte) {
	t.Helper()
	n, err := DecodedLen(block)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Fatalf("decoded length %d, want %d", n, len(want))
	}
	// Guard bytes after the capacity.
	buf := bytes.Repeat([]byte{0xfe}, n+64)
	got, err := Decode(buf[:0:n], block)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("mismatch at %d of %d: got %d, want %d", i, len(want), got[i], want[i])
			}
		}
	}
	for _, v := range buf[n:] {
		if v != 0xfe {
			t.Fatal("wrote past dst")
		}
	}
	// Hashing chunk by chunk gives the hash of the output.
	_, sb, err := simdHeader(block[1:])
	if err != nil {
		t.Fatal(err)
	}
	if h, err := decodeSIMDHash(make([]byte, n), sb, true); err != nil || h != xxh3.Hash(want) {
		t.Fatalf("hash %x, want %x (%v)", h, xxh3.Hash(want), err)
	}
}

func TestSIMDRvarint(t *testing.T) {
	for _, tc := range []struct {
		v uint64
		b []byte
	}{{5, []byte{0x05}}, {300, []byte{0x02, 0xac}}, {100000, []byte{0x06, 0x8d, 0xa0}}} {
		if got := simdTestRvarint(nil, tc.v); !bytes.Equal(got, tc.b) {
			t.Errorf("%d: got %x, want %x", tc.v, got, tc.b)
		}
		v, n := rvarint(append([]byte{0x80, 0xff}, tc.b...))
		if v != tc.v || n != len(tc.b) {
			t.Errorf("%x: got %d, %d", tc.b, v, n)
		}
	}
	for _, v := range []uint64{0, 127, 128, 16383, 16384, 1 << 21, 1<<35 + 1, 1<<63 + 5, ^uint64(0)} {
		b := simdTestRvarint([]byte{0x80}, v)
		if got, n := rvarint(b); got != v || n != len(b)-1 {
			t.Errorf("%d: got %d, %d", v, got, n)
		}
		if _, n := rvarint(b[2:]); n != 0 && v >= 128 {
			t.Errorf("%d: truncated varint accepted", v)
		}
	}
	if _, n := rvarint(bytes.Repeat([]byte{0x80}, 10)); n != 0 {
		t.Error("unterminated varint accepted")
	}
	if _, n := rvarint(append([]byte{2}, bytes.Repeat([]byte{0xff}, 9)...)); n != 0 {
		t.Error("overflow accepted")
	}
}

func TestSIMDOffsets(t *testing.T) {
	next := 1
	for s := 1; s <= 175; s++ {
		base, n := int(simdOffTab[s]&0xffffff), int(simdOffTab[s]>>24)
		if base != next {
			t.Fatalf("symbol %d: base %d, want %d", s, base, next)
		}
		next = base + 1<<n
		for _, off := range []int{base, base + 1<<n - 1} {
			if sym, raw, nb := simdTestOffset(off); int(sym) != s || nb != n || int(raw) != off-base {
				t.Fatalf("offset %d: symbol %d/%d bits, want %d/%d", off, sym, nb, s, n)
			}
		}
	}
	if next != 1<<24 {
		t.Fatalf("offsets end at %d", next)
	}
	for s, want := range map[int][2]int{16: {16, 17}, 24: {32, 35}, 175: {15728640, 16777215}} {
		base, n := int(simdOffTab[s]&0xffffff), int(simdOffTab[s]>>24)
		if base != want[0] || base+1<<n-1 != want[1] {
			t.Errorf("symbol %d: %d-%d, want %v", s, base, base+1<<n-1, want)
		}
	}
	if simdOffTab[0] != 1 {
		t.Error("symbol 0 must decode as offset 1")
	}
}

// TestSIMDLayoutsSpec compares the class layouts with the arrays in SPEC_SIMD.md.
func TestSIMDLayoutsSpec(t *testing.T) {
	spec, err := os.ReadFile("SPEC_SIMD.md")
	if err != nil {
		t.Fatal(err)
	}
	streams := map[string]int{"A.1": simdLL, "A.2": simdEsc, "A.3": simdOffc, "A.4": simdTok, "A.5": simdLits, "A.6": simdDelta}
	caption := regexp.MustCompile(`^(Class|In-class length), [a-z ]+ (\d+)-(\d+):$`)
	found := map[string]int{}
	section, kind, lo, idx := "", "", 0, 0
	for line := range strings.Lines(strings.ReplaceAll(string(spec), "\r", "")) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "## A.") {
			section, kind = line[3:6], ""
			continue
		}
		k, ok := streams[section]
		if !ok {
			continue
		}
		if m := caption.FindStringSubmatch(line); m != nil {
			kind, idx = m[1], 0
			lo, _ = strconv.Atoi(m[2])
			hi, _ := strconv.Atoi(m[3])
			found[section+kind] = hi - lo + 1
			continue
		}
		if line == "" || line == "```" {
			kind = ""
			continue
		}
		if kind == "" {
			continue
		}
		for f := range strings.SplitSeq(line, ",") {
			if f = strings.TrimSpace(f); f == "" {
				continue
			}
			want, err := strconv.Atoi(f)
			if err != nil {
				t.Fatalf("%s: %q", section, line)
			}
			v, l := lo+idx, &simdLayouts[k]
			got := int(l.class[v])
			if kind != "Class" {
				got = int(l.in[v])
			}
			if got != want {
				t.Errorf("%s %s of %d: got %d, want %d", section, kind, v, got, want)
			}
			idx++
			found[section+kind]--
		}
	}
	for s := range streams {
		for _, kind := range []string{"Class", "In-class length"} {
			if n, ok := found[s+kind]; !ok || n != 0 {
				t.Errorf("%s %s: %d values missing", s, kind, n)
			}
		}
	}
	// Every in-class code is complete, and the table sizes are those of 4.2.
	for k, want := range map[int]int{simdTok: 7, simdLL: 6, simdOffc: 12, simdEsc: 8, simdLits: 7, simdDelta: 5} {
		l := &simdLayouts[k]
		var kraft [24]float64
		for v, c := range l.class {
			if c != 0xff {
				kraft[c] += 1 / float64(int(1)<<l.in[v])
			}
		}
		for c := range l.n {
			if kraft[c] != 1 {
				t.Errorf("stream %d class %d: Kraft sum %v", k, c, kraft[c])
			}
		}
		if got := (l.n + 1) / 2; got != want {
			t.Errorf("stream %d: %d bytes, want %d", k, got, want)
		}
	}
}

func TestSIMDDecodeBuilt(t *testing.T) { simdTiers(t, testSIMDDecodeBuilt) }

func testSIMDDecodeBuilt(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	b := newSIMDTestBuilder(t)
	modes := []int{simdStored, simdCodeLens, simdCurrent, simdClassLens, simdCurrent}
	litModes := []int{simdStored, simdCodeLens, simdCurrent, simdClassLens, simdDeltaLens}
	for i := range 40 {
		ops, lits := simdTestOps(rng, 1+rng.Intn(5000), "aaabbcdef\x00\xff", 288, 1<<rng.Intn(25))
		tb := simdTestBlock{ops: ops, lits: lits, limit: rng.Intn(6), chunk: 1 + rng.Intn(1500), litBlock: 1 + rng.Intn(9000),
			modes: modes[:1+i%len(modes)], litModes: litModes[:1+i%len(litModes)]}
		tb.chunk = min(tb.chunk, 1024<<tb.limit)
		if i%3 == 1 {
			tb.delta = rng.Uint64()
		}
		e, want := simdTestPad(b, &tb)
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			simdTestDecode(t, e.bytes(), want)
		})
	}
}

// TestSIMDStoredKeepsTable checks that a stored entry keeps the current table (SPEC_SIMD.md 2.2),
// in the chunks and the literal blocks.
func TestSIMDStoredKeepsTable(t *testing.T) {
	ops := make([]simdTestOp, 3*64)
	for i := range ops {
		ops[i] = simdTestOp{ll: 4, ml: 200, off: 40}
	}
	// The first match must not be clamped below 32 (4.3).
	ops[0].ll = 40
	b := newSIMDTestBuilder(t)
	modes := []int{simdCodeLens, simdStored, simdCurrent}
	e, want := b.build(simdTestBlock{ops: ops, lits: bytes.Repeat([]byte("abcd"), len(ops)+9),
		chunk: 64, litBlock: 256, modes: modes, litModes: modes})
	for i, m := range modes {
		if e.chunks[i].s[simdTok].mode != m || e.lits[i].mode != m {
			t.Fatalf("entry %d: token mode %d, literal mode %d, want %d", i, e.chunks[i].s[simdTok].mode, e.lits[i].mode, m)
		}
	}
	simdTestDecode(t, e.bytes(), want)
	if ref, err := reference.DecodeSIMDBlock(e.bytes()); err != nil || !bytes.Equal(ref, want) {
		t.Fatal("reference:", err)
	}
}

func TestSIMDDeltaExample(t *testing.T) {
	// SPEC_SIMD.md 4.5: the output ends with 41 42 43 07 00 41 42 43, and the repeat offset is 5.
	lits := []byte{0x41, 0x42, 0x43, 0x07, 0x00, 0x41, 0x42, 0x43, 0x07, 0x01, 0x41, 0x42, 0x44, 0x07}
	ops := []simdTestOp{{ll: 8, ml: 0, off: 5}, {ll: 6, ml: 4, off: 0}}
	b := newSIMDTestBuilder(t)
	tb := simdTestBlock{ops: ops, lits: lits, chunk: 1, delta: 2}
	e, want := simdTestPad(b, &tb)
	if !bytes.Equal(e.stored[8:], []byte{0x00, 0x01, 0x00, 0x00, 0x01, 0x00}) {
		t.Fatalf("stored %x", e.stored[8:])
	}
	simdTestDecode(t, e.bytes(), want)
}

// simdTestExecs are the operation loops to compare. dispatch runs the assembly loops of each tier.
var simdTestExecs = []struct {
	name string
	fn   func(dst, lits []byte, c *simdChunk, n int, st *[4]int)
}{{"go", simdExecGo}, {"dispatch", simdExec}, {"exact", func(dst, lits []byte, c *simdChunk, n int, st *[4]int) {
	simdExecExact(dst, lits, &simdChunk{tok: c.tok[:n], ll: c.ll, offc: c.offc, esc: c.esc, raw: c.raw, delta: c.delta}, 0, [4]int{}, st)
}}}

func TestSIMDExec(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	b := newSIMDTestBuilder(t)
	type tcase struct {
		ops  []simdTestOp
		lits []byte
		bad  bool // a match longer than 32 bytes has an offset below 32
	}
	var cases []tcase
	for d0 := 1; d0 <= 40; d0 += 3 {
		for off := 1; off <= 40; off++ {
			for ml := 0; ml <= 40; ml += 3 {
				lits := make([]byte, d0+40)
				rng.Read(lits)
				// The second match starts at d0+5.
				bad := ml > simdMaxLen && min(off, d0+5) < simdMaxLen
				cases = append(cases, tcase{[]simdTestOp{{ll: d0, ml: 3, off: 7}, {ll: 2, ml: ml, off: off}, {ll: 35, ml: 5, off: 0}}, lits, bad})
			}
		}
	}
	for range 200 {
		ops, lits := simdTestOps(rng, 1+rng.Intn(300), "abcab\x00", 288, 1<<rng.Intn(16))
		cases = append(cases, tcase{ops, lits, false})
	}
	// Buffers end at an inaccessible page, right after what simdExecGo reads or writes.
	var guards [7][]byte
	for i := range guards {
		g, free, err := guard.Map(1<<20, true)
		if errors.Is(err, errors.ErrUnsupported) {
			g, free, err = make([]byte, 1<<20), func() {}, nil
		}
		if err != nil {
			t.Fatal(err)
		}
		defer free()
		guards[i] = g
	}
	// at returns the last n bytes of guard i, starting with b.
	at := func(i int, b []byte, n int) []byte {
		s := guards[i][len(guards[i])-n:]
		clear(s[copy(s, b):])
		return s
	}
	simdTiers(t, func(t *testing.T) {
		for _, ex := range simdTestExecs {
			for i, tc := range cases {
				for _, delta := range []bool{false, true} {
					tb := simdTestBlock{ops: tc.ops, lits: tc.lits, chunk: len(tc.ops)}
					if delta {
						tb.delta = 1
					}
					e, want := b.build(tb)
					v := e.chunks[0].vals
					l := 0
					for _, o := range tc.ops {
						l += o.ll
					}
					d := l + len(want) - len(e.stored)
					bits, _ := simdOffStats(v[simdOffc])
					// ll and offc are read one value ahead, raw 8 bytes at each bit position. simdExec takes &c.esc[0].
					c := simdChunk{tok: at(0, v[simdTok], len(v[simdTok])), ll: at(1, v[simdLL], len(v[simdLL])+1),
						offc: at(2, v[simdOffc], len(v[simdOffc])+1), esc: at(3, v[simdEsc], max(len(v[simdEsc]), 1)),
						raw: at(4, v[4], bits>>3+8), delta: delta}
					dst := at(5, nil, d+simdSlack)
					lits := at(6, e.stored[:l], l+simdSlack)
					st := [4]int{0, 0, 1, 0}
					ex.fn(dst, lits, &c, len(c.tok), &st)
					if tc.bad {
						if st[3] == 0 {
							t.Fatalf("%s case %d delta %v: long match below offset 32 not flagged", ex.name, i, delta)
						}
						continue
					}
					if st[3] != 0 || st[0] != d || st[1] != l || !bytes.Equal(dst[:d], want[:d]) {
						t.Fatalf("%s case %d delta %v: state %v, output mismatch %v", ex.name, i, delta, st, !bytes.Equal(dst[:d], want[:d]))
					}
				}
			}
		}
	})
}

// TestSIMDExecEmpty checks that running no operations writes nothing and keeps the state.
func TestSIMDExecEmpty(t *testing.T) { simdTiers(t, testSIMDExecEmpty) }

func testSIMDExecEmpty(t *testing.T) {
	// 1 is a repeat with no match, a literal length and an offset symbol without raw bits.
	s := bytes.Repeat([]byte{1}, simdPad)
	lits := bytes.Repeat([]byte{0xff}, 100)
	st0 := [4]int{40, 10, 9, 0}
	for _, ex := range simdTestExecs {
		for _, delta := range []bool{false, true} {
			c := simdChunk{tok: s, ll: s, offc: s, esc: s, raw: s, delta: delta}
			dst := make([]byte, 100)
			st := st0
			ex.fn(dst, lits, &c, 0, &st)
			if written := !bytes.Equal(dst, make([]byte, len(dst))); st != st0 || written {
				t.Fatalf("%s delta %v: state %v, output written %v", ex.name, delta, st, written)
			}
		}
	}
}

// TestSIMDExecRandom compares simdExec with simdExecGo on random streams. Offsets use every symbol,
// and most chunks start late enough in dst that even the largest are not clamped.
// Only a prefix of the operations runs, as before the exact loop.
func TestSIMDExecRandom(t *testing.T) { simdTiers(t, testSIMDExecRandom) }

func testSIMDExecRandom(t *testing.T) {
	const hist = 1 << 24
	rng := rand.New(rand.NewSource(8))
	base := make([]byte, hist+1<<20)
	rng.Read(base)
	want, got := bytes.Clone(base), bytes.Clone(base)
	lits := make([]byte, 400*(simdMaxLen+256)+1000+simdPad)
	pad := func(b []byte) []byte { return append(b, make([]byte, simdPad)...) }
	cases := 3000
	if testing.Short() {
		cases = 300
	}
	for i := range cases {
		var c simdChunk
		c.delta = i&1 != 0
		// Mostly new offsets with 20 raw bits span the most raw bytes per 16 operations.
		wide := i%3 == 1
		d0, l0, last := hist, rng.Intn(1000), 1+rng.Intn(hist-1)
		if i%4 == 0 {
			d0 = 1 + rng.Intn(100)
		}
		st0 := [4]int{d0, l0, last, 0}
		var acc uint64
		var nacc int
		d := d0
		for range 1 + rng.Intn(300) {
			tok := byte(rng.Intn(4))
			if wide && rng.Intn(16) > 0 {
				tok &^= 1
			}
			if tok&2 != 0 {
				ll := 1 + rng.Intn(simdMaxLen+1)
				c.ll = append(c.ll, byte(ll))
				if ll > simdMaxLen {
					e := rng.Intn(256)
					c.esc = append(c.esc, byte(e))
					ll += e
				}
				d += ll
			}
			if tok&1 == 0 {
				sym := byte(1 + rng.Intn(175))
				if wide {
					sym = byte(168 + rng.Intn(8))
				}
				c.offc = append(c.offc, sym)
				ot := simdOffTab[sym]
				raw := rng.Uint64() & (1<<(ot>>24) - 1)
				last = int(ot&0xffffff) + int(raw)
				acc |= raw << nacc
				for nacc += int(ot >> 24); nacc >= 8; nacc -= 8 {
					c.raw = append(c.raw, byte(acc))
					acc >>= 8
				}
			}
			ml := rng.Intn(simdMaxLen + 2)
			if ml > simdMaxLen {
				if min(last, d) < simdMaxLen {
					ml = simdMaxLen
				} else {
					e := rng.Intn(256)
					c.esc = append(c.esc, byte(e))
					d += e
				}
			}
			d += ml
			c.tok = append(c.tok, tok|byte(ml<<2))
		}
		c.raw = append(c.raw, byte(acc), 0, 0, 0, 0, 0, 0, 0)
		c.ll, c.offc, c.esc = pad(c.ll), pad(c.offc), pad(c.esc)
		rng.Read(lits)
		n := 1 + rng.Intn(len(c.tok))
		sw, sg := st0, st0
		simdExecGo(want, lits, &c, n, &sw)
		simdExec(got, lits, &c, n, &sg)
		if sw != sg || !bytes.Equal(want[d0:sw[0]], got[d0:sw[0]]) {
			t.Fatalf("case %d (%d of %d operations, delta %v, start %v): state %v, want %v", i, n, len(c.tok), c.delta, st0, sg, sw)
		}
		end := max(sw[0], sg[0]) + simdSlack + 2*simdMaxLen
		copy(want[d0:end], base[d0:end])
		copy(got[d0:end], base[d0:end])
	}
}

// TestSIMDTail moves the end of the block over the last operations, so the exact loop takes over at each of them.
// TestSIMDLongMatchTail checks the long match rule (4.3) on the last operation of a block,
// which runs in simdExecExact.
func TestSIMDLongMatchTail(t *testing.T) { simdTiers(t, testSIMDLongMatchTail) }

func testSIMDLongMatchTail(t *testing.T) {
	b := newSIMDTestBuilder(t)
	for _, tc := range []struct {
		last simdTestOp
		ok   bool
	}{
		{simdTestOp{ll: 1, ml: 40, off: 32}, true},
		{simdTestOp{ll: 1, ml: 40, off: 20}, false},
		{simdTestOp{ll: 1, ml: 40}, false}, // The repeat offset is 20.
	} {
		ops := []simdTestOp{{ll: 40, ml: 288, off: 32}, {ml: 288}, {ml: 288}, {ll: 1, ml: 4, off: 20}, tc.last}
		// No literals follow the operations, so the last one ends the output.
		e, want := b.build(simdTestBlock{ops: ops, lits: bytes.Repeat([]byte("abcdef"), 7)})
		block := e.bytes()
		if len(block)-1+simdMinSaving > len(want) {
			t.Fatal("block too large")
		}
		if tc.ok {
			simdTestDecode(t, block, want)
			continue
		}
		if _, err := Decode(nil, block); !errors.Is(err, ErrCorrupt) {
			t.Errorf("last operation %+v: %v", tc.last, err)
		}
	}
}

func TestSIMDTail(t *testing.T) { simdTiers(t, testSIMDTail) }

func testSIMDTail(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	b := newSIMDTestBuilder(t)
	for tail := range 40 {
		for _, delta := range []uint64{0, 1} {
			ops, lits := simdTestOps(rng, 200, "abcd", 288, 1000)
			for range 20 {
				ops = append(ops, simdTestOp{ll: rng.Intn(3), ml: rng.Intn(4), off: rng.Intn(2) * (1 + rng.Intn(10))})
			}
			n := 0
			for _, o := range ops {
				n += o.ll
			}
			for len(lits) < n+tail {
				lits = append(lits, 'x')
			}
			tb := simdTestBlock{ops: ops, lits: lits[:n+tail], delta: delta}
			e, want := simdTestPad(b, &tb)
			simdTestDecode(t, e.bytes(), want)
		}
	}
}

// TestSIMDGuardPage decodes blocks that end at an inaccessible page.
// With few literals, the offset bits of the last chunk are close to the end of the block.
func TestSIMDGuardPage(t *testing.T) { simdTiers(t, testSIMDGuardPage) }

func testSIMDGuardPage(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	b := newSIMDTestBuilder(t)
	var blocks, wants [][]byte
	for n := 1; n <= 300; n += 13 {
		// 17 raw bits per operation.
		ops := []simdTestOp{{ll: 1, ml: 32, off: 1 << 20}}
		for i := range n {
			ops = append(ops, simdTestOp{ml: 32, off: 1<<20 + i})
		}
		tb := simdTestBlock{ops: ops, lits: []byte("a"), delta: uint64(n & 1)}
		e, want := simdTestPad(b, &tb)
		blocks, wants = append(blocks, e.bytes()), append(wants, want)
	}
	for _, size := range []int{4 << 10, 64 << 10} {
		// Copies of earlier data, after 4 literals.
		src := make([]byte, 4, size)
		rng.Read(src)
		for len(src) < size {
			p := rng.Intn(len(src))
			src = append(src, src[p:min(p+32, len(src))]...)
		}
		for _, level := range simdLevels {
			block, err := Encode(nil, src, level)
			if err != nil {
				t.Fatal(err)
			}
			blocks, wants = append(blocks, block), append(wants, src)
		}
	}
	for i, block := range blocks {
		buf, free, err := guard.Map(len(block), true)
		if errors.Is(err, errors.ErrUnsupported) {
			t.Skip(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		copy(buf, block)
		got, err := Decode(nil, buf)
		free()
		if err != nil || !bytes.Equal(got, wants[i]) {
			t.Fatalf("block %d: %v", i, err)
		}
	}
}

func TestSIMDValidation(t *testing.T) { simdTiers(t, testSIMDValidation) }

func testSIMDValidation(t *testing.T) {
	b := newSIMDTestBuilder(t)
	type tcase struct {
		name  string
		modes []int
		ok    bool
		ops   []simdTestOp // replace the operations
		chunk int
		// change changes the block, and returns its output.
		change func(e *simdTestEnc, want []byte) []byte
		raw    func(b []byte) []byte
	}
	stored, coded := []int{simdStored}, []int{simdCodeLens, simdCurrent, simdClassLens}
	chunk := func(e *simdTestEnc, k int) *simdTestStream { return &e.chunks[0].s[k] }
	same := func(f func(e *simdTestEnc)) func(e *simdTestEnc, want []byte) []byte {
		return func(e *simdTestEnc, want []byte) []byte { f(e); return want }
	}
	litsOnly := func(noLZ bool) func(e *simdTestEnc, want []byte) []byte {
		return func(e *simdTestEnc, _ []byte) []byte {
			lits := bytes.Repeat([]byte("aaab"), 400)
			*e = simdTestEnc{typ: simdBlockType, size: len(lits), noLZ: noLZ, lits: []simdTestStream{newSIMDTestBuilder(t).stream(simdLits, simdCodeLens, lits)}}
			return lits
		}
	}
	trim := func(k int) func(e *simdTestEnc, want []byte) []byte {
		return same(func(e *simdTestEnc) { s := chunk(e, k); s.data = s.data[:len(s.data)-1] })
	}
	extend := func(k int) func(e *simdTestEnc, want []byte) []byte {
		return same(func(e *simdTestEnc) { s := chunk(e, k); s.data = append(s.data, 1) })
	}
	// recode codes stream k of the first chunk with lens, in an entry of mode and table.
	recode := func(e *simdTestEnc, k, mode int, table []byte, lens *[256]uint8) {
		var tab pivco.Table
		if err := tab.SetLengths(lens); err != nil {
			t.Fatal(err)
		}
		d, err := b.code(k, e.chunks[0].vals[k], &tab)
		if err != nil {
			t.Fatal(err)
		}
		*chunk(e, k) = simdTestStream{mode: mode, table: table, data: d}
	}
	// delta codes stream k with a valid delta class table, which only literals may use.
	delta := func(k int) func(e *simdTestEnc, want []byte) []byte {
		return same(func(e *simdTestEnc) {
			cl, lens := simdTestClassLens(simdLits, simdDeltaLens)
			recode(e, k, simdDeltaLens, simdTestNibbles(cl), &lens)
		})
	}
	// codeFor0 codes stream k with a complete code lengths table that also has a code for 0.
	codeFor0 := func(k int) func(e *simdTestEnc, want []byte) []byte {
		return same(func(e *simdTestEnc) {
			var h [256]uint64
			for _, v := range e.chunks[0].vals[k] {
				h[v]++
			}
			h[0] = 1
			var lens [256]uint8
			pivco.BuildLengths(h[:], &lens)
			recode(e, k, simdCodeLens, simdTestNibbles(lens[:2*simdCodeBytes[k]]), &lens)
		})
	}
	// sections returns the start of the LZ section, its size and the size of L.
	sections := func(b []byte) (int, int, int) {
		_, h := binary.Uvarint(b[1:])
		lz, n := rvarint(b)
		return 2 + h, int(lz), n
	}
	// past sets the reverse varint that ends at end to 1 more than the bytes from start to it.
	past := func(b []byte, start, end int) []byte {
		_, n := rvarint(b[:end])
		return append(simdTestRvarint(bytes.Clone(b[:end-n]), uint64(end-n-start+1)), b[end:]...)
	}
	// escapeTable codes escape 0 as the only value, in 1 bit.
	escapeTable := make([]byte, simdCodeBytes[simdEsc])
	escapeTable[0] = 1
	// escapes returns a block of one chunk with 65536 escapes, stored or coded.
	escapes := func(coded bool) func(e *simdTestEnc, want []byte) []byte {
		return func(e *simdTestEnc, _ []byte) []byte {
			ops := make([]simdTestOp, simdMaxChunk)
			for i := range ops {
				ops[i] = simdTestOp{ll: simdMaxLen + 1, ml: simdMaxLen + 1}
			}
			ops[0].off = simdMaxLen
			ne, want := b.build(simdTestBlock{ops: ops, lits: bytes.Repeat([]byte("abc"), 11*len(ops)), limit: 5})
			*e = *ne
			if coded {
				*chunk(e, simdEsc) = simdTestStream{mode: simdCodeLens, table: escapeTable, data: make([]byte, 2*simdMaxChunk/8)}
			}
			return want
		}
	}
	cases := []tcase{
		{name: "valid", ok: true},
		{name: "valid coded", modes: coded, ok: true},
		{name: "valid class", modes: []int{simdClassLens}, ok: true},
		{name: "type 2", change: same(func(e *simdTestEnc) { e.typ = 2 })},
		{name: "type 255", change: same(func(e *simdTestEnc) { e.typ = 255 })},
		{name: "size above 8MiB", change: same(func(e *simdTestEnc) { e.size = MaxBlockSize + 1 })},
		{name: "size+1", change: same(func(e *simdTestEnc) { e.size++ })},
		{name: "size-1", change: same(func(e *simdTestEnc) { e.size-- })},
		{name: "flags bit 5", change: same(func(e *simdTestEnc) { e.flags |= 1 << 5 })},
		{name: "flags bit 6", change: same(func(e *simdTestEnc) { e.flags |= 1 << 6 })},
		{name: "flags bit 7", change: same(func(e *simdTestEnc) { e.flags |= 1 << 7 })},
		{name: "chunk limit 5", ok: true, change: same(func(e *simdTestEnc) { e.flags |= 5 })},
		{name: "chunk limit 6", change: same(func(e *simdTestEnc) { e.flags |= 6 })},
		{name: "chunk limit 7", change: same(func(e *simdTestEnc) { e.flags |= 7 })},
		{name: "hint bits", ok: true, change: same(func(e *simdTestEnc) { e.flags |= 3 << 3 })},
		{name: "no flags", raw: func(b []byte) []byte { return b[:5] }},
		{name: "L too large", raw: func(b []byte) []byte {
			_, n := rvarint(b)
			return simdTestRvarint(b[:len(b)-n], uint64(len(b)))
		}},
		{name: "L unterminated", raw: func(b []byte) []byte { return append(b[:6], 0x80, 0x80) }},
		{name: "L past its bytes", raw: func(b []byte) []byte {
			start, _, _ := sections(b)
			return past(b, start, len(b))
		}},
		{name: "literal records past their bytes", raw: func(b []byte) []byte {
			start, lz, n := sections(b)
			return past(b, start+lz, len(b)-n)
		}},
		{name: "chunk records past their bytes", raw: func(b []byte) []byte {
			start, lz, n := sections(b)
			c := past(b, start, start+lz)
			return simdTestRvarint(c[:len(c)-n], uint64(lz+len(c)-len(b)))
		}},
		{name: "literal record extra", change: same(func(e *simdTestEnc) { e.litRecExtra = []byte{0x80} })},
		{name: "literal record for missing data", change: same(func(e *simdTestEnc) { e.litRecExtra = []byte{1 << 3} })},
		{name: "literal data extra", change: same(func(e *simdTestEnc) { e.litExtra = []byte{1} })},
		{name: "chunk data extra", change: same(func(e *simdTestEnc) { e.lzExtra = []byte{1} })},
		{name: "chunk record truncated", change: same(func(e *simdTestEnc) { e.chunkRecExtra = []byte{0} })},
		{name: "no literal blocks", change: same(func(e *simdTestEnc) { e.lits = nil })},
		{name: "literal block empty", change: same(func(e *simdTestEnc) { e.lits = append(e.lits, simdTestStream{}) })},
		{name: "literal block of 65536", change: same(func(e *simdTestEnc) {
			e.size += 65536
			e.lits = append(e.lits, simdTestStream{data: make([]byte, 65536)})
		})},
		{name: "literal block of 65535", ok: true, change: func(e *simdTestEnc, want []byte) []byte {
			e.size += 65535
			e.lits = append(e.lits, simdTestStream{data: make([]byte, 65535)})
			return append(want, make([]byte, 65535)...)
		}},
		{name: "literals above size", change: same(func(e *simdTestEnc) {
			e.lits = append(e.lits, simdTestStream{data: make([]byte, e.size)})
		})},
		{name: "chunk without operations", change: same(func(e *simdTestEnc) {
			e.chunks = append(e.chunks, simdTestChunk{})
		})},
		{name: "chunk above limit", ops: make([]simdTestOp, 1025)},
		{name: "chunk at limit", ok: true, ops: make([]simdTestOp, 1024)},
		{name: "chunk flag bit 4", change: same(func(e *simdTestEnc) { e.chunks[0].flags |= 1 << 4 })},
		{name: "chunk flag bit 7", change: same(func(e *simdTestEnc) { e.chunks[0].flags |= 1 << 7 })},
		{name: "chunk hint bits", ok: true, change: same(func(e *simdTestEnc) { e.chunks[0].flags |= 3 << 2 })},
		{name: "chunk no-overlap flag", ok: true, change: same(func(e *simdTestEnc) { e.chunks[0].flags |= 2 })},
		{name: "literal mode 5", change: same(func(e *simdTestEnc) { e.lits[0].mode = 5 })},
		{name: "literal mode 7", change: same(func(e *simdTestEnc) { e.lits[0].mode = 7 })},
		{name: "token mode 6", change: same(func(e *simdTestEnc) { chunk(e, simdTok).mode = 6 })},
		// Tokens read a token class table in mode 4 too, so only the mode is wrong.
		{name: "token mode 4", modes: []int{simdClassLens}, change: same(func(e *simdTestEnc) { chunk(e, simdTok).mode = simdDeltaLens })},
		{name: "ll mode 4", change: delta(simdLL)},
		{name: "offset mode 4", change: delta(simdOffc)},
		{name: "escape mode 4", change: delta(simdEsc)},
		{name: "offset bits mode 1", change: same(func(e *simdTestEnc) { chunk(e, 4).mode = simdCurrent })},
		{name: "offset bits mode 2", change: same(func(e *simdTestEnc) { chunk(e, 4).mode = simdCodeLens })},
		{name: "literal mode 1 first", modes: coded, change: same(func(e *simdTestEnc) { e.lits[0].mode, e.lits[0].table = simdCurrent, nil })},
		{name: "token mode 1 first", modes: coded, change: same(func(e *simdTestEnc) { chunk(e, simdTok).mode, chunk(e, simdTok).table = simdCurrent, nil })},
		{name: "table length 12", modes: coded, change: same(func(e *simdTestEnc) { e.lits[0].table[0x61/2] = 0xcc })},
		{name: "class length 12", modes: []int{simdClassLens}, change: same(func(e *simdTestEnc) { chunk(e, simdOffc).table[0] = 0xcc })},
		// Literal class 10 has in-class lengths of 6.
		{name: "class plus in-class above 11", modes: []int{simdClassLens}, change: same(func(e *simdTestEnc) { e.lits[0].table[5] = 0x66 })},
		{name: "class pad nibble", modes: []int{simdClassLens}, change: same(func(e *simdTestEnc) { e.lits[0].table[6] |= 0x10 })},
		{name: "incomplete table", modes: coded, change: same(func(e *simdTestEnc) {
			s := chunk(e, simdTok)
			clear(s.table)
			s.table[0] = 0x22
		})},
		{name: "single value of length 2", modes: coded, change: same(func(e *simdTestEnc) {
			s := chunk(e, simdTok)
			clear(s.table)
			s.table[0] = 0x02
		})},
		{name: "ll code for 0", change: codeFor0(simdLL)},
		{name: "offset code for 0", change: codeFor0(simdOffc)},
		{name: "pivco block truncated", modes: coded, change: trim(simdTok)},
		{name: "pivco block extra", modes: coded, change: extend(simdTok)},
		{name: "token count missing", modes: coded, change: same(func(e *simdTestEnc) { chunk(e, simdTok).data = nil })},
		{name: "token count truncated", modes: coded, change: same(func(e *simdTestEnc) { s := chunk(e, simdTok); s.data = s.data[:1] })},
		{name: "literal count missing", modes: coded, change: same(func(e *simdTestEnc) { e.lits[0].data = nil })},
		{name: "literal count truncated", modes: coded, change: same(func(e *simdTestEnc) { e.lits[0].data = e.lits[0].data[:1] })},
		// No length is above 32, so there are no escapes.
		{name: "coded empty stream", ops: []simdTestOp{{ll: 1, ml: 4, off: 3}, {ml: 4}}, change: same(func(e *simdTestEnc) {
			*chunk(e, simdEsc) = simdTestStream{mode: simdCodeLens, table: escapeTable}
		})},
		{name: "65536 escapes", ok: true, change: escapes(false)},
		{name: "coded 65536 escapes", change: escapes(true)},
		{name: "token 136", change: same(func(e *simdTestEnc) { chunk(e, simdTok).data[1] = 136 })},
		{name: "ll 0", change: same(func(e *simdTestEnc) { chunk(e, simdLL).data[1] = 0 })},
		{name: "ll 34", change: same(func(e *simdTestEnc) { chunk(e, simdLL).data[1] = 34 })},
		{name: "offset symbol 0", change: same(func(e *simdTestEnc) { chunk(e, simdOffc).data[1] = 0 })},
		// Symbol 2 has no raw bits, like 176. The last of 13 symbols is outside the 8-byte words.
		{name: "offset symbol 176", ops: slices.Repeat([]simdTestOp{{ll: 1, ml: 4, off: 2}}, 13), change: same(func(e *simdTestEnc) {
			d := chunk(e, simdOffc).data
			d[len(d)-1] = 176
		})},
		{name: "ll extra", change: extend(simdLL)},
		{name: "ll missing", change: trim(simdLL)},
		{name: "offset symbol extra", change: extend(simdOffc)},
		{name: "offset symbol missing", change: trim(simdOffc)},
		{name: "escape extra", change: extend(simdEsc)},
		{name: "escape missing", change: trim(simdEsc)},
		// The pivco block of a literal length stream does not store its count: one more value
		// may code to the same bytes, but eight more make the root bitmap longer.
		{name: "coded ll count", modes: coded, change: same(func(e *simdTestEnc) {
			e.chunks[0].s[simdLL] = newSIMDTestBuilder(t).stream(simdLL, simdCodeLens, append(bytes.Clone(e.chunks[0].vals[simdLL]), 1, 2, 3, 4, 5, 6, 7, 8))
		})},
		{name: "coded ll extra", modes: coded, change: extend(simdLL)},
		{name: "coded ll truncated", modes: coded, change: trim(simdLL)},
		{name: "coded escape truncated", modes: coded, change: trim(simdEsc)},
		{name: "offset bits extra", change: extend(4)},
		{name: "offset bits missing", change: trim(4)},
		{name: "offset bits cut", change: func(e *simdTestEnc, _ []byte) []byte {
			// The offset bits of the last chunk are more than the bytes after them.
			ops := []simdTestOp{{ll: 1, ml: 32, off: 1 << 20}}
			for i := range 3000 {
				ops = append(ops, simdTestOp{ml: 32, off: 1<<20 + i})
			}
			ne, want := b.build(simdTestBlock{ops: ops, lits: []byte("a")})
			*e = *ne
			e.lzCut = len(e.chunks[len(e.chunks)-1].s[4].data) - 8
			return want
		}},
		{name: "offset bits unused", ok: true, change: same(func(e *simdTestEnc) {
			for i := range e.chunks {
				c := &e.chunks[i]
				var n int
				for _, s := range c.vals[simdOffc] {
					n += int(simdOffTab[s] >> 24)
				}
				if n%8 != 0 {
					c.s[4].data[len(c.s[4].data)-1] |= 0xff << (n % 8)
				}
			}
		})},
		{name: "long match offset 20", ops: []simdTestOp{{ll: 100, ml: 40, off: 20}}},
		{name: "long match clamped", ops: []simdTestOp{{ll: 20, ml: 40, off: 40}}},
		{name: "long match repeat 20", ops: []simdTestOp{{ll: 100, ml: 30, off: 20}, {ml: 33}}},
		{name: "long match offset 32", ok: true, ops: []simdTestOp{{ll: 100, ml: 288, off: 32}}},
		{name: "offset beyond position", ok: true, ops: []simdTestOp{{ll: 10, ml: 20, off: 1000}}},
		{name: "first op without literals", ops: []simdTestOp{{ml: 20, off: 1}, {ll: 10, ml: 20, off: 1}}},
		{name: "chunk output below its operations", chunk: 8, ops: []simdTestOp{{ll: 7}, {}, {}, {}, {}, {}, {}, {}}},
		{name: "chunk output at its operations", ok: true, chunk: 8, ops: []simdTestOp{{ll: 8}, {}, {}, {}, {}, {}, {}, {}}},
		{name: "literal over-read", change: same(func(e *simdTestEnc) { e.lits = e.lits[:len(e.lits)-1] })},
		{name: "no operations", ok: true, change: litsOnly(true)},
		{name: "no chunks", ok: true, change: litsOnly(false)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			modes := tc.modes
			if modes == nil {
				modes = stored
			}
			ops, lits := simdTestOps(rand.New(rand.NewSource(4)), 2000, "abcdefgh", 288, 4000)
			tb := simdTestBlock{ops: ops, lits: lits, modes: modes, litModes: modes, litBlock: 3000, chunk: 600}
			if tc.ops != nil {
				tb = simdTestBlock{ops: tc.ops, lits: bytes.Repeat([]byte("abc"), 3000), chunk: tc.chunk}
				if tc.ops[0] == (simdTestOp{}) {
					// One chunk of len(tc.ops) operations, with chunk limit 0.
					for i := range tc.ops {
						tc.ops[i] = simdTestOp{ll: 1, ml: 4, off: 3}
					}
					tb.chunk = len(tc.ops)
				}
			}
			e, want := simdTestPad(b, &tb)
			if tc.change != nil {
				want = tc.change(e, want)
			}
			block := e.bytes()
			if tc.raw != nil {
				block = tc.raw(block)
			}
			// Reads past the block fault.
			if buf, free, err := guard.Map(len(block), true); err == nil {
				defer free()
				block = buf[:copy(buf, block)]
			}
			ref, rerr := reference.DecodeSIMDBlock(block)
			if tc.ok {
				simdTestDecode(t, block, want)
				if rerr != nil || !bytes.Equal(ref, want) {
					t.Fatal("reference:", rerr)
				}
				return
			}
			if rerr == nil {
				t.Fatal("reference: no error")
			}
			dst := bytes.Repeat([]byte{0xfe}, len(want)+64)
			_, err := Decode(dst[:0:len(want)], block)
			if err == nil {
				t.Fatal("no error")
			}
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrUnsupported) && !errors.Is(err, ErrTooLarge) {
				t.Fatal(err)
			}
			for _, v := range dst[len(want):] {
				if v != 0xfe {
					t.Fatal("wrote past dst")
				}
			}
		})
	}
	t.Run("chunks", func(t *testing.T) {
		for _, n := range []int{simdMaxChunks, simdMaxChunks + 1} {
			ops := []simdTestOp{{ll: 1, ml: simdMaxLen, off: 1}}
			for len(ops) < n {
				ops = append(ops, simdTestOp{ml: simdMaxLen})
			}
			e, want := b.build(simdTestBlock{ops: ops, lits: []byte("a"), chunk: 1})
			block := e.bytes()
			if len(e.chunks) != n || len(block)-1+simdMinSaving > len(want) {
				t.Fatalf("%d chunks, block of %d bytes for %d", len(e.chunks), len(block), len(want))
			}
			_, rerr := reference.DecodeSIMDBlock(block)
			if n <= simdMaxChunks {
				simdTestDecode(t, block, want)
				if rerr != nil {
					t.Fatalf("%d chunks: reference: %v", n, rerr)
				}
				continue
			}
			if _, err := Decode(nil, block); !errors.Is(err, ErrCorrupt) || rerr == nil {
				t.Fatalf("%d chunks: %v, reference %v", n, err, rerr)
			}
		}
	})
	t.Run("max size", func(t *testing.T) {
		ops := []simdTestOp{{ll: 40, ml: 288, off: 32}}
		for len(ops) < (MaxBlockSize-40)/288 {
			ops = append(ops, simdTestOp{ml: 288})
		}
		for _, size := range []int{MaxBlockSize, MaxBlockSize + 1} {
			e, want := b.build(simdTestBlock{ops: ops, lits: bytes.Repeat([]byte("a"), size-288*len(ops))})
			block := e.bytes()
			ref, rerr := reference.DecodeSIMDBlock(block)
			if size <= MaxBlockSize {
				simdTestDecode(t, block, want)
				if rerr != nil || !bytes.Equal(ref, want) {
					t.Fatalf("size %d: reference: %v", size, rerr)
				}
				continue
			}
			if _, err := Decode(nil, block); !errors.Is(err, ErrTooLarge) || rerr == nil {
				t.Fatalf("size %d: %v, reference %v", size, err, rerr)
			}
		}
	})
	t.Run("64 bytes", func(t *testing.T) {
		for _, n := range []int{100, 1000} {
			src := append(binary.AppendUvarint(nil, 1<<24|uint64(n)), make([]byte, n-64-4)...)
			if _, _, err := simdHeader(src); err != nil {
				t.Errorf("size %d: %v", n, err)
			}
			if _, _, err := simdHeader(append(src, 0)); err == nil {
				t.Errorf("size %d: 63 bytes smaller accepted", n)
			}
		}
		if _, _, err := simdHeader(binary.AppendUvarint(nil, 1<<32|100)); err == nil {
			t.Error("varint above 32 bits accepted")
		}
	})
}

func TestSIMDDecodeAllocs(t *testing.T) { simdTiers(t, testSIMDDecodeAllocs) }

func testSIMDDecodeAllocs(t *testing.T) {
	if race.Enabled {
		t.Skip("sync.Pool drops items with -race")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	b := newSIMDTestBuilder(t)
	ops, lits := simdTestOps(rand.New(rand.NewSource(5)), 5000, "abcdef", 288, 30000)
	tb := simdTestBlock{ops: ops, lits: lits, modes: []int{simdCodeLens, simdCurrent, simdClassLens}, litModes: []int{simdCodeLens, simdDeltaLens}, chunk: 2000, limit: 1, delta: 2, litBlock: 3000}
	e, want := simdTestPad(b, &tb)
	block := e.bytes()
	dst := make([]byte, len(want))
	if _, err := Decode(dst, block); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(20, func() { Decode(dst, block) }); n != 0 {
		t.Errorf("%v allocations", n)
	}
	_, body, err := simdHeader(block[1:])
	if err != nil {
		t.Fatal(err)
	}
	decodeSIMDHash(dst, body, true)
	if n := testing.AllocsPerRun(20, func() { decodeSIMDHash(dst, body, true) }); n != 0 {
		t.Errorf("hashing: %v allocations", n)
	}
}

// TestSIMDCorruptClears checks that a failed decode leaves nothing in dst, such as the literals
// of an earlier block that the pooled decoder keeps.
func TestSIMDCorruptClears(t *testing.T) { simdTiers(t, testSIMDCorruptClears) }

func testSIMDCorruptClears(t *testing.T) {
	if race.Enabled {
		t.Skip("sync.Pool drops items with -race")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	b := newSIMDTestBuilder(t)
	e, _ := b.build(simdTestBlock{lits: bytes.Repeat([]byte("Z"), 80000), litModes: []int{simdCodeLens}})
	if _, err := Decode(nil, e.bytes()); err != nil {
		t.Fatal(err)
	}
	// The "literal over-read" block of TestSIMDValidation.
	ops, lits := simdTestOps(rand.New(rand.NewSource(4)), 2000, "abcdefgh", 288, 4000)
	tb := simdTestBlock{ops: ops, lits: lits, litBlock: 3000, chunk: 600}
	e, want := simdTestPad(b, &tb)
	e.lits = e.lits[:len(e.lits)-1]
	dst := make([]byte, len(want))
	if _, err := Decode(dst, e.bytes()); err == nil || !bytes.Equal(dst, make([]byte, len(dst))) {
		t.Fatalf("error %v, %d Z bytes in dst", err, bytes.Count(dst, []byte("Z")))
	}
}

// TestSIMDReference compares Decode with reference.DecodeSIMDBlock on built blocks, and on changed ones.
func TestSIMDReference(t *testing.T) { simdTiers(t, testSIMDReference) }

func testSIMDReference(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	b := newSIMDTestBuilder(t)
	modes := []int{simdStored, simdCodeLens, simdCurrent, simdClassLens, simdCurrent}
	litModes := []int{simdStored, simdCodeLens, simdCurrent, simdClassLens, simdDeltaLens}
	nib := func(tab []byte, i int) byte { return tab[i/2] >> (4 * (i & 1)) & 15 }
	setNib := func(tab []byte, i int, v byte) { tab[i/2] = tab[i/2]&^(15<<(4*(i&1))) | v<<(4*(i&1)) }
	// randomCode returns the lengths of a random complete code over all or some of n values.
	// Lengths are at most 6, so most class tables stay within 11 bits per value.
	randomCode := func(n int) []uint8 {
		depth, m := []uint8{0}, n
		if rng.Intn(2) == 0 {
			m = 2 + rng.Intn(n-2)
		}
		for len(depth) < m {
			if i := rng.Intn(len(depth)); depth[i] < 6 {
				depth[i]++
				depth = append(depth, depth[i])
			}
		}
		l := make([]uint8, n)
		for i, v := range rng.Perm(n)[:len(depth)] {
			l[v] = depth[i]
		}
		return l
	}
	var sd simdDecoder
	change := func(e *simdTestEnc, litBlock int) {
		op := rng.Intn(13)
		var streams []*simdTestStream
		add := func(s *simdTestStream) {
			// Table changes need a table.
			if op != 1 && op != 2 || len(s.table) > 0 {
				streams = append(streams, s)
			}
		}
		for i := range e.lits {
			add(&e.lits[i])
		}
		for i := range e.chunks {
			for k := range e.chunks[i].s {
				add(&e.chunks[i].s[k])
			}
		}
		if len(streams) == 0 {
			return
		}
		s := streams[rng.Intn(len(streams))]
		switch op {
		case 0:
			s.mode = rng.Intn(8)
		case 1:
			n := 2 * len(s.table)
			x, y := rng.Intn(n), rng.Intn(n)
			if rng.Intn(2) == 0 {
				// Value 0 is outside the range of some streams.
				x = 0
			}
			vx, vy := nib(s.table, x), nib(s.table, y)
			setNib(s.table, x, vy)
			setNib(s.table, y, vx)
		case 2:
			// The last nibble may be a pad nibble.
			used := []int{2*len(s.table) - 1}
			for x := range 2 * len(s.table) {
				if nib(s.table, x) != 0 {
					used = append(used, x)
				}
			}
			setNib(s.table, used[rng.Intn(len(used))], byte(rng.Intn(16)))
		case 3:
			if len(s.data) > 0 {
				s.data = s.data[:len(s.data)-1]
			}
		case 4:
			s.data = append(s.data, byte(rng.Intn(256)))
		case 5:
			if len(s.data) > 0 {
				s.data[rng.Intn(len(s.data))] = byte(rng.Intn(256))
			}
		case 6:
			e.size = []int{e.size - 1, e.size + 1, MaxBlockSize + 1}[rng.Intn(3)]
		case 7:
			e.typ = 1 + rng.Intn(255)
		case 8:
			e.flags ^= 1 << rng.Intn(8)
		case 9:
			e.chunks[rng.Intn(len(e.chunks))].flags ^= 1 << rng.Intn(8)
		case 10:
			i := rng.Intn(len(e.lits))
			e.lits = append(e.lits[:i], e.lits[i+1:]...)
		case 11:
			if tok := &e.chunks[0].s[simdTok]; tok.mode == simdStored {
				tok.data[0] &^= 2
			}
		case 12:
			// A random class table, coded with the decoder's table, so the reference checks its mapping.
			k := rng.Intn(simdLits + 1)
			mode, lay := simdClassLens, k
			var st *simdTestStream
			var vals []byte
			if k == simdLits {
				i := rng.Intn(len(e.lits))
				st, vals = &e.lits[i], e.stored[i*litBlock:min(i*litBlock+litBlock, len(e.stored))]
				if rng.Intn(2) == 0 {
					mode, lay = simdDeltaLens, simdDelta
				}
			} else {
				c := &e.chunks[rng.Intn(len(e.chunks))]
				st, vals = &c.s[k], c.vals[k]
			}
			cl := randomCode(simdLayouts[lay].n)
			if k == simdTok {
				cl = append(append(cl, randomCode(4)...), randomCode(4)...)
			}
			st.mode, st.table = mode, simdTestNibbles(cl)
			if _, ok := sd.setTable(k, mode, st.table); ok && len(vals) > 0 {
				if d, err := b.code(k, vals, &sd.tab[k]); err == nil {
					st.data = d
				}
			}
		}
	}
	// Operations at the edge of validity, not all valid.
	edge := [][]simdTestOp{
		{{ll: 100, ml: 33, off: 20}},
		{{ll: 100, ml: 33, off: 32}},
		{{ll: 20, ml: 33, off: 40}},
		{{ll: 10, ml: 20, off: 1000}},
		{{ml: 20, off: 1}, {ll: 10, ml: 20, off: 1}},
	}
	// Literals of every value, so the literal class layouts matter.
	var all []byte
	for c := range 256 {
		all = append(all, byte(c))
	}
	blocks, valid := 100, 0
	if testing.Short() {
		blocks = 20
	}
	for i := range blocks {
		alphabet := "aaabbcdef\x00\xff"
		if i%2 == 0 {
			alphabet = string(all)
		}
		ops, lits := simdTestOps(rng, 1+rng.Intn(1500), alphabet, 288, 1<<rng.Intn(25))
		switch {
		case i < len(edge):
			ops, lits = edge[i], bytes.Repeat([]byte("abc"), 100)
		case i%5 == 3:
			// Single-value streams.
			ops = make([]simdTestOp, 1+rng.Intn(500))
			for j := range ops {
				ops[j] = simdTestOp{ll: 1, ml: 4, off: 3}
			}
			lits = bytes.Repeat([]byte("a"), len(ops))
		}
		tb := simdTestBlock{ops: ops, lits: lits, limit: rng.Intn(6), chunk: 32 + rng.Intn(300), litBlock: 64 + rng.Intn(2000),
			modes: modes[:1+i%len(modes)], litModes: litModes[:1+i%len(litModes)], delta: rng.Uint64()}
		if i%4 == 1 {
			tb.chunk = 1024 << tb.limit
		}
		pad, _ := simdTestPad(b, &tb)
		orig := pad.bytes()
		for j := range 60 {
			e, want := b.build(tb)
			if j%2 == 1 {
				change(e, tb.litBlock)
			}
			block := e.bytes()
			if j > 0 && j%2 == 0 {
				_, n := binary.Uvarint(block[1:])
				p := 1 + n + rng.Intn(len(block)-1-n)
				switch rng.Intn(4) {
				case 0:
					block[p] ^= 1 << rng.Intn(8)
				case 1:
					block[p] = byte(rng.Intn(256))
				case 2:
					block = append(block[:p], block[p+1:]...)
				case 3:
					block = block[:p]
				}
			}
			got, err := Decode(nil, block)
			ref, rerr := reference.DecodeSIMDBlock(block)
			if (err == nil) != (rerr == nil) || err == nil && !bytes.Equal(got, ref) || j == 0 && i >= len(edge) && !bytes.Equal(ref, want) {
				t.Fatalf("block %d change %d: Decode: %v, reference: %v, same output: %v", i, j, err, rerr, bytes.Equal(got, ref))
			}
			if err == nil && !bytes.Equal(block, orig) {
				valid++
			}
		}
	}
	t.Logf("%d changed blocks decoded", valid)
}

func TestSIMDStats(t *testing.T) { simdTiers(t, testSIMDStats) }

func testSIMDStats(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := range 20000 {
		b := make([]byte, rng.Intn(200))
		for j := range b {
			switch i % 4 {
			case 0:
				b[j] = byte(rng.Intn(256))
			case 1:
				b[j] = byte(rng.Intn(136))
			case 2:
				b[j] = byte(1 + rng.Intn(33))
			default:
				b[j] = byte(1 + rng.Intn(175))
			}
		}
		var out, nLit, nOff, nEsc, sum, nLLEsc, nb int
		tokOK, llOK, offOK := true, true, true
		for _, v := range b {
			out += int(v >> 2)
			nLit += int(v>>1) & 1
			nOff += int(v&1) ^ 1
			if v>>2 == 33 {
				nEsc++
			}
			tokOK = tokOK && v <= 135
			sum += int(v)
			if v == 33 {
				nLLEsc++
			}
			llOK = llOK && v >= 1 && v <= 33
			nb += int(simdOffTab[v] >> 24)
			offOK = offOK && v >= 1 && v <= 175
		}
		if o, l, n, e, ok := simdTokStats(b); ok != tokOK || ok && (o != out || l != nLit || n != nOff || e != nEsc) {
			t.Fatalf("%x: tokens %v %v %v %v %v", b, o, l, n, e, ok)
		}
		if a, c, ok := simdLLStats(b); ok != llOK || ok && (a != sum || c != nLLEsc) {
			t.Fatalf("%x: literal lengths %v %v %v", b, a, c, ok)
		}
		if a, ok := simdOffStats(b); ok != offOK || ok && a != nb {
			t.Fatalf("%x: offsets %v %v, want %v", b, a, ok, nb)
		}
	}
}

// TestSIMDStatsTail checks that the stream checks ignore the bytes after a stream,
// and find a bad byte at every position.
func TestSIMDStatsTail(t *testing.T) { simdTiers(t, testSIMDStatsTail) }

func testSIMDStatsTail(t *testing.T) {
	// 0xff is bad in every stream, and adds to every count. 1 is a repeat with no match,
	// a literal length and an offset symbol without raw bits.
	buf := bytes.Repeat([]byte{0xff}, 300)
	for n := 1; n <= 200; n++ {
		b := buf[:n]
		for i := range b {
			b[i] = 1
		}
		out, nLit, nOff, nEsc, tokOK := simdTokStats(b)
		sum, nLLEsc, llOK := simdLLStats(b)
		nb, offOK := simdOffStats(b)
		if !tokOK || !llOK || !offOK || out+nLit+nOff+nEsc+nLLEsc+nb != 0 || sum != n {
			t.Fatalf("length %d: tokens %v %v %v %v %v, literal lengths %v %v %v, offsets %v %v",
				n, out, nLit, nOff, nEsc, tokOK, sum, nLLEsc, llOK, nb, offOK)
		}
		for i := range b {
			b[i] = 0xff
			_, _, _, _, tokOK = simdTokStats(b)
			_, _, llOK = simdLLStats(b)
			_, offOK = simdOffStats(b)
			if tokOK || llOK || offOK {
				t.Fatalf("length %d: bad byte at %d not found: %v %v %v", n, i, tokOK, llOK, offOK)
			}
			b[i] = 1
		}
		// The edges of each range.
		tok := func(b []byte) bool { _, _, _, _, ok := simdTokStats(b); return ok }
		ll := func(b []byte) bool { _, _, ok := simdLLStats(b); return ok }
		off := func(b []byte) bool { _, ok := simdOffStats(b); return ok }
		for _, e := range []struct {
			name      string
			good, bad byte
			ok        func([]byte) bool
		}{{"token", 135, 136, tok}, {"literal length", 33, 34, ll}, {"literal length", 1, 0, ll},
			{"offset symbol", 175, 176, off}, {"offset symbol", 1, 0, off}} {
			for i := range b {
				b[i] = e.good
			}
			if !e.ok(b) {
				t.Fatalf("length %d: %s %d not accepted", n, e.name, e.good)
			}
			for i := range b {
				b[i] = e.bad
				if e.ok(b) {
					t.Fatalf("length %d: %s %d at %d not found", n, e.name, e.bad, i)
				}
				b[i] = e.good
			}
		}
	}
}

// TestSIMDTokenComponents checks A.4: the group lengths and both flag codes of a token class table
// must be complete codes, even when the token code they give is.
func TestSIMDTokenComponents(t *testing.T) {
	ops, short, long := []simdTestOp{{ll: 1}}, []simdTestOp{{ll: 1, ml: 4}}, []simdTestOp{{ll: 1, ml: 32}}
	for i := range 200 {
		ops = append(ops, simdTestOp{ml: 32})
		short = append(short, simdTestOp{ml: 4 + i%28})
		long = append(long, simdTestOp{ml: 32})
	}
	b := newSIMDTestBuilder(t)
	for _, tc := range []struct {
		ok  bool
		g   []uint8
		ops []simdTestOp
	}{
		// Both give the same token lengths.
		{true, []uint8{0: 1, 12: 1, 14: 1, 2, 3, 3, 18: 2, 2, 2, 2}, ops},
		{false, []uint8{0: 1, 12: 2, 14: 1, 2, 3, 3, 18: 1, 1, 1, 1}, ops},
		// Only one flag code is incomplete, and no group uses it.
		{false, []uint8{3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 2, 2, 2, 2, 0, 0, 0, 0}, short},
		{false, []uint8{12: 1, 1, 14: 1, 18: 2, 2, 2, 2}, long},
	} {
		e, want := b.build(simdTestBlock{ops: tc.ops, lits: []byte("abc")})
		var lens [256]uint8
		grp := &simdLayouts[simdTok]
		for v := range 136 {
			f := tc.g[14+v&3]
			if v>>2 >= 32 {
				f = tc.g[18+v&3]
			}
			if gl := tc.g[grp.class[v>>2]]; gl != 0 && f != 0 {
				lens[v] = gl + grp.in[v>>2] + f
			}
		}
		var tab pivco.Table
		if err := tab.SetLengths(&lens); err != nil {
			t.Fatal(err)
		}
		d, err := b.code(simdTok, e.chunks[0].vals[simdTok], &tab)
		if err != nil {
			t.Fatal(err)
		}
		e.chunks[0].s[simdTok] = simdTestStream{mode: simdClassLens, table: simdTestNibbles(tc.g), data: d}
		block := e.bytes()
		got, err := Decode(nil, block)
		ref, refErr := reference.DecodeSIMDBlock(block)
		if (err == nil) != tc.ok || (refErr == nil) != tc.ok {
			t.Fatalf("complete components %v: decode error %v, reference error %v", tc.ok, err, refErr)
		}
		if tc.ok && (!bytes.Equal(got, want) || !bytes.Equal(ref, want)) {
			t.Fatal("output mismatch")
		}
	}
}

// TestSIMDClassLen16 checks a literal class table where class 10 (0x80-0xbf, in-class length 6)
// has class length 10. pivco reads lengths as 4 bits, so 16 would be an unused value of the complete code
// of classes 5 and 6 (the lowercase letters), and the block would decode.
func TestSIMDClassLen16(t *testing.T) { simdTiers(t, testSIMDClassLen16) }

func testSIMDClassLen16(t *testing.T) {
	lay := &simdLayouts[simdLits]
	cl := make([]uint8, lay.n)
	cl[5], cl[6] = 1, 1
	var lens [256]uint8
	for v, c := range lay.class {
		if c == 5 || c == 6 {
			lens[v] = 1 + lay.in[v]
		}
	}
	var tab pivco.Table
	if err := tab.SetLengths(&lens); err != nil {
		t.Fatal(err)
	}
	lits := bytes.Repeat([]byte("aaab"), 400)
	d, err := newSIMDTestBuilder(t).code(simdLits, lits, &tab)
	if err != nil {
		t.Fatal(err)
	}
	for _, c10 := range []uint8{0, 10} {
		cl[10] = c10
		e := simdTestEnc{typ: simdBlockType, size: len(lits), noLZ: true,
			lits: []simdTestStream{{mode: simdClassLens, table: simdTestNibbles(cl), data: d}}}
		block := e.bytes()
		_, rerr := reference.DecodeSIMDBlock(block)
		if c10 == 0 {
			simdTestDecode(t, block, lits)
			if rerr != nil {
				t.Fatalf("reference: %v", rerr)
			}
			continue
		}
		if _, err := Decode(nil, block); !errors.Is(err, ErrCorrupt) || rerr == nil {
			t.Fatalf("length 16: %v, reference %v", err, rerr)
		}
	}
}

// simdTestLitDecode decodes block with a new decoder. Its literal buffer holds bytes of an earlier block, and
// ends at an inaccessible page right after the size+simdPad bytes that decode needs.
func simdTestLitDecode(t *testing.T, block []byte) ([]byte, error) {
	t.Helper()
	_, _, _, body, size, err := isMinLZ(block)
	if err != nil {
		return nil, err
	}
	lits, free, err := guard.Map(size+simdPad, true)
	if errors.Is(err, errors.ErrUnsupported) {
		lits, free, err = make([]byte, size+simdPad), func() {}, nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer free()
	for i := range lits {
		lits[i] = byte(i) | 0x80
	}
	s := simdDecoder{lits: lits[:0]}
	dst := bytes.Repeat([]byte{0xfe}, size+64)
	ok := s.decode(dst[:size], body, nil)
	for _, v := range dst[size:] {
		if v != 0xfe {
			t.Fatal("wrote past dst")
		}
	}
	if !ok {
		return nil, ErrCorrupt
	}
	return dst[:size], nil
}

// TestSIMDLitLazy decodes blocks whose chunks take their literals from literal blocks decoded just before them:
// chunks that need many literal blocks, literal blocks that serve many chunks, stored and coded literal blocks,
// delta chunks, and literals after the last operation. Changed blocks, among them blocks whose literals run out
// or are more than the block holds, must be accepted and decoded as the reference does.
func TestSIMDLitLazy(t *testing.T) { simdTiers(t, testSIMDLitLazy) }

func testSIMDLitLazy(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	b := newSIMDTestBuilder(t)
	litModes := []int{simdStored, simdCodeLens, simdCurrent, simdClassLens, simdDeltaLens, simdStored}
	type tcase struct {
		name string
		e    *simdTestEnc
		want []byte
	}
	var cases []tcase
	add := func(name string, tb simdTestBlock) {
		e, want := simdTestPad(b, &tb)
		cases = append(cases, tcase{name, e, want})
	}
	for i := range 6 {
		ops, lits := simdTestOps(rng, 3000+rng.Intn(3000), "abcdefgh\x00", 288, 1<<rng.Intn(20))
		delta := uint64(i&1) * rng.Uint64()
		add(fmt.Sprint("many literal blocks per chunk ", i), simdTestBlock{ops: ops, lits: lits, limit: 2, chunk: 500 + rng.Intn(3000),
			litBlock: 1 + rng.Intn(200), modes: []int{simdCodeLens, simdCurrent}, litModes: litModes[i:], delta: delta})
		add(fmt.Sprint("many chunks per literal block ", i), simdTestBlock{ops: ops, lits: lits, chunk: len(ops)/90 + rng.Intn(20),
			litBlock: 5000 + rng.Intn(60000), litModes: litModes[i:], delta: delta})
	}
	// Few literals in the operations, many after them.
	tail := make([]byte, 30000)
	for i := range tail {
		tail[i] = "abcd"[rng.Intn(4)]
	}
	add("literals after the operations", simdTestBlock{ops: []simdTestOp{{ll: 40, ml: 32, off: 7}, {ll: 2, ml: 30}}, lits: tail,
		chunk: 1, litBlock: 2000, litModes: []int{simdStored, simdCodeLens, simdCurrent}, delta: 1})
	e := &simdTestEnc{typ: simdBlockType, size: len(tail), noLZ: true}
	for i := 0; i < len(tail); i += 7000 {
		e.lits = append(e.lits, b.stream(simdLits, []int{simdCodeLens, simdCurrent}[i%2], tail[i:min(i+7000, len(tail))]))
	}
	cases = append(cases, tcase{"no operations", e, tail})

	extra := bytes.Repeat([]byte("yz"), 500)
	changes := 60
	if testing.Short() {
		changes = 12
	}
	for _, tc := range cases {
		block := tc.e.bytes()
		if ref, err := reference.DecodeSIMDBlock(block); err != nil || !bytes.Equal(ref, tc.want) {
			t.Fatalf("%s: reference: %v", tc.name, err)
		}
		simdTestDecode(t, block, tc.want)
		if got, err := simdTestLitDecode(t, block); err != nil || !bytes.Equal(got, tc.want) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		for j := range changes {
			e := *tc.e
			e.lits = slices.Clone(e.lits)
			var want []byte
			switch j % 6 {
			case 0:
				// The literals run out, mostly in the operations.
				e.lits = e.lits[:rng.Intn(len(e.lits))]
			case 1:
				i := rng.Intn(len(e.lits))
				e.lits = slices.Delete(e.lits, i, i+1)
			case 2:
				// One literal more than the block holds.
				e.lits = append(e.lits, simdTestStream{data: []byte("x")})
			case 3:
				e.size += len(extra)
				e.lits = append(e.lits, b.stream(simdLits, simdCodeLens, extra))
				want = append(bytes.Clone(tc.want), extra...)
			case 4:
				s := &e.lits[rng.Intn(len(e.lits))]
				s.data = s.data[:max(len(s.data)-1-rng.Intn(3), 0)]
			}
			block := e.bytes()
			if j%6 == 5 {
				_, h := binary.Uvarint(block[1:])
				block[1+h+rng.Intn(len(block)-1-h)] ^= 1 << rng.Intn(8)
			}
			got, err := simdTestLitDecode(t, block)
			ref, rerr := reference.DecodeSIMDBlock(block)
			if (err == nil) != (rerr == nil) || err == nil && !bytes.Equal(got, ref) || want != nil && !bytes.Equal(got, want) {
				t.Fatalf("%s change %d: %v, reference %v", tc.name, j, err, rerr)
			}
		}
	}
}

// TestSIMDLitFill checks that fill decodes literal blocks only until the literals a chunk needs are decoded,
// and that zeros follow them.
func TestSIMDLitFill(t *testing.T) {
	rng := rand.New(rand.NewSource(12))
	b := newSIMDTestBuilder(t)
	lits := make([]byte, 50000)
	for i := range lits {
		lits[i] = "abcdefgh"[rng.Intn(8)]
	}
	e := simdTestEnc{typ: simdBlockType, size: len(lits), noLZ: true}
	var ends []int
	for i := 0; i < len(lits); i = ends[len(ends)-1] {
		v := lits[i:min(i+1+rng.Intn(4000), len(lits))]
		e.lits = append(e.lits, b.stream(simdLits, []int{simdStored, simdCodeLens, simdCurrent}[len(ends)%3], v))
		ends = append(ends, i+len(v))
	}
	raw := e.bytes()
	_, h := binary.Uvarint(raw[1:])
	body := raw[2+h:]
	_, n := rvarint(body)
	s := simdDecoder{lits: bytes.Repeat([]byte{0xa5}, len(lits)+simdPad)[:0]}
	r, ok := s.literals(body[:len(body)-n], len(lits))
	if !ok {
		t.Fatal("no literal section")
	}
	for l := 0; len(r.recs) > 0; {
		need := rng.Intn(6000)
		want := len(s.lits)
		for i := 0; want-l < need && i < len(ends); i++ {
			want = max(want, ends[i])
		}
		if !s.fill(&r, l, need, len(lits)) {
			t.Fatal("fill failed")
		}
		if len(s.lits) != want || !bytes.Equal(s.lits, lits[:want]) || !bytes.Equal(s.lits[want:want+simdPad], make([]byte, simdPad)) {
			t.Fatalf("%d literals from %d: decoded %d, want %d", need, l, len(s.lits), want)
		}
		l += rng.Intn(min(need, want-l) + 1)
	}
	if r.pos != r.end {
		t.Fatal("literal data left")
	}
}
