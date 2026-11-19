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
	"fmt"
	"math/bits"
	"sync"
)

// The ultra levels parse like zstd's btultra2 (zstd_opt.c): a binary tree finds the matches at a position,
// and a forward dynamic program over segments of up to ultraOptNum bytes picks the cheapest path through them.
// Classic blocks have exact byte costs. SIMD blocks are priced by adaptive statistics of their streams.

const (
	ultraOptNum   = 4096
	ultraMaxPrice = 1 << 30
	// ultraByte is the price of a byte: prices are in 1/256 bits.
	ultraByte = 8 << 8
	// ultraCompares is the depth of a tree search. 32 is 3-5% faster for 0.2% larger output.
	ultraCompares = 64
	// ultraSufficient commits a longer match at once. The DP only prices matches up to this length.
	ultraSufficient = 256
	// ultraRescale is the input after which the SIMD statistics are scaled down, so prices follow the data.
	ultraRescale = 1 << 18
	// ultraStoredMax is the largest SIMD block that prices its operations as stored streams.
	ultraStoredMax = 2 << 10
	// ultraTwoPassMax is the largest SIMD block that is parsed twice, to find the streams that are stored.
	ultraTwoPassMax = 16 << 10
	// ultraAllStored has the bits of the operation streams, simdTok to simdEsc.
	ultraAllStored = 1<<simdTok | 1<<simdLL | 1<<simdOffc | 1<<simdEsc
)

type ultraMatch struct{ off, len int }

// ultraSeq is ll literals, then a match of ml bytes at off. int32 halves the memory of a block's sequences.
type ultraSeq struct{ ll, off, ml int32 }

// ultraOpt is the cheapest path found to a position: a match of mlen bytes at off, then litlen literals.
// rep is the repeat offset after it.
type ultraOpt struct{ price, off, mlen, litlen, rep int32 }

// ultraStats is zstd's optState_t for the SIMD streams: frequencies, and prices in 1/256 bits.
type ultraStats struct {
	lit                                   [256]uint32
	tok                                   [136]uint32
	ll                                    [34]uint32
	off                                   [176]uint32
	esc                                   [256]uint32
	litSum, tokSum, llSum, offSum, escSum uint32
	// stored has a bit for each operation stream that is stored, at 8 bits a value, by its simdTok to simdEsc.
	stored uint8
	litP   [256]int32
	tokP   [136]int32
	llP    [34]int32
	offP   [176]int32
	escP   [256]int32
}

type ultraParser struct {
	simd   bool
	minRep int
	src    []byte

	// The tree: hash heads, and two children per position, as positions+1 so 0 is none.
	// Position p has the children bt[2*(p&btMask)] and bt[2*(p&btMask)+1], so maxOff must not exceed btMask.
	hashBits uint
	hash, bt []uint32
	btMask   int
	next     int
	maxOff   int

	opt  [ultraOptNum + 16]ultraOpt
	ms   []ultraMatch
	seqs []ultraSeq
	tmp  []ultraSeq
	st   ultraStats
}

var ultraPool sync.Pool

func getUltra() *ultraParser {
	if u, _ := ultraPool.Get().(*ultraParser); u != nil {
		return u
	}
	return new(ultraParser)
}

// encodeBlockUltra encodes src like encodeBlockBest, and shares its assumptions.
func encodeBlockUltra(dst, src []byte) int {
	u := getUltra()
	defer ultraPool.Put(u)
	// Offsets are below len(src), which sizes the tree.
	u.simd, u.minRep, u.maxOff = false, 3, min(len(src)-1, maxCopy3Offset)
	return ultraEmitClassic(dst, src, u.parse(src))
}

// encodeBlockUltra parses src for LevelUltraSIMD.
func (e *simdEncoder) encodeBlockUltra(src []byte) {
	u := getUltra()
	defer ultraPool.Put(u)
	ultraEmitSIMD(e, src, u.parseSIMD(src))
}

// parseSIMD returns the sequences of src for a SIMD block.
func (u *ultraParser) parseSIMD(src []byte) []ultraSeq {
	u.simd, u.minRep, u.maxOff = true, 2, len(src)-1
	u.st.init(src)
	if len(src) > ultraStoredMax && len(src) <= ultraTwoPassMax {
		// Streams of few values are stored, which the statistics don't know. A first parse finds them,
		// and its statistics start the second, as zstd's first block.
		first := u.st
		u.parse(src)
		u.st.stored = ultraStoredStreams(&u.st, &first)
		u.st.rescale()
	}
	return u.parse(src)
}

var ultraBufs sync.Pool

// ultraFallback encodes src with LevelSmallestSIMD, and moves it to dst when it is smaller than the n bytes there.
// On data of few symbols without long matches, such as hex or DNA, the adaptive prices can settle on many short
// matches: at the prices they make, each is cheaper than its literals, but fewer matches would cost less in total.
func (e *simdEncoder) ultraFallback(dst, src []byte, n int) int {
	bp, _ := ultraBufs.Get().(*[]byte)
	if bp == nil {
		bp = new([]byte)
	}
	defer ultraBufs.Put(bp)
	if cap(*bp) < len(dst) {
		*bp = make([]byte, len(dst))
	}
	// The encoder checks the capacity, which must be that of dst.
	b := (*bp)[:len(dst):len(dst)]
	e.out = b[:0]
	e.parse(src, LevelSmallestSIMD)
	if m := e.finish(); m > 0 && (n == 0 || m < n) {
		return copy(dst, b[:m])
	}
	return n
}

// ultraCount returns how many bytes match at src[a:] and src[b:], b < a, not counting past end.
func ultraCount(src []byte, a, b, end int) int {
	n := 0
	for a+n+8 <= end {
		if d := load64(src, a+n) ^ load64(src, b+n); d != 0 {
			return n + bits.TrailingZeros64(d)>>3
		}
		n += 8
	}
	for a+n < end && src[a+n] == src[b+n] {
		n++
	}
	return n
}

func (u *ultraParser) h4(p int) uint32 { return (load32(u.src, p) * 2654435761) >> (32 - u.hashBits) }

// insert is ZSTD_insertBt1. It returns how many positions to advance.
func (u *ultraParser) insert(cur, end int) int {
	src, bt, mask := u.src, u.bt, u.btMask
	h := u.h4(cur)
	mi := int(u.hash[h]) - 1
	u.hash[h] = uint32(cur + 1)
	low := max(0, cur-u.maxOff)
	sp, lp := 2*(cur&mask), 2*(cur&mask)+1
	cs, cl := 0, 0
	best, endIdx := 8, cur+9
	for n := ultraCompares; n > 0 && mi >= low; n-- {
		ml := min(cs, cl)
		ml += ultraCount(src, cur+ml, mi+ml, end)
		if ml > best {
			best = ml
			if ml > endIdx-mi {
				endIdx = mi + ml
			}
		}
		if cur+ml == end {
			break
		}
		if src[mi+ml] < src[cur+ml] {
			bt[sp] = uint32(mi + 1)
			cs = ml
			sp = 2*(mi&mask) + 1
			mi = int(bt[sp]) - 1
		} else {
			bt[lp] = uint32(mi + 1)
			cl = ml
			lp = 2 * (mi & mask)
			mi = int(bt[lp]) - 1
		}
	}
	bt[sp], bt[lp] = 0, 0
	skip := 0
	if best > 384 {
		skip = min(192, best-384)
	}
	return max(skip, endIdx-(cur+8))
}

// getAll is ZSTD_btGetAllMatches with one repeat offset, rep, or none if 0.
// It returns matches of increasing length; each has the smallest offset found for its length.
func (u *ultraParser) getAll(cur, end, rep int) []ultraMatch {
	out := u.ms[:0]
	if cur < u.next {
		return out
	}
	for u.next < cur {
		u.next += u.insert(u.next, end)
	}
	u.next = cur
	src, bt, mask := u.src, u.bt, u.btMask
	best := 3
	if rep > 0 && rep <= cur {
		if l := ultraCount(src, cur, cur-rep, end); l >= u.minRep {
			out = append(out, ultraMatch{rep, l})
			best = max(best, l)
			if l > ultraSufficient || cur+l == end {
				u.ms = out
				return out
			}
		}
	}
	low := max(0, cur-u.maxOff)
	h := u.h4(cur)
	mi := int(u.hash[h]) - 1
	u.hash[h] = uint32(cur + 1)
	sp, lp := 2*(cur&mask), 2*(cur&mask)+1
	cs, cl := 0, 0
	endIdx := cur + 9
	for n := ultraCompares; n > 0 && mi >= low; n-- {
		ml := min(cs, cl)
		ml += ultraCount(src, cur+ml, mi+ml, end)
		if ml > best {
			if ml > endIdx-mi {
				endIdx = mi + ml
			}
			best = ml
			out = append(out, ultraMatch{cur - mi, ml})
			if ml > ultraOptNum || cur+ml == end {
				break
			}
		}
		if src[mi+ml] < src[cur+ml] {
			bt[sp] = uint32(mi + 1)
			cs = ml
			sp = 2*(mi&mask) + 1
			mi = int(bt[sp]) - 1
		} else {
			bt[lp] = uint32(mi + 1)
			cl = ml
			lp = 2 * (mi & mask)
			mi = int(bt[lp]) - 1
		}
	}
	bt[sp], bt[lp] = 0, 0
	u.next = endIdx - 8
	u.ms = out
	return out
}

// ultraW is zstd's WEIGHT with fractional bits: about 256*log2(x+1).
func ultraW(x uint32) int32 {
	s := x + 1
	h := bits.Len32(s) - 1
	return int32(h*256) + int32((s<<8)>>h)
}

func ultraPrices(f []uint32, sum uint32, p []int32) {
	base := ultraW(sum)
	for i, v := range f {
		p[i] = base - ultraW(v)
	}
}

func ultraSum(t []uint32) (n uint32) {
	for _, v := range t {
		n += v
	}
	return n
}

// init starts the statistics of the block src, as the first block of ZSTD_rescaleFreqs.
func (s *ultraStats) init(src []byte) {
	var h [256]uint32
	for _, b := range src {
		h[b]++
	}
	for i, c := range h {
		s.lit[i] = c>>8 + min(c, 1)
	}
	for i := range s.tok {
		s.tok[i] = 1
	}
	for i := range s.ll {
		s.ll[i] = min(uint32(i), 1)
	}
	for i := range s.off {
		s.off[i] = min(uint32(i), 1)
	}
	for i := range s.esc {
		s.esc[i] = 1
	}
	s.litSum, s.tokSum, s.llSum = ultraSum(s.lit[:]), ultraSum(s.tok[:]), ultraSum(s.ll[:])
	s.offSum, s.escSum = ultraSum(s.off[:]), ultraSum(s.esc[:])
	s.stored = 0
	if len(src) <= ultraStoredMax {
		s.stored = ultraAllStored
	}
}

// setPrices is ZSTD_setBasePrices.
func (s *ultraStats) setPrices() {
	base := ultraW(s.litSum)
	for i, v := range s.lit {
		s.litP[i] = base - min(ultraW(v), base-256)
	}
	prices := func(k int, f []uint32, sum uint32, p []int32) {
		if s.stored>>k&1 == 0 {
			ultraPrices(f, sum, p)
			return
		}
		for i := range p {
			p[i] = ultraByte
		}
	}
	prices(simdTok, s.tok[:], s.tokSum, s.tokP[:])
	prices(simdLL, s.ll[:], s.llSum, s.llP[:])
	prices(simdOffc, s.off[:], s.offSum, s.offP[:])
	prices(simdEsc, s.esc[:], s.escSum, s.escP[:])
}

// ultraStoredStreams returns the stored bits of the operation streams whose values from first to cur put stores:
// those without a table that codes them smaller, with freeTables.
func ultraStoredStreams(cur, first *ultraStats) (stored uint8) {
	var h [256]uint64
	var t simdTable
	for k, f := range [...][2][]uint32{{cur.tok[:], first.tok[:]}, {cur.ll[:], first.ll[:]}, {cur.off[:], first.off[:]}, {cur.esc[:], first.esc[:]}} {
		clear(h[:])
		n := 0
		for i, v := range f[0] {
			h[i] = uint64(v - f[1][i])
			n += int(h[i])
		}
		coded := false
		if n > simdBlockBits/7 {
			syms := len(f[0])
			t.setCode(k, &h)
			c, ok := t.cost(&h, syms)
			coded = ok && c+8*t.n+simdBlockBits < 8*n
			if !coded {
				t.setClass(k, &h, syms)
				c, ok = t.cost(&h, syms)
				coded = ok && c+8*t.n+simdBlockBits < 8*n
			}
		}
		if !coded {
			stored |= 1 << k
		}
	}
	return stored
}

// rescale is ZSTD_scaleStats.
func (s *ultraStats) rescale() {
	sc := func(t []uint32, logTarget uint) uint32 {
		if f := ultraSum(t) >> logTarget; f > 1 {
			sh := bits.Len32(f) - 1
			for i, v := range t {
				t[i] = 1 + v>>sh
			}
		}
		return ultraSum(t)
	}
	s.litSum = sc(s.lit[:], 12)
	s.tokSum = sc(s.tok[:], 11)
	s.llSum = sc(s.ll[:], 11)
	s.offSum = sc(s.off[:], 11)
	s.escSum = sc(s.esc[:], 11)
}

// update adds a committed sequence to the statistics, as ultraEmitSIMD emits it.
func (s *ultraStats) update(src []byte, q ultraSeq, rep int) {
	off, ml := int(q.off), int(q.ml)
	for _, b := range src {
		s.lit[b] += 2
	}
	s.litSum += 2 * uint32(len(src))
	t := 0
	if ll := int(q.ll); ll > 0 {
		t = 2
		for ; ll > simdMaxLong; ll -= simdMaxLong {
			s.addTok(3)
			s.addLL(simdMaxLen + 1)
			s.addEsc(255)
		}
		if ll > simdMaxLen {
			s.addLL(simdMaxLen + 1)
			s.addEsc(ll - simdMaxLen - 1)
		} else {
			s.addLL(ll)
		}
	}
	if off == rep {
		t |= 1
	} else {
		s.addOff(off)
	}
	if off < simdMaxLen && ml > simdMaxLen {
		s.addTok(simdMaxLen<<2 | t)
		if ml -= simdMaxLen; ml > simdMaxLen {
			s.addOff(ultraRepAfter(off, ml+simdMaxLen))
			t = 0
		} else {
			t = 1
		}
	}
	for {
		n := min(ml, simdMaxLong)
		if n > simdMaxLen {
			s.addTok((simdMaxLen+1)<<2 | t)
			s.addEsc(n - simdMaxLen - 1)
		} else {
			s.addTok(n<<2 | t)
		}
		if ml -= n; ml == 0 {
			return
		}
		t = 1
	}
}

func (s *ultraStats) addTok(v int) { s.tok[v]++; s.tokSum++ }
func (s *ultraStats) addLL(v int)  { s.ll[v]++; s.llSum++ }
func (s *ultraStats) addEsc(v int) { s.esc[v]++; s.escSum++ }
func (s *ultraStats) addOff(off int) {
	sym, _ := ultraOffSym(off)
	s.off[sym]++
	s.offSum++
}

// ultraOffSym returns the offset symbol of off and its count of raw bits.
func ultraOffSym(off int) (sym, raw int) {
	b := bits.Len(uint(off)|8) - 4
	return 8*b + off>>b, b
}

// ultraRepAfter returns the repeat offset after a SIMD match: a long match with a short period continues at a multiple.
func ultraRepAfter(off, ml int) int {
	if off < simdMaxLen && ml > 2*simdMaxLen {
		return off * ((simdMaxLen + off - 1) / off)
	}
	return off
}

// ultraHdr returns the size of the header of n literals, or of a repeat of n bytes, in a classic block.
func ultraHdr(n int) int {
	switch {
	case n == 0:
		return 0
	case n <= 29:
		return 1
	case n <= 285:
		return 2
	case n <= 65565:
		return 3
	}
	return 4
}

func ultraCopy2Size(ml int) int {
	switch {
	case ml <= 64:
		return 3
	case ml <= 319:
		return 4
	case ml <= 65599:
		return 5
	}
	return 6
}

// ultraClassicCopy returns the size of the cheapest copy in a classic block,
// less the literal header that a fused copy absorbs.
func ultraClassicCopy(off, ml, litlen int) int {
	best := 1 << 20
	if off <= maxCopy1Offset {
		switch {
		case ml <= 18:
			best = 2
		case ml <= 273:
			best = 3
		default:
			best = 2 + ultraHdr(ml-18)
		}
	}
	if off >= minCopy2Offset && off <= maxCopy2Offset {
		s := ultraCopy2Size(ml)
		if litlen >= 1 && litlen <= maxCopy2Lits && ml <= copy2LitMaxLen {
			s--
		}
		best = min(best, s)
	}
	if off >= minCopy3Offset {
		s := ultraCopy2Size(ml) + 1
		if litlen >= 1 && litlen <= maxCopy3Lits {
			s--
		}
		best = min(best, s)
	}
	return best
}

// llPrice prices the literal lengths of a run of ll literals.
func (u *ultraParser) llPrice(ll int) int {
	if !u.simd {
		return ultraHdr(ll) * ultraByte
	}
	if ll == 0 {
		return 0
	}
	st := &u.st
	p := 0
	if ll > simdMaxLong {
		// emitCopyLits adds operations of simdMaxLong literals.
		ops := (ll - 1) / simdMaxLong
		p = ops * int(st.tokP[3]+st.llP[simdMaxLen+1]+st.escP[255])
		ll -= ops * simdMaxLong
	}
	if ll > simdMaxLen {
		return p + int(st.llP[simdMaxLen+1]+st.escP[ll-simdMaxLen-1])
	}
	return p + int(st.llP[ll])
}

func (u *ultraParser) litPrice(pos int) int {
	if !u.simd {
		return ultraByte
	}
	return int(u.st.litP[u.src[pos]])
}

func (u *ultraParser) offPrice(off int) int {
	s, raw := ultraOffSym(off)
	return int(u.st.offP[s]) + raw*256
}

// relax prices the matches ms, at most ultraSufficient long, from opt[cur], where r0 is the repeat offset
// that was searched, and keeps the cheaper path at the positions they reach. It returns the new last position.
func (u *ultraParser) relax(cur, lastPos int, ms []ultraMatch, r0 int) int {
	opt := &u.opt
	st := opt[cur]
	rep, litlen := int(st.rep), int(st.litlen)
	tokP, escP := &u.st.tokP, &u.st.escP
	for i, m := range ms {
		start := u.minLen(m, r0)
		if i > 0 {
			start = max(start, ms[i-1].len+1)
		}
		off := m.off
		// SIMD: the offset and the token bits are the same for every length. zstd adds 51 to an operation,
		// for fewer of them. A match over 32 bytes needs an offset of at least 32: 32 bytes, then a repeat,
		// or the rest at a multiple of the offset.
		t, p0, p32, p64 := 0, int(st.price)+51, 0, 0
		if u.simd {
			if litlen > 0 {
				t = 2
			}
			if off == rep {
				t |= 1
			} else {
				p0 += u.offPrice(off)
			}
			if off < simdMaxLen {
				p32 = p0 + int(tokP[simdMaxLen<<2|t])
				p64 = p32 + u.offPrice(ultraRepAfter(off, 3*simdMaxLen))
			}
		}
		for ml := m.len; ml >= start; ml-- {
			var price int
			switch {
			case !u.simd && off == rep:
				price = int(st.price) + ultraHdr(ml)*ultraByte
			case !u.simd:
				price = int(st.price) + ultraClassicCopy(off, ml, litlen)*ultraByte
			case ml <= simdMaxLen:
				price = p0 + int(tokP[ml<<2|t])
			case off >= simdMaxLen:
				price = p0 + int(tokP[(simdMaxLen+1)<<2|t]+escP[ml-simdMaxLen-1])
			case ml <= 2*simdMaxLen:
				price = p32 + int(tokP[(ml-simdMaxLen)<<2|1])
			default:
				price = p64 + int(tokP[(simdMaxLen+1)<<2]+escP[ml-2*simdMaxLen-1])
			}
			pos := cur + ml
			if pos > lastPos || price < int(opt[pos].price) {
				for lastPos < pos {
					lastPos++
					opt[lastPos] = ultraOpt{price: ultraMaxPrice, litlen: 1}
				}
				opt[pos] = ultraOpt{price: int32(price), off: int32(off), mlen: int32(ml), rep: int32(u.repAfter(off, ml))}
			}
		}
	}
	return lastPos
}

func (u *ultraParser) repAfter(off, ml int) int {
	if u.simd {
		return ultraRepAfter(off, ml)
	}
	return off
}

// minLen returns the shortest length of m to price, with r0 from repFor.
func (u *ultraParser) minLen(m ultraMatch, r0 int) int {
	if m.off == r0 {
		return u.minRep
	}
	return 4
}

// parse returns the sequences of src. Literals follow the last one.
func (u *ultraParser) parse(src []byte) []ultraSeq {
	u.src = src
	n := len(src)
	u.seqs = u.seqs[:0]
	// The tree sorts the positions of a hash by their bytes, so collisions hardly matter:
	// 2^20 heads for 8 MiB matches 2^22 within 0.001%, and is faster.
	u.hashBits = uint(min(20, max(12, bits.Len(uint(n-1)))))
	if size := 1 << u.hashBits; cap(u.hash) < size {
		u.hash = make([]uint32, size)
	} else {
		u.hash = u.hash[:size]
		clear(u.hash)
	}
	// Every position writes its children when it is inserted, before the tree can lead to it, so bt needs no clear.
	u.btMask = 1<<bits.Len(uint(u.maxOff)) - 1
	if size := 2 * (u.btMask + 1); cap(u.bt) < size {
		u.bt = make([]uint32, size)
	} else {
		u.bt = u.bt[:size]
	}
	u.next = 0
	if u.simd {
		u.st.setPrices()
	}
	opt := &u.opt
	ilimit := n - 8
	ip, anchor, rep := 1, 0, 1
	for ip < ilimit {
		litlen := ip - anchor
		r0 := u.repFor(rep, litlen)
		ms := u.getAll(ip, n, r0)
		if len(ms) == 0 {
			ip++
			continue
		}
		opt[0] = ultraOpt{price: int32(u.llPrice(litlen)), litlen: int32(litlen), rep: int32(rep)}
		var last ultraOpt
		lastPos, cur := 0, 0
		if L := ms[len(ms)-1]; L.len > ultraSufficient {
			last, lastPos = ultraOpt{mlen: int32(L.len), off: int32(L.off)}, L.len
		} else {
			lastPos = L.len
			for pos := 1; pos <= lastPos; pos++ {
				opt[pos] = ultraOpt{price: ultraMaxPrice, litlen: int32(litlen + pos)}
			}
			u.relax(0, lastPos, ms, r0)
			opt[lastPos+1].price = ultraMaxPrice
			immediate := false
			for cur = 1; cur <= lastPos; cur++ {
				inr := ip + cur
				prev := &opt[cur-1]
				ll := int(prev.litlen) + 1
				price := int(prev.price) + u.litPrice(inr-1) + u.llPrice(ll) - u.llPrice(ll-1)
				if price <= int(opt[cur].price) {
					old := opt[cur]
					opt[cur] = ultraOpt{price: int32(price), off: prev.off, mlen: prev.mlen, litlen: int32(ll), rep: prev.rep}
					// zstd's rescue of a match that the literal replaced: the match, then a literal.
					// Classic blocks don't need it, as a literal after a match always costs its header.
					if u.simd && old.litlen == 0 && old.mlen > 0 && inr < n {
						w1 := int(old.price) + u.litPrice(inr) + u.llPrice(1)
						wm := price + u.litPrice(inr) + u.llPrice(ll+1) - u.llPrice(ll)
						if w1 < wm && w1 < int(opt[cur+1].price) {
							old.litlen, old.price = 1, int32(w1)
							opt[cur+1] = old
							if cur+1 > lastPos {
								lastPos = cur + 1
								opt[lastPos+1].price = ultraMaxPrice
							}
						}
					}
				}
				if inr >= ilimit {
					continue
				}
				if cur == lastPos {
					break
				}
				st := opt[cur]
				srep := int(st.rep)
				r0 := u.repFor(srep, int(st.litlen))
				ms = u.getAll(inr, n, r0)
				if len(ms) == 0 {
					continue
				}
				if L := ms[len(ms)-1]; L.len > ultraSufficient || cur+L.len >= ultraOptNum || inr+L.len >= n {
					last, lastPos, immediate = ultraOpt{mlen: int32(L.len), off: int32(L.off)}, cur+L.len, true
					break
				}
				lastPos = u.relax(cur, lastPos, ms, r0)
				opt[lastPos+1].price = ultraMaxPrice
			}
			if !immediate {
				last = opt[lastPos]
				cur = lastPos - int(last.mlen) - int(last.litlen)
			}
		}
		if last.mlen == 0 {
			ip += lastPos
			continue
		}
		// Backtrace from the match of last, which starts at cur.
		u.tmp = u.tmp[:0]
		s, sp := last, cur
		for {
			nx := &opt[sp]
			u.tmp = append(u.tmp, ultraSeq{ll: nx.litlen, off: s.off, ml: s.mlen})
			if nx.mlen == 0 {
				break
			}
			s = *nx
			sp -= int(nx.litlen + nx.mlen)
		}
		for i := len(u.tmp) - 1; i >= 0; i-- {
			q := u.tmp[i]
			if u.simd {
				u.st.update(src[anchor:anchor+int(q.ll)], q, rep)
			}
			u.seqs = append(u.seqs, q)
			rep = u.repAfter(int(q.off), int(q.ml))
			anchor += int(q.ll + q.ml)
		}
		if u.simd {
			if (ip-1)/ultraRescale != (anchor-1)/ultraRescale {
				u.st.rescale()
			}
			u.st.setPrices()
		}
		ip = anchor
	}
	return u.seqs
}

// repFor returns the repeat offset to search after litlen literals. A SIMD operation without literals
// gets no repeat candidate: it would only split the match before it, and it bounds the operations of a block.
func (u *ultraParser) repFor(rep, litlen int) int {
	if u.simd && litlen == 0 {
		return 0
	}
	return rep
}

// ultraEmitSIMD adds seqs and the literals after them to e.
func ultraEmitSIMD(e *simdEncoder, src []byte, seqs []ultraSeq) {
	anchor := 0
	for _, q := range seqs {
		ml, off := int(q.ml), int(q.off)
		lits := src[anchor : anchor+int(q.ll)]
		n := ml
		if off < simdMaxLen && n > 2*simdMaxLen {
			n = simdMaxLen
		}
		if debugEncode && off == e.rep && q.ll == 0 && ml < 4 {
			panic(fmt.Sprintf("ultra: repeat of %d bytes without literals", ml))
		}
		if off == e.rep {
			e.emitRepeatLits(lits, n)
		} else {
			e.emitCopyLits(lits, off, n)
		}
		if n < ml {
			e.emitCopy(ultraRepAfter(off, ml), ml-n)
		}
		if debugEncode && e.rep != ultraRepAfter(off, ml) {
			panic(fmt.Sprintf("ultra: repeat offset %d, parsed %d", e.rep, ultraRepAfter(off, ml)))
		}
		anchor += len(lits) + ml
	}
	if anchor < len(src) {
		e.emitLiterals(src[anchor:])
	}
}

// ultraEmitClassic emits seqs and the literals after them to dst, and returns the size, or 0 if src doesn't compress.
func ultraEmitClassic(dst, src []byte, seqs []ultraSeq) int {
	d, anchor, rep := 0, 0, 1
	limit := len(src) - 5
	for _, q := range seqs {
		lits := src[anchor : anchor+int(q.ll)]
		off, ml, nl := int(q.off), int(q.ml), len(lits)
		// The emitters store up to 12 bytes past the literals, and the assembly ones one more.
		// When that could pass the end of dst, emit to a scratch buffer and keep it only if it fits.
		var full []byte
		fullD := d
		if len(dst)-d < nl+16 {
			full, dst, d = dst, make([]byte, nl+16), 0
		}
		d += ultraEmitOp(dst[d:], lits, off, ml, rep)
		if full != nil {
			if fullD+d > limit {
				return 0
			}
			copy(full[fullD:], dst[:d])
			dst, d = full, fullD+d
		}
		rep = off
		anchor += nl + ml
		if d > limit {
			return 0
		}
	}
	nl := len(src) - anchor
	if d+nl+emitLiteralSizeN(nl) > limit {
		return 0
	}
	return d + emitLiteral(dst[d:], src[anchor:])
}

// ultraEmitOp emits lits, then a match of ml bytes at off in the encoding that the parser priced, and returns the size.
func ultraEmitOp(dst, lits []byte, off, ml, rep int) int {
	nl := len(lits)
	switch {
	case off == rep:
		d := emitLiteral(dst, lits)
		return d + emitRepeat(dst[d:], ml)
	case nl >= 1 && nl <= maxCopy2Lits && off >= minCopy2Offset && off <= maxCopy2Offset && ml <= copy2LitMaxLen:
		return emitCopyLits2(dst, lits, off, ml)
	case nl >= 1 && nl <= maxCopy3Lits && off >= minCopy3Offset:
		return emitCopyLits3(dst, lits, off, ml)
	}
	d := emitLiteral(dst, lits)
	// emitCopy takes Copy1 up to its offset limit, where Copy2 can be smaller.
	if off >= minCopy2Offset && off <= maxCopy2Offset && ultraCopy2Size(ml) <= ultraClassicCopy(off, ml, 0) {
		return d + encodeCopy2(dst[d:], off, ml)
	}
	return d + emitCopy(dst[d:], off, ml)
}
