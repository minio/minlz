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

//go:build (amd64 || arm64) && !appengine && !noasm && gc && !purego

package minlz

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/minio/minlz/internal/fuzz"
)

func init() {
	simdAsmReturns = func(f func()) (n int) {
		defer func() { simdAsmHook = nil }()
		simdAsmHook = func(*simdEncoder, int) { n++ }
		f()
		return n
	}
}

// TestSIMDAsmLayout checks the simdStreams offsets and the constants that _generate/gen-simd-enc.go mirrors.
func TestSIMDAsmLayout(t *testing.T) {
	var s simdStreams
	got := []uintptr{
		unsafe.Offsetof(s.tok), unsafe.Offsetof(s.ll), unsafe.Offsetof(s.offc), unsafe.Offsetof(s.esc), unsafe.Offsetof(s.raw),
		unsafe.Offsetof(s.nTok), unsafe.Offsetof(s.nLL), unsafe.Offsetof(s.nOffc), unsafe.Offsetof(s.nEsc), unsafe.Offsetof(s.nRaw),
		unsafe.Offsetof(s.nLits), unsafe.Offsetof(s.nacc), unsafe.Offsetof(s.acc), unsafe.Offsetof(s.pos), unsafe.Offsetof(s.rep),
		unsafe.Offsetof(s.asmSite), unsafe.Offsetof(s.asmBase), unsafe.Offsetof(s.asmLen), unsafe.Offsetof(s.asmOff),
		unsafe.Offsetof(s.lits), unsafe.Offsetof(s.overlap),
	}
	const n, a = simdChunkOps, 105
	const c = a + 5*n + 1<<16 + 7
	want := []uintptr{a, a + n, a + 2*n, a + 3*n, a + 5*n, 0, 8, 16, 24, 32, 40, 48, 56, 64, 72, c, c + 8, c + 16, c + 24, 80, 104}
	if !slices.Equal(got, want) {
		t.Errorf("simdStreams offsets %v, want %v; update _generate/gen-simd-enc.go", got, want)
	}
	consts := []int{simdChunkOps, simdMaxLen, simdMaxLong, simdAsmDone, simdAsmFull, simdAsmCopy, simdAsmRepeat}
	if want := []int{1 << 14, 32, 288, 0, 1, 2, 3}; !slices.Equal(consts, want) {
		t.Errorf("constants %v, want %v; update _generate/gen-simd-enc.go", consts, want)
	}
}

// simdParseAsmInputs returns the inputs of TestSIMDParseAsm, and with $SIMDBENCH, its files in blocks.
func simdParseAsmInputs(t *testing.T) (names []string, srcs [][]byte) {
	add := func(name string, b []byte) {
		names, srcs = append(names, name), append(srcs, b)
	}
	for _, z := range []string{"testdata/enc_regressions.zip", "testdata/fuzz/block-corpus-raw.zip"} {
		fuzz.ReturnFromZip(t, z, fuzz.TypeRaw, func(b []byte) { add(fmt.Sprintf("%s#%d", filepath.Base(z), len(srcs)), b) })
	}
	rng := rand.New(rand.NewSource(1))
	parse := simdTestParseInput(rng)
	add("parse-64k", parse[:64<<10])
	add("parse-64k+1", parse[:64<<10+1])
	add("parse", parse)
	add("text", genEncText(rng, 1<<20))
	add("zeros", make([]byte, 8<<20))
	for p := 2; p < 32; p++ {
		add(fmt.Sprintf("period-%d", p), bytes.Repeat(genEncRandom(rng, p), 256<<10/p))
	}
	for n := 16; n <= 64; n++ {
		add(fmt.Sprintf("parse-%d", n), parse[rng.Intn(len(parse)-n):][:n])
		add(fmt.Sprintf("period-%d", n), bytes.Repeat(genEncRandom(rng, 1+n%5), n)[:n])
	}
	// The last match ends 1 to 40 bytes before the end of the block, after a repeat or a copy.
	for k := 1; k <= 40; k++ {
		for _, n := range []int{200, 70000} {
			b := genEncRandom(rng, n)
			b = append(b, b[n/2:n/2+100]...)
			add(fmt.Sprintf("tail-%d-%d", n, k), append(slices.Clip(b), genEncRandom(rng, k)...))
			b = append(b, genEncRandom(rng, 3)...)
			for range 60 {
				b = append(b, b[len(b)-n/2])
			}
			add(fmt.Sprintf("tail-rep-%d-%d", n, k), append(b, genEncRandom(rng, k)...))
		}
	}
	// Chunks that end at a fast operation, at a repeat and at a slow operation.
	ops := func(slow bool, off func() int) []byte {
		b := genEncRandom(rng, 100)
		for i := range simdChunkOps + 1000 {
			b = append(b, genEncRandom(rng, 1+rng.Intn(3))...)
			ml := 10 + rng.Intn(10)
			if slow && i >= simdChunkOps-500 {
				ml = 300 + rng.Intn(300)
			}
			for o := off(); ml > 0; ml-- {
				b = append(b, b[len(b)-o])
			}
		}
		return b
	}
	add("ops", ops(false, func() int { return 50 + rng.Intn(50) }))
	add("ops-repeat", ops(false, func() int { return 37 }))
	add("ops-slow", ops(true, func() int { return 50 + rng.Intn(50) }))
	// Repeats at offsets below simdMaxLen, of up to simdMaxLong bytes, which only their offset makes slow.
	for _, p := range []int{7, 16, simdMaxLen - 1} {
		pat, b := genEncRandom(rng, p), []byte{}
		for len(b) < 128<<10 {
			for range 40 + rng.Intn(200) {
				b = append(b, pat[len(b)%p])
			}
			b = append(b, byte(rng.Intn(256)))
		}
		add(fmt.Sprintf("period-%d-breaks", p), b)
	}
	for i := range 64 {
		add(fmt.Sprintf("overlap-edge-%d", i), simdOverlapEdgeInput(rng, 40))
	}
	add("lits-edge", simdLitsEdgeInput(rng, 128<<10))
	add("long-edges", simdLongEdgeInput(rng))
	// Blocks where only the split of a run at offset 31 overlaps, at each site.
	for site, sn := range simdSiteNames {
		b := simdRun(simdRunPrefix(rng, genEncRandom(rng, 16), site, simdMaxLen-1), simdMaxLen-1, 40)
		add("overlap-split-"+sn, append(b, genEncRandom(rng, 16)...))
	}
	// Its own source fixes its bytes: with others, a later position can take the short table entry that it needs.
	add("far-repeat", simdFarRepeatInput(rand.New(rand.NewSource(1))))

	dir := os.Getenv("SIMDBENCH")
	if dir == "" {
		return names, srcs
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	for _, fn := range files {
		if filepath.Base(fn) == "10gb.tar" {
			continue
		}
		f, err := os.Open(fn)
		if err != nil {
			t.Fatal(err)
		}
		src, err := io.ReadAll(io.LimitReader(f, 8<<20))
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, bs := range []int{4 << 10, 64 << 10, 64<<10 + 1, 1 << 20, 8 << 20} {
			for b := 0; b < len(src); b += bs {
				add(fmt.Sprintf("%s@%d+%d", filepath.Base(fn), bs, b), src[b:min(b+bs, len(src))])
			}
		}
	}
	return names, srcs
}

// simdOverlapEdgeInput returns a block of copies and repeats as long as their offsets, which the search,
// the post-match loop and the repeat check find, so that only an exact overlap test keeps its chunk free of overlap.
func simdOverlapEdgeInput(rng *rand.Rand, groups int) []byte {
	other := func(not ...byte) byte { return simdOtherByte(rng, not...) }
	// SuperFast checks s = 1, 6 and 11, Fastest 9 to 11, and both find a copy of 10 bytes at offset 10.
	b := genEncRandom(rng, 11)
	b[10] = other(b[0])
	b = append(b, b[1:]...)
	rep, afterCopy := 10, true
	for range groups {
		e := len(b)
		if rng.Intn(3) == 0 {
			// A literal, then a repeat of rep bytes.
			b = append(b, other(b[e-rep]))
			for range rep {
				b = append(b, b[len(b)-rep])
			}
			afterCopy = false
			continue
		}
		// Literals, then a copy A of the table entry at q, then a copy B of the literals and A,
		// which the post-match loop finds at offset len(B). After a copy the table holds e-2,
		// and SuperFast's search checks from e+1, stepping by 5; Fastest extends A back to its start.
		// After a repeat both are e, and as B starts with src[q], A must end before its offset.
		q, first, gap := e-2, 1, 0
		if !afterCopy {
			q, first, gap = e, 0, 1
		}
		var nl, na int
		for {
			nl = 1 + rng.Intn(simdMaxLen-8)
			maxA := min(e+nl-q-gap, simdMaxLen-nl)
			if (nl-first)%5 < 3 && maxA >= 8 && e+nl-q != rep {
				na = 8 + rng.Intn(maxA-7)
				break
			}
		}
		b = append(append(b, 0), genEncRandom(rng, nl-1)...)
		b[len(b)-1] = other(b[q-1]) // A must not extend back
		end := b[q]                 // the byte after A's source, which B must not start with
		if q+na < len(b) {
			end = b[q+na]
		}
		b[e] = other(b[e-rep], end, b[q-1])
		b = append(b, b[q:q+na]...)
		b = append(b, b[e:]...)
		rep, afterCopy = nl+na, true
	}
	return append(append(b, other(b[len(b)-rep])), genEncRandom(rng, 16)...)
}

// simdLitsEdgeInput returns about n bytes of literal runs of 286 to 292 bytes, each followed by a copy of some of them
// or a repeat, which the parsers that extend matches back find at the end of the literals.
func simdLitsEdgeInput(rng *rand.Rand, n int) []byte {
	b, off, stop := genEncRandom(rng, 16), 0, 0
	for len(b) < n {
		e, ll, ml := len(b), 286+rng.Intn(7), 40+rng.Intn(61)
		if off == 0 || rng.Intn(4) > 0 {
			off = ml + rng.Intn(ll-ml+1)
		}
		b = append(b, genEncRandom(rng, ll)...)
		b[e] = simdOtherByte(rng, b[stop])            // ends the previous match
		b[e+ll-1] = simdOtherByte(rng, b[e+ll-off-1]) // and this one starts after the literals
		for range ml {
			b = append(b, b[len(b)-off])
		}
		stop = len(b) - off
	}
	return append(b, genEncRandom(rng, 64)...)
}

// simdFarRepeatInput returns a block where Balanced finds a copy of 4 bytes at an offset over 65535 that is the repeat
// offset, which it keeps, where it passes over those at other offsets. The repeat check finds such copies at the positions
// that the search checks one after another, so this one is at a position that it reaches with a step of 2.
func simdFarRepeatInput(rng *rand.Rand) []byte {
	const far = 70000
	// Zeros end an operation at q, so the search checks q to q+128, then every other position.
	b := append(genEncRandom(rng, 16), make([]byte, 300)...)
	b[15] = simdOtherByte(rng, 0)
	q := len(b)
	b = append(b, genEncRandom(rng, 200)...)
	b[q], b[q+199] = simdOtherByte(rng, 0), simdOtherByte(rng, 0)
	b = append(b, make([]byte, far)...)
	b = append(b, genEncRandom(rng, 20)...)
	b[q+200+far] = simdOtherByte(rng, 0)
	// A copy of 30 bytes at q+10 sets the repeat offset. 140 literals later, the search checks with a step of 2,
	// where 4 bytes at the repeat offset follow, those at q+180.
	t := len(b)
	b = append(b, b[q+10:q+40]...)
	b = append(b, genEncRandom(rng, 200)...)
	b[t-1], b[t+30] = simdOtherByte(rng, b[q+9]), simdOtherByte(rng, b[q+40])
	s := t + 30 + 140
	copy(b[s:s+4], b[q+180:])
	b[s-1], b[s+4] = simdOtherByte(rng, b[q+179]), simdOtherByte(rng, b[q+184])
	return b
}

// The sites where the parsers find a run of simdRunPrefix: a copy after literals or a repeat, found by the search,
// or a copy without literals, found by the post-match loop of SuperFast and Fastest.
const (
	simdSiteCopy = iota
	simdSiteRepeat
	simdSitePost
)

var simdSiteNames = []string{"copy", "repeat", "post"}

// simdRunPrefix appends bytes to b that a run at offset off, of simdRun, continues as a match found at site.
func simdRunPrefix(rng *rand.Rand, b []byte, site, off int) []byte {
	switch site {
	case simdSiteCopy:
		b = append(b, genEncRandom(rng, max(off, simdMaxLen)+8)...)
		b[len(b)-1] = simdOtherByte(rng, b[len(b)-1-off]) // no back-extension
		return b
	case simdSiteRepeat:
		// A copy at off, which doesn't overlap, sets the repeat offset.
		return simdRun(simdRunPrefix(rng, b, simdSiteCopy, off), off, off)
	}
	// The post-match loop of a copy C0 indexes its end e. The search finds a copy C1 of literals at its start,
	// which ends at e+off, where the post-match loop finds the run from e.
	r := len(b)
	b = append(b, genEncRandom(rng, 64)...)
	b = append(b, b[r+8:r+56]...)
	e, k := len(b), off/2+1
	b = append(b, genEncRandom(rng, k)...)
	for range off - k {
		b = append(b, b[len(b)-k+1])
	}
	// It ends C0, starts the run, and doesn't continue C1.
	b[e] = simdOtherByte(rng, b[e-56], b[e+off-k+1])
	return b
}

// simdRun appends n bytes at offset off to b, and a byte that ends them.
func simdRun(b []byte, off, n int) []byte {
	for range n {
		b = append(b, b[len(b)-off])
	}
	return append(b, b[len(b)-off]+1)
}

// simdLongEdgeInput returns a block of runs at offsets 31 to 33, where the split step changes, of lengths where the
// splits change, found at every site. Runs at the copy site of SuperFast, which doesn't extend back, can start later.
func simdLongEdgeInput(rng *rand.Rand) []byte {
	b := genEncRandom(rng, 16)
	for _, n := range []int{32, 33, 288, 289, 320, 576, 577, 608} {
		// The offset changes, so the copy isn't at the repeat offset.
		for off := simdMaxLen - 1; off <= simdMaxLen+1; off++ {
			for site := range simdSiteNames {
				b = simdRun(simdRunPrefix(rng, b, site, off), off, n)
			}
		}
	}
	return append(b, genEncRandom(rng, 64)...)
}

// simdSplitEdgeInputs returns blocks where the first chunk of parser p ends in a run at offset off, found at site,
// which emitLong splits into operations of 32 bytes, or of 288 at offsets from 32: in fit they fill the chunk exactly,
// in over they need one more.
func simdSplitEdgeInputs(t *testing.T, p simdAsmParser, site, off int) (fit, over []byte) {
	rng := rand.New(rand.NewSource(int64(off + 100*site)))
	// Short copies for most of a chunk, then the run.
	pre := genEncRandom(rng, 100)
	for range simdChunkOps - 3400 {
		pre = append(pre, genEncRandom(rng, 1+rng.Intn(3))...)
		for o, n := 50+rng.Intn(50), 10+rng.Intn(10); n > 0; n-- {
			pre = append(pre, pre[len(pre)-o])
		}
	}
	pre = simdRunPrefix(rng, pre, site, off)
	tail := genEncRandom(rng, 32)
	block := func(n int) []byte {
		return append(simdRun(slices.Clip(pre), off, n), tail...)
	}
	// The run is too long for any chunk, so the parser leaves its match to Go, which shows where it starts.
	n := simdChunkOps*simdMaxLong + 64
	end := len(pre) + n
	var nTok, length, offset, found int
	defer func() { simdAsmHook = nil }()
	simdAsmHook = func(e *simdEncoder, r int) {
		if r >= simdAsmCopy && e.asmBase+e.asmLen == end {
			nTok, length, offset, found = e.nTok, e.asmLen, e.asmOff, simdSiteCopy
			switch {
			case r == simdAsmRepeat:
				offset, found = e.rep, simdSiteRepeat
			case e.asmBase == e.pos:
				found = simdSitePost
			}
		}
	}
	simdParseRun(new(simdEncoder), block(n), p, true)
	if length == 0 || found != site || offset != off {
		t.Fatalf("%s: the %s run at offset %d ends in a %s match of %d bytes at offset %d", p.name, simdSiteNames[site], off, simdSiteNames[found], length, offset)
	}
	step := simdMaxLen
	if offset >= simdMaxLen {
		step = simdMaxLong
	}
	n -= length - (simdChunkOps-nTok)*step
	return block(n), block(n + 1)
}

// simdOtherByte returns a random byte that isn't in not.
func simdOtherByte(rng *rand.Rand, not ...byte) byte {
	for {
		if c := byte(rng.Intn(256)); !slices.Contains(not, c) {
			return c
		}
	}
}

// TestSIMDParseAsm checks that the assembly parsers leave the encoder as the Go parsers do,
// and that their returns to Go cover every kind and resume site.
func TestSIMDParseAsm(t *testing.T) {
	names, srcs := simdParseAsmInputs(t)
	defer func() { simdAsmHook = nil }()
	eg, ea := new(simdEncoder), new(simdEncoder)
	for _, p := range simdAsmParsers {
		names, srcs := names, srcs
		if p.max > 64<<10 {
			// Every site, as each checks the room for the operations of a long match itself,
			// at the offsets where the split steps change from 32 to 288 bytes.
			for site, sn := range simdSiteNames {
				if site == simdSitePost && p.k == simdAsmBetter {
					continue
				}
				for _, off := range []int{simdMaxLen - 1, simdMaxLen} {
					fit, over := simdSplitEdgeInputs(t, p, site, off)
					names = append(slices.Clip(names), fmt.Sprintf("split-fit-%s-%d", sn, off), fmt.Sprintf("split-over-%s-%d", sn, off))
					srcs = append(slices.Clip(srcs), fit, over)
				}
			}
		}
		// Returns by code and resume site, full returns after a copy and a repeat, copies without literals,
		// slow operations that end a chunk, operations that only a literal length of simdMaxLong+1 makes slow,
		// chunks that the operations of a long match fill, and long matches that need one operation more than the chunk has,
		// both by the step of the split, and the returns of operations that the assembly parser adds itself.
		var cov [4][2]int
		var noLits, slowEdge, fastOps int
		var full, llEdge, splitFit, splitOver [2]int
		var edge [2]bool // the first chunk ends at a split that fills it, or that needs one operation more
		last, lastChunks := -1, 0
		simdAsmHook = func(e *simdEncoder, r int) {
			if (last == simdAsmCopy || last == simdAsmRepeat) && e.chunks > lastChunks {
				slowEdge++
			}
			last, lastChunks = r, e.chunks
			cov[r][e.asmSite]++
			if r == simdAsmFull {
				t := e.tok[simdChunkOps-1]
				full[t&1]++
				// Only the operations after the first of a long match have no literals at the repeat offset.
				if t&3 == 1 {
					splitFit[e.tok[simdChunkOps-2]>>2/(simdMaxLen+1)]++
					edge[0] = edge[0] || e.chunks == 0
				}
			}
			if r < simdAsmCopy {
				return
			}
			if r == simdAsmCopy && e.asmBase == e.pos {
				noLits++
			}
			off := e.asmOff
			if r == simdAsmRepeat {
				off = e.rep
			}
			// The operations of the match, as emitLong splits it, less the room in the chunk.
			step := simdMaxLen
			if off >= simdMaxLen {
				step = simdMaxLong
			}
			over := (e.asmLen+step-1)/step - (simdChunkOps - e.nTok)
			// Copies of the post-match loop need only fit the chunk, but can't be told apart,
			// so only operations within the literal limits are checked.
			if e.asmBase-e.pos <= simdMaxLong && len(e.src)-e.pos >= simdMaxLen {
				if over <= 0 {
					fastOps++
				} else if over == 1 {
					splitOver[step/simdMaxLong]++
					edge[1] = edge[1] || e.chunks == 0
				}
			}
			if e.asmBase-e.pos == simdMaxLong+1 && len(e.src)-e.pos >= simdMaxLen && over <= 0 {
				llEdge[r-simdAsmCopy]++
			}
		}
		blocks := 0
		for i, src := range srcs {
			// The 64K parsers take the start of longer inputs.
			if src = src[:min(len(src), p.max)]; len(src) < minNonLiteralBlockSize {
				continue
			}
			blocks++
			last, edge = -1, [2]bool{}
			g := simdParseRun(eg, src, p, false)
			if d := simdParseDiff(g, simdParseRun(ea, src, p, true)); d != "" {
				t.Errorf("%s %s (%d bytes): %s", p.name, names[i], len(src), d)
			}
			// Flag 2 of the chunk marks it free of overlap, which the edges must not lose.
			if strings.HasPrefix(names[i], "overlap-edge") && g.recs[0]&2 == 0 {
				t.Errorf("%s %s: the chunk overlaps", p.name, names[i])
			}
			if strings.HasPrefix(names[i], "split-fit") && !edge[0] || strings.HasPrefix(names[i], "split-over") && !edge[1] {
				t.Errorf("%s %s: the first chunk doesn't end at the split", p.name, names[i])
			}
		}
		t.Logf("%s: %d blocks; returns done %v, full %v, copy %v, repeat %v; full after copy and repeat %v, %d copies without literals, %d slow operations end chunks, literal edges %v, split chunk ends fit %v, over %v",
			p.name, blocks, cov[simdAsmDone], cov[simdAsmFull], cov[simdAsmCopy], cov[simdAsmRepeat], full, noLits, slowEdge, llEdge, splitFit, splitOver)
		if fastOps > 0 {
			t.Errorf("%s: %d operations that the assembly parser adds itself are left to Go", p.name, fastOps)
		}
		// Copies resume after the match, at site 1, but for Balanced, which resumes only at the search.
		cs := 1
		if p.k == simdAsmBetter || p.k == simdAsmBetter64K {
			cs = 0
		}
		if cov[simdAsmDone][0] == 0 || cov[simdAsmCopy][cs] == 0 || cov[simdAsmRepeat][0] == 0 || noLits == 0 || noLits == cov[simdAsmCopy][cs] ||
			// Without back-extension, SuperFast copies follow no literal run of 286 to 292 bytes.
			llEdge[0] == 0 && p.k != simdAsmFast && p.k != simdAsmFast64K || llEdge[1] == 0 ||
			// 64 KB have fewer operations than a chunk, as each covers at least 4 bytes.
			p.max > 64<<10 && (cov[simdAsmFull][0] == 0 || cov[simdAsmFull][cs] == 0 || full[0] == 0 || full[1] == 0 || slowEdge == 0) {
			t.Errorf("%s: returns not covered", p.name)
		}
		// The end and repeats resume at the search, and so does all of Balanced.
		if cov[simdAsmDone][1]+cov[simdAsmRepeat][1]+cov[simdAsmCopy][1-cs] != 0 || cs == 0 && cov[simdAsmFull][1] != 0 {
			t.Errorf("%s: returns resume at the wrong site", p.name)
		}
	}
}

// TestSIMDEncodeAsm checks that the levels without delta literals parse with their assembly parser
// for either size of block, which encodes as the Go parser, and that the others don't.
func TestSIMDEncodeAsm(t *testing.T) {
	defer func(asm bool) { simdEncAsm = asm }(simdEncAsm)
	rng := rand.New(rand.NewSource(1))
	parse, text := simdTestParseInput(rng), genEncText(rng, 1<<20)
	gd, ad := make([]byte, MaxEncodedLen(1<<20)), make([]byte, MaxEncodedLen(1<<20))
	for _, level := range simdLevels {
		asm := level&LevelSIMDDelta == 0 && level != LevelSmallestSIMD
		for _, n := range []int{4 << 10, 64 << 10, 64<<10 + 1, 1 << 20} {
			for _, src := range [][]byte{parse[:n], text[:n]} {
				simdEncAsm = false
				g := gd[:encodeBlockSIMD(gd, src, level)]
				simdEncAsm = true
				var a []byte
				if used := simdAsmReturns(func() { a = ad[:encodeBlockSIMD(ad, src, level)] }) > 0; used != asm {
					t.Errorf("level %d, %d bytes: the assembly parsers were used: %v", level, n, used)
				}
				if !bytes.Equal(g, a) {
					t.Errorf("level %d, %d bytes: the block differs, %d bytes, with the assembly parsers %d", level, n, len(g), len(a))
				}
			}
		}
	}
}
