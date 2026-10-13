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

package kernel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"
)

// Tests of the vector executors (_generate/pivco dec_avx512.go, dec_avx2.go) against execGeneric.

// vecTiers returns the usable tiers other than TierGeneric, or skips.
func vecTiers(t testing.TB) []Tier {
	t.Helper()
	ts := Tiers()
	if len(ts) == 1 {
		t.Skip("no vector tier on this CPU/build")
	}
	return ts[:len(ts)-1]
}

// clone returns a copy of e with its own Arena, Dst and ops; In is shared.
func (e *env) clone() *env {
	c := &env{p: e.p, dstAll: bytes.Clone(e.dstAll), r2s: e.r2s}
	c.p.Ops = slices.Clone(e.p.Ops)
	c.p.Arena = bytes.Clone(e.p.Arena)
	c.p.Dst = c.dstAll[:len(e.p.Dst):len(e.p.Dst)]
	if e.p.R2S != nil {
		c.p.R2S = &c.r2s
	}
	return c
}

// TestExecLayout checks the Op/Program offsets and constants _generate/pivco hardcodes.
func TestExecLayout(t *testing.T) {
	var o Op
	var p Program
	got := []uintptr{
		unsafe.Offsetof(o.Kind), unsafe.Offsetof(o.Flags), unsafe.Offsetof(o.D), unsafe.Offsetof(o.P),
		unsafe.Offsetof(o.Q), unsafe.Offsetof(o.K), unsafe.Offsetof(o.KR), unsafe.Offsetof(o.Bm),
		unsafe.Offsetof(o.Out), unsafe.Offsetof(o.L), unsafe.Offsetof(o.R), unsafe.Sizeof(o),
	}
	if want := []uintptr{0, 1, 2, 3, 4, 8, 12, 16, 20, 24, 28, 32}; !slices.Equal(got, want) {
		t.Errorf("Op offsets %v, want %v", got, want)
	}
	consts := []int{OpVV, OpCV, OpCC, OpV512, OpV128, OpNat, OpCopy, FlagDst, ArenaSlack, R2SLen}
	if want := []int{0, 1, 2, 3, 4, 5, 6, 1, 128, 320}; !slices.Equal(consts, want) {
		t.Errorf("constants %v, want %v", consts, want)
	}
	// The generators resolve the Program fields at generation time; stale assembly would read
	// the wrong ones.
	w := unsafe.Sizeof(uintptr(0))
	got = []uintptr{
		unsafe.Offsetof(p.Ops), unsafe.Offsetof(p.In), unsafe.Offsetof(p.Arena),
		unsafe.Offsetof(p.Dst), unsafe.Offsetof(p.R2S), unsafe.Sizeof(p),
	}
	if want := []uintptr{0, 3 * w, 6 * w, 9 * w, 12 * w, 13 * w}; !slices.Equal(got, want) {
		t.Errorf("Program offsets %v, want %v; regenerate the assembly", got, want)
	}
}

// decDiffExec runs e.p on the generic tier (on copies of Arena and Dst) and on tier, and
// requires the same result and memory, untouched inputs and Dst guard bytes. Only
// a failing op's own output may differ: VV/CV write before their KR check.
func decDiffExec(t *testing.T, tier Tier, e *env, what string) int {
	t.Helper()
	q := e.p
	q.Arena, q.Dst = bytes.Clone(e.p.Arena), bytes.Clone(e.p.Dst)
	want := TierGeneric.Exec(&q)
	dstAll, in, r2s := bytes.Clone(e.dstAll), bytes.Clone(e.p.In), e.r2s
	got := tier.Exec(&e.p)
	if got != want {
		t.Fatalf("%v %s: Exec = %d, generic %d; op %+v", tier, what, got, want, decOpAt(e.p.Ops, max(got, want)))
	}
	if want >= 0 {
		op := e.p.Ops[want]
		out, ref := e.p.Arena, q.Arena
		if op.Flags&FlagDst != 0 {
			out, ref = e.p.Dst, q.Dst
		}
		lo, hi := min(uint64(op.Out), uint64(len(out))), min(uint64(op.Out)+uint64(op.K), uint64(len(out)))
		copy(ref[lo:hi], out[lo:hi])
	}
	if i := decMismatch(q.Arena, e.p.Arena); i >= 0 {
		t.Fatalf("%v %s: arena differs from generic at %d (ret %d)", tier, what, i, got)
	}
	if i := decMismatch(q.Dst, e.p.Dst); i >= 0 {
		t.Fatalf("%v %s: dst differs from generic at %d (ret %d)", tier, what, i, got)
	}
	if !bytes.Equal(dstAll[len(e.p.Dst):], e.dstAll[len(e.p.Dst):]) || !bytes.Equal(in, e.p.In) || r2s != e.r2s {
		t.Fatalf("%v %s: wrote outside its outputs (ret %d)", tier, what, got)
	}
	return got
}

func decOpAt(ops []Op, i int) Op {
	if i >= 0 && i < len(ops) {
		return ops[i]
	}
	return Op{}
}

func decMismatch(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return min(len(a), len(b))
	}
	return -1
}

// decMutate breaks one op of an upstream-placed program in a way that cannot create aliasing the
// placement proof does not cover, so both tiers must still agree byte for byte.
func decMutate(rng *rand.Rand, e *env, ops []Op) {
	i := rng.IntN(len(ops))
	op := &ops[i]
	srcLen := len(e.p.In)
	outLim := len(e.p.Arena) - ArenaSlack
	if op.Flags&FlagDst != 0 {
		outLim = len(e.p.Dst)
	}
	alim := uint32(len(e.p.Arena) - ArenaSlack)
	over := func(lim int) uint32 { return uint32(max(lim-int(op.K)+1+rng.IntN(3), 0)) }
	switch rng.IntN(10) {
	case 0:
		op.KR += uint32([]int{-1, 1, rng.IntN(65536)}[rng.IntN(3)])
	case 1:
		op.Kind = []uint8{OpCC, OpV512, OpV128, OpNat, OpCopy, 7, uint8(7 + rng.IntN(249))}[rng.IntN(7)]
	case 2:
		op.D = uint8(rng.IntN(11))
		if rng.IntN(8) == 0 {
			op.D = uint8(rng.Uint32())
		}
	case 3:
		op.P, op.Q = uint8(rng.Uint32()), uint8(rng.Uint32())
	case 4:
		op.Flags ^= 2 // not a flag: ignored
	case 5:
		op.Flags |= FlagDst
	case 6:
		op.Out = over(outLim)
	case 7:
		if op.Kind == OpVV || op.Kind == OpCV {
			if rng.IntN(2) == 0 {
				op.L = over(int(alim))
			} else {
				op.R = over(int(alim))
			}
		} else {
			op.K = ^uint32(rng.IntN(4))
		}
	case 8:
		need := (int(op.K)*max(int(op.D), 1) + 7) / 8
		op.Bm = uint32(max(srcLen-need+1+rng.IntN(2), 0))
	case 9:
		op.K = ^uint32(rng.IntN(1 << 20))
	}
}

// TestExecConformance runs random upstream-placed programs (as TestExecTree) on every vector
// tier and the generic one, half of them with one broken op.
func TestExecConformance(t *testing.T) {
	tiers := vecTiers(t)
	rng := rand.New(rand.NewPCG(3, 4))
	iters := 3000
	if testing.Short() {
		iters = 300
	}
	for iter := range iters {
		n := testSizes[rng.IntN(len(testSizes))]
		if n == 0 || iter%97 == 0 {
			n = 65535
		}
		var r2s [R2SLen]byte
		randBytes(rng, r2s[:])
		g := &treeGen{rng: rng, r2s: &r2s}
		root, want := g.node(n, 0)
		ops := root.root(nil)
		e := newEnv(rng, 2*n, n, g.in)
		e.r2s = r2s
		broken := iter%2 == 1 && len(ops) > 0
		if broken {
			decMutate(rng, e, ops)
		}
		e.p.Ops = ops
		for _, tier := range tiers {
			c := e.clone()
			if ret := decDiffExec(t, tier, c, "tree"); ret >= 0 && !broken {
				t.Fatalf("%v iter %d n=%d: valid op %d (%+v) failed", tier, iter, n, ret, ops[ret])
			}
			if !broken && !bytes.Equal(c.p.Dst, want) {
				t.Fatalf("%v iter %d n=%d: wrong output", tier, iter, n)
			}
		}
	}
}

// TestExecKindSweep runs every Kind and many D values with valid geometry on every tier.
func TestExecKindSweep(t *testing.T) {
	tiers := vecTiers(t)
	rng := newRNG()
	ds := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 64, 128, 255}
	for kind := range 256 {
		for _, d := range ds {
			for _, k := range []int{0, 1, 128, 512, 700} {
				in := randBytes(rng, make([]byte, k+16))
				e := newEnv(rng, 3*k+64, k, in)
				ku := uint32(k)
				// Out, L and R do not overlap; P is random, so some flats fail on R2S.
				e.p.Ops = []Op{{Kind: uint8(kind), D: d, P: uint8(rng.IntN(256)), Q: 9, K: ku,
					KR: uint32(decPopcnt(in[3:], k)), Bm: 3, Out: 0, L: ku, R: 2 * ku}}
				for _, tier := range tiers {
					decDiffExec(t, tier, e.clone(), "sweep")
				}
			}
		}
	}
}

// decPopcnt returns the number of set bits in the first k bits of b.
func decPopcnt(b []byte, k int) int {
	n := 0
	for j := range k {
		n += int(b[j/8] >> (j % 8) & 1)
	}
	return n
}

// TestExecEmpty checks that an empty program succeeds without writing.
func TestExecEmpty(t *testing.T) {
	for _, tier := range Tiers() {
		e := newEnv(newRNG(), 64, 24, []byte{0xff})
		if ret := e.exec(t, tier, 0); ret != -1 {
			t.Fatalf("%v: empty program: ret %d", tier, ret)
		}
	}
}

// pageGuarded returns n bytes between two inaccessible pages, flush against the one after
// them if atEnd, else against the one before. It skips t where that is not supported.
func pageGuarded(t testing.TB, n int, atEnd bool) []byte {
	t.Helper()
	b, free, err := mapGuarded(n, atEnd)
	if err != nil {
		t.Skip("page guards:", err)
	}
	t.Cleanup(free)
	return b
}

// TestExecPageGuard places every input flush against an inaccessible page, before and
// after it, so any read or write outside the exact ranges faults.
func TestExecPageGuard(t *testing.T) {
	// 0..80 reaches every branch of the exact 0..16-byte tail loads and stores.
	sizes := []int{127, 128, 129, 200, 511, 512, 513, 1000, 1024, 4096}
	for k := range 81 {
		sizes = append(sizes, k)
	}
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, atEnd := range []bool{true, false} {
			for _, k := range sizes {
				// A subtest per size releases its guarded pages before the next one.
				t.Run(fmt.Sprintf("%v/atEnd=%v/k=%d", tier, atEnd, k), func(t *testing.T) {
					decGuardMerges(t, rng, tier, k, atEnd)
					for d := uint8(2); d <= 8; d++ {
						decGuardFlat(t, rng, tier, k, d, atEnd)
					}
				})
			}
		}
	}
}

// decGuardedProgram builds a Program with In = src, Dst = k bytes and R2S, each in its own
// guarded pages, and an arena of arena+ArenaSlack bytes flush against its guard.
func decGuardedProgram(t *testing.T, rng *rand.Rand, src []byte, k, arena int, atEnd bool) *Program {
	p := &Program{In: pageGuarded(t, len(src), atEnd)}
	copy(p.In, src)
	p.Dst = randBytes(rng, pageGuarded(t, k, atEnd))
	p.Arena = randBytes(rng, pageGuarded(t, arena+ArenaSlack, true))
	r := randBytes(rng, pageGuarded(t, R2SLen, atEnd))
	p.R2S = (*[R2SLen]byte)(r)
	return p
}

func decGuardMerges(t *testing.T, rng *rand.Rand, tier Tier, k int, atEnd bool) {
	bm, kr := randBits(rng, k, 0.5)
	ku, kl := uint32(k), uint32(k-kr)
	l, r := randBytes(rng, make([]byte, kl)), randBytes(rng, make([]byte, kr))
	for _, op := range []Op{
		{Kind: OpVV, Flags: FlagDst, K: ku, KR: uint32(kr), L: ku - kl, R: 2*ku - uint32(kr)},
		{Kind: OpVV, K: ku, KR: uint32(kr), Out: 0, L: ku, R: 2 * ku},
		{Kind: OpCV, Flags: FlagDst, P: 7, K: ku, KR: uint32(kr), R: 2*ku - uint32(kr)},
		{Kind: OpCC, Flags: FlagDst, P: 7, Q: 9, K: ku},
	} {
		p := decGuardedProgram(t, rng, bm, k, 3*k, atEnd)
		copy(p.Arena[op.L:], l)
		copy(p.Arena[op.R:], r)
		var want []byte
		switch op.Kind {
		case OpVV:
			want, _ = refMerge(bm, k, l, r)
		case OpCV:
			want, _ = refMerge(bm, k, bytes.Repeat([]byte{7}, k), r)
		default:
			want, _ = refMerge(bm, k, bytes.Repeat([]byte{7}, k), bytes.Repeat([]byte{9}, k))
		}
		p.Ops = []Op{op}
		if ret := tier.Exec(p); ret != -1 {
			t.Fatalf("%v k=%d kind %d: Exec = %d", tier, k, op.Kind, ret)
		}
		out := p.Dst
		if op.Flags&FlagDst == 0 {
			out = p.Arena[:k]
		}
		if !bytes.Equal(out, want) {
			t.Fatalf("%v k=%d kind %d atEnd=%v: wrong output", tier, k, op.Kind, atEnd)
		}
	}
}

func decGuardFlat(t *testing.T, rng *rand.Rand, tier Tier, k int, d uint8, atEnd bool) {
	region := randBytes(rng, make([]byte, (k*int(d)+7)/8))
	if s := k * int(d) % 8; s != 0 {
		region[len(region)-1] &= 1<<s - 1
	}
	p := decGuardedProgram(t, rng, region, k, 0, atEnd)
	// The table is as close to its guard page as P allows.
	first := uint8(0)
	if d < 8 && atEnd {
		first = uint8(min(255, R2SLen-1<<d))
	}
	p.Ops = flatOps(k, d, first, FlagDst, 0, 0)
	if ret := tier.Exec(p); ret != -1 {
		t.Fatalf("%v k=%d d=%d: Exec = %d", tier, k, d, ret)
	}
	want := region
	if d < 8 {
		want = refUnpack(region, k, int(d))
		for i, c := range want {
			want[i] = p.R2S[int(first)+int(c)]
		}
	}
	if !bytes.Equal(p.Dst, want) {
		t.Fatalf("%v k=%d d=%d atEnd=%v: wrong output", tier, k, d, atEnd)
	}
}

// FuzzExec decodes up to four ops from the input, with the arena split into zones so
// that outputs never alias merge inputs, and compares the tiers.
func FuzzExec(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 64, 0, 32, 0, 0, 0, 0, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55})
	f.Add([]byte{3, 2, 3, 1, 0, 0, 2, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{5, 1, 7, 9, 0, 77, 0, 0, 0, 5, 0, 0, 9, 8, 7, 6})
	f.Fuzz(func(t *testing.T, data []byte) {
		tiers := vecTiers(t)
		// Out+K <= zone+7 < L <= L+K < R: no output overlaps a merge input.
		const zone, hdr = 1 << 12, 12
		nops := min(len(data)/hdr, 4)
		in := data[nops*hdr:]
		ops := make([]Op, nops)
		for i := range ops {
			h := data[i*hdr:]
			op := &ops[i]
			op.Kind, op.Flags, op.D, op.P, op.Q = h[0]%9, h[1]&3, h[2]%10, h[3], h[4]
			op.K = uint32(binary.LittleEndian.Uint16(h[5:]) % (zone + 1))
			op.Bm = uint32(h[9]) | uint32(h[10]&15)<<8
			op.Out = uint32(h[11] & 7)
			op.L, op.R = zone+16+uint32(h[11]>>3), 2*zone+64+uint32(h[11]>>5)
			// Mostly the right KR, so that merges run to the end.
			if int(op.Bm)+int(op.K+7)/8 <= len(in) {
				op.KR = uint32(decPopcnt(in[op.Bm:], int(op.K)))
			}
			op.KR += uint32([]int{0, 0, 1, -1}[h[7]&3])
		}
		rng := rand.New(rand.NewPCG(uint64(len(data)), 7))
		e := newEnv(rng, 3*zone+128, zone+8, in)
		e.p.Ops = ops
		for _, tier := range tiers {
			decDiffExec(t, tier, e.clone(), "fuzz")
		}
	})
}

// BenchmarkExecOverhead runs programs of many tiny ops: the time per op is the
// executor's dispatch and setup cost.
func BenchmarkExecOverhead(b *testing.B) {
	tiers := vecTiers(b)
	const nops = 256
	rng := newRNG()
	in := randBytes(rng, make([]byte, 64))
	arena := randBytes(rng, make([]byte, 4096+ArenaSlack))
	var r2s [R2SLen]byte
	for _, c := range []struct {
		name string
		op   Op
	}{
		{"cc1", Op{Kind: OpCC, K: 1, P: 1, Q: 2, Out: 64}},
		{"vv16", Op{Kind: OpVV, K: 16, KR: uint32(decPopcnt(in, 16)), Out: 64, L: 1024, R: 2048}},
		{"cv100", Op{Kind: OpCV, K: 100, KR: uint32(decPopcnt(in, 100)), Out: 64, R: 2048}},
		{"nat5x40", Op{Kind: OpNat, D: 5, K: 40, Out: 64}},
		{"v128d3", Op{Kind: OpV128, D: 3, K: 128, Out: 64}},
	} {
		for _, tier := range tiers {
			b.Run(tier.String()+"/"+c.name, func(b *testing.B) {
				p := Program{Ops: slices.Repeat([]Op{c.op}, nops), In: in, Arena: arena, R2S: &r2s}
				for _, t := range Tiers() {
					if t.Exec(&p) != -1 {
						b.Fatalf("%v failed", t)
					}
				}
				for b.Loop() {
					tier.Exec(&p)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/nops, "ns/exec-op")
			})
		}
	}
}
