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
	"math/rand/v2"
	"testing"
)

const opFlat = 255 // test-only node kind: any flat region

// env is a Program over guarded buffers. Arena and Dst start with junk, so kernels cannot
// rely on zeroed memory.
type env struct {
	p      Program
	dstAll []byte
	r2s    [R2SLen]byte
}

func newEnv(rng *rand.Rand, arenaLen, dstLen int, in []byte) *env {
	e := &env{}
	e.p.Arena = randBytes(rng, make([]byte, arenaLen+ArenaSlack))
	e.p.Dst, e.dstAll = guarded(randBytes(rng, make([]byte, dstLen)))
	e.p.In, _ = guarded(in)
	randBytes(rng, e.r2s[:])
	e.p.R2S = &e.r2s
	return e
}

// exec runs Exec and checks that nothing changed except the outputs of Ops[:upto]
// (the ops allowed to write).
func (e *env) exec(t *testing.T, tier Tier, upto int) int {
	t.Helper()
	arena, dst := bytes.Clone(e.p.Arena), bytes.Clone(e.dstAll)
	in, r2s := bytes.Clone(e.p.In), e.r2s
	ops := e.p.Ops[:upto]
	ret := tier.Exec(&e.p)
	for _, op := range ops {
		if op.Flags&FlagDst != 0 {
			copy(dst[op.Out:][:op.K], e.dstAll[op.Out:])
		} else {
			copy(arena[op.Out:][:op.K], e.p.Arena[op.Out:])
		}
	}
	if !bytes.Equal(arena, e.p.Arena) || !bytes.Equal(dst, e.dstAll) || !bytes.Equal(in, e.p.In) || r2s != e.r2s {
		t.Fatalf("%v: Exec wrote outside its outputs (ret %d)", tier, ret)
	}
	return ret
}

func (e *env) out(op Op) []byte {
	if op.Flags&FlagDst != 0 {
		return e.p.Dst[op.Out : op.Out+op.K]
	}
	return e.p.Arena[op.Out : op.Out+op.K]
}

func TestExecMerge(t *testing.T) {
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, k := range testSizes {
			for _, density := range []float64{0, 1, 0.5, 0.05, 0.95} {
				testMerge(t, tier, rng, k, density)
			}
		}
	}
}

func testMerge(t *testing.T, tier Tier, rng *rand.Rand, k int, density float64) {
	t.Helper()
	bm, kr := randBits(rng, k, density)
	kl := k - kr
	lv, rv := randBytes(rng, make([]byte, kl)), randBytes(rng, make([]byte, kr))
	c0, c1 := byte(rng.Uint32()), byte(rng.Uint32())
	wantVV, _ := refMerge(bm, k, lv, rv)
	wantCV, _ := refMerge(bm, k, bytes.Repeat([]byte{c0}, k), rv)
	wantCC, _ := refMerge(bm, k, bytes.Repeat([]byte{c0}, k), bytes.Repeat([]byte{c1}, k))

	// The bitmap sits at the end of In behind a junk prefix: reads must be exact.
	const pre, off = 3, 5
	src := append(randBytes(rng, make([]byte, pre)), bm...)
	ku, kru, klu := uint32(k), uint32(kr), uint32(kl)
	type place struct {
		at uint32
		v  []byte
	}
	for _, c := range []struct {
		name   string
		op     Op
		places []place
		want   []byte
	}{
		{"vv", Op{Kind: OpVV, Out: off, L: off + ku, R: off + 2*ku}, []place{{off + ku, lv}, {off + 2*ku, rv}}, wantVV},
		{"vv/inplace-left", Op{Kind: OpVV, Out: off, L: off + kru, R: off + 2*ku}, []place{{off + kru, lv}, {off + 2*ku, rv}}, wantVV},
		{"vv/inplace-right", Op{Kind: OpVV, Out: off, L: off + 2*ku, R: off + klu}, []place{{off + 2*ku, lv}, {off + klu, rv}}, wantVV},
		{"vv/dst", Op{Kind: OpVV, Flags: FlagDst, L: off, R: off + ku}, []place{{off, lv}, {off + ku, rv}}, wantVV},
		{"cv", Op{Kind: OpCV, P: c0, Out: off, R: off + ku}, []place{{off + ku, rv}}, wantCV},
		{"cv/inplace", Op{Kind: OpCV, P: c0, Out: off, R: off + klu}, []place{{off + klu, rv}}, wantCV},
		{"cv/dst", Op{Kind: OpCV, Flags: FlagDst, P: c0, R: off}, []place{{off, rv}}, wantCV},
		{"cc", Op{Kind: OpCC, P: c0, Q: c1, Out: off}, nil, wantCC},
		{"cc/dst", Op{Kind: OpCC, Flags: FlagDst, P: c0, Q: c1}, nil, wantCC},
	} {
		for _, krDelta := range []int{0, 1, -1} {
			if krDelta != 0 && (c.op.Kind == OpCC || kr+krDelta < 0 || kr+krDelta > k) {
				continue
			}
			op := c.op
			op.K, op.KR, op.Bm = ku, uint32(kr+krDelta), pre
			e := newEnv(rng, off+3*k, k, src)
			for _, pl := range c.places {
				copy(e.p.Arena[pl.at:], pl.v)
			}
			e.p.Ops = []Op{op}
			want := -1
			if krDelta != 0 {
				want = 0
			}
			if ret := e.exec(t, tier, 1); ret != want {
				t.Fatalf("%v %s k=%d kr=%d (op KR %d): Exec = %d, want %d", tier, c.name, k, kr, op.KR, ret, want)
			}
			if krDelta == 0 && !bytes.Equal(e.out(op), c.want) {
				t.Fatalf("%v %s k=%d kr=%d density=%v: wrong output", tier, c.name, k, kr, density)
			}
		}
	}
}

// flatOps splits an n-code flat region like the planner does.
func flatOps(n int, d, p uint8, flags uint8, bm, out uint32) []Op {
	if d == 8 {
		return []Op{{Kind: OpCopy, Flags: flags, D: 8, K: uint32(n), Bm: bm, Out: out}}
	}
	n512, n128 := VertSplit(n, d)
	var ops []Op
	for _, s := range []struct {
		kind uint8
		k    int
	}{{OpV512, n512}, {OpV128, n128}, {OpNat, n - n512 - n128}} {
		if s.k == 0 && (n != 0 || s.kind != OpNat) {
			continue
		}
		ops = append(ops, Op{Kind: s.kind, Flags: flags, D: d, P: p, K: uint32(s.k), Bm: bm, Out: out})
		bm += uint32(s.k * int(d) / 8)
		out += uint32(s.k)
	}
	return ops
}

func TestExecFlat(t *testing.T) {
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, n := range testSizes {
			for d := uint8(2); d <= 8; d++ {
				// Random regions: padding bits are the planner's business.
				region := randBytes(rng, make([]byte, (n*int(d)+7)/8))
				codes := refUnpack(region, n, int(d))
				const pre, off = 2, 7
				src := append(randBytes(rng, make([]byte, pre)), region...)
				flags := uint8(n % 2)
				e := newEnv(rng, off+n, n, src)
				p := uint8(0)
				if d < 8 {
					p = uint8(rng.IntN(257 - 1<<d))
				}
				out := uint32(off)
				if flags&FlagDst != 0 {
					out = 0
				}
				e.p.Ops = flatOps(n, d, p, flags, pre, out)
				want := region
				if d < 8 {
					want = make([]byte, n)
					for i, c := range codes {
						want[i] = e.r2s[int(p)+int(c)]
					}
				}
				if ret := e.exec(t, tier, len(e.p.Ops)); ret != -1 {
					t.Fatalf("%v n=%d d=%d: Exec = %d", tier, n, d, ret)
				}
				got := e.p.Dst
				if flags&FlagDst == 0 {
					got = e.p.Arena[off : off+n]
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%v n=%d d=%d flags=%d: wrong output", tier, n, d, flags)
				}
			}
			// Natural D=8 through R2S (not emitted by the planner, but a valid op).
			region := randBytes(rng, make([]byte, n))
			e := newEnv(rng, 0, n, region)
			p := uint8(rng.IntN(R2SLen - 256 + 1))
			e.p.Ops = []Op{{Kind: OpNat, Flags: FlagDst, D: 8, P: p, K: uint32(n)}}
			if ret := e.exec(t, tier, 1); ret != -1 {
				t.Fatalf("%v nat8 n=%d: Exec = %d", tier, n, ret)
			}
			for i, c := range region {
				if e.p.Dst[i] != e.r2s[int(p)+int(c)] {
					t.Fatalf("%v nat8 n=%d: wrong output at %d", tier, n, i)
				}
			}
		}
	}
}

// tnode is a random decode tree whose ops are placed exactly like the upstream decoder
// (pivco_decode, codec_decode_subtree), which exercises the nested in-place and
// ping-pong aliasing.
type tnode struct {
	kind, d, p, q uint8
	k, kr         int
	bm            uint32
	l, r          *tnode
}

type treeGen struct {
	rng *rand.Rand
	in  []byte
	r2s *[R2SLen]byte
}

// store appends b to In and returns its offset.
func (g *treeGen) store(b []byte) uint32 {
	g.in = append(g.in, b...)
	return uint32(len(g.in) - len(b))
}

// node returns a subtree decoding to k values and those values.
func (g *treeGen) node(k, depth int) (*tnode, []byte) {
	n := &tnode{k: k}
	if k == 0 {
		return n, nil
	}
	sel := g.rng.IntN(10)
	if depth >= 7 || k < 2 {
		sel = 7 + g.rng.IntN(3)
	}
	if sel >= 8 {
		n.kind, n.d = opFlat, uint8(2+g.rng.IntN(7))
		region := randBytes(g.rng, make([]byte, (k*int(n.d)+7)/8))
		n.bm = g.store(region)
		if n.d == 8 {
			return n, region
		}
		n.p = uint8(g.rng.IntN(257 - 1<<n.d))
		out := refUnpack(region, k, int(n.d))
		for i, c := range out {
			out[i] = g.r2s[int(n.p)+int(c)]
		}
		return n, out
	}
	densities := []float64{0, 0.02, 0.3, 0.5, 0.7, 0.98, 1}
	bm, kr := randBits(g.rng, k, densities[g.rng.IntN(len(densities))])
	n.kr, n.p, n.q = kr, byte(g.rng.Uint32()), byte(g.rng.Uint32())
	n.bm = g.store(bm)
	var lout, rout []byte
	switch {
	case sel < 5:
		n.kind = OpVV
		n.l, lout = g.node(k-kr, depth+1)
		n.r, rout = g.node(kr, depth+1)
	case sel < 7:
		n.kind = OpCV
		lout = bytes.Repeat([]byte{n.p}, k)
		n.r, rout = g.node(kr, depth+1)
	default:
		n.kind = OpCC
		lout, rout = bytes.Repeat([]byte{n.p}, k), bytes.Repeat([]byte{n.q}, k)
	}
	out, _ := refMerge(bm, k, lout, rout)
	return n, out
}

func (n *tnode) op(out, l, r uint32) []Op {
	if n.kind == opFlat {
		return flatOps(n.k, n.d, n.p, 0, n.bm, out)
	}
	return []Op{{Kind: n.kind, P: n.p, Q: n.q, K: uint32(n.k), KR: uint32(n.kr), Bm: n.bm, Out: out, L: l, R: r}}
}

// sub mirrors the upstream subtree placement: out[0:K), scratch at tmp.
func (n *tnode) sub(ops []Op, out, tmp uint32) []Op {
	if n.k == 0 {
		return ops
	}
	kl, kr := uint32(n.k-n.kr), uint32(n.kr)
	switch n.kind {
	case OpCV:
		rb := out + kl
		ops = n.r.sub(ops, rb, tmp)
		return append(ops, n.op(out, 0, rb)...)
	case OpVV:
		var lb, rb uint32
		if kr > kl {
			rb, lb = out+kl, tmp
			ops = n.r.sub(ops, rb, tmp)
			ops = n.l.sub(ops, lb, out)
		} else {
			lb, rb = out+kr, tmp
			ops = n.l.sub(ops, lb, tmp)
			ops = n.r.sub(ops, rb, out)
		}
		return append(ops, n.op(out, lb, rb)...)
	}
	return append(ops, n.op(out, 0, 0)...)
}

// root mirrors the upstream root placement; the root op writes Dst.
func (n *tnode) root(ops []Op) []Op {
	kl, kr, k := uint32(n.k-n.kr), uint32(n.kr), uint32(n.k)
	var l, r uint32
	switch n.kind {
	case OpCV:
		ops = n.r.sub(ops, 0, kr)
	case OpVV:
		if kr > kl {
			ops = n.r.sub(ops, 0, kr)
			ops = n.l.sub(ops, kr, k)
			l, r = kr, 0
		} else {
			ops = n.l.sub(ops, 0, kl)
			ops = n.r.sub(ops, kl, k)
			l, r = 0, kl
		}
	}
	m := len(ops)
	ops = append(ops, n.op(0, l, r)...)
	for i := range ops[m:] {
		ops[m+i].Flags |= FlagDst
	}
	return ops
}

func TestExecTree(t *testing.T) {
	for _, tier := range Tiers() {
		rng := newRNG()
		for iter := range 400 {
			n := testSizes[rng.IntN(len(testSizes))]
			if n == 0 || iter%50 == 0 {
				n = 65535
			}
			var r2s [R2SLen]byte
			randBytes(rng, r2s[:])
			g := &treeGen{rng: rng, r2s: &r2s}
			root, want := g.node(n, 0)
			e := newEnv(rng, 2*n, n, g.in)
			e.r2s = r2s
			ops := root.root(nil)
			e.p.Ops = ops
			if ret := e.exec(t, tier, len(ops)); ret != -1 {
				t.Fatalf("%v iter %d n=%d: op %d (%+v) failed", tier, iter, n, ret, ops[ret])
			}
			if !bytes.Equal(e.p.Dst, want) {
				t.Fatalf("%v iter %d n=%d: wrong output (%d ops)", tier, iter, n, len(ops))
			}
		}
	}
}

func TestExecFailClosed(t *testing.T) {
	const arenaLen, dstLen = 1024, 64
	inLen := 256
	lim := uint32(arenaLen) // last addressable arena byte + 1
	valid := func(out uint32) Op { return Op{Kind: OpCC, P: 1, Q: 2, K: 16, Bm: 8, Out: out} }
	bad := []struct {
		name    string
		op      Op
		written bool // the op runs and is rejected afterwards
	}{
		{"kind7", Op{Kind: 7, K: 8, Out: 200}, false},
		{"kind8", Op{Kind: 8, K: 8, Out: 200}, false},
		{"kind255", Op{Kind: 255, K: 8, Out: 200}, false},
		{"v512/d0", Op{Kind: OpV512, D: 0, K: 512, Out: 200}, false},
		{"v512/d1", Op{Kind: OpV512, D: 1, K: 512, Out: 200}, false},
		{"v512/d8", Op{Kind: OpV512, D: 8, K: 0, Out: 200}, false},
		{"v128/d8", Op{Kind: OpV128, D: 8, K: 128, Out: 200}, false},
		{"v128/d9", Op{Kind: OpV128, D: 9, K: 128, Out: 200}, false},
		{"nat/d0", Op{Kind: OpNat, D: 0, K: 8, Out: 200}, false},
		{"nat/d1", Op{Kind: OpNat, D: 1, K: 8, Out: 200}, false},
		{"nat/d9", Op{Kind: OpNat, D: 9, K: 8, Out: 200}, false},
		{"copy/d7", Op{Kind: OpCopy, D: 7, K: 8, Out: 200}, false},
		{"copy/d0", Op{Kind: OpCopy, D: 0, K: 8, Out: 200}, false},
		{"v512/k", Op{Kind: OpV512, D: 2, K: 128, Out: 200}, false},
		{"v128/k", Op{Kind: OpV128, D: 2, K: 64, Out: 200}, false},
		{"r2s", Op{Kind: OpNat, D: 7, P: 255, K: 8, Out: 200}, false},
		{"r2s/d8", Op{Kind: OpNat, D: 8, P: 65, K: 8, Out: 200}, false},
		{"out", Op{Kind: OpCC, K: 8, Out: lim - 7}, false},
		{"out/huge", Op{Kind: OpCC, K: 0xffffffff, Out: 0xffffffff}, false},
		{"out/dst", Op{Kind: OpCC, Flags: FlagDst, K: 8, Out: dstLen - 7}, false},
		{"bm", Op{Kind: OpCC, K: 64, Bm: uint32(inLen) - 7, Out: 200}, false},
		{"region", Op{Kind: OpNat, D: 3, K: 8, Bm: uint32(inLen) - 2, Out: 200}, false},
		{"region/copy", Op{Kind: OpCopy, D: 8, K: 8, Bm: uint32(inLen) - 7, Out: 200}, false},
		{"region/huge", Op{Kind: OpNat, D: 8, K: 0xffffffff, Bm: 0, Out: 200}, false},
		{"vv/l", Op{Kind: OpVV, K: 8, L: lim - 7, R: 300, Out: 200}, false},
		{"vv/r", Op{Kind: OpVV, K: 8, L: 300, R: lim - 7, Out: 200}, false},
		{"cv/r", Op{Kind: OpCV, K: 8, R: 0xfffffff0, Out: 200}, false},
		{"vv/kr+1", Op{Kind: OpVV, K: 64, KR: 33, L: 300, R: 400, Out: 200}, true},
		{"vv/kr-1", Op{Kind: OpVV, K: 64, KR: 31, L: 300, R: 400, Out: 200}, true},
		{"vv/kr-huge", Op{Kind: OpVV, K: 64, KR: 0xffffffff, L: 300, R: 400, Out: 200}, true},
		{"cv/kr+1", Op{Kind: OpCV, K: 64, KR: 33, R: 400, Out: 200}, true},
		{"cv/kr0", Op{Kind: OpCV, K: 64, KR: 0, R: 400, Out: 200}, true},
	}
	for _, tier := range Tiers() {
		rng := newRNG()
		for _, c := range bad {
			in := randBytes(rng, make([]byte, inLen))
			for i := range 8 {
				in[i] = 0x55 // 32 of 64 bits set at Bm=0
			}
			in[8], in[9] = 0, 0 // the valid ops' bitmap
			e := newEnv(rng, arenaLen, dstLen, in)
			e.p.Ops = []Op{valid(0), valid(100), c.op, valid(150)}
			upto := 2
			if c.written {
				upto = 3
			}
			if ret := e.exec(t, tier, upto); ret != 2 {
				t.Fatalf("%v %s: Exec = %d, want 2", tier, c.name, ret)
			}
			if !bytes.Equal(e.out(valid(100)), bytes.Repeat([]byte{1}, 16)) {
				t.Fatalf("%v %s: op before the failure did not run", tier, c.name)
			}
		}
		// A nil R2S fails flat lookups instead of crashing.
		e := newEnv(rng, arenaLen, dstLen, make([]byte, 8))
		e.p.R2S = nil
		e.p.Ops = []Op{{Kind: OpNat, D: 4, K: 8}}
		if ret := e.exec(t, tier, 0); ret != 0 {
			t.Fatalf("%v nil R2S: Exec = %d", tier, ret)
		}
	}
}
