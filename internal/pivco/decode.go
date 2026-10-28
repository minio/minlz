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

package pivco

import (
	"math/bits"

	"github.com/minio/minlz/internal/pivco/kernel"
)

const (
	// arenaSize holds the children of any block: with K_right <= K checked, upstream's
	// placement keeps every buffer inside 2N.
	arenaSize = 2*MaxBlockSize + kernel.ArenaSlack
	// maxOps is one op per record plus two for each of at most 64 flats split by layout.
	maxOps = 384
)

// Decoder decodes blocks. The zero value is ready to use and allocates its scratch on
// first use. A Decoder is not safe for concurrent use and must not be copied after first use.
type Decoder struct {
	tier  kernel.Tier
	arena []byte // child buffers of one block
	ops   [maxOps]kernel.Op
}

func (d *Decoder) setup() {
	if d.arena == nil {
		d.arena = make([]byte, arenaSize)
		d.tier = kernel.Best()
	}
}

// Decode decodes the block src, coded with t, to the len(dst) symbols of dst.
// src must be exactly the block and must not overlap dst.
// It returns ErrBlockSize unless 1 <= len(dst) <= MaxBlockSize, ErrInvalidTable if t is
// not built, and an error wrapping ErrCorrupt if src is not exactly a block of len(dst)
// symbols as Encode writes it. On error, dst holds unspecified bytes.
func (d *Decoder) Decode(dst, src []byte, t *Table) error {
	n := len(dst)
	if n == 0 || n > MaxBlockSize {
		return ErrBlockSize
	}
	if !t.valid() {
		return ErrInvalidTable
	}
	d.setup()
	pl := planner{t: t, in: src, ops: d.ops[:0]}
	if err := pl.node(0, uint32(n), 0, 0, 0); err != nil {
		return err
	}
	if pl.pos != len(src) {
		return errTrailing
	}
	// The Program lives on this stack frame: kept in anything that outlives the call,
	// the caller's slices would escape.
	p := kernel.Program{Ops: pl.ops, In: src, Arena: d.arena, Dst: dst, R2S: &t.r2s}
	if d.tier.Exec(&p) >= 0 {
		return errMerge
	}
	return nil
}

// planner parses and validates a block and turns it into kernel ops, in upstream's
// post-order and with its buffer placement (pivco_decode and codec_decode_subtree in
// src/pivco_huffman_codec.c). All ops run once the whole block is parsed.
type planner struct {
	t   *Table
	in  []byte // the block
	ops []kernel.Op
	pos int // read cursor in in
}

// node plans the subtree of record rec at depth holding k symbols, decoded to out with
// tmp as the ping-pong partner. The root decodes to Dst and places its children at the
// start of the arena instead.
func (pl *planner) node(rec int, k, out, tmp uint32, depth int) error {
	if k == 0 {
		return nil
	}
	r := pl.t.sched[rec]
	root := depth == 0
	var fl uint8
	if root {
		fl = kernel.FlagDst
	}
	kind := r.Kind()
	switch kind {
	case kernel.KindFlat:
		return pl.flat(r, k, out, fl)
	case kernel.KindBoth:
		b, bm, err := pl.bitmap(k)
		if err != nil {
			return err
		}
		p, q := pl.t.r2s[r.Param], pl.t.r2s[int(r.Param)+1]
		// Equal leaves mean a single-symbol table, whose bitmap upstream writes as zeros.
		if p == q && ones(b) != 0 {
			return errSingle
		}
		o, err := pl.op()
		if err != nil {
			return err
		}
		o.Kind, o.Flags, o.P, o.Q, o.K, o.Bm, o.Out = kernel.OpCC, fl, p, q, k, bm, out
		return nil
	}
	kr, err := pl.kr(k)
	if err != nil {
		return err
	}
	kl := k - kr
	opk, p, l, rr := uint8(kernel.OpVV), uint8(0), uint32(0), uint32(0)
	if kind == kernel.KindLeafLeft {
		// The right child decodes in place into the tail of out.
		opk, p, rr = kernel.OpCV, pl.t.r2s[r.Param], out+kl
		if root {
			rr, tmp = 0, kr
		}
		if err := pl.node(rec+1, kr, rr, tmp, depth+1); err != nil {
			return err
		}
	} else {
		// The larger child (first on the wire, ties left) decodes in place at
		// out+ksmall, the smaller one into tmp with out as its partner.
		rightFirst := kr > kl
		first, second, kf, ks := rec+int(r.Right), rec+1, kr, kl
		if !rightFirst {
			first, second, kf, ks = second, first, kl, kr
		}
		fo, ft, so, st := out+ks, tmp, tmp, out
		if root {
			fo, ft, so, st = 0, kf, kf, k
		}
		if err := pl.node(first, kf, fo, ft, depth+1); err != nil {
			return err
		}
		if err := pl.node(second, ks, so, st, depth+1); err != nil {
			return err
		}
		l, rr = fo, so
		if rightFirst {
			l, rr = so, fo
		}
	}
	_, bm, err := pl.bitmap(k)
	if err != nil {
		return err
	}
	o, err := pl.op()
	if err != nil {
		return err
	}
	o.Kind, o.Flags, o.P, o.K, o.KR, o.Bm, o.Out, o.L, o.R = opk, fl, p, k, kr, bm, out, l, rr
	return nil
}

// kr reads a K_right header.
func (pl *planner) kr(k uint32) (uint32, error) {
	if len(pl.in)-pl.pos < 2 {
		return 0, errTruncated
	}
	kr := uint32(kernel.Load16(pl.in, pl.pos))
	pl.pos += 2
	if kr > k {
		return 0, errSplit
	}
	return kr, nil
}

// bitmap reads a k-bit bitmap and returns it with its offset. Its set bits are checked
// against K_right when the ops run.
func (pl *planner) bitmap(k uint32) ([]byte, uint32, error) {
	in, nb := pl.in, int(k+7)>>3
	if len(in)-pl.pos < nb {
		return nil, 0, errTruncated
	}
	at := pl.pos
	pl.pos += nb
	return in[at:pl.pos], uint32(at), padding(in[pl.pos-1], k)
}

// ones returns the number of set bits in b.
func ones(b []byte) int {
	n := 0
	for ; len(b) >= 8; b = b[8:] {
		n += bits.OnesCount64(kernel.Load64(b, 0))
	}
	for _, c := range b {
		n += bits.OnesCount8(c)
	}
	return n
}

// padding checks that the bits of the last byte of a bits-long region past its end are 0.
func padding(last byte, bits uint32) error {
	if s := bits & 7; s != 0 && last>>s != 0 {
		return errPadding
	}
	return nil
}

// flat reads a flat region and emits one op per layout part: 512-code and 128-code
// vertical blocks, then a natural tail; a depth-8 flat is the identity code.
func (pl *planner) flat(r kernel.Rec, k, out uint32, fl uint8) error {
	in := pl.in
	d := uint32(r.FlatDepth())
	tb := int(k*d+7) >> 3
	if len(in)-pl.pos < tb {
		return errTruncated
	}
	bm := uint32(pl.pos)
	pl.pos += tb
	if err := padding(in[pl.pos-1], k*d); err != nil {
		return err
	}
	if d == 8 {
		o, err := pl.op()
		if err != nil {
			return err
		}
		o.Kind, o.Flags, o.D, o.P, o.K, o.Bm, o.Out = kernel.OpCopy, fl, 8, r.Param, k, bm, out
		return nil
	}
	n512, n128 := kernel.VertSplit(int(k), uint8(d))
	// In op kind order: OpV512, OpV128, OpNat.
	ns := [3]uint32{uint32(n512), uint32(n128), k - uint32(n512) - uint32(n128)}
	for i, n := range ns {
		if n == 0 {
			continue
		}
		o, err := pl.op()
		if err != nil {
			return err
		}
		o.Kind, o.Flags, o.D, o.P, o.K, o.Bm, o.Out = kernel.OpV512+uint8(i), fl, uint8(d), r.Param, n, bm, out
		bm += n * d / 8
		out += n
	}
	return nil
}

// op appends a zeroed op. Its geometry is not checked here: every tier's Exec checks it.
// The caller assigns the fields one by one; an Op value built first and copied in
// costs store-forwarding stalls.
func (pl *planner) op() (*kernel.Op, error) {
	n := len(pl.ops)
	if n >= cap(pl.ops) {
		return nil, errGeometry
	}
	// Not append: escape analysis sees through storing pl.ops only in this form.
	pl.ops = pl.ops[:n+1]
	o := &pl.ops[n]
	*o = kernel.Op{}
	return o, nil
}
