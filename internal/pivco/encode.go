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

import "github.com/minio/minlz/internal/pivco/kernel"

// stageSlots is one bitmap staging slot per depth of internal nodes, 0..MaxCodeLen-1.
const stageSlots = MaxCodeLen

// histDirect is the shortest input that Histogram counts with a kernel: a plain loop beats
// the kernels' fixed cost below it.
const histDirect = 512

// Encoder codes blocks. The zero value is ready to use and allocates its scratch,
// sized for MaxBlockSize, on first use. An Encoder is not safe for concurrent use
// and must not be copied after first use.
type Encoder struct {
	tier     kernel.Tier
	ranks    []byte // gather output, root partition input
	tmp      []byte // right-child ranks of the walk
	stage    [stageSlots][]byte
	hcount   [256]uint32
	hscratch []byte
}

func (e *Encoder) setup() {
	if e.ranks != nil {
		return
	}
	// Right ranks stack up along a path through the nodes that write them, FULL and
	// LEAF_LEFT, which are at most MaxCodeLen-2 deep: at most n per node.
	const n = MaxBlockSize
	rs, ts, bs := n+kernel.RankSlack, (MaxCodeLen-1)*n+kernel.RankSlack, (n+7)/8+kernel.RankSlack
	slab := make([]byte, rs+ts+stageSlots*bs)
	e.ranks, slab = slab[:rs:rs], slab[rs:]
	e.tmp, slab = slab[:ts:ts], slab[ts:]
	for i := range e.stage {
		e.stage[i], slab = slab[:bs:bs], slab[bs:]
	}
	e.tier = kernel.Best()
}

// Histogram adds the byte counts of src to hist.
// src must be shorter than 4 GiB: the kernels count in uint32.
func (e *Encoder) Histogram(hist *[256]uint64, src []byte) {
	if len(src) < histDirect {
		for _, b := range src {
			hist[b]++
		}
		return
	}
	e.setup()
	if e.hscratch == nil && e.tier.HistUsesScratch() {
		e.hscratch = make([]byte, kernel.HistScratch)
	}
	e.hcount = [256]uint32{}
	e.tier.Histogram(&e.hcount, src, e.hscratch)
	for i, v := range e.hcount {
		hist[i] += uint64(v)
	}
}

// Encode writes the block that codes src with t to dst, which must be exactly
// t.BlockSize of the symbol counts of src; it panics if it is not.
// It returns ErrBlockSize unless 1 <= len(src) <= MaxBlockSize, ErrInvalidTable if t is
// not built and ErrSymbolNotInTable if src has a symbol without a code.
func (e *Encoder) Encode(dst, src []byte, t *Table) error {
	n := len(src)
	if n == 0 || n > MaxBlockSize {
		return ErrBlockSize
	}
	if !t.valid() {
		return ErrInvalidTable
	}
	e.setup()
	// Unused symbols map to rank 0xFF, which is a real rank only in a full table.
	if e.tier.Gather(e.ranks, src, &t.s2r) == 0xFF && t.nranks < 256 {
		return ErrSymbolNotInTable
	}
	w := encWalk{e: e, t: t, out: dst}
	w.node(0, e.ranks, n, e.tmp, 0)
	if w.pos != len(dst) {
		panic("pivco: dst is not the block size")
	}
	return nil
}

// encWalk is the state of one block's encode walk.
type encWalk struct {
	e   *Encoder
	t   *Table
	out []byte // regions are written at out[pos:]
	pos int
}

// node ports upstream codec_encode_node (src/pivco_huffman_codec.c): the K_right header in
// pre-order, the children larger first, the bitmap in post-order. ranks[:n] are the
// subtree's symbol ranks; tmp is scratch for the right ranks of this and all deeper nodes.
func (w *encWalk) node(rec int, ranks []byte, n int, tmp []byte, depth int) {
	if n == 0 {
		return
	}
	e, r := w.e, w.t.sched[rec]
	kind := r.Kind()
	if kind == kernel.KindFlat {
		d := r.FlatDepth()
		tb := (n*int(d) + 7) >> 3
		e.tier.Pack(w.out[w.pos:w.pos+tb], ranks, n, d, r.Param)
		w.pos += tb
		return
	}
	bm := e.stage[depth]
	var nr int
	switch kind {
	case kernel.KindBoth:
		nr = e.tier.PartNone(ranks, n, bm, r.Param)
	case kernel.KindLeafLeft:
		nr = e.tier.PartRight(ranks, n, tmp, bm, r.Param)
	default:
		nr = e.tier.PartFull(ranks, n, tmp, bm, r.Param)
	}
	nl := n - nr
	if kind != kernel.KindBoth {
		w.out[w.pos], w.out[w.pos+1] = byte(nr), byte(nr>>8)
		w.pos += 2
		sub, right := tmp[nr:], rec+int(r.Right)
		switch {
		case kind == kernel.KindLeafLeft:
			w.node(rec+1, tmp, nr, sub, depth+1)
		case nr > nl:
			w.node(right, tmp, nr, sub, depth+1)
			w.node(rec+1, ranks, nl, sub, depth+1)
		default:
			w.node(rec+1, ranks, nl, sub, depth+1)
			w.node(right, tmp, nr, sub, depth+1)
		}
	}
	nb := (n + 7) >> 3
	w.pos += copy(w.out[w.pos:w.pos+nb], bm[:nb])
}
