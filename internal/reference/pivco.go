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
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"slices"
)

// Node kinds of the coding tree (SPEC_PIVCO.md 2.3). Groups are pivcoBoth and up.
const (
	pivcoFull = iota
	pivcoLeafLeft
	pivcoBoth
	pivcoFlat
	pivcoLeaf
)

type pivcoNode struct {
	kind        int
	left, right *pivcoNode
	first, d    int // of a group: its first rank, and log2 of its number of values
}

// pivcoTable is the coding tree of a Huffman table (SPEC_PIVCO.md 2).
type pivcoTable struct {
	root   *pivcoNode
	ranks  []byte // the value of each rank
	single bool   // the table codes one value (2.5)
}

// newPivcoTable builds the coding tree of the code lengths lens (SPEC_PIVCO.md 1.2, 2).
func newPivcoTable(lens *[256]uint8) (*pivcoTable, error) {
	var coded []byte
	kraft := 0
	for v, l := range lens {
		if l > maxCodeLen {
			return nil, fmt.Errorf("code length %d of value %d above %d", l, v, maxCodeLen)
		}
		if l > 0 {
			coded = append(coded, byte(v))
			kraft += 1 << (maxCodeLen - l)
		}
	}
	switch {
	case len(coded) == 0:
		return nil, errors.New("table codes no value")
	case len(coded) == 1:
		v := coded[0]
		return &pivcoTable{root: &pivcoNode{kind: pivcoBoth, d: 1}, ranks: []byte{v, v}, single: true}, nil
	case kraft != 1<<maxCodeLen:
		return nil, errors.New("table is not a complete code")
	}

	type group struct {
		depth, length int
		values        []byte
	}
	var groups []group
	for l := 1; l <= maxCodeLen; l++ {
		var vals []byte
		for _, v := range coded {
			if int(lens[v]) == l {
				vals = append(vals, v)
			}
		}
		for b := 8; b >= 0; b-- {
			if len(vals)>>b&1 == 1 {
				groups = append(groups, group{depth: l - b, length: l, values: vals[:1<<b]})
				vals = vals[1<<b:]
			}
		}
	}
	slices.SortFunc(groups, func(a, b group) int { return cmp.Or(a.depth-b.depth, a.length-b.length) })

	t := &pivcoTable{}
	errTree := errors.New("table paths do not form a tree")
	path := 0
	for i, g := range groups {
		if i > 0 {
			path = (path + 1) << (g.depth - groups[i-1].depth)
		}
		if path >= 1<<g.depth {
			return nil, errTree
		}
		p := &t.root
		for bit := g.depth - 1; bit >= 0; bit-- {
			if *p == nil {
				*p = &pivcoNode{kind: pivcoFull}
			}
			if (*p).kind != pivcoFull {
				return nil, errTree
			}
			if path>>bit&1 == 0 {
				p = &(*p).left
			} else {
				p = &(*p).right
			}
		}
		if *p != nil {
			return nil, errTree
		}
		nd := &pivcoNode{first: len(t.ranks), d: bits.Len(uint(len(g.values))) - 1}
		switch nd.d {
		case 0:
			nd.kind = pivcoLeaf
		case 1:
			nd.kind = pivcoBoth
		default:
			nd.kind = pivcoFlat
		}
		*p = nd
		t.ranks = append(t.ranks, g.values...)
	}
	var kinds func(nd *pivcoNode) error
	kinds = func(nd *pivcoNode) error {
		if nd.kind != pivcoFull {
			return nil
		}
		// 2.3: the right child of an internal node is never a leaf.
		if nd.left == nil || nd.right == nil || nd.right.kind == pivcoLeaf {
			return errTree
		}
		if nd.left.kind == pivcoLeaf {
			nd.kind = pivcoLeafLeft
		}
		return cmp.Or(kinds(nd.left), kinds(nd.right))
	}
	return t, kinds(t.root)
}

// decode decodes the pivco block src of n values (SPEC_PIVCO.md 5, 6).
func (t *pivcoTable) decode(src []byte, n int) ([]byte, error) {
	if n < 1 || n > 65535 {
		return nil, fmt.Errorf("pivco block of %d values", n)
	}
	r := pivcoReader{t: t, src: src}
	v, err := r.node(t.root, n)
	if err != nil {
		return nil, err
	}
	if len(r.src) != 0 {
		return nil, fmt.Errorf("%d bytes after the pivco block", len(r.src))
	}
	return v, nil
}

// pivcoReader reads the regions of a pivco block in order.
type pivcoReader struct {
	t   *pivcoTable
	src []byte
}

func (r *pivcoReader) take(n int) ([]byte, error) {
	if n > len(r.src) {
		return nil, fmt.Errorf("pivco region of %d bytes runs past the end of the block", n)
	}
	b := r.src[:n]
	r.src = r.src[n:]
	return b, nil
}

// bitmap reads the bitmap of a node holding k values (SPEC_PIVCO.md 3.2).
func (r *pivcoReader) bitmap(k int) ([]byte, error) {
	b, err := r.take((k + 7) / 8)
	if err == nil && k&7 != 0 && b[len(b)-1]>>(k&7) != 0 {
		err = errors.New("pivco bitmap has an unused bit set")
	}
	return b, err
}

// node decodes the subtree of nd, which holds k values.
func (r *pivcoReader) node(nd *pivcoNode, k int) ([]byte, error) {
	if k == 0 {
		return nil, nil
	}
	out := make([]byte, k)
	switch nd.kind {
	case pivcoBoth:
		bm, err := r.bitmap(k)
		if err != nil {
			return nil, err
		}
		for j := range out {
			bit := int(bm[j>>3] >> (j & 7) & 1)
			if r.t.single && bit != 0 {
				return nil, errors.New("pivco bitmap bit set with one coded value")
			}
			out[j] = r.t.ranks[nd.first+bit]
		}
		return out, nil
	case pivcoFlat:
		b, err := r.take((k*nd.d + 7) / 8)
		if err != nil {
			return nil, err
		}
		codes, err := flatCodes(b, k, nd.d)
		if err != nil {
			return nil, err
		}
		for j, x := range codes {
			out[j] = r.t.ranks[nd.first+x]
		}
		return out, nil
	}

	rc, err := r.take(2)
	if err != nil {
		return nil, err
	}
	kr := int(binary.LittleEndian.Uint16(rc))
	if kr > k {
		return nil, fmt.Errorf("pivco right count %d above %d values", kr, k)
	}
	var left, right []byte
	switch {
	case nd.kind == pivcoLeafLeft:
		right, err = r.node(nd.right, kr)
		left = bytes.Repeat([]byte{r.t.ranks[nd.left.first]}, k-kr)
	case k-kr >= kr:
		if left, err = r.node(nd.left, k-kr); err == nil {
			right, err = r.node(nd.right, kr)
		}
	default:
		if right, err = r.node(nd.right, kr); err == nil {
			left, err = r.node(nd.left, k-kr)
		}
	}
	if err != nil {
		return nil, err
	}
	bm, err := r.bitmap(k)
	if err != nil {
		return nil, err
	}
	set := 0
	for _, b := range bm {
		set += bits.OnesCount8(b)
	}
	if set != kr {
		return nil, fmt.Errorf("pivco bitmap has %d bits set, right count %d", set, kr)
	}
	for j := range out {
		if bm[j>>3]>>(j&7)&1 == 1 {
			out[j], right = right[0], right[1:]
		} else {
			out[j], left = left[0], left[1:]
		}
	}
	return out, nil
}

// flatCodes reads the k flat codes of d bits in b (SPEC_PIVCO.md 3.3).
func flatCodes(b []byte, k, d int) ([]int, error) {
	var n512, n128 int
	if d < 8 {
		n512 = 512 * (k / 512)
		n128 = 128 * ((k - n512) / 128)
	}
	var c []int
	for len(c) < n512+n128 {
		w := 16
		if len(c) < n512 {
			w = 64
		}
		blk := make([]int, 8*w)
		for l := range w {
			var lane uint64
			for i := range d {
				lane |= uint64(b[i*w+l]) << (8 * i)
			}
			for m := range 8 {
				blk[m*w+l] = int(lane >> (m * d) & (1<<d - 1))
			}
		}
		c = append(c, blk...)
		b = b[w*d:]
	}
	rest := k - len(c)
	for i := range rest {
		x := 0
		for m := range d {
			bit := i*d + m
			x |= int(b[bit>>3]>>(bit&7)&1) << m
		}
		c = append(c, x)
	}
	if end := rest * d; end&7 != 0 && b[end>>3]>>(end&7) != 0 {
		return nil, errors.New("pivco flat codes have an unused bit set")
	}
	return c, nil
}
