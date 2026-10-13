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

// Package kernel holds the data-parallel primitives of the pivco codec
// (upstream src/pivco_huffman_primitives.h), per CPU tier.
// The types in this file are the contract the tiers implement; changing them requires
// regenerating the assembly (_generate/pivco).
//
// Dispatch is a switch on Tier with direct calls. Calls through function values
// would make every argument escape to the heap.
package kernel

// Rec is one pre-order schedule record (upstream pivco_sched_rec_t).
//
//	Kind = Op&3; flat depth D = Op>>2 (2..8).
//	Param: FULL/LEAF_LEFT/BOTH: largest rank of the left subtree (ranks > Param go right);
//	       FLAT: first rank of the subtree.
//	Right: offset to the right child's record (FULL), 1 (LEAF_LEFT), 0 otherwise.
//	The left child, when it has a record, is at index+1.
type Rec struct{ Op, Param, Right uint8 }

// Record kinds.
const (
	KindFull     = 0
	KindFlat     = 1
	KindBoth     = 2
	KindLeafLeft = 3
)

// Kind returns the record kind.
func (r Rec) Kind() uint8 { return r.Op & 3 }

// FlatDepth returns D for flat records.
func (r Rec) FlatDepth() uint8 { return r.Op >> 2 }

// Decoder op kinds. All merges read bitmap bit j as (bm[j>>3]>>(j&7))&1.
const (
	OpVV   = iota // out[j] = bit ? R[r++] : L[l++]
	OpCV          // out[j] = bit ? R[r++] : P
	OpCC          // out[j] = bit ? Q : P
	OpV512        // flat, vertical layout, 64-lane blocks (K is a multiple of 512)
	OpV128        // flat, vertical layout, 16-lane blocks (K is a multiple of 128)
	OpNat         // flat, natural LSB-first packing
	OpCopy        // flat D=8: out = region (identity code to symbol map)
)

// FlagDst makes Out index Program.Dst instead of Program.Arena.
const FlagDst = 1

// Op is one decoder step. It is pointer-free and 32 bytes; the assembly reads its fields
// by offset. L and R index Program.Arena. For flats, P is the first rank into
// Program.R2S and Bm the start of the packed region; codes are masked to D bits before lookup.
type Op struct {
	Kind, Flags, D uint8
	P, Q           uint8
	_              [3]uint8
	K, KR          uint32 // KR: number of set bits required in bitmap[0:K) (VV, CV)
	Bm, Out, L, R  uint32
}

// R2SLen is the length of the padded rank-to-symbol table; vector kernels may load up
// to 64 bytes at any first rank.
const R2SLen = 256 + 64

// Program is the list of decoder ops for one block. Exec runs the ops in order and
// checks each one against the slices before it runs it, so a malformed op fails
// closed; beyond that, kernels only rely on the slack documented below.
type Program struct {
	Ops   []Op
	In    []byte // the block; reads stay inside it (no slack), not always inside an op's region
	Arena []byte // child buffers; kernels may read up to ArenaSlack bytes past any cursor
	Dst   []byte // final output; writes are exact
	R2S   *[R2SLen]byte
}

// Slack required by the kernels, in bytes.
const (
	// RankSlack: rank, right-output and bitmap staging slices passed to the encoder kernels
	// must have at least n+RankSlack (bitmaps: ceil(n/8)+RankSlack) bytes; kernels may store
	// junk there. In-place left outputs of PartFull are exact.
	RankSlack = 64
	// ArenaSlack: Program.Arena must extend this far past the last byte any op can address.
	ArenaSlack = 128
	// HistScratch is the scratch size passed to Histogram.
	HistScratch = 4*16384 + 64
)

// Tier selects a kernel implementation.
type Tier uint8

// Kernel tiers, lowest first.
const (
	TierGeneric Tier = iota
	TierAVX2
	TierAVX512
)

// String returns the tier name.
func (t Tier) String() string {
	switch t {
	case TierGeneric:
		return "generic"
	case TierAVX2:
		return "avx2"
	case TierAVX512:
		return "avx512"
	}
	return "unknown"
}

// Best returns the fastest tier usable on this CPU. CPU features are read on every call,
// so tests can disable them with cpuid.CPU.Disable.
func Best() Tier {
	switch {
	case hasAVX512():
		return TierAVX512
	case hasAVX2():
		return TierAVX2
	}
	return TierGeneric
}

// Tiers returns all usable tiers, best first; TierGeneric is always last.
func Tiers() []Tier {
	var ts []Tier
	if hasAVX512() {
		ts = append(ts, TierAVX512)
	}
	if hasAVX2() {
		ts = append(ts, TierAVX2)
	}
	return append(ts, TierGeneric)
}

// Gather sets ranks[i] = s2r[src[i]] and returns the largest rank written.
func (t Tier) Gather(ranks, src []byte, s2r *[256]byte) uint8 {
	switch t {
	case TierAVX512:
		return gatherAVX512(ranks, src, s2r)
	case TierAVX2:
		return gatherAVX2(ranks, src, s2r)
	}
	return gatherGeneric(ranks, src, s2r)
}

// PartFull partitions ranks[:n] by rank > thr: writes the bitmap to bm (padding bits zero),
// moves right ranks in order to right[:nr], compacts left ranks in order into ranks[:n-nr],
// and returns nr.
func (t Tier) PartFull(ranks []byte, n int, right, bm []byte, thr uint8) int {
	switch t {
	case TierAVX512:
		return partFullAVX512(ranks, n, right, bm, thr)
	case TierAVX2:
		return partFullAVX2(ranks, n, right, bm, thr)
	}
	return partFullGeneric(ranks, n, right, bm, thr)
}

// PartRight is PartFull without compacting the left ranks; ranks is read-only.
func (t Tier) PartRight(ranks []byte, n int, right, bm []byte, thr uint8) int {
	switch t {
	case TierAVX512:
		return partRightAVX512(ranks, n, right, bm, thr)
	case TierAVX2:
		return partRightAVX2(ranks, n, right, bm, thr)
	}
	return partRightGeneric(ranks, n, right, bm, thr)
}

// PartNone writes only the bitmap and returns the number of set bits.
func (t Tier) PartNone(ranks []byte, n int, bm []byte, thr uint8) int {
	switch t {
	case TierAVX512:
		return partNoneAVX512(ranks, n, bm, thr)
	case TierAVX2:
		return partNoneAVX2(ranks, n, bm, thr)
	}
	return partNoneGeneric(ranks, n, bm, thr)
}

// Pack writes the codes ranks[i]-base (each < 1<<d) of a flat region in the vertical
// layout to exactly dst[:(n*d+7)/8], with zero padding bits.
func (t Tier) Pack(dst, ranks []byte, n int, d, base uint8) {
	switch t {
	case TierAVX512:
		packAVX512(dst, ranks, n, d, base)
	case TierAVX2:
		packAVX2(dst, ranks, n, d, base)
	default:
		packGeneric(dst, ranks, n, d, base)
	}
}

// Exec runs p.Ops. It returns -1 on success, or the index of the first op that is
// malformed (unknown Kind, D out of range, K not a multiple of its layout block, nil R2S),
// out of range of the slices, or whose bitmap popcount does not match its KR.
func (t Tier) Exec(p *Program) int {
	switch t {
	case TierAVX512:
		return execAVX512(p)
	case TierAVX2:
		return execAVX2(p)
	}
	return execGeneric(p)
}

// Histogram adds the byte counts of src to h. scratch must hold HistScratch bytes if
// t.HistUsesScratch(), else it may be nil.
func (t Tier) Histogram(h *[256]uint32, src, scratch []byte) {
	if t == TierAVX512 {
		histogramAVX512(h, src, scratch)
		return
	}
	histogramGeneric(h, src)
}

// HistUsesScratch reports whether Histogram needs its scratch, so that callers
// allocate it only for tiers that do.
func (t Tier) HistUsesScratch() bool {
	return t == TierAVX512 && hasHistAVX512()
}
