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
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minlz/internal/race"
)

// The vector encoder kernels against the references, with the tails at every alignment
// and the exactness the contract promises: gather writes exactly ranks[:n], and pack
// writes exactly the region.

var encSizes = func() []int {
	s := make([]int, 0, 310)
	for n := range 301 {
		s = append(s, n)
	}
	return append(s, 511, 512, 513, 1023, 1024, 4095, 4096, 65535)
}()

func TestVecGather(t *testing.T) {
	for _, tier := range vecTiers(t) {
		testVecGather(t, tier)
	}
}

func testVecGather(t *testing.T, tier Tier) {
	rng := newRNG()
	for _, n := range encSizes {
		for _, alpha := range []int{1, 7, 128, 256} {
			// Symbols >= alpha are absent: rank 0xFF, as in Table.s2r. A full table has
			// the real rank 255.
			var s2r [256]byte
			for s, r := range rng.Perm(256) {
				s2r[s] = byte(r)
				if alpha < 256 {
					s2r[s] = byte(r % 255)
				}
			}
			for s := alpha; s < 256; s++ {
				s2r[s] = 0xFF
			}
			src, _ := guarded(make([]byte, n))
			for i := range src {
				src[i] = byte(rng.IntN(alpha))
			}
			if n > 0 && rng.IntN(4) == 0 {
				src[rng.IntN(n)] = 255
			}
			want := make([]byte, n)
			wmx := gatherGeneric(want, src, &s2r)
			buf := fill(make([]byte, n+guardLen), guardByte)
			mx := tier.Gather(buf[:n:n], src, &s2r)
			if mx != wmx || !bytes.Equal(buf[:n], want) || !intact(buf[n:]) {
				t.Fatalf("%v n=%d alpha=%d: max %d want %d, ranks ok %v, guard ok %v",
					tier, n, alpha, mx, wmx, bytes.Equal(buf[:n], want), intact(buf[n:]))
			}
		}
	}
}

// TestVecPartition runs the shared partition checks (testPartition: bitmap with zero
// padding, both sides in order, guard bytes right after ranks[:n] and after the slack of
// right and bm, and right == ranks[n:]) on all-left, all-right and mixed inputs.
func TestVecPartition(t *testing.T) {
	tiers := vecTiers(t)
	rng := newRNG()
	for _, n := range encSizes {
		for _, thr := range []uint8{0, 1, uint8(2 + rng.IntN(251)), 253, 254} {
			for pat := range 4 {
				ranks := randBytes(rng, make([]byte, n))
				for i := range ranks {
					switch pat {
					case 0: // all left
						ranks[i] = byte(rng.IntN(int(thr) + 1))
					case 1: // all right
						ranks[i] = byte(int(thr) + 1 + rng.IntN(255-int(thr)))
					case 2: // runs around thr
						if (i/37)&1 == 0 {
							ranks[i] = thr
						} else {
							ranks[i] = thr + 1
						}
					}
				}
				for _, tier := range tiers {
					testPartition(t, tier, ranks, thr)
				}
			}
		}
	}
}

func TestVecPack(t *testing.T) {
	tiers := vecTiers(t)
	rng := newRNG()
	sizes := append(encSizes, 639, 640, 641, 1025, 1535, 1536, 1537)
	for _, n := range sizes {
		for d := uint8(2); d <= 8; d++ {
			base := uint8(rng.IntN(257 - 1<<d))
			ranks := make([]byte, n, n+guardLen)
			for i := range ranks {
				ranks[i] = base + byte(rng.Uint32())&(1<<d-1)
			}
			fill(ranks[n:cap(ranks)], guardByte)
			size := (n*int(d) + 7) / 8
			want := make([]byte, size)
			packGeneric(want, ranks, n, d, base)
			for _, tier := range tiers {
				// Junk in dst: every byte must be written. Guard bytes right after it.
				dst := randBytes(rng, make([]byte, size+guardLen))
				fill(dst[size:], guardByte)
				tier.Pack(dst[:size:size], ranks, n, d, base)
				if !bytes.Equal(dst[:size], want) || !intact(dst[size:]) || !intact(ranks[n:cap(ranks)]) {
					t.Fatalf("%v n=%d d=%d base=%d: region ok %v, guard ok %v", tier, n, d, base,
						bytes.Equal(dst[:size], want), intact(dst[size:]))
				}
			}
		}
	}
}

// TestVecPageGuard places every exactly-accessed buffer flush against an inaccessible
// page, after and before it, so that any read or write outside fault.
func TestVecPageGuard(t *testing.T) {
	rng := newRNG()
	const maxN = 4096 + 300
	// Whole pages, so that both ends are flush against a guard.
	pg := os.Getpagesize()
	size := (maxN + pg - 1) / pg * pg
	memA, memB := pageGuarded(t, size, true), pageGuarded(t, size, true)
	var s2r [256]byte
	randBytes(rng, s2r[:])
	scratch := make([]byte, HistScratch)
	right := make([]byte, maxN+RankSlack)
	bm := make([]byte, maxN/8+1+RankSlack)
	// 0..80 reaches every branch of the exact 0..16-byte tail loads and stores.
	sizes := []int{127, 128, 200, 511, 512, 513, 640, 1000, 4095, 4096 + 300}
	for n := range 81 {
		sizes = append(sizes, n)
	}
	for _, tier := range Tiers() {
		for _, n := range sizes {
			for _, atEnd := range []bool{true, false} {
				at := func(b []byte, k int) []byte {
					if atEnd {
						return b[len(b)-k:]
					}
					return b[:k:k]
				}
				// Gather: src and ranks exact.
				src := at(memA, n)
				randBytes(rng, src)
				ranks := at(memB, n)
				tier.Gather(ranks, src, &s2r)
				// Partitions: ranks exact.
				for _, thr := range []uint8{0, 100, 254} {
					tier.PartNone(ranks, n, bm, thr)
					tier.PartRight(ranks, n, right, bm, thr)
					tier.PartFull(ranks, n, right, bm, thr)
				}
				// Pack: ranks and region exact.
				for d := uint8(2); d <= 8; d++ {
					for i := range ranks {
						ranks[i] &= 1<<d - 1
					}
					tier.Pack(at(memA, (n*int(d)+7)/8), ranks, n, d, 0)
				}
				// Histogram: src exact, from the four-bin loop and, with bytes below 0x80,
				// the two-bin loop up to the guard.
				var h [256]uint32
				randBytes(rng, src)
				tier.Histogram(&h, src, scratch)
				for i := range src {
					src[i] &= 0x7F
				}
				tier.Histogram(&h, src, scratch)
			}
		}
	}
}

// TestVecRace requires the race detector to see every buffer the vector kernels access:
// each case runs one kernel in a child process while a goroutine writes one of its
// buffers (or reads one Exec writes), and the child must report the race.
func TestVecRace(t *testing.T) {
	if !race.Enabled {
		t.Skip("needs -race")
	}
	tiers := vecTiers(t)
	if c := os.Getenv("PIVCO_VEC_RACE"); c != "" {
		name, c, _ := strings.Cut(c, ":")
		for _, tier := range tiers {
			if tier.String() == name {
				vecRaceChild(tier, c)
			}
		}
		return
	}
	for _, tier := range tiers {
		for _, c := range []string{
			"gather/src", "gather/ranks", "gather/s2r",
			"full/ranks", "full/right", "full/bm", "right/ranks", "right/right", "right/bm",
			"none/ranks", "none/bm", "pack/ranks", "pack/dst", "hist/src", "hist/scratch", "hist/h",
			"exec/ops", "exec/in", "exec/r2s", "exec/arena", "exec/dst",
		} {
			if strings.HasPrefix(c, "hist/") && !tier.HistUsesScratch() {
				continue // the generic histogram: no assembly to annotate
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestVecRace$", "-test.count=1")
			cmd.Env = append(os.Environ(), "PIVCO_VEC_RACE="+tier.String()+":"+c)
			if out, _ := cmd.CombinedOutput(); !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
				t.Errorf("%v %s: race not reported:\n%s", tier, c, out)
			}
		}
	}
}

var raceSink byte

func vecRaceChild(tier Tier, c string) {
	const n = 1000
	src, ranks, dst := make([]byte, n), make([]byte, n), make([]byte, n)
	right, bm := make([]byte, n+RankSlack), make([]byte, n/8+1+RankSlack)
	scratch := make([]byte, HistScratch)
	var s2r [256]byte
	var h [256]uint32
	in, arena := make([]byte, n), make([]byte, n+ArenaSlack)
	var r2s [R2SLen]byte
	p := Program{Ops: []Op{{Kind: OpCC, Flags: FlagDst, P: 1, Q: 2, K: n}}, In: in, Arena: arena,
		Dst: dst, R2S: &r2s}
	bufs := map[string][]byte{"src": src, "ranks": ranks, "dst": dst, "right": right, "bm": bm,
		"scratch": scratch, "s2r": s2r[:], "in": in, "arena": arena, "r2s": r2s[:]}
	kern, buf, _ := strings.Cut(c, "/")
	var wg sync.WaitGroup
	wg.Go(func() {
		switch {
		case buf == "h":
			h[0]++
		case buf == "ops":
			// Not a field the literal set: the detector keeps only 4 accesses per 8-byte
			// word, and with them full a race can go unseen.
			p.Ops[0].R++
		case kern == "exec" && (buf == "arena" || buf == "dst"):
			raceSink = bufs[buf][0] // Exec's outputs: a concurrent read races too
		default:
			bufs[buf][0]++
		}
	})
	switch kern {
	case "gather":
		tier.Gather(ranks, src, &s2r)
	case "full":
		tier.PartFull(ranks, n, right, bm, 0)
	case "right":
		tier.PartRight(ranks, n, right, bm, 0)
	case "none":
		tier.PartNone(ranks, n, bm, 0)
	case "pack":
		tier.Pack(dst, ranks, n, 8, 0)
	case "hist":
		tier.Histogram(&h, src, scratch)
	case "exec":
		tier.Exec(&p)
	}
	wg.Wait()
}
