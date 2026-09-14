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

// Same constraint as encode_asm.go: the pools this test inspects exist only
// where the assembly encoders do.

//go:build (amd64 || arm64) && !appengine && !noasm && gc && !purego

package minlz

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/minio/minlz/internal/race"
)

// TestEncodePoolsRoundTrip checks that every assembly encoder returns its
// scratch table to the pool it took it from, typed as that pool's Get expects.
//
// sync.Pool promises nothing about Get returning what Put just stored, but
// with no allocation in between it does in practice, and the failure this
// guards against is deterministic: a table handed to another family's pool
// makes that family's next Get fail its type assertion and allocate, while the
// family that lost it allocates on every call. Both happened on amd64 before
// encodeBlockFast's Put was pointed at encFastPools.
//
// That "in practice" is only true for a normal build. Under the race
// detector, sync.Pool.Put deliberately drops its argument on the floor about
// one time in four (see the "Randomly drop x on floor" branch in
// $GOROOT/src/sync/pool.go) specifically to keep callers honest about not
// relying on Get returning what Put just stored. That makes every subtest
// here independently ~25% likely to see an empty pool, so the test flakes on
// -race no matter how carefully the surrounding code avoids GCs or
// goroutine switches: skip it there rather than chase a race that is a
// documented property of the allocator, not a bug in the encoders.
func TestEncodePoolsRoundTrip(t *testing.T) {
	if race.Enabled {
		t.Skip("sync.Pool.Put randomly drops its argument under the race detector; this test's pool round-trip check cannot pass reliably there")
	}

	type family struct {
		name   string
		encode func(dst, src []byte) int
		pool   func(i int) *sync.Pool
	}
	families := []family{
		{"fast", encodeBlockFast, func(i int) *sync.Pool { return &encFastPools[i] }},
		{"default", encodeBlock, func(i int) *sync.Pool { return &encPools[i] }},
		{"better", encodeBlockBetter, func(i int) *sync.Pool { return &encBetterPools[i] }},
	}
	// One input size per dispatch class, with the pool index and table size
	// encode_asm.go uses for it, per family.
	classes := []struct {
		name  string
		size  int
		pool  [3]int // fast, default, better
		table [3]int
	}{
		{"1k", 1 << 10, [3]int{4, 6, 5}, [3]int{1024, 1024, 4608}},
		{"4k", 4 << 10, [3]int{3, 5, 4}, [3]int{2048, 2048, 10240}},
		{"16k", 16 << 10, [3]int{2, 4, 3}, [3]int{4096, 8192, 36864}},
		{"64k", 64 << 10, [3]int{1, 3, 2}, [3]int{8192, 16384, 73728}},
		{"512k", 512 << 10, [3]int{0, 2, 1}, [3]int{32768, 65536, 294912}},
		{"2m", 2 << 20, [3]int{0, 0, 0}, [3]int{32768, 131072, 589824}},
		{"8m", 2<<20 + 1, [3]int{5, 0, 0}, [3]int{65536, 131072, 589824}},
	}
	rng := rand.New(rand.NewSource(1))
	for fi, f := range families {
		for _, c := range classes {
			t.Run(f.name+"/"+c.name, func(t *testing.T) {
				src := genEncText(rng, c.size)
				dst := make([]byte, MaxEncodedLen(len(src)))
				f.encode(dst, src)

				pool := f.pool(c.pool[fi])
				got := pool.Get()
				if got == nil {
					t.Fatalf("pool %d is empty after encoding: the table was returned somewhere else", c.pool[fi])
				}
				defer pool.Put(got)
				want := reflect.PointerTo(reflect.ArrayOf(c.table[fi], reflect.TypeOf(byte(0))))
				if reflect.TypeOf(got) != want {
					t.Fatalf("pool %d holds %v, want %v", c.pool[fi], reflect.TypeOf(got), want)
				}
			})
		}
	}
}
