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
	"testing"

	"github.com/minio/minlz/internal/race"
)

func checkAllocs(t *testing.T, what string, f func()) {
	t.Helper()
	f() // warm up
	if n := testing.AllocsPerRun(20, f); n != 0 {
		t.Errorf("%s: %v allocs", what, n)
	}
}

func TestAllocs(t *testing.T) {
	if race.Enabled {
		t.Skip("race detector allocates")
	}
	h := dist("english")
	big := sample(h, MaxBlockSize, 1)
	small := sample(h, 4096, 2)
	lens := lensOf(t, h)
	tb := mustTable(t, &lens)
	for _, c := range coders(t) {
		enc, dec := c.enc, c.dec
		buf := make([]byte, 0, 2*MaxBlockSize)
		out := make([]byte, MaxBlockSize)
		checkAllocs(t, "heap buffers", func() {
			var h [256]uint64
			enc.Histogram(&h, big)
			var l [256]uint8
			BuildLengths(h[:], &l)
			if tb.SetLengths(&l) != nil {
				t.Fatal("SetLengths")
			}
			b := buf[:tb.BlockSize(h[:])]
			if err := enc.Encode(b, big, tb); err != nil {
				t.Fatal(err)
			}
			if err := dec.Decode(out, b, tb); err != nil {
				t.Fatal(err)
			}
		})
		// Tables, histograms and buffers on the stack must stay there.
		checkAllocs(t, "stack locals", func() {
			var src [4096]byte
			copy(src[:], small)
			var h [256]uint64
			enc.Histogram(&h, src[:])
			var l [256]uint8
			BuildLengths(h[:], &l)
			var t1 Table
			if t1.SetLengths(&l) != nil {
				t.Fatal("SetLengths")
			}
			var enc1 [8192]byte
			b := enc1[:t1.BlockSize(h[:])]
			if err := enc.Encode(b, src[:], &t1); err != nil {
				t.Fatal(err)
			}
			var dec1 [4096]byte
			if err := dec.Decode(dec1[:], b, &t1); err != nil {
				t.Fatal(err)
			}
		})
		checkAllocs(t, "errors", func() {
			var t1 Table
			var d [64]byte
			enc.Encode(d[:], small, &t1)
			enc.Encode(d[:], nil, tb)
			dec.Decode(d[:3], d[:3], tb)
			dec.Decode(d[:1], []byte{7}, tb)
			dec.Decode(nil, d[:], tb)
			var l [256]uint8
			t1.SetLengths(&l)
		})
	}
}
