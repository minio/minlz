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

package guard

import (
	"runtime/debug"
	"testing"
	"unsafe"
)

func TestFault(t *testing.T) {
	if !supported {
		t.Skip("page guards unsupported")
	}
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	g, err := New(10000)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Free()
	for _, n := range []int{1, 100, 10000} {
		for _, atEnd := range []bool{false, true} {
			b := g.Slice(n, atEnd)
			if len(b) != n || cap(b) != n {
				t.Fatalf("n %d atEnd %v: len %d cap %d", n, atEnd, len(b), cap(b))
			}
			for i := range b {
				b[i] = byte(i)
			}
			off := -1
			if atEnd {
				off = n
			}
			p := unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), off)
			if !faults(func() { *(*byte)(p) = 1 }) {
				t.Errorf("n %d atEnd %v: no fault at offset %d", n, atEnd, off)
			}
		}
	}
}

func faults(f func()) (ok bool) {
	defer func() { ok = recover() != nil }()
	f()
	return false
}
