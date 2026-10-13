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
	"strings"
	"testing"
)

// TestPivcoExamples decodes the examples of PIVCO_SPEC.md appendix A, and changed copies of them.
func TestPivcoExamples(t *testing.T) {
	table := func(lens map[byte]uint8) *pivcoTable {
		var l [256]uint8
		for v, n := range lens {
			l[v] = n
		}
		tab, err := newPivcoTable(&l)
		if err != nil {
			t.Fatal(err)
		}
		return tab
	}
	a1 := table(map[byte]uint8{'a': 3, 'b': 3, 'c': 3, 'd': 3, 'e': 2, 'f': 3, 'g': 3})
	if got := string(a1.ranks); got != "abcdefg" {
		t.Fatalf("A.1 ranks %q", got)
	}
	a5 := table(map[byte]uint8{'a': 3, 'b': 3, 'c': 3, 'd': 3, 'e': 3, 'f': 3, 'g': 3, 'h': 3})
	a5vals := strings.Repeat("a", 16) + strings.Repeat("b", 16) + strings.Repeat("c", 16) + strings.Repeat("d", 16) +
		strings.Repeat("e", 16) + strings.Repeat("f", 16) + strings.Repeat("g", 16) + strings.Repeat("h", 16) + "aa"
	a5blk := bytes.Repeat([]byte{0x88}, 16)
	a5blk = append(append(append(a5blk, bytes.Repeat([]byte{0xc6}, 16)...), bytes.Repeat([]byte{0xfa}, 16)...), 0)

	tests := []struct {
		name string
		t    *pivcoTable
		blk  []byte
		vals string
	}{
		{"A.2", a1, []byte{0x08, 0x00, 0x03, 0x00, 0x00, 0x85, 0x1b, 0x07, 0xc3, 0x39}, "fedcbafeedbeef"},
		{"A.3 eee", a1, []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x07}, "eee"},
		{"A.3 abba", a1, []byte{0x00, 0x00, 0x14, 0x00}, "abba"},
		{"A.4", table(map[byte]uint8{'x': 1}), []byte{0, 0}, "xxxxxxxxxxx"},
		{"A.5", a5, a5blk, a5vals},
	}
	for _, tc := range tests {
		got, err := tc.t.decode(tc.blk, len(tc.vals))
		if err != nil || string(got) != tc.vals {
			t.Fatalf("%s: got %q, %v", tc.name, got, err)
		}
		// Every changed bit or size must fail or decode to other values (6).
		for i := range tc.blk {
			for b := range 8 {
				c := bytes.Clone(tc.blk)
				c[i] ^= 1 << b
				if got, err := tc.t.decode(c, len(tc.vals)); err == nil && string(got) == tc.vals {
					t.Errorf("%s: byte %d bit %d changed decodes the same", tc.name, i, b)
				}
			}
		}
		if _, err := tc.t.decode(tc.blk[:len(tc.blk)-1], len(tc.vals)); err == nil {
			t.Errorf("%s: truncated block decodes", tc.name)
		}
		if _, err := tc.t.decode(append(bytes.Clone(tc.blk), 0), len(tc.vals)); err == nil {
			t.Errorf("%s: extended block decodes", tc.name)
		}
	}
}
