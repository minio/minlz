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
	"encoding/binary"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestSIMDLayoutsSpec compares the class layouts with the arrays in SPEC_SIMD.md.
func TestSIMDLayoutsSpec(t *testing.T) {
	spec, err := os.ReadFile("../../SPEC_SIMD.md")
	if err != nil {
		t.Skip(err)
	}
	layouts := map[string]classLayout{
		"1": simdStreams[streamLitLens].classes,
		"2": simdStreams[streamEscapes].classes,
		"3": simdStreams[streamOffsets].classes,
		"4": simdStreams[streamTokens].classes,
		"5": simdStreams[streamLiterals].classes,
		"6": deltaClasses,
	}
	array := regexp.MustCompile(`(Class|In-class length), [a-z ]+ (\d+)-(\d+):\n([\d, \n]+)`)
	found := 0
	for _, sec := range strings.Split(strings.ReplaceAll(string(spec), "\r", ""), "\n## A.")[1:] {
		l, ok := layouts[sec[:1]]
		if !ok {
			continue
		}
		var class, in [256]int
		for c, vals := range l {
			for i, n := range inClassLengths(len(vals)) {
				class[vals[i]], in[vals[i]] = c, n
			}
		}
		for _, m := range array.FindAllStringSubmatch(sec, -1) {
			want := &class
			if m[1] != "Class" {
				want = &in
			}
			v, _ := strconv.Atoi(m[2])
			hi, _ := strconv.Atoi(m[3])
			for _, f := range strings.FieldsFunc(m[4], func(r rune) bool { return r == ',' || r == ' ' || r == '\n' }) {
				if n, _ := strconv.Atoi(f); v > hi || n != want[v] {
					t.Fatalf("A.%s %s of %d: spec %d, got %d", sec[:1], m[1], v, n, want[v])
				}
				v++
			}
			if v != hi+1 {
				t.Errorf("A.%s %s: %d values", sec[:1], m[1], v)
			}
			found++
		}
	}
	if found != 2*len(layouts) {
		t.Errorf("found %d arrays", found)
	}
}

// TestSIMDDeltaExample decodes the example of SPEC_SIMD.md 4.5.
func TestSIMDDeltaExample(t *testing.T) {
	rv := func(b []byte, v int) []byte {
		r := binary.AppendUvarint(nil, uint64(v))
		slices.Reverse(r)
		return append(b, r...)
	}
	// Literals 41 42 43 07 00 41 42 43 with offset 5, then 07 01 41 42 44 07 and a repeat of 4,
	// then 3 repeats of 32 to pass the 64-byte rule.
	lits := []byte{0x41, 0x42, 0x43, 0x07, 0x00, 0x41, 0x42, 0x43, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00}
	streams := [][]byte{{2, 4<<2 | 3, 32<<2 | 1, 32<<2 | 1, 32<<2 | 1}, {8, 6}, {5}, nil, nil}
	want := []byte{0x41, 0x42, 0x43, 0x07, 0x00, 0x41, 0x42, 0x43, 0x07, 0x01, 0x41, 0x42, 0x44, 0x07}
	for len(want) < 14+4+96 {
		want = append(want, want[len(want)-5])
	}
	b := append(binary.AppendUvarint([]byte{0}, 1<<24|uint64(len(want))), 0)
	start := len(b)
	tab := []byte{1}
	for _, s := range streams {
		b = append(b, s...)
		tab = append(tab, byte(len(s)<<3))
	}
	b = rv(append(b, tab...), len(tab))
	lz := len(b) - start
	b = append(b, lits...)
	b = rv(append(b, byte(len(lits)<<3)), 1)
	b = rv(b, lz)
	got, err := DecodeSIMDBlock(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}
