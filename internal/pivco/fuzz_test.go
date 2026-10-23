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
	"bytes"
	"encoding/binary"
	"testing"
)

// fuzzTable builds a table from 128 bytes of length nibbles if they are valid, else from
// the histogram of data, so that most inputs reach the codec.
func fuzzTable(lens, data []byte) *Table {
	var l [256]uint8
	for i, b := range lens {
		l[2*i], l[2*i+1] = b&15, b>>4
	}
	var tb Table
	if tb.SetLengths(&l) == nil {
		return &tb
	}
	var h [256]uint64
	for _, c := range data {
		h[c]++
	}
	h[0]++
	BuildLengths(h[:], &l)
	if tb.SetLengths(&l) != nil {
		panic("built lengths rejected")
	}
	return &tb
}

// FuzzDecodeBlock: [128 bytes of length nibbles][symbol count, u16][block]. Invalid
// lengths fall back to a table built from the block bytes.
func FuzzDecodeBlock(f *testing.F) {
	for _, cc := range corruptCases(f) {
		var b [128]byte
		for s, v := range cc.tb.lens {
			b[s/2] |= v << (4 * (s & 1))
		}
		f.Add(append(binary.LittleEndian.AppendUint16(b[:], uint16(len(cc.src))), cc.blk...))
	}
	cs := coders(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 130 {
			return
		}
		blk := data[130:]
		fuzzDecode(t, cs, fuzzTable(data[:128], blk), int(binary.LittleEndian.Uint16(data[128:])), blk)
	})
}

// FuzzDecodeBlockFixed: [table][symbol count, u16][block] over a fixed set of tables,
// so that mutating a block keeps its table.
func FuzzDecodeBlockFixed(f *testing.F) {
	var tabs []*Table
	for i, cc := range corruptCases(f) {
		tabs = append(tabs, cc.tb)
		f.Add(append(binary.LittleEndian.AppendUint16([]byte{byte(i)}, uint16(len(cc.src))), cc.blk...))
	}
	for _, name := range testDists {
		lens := lensOf(f, dist(name))
		tabs = append(tabs, mustTable(f, &lens))
	}
	cs := coders(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 {
			return
		}
		fuzzDecode(t, cs, tabs[int(data[0])%len(tabs)], int(binary.LittleEndian.Uint16(data[1:])), data[3:])
	})
}

// fuzzDecode decodes n symbols of blk on every tier, which must agree, and checks that
// an accepted block is what the encoder writes for its output.
func fuzzDecode(t *testing.T, cs []coder, tb *Table, n int, blk []byte) {
	if n == 0 {
		return
	}
	var ref []byte
	var refErr error
	for i, c := range cs {
		got, err := decodeErr(t, c, blk, n, tb)
		if i == 0 {
			ref, refErr = got, err
		} else if !bytes.Equal(got, ref) || (err == nil) != (refErr == nil) {
			t.Fatalf("%v: (%v) differs from %v (%v)", c.tier, err, cs[0].tier, refErr)
		}
	}
	if refErr == nil {
		canon(t, cs[0], tb, blk, ref)
	}
}

// FuzzRoundTripBlock: a flags byte (ghost symbols in the table), then the data.
func FuzzRoundTripBlock(f *testing.F) {
	f.Add([]byte("\x00abcab"))
	f.Add(append([]byte{5}, sample(dist("english"), 3000, 1)...))
	f.Add(append([]byte{8}, sample(dist("proba80"), 9000, 1)...))
	f.Add(append([]byte{1}, sample(dist("sym0_255"), 700, 1)...))
	cs := coders(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		flags, src := data[0], data[1:][:min(len(data)-1, MaxBlockSize)]
		var h [256]uint64
		for _, c := range src {
			h[c]++
		}
		if flags&8 != 0 {
			for i := range h {
				h[i] += uint64(i % 3)
			}
		}
		var l [256]uint8
		BuildLengths(h[:], &l)
		tb := mustTable(t, &l)
		var ref []byte
		for _, c := range cs {
			blk := roundTrip(t, c, src, tb)
			if ref == nil {
				ref = blk
			} else if !bytes.Equal(blk, ref) {
				t.Fatalf("%v differs", c.tier)
			}
		}
	})
}
