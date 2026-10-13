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
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
)

// The goldens in testdata were produced by the upstream C library at the commit in doc.go:
// tables.txt.gz holds its code lengths and schedules, blocks.txt.gz its PH blocks with the
// FlatVertical layout, converted to this package's form by removing the symbol count, the
// marker prefix byte and the marker byte of each flat region. Digests are FNV-1a 64.
// joint.txt.gz holds its effort 2 (PIVCO_EFFORT_FASTER_DECOMPRESS) code lengths, built with
// PIVCO_JOINT_SCALAR, -ffp-contract=off, PIVCO_BLOCK_SIZE=65536 (one block per table) and
// FSE off; ranged cases ran on the alphabet rotated by lo, kept as baseline if a ghost fell
// outside [lo,hi).

// goldenTables returns the tables of tables.txt.gz by name, checking each against C.
func goldenTables(t *testing.T) map[string]record {
	t.Helper()
	tabs := map[string]record{}
	for _, r := range readManifest(t, "tables.txt.gz") {
		if r.kind != "T" {
			t.Fatalf("line %d: kind %q", r.line, r.kind)
		}
		tabs[r.f["name"]] = r
	}
	return tabs
}

// TestTableGolden checks BuildLengths and the schedules against the C library.
func TestTableGolden(t *testing.T) {
	tabs := goldenTables(t)
	for name, r := range tabs {
		want := r.lens(t, "lens")
		if r.f["gen"] != "" {
			h := r.hist(t)
			var raw []byte
			for _, f := range h {
				raw = binary.LittleEndian.AppendUint64(raw, f)
			}
			if fnv1a64(raw) != r.uint(t, "hist", 16) {
				t.Fatalf("%s: histogram generator differs from C", name)
			}
			var got [256]uint8
			BuildLengths(h, &got)
			if got != *want {
				t.Fatalf("%s: lengths\n got %x\nwant %x", name, got, *want)
			}
		}
		tb := mustTable(t, want)
		var sched []byte
		for _, x := range tb.recs() {
			sched = append(sched, x.Op, x.Param, x.Right)
		}
		for key, b := range map[string][]byte{"sched": sched, "r2s": tb.r2s[:tb.nranks], "s2r": tb.s2r[:]} {
			if fnv1a64(b) != r.uint(t, key, 16) {
				t.Errorf("%s: %s digest mismatch", name, key)
			}
		}
		if r.uint(t, "nsched", 10) != uint64(tb.nsched) || r.uint(t, "nranks", 10) != uint64(tb.nranks) {
			t.Errorf("%s: nsched %d, nranks %d", name, tb.nsched, tb.nranks)
		}
	}
	if len(tabs) == 0 {
		t.Fatal("no tables")
	}
}

// goldenInput regenerates the input of a block record: "s:<seed>" samples the table's
// histogram, "l:<seed>" its lengths, "hex:<bytes>" is the input.
func goldenInput(t *testing.T, tabs map[string]record, r record) ([]byte, *[256]uint8) {
	t.Helper()
	tr, ok := tabs[r.f["table"]]
	if !ok {
		t.Fatalf("line %d: unknown table %q", r.line, r.f["table"])
	}
	lens := tr.lens(t, "lens")
	n := int(r.uint(t, "n", 10))
	k, v, _ := strings.Cut(r.f["gen"], ":")
	var in []byte
	switch k {
	case "s", "l":
		seed, err := strconv.ParseUint(v, 0, 64)
		if err != nil {
			t.Fatal(err)
		}
		if k == "l" {
			in = sampleLens(lens, n, seed)
		} else {
			in = sample(tr.hist(t), n, seed)
		}
	case "hex":
		var err error
		if in, err = hex.DecodeString(v); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("line %d: bad generator %q", r.line, r.f["gen"])
	}
	if len(in) != n || fnv1a64(in) != r.uint(t, "in", 16) {
		t.Fatalf("line %d: input generator differs from C", r.line)
	}
	return in, lens
}

// TestBlockGolden encodes every block of blocks.txt.gz on every tier, requires C's regions
// byte for byte and BlockSize to predict their size, then decodes them.
func TestBlockGolden(t *testing.T) {
	tabs := goldenTables(t)
	recs := readManifest(t, "blocks.txt.gz")
	for _, c := range coders(t) {
		for _, r := range recs {
			in, lens := goldenInput(t, tabs, r)
			var tb Table
			if err := tb.SetLengths(lens); err != nil {
				t.Fatal(err)
			}
			n, digest := r.lenDigest(t, "out")
			var h [256]uint64
			c.enc.Histogram(&h, in)
			if size := tb.BlockSize(&h); size != n {
				t.Fatalf("%s: BlockSize %d, want %d", r.f["case"], size, n)
			}
			out := make([]byte, n)
			if err := c.enc.Encode(out, in, &tb); err != nil {
				t.Fatalf("%s: %v", r.f["case"], err)
			}
			if fnv1a64(out) != digest {
				t.Fatalf("%s (table %s n %d): differs from C", r.f["case"], r.f["table"], len(in))
			}
			if x, ok := r.f["hex"]; ok {
				if want, _ := hex.DecodeString(x); !bytes.Equal(out, want) {
					t.Fatalf("%s: got %x\nwant %x", r.f["case"], out, want)
				}
			}
			got := make([]byte, len(in))
			if err := c.dec.Decode(got, out, &tb); err != nil || !bytes.Equal(got, in) {
				t.Fatalf("%s: decode: %v", r.f["case"], err)
			}
		}
		t.Logf("%v: %d blocks", c.tier, len(recs))
	}
}

// TestJointGolden checks BuildLengths and Shape against the C library's effort 2 lengths,
// over the byte alphabet and over the stream's symbol range.
func TestJointGolden(t *testing.T) {
	var s Shaper
	recs := readManifest(t, "joint.txt.gz")
	for _, r := range recs {
		num := func(v string) uint64 {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				t.Fatalf("line %d: %v", r.line, err)
			}
			return n
		}
		var h [256]uint64
		for i, v := range strings.Split(r.f["h"], ",") {
			if v != "" {
				h[i] = num(v)
			}
		}
		var lens [256]uint8
		BuildLengths(&h, &lens)
		if lens != *r.lens(t, "base") {
			t.Fatalf("%s: BuildLengths\n got %x\nwant %x", r.f["name"], lens, *r.lens(t, "base"))
		}
		lo, hi, out := 0, 256, "out"
		if v, ok := r.f["r"]; ok {
			a, b, _ := strings.Cut(v, ":")
			lo, hi, out = int(num(a)), int(num(b)), "rout"
		}
		for _, c := range []struct {
			key    string
			lo, hi int
		}{{"out", 0, 256}, {out, lo, hi}} {
			want, got := lens, lens
			if r.f[c.key] != "-" {
				want = *r.lens(t, c.key)
			}
			if s.Shape(&h, &got, c.lo, c.hi) != (want != lens) || got != want {
				t.Fatalf("%s %s: Shape\n got %x\nwant %x", r.f["name"], c.key, got, want)
			}
		}
	}
	if len(recs) == 0 {
		t.Fatal("no cases")
	}
}
