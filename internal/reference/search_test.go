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
	"testing"
)

// makeBlock returns a 4 KiB block that contains every needle exactly at the
// position recorded in placements. The block body is filled with a benign
// repeating pattern so that needles stand out.
func makeBlock(needles [][]byte) (block []byte, placements []int) {
	block = bytes.Repeat([]byte{'.'}, 4096)
	pos := 16
	for _, n := range needles {
		copy(block[pos:], n)
		placements = append(placements, pos)
		pos += len(n) + 24
	}
	return block, placements
}

func TestRoundTripNoPrefix(t *testing.T) {
	needles := [][]byte{
		[]byte("hello-world"),
		[]byte("MinIO"),
		[]byte("\"timestamp\":\"2026-06-04\""),
	}
	block, _ := makeBlock(needles)

	cfg := SearchConfig{
		MatchLen:      5,
		BaseTableSize: 12, // 4096-bit table = 512 bytes
		TableType:     TableTypeNoPrefix,
	}
	table, reductions := BuildSearchTable(cfg, block, nil)

	// 0x44 round-trip.
	chunk44 := AppendSearchInfoChunk(nil, cfg)
	cfg44, err := ParseSearchInfoChunk(chunk44[4:])
	if err != nil {
		t.Fatalf("ParseSearchInfoChunk: %v", err)
	}
	if cfg44.MatchLen != cfg.MatchLen || cfg44.BaseTableSize != cfg.BaseTableSize || cfg44.TableType != cfg.TableType {
		t.Fatalf("0x44 config mismatch: got %+v want %+v", cfg44, cfg)
	}

	// 0x45 round-trip.
	chunk45 := AppendSearchTableChunk(nil, cfg, reductions, table)
	parsed, err := ParseSearchTableChunk(chunk45[4:])
	if err != nil {
		t.Fatalf("ParseSearchTableChunk: %v", err)
	}
	if !bytes.Equal(parsed.Table, table) {
		t.Fatalf("0x45 table bytes differ (lens %d vs %d)", len(parsed.Table), len(table))
	}
	if parsed.Reductions != reductions {
		t.Fatalf("0x45 reductions mismatch: got %d want %d", parsed.Reductions, reductions)
	}

	// 0x46 round-trip.
	chunk46 := AppendSearchTableCompressedChunk(nil, cfg, reductions, table)
	parsed46, err := ParseSearchTableCompressedChunk(chunk46[4:])
	if err != nil {
		t.Fatalf("ParseSearchTableCompressedChunk: %v", err)
	}
	if !bytes.Equal(parsed46.Table, table) {
		t.Fatalf("0x46 reconstructed bitmap differs from 0x45 bitmap")
	}

	// Contains must return true for every embedded needle.
	for _, n := range needles {
		if !parsed.Contains(n) {
			t.Errorf("0x45 Contains(%q) = false; expected true (table is a possible-match summary)", n)
		}
		if !parsed46.Contains(n) {
			t.Errorf("0x46 Contains(%q) = false; expected true", n)
		}
	}
}

func TestRoundTripBytePrefix(t *testing.T) {
	needles := [][]byte{
		[]byte("\"id\":42"),
		[]byte("\"name\":\"alice\""),
		[]byte("=true"),
	}
	block, _ := makeBlock(needles)

	cfg := SearchConfig{
		MatchLen:      4,
		BaseTableSize: 12,
		TableType:     TableTypeBytePrefix,
		PrefixBytes:   []byte{':', '"', '='},
	}
	table, reductions := BuildSearchTable(cfg, block, nil)

	chunk45 := AppendSearchTableChunk(nil, cfg, reductions, table)
	parsed, err := ParseSearchTableChunk(chunk45[4:])
	if err != nil {
		t.Fatalf("ParseSearchTableChunk: %v", err)
	}
	// The 0x45 wire encodes 8 prefix bytes (padded). After parse, we should see
	// the originals plus padding bytes.
	if len(parsed.Cfg.PrefixBytes) != 8 {
		t.Fatalf("expected 8 prefix bytes on the wire, got %d", len(parsed.Cfg.PrefixBytes))
	}
	if parsed.Cfg.PrefixBytes[0] != ':' || parsed.Cfg.PrefixBytes[1] != '"' || parsed.Cfg.PrefixBytes[2] != '=' {
		t.Fatalf("byte-prefix unpacked wrong: %v", parsed.Cfg.PrefixBytes[:3])
	}

	// Each needle has a prefix byte followed by content — that suffix must hash present.
	for _, n := range needles {
		for i := 1; i+int(cfg.MatchLen) <= len(n); i++ {
			isPfx := false
			for _, p := range cfg.PrefixBytes {
				if n[i-1] == p {
					isPfx = true
					break
				}
			}
			if !isPfx {
				continue
			}
			if !parsed.Contains(n[i : i+int(cfg.MatchLen)]) {
				t.Errorf("byte-prefix Contains(%q) at i=%d in %q = false", n[i:i+int(cfg.MatchLen)], i, n)
			}
		}
	}
}

func TestRoundTripMaskPrefix(t *testing.T) {
	needles := [][]byte{
		[]byte("\"id\":42"),
		[]byte(":99"),
	}
	block, _ := makeBlock(needles)

	cfg := SearchConfig{
		MatchLen:      3,
		BaseTableSize: 10,
		TableType:     TableTypeMaskPrefix,
	}
	cfg.PrefixMask[byte(':')>>3] |= 1 << (byte(':') & 7)
	cfg.PrefixMask[byte('"')>>3] |= 1 << (byte('"') & 7)

	table, reductions := BuildSearchTable(cfg, block, nil)
	chunk45 := AppendSearchTableChunk(nil, cfg, reductions, table)
	parsed, err := ParseSearchTableChunk(chunk45[4:])
	if err != nil {
		t.Fatalf("ParseSearchTableChunk: %v", err)
	}
	if parsed.Cfg.PrefixMask != cfg.PrefixMask {
		t.Fatalf("mask-prefix round-trip mismatch")
	}
}

func TestRoundTripLongPrefix(t *testing.T) {
	needles := [][]byte{
		[]byte("\":\"alpha\""),
		[]byte("\":\"beta\""),
	}
	block, _ := makeBlock(needles)

	cfg := SearchConfig{
		MatchLen:      4,
		BaseTableSize: 10,
		TableType:     TableTypeLongPrefix,
		LongPrefix:    []byte("\":\""),
	}
	table, reductions := BuildSearchTable(cfg, block, nil)
	chunk45 := AppendSearchTableChunk(nil, cfg, reductions, table)
	parsed, err := ParseSearchTableChunk(chunk45[4:])
	if err != nil {
		t.Fatalf("ParseSearchTableChunk: %v", err)
	}
	if !bytes.Equal(parsed.Cfg.LongPrefix, cfg.LongPrefix) {
		t.Fatalf("long-prefix round-trip mismatch: %q vs %q", parsed.Cfg.LongPrefix, cfg.LongPrefix)
	}
	// Each needle starts with the prefix; the matchLen bytes after the prefix must hash present.
	for _, n := range needles {
		after := n[len(cfg.LongPrefix):]
		if len(after) < int(cfg.MatchLen) {
			continue
		}
		if !parsed.Contains(after[:cfg.MatchLen]) {
			t.Errorf("long-prefix Contains(%q after %q) = false", after[:cfg.MatchLen], cfg.LongPrefix)
		}
	}
}

func TestRoundTripLongPrefixExtras(t *testing.T) {
	// Block has the prefix `":"` followed by distinct 16-byte payloads
	// so each occurrence produces 4 different hash entries (extras=3).
	prefix := []byte(`":"`)
	const ml, ex = 4, 3
	needles := [][]byte{
		bytes.Join([][]byte{prefix, []byte("ALPHAALPHAALPHAA")}, nil),
		bytes.Join([][]byte{prefix, []byte("BETABETABETABETA")}, nil),
	}
	block, placements := makeBlock(needles)

	cfg := SearchConfig{
		MatchLen:      ml,
		BaseTableSize: 12,
		TableType:     TableTypeLongPrefix,
		Extras:        ex,
		LongPrefix:    prefix,
	}
	table, reductions := BuildSearchTable(cfg, block, nil)

	chunk44 := AppendSearchInfoChunk(nil, cfg)
	cfg44, err := ParseSearchInfoChunk(chunk44[4:])
	if err != nil {
		t.Fatalf("ParseSearchInfoChunk: %v", err)
	}
	if cfg44.Extras != cfg.Extras {
		t.Fatalf("0x44 extras roundtrip: got %d want %d", cfg44.Extras, cfg.Extras)
	}

	chunk45 := AppendSearchTableChunk(nil, cfg, reductions, table)
	parsed, err := ParseSearchTableChunk(chunk45[4:])
	if err != nil {
		t.Fatalf("ParseSearchTableChunk: %v", err)
	}
	if parsed.Cfg.Extras != cfg.Extras {
		t.Fatalf("0x45 extras roundtrip: got %d want %d", parsed.Cfg.Extras, cfg.Extras)
	}
	if !bytes.Equal(parsed.Cfg.LongPrefix, cfg.LongPrefix) {
		t.Fatalf("0x45 long prefix roundtrip mismatch")
	}

	// Every E+1 window after each prefix occurrence must be present.
	for _, pos := range placements {
		base := pos + len(prefix)
		for j := 0; j <= ex; j++ {
			if !parsed.Contains(block[base+j : base+j+ml]) {
				t.Errorf("Contains window j=%d at pos=%d = false", j, pos)
			}
		}
	}
}

func TestLongPrefixExtrasRejectMalformed(t *testing.T) {
	// Build a payload manually with matchLen=8 + extras=9 (sum 17, illegal).
	payload := []byte{
		byte(TableTypeLongPrefix),
		8,             // matchLen
		12,            // baseTableSize
		2,             // prefix length - 1
		9,             // extras (matchLen+extras = 17 > 16)
		'i', 'd', ':', // prefix
	}
	if _, err := ParseSearchInfoChunk(payload); err == nil {
		t.Fatal("expected error for matchLen+extras > 16")
	}
}

func TestRemoteBlockRefRoundTrip(t *testing.T) {
	refs := []RemoteBlockRef{
		{Offset: 100, MaxMinusActualLen: 0},
		{Offset: 4196, MaxMinusActualLen: 17},
		{Offset: 9000000, MaxMinusActualLen: 1024},
	}
	chunk := AppendRemoteBlockRefChunk(nil, refs)
	got, err := ParseRemoteBlockRefChunk(chunk[4:])
	if err != nil {
		t.Fatalf("ParseRemoteBlockRefChunk: %v", err)
	}
	if len(got) != len(refs) {
		t.Fatalf("ref count mismatch: %d vs %d", len(got), len(refs))
	}
	for i := range refs {
		if got[i] != refs[i] {
			t.Fatalf("ref[%d] mismatch: got %+v want %+v", i, got[i], refs[i])
		}
	}

	// Non-ascending offsets must be rejected by the parser. Hand-craft a
	// payload with a zero delta on the second ref (the encoder panics on
	// this input by design).
	bad := []byte{}
	bad = binary.AppendUvarint(bad, 200) // first ref absolute
	bad = binary.AppendUvarint(bad, 0)
	bad = binary.AppendUvarint(bad, 0) // second ref delta = 0 → non-ascending
	bad = binary.AppendUvarint(bad, 0)
	if _, err := ParseRemoteBlockRefChunk(bad); err == nil {
		t.Fatal("expected ascending-offset error, got nil")
	}

	// And: appender panics on bad input.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on non-ascending refs, got none")
		}
	}()
	_ = AppendRemoteBlockRefChunk(nil, []RemoteBlockRef{{Offset: 200}, {Offset: 100}})
}

func TestContainsAbsent(t *testing.T) {
	// A block of all 'a' — only the single 5-byte window "aaaaa" should be present.
	block := bytes.Repeat([]byte{'a'}, 4096)
	cfg := SearchConfig{
		MatchLen:      5,
		BaseTableSize: 14, // big table → few false positives
		TableType:     TableTypeNoPrefix,
	}
	table, reductions := BuildSearchTable(cfg, block, nil)
	parsed, err := ParseSearchTableChunk(AppendSearchTableChunk(nil, cfg, reductions, table)[4:])
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Contains([]byte("aaaaa")) {
		t.Fatal("Contains(aaaaa) = false; expected true")
	}
	// Try several values that should not be present. A few false positives are
	// acceptable, but most should report false.
	misses := 0
	for _, needle := range [][]byte{
		[]byte("XYZWQ"), []byte("12345"), []byte("hello"), []byte("zzzzz"),
		[]byte("====="), []byte("bcdef"), []byte("MinIO"),
	} {
		if !parsed.Contains(needle) {
			misses++
		}
	}
	if misses < 5 {
		t.Errorf("expected most random needles to be absent; only %d/7 reported absent", misses)
	}
}

func TestCRCMismatch(t *testing.T) {
	block := bytes.Repeat([]byte{'x'}, 256)
	cfg := SearchConfig{MatchLen: 4, BaseTableSize: 8, TableType: TableTypeNoPrefix}
	table, reductions := BuildSearchTable(cfg, block, nil)
	chunk := AppendSearchTableChunk(nil, cfg, reductions, table)
	// Flip a bit in the table data; offset 4 (header) + 3 (config) + 1 (reductions) + 4 (CRC) = 12 is first table byte.
	chunk[12+1] ^= 0x01
	if _, err := ParseSearchTableChunk(chunk[4:]); err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
}

func TestSparseBitTableDecoder(t *testing.T) {
	// Hand-build a sparse encoding: positions 0, 5, 8, 263 set in a 32-byte bitmap.
	// Distances from previous set bit (or origin): 0, 5, 3, 255 (which means "+255") then 0 (= position 263).
	// Encoding: gap-then-byte. For each set bit: emit (sum-of-gaps-to-here-from-prev)-as-byte
	// Actually: each byte is the distance to the next set bit; 255 means add 255 and continue.
	// position 0:   distance from previous = 0 (count from origin = position before increment).
	// Re-check the encoder: starts gap=0, scans bits. For each set bit, appends gap-as-byte
	// (with 255-escaping for large gaps), then pos++, gap=0. Re-derive expected payload:
	//   bit at pos 0: gap = 0, append 0, pos = 1.
	//   bit at pos 5: gap = 5 - 1 = 4, append 4, pos = 6.
	//   bit at pos 8: gap = 8 - 6 = 2, append 2, pos = 9.
	//   bit at pos 263: gap = 263 - 9 = 254, append 254, pos = 264.
	src := []byte{0, 4, 2, 254}
	dst := make([]byte, 64) // 512 bits
	if err := decodeSparseBitTable(dst, src); err != nil {
		t.Fatalf("decodeSparseBitTable: %v", err)
	}
	expected := map[int]bool{0: true, 5: true, 8: true, 263: true}
	for i := 0; i < len(dst)*8; i++ {
		want := expected[i]
		got := dst[i>>3]&(1<<(i&7)) != 0
		if got != want {
			t.Fatalf("bit %d: got %v want %v", i, got, want)
		}
	}
}
