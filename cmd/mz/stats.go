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

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minlz/cmd/internal/filepathx"
)

// Chunk types (redeclared from minlz package; spec-stable).
const (
	chunkLegacy        = 0x00
	chunkUncompressed  = 0x01
	chunkMinLZ         = 0x02
	chunkMinLZCompCRC  = 0x03
	chunkEOF           = 0x20
	chunkIndex         = 0x40
	chunkSearchInfo    = 0x44
	chunkSearchTable   = 0x45
	chunkPadding       = 0xfe
	chunkStreamID      = 0xff
	chunkUserSkipLo    = 0x80
	chunkUserSkipHi    = 0xbf
	chunkUserNoSkipLo  = 0xc0
	chunkUserNoSkipHi  = 0xfd
	maxNonSkippableChk = 0x3f
)

const (
	minCopy2Offset = 64
	minCopy3Offset = 65536
	maxBlockLog    = 23

	numHistBuckets = 25
)

// Op type indices.
const (
	opLiteral = iota
	opRepeat
	opCopy1
	opCopy2
	opCopy2F
	opCopy3
	opCopy3F
	opFusedLit
	numOpTypes
)

var opNames = [numOpTypes]string{"Literal", "Repeat", "Copy1", "Copy2", "Copy2-F", "Copy3", "Copy3-F", "FusedLit"}

func opTypeName(i int) string {
	if i < 0 || i >= numOpTypes {
		return "?"
	}
	return opNames[i]
}

// opStats accumulates statistics for a single op type within a block (or aggregated).
type opStats struct {
	Count          uint64                 `json:"count"`
	OutBytes       uint64                 `json:"outBytes"`
	HeaderBytes    uint64                 `json:"headerBytes"`
	FusedLitBytes  uint64                 `json:"fusedLitBytes,omitempty"`
	FusedOpCount   uint64                 `json:"fusedOpCount,omitempty"`
	MinLen         uint32                 `json:"minLen,omitempty"`
	MaxLen         uint32                 `json:"maxLen,omitempty"`
	SumLen         uint64                 `json:"sumLen,omitempty"`
	MinOff         uint32                 `json:"minOff,omitempty"`
	MaxOff         uint32                 `json:"maxOff,omitempty"`
	SumOff         uint64                 `json:"sumOff,omitempty"`
	LenHist        [numHistBuckets]uint64 `json:"lenHist"`
	OffHist        [numHistBuckets]uint64 `json:"offHist,omitempty"`
	HeaderSizeHist [8]uint64              `json:"headerSizeHist"`
	FusedLenHist   [5]uint64              `json:"fusedLenHist,omitempty"`
}

func (s *opStats) addLen(v uint32) {
	if s.MinLen == 0 || v < s.MinLen {
		s.MinLen = v
	}
	if v > s.MaxLen {
		s.MaxLen = v
	}
	s.SumLen += uint64(v)
	s.LenHist[bucket(v)]++
}

func (s *opStats) addOff(v uint32) {
	if s.MinOff == 0 || v < s.MinOff {
		s.MinOff = v
	}
	if v > s.MaxOff {
		s.MaxOff = v
	}
	s.SumOff += uint64(v)
	s.OffHist[bucket(v)]++
}

func (s *opStats) merge(o *opStats) {
	if o.Count == 0 {
		return
	}
	if s.MinLen == 0 || (o.MinLen > 0 && o.MinLen < s.MinLen) {
		s.MinLen = o.MinLen
	}
	if o.MaxLen > s.MaxLen {
		s.MaxLen = o.MaxLen
	}
	if s.MinOff == 0 || (o.MinOff > 0 && o.MinOff < s.MinOff) {
		s.MinOff = o.MinOff
	}
	if o.MaxOff > s.MaxOff {
		s.MaxOff = o.MaxOff
	}
	s.Count += o.Count
	s.OutBytes += o.OutBytes
	s.HeaderBytes += o.HeaderBytes
	s.FusedLitBytes += o.FusedLitBytes
	s.FusedOpCount += o.FusedOpCount
	s.SumLen += o.SumLen
	s.SumOff += o.SumOff
	for i := range s.LenHist {
		s.LenHist[i] += o.LenHist[i]
	}
	for i := range s.OffHist {
		s.OffHist[i] += o.OffHist[i]
	}
	for i := range s.HeaderSizeHist {
		s.HeaderSizeHist[i] += o.HeaderSizeHist[i]
	}
	for i := range s.FusedLenHist {
		s.FusedLenHist[i] += o.FusedLenHist[i]
	}
}

// blockStats holds the result of analyzing one block.
type blockStats struct {
	Index         int                 `json:"index"`
	ChunkType     uint8               `json:"chunkType"`
	CompSize      int                 `json:"compSize"`
	UncompSize    int                 `json:"uncompSize"`
	Ops           [numOpTypes]opStats `json:"ops"`
	RepeatPrev    [numOpTypes]uint64  `json:"repeatPrev"`
	LongestCopy   uint32              `json:"longestCopy"`
	LongestLitRun uint32              `json:"longestLitRun"`
}

func (b *blockStats) totalOps() uint64 {
	var n uint64
	for i := range b.Ops {
		if i == opFusedLit {
			continue
		}
		n += b.Ops[i].Count
	}
	return n
}

// streamStats holds chunk-level statistics for a .mz stream.
type streamStats struct {
	ChunkCount     [256]uint64            `json:"chunkCount"`
	ChunkBytes     [256]uint64            `json:"chunkBytes"`
	DeclaredMaxBlk int                    `json:"declaredMaxBlk"`
	StreamIDCount  int                    `json:"streamIDCount"`
	CrcMode02      uint64                 `json:"crcMode02"`
	CrcMode03      uint64                 `json:"crcMode03"`
	BlockSizeHist  [numHistBuckets]uint64 `json:"blockSizeHist"`
	UncompSizeHist [numHistBuckets]uint64 `json:"uncompSizeHist"`
	RatioBuckets   [11]uint64             `json:"ratioBuckets"`
	LegacyBlocks   uint64                 `json:"legacyBlocks"`
	UncompBlocks   uint64                 `json:"uncompBlocks"`
	MinlzBlocks    uint64                 `json:"minlzBlocks"`
}

// fileReport is the per-file result.
type fileReport struct {
	Name             string              `json:"name"`
	InputSize        int64               `json:"inputSize"`
	Format           string              `json:"format"`
	Elapsed          string              `json:"elapsed"`
	Stream           streamStats         `json:"stream"`
	OpTotals         [numOpTypes]opStats `json:"opTotals"`
	Blocks           []blockStats        `json:"blocks,omitempty"`
	TotalOps         uint64              `json:"totalOps"`
	TotalOut         uint64              `json:"totalOut"`
	TotalEnc         uint64              `json:"totalEnc"`
	TotalLitBytes    uint64              `json:"totalLitBytes"`
	TotalCopyBytes   uint64              `json:"totalCopyBytes"`
	TotalRepeatBytes uint64              `json:"totalRepeatBytes"`
	LongestCopy      uint32              `json:"longestCopy"`
	LongestLitRun    uint32              `json:"longestLitRun"`
	RepeatPrev       [numOpTypes]uint64  `json:"repeatPrev"`
}

type statsOpts struct {
	json     bool
	csv      bool
	perBlock bool
	noHist   bool
	quiet    bool
}

func mainStats(args []string) {
	var (
		fs       = flag.NewFlagSet("stats", flag.ExitOnError)
		asJSON   = fs.Bool("json", false, "Emit JSON instead of text")
		asCSV    = fs.Bool("csv", false, "Emit CSV instead of text")
		perBlock = fs.Bool("per-block", false, "Include per-block one-line summary")
		noHist   = fs.Bool("no-hist", false, "Omit histograms")
		quiet    = fs.Bool("q", false, "Quiet: don't print filenames/timing")
		help     = fs.Bool("help", false, "Display help")
	)
	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintln(w, `Analyze a MinLZ stream (.mz) or raw block (.mzb) and print a breakdown
of how the input was encoded: operation mix, encoding overhead, length and
offset distributions.

Format is auto-detected. Use - to read from stdin. Wildcards accepted.

Options:`)
		fs.PrintDefaults()
		fmt.Fprintf(w, "\nUsage: %v stats [options] <input...>\n", os.Args[0])
	}
	fs.Parse(args)
	args = fs.Args()
	if *help || len(args) == 0 {
		fs.Usage()
		if *help {
			os.Exit(0)
		}
		os.Exit(1)
	}
	if *asJSON && *asCSV {
		exitErr(errors.New("-json and -csv are mutually exclusive"))
	}
	opts := statsOpts{json: *asJSON, csv: *asCSV, perBlock: *perBlock, noHist: *noHist, quiet: *quiet}

	var files []string
	for _, p := range args {
		if p == "-" || isHTTP(p) {
			files = append(files, p)
			continue
		}
		if strings.ContainsAny(p, "*?[") {
			matches, err := filepathx.Glob(p)
			exitErr(err)
			if len(matches) == 0 {
				exitErr(fmt.Errorf("no files matched %v", p))
			}
			files = append(files, matches...)
			continue
		}
		files = append(files, p)
	}

	reports := make([]fileReport, 0, len(files))
	for _, file := range files {
		rep, err := statsFile(file, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", file, err)
			continue
		}
		reports = append(reports, rep)
	}

	switch {
	case opts.json:
		out := jsonReport{Files: make([]jsonFile, 0, len(reports))}
		for i := range reports {
			out.Files = append(out.Files, buildJSONFile(&reports[i], opts))
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		exitErr(enc.Encode(out))
	case opts.csv:
		exitErr(renderCSV(os.Stdout, reports, opts))
	default:
		for _, rep := range reports {
			renderText(os.Stdout, &rep, opts)
		}
	}
}

func statsFile(file string, opts statsOpts) (fileReport, error) {
	rep := fileReport{Name: file}
	var rc io.ReadCloser
	var size int64
	switch {
	case file == "-":
		rc = io.NopCloser(os.Stdin)
		size = -1
	case isHTTP(file):
		rc, size, _ = openFile(file, false)
	default:
		f, err := os.Open(file)
		if err != nil {
			return rep, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return rep, err
		}
		rc = f
		size = st.Size()
	}
	defer rc.Close()
	rep.InputSize = size

	var rd io.Reader = rc
	var counter *rCounter
	if size < 0 {
		counter = &rCounter{in: rd}
		rd = counter
	}
	br := bufio.NewReaderSize(rd, 1<<20)
	first, err := br.Peek(1)
	if err != nil {
		return rep, fmt.Errorf("read: %w", err)
	}

	start := time.Now()
	switch first[0] {
	case 0xff:
		rep.Format = "stream"
		if err := analyzeStream(br, &rep, opts); err != nil {
			return rep, err
		}
	case 0x00:
		rep.Format = "block"
		data, err := io.ReadAll(br)
		if err != nil {
			return rep, err
		}
		if err := analyzeRawBlock(data, &rep, opts); err != nil {
			return rep, err
		}
	default:
		return rep, fmt.Errorf("not a MinLZ stream or block (first byte 0x%02x)", first[0])
	}
	if counter != nil {
		rep.InputSize = counter.BytesRead() - int64(br.Buffered())
	}
	rep.Elapsed = time.Since(start).Round(time.Microsecond).String()
	return rep, nil
}

// analyzeRawBlock parses a .mzb file: leading 0x00, varint decoded size, then ops.
func analyzeRawBlock(data []byte, rep *fileReport, opts statsOpts) error {
	if len(data) == 0 || data[0] != 0 {
		return errors.New("not a MinLZ block: missing 0x00 header byte")
	}
	if len(data) == 1 {
		return nil // empty block
	}
	uncomp, n := binary.Uvarint(data[1:])
	if n <= 0 {
		return errors.New("corrupt varint length in block")
	}
	hdr := 1 + n
	body := data[hdr:]
	bs := blockStats{Index: 0, ChunkType: 0, CompSize: len(data), UncompSize: int(uncomp)}
	if uncomp == 0 {
		// All literals.
		bs.Ops[opLiteral].Count = 1
		bs.Ops[opLiteral].OutBytes = uint64(len(body))
		bs.Ops[opLiteral].HeaderBytes = uint64(hdr)
		bs.Ops[opLiteral].SumLen = uint64(len(body))
		bs.Ops[opLiteral].MinLen = uint32(len(body))
		bs.Ops[opLiteral].MaxLen = uint32(len(body))
		bs.Ops[opLiteral].LenHist[bucket(uint32(len(body)))]++
		bs.Ops[opLiteral].HeaderSizeHist[clampHdr(hdr)]++
		bs.LongestLitRun = uint32(len(body))
	} else {
		if err := walkBlock(body, int(uncomp), &bs); err != nil {
			return err
		}
	}
	mergeBlockIntoReport(rep, &bs, opts)
	return nil
}

// analyzeStream parses .mz framing chunks.
func analyzeStream(br *bufio.Reader, rep *fileReport, opts statsOpts) error {
	var hdr [4]byte
	streamSawID := false
	wantEOF := false
	var uncompEmitted uint64
	for {
		_, err := io.ReadFull(br, hdr[:])
		if err == io.EOF {
			if wantEOF {
				return errors.New("unexpected EOF: stream ended without 0x20 EOF chunk")
			}
			return nil
		}
		if err != nil {
			return err
		}
		ct := hdr[0]
		clen := int(hdr[1]) | int(hdr[2])<<8 | int(hdr[3])<<16
		rep.Stream.ChunkCount[ct]++
		rep.Stream.ChunkBytes[ct] += uint64(clen) + 4

		switch ct {
		case chunkStreamID:
			body := make([]byte, clen)
			if _, err := io.ReadFull(br, body); err != nil {
				return err
			}
			if clen < 6 {
				return fmt.Errorf("invalid stream identifier length %d", clen)
			}
			magic := string(body[:5])
			if magic != "MinLz" {
				return fmt.Errorf("unsupported stream magic %q (only MinLz supported by stats)", magic)
			}
			szByte := body[5]
			if szByte&0xc0 != 0 {
				return errors.New("invalid stream identifier flags")
			}
			n := int(szByte&15) + 10
			if n > maxBlockLog {
				return fmt.Errorf("invalid declared block size byte 0x%02x", szByte)
			}
			rep.Stream.DeclaredMaxBlk = 1 << n
			rep.Stream.StreamIDCount++
			streamSawID = true
			wantEOF = true
			uncompEmitted = 0

		case chunkMinLZ, chunkMinLZCompCRC:
			if !streamSawID {
				return errors.New("compressed chunk before stream identifier")
			}
			if clen < 4 {
				return fmt.Errorf("compressed chunk too short %d", clen)
			}
			buf := make([]byte, clen)
			if _, err := io.ReadFull(br, buf); err != nil {
				return err
			}
			// Skip 4-byte CRC.
			payload := buf[4:]
			uncomp, n := binary.Uvarint(payload)
			if n <= 0 {
				return errors.New("corrupt varint in compressed chunk")
			}
			body := payload[n:]
			bs := blockStats{
				Index:      len(rep.Blocks),
				ChunkType:  ct,
				CompSize:   clen, // chunk payload size (4 CRC + varint + ops)
				UncompSize: int(uncomp),
			}
			if uncomp != 0 {
				if err := walkBlock(body, int(uncomp), &bs); err != nil {
					return fmt.Errorf("block %d: %w", bs.Index, err)
				}
			} else {
				// All-literals form: entire body is literal data.
				ll := uint32(len(body))
				bs.Ops[opLiteral].Count = 1
				bs.Ops[opLiteral].OutBytes = uint64(ll)
				bs.Ops[opLiteral].HeaderBytes = uint64(4 + n) // CRC + varint
				bs.Ops[opLiteral].SumLen = uint64(ll)
				bs.Ops[opLiteral].MinLen = ll
				bs.Ops[opLiteral].MaxLen = ll
				bs.Ops[opLiteral].LenHist[bucket(ll)]++
				bs.LongestLitRun = ll
			}
			rep.Stream.MinlzBlocks++
			if ct == chunkMinLZ {
				rep.Stream.CrcMode02++
			} else {
				rep.Stream.CrcMode03++
			}
			rep.Stream.BlockSizeHist[bucket(uint32(clen))]++
			rep.Stream.UncompSizeHist[bucket(uint32(uncomp))]++
			rep.Stream.RatioBuckets[ratioBucket(int(uncomp), clen)]++
			mergeBlockIntoReport(rep, &bs, opts)
			uncompEmitted += uncomp

		case chunkUncompressed:
			if clen < 4 {
				return fmt.Errorf("uncompressed chunk too short %d", clen)
			}
			// Skip CRC + body.
			if _, err := io.CopyN(io.Discard, br, int64(clen)); err != nil {
				return err
			}
			rep.Stream.UncompBlocks++
			n := clen - 4
			rep.Stream.BlockSizeHist[bucket(uint32(clen))]++
			rep.Stream.UncompSizeHist[bucket(uint32(n))]++
			rep.Stream.RatioBuckets[ratioBucket(n, clen)]++
			uncompEmitted += uint64(n)

		case chunkLegacy:
			if _, err := io.CopyN(io.Discard, br, int64(clen)); err != nil {
				return err
			}
			rep.Stream.LegacyBlocks++

		case chunkEOF:
			if clen > binary.MaxVarintLen64 {
				return fmt.Errorf("EOF chunk too long: %d", clen)
			}
			if clen > 0 {
				buf := make([]byte, clen)
				if _, err := io.ReadFull(br, buf); err != nil {
					return err
				}
				wantSize, n := binary.Uvarint(buf)
				if n != clen {
					return fmt.Errorf("EOF chunk varint length mismatch: parsed %d of %d", n, clen)
				}
				if wantSize != uncompEmitted {
					return fmt.Errorf("EOF size mismatch: declared %d, emitted %d", wantSize, uncompEmitted)
				}
			}
			wantEOF = false
			streamSawID = false

		case chunkPadding, chunkIndex, chunkSearchInfo, chunkSearchTable:
			if _, err := io.CopyN(io.Discard, br, int64(clen)); err != nil {
				return err
			}

		default:
			if ct <= maxNonSkippableChk {
				return fmt.Errorf("unknown non-skippable chunk 0x%02x", ct)
			}
			if ct >= chunkUserNoSkipLo && ct <= chunkUserNoSkipHi {
				// User-non-skippable: we don't know how to interpret; record and skip.
			}
			if _, err := io.CopyN(io.Discard, br, int64(clen)); err != nil {
				return err
			}
		}
	}
}

// walkBlock walks ops in body using the declared decompressed size.
// It updates bs.Ops, bs.RepeatPrev, bs.LongestCopy, bs.LongestLitRun.
func walkBlock(src []byte, wantSize int, bs *blockStats) error {
	var d, s int
	offset := 1
	prevOp := -1
	prevSet := false
	litRun := uint32(0)

	emit := func(op int, hdr int, length uint32, fusedLits uint32, off uint32) {
		st := &bs.Ops[op]
		st.Count++
		st.OutBytes += uint64(length)
		st.HeaderBytes += uint64(hdr)
		if op != opFusedLit {
			st.HeaderSizeHist[clampHdr(hdr)]++
		}
		if length > 0 {
			st.addLen(length)
		}
		if op == opCopy2F || op == opCopy3F {
			st.FusedLitBytes += uint64(fusedLits)
			st.FusedOpCount++
			st.FusedLenHist[fusedLits]++
		}
		if op != opLiteral && op != opFusedLit {
			st.addOff(off)
			if length > bs.LongestCopy {
				bs.LongestCopy = length
			}
		}
		if op == opRepeat && prevSet {
			bs.RepeatPrev[prevOp]++
		}
		switch op {
		case opLiteral:
			litRun += length
			if litRun > bs.LongestLitRun {
				bs.LongestLitRun = litRun
			}
		case opCopy2F, opCopy3F:
			if fusedLits > 0 {
				lr := litRun + fusedLits
				if lr > bs.LongestLitRun {
					bs.LongestLitRun = lr
				}
			}
			litRun = 0
		case opFusedLit:
			// Handled by the parent fused copy above; this pseudo-op tracks
			// fused-lit stats without disturbing the litRun chain.
		default:
			litRun = 0
		}
		if op != opFusedLit {
			prevOp = op
			prevSet = true
		}
	}

	for s < len(src) {
		opStart := s
		tag := src[s]
		switch tag & 0x03 {
		case 0x00: // literal or repeat
			isRepeat := tag&4 != 0
			x := uint32(tag >> 3)
			var length uint32
			switch {
			case x < 29:
				s++
				length = x + 1
			case x == 29:
				if s+2 > len(src) {
					return errors.New("truncated literal length (1)")
				}
				length = 30 + uint32(src[s+1])
				s += 2
			case x == 30:
				if s+3 > len(src) {
					return errors.New("truncated literal length (2)")
				}
				length = 30 + (uint32(src[s+1]) | uint32(src[s+2])<<8)
				s += 3
			default: // x == 31
				if s+4 > len(src) {
					return errors.New("truncated literal length (3)")
				}
				length = 30 + (uint32(src[s+1]) | uint32(src[s+2])<<8 | uint32(src[s+3])<<16)
				s += 4
			}
			hdr := s - opStart
			if isRepeat {
				if !prevSet && offset == 1 {
					// allowed; repeat after no prior copy uses offset=1
				}
				if length == 0 || length > uint32(wantSize-d) || uint32(offset) > uint32(d) {
					return fmt.Errorf("repeat: bad length %d offset %d at dst %d", length, offset, d)
				}
				emit(opRepeat, hdr, length, 0, uint32(offset))
				d += int(length)
				continue
			}
			if int(length) > wantSize-d || s+int(length) > len(src) {
				return fmt.Errorf("literal: bad length %d at dst %d (avail %d, src %d)", length, d, wantSize-d, len(src)-s)
			}
			emit(opLiteral, hdr, length, 0, 0)
			d += int(length)
			s += int(length)

		case 0x01: // copy1
			if s+2 > len(src) {
				return errors.New("truncated copy1")
			}
			length := uint32(tag>>2) & 15
			offset = int(uint16(src[s])|uint16(src[s+1])<<8)>>6 + 1
			if length == 15 {
				if s+3 > len(src) {
					return errors.New("truncated copy1 ext")
				}
				length = uint32(src[s+2]) + 18
				s += 3
			} else {
				length += 4
				s += 2
			}
			hdr := s - opStart
			if length > uint32(wantSize-d) || uint32(offset) > uint32(d) {
				return fmt.Errorf("copy1: bad length %d offset %d at dst %d", length, offset, d)
			}
			emit(opCopy1, hdr, length, 0, uint32(offset))
			d += int(length)

		case 0x02: // copy2
			if s+3 > len(src) {
				return errors.New("truncated copy2")
			}
			length := uint32(tag) >> 2
			off := int(uint16(src[s+1]) | uint16(src[s+2])<<8)
			switch {
			case length <= 60:
				length += 4
				s += 3
			case length == 61:
				if s+4 > len(src) {
					return errors.New("truncated copy2.61")
				}
				length = uint32(src[s+3]) + 64
				s += 4
			case length == 62:
				if s+5 > len(src) {
					return errors.New("truncated copy2.62")
				}
				length = uint32(src[s+3]) | uint32(src[s+4])<<8 + 64
				s += 5
			default: // 63
				if s+6 > len(src) {
					return errors.New("truncated copy2.63")
				}
				length = uint32(src[s+3]) | uint32(src[s+4])<<8 | uint32(src[s+5])<<16 + 64
				s += 6
			}
			offset = off + minCopy2Offset
			hdr := s - opStart
			if length > uint32(wantSize-d) || uint32(offset) > uint32(d) {
				return fmt.Errorf("copy2: bad length %d offset %d at dst %d", length, offset, d)
			}
			emit(opCopy2, hdr, length, 0, uint32(offset))
			d += int(length)

		case 0x03: // copy2-fused or copy3
			if s+4 > len(src) {
				return errors.New("truncated copy3/copy2f")
			}
			val := binary.LittleEndian.Uint32(src[s:])
			isCopy3 := val&4 != 0
			litLen := uint32(val>>3) & 3
			var length uint32
			var op int
			if !isCopy3 {
				op = opCopy2F
				length = 4 + (val>>5)&7
				offset = int((val>>8)&65535) + minCopy2Offset
				s += 3
				litLen++ // copy2-fused encodes (litLen-1) in bits, so adjust
			} else {
				op = opCopy3
				lengthTmp := (val >> 5) & 63
				offset = int(val>>11) + minCopy3Offset
				switch {
				case lengthTmp < 61:
					length = lengthTmp + 4
					s += 4
				case lengthTmp == 61:
					if s+5 > len(src) {
						return errors.New("truncated copy3.61")
					}
					length = uint32(src[s+4]) + 64
					s += 5
				case lengthTmp == 62:
					if s+6 > len(src) {
						return errors.New("truncated copy3.62")
					}
					length = uint32(src[s+4]) | uint32(src[s+5])<<8 + 64
					s += 6
				default: // 63
					if s+7 > len(src) {
						return errors.New("truncated copy3.63")
					}
					length = uint32(src[s+4]) | uint32(src[s+5])<<8 | uint32(src[s+6])<<16 + 64
					s += 7
				}
				if litLen > 0 {
					op = opCopy3F
				}
			}
			hdr := s - opStart
			if litLen > 0 {
				if s+int(litLen) > len(src) || d+int(litLen) > wantSize {
					return fmt.Errorf("copy-fused: bad lit len %d at dst %d", litLen, d)
				}
				s += int(litLen)
				d += int(litLen)
			}
			if length > uint32(wantSize-d) || uint32(offset) > uint32(d) {
				return fmt.Errorf("copy3/2f: bad length %d offset %d at dst %d", length, offset, d)
			}
			emit(op, hdr, length, litLen, uint32(offset))
			if litLen > 0 {
				emit(opFusedLit, 0, litLen, 0, 0)
			}
			d += int(length)
		}
	}

	if d != wantSize {
		return fmt.Errorf("block ended with d=%d want=%d", d, wantSize)
	}
	return nil
}

func mergeBlockIntoReport(rep *fileReport, bs *blockStats, opts statsOpts) {
	for i := range bs.Ops {
		rep.OpTotals[i].merge(&bs.Ops[i])
		// FusedLit is a pseudo-op (one emission per fused copy); don't count
		// in TotalOps which represents real codec operations.
		if i != opFusedLit {
			rep.TotalOps += bs.Ops[i].Count
		}
		rep.TotalOut += bs.Ops[i].OutBytes
		rep.TotalEnc += bs.Ops[i].HeaderBytes
	}
	// Literal bodies and fused-lit bytes are in the compressed stream too.
	rep.TotalEnc += bs.Ops[opLiteral].OutBytes + bs.Ops[opFusedLit].OutBytes
	rep.TotalLitBytes += bs.Ops[opLiteral].OutBytes + bs.Ops[opFusedLit].OutBytes
	for _, o := range []int{opCopy1, opCopy2, opCopy2F, opCopy3, opCopy3F} {
		rep.TotalCopyBytes += bs.Ops[o].OutBytes
	}
	rep.TotalRepeatBytes += bs.Ops[opRepeat].OutBytes
	if bs.LongestCopy > rep.LongestCopy {
		rep.LongestCopy = bs.LongestCopy
	}
	if bs.LongestLitRun > rep.LongestLitRun {
		rep.LongestLitRun = bs.LongestLitRun
	}
	for i := range bs.RepeatPrev {
		rep.RepeatPrev[i] += bs.RepeatPrev[i]
	}
	if opts.perBlock {
		rep.Blocks = append(rep.Blocks, *bs)
	}
}

// bucket returns the log2 bucket index for v (capped at numHistBuckets-1).
func bucket(v uint32) int {
	if v == 0 {
		return 0
	}
	b := bits.Len32(v) - 1
	if b >= numHistBuckets {
		b = numHistBuckets - 1
	}
	return b
}

func bucketLabel(i int) string {
	if i == 0 {
		return "1"
	}
	if i >= numHistBuckets-1 {
		return fmt.Sprintf(">=%s", abbrevSize(uint64(1)<<i))
	}
	lo := uint64(1) << i
	hi := (uint64(1) << (i + 1)) - 1 // inclusive
	return fmt.Sprintf("%s-%s", abbrevSize(lo), abbrevHi(hi))
}

// abbrevSize abbreviates only clean K/M multiples; below 1K stays raw.
func abbrevSize(v uint64) string {
	if v >= 1<<20 && v%(1<<20) == 0 {
		return fmt.Sprintf("%dM", v>>20)
	}
	if v >= 1024 && v%1024 == 0 {
		return fmt.Sprintf("%dK", v>>10)
	}
	return strconv.FormatUint(v, 10)
}

// abbrevHi renders an inclusive upper bound. When v+1 is a clean K/M boundary,
// display as that abbreviation (technically off by one). Otherwise raw.
func abbrevHi(v uint64) string {
	next := v + 1
	if next >= 1<<20 && next%(1<<20) == 0 {
		return fmt.Sprintf("%dM", next>>20)
	}
	if next >= 1024 && next%1024 == 0 {
		return fmt.Sprintf("%dK", next>>10)
	}
	return strconv.FormatUint(v, 10)
}

// ratioBucket returns 0..10 for compressed/uncompressed ratio.
// 0 = 0-10%, 9 = 90-100%, 10 = >100% (expanded).
func ratioBucket(uncomp, comp int) int {
	if uncomp <= 0 {
		return 10
	}
	r := comp * 100 / uncomp
	if r >= 100 {
		return 10
	}
	return r / 10
}

func clampHdr(n int) int {
	if n < 0 {
		return 0
	}
	if n > 7 {
		return 7
	}
	return n
}

// ============================================================================
// JSON output
// ============================================================================

type jsonReport struct {
	Files []jsonFile `json:"files"`
}

type jsonFile struct {
	Name              string               `json:"name"`
	InputSize         int64                `json:"inputSize"`
	Format            string               `json:"format"`
	Elapsed           string               `json:"elapsed"`
	Totals            jsonTotals           `json:"totals"`
	Ops               []jsonOp             `json:"ops"`
	FusedLitBreakdown []jsonFusedBreakdown `json:"fusedLitBreakdown,omitempty"`
	RepeatLineage     []jsonRepeatLineage  `json:"repeatLineage,omitempty"`
	Stream            *jsonStream          `json:"stream,omitempty"`
	Blocks            []jsonBlock          `json:"blocks,omitempty"`
}

type jsonTotals struct {
	Ops            uint64  `json:"ops"`
	OutputBytes    uint64  `json:"outputBytes"`
	EncodingBytes  uint64  `json:"encodingBytes"`
	BytesPerOutput float64 `json:"bytesPerOutput"`
	OutputPerOp    float64 `json:"outputPerOp"`
	OpsPerKBOutput float64 `json:"opsPerKBOutput"`
	LiteralBytes   uint64  `json:"literalBytes"`
	LiteralPct     float64 `json:"literalPct"`
	CopyBytes      uint64  `json:"copyBytes"`
	CopyPct        float64 `json:"copyPct"`
	RepeatBytes    uint64  `json:"repeatBytes"`
	RepeatPct      float64 `json:"repeatPct"`
	LongestCopy    uint32  `json:"longestCopy"`
	LongestLitRun  uint32  `json:"longestLitRun"`
}

type jsonOp struct {
	Type           string        `json:"type"`
	Count          uint64        `json:"count"`
	OutBytes       uint64        `json:"outBytes"`
	EncBytes       uint64        `json:"encBytes"`
	HeaderBytes    uint64        `json:"headerBytes"`
	AvgOut         float64       `json:"avgOut"`
	AvgEnc         float64       `json:"avgEnc"`
	PctOps         float64       `json:"pctOps,omitempty"`
	PctOut         float64       `json:"pctOut"`
	PctEnc         float64       `json:"pctEnc"`
	Saved          int64         `json:"saved"`
	MinLen         uint32        `json:"minLen,omitempty"`
	MaxLen         uint32        `json:"maxLen,omitempty"`
	AvgLen         float64       `json:"avgLen,omitempty"`
	MinOff         uint32        `json:"minOff,omitempty"`
	MaxOff         uint32        `json:"maxOff,omitempty"`
	AvgOff         float64       `json:"avgOff,omitempty"`
	HeaderSizeHist []jsonHdrSize `json:"headerSizeHist,omitempty"`
	LenHist        []jsonBucket  `json:"lenHist,omitempty"`
	OffHist        []jsonBucket  `json:"offHist,omitempty"`
}

type jsonHdrSize struct {
	Bytes int    `json:"bytes"`
	Count uint64 `json:"count"`
}

type jsonBucket struct {
	Label string  `json:"label"`
	Lo    uint64  `json:"lo"`
	Hi    uint64  `json:"hi,omitempty"` // 0 = open-ended
	Count uint64  `json:"count"`
	Pct   float64 `json:"pct"`
}

type jsonFusedBreakdown struct {
	Type     string         `json:"type"`
	Count    uint64         `json:"count"`
	Bytes    uint64         `json:"bytes"`
	AvgBytes float64        `json:"avgBytes"`
	Lengths  []jsonFusedLen `json:"lengths"`
}

type jsonFusedLen struct {
	Length int     `json:"length"`
	Count  uint64  `json:"count"`
	Pct    float64 `json:"pct"`
}

type jsonRepeatLineage struct {
	PrevType string  `json:"prevType"`
	Count    uint64  `json:"count"`
	Pct      float64 `json:"pct"`
}

type jsonStream struct {
	DeclaredMaxBlockSize int               `json:"declaredMaxBlockSize"`
	StreamIdentifiers    int               `json:"streamIdentifiers"`
	Blocks               jsonStreamBlocks  `json:"blocks"`
	ChunkTypes           []jsonChunkType   `json:"chunkTypes"`
	CompBlockSizeHist    []jsonBucket      `json:"compressedBlockSizeHist,omitempty"`
	UncompBlockSizeHist  []jsonBucket      `json:"uncompressedBlockSizeHist,omitempty"`
	RatioHist            []jsonRatioBucket `json:"ratioHist,omitempty"`
}

type jsonStreamBlocks struct {
	Minlz           uint64 `json:"minlz"`
	CrcUncompressed uint64 `json:"crcUncompressed"`
	CrcCompressed   uint64 `json:"crcCompressed"`
	Uncompressed    uint64 `json:"uncompressed"`
	LegacyS2        uint64 `json:"legacyS2"`
}

type jsonChunkType struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count uint64 `json:"count"`
	Bytes uint64 `json:"bytes"`
}

type jsonRatioBucket struct {
	Label string `json:"label"`
	Count uint64 `json:"count"`
}

type jsonBlock struct {
	Index      int           `json:"index"`
	ChunkType  string        `json:"chunkType"`
	CompSize   int           `json:"compSize"`
	UncompSize int           `json:"uncompSize"`
	RatioPct   float64       `json:"ratioPct"`
	Ops        uint64        `json:"ops"`
	OpCounts   []jsonOpCount `json:"opCounts"`
}

type jsonOpCount struct {
	Type  string `json:"type"`
	Count uint64 `json:"count"`
}

func buildJSONFile(rep *fileReport, opts statsOpts) jsonFile {
	jf := jsonFile{
		Name:      rep.Name,
		InputSize: rep.InputSize,
		Format:    rep.Format,
		Elapsed:   rep.Elapsed,
	}

	jf.Totals = jsonTotals{
		Ops:           rep.TotalOps,
		OutputBytes:   rep.TotalOut,
		EncodingBytes: rep.TotalEnc,
		LiteralBytes:  rep.TotalLitBytes,
		CopyBytes:     rep.TotalCopyBytes,
		RepeatBytes:   rep.TotalRepeatBytes,
		LongestCopy:   rep.LongestCopy,
		LongestLitRun: rep.LongestLitRun,
	}
	if rep.TotalOut > 0 {
		jf.Totals.BytesPerOutput = float64(rep.TotalEnc) / float64(rep.TotalOut)
		jf.Totals.LiteralPct = float64(rep.TotalLitBytes) * 100 / float64(rep.TotalOut)
		jf.Totals.CopyPct = float64(rep.TotalCopyBytes) * 100 / float64(rep.TotalOut)
		jf.Totals.RepeatPct = float64(rep.TotalRepeatBytes) * 100 / float64(rep.TotalOut)
	}
	if rep.TotalOps > 0 {
		jf.Totals.OutputPerOp = float64(rep.TotalOut) / float64(rep.TotalOps)
		if rep.TotalOut > 0 {
			jf.Totals.OpsPerKBOutput = float64(rep.TotalOps) * 1024 / float64(rep.TotalOut)
		}
	}

	for i := range numOpTypes {
		s := &rep.OpTotals[i]
		if s.Count == 0 {
			continue
		}
		enc := s.HeaderBytes
		if i == opLiteral || i == opFusedLit {
			enc += s.OutBytes
		}
		jo := jsonOp{
			Type:        opNames[i],
			Count:       s.Count,
			OutBytes:    s.OutBytes,
			EncBytes:    enc,
			HeaderBytes: s.HeaderBytes,
			MinLen:      s.MinLen,
			MaxLen:      s.MaxLen,
			AvgOut:      float64(s.OutBytes) / float64(s.Count),
			AvgEnc:      float64(enc) / float64(s.Count),
		}
		if s.SumLen > 0 {
			jo.AvgLen = float64(s.SumLen) / float64(s.Count)
		}
		if rep.TotalOps > 0 && i != opFusedLit {
			jo.PctOps = float64(s.Count) * 100 / float64(rep.TotalOps)
		}
		if rep.TotalOut > 0 {
			jo.PctOut = float64(s.OutBytes) * 100 / float64(rep.TotalOut)
		}
		if rep.TotalEnc > 0 {
			jo.PctEnc = float64(enc) * 100 / float64(rep.TotalEnc)
		}
		if i != opLiteral && i != opFusedLit {
			jo.Saved = int64(s.OutBytes) - int64(enc)
			jo.MinOff = s.MinOff
			jo.MaxOff = s.MaxOff
			if s.SumOff > 0 {
				jo.AvgOff = float64(s.SumOff) / float64(s.Count)
			}
		}
		if !opts.noHist {
			jo.LenHist = buildJSONBuckets(s.LenHist[:], s.Count)
			if i != opLiteral && i != opFusedLit {
				jo.OffHist = buildJSONBuckets(s.OffHist[:], s.Count)
			}
		}
		if i != opFusedLit {
			for n := 1; n <= 7; n++ {
				if s.HeaderSizeHist[n] > 0 {
					jo.HeaderSizeHist = append(jo.HeaderSizeHist, jsonHdrSize{Bytes: n, Count: s.HeaderSizeHist[n]})
				}
			}
		}
		jf.Ops = append(jf.Ops, jo)
	}

	for _, i := range []int{opCopy2F, opCopy3F} {
		s := &rep.OpTotals[i]
		if s.Count == 0 {
			continue
		}
		fb := jsonFusedBreakdown{
			Type:     opNames[i],
			Count:    s.Count,
			Bytes:    s.FusedLitBytes,
			AvgBytes: float64(s.FusedLitBytes) / float64(s.Count),
		}
		for n := 1; n <= 4; n++ {
			if s.FusedLenHist[n] == 0 {
				continue
			}
			fb.Lengths = append(fb.Lengths, jsonFusedLen{
				Length: n,
				Count:  s.FusedLenHist[n],
				Pct:    float64(s.FusedLenHist[n]) * 100 / float64(s.Count),
			})
		}
		jf.FusedLitBreakdown = append(jf.FusedLitBreakdown, fb)
	}

	if rep.OpTotals[opRepeat].Count > 0 {
		var total uint64
		for i := range numOpTypes {
			total += rep.RepeatPrev[i]
		}
		for i := range numOpTypes {
			c := rep.RepeatPrev[i]
			if c == 0 {
				continue
			}
			var pct float64
			if total > 0 {
				pct = float64(c) * 100 / float64(total)
			}
			jf.RepeatLineage = append(jf.RepeatLineage, jsonRepeatLineage{
				PrevType: opNames[i],
				Count:    c,
				Pct:      pct,
			})
		}
	}

	if rep.Format == "stream" {
		js := &jsonStream{
			DeclaredMaxBlockSize: rep.Stream.DeclaredMaxBlk,
			StreamIdentifiers:    rep.Stream.StreamIDCount,
			Blocks: jsonStreamBlocks{
				Minlz:           rep.Stream.MinlzBlocks,
				CrcUncompressed: rep.Stream.CrcMode02,
				CrcCompressed:   rep.Stream.CrcMode03,
				Uncompressed:    rep.Stream.UncompBlocks,
				LegacyS2:        rep.Stream.LegacyBlocks,
			},
		}
		for i := range 256 {
			if rep.Stream.ChunkCount[i] == 0 {
				continue
			}
			js.ChunkTypes = append(js.ChunkTypes, jsonChunkType{
				ID:    fmt.Sprintf("0x%02x", i),
				Name:  chunkTypeName(byte(i)),
				Count: rep.Stream.ChunkCount[i],
				Bytes: rep.Stream.ChunkBytes[i],
			})
		}
		if !opts.noHist {
			js.CompBlockSizeHist = buildJSONBuckets(rep.Stream.BlockSizeHist[:], sumU64(rep.Stream.BlockSizeHist[:]))
			js.UncompBlockSizeHist = buildJSONBuckets(rep.Stream.UncompSizeHist[:], sumU64(rep.Stream.UncompSizeHist[:]))
			ratioLabels := []string{"0-10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%", ">=100%"}
			for i, l := range ratioLabels {
				if rep.Stream.RatioBuckets[i] == 0 {
					continue
				}
				js.RatioHist = append(js.RatioHist, jsonRatioBucket{Label: l, Count: rep.Stream.RatioBuckets[i]})
			}
		}
		jf.Stream = js
	}

	if opts.perBlock {
		for bi := range rep.Blocks {
			b := &rep.Blocks[bi]
			jb := jsonBlock{
				Index:      b.Index,
				ChunkType:  fmt.Sprintf("0x%02x", b.ChunkType),
				CompSize:   b.CompSize,
				UncompSize: b.UncompSize,
				Ops:        b.totalOps(),
			}
			if b.UncompSize > 0 {
				jb.RatioPct = float64(b.CompSize) * 100 / float64(b.UncompSize)
			}
			for i := range numOpTypes {
				if b.Ops[i].Count == 0 {
					continue
				}
				jb.OpCounts = append(jb.OpCounts, jsonOpCount{
					Type:  opNames[i],
					Count: b.Ops[i].Count,
				})
			}
			jf.Blocks = append(jf.Blocks, jb)
		}
	}

	return jf
}

func buildJSONBuckets(h []uint64, total uint64) []jsonBucket {
	var out []jsonBucket
	for i, c := range h {
		if c == 0 {
			continue
		}
		var pct float64
		if total > 0 {
			pct = float64(c) * 100 / float64(total)
		}
		var lo, hi uint64
		switch {
		case i == 0:
			lo, hi = 1, 1
		case i >= numHistBuckets-1:
			lo = uint64(1) << i
			// hi left 0 = open-ended
		default:
			lo = uint64(1) << i
			hi = (uint64(1) << (i + 1)) - 1
		}
		out = append(out, jsonBucket{
			Label: bucketLabel(i),
			Lo:    lo,
			Hi:    hi,
			Count: c,
			Pct:   pct,
		})
	}
	return out
}

// ============================================================================
// Renderers
// ============================================================================

func renderText(w io.Writer, rep *fileReport, opts statsOpts) {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	if !opts.quiet {
		fmt.Fprintf(bw, "\n========== %s ==========\n", rep.Name)
		fmt.Fprintf(bw, "Input size:  %s (%d bytes)\n", humanSize(rep.InputSize), rep.InputSize)
		fmt.Fprintf(bw, "Format:      %s\n", rep.Format)
		fmt.Fprintf(bw, "Elapsed:     %s\n", rep.Elapsed)
	}

	if rep.Format == "stream" {
		renderStreamSummary(bw, &rep.Stream)
	}

	// Aggregate compression
	fmt.Fprintln(bw, "\n--- Aggregate ---")
	fmt.Fprintf(bw, "Total ops:        %d\n", rep.TotalOps)
	fmt.Fprintf(bw, "Output bytes:     %s (%d)\n", humanSize(int64(rep.TotalOut)), rep.TotalOut)
	fmt.Fprintf(bw, "Encoding bytes:   %s (%d)  (headers + literal bodies + fused lits)\n", humanSize(int64(rep.TotalEnc)), rep.TotalEnc)
	if rep.TotalOut > 0 {
		fmt.Fprintf(bw, "Bytes/output:     %.4f  (lower = better)\n", float64(rep.TotalEnc)/float64(rep.TotalOut))
		fmt.Fprintf(bw, "Output/op:        %.2f bytes/op\n", float64(rep.TotalOut)/float64(rep.TotalOps))
		fmt.Fprintf(bw, "Ops/KB out:       %.2f\n", float64(rep.TotalOps)*1024/float64(rep.TotalOut))
		fmt.Fprintf(bw, "Literal bytes:    %s  (%.1f%% of output)\n", humanSize(int64(rep.TotalLitBytes)), float64(rep.TotalLitBytes)*100/float64(rep.TotalOut))
		fmt.Fprintf(bw, "Copy bytes:       %s  (%.1f%%)\n", humanSize(int64(rep.TotalCopyBytes)), float64(rep.TotalCopyBytes)*100/float64(rep.TotalOut))
		fmt.Fprintf(bw, "Repeat bytes:     %s  (%.1f%%)\n", humanSize(int64(rep.TotalRepeatBytes)), float64(rep.TotalRepeatBytes)*100/float64(rep.TotalOut))
	}
	fmt.Fprintf(bw, "Longest copy:     %d bytes\n", rep.LongestCopy)
	fmt.Fprintf(bw, "Longest lit run:  %d bytes\n", rep.LongestLitRun)

	// Per-op table
	fmt.Fprintln(bw, "\n--- Per-op breakdown ---")
	fmt.Fprintf(bw, "%-8s %10s %6s %12s %6s %12s %6s %8s %8s %12s\n",
		"Type", "Count", "%Ops", "OutBytes", "%Out", "EncBytes", "%Enc", "AvgOut", "AvgEnc", "Saved")
	for i := range numOpTypes {
		s := &rep.OpTotals[i]
		if s.Count == 0 {
			continue
		}
		pctOut := float64(s.OutBytes) * 100 / float64(rep.TotalOut)
		encTotal := s.HeaderBytes
		if i == opLiteral || i == opFusedLit {
			encTotal += s.OutBytes
		}
		pctEnc := float64(encTotal) * 100 / float64(rep.TotalEnc)
		avgOut := float64(s.OutBytes) / float64(s.Count)
		avgEnc := float64(encTotal) / float64(s.Count)
		var saved int64
		if i != opLiteral && i != opFusedLit {
			saved = int64(s.OutBytes) - int64(encTotal)
		}
		// FusedLit is a pseudo-op derived from Copy2-F/Copy3-F counts and is
		// excluded from TotalOps, so a %Ops figure would inflate the column past 100%.
		pctOpsStr := "      —"
		if i != opFusedLit {
			pctOpsStr = fmt.Sprintf("%5.1f%% ", float64(s.Count)*100/float64(rep.TotalOps))
		}
		fmt.Fprintf(bw, "%-8s %10d %s %12d %5.1f%% %12d %5.1f%% %8.2f %8.2f %12d\n",
			opNames[i], s.Count, pctOpsStr, s.OutBytes, pctOut, encTotal, pctEnc, avgOut, avgEnc, saved)
	}

	// Fused stats
	if rep.OpTotals[opCopy2F].Count > 0 || rep.OpTotals[opCopy3F].Count > 0 {
		fmt.Fprintln(bw, "\n--- Fused literal stats ---")
		for _, i := range []int{opCopy2F, opCopy3F} {
			s := &rep.OpTotals[i]
			if s.Count == 0 {
				continue
			}
			fmt.Fprintf(bw, "%s: %d ops, %d fused-lit bytes (%.2f avg/op)\n",
				opNames[i], s.Count, s.FusedLitBytes, float64(s.FusedLitBytes)/float64(s.Count))
			for n := 0; n <= 4; n++ {
				if s.FusedLenHist[n] == 0 {
					continue
				}
				fmt.Fprintf(bw, "    lit=%d: %d (%.1f%%)\n", n, s.FusedLenHist[n], float64(s.FusedLenHist[n])*100/float64(s.Count))
			}
		}
	}

	// Header-size dist
	fmt.Fprintln(bw, "\n--- Header-size distribution (bytes/op) ---")
	fmt.Fprintf(bw, "%-8s", "Type")
	for n := 1; n <= 7; n++ {
		fmt.Fprintf(bw, " %8d", n)
	}
	fmt.Fprintln(bw)
	for i := range numOpTypes {
		if i == opFusedLit {
			continue // FusedLit has no header bytes
		}
		s := &rep.OpTotals[i]
		if s.Count == 0 {
			continue
		}
		fmt.Fprintf(bw, "%-8s", opNames[i])
		for n := 1; n <= 7; n++ {
			fmt.Fprintf(bw, " %8d", s.HeaderSizeHist[n])
		}
		fmt.Fprintln(bw)
	}

	// Repeat lineage
	if rep.OpTotals[opRepeat].Count > 0 {
		fmt.Fprintln(bw, "\n--- Repeat lineage (op preceding each repeat) ---")
		total := uint64(0)
		for i := range numOpTypes {
			total += rep.RepeatPrev[i]
		}
		for i := range numOpTypes {
			c := rep.RepeatPrev[i]
			if c == 0 {
				continue
			}
			fmt.Fprintf(bw, "%-8s %10d  %5.1f%%\n", opNames[i], c, float64(c)*100/float64(total))
		}
	}

	if !opts.noHist {
		// Length histograms
		fmt.Fprintln(bw, "\n--- Length histograms (log2 buckets) ---")
		for i := range numOpTypes {
			s := &rep.OpTotals[i]
			if s.Count == 0 {
				continue
			}
			fmt.Fprintf(bw, "\n%s: count=%d  min=%d max=%d avg=%.2f\n", opNames[i], s.Count, s.MinLen, s.MaxLen, float64(s.SumLen)/float64(s.Count))
			renderHist(bw, s.LenHist[:], s.Count)
		}

		// Offset histograms
		fmt.Fprintln(bw, "\n--- Offset histograms (log2 buckets) ---")
		for i := range numOpTypes {
			if i == opLiteral || i == opFusedLit {
				continue
			}
			s := &rep.OpTotals[i]
			if s.Count == 0 {
				continue
			}
			fmt.Fprintf(bw, "\n%s: count=%d  min=%d max=%d avg=%.2f\n", opNames[i], s.Count, s.MinOff, s.MaxOff, float64(s.SumOff)/float64(s.Count))
			renderHist(bw, s.OffHist[:], s.Count)
		}
	}

	if rep.Format == "stream" && !opts.noHist {
		fmt.Fprintln(bw, "\n--- Block size histogram (compressed chunk bytes) ---")
		renderHist(bw, rep.Stream.BlockSizeHist[:], sumU64(rep.Stream.BlockSizeHist[:]))
		fmt.Fprintln(bw, "\n--- Block size histogram (uncompressed bytes) ---")
		renderHist(bw, rep.Stream.UncompSizeHist[:], sumU64(rep.Stream.UncompSizeHist[:]))
		fmt.Fprintln(bw, "\n--- Per-block ratio buckets (comp/uncomp) ---")
		ratioLabels := []string{"0-10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%", ">=100%"}
		total := sumU64(rep.Stream.RatioBuckets[:])
		for i, l := range ratioLabels {
			c := rep.Stream.RatioBuckets[i]
			if c == 0 {
				continue
			}
			fmt.Fprintf(bw, "  %-8s %8d  %s\n", l, c, bar(c, total, 40))
		}
	}

	if opts.perBlock && len(rep.Blocks) > 0 {
		fmt.Fprintln(bw, "\n--- Per-block summary ---")
		fmt.Fprintf(bw, "%-6s %-6s %10s %10s %7s %8s   %s\n",
			"idx", "ct", "comp", "uncomp", "ratio", "ops", "L/R/C1/C2/C2F/C3/C3F/FL")
		for _, b := range rep.Blocks {
			ratio := 0.0
			if b.UncompSize > 0 {
				ratio = float64(b.CompSize) * 100 / float64(b.UncompSize)
			}
			counts := make([]string, numOpTypes)
			for i := range b.Ops {
				counts[i] = strconv.FormatUint(b.Ops[i].Count, 10)
			}
			fmt.Fprintf(bw, "%-6d 0x%02x   %10d %10d %6.1f%% %8d   %s\n",
				b.Index, b.ChunkType, b.CompSize, b.UncompSize, ratio, b.totalOps(),
				strings.Join(counts, "/"))
		}
	}
}

func renderStreamSummary(bw *bufio.Writer, ss *streamStats) {
	fmt.Fprintln(bw, "\n--- Stream ---")
	fmt.Fprintf(bw, "Declared max block size: %s\n", humanSize(int64(ss.DeclaredMaxBlk)))
	fmt.Fprintf(bw, "Stream identifiers seen: %d\n", ss.StreamIDCount)
	fmt.Fprintf(bw, "MinLZ blocks:            %d  (0x02 CRC-uncomp: %d, 0x03 CRC-comp: %d)\n",
		ss.MinlzBlocks, ss.CrcMode02, ss.CrcMode03)
	fmt.Fprintf(bw, "Uncompressed blocks:     %d\n", ss.UncompBlocks)
	if ss.LegacyBlocks > 0 {
		fmt.Fprintf(bw, "Legacy S2 blocks:        %d\n", ss.LegacyBlocks)
	}

	fmt.Fprintln(bw, "\nChunk type breakdown:")
	type ctRow struct{ id, count, bytes uint64 }
	var rows []ctRow
	for i := range 256 {
		if ss.ChunkCount[i] == 0 {
			continue
		}
		rows = append(rows, ctRow{uint64(i), ss.ChunkCount[i], ss.ChunkBytes[i]})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	fmt.Fprintf(bw, "  %-4s %-22s %10s %14s\n", "id", "name", "count", "bytes")
	for _, r := range rows {
		fmt.Fprintf(bw, "  0x%02x %-22s %10d %14d\n", r.id, chunkTypeName(byte(r.id)), r.count, r.bytes)
	}
}

func renderHist(bw *bufio.Writer, h []uint64, total uint64) {
	if total == 0 {
		return
	}
	var maxV uint64
	for _, v := range h {
		if v > maxV {
			maxV = v
		}
	}
	for i, c := range h {
		if c == 0 {
			continue
		}
		fmt.Fprintf(bw, "  %-12s %10d  %s  %.1f%%\n", bucketLabel(i), c, bar(c, maxV, 40), float64(c)*100/float64(total))
	}
}

func bar(v, max uint64, width int) string {
	if max == 0 {
		return ""
	}
	n := int(uint64(width) * v / max)
	if n < 1 && v > 0 {
		n = 1
	}
	return strings.Repeat("█", n)
}

func sumU64(h []uint64) uint64 {
	var n uint64
	for _, v := range h {
		n += v
	}
	return n
}

func humanSize(b int64) string {
	if b < 0 {
		return "?"
	}
	const k = 1024.0
	v := float64(b)
	switch {
	case v < k:
		return fmt.Sprintf("%d B", b)
	case v < k*k:
		return fmt.Sprintf("%.2f KiB", v/k)
	case v < k*k*k:
		return fmt.Sprintf("%.2f MiB", v/k/k)
	default:
		return fmt.Sprintf("%.2f GiB", v/k/k/k)
	}
}

func chunkTypeName(b byte) string {
	switch b {
	case chunkLegacy:
		return "legacy-comp(s2)"
	case chunkUncompressed:
		return "uncompressed"
	case chunkMinLZ:
		return "minlz(crc-unc)"
	case chunkMinLZCompCRC:
		return "minlz(crc-cmp)"
	case chunkEOF:
		return "eof"
	case chunkIndex:
		return "index"
	case chunkSearchInfo:
		return "search-info"
	case chunkSearchTable:
		return "search-table"
	case chunkPadding:
		return "padding"
	case chunkStreamID:
		return "stream-id"
	}
	switch {
	case b >= chunkUserSkipLo && b <= chunkUserSkipHi:
		return "user-skippable"
	case b >= chunkUserNoSkipLo && b <= chunkUserNoSkipHi:
		return "user-non-skip"
	case b <= maxNonSkippableChk:
		return "reserved-non-skip"
	}
	return "reserved-skippable"
}

// ============================================================================
// CSV renderer
// ============================================================================

func renderCSV(out io.Writer, reps []fileReport, opts statsOpts) error {
	bw := bufio.NewWriter(out)
	defer bw.Flush()
	w := csv.NewWriter(bw)
	defer w.Flush()

	for _, rep := range reps {
		w.Write([]string{"# file", rep.Name, "format", rep.Format, "inputSize", strconv.FormatInt(rep.InputSize, 10)})
		w.Write([]string{"# section", "per-op"})
		w.Write([]string{"type", "count", "outBytes", "headerBytes", "fusedLitBytes", "fusedOps", "minLen", "maxLen", "sumLen", "minOff", "maxOff", "sumOff"})
		for i := range numOpTypes {
			s := &rep.OpTotals[i]
			w.Write([]string{
				opNames[i],
				strconv.FormatUint(s.Count, 10),
				strconv.FormatUint(s.OutBytes, 10),
				strconv.FormatUint(s.HeaderBytes, 10),
				strconv.FormatUint(s.FusedLitBytes, 10),
				strconv.FormatUint(s.FusedOpCount, 10),
				strconv.FormatUint(uint64(s.MinLen), 10),
				strconv.FormatUint(uint64(s.MaxLen), 10),
				strconv.FormatUint(s.SumLen, 10),
				strconv.FormatUint(uint64(s.MinOff), 10),
				strconv.FormatUint(uint64(s.MaxOff), 10),
				strconv.FormatUint(s.SumOff, 10),
			})
		}

		w.Write([]string{"# section", "len-hist"})
		hdr := []string{"type"}
		for i := range numHistBuckets {
			hdr = append(hdr, bucketLabel(i))
		}
		w.Write(hdr)
		for i := range numOpTypes {
			s := &rep.OpTotals[i]
			row := []string{opNames[i]}
			for _, v := range s.LenHist {
				row = append(row, strconv.FormatUint(v, 10))
			}
			w.Write(row)
		}

		w.Write([]string{"# section", "off-hist"})
		w.Write(hdr)
		for i := range numOpTypes {
			s := &rep.OpTotals[i]
			row := []string{opNames[i]}
			for _, v := range s.OffHist {
				row = append(row, strconv.FormatUint(v, 10))
			}
			w.Write(row)
		}

		w.Write([]string{"# section", "header-size-hist"})
		hdrSz := []string{"type", "1", "2", "3", "4", "5", "6", "7"}
		w.Write(hdrSz)
		for i := range numOpTypes {
			s := &rep.OpTotals[i]
			row := []string{opNames[i]}
			for n := 1; n <= 7; n++ {
				row = append(row, strconv.FormatUint(s.HeaderSizeHist[n], 10))
			}
			w.Write(row)
		}

		if rep.Format == "stream" {
			w.Write([]string{"# section", "chunk-types"})
			w.Write([]string{"id", "name", "count", "bytes"})
			for i := range 256 {
				if rep.Stream.ChunkCount[i] == 0 {
					continue
				}
				w.Write([]string{
					fmt.Sprintf("0x%02x", i),
					chunkTypeName(byte(i)),
					strconv.FormatUint(rep.Stream.ChunkCount[i], 10),
					strconv.FormatUint(rep.Stream.ChunkBytes[i], 10),
				})
			}
			w.Write([]string{"# section", "block-size-hist"})
			w.Write(hdr)
			row := []string{"compressed"}
			for _, v := range rep.Stream.BlockSizeHist {
				row = append(row, strconv.FormatUint(v, 10))
			}
			w.Write(row)
			row = []string{"uncompressed"}
			for _, v := range rep.Stream.UncompSizeHist {
				row = append(row, strconv.FormatUint(v, 10))
			}
			w.Write(row)
		}

		if opts.perBlock {
			w.Write([]string{"# section", "per-block"})
			w.Write([]string{"index", "chunkType", "compSize", "uncompSize", "ops", "lit", "rep", "c1", "c2", "c2f", "c3", "c3f", "fl"})
			for _, b := range rep.Blocks {
				row := []string{
					strconv.Itoa(b.Index),
					fmt.Sprintf("0x%02x", b.ChunkType),
					strconv.Itoa(b.CompSize),
					strconv.Itoa(b.UncompSize),
					strconv.FormatUint(b.totalOps(), 10),
				}
				for i := range numOpTypes {
					row = append(row, strconv.FormatUint(b.Ops[i].Count, 10))
				}
				w.Write(row)
			}
		}
		w.Write([]string{}) // blank line between files
	}
	return w.Error()
}
