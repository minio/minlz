// Copyright 2025 MinIO Inc.
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
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minlz"
	"github.com/minio/minlz/cmd/internal/filepathx"
	"github.com/minio/minlz/cmd/internal/readahead"
)

const maxBlockSize = 8 << 20

func mainDecompress(args []string, cat, tail bool) {
	defTail := ""
	if tail {
		defTail = "1KB+nl"
	}
	var (
		fs = flag.NewFlagSet("decompress", flag.ExitOnError)

		tailString  = fs.String("tail", defTail, "Return last of compressed file. Examples: 92, 64K, 256K, 1M, 4M. Requires Index")
		limitString = fs.String("limit", "", "Return at most this much data. Examples: 92, 64K, 256K, 1M, 4M")

		// Shared
		block      = fs.Bool("block", false, "Decompress single block. Will load content into memory. Max 8MB")
		blockDebug = fs.Bool("block-debug", false, "Print block encoding")
		safe       = fs.Bool("safe", false, "Do not overwrite output files")
		cpu        = fs.Int("cpu", runtime.GOMAXPROCS(0), "Maximum number of threads to use")
		stdout     = fs.Bool("c", cat || tail, "Write all output to stdout. Multiple input files will be concatenated")
		out        = fs.String("o", "", "Write output to another file. Single input file only")
		remove     = fs.Bool("rm", false, "Delete source file(s) after success")
		quiet      = fs.Bool("q", false, "Don't write any output to terminal, except errors")
		help       = fs.Bool("help", false, "Display help")
		verify     = fs.Bool("verify", false, "Verify files, but do not write output")
	)

	var offsetString *string
	if !tail {
		offsetString = fs.String("offset", "", "Start at offset. Examples: 92, 64K, 256K, 1M, 4M. Requires Index")
	} else {
		var s string
		offsetString = &s
	}
	var bench *int
	if cat || tail {
		var i int
		bench = &i
	} else {
		bench = fs.Int("bench", 0, "Run benchmark n times. No output will be written")
	}

	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintln(w, `Decompresses all files supplied as input. Input files must end with '`+minlzExt+`', '`+s2Ext+`' or '`+snappyExt+`'.
Output file names have the extension removed. By default output files will be overwritten.
Use - as the only file name to read from stdin and write to stdout.

Wildcards are accepted: testdir/*.txt will decompress all files in testdir ending with .txt
Directories can be wildcards as well. testdir/*/*.txt will match testdir/subdir/b.txt

File names beginning with 'http://' and 'https://' will be downloaded and decompressed.
Extensions on downloaded files are ignored. Only http response code 200 is accepted.

Options:`)
		fs.PrintDefaults()
		fmt.Fprintf(w, "\nUsage: %v %s [options] <input>\n", os.Args[0], os.Args[1])
	}
	fs.Parse(args)

	args = fs.Args()
	if len(args) == 0 || *help {
		fs.Usage()
		os.Exit(1)
	}
	var tailNextNL bool
	if c, ok := strings.CutSuffix(*tailString, "+nl"); ok {
		tailNextNL = true
		*tailString = c
	}
	tailBytes, err := toSize(*tailString)
	exitErr(err)
	if c, ok := strings.CutSuffix(*offsetString, "+nl"); ok {
		tailNextNL = true
		*offsetString = c
	}
	offset, err := toSize(*offsetString)
	exitErr(err)
	if tailBytes > 0 && offset > 0 {
		exitErr(errors.New("--offset and --tail cannot be used together"))
	}
	limitBytes := int64(-1)
	var limitNextNL bool
	if c, ok := strings.CutSuffix(*limitString, "+nl"); ok {
		limitNextNL = true
		*limitString = c
	}

	if *limitString != "" {
		limitBytes, err = toSize(*limitString)
		exitErr(err)
	}

	*block = *block || *blockDebug
	r := minlz.NewReader(nil, minlz.ReaderFallback(true))

	if len(args) == 1 && args[0] == "-" {
		if limitBytes > 0 || offset > 0 || tailBytes > 0 {
			exitErr(errors.New("--offset, --tail and --limit cannot be used with stdin"))
		}
		if *block {
			all, err := io.ReadAll(io.LimitReader(os.Stdin, int64(minlz.MaxEncodedLen(minlz.MaxBlockSize))))
			exitErr(err)

			if *blockDebug {
				DecodeDebug(nil, all)
				os.Exit(0)
			}
			b, err := minlz.Decode(nil, all)
			exitErr(err)
			_, err = os.Stdout.Write(b)
			exitErr(err)
			os.Exit(0)
		}
		r.Reset(os.Stdin)
		if *verify {
			_, err := io.Copy(io.Discard, r)
			exitErr(err)
			return
		}
		if *out == "" {
			_, err := io.Copy(os.Stdout, r)
			exitErr(err)
			return
		}
		dstFilename := *out
		if *safe {
			_, err := os.Stat(dstFilename)
			if !os.IsNotExist(err) {
				exitErr(errors.New("destination files exists"))
			}
		}
		dstFile, err := os.OpenFile(dstFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		exitErr(err)
		defer dstFile.Close()
		bw := bufio.NewWriterSize(dstFile, 4<<20)
		defer bw.Flush()
		_, err = io.Copy(bw, r)
		exitErr(err)
		return
	}
	var files []string

	for _, pattern := range args {
		if isHTTP(pattern) {
			files = append(files, pattern)
			continue
		}

		found, err := filepathx.Glob(pattern)
		exitErr(err)
		if len(found) == 0 {
			exitErr(fmt.Errorf("unable to find file %v", pattern))
		}
		files = append(files, found...)
	}

	*quiet = *quiet || *stdout

	if *bench > 0 {
		if limitBytes > 0 || offset > 0 || tailBytes > 0 {
			exitErr(errors.New("--offset, --tail and --limit cannot be used with benchmarks"))
		}
		benchDecompOnly(files, block, quiet, bench, cpu, r)
		return
	}

	if *out != "" && len(files) > 1 {
		exitErr(errors.New("-out parameter can only be used with one input"))
	}

	for _, filename := range files {
		dstFilename := cleanFileName(filename)
		block := *block
		if strings.HasSuffix(dstFilename, minlzBlockExt) {
			dstFilename = strings.TrimSuffix(dstFilename, minlzBlockExt)
			block = true
		}

		switch {
		case *out != "":
			dstFilename = *out
		case block:
		case strings.HasSuffix(dstFilename, minlzExt):
			dstFilename = strings.TrimSuffix(dstFilename, minlzExt)
		case strings.HasSuffix(dstFilename, s2Ext):
			dstFilename = strings.TrimSuffix(dstFilename, s2Ext)
		case strings.HasSuffix(dstFilename, snappyExt):
			dstFilename = strings.TrimSuffix(dstFilename, snappyExt)
		case strings.HasSuffix(dstFilename, ".snappy"):
			dstFilename = strings.TrimSuffix(dstFilename, ".snappy")
		default:
			if !isHTTP(filename) {
				fmt.Println("Skipping", filename)
				continue
			}
		}
		if offset > 0 || tailBytes > 0 || limitBytes > 0 {
			dstFilename += ".part"
		}
		if *verify {
			dstFilename = "(verify)"
		}
		decompressFile(quiet, filename, dstFilename, block, tailBytes, offset, safe, verify, stdout, blockDebug, tailNextNL, r, limitBytes, limitNextNL, cpu, remove)
	}
}

func decompressFile(quiet *bool, filename string, dstFilename string, block bool, tailBytes int64, offset int64, safe *bool, verify *bool, stdout *bool, blockDebug *bool, tailNextNL bool, r *minlz.Reader, limitBytes int64, limitNextNL bool, cpu *int, remove *bool) {
	var closeOnce sync.Once
	if !*quiet {
		fmt.Print("Decompressing ", filename, " -> ", dstFilename)
	}
	seeker := !block && (tailBytes > 0 || offset > 0)
	// Input file.
	file, _, mode := openFile(filename, seeker)
	defer closeOnce.Do(func() { file.Close() })
	var rc interface {
		io.Reader
		BytesRead() int64
	}
	if seeker {
		rs, ok := file.(io.ReadSeeker)
		if !ok && tailBytes > 0 {
			exitErr(errors.New("cannot tail with non-seekable input"))
		}
		if ok {
			rc = &rCountSeeker{in: rs}
		} else {
			rc = &rCounter{in: file}
		}
	} else {
		rc = &rCounter{in: file}
	}
	var src io.Reader
	if !seeker {
		ra, err := readahead.NewReaderSize(rc, 2, 8<<20)
		exitErr(err)
		defer ra.Close()
		src = ra
	} else {
		src = rc
	}
	if *safe {
		_, err := os.Stat(dstFilename)
		if !os.IsNotExist(err) {
			exitErr(errors.New("destination files exists"))
		}
	}
	var out io.Writer
	switch {
	case *verify:
		out = io.Discard
	case *stdout:
		out = os.Stdout
	default:
		dstFile, err := os.OpenFile(dstFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		exitErr(err)
		defer dstFile.Close()
		out = dstFile
		if !block {
			bw := bufio.NewWriterSize(dstFile, 4<<20)
			defer bw.Flush()
			out = bw
		}
	}
	var decoded io.Reader
	start := time.Now()
	if block {
		all, err := io.ReadAll(src)
		exitErr(err)
		if *blockDebug {
			DecodeDebug(nil, all)
		}
		b, err := minlz.Decode(nil, all)
		if offset > 0 {
			b = b[min(offset, int64(len(all))):]
		}
		if tailBytes > 0 {
			b = b[len(b)-min(len(b), int(tailBytes)):]
			for len(b) > 0 && tailNextNL {
				if b[0] == '\n' {
					b = b[1:]
					break
				}
				b = b[1:]
			}
		}
		// Limit applied later...
		exitErr(err)
		decoded = bytes.NewReader(b)
	} else {
		r.Reset(src)
		if tailBytes > 0 || offset > 0 {
			rs, err := r.ReadSeeker(nil)
			exitErr(err)
		retry:
			if tailBytes > 0 {
				_, err = rs.Seek(-tailBytes, io.SeekEnd)
			} else {
				_, err = rs.Seek(offset, io.SeekStart)
			}
			exitErr(err)
			if tailNextNL {
				for {
					v, err := r.ReadByte()
					if err == io.EOF {
						tailNextNL = false
						goto retry
					}
					if v == '\n' {
						break
					}
					if err != nil {
						exitErr(err)
					}
				}
			}
		}
		decoded = r
	}
	if limitBytes > 0 {
		decoded = limitReaderNL(decoded, limitBytes, limitNextNL)
	}
	var err error
	var output int64
	if dec, ok := decoded.(*minlz.Reader); ok && tailBytes == 0 && offset == 0 {
		output, err = dec.DecodeConcurrent(out, *cpu)
	} else {
		output, err = io.Copy(out, decoded)
	}
	exitErr(err)
	if !*quiet {
		elapsed := time.Since(start)
		mbPerSec := (float64(output) / 1e6) / (float64(elapsed) / (float64(time.Second)))
		pct := float64(output) * 100 / float64(rc.BytesRead())
		fmt.Printf(" %d -> %d [%.02f%%]; %.01fMB/s\n", rc.BytesRead(), output, pct, mbPerSec)
	}
	if *remove && !*verify {
		closeOnce.Do(func() {
			file.Close()
			if !*quiet {
				fmt.Println("Removing", filename)
			}
			err := os.Remove(filename)
			exitErr(err)
		})
	}
}

func benchDecompOnly(files []string, block *bool, quiet *bool, bench *int, cpu *int, r *minlz.Reader) {
	debug.SetGCPercent(10)
	for _, filename := range files {
		block := *block
		dstFilename := cleanFileName(filename)
		switch {
		case strings.HasSuffix(filename, minlzBlockExt):
			dstFilename = strings.TrimSuffix(dstFilename, minlzBlockExt)
			block = true
		case strings.HasSuffix(dstFilename, minlzExt):
		case strings.HasSuffix(dstFilename, s2Ext):
		case strings.HasSuffix(dstFilename, snappyExt):
		case strings.HasSuffix(dstFilename, ".snappy"):
		default:
			if !isHTTP(filename) {
				fmt.Println("Skipping", filename)
				continue
			}
		}

		if !*quiet {
			fmt.Print("Reading ", filename, "...")
		}
		// Input file.
		file, size, _ := openFile(filename, false)
		compressed := make([]byte, size)
		_, err := io.ReadFull(file, compressed)
		exitErr(err)
		file.Close()

		if block {
			decompresBlockBench(quiet, bench, compressed, cpu)
			continue
		}

		for i := 0; i < *bench; i++ {
			if !*quiet {
				fmt.Print("\nDecompressing...")
			}
			runtime.GC()
			start := time.Now()
			var output int64
			r.Reset(bytes.NewBuffer(compressed))
			output, err = r.DecodeConcurrent(io.Discard, *cpu)
			exitErr(err)
			if !*quiet {
				elapsed := time.Since(start)
				ms := elapsed.Round(time.Millisecond)
				mbPerSec := (float64(output) / 1e6) / (float64(elapsed) / (float64(time.Second)))
				pct := float64(output) * 100 / float64(len(compressed))
				fmt.Printf(" %d -> %d [%.02f%%]; %v, %.01fMB/s", len(compressed), output, pct, ms, mbPerSec)
			}
		}
		if !*quiet {
			fmt.Println("")
		}
	}
}

func decompresBlockBench(quiet *bool, bench *int, compressed []byte, cpu *int) {
	if !*quiet {
		fmt.Print("\n\nDecompressing Block using 1 thread...\n")
	}
	var singleSpeed float64
	start := time.Now()
	end := time.Now().Add(time.Duration(*bench) * time.Second)
	lastUpdate := start
	dSize, err := minlz.DecodedLen(compressed)
	exitErr(err)
	decomp := make([]byte, 0, dSize)
	n := 0
	runtime.GC()
	for time.Now().Before(end) {
		decomp, err = minlz.Decode(decomp, compressed)
		exitErr(err)
		if len(decomp) != dSize {
			exitErr(fmt.Errorf("unexpected size, want %d, got %d", dSize, len(decomp)))
		}
		n++
		if !*quiet && time.Since(lastUpdate) > time.Second/6 {
			input := float64(dSize) * float64(n)
			output := float64(len(compressed)) * float64(n)
			elapsed := time.Since(start)
			singleSpeed = (input / 1e6) / (float64(elapsed) / (float64(time.Second)))
			pct := input * 100 / output
			ms := elapsed.Round(time.Millisecond)
			fmt.Printf(" * %d -> %d bytes [%.02f%%]; %v, %.01fMB/s                  \r", len(compressed), len(decomp), pct, ms, singleSpeed)
			lastUpdate = time.Now()
		}
	}
	if *cpu > 1 {
		if !*quiet {
			fmt.Printf("\n\nDecompressing block (%d threads)...\n", *cpu)
		}
		b := decomp
		dsts := make([]byte, 0, *cpu*len(b))

		var n atomic.Int64
		var wg sync.WaitGroup
		start := time.Now()
		end := time.Now().Add(time.Duration(*bench) * time.Second)
		wg.Add(*cpu)
		for i := 0; i < *cpu; i++ {
			go func(i int) {
				decomp := dsts[i*len(b) : i*len(b)+len(b) : i*len(b)+len(b)]
				defer wg.Done()
				for time.Now().Before(end) {
					decomp, err = minlz.Decode(decomp, compressed)
					exitErr(err)
					if len(decomp) != len(b) {
						exitErr(fmt.Errorf("unexpected size, want %d, got %d", len(b), len(decomp)))
					}
					n.Add(1)
				}
			}(i)
		}
		for !*quiet && time.Now().Before(end) {
			input := float64(len(b)) * float64(n.Load())
			output := float64(len(compressed)) * float64(n.Load())
			elapsed := time.Since(start)
			mbpersec := (input / 1e6) / (float64(elapsed) / (float64(time.Second)))
			scale := mbpersec / singleSpeed
			pct := output * 100 / input
			ms := elapsed.Round(time.Millisecond)
			fmt.Printf(" * %d -> %d bytes [%.02f%%]; %v, %.01fMB/s (%.1fx)          \r", len(compressed), len(decomp), pct, ms, mbpersec, scale)
			time.Sleep(time.Second / 6)
		}
		wg.Wait()
		runtime.GC()
	}
	fmt.Println("")
}

const (
	tagLiteral = 0x00
	tagCopy1   = 0x01
	tagCopy2   = 0x02
	tagCopy3   = 0x03
	tagRepeat  = tagCopy3 | 4

	decodeErrCodeCorrupt = 1

	copyLitBits = 2
)

// IsMinLZ returns true if the block is a minlz block.
func isMinLZ(src []byte) (ok, literals bool, block []byte, size int, err error) {
	if len(src) <= 1 {
		if len(src) == 0 {
			err = errors.New("corrupt length")
			return
		}
		if src[0] == 0 {
			// Size 0 block. Could be MinLZ.
			return true, true, src[1:], 0, nil
		}
	}
	if src[0] != 0 {
		// Older...
		v, _, err := decodedLen(src)
		return false, false, src, v, err
	}
	src = src[1:]
	v, headerLen, err := decodedLen(src)
	if err != nil {
		return false, false, nil, 0, err
	}
	if v > maxBlockSize {
		return false, false, nil, 0, errors.New("too large")
	}
	src = src[headerLen:]
	if len(src) == 0 {
		return false, false, nil, 0, errors.New("corrupt length")
	}
	if v == 0 {
		// Literals, rest of block...
		return true, true, src, len(src), nil
	}
	if v < len(src) {
		return false, false, nil, 0, fmt.Errorf("decompressed smaller than compressed size %d", v)
	}

	return true, false, src, v, nil
}

// decodedLen returns the length of the decoded block and the number of bytes
// that the length header occupied.
func decodedLen(src []byte) (blockLen, headerLen int, err error) {
	v, n := binary.Uvarint(src)
	if n <= 0 || v > 0xffffffff {
		return 0, 0, errors.New("corrupt length")
	}

	const wordSize = 32 << (^uint(0) >> 32 & 1)
	if wordSize == 32 && v > 0x7fffffff {
		return 0, 0, errors.New("too large")
	}
	return int(v), n, nil
}

// DecodeDebug returns the decoded form of src. The returned slice may be a sub-
// slice of dst if dst was large enough to hold the entire decoded block.
// Otherwise, a newly allocated slice will be returned.
//
// The dst and src must not overlap. It is valid to pass a nil dst.
func DecodeDebug(dst, src []byte) (ok bool) {
	fmt.Println("")
	isMLZ, lits, block, dLen, err := isMinLZ(src)
	if err != nil {
		return false
	}
	if lits {
		fmt.Println("Uncompressed block", lits, "bytes")
		return true
	}

	if !isMLZ {
		fmt.Println("Not MinLZ block", lits, "bytes")
	}
	if dLen <= cap(dst) {
		dst = dst[:dLen]
	} else {
		dst = make([]byte, dLen, dLen+11)
	}
	return minLZDecodeDebug(dst, block) == 0
}

func minLZDecodeDebug(dst, src []byte) int {
	const debug = true
	const debugErrors = true
	if true {
		fmt.Println("Starting decode, src:", len(src), "dst len:", len(dst))
	}
	var d, s, length int
	offset := 1

	// Remaining with extra checks...
	for s < len(src) {
		if debug {
			// fmt.Printf("in:%x, tag: %02b va:%x - src: %d, dst: %d\n", src[s], src[s]&3, src[s]>>2, s, d)
		}
		switch src[s] & 0x03 {
		case tagLiteral:
			isRepeat := src[s]&4 != 0
			x := uint32(src[s] >> 3)
			switch {
			case x < 29:
				s++
				length = int(x + 1)
			case x == 29:
				s += 2
				if s > len(src) {
					if debugErrors {
						fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
					}
					return decodeErrCodeCorrupt
				}
				length = int(uint32(src[s-1]) + 30)
			case x == 30:
				s += 3
				if s > len(src) {
					if debugErrors {
						fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
					}
					return decodeErrCodeCorrupt
				}
				length = int(uint32(src[s-2]) | uint32(src[s-1])<<8 + 30)
			default:
				//			case x == 31:
				s += 4
				if s > len(src) {
					if debugErrors {
						fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
					}
					return decodeErrCodeCorrupt
				}
				length = int(uint32(src[s-3]) | uint32(src[s-2])<<8 | uint32(src[s-1])<<16 + 30)
			}
			if isRepeat {
				if debug {
					fmt.Print(d, ": (repeat)")
				}
				goto doCopy2
			}
			if length > len(dst)-d || length > len(src)-s || (strconv.IntSize == 32 && length <= 0) {
				if debugErrors {
					fmt.Println("corrupt: lit size", length, "dst avail:", len(dst)-d, "src avail:", len(src)-s, "dst pos:", d)
				}
				return decodeErrCodeCorrupt
			}
			if debug {
				fmt.Print(d, ": (literals), length: ", length, "... [d-after: ", d+length, " s-after:", s+length, "]")
			}

			copy(dst[d:], src[s:s+length])
			d += length
			s += length
			if debug {
				fmt.Println("")
			}
			continue

		case tagCopy1:
			if debug {
				fmt.Print(d, ": (copy1)")
			}
			s += 2
			if s > len(src) {
				return decodeErrCodeCorrupt
			}

			length = int(src[s-2]) >> 2 & 15
			offset = int(binary.LittleEndian.Uint16(src[s-2:])>>6) + 1
			if length == 15 {
				s++
				if s > len(src) {
					if debugErrors {
						fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
					}
					return decodeErrCodeCorrupt
				}
				length = int(src[s-1]) + 18
			} else {
				length += 4
			}
		case tagCopy2:
			if debug {
				fmt.Print(d, ": (copy2)")
			}
			s += 3
			if uint(s) > uint(len(src)) {
				if debugErrors {
					fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
				}
				return decodeErrCodeCorrupt
			}
			length = int(src[s-3]) >> 2
			offset = int(uint32(src[s-2]) | uint32(src[s-1])<<8)
			if length <= 60 {
				length += 4
			} else {
				switch length {
				case 61:
					s++
					if uint(s) > uint(len(src)) {
						if debugErrors {
							fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
						}
						return decodeErrCodeCorrupt
					}
					length = int(src[s-1]) + 64
				case 62:
					s += 2
					if uint(s) > uint(len(src)) {
						if debugErrors {
							fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
						}
						return decodeErrCodeCorrupt
					}
					length = int(src[s-2]) | int(src[s-1])<<8 + 64
				case 63:
					s += 3
					if s > len(src) {
						if debugErrors {
							fmt.Println("read out of bounds, src pos:", s, "dst pos:", d)
						}
						return decodeErrCodeCorrupt
					}
					length = int(src[s-3]) | int(src[s-2])<<8 | int(src[s-1])<<16 + 64
				}
			}
			offset += 64
		case tagCopy3:
			s += 4
			if s > len(src) {
				if debugErrors {
					fmt.Println("(11)read out of bounds, src pos:", s, "dst pos:", d)
				}
				return decodeErrCodeCorrupt
			}
			val := binary.LittleEndian.Uint32(src[s-4:])
			isCopy3 := val&4 != 0
			litLen := int(val>>3) & 3
			if !isCopy3 {
				if debug {
					fmt.Print(d, ": (copy2f)")
				}
				length = 4 + int(val>>5)&7
				offset = int(val>>8)&65535 + 64
				s--
				litLen++
			} else {
				if debug {
					fmt.Print(d, ": (copy3)")
				}
				lengthTmp := (val >> 5) & 63
				offset = int(val>>11) + 65536
				if lengthTmp >= 61 {
					switch lengthTmp {
					case 61:
						s++
						if s > len(src) {
							if debugErrors {
								fmt.Println("(13)read out of bounds, src pos:", s, "dst pos:", d)
							}
							return decodeErrCodeCorrupt
						}
						length = int(src[s-1]) + 64
					case 62:
						s += 2
						if s > len(src) {
							if debugErrors {
								fmt.Println("(14)read out of bounds, src pos:", s, "dst pos:", d)
							}
							return decodeErrCodeCorrupt
						}
						length = (int(src[s-2]) | int(src[s-1])<<8) + 64
					default:
						s += 3
						if s > len(src) {
							if debugErrors {
								fmt.Println("(15)read out of bounds, src pos:", s, "dst pos:", d)
							}
							return decodeErrCodeCorrupt
						}
						length = int(src[s-3]) | int(src[s-2])<<8 | int(src[s-1])<<16 + 64
					}
				} else {
					length = int(lengthTmp + 4)
				}
			}

			if litLen > 0 {
				if debug {
					fmt.Print(" (fused lits: ", litLen, ")")
				}

				if litLen > len(dst)-d || s+litLen > len(src) {
					if debugErrors {
						fmt.Println("corrupt: lits size", litLen, "dst avail:", len(dst)-d, "src avail:", len(src)-s)
					}
					return decodeErrCodeCorrupt
				}
				copy(dst[d:], src[s:s+litLen])
				d += litLen
				s += litLen
			}
		}

	doCopy2:
		if offset <= 0 || d < offset || length > len(dst)-d {
			if debugErrors {
				fmt.Println("corrupt: match, length", length, "offset:", offset, "dst avail:", len(dst)-d, "dst pos:", d)
			}
			return decodeErrCodeCorrupt
		}

		if debug {
			fmt.Println(" - copy, length:", length, "offset:", offset, "... [d-after:", d+length, "s-after:", s, "]")
		}

		// Copy from an earlier sub-slice of dst to a later sub-slice.
		// If no overlap, use the built-in copy:
		if offset > length {
			copy(dst[d:d+length], dst[d-offset:])
			d += length
			continue
		}

		// Unlike the built-in copy function, this byte-by-byte copy always runs
		// forwards, even if the slices overlap. Conceptually, this is:
		//
		// d += forwardCopy(dst[d:d+length], dst[d-offset:])
		//
		// We align the slices into a and b and show the compiler they are the same size.
		// This allows the loop to run without bounds checks.
		a := dst[d : d+length]
		b := dst[d-offset:]
		b = b[:len(a)]
		for i := range a {
			a[i] = b[i]
		}
		d += length
	}
	if debug {
		fmt.Println("Done, d:", d, "s:", s, len(dst))
	}
	if d != len(dst) {
		if debugErrors {
			fmt.Println("corrupt: dst len:", len(dst), "d:", d)
		}
		return decodeErrCodeCorrupt
	}
	return 0
}

// limitReaderNL returns a Reader that reads from r
// but stops with EOF after n bytes, and optionally waits for a '\n'.
// The underlying implementation is a *LimitedReader.
func limitReaderNL(r io.Reader, n int64, nextNL bool) io.Reader {
	return &limitedReaderNL{R: r, N: n, NL: nextNL}
}

// A limitedReaderNL reads from R but limits the amount of
// data returned to just N bytes. Each call to Read
// updates N to reflect the new amount remaining.
// Read returns EOF when N <= 0 or when the underlying R returns EOF.
type limitedReaderNL struct {
	R  io.Reader // underlying reader
	N  int64     // max bytes remaining
	NL bool      // wait for newline.
}

func (l *limitedReaderNL) Read(p []byte) (n int, err error) {
	if l.N <= 0 && !l.NL {
		return 0, io.EOF
	}
	if !l.NL && int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	if l.NL && int64(n) > l.N {
		end := int(l.N)
		for end < n {
			if p[end] == '\n' {
				err = io.EOF
				l.NL = false
				n = end
				break
			}
			end++
		}
		l.N = 0
	} else {
		l.N -= int64(n)
	}
	return
}
