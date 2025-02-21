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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minlz"
	"github.com/minio/minlz/cmd/internal/filepathx"
	"github.com/minio/minlz/cmd/internal/readahead"
)

func mainCompress(args []string) {
	var (
		fs = flag.NewFlagSet("compress", flag.ExitOnError)

		faster    = fs.Bool("1", false, "Compress faster, but with a minor compression loss")
		_         = fs.Bool("2", true, "Default compression speed")
		slower    = fs.Bool("3", false, "Compress more, but a lot slower")
		recomp    = fs.Bool("recomp", false, "Recompress MinLZ, Snappy or S2 input")
		blockSize = fs.String("bs", "8M", "Max block size. Examples: 64K, 256K, 1M, 8M. Must be power of two and <= 8MB")
		index     = fs.Bool("index", true, "Add seek index")
		padding   = fs.String("pad", "1", "Pad size to a multiple of this value, Examples: 500, 64K, 256K, 1M, 4M, etc")

		// Shared
		block  = fs.Bool("block", false, "Use as a single block. Will load content into memory. Max 8MB.")
		safe   = fs.Bool("safe", false, "Do not overwrite output files")
		cpu    = fs.Int("cpu", runtime.GOMAXPROCS(0), "Maximum number of threads to use")
		stdout = fs.Bool("c", false, "Write all output to stdout. Multiple input files will be concatenated")
		out    = fs.String("o", "", "Write output to another file. Single input file only")
		remove = fs.Bool("rm", false, "Delete source file(s) after success")
		quiet  = fs.Bool("q", false, "Don't write any output to terminal, except errors")
		bench  = fs.Int("bench", 0, "Run benchmark n times. No output will be written")
		verify = fs.Bool("verify", false, "Verify files, but do not write output")
		help   = fs.Bool("help", false, "Display help")
	)
	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintln(w, `Compresses all files supplied as input separately.
Output files are written as 'filename.ext`+minlzExt+`.
By default output files will be overwritten.
Use - as the only file name to read from stdin and write to stdout.

Wildcards are accepted: testdir/*.txt will compress all files in testdir ending with .txt
Directories can be wildcards as well. testdir/*/*.txt will match testdir/subdir/b.txt

File names beginning with 'http://' and 'https://' will be downloaded and compressed.
Only http response code 200 is accepted.

Options:`)
		fs.PrintDefaults()
		fmt.Fprintf(w, "\nUsage: %v c [options] <input>\n", os.Args[0])
	}
	fs.Parse(args)
	args = fs.Args()

	sz, err := toSize(*blockSize)
	exitErr(err)
	pad, err := toSize(*padding)
	exitErr(err)
	if *help {
		fs.Usage()
		os.Exit(0)
	}
	if len(args) == 0 || (*slower && *faster) {
		fs.Usage()
		os.Exit(1)
	}
	level := minlz.LevelBalanced
	if *faster {
		level = minlz.LevelFastest
	}
	if *slower {
		level = minlz.LevelSmallest
	}
	opts := []minlz.WriterOption{minlz.WriterBlockSize(int(sz)), minlz.WriterConcurrency(*cpu), minlz.WriterPadding(int(pad)), minlz.WriterLevel(level), minlz.WriterAddIndex(*index)}
	wr := minlz.NewWriter(nil, opts...)

	// No args, use stdin/stdout
	if len(args) == 1 && args[0] == "-" {
		// Catch interrupt, so we don't exit at once.
		// os.Stdin will return EOF, so we should be able to get everything.
		signal.Notify(make(chan os.Signal, 1), os.Interrupt)
		if len(*out) == 0 {
			wr.Reset(os.Stdout)
		} else {
			if *safe {
				_, err := os.Stat(*out)
				if !os.IsNotExist(err) {
					exitErr(errors.New("destination file exists"))
				}
			}
			dstFile, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
			exitErr(err)
			defer dstFile.Close()
			bw := bufio.NewWriterSize(dstFile, int(sz*2))
			defer bw.Flush()
			wr.Reset(bw)
		}
		_, err = wr.ReadFrom(os.Stdin)
		printErr(err)
		printErr(wr.Close())
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
		err = runBench(files, block, quiet, err, bench, level, cpu, verify, wr)
		return
	}
	ext := minlzExt
	if *block {
		ext = ".mzb"
	}
	if *out != "" && len(files) > 1 {
		exitErr(errors.New("-out parameter can only be used with one input"))
	}
	for _, filename := range files {
		if *block {
			processBlock(recomp, filename, ext, out, quiet, err, stdout, safe, level, verify, remove)
		} else {
			processStream(filename, recomp, ext, out, quiet, stdout, remove, err, cpu, safe, sz, verify, wr)
		}
	}
}

func processStream(filename string, recomp *bool, ext string, outFile *string, quiet *bool, stdout *bool, remove *bool, err error, cpu *int, safe *bool, sz int64, verify *bool, wr *minlz.Writer) {
	var closeOnce sync.Once
	outFileBase := filename
	if *recomp {
		switch {
		case strings.HasSuffix(outFileBase, minlzExt):
			outFileBase = strings.TrimSuffix(outFileBase, minlzExt)
		case strings.HasSuffix(outFileBase, s2Ext):
			outFileBase = strings.TrimSuffix(outFileBase, s2Ext)
		case strings.HasSuffix(outFileBase, snappyExt):
			outFileBase = strings.TrimSuffix(outFileBase, snappyExt)
		case strings.HasSuffix(outFileBase, ".snappy"):
			outFileBase = strings.TrimSuffix(outFileBase, ".snappy")
		}
	}
	dstFilename := cleanFileName(fmt.Sprintf("%s%s", outFileBase, ext))
	if *outFile != "" {
		dstFilename = *outFile
	}
	if !*quiet {
		fmt.Print("Compressing ", filename, " -> ", dstFilename)
	}

	if dstFilename == filename && !*stdout {
		if *remove {
			exitErr(errors.New("cannot remove when input = output"))
		}
		renameDst := dstFilename
		dstFilename = fmt.Sprintf(".tmp-%s%s", time.Now().Format("2006-01-02T15-04-05Z07"), ext)
		defer func() {
			exitErr(os.Rename(dstFilename, renameDst))
		}()
	}

	// Input file.
	file, _, mode := openFile(filename, false)
	exitErr(err)
	defer closeOnce.Do(func() { file.Close() })
	src, err := readahead.NewReaderSize(file, *cpu+1, 1<<20)
	exitErr(err)
	defer src.Close()
	var rc = &rCounter{
		in: src,
	}
	if !*quiet {
		// We only need to count for printing
		src = rc
	}
	if *recomp {
		dec := minlz.NewReader(src)
		pr, pw := io.Pipe()
		go func() {
			_, err := dec.DecodeConcurrent(pw, *cpu)
			pw.CloseWithError(err)
		}()
		src = pr
	}

	var out io.Writer
	switch {
	case *stdout:
		out = os.Stdout
	default:
		if *safe {
			_, err := os.Stat(dstFilename)
			if !os.IsNotExist(err) {
				exitErr(errors.New("destination file exists"))
			}
		}
		dstFile, err := os.OpenFile(dstFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		exitErr(err)
		defer dstFile.Close()
		bw := bufio.NewWriterSize(dstFile, int(sz)*2)
		defer bw.Flush()
		out = bw
	}
	out, errFn := verifyTo(out, *verify, *quiet, *cpu)
	wc := wCounter{out: out}
	start := time.Now()
	wr.Reset(&wc)
	defer wr.Close()
	_, err = wr.ReadFrom(src)
	exitErr(err)
	err = wr.Close()

	exitErr(err)
	if !*quiet {
		input := rc.n
		elapsed := time.Since(start)
		mbpersec := (float64(input) / 1e6) / (float64(elapsed) / (float64(time.Second)))
		pct := float64(wc.n) * 100 / float64(input)
		fmt.Printf(" %d -> %d [%.02f%%]; %.01fMB/s\n", input, wc.n, pct, mbpersec)
	}
	exitErr(errFn())
	if *remove {
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

func processBlock(recomp *bool, filename string, ext string, outFile *string, quiet *bool, err error, stdout *bool, safe *bool, level int, verify *bool, remove *bool) {
	if *recomp {
		exitErr(errors.New("cannot recompress blocks (yet)"))
	}
	func() {
		var closeOnce sync.Once
		dstFilename := cleanFileName(fmt.Sprintf("%s%s", filename, ext))
		if *outFile != "" {
			dstFilename = *outFile
		}
		if !*quiet {
			fmt.Print("Compressing ", filename, " -> ", dstFilename)
		}
		// Input file.
		file, size, mode := openFile(filename, false)
		if size > minlz.MaxBlockSize {
			exitErr(errors.New("maximum block size of 8MB exceeded"))
		}
		exitErr(err)
		defer closeOnce.Do(func() { file.Close() })
		inBytes, err := io.ReadAll(file)
		exitErr(err)

		var out io.Writer
		switch {
		case *stdout:
			out = os.Stdout
		default:
			if *safe {
				_, err := os.Stat(dstFilename)
				if !os.IsNotExist(err) {
					exitErr(errors.New("destination file exists"))
				}
			}
			dstFile, err := os.OpenFile(dstFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			exitErr(err)
			defer dstFile.Close()
			out = dstFile
		}
		start := time.Now()
		compressed, err := minlz.Encode(nil, inBytes, level)
		exitErr(err)
		_, err = out.Write(compressed)
		exitErr(err)
		if !*quiet {
			elapsed := time.Since(start)
			mbpersec := (float64(len(inBytes)) / 1e6) / (float64(elapsed) / (float64(time.Second)))
			pct := float64(len(compressed)) * 100 / float64(len(inBytes))
			fmt.Printf(" %d -> %d [%.02f%%]; %.01fMB/s\n", len(inBytes), len(compressed), pct, mbpersec)
		}
		if *verify {
			got, err := minlz.Decode(make([]byte, 0, len(inBytes)), compressed)
			exitErr(err)
			if !bytes.Equal(got, inBytes) {
				exitErr(fmt.Errorf("decoded content mismatch"))
			}
			if !*quiet {
				fmt.Print("... Verified ok.")
			}
		}
		if *remove {
			closeOnce.Do(func() {
				file.Close()
				if !*quiet {
					fmt.Println("Removing", filename)
				}
				err := os.Remove(filename)
				exitErr(err)
			})
		}
	}()
}

func runBench(files []string, block *bool, quiet *bool, err error, bench *int, level int, cpu *int, verify *bool, wr *minlz.Writer) error {
	debug.SetGCPercent(10)
	dec := minlz.NewReader(nil)
	for _, filename := range files {
		if *block {
			err = runBenchBlock(quiet, filename, err, bench, level, cpu, verify, wr)
		} else {
			err = runBenchStream(quiet, filename, err, bench, verify, wr, dec, cpu)
		}
	}
	return err
}

func runBenchStream(quiet *bool, filename string, err error, bench *int, verify *bool, wr *minlz.Writer, dec *minlz.Reader, cpu *int) error {
	if !*quiet {
		fmt.Print("Reading ", filename, "...")
	}

	// Input file.
	file, size, _ := openFile(filename, false)
	b := make([]byte, size)
	_, err = io.ReadFull(file, b)
	exitErr(err)
	file.Close()
	var buf *bytes.Buffer
	for i := 0; i < *bench; i++ {
		w := io.Discard
		// Verify with this buffer...
		if *verify {
			if buf == nil {
				buf = bytes.NewBuffer(make([]byte, 0, len(b)+(len(b)>>8)))
			}
			buf.Reset()
			w = buf
		}
		if i == 0 && false {
			if !*quiet {
				fmt.Print("\nPrewarm...")
			}
			start := time.Now()
			wr.Reset(io.Discard)
			for time.Since(start) < 3*time.Second {
				exitErr(wr.EncodeBuffer(b))
				exitErr(wr.Flush())
			}
			exitErr(wr.Close())
			if !*quiet {
				fmt.Printf("Done - %v", time.Since(start).Round(time.Millisecond))
			}
		}
		wc := wCounter{out: w}
		if !*quiet {
			fmt.Print("\nCompressing...")
		}
		wr.Reset(&wc)
		runtime.GC()
		start := time.Now()
		err := wr.EncodeBuffer(b)
		exitErr(err)
		err = wr.Close()
		exitErr(err)
		if !*quiet {
			input := len(b)
			elapsed := time.Since(start)
			mbpersec := (float64(input) / 1e6) / (float64(elapsed) / (float64(time.Second)))
			pct := float64(wc.n) * 100 / float64(input)
			ms := elapsed.Round(time.Millisecond)
			fmt.Printf(" %d -> %d [%.02f%%]; %v, %.01fMB/s", input, wc.n, pct, ms, mbpersec)
		}
		if *verify {
			runtime.GC()
			var speedIdx int
			if !*quiet {
				fmt.Print("\nDecompressing.")
				speedIdx = 14
			}
			allData := buf.Bytes()
			start := time.Now()
			dec.Reset(buf)
			n, err := dec.DecodeConcurrent(io.Discard, *cpu)
			exitErr(err)
			if int(n) != len(b) {
				exitErr(fmt.Errorf("unexpected size, want %d, got %d", len(b), n))
			}
			input := len(b)
			elapsed := time.Since(start)
			mbpersecConc := (float64(input) / 1e6) / (float64(elapsed) / (float64(time.Second)))
			if !*quiet {
				pct := float64(input) * 100 / float64(wc.n)
				ms := elapsed.Round(time.Millisecond)
				str := fmt.Sprintf(" %d -> %d [%.02f%%]; %v, %.01fMB/s", wc.n, n, pct, ms, mbpersecConc)
				fmt.Print(str)
				speedIdx += strings.IndexByte(str, ';') + 1
			}

			if *cpu > 1 {
				type wrapper struct {
					io.Reader
				}
				start = time.Now()
				dec.Reset(bytes.NewBuffer(allData))

				n, err = io.Copy(io.Discard, wrapper{Reader: dec})
				exitErr(err)
				if int(n) != len(b) {
					exitErr(fmt.Errorf("unexpected size, want %d, got %d", len(b), n))
				}
				elapsed = time.Since(start)
				mbpersecSingle := (float64(input) / 1e6) / (float64(elapsed) / (float64(time.Second)))
				if !*quiet {
					ms := elapsed.Round(time.Millisecond)
					spacing := strings.Repeat(" ", speedIdx-13)
					fmt.Printf("\n%s - 1 thread ; %v, %.01fMB/s (%.1fx)", spacing, ms, mbpersecSingle, mbpersecConc/mbpersecSingle)
				}
				dec.Reset(nil)
			}

		}
	}
	if !*quiet {
		fmt.Println("")
	}
	wr.Close()
	return err
}

func runBenchBlock(quiet *bool, filename string, err error, bench *int, level int, cpu *int, verify *bool, wr *minlz.Writer) error {
	if !*quiet {
		fmt.Print("Reading ", filename, "...")
	}
	// Input file.
	file, size, _ := openFile(filename, false)
	if size > minlz.MaxBlockSize {
		exitErr(errors.New("maximum block size of 8MB exceeded"))
	}
	b := make([]byte, size)
	_, err = io.ReadFull(file, b)
	exitErr(err)
	file.Close()

	if !*quiet {
		fmt.Print("\n\nCompressing block (1 thread)...\n")
	}
	mel := minlz.MaxEncodedLen(len(b))
	compressed := make([]byte, mel)
	var singleSpeed float64
	runtime.GC()

	start := time.Now()
	end := time.Now().Add(time.Duration(*bench) * time.Second)
	lastUpdate := start
	var n int
	for time.Now().Before(end) {
		compressed, err = minlz.Encode(compressed, b, level)
		exitErr(err)

		n++
		if !*quiet && time.Since(lastUpdate) > time.Second/6 {
			input := float64(len(b)) * float64(n)
			output := float64(len(compressed)) * float64(n)
			elapsed := time.Since(start)
			singleSpeed = (input / 1e6) / (float64(elapsed) / (float64(time.Second)))
			pct := output * 100 / input
			ms := elapsed.Round(time.Millisecond)
			fmt.Printf(" * %d -> %d bytes [%.02f%%]; %v, %.01fMB/s           \r", len(b), len(compressed), pct, ms, singleSpeed)
			lastUpdate = time.Now()
		}
	}
	runtime.GC()
	if *cpu > 1 {
		if !*quiet {
			fmt.Printf("\n\nCompressing block (%d threads)...\n", *cpu)
		}
		var n atomic.Int64
		var wg sync.WaitGroup
		dsts := make([]byte, *cpu*mel)
		start := time.Now()
		end := time.Now().Add(time.Duration(*bench) * time.Second)
		wg.Add(*cpu)
		for i := 0; i < *cpu; i++ {
			go func(i int) {
				compressed := dsts[i*mel : i*mel+mel : i*mel+mel]
				defer wg.Done()
				for time.Now().Before(end) {
					compressed, err = minlz.Encode(compressed, b, level)
					exitErr(err)
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
			fmt.Printf(" * %d -> %d bytes [%.02f%%]; %v, %.01fMB/s (%.1fx)          \r", len(b), len(compressed), pct, ms, mbpersec, scale)
			time.Sleep(time.Second / 6)
		}
		wg.Wait()
		runtime.GC()
	}
	if *verify {
		compressed, err := minlz.Encode(nil, b, level)
		exitErr(err)
		if !*quiet {
			fmt.Print("\n\nDecompressing block (1 thread)...\n")
		}
		start := time.Now()
		end := time.Now().Add(time.Duration(*bench) * time.Second)
		lastUpdate := start
		decomp := make([]byte, 0, len(b))
		n = 0
		for time.Now().Before(end) {
			decomp, err = minlz.Decode(decomp, compressed)
			exitErr(err)
			if len(decomp) != len(b) {
				exitErr(fmt.Errorf("unexpected size, want %d, got %d", len(b), len(decomp)))
			}
			n++
			if !*quiet && time.Since(lastUpdate) > time.Second/6 {
				input := float64(len(b)) * float64(n)
				output := float64(len(compressed)) * float64(n)
				elapsed := time.Since(start)
				singleSpeed = (input / 1e6) / (float64(elapsed) / (float64(time.Second)))
				pct := input * 100 / output
				ms := elapsed.Round(time.Millisecond)
				fmt.Printf(" * %d -> %d bytes [%.02f%%]; %v, %.01fMB/s               \r", len(compressed), len(decomp), pct, ms, singleSpeed)
				lastUpdate = time.Now()
			}
		}
		if *cpu > 1 {
			if !*quiet {
				fmt.Printf("\n\nDecompressing block (%d threads)...\n", *cpu)
			}
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
				pct := input * 100 / output
				ms := elapsed.Round(time.Millisecond)
				fmt.Printf(" * %d -> %d bytes [%.02f%%]; %v, %.01fMB/s (%.1fx)          \r", len(b), len(compressed), pct, ms, mbpersec, scale)
				time.Sleep(time.Second / 6)
			}
			wg.Wait()
			runtime.GC()
		}
	}
	if !*quiet {
		fmt.Println("")
	}
	wr.Close()
	return err
}

func verifyTo(w io.Writer, verify, quiet bool, cpu int) (io.Writer, func() error) {
	if !verify {
		return w, func() error {
			return nil
		}
	}
	pr, pw := io.Pipe()
	writer := io.MultiWriter(w, pw)
	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := minlz.NewReader(pr)
		_, err = r.DecodeConcurrent(io.Discard, cpu)
		pr.CloseWithError(fmt.Errorf("verify: %w", err))
	}()
	return writer, func() error {
		pw.Close()
		wg.Wait()
		if err == nil {
			if !quiet {
				fmt.Print("... Verified ok.")
			}
		}
		return err
	}
}

func printErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "\nERROR:", err.Error())
	}
}
