package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/minio/minlz"
	"github.com/minio/minlz/cmd/internal/filepathx"
)

func mainSearch(args []string) {
	var (
		fs = flag.NewFlagSet("search", flag.ExitOnError)

		count   = fs.Bool("c", false, "Only print count of matching blocks/lines")
		noColor = fs.Bool("no-color", false, "Disable colored output")
		lineN   = fs.Bool("n", false, "Print line numbers")
		bail    = fs.Bool("bail", false, "Return error if search tables cannot be used")
		quiet   = fs.Bool("q", false, "Quiet: only set exit code (0=found, 1=not found)")
		lines   = fs.Bool("l", true, "Print matching lines instead of whole blocks")
		verbose = fs.Bool("v", false, "Print data")
		help    = fs.Bool("help", false, "Display help")
	)
	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintln(w, `Search for a pattern in compressed MinLZ streams.

The pattern is a literal byte string (not a regex).

Options:`)
		fs.PrintDefaults()
		fmt.Fprintf(w, "\nUsage: %v search [options] <pattern> <input...>\n", os.Args[0])
	}
	fs.Parse(args)
	args = fs.Args()
	if *help || len(args) < 2 {
		fs.Usage()
		if *help {
			os.Exit(0)
		}
		os.Exit(1)
	}
	_ = noColor
	pattern := []byte(args[0])
	files := args[1:]

	exitCode := 1 // 1 = not found
	multiFile := len(files) > 1

	for _, fileArg := range files {
		var matches []string
		var err error
		if strings.ContainsAny(fileArg, "*?[") {
			matches, err = filepathx.Glob(fileArg)
			exitErr(err)
		} else {
			matches = []string{fileArg}
		}

		for _, file := range matches {
			start := time.Now()
			found, stats, err := searchFile(file, pattern, searchOpts{
				count:     *count,
				lineNums:  *lineN,
				bail:      *bail,
				quiet:     *quiet,
				lines:     *lines,
				multiFile: multiFile,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %v\n", file, err)
				continue
			}
			if found {
				exitCode = 0
			}
			if *verbose {
				elapsed := time.Since(start).Round(time.Millisecond)
				mbps := float64(stats.UncompressedSize) / elapsed.Seconds() / 1e6
				fmt.Fprintf(os.Stderr, "%s took %v %.01f MB/s\n", file, elapsed, mbps)
				stats.Fprint(os.Stderr)
			}
		}
	}
	if *quiet {
		os.Exit(exitCode)
	}
}

type searchOpts struct {
	count     bool
	lineNums  bool
	bail      bool
	quiet     bool
	lines     bool
	multiFile bool
}

func searchFile(file string, pattern []byte, opts searchOpts) (found bool, stats minlz.SearchStats, err error) {
	var r io.Reader
	if file == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(file)
		if err != nil {
			return false, stats, err
		}
		defer f.Close()
		r = f
	}

	var bsOpts []minlz.BlockSearchOption
	if opts.bail {
		bsOpts = append(bsOpts, minlz.BlockSearchBailOnMissing())
	}
	searcher := minlz.NewBlockSearcher(r, bsOpts...)

	matchCount := 0
	lineOffset := int64(1) // for line number tracking across blocks
	prevBlockEnd := int64(0)

	err = searcher.Search(pattern, func(r minlz.SearchResult) bool {
		if opts.lineNums && r.BlockStart > prevBlockEnd {
			// Line count for skipped blocks is unknown.
		}
		prevBlockEnd = r.BlockStart + int64(len(r.Data))

		if opts.lines {
			// Check for matches spanning the block boundary using the previous block.
			if prev := r.PrevBlock(); prev != nil && len(pattern) > 1 {
				searchBoundaryLine(prev, r.Data, pattern, r.BlockStart-int64(len(prev)), file, &matchCount, opts)
			}
			if searchLines(r.Data, pattern, r.BlockStart, file, &lineOffset, &matchCount, opts) {
				if matchCount > 0 {
					found = true
				}
				return true
			}
			found = matchCount > 0
			return false
		}
		// Check boundary between previous and current block.
		if prev := r.PrevBlock(); prev != nil && len(pattern) > 1 {
			boundary := append(prev[max(0, len(prev)-len(pattern)+1):], r.Data[:min(len(r.Data), len(pattern)-1)]...)
			if idx := bytes.Index(boundary, pattern); idx >= 0 {
				matchCount++
				found = true
				if opts.quiet {
					return false
				}
				if !opts.count {
					prefix := ""
					if opts.multiFile {
						prefix = file + ": "
					}
					bStart := r.BlockStart - int64(len(pattern)-1-idx)
					fmt.Printf("%s%d: (boundary match)\n", prefix, bStart)
				}
			}
		}
		// Whole-block mode: report each occurrence with uncompressed offset.
		off := 0
		for {
			idx := bytes.Index(r.Data[off:], pattern)
			if idx < 0 {
				break
			}
			matchCount++
			found = true
			if opts.quiet {
				return false
			}
			if !opts.count {
				prefix := ""
				if opts.multiFile {
					prefix = file + ": "
				}
				fmt.Printf("%s%d: (block +%d, %d bytes)\n", prefix, r.BlockStart+int64(off+idx), off+idx, len(r.Data))
			}
			off += idx + len(pattern)
		}
		return true
	})
	stats = searcher.Stats()
	if err != nil {
		return found, stats, err
	}
	if opts.count && !opts.quiet {
		prefix := ""
		if opts.multiFile {
			prefix = file + ": "
		}
		fmt.Printf("%s%d\n", prefix, matchCount)
	}
	return found, stats, nil
}

// searchBoundaryLine checks the region where the previous block ends and the current
// block begins for a line containing the pattern that spans the boundary.
func searchBoundaryLine(prev, cur, pattern []byte, prevStart int64, file string, matchCount *int, opts searchOpts) {
	// Build the boundary region: last partial line of prev + first partial line of cur.
	// Find last newline in prev.
	tailStart := bytes.LastIndexByte(prev, '\n')
	if tailStart < 0 {
		tailStart = 0
	} else {
		tailStart++ // skip the newline itself
	}
	tail := prev[tailStart:]

	// Find first newline in cur.
	headEnd := bytes.IndexByte(cur, '\n')
	if headEnd < 0 {
		headEnd = len(cur)
	}
	head := cur[:headEnd]

	// The boundary line is tail + head.
	boundaryLine := append(tail, head...)
	if !bytes.Contains(boundaryLine, pattern) {
		return
	}
	*matchCount++
	if opts.quiet || opts.count {
		return
	}
	prefix := ""
	if opts.multiFile {
		prefix = file + ":"
	}
	streamOff := prevStart + int64(tailStart)
	fmt.Printf("%s%d:%s\n", prefix, streamOff, string(boundaryLine))
}

func searchLines(data, pattern []byte, blockStart int64, file string, lineOffset *int64, matchCount *int, opts searchOpts) bool {
	byteOff := int64(0) // byte offset within block
	lines := bytes.Split(data, []byte("\n"))
	for i, line := range lines {
		if bytes.Contains(line, pattern) {
			*matchCount++
			if opts.quiet {
				return false
			}
			if opts.count {
				*lineOffset += int64(i) + 1
				continue
			}
			prefix := ""
			if opts.multiFile {
				prefix = file + ":"
			}
			streamOff := blockStart + byteOff
			if opts.lineNums {
				fmt.Printf("%s%d:%d:%s\n", prefix, *lineOffset+int64(i), streamOff, string(line))
			} else {
				fmt.Printf("%s%d:%s\n", prefix, streamOff, string(line))
			}
		}
		byteOff += int64(len(line)) + 1 // +1 for the \n
	}
	*lineOffset += int64(len(lines))
	return true
}
