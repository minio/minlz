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
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/minio/minlz"
	"github.com/minio/minlz/cmd/internal/filepathx"
)

func mainSearch(args []string) {
	var (
		fs = flag.NewFlagSet("search", flag.ExitOnError)

		count     = fs.Bool("c", false, "Only print count of matching blocks/lines")
		noColor   = fs.Bool("no-color", false, "Disable colored output")
		lineN     = fs.Bool("n", false, "Print match line numbers")
		bail      = fs.Bool("bail", false, "Return error if search tables cannot be used")
		quiet     = fs.Bool("q", false, "Quiet: only set exit code (0=found, 1=not found, 2=error)")
		lines     = fs.Bool("l", true, "Print matching lines instead of whole blocks")
		verbose   = fs.Bool("v", false, "Print data")
		winStats  = fs.Bool("window-stats", false, "Print per-pattern-window table presence counts after the search (implies -v)")
		sidecar   = fs.String("sidecar", "", "Search using the given sidecar (.mzs) file; the input must support random access. If empty, <input>"+minlzSidecarExt+" is auto-detected when present.")
		noSidecar = fs.Bool("no-sidecar", false, "Disable sidecar auto-detection; force inline search")
		cpu       = fs.Int("cpu", runtime.GOMAXPROCS(0), "Maximum number of threads to use for sidecar search")
		help      = fs.Bool("help", false, "Display help")
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
		os.Exit(2)
	}
	_ = noColor
	pattern := []byte(args[0])
	files := args[1:]
	verboseOut := *verbose || *winStats

	exitCode := 1 // 1 = not found
	failed := false
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
				verbose:   verboseOut,
				multiFile: multiFile,
				sidecar:   *sidecar,
				noSidecar: *noSidecar,
				cpu:       *cpu,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %v\n", file, err)
				failed = true
				continue
			}
			if found {
				exitCode = 0
			}
			if verboseOut {
				elapsed := time.Since(start).Round(time.Millisecond)
				mbps := float64(stats.UncompressedSize) / elapsed.Seconds() / 1e6
				fmt.Fprintf(os.Stderr, "%s took %v %.01f MB/s\n", file, elapsed, mbps)
				if *winStats {
					stats.FprintExtended(os.Stderr)
				} else {
					stats.Fprint(os.Stderr)
				}
			}
		}
	}
	if failed {
		os.Exit(2)
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
	verbose   bool
	multiFile bool
	sidecar   string
	noSidecar bool
	cpu       int
}

func searchFile(file string, pattern []byte, opts searchOpts) (found bool, stats minlz.SearchStats, err error) {
	// Auto-detect a sidecar at <file>.mzs unless one was explicitly given
	// or auto-detection was disabled.
	if opts.sidecar == "" && !opts.noSidecar && file != "-" {
		candidate := file + minlzSidecarExt
		if st, serr := os.Stat(candidate); serr == nil && !st.IsDir() {
			opts.sidecar = candidate
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "%s: using sidecar %s\n", file, candidate)
			}
		}
	}

	var bsOpts []minlz.BlockSearchOption
	if opts.cpu > 0 {
		bsOpts = append(bsOpts, minlz.BlockSearchConcurrency(opts.cpu))
	}
	if opts.bail {
		bsOpts = append(bsOpts, minlz.BlockSearchBailOnMissing())
	}
	if opts.verbose {
		bsOpts = append(bsOpts, minlz.BlockSearchCollectStats())
		bsOpts = append(bsOpts, minlz.BlockSearchInfoCallback(func(cfg minlz.SearchTableConfig) {
			fmt.Fprintf(os.Stderr, "%s: search info: %s\n", file, cfg)
		}))
	}

	type genericSearcher interface {
		Search(pattern []byte, fn func(minlz.SearchResult) error) error
		Stats() minlz.SearchStats
	}
	var searcher genericSearcher

	if opts.sidecar != "" {
		// Sidecar search: main must support io.ReaderAt.
		if file == "-" {
			return false, stats, fmt.Errorf("sidecar search requires a seekable input, not stdin")
		}
		mainF, err := os.Open(file)
		if err != nil {
			return false, stats, err
		}
		defer mainF.Close()
		sideF, err := os.Open(opts.sidecar)
		if err != nil {
			return false, stats, err
		}
		defer sideF.Close()
		searcher = minlz.NewSidecarSearcher(mainF, sideF, bsOpts...)
	} else {
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
		searcher = minlz.NewBlockSearcher(r, bsOpts...)
	}

	matchCount := 0
	lineOffset := int64(1)
	lastLineStart := int64(-1)
	// contigEnd is the stream offset just past the last block a match was
	// resolved in. A still-open line (no newline in the current or previous
	// block) reuses the open key only when the match is at most one block past
	// contigEnd — that single intervening block is the previous block, which
	// lineStartOffset actually scans (lazily decoding it even when skipped), so a
	// separating newline there is caught. A wider gap of 2+ skipped blocks is
	// never decoded; since blocks are large, a line spanning them is unlikely, so
	// we assume the gap held a newline and start a new line.
	contigEnd := int64(-1)

	err = searcher.Search(pattern, func(r minlz.SearchResult) error {
		found = true
		if opts.quiet {
			return fmt.Errorf("done")
		}

		prefix := ""
		if opts.multiFile {
			prefix = file + ":"
		}

		if opts.lines {
			// Count each matching line once, keyed by the line's start offset.
			// A match with no preceding newline in the current or previous block
			// belongs to a line that began earlier; reuse the open key only when
			// the run is contiguous (see contigEnd) so a newline-sparse line
			// spanning a block plus one skipped block is counted once. Across a
			// wider gap we assume a newline was skipped and count a new line.
			ls, found := lineStartOffset(r)
			if !found && lastLineStart >= 0 && r.BlockStart <= contigEnd {
				ls = lastLineStart
			}
			contigEnd = r.BlockStart + int64(r.PrevBlockLen) + int64(len(r.Blocks[1]))
			if ls == lastLineStart {
				return nil
			}
			lastLineStart = ls
			if opts.count {
				matchCount++
				return nil
			}
			line := extractLine(r, pattern)
			matchCount++
			if opts.lineNums {
				fmt.Printf("%s%d:%d:%s\n", prefix, lineOffset, r.StreamOffset, line)
			} else {
				fmt.Printf("%s%d:%s\n", prefix, r.StreamOffset, line)
			}
			lineOffset++
		} else {
			matchCount++
			if opts.count {
				return nil
			}
			fmt.Printf("%s%d:\n", prefix, r.StreamOffset)
		}
		return nil
	})
	if err != nil && err.Error() == "done" {
		err = nil
	}

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

// lineStartOffset returns the absolute stream offset of the start of the line
// containing the match, and whether a preceding newline was actually found. It
// scans the current block back to the preceding newline and only consults the
// previous block (via PrevBlock, which may lazily decode) when the line begins
// before the current block — so counting a line that lies within one block
// never touches the previous block. When no newline is found in either block
// (the line began before them) it returns (r.BlockStart, false); the caller
// recovers the true start from carried continued-line state.
func lineStartOffset(r minlz.SearchResult) (int64, bool) {
	pl := r.PrevBlockLen
	if posInCur := r.Offset - pl; posInCur > 0 {
		if nl := bytes.LastIndexByte(r.Blocks[1][:posInCur], '\n'); nl >= 0 {
			return r.BlockStart + int64(pl+nl+1), true
		}
	}
	prev := r.PrevBlock()
	if end := min(r.Offset, len(prev)); end > 0 {
		if nl := bytes.LastIndexByte(prev[:end], '\n'); nl >= 0 {
			return r.BlockStart + int64(nl+1), true
		}
	}
	return r.BlockStart, false
}

// extractLine returns the line containing the match. It copies only the line's
// bytes (never whole blocks): a sub-slice of one block when the line fits in it,
// or the previous block's tail joined with the current block's head when the
// line straddles the boundary. The line is truncated at the current block's end
// if it continues into the next block (no forward block is fetched).
func extractLine(r minlz.SearchResult, pattern []byte) string {
	cur := r.Blocks[1]
	// Match start relative to the current block (PrevBlockLen is known without
	// decoding prev). <0 means the match itself begins in the previous block.
	mInCur := r.Offset - r.PrevBlockLen

	// Fast path: the match is in the current block. The line end is found by
	// scanning forward within cur (a line continuing past the block is truncated
	// here — no next block is fetched), so it never needs prev. If the line start
	// is also within cur, the whole line lives here and we return it without
	// calling PrevBlock (which would lazily decode a skipped previous block).
	if mInCur >= 0 {
		end := len(cur)
		if e := mInCur + len(pattern); e <= len(cur) {
			if nl := bytes.IndexByte(cur[e:], '\n'); nl >= 0 {
				end = e + nl
			}
		}
		if nl := bytes.LastIndexByte(cur[:mInCur], '\n'); nl >= 0 {
			return string(cur[nl+1 : end])
		}
	}

	// The line's start (or the match itself) crosses into the previous block;
	// fetch it now and use the logical prev||cur boundary logic.
	prev := r.PrevBlock()
	pl := len(prev)
	start := 0
	if nl := lastNewline(prev, cur, r.Offset); nl >= 0 {
		start = nl + 1
	}
	end := pl + len(cur)
	if nl := firstNewline(prev, cur, r.Offset+len(pattern)); nl >= 0 {
		end = nl
	}

	switch {
	case end <= pl:
		return string(prev[start:end])
	case start >= pl:
		return string(cur[start-pl : end-pl])
	default:
		buf := make([]byte, 0, end-start)
		buf = append(buf, prev[start:]...)
		buf = append(buf, cur[:end-pl]...)
		return string(buf)
	}
}

// lastNewline returns the index of the last '\n' strictly before upto in the
// logical buffer prev||cur, or -1. firstNewline returns the index of the first
// '\n' at or after from. Both index the concatenation without materializing it.
func lastNewline(prev, cur []byte, upto int) int {
	pl := len(prev)
	if upto > pl {
		if i := bytes.LastIndexByte(cur[:upto-pl], '\n'); i >= 0 {
			return pl + i
		}
		upto = pl
	}
	if upto > 0 {
		return bytes.LastIndexByte(prev[:min(upto, pl)], '\n')
	}
	return -1
}

func firstNewline(prev, cur []byte, from int) int {
	pl := len(prev)
	if from < 0 {
		from = 0
	}
	if from < pl {
		if i := bytes.IndexByte(prev[from:], '\n'); i >= 0 {
			return from + i
		}
		from = pl
	}
	if from-pl < len(cur) {
		if i := bytes.IndexByte(cur[from-pl:], '\n'); i >= 0 {
			return from + i
		}
	}
	return -1
}
