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
		lineN   = fs.Bool("n", false, "Print match line numbers")
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
	lineOffset := int64(1)
	lastLineEnd := int64(-1)

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
			// Skip matches within an already-emitted line.
			if r.StreamOffset < lastLineEnd {
				return nil
			}
			line, endOff := extractLine(r, pattern)
			lastLineEnd = r.StreamOffset + int64(endOff)
			matchCount++
			if opts.count {
				return nil
			}
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

// extractLine extracts the full line containing the match from the block context.
// extractLine extracts the full line containing the match.
// Returns the line and the distance from the match start to the line end.
func extractLine(r minlz.SearchResult, pattern []byte) (string, int) {
	var data []byte
	var matchPos int
	if r.Blocks[0] != nil {
		data = append(r.Blocks[0], r.Blocks[1]...)
		matchPos = r.Offset
	} else {
		data = r.Blocks[1]
		matchPos = r.Offset
	}

	lineStart := bytes.LastIndexByte(data[:matchPos], '\n')
	if lineStart < 0 {
		lineStart = 0
	} else {
		lineStart++
	}
	lineEnd := bytes.IndexByte(data[matchPos+len(pattern):], '\n')
	if lineEnd < 0 {
		lineEnd = len(data)
	} else {
		lineEnd += matchPos + len(pattern)
	}
	return string(data[lineStart:lineEnd]), lineEnd - matchPos
}
