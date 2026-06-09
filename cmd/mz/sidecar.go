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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minlz"
)

// mainSidecar dispatches `mz sidecar <subcommand> ...`.
func mainSidecar(args []string) {
	if len(args) < 1 {
		sidecarUsage(os.Stderr)
		os.Exit(1)
	}
	sub := args[0]
	switch sub {
	case "build":
		mainSidecarBuild(args[1:])
	case "extract":
		mainSidecarExtract(args[1:])
	case "help", "-h", "--help":
		sidecarUsage(os.Stdout)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown sidecar subcommand: %q\n\n", sub)
		sidecarUsage(os.Stderr)
		os.Exit(1)
	}
}

func sidecarUsage(w io.Writer) {
	fmt.Fprintln(w, `Sidecar search-index streams.

Subcommands:
  build    Build a new sidecar by reading a MinLZ stream and indexing each block.
  extract  Extract existing search-index chunks from a MinLZ stream into a sidecar.
           Optionally produce a stripped main stream containing only data chunks.

The sidecar is a valid MinLZ stream containing search indexes (0x44/0x45/0x46)
and remote-block references (0x47) pointing into the main stream. It can be
used to search a main stream without embedding the search index in it.`)
	fmt.Fprintf(w, "\nUsage: %v sidecar <build|extract> [options] <input>\n", os.Args[0])
}

func mainSidecarBuild(args []string) {
	var (
		fs = flag.NewFlagSet("sidecar build", flag.ExitOnError)

		outFile            = fs.String("o", "", "Sidecar output filename (default: <input>"+minlzSidecarExt+")")
		searchLens         = fs.String("search.lens", "6", "Comma-separated list of match lengths (1-8); one config per length")
		searchPfx          = fs.String("search.prefixes", "", "Search prefix bytes (e.g. ':,\"')")
		searchPfxLong      = fs.String("search.prefix", "", "Single longer prefix string (e.g. 'id\":\"')")
		searchExtras       = fs.Int("search.extras", 0, "Requires -search.prefix: emit Extras+1 windows per prefix occurrence (0-15). matchLen+extras must be <= 16")
		searchMax          = fs.Int("search.max", 75, "Discard search-table entries with population % > this")
		searchLim          = fs.Int("search.lim", 25, "Stop reducing search tables when reduced population exceeds this %. Auto-tightened to 10 when a prefix is set, unless explicitly overridden.")
		searchUncompressed = fs.Bool("search.uncompressed", false, "Disable per-block search-table compression (default emits 0x46 chunks)")
		safe               = fs.Bool("safe", false, "Do not overwrite an existing sidecar")
		quiet              = fs.Bool("q", false, "Quiet")
		help               = fs.Bool("help", false, "Display help")
	)
	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintln(w, `Build a sidecar search-index stream from an existing compressed MinLZ stream.

The input stream does not need to contain search indexes; this command will
decompress each block, compute fresh search tables per the provided configs,
and write them (plus 0x47 Remote Block References) to the sidecar.

Multiple --search.lens values produce multiple configs in one sidecar. The
SidecarSearcher tries each config and ANDs the skip decisions.

Options:`)
		fs.PrintDefaults()
		fmt.Fprintf(w, "\nUsage: %v sidecar build [options] <input>\n", os.Args[0])
	}
	fs.Parse(args)
	rest := fs.Args()
	if *help || len(rest) != 1 {
		fs.Usage()
		if *help {
			os.Exit(0)
		}
		os.Exit(1)
	}
	src := rest[0]
	dst := *outFile
	if dst == "" {
		dst = src + minlzSidecarExt
	}
	if samePath(src, dst) {
		exitErr(fmt.Errorf("sidecar destination %q would overwrite the input %q", dst, src))
	}

	// Build the configs.
	lens := strings.Split(*searchLens, ",")
	if len(lens) == 0 {
		exitErr(errors.New("at least one --search.lens value required"))
	}
	if len(*searchPfx) > 0 && len(*searchPfxLong) > 0 {
		exitErr(errors.New("cannot use both --search.prefix and --search.prefixes"))
	}
	searchLimSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "search.lim" {
			searchLimSet = true
		}
	})
	var opts []minlz.SidecarOption
	for _, ls := range lens {
		ls = strings.TrimSpace(ls)
		if ls == "" {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(ls, "%d", &n); err != nil {
			exitErr(fmt.Errorf("bad --search.lens value %q: %v", ls, err))
		}
		if n < 1 || n > 8 {
			exitErr(fmt.Errorf("--search.lens %d out of range (1-8)", n))
		}
		cfg := minlz.NewSearchTableConfig().WithMatchLen(n).WithMaxPopulation(*searchMax)
		// With -search.extras > 0 we route -search.prefix to a long prefix
		// (type 4) even if it is a single byte — extras only applies to type 4.
		useLongPrefix := *searchExtras > 0
		if len(*searchPfxLong) > 0 {
			if useLongPrefix || len(*searchPfxLong) > 1 {
				cfg = cfg.WithLongPrefix([]byte(*searchPfxLong))
			} else {
				cfg = cfg.WithBytePrefix((*searchPfxLong)[0])
			}
		} else if len(*searchPfx) > 0 {
			cfg = cfg.WithBytePrefix([]byte(*searchPfx)...)
		}
		if *searchExtras != 0 {
			if *searchExtras < 0 || *searchExtras > 15 {
				exitErr(fmt.Errorf("search extras must be 0-15"))
			}
			if n+*searchExtras > 16 {
				exitErr(fmt.Errorf("matchLen+extras must be <= 16, got matchLen=%d extras=%d", n, *searchExtras))
			}
			if len(*searchPfxLong) == 0 {
				exitErr(fmt.Errorf("-search.extras requires -search.prefix"))
			}
			cfg = cfg.WithExtras(*searchExtras)
		}
		if searchLimSet {
			cfg = cfg.WithMaxReducedPopulation(*searchLim)
		}
		if *searchUncompressed {
			cfg = cfg.WithoutCompression()
		}
		opts = append(opts, minlz.SidecarSearchTable(cfg))
	}
	if len(opts) == 0 {
		exitErr(errors.New("no usable search configs"))
	}

	// Open files.
	in, err := os.Open(src)
	exitErr(err)
	defer in.Close()

	if *safe {
		if _, err := os.Stat(dst); !os.IsNotExist(err) {
			exitErr(fmt.Errorf("destination %s exists (use without -safe to overwrite)", dst))
		}
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	exitErr(err)
	defer out.Close()

	if !*quiet {
		fmt.Printf("Building sidecar: %s -> %s\n", src, dst)
	}

	if err := minlz.BuildSidecar(out, in, opts...); err != nil {
		exitErr(err)
	}
	if !*quiet {
		st, _ := os.Stat(dst)
		if st != nil {
			fmt.Printf("Sidecar size: %d bytes\n", st.Size())
		}
	}
}

func mainSidecarExtract(args []string) {
	var (
		fs = flag.NewFlagSet("sidecar extract", flag.ExitOnError)

		outFile   = fs.String("o", "", "Sidecar output filename (default: <input>"+minlzSidecarExt+")")
		newStream = fs.String("newstream", "", "If set, also write a stripped main stream (without search indexes) here")
		safe      = fs.Bool("safe", false, "Do not overwrite existing files")
		quiet     = fs.Bool("q", false, "Quiet")
		help      = fs.Bool("help", false, "Display help")
	)
	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintln(w, `Extract existing search-index chunks from a MinLZ stream into a sidecar.

The original chunks (0x44/0x45/0x46) are preserved verbatim — no rebuilding.

If --newstream is supplied, the main stream is also written there minus the
search-index chunks; sidecar offsets reference the new layout. A fresh seek
index is appended to --newstream so it remains independently seekable.

Without --newstream, sidecar offsets reference the original input layout and
the original file must be used for searching.

Options:`)
		fs.PrintDefaults()
		fmt.Fprintf(w, "\nUsage: %v sidecar extract [options] <input>\n", os.Args[0])
	}
	fs.Parse(args)
	rest := fs.Args()
	if *help || len(rest) != 1 {
		fs.Usage()
		if *help {
			os.Exit(0)
		}
		os.Exit(1)
	}
	src := rest[0]
	dst := *outFile
	if dst == "" {
		dst = src + minlzSidecarExt
	}
	if samePath(src, dst) {
		exitErr(fmt.Errorf("sidecar destination %q would overwrite the input %q", dst, src))
	}
	if *newStream != "" {
		if samePath(src, *newStream) {
			exitErr(fmt.Errorf("--newstream %q would overwrite the input %q", *newStream, src))
		}
		if samePath(dst, *newStream) {
			exitErr(fmt.Errorf("--newstream %q and sidecar %q are the same file", *newStream, dst))
		}
	}

	in, err := os.Open(src)
	exitErr(err)
	defer in.Close()

	if *safe {
		if _, err := os.Stat(dst); !os.IsNotExist(err) {
			exitErr(fmt.Errorf("destination %s exists", dst))
		}
		if *newStream != "" {
			if _, err := os.Stat(*newStream); !os.IsNotExist(err) {
				exitErr(fmt.Errorf("--newstream destination %s exists", *newStream))
			}
		}
	}

	sideOut, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	exitErr(err)
	defer sideOut.Close()

	var newOut *os.File
	if *newStream != "" {
		newOut, err = os.OpenFile(*newStream, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		exitErr(err)
		defer newOut.Close()
	}

	if !*quiet {
		fmt.Printf("Extracting sidecar: %s -> %s", src, dst)
		if newOut != nil {
			fmt.Printf("  (+ stripped main: %s)", *newStream)
		}
		fmt.Println()
	}

	var newDst io.Writer
	if newOut != nil {
		newDst = newOut
	}
	if err := minlz.ExtractSidecar(sideOut, newDst, in); err != nil {
		exitErr(err)
	}
}

// samePath reports whether a and b resolve to the same filesystem path. Used
// to refuse outputs that would truncate the input or collide with each other.
func samePath(a, b string) bool {
	pa, ea := filepath.Abs(a)
	pb, eb := filepath.Abs(b)
	return ea == nil && eb == nil && pa == pb
}
