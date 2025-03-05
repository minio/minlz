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
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"unicode"

	"github.com/minio/minlz/cmd/internal/shttp"
)

var (
	version = "[dev]"
	date    = "[unknown]"
)

const (
	minlzExt      = ".mz"
	s2Ext         = ".s2"
	snappyExt     = ".sz" // https://github.com/google/snappy/blob/main/framing_format.txt#L34
	minlzBlockExt = ".mzb"
)

var debugErrs bool

func main() {
	var cpuprofile, memprofile, traceprofile string
	if true {
		flag.StringVar(&cpuprofile, "cpuprof", "", "write cpu profile to file")
		flag.StringVar(&memprofile, "memprof", "", "write mem profile to file")
		flag.StringVar(&traceprofile, "traceprof", "", "write trace profile to file")
		flag.Bool("help", false, "print usage")
		flag.BoolVar(&debugErrs, "debug", false, "trace errors")
	}
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "MinLZ compression tool v%v built at %v, (c) 2025 MinIO Inc.\n", version, date)
		fmt.Fprint(w, "Released under Apache 2.0 License. Homepage: https://github.com/minio/minlz\n\n")
		fmt.Fprintf(w, "Usage:\nCompress:     %s c [options] <input>\n", os.Args[0])
		fmt.Fprintf(w, "Decompress:   %s d [options] <input>\n", os.Args[0])
		fmt.Fprintf(w, " (cat)    :   %s cat [options] <input>\n", os.Args[0])
		fmt.Fprintf(w, " (tail)   :   %s tail [options] <input>\n\n", os.Args[0])
		fmt.Fprintf(w, "Without options 'c' and 'd' can be omitted. Extension decides if decompressing.\n")
		fmt.Fprintf(w, "Compress file:    %s file.txt\n", os.Args[0])
		fmt.Fprintf(w, "Compress stdin:   %s -\n", os.Args[0])
		fmt.Fprintf(w, "Decompress file:  %s file.txt.mz\n", os.Args[0])
		fmt.Fprintf(w, "Decompress stdin: %s d -\n", os.Args[0])
		// Don't print flags.
	}
	flag.Parse()

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}
	if traceprofile != "" {
		f, err := os.Create(traceprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		err = trace.Start(f)
		if err != nil {
			log.Fatal(err)
		}
		defer trace.Stop()
	}

	switch flag.Arg(0) {
	case "c", "compress":
		mainCompress(flag.Args()[1:])
	case "d", "decompress", "tail", "cat":
		mainDecompress(flag.Args()[1:], flag.Arg(0) == "cat", flag.Arg(0) == "tail")
	default:
		if len(flag.Args()) > 0 {
			cmp := strings.ToLower(flag.Arg(0))
			for _, ext := range autoDecompressExt {
				if strings.HasSuffix(cmp, ext) {
					mainDecompress(flag.Args(), false, false)
					return
				}
			}
			mainCompress(flag.Args())
		} else {
			flag.Usage()
			os.Exit(1)
		}
	}
}

var autoDecompressExt = []string{
	minlzBlockExt,
	minlzExt,
	s2Ext,
	snappyExt,
	".snappy",
}

func exitErr(err error) {
	if err != nil {
		if debugErrs {
			debug.PrintStack()
		}
		fmt.Fprintln(os.Stderr, "\nERROR:", err.Error())
		os.Exit(2)
	}
}

func isHTTP(name string) bool {
	return strings.HasPrefix(name, "http://") || strings.HasPrefix(name, "https://")
}

type shttpLogger struct{}

func (s shttpLogger) Infof(format string, args ...interface{}) {
	if debugErrs {
		log.Printf(format, args...)
	}
}

func (s shttpLogger) Debugf(format string, args ...interface{}) {
	if debugErrs {
		log.Printf("DEBUG: "+format, args...)
	}
}

func openFile(name string, seek bool) (rc io.ReadCloser, size int64, mode os.FileMode) {
	if isHTTP(name) {
		if seek {
			seeker := shttp.New(name)
			seeker.Logger = shttpLogger{}
			sz, err := seeker.Size()
			exitErr(err)
			return seeker, sz, os.ModePerm
		}
		resp, err := http.Get(name)
		exitErr(err)
		if resp.StatusCode != http.StatusOK {
			exitErr(fmt.Errorf("unexpected response status code %v, want OK", resp.Status))
		}
		return resp.Body, resp.ContentLength, os.ModePerm
	}
	file, err := os.Open(name)
	exitErr(err)
	st, err := file.Stat()
	exitErr(err)
	return file, st.Size(), st.Mode()
}

func cleanFileName(s string) string {
	if isHTTP(s) {
		s = strings.TrimPrefix(s, "http://")
		s = strings.TrimPrefix(s, "https://")
		s = strings.Map(func(r rune) rune {
			switch r {
			case '\\', '/', '*', '?', ':', '|', '<', '>', '~':
				return '_'
			}
			if r < 20 {
				return '_'
			}
			return r
		}, s)
	}
	return s
}

type rCounter struct {
	n  int64
	in io.Reader
}

func (w *rCounter) Read(p []byte) (n int, err error) {
	n, err = w.in.Read(p)
	w.n += int64(n)
	return n, err
}

func (w *rCounter) BytesRead() int64 {
	return w.n
}

func (w *rCounter) Close() (err error) {
	return nil
}

type rCountSeeker struct {
	n  int64
	in io.ReadSeeker
}

func (w *rCountSeeker) Read(p []byte) (n int, err error) {
	n, err = w.in.Read(p)
	w.n += int64(n)
	return n, err
}

func (w *rCountSeeker) Seek(offset int64, whence int) (int64, error) {
	return w.in.Seek(offset, whence)
}

func (w *rCountSeeker) BytesRead() int64 {
	return w.n
}

type wCounter struct {
	n   int
	out io.Writer
}

func (w *wCounter) Write(p []byte) (n int, err error) {
	n, err = w.out.Write(p)
	w.n += n
	return n, err

}

// toSize converts a size indication to bytes.
func toSize(size string) (int64, error) {
	if len(size) == 0 {
		return 0, nil
	}
	size = strings.ToUpper(strings.TrimSpace(size))
	firstLetter := strings.IndexFunc(size, unicode.IsLetter)
	if firstLetter == -1 {
		firstLetter = len(size)
	}

	bytesString, multiple := size[:firstLetter], size[firstLetter:]
	sz, err := strconv.ParseInt(bytesString, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse size: %v", err)
	}

	if sz < 0 {
		return 0, errors.New("negative size given")
	}
	switch multiple {
	case "T", "TB", "TIB":
		return sz * 1 << 40, nil
	case "G", "GB", "GIB":
		return sz * 1 << 30, nil
	case "M", "MB", "MIB":
		return sz * 1 << 20, nil
	case "K", "KB", "KIB":
		return sz * 1 << 10, nil
	case "B", "":
		return sz, nil
	default:
		return 0, fmt.Errorf("unknown size suffix: %v", multiple)
	}
}
