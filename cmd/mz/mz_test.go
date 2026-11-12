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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/minio/minlz"
)

// TestMain lets tests run the CLI in a child process, since exitErr exits.
func TestMain(m *testing.M) {
	if os.Getenv("MZ_TEST_RUN_MAIN") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestSameFileOutput checks that an output naming the input, in any spelling,
// never truncates the input: mz either refuses or writes via a temp file.
func TestSameFileOutput(t *testing.T) {
	// Larger than the 2 x 1 MiB readahead at -cpu 1, so a truncated input loses data.
	data := bytes.Repeat([]byte("same file output test\n"), 1<<17)
	var buf bytes.Buffer
	w := minlz.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	stream := buf.Bytes()
	block, err := minlz.Encode(nil, data, minlz.LevelBalanced)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		file  string // input, holding in
		in    []byte
		link  string // hard link to file, if set
		args  []string
		out   string   // decodes to data; empty when mz must refuse
		files []string // directory contents afterwards
	}{
		{"d-o", "x.mz", stream, "", []string{"d", "-q", "-o", "x.mz", "x.mz"}, "", []string{"x.mz"}},
		{"d-o-dot", "x.mz", stream, "", []string{"d", "-q", "-o", "./x.mz", "x.mz"}, "", []string{"x.mz"}},
		{"d-rm", "x.mz", stream, "", []string{"d", "-q", "-rm", "-o", "./x.mz", "x.mz"}, "", []string{"x.mz"}},
		{"d-hardlink", "x.mz", stream, "y", []string{"d", "-q", "-o", "y", "x.mz"}, "", []string{"x.mz", "y"}},
		{"d-block", "x.bin", block, "", []string{"d", "-q", "-block", "x.bin"}, "", []string{"x.bin"}},
		{"d-block-debug", "x.bin", block, "", []string{"d", "-q", "-block-debug", "x.bin"}, "", []string{"x.bin"}},
		{"c-block-rm", "x", data, "", []string{"c", "-q", "-block", "-rm", "-o", "./x", "x"}, "", []string{"x"}},
		{"c-sidecar", "x.mzs", data, "", []string{"c", "-q", "-search", "-search.sidecar", "-o", "x", "x.mzs"}, "", []string{"x.mzs"}},
		{"c-safe", "x", data, "", []string{"c", "-q", "-safe", "-o", "./x", "x"}, "", []string{"x"}},
		{"c-safe-recomp", "x.mz", stream, "", []string{"c", "-q", "-safe", "-recomp", "x.mz"}, "", []string{"x.mz"}},
		{"c-o-dot", "x", data, "", []string{"c", "-q", "-cpu", "1", "-o", "./x", "x"}, "x", []string{"x"}},
		{"c-hardlink", "x", data, "y", []string{"c", "-q", "-o", "y", "x"}, "y", []string{"x", "y"}},
		{"c-recomp-sidecar", "x.mz", stream, "", []string{"c", "-q", "-recomp", "-search", "-search.sidecar", "x.mz"}, "x.mz", []string{"x.mz", "x.mz.mzs"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, tc.file)
			if err := os.WriteFile(path, tc.in, 0o644); err != nil {
				t.Fatal(err)
			}
			if tc.link != "" {
				if err := os.Link(path, filepath.Join(dir, tc.link)); err != nil {
					t.Skip("hard links not supported:", err)
				}
			}
			output, err := runMZ(t, dir, "", tc.args...)
			if (err == nil) != (tc.out != "") {
				t.Fatalf("mz %v: got err %v, want refusal %v\n%s", tc.args, err, tc.out == "", output)
			}
			if tc.out != tc.file {
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, tc.in) {
					t.Errorf("input changed (read err %v)", err)
				}
			}
			if tc.out != "" {
				f, err := os.Open(filepath.Join(dir, tc.out))
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if got, err := io.ReadAll(minlz.NewReader(f)); err != nil || !bytes.Equal(got, data) {
					t.Errorf("output does not decode to the input: %v", err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			if !slices.Equal(names, tc.files) {
				t.Errorf("directory has %v, want %v", names, tc.files)
			}
		})
	}
}

// TestOutputSafety checks -safe, -rm and the removal of partial outputs.
func TestOutputSafety(t *testing.T) {
	data := bytes.Repeat([]byte("output safety test\n"), 1<<16)
	var buf bytes.Buffer
	w := minlz.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	stream := buf.Bytes()
	old := []byte("old")

	cases := []struct {
		name  string
		pre   map[string][]byte
		link  string // dangling symlink to "target", if set
		stdin string
		args  []string
		ok    bool
		out   string   // holds or decodes to data, if set
		files []string // directory contents afterwards
	}{
		{"c-safe", map[string][]byte{"x": data, "x.mz": old}, "", "", []string{"c", "-q", "-safe", "x"}, false, "", []string{"x", "x.mz"}},
		{"c-safe-block", map[string][]byte{"x": data, "x.mzb": old}, "", "", []string{"c", "-q", "-safe", "-block", "x"}, false, "", []string{"x", "x.mzb"}},
		{"c-safe-stdin", map[string][]byte{"x": data, "y": old}, "", "x", []string{"c", "-q", "-safe", "-o", "y", "-"}, false, "", []string{"x", "y"}},
		{"c-safe-sidecar", map[string][]byte{"x": data, "x.mz.mzs": old}, "", "", []string{"c", "-q", "-safe", "-search", "-search.sidecar", "x"}, false, "", []string{"x", "x.mz.mzs"}},
		{"c-safe-stdin-sidecar", map[string][]byte{"x": data, "y.mzs": old}, "", "x", []string{"c", "-q", "-safe", "-search", "-search.sidecar", "-o", "y", "-"}, false, "", []string{"x", "y.mzs"}},
		{"c-safe-link", map[string][]byte{"x": data}, "x.mz", "", []string{"c", "-q", "-safe", "x"}, false, "", []string{"x", "x.mz"}},
		{"d-safe", map[string][]byte{"x.mz": stream, "x": old}, "", "", []string{"d", "-q", "-safe", "x.mz"}, false, "", []string{"x", "x.mz"}},
		{"d-safe-stdin", map[string][]byte{"x.mz": stream, "y": old}, "", "x.mz", []string{"d", "-q", "-safe", "-o", "y", "-"}, false, "", []string{"x.mz", "y"}},
		{"d-safe-link", map[string][]byte{"x.mz": stream}, "x", "", []string{"d", "-q", "-safe", "x.mz"}, false, "", []string{"x", "x.mz"}},
		{"sidecar-build-safe", map[string][]byte{"x.mz": stream, "x.mz.mzs": old}, "", "", []string{"sidecar", "build", "-q", "-safe", "x.mz"}, false, "", []string{"x.mz", "x.mz.mzs"}},
		{"sidecar-extract-safe", map[string][]byte{"x.mz": stream, "y": old}, "", "", []string{"sidecar", "extract", "-q", "-safe", "-newstream", "y", "x.mz"}, false, "", []string{"x.mz", "y"}},
		{"c-rm-nodir", map[string][]byte{"x": data}, "", "", []string{"c", "-q", "-rm", "-o", "nodir/x.mz", "x"}, false, "", []string{"x"}},
		{"c-rm-block-nodir", map[string][]byte{"x": data}, "", "", []string{"c", "-q", "-rm", "-block", "-o", "nodir/x", "x"}, false, "", []string{"x"}},
		{"d-rm-nodir", map[string][]byte{"x.mz": stream}, "", "", []string{"d", "-q", "-rm", "-o", "nodir/x", "x.mz"}, false, "", []string{"x.mz"}},
		{"d-rm-offset", map[string][]byte{"x.mz": stream}, "", "", []string{"d", "-q", "-rm", "-offset", "60000", "x.mz"}, false, "", []string{"x.mz"}},
		{"d-rm-limit", map[string][]byte{"x.mz": stream}, "", "", []string{"d", "-q", "-rm", "-limit", "1000", "x.mz"}, false, "", []string{"x.mz"}},
		{"d-stdin-same", map[string][]byte{"x.mz": stream}, "", "x.mz", []string{"d", "-q", "-o", "x.mz", "-"}, false, "", []string{"x.mz"}},
		{"c-stdin-same", map[string][]byte{"x": data}, "", "x", []string{"c", "-q", "-o", "x", "-"}, false, "", []string{"x"}},
		{"c-stdin-same-sidecar", map[string][]byte{"x.mzs": data}, "", "x.mzs", []string{"c", "-q", "-search", "-search.sidecar", "-o", "x", "-"}, false, "", []string{"x.mzs"}},
		{"d-rm-corrupt", map[string][]byte{"x.mz": stream[:len(stream)/2]}, "", "", []string{"d", "-q", "-rm", "x.mz"}, false, "", []string{"x.mz"}},
		// Writes to /dev/full fail after the output is opened.
		{"c-rm-full", map[string][]byte{"x": data}, "", "", []string{"c", "-q", "-rm", "-o", "/dev/full", "x"}, false, "", []string{"x"}},
		{"d-rm-full", map[string][]byte{"x.mz": stream}, "", "", []string{"d", "-q", "-rm", "-o", "/dev/full", "x.mz"}, false, "", []string{"x.mz"}},
		{"c-rm-null", map[string][]byte{"x": data}, "", "", []string{"c", "-q", "-rm", "-o", os.DevNull, "x"}, true, "", nil},
		{"d-rm-null", map[string][]byte{"x.mz": stream}, "", "", []string{"d", "-q", "-rm", "-o", os.DevNull, "x.mz"}, true, "", nil},
		{"c-rm", map[string][]byte{"x": data}, "", "", []string{"c", "-q", "-rm", "x"}, true, "x.mz", []string{"x.mz"}},
		{"d-rm", map[string][]byte{"x.mz": stream}, "", "", []string{"d", "-q", "-rm", "x.mz"}, true, "x", []string{"x"}},
		{"c-block-inplace", map[string][]byte{"x": data}, "", "", []string{"c", "-q", "-block", "-o", "./x", "x"}, true, "x", []string{"x"}},
		// The sidecar can't be created, so the in-place temp file must be removed.
		{"c-inplace-fail", map[string][]byte{"x": data, "x.mzs/f": old}, "", "", []string{"c", "-q", "-search", "-search.sidecar", "-o", "./x", "x"}, false, "", []string{"x", "x.mzs"}},
		// A later input's failure must keep the outputs of earlier inputs.
		{"c-batch", map[string][]byte{"x": data, "y": data, "y.mz": old}, "", "", []string{"c", "-q", "-safe", "x", "y"}, false, "x.mz", []string{"x", "x.mz", "y", "y.mz"}},
		{"c-batch-block", map[string][]byte{"x": data, "y": data, "y.mzb": old}, "", "", []string{"c", "-q", "-safe", "-block", "x", "y"}, false, "x.mzb", []string{"x", "x.mzb", "y", "y.mzb"}},
		{"d-batch", map[string][]byte{"x.mz": stream, "y.mz": stream, "y": old}, "", "", []string{"d", "-q", "-safe", "x.mz", "y.mz"}, false, "x", []string{"x", "x.mz", "y", "y.mz"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, b := range tc.pre {
				p := filepath.Join(dir, name)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, b, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if slices.Contains(tc.args, "/dev/full") {
				if _, err := os.Stat("/dev/full"); err != nil {
					t.Skip("no /dev/full:", err)
				}
			}
			if tc.link != "" {
				if err := os.Symlink(filepath.Join(dir, "target"), filepath.Join(dir, tc.link)); err != nil {
					t.Skip("symlinks not supported:", err)
				}
			}
			output, err := runMZ(t, dir, tc.stdin, tc.args...)
			if (err == nil) != tc.ok {
				t.Fatalf("mz %v: got err %v, want success %v\n%s", tc.args, err, tc.ok, output)
			}
			if !tc.ok {
				for name, b := range tc.pre {
					if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || !bytes.Equal(got, b) {
						t.Errorf("%s changed (read err %v)", name, err)
					}
				}
			}
			if tc.out != "" {
				got, err := os.ReadFile(filepath.Join(dir, tc.out))
				switch {
				case err != nil, tc.args[0] == "d":
				case slices.Contains(tc.args, "-block"):
					got, err = minlz.Decode(nil, got)
				default:
					got, err = io.ReadAll(minlz.NewReader(bytes.NewReader(got)))
				}
				if err != nil || !bytes.Equal(got, data) {
					t.Errorf("%s does not hold the input: %v", tc.out, err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			if !slices.Equal(names, tc.files) {
				t.Errorf("directory has %v, want %v", names, tc.files)
			}
		})
	}
}

// TestOutputModes checks that outputs don't inherit set-ID or sticky bits from the input.
func TestOutputModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no set-ID bits on Windows")
	}
	const special = os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x"), bytes.Repeat([]byte("mode test\n"), 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	// The in-place case runs last, as it replaces x.
	for _, tc := range []struct {
		in, out string
		args    []string
	}{
		{"x", "x.mz", []string{"c", "-q", "x"}},
		{"x", "x.mzb", []string{"c", "-q", "-block", "x"}},
		{"x", "x.mz.mzs", []string{"c", "-q", "-search", "-search.sidecar", "-o", "x.mz", "x"}},
		{"x.mz", "y", []string{"d", "-q", "-o", "y", "x.mz"}},
		{"x.mzb", "z", []string{"d", "-q", "-o", "z", "x.mzb"}},
		{"x", "x", []string{"c", "-q", "-o", "./x", "x"}},
	} {
		in := filepath.Join(dir, tc.in)
		if err := os.Chmod(in, 0o755|special); err != nil {
			// BSDs refuse the sticky bit on a file for a non-root user.
			t.Skip("cannot set special mode bits:", err)
		}
		if st, err := os.Stat(in); err != nil || st.Mode()&special == 0 {
			t.Skip("cannot set special mode bits:", err)
		}
		if output, err := runMZ(t, dir, "", tc.args...); err != nil {
			t.Fatalf("mz %v: %v\n%s", tc.args, err, output)
		}
		st, err := os.Stat(filepath.Join(dir, tc.out))
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode()&special != 0 {
			t.Errorf("mz %v: %s has mode %v", tc.args, tc.out, st.Mode())
		}
	}
}

func TestSearchPrefixBytes(t *testing.T) {
	for _, tc := range []struct {
		literal, esc, want string
	}{
		{"", "", ""},
		{"::", "space, quote,nl,:,{", "\n \":{"},
		{"", "0x10-0x12,65,a,-,!--", "\x10\x11\x12!\"#$%&'()*+,-Aa"},
		{"", "NUL,Tab,cr,lf,apos,comma,backslash", "\x00\t\n\r',\\"},
		{"", "5,0X0A,255", "\x05\n\xff"},
		{"", "😀,é", "\x80\x98\x9f\xa9\xc3\xf0"},
		{"", "\xff", "\xff"},
	} {
		got, err := searchPrefixBytes(tc.literal, tc.esc)
		if err != nil || string(got) != tc.want {
			t.Errorf("%q, %q: got %q, %v; want %q", tc.literal, tc.esc, got, err, tc.want)
		}
	}
	for _, esc := range []string{"a,", "256", "0x100", "0x", "z-a", "é-ü", "-5", "ab", "nlx"} {
		if got, err := searchPrefixBytes("", esc); err == nil {
			t.Errorf("%q: got %q, want an error", esc, got)
		}
	}
}

// TestSearchPrefixesEsc checks that mz c and mz sidecar build use
// -search.prefixes.esc, by the table config mz search -v prints.
func TestSearchPrefixesEsc(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x"), bytes.Repeat([]byte("prefix escape test: {\"a\": 1}\n"), 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		build [][]string
		want  string
	}{
		// Repeats don't count towards the 8 bytes of a byte-prefix table.
		{[][]string{{"c", "-q", "-search", "-search.prefixes", "::::", "-search.prefixes.esc", "space,quote,.,:,[,{,nl", "-o", "x.mz", "x"}},
			`byte-prefix="\n \".:[{"`},
		{[][]string{{"c", "-q", "-o", "x.mz", "x"}, {"sidecar", "build", "-q", "-search.prefixes.esc", "nl,0x80-0x87", "x.mz"}},
			`mask-prefix="\n\x80\x81\x82\x83\x84\x85\x86\x87" (9 bytes)`},
	} {
		for _, args := range tc.build {
			if output, err := runMZ(t, dir, "", args...); err != nil {
				t.Fatalf("mz %v: %v\n%s", args, err, output)
			}
		}
		output, _ := runMZ(t, dir, "", "search", "-v", "-c", "test", "x.mz")
		if !bytes.Contains(output, []byte(tc.want)) {
			t.Errorf("mz %v: want %s in:\n%s", tc.build, tc.want, output)
		}
	}
}

// runMZ runs mz in dir, with stdin read from the file stdin if set.
func runMZ(t *testing.T, dir, stdin string, args ...string) ([]byte, error) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "MZ_TEST_RUN_MAIN=1")
	if stdin != "" {
		f, err := os.Open(filepath.Join(dir, stdin))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		cmd.Stdin = f
	}
	output, err := cmd.CombinedOutput()
	if _, ok := err.(*exec.ExitError); err != nil && !ok {
		t.Fatal(err)
	}
	return output, err
}
