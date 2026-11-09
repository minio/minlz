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
	exe, err := os.Executable()
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
			cmd := exec.Command(exe, tc.args...)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "MZ_TEST_RUN_MAIN=1")
			output, err := cmd.CombinedOutput()
			if _, ok := err.(*exec.ExitError); err != nil && !ok {
				t.Fatal(err)
			}
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
