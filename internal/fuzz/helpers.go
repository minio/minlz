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

package fuzz

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"strconv"
	"testing"
)

type InputType uint8

const (
	// TypeRaw indicates that files are raw bytes.
	TypeRaw InputType = iota
	// TypeGoFuzz indicates files are from Go Fuzzer.
	TypeGoFuzz
	// TypeOSSFuzz indicates that files are from OSS fuzzer with size before data.
	TypeOSSFuzz
)

// AddFromZip will read the supplied zip and add all as corpus for f.
// Byte slices only.
func AddFromZip(f *testing.F, filename string, t InputType, short bool) {
	file, err := os.Open(filename)
	if err != nil {
		f.Fatal(err)
	}
	fi, err := file.Stat()
	if err != nil {
		f.Fatal(err)
	}
	zr, err := zip.NewReader(file, fi.Size())
	if err != nil {
		f.Fatal(err)
	}
	for i, file := range zr.File {
		if short && i%10 != 0 {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			f.Fatal(err)
		}

		b, err := io.ReadAll(rc)
		if err != nil {
			f.Fatal(err)
		}
		rc.Close()
		t := t
		if t == TypeOSSFuzz {
			t = TypeRaw // Fallback
			if len(b) >= 4 {
				sz := binary.BigEndian.Uint32(b)
				if sz <= uint32(len(b))-4 {
					f.Add(b[4 : 4+sz])
					continue
				}
			}
		}

		if bytes.HasPrefix(b, []byte("go test fuzz")) {
			t = TypeGoFuzz
		} else {
			t = TypeRaw
		}

		if t == TypeRaw {
			f.Add(b)
			continue
		}
		vals, err := unmarshalCorpusFile(b)
		if err != nil {
			f.Fatal(err)
		}
		for _, v := range vals {
			f.Add(v)
		}
	}
}

// unmarshalCorpusFile decodes corpus bytes into their respective values.
func unmarshalCorpusFile(b []byte) ([][]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("cannot unmarshal empty string")
	}
	lines := bytes.Split(b, []byte("\n"))
	if len(lines) < 2 {
		return nil, fmt.Errorf("must include version and at least one value")
	}
	var vals = make([][]byte, 0, len(lines)-1)
	for _, line := range lines[1:] {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		v, err := parseCorpusValue(line)
		if err != nil {
			return nil, fmt.Errorf("malformed line %q: %v", line, err)
		}
		vals = append(vals, v)
	}
	return vals, nil
}

// parseCorpusValue
func parseCorpusValue(line []byte) ([]byte, error) {
	fs := token.NewFileSet()
	expr, err := parser.ParseExprFrom(fs, "(test)", line, 0)
	if err != nil {
		return nil, err
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return nil, fmt.Errorf("expected call expression")
	}
	if len(call.Args) != 1 {
		return nil, fmt.Errorf("expected call expression with 1 argument; got %d", len(call.Args))
	}
	arg := call.Args[0]

	if arrayType, ok := call.Fun.(*ast.ArrayType); ok {
		if arrayType.Len != nil {
			return nil, fmt.Errorf("expected []byte or primitive type")
		}
		elt, ok := arrayType.Elt.(*ast.Ident)
		if !ok || elt.Name != "byte" {
			return nil, fmt.Errorf("expected []byte")
		}
		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return nil, fmt.Errorf("string literal required for type []byte")
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
	return nil, fmt.Errorf("expected []byte")
}

// ReturnFromZip will read the supplied zip and add all as corpus for f.
// Byte slices only.
func ReturnFromZip(tb testing.TB, filename string, t InputType, fn func([]byte)) {
	file, err := os.Open(filename)
	if err != nil {
		tb.Fatal(err)
	}
	fi, err := file.Stat()
	if err != nil {
		tb.Fatal(err)
	}
	zr, err := zip.NewReader(file, fi.Size())
	if err != nil {
		tb.Fatal(err)
	}
	for _, file := range zr.File {
		rc, err := file.Open()
		if err != nil {
			tb.Fatal(err)
		}

		b, err := io.ReadAll(rc)
		if err != nil {
			tb.Fatal(err)
		}
		rc.Close()
		t := t
		if t == TypeOSSFuzz {
			t = TypeRaw // Fallback
			if len(b) >= 4 {
				sz := binary.BigEndian.Uint32(b)
				if sz <= uint32(len(b))-4 {
					fn(b[4 : 4+sz])
					continue
				}
			}
		}

		if bytes.HasPrefix(b, []byte("go test fuzz")) {
			t = TypeGoFuzz
		} else {
			t = TypeRaw
		}

		if t == TypeRaw {
			fn(b)
			continue
		}
		vals, err := unmarshalCorpusFile(b)
		if err != nil {
			tb.Fatal(err)
		}
		for _, v := range vals {
			fn(v)
		}
	}
}
