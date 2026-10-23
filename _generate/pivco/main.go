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

// Command pivco generates the amd64 kernels of internal/pivco/kernel: the AVX-512 and
// AVX2 decoder executors and encoder kernels, and the AVX-512 histogram. They port the
// x86 primitives of PivCo-Huffman (see internal/pivco/doc.go).
package main

//go:generate go run . -out ../../internal/pivco/kernel/asm_amd64.s -stubs ../../internal/pivco/kernel/asm_amd64.go -pkg=kernel
//go:generate go tool asmfmt -w ../../internal/pivco/kernel/asm_amd64.s
//go:generate gofmt -w ../../internal/pivco/kernel/asm_amd64.go

import (
	. "github.com/honeycombio/avo/build"
	"github.com/honeycombio/avo/gotypes"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

func main() {
	ConstraintExpr("!appengine,!noasm,gc,!purego")
	Package("github.com/minio/minlz/internal/pivco/kernel")
	genDecAVX512()
	genDecAVX2()
	genEncAVX512()
	genEncAVX2()
	genHist()
	Generate()
}

func ld(name string) reg.GPVirtual {
	r := GP64()
	Load(Param(name), r)
	return r
}

func field(c gotypes.Component) Mem {
	b, err := c.Resolve()
	if err != nil {
		panic(err)
	}
	return b.Addr
}
