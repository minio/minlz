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

// Shared by enc_avx2.go and dec_avx2.go: a RODATA table blob and exact loads and
// stores of up to 16 bytes, which the AVX2 tails need because AVX2 has no byte-masked
// loads or stores.

import (
	"encoding/binary"
	"fmt"

	. "github.com/honeycombio/avo/build"
	. "github.com/honeycombio/avo/operand"
	"github.com/honeycombio/avo/reg"
)

// blob collects one RODATA symbol; GLOBL and DATA are emitted at the end, after the code
// that asked for splat rows.
type blob struct {
	name   string
	b      []byte
	splats map[byte]int
	named  map[string]int
}

func newBlob(name string) *blob {
	return &blob{name: name, splats: map[byte]int{}, named: map[string]int{}}
}

func (t *blob) mem() Mem { return NewDataAddr(NewStaticSymbol(t.name), 0) }

// add appends data at a multiple of align and returns its offset.
func (t *blob) add(data []byte, align int) int {
	for len(t.b)%align != 0 {
		t.b = append(t.b, 0)
	}
	off := len(t.b)
	t.b = append(t.b, data...)
	return off
}

// splat returns the offset of 32 bytes of v.
func (t *blob) splat(v byte) int {
	if off, ok := t.splats[v]; ok {
		return off
	}
	r := make([]byte, 32)
	for i := range r {
		r[i] = v
	}
	off := t.add(r, 32)
	t.splats[v] = off
	return off
}

// shiftOff returns the offset of the 48-byte shuffle table {0x80 x16, 0..15, 0x80 x16}:
// PSHUFB with the 16 bytes at shiftOff+16+k shifts down by k lanes, at shiftOff+16-k up
// by k, zero filling.
func (t *blob) shiftOff() int {
	if off, ok := t.named["shift"]; ok {
		return off
	}
	r := make([]byte, 48)
	for i := range r {
		r[i] = 0x80
	}
	for i := range 16 {
		r[16+i] = byte(i)
	}
	off := t.add(r, 64)
	t.named["shift"] = off
	return off
}

func (t *blob) emit() {
	for len(t.b)%8 != 0 {
		t.b = append(t.b, 0)
	}
	GLOBL(t.name, RODATA|NOPTR)
	for i := 0; i < len(t.b); i += 8 {
		DATA(i, U64(binary.LittleEndian.Uint64(t.b[i:])))
	}
}

var labelSeq int

func uniq(name string) string {
	labelSeq++
	return fmt.Sprintf("%s_%d", name, labelSeq)
}

// loadExact loads the k (0..16) bytes at p into the low lanes of x and zeroes the others,
// reading nothing outside p[:k]. sh addresses the blob's shift table.
func loadExact(p, k reg.Register, x, xt reg.Register, t0, t1 reg.Register, sh Mem, name string) {
	name = uniq(name)
	g0, g1 := t0.(reg.GP), t1.(reg.GP)
	CMPQ(k, U8(8))
	JB(LabelRef(name + "_lt8"))
	VMOVQ(Mem{Base: p}, x)
	VMOVQ(Mem{Base: p, Index: k, Scale: 1, Disp: -8}, xt)
	// Bytes k-8..k-1 go up by k-8 lanes.
	MOVQ(k, t0)
	NEGQ(t0)
	VPSHUFB(Mem{Base: sh.Base, Index: t0, Scale: 1, Disp: sh.Disp + 24}, xt, xt)
	VPOR(xt, x, x)
	JMP(LabelRef(name + "_done"))
	Label(name + "_lt8")
	CMPQ(k, U8(4))
	JB(LabelRef(name + "_lt4"))
	MOVL(Mem{Base: p, Index: k, Scale: 1, Disp: -4}, g1.As32())
	LEAL(Mem{Base: k, Disp: -4}, g0.As32())
	SHLL(U8(3), g0.As32())
	SHLXQ(t0, t1, t1)
	MOVL(Mem{Base: p}, g0.As32())
	ORQ(t1, t0)
	VMOVQ(t0, x)
	JMP(LabelRef(name + "_done"))
	Label(name + "_lt4")
	CMPQ(k, U8(2))
	JB(LabelRef(name + "_lt2"))
	MOVWLZX(Mem{Base: p, Index: k, Scale: 1, Disp: -2}, g1.As32())
	LEAL(Mem{Base: k, Disp: -2}, g0.As32())
	SHLL(U8(3), g0.As32())
	SHLXQ(t0, t1, t1)
	MOVWLZX(Mem{Base: p}, g0.As32())
	ORQ(t1, t0)
	VMOVQ(t0, x)
	JMP(LabelRef(name + "_done"))
	Label(name + "_lt2")
	XORL(g0.As32(), g0.As32())
	TESTQ(k, k)
	JZ(LabelRef(name + "_one"))
	MOVBLZX(Mem{Base: p}, g0.As32())
	Label(name + "_one")
	VMOVQ(t0, x)
	Label(name + "_done")
}

// storeExact stores the low k (0..16) bytes of x at p and nothing else; overlapping
// stores write the same bytes twice.
func storeExact(p, k reg.Register, x, xt reg.Register, t0, t1 reg.Register, sh Mem, name string) {
	name = uniq(name)
	g0, g1 := t0.(reg.GP), t1.(reg.GP)
	CMPQ(k, U8(8))
	JB(LabelRef(name + "_lt8"))
	VMOVQ(x, Mem{Base: p})
	// Bytes k-8..k-1 come down by k-8 lanes.
	VPSHUFB(Mem{Base: sh.Base, Index: k, Scale: 1, Disp: sh.Disp + 8}, x, xt)
	VMOVQ(xt, Mem{Base: p, Index: k, Scale: 1, Disp: -8})
	JMP(LabelRef(name + "_done"))
	Label(name + "_lt8")
	VMOVQ(x, t0)
	CMPQ(k, U8(4))
	JB(LabelRef(name + "_lt4"))
	MOVL(g0.As32(), Mem{Base: p})
	LEAL(Mem{Base: k, Disp: -4}, g1.As32())
	SHLL(U8(3), g1.As32())
	SHRXQ(t1, t0, t0)
	MOVL(g0.As32(), Mem{Base: p, Index: k, Scale: 1, Disp: -4})
	JMP(LabelRef(name + "_done"))
	Label(name + "_lt4")
	CMPQ(k, U8(2))
	JB(LabelRef(name + "_lt2"))
	MOVW(g0.As16(), Mem{Base: p})
	LEAL(Mem{Base: k, Disp: -2}, g1.As32())
	SHLL(U8(3), g1.As32())
	SHRXQ(t1, t0, t0)
	MOVW(g0.As16(), Mem{Base: p, Index: k, Scale: 1, Disp: -2})
	JMP(LabelRef(name + "_done"))
	Label(name + "_lt2")
	TESTQ(k, k)
	JZ(LabelRef(name + "_done"))
	MOVB(g0.As8(), Mem{Base: p})
	Label(name + "_done")
}
