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

//go:build amd64 && !appengine && !noasm && gc && !purego

package kernel

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/klauspost/cpuid/v2"
)

// TestAVX2AsmISA scans the AVX2 tier's generated functions for anything an AVX2-only CPU
// cannot run: on a host with AVX-512 the other tests would not notice an EVEX encoding
// (X16..X31, opmasks, ZMM) or an AVX-512 instruction.
func TestAVX2AsmISA(t *testing.T) {
	allowed := map[string]bool{}
	for m := range strings.FieldsSeq(`
		ADDL ADDQ ADDW ANDL BTL BZHIL CMOVLHI CMOVQLT CMOVQNE CMPL CMPQ DECQ IMUL3Q IMULQ INCQ
		JA JAE JB JBE JE JEQ JG JMP JNC JNE JNZ JZ LEAL LEAQ MOVB MOVBLZX MOVBQZX MOVL MOVQ
		MOVW MOVWLZX NEGQ NOTL ORQ PCALIGN POPCNTL RET RORXL SHLL SHLQ SHLXQ SHRQ SHRXQ SUBQ
		TESTB TESTL TESTQ XORL
		VBROADCASTI128 VEXTRACTI128 VINSERTI128 VMOVD VMOVDQU VMOVQ VPACKUSWB VPADDB VPAND
		VPBLENDVB VPBROADCASTB VPBROADCASTD VPCMPEQB VPERMQ VPMADDUBSW VPMADDWD VPMAXUB
		VPMINUB VPMOVMSKB VPMOVZXBW VPMULLW VPOR VPSHUFB VPSLLW VPSRLDQ VPSRLQ VPSRLW
		VPSUBUSB VPXOR VZEROUPPER`) {
		allowed[m] = true
	}
	evexReg := regexp.MustCompile(`\b([XY](1[6-9]|2[0-9]|3[01])|Z[0-9]+|K[0-7])\b`)
	const f = "asm_amd64.s"
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatal(err)
	}
	// Each function starts with its "// func name(" comment; data follows the code.
	avx2, funcs := false, 0
	for i, line := range strings.Split(string(b), "\n") {
		if name, ok := strings.CutPrefix(line, "// func "); ok {
			avx2 = strings.Contains(name, "AVX2Asm(")
			funcs += b2i(avx2)
		}
		if strings.HasPrefix(line, "DATA") || strings.HasPrefix(line, "GLOBL") {
			avx2 = false
		}
		if !avx2 {
			continue
		}
		if strings.HasPrefix(line, "// Requires:") && strings.Contains(line, "AVX512") {
			t.Errorf("%s:%d: %s", f, i+1, line)
		}
		if !strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "\t//") {
			continue
		}
		fs := strings.Fields(line)
		if len(fs) == 0 {
			continue
		}
		if !allowed[fs[0]] || evexReg.MatchString(line) {
			t.Errorf("%s:%d: not in the AVX2 tier's instruction set: %s", f, i+1, strings.TrimSpace(line))
		}
	}
	if funcs != 6 {
		t.Errorf("%d AVX2 functions, want 6", funcs)
	}
}

// TestAVX2Selected checks that the AVX2 tier is the best one without AVX-512 and that
// it needs BMI2, using a restored snapshot of the cpuid features.
func TestAVX2Selected(t *testing.T) {
	if !hasAVX2() {
		t.Skip("no AVX2")
	}
	saved := cpuid.CPU
	t.Cleanup(func() { cpuid.CPU = saved })
	cpuid.CPU.Disable(cpuid.AVX512F)
	if got := Best(); got != TierAVX2 {
		t.Fatalf("without AVX512F: Best() = %v", got)
	}
	if ts := Tiers(); len(ts) != 2 || ts[0] != TierAVX2 {
		t.Fatalf("without AVX512F: Tiers() = %v", ts)
	}
	if TierAVX2.HistUsesScratch() {
		t.Fatal("the AVX2 histogram is generic")
	}
	cpuid.CPU.Disable(cpuid.BMI2)
	if got := Best(); got != TierGeneric {
		t.Fatalf("without BMI2: Best() = %v", got)
	}
}
