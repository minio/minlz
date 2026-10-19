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

// Package cpu holds the CPU features the assembly tiers dispatch on.
// They are plain variables so hot paths test them cheaply, and so tests and cmd/mz
// can clear them after init to run the lower tiers. Only clear them: AVX512 implies AVX2.
package cpu

import (
	"runtime"

	"github.com/klauspost/cpuid/v2"
)

var (
	AVX2   = cpuid.CPU.Supports(cpuid.AVX2, cpuid.BMI2)
	AVX512 = AVX2 && cpuid.CPU.Supports(cpuid.AVX512F, cpuid.AVX512BW, cpuid.AVX512VL, cpuid.AVX512VBMI, cpuid.AVX512VBMI2, cpuid.POPCNT)
	// ASIMD is part of ARMv8.
	NEON = runtime.GOARCH == "arm64"
)
