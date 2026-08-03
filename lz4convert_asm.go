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

//go:build amd64 && !appengine && !noasm && gc && !purego

package minlz

// hasLZ4ConvertAsm reports whether cvtLZ4BlockAsm is a real assembly routine.
// This is narrower than hasAsm, which only promises that *some* assembly is in
// use and is true on arm64 as well; there cvtLZ4BlockAsm is still the stub in
// asm_none.go that panics.
const hasLZ4ConvertAsm = true
