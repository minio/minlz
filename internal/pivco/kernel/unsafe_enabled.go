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

// We enable 64 bit LE platforms:

//go:build (amd64 || arm64 || ppc64le || riscv64) && !nounsafe && !purego && !appengine

package kernel

import "unsafe"

// Load16 loads a little endian value from b[i:]; it does not check bounds.
func Load16(b []byte, i int) uint16 {
	return *(*uint16)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i))
}

// Load64 loads a little endian value from b[i:]; it does not check bounds.
func Load64(b []byte, i int) uint64 {
	return *(*uint64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i))
}

// Store64 stores v little endian to b[i:]; it does not check bounds.
func Store64(b []byte, i int, v uint64) {
	*(*uint64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i)) = v
}
