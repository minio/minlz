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

// We enable 64 bit LE platforms:

//go:build (amd64 || arm64 || ppc64le || riscv64) && !nounsafe && !purego && !appengine

package minlz

import (
	"math/bits"
	"unsafe"
)

func load8(b []byte, i int) byte {
	return *(*byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i))
}

func load16(b []byte, i int) uint16 {
	//return binary.LittleEndian.Uint16(b[i:])
	//return *(*uint16)(unsafe.Pointer(&b[i]))
	return *(*uint16)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i))
}

func load32(b []byte, i int) uint32 {
	//return binary.LittleEndian.Uint32(b[i:])
	//return *(*uint32)(unsafe.Pointer(&b[i]))
	return *(*uint32)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i))
}

func load64(b []byte, i int) uint64 {
	//return binary.LittleEndian.Uint64(b[i:])
	//return *(*uint64)(unsafe.Pointer(&b[i]))
	return *(*uint64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), i))
}

func store8(b []byte, idx int, v uint8) {
	*(*uint8)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), idx)) = v
}

func store16(b []byte, idx int, v uint16) {
	//binary.LittleEndian.PutUint16(b[idx:], v)
	*(*uint16)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), idx)) = v
}

func store32(b []byte, idx int, v uint32) {
	//binary.LittleEndian.PutUint32(b[idx:], v)
	*(*uint32)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(b)), idx)) = v
}

func tablePopulation(table []byte) (setBits, totalBits int) {
	u64s := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(table))), len(table)/8)
	for _, v := range u64s {
		setBits += bits.OnesCount64(v)
	}
	return setBits, len(table) * 8
}

func reduceTable(table []byte, origPopcount, maxReducedPopPct int) ([]byte, uint8) {
	u64s := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(table))), len(table)/8)
	if origPopcount == 0 {
		reductions := uint8(0)
		for len(u64s)/2 >= 4 {
			u64s = u64s[:len(u64s)/2]
			reductions++
		}
		return table[:len(u64s)*8], reductions
	}
	reductions := uint8(0)
	for len(u64s)/2 >= 4 {
		half := len(u64s) / 2
		lower := u64s[:half]
		upper := u64s[half : half+half]
		pop := 0
		for i := range lower {
			pop += bits.OnesCount64(lower[i] | upper[i])
		}
		if pop*100 > half*64*maxReducedPopPct {
			break
		}
		for i := range lower {
			lower[i] |= upper[i]
		}
		u64s = lower
		reductions++
	}
	return table[:len(u64s)*8], reductions
}
