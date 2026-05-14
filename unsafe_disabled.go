//go:build !(amd64 || arm64 || ppc64le || riscv64) || nounsafe || purego || appengine

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

package minlz

import (
	"encoding/binary"
	"math/bits"
)

func load8(b []byte, i int) byte {
	return b[i]
}

func load16(b []byte, i int) uint16 {
	return binary.LittleEndian.Uint16(b[i:])
}

func load32(b []byte, i int) uint32 {
	return binary.LittleEndian.Uint32(b[i:])
}

func load64(b []byte, i int) uint64 {
	return binary.LittleEndian.Uint64(b[i:])
}

func store8(b []byte, idx int, v uint8) {
	b[idx] = v
}

func store16(b []byte, idx int, v uint16) {
	binary.LittleEndian.PutUint16(b[idx:], v)
}

func store32(b []byte, idx int, v uint32) {
	binary.LittleEndian.PutUint32(b[idx:], v)
}

func tablePopulation(table []byte) (setBits, totalBits int) {
	for _, b := range table {
		setBits += bits.OnesCount8(b)
	}
	return setBits, len(table) * 8
}

func reduceTable(table []byte, origPopcount, maxReducedPopPct int) ([]byte, uint8) {
	if origPopcount == 0 {
		reductions := uint8(0)
		for len(table)/2 >= 32 {
			table = table[:len(table)/2]
			reductions++
		}
		return table, reductions
	}
	reductions := uint8(0)
	for len(table)/2 >= 32 {
		half := len(table) / 2
		lower := table[:half]
		upper := table[half : half+half]
		pop := 0
		for i := range lower {
			pop += bits.OnesCount8(lower[i] | upper[i])
		}
		if pop*100 > half*8*maxReducedPopPct {
			break
		}
		for i := range lower {
			lower[i] |= upper[i]
		}
		table = lower
		reductions++
	}
	return table, reductions
}
