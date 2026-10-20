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

package guard

import (
	"fmt"
	"syscall"
	"unsafe"
)

const supported = true

var (
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	virtualAlloc   = kernel32.NewProc("VirtualAlloc")
	virtualProtect = kernel32.NewProc("VirtualProtect")
	virtualFree    = kernel32.NewProc("VirtualFree")
)

const (
	memCommit, memReserve, memRelease = 0x1000, 0x2000, 0x8000
	pageNoAccess, pageReadWrite       = 0x01, 0x04
)

// mapPages returns at least size bytes of whole pages between two PAGE_NOACCESS pages.
func mapPages(size int) ([]byte, func(), error) {
	pg := syscall.Getpagesize()
	body := max((size+pg-1)/pg, 1) * pg
	total := body + 2*pg
	addr, _, err := virtualAlloc.Call(0, uintptr(total), memCommit|memReserve, pageReadWrite)
	if addr == 0 {
		return nil, nil, fmt.Errorf("VirtualAlloc: %w", err)
	}
	free := func() { virtualFree.Call(addr, 0, memRelease) }
	var old uint32
	for _, off := range []int{0, pg + body} {
		if r, _, err := virtualProtect.Call(addr+uintptr(off), uintptr(pg), pageNoAccess, uintptr(unsafe.Pointer(&old))); r == 0 {
			free()
			return nil, nil, fmt.Errorf("VirtualProtect: %w", err)
		}
	}
	// Not unsafe.Pointer(addr): vet flags uintptr conversions it cannot prove valid.
	all := unsafe.Slice((*byte)(*(*unsafe.Pointer)(unsafe.Pointer(&addr))), total)
	return all[pg : pg+body : pg+body], free, nil
}
