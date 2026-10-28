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

//go:build linux || darwin

package guard

import "syscall"

const supported = true

// mapPages returns at least size bytes of whole pages between two PROT_NONE pages.
func mapPages(size int) ([]byte, func(), error) {
	pg := syscall.Getpagesize()
	body := max((size+pg-1)/pg, 1) * pg
	all, err := syscall.Mmap(-1, 0, body+2*pg, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, nil, err
	}
	free := func() { syscall.Munmap(all) }
	for _, g := range [][]byte{all[:pg], all[pg+body:]} {
		if err := syscall.Mprotect(g, syscall.PROT_NONE); err != nil {
			free()
			return nil, nil, err
		}
	}
	return all[pg : pg+body : pg+body], free, nil
}
