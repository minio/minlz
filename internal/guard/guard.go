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

// Package guard provides test memory between inaccessible pages, so accesses outside a buffer fault.
package guard

import "errors"

// Buffer is memory between two inaccessible pages.
type Buffer struct {
	b    []byte
	free func()
}

// New returns a Buffer with room for size bytes.
// Where page protection is unsupported, the memory is not guarded.
func New(size int) (*Buffer, error) {
	b, free, err := mapPages(size)
	if err != nil {
		return nil, err
	}
	return &Buffer{b: b, free: free}, nil
}

// Slice returns n bytes flush against the inaccessible page after them if atEnd,
// else against the one before them.
func (g *Buffer) Slice(n int, atEnd bool) []byte {
	if atEnd {
		return g.b[len(g.b)-n : len(g.b) : len(g.b)]
	}
	return g.b[:n:n]
}

// Free unmaps the Buffer.
func (g *Buffer) Free() {
	g.free()
}

// Map returns n bytes flush against an inaccessible page after them if atEnd, else before them,
// and a function that unmaps them. Where page protection is unsupported, it returns errors.ErrUnsupported.
func Map(n int, atEnd bool) ([]byte, func(), error) {
	if !supported {
		return nil, nil, errors.ErrUnsupported
	}
	g, err := New(n)
	if err != nil {
		return nil, nil, err
	}
	return g.Slice(n, atEnd), g.Free, nil
}
