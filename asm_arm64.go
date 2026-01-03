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

//go:build arm64 && !appengine && !noasm && gc && !purego

package minlz

// decodeBlockAsm decodes a non-empty src to a guaranteed-large-enough dst.
// It assumes that the varint-encoded length of the decompressed bytes has already been read.
//
//go:noescape
func decodeBlockAsm(dst []byte, src []byte) int
