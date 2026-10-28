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

package pivco

import (
	"errors"
	"fmt"
)

const (
	// MaxBlockSize is the largest number of symbols in one block; K_right headers are 16 bits.
	MaxBlockSize = 65535
	// MaxCodeLen is the longest Huffman code length.
	MaxCodeLen = 11
)

var (
	// ErrCorrupt is returned for malformed input.
	ErrCorrupt = errors.New("pivco: corrupt input")
	// ErrBlockSize is returned for blocks of 0 or more than MaxBlockSize symbols.
	ErrBlockSize = errors.New("pivco: block size must be 1..65535")
	// ErrSymbolNotInTable is returned when the input has a symbol without a code.
	ErrSymbolNotInTable = errors.New("pivco: symbol not in table")
	// ErrInvalidTable is returned when a Table was not built successfully.
	ErrInvalidTable = errors.New("pivco: table not built")
)

// Detailed errors are package-level so that returning them never allocates.
var (
	errCodeLen   = fmt.Errorf("%w: code length above %d", ErrCorrupt, MaxCodeLen)
	errNoSymbols = fmt.Errorf("%w: no code lengths", ErrCorrupt)
	errKraft     = fmt.Errorf("%w: code lengths are not a complete prefix code", ErrCorrupt)
	errTruncated = fmt.Errorf("%w: truncated block", ErrCorrupt)
	errSingle    = fmt.Errorf("%w: set bits in a single-symbol bitmap", ErrCorrupt)
	errSplit     = fmt.Errorf("%w: right count exceeds node count", ErrCorrupt)
	errPadding   = fmt.Errorf("%w: non-zero padding bits", ErrCorrupt)
	errMerge     = fmt.Errorf("%w: bitmap does not match its right count", ErrCorrupt)
	errGeometry  = fmt.Errorf("%w: buffer placement out of range", ErrCorrupt)
	errTrailing  = fmt.Errorf("%w: trailing bytes after block", ErrCorrupt)
)
