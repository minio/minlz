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

package reference

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	IndexHeader     = "s2idx\x00"
	IndexTrailer    = "\x00xdi2s"
	MaxIndexEntries = 1 << 16
)

// Index represents a MinLZ index.
type Index struct {
	TotalUncompressed int64 // Total Uncompressed size.
	TotalCompressed   int64 // Total Compressed size if known. Will be -1 if unknown.

	// Compressed -> Uncompressed map of block start positions.
	// Not all blocks will be in here
	Blocks []struct {
		CompressedOffset   int64
		UncompressedOffset int64
	}
	estBlockUncomp int64
}

// LoadIndex will load and parse a complete index.
func LoadIndex(b []byte) (*Index, error) {
	if len(b) <= 4+len(IndexHeader)+len(IndexTrailer) {
		return nil, io.ErrUnexpectedEOF
	}
	if b[0] != ChunkIndex {
		return nil, errors.New("unknown chunk type")
	}
	chunkLen := int(b[1]) | int(b[2])<<8 | int(b[3])<<16
	b = b[4:]

	// Validate we have enough...
	if len(b) < chunkLen {
		return nil, io.ErrUnexpectedEOF
	}
	// The above is the standard chunk header.
	// Now parse the rest...
	return LoadIndexAfterHeader(b)
}

// LoadIndexAfterHeader will load and parse an index, after the stream header has been parsed.
func LoadIndexAfterHeader(b []byte) (*Index, error) {
	var i Index
	if !bytes.Equal(b[:len(IndexHeader)], []byte(IndexHeader)) {
		return nil, errors.New("invalid index header")
	}
	b = b[len(IndexHeader):]

	// Total Uncompressed Size
	if v, n := binary.Varint(b); n <= 0 || v < 0 {
		return nil, errors.New("unable to read uncompressed size")
	} else {
		i.TotalUncompressed = v
		b = b[n:]
	}

	// Total Compressed Size (or -1)
	if v, n := binary.Varint(b); n <= 0 {
		return nil, errors.New("unable to read compressed size")
	} else {
		i.TotalCompressed = v
		b = b[n:]
	}

	// Read Estimated Uncompressed Block Size.
	if v, n := binary.Varint(b); n <= 0 {
		return nil, errors.New("unable to read estimated compressed size")
	} else {
		if v < 0 {
			return nil, fmt.Errorf("invalid estimated uncompressed size: %d", v)
		}
		i.estBlockUncomp = v
		b = b[n:]
	}

	var entries int
	if v, n := binary.Varint(b); n <= 0 {
		return nil, errors.New("unable to read entry count")
	} else {
		if v < 0 || v > MaxIndexEntries {
			return nil, fmt.Errorf("invalid entry count: %d", v)
		}
		entries = int(v)
		b = b[n:]
	}
	i.Blocks = make([]struct {
		CompressedOffset   int64
		UncompressedOffset int64
	}, entries)

	if len(b) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	hasUncompressed := b[0]
	b = b[1:]
	if hasUncompressed&1 != hasUncompressed {
		return nil, errors.New("invalid has uncompressed value")
	}

	// Add each uncompressed entry
	for idx := range i.Blocks {
		var uOff int64
		if hasUncompressed != 0 {
			// Load delta
			if v, n := binary.Varint(b); n <= 0 {
				return nil, errors.New("unable to load uncompressed delta")
			} else {
				uOff = v
				b = b[n:]
			}
		}

		if idx > 0 {
			prev := i.Blocks[idx-1].UncompressedOffset
			uOff += prev + (i.estBlockUncomp)
			if uOff <= prev {
				return nil, fmt.Errorf("new uncompressed offset %d less than previous %d", uOff, prev)
			}
		}
		if uOff < 0 {
			return nil, errors.New("negative uncompressed offset")
		}
		i.Blocks[idx].UncompressedOffset = uOff
	}

	// Initial compressed size estimate.
	cPredict := i.estBlockUncomp / 2

	// Add each compressed entry
	for idx := range i.Blocks {
		var cOff int64
		if v, n := binary.Varint(b); n <= 0 {
			return nil, errors.New("unable to load delta")
		} else {
			cOff = v
			b = b[n:]
		}

		if idx > 0 {
			// Update compressed size prediction, with half the error.
			cPredictNew := cPredict + cOff/2

			prev := i.Blocks[idx-1].CompressedOffset
			cOff += prev + cPredict
			if cOff <= prev {
				return nil, fmt.Errorf("new compressed offset %d less than previous %d", cOff, prev)
			}
			cPredict = cPredictNew
		}
		if cOff < 0 {
			return nil, errors.New("negative compressed offset")
		}
		i.Blocks[idx].CompressedOffset = cOff
	}
	if len(b) < 4+len(IndexTrailer) {
		return nil, io.ErrUnexpectedEOF
	}

	// Skip size...
	b = b[4:]

	// Check trailer...
	if !bytes.Equal(b[:len(IndexTrailer)], []byte(IndexTrailer)) {
		return nil, errors.New("invalid index trailer")
	}
	return &i, nil
}
