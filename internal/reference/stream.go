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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	ChunkLegacyCompressed = 0x00
	ChunkUncompressed     = 0x01
	ChunkMinLZBlock       = 0x02
	ChunkMinLZCompCRC     = 0x03
	ChunkIndex            = 0x40
	ChunkEOF              = 0x20
	ChunkPadding          = 0xfe
	ChunkStreamID         = 0xff

	maxNonSkippableChunk     = 0x3f
	minUserSkippableChunk    = 0x80
	maxUserSkippableChunk    = 0xbf
	minUserNonSkippableChunk = 0xc0
	maxUserNonSkippableChunk = 0xfd
)

func ReadStream(r io.Reader, debugOut io.Writer) error {
	// Print debug information to debugOut if it is not nil.
	println := func(args ...interface{}) {
		if debugOut != nil {
			_, _ = fmt.Fprintln(debugOut, args...)
		}
	}

	// Verify checksum of buffer
	var crcTable = crc32.MakeTable(crc32.Castagnoli)
	verifyChecksum := func(b []byte, want uint32) error {
		c := crc32.Update(0, crcTable, b)
		c = c>>15 | c<<17 + 0xa282ead8
		if c != want {
			return fmt.Errorf("CRC mismatch: got %08x, want %08x", c, want)
		}
		return nil
	}
	var readMaxBlockSize = 0
	var tmp [8]byte
	decompressedSize := 0
	for {
		// Read the next byte from the stream.
		// This is the chunk type.
		_, err := io.ReadFull(r, tmp[:1])
		if err != nil {
			if err == io.EOF {
				return nil
			}
			println("Error reading chunk type:", err)
			return err
		}
		chunkType := tmp[0]

		// The next 3 bytes are the chunk length as "little endian".
		_, err = io.ReadFull(r, tmp[:3])
		if err != nil {
			println("Error reading chunk length:", err)
			return err
		}
		chunkLen := int(tmp[0]) | int(tmp[1])<<8 | int(tmp[2])<<16
		println("Chunk type:", chunkType, "Chunk length:", chunkLen)

		// Read the chunk data.
		chunkData := make([]byte, chunkLen)
		_, err = io.ReadFull(r, chunkData)
		if err != nil {
			println("Error reading chunk data", err)
			return err
		}

		switch chunkType {
		case ChunkLegacyCompressed:
			println("Legacy compressed data")
			continue
		case ChunkUncompressed:
			if len(chunkData) < 4 {
				println("Error: uncompressed data too short")
				return fmt.Errorf("uncompressed data too short")
			}
			println("Uncompressed data")
			// First 4 bytes are the CRC checksum.
			checksum := uint32(chunkData[0]) | uint32(chunkData[1])<<8 | uint32(chunkData[2])<<16 | uint32(chunkData[3])<<24
			chunkData = chunkData[4:]
			if err = verifyChecksum(chunkData, checksum); err != nil {
				println(err)
				return err
			}
			println("Checksum verified")
			if len(chunkData) > readMaxBlockSize {
				err := fmt.Errorf("uncompressed data too large: %d > %d", len(chunkData), readMaxBlockSize)
				println(err)
				return err
			}
			decompressedSize += len(chunkData)
			continue
		case ChunkMinLZBlock:
			if len(chunkData) < 4 {
				println("Error: compressed data too short")
				return fmt.Errorf("compressed data too short")
			}
			println("MinLZ compressed data")
			// First 4 bytes are the CRC checksum.
			checksum := uint32(chunkData[0]) | uint32(chunkData[1])<<8 | uint32(chunkData[2])<<16 | uint32(chunkData[3])<<24
			chunkData = chunkData[4:]
			decoded, err := DecodeBlock(chunkData)
			if err != nil {
				println("Error decoding MinLZ compressed data:", err)
				return err
			}
			println("block decoded to", len(decoded), "bytes")
			if len(chunkData) > len(decoded) {
				err := fmt.Errorf("compressed data size %d larger than compressed size %d", len(decoded), len(chunkData))
				println(err)
				return err
			}

			if len(decoded) > readMaxBlockSize {
				err := fmt.Errorf("uncompressed data too large: %d > %d", len(chunkData), readMaxBlockSize)
				println(err)
				return err
			}

			if err = verifyChecksum(decoded, checksum); err != nil {
				println(err)
				return err
			}
			println("Checksum verified")
			decompressedSize += len(decoded)
			continue

		case ChunkMinLZCompCRC:
			if len(chunkData) < 4 {
				println("Error: compressed data too short")
				return fmt.Errorf("compressed data too short")
			}
			println("MinLZ compressed data")
			// First 4 bytes are the CRC checksum.
			checksum := uint32(chunkData[0]) | uint32(chunkData[1])<<8 | uint32(chunkData[2])<<16 | uint32(chunkData[3])<<24
			chunkData = chunkData[4:]
			// Verify the checksum of the compressed data.
			if err = verifyChecksum(chunkData, checksum); err != nil {
				println(err)
				return err
			}
			println("Checksum verified")
			// Decode the compressed data.
			decoded, err := DecodeBlock(chunkData)
			if err != nil {
				println("Error decoding MinLZ compressed data:", err)
				return err
			}
			println("block decoded to", len(decoded), "bytes")
			if len(chunkData) > len(decoded) {
				err := fmt.Errorf("compressed data size %d larger than compressed size %d", len(decoded), len(chunkData))
				println(err)
				return err
			}
			if len(decoded) > readMaxBlockSize {
				err := fmt.Errorf("uncompressed data too large: %d > %d", len(chunkData), readMaxBlockSize)
				println(err)
				return err
			}
			decompressedSize += len(decoded)
			continue

		case ChunkEOF:
			println("MinLZ compressed data")
			if chunkLen > binary.MaxVarintLen64 {
				println("Error: EOF chunk length too large")
				return fmt.Errorf("EOF chunk length too large (%d)", chunkLen)
			}
			// Read the varint-encoded length of the decompressed data if set.
			if chunkLen != 0 {
				wantSize, n := binary.Uvarint(chunkData)
				if n != chunkLen {
					err = fmt.Errorf("EOF chunk was not varint-encoded. got %d, want %d", n, chunkLen)
					println("Error reading EOF chunk:", err)
					return err
				}
				if int(wantSize) != decompressedSize {
					err = fmt.Errorf("stream size mismatch: got %d bytes, want %d", decompressedSize, wantSize)
					println(err)
					return err
				}
				println("Stream size verified")
			}
			// We continue since multiple streams can be concatenated.
			continue
		case ChunkStreamID:
			println("Stream identifier")
			const magicBody = "MinLz"
			if len(chunkData) != len(magicBody)-1 {
				err := fmt.Errorf("stream identifier length mismatch: got %d, want %d", chunkLen, len(magicBody)+1)
				println(err)
				return err
			}
			if string(chunkData[:len(magicBody)]) != magicBody {
				err := fmt.Errorf("stream identifier mismatch: got %q, want %q", chunkData[:len(magicBody)], magicBody)
				println(err)
				return err
			}
			// Trim off the magic
			chunkData = chunkData[:len(magicBody)]

			// Upper 2 bits most be 0
			if chunkData[0]>>6 != 0 {
				err := fmt.Errorf("reserved bits set: %d", chunkData[0]>>6)
				println(err)
				return err
			}

			// Read max block size
			readMaxBlockSize = 1 << ((uint)(chunkData[0]&15) + 10)
			if readMaxBlockSize > maxBlockSize {
				err := fmt.Errorf("invalid max block size: %d", readMaxBlockSize)
				println(err)
				return err
			}
			println("Max Block Size:", readMaxBlockSize)

		case ChunkIndex:
			// Optional index.
			// We have already parsed the chunk header, so the chunkdata is the index
			idx, err := LoadIndexAfterHeader(chunkData)
			if err != nil {
				err := fmt.Errorf("unable to parse index: %w", err)
				println(err)
				return err
			}
			println("Loaded index:", idx)

		default:
			// Handle "unknown" chunks.
			// 0x00 -> 0x3f
			if chunkType <= maxNonSkippableChunk {
				err := fmt.Errorf("unknown unskippable block found. id: %d", chunkType)
				println(err)
				return err
			}
			// 0x40 -> 0x7f
			if chunkType > maxNonSkippableChunk && chunkType < minUserSkippableChunk {
				println("Found internal skippable chunk with id", chunkType, "length", len(chunkData))
				continue
			}
			// 0x80 -> 0xbf
			if chunkType >= minUserSkippableChunk && chunkType <= maxUserSkippableChunk {
				println("Found user skippable chunk with id", chunkType, "length", len(chunkData))
				continue
			}
			// 0xc0 -> 0xbf
			if chunkType >= minUserNonSkippableChunk && chunkType <= maxUserNonSkippableChunk {
				println("Found user non-skippable chunk with id", chunkType, "length", len(chunkData))
				continue
			}

			if chunkType == ChunkPadding {
				println("Found padding, length", len(chunkData))
				continue
			}
		}
	}
}
