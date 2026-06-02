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

package minlz

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrSidecarInvalid is returned when a sidecar stream is malformed.
var ErrSidecarInvalid = errors.New("minlz: invalid sidecar stream")

// remoteRef is a single block reference inside a 0x47 chunk.
type remoteRef struct {
	offset     int64 // absolute byte offset in the main stream
	uncompSize int   // uncompressed size of the block
}

// appendRemoteBlockRef appends a 0x47 chunk containing a single absolute
// block reference. maxMinusActual = streamMaxBlockSize - blockUncompressedSize.
func appendRemoteBlockRef(dst []byte, blockOffset int64, maxMinusActual int) []byte {
	if blockOffset < 0 || maxMinusActual < 0 {
		return dst
	}
	var pl [binary.MaxVarintLen64 * 2]byte
	n := binary.PutUvarint(pl[:], uint64(blockOffset))
	n += binary.PutUvarint(pl[n:], uint64(maxMinusActual))
	dst = appendChunkHeader(dst, chunkTypeRemoteBlockRef, n)
	return append(dst, pl[:n]...)
}

// parseRemoteBlockRef parses the payload of a 0x47 chunk into one or more
// refs. The first offset is absolute; subsequent offsets within the same
// chunk are relative to the previous (per spec §2.3).
// maxBlockSize is the stream's maximum block size used to compute the actual
// uncompressed block size from the "max - actual" field.
func parseRemoteBlockRef(payload []byte, maxBlockSize int) ([]remoteRef, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("%w: empty 0x47 chunk", ErrSidecarInvalid)
	}
	var refs []remoteRef
	var prev int64 = -1
	for len(payload) > 0 {
		off, n1 := binary.Uvarint(payload)
		if n1 <= 0 {
			return nil, fmt.Errorf("%w: bad offset varint in 0x47", ErrSidecarInvalid)
		}
		payload = payload[n1:]
		minusActual, n2 := binary.Uvarint(payload)
		if n2 <= 0 {
			return nil, fmt.Errorf("%w: bad size varint in 0x47", ErrSidecarInvalid)
		}
		payload = payload[n2:]
		var abs int64
		if prev < 0 {
			abs = int64(off)
		} else {
			abs = prev + int64(off)
			if int64(off) == 0 {
				return nil, fmt.Errorf("%w: non-ascending offset in 0x47", ErrSidecarInvalid)
			}
		}
		if abs < 0 {
			return nil, fmt.Errorf("%w: negative offset in 0x47", ErrSidecarInvalid)
		}
		if int(minusActual) > maxBlockSize {
			return nil, fmt.Errorf("%w: max-actual %d exceeds block size %d", ErrSidecarInvalid, minusActual, maxBlockSize)
		}
		uncomp := maxBlockSize - int(minusActual)
		if uncomp <= 0 || uncomp > maxBlockSize {
			return nil, fmt.Errorf("%w: bogus uncompressed size %d", ErrSidecarInvalid, uncomp)
		}
		refs = append(refs, remoteRef{offset: abs, uncompSize: uncomp})
		prev = abs
	}
	return refs, nil
}

// sidecarOpts holds configuration shared by BuildSidecar/ExtractSidecar.
type sidecarOpts struct {
	cfgs      []SearchTableConfig
	ignoreCRC bool
}

// SidecarOption configures sidecar generation.
type SidecarOption func(*sidecarOpts) error

// SidecarSearchTable adds a search-table config. May be called multiple
// times to embed multiple configs in one sidecar. Each config produces its
// own 0x44 info chunk and per-block 0x45/0x46 chunks. The searcher tries
// every usable config and ANDs the skip decisions.
func SidecarSearchTable(cfg SearchTableConfig) SidecarOption {
	return func(o *sidecarOpts) error {
		if err := cfg.validate(); err != nil {
			return err
		}
		o.cfgs = append(o.cfgs, cfg)
		return nil
	}
}

// SidecarIgnoreCRC disables CRC validation when reading the source stream.
func SidecarIgnoreCRC() SidecarOption {
	return func(o *sidecarOpts) error {
		o.ignoreCRC = true
		return nil
	}
}

// streamBlockSizeFromHeaderByte decodes the header's final byte (after the
// magic body) into the max block size (in bytes). It checks the same
// constraints as the runtime decoder: upper 2 bits must be zero, log2 must
// be within range.
func streamBlockSizeFromHeaderByte(b byte) (int, error) {
	if b&(3<<6) != 0 {
		return 0, ErrCorrupt
	}
	n := int(b&15) + 10
	if n > maxBlockLog {
		return 0, ErrCorrupt
	}
	return 1 << n, nil
}

// appendCfgSearchTableChunk encodes a 0x45 (or 0x46 if compression is
// configured and beneficial) search-table chunk into dst for cfg.
func appendCfgSearchTableChunk(dst []byte, cfg *SearchTableConfig, reductions uint8, table []byte) ([]byte, error) {
	if cfg.compression != nil && cfg.compression.enabled {
		e := cstEncoderPool.Get().(*cstEncoder)
		out, ok, err := appendSearchTableCompressedChunk(dst, cfg, reductions, table, e)
		cstEncoderPool.Put(e)
		if err != nil {
			return nil, err
		}
		if ok {
			return out, nil
		}
	}
	return appendSearchTableChunk(dst, cfg, reductions, table), nil
}

// chunkReader walks a MinLZ stream sequentially, exposing data blocks and
// other chunks. Used by BuildSidecar and ExtractSidecar.
type chunkReader struct {
	src           io.Reader
	bytesConsumed int64
	maxBlockSize  int
	ignoreCRC     bool
	chunkBuf      []byte // scratch for reading chunk payloads
}

// dataBlock holds a decoded data block.
type dataBlock struct {
	decoded     []byte // newly-allocated decoded bytes
	chunkOffset int64  // absolute offset of the chunk header in src
	chunkLen    int    // wire-format chunk length (excluding the 4-byte header)
	chunkType   byte   // chunkTypeUncompressedData / chunkTypeMinLZCompressedData / *CompCRC
	chunkBytes  []byte // verbatim chunk payload (for re-emission); same backing as cr.chunkBuf
}

// readStreamHeader reads the stream identifier and returns maxBlockSize.
func (cr *chunkReader) readStreamHeader() (int, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(cr.src, hdr[:]); err != nil {
		return 0, fmt.Errorf("minlz: reading stream header: %w", err)
	}
	if hdr[0] != ChunkTypeStreamIdentifier {
		return 0, ErrCorrupt
	}
	chunkLen := int(hdr[1]) | int(hdr[2])<<8 | int(hdr[3])<<16
	if chunkLen != magicBodyLen {
		return 0, ErrCorrupt
	}
	var body [magicBodyLen]byte
	if _, err := io.ReadFull(cr.src, body[:]); err != nil {
		return 0, err
	}
	if string(body[:len(magicBody)]) != magicBody {
		return 0, ErrUnsupported
	}
	bs, err := streamBlockSizeFromHeaderByte(body[magicBodyLen-1])
	if err != nil {
		return 0, err
	}
	cr.bytesConsumed = 4 + int64(magicBodyLen)
	cr.maxBlockSize = bs
	return bs, nil
}

// nextChunk reads the next chunk's 4-byte header and returns metadata.
// The payload is NOT yet read; call readChunkPayload/discardChunkPayload next.
func (cr *chunkReader) nextChunk() (chunkType byte, chunkLen int, chunkOffset int64, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(cr.src, hdr[:]); err != nil {
		return 0, 0, 0, err
	}
	chunkType = hdr[0]
	chunkLen = int(hdr[1]) | int(hdr[2])<<8 | int(hdr[3])<<16
	chunkOffset = cr.bytesConsumed
	cr.bytesConsumed += 4 + int64(chunkLen)
	return chunkType, chunkLen, chunkOffset, nil
}

// readChunkPayload reads the payload into cr.chunkBuf and returns a slice.
func (cr *chunkReader) readChunkPayload(chunkLen int) ([]byte, error) {
	if cap(cr.chunkBuf) < chunkLen {
		cr.chunkBuf = make([]byte, chunkLen)
	}
	cr.chunkBuf = cr.chunkBuf[:chunkLen]
	if _, err := io.ReadFull(cr.src, cr.chunkBuf); err != nil {
		return nil, err
	}
	return cr.chunkBuf, nil
}

// discardChunkPayload reads and discards chunkLen bytes.
func (cr *chunkReader) discardChunkPayload(chunkLen int) error {
	if chunkLen == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, cr.src, int64(chunkLen))
	return err
}

// decodeBlockInto decompresses (or copies) a data chunk into dst[:n]. Returns
// the decoded slice (always dst[:n] when n > 0) and any error.
func (cr *chunkReader) decodeBlockInto(chunkType byte, chunkLen int, dst []byte) (decoded []byte, err error) {
	payload, err := cr.readChunkPayload(chunkLen)
	if err != nil {
		return nil, err
	}
	if chunkLen < checksumSize {
		return nil, ErrCorrupt
	}
	checksum := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
	body := payload[checksumSize:]
	switch chunkType {
	case chunkTypeUncompressedData:
		if !cr.ignoreCRC && crc(body) != checksum {
			return nil, ErrCRC
		}
		if cap(dst) < len(body) {
			dst = make([]byte, len(body))
		}
		dst = dst[:len(body)]
		copy(dst, body)
		return dst, nil
	case chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC:
		n, hdrSize, err := decodedLen(body)
		if err != nil {
			return nil, err
		}
		if n > cr.maxBlockSize {
			return nil, ErrTooLarge
		}
		if cap(dst) < n {
			dst = make([]byte, n)
		}
		dst = dst[:n]
		if ret := minLZDecode(dst, body[hdrSize:]); ret != 0 {
			return nil, ErrCorrupt
		}
		toCRC := dst
		if chunkType == chunkTypeMinLZCompressedDataCompCRC {
			toCRC = body[hdrSize:]
		}
		if !cr.ignoreCRC && crc(toCRC) != checksum {
			return nil, ErrCRC
		}
		return dst, nil
	}
	return nil, ErrUnsupported
}

// isDataChunk reports whether chunkType is a data block.
func isDataChunk(chunkType byte) bool {
	return chunkType == chunkTypeUncompressedData ||
		chunkType == chunkTypeMinLZCompressedData ||
		chunkType == chunkTypeMinLZCompressedDataCompCRC
}

// emitEOFChunk writes a chunk-type-0x20 chunk with the encoded uncompressed
// size (typically 0 for sidecars). Returns the number of bytes written.
func emitEOFChunk(dst io.Writer, uncompSize uint64) (int, error) {
	var tmp [4 + binary.MaxVarintLen64]byte
	tmp[0] = chunkTypeEOF
	n := binary.PutUvarint(tmp[4:], uncompSize)
	tmp[1] = uint8(n)
	return dst.Write(tmp[:4+n])
}

// BuildSidecar reads a compressed MinLZ stream from src, generates fresh
// per-block search tables according to the provided SidecarSearchTable
// configs, and writes the sidecar to dst. src is read sequentially and is
// not modified. The 0x47 Remote Block References in the sidecar point at
// the absolute offsets of data blocks within src.
//
// At least one SidecarSearchTable option must be supplied. Multiple configs
// produce multiple 0x44 info chunks and per-block 0x45/0x46 chunks; the
// SidecarSearcher tries each config independently and ANDs the skip results.
//
// Compression-only blocks in src (chunk types 0x02 / 0x03) are decoded to
// build the tables; uncompressed blocks (0x01) are also indexed. Other
// chunks (existing 0x45/0x46 in src, padding, index, user) are skipped.
func BuildSidecar(dst io.Writer, src io.Reader, opts ...SidecarOption) error {
	var o sidecarOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	if len(o.cfgs) == 0 {
		return errors.New("minlz: BuildSidecar requires at least one SidecarSearchTable")
	}

	cr := &chunkReader{src: src, ignoreCRC: o.ignoreCRC}
	maxBlock, err := cr.readStreamHeader()
	if err != nil {
		return err
	}

	// Configure baseTableSize per config and validate it fits.
	for i := range o.cfgs {
		o.cfgs[i].baseTableSize = autoTableSize(maxBlock)
		if o.cfgs[i].baseTableSize < searchTableMinLog2 {
			return fmt.Errorf("minlz: block size %d too small for search tables", maxBlock)
		}
		o.cfgs[i].resolveDefaults()
	}

	// Compute the largest matchLen across configs — we need (matchLen-1)
	// bytes of overlap from the next block when indexing the current.
	maxMatchLen := 0
	for _, c := range o.cfgs {
		if int(c.matchLen) > maxMatchLen {
			maxMatchLen = int(c.matchLen)
		}
	}
	overlapLen := max(0, maxMatchLen-1)

	// Write sidecar stream header + 0x44 info chunks (one per config).
	if _, err := dst.Write(makeHeader(maxBlock)); err != nil {
		return err
	}
	for i := range o.cfgs {
		info := o.cfgs[i].marshalSearchInfoChunk()
		if _, err := dst.Write(info); err != nil {
			return err
		}
	}

	// 2-block lookahead so we can supply overlap to the current block from
	// the next block's first bytes.
	var prevBuf, currBuf []byte
	var prev dataBlock
	havePrev := false
	scratch := make([]byte, 0, 1024)

	emitPrev := func(overlap []byte) error {
		// Build & emit a 0x45/0x46 chunk for every config.
		for i := range o.cfgs {
			cfg := &o.cfgs[i]
			table, reductions := cfg.buildSearchTable(prev.decoded, overlap, nil, false)
			if table == nil {
				continue
			}
			scratch = scratch[:0]
			chunk, err := appendCfgSearchTableChunk(scratch, cfg, reductions, table)
			if err != nil {
				return err
			}
			if _, err := dst.Write(chunk); err != nil {
				return err
			}
			scratch = chunk
		}
		// Emit 0x47 remote block reference.
		var rb [4 + binary.MaxVarintLen64*2]byte
		ref := appendRemoteBlockRef(rb[:0], prev.chunkOffset, maxBlock-len(prev.decoded))
		if _, err := dst.Write(ref); err != nil {
			return err
		}
		return nil
	}

	// streamOpen tracks whether the current sidecar stream still needs an
	// EOF chunk. It flips false on chunkTypeEOF (we just wrote the sidecar
	// EOF) and true again when a new stream identifier opens the next.
	streamOpen := true

	for {
		chunkType, chunkLen, chunkOffset, err := cr.nextChunk()
		if err != nil {
			if err == io.EOF {
				if streamOpen {
					// Source ended without an EOF chunk for the current
					// stream. Flush any pending prev, then close the
					// sidecar's stream with our own EOF.
					if havePrev {
						if err := emitPrev(nil); err != nil {
							return err
						}
					}
					_, err := emitEOFChunk(dst, 0)
					return err
				}
				return nil
			}
			return err
		}
		switch {
		case isDataChunk(chunkType):
			// Decode into the next buffer slot.
			if currBuf == nil {
				currBuf = make([]byte, 0, maxBlock)
			}
			decoded, err := cr.decodeBlockInto(chunkType, chunkLen, currBuf[:0])
			if err != nil {
				return err
			}
			currBuf = decoded[:cap(decoded)]
			curr := dataBlock{
				decoded:     decoded,
				chunkOffset: chunkOffset,
				chunkLen:    chunkLen,
				chunkType:   chunkType,
			}
			if havePrev {
				// Emit prev with overlap drawn from curr's start.
				ovl := decoded
				if len(ovl) > overlapLen {
					ovl = ovl[:overlapLen]
				}
				if err := emitPrev(ovl); err != nil {
					return err
				}
				// Swap buffers so prev points at this block; reuse old prev as next currBuf.
				prevBuf, currBuf = decoded[:cap(decoded)], prevBuf
				prev = curr
			} else {
				// First block — keep as prev, allocate a new currBuf next iteration.
				prevBuf = decoded[:cap(decoded)]
				currBuf = nil
				prev = curr
				havePrev = true
			}

		case chunkType == chunkTypeEOF:
			if chunkLen > binary.MaxVarintLen64 {
				return ErrCorrupt
			}
			if err := cr.discardChunkPayload(chunkLen); err != nil {
				return err
			}
			if havePrev {
				if err := emitPrev(nil); err != nil {
					return err
				}
				havePrev = false
			}
			if _, err := emitEOFChunk(dst, 0); err != nil {
				return err
			}
			streamOpen = false
			// Loop continues — another stream may follow (concatenation).

		case chunkType == ChunkTypeStreamIdentifier:
			// Concatenated stream starts here. Flush any pending prev
			// (no overlap across the stream boundary), then mirror the
			// stream identifier + 0x44 chunks into the sidecar.
			if havePrev {
				if err := emitPrev(nil); err != nil {
					return err
				}
				havePrev = false
			}
			if streamOpen {
				// Previous main stream lacked its EOF chunk. Be strict.
				return ErrCorrupt
			}
			if chunkLen != magicBodyLen {
				return ErrCorrupt
			}
			payload, err := cr.readChunkPayload(chunkLen)
			if err != nil {
				return err
			}
			if string(payload[:len(magicBody)]) != magicBody {
				return ErrUnsupported
			}
			newBs, err := streamBlockSizeFromHeaderByte(payload[magicBodyLen-1])
			if err != nil {
				return err
			}
			maxBlock = newBs
			cr.maxBlockSize = newBs
			for i := range o.cfgs {
				o.cfgs[i].baseTableSize = autoTableSize(newBs)
				if o.cfgs[i].baseTableSize < searchTableMinLog2 {
					return fmt.Errorf("minlz: block size %d too small for search tables", newBs)
				}
				o.cfgs[i].resolveDefaults()
			}
			if _, err := dst.Write(makeHeader(newBs)); err != nil {
				return err
			}
			for i := range o.cfgs {
				info := o.cfgs[i].marshalSearchInfoChunk()
				if _, err := dst.Write(info); err != nil {
					return err
				}
			}
			streamOpen = true
			// Drop reused buffers — they belong to the previous stream.
			currBuf = nil
			prevBuf = nil

		default:
			if chunkType <= maxNonSkippableChunk {
				return ErrUnsupported
			}
			if err := cr.discardChunkPayload(chunkLen); err != nil {
				return err
			}
		}
		_ = prevBuf // keep buffer reachable
	}
}

// ExtractSidecar reads a MinLZ stream from src that already contains search
// tables (0x44/0x45/0x46) and writes a sidecar to sidecarDst. The tables are
// preserved verbatim (no rebuilding).
//
// If newStreamDst is non-nil, src is also re-emitted to it WITHOUT the
// search-table chunks; sidecar offsets reference the new layout. The seek
// index (chunk 0x40) in src is dropped and a fresh one is appended to
// newStreamDst before EOF.
//
// If newStreamDst is nil, sidecar offsets reference src's original layout
// and src must be the stream used for subsequent searches.
func ExtractSidecar(sidecarDst, newStreamDst io.Writer, src io.Reader, opts ...SidecarOption) error {
	var o sidecarOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	if len(o.cfgs) != 0 {
		return errors.New("minlz: ExtractSidecar does not accept SidecarSearchTable; tables are taken from src")
	}

	cr := &chunkReader{src: src, ignoreCRC: o.ignoreCRC}
	maxBlock, err := cr.readStreamHeader()
	if err != nil {
		return err
	}

	// Write sidecar header (no 0x44 yet — we copy 0x44 from src if present).
	if _, err := sidecarDst.Write(makeHeader(maxBlock)); err != nil {
		return err
	}
	// Write new-stream header.
	if newStreamDst != nil {
		if _, err := newStreamDst.Write(makeHeader(maxBlock)); err != nil {
			return err
		}
	}

	// For the new (stripped) stream we may need to rebuild a seek index.
	var newIdx *Index
	if newStreamDst != nil {
		newIdx = &Index{}
		newIdx.reset(maxBlock)
	}

	// Track how many bytes we've written to newStreamDst, to use as
	// sidecar offsets when newStreamDst is non-nil.
	newStreamWritten := int64(magicChunk1Len())
	// uncompWritten across the CURRENT main stream's data blocks (resets at
	// each stream boundary).
	var newUncompWritten int64
	streamOpen := true

	for {
		chunkType, chunkLen, chunkOffset, err := cr.nextChunk()
		if err != nil {
			if err == io.EOF {
				if streamOpen {
					return errors.New("minlz: ExtractSidecar: stream ended without EOF chunk")
				}
				return nil
			}
			return err
		}
		switch {
		case chunkType == chunkTypeSearchInfo, chunkType == chunkTypeSearchTable, chunkType == chunkTypeSearchTableCompressed:
			// Copy verbatim to sidecar.
			payload, err := cr.readChunkPayload(chunkLen)
			if err != nil {
				return err
			}
			hdr := []byte{chunkType, uint8(chunkLen), uint8(chunkLen >> 8), uint8(chunkLen >> 16)}
			if _, err := sidecarDst.Write(hdr); err != nil {
				return err
			}
			if _, err := sidecarDst.Write(payload); err != nil {
				return err
			}

		case isDataChunk(chunkType):
			payload, err := cr.readChunkPayload(chunkLen)
			if err != nil {
				return err
			}
			// Determine uncompressed size for 0x47 + index tracking.
			var uncomp int
			switch chunkType {
			case chunkTypeUncompressedData:
				if chunkLen < checksumSize {
					return ErrCorrupt
				}
				uncomp = chunkLen - checksumSize
			default:
				if chunkLen < checksumSize {
					return ErrCorrupt
				}
				n, _, err := decodedLen(payload[checksumSize:])
				if err != nil {
					return err
				}
				uncomp = n
			}
			if uncomp <= 0 || uncomp > maxBlock {
				return ErrCorrupt
			}
			// Decide reference offset.
			refOffset := chunkOffset
			if newStreamDst != nil {
				refOffset = newStreamWritten
				// Write the data chunk (header + payload) to newStreamDst.
				hdr := [4]byte{chunkType, uint8(chunkLen), uint8(chunkLen >> 8), uint8(chunkLen >> 16)}
				if _, err := newStreamDst.Write(hdr[:]); err != nil {
					return err
				}
				if _, err := newStreamDst.Write(payload); err != nil {
					return err
				}
				if err := newIdx.add(newStreamWritten, newUncompWritten); err != nil {
					return err
				}
				newStreamWritten += 4 + int64(chunkLen)
				newUncompWritten += int64(uncomp)
			}
			// Emit 0x47 to sidecar.
			var rb [4 + binary.MaxVarintLen64*2]byte
			ref := appendRemoteBlockRef(rb[:0], refOffset, maxBlock-uncomp)
			if _, err := sidecarDst.Write(ref); err != nil {
				return err
			}

		case chunkType == chunkTypeEOF:
			if chunkLen > binary.MaxVarintLen64 {
				return ErrCorrupt
			}
			// Discard src's EOF payload (we'll write our own).
			if err := cr.discardChunkPayload(chunkLen); err != nil {
				return err
			}
			// Write sidecar EOF (0 uncompressed payload).
			if _, err := emitEOFChunk(sidecarDst, 0); err != nil {
				return err
			}
			if newStreamDst != nil {
				// EOF for newStream carries the cumulative uncompressed size
				// of the CURRENT stream (per-stream, mirroring main behavior).
				n, err := emitEOFChunk(newStreamDst, uint64(newUncompWritten))
				if err != nil {
					return err
				}
				newStreamWritten += int64(n)
				// Append rebuilt seek index for the just-finished stream and
				// reset the index in case another stream follows.
				idx := newIdx.appendTo(nil, newUncompWritten, -1)
				if _, err := newStreamDst.Write(idx); err != nil {
					return err
				}
				newStreamWritten += int64(len(idx))
				newIdx.reset(maxBlock)
				newUncompWritten = 0
			}
			streamOpen = false
			// Continue — a concatenated stream may follow.

		case chunkType == chunkTypeIndex, chunkType == legacyIndexChunk:
			// Drop the existing index — we may rebuild a fresh one for newStreamDst.
			if err := cr.discardChunkPayload(chunkLen); err != nil {
				return err
			}

		case chunkType == ChunkTypeStreamIdentifier:
			// Concatenated stream begins. Validate the body and mirror
			// the identifier into the sidecar (and newStreamDst).
			if streamOpen {
				return ErrCorrupt
			}
			if chunkLen != magicBodyLen {
				return ErrCorrupt
			}
			payload, err := cr.readChunkPayload(chunkLen)
			if err != nil {
				return err
			}
			if string(payload[:len(magicBody)]) != magicBody {
				return ErrUnsupported
			}
			newBs, err := streamBlockSizeFromHeaderByte(payload[magicBodyLen-1])
			if err != nil {
				return err
			}
			maxBlock = newBs
			cr.maxBlockSize = newBs
			if _, err := sidecarDst.Write(makeHeader(newBs)); err != nil {
				return err
			}
			if newStreamDst != nil {
				if _, err := newStreamDst.Write(makeHeader(newBs)); err != nil {
					return err
				}
				newStreamWritten += int64(magicChunk1Len())
			}
			streamOpen = true

		default:
			if chunkType <= maxNonSkippableChunk {
				return ErrUnsupported
			}
			// Padding & user chunks: pass through to newStreamDst if present;
			// drop from sidecar (sidecar carries only search-related chunks).
			payload, err := cr.readChunkPayload(chunkLen)
			if err != nil {
				return err
			}
			if newStreamDst != nil {
				hdr := []byte{chunkType, uint8(chunkLen), uint8(chunkLen >> 8), uint8(chunkLen >> 16)}
				if _, err := newStreamDst.Write(hdr); err != nil {
					return err
				}
				if _, err := newStreamDst.Write(payload); err != nil {
					return err
				}
				newStreamWritten += 4 + int64(chunkLen)
			}
		}
	}
}

// magicChunk1Len returns the on-wire length of the stream identifier chunk.
func magicChunk1Len() int {
	return len(magicChunk) + 1
}
