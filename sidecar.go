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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sync/atomic"
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
// Panics on negative inputs — these indicate a writer bug, and silently
// producing an empty chunk would corrupt the sidecar.
func appendRemoteBlockRef(dst []byte, blockOffset int64, maxMinusActual int) []byte {
	if blockOffset < 0 || maxMinusActual < 0 {
		panic(fmt.Sprintf("minlz: appendRemoteBlockRef: negative input (offset=%d, maxMinusActual=%d)", blockOffset, maxMinusActual))
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
		if minusActual > uint64(maxBlockSize) {
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
	cfgs        []SearchTableConfig
	ignoreCRC   bool
	concurrency int
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

// SidecarConcurrency sets the number of blocks BuildSidecar decodes and
// indexes concurrently. Default is runtime.GOMAXPROCS(0).
func SidecarConcurrency(n int) SidecarOption {
	return func(o *sidecarOpts) error {
		if n <= 0 {
			return errors.New("concurrency must be at least 1")
		}
		o.concurrency = n
		return nil
	}
}

// streamBlockSizeFromHeaderByte decodes the header's final byte (after the
// magic body) into the max block size (in bytes). It checks the same
// constraints as the runtime decoder: upper bit must be zero, log2 must
// be within range.
func streamBlockSizeFromHeaderByte(b byte) (int, error) {
	if b&(1<<7) != 0 {
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
	simd          bool   // the stream identifier allows SIMD chunks
	chunkBuf      []byte // scratch for reading chunk payloads
}

// sidecarOut is BuildSidecar output, written in stream order.
type sidecarOut struct {
	b   []byte
	err error
}

// sidecarQueue orders BuildSidecar output; each entry delivers one sidecarOut.
type sidecarQueue chan chan sidecarOut

// Write queues a copy of b behind the output queued before it.
func (q sidecarQueue) Write(b []byte) (int, error) {
	c := make(chan sidecarOut, 1)
	c <- sidecarOut{b: bytes.Clone(b)}
	q <- c
	return len(b), nil
}

// sidecarBufs recycles BuildSidecar payload and decoded buffers.
// Unlike a sync.Pool it holds at most its capacity and survives GC.
type sidecarBufs chan []byte

// get returns a buffer of length n.
func (p sidecarBufs) get(n int) []byte {
	var b []byte
	select {
	case b = <-p:
	default:
	}
	return slices.Grow(b[:0], n)[:n]
}

func (p sidecarBufs) put(b []byte) {
	select {
	case p <- b:
	default:
	}
}

// sidecarBlock is a data block that BuildSidecar decodes and indexes concurrently.
type sidecarBlock struct {
	ready       chan struct{} // closed when decoded, head and err are set
	decoded     []byte
	head        []byte // overlap for the previous block
	err         error
	chunkOffset int64 // absolute offset of the chunk header in src
	maxBlock    int
	cfgs        []SearchTableConfig
	bufs        sidecarBufs
	// slots limits concurrent decoding and indexing. A slot is only held
	// while working, never while waiting for another block.
	slots chan struct{}
}

func (b *sidecarBlock) decode(chunkType byte, payload []byte, simd, ignoreCRC bool, overlapLen int) {
	b.slots <- struct{}{}
	b.decoded, b.err = decodeDataChunk(b.bufs.get(0), chunkType, payload, 0, b.maxBlock, simd, ignoreCRC)
	<-b.slots
	b.bufs.put(payload)
	// A copy, since decoded is recycled when b is indexed.
	b.head = bytes.Clone(b.decoded[:min(overlapLen, len(b.decoded))])
	close(b.ready)
}

// index returns the search table chunks and 0x47 reference of b.
// next is the following block of the same stream, or nil.
func (b *sidecarBlock) index(next *sidecarBlock, concurrency int) sidecarOut {
	<-b.ready
	if b.err != nil {
		return sidecarOut{err: b.err}
	}
	// Only compressed blocks are checked against the block size when decoded.
	if len(b.decoded) > b.maxBlock {
		return sidecarOut{err: ErrTooLarge}
	}
	defer b.bufs.put(b.decoded)
	var overlap []byte
	if next != nil {
		<-next.ready
		overlap = next.head
	}
	b.slots <- struct{}{}
	defer func() { <-b.slots }()
	var out []byte
	for i := range b.cfgs {
		cfg := &b.cfgs[i]
		table, reductions := cfg.buildSearchTable(b.decoded, overlap, nil, cfg.shouldPack(concurrency))
		if table == nil {
			continue
		}
		var err error
		if out, err = appendCfgSearchTableChunk(out, cfg, reductions, table); err != nil {
			return sidecarOut{err: err}
		}
	}
	return sidecarOut{b: appendRemoteBlockRef(out, b.chunkOffset, b.maxBlock-len(b.decoded))}
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
	cr.simd = body[magicBodyLen-1]&streamFlagSIMD != 0
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

// isDataChunk reports whether chunkType is a data block.
func isDataChunk(chunkType byte) bool {
	return chunkType == chunkTypeUncompressedData ||
		chunkType == chunkTypeMinLZCompressedData ||
		chunkType == chunkTypeMinLZCompressedDataCompCRC ||
		chunkType == chunkTypeMinLZSIMD
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
// Compression-only blocks in src (chunk types 0x02 / 0x03 / 0x10) are decoded to
// build the tables; uncompressed blocks (0x01) are also indexed. Other
// chunks (existing 0x45/0x46 in src, padding, index, user) are skipped.
func BuildSidecar(dst io.Writer, src io.Reader, opts ...SidecarOption) (err error) {
	o := sidecarOpts{concurrency: runtime.GOMAXPROCS(0)}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	if len(o.cfgs) == 0 {
		return errors.New("minlz: BuildSidecar requires at least one SidecarSearchTable")
	}

	cr := &chunkReader{src: src}
	maxBlock, err := cr.readStreamHeader()
	if err != nil {
		return err
	}

	// Blocks of the previous stream may still be indexing, so each stream
	// gets its own copy of the configs.
	streamCfgs := func(bs int) ([]SearchTableConfig, error) {
		cfgs := slices.Clone(o.cfgs)
		for i := range cfgs {
			cfgs[i].baseTableSize = autoTableSize(bs)
			if cfgs[i].baseTableSize < searchTableMinLog2 {
				return nil, fmt.Errorf("minlz: block size %d too small for search tables", bs)
			}
			cfgs[i].resolveDefaults()
		}
		return cfgs, nil
	}
	cfgs, err := streamCfgs(maxBlock)
	if err != nil {
		return err
	}

	// Compute the largest overlap needed across configs. Type 4 with
	// extras=E needs matchLen+E-1 bytes of overlap from the next block;
	// other types need matchLen-1.
	overlapLen := 0
	for _, c := range o.cfgs {
		if n := c.overlapBytes(); n > overlapLen {
			overlapLen = n
		}
	}

	// Write sidecar stream header + 0x44 info chunks (one per config).
	if _, err := dst.Write(makeHeader(maxBlock, cr.simd)); err != nil {
		return err
	}
	for i := range cfgs {
		if _, err := dst.Write(cfgs[i].marshalSearchInfoChunk()); err != nil {
			return err
		}
	}

	// Blocks are decoded and indexed concurrently; q writes the output in stream order.
	q := make(sidecarQueue, o.concurrency)
	// Each block in flight holds up to a payload and a decoded buffer.
	bufs := make(sidecarBufs, 2*(o.concurrency+3))
	slots := make(chan struct{}, o.concurrency)
	var failed atomic.Bool
	werr := make(chan error, 1)
	go func() {
		var err error
		for c := range q {
			r := <-c
			if err == nil {
				if err = r.err; err == nil {
					_, err = dst.Write(r.b)
				}
				failed.Store(err != nil)
			}
		}
		werr <- err
	}()
	defer func() {
		close(q)
		// Queued output comes before the point where the loop stopped, so its error wins.
		if e := <-werr; e != nil {
			err = e
		}
	}()

	// prev is indexed once the next block supplies its overlap.
	var prev *sidecarBlock
	emitPrev := func(next *sidecarBlock) {
		blk := prev
		c := make(chan sidecarOut, 1)
		q <- c
		go func() { c <- blk.index(next, o.concurrency) }()
	}

	// streamOpen tracks whether the current sidecar stream still needs an
	// EOF chunk. It flips false on chunkTypeEOF (we just wrote the sidecar
	// EOF) and true again when a new stream identifier opens the next.
	streamOpen := true

	for {
		if failed.Load() {
			// The deferred func returns the write error.
			return nil
		}
		chunkType, chunkLen, chunkOffset, err := cr.nextChunk()
		if err != nil {
			if err == io.EOF {
				if streamOpen {
					// Source ended without an EOF chunk for the current
					// stream. Flush any pending prev, then close the
					// sidecar's stream with our own EOF.
					if prev != nil {
						emitPrev(nil)
					}
					_, err := emitEOFChunk(q, 0)
					return err
				}
				return nil
			}
			return err
		}
		switch {
		case isDataChunk(chunkType):
			payload := bufs.get(chunkLen)
			if _, err := io.ReadFull(cr.src, payload); err != nil {
				return err
			}
			curr := &sidecarBlock{ready: make(chan struct{}), chunkOffset: chunkOffset, maxBlock: maxBlock, cfgs: cfgs, bufs: bufs, slots: slots}
			go curr.decode(chunkType, payload, cr.simd, o.ignoreCRC, overlapLen)
			if prev != nil {
				emitPrev(curr)
			}
			prev = curr

		case chunkType == chunkTypeEOF:
			if chunkLen > binary.MaxVarintLen64 {
				return ErrCorrupt
			}
			if err := cr.discardChunkPayload(chunkLen); err != nil {
				return err
			}
			if !streamOpen {
				// An EOF chunk after an EOF chunk is an empty stream (SPEC 4.6), which has no sidecar stream.
				continue
			}
			if prev != nil {
				emitPrev(nil)
				prev = nil
			}
			if _, err := emitEOFChunk(q, 0); err != nil {
				return err
			}
			streamOpen = false
			// Loop continues — another stream may follow (concatenation).

		case chunkType == ChunkTypeStreamIdentifier:
			// Concatenated stream starts here. Flush any pending prev
			// (no overlap across the stream boundary), then mirror the
			// stream identifier + 0x44 chunks into the sidecar.
			if prev != nil {
				emitPrev(nil)
				prev = nil
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
			cr.simd = payload[magicBodyLen-1]&streamFlagSIMD != 0
			if cfgs, err = streamCfgs(newBs); err != nil {
				return err
			}
			if _, err := q.Write(makeHeader(newBs, cr.simd)); err != nil {
				return err
			}
			for i := range cfgs {
				info := cfgs[i].marshalSearchInfoChunk()
				if _, err := q.Write(info); err != nil {
					return err
				}
			}
			streamOpen = true

		default:
			if chunkType <= maxNonSkippableChunk {
				return ErrUnsupported
			}
			if err := cr.discardChunkPayload(chunkLen); err != nil {
				return err
			}
		}
	}
}

// ExtractSidecar reads a MinLZ stream from src that already contains search
// tables (0x44/0x45/0x46) and writes a sidecar to sidecarDst. The tables are
// preserved verbatim (no rebuilding).
//
// If newStreamDst is non-nil, src is also re-emitted to it WITHOUT the
// search-table chunks; sidecar offsets reference the new layout. The seek
// index (chunk 0x40) in src is dropped and a fresh one is appended to
// newStreamDst after the EOF chunk (mirroring Writer.closeIndex).
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

	cr := &chunkReader{src: src}
	maxBlock, err := cr.readStreamHeader()
	if err != nil {
		return err
	}

	// Write sidecar header (no 0x44 yet — we copy 0x44 from src if present).
	if _, err := sidecarDst.Write(makeHeader(maxBlock, cr.simd)); err != nil {
		return err
	}
	// Write new-stream header.
	if newStreamDst != nil {
		if _, err := newStreamDst.Write(makeHeader(maxBlock, cr.simd)); err != nil {
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
			case chunkTypeMinLZSIMD:
				if !cr.simd {
					return ErrCorrupt
				}
				if uncomp, _, err = simdChunkHeader(payload, maxBlock); err != nil {
					return err
				}
			default:
				if chunkLen < checksumSize {
					return ErrCorrupt
				}
				n, _, err := minLZChunkLen(payload[checksumSize:])
				if err != nil {
					return err
				}
				uncomp = n
			}
			if uncomp <= 0 {
				return ErrCorrupt
			}
			if uncomp > maxBlock {
				return ErrTooLarge
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
			if !streamOpen {
				// An EOF chunk after an EOF chunk is an empty stream (SPEC 4.6), which has no sidecar stream.
				continue
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
			cr.simd = payload[magicBodyLen-1]&streamFlagSIMD != 0
			if _, err := sidecarDst.Write(makeHeader(newBs, cr.simd)); err != nil {
				return err
			}
			if newStreamDst != nil {
				if _, err := newStreamDst.Write(makeHeader(newBs, cr.simd)); err != nil {
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
