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
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"
	"runtime"
	"sync"
)

// NewWriter returns a new Writer that compresses as an MinLZ stream to w.
//
// Users must call Close to guarantee all data has been forwarded to
// the underlying io.Writer and that resources are released.
func NewWriter(w io.Writer, opts ...WriterOption) *Writer {
	w2 := Writer{
		blockSize:   defaultBlockSize,
		concurrency: runtime.GOMAXPROCS(0),
		randSrc:     rand.Reader,
		level:       LevelBalanced,
		genIndex:    true,
	}
	for _, opt := range opts {
		if err := opt(&w2); err != nil {
			w2.errState = err
			return &w2
		}
	}
	if w2.searchCfg != nil {
		w2.searchCfg.baseTableSize = autoTableSize(w2.blockSize)
		if w2.searchCfg.baseTableSize < searchTableMinLog2 {
			w2.errState = fmt.Errorf("minlz: block size %d too small for search tables (min table size %d bits)",
				w2.blockSize, 1<<searchTableMinLog2)
			return &w2
		}
		w2.searchCfg.resolveDefaults()
	}
	if w2.sidecar != nil {
		if w2.searchCfg == nil {
			w2.errState = errors.New("minlz: WriterSidecar requires WriterSearchTable")
			return &w2
		}
		w2.sidecarMaxBlock = w2.blockSize
	}
	w2.obufLen = obufHeaderLen + MaxEncodedLen(w2.blockSize)
	if w2.searchCfg != nil {
		// 0x46 is only emitted when strictly smaller than the equivalent
		// 0x45, so a single reservation of cfg.maxChunkSize() covers both
		// chunk forms — no headroom needed.
		w2.searchMaxChunk = w2.searchCfg.maxChunkSize()
		w2.obufLen += w2.searchMaxChunk
	}
	w2.paramsOK = true
	w2.ibuf = make([]byte, 0, w2.blockSize)
	w2.buffers.New = func() interface{} {
		return make([]byte, w2.obufLen)
	}
	w2.Reset(w)
	return &w2
}

// Writer is an io.Writer that can write Snappy-compressed bytes.
type Writer struct {
	errMu    sync.Mutex
	errState error

	// ibuf is a buffer for the incoming (uncompressed) bytes.
	ibuf []byte

	blockSize     int
	obufLen       int
	concurrency   int
	written       int64
	uncompWritten int64 // Bytes sent to compression
	output        chan chan result
	buffers       sync.Pool
	pad           int

	writer    io.Writer
	randSrc   io.Reader
	writerWg  sync.WaitGroup
	index     *Index
	customEnc func(dst, src []byte) int

	searchCfg      *SearchTableConfig
	searchInfoBuf  []byte // cached 0x44 chunk
	searchHdrBuf   []byte // scratch 0x45 header (sync path)
	searchMaxChunk int    // max 0x45 chunk size, 0 if no search

	// sidecar holds the optional sidecar destination. When non-nil, the
	// 0x44/0x45/0x46 chunks and a 0x47 remote-block-reference per data
	// block are written to sidecar; the main writer receives only the
	// stream header, data chunks, EOF, and (optionally) the seek index.
	sidecar io.Writer
	// sidecarHeaderWritten tracks whether the sidecar stream header and
	// 0x44 info chunk have been emitted.
	sidecarHeaderWritten bool
	// sidecarMaxBlock is the stream's max block size; used to compute the
	// (max - actual) field encoded in 0x47 chunks. Derived from w.blockSize
	// at first emit.
	sidecarMaxBlock int

	// wroteStreamHeader is whether we have written the stream header.
	wroteStreamHeader bool
	paramsOK          bool
	flushOnWrite      bool
	appendIndex       bool
	genIndex          bool
	level             int8
}

type result struct {
	b []byte
	// pooled is the underlying w.buffers buffer that b is sliced from
	// (b's cap may be reduced by front-padding for search chunks). The
	// writer goroutine returns this to w.buffers after writing b. Nil for
	// results whose b is not pool-owned (stream headers, search-info
	// chunk, flush sentinels).
	pooled []byte
	// Uncompressed start offset
	startOffset int64

	// Sidecar fields (only populated when Writer.sidecar != nil).
	// sidecarPre holds the 0x45/0x46 search-table chunk to write to the
	// sidecar immediately before the corresponding 0x47 reference. It may
	// be nil if no search table was produced for this block.
	sidecarPre []byte
	// uncompSize is the block's uncompressed size, used to encode the
	// 0x47 chunk's (maxUncompressed - actualUncompressed) field. A value
	// of 0 signals "no sidecar emission for this result" — for example
	// when the result represents the stream header or a flush sentinel.
	uncompSize int
}

var (
	errClosed    = errors.New("minlz: Writer is closed")
	errNilWriter = errors.New("minlz: Writer has not been set")
)

// err returns the previously set error.
// If no error has been set it is set to err if not nil.
func (w *Writer) err(err error) error {
	w.errMu.Lock()
	errSet := w.errState
	if errSet == nil && err != nil {
		w.errState = err
		errSet = err
	}
	w.errMu.Unlock()
	return errSet
}

// Reset discards the writer's state and switches the Snappy writer to write to w.
// This permits reusing a Writer rather than allocating a new one.
func (w *Writer) Reset(writer io.Writer) {
	if !w.paramsOK {
		return
	}
	// Close previous writer, if any.
	if w.output != nil {
		close(w.output)
		w.writerWg.Wait()
		w.output = nil
	}
	if w.genIndex && w.index == nil {
		w.index = &Index{}
	}
	w.errState = nil
	w.ibuf = w.ibuf[:0]
	w.wroteStreamHeader = false
	w.searchInfoBuf = nil
	w.written = 0
	w.writer = writer
	w.uncompWritten = 0
	w.sidecarHeaderWritten = false
	w.index.reset(w.blockSize)

	// If we didn't get a writer, stop here.
	if writer == nil {
		w.err(errNilWriter)
		return
	}
	// If no concurrency requested, don't spin up writer goroutine.
	if w.concurrency == 1 {
		return
	}

	toWrite := make(chan chan result, w.concurrency)
	w.output = toWrite
	w.writerWg.Add(1)

	// Start a writer goroutine that will write all output in order.
	go func() {
		defer w.writerWg.Done()

		// Get a queued write.
		for write := range toWrite {
			// Wait for the data to be available.
			input := <-write
			in := input.b
			// Record the main offset where this block's data will land, BEFORE
			// the main Write advances w.written. The sidecar's 0x47 chunk
			// uses this offset.
			mainBlockOffset := w.written
			if len(in) > 0 {
				if w.err(nil) == nil {
					// Don't expose data from previous buffers.
					toWrite := in[:len(in):len(in)]
					// Write to output.
					n, err := writer.Write(toWrite)
					if err == nil && n != len(toWrite) {
						err = io.ErrShortBuffer
					}
					_ = w.err(err)
					w.err(w.index.add(w.written, input.startOffset))
					w.written += int64(n)
				}
			}
			// Sidecar emission for data blocks. uncompSize > 0 indicates the
			// result represents a data block (not the stream header or a
			// flush sentinel).
			if w.sidecar != nil && input.uncompSize > 0 && w.err(nil) == nil {
				if err := w.writeSidecarStartIfNeeded(); err == nil {
					if len(input.sidecarPre) > 0 {
						if _, err := w.sidecar.Write(input.sidecarPre); err != nil {
							_ = w.err(err)
						}
					}
					if w.err(nil) == nil {
						_ = w.writeSidecarRemoteRef(mainBlockOffset, input.uncompSize)
					}
				}
			}
			if input.pooled != nil {
				w.buffers.Put(input.pooled)
			} else if cap(in) >= w.obufLen {
				w.buffers.Put(in)
			}
			// close the incoming write request.
			// This can be used for synchronizing flushes.
			close(write)
		}
	}()
}

// Write satisfies the io.Writer interface.
func (w *Writer) Write(p []byte) (nRet int, errRet error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if w.flushOnWrite {
		return w.write(p)
	}
	// If we exceed the input buffer size, start writing
	for len(p) > (cap(w.ibuf)-len(w.ibuf)) && w.err(nil) == nil {
		var n int
		if len(w.ibuf) == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, _ = w.write(p)
		} else {
			n = copy(w.ibuf[len(w.ibuf):cap(w.ibuf)], p)
			w.ibuf = w.ibuf[:len(w.ibuf)+n]
			w.write(w.ibuf)
			w.ibuf = w.ibuf[:0]
		}
		nRet += n
		p = p[n:]
	}
	if err := w.err(nil); err != nil {
		return nRet, err
	}
	// p should always be able to fit into w.ibuf now.
	n := copy(w.ibuf[len(w.ibuf):cap(w.ibuf)], p)
	w.ibuf = w.ibuf[:len(w.ibuf)+n]
	nRet += n
	return nRet, nil
}

// ReadFrom implements the io.ReaderFrom interface.
// Using this is typically more efficient since it avoids a memory copy.
// ReadFrom reads data from r until EOF or error.
// The return value n is the number of bytes read.
// Any error except io.EOF encountered during the read is also returned.
func (w *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if len(w.ibuf) > 0 {
		err := w.AsyncFlush()
		if err != nil {
			return 0, err
		}
	}
	if br, ok := r.(byter); ok {
		buf := br.Bytes()
		if err := w.EncodeBuffer(buf); err != nil {
			return 0, err
		}
		return int64(len(buf)), w.AsyncFlush()
	}
	for {
		inbuf := w.buffers.Get().([]byte)[:w.blockSize+obufHeaderLen]
		n2, err := io.ReadFull(r, inbuf[obufHeaderLen:])
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
			if err != io.EOF {
				return n, w.err(err)
			}
		}
		if n2 == 0 {
			break
		}
		n += int64(n2)
		err2 := w.writeFull(inbuf[:n2+obufHeaderLen])
		if w.err(err2) != nil {
			break
		}

		if err != nil {
			// We got EOF and wrote everything
			break
		}
	}

	return n, w.err(nil)
}

// AddUserChunk will add a (non)skippable chunk to the stream.
// The ID must be in the range 0x80 -> 0xfe - inclusive.
// The length of the block must be <= MaxUserChunkSize bytes.
func (w *Writer) AddUserChunk(id uint8, data []byte) (err error) {
	if err := w.err(nil); err != nil {
		return err
	}
	if id < MinUserSkippableChunk || id > ChunkTypePadding {
		return fmt.Errorf("invalid skippable block id %x", id)
	}
	if len(data) > MaxUserChunkSize {
		return fmt.Errorf("user chunk exceeds maximum size")
	}
	var header [4]byte
	chunkLen := len(data)
	header[0] = id
	header[1] = uint8(chunkLen >> 0)
	header[2] = uint8(chunkLen >> 8)
	header[3] = uint8(chunkLen >> 16)
	if w.concurrency == 1 {
		write := func(b []byte) error {
			n, err := w.writer.Write(b)
			if err = w.err(err); err != nil {
				return err
			}
			if n != len(b) {
				return w.err(io.ErrShortWrite)
			}
			w.written += int64(n)
			return w.err(nil)
		}
		if !w.wroteStreamHeader {
			w.wroteStreamHeader = true
			if err := write(makeHeader(w.blockSize)); err != nil {
				return err
			}
		}
		if w.uncompWritten > 0 {
			if err = w.err(w.index.add(w.written, w.uncompWritten)); err != nil {
				return err
			}
		}
		if err := write(header[:]); err != nil {
			return err
		}
		return write(data)
	}

	// Create output...
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		hWriter := make(chan result)
		w.output <- hWriter
		hWriter <- result{startOffset: w.uncompWritten, b: makeHeader(w.blockSize)}
	}

	// Copy input.
	inbuf := w.buffers.Get().([]byte)[:4]
	copy(inbuf, header[:])
	inbuf = append(inbuf, data...)

	output := make(chan result, 1)
	// Queue output.
	w.output <- output
	output <- result{startOffset: w.uncompWritten, b: inbuf}

	return nil
}

// EncodeBuffer will add a buffer to the stream.
// This is the fastest way to encode a stream,
// but the input buffer cannot be written to by the caller
// until Flush or Close has been called when concurrency != 1.
//
// If you cannot control that, use the regular Write function.
//
// Note that input is not buffered.
// This means that each write will result in discrete blocks being created.
// For buffered writes, use the regular Write function.
func (w *Writer) EncodeBuffer(buf []byte) (err error) {
	if err := w.err(nil); err != nil {
		return err
	}

	if w.flushOnWrite {
		_, err := w.write(buf)
		return err
	}
	// Flush queued data first.
	if len(w.ibuf) > 0 {
		err := w.AsyncFlush()
		if err != nil {
			return err
		}
	}
	if w.concurrency == 1 {
		_, err := w.writeSync(buf)
		return err
	}

	// Spawn goroutine and write block to output channel.
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		hWriter := make(chan result)
		w.output <- hWriter
		hWriter <- result{startOffset: w.uncompWritten, b: makeHeader(w.blockSize)}
		if w.searchCfg != nil && w.searchInfoBuf == nil {
			w.searchInfoBuf = w.searchCfg.marshalSearchInfoChunk()
			infoOut := make(chan result)
			w.output <- infoOut
			infoOut <- result{startOffset: w.uncompWritten, b: w.searchInfoBuf}
		}
	}

	for len(buf) > 0 {
		// Cut input.
		uncompressed := buf
		if len(uncompressed) > w.blockSize {
			uncompressed = uncompressed[:w.blockSize]
		}
		buf = buf[len(uncompressed):]

		// Overlap for search table boundary patterns.
		var overlap []byte
		if w.searchCfg != nil && len(buf) > 0 {
			overlap = buf[:min(len(buf), int(w.searchCfg.matchLen)-1)]
		}

		// Get output buffer. Front is reserved for search chunk, data starts at searchMaxChunk.
		smc := w.searchMaxChunk
		obuf := w.buffers.Get().([]byte)[:smc+len(uncompressed)+obufHeaderLen]
		output := make(chan result)
		w.output <- output
		res := result{
			startOffset: w.uncompWritten,
		}
		w.uncompWritten += int64(len(uncompressed))
		go func(searchCfg *SearchTableConfig, overlap []byte) {
			dbuf := obuf[smc:]
			checksum := crc(uncompressed)

			chunkType := uint8(chunkTypeUncompressedData)
			chunkLen := 4 + len(uncompressed)

			n := binary.PutUvarint(dbuf[obufHeaderLen:], uint64(len(uncompressed)))
			n2 := w.encodeBlock(dbuf[obufHeaderLen+n:], uncompressed)

			if n2 > 0 {
				chunkType = uint8(chunkTypeMinLZCompressedData)
				chunkLen = 4 + n + n2
				dbuf = dbuf[:obufHeaderLen+n+n2]
			} else {
				copy(dbuf[obufHeaderLen:], uncompressed)
			}

			dbuf[0] = chunkType
			dbuf[1] = uint8(chunkLen >> 0)
			dbuf[2] = uint8(chunkLen >> 8)
			dbuf[3] = uint8(chunkLen >> 16)
			dbuf[4] = uint8(checksum >> 0)
			dbuf[5] = uint8(checksum >> 8)
			dbuf[6] = uint8(checksum >> 16)
			dbuf[7] = uint8(checksum >> 24)

			// Only index compressible blocks.
			searchLen := 0
			if searchCfg != nil && n2 > 0 {
				var stBuf []byte
				if v := searchTablePool.Get(); v != nil {
					stBuf = v.([]byte)
				}
				table, reductions := searchCfg.buildSearchTable(uncompressed, overlap, stBuf, searchCfg.shouldPack(w.concurrency))
				if table != nil {
					searchLen = len(w.appendSearchTableEitherChunk(obuf[:0], reductions, table))
				}
				searchTablePool.Put(table)
			}

			if w.sidecar != nil {
				// Sidecar mode: data chunk stays in main; the search chunk
				// (if any) plus a 0x47 ref go to the sidecar. We copy the
				// search chunk out of obuf so obuf can be safely recycled.
				if searchLen > 0 {
					res.sidecarPre = append([]byte(nil), obuf[:searchLen]...)
				}
				res.uncompSize = len(uncompressed)
				res.b = dbuf
			} else if searchLen > 0 {
				start := smc - searchLen
				copy(obuf[start:], obuf[:searchLen])
				res.b = obuf[start : smc+len(dbuf)]
			} else {
				res.b = dbuf
			}
			res.pooled = obuf
			output <- res
		}(w.searchCfg, overlap)
	}
	return nil
}

func (w *Writer) encodeBlock(obuf, uncompressed []byte) int {
	if w.customEnc != nil {
		if ret := w.customEnc(obuf, uncompressed); ret >= 0 {
			return ret
		}
	}
	var n int

	switch w.level {
	case LevelSuperFast:
		n = encodeBlockFast(obuf, uncompressed)
	case LevelFastest:
		n = encodeBlock(obuf, uncompressed)
	case LevelBalanced:
		n = encodeBlockBetter(obuf, uncompressed)
	case LevelSmallest:
		n = encodeBlockBest(obuf, uncompressed, nil)
	}

	if debugValidateBlocks && n > 0 {
		fmt.Println("debugValidateBlocks:", len(uncompressed), "->", n)
		// debug.PrintStack()
		src := uncompressed
		block := obuf[:n]
		dst := make([]byte, len(src))
		ret := minLZDecode(dst, block)
		if ret != 0 || !bytes.Equal(dst, src) {
			n := matchLen(dst, src)
			x := crc32.ChecksumIEEE(src)
			name := fmt.Sprintf("errs/block-%08x-%d", x, ret)
			fmt.Println(name, "mismatch at pos", n)
			os.WriteFile(name+"input.bin", src, 0o644)
			os.WriteFile(name+"decoded.bin", dst, 0o644)
			os.WriteFile(name+"compressed.bin", block, 0o644)
		}
	}
	return n
}

func (w *Writer) write(p []byte) (nRet int, errRet error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if w.concurrency == 1 {
		return w.writeSync(p)
	}

	// Spawn goroutine and write block to output channel.
	for len(p) > 0 {
		if !w.wroteStreamHeader {
			w.wroteStreamHeader = true
			hWriter := make(chan result)
			w.output <- hWriter
			hWriter <- result{startOffset: w.uncompWritten, b: makeHeader(w.blockSize)}
			// In sidecar mode, the 0x44 info chunk goes on the sidecar
			// (emitted lazily on first block); skip the inline emit.
			if w.sidecar == nil && w.searchCfg != nil && w.searchInfoBuf == nil {
				w.searchInfoBuf = w.searchCfg.marshalSearchInfoChunk()
				infoOut := make(chan result)
				w.output <- infoOut
				infoOut <- result{startOffset: w.uncompWritten, b: w.searchInfoBuf}
			}
		}

		var uncompressed []byte
		if len(p) > w.blockSize {
			uncompressed, p = p[:w.blockSize], p[w.blockSize:]
		} else {
			uncompressed, p = p, nil
		}

		// Overlap for search table from contiguous p (before copying to inbuf).
		var overlap []byte
		if w.searchCfg != nil && len(p) > 0 {
			end := min(len(p), int(w.searchCfg.matchLen)-1)
			overlap = make([]byte, end)
			copy(overlap, p[:end])
		}

		// Copy input.
		// If the block is incompressible, this is used for the result.
		smc := w.searchMaxChunk
		inbuf := w.buffers.Get().([]byte)[:len(uncompressed)+obufHeaderLen]
		obuf := w.buffers.Get().([]byte)[:w.obufLen]
		copy(inbuf[obufHeaderLen:], uncompressed)
		uncompressed = inbuf[obufHeaderLen:]

		output := make(chan result)
		w.output <- output
		res := result{
			startOffset: w.uncompWritten,
		}
		w.uncompWritten += int64(len(uncompressed))

		go func(searchCfg *SearchTableConfig, overlap []byte) {
			dbuf := obuf[smc:]
			checksum := crc(uncompressed)

			chunkType := uint8(chunkTypeUncompressedData)
			chunkLen := 4 + len(uncompressed)

			n := binary.PutUvarint(dbuf[obufHeaderLen:], uint64(len(uncompressed)))
			n2 := w.encodeBlock(dbuf[obufHeaderLen+n:], uncompressed)

			if n2 > 0 {
				chunkType = uint8(chunkTypeMinLZCompressedData)
				chunkLen = 4 + n + n2
				dbuf = dbuf[:obufHeaderLen+n+n2]
			} else {
				obuf, inbuf = inbuf, obuf
				dbuf = obuf
			}

			dbuf[0] = chunkType
			dbuf[1] = uint8(chunkLen >> 0)
			dbuf[2] = uint8(chunkLen >> 8)
			dbuf[3] = uint8(chunkLen >> 16)
			dbuf[4] = uint8(checksum >> 0)
			dbuf[5] = uint8(checksum >> 8)
			dbuf[6] = uint8(checksum >> 16)
			dbuf[7] = uint8(checksum >> 24)

			// Only index compressible blocks.
			searchLen := 0
			if searchCfg != nil && n2 > 0 {
				var stBuf []byte
				if v := searchTablePool.Get(); v != nil {
					stBuf = v.([]byte)
				}
				table, reductions := searchCfg.buildSearchTable(uncompressed, overlap, stBuf, searchCfg.shouldPack(w.concurrency))
				if table != nil {
					// obuf still points to the pool buffer with smc space at front.
					searchLen = len(w.appendSearchTableEitherChunk(inbuf[:0], reductions, table))
				}
				searchTablePool.Put(table)
			}

			if w.sidecar != nil {
				// Sidecar mode: data chunk to main; search chunk + 0x47 to sidecar.
				if searchLen > 0 {
					res.sidecarPre = append([]byte(nil), inbuf[:searchLen]...)
				}
				res.uncompSize = len(uncompressed)
				res.b = dbuf
				res.pooled = obuf
			} else if searchLen > 0 {
				// Search chunk in inbuf[:searchLen] (the unused buffer), data in dbuf.
				// Slide into inbuf since it has capacity.
				start := smc - searchLen
				copy(inbuf[start:], inbuf[:searchLen])
				copy(inbuf[smc:], dbuf)
				res.b = inbuf[start : smc+len(dbuf)]
				res.pooled = inbuf
				obuf, inbuf = inbuf, obuf
			} else {
				res.b = dbuf
				res.pooled = obuf
			}
			output <- res

			w.buffers.Put(inbuf)
		}(w.searchCfg, overlap)
		nRet += len(uncompressed)
	}
	return nRet, nil
}

// writeFull is a special version of write that will always write the full buffer.
// Data to be compressed should start at offset obufHeaderLen and fill the remainder of the buffer.
// The data will be written as a single block.
// The caller is not allowed to use inbuf after this function has been called.
func (w *Writer) writeFull(inbuf []byte) (errRet error) {
	if err := w.err(nil); err != nil {
		return err
	}

	if w.concurrency == 1 {
		_, err := w.writeSync(inbuf[obufHeaderLen:])
		return err
	}

	// Spawn goroutine and write block to output channel.
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		hWriter := make(chan result)
		w.output <- hWriter
		hWriter <- result{startOffset: w.uncompWritten, b: makeHeader(w.blockSize)}
		// In sidecar mode, the 0x44 info chunk goes on the sidecar
		// (emitted lazily on first block); skip the inline emit.
		if w.sidecar == nil && w.searchCfg != nil && w.searchInfoBuf == nil {
			w.searchInfoBuf = w.searchCfg.marshalSearchInfoChunk()
			infoOut := make(chan result)
			w.output <- infoOut
			infoOut <- result{startOffset: w.uncompWritten, b: w.searchInfoBuf}
		}
	}

	// Get an output buffer.
	smc := w.searchMaxChunk
	obuf := w.buffers.Get().([]byte)[:w.obufLen]
	uncompressed := inbuf[obufHeaderLen:]

	output := make(chan result)
	w.output <- output
	res := result{
		startOffset: w.uncompWritten,
	}
	w.uncompWritten += int64(len(uncompressed))

	go func(searchCfg *SearchTableConfig) {
		dbuf := obuf[smc:]
		checksum := crc(uncompressed)

		chunkType := uint8(chunkTypeUncompressedData)
		chunkLen := 4 + len(uncompressed)

		n := binary.PutUvarint(dbuf[obufHeaderLen:], uint64(len(uncompressed)))
		n2 := w.encodeBlock(dbuf[obufHeaderLen+n:], uncompressed)

		if n2 > 0 {
			chunkType = uint8(chunkTypeMinLZCompressedData)
			chunkLen = 4 + n + n2
			dbuf = dbuf[:obufHeaderLen+n+n2]
		} else {
			obuf, inbuf = inbuf, obuf
			dbuf = obuf
		}

		dbuf[0] = chunkType
		dbuf[1] = uint8(chunkLen >> 0)
		dbuf[2] = uint8(chunkLen >> 8)
		dbuf[3] = uint8(chunkLen >> 16)
		dbuf[4] = uint8(checksum >> 0)
		dbuf[5] = uint8(checksum >> 8)
		dbuf[6] = uint8(checksum >> 16)
		dbuf[7] = uint8(checksum >> 24)

		// Only index compressible blocks.
		searchLen := 0
		if searchCfg != nil && chunkType != chunkTypeUncompressedData {
			var stBuf []byte
			if v := searchTablePool.Get(); v != nil {
				stBuf = v.([]byte)
			}
			table, reductions := searchCfg.buildSearchTable(uncompressed, nil, stBuf, searchCfg.shouldPack(w.concurrency))
			if table != nil {
				// Search chunk is in the original obuf (may have been swapped if incompressible,
				// but we only reach here for compressible blocks so obuf is unchanged).
				searchLen = len(w.appendSearchTableEitherChunk(obuf[:0], reductions, table))
			}
			searchTablePool.Put(table)
		}

		if w.sidecar != nil {
			// Sidecar mode: data chunk to main; search chunk + 0x47 to sidecar.
			if searchLen > 0 {
				res.sidecarPre = append([]byte(nil), obuf[:searchLen]...)
			}
			res.uncompSize = len(uncompressed)
			res.b = dbuf
		} else if searchLen > 0 {
			start := smc - searchLen
			copy(obuf[start:], obuf[:searchLen])
			res.b = obuf[start : smc+len(dbuf)]
		} else {
			res.b = dbuf
		}
		output <- res

		w.buffers.Put(inbuf)
	}(w.searchCfg)
	return nil
}

func (w *Writer) writeSync(p []byte) (nRet int, errRet error) {
	if err := w.err(nil); err != nil {
		return 0, err
	}
	if !w.wroteStreamHeader {
		w.wroteStreamHeader = true
		var n int
		var err error
		n, err = w.writer.Write(makeHeader(w.blockSize))
		if err != nil {
			return 0, w.err(err)
		}
		if n != len(magicChunk)+1 {
			return 0, w.err(io.ErrShortWrite)
		}
		w.written += int64(n)
		if err := w.writeSearchInfoSync(); err != nil {
			return 0, err
		}
	}

	for len(p) > 0 {
		var uncompressed []byte
		if len(p) > w.blockSize {
			uncompressed, p = p[:w.blockSize], p[w.blockSize:]
		} else {
			uncompressed, p = p, nil
		}

		obuf := w.buffers.Get().([]byte)[:w.obufLen]
		checksum := crc(uncompressed)

		chunkType := uint8(chunkTypeUncompressedData)
		chunkLen := 4 + len(uncompressed)

		n := binary.PutUvarint(obuf[obufHeaderLen:], uint64(len(uncompressed)))
		n2 := w.encodeBlock(obuf[obufHeaderLen+n:], uncompressed)

		if n2 > 0 {
			chunkType = uint8(chunkTypeMinLZCompressedData)
			chunkLen = 4 + n + n2
			obuf = obuf[:obufHeaderLen+n+n2]
		} else {
			obuf = obuf[:8]
		}

		obuf[0] = chunkType
		obuf[1] = uint8(chunkLen >> 0)
		obuf[2] = uint8(chunkLen >> 8)
		obuf[3] = uint8(chunkLen >> 16)
		obuf[4] = uint8(checksum >> 0)
		obuf[5] = uint8(checksum >> 8)
		obuf[6] = uint8(checksum >> 16)
		obuf[7] = uint8(checksum >> 24)

		// In sidecar mode, emit the sidecar header lazily before the first
		// block, then write the search table (if any) + 0x47 ref to sidecar.
		// Otherwise, the search table is written inline before the data chunk.
		mainBlockOffset := w.written
		if w.sidecar != nil {
			if err := w.writeSidecarStartIfNeeded(); err != nil {
				return 0, err
			}
			if w.searchCfg != nil && n2 > 0 {
				overlap := p[:min(len(p), int(w.searchCfg.matchLen)-1)]
				if err := w.writeSearchTableSync(uncompressed, overlap); err != nil {
					return 0, err
				}
			}
			if err := w.writeSidecarRemoteRef(mainBlockOffset, len(uncompressed)); err != nil {
				return 0, err
			}
		} else if w.searchCfg != nil && n2 > 0 {
			overlap := p[:min(len(p), int(w.searchCfg.matchLen)-1)]
			if err := w.writeSearchTableSync(uncompressed, overlap); err != nil {
				return 0, err
			}
		}

		n, err := w.writer.Write(obuf)
		if err != nil {
			return 0, w.err(err)
		}
		if n != len(obuf) {
			return 0, w.err(io.ErrShortWrite)
		}
		w.err(w.index.add(w.written, w.uncompWritten))
		w.written += int64(n)
		w.uncompWritten += int64(len(uncompressed))

		if chunkType == chunkTypeUncompressedData {
			// Write uncompressed data.
			n, err := w.writer.Write(uncompressed)
			if err != nil {
				return 0, w.err(err)
			}
			if n != len(uncompressed) {
				return 0, w.err(io.ErrShortWrite)
			}
			w.written += int64(n)
		}
		w.buffers.Put(obuf)
		// Queue final output.
		nRet += len(uncompressed)
	}
	return nRet, nil
}

// AsyncFlush writes any buffered bytes to a block and starts compressing it.
// It does not wait for the output has been written as Flush() does.
func (w *Writer) AsyncFlush() error {
	if err := w.err(nil); err != nil {
		return err
	}

	// Queue any data still in input buffer.
	if len(w.ibuf) != 0 {
		if !w.wroteStreamHeader {
			_, err := w.writeSync(w.ibuf)
			w.ibuf = w.ibuf[:0]
			return w.err(err)
		} else {
			_, err := w.write(w.ibuf)
			w.ibuf = w.ibuf[:0]
			err = w.err(err)
			if err != nil {
				return err
			}
		}
	}
	return w.err(nil)
}

// Flush flushes the Writer to its underlying io.Writer.
// This does not apply padding.
func (w *Writer) Flush() error {
	if err := w.AsyncFlush(); err != nil {
		return err
	}

	if w.output == nil {
		return w.err(nil)
	}

	// Send empty buffer
	res := make(chan result)
	w.output <- res
	// Block until this has been picked up.
	res <- result{b: nil, startOffset: w.uncompWritten}
	// When it is closed, we have flushed.
	<-res
	return w.err(nil)
}

// Close calls Flush and then closes the Writer.
// This is required to mark the end of the stream.
// Calling Close multiple times is ok,
// but calling CloseIndex after this will make it not return the index.
func (w *Writer) Close() error {
	_, err := w.closeIndex(w.appendIndex)
	return err
}

// Written returns the number of uncompressed (input) and compressed bytes (output)
// that has been processed since start or last Reset call.
// This is only safe to call after Flush() or Close/CloseIndex has been called.
func (w *Writer) Written() (input, output int64) {
	return w.uncompWritten, w.written
}

// CloseIndex calls Close and returns an index on first call.
// This is not required if you are only adding index to a stream.
func (w *Writer) CloseIndex() ([]byte, error) {
	return w.closeIndex(true)
}

func (w *Writer) closeIndex(idx bool) ([]byte, error) {
	err := w.Flush()
	if w.output != nil {
		close(w.output)
		w.writerWg.Wait()
		w.output = nil
	}
	if idx && w.index == nil {
		return nil, errors.New("index requested, but was asked to not generate one")
	}
	// Write EOF marker.
	if w.err(err) == nil && w.writer != nil {
		var tmp [4 + binary.MaxVarintLen64]byte
		tmp[0] = chunkTypeEOF
		// Write uncompressed size.
		n := binary.PutUvarint(tmp[4:], uint64(w.uncompWritten))
		tmp[1] = uint8(n)
		n += 4
		_, err := w.writer.Write(tmp[:n])
		_ = w.err(err)
		w.written += int64(n)
	}
	// Write the sidecar EOF (also writes the sidecar stream header lazily
	// if no blocks were ever emitted, so the sidecar is a valid empty stream).
	if w.err(err) == nil && w.sidecar != nil {
		_ = w.writeSidecarEOF()
	}
	var index []byte
	if w.err(err) == nil && w.writer != nil {
		// Create index.
		if idx {
			compSize := int64(-1)
			if w.pad <= 1 {
				compSize = w.written
			}
			index = w.index.appendTo(w.ibuf[:0], w.uncompWritten, compSize)
			// Count as written for padding.
			if w.appendIndex {
				w.written += int64(len(index))
			}
		}
		// Add padding.
		if w.pad > 1 {
			tmp := w.ibuf[:0]
			if len(index) > 0 {
				// Allocate another buffer.
				tmp = w.buffers.Get().([]byte)[:0]
				defer w.buffers.Put(tmp)
			}
			add := calcSkippableFrame(w.written, int64(w.pad))
			frame, err := skippableFrame(tmp, add, w.randSrc)
			if err = w.err(err); err != nil {
				return nil, err
			}
			n, err2 := w.writer.Write(frame)
			if err2 == nil && n != len(frame) {
				err2 = io.ErrShortWrite
			}
			w.written += int64(n)
			_ = w.err(err2)
		}
		// Add index.
		if len(index) > 0 && w.appendIndex {
			n, err2 := w.writer.Write(index)
			if err2 == nil && n != len(index) {
				err2 = io.ErrShortWrite
			}
			// (index already accounted for in w.written)
			_ = w.err(err2)
		}
	}
	err = w.err(errClosed)
	if err == errClosed || err == errNilWriter {
		return index, nil
	}
	return nil, err
}

// calcSkippableFrame will return a total size to be added for written
// to be divisible by multiple.
// The value will always be > skippableFrameHeader.
// The function will panic if written < 0 or wantMultiple <= 0.
func calcSkippableFrame(written, wantMultiple int64) int {
	if wantMultiple <= 0 {
		panic("wantMultiple <= 0")
	}
	if written < 0 {
		panic("written < 0")
	}
	leftOver := written % wantMultiple
	if leftOver == 0 {
		return 0
	}
	toAdd := wantMultiple - leftOver
	for toAdd < skippableFrameHeader {
		toAdd += wantMultiple
	}
	return int(toAdd)
}

// skippableFrame will add a skippable frame with a total size of bytes.
// total should be >= skippableFrameHeader and < maxBlockSize + skippableFrameHeader
func skippableFrame(dst []byte, total int, r io.Reader) ([]byte, error) {
	if total == 0 {
		return dst, nil
	}
	if total < skippableFrameHeader {
		return dst, fmt.Errorf("s2: requested skippable frame (%d) < 4", total)
	}
	if int64(total) >= maxBlockSize+skippableFrameHeader {
		return dst, fmt.Errorf("s2: requested skippable frame (%d) >= max 1<<24", total)
	}
	// Chunk type 0xfe "Section 4.4 Padding (chunk type 0xfe)"
	dst = append(dst, ChunkTypePadding)
	f := uint32(total - skippableFrameHeader)
	// Add chunk length.
	dst = append(dst, uint8(f), uint8(f>>8), uint8(f>>16))
	// Add data
	start := len(dst)
	dst = append(dst, make([]byte, f)...)
	_, err := io.ReadFull(r, dst[start:])
	return dst, err
}

// WriterOption is an option for creating a encoder.
type WriterOption func(*Writer) error

// WriterConcurrency will set the concurrency,
// meaning the maximum number of decoders to run concurrently.
// The value supplied must be at least 1.
// By default this will be set to GOMAXPROCS.
func WriterConcurrency(n int) WriterOption {
	return func(w *Writer) error {
		if n <= 0 {
			return errors.New("concurrency must be at least 1")
		}
		w.concurrency = n
		return nil
	}
}

// WriterAddIndex will append an index to the end of a stream
// when it is closed.
func WriterAddIndex(b bool) WriterOption {
	return func(w *Writer) error {
		if b && !w.genIndex {
			return errors.New("WriterAddIndex: WriterCreateIndex has been called with false parameter")
		}
		w.appendIndex = b
		return nil
	}
}

// WriterLevel will set the compression level.
func WriterLevel(n int) WriterOption {
	return func(w *Writer) error {
		if n < LevelSuperFast || n > LevelSmallest {
			return ErrInvalidLevel
		}
		w.level = int8(n)
		return nil
	}
}

// WriterUncompressed will bypass compression.
// The stream will be written as uncompressed blocks only.
// If concurrency is > 1 CRC and output will still be done async.
func WriterUncompressed() WriterOption {
	return func(w *Writer) error {
		w.level = 0
		return nil
	}
}

// WriterBlockSize allows to override the default block size.
// Blocks will be this size or smaller.
// Minimum size is 4KB and and maximum size is 4MB.
//
// Bigger blocks may give bigger throughput on systems with many cores,
// and will increase compression slightly, but it will limit the possible
// concurrency for smaller payloads for both encoding and decoding.
// Default block size is 1MB.
//
// When writing Snappy compatible output using WriterSnappyCompat,
// the maximum block size is 64KB.
func WriterBlockSize(n int) WriterOption {
	return func(w *Writer) error {
		if n > maxBlockSize || n < minBlockSize {
			return errors.New("minlz: block size out of bounds. Must be <= 4MB and >=4KB")
		}
		w.blockSize = n
		return nil
	}
}

// WriterPadding will add padding to all output so the size will be a multiple of n.
// This can be used to obfuscate the exact output size or make blocks of a certain size.
// The contents will be a skippable frame, so it will be invisible by the decoder.
// n must be > 0 and <= 4MB.
// The padded area will be filled with data from crypto/rand.Reader.
// The padding will be applied whenever Close is called on the writer.
func WriterPadding(n int) WriterOption {
	return func(w *Writer) error {
		if n <= 0 {
			return fmt.Errorf("s2: padding must be at least 1")
		}
		// No need to waste our time.
		if n == 1 {
			w.pad = 0
		}
		if n > maxBlockSize {
			return fmt.Errorf("s2: padding must less than 4MB")
		}
		w.pad = n
		return nil
	}
}

// WriterPaddingSrc will get random data for padding from the supplied source.
// By default, crypto/rand is used.
func WriterPaddingSrc(reader io.Reader) WriterOption {
	return func(w *Writer) error {
		w.randSrc = reader
		return nil
	}
}

// WriterFlushOnWrite will compress blocks on each call to the Write function.
//
// This is quite inefficient as blocks size will depend on the write size.
//
// Use WriterConcurrency(1) to also make sure that output is flushed.
// When Write calls return, otherwise they will be written when compression is done.
func WriterFlushOnWrite() WriterOption {
	return func(w *Writer) error {
		w.flushOnWrite = true
		return nil
	}
}

// WriterCustomEncoder allows to override the encoder for blocks on the stream.
// The function must compress 'src' into 'dst' and return the bytes used in dst as an integer.
// Block size (initial varint) should not be added by the encoder.
// Returning value 0 indicates the block could not be compressed.
// Returning a negative value indicates that compression should be attempted.
// The function should expect to be called concurrently.
func WriterCustomEncoder(fn func(dst, src []byte) int) WriterOption {
	return func(w *Writer) error {
		w.customEnc = fn
		return nil
	}
}

// WriterCreateIndex allows to disable the default index creation.
// This can be used when no index will be needed - for example on network streams.
func WriterCreateIndex(b bool) WriterOption {
	return func(w *Writer) error {
		w.genIndex = b
		if !w.genIndex && w.appendIndex {
			return errors.New("WriterCreateIndex: Cannot disable when WriterAddIndex has been requested")
		}
		return nil
	}
}

func (w *Writer) writeSearchInfoSync() error {
	if w.searchCfg == nil || w.searchInfoBuf != nil {
		return nil
	}
	// In sidecar mode, the 0x44 info chunk lives on the sidecar instead
	// of the main stream; it is emitted lazily via writeSidecarStartIfNeeded.
	if w.sidecar != nil {
		return nil
	}
	w.searchInfoBuf = w.searchCfg.marshalSearchInfoChunk()
	n, err := w.writer.Write(w.searchInfoBuf)
	if err != nil {
		return w.err(err)
	}
	if n != len(w.searchInfoBuf) {
		return w.err(io.ErrShortWrite)
	}
	w.written += int64(n)
	return nil
}

// dstWriter selects the destination for the search-table chunk in sync mode.
// In sidecar mode the chunk goes to the sidecar; otherwise it is inlined in
// the main stream and counts towards w.written.
func (w *Writer) writeSearchTableSync(uncompressed, overlap []byte) error {
	var stBuf []byte
	if v := searchTablePool.Get(); v != nil {
		stBuf = v.([]byte)
	}
	table, reductions := w.searchCfg.buildSearchTable(uncompressed, overlap, stBuf, w.searchCfg.shouldPack(1))
	defer searchTablePool.Put(table)
	if table == nil {
		return nil
	}
	// Pick destination. The sidecar's writes do NOT advance w.written.
	dst := w.writer
	countWritten := true
	if w.sidecar != nil {
		dst = w.sidecar
		countWritten = false
	}
	// Try compressed form first. When emitted, the chunk is fully serialized
	// (no large-bitmap separate write).
	if w.searchCfg.compression != nil && w.searchCfg.compression.enabled {
		e := cstEncoderPool.Get().(*cstEncoder)
		out, ok, err := appendSearchTableCompressedChunk(w.searchHdrBuf[:0], w.searchCfg, reductions, table, e)
		cstEncoderPool.Put(e)
		if err != nil {
			return w.err(err)
		}
		if ok {
			w.searchHdrBuf = out
			n, err := dst.Write(out)
			if err != nil {
				return w.err(err)
			}
			if n != len(out) {
				return w.err(io.ErrShortWrite)
			}
			if countWritten {
				w.written += int64(n)
			}
			return nil
		}
	}
	w.searchHdrBuf = appendSearchTableHeader(w.searchHdrBuf[:0], w.searchCfg, reductions, table)
	n, err := dst.Write(w.searchHdrBuf)
	if err != nil {
		return w.err(err)
	}
	if n != len(w.searchHdrBuf) {
		return w.err(io.ErrShortWrite)
	}
	if countWritten {
		w.written += int64(n)
	}
	n, err = dst.Write(table)
	if err != nil {
		return w.err(err)
	}
	if n != len(table) {
		return w.err(io.ErrShortWrite)
	}
	if countWritten {
		w.written += int64(n)
	}
	return nil
}

// writeSidecarStartIfNeeded writes the sidecar's stream header and 0x44 info
// chunk on first use. Idempotent. Must only be called while w.sidecar != nil.
func (w *Writer) writeSidecarStartIfNeeded() error {
	if w.sidecarHeaderWritten {
		return nil
	}
	hdr := makeHeader(w.blockSize)
	n, err := w.sidecar.Write(hdr)
	if err != nil {
		return w.err(err)
	}
	if n != len(hdr) {
		return w.err(io.ErrShortWrite)
	}
	info := w.searchCfg.marshalSearchInfoChunk()
	n, err = w.sidecar.Write(info)
	if err != nil {
		return w.err(err)
	}
	if n != len(info) {
		return w.err(io.ErrShortWrite)
	}
	w.sidecarHeaderWritten = true
	return nil
}

// writeSidecarRemoteRef writes a single-block 0x47 Remote Block Reference
// pointing at mainOffset in the main stream with the given uncompressed
// block size. Must only be called while w.sidecar != nil.
func (w *Writer) writeSidecarRemoteRef(mainOffset int64, uncompSize int) error {
	var rb [4 + binary.MaxVarintLen64*2]byte
	chunk := appendRemoteBlockRef(rb[:0], mainOffset, w.sidecarMaxBlock-uncompSize)
	n, err := w.sidecar.Write(chunk)
	if err != nil {
		return w.err(err)
	}
	if n != len(chunk) {
		return w.err(io.ErrShortWrite)
	}
	return nil
}

// writeSidecarEOF writes the sidecar's EOF chunk. Called once during Close.
// Safe to call when w.sidecar == nil (no-op).
func (w *Writer) writeSidecarEOF() error {
	if w.sidecar == nil {
		return nil
	}
	if err := w.writeSidecarStartIfNeeded(); err != nil {
		return err
	}
	var tmp [4 + binary.MaxVarintLen64]byte
	tmp[0] = chunkTypeEOF
	// Sidecar carries no uncompressed payload — encode 0.
	n := binary.PutUvarint(tmp[4:], 0)
	tmp[1] = uint8(n)
	buf := tmp[:4+n]
	wn, err := w.sidecar.Write(buf)
	if err != nil {
		return w.err(err)
	}
	if wn != len(buf) {
		return w.err(io.ErrShortWrite)
	}
	return nil
}

// appendSearchTableEitherChunk dispatches between the compressed (0x46) and
// uncompressed (0x45) search-table chunk encoders. When compression is
// disabled or not beneficial, falls back to 0x45. An internal encoder error
// is recorded on the Writer (visible to subsequent operations via w.err) so
// it isn't silently masked by the 0x45 fallback.
func (w *Writer) appendSearchTableEitherChunk(dst []byte, reductions uint8, table []byte) []byte {
	if w.searchCfg.compression != nil && w.searchCfg.compression.enabled {
		e := cstEncoderPool.Get().(*cstEncoder)
		out, ok, err := appendSearchTableCompressedChunk(dst, w.searchCfg, reductions, table, e)
		cstEncoderPool.Put(e)
		if err != nil {
			_ = w.err(err)
		}
		if ok {
			return out
		}
	}
	return appendSearchTableChunk(dst, w.searchCfg, reductions, table)
}

// WriterSearchTable enables per-block search table generation.
// The config controls match length, prefix filtering, and heuristics.
// Use NewSearchTableConfig to create the config.
func WriterSearchTable(cfg SearchTableConfig) WriterOption {
	return func(w *Writer) error {
		if err := cfg.validate(); err != nil {
			return err
		}
		c := cfg
		w.searchCfg = &c
		return nil
	}
}

// WriterSidecar redirects search-index chunks (0x44/0x45/0x46) to a separate
// sidecar writer rather than embedding them in the main stream. For each
// data block written to the main stream a 0x47 Remote Block Reference is
// appended to the sidecar that points at the block's offset in the main
// stream.
//
// WriterSearchTable must also be supplied. Both the main stream and the
// sidecar are valid MinLZ streams on their own; the sidecar can be searched
// using NewSidecarSearcher with the main stream as the data source.
func WriterSidecar(sidecar io.Writer) WriterOption {
	return func(w *Writer) error {
		if sidecar == nil {
			return errors.New("minlz: WriterSidecar requires a non-nil writer")
		}
		w.sidecar = sidecar
		return nil
	}
}

// SetSidecar configures or replaces the sidecar destination for subsequent
// blocks. Pass nil to disable sidecar mode. Must be called before any data
// is written for the current stream (i.e., immediately after NewWriter or
// after Reset, before the first Write/EncodeBuffer/ReadFrom). The Writer
// must have been constructed with WriterSearchTable.
//
// This is primarily intended for tools that reuse a single Writer across
// many files and want to direct each file's index to a separate sidecar.
func (w *Writer) SetSidecar(sidecar io.Writer) error {
	if !w.paramsOK {
		return errClosed
	}
	if w.wroteStreamHeader {
		return errors.New("minlz: SetSidecar must be called before the first write")
	}
	if sidecar != nil && w.searchCfg == nil {
		return errors.New("minlz: SetSidecar requires WriterSearchTable")
	}
	w.sidecar = sidecar
	w.sidecarHeaderWritten = false
	if sidecar != nil {
		w.sidecarMaxBlock = w.blockSize
	}
	return nil
}

func makeHeader(blockSize int) []byte {
	hdr := append(make([]byte, 0, len(magicChunk)+1), magicChunk...)
	return append(hdr, byte(bits.Len(uint(blockSize-1)))-10)
}
