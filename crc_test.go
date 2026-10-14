package minlz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

func crcTestChunk(t *testing.T, data []byte, kind byte) []byte {
	t.Helper()
	encoded, err := Encode(nil, data, LevelFastest)
	if err != nil || len(encoded) == 0 || encoded[0] != 0 {
		t.Fatalf("expected compressed MinLZ block: %v", err)
	}
	block := encoded[1:]
	_, hdrLen, err := decodedLen(block)
	if err != nil {
		t.Fatal(err)
	}
	checksum := crc(data)
	if kind == chunkTypeMinLZCompressedDataCompCRC {
		checksum = crc(block[hdrLen:])
	}
	payload := make([]byte, checksumSize+len(block))
	binary.LittleEndian.PutUint32(payload, checksum)
	copy(payload[checksumSize:], block)
	return payload
}

func TestDeferredBlockCRC(t *testing.T) {
	for _, size := range []int{64, 16 << 10, 256 << 10} {
		data := bytes.Repeat([]byte("overlap and checksum\n"), size/21+1)[:size]
		for _, kind := range []byte{chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC} {
			for _, corrupt := range []bool{false, true} {
				for _, ignore := range []bool{false, true} {
					t.Run(fmt.Sprintf("size%d/chunk%02x/corrupt%v/ignore%v", size, kind, corrupt, ignore), func(t *testing.T) {
						payload := crcTestChunk(t, data, kind)
						if corrupt {
							payload[0] ^= 1
						}
						t.Run("lazy", func(t *testing.T) {
							lb := lazyBlock{chunkData: payload, chunkType: kind, decompLen: len(data), ignoreCRC: ignore}
							got := lb.decode()
							if corrupt && !ignore {
								if got != nil {
									t.Fatal("invalid CRC accepted")
								}
							} else if !bytes.Equal(got, data) {
								t.Fatalf("decoded %d bytes, want %d", len(got), len(data))
							}
						})
						t.Run("pending", func(t *testing.T) {
							s := BlockSearcher{maxBlock: MaxBlockSize, ignoreCRC: ignore}
							p := pendingBlock{lazy: lazyBlock{chunkData: payload, chunkType: kind, decompLen: len(data)}}
							got, err := s.decodePending(&p)
							if corrupt && !ignore {
								if !errors.Is(err, ErrCRC) {
									t.Fatalf("invalid CRC: got %v", err)
								}
							} else if err != nil || !bytes.Equal(got, data) {
								t.Fatalf("decode: %v; decoded %d bytes", err, len(got))
							}
						})
					})
				}
			}
		}
	}
}

func TestDeferredBlockCRCRejectsSizeHeaderChecksum(t *testing.T) {
	data := bytes.Repeat([]byte("compressed CRC excludes the size header\n"), 500)
	payload := crcTestChunk(t, data, chunkTypeMinLZCompressedDataCompCRC)
	binary.LittleEndian.PutUint32(payload, crc(payload[checksumSize:]))
	lb := lazyBlock{chunkData: payload, chunkType: chunkTypeMinLZCompressedDataCompCRC, decompLen: len(data)}
	if lb.decode() != nil {
		t.Fatal("CRC covering the size header accepted")
	}
	s := BlockSearcher{maxBlock: MaxBlockSize}
	p := pendingBlock{lazy: lb}
	if _, err := s.decodePending(&p); !errors.Is(err, ErrCRC) {
		t.Fatalf("CRC covering the size header: got %v", err)
	}
}

func TestDeferredBlockCRCIgnoreRejectsMalformed(t *testing.T) {
	for _, payload := range [][]byte{nil, {0, 0, 0, 0, 0x80}} {
		lb := lazyBlock{chunkData: payload, chunkType: chunkTypeMinLZCompressedDataCompCRC, decompLen: 32, ignoreCRC: true}
		if lb.decode() != nil {
			t.Fatal("malformed block accepted with CRC disabled")
		}
		s := BlockSearcher{maxBlock: MaxBlockSize, ignoreCRC: true}
		p := pendingBlock{lazy: lb}
		if _, err := s.decodePending(&p); err == nil {
			t.Fatal("malformed pending block accepted with CRC disabled")
		}
	}
}

// The Writer emits 0x02. Rewrite only its checksum and chunk type to create
// equivalent 0x03 streams; chunk lengths and search tables stay unchanged.
func crcTestCompressedStream(t *testing.T, stream []byte) ([]byte, []int) {
	t.Helper()
	converted := append([]byte(nil), stream...)
	var checksums []int
	for off := 0; off < len(converted); {
		if off+4 > len(converted) {
			t.Fatal("truncated stream header")
		}
		size := int(converted[off+1]) | int(converted[off+2])<<8 | int(converted[off+3])<<16
		end := off + 4 + size
		if end > len(converted) {
			t.Fatal("truncated chunk")
		}
		if converted[off] == chunkTypeMinLZCompressedData {
			payload := converted[off+4 : end]
			_, hdrLen, err := decodedLen(payload[checksumSize:])
			if err != nil {
				t.Fatal(err)
			}
			binary.LittleEndian.PutUint32(payload, crc(payload[checksumSize+hdrLen:]))
			converted[off] = chunkTypeMinLZCompressedDataCompCRC
			checksums = append(checksums, off+4)
		}
		off = end
	}
	if len(checksums) == 0 {
		t.Fatal("fixture has no compressed chunks")
	}
	return converted, checksums
}

func TestConcurrentChunkCRC(t *testing.T) {
	const blockSize = 256 << 10
	blocks, attempts := 96, 3
	if testing.Short() {
		blocks, attempts = 12, 1
	}
	input := make([]byte, blocks*blockSize)
	rng := rand.New(rand.NewSource(82))
	for block := 0; block < blocks; block++ {
		b := input[block*blockSize : (block+1)*blockSize]
		for i := range b {
			b[i] = byte(block)
		}
		if block%2 != 0 {
			for i := 0; i < blockSize/2; i++ {
				b[i] = byte(i%64 + block)
			}
			if _, err := rng.Read(b[blockSize/2:]); err != nil {
				t.Fatal(err)
			}
		}
	}
	var stream bytes.Buffer
	w := NewWriter(&stream, WriterBlockSize(blockSize), WriterConcurrency(1), WriterLevel(LevelFastest))
	if _, err := w.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	compStream, checksums := crcTestCompressedStream(t, stream.Bytes())
	if len(checksums) != blocks {
		t.Fatalf("compressed blocks: got %d, want %d", len(checksums), blocks)
	}
	for _, kind := range []byte{chunkTypeMinLZCompressedData, chunkTypeMinLZCompressedDataCompCRC} {
		valid := stream.Bytes()
		if kind == chunkTypeMinLZCompressedDataCompCRC {
			valid = compStream
		}
		got, err := io.ReadAll(NewReader(bytes.NewReader(valid)))
		if err != nil || !bytes.Equal(got, input) {
			t.Fatalf("chunk%02x sequential control: %v", kind, err)
		}
		for _, concurrent := range []int{1, 2, 4} {
			for _, mode := range []string{"valid", "corrupt-first", "corrupt-last", "ignore-corrupt"} {
				t.Run(fmt.Sprintf("chunk%02x/cpu%d/%s", kind, concurrent, mode), func(t *testing.T) {
					encoded := append([]byte(nil), valid...)
					var options []ReaderOption
					if mode != "valid" {
						idx := 0
						if mode == "corrupt-last" {
							idx = len(checksums) - 1
						}
						encoded[checksums[idx]] ^= 1
					}
					if mode == "ignore-corrupt" {
						options = append(options, ReaderIgnoreCRC())
					}
					for attempt := 0; attempt < attempts; attempt++ {
						var out bytes.Buffer
						n, err := NewReader(bytes.NewReader(encoded), options...).DecodeConcurrent(&out, concurrent)
						if mode == "corrupt-first" || mode == "corrupt-last" {
							if !errors.Is(err, ErrCRC) {
								t.Fatalf("invalid checksum: got %v", err)
							}
							continue
						}
						if err != nil || n != int64(len(input)) || !bytes.Equal(out.Bytes(), input) {
							t.Fatalf("attempt %d: %v, bytes %d/%d", attempt, err, n, len(input))
						}
					}
				})
			}
		}
	}
}

func TestSearchCompressedCRCStraddle(t *testing.T) {
	bs := minBlockSize
	filler := bytes.Repeat([]byte("the quick brown fox jumps over\n"), bs/31+2)
	first := append([]byte(nil), filler[:bs]...)
	first[bs-1] = ' '
	pattern := []byte(" zqxjvkmarker9")
	second := append(append([]byte(nil), pattern[1:]...), filler...)
	var stream bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(6).WithBytePrefix(' ')
	w := NewWriter(&stream, WriterSearchTable(cfg), WriterBlockSize(bs), WriterConcurrency(1))
	for _, data := range [][]byte{first, second} {
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	converted, _ := crcTestCompressedStream(t, stream.Bytes())
	for _, encoded := range [][]byte{stream.Bytes(), converted} {
		var offsets []int64
		err := NewBlockSearcher(bytes.NewReader(encoded)).Search(pattern, func(r SearchResult) error {
			offsets = append(offsets, r.StreamOffset)
			joined := append(append([]byte(nil), r.PrevBlock()...), r.Blocks[1]...)
			if !bytes.Contains(joined, pattern) {
				t.Fatal("match context does not contain the pattern")
			}
			return nil
		})
		if err != nil || len(offsets) != 1 || offsets[0] != int64(bs-1) {
			t.Fatalf("search: %v; offsets %v, want [%d]", err, offsets, bs-1)
		}
	}
}

func TestSearchCompressedCRCLazyPrevious(t *testing.T) {
	bs := minBlockSize
	first := bytes.Repeat([]byte("x"), bs)
	second := bytes.Repeat([]byte("y"), bs)
	pattern := []byte(" zqxjvkmarker9")
	copy(second[bs/2:], pattern)
	var stream bytes.Buffer
	cfg := NewSearchTableConfig().WithMatchLen(6).WithBytePrefix(' ')
	w := NewWriter(&stream, WriterSearchTable(cfg), WriterBlockSize(bs), WriterConcurrency(1))
	for _, data := range [][]byte{first, second} {
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	converted, _ := crcTestCompressedStream(t, stream.Bytes())
	for _, encoded := range [][]byte{stream.Bytes(), converted} {
		found := 0
		err := NewBlockSearcher(bytes.NewReader(encoded)).Search(pattern, func(r SearchResult) error {
			found++
			if r.prevLazy == nil {
				t.Fatal("fixture did not retain a lazy previous block")
			}
			if !bytes.Equal(r.PrevBlock(), first) {
				t.Fatal("previous block context was lost")
			}
			if r.StreamOffset != int64(bs+bs/2) {
				t.Fatalf("match offset: got %d", r.StreamOffset)
			}
			return nil
		})
		if err != nil || found != 1 {
			t.Fatalf("search: %v; matches %d", err, found)
		}
	}
}
