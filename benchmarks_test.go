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
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minlz/internal/reference"
)

// testFiles' values are copied directly from
// https://raw.githubusercontent.com/google/snappy/master/snappy_unittest.cc
// The label field is unused in snappy-go.
var testFiles = []struct {
	label     string
	filename  string
	sizeLimit int
}{
	{"html", "html", 0},
	{"urls", "urls.10K", 0},
	{"jpg", "fireworks.jpeg", 0},
	{"jpg_200b", "fireworks.jpeg", 200},
	{"pdf", "paper-100k.pdf", 0},
	{"html4", "html_x_4", 0},
	{"txt1", "alice29.txt", 0},
	{"txt2", "asyoulik.txt", 0},
	{"txt3", "lcet10.txt", 0},
	{"txt4", "plrabn12.txt", 0},
	{"pb", "geo.protodata", 0},
	{"gaviota", "kppkn.gtb", 0},
	{"txt1_128b", "alice29.txt", 128},
	{"txt1_1000b", "alice29.txt", 1000},
	{"txt1_10000b", "alice29.txt", 10000},
	{"txt1_20000b", "alice29.txt", 20000},
}

const (
	// The benchmark data files are at this canonical URL.
	benchURL = "https://raw.githubusercontent.com/google/snappy/master/testdata/"
)

func BenchmarkDecodeBlockSingle(b *testing.B) {
	for i := range testFiles {
		b.Run(fmt.Sprint(i, "-", testFiles[i].label), func(b *testing.B) {
			if err := downloadBenchmarkFiles(b, testFiles[i].filename); err != nil {
				b.Fatalf("failed to download testdata: %s", err)
			}
			bDir := filepath.FromSlash(*benchdataDir)
			data := readFile(b, filepath.Join(bDir, testFiles[i].filename))
			if testFiles[i].sizeLimit > 0 && len(data) > testFiles[i].sizeLimit {
				data = data[:testFiles[i].sizeLimit]
			}
			benchDecode(b, data)
		})
	}
}

func BenchmarkDecodeBlockParallel(b *testing.B) {
	for i := range testFiles {
		b.Run(fmt.Sprint(i, "-", testFiles[i].label), func(b *testing.B) {
			benchFile(b, i, true)
		})
	}
}

func BenchmarkEncodeBlockSingle(b *testing.B) {
	for i := range testFiles {
		b.Run(fmt.Sprint(i, "-", testFiles[i].label), func(b *testing.B) {
			if err := downloadBenchmarkFiles(b, testFiles[i].filename); err != nil {
				b.Fatalf("failed to download testdata: %s", err)
			}
			bDir := filepath.FromSlash(*benchdataDir)
			data := readFile(b, filepath.Join(bDir, testFiles[i].filename))
			if testFiles[i].sizeLimit > 0 && len(data) > testFiles[i].sizeLimit {
				data = data[:testFiles[i].sizeLimit]
			}
			benchEncode(b, data)
		})
	}
}

func BenchmarkEncodeBlockParallel(b *testing.B) {
	for i := range testFiles {
		b.Run(fmt.Sprint(i, "-", testFiles[i].label), func(b *testing.B) {
			benchFile(b, i, false)
		})
	}
}

func BenchmarkTwainEncode1e1(b *testing.B) { benchTwain(b, 1e1, false) }
func BenchmarkTwainEncode1e2(b *testing.B) { benchTwain(b, 1e2, false) }
func BenchmarkTwainEncode1e3(b *testing.B) { benchTwain(b, 1e3, false) }
func BenchmarkTwainEncode1e4(b *testing.B) { benchTwain(b, 1e4, false) }
func BenchmarkTwainEncode1e5(b *testing.B) { benchTwain(b, 1e5, false) }
func BenchmarkTwainEncode1e6(b *testing.B) { benchTwain(b, 1e6, false) }

func BenchmarkTwainDecode1e1(b *testing.B) { benchTwain(b, 1e1, true) }
func BenchmarkTwainDecode1e2(b *testing.B) { benchTwain(b, 1e2, true) }
func BenchmarkTwainDecode1e3(b *testing.B) { benchTwain(b, 1e3, true) }
func BenchmarkTwainDecode1e4(b *testing.B) { benchTwain(b, 1e4, true) }
func BenchmarkTwainDecode1e5(b *testing.B) { benchTwain(b, 1e5, true) }
func BenchmarkTwainDecode1e6(b *testing.B) { benchTwain(b, 1e6, true) }

func benchTwain(b *testing.B, n int, decode bool) {
	data := expand(readFile(b, "testdata/Mark.Twain-Tom.Sawyer.txt"), n)
	if decode {
		benchDecode(b, data)
	} else {
		benchEncode(b, data)
	}
}

func benchDecode(b *testing.B, src []byte) {
	b.Run("ref", func(b *testing.B) {
		encoded, _ := reference.EncodeBlock(src)
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := reference.DecodeBlock(encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(100*float64(len(encoded))/float64(len(src)), "pct")
	})
	b.Run("level-1", func(b *testing.B) {
		encoded := encodeGo(nil, src, LevelFastest)
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Decode(src[:0], encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(100*float64(len(encoded))/float64(len(src)), "pct")
	})
	b.Run("level-2", func(b *testing.B) {
		encoded := encodeGo(nil, src, LevelBalanced)
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Decode(src[:0], encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(100*float64(len(encoded))/float64(len(src)), "pct")
	})
	b.Run("level-3", func(b *testing.B) {
		encoded := encodeGo(nil, src, LevelSmallest)
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Decode(src[:0], encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(100*float64(len(encoded))/float64(len(src)), "pct")
	})
}

func benchEncode(b *testing.B, src []byte) {
	// Bandwidth is in amount of uncompressed data.
	dst := make([]byte, MaxEncodedLen(len(src)))
	b.ResetTimer()
	b.Run("ref", func(b *testing.B) {
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reference.EncodeBlock(src)
		}
		enc, _ := reference.EncodeBlock(src)
		b.ReportMetric(100*float64(len(enc))/float64(len(src)), "pct")
	})
	b.Run("level-1", func(b *testing.B) {
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(dst, src, LevelFastest)
		}
		enc, _ := Encode(dst, src, LevelFastest)
		b.ReportMetric(100*float64(len(enc))/float64(len(src)), "pct")
	})
	b.Run("level-2", func(b *testing.B) {
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(dst, src, LevelBalanced)
		}
		enc, _ := Encode(dst, src, LevelBalanced)
		b.ReportMetric(100*float64(len(enc))/float64(len(src)), "pct")
	})
	b.Run("level-3", func(b *testing.B) {
		b.SetBytes(int64(len(src)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Encode(dst, src, LevelSmallest)
		}
		enc, _ := Encode(dst, src, LevelSmallest)
		b.ReportMetric(100*float64(len(enc))/float64(len(src)), "pct")
	})
	/*
		b.Run("snappy", func(b *testing.B) {
			dst := snappy.Encode(dst, src)
			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				snappy.Encode(dst, src)
			}
			b.ReportMetric(100*float64(len(dst))/float64(len(src)), "pct")
		})
		b.Run("lz4-0", func(b *testing.B) {
			var c lz4.Compressor
			dst := make([]byte, lz4.CompressBlockBound(len(src)))
			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.CompressBlock(src, dst)
			}
			enc, _ := c.CompressBlock(src, dst)
			b.ReportMetric(100*float64(enc)/float64(len(src)), "pct")
		})
		b.Run("lz4-9", func(b *testing.B) {
			var c lz4.CompressorHC
			c.Level = lz4.Level9
			dst := make([]byte, lz4.CompressBlockBound(len(src)))
			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.CompressBlock(src, dst)
			}
			enc, _ := c.CompressBlock(src, dst)
			b.ReportMetric(100*float64(enc)/float64(len(src)), "pct")
		})
	*/
}

func BenchmarkRandomEncodeBlock1MB(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	data := make([]byte, 1<<20)
	_, err := io.ReadFull(rng, data)
	if err != nil {
		b.Fatal(err)
	}
	benchEncode(b, data)
}

func BenchmarkRandomEncodeBlock8MB(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	data := make([]byte, MaxBlockSize)
	_, err := io.ReadFull(rng, data)
	if err != nil {
		b.Fatal(err)
	}
	benchEncode(b, data)
}

func downloadBenchmarkFiles(b testing.TB, basename string) (errRet error) {
	bDir := filepath.FromSlash(*benchdataDir)
	filename := filepath.Join(bDir, basename)
	if stat, err := os.Stat(filename); err == nil && stat.Size() != 0 {
		return nil
	}

	if !*download {
		b.Skipf("test data not found; skipping %s without the -download flag", testOrBenchmark(b))
	}
	// Download the official snappy C++ implementation reference test data
	// files for benchmarking.
	if err := os.MkdirAll(bDir, 0777); err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to create %s: %s", bDir, err)
	}

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create %s: %s", filename, err)
	}
	defer f.Close()
	defer func() {
		if errRet != nil {
			os.Remove(filename)
		}
	}()
	url := benchURL + basename
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download %s: %s", url, err)
	}
	defer resp.Body.Close()
	if s := resp.StatusCode; s != http.StatusOK {
		return fmt.Errorf("downloading %s: HTTP status code %d (%s)", url, s, http.StatusText(s))
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to download %s to %s: %s", url, filename, err)
	}
	return nil
}

func benchFile(b *testing.B, i int, decode bool) {
	if err := downloadBenchmarkFiles(b, testFiles[i].filename); err != nil {
		b.Fatalf("failed to download testdata: %s", err)
	}
	bDir := filepath.FromSlash(*benchdataDir)
	data := readFile(b, filepath.Join(bDir, testFiles[i].filename))
	if n := testFiles[i].sizeLimit; 0 < n && n < len(data) {
		data = data[:n]
	}
	b.Run("ref", func(b *testing.B) {
		if decode {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				encoded, _ := reference.EncodeBlock(data)
				for pb.Next() {
					var err error
					_, err = reference.DecodeBlock(encoded)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		} else {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				tmp := make([]byte, len(data))
				for pb.Next() {
					res, _ := reference.EncodeBlock(data)
					if len(res) == 0 {
						panic(0)
					}
					if debugValidateBlocks {
						tmp, _ = Decode(tmp, res)
						if !bytes.Equal(tmp, data) {
							panic("wrong")
						}
					}
				}
			})
		}
		enc, _ := reference.EncodeBlock(data)
		b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
		b.ReportMetric(float64(len(enc)), "B")
	})

	b.Run("level-1", func(b *testing.B) {
		if decode {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				encoded := encodeGo(nil, data, LevelFastest)
				tmp := make([]byte, len(data), len(data)+16)
				for pb.Next() {
					var err error
					tmp, err = Decode(tmp, encoded)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		} else {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				dst := make([]byte, MaxEncodedLen(len(data)))
				tmp := make([]byte, len(data))
				for pb.Next() {
					res, _ := Encode(dst, data, LevelFastest)
					if len(res) == 0 {
						panic(0)
					}
					if debugValidateBlocks {
						tmp, _ = Decode(tmp, res)
						if !bytes.Equal(tmp, data) {
							panic("wrong")
						}
					}
				}
			})
		}
		enc, _ := Encode(nil, data, LevelFastest)
		b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
		b.ReportMetric(float64(len(enc)), "B")
	})
	if hasAsm {
		b.Run("level-1-noasm", func(b *testing.B) {
			if decode {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					encoded := encodeGo(nil, data, LevelFastest)
					tmp := make([]byte, len(data), len(data)+16)
					for pb.Next() {
						var err error
						tmp, err = decodeGo(tmp, encoded)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			} else {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					dst := make([]byte, MaxEncodedLen(len(data)))
					tmp := make([]byte, len(data))
					for pb.Next() {
						res := encodeGo(dst, data, LevelFastest)
						if len(res) == 0 {
							panic(0)
						}
						if debugValidateBlocks {
							tmp, _ = decodeGo(tmp, res)
							if !bytes.Equal(tmp, data) {
								panic("wrong")
							}
						}
					}
				})
			}
			enc := encodeGo(nil, data, LevelFastest)
			b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
			b.ReportMetric(float64(len(enc)), "B")
		})
	}
	b.Run("level-2", func(b *testing.B) {
		if decode {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				encoded := encodeGo(nil, data, LevelBalanced)
				tmp := make([]byte, len(data), len(data)+16)
				for pb.Next() {
					var err error
					tmp, err = Decode(tmp, encoded)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		} else {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				dst := make([]byte, MaxEncodedLen(len(data)))
				tmp := make([]byte, len(data))
				for pb.Next() {
					res, _ := Encode(dst, data, LevelBalanced)
					if len(res) == 0 {
						panic(0)
					}
					if debugValidateBlocks {
						tmp, _ = Decode(tmp, res)
						if !bytes.Equal(tmp, data) {
							panic("wrong")
						}
					}
				}
			})
		}
		enc, _ := Encode(nil, data, LevelBalanced)
		b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
		b.ReportMetric(float64(len(enc)), "B")
	})
	if hasAsm {
		b.Run("level-2-noasm", func(b *testing.B) {
			if decode {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					encoded := encodeGo(nil, data, LevelBalanced)
					tmp := make([]byte, len(data), len(data)+16)
					for pb.Next() {
						var err error
						tmp, err = decodeGo(tmp, encoded)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			} else {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					dst := make([]byte, MaxEncodedLen(len(data)))
					tmp := make([]byte, len(data))
					for pb.Next() {
						res := encodeGo(dst, data, LevelBalanced)
						if len(res) == 0 {
							panic(0)
						}
						if debugValidateBlocks {
							tmp, _ = decodeGo(tmp, res)
							if !bytes.Equal(tmp, data) {
								panic("wrong")
							}
						}
					}
				})
			}
			enc := encodeGo(nil, data, LevelBalanced)
			b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
			b.ReportMetric(float64(len(enc)), "B")
		})
	}

	b.Run("level-3", func(b *testing.B) {
		if decode {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				encoded := encodeGo(nil, data, LevelSmallest)
				tmp := make([]byte, len(data), len(data)+16)
				for pb.Next() {
					var err error
					tmp, err = Decode(tmp[:0], encoded)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		} else {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				dst := make([]byte, MaxEncodedLen(len(data)))
				tmp := make([]byte, len(data))
				for pb.Next() {
					res, _ := Encode(dst, data, LevelSmallest)
					if len(res) == 0 {
						panic(0)
					}
					if debugValidateBlocks {
						tmp, _ = Decode(tmp, res)
						if !bytes.Equal(tmp, data) {
							panic("wrong")
						}
					}
				}
			})
		}
		enc, _ := Encode(nil, data, LevelSmallest)
		b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
		b.ReportMetric(float64(len(enc)), "B")
	})
	if decode && hasAsm {
		b.Run("level-3-noasm", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				encoded := encodeGo(nil, data, LevelSmallest)
				tmp := make([]byte, len(data), len(data)+16)
				for pb.Next() {
					var err error
					tmp, err = decodeGo(tmp[:0], encoded)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			enc := encodeGo(nil, data, LevelSmallest)
			b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
			b.ReportMetric(float64(len(enc)), "B")
		})
	}
	/*
		b.Run("snappy", func(b *testing.B) {
			if decode {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					encoded := snappy.Encode(nil, data)
					tmp := make([]byte, len(data), len(data)+16)
					for pb.Next() {
						var err error
						tmp, err = snappy.Decode(tmp, encoded)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			} else {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					dst := make([]byte, snappy.MaxEncodedLen(len(data)))
					tmp := make([]byte, len(data))
					for pb.Next() {
						res := snappy.Encode(dst, data)
						if len(res) == 0 {
							panic(0)
						}
						if debugValidateBlocks {
							tmp, _ = Decode(tmp, res)
							if !bytes.Equal(tmp, data) {
								panic("wrong")
							}
						}
					}
				})
			}
			enc := snappy.Encode(nil, data)
			b.ReportMetric(100*float64(len(enc))/float64(len(data)), "pct")
			b.ReportMetric(float64(len(enc)), "B")
		})

		b.Run("lz4-0", func(b *testing.B) {
			if decode {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var c lz4.Compressor
					encoded := make([]byte, lz4.CompressBlockBound(len(data)))
					encSize, err := c.CompressBlock(data, encoded)
					if err != nil {
						b.Fatal(err)
					}
					encoded = encoded[:encSize]
					tmp := make([]byte, len(data), len(data)+16)
					for pb.Next() {
						var err error
						_, err = lz4.UncompressBlock(encoded, tmp)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			} else {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					dst := make([]byte, lz4.CompressBlockBound(len(data)))
					var c lz4.Compressor
					for pb.Next() {
						_, err := c.CompressBlock(data, dst)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
			var c lz4.Compressor
			encSize, _ := c.CompressBlock(data, make([]byte, lz4.CompressBlockBound(len(data))))
			b.ReportMetric(100*float64(encSize)/float64(len(data)), "pct")
			b.ReportMetric(float64(encSize), "B")
		})
		b.Run("lz4-9", func(b *testing.B) {
			if decode {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					c := lz4.CompressorHC{Level: lz4.Level9}
					encoded := make([]byte, lz4.CompressBlockBound(len(data)))
					encSize, err := c.CompressBlock(data, encoded)
					if err != nil {
						b.Fatal(err)
					}
					encoded = encoded[:encSize]
					tmp := make([]byte, len(data), len(data)+16)
					for pb.Next() {
						var err error
						_, err = lz4.UncompressBlock(encoded, tmp)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			} else {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					dst := make([]byte, lz4.CompressBlockBound(len(data)))
					c := lz4.CompressorHC{Level: lz4.Level9}
					for pb.Next() {
						_, err := c.CompressBlock(data, dst)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
			c := lz4.CompressorHC{Level: lz4.Level9}
			encSize, _ := c.CompressBlock(data, make([]byte, lz4.CompressBlockBound(len(data))))
			b.ReportMetric(100*float64(encSize)/float64(len(data)), "pct")
			b.ReportMetric(float64(encSize), "B")
		})
	*/
}

func BenchmarkWriterRandom(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	// Make max window so we never get matches.
	data := make([]byte, 4<<20)
	for i := range data {
		data[i] = uint8(rng.Intn(256))
	}

	for name, opts := range testOptions(b) {
		w := NewWriter(io.Discard, opts...)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				err := w.EncodeBuffer(data)
				if err != nil {
					b.Fatal(err)
				}
			}
			// Flush output
			w.Flush()
		})
		w.Close()
	}
}

func BenchmarkIndexFind(b *testing.B) {
	fatalErr := func(t testing.TB, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	for blocks := 1; blocks <= 65536; blocks *= 2 {
		if blocks == 65536 {
			blocks = 65535
		}

		var index Index
		index.reset(100)
		index.TotalUncompressed = int64(blocks) * 100
		index.TotalCompressed = int64(blocks) * 100
		for i := 0; i < blocks; i++ {
			err := index.add(int64(i*100), int64(i*100))
			fatalErr(b, err)
		}

		rng := rand.New(rand.NewSource(0xabeefcafe))
		b.Run(fmt.Sprintf("blocks-%d", len(index.Offsets)), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			const prime4bytes = 2654435761
			rng2 := rng.Int63()
			for i := 0; i < b.N; i++ {
				rng2 = ((rng2 + prime4bytes) * prime4bytes) >> 32
				// Find offset:
				_, _, err := index.Find(rng2 % (int64(blocks) * 100))
				fatalErr(b, err)
			}
		})
	}
}
