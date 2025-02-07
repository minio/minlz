# MinLZ

MinLZ is a LZ77-type compressor with a fixed byte-aligned encoding, in the similar class to Snappy and LZ4.

The goal of MinLZ is to provide a fast, low memory compression algorithm that can be used for fast compression of data, 
where encoding and/or decoding speed is the primary concern. 

MinLZ is designed to operate *faster than IO* for both compression and decompression and be a viable "always on"
option even if some content already is compressed.
If slow compression is acceptable, MinLZ can be configured to produce high compression ratio, 
but retain high decompression speed.

* Best in class compression
* Block or Streaming interfaces
* Very fast decompression, even as pure Go
* AMD64 encoder+decoder assembly
* Adjustable Compression (3 levels)
* Concurrent stream Compression
* Concurrent stream Decompression
* Skip forward in compressed stream via independent blocks
* Random seeking with optional indexes
* Stream EOF validation
* Automatic stream size padding
* Custom encoders for small blocks
* Skippable/Non-skippable user blocks
* Detailed control of memory under decompression
* Fast detection of pre-compressed data
* Powerful commandline utility

This package implements the MinLZ specification v1.0.

For format specification see the included [SPEC.md](SPEC.md).

# Usage

MinLZ can operate on *blocks* up to 8 MB or *streams* with unlimited length.

Blocks are the simplest, but do not provide any output validation. 
Blocks are mainly useful for small data sizes.

Streams are a collection of independent blocks, which each have checksums and EOF checks, 
which ensures against corruption and truncation.

3 compression levels are provided:

* Level 1, "Fastest": Provides the fastest compression with reasonable compression. 
* Level 2, "Balanced": Provides a good balance between compression and speed. ~50% the speed of the fastest level.
* Level 3, "Smallest": Provides the smallest output possible. Not tuned for speed.

A secondary option to control speed/compression is adjusting the block size.
See "Writer Block Size" section below.

## Blocks

MinLZ provides a block encoding interface with blocks up to 8MB.
Blocks do not perform any data integrity check of the content, 
so additional checksum is recommended.

A basic roundtrip looks like this:

```Go
   compressed, err := minlz.Encode(nil, src, minlz.LevelBalanced)
   if err != nil {
       // Handle error
   }
   
    decompressed, err := minlz.Decode(nil, compressed)
    if err != nil {
        // Handle error 
    }
```

In both cases, a destination buffer can be provided, which will be overwritten. 
If the destination buffer is too small, an appropriately sized buffer will be allocated and returned.

It is possible to get the decompressed buffer size by using `minlz.DecodedLen(block []byte) (int, error)`.

You can use the predefined `LevelFastest`, `LevelBalanced` or `LevelSmallest` which correspond to
levels 1,2 and 3 respectively.

MinLZ does not track the compressed size of buffers and the decode input must match the output exactly.
Extra bytes given to decompression will return an error.

It is possible to use `minlz.TryEncode`, which will only return compressed bytes if the output size 
is strictly less than input.
Use `minlz.AppendEncoded` and `minlz.AppendDecoded` to append to existing slices.

## Streams

Streams provide much more safety and allow for unlimited length encoding, 
as well as seeking and concurrent encoding/decoding.

Generally, you do not need buffering on the input or output side as reads and writes 
are done in rather big blocks. 
Reading and writing data on streams are buffered, 
and only non-concurrent will block for input/output.    

When dealing with many streams, it is recommended to re-use the Readers and Writers.
If you are dealing with short streams, consider limiting the concurrency, so 
`block_size * concurrency` doesn't exceed the expected stream size.

### Encoding

Streams are the recommended way to use MinLZ.
They provide end-to-end validation against corruption and truncation.

```Go
    // Create a new stream encoder.
    // The encoder will write to the provided io.Writer.
    enc := minlz.NewWriter(output)
	
	// We defer a call to Close.
	// This will flush any pending data and indicate we have reached the end of the stream.
	defer enc.Close()

	// Write data to the encoder.
	// The encoder will write the compressed data to the underlying io.Writer.
	js := json.NewEncoder(enc)
	err := js.Encode(data)
```

Encoders can be reused by calling `Reset` on them with another output.
This will reset the encoder to its initial state.

The encoder supports the [io.ReaderFrom](https://pkg.go.dev/io#ReaderFrom) interface, 
which can be used for encoding data from an io.Reader. 
This will typically be faster than writing data to the encoder, since it avoids a memory copy.

If you have a single big buffer to encode, you can use the `EncodeBuffer([]byte) error` 
to encode it. This will encode the buffer with minimal overhead.
If you plan to do multiple writes, use the regular `Write` function.

### Options

There are various options that can be set on the stream encoder.
This can be used to control resource usage on compression and some aspects of decompression.
If invalid options are set, the encoder will return an error when used.

We will cover the most common options here. Refer to the godoc for a complete list.

#### Writer Compression Level 

The `WriterLevel` option controls the compression level of the stream encoder.

You can use the predefined `LevelFastest`, `LevelBalanced` or `LevelSmallest` which correspond to 
levels 1,2 and 3 respectively.

Setting level 0 will disable compression and write the data as an uncompressed stream.

The default level is `LevelBalanced`.

#### Writer Block Size

The `WriterBlockSize` allows to set the maximum size of each block on the stream encoder.
The blocksize - rounded up to a power of 2 - is communicated in the stream, and 
the decoder will use this to allocate memory during decompression.

Smaller blocks will take up less memory on both compression and decompression, 
but will result in a larger output. 

Block size further allows trading off speed vs. size; Here is a sample chart of how 
speed and block size can correlate, using the fastest encoder setting:

| Block Size | Output Size   | E MB/s | Size | E Speed | D Speed |
|------------|---------------|--------|------|---------|---------|
| 8MB        | 840,198,535   | 6419   | 100% | 100%    | 100%    |
| 4MB        | 862,923,396   | 8470   | 103% | 132%    | 124%    |
| 2MB        | 921,750,327   | 9660   | 110% | 150%    | 131%    |
| 1MB        | 950,153,883   | 10407  | 113% | 162%    | 125%    |
| 512KB      | 1,046,061,990 | 11459  | 125% | 179%    | 113%    |

Input is a `3,325,605,752` byte [CSV file](https://files.klauspost.com/compress/nyc-taxi-data-10M.csv.zst)
compressed on a 16 core CPU.

The actual scaling mostly depends on the amount of CPU L2 cache (speed) 
and the nature of the compressed data (size). 

Decompression speed is affected similarly, but less predictably, 
since it is more likely to be limited by memory throughput, 
and larger output also tends to affect it more negatively.

If your software is very sensitive to GC stoppages, also note that with assembly 
single block de/compression cannot be pre-empted, so stop-the-world events may take 
longer on bigger blocks.

The default block size is 2 MB.

#### Writer Concurrency

The `WriterConcurrency` option allows setting the number of concurrent blocks that can be compressed.
Higher concurrency will increase the throughput of the encoder, but will also increase memory usage. 

If `WriterConcurrency(1)` is used no async goroutines will be used and the encoder will run in the calling goroutine.

The default concurrency is `GOMAXPROCS`.

### Decoding

Decoding streams mostly just involves sending the compressed stream to a Reader.

Anything accepting an `io.Reader` as input will then be able to read the decompressed data.

```Go
	// Create a new stream decoder. 
	// The encoder will read from the provided io.Reader.
	dec := minlz.NewReader(input)
	
	// Read decompressed input.
	js := json.NewDecoder(dec)
	err := js.Decode(&data)
```

If you would like the output to be written to an `io.Writer`, the easiest is to use
the `WriteTo` functionality.

```Go
	// Our input and output
	in, _ := os.Create("input.mz")
	out, _ := os.Create("output.txt")
	
	// Create a new stream decoder
	dec := minlz.NewReader(in)

	// Write all decompressed data to output
	n, err := dec.WriteTo(out)
	fmt.Println("Wrote", n, "bytes. Error:", err)
```

The `DecompressConcurrent` has similar functionality to `WriteTo`, but allows specifying the concurrency.
By default `WriteTo` uses `runtime.NumCPU()` concurrent decompressors.

For memory-sensitive systems, the maximum block size can be set below 8MB. For this use the `ReaderMaxBlockSize(int)`
option.

#### Skipping and Seeking

Streams can be skipped forward by calling `(*Reader).Skip(n int64) error`.
This will skip forward in the stream by `n` bytes. 
Intermediate blocks be read, but will not be decompressed unless the skip ends inside the block.

Full random seeking is supported by using an *index*. An index can be created when the stream is encoded.
The index can either be added to the stream or stored separately. 
For existing streams the `IndexStream(r io.Reader) ([]byte, error)` function can be used to create an index.

To add an index at the end of streams, use the `WriterAddIndex()` option when creating the writer, 
then the index will be added to the stream when it is closed.
To keep the index separate, use the `(*Writer).CloseIndex() ([]byte, error)` method to retrieve 
the index when finishing a stream.

To get a fully seekable reader use `(*Reader).ReadSeeker(index []byte) (*ReadSeeker, error)`.
The returned reader will implement `io.Seeker`, `io.ReaderAt` in addition to the existing `Reader` methods
and can be used to seek to any position in the stream.

If an index is not provided in the call, the reader will attempt to read the index from the end of the stream.
If the input stream does not support `io.Seeker` an error will be returned.

## Custom User Data

Streams can contain user-defined data, that isn't part of the stream. 
Each "chunk" has an ID, which allows for processing of different types.

This data can either be "skippable" - meaning it is ignored if the user hasn't provided a handler for these.
If the chunk is non-skippable, the encoder will error out if this chunk isn't handled by the user.

`MinUserSkippableChunk` is the minimum chunk id with user data and `MaxUserSkippableChunk` is the maximum.

`MinUserNonSkippableChunk` is the minimum ID that will not automatically be skipped if unhandled by the user. 
Finally `MaxUserNonSkippableChunk` is the final ID that can be used for this.

The custom data will not be compressed or modified in any way.

```go
func ExampleWriterAddUserChunk() {
	var buf bytes.Buffer
	w := minlz.NewWriter(&buf)
	// Add a skippable chunk
	w.AddUserChunk(minlz.MinUserSkippableChunk, []byte("Chunk Custom Data"))
	// Write content to stream.
	w.Write([]byte("some data"))
	w.Close()

	// Read back what we wrote.
	r := minlz.NewReader(&buf)
	r.SkippableCB(minlz.MinUserSkippableChunk, func(sr io.Reader) error {
		b, err := io.ReadAll(sr)
		fmt.Println("Callback:", string(b), err)
		return err
	})

	// Read stream data
	b, err := io.ReadAll(r)
	fmt.Println("Stream data:", string(b))

	//OUTPUT:
	//Callback: Chunk Custom Data <nil>
	//Stream data: some data
}
```

The maximum single chunk size is 16MB, but as many chunks as needed can be added. 

## Build Tags

The following build tags can be used to control which speed improvements are used:

* `noasm` disables all assembly.
* `nounsafe` disables all use of unsafe package.
* `purego` disables assembly and unsafe usage.  

Using assembly/non-assembly versions will often produce slightly different output.

# Performance

All sizes are base 1024 where relevant.

## Why is concurrent block and stream speed so different?

In most cases, MinLZ will be limited by memory bandwidth.

Since streams consist of mostly "unseen" data, it will often mean that memory
reads are outside any CPU cache.

Contrast that to blocks, where data has often just been read/produced and therefore
already is in one of the CPU caches.
Therefore, block (de)compression will more often take place with data read from cache 
rather than a stream, where data can be coming from memory.

Even if data is streamed into cache, the "penalty" will still have to paid at some 
place in the chain. So streams will mostly appear slower in benchmarks.

# Commandline utility

Official releases can be downloaded from the [releases](https://github.com/minio/minlz/releases) section
with binaries for most platforms.

To install from source execute `go install github.com/minio/minlz@latest`.

## Usage

```
λ mz
MinLZ compression tool vx.x built at home, (c) 2025 MinIO Inc.
Homepage: https://github.com/minio/minlz

Usage:
Compress:     mz c [options] <inputs>
Decompress:   mz d [options] <inputs>
 (cat)    :   mz cat [options] <inputs>
 (tail)   :   mz tail [options] <inputs>

Compress file:    mz c file.txt
Compress stdin:   mz c -
Decompress file:  mz d file.txt.mz
Decompress stdin: mz d -
```

Note that all sizes KB, MB, etc. are base 1024 in the commandline tool, 
except speed indications, which are base 10. 

### Compressing

```
Usage: mz c [options] <input>

Compresses all files supplied as input separately.
Output files are written as 'filename.ext.mz.
By default output files will be overwritten.
Use - as the only file name to read from stdin and write to stdout.

Wildcards are accepted: testdir/*.txt will compress all files in testdir ending with .txt
Directories can be wildcards as well. testdir/*/*.txt will match testdir/subdir/b.txt

File names beginning with 'http://' and 'https://' will be downloaded and compressed.
Only http response code 200 is accepted.

Options:
  -1    Compress faster, but with a minor compression loss
  -2    Default compression speed (default true)
  -3    Compress more, but a lot slower
  -bench int
        Run benchmark n times. No output will be written
  -block
        Use as a single block. Will load content into memory. Max 8MB.
  -bs string
        Max block size. Examples: 64K, 256K, 1M, 8M. Must be power of two and <= 8MB (default "8M")
  -c    Write all output to stdout. Multiple input files will be concatenated
  -cpu int
        Maximum number of threads to use (default 32)
  -help
        Display help
  -index
        Add seek index (default true)
  -o string
        Write output to another file. Single input file only
  -pad string
        Pad size to a multiple of this value, Examples: 500, 64K, 256K, 1M, 4M, etc (default "1")
  -q    Don't write any output to terminal, except errors
  -recomp
        Recompress MinLZ, Snappy or S2 input
  -rm
        Delete source file(s) after success
  -safe
        Do not overwrite output files
  -verify
        Verify files, but do not write output

Example:

λ mz c apache.log
Compressing apache.log -> apache.log.mz 2622574440 -> 170960982 [6.52%]; 4155.2MB/s
```

## Decompressing

```
Usage: mz d [options] <input>

Decompresses all files supplied as input. Input files must end with '.mz', '.s2' or '.sz'.
Output file names have the extension removed. By default output files will be overwritten.
Use - as the only file name to read from stdin and write to stdout.

Wildcards are accepted: testdir/*.txt will decompress all files in testdir ending with .txt
Directories can be wildcards as well. testdir/*/*.txt will match testdir/subdir/b.txt

File names beginning with 'http://' and 'https://' will be downloaded and decompressed.
Extensions on downloaded files are ignored. Only http response code 200 is accepted.

Options:
  -bench int
        Run benchmark n times. No output will be written
  -block
        Decompress single block. Will load content into memory. Max 8MB.
  -block-debug
        Print block encoding
  -c    Write all output to stdout. Multiple input files will be concatenated
  -cpu int
        Maximum number of threads to use (default 32)
  -help
        Display help
  -limit string
        Return at most this much data. Examples: 92, 64K, 256K, 1M, 4M        
  -o string
        Write output to another file. Single input file only
  -offset string
        Start at offset. Examples: 92, 64K, 256K, 1M, 4M. Requires Index
  -q    Don't write any output to terminal, except errors
  -rm
        Delete source file(s) after success
  -safe
        Do not overwrite output files
  -tail string
        Return last of compressed file. Examples: 92, 64K, 256K, 1M, 4M. Requires Index
  -verify
        Verify files, but do not write output

Example:

λ mz d apache.log.mz
Decompressing apache.log.mz -> apache.log 170960982 -> 2622574440 [1534.02%]; 2660.2MB/s
```

Tail, Offset and Limit can be made to forward to the next newline by adding `+nl`.

For example `mz d -c -offset=50MB+nl -limit=1KB+nl enwik9.mz` will skip 50MB, 
search for the next newline, start outputting data. 
After 1KB, it will stop at the next newline.

Partial files - decoded with tail, offset or limit will have `.part` extension.

# Snappy/S2 Compatibility

MinLZ is designed to be easily upgradable from [Snappy](https://github.com/google/snappy) 
and [S2](https://github.com/klauspost/compress/tree/master/s2#s2-compression).

Both the streaming and block interfaces in the Go port provide seamless
compatibility with existing Snappy and S2 content.
This means that any content encoded with either will be decoded correctly by MinLZ.

Content encoded with MinLZ cannot be decoded by Snappy or S2.  

| Version        | Snappy Decoder | S2 Decoder | MinLZ Decoder |
|----------------|----------------|------------|---------------|
| Snappy Encoder | ✔              | ✔          | ✔ (*)         |
| S2 Encoder     | x              | ✔          | ✔ (*)         |
| MinLZ Encoder  | x              | x          | ✔             |

(*) MinLZ decoders *may* implement fallback to S2/Snappy. 
This is however not required and ports may not support this.

# License

MinLZ is Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Based on code from [snappy-go](https://github.com/golang/snappy) project.

# Ports

Reference code is provided in the `internal/reference` folder.
This provides simplified, but explicit versions of the block de/encoder;
stream and index decoders with minimal dependencies.

Currently, there are no ports of MinLZ to other languages. 
If you are interested in porting MinLZ to another language, open a discussion topic.

If you do a port, feel free to send in a PR for this table:

| Language | Repository Link                                          | License    | Features as described in SPEC.md                                                                                |
|----------|----------------------------------------------------------|------------|-----------------------------------------------------------------------------------------------------------------|
| Go       | [github.com/minio/minlz](https://github.com/minio/minlz) | Apache 2.0 | `[x] Block Read [x] Block Write [x] Stream Read [x] Stream Write [x] Index Support [x] Snappy Read Fallback`    |

Indicated features must support all parts of each feature as described in the specification.
However, it is up to the implementation to decide the encoding implementation(s).  
