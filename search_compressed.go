package minlz

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sync"

	"github.com/klauspost/compress/huff0"
)

const (
	cstDispRaw                = 16
	cstDispRLE                = 17
	cstDispSparse             = 18
	cstMaxHuff0Tables         = 16
	cstDefaultSkipPctTimes100 = 1000 // 10.00%
	// Absolute minimum bitmap that 0x46 can wrap (= smallest search table = 32 B).
	// huff0 may fail to compress at this size, but the RLE / sparse / raw
	// fallbacks still win against 0x45's table+CRC+config overhead.
	cstMinBitmapForCompression = 32
	cstMinHuff0BlockLog2       = 5
	cstMaxHuff0BlockLog2       = 17
	// cstOwnTableBias penalizes picking a per-block (non-shared) table so the
	// encoder only emits one when it saves strictly more than this many bytes
	// vs the next-best alternative (raw / RLE / global). Smaller table count
	// = faster decoder setup.
	cstOwnTableBias = 8
)

// sparseBitTableEstimate returns an upper bound on the byte count
// appendSparseBitTable would produce for a bitmap with the given size and
// popcount. For uniformly distributed bits the estimate is tight (within
// a byte or two of actual).
//
// Derivation: each set bit contributes one final byte to its gap encoding,
// plus floor(gap/255) "255" bytes. Sum of gaps = position[k-1] - (k-1) ≤
// N_bits - k. So total ≤ k + (N_bits - k)/255.
func sparseBitTableEstimate(bitmapBytes, popcount int) int {
	nBits := bitmapBytes * 8
	if popcount >= nBits {
		return popcount
	}
	return popcount + (nBits-popcount)/255
}

// appendSparseBitTable encodes set-bit positions in bitmap as variable-length
// gaps. Each gap < 255 emits one byte; longer gaps emit 255 bytes for each
// full 255 and then a final byte < 255 for the remainder. The encoding is
// terminated implicitly by the chunk's length prefix.
//
// bitmap length must be a multiple of 8. Reads 8 bytes at a time as a
// little-endian uint64 and uses TrailingZeros64 to skip directly between
// set bits — one inner-loop iteration per set bit, not per bit position.
func appendSparseBitTable(dst, bitmap []byte) []byte {
	gap := 0
	for i := 0; i+8 <= len(bitmap); i += 8 {
		v := binary.LittleEndian.Uint64(bitmap[i:])
		if v == 0 {
			gap += 64
			continue
		}
		pos := 0
		for v != 0 {
			tz := bits.TrailingZeros64(v)
			gap += tz - pos
			for gap >= 255 {
				dst = append(dst, 255)
				gap -= 255
			}
			dst = append(dst, byte(gap))
			gap = 0
			pos = tz + 1
			v &= v - 1 // clear lowest set bit
		}
		gap += 64 - pos
	}
	return dst
}

// decodeSparseBitTable populates dst (assumed pre-zeroed and sized to the
// bitmap length) from src using the sparse-bit-table format.
func decodeSparseBitTable(dst, src []byte) error {
	totalBits := len(dst) * 8
	pos := 0
	gap := 0
	for _, b := range src {
		gap += int(b)
		if b == 255 {
			continue
		}
		pos += gap
		if pos >= totalBits {
			return fmt.Errorf("minlz: sparse bit table position %d out of range (max %d)", pos, totalBits-1)
		}
		dst[pos>>3] |= 1 << (pos & 7)
		pos++
		gap = 0
	}
	if gap != 0 {
		// Trailing 255-bytes without a terminator.
		return fmt.Errorf("minlz: sparse bit table truncated (pending gap %d)", gap)
	}
	return nil
}

// CompressedSearchStats reports per-bitmap stats produced by the compressed
// search-table encoder. It is delivered via CompressedSearchStatsHook.
type CompressedSearchStats struct {
	BitmapBytes   int
	Reductions    uint8 // reductions applied to the bitmap before encoding
	SubBlockSize  int   // 1<<h0_bs, or 0 if SkippedBand or below minimum size
	SubBlocks     int
	SetBits       int
	TotalBits     int
	SkippedBand   bool // popcount band rejected compression
	SkippedSize   bool // bitmap below cstMinBitmapForCompression
	Chunk0x45Size int  // alternative uncompressed-form size
	Chunk0x46Size int  // produced compressed-form size; 0 if not emitted
	Emitted0x46   bool
	Tables        int // distinct tables embedded

	// Per-disposition sub-block counts (sum to SubBlocks when emitted).
	BlocksOwnTable    int
	BlocksGlobalTable int
	BlocksRaw         int
	BlocksRLE         int
	BlocksSparse      int

	// Per-disposition on-wire payload bytes (excludes disposition byte;
	// includes the uvarint length for table-using blocks).
	BytesOwnPayload    int
	BytesGlobalPayload int
	BytesRawPayload    int
	BytesRLEPayload    int
	BytesSparsePayload int

	// Bytes consumed by serialized table headers (sums over the tables that
	// were actually embedded in the chunk).
	BytesOwnTables   int
	BytesGlobalTable int
}

type compressedOpts struct {
	enabled         bool
	skipPctTimes100 int
	statsHook       func(CompressedSearchStats)
	forceCompressed bool
}

// CompressedSearchOption configures the compressed search-table encoder.
type CompressedSearchOption func(*compressedOpts)

// CompressedSearchSkipPct sets the popcount-band half-width (in percent) below
// which compression is skipped. The default is 5.0.
func CompressedSearchSkipPct(pct float64) CompressedSearchOption {
	return func(o *compressedOpts) {
		if pct < 0 {
			pct = 0
		}
		if pct > 50 {
			pct = 50
		}
		o.skipPctTimes100 = int(pct * 100)
	}
}

// CompressedSearchStatsHook installs a callback receiving stats for every
// search-table bitmap processed by the encoder. The callback must not retain
// references into the supplied struct.
func CompressedSearchStatsHook(fn func(CompressedSearchStats)) CompressedSearchOption {
	return func(o *compressedOpts) { o.statsHook = fn }
}

// CompressedSearchForce forces the encoder to emit the compressed chunk type
// (0x46) even when it is larger than the uncompressed (0x45) alternative.
// Intended for benchmarking only.
func CompressedSearchForce() CompressedSearchOption {
	return func(o *compressedOpts) { o.forceCompressed = true }
}

// huff0BlockSize picks the huff0 block partition for a bitmap of the given
// byte length. Returns (log2(blockSize), nBlocks) with nBlocks ≤ 16.
// Policy:
//
//	bitmap ≤ 32 KiB: single block of size = bitmap.
//	bitmap ≤ 512 KiB: 32 KiB blocks.
//	else: 64 KiB blocks (caps at 16 blocks for the 1 MiB max bitmap).
func huff0BlockSize(bitmapLen int) (log2bs uint8, nBlocks int) {
	if bitmapLen <= 32<<10 {
		return uint8(bits.Len(uint(bitmapLen) - 1)), 1
	}
	if bitmapLen <= 16*32<<10 {
		return 15, bitmapLen >> 15
	}
	return 16, bitmapLen >> 16
}

func uvarintLen(n int) int {
	if n < 0 {
		return 0
	}
	switch {
	case n < 1<<7:
		return 1
	case n < 1<<14:
		return 2
	case n < 1<<21:
		return 3
	case n < 1<<28:
		return 4
	default:
		return 5
	}
}

func popcountBytes(b []byte) int {
	n := 0
	// Process 8 bytes at a time when possible.
	i := 0
	for ; i+8 <= len(b); i += 8 {
		v := binary.LittleEndian.Uint64(b[i:])
		n += bits.OnesCount64(v)
	}
	for ; i < len(b); i++ {
		n += bits.OnesCount8(b[i])
	}
	return n
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// cstEncoder holds per-stream-worker state for compressing search bitmaps.
// One encoder must be used from a single goroutine at a time (it owns the
// huff0 scratches and output buffers); concurrency in the Writer is achieved
// via a sync.Pool of cstEncoder instances.
type cstEncoder struct {
	perBlockSc [cstMaxHuff0Tables]huff0.Scratch
	globalSc   huff0.Scratch
	// workSc is used to compress against a transferred (global or shared)
	// table without disturbing perBlockSc state.
	workSc huff0.Scratch
	// Per-block buffers preserved across blocks. Compress4X writes into
	// Scratch.Out — slicing OutTable/OutData out of it remains valid until
	// the next Compress on that Scratch.
	// hist[i] is the byte histogram for block i.
	hist [cstMaxHuff0Tables][256]uint32
	// outTable[i], outData[i] are owned copies (we copy out of OutTable/OutData
	// to detach from Scratch.Out so the same Scratch can be reused for
	// global-table compression in the next phase).
	outTable [cstMaxHuff0Tables][]byte
	outData  [cstMaxHuff0Tables][]byte
	// globalTableBytes holds the serialized global huff0 table header.
	globalTableBytes []byte
	// globalData[i] holds the data compressed using the global table for
	// block i (filled only for blocks that were assigned the global table).
	globalData [cstMaxHuff0Tables][]byte
	// sparseBufs[i] holds the actual sparse-bit-table encoding for block i
	// (filled only for blocks that were assigned the sparse disposition).
	sparseBufs [cstMaxHuff0Tables][]byte
	// scratch byte slice used while emitting the final chunk.
	chunkBuf []byte
}

func newCSTEncoder() *cstEncoder { return &cstEncoder{} }

func (e *cstEncoder) reset(n int) {
	for i := 0; i < n; i++ {
		e.hist[i] = [256]uint32{}
		e.outTable[i] = e.outTable[i][:0]
		e.outData[i] = e.outData[i][:0]
		e.globalData[i] = e.globalData[i][:0]
		e.sparseBufs[i] = e.sparseBufs[i][:0]
	}
	e.globalTableBytes = e.globalTableBytes[:0]
	e.chunkBuf = e.chunkBuf[:0]
}

// appendSearchTableCompressedChunk evaluates the compressed (0x46) chunk form
// and emits it when strictly smaller than the uncompressed (0x45) chunk form
// (unless co.forceCompressed). Returns:
//   - (newDst, true, nil) when 0x46 was emitted to dst.
//   - (nil, false, nil) when 0x45 should be emitted instead (band skip,
//     bitmap too small, compression unhelpful, etc).
//   - (nil, false, err) on internal error.
func appendSearchTableCompressedChunk(dst []byte, cfg *SearchTableConfig, reductions uint8, bitmap []byte, e *cstEncoder) ([]byte, bool, error) {
	co := cfg.compression
	if co == nil || !co.enabled || e == nil {
		return nil, false, nil
	}

	setBits := popcountBytes(bitmap)
	totalBits := len(bitmap) * 8
	chunk45Size := 4 + cfg.searchTablePayloadSize(len(bitmap))

	stats := CompressedSearchStats{
		BitmapBytes:   len(bitmap),
		Reductions:    reductions,
		SetBits:       setBits,
		TotalBits:     totalBits,
		Chunk0x45Size: chunk45Size,
	}
	emitStats := func() {
		if co.statsHook != nil {
			co.statsHook(stats)
		}
	}

	if len(bitmap) < cstMinBitmapForCompression {
		stats.SkippedSize = true
		emitStats()
		return nil, false, nil
	}
	// Popcount band: skip when |setBits/totalBits - 0.5| < skipPct/100.
	// Equivalent: |2*setBits - totalBits| * 5000 < skipPctTimes100 * totalBits.
	// Computed in int64 — totalBits can be up to 8 MiB so the int32-product
	// would overflow on 32-bit GOARCH.
	if int64(absInt(2*setBits-totalBits))*5000 < int64(co.skipPctTimes100)*int64(totalBits) {
		stats.SkippedBand = true
		emitStats()
		return nil, false, nil
	}

	log2bs, nBlocks := huff0BlockSize(len(bitmap))
	bsize := 1 << log2bs
	if nBlocks > cstMaxHuff0Tables || nBlocks*bsize != len(bitmap) || log2bs < cstMinHuff0BlockLog2 || log2bs > cstMaxHuff0BlockLog2 {
		// Shouldn't happen for valid bitmaps; bail to 0x45.
		return nil, false, nil
	}
	stats.SubBlockSize = bsize
	stats.SubBlocks = nBlocks

	e.reset(nBlocks)

	// Phase 1: per-block compression with own table.
	type runResult struct {
		ok     bool // own table available
		rleSym byte
	}
	results := make([]runResult, nBlocks)
	runOne := func(i int) {
		slice := bitmap[i*bsize : (i+1)*bsize]
		// Build histogram.
		var h [256]uint32
		for _, b := range slice {
			h[b]++
		}
		e.hist[i] = h
		// Compress with no reuse.
		sc := &e.perBlockSc[i]
		sc.Reuse = huff0.ReusePolicyNone
		sc.Out = sc.Out[:0]
		_, _, err := huff0.Compress4X(slice, sc)
		if err == nil {
			// Copy out detached buffers so we can reuse sc later for global compression.
			e.outTable[i] = append(e.outTable[i][:0], sc.OutTable...)
			e.outData[i] = append(e.outData[i][:0], sc.OutData...)
			results[i].ok = true
			return
		}
		if errors.Is(err, huff0.ErrUseRLE) {
			// Pick any non-zero histogram bucket as the RLE byte.
			for s, v := range h {
				if v > 0 {
					results[i].rleSym = byte(s)
					break
				}
			}
		}
		// On ErrIncompressible: results[i].ok stays false, no own table.
	}

	if nBlocks == 1 {
		runOne(0)
	} else {
		var wg sync.WaitGroup
		for i := 0; i < nBlocks; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				runOne(i)
			}(i)
		}
		wg.Wait()
	}

	// Phase 2: build the global table from the summed histogram.
	var globalSerSize int
	var sumHist [256]uint32
	globalAvailable := false
	buildGlobalFrom := func(h *[256]uint32) bool {
		if err := e.globalSc.BuildCTable(h); err != nil {
			return false
		}
		tbl, err := e.globalSc.AppendTable(e.globalTableBytes[:0])
		if err != nil {
			return false
		}
		e.globalTableBytes = tbl
		globalSerSize = len(tbl)
		return true
	}
	if nBlocks > 1 {
		for i := 0; i < nBlocks; i++ {
			for j, v := range e.hist[i] {
				sumHist[j] += v
			}
		}
		globalAvailable = buildGlobalFrom(&sumHist)
	}

	// Per-block payload candidates.
	type blockCost struct {
		ownPayload   int // 1 + uvarint(len) + len  (or -1 if no own table)
		ownTableSize int
		rawPayload   int
		rlePayload   int // -1 if not RLE
		rleSym       byte
		// For "with global" decision:
		globalEst     int // estimated compressed bytes; -1 if cannot use
		globalPayload int // 1 + uvarint(est) + est ; -1 if cannot use
		// Sparse bit table (no shared state, no decoder setup).
		sparseLen     int // bytes the sparse encoding would produce
		sparsePayload int // 1 + uvarint(sparseLen) + sparseLen
	}
	costs := make([]blockCost, nBlocks)
	rleAble := func(h *[256]uint32) (byte, bool) {
		var sym byte
		count := 0
		for s, v := range h {
			if v > 0 {
				count++
				if count > 1 {
					return 0, false
				}
				sym = byte(s)
			}
		}
		return sym, count == 1
	}
	for i := 0; i < nBlocks; i++ {
		c := &costs[i]
		c.rawPayload = 1 + bsize
		if sym, ok := rleAble(&e.hist[i]); ok {
			c.rleSym = sym
			c.rlePayload = 2
		} else {
			c.rlePayload = -1
		}
		if results[i].ok {
			c.ownPayload = 1 + uvarintLen(len(e.outData[i])) + len(e.outData[i])
			c.ownTableSize = len(e.outTable[i])
		} else {
			c.ownPayload = -1
		}
		// Sparse size estimate from popcount only (O(N/8) via 64-bit popcount).
		// The exact byte count is computed at emission time, at which point
		// the chunk header is patched to reflect actuals.
		popcount := popcountBytes(bitmap[i*bsize : (i+1)*bsize])
		c.sparseLen = sparseBitTableEstimate(bsize, popcount)
		c.sparsePayload = 1 + uvarintLen(c.sparseLen) + c.sparseLen
		if globalAvailable && e.globalSc.CanUseTable(&e.hist[i]) {
			c.globalEst = e.globalSc.EstimateSize(&e.hist[i])
			c.globalPayload = 1 + uvarintLen(c.globalEst) + c.globalEst
		} else {
			c.globalEst = -1
			c.globalPayload = -1
		}
	}

	// Decision: compare "without global" vs "with global".
	// Each block independently picks the cheapest disposition.
	// pick.size is the per-block ON-WIRE payload (disposition byte + length +
	// data). Own-table cost is tracked separately so it isn't double-counted
	// in the chunk size.
	type pick struct {
		kind uint8 // 0=own, 1=global, cstDispRaw=raw, cstDispRLE=rle
		size int   // per-block payload bytes on wire (no table contribution)
	}
	// pickBest returns the disposition with the smallest effective cost
	// (payload + amortized table cost). selfCost is used purely for comparison
	// across dispositions; the returned pick.size is the on-wire payload only.
	pickBest := func(i int, allowGlobal bool) (pick, int) {
		c := costs[i]
		best := pick{kind: cstDispRaw, size: c.rawPayload}
		bestEff := c.rawPayload
		if c.rlePayload > 0 && c.rlePayload < bestEff {
			best = pick{kind: cstDispRLE, size: c.rlePayload}
			bestEff = c.rlePayload
		}
		// Sparse: no shared state, no table header; check after RLE so an
		// all-zero / all-one bitmap (which fits both) prefers RLE's simpler
		// decode.
		if c.sparsePayload < bestEff {
			best = pick{kind: cstDispSparse, size: c.sparsePayload}
			bestEff = c.sparsePayload
		}
		if c.ownPayload > 0 {
			// Bias non-shared tables so they only win when they save more than
			// cstOwnTableBias bytes vs any alternative. Reduces the per-chunk
			// table count, which is the dominant cost on the decode side.
			eff := c.ownPayload + c.ownTableSize + cstOwnTableBias
			if eff < bestEff {
				best = pick{kind: 0, size: c.ownPayload}
				bestEff = eff
			}
		}
		if allowGlobal && c.globalPayload > 0 && c.globalPayload <= bestEff {
			best = pick{kind: 1, size: c.globalPayload}
			bestEff = c.globalPayload
		}
		return best, bestEff
	}

	// recomputeGlobalCosts updates each block's globalEst/globalPayload from
	// the currently installed global table (whatever it was last built from).
	recomputeGlobalCosts := func() {
		for i := 0; i < nBlocks; i++ {
			if globalAvailable && e.globalSc.CanUseTable(&e.hist[i]) {
				costs[i].globalEst = e.globalSc.EstimateSize(&e.hist[i])
				costs[i].globalPayload = 1 + uvarintLen(costs[i].globalEst) + costs[i].globalEst
			} else {
				costs[i].globalEst = -1
				costs[i].globalPayload = -1
			}
		}
	}

	picksNG := make([]pick, nBlocks)
	totalNG := 0
	for i := 0; i < nBlocks; i++ {
		var eff int
		picksNG[i], eff = pickBest(i, false)
		totalNG += eff
	}

	pickWG := func() ([]pick, int, int) {
		out := make([]pick, nBlocks)
		total := globalSerSize
		users := 0
		for i := 0; i < nBlocks; i++ {
			var eff int
			out[i], eff = pickBest(i, true)
			total += eff
			if out[i].kind == 1 {
				users++
			}
		}
		return out, total, users
	}

	picksWG, totalWG, wgUsers := pickWG()

	// If ≥2 blocks would use the global table, rebuild it from just their
	// histograms (the others' histograms only diluted the table). Commit only
	// when the rebuild improves total bytes AND keeps ≥2 users.
	if globalAvailable && wgUsers >= 2 {
		var subHist [256]uint32
		for i := 0; i < nBlocks; i++ {
			if picksWG[i].kind == 1 {
				for j, v := range e.hist[i] {
					subHist[j] += v
				}
			}
		}
		if buildGlobalFrom(&subHist) {
			recomputeGlobalCosts()
			newPicks, newTotal, newUsers := pickWG()
			if newUsers >= 2 && newTotal < totalWG {
				picksWG, totalWG, wgUsers = newPicks, newTotal, newUsers
			} else {
				// Roll back to the all-blocks global so estimates and the
				// committed table match what picksWG referenced.
				buildGlobalFrom(&sumHist)
				recomputeGlobalCosts()
			}
		}
	}

	// A "shared" table used by only 0 or 1 block isn't shared — treat as
	// wasted overhead and force NG to win.
	wgUsesGlobal := wgUsers >= 2
	if !wgUsesGlobal {
		totalWG = totalNG + globalSerSize
	}

	useGlobal := wgUsesGlobal && totalWG < totalNG
	finalPicks := picksNG
	if useGlobal {
		finalPicks = picksWG
	}

	// Phase 3: if global table is in use, actually compress the global-using
	// blocks against the global table to produce real data lengths.
	if useGlobal {
		e.workSc.TransferCTable(&e.globalSc)
		e.workSc.Reuse = huff0.ReusePolicyMust
		for i := 0; i < nBlocks; i++ {
			if finalPicks[i].kind != 1 {
				continue
			}
			slice := bitmap[i*bsize : (i+1)*bsize]
			e.workSc.Out = e.workSc.Out[:0]
			_, reused, err := huff0.Compress4X(slice, &e.workSc)
			if err != nil || !reused {
				// Global table didn't apply after all; downgrade this block.
				if c := costs[i]; c.ownPayload > 0 && c.ownPayload+c.ownTableSize < c.rawPayload {
					finalPicks[i] = pick{kind: 0, size: c.ownPayload}
				} else if c.rlePayload > 0 && c.rlePayload < c.rawPayload {
					finalPicks[i] = pick{kind: cstDispRLE, size: c.rlePayload}
				} else {
					finalPicks[i] = pick{kind: cstDispRaw, size: c.rawPayload}
				}
				continue
			}
			e.globalData[i] = append(e.globalData[i][:0], e.workSc.OutData...)
			finalPicks[i].size = 1 + uvarintLen(len(e.globalData[i])) + len(e.globalData[i])
		}
	}

	// Build the final table-index map and accumulate actual table bytes.
	// Tables order in the chunk: each retained own table in block order, then
	// the global table (if used).
	type tableRef struct {
		ownBlock int // index in own-tables source (or -1 for global)
	}
	tableList := make([]tableRef, 0, cstMaxHuff0Tables)
	ownTableIdx := make([]int, nBlocks)
	for i := range ownTableIdx {
		ownTableIdx[i] = -1
	}
	for i := 0; i < nBlocks; i++ {
		if finalPicks[i].kind == 0 {
			ownTableIdx[i] = len(tableList)
			tableList = append(tableList, tableRef{ownBlock: i})
		}
	}
	globalTableIdx := -1
	if useGlobal {
		globalTableIdx = len(tableList)
		tableList = append(tableList, tableRef{ownBlock: -1})
	}
	if len(tableList) > cstMaxHuff0Tables {
		// Truncation case: drop the global table to make room. Re-pick blocks
		// that were using global.
		if useGlobal && len(tableList) == cstMaxHuff0Tables+1 {
			globalTableIdx = -1
			tableList = tableList[:cstMaxHuff0Tables]
			for i := 0; i < nBlocks; i++ {
				if finalPicks[i].kind == 1 {
					c := costs[i]
					if c.ownPayload > 0 && c.ownPayload+c.ownTableSize < c.rawPayload {
						// promote to own (would also add table to list — but list is full,
						// so fall back to raw to keep things simple in the rare case).
						finalPicks[i] = pick{kind: cstDispRaw, size: c.rawPayload}
					} else if c.rlePayload > 0 && c.rlePayload < c.rawPayload {
						finalPicks[i] = pick{kind: cstDispRLE, size: c.rlePayload}
					} else {
						finalPicks[i] = pick{kind: cstDispRaw, size: c.rawPayload}
					}
				}
			}
		} else {
			// Should never happen: tableList = own_n (n ≤ 16) + global = at most 17.
			return nil, false, fmt.Errorf("minlz: too many search tables: %d", len(tableList))
		}
	}

	// Pre-emit sparse blocks so we know the actual byte count (selection used
	// an O(1) upper-bound estimate; emission needs the exact length).
	for i := 0; i < nBlocks; i++ {
		if finalPicks[i].kind == cstDispSparse {
			e.sparseBufs[i] = appendSparseBitTable(e.sparseBufs[i][:0], bitmap[i*bsize:(i+1)*bsize])
			actual := len(e.sparseBufs[i])
			finalPicks[i].size = 1 + uvarintLen(actual) + actual
		}
	}

	// Compute final payload size & compare to 0x45.
	tablesSize := 0
	for _, t := range tableList {
		if t.ownBlock >= 0 {
			tablesSize += len(e.outTable[t.ownBlock])
		} else {
			tablesSize += len(e.globalTableBytes)
		}
	}
	payloadSize := 3 + cfg.prefixSize() + 1 /*reductions*/ + 4 /*crc*/ + 2 /*h0_bs+h0_tc*/ + tablesSize
	for i := 0; i < nBlocks; i++ {
		payloadSize += finalPicks[i].size
	}
	totalChunkSize := 4 + payloadSize

	stats.Tables = len(tableList)
	stats.Chunk0x46Size = totalChunkSize
	for i := 0; i < nBlocks; i++ {
		payload := finalPicks[i].size - 1 // exclude disposition byte
		switch finalPicks[i].kind {
		case 0:
			stats.BlocksOwnTable++
			stats.BytesOwnPayload += payload
		case 1:
			stats.BlocksGlobalTable++
			stats.BytesGlobalPayload += payload
		case cstDispRaw:
			stats.BlocksRaw++
			stats.BytesRawPayload += bsize
		case cstDispRLE:
			stats.BlocksRLE++
			stats.BytesRLEPayload++ // 1-byte RLE symbol
		case cstDispSparse:
			stats.BlocksSparse++
			stats.BytesSparsePayload += payload
		}
	}
	for _, t := range tableList {
		if t.ownBlock >= 0 {
			stats.BytesOwnTables += len(e.outTable[t.ownBlock])
		} else {
			stats.BytesGlobalTable = len(e.globalTableBytes)
		}
	}

	if !co.forceCompressed && totalChunkSize >= chunk45Size {
		emitStats()
		return nil, false, nil
	}
	stats.Emitted0x46 = true

	// Serialize the chunk.
	dst = appendChunkHeader(dst, chunkTypeSearchTableCompressed, payloadSize)
	dst = cfg.appendConfig(dst)
	dst = append(dst, reductions)
	dst = binary.LittleEndian.AppendUint32(dst, crc(bitmap))
	dst = append(dst, log2bs, uint8(len(tableList)))
	for _, t := range tableList {
		if t.ownBlock >= 0 {
			dst = append(dst, e.outTable[t.ownBlock]...)
		} else {
			dst = append(dst, e.globalTableBytes...)
		}
	}
	for i := 0; i < nBlocks; i++ {
		p := finalPicks[i]
		switch p.kind {
		case 0:
			dst = append(dst, uint8(ownTableIdx[i]))
			dst = binary.AppendUvarint(dst, uint64(len(e.outData[i])))
			dst = append(dst, e.outData[i]...)
		case 1:
			dst = append(dst, uint8(globalTableIdx))
			dst = binary.AppendUvarint(dst, uint64(len(e.globalData[i])))
			dst = append(dst, e.globalData[i]...)
		case cstDispRaw:
			dst = append(dst, cstDispRaw)
			dst = append(dst, bitmap[i*bsize:(i+1)*bsize]...)
		case cstDispRLE:
			dst = append(dst, cstDispRLE, costs[i].rleSym)
		case cstDispSparse:
			dst = append(dst, cstDispSparse)
			dst = binary.AppendUvarint(dst, uint64(len(e.sparseBufs[i])))
			dst = append(dst, e.sparseBufs[i]...)
		}
	}

	emitStats()
	return dst, true, nil
}

// cstJob describes a single huff0 sub-block to decode/copy/expand.
type cstJob struct {
	dst []byte
	src []byte
	ti  uint8
	rle byte
}

// cstDecoder holds reusable state for decoding 0x46 chunks.
type cstDecoder struct {
	tableSc    [cstMaxHuff0Tables]huff0.Scratch
	tableValid [cstMaxHuff0Tables]bool
	decoders   [cstMaxHuff0Tables]*huff0.Decoder
	// jobs is the per-sub-block work list, pre-allocated up to the max of
	// cstMaxHuff0Tables blocks so parseSearchTableCompressed doesn't
	// allocate on every call.
	jobs [cstMaxHuff0Tables]cstJob
	// bitmapBuf is the reusable decoded-bitmap output. Returned to the
	// caller via parseSearchTableCompressed; the next call re-slices it.
	bitmapBuf []byte
	// last parse stats (populated by parseSearchTableCompressed)
	lastTables           int // number of huff0 tables emitted in the chunk
	lastBlocks           int // number of huff0 sub-blocks
	lastBlocksRaw        int // sub-blocks with disposition = raw
	lastBlocksRLE        int // sub-blocks with disposition = RLE
	lastBlocksSparse     int // sub-blocks with disposition = sparse bit table
	lastBytesTabled      int // sum of (uvarint + data) bytes for tabled blocks
	lastBytesRaw         int // sum of raw payload bytes
	lastBytesRLE         int // sum of RLE payload bytes (1 per block)
	lastBytesSparse      int // sum of (uvarint + data) bytes for sparse blocks
	lastBytesTableHeader int // sum of serialized huff0 table-header bytes
}

func newCSTDecoder() *cstDecoder { return &cstDecoder{} }

// runJob executes a single cstJob (decode/copy/RLE/sparse). Kept as a method
// rather than a closure so parseSearchTableCompressed doesn't allocate a
// closure on the heap when goroutines capture it.
func (dec *cstDecoder) runJob(j *cstJob) error {
	switch {
	case j.ti <= 15:
		out, derr := dec.decoders[j.ti].Decompress4X(j.dst[:0], j.src)
		if derr != nil {
			return derr
		}
		if len(out) != len(j.dst) {
			return fmt.Errorf("minlz: decompressed length %d != expected %d", len(out), len(j.dst))
		}
	case j.ti == cstDispRaw:
		copy(j.dst, j.src)
	case j.ti == cstDispRLE:
		for k := range j.dst {
			j.dst[k] = j.rle
		}
	case j.ti == cstDispSparse:
		// dst is a slice of the (already-zeroed) bitmapBuf; decodeSparseBitTable
		// will OR in the set bits. The slice has cap == len so reuse-from-prior
		// chunk is safe — the slice's window is exclusive to this huff0 block.
		for k := range j.dst {
			j.dst[k] = 0
		}
		if e := decodeSparseBitTable(j.dst, j.src); e != nil {
			return e
		}
	}
	return nil
}

// parseSearchTableCompressedHeader parses just the config + reductions from a
// 0x46 chunk payload, without touching the bitmap data. Use this when the
// caller may want to skip the (expensive) bitmap decode based on the config
// alone, then call parseSearchTableCompressed only when the bitmap is needed.
func parseSearchTableCompressedHeader(payload []byte) (cfg SearchTableConfig, reductions uint8, err error) {
	cfg, err = parseSearchInfo(payload)
	if err != nil {
		return
	}
	off := 3 + cfg.prefixSize()
	// Mirror parseSearchTableCompressed's structural check: require room for the
	// full fixed header (reductions + crc + log2bs + tc) so a truncated chunk is
	// rejected here rather than slipping past the caller's skip decision.
	if off+1+4+2 > len(payload) {
		err = fmt.Errorf("minlz: compressed search table chunk too short for header")
		return
	}
	reductions = payload[off]
	return
}

// parseSearchTableCompressed parses a 0x46 chunk payload (the bytes after the
// 4-byte chunk header) and reconstructs the uncompressed bitmap. The returned
// table slice is freshly allocated; callers are responsible for any pooling.
func parseSearchTableCompressed(payload []byte, dec *cstDecoder, ignoreCRC bool) (cfg SearchTableConfig, reductions uint8, table []byte, err error) {
	cfg, err = parseSearchInfo(payload)
	if err != nil {
		return
	}
	off := 3 + cfg.prefixSize()
	if off+1+4+2 > len(payload) {
		err = fmt.Errorf("minlz: compressed search table chunk too short for header")
		return
	}
	reductions = payload[off]
	off++
	crc32 := binary.LittleEndian.Uint32(payload[off:])
	off += 4
	log2bs := payload[off]
	off++
	tc := int(payload[off])
	off++

	if log2bs < cstMinHuff0BlockLog2 || log2bs > cstMaxHuff0BlockLog2 {
		err = fmt.Errorf("minlz: invalid huff0 block log2 %d", log2bs)
		return
	}
	if tc > cstMaxHuff0Tables {
		err = fmt.Errorf("minlz: invalid huff0 table count %d", tc)
		return
	}
	if cfg.baseTableSize <= reductions+3 {
		err = fmt.Errorf("minlz: invalid reductions %d for baseTableSize %d", reductions, cfg.baseTableSize)
		return
	}
	expectedSize := 1 << (cfg.baseTableSize - reductions - 3)
	blockSize := 1 << log2bs
	if expectedSize%blockSize != 0 {
		err = fmt.Errorf("minlz: bitmap size %d not divisible by huff0 block size %d", expectedSize, blockSize)
		return
	}
	nBlocks := expectedSize / blockSize
	if nBlocks < 1 || nBlocks > cstMaxHuff0Tables {
		err = fmt.Errorf("minlz: invalid huff0 block count %d", nBlocks)
		return
	}

	// Parse tables.
	if dec == nil {
		dec = newCSTDecoder()
	}
	rem := payload[off:]
	dec.lastBytesTableHeader = 0
	for i := 0; i < tc; i++ {
		dec.tableSc[i].Out = dec.tableSc[i].Out[:0]
		before := len(rem)
		_, after, err2 := huff0.ReadTable(rem, &dec.tableSc[i])
		if err2 != nil {
			err = fmt.Errorf("minlz: huff0 ReadTable[%d]: %w", i, err2)
			return
		}
		dec.lastBytesTableHeader += before - len(after)
		dec.tableValid[i] = true
		dec.decoders[i] = dec.tableSc[i].Decoder()
		rem = after
	}
	// Invalidate unused decoders from a prior chunk.
	for i := tc; i < cstMaxHuff0Tables; i++ {
		dec.tableValid[i] = false
		dec.decoders[i] = nil
	}

	// Parse per-block records (sequentially) and decompress (concurrently).
	if cap(dec.bitmapBuf) < expectedSize {
		dec.bitmapBuf = make([]byte, expectedSize)
	}
	table = dec.bitmapBuf[:expectedSize]
	jobs := dec.jobs[:nBlocks]
	// Reset per-parse stats.
	dec.lastTables = tc
	dec.lastBlocks = nBlocks
	dec.lastBlocksRaw = 0
	dec.lastBlocksRLE = 0
	dec.lastBlocksSparse = 0
	dec.lastBytesTabled = 0
	dec.lastBytesRaw = 0
	dec.lastBytesRLE = 0
	dec.lastBytesSparse = 0
	for i := 0; i < nBlocks; i++ {
		if len(rem) < 1 {
			err = fmt.Errorf("minlz: truncated chunk at block %d disposition", i)
			return
		}
		ti := rem[0]
		rem = rem[1:]
		// 3-index slice limits cap to blockSize so huff0.Decompress4X computes
		// the right per-stream length (it uses cap(dst), not len(dst)).
		dst := table[i*blockSize : (i+1)*blockSize : (i+1)*blockSize]
		switch {
		case ti <= 15:
			if int(ti) >= tc {
				err = fmt.Errorf("minlz: block %d references unknown table %d", i, ti)
				return
			}
			n, nl := binary.Uvarint(rem)
			// Compare in uint64 space: an attacker-supplied uvarint can be > 2^63,
			// and casting to int first would wrap to negative.
			if nl <= 0 || nl > len(rem) || n > uint64(len(rem)-nl) {
				err = fmt.Errorf("minlz: block %d invalid compressed length", i)
				return
			}
			nInt := int(n)
			jobs[i] = cstJob{dst: dst, src: rem[nl : nl+nInt], ti: ti}
			rem = rem[nl+nInt:]
			dec.lastBytesTabled += nl + nInt
		case ti == cstDispRaw:
			if len(rem) < blockSize {
				err = fmt.Errorf("minlz: block %d raw payload truncated", i)
				return
			}
			jobs[i] = cstJob{dst: dst, src: rem[:blockSize], ti: ti}
			rem = rem[blockSize:]
			dec.lastBlocksRaw++
			dec.lastBytesRaw += blockSize
		case ti == cstDispRLE:
			if len(rem) < 1 {
				err = fmt.Errorf("minlz: block %d RLE payload truncated", i)
				return
			}
			jobs[i] = cstJob{dst: dst, ti: ti, rle: rem[0]}
			rem = rem[1:]
			dec.lastBlocksRLE++
			dec.lastBytesRLE++
		case ti == cstDispSparse:
			n, nl := binary.Uvarint(rem)
			if nl <= 0 || nl > len(rem) || n > uint64(len(rem)-nl) {
				err = fmt.Errorf("minlz: block %d invalid sparse length", i)
				return
			}
			nInt := int(n)
			jobs[i] = cstJob{dst: dst, src: rem[nl : nl+nInt], ti: ti}
			rem = rem[nl+nInt:]
			dec.lastBlocksSparse++
			dec.lastBytesSparse += nl + nInt
		default:
			err = fmt.Errorf("minlz: invalid disposition %d", ti)
			return
		}
	}
	if len(rem) != 0 {
		err = fmt.Errorf("minlz: trailing %d bytes after chunk", len(rem))
		return
	}

	if nBlocks == 1 {
		if derr := dec.runJob(&jobs[0]); derr != nil {
			err = derr
			return
		}
	} else {
		var wg sync.WaitGroup
		var derrMu sync.Mutex
		var derr error
		for i := range jobs {
			wg.Add(1)
			go func(j *cstJob) {
				defer wg.Done()
				if e := dec.runJob(j); e != nil {
					derrMu.Lock()
					if derr == nil {
						derr = e
					}
					derrMu.Unlock()
				}
			}(&jobs[i])
		}
		wg.Wait()
		if derr != nil {
			err = derr
			return
		}
	}

	if !ignoreCRC && crc(table) != crc32 {
		err = fmt.Errorf("minlz: compressed search table CRC mismatch")
		return
	}
	return
}

var cstEncoderPool = sync.Pool{New: func() any { return newCSTEncoder() }}
