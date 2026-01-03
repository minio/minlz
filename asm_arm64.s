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

//go:build arm64 && !appengine && !noasm && gc && !purego

#include "textflag.h"

// Register allocation:
// R0:  return value (0=success, 1=corrupt)
// R1:  dst base (parameter, then dst current pointer)
// R2:  dst len (parameter)
// R3:  src base (parameter, then src current pointer)
// R4:  src len (parameter)
// R5:  dstEnd (dst base + dst len)
// R6:  srcEnd (src base + src len)
// R7:  offset (last copy offset, init to 1)
// R8:  dstPos (current position in dst for bounds checking)
// R9:  scratch / length
// R10: scratch / tag value
// R11: scratch / value (tag >> 2)
// R12: srcLimit (srcEnd - margin)
// R13: dstLimit (dstEnd - margin)
// R14: copySrc pointer for copies
// R15: scratch
// R16: scratch
// R17: scratch
// R19-R28: callee-saved (we save/restore as needed)

// Constants
#define tagLiteral 0x00
#define tagCopy1   0x01
#define tagCopy2   0x02
#define tagCopy3   0x03
#define minCopy2Offset 64
#define minCopy3Offset 65536

// func decodeBlockAsm(dst []byte, src []byte) int
TEXT ·decodeBlockAsm(SB), NOSPLIT, $0-56
	// Load parameters
	// dst is at 0(FP): base=0, len=8, cap=16
	// src is at 24(FP): base=24, len=32, cap=40
	MOVD dst_base+0(FP), R1     // dst base -> R1 (current dst ptr)
	MOVD dst_len+8(FP), R2      // dst len
	MOVD src_base+24(FP), R3    // src base -> R3 (current src ptr)
	MOVD src_len+32(FP), R4     // src len

	// Save dst base for final validation
	MOVD R1, R19                // save original dst base

	// Calculate end pointers
	ADD R1, R2, R5              // dstEnd = dst + len
	ADD R3, R4, R6              // srcEnd = src + len

	// Initialize offset to 1 (for repeat operations)
	MOVD $1, R7

	// Initialize dstPos to 0
	MOVD ZR, R8

	// Check if we have enough data for fast loop margins
	// Skip directly to slow loop if srcLen < 36 or dstLen < 32
	// We need 36-byte src margin (32 for NEON loads + 4 for max tag header size)
	// We need 32-byte dst margin for safe NEON over-writes
	CMP $36, R4
	BLO decode_remain_loop      // srcLen < 36, use slow loop
	CMP $32, R2
	BLO decode_remain_loop      // dstLen < 32, use slow loop

	// Calculate limits with margins for fast loop
	// srcLimit = srcEnd - 36 (ensures safe 32-byte reads after 4-byte tag consumption)
	// dstLimit = dstEnd - 32
	SUB $36, R6, R12            // srcLimit
	SUB $32, R5, R13            // dstLimit

	// ============================================
	// FAST LOOP - with 36-byte src / 32-byte dst margins
	// ============================================
decode_fast_loop:
	// Note: Go ARM64 CMP Rm, Rn computes Rn - Rm, so CMP Ra, Rb; BHS branches when Rb >= Ra
	CMP R12, R3
	BHS decode_remain_entry     // if src >= srcLimit (R3 - R12 >= 0), switch to slow loop
	CMP R13, R1
	BHS decode_remain_entry     // if dst >= dstLimit (R1 - R13 >= 0), switch to slow loop

	// Read tag byte
	MOVBU (R3), R10             // tag byte
	MOVD R10, R11
	LSR $2, R11, R11            // value = tag >> 2

	// Check tag type (lower 2 bits)
	AND $0x03, R10, R16
	CBNZ R16, decode_fast_copy  // if tag != 0, it's a copy

	// ----------------------------------------
	// TAG 0: Literals or Repeat
	// ----------------------------------------
decode_fast_lits:
	// Extract length field (bits 3-7)
	LSR $1, R11, R9             // length field = value >> 1
	CMP $29, R9
	BLT decode_fast_lit_short   // 0-28: short literal
	BEQ decode_fast_lit_1byte   // 29: 1 byte length
	CMP $30, R9
	BEQ decode_fast_lit_2byte   // 30: 2 byte length
	B decode_fast_lit_3byte     // 31: 3 byte length

decode_fast_lit_short:
	// Length = field + 1, single byte tag
	ADD $1, R9, R9
	ADD $1, R3, R3              // consume tag byte
	B decode_fast_lit_check_repeat

decode_fast_lit_1byte:
	// Length = 30 + next byte
	MOVBU 1(R3), R9
	ADD $30, R9, R9
	ADD $2, R3, R3
	B decode_fast_lit_check_repeat

decode_fast_lit_2byte:
	// Length = 30 + next 2 bytes (little endian)
	MOVHU 1(R3), R9
	ADD $30, R9, R9
	ADD $3, R3, R3
	B decode_fast_lit_check_repeat

decode_fast_lit_3byte:
	// Length = 30 + next 3 bytes (little endian)
	// Load 4 bytes and mask to 24 bits
	MOVWU (R3), R9
	LSR $8, R9, R9              // shift out the tag byte
	ADD $30, R9, R9
	ADD $4, R3, R3

decode_fast_lit_check_repeat:
	// Check if this is a repeat (bit 2 of original tag)
	TBZ $0, R11, decode_fast_lit_copy // if bit 0 of value (bit 2 of tag) is 0, do literal

	// This is a REPEAT - use stored offset
	B decode_fast_copy_exec

decode_fast_lit_copy:
	// Bounds check: dst + length <= dstEnd
	ADD R1, R9, R16
	CMP R5, R16
	BHI corrupt                 // if dst + length > dstEnd, corrupt

	// Bounds check: src + length <= srcEnd
	ADD R3, R9, R16
	CMP R6, R16
	BHI corrupt                 // if src + length > srcEnd, corrupt

	// Copy literals
	CMP $16, R9
	BLE decode_fast_lit_copy_16
	CMP $32, R9
	BLE decode_fast_lit_copy_32
	B decode_fast_lit_copy_long

decode_fast_lit_copy_16:
	// Copy up to 16 bytes using NEON
	VLD1 (R3), [V0.B16]
	VST1 [V0.B16], (R1)
	ADD R9, R3, R3
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_lit_copy_32:
	// Copy up to 32 bytes
	VLD1 (R3), [V0.B16, V1.B16]
	VST1 [V0.B16, V1.B16], (R1)
	ADD R9, R3, R3
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_lit_copy_long:
	// Copy 64+ bytes or handle 33-64
	CMP $64, R9
	BLE decode_fast_lit_copy_64

	// AMD64-style long literal copy using overlapping writes
	// This avoids byte-by-byte remainder handling:
	// 1. Load first 32 bytes into V0,V1
	// 2. Load last 32 bytes into V2,V3
	// 3. Loop through middle 32-byte chunks
	// 4. Write first 32 and last 32 bytes (overlapping handles remainder)

	// Load first 32 bytes
	VLD1 (R3), [V0.B16, V1.B16]

	// Load last 32 bytes
	SUB $32, R9, R16              // R16 = length - 32
	ADD R16, R3, R17              // R17 = src + length - 32
	VLD1 (R17), [V2.B16, V3.B16]

	// Calculate middle length (bytes 32 to length-32)
	// Middle exists if length > 64 (length - 64 > 0)
	MOVD R9, R15                  // R15 = length
	SUB $64, R15, R15             // R15 = middle length

	// Setup middle pointers
	ADD $32, R3, R14              // R14 = src + 32 (middle src pointer)
	ADD $32, R1, R17              // R17 = dst + 32 (middle dst pointer)

	// Copy middle bytes in 32-byte chunks
	CMP ZR, R15
	BLE decode_fast_lit_copy_long_finish

decode_fast_lit_copy_long_loop:
	VLD1.P 32(R14), [V4.B16, V5.B16]
	VST1.P [V4.B16, V5.B16], 32(R17)
	SUB $32, R15, R15
	CMP ZR, R15
	BGT decode_fast_lit_copy_long_loop

decode_fast_lit_copy_long_finish:
	// Write first 32 bytes at dst
	VST1 [V0.B16, V1.B16], (R1)

	// Write last 32 bytes at dst + length - 32
	SUB $32, R9, R16              // R16 = length - 32
	ADD R16, R1, R17              // R17 = dst + length - 32
	VST1 [V2.B16, V3.B16], (R17)

	// Advance pointers by full length
	ADD R9, R3, R3
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_lit_copy_64:
	// Copy 33-64 bytes
	VLD1 (R3), [V0.B16, V1.B16]
	SUB $32, R9, R16
	ADD R16, R3, R17            // src + length - 32
	VLD1 (R17), [V2.B16, V3.B16]
	VST1 [V0.B16, V1.B16], (R1)
	ADD R16, R1, R17            // dst + length - 32
	VST1 [V2.B16, V3.B16], (R17)
	ADD R9, R3, R3
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

	// ----------------------------------------
	// Copy operations
	// ----------------------------------------
decode_fast_copy:
	CMP $2, R16
	BLT decode_fast_copy1
	BEQ decode_fast_copy2
	B decode_fast_copy3

	// ----------------------------------------
	// TAG 1: Copy with 10-bit offset
	// ----------------------------------------
decode_fast_copy1:
	// Format: [offset_lo:2 | length:4 | tag:2] [offset_hi:8]
	// Length: 4-18 (0-14 inline) or 18+byte (15 extended)
	// Offset: 1-1024 (stored as 0-1023)

	// Read 2 bytes for tag+offset
	MOVHU (R3), R16             // load 2 bytes

	// Extract length (bits 2-5 of first byte)
	AND $0x0F, R11, R9          // length field = (tag >> 2) & 0x0F

	// Extract offset
	LSR $6, R16, R7             // offset = word >> 6
	ADD $1, R7, R7              // offset += 1 (min offset is 1)

	// Check if extended length
	CMP $15, R9
	BEQ decode_fast_copy1_extended

	// Short length: 4 + field
	ADD $4, R9, R9
	ADD $2, R3, R3              // consume 2 bytes
	B decode_fast_copy_exec

decode_fast_copy1_extended:
	// Extended length: 18 + next byte
	MOVBU 2(R3), R9
	ADD $18, R9, R9
	ADD $3, R3, R3              // consume 3 bytes
	B decode_fast_copy_exec

	// ----------------------------------------
	// TAG 2: Copy with 16-bit offset
	// ----------------------------------------
decode_fast_copy2:
	// Format: [length:6 | tag:2] [offset:16]
	// Length: 4-64 (0-60 inline) or 64+1/2/3 bytes (61/62/63 extended)
	// Offset: 64-65599 (stored + 64)

	// value already has tag >> 2 = length field
	MOVD R11, R9                // length field

	CMP $61, R9
	BGE decode_fast_copy2_extended

	// Short length: 4 + field
	ADD $4, R9, R9

	// Read 16-bit offset
	MOVHU 1(R3), R7
	ADD $minCopy2Offset, R7, R7
	ADD $3, R3, R3
	B decode_fast_copy_exec

decode_fast_copy2_extended:
	BEQ decode_fast_copy2_ext1
	CMP $62, R9
	BEQ decode_fast_copy2_ext2
	// 63: 3 byte length extension

	// Read offset first
	MOVHU 1(R3), R7
	ADD $minCopy2Offset, R7, R7

	// Read 3-byte length (load 4, shift)
	MOVWU 2(R3), R9
	LSR $8, R9, R9
	ADD $64, R9, R9
	ADD $6, R3, R3
	B decode_fast_copy_exec

decode_fast_copy2_ext2:
	// 2 byte length extension
	MOVHU 1(R3), R7
	ADD $minCopy2Offset, R7, R7
	MOVHU 3(R3), R9
	ADD $64, R9, R9
	ADD $5, R3, R3
	B decode_fast_copy_exec

decode_fast_copy2_ext1:
	// 1 byte length extension
	MOVHU 1(R3), R7
	ADD $minCopy2Offset, R7, R7
	MOVBU 3(R3), R9
	ADD $64, R9, R9
	ADD $4, R3, R3
	B decode_fast_copy_exec

	// ----------------------------------------
	// TAG 3: Fused Copy2/3
	// ----------------------------------------
decode_fast_copy3:
	// Load 4 bytes for full analysis
	MOVWU (R3), R16

	// Check if Copy3 (bit 2 set) or Fused Copy2 (bit 2 clear)
	TBZ $2, R16, decode_fast_copy2_fused

	// ---- Copy3 with optional fused literals ----
	// Extract literal count (bits 3-4)
	LSR $3, R16, R17
	AND $0x03, R17, R14         // litLen

	// Extract length field (bits 5-10)
	LSR $5, R16, R9
	AND $0x3F, R9, R9

	// Extract offset (bits 11-31, 21 bits) + 65536
	LSR $11, R16, R7
	ADD $minCopy3Offset, R7, R7

	ADD $4, R3, R3              // consume base 4 bytes

	// Check for extended length
	CMP $61, R9
	BGE decode_fast_copy3_extended

	// Short length: 4 + field
	ADD $4, R9, R9
	B decode_fast_copy3_lits

decode_fast_copy3_extended:
	BEQ decode_fast_copy3_ext1
	CMP $62, R9
	BEQ decode_fast_copy3_ext2

	// 3 byte length extension
	MOVWU -1(R3), R9            // reload from offset-1 to get bytes
	LSR $8, R9, R9
	ADD $64, R9, R9
	ADD $3, R3, R3
	B decode_fast_copy3_lits

decode_fast_copy3_ext2:
	MOVHU (R3), R9
	ADD $64, R9, R9
	ADD $2, R3, R3
	B decode_fast_copy3_lits

decode_fast_copy3_ext1:
	MOVBU (R3), R9
	ADD $64, R9, R9
	ADD $1, R3, R3

decode_fast_copy3_lits:
	// Handle fused literals (0-3 bytes)
	CBZ R14, decode_fast_copy_exec

	// Copy fused literals (1-3 bytes)
	// Bounds check
	ADD R1, R14, R16
	CMP R5, R16
	BHI corrupt

	// Load and store up to 4 bytes (we have margin)
	MOVWU (R3), R16
	MOVW R16, (R1)
	ADD R14, R3, R3
	ADD R14, R1, R1
	ADD R14, R8, R8
	B decode_fast_copy_exec

decode_fast_copy2_fused:
	// Fused Copy2: [len:3 | litLen:2 | 0 | tag:2] [offset:16] [lits:1-4]
	// litLen is 0-3, but actual count is litLen+1 (1-4)
	// length is 4-11

	// Extract literal count (bits 3-4) + 1
	LSR $3, R16, R14
	AND $0x03, R14, R14
	ADD $1, R14, R14            // litLen = field + 1

	// Extract length (bits 5-7) + 4
	LSR $5, R16, R9
	AND $0x07, R9, R9
	ADD $4, R9, R9

	// Extract offset (bits 8-23) + 64
	LSR $8, R16, R7
	AND $0xFFFF, R7, R7
	ADD $minCopy2Offset, R7, R7

	ADD $3, R3, R3              // consume 3 bytes (tag + offset)

	// Copy fused literals (1-4 bytes)
	// Bounds check
	ADD R1, R14, R16
	CMP R5, R16
	BHI corrupt

	// Load and store 4 bytes (we have margin for over-read)
	MOVWU (R3), R16
	MOVW R16, (R1)
	ADD R14, R3, R3
	ADD R14, R1, R1
	ADD R14, R8, R8
	// Fall through to copy exec

	// ----------------------------------------
	// Execute copy operation
	// R7 = offset, R9 = length
	// ----------------------------------------
decode_fast_copy_exec:
	// Bounds check: offset <= dstPos
	// CMP R8, R7 computes R7 - R8; BHI branches when R7 > R8 (offset > dstPos)
	CMP R8, R7
	BHI corrupt

	// Bounds check: dst + length <= dstEnd
	ADD R1, R9, R16
	CMP R5, R16
	BHI corrupt

	// Calculate source pointer
	SUB R7, R1, R14             // copySrc = dst - offset

	// Check for overlap: if offset < length, we have overlap
	// CMP R9, R7 computes R7 - R9; BLO branches when R7 < R9 (offset < length)
	CMP R9, R7
	BLO decode_fast_copy_overlap

	// No overlap - can use fast copy
	CMP $16, R9
	BLE decode_fast_copy_16
	CMP $32, R9
	BLE decode_fast_copy_32
	CMP $64, R9
	BLE decode_fast_copy_64
	B decode_fast_copy_long_nool

decode_fast_copy_16:
	VLD1 (R14), [V0.B16]
	VST1 [V0.B16], (R1)
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_copy_32:
	VLD1 (R14), [V0.B16, V1.B16]
	VST1 [V0.B16, V1.B16], (R1)
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_copy_64:
	// For 33-64 bytes, use overlapping reads/writes (like decode_fast_lit_copy_64)
	// This avoids over-writing past dst+length
	VLD1 (R14), [V0.B16, V1.B16]      // load 32 bytes at copySrc
	SUB $32, R9, R16                   // R16 = length - 32
	ADD R16, R14, R17                  // R17 = copySrc + length - 32
	VLD1 (R17), [V2.B16, V3.B16]      // load 32 bytes at copySrc + length - 32
	VST1 [V0.B16, V1.B16], (R1)       // store 32 bytes at dst
	ADD R16, R1, R17                   // R17 = dst + length - 32
	VST1 [V2.B16, V3.B16], (R17)      // store 32 bytes at dst + length - 32
	ADD R9, R1, R1
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_copy_long_nool:
	// Long copy without overlap - loop
	MOVD R9, R15

decode_fast_copy_long_loop:
	VLD1.P 32(R14), [V0.B16, V1.B16]
	VST1.P [V0.B16, V1.B16], 32(R1)
	SUB $32, R15, R15
	CMP $32, R15
	BGE decode_fast_copy_long_loop

	// Handle remaining 0-31 bytes
	CBZ R15, decode_fast_copy_long_done

	// Copy remainder byte-by-byte for correctness
decode_fast_copy_long_remainder:
	MOVBU (R14), R16
	MOVB R16, (R1)
	ADD $1, R14, R14
	ADD $1, R1, R1
	SUB $1, R15, R15
	CBNZ R15, decode_fast_copy_long_remainder

decode_fast_copy_long_done:
	ADD R9, R8, R8
	B decode_fast_loop

decode_fast_copy_overlap:
	// Overlapping copy - need byte-by-byte or special handling
	// Check for special small offsets

	CMP $1, R7
	BEQ decode_fast_copy_overlap_1
	CMP $2, R7
	BEQ decode_fast_copy_overlap_2
	CMP $3, R7
	BEQ decode_fast_copy_overlap_3
	B decode_fast_copy_overlap_4plus

decode_fast_copy_overlap_1:
	// Offset 1: RLE - repeat single byte
	MOVBU (R14), R16
	ADD R9, R8, R8

	// For RLE, use simple byte loop - it's not that slow for small lengths
	// and avoids complex SIMD setup
decode_overlap1_byte_loop:
	MOVB R16, (R1)
	ADD $1, R1, R1
	SUB $1, R9, R9
	CBNZ R9, decode_overlap1_byte_loop
	B decode_fast_loop

decode_fast_copy_overlap_2:
	// Offset 2: repeat 2-byte pattern
	MOVHU (R14), R16
	ADD R9, R8, R8

decode_overlap2_word_loop:
	// Write 2 bytes at a time as long as we have >= 2 bytes left
	CMP $2, R9
	BLT decode_overlap2_finish
	MOVH R16, (R1)
	ADD $2, R1, R1
	SUB $2, R9, R9
	B decode_overlap2_word_loop

decode_overlap2_finish:
	// Handle remaining 0 or 1 byte at the END
	CBZ R9, decode_fast_loop
	MOVB R16, (R1)
	ADD $1, R1, R1
	B decode_fast_loop

decode_fast_copy_overlap_3:
	// Offset 3: repeat 3-byte pattern
	MOVWU (R14), R16            // load 4 bytes
	AND $0xFFFFFF, R16, R16     // keep only 3 bytes

	ADD R9, R8, R8

decode_overlap3_loop:
	// Check if we have at least 3 bytes left
	CMP $3, R9
	BLT decode_overlap3_finish
	// Write 3 bytes (actually writes 4, but we only advance by 3)
	MOVW R16, (R1)
	ADD $3, R1, R1
	SUB $3, R9, R9
	B decode_overlap3_loop

decode_overlap3_finish:
	// Handle remaining 1-2 bytes
	CBZ R9, decode_fast_loop
	CMP $1, R9
	BEQ decode_overlap3_1byte
	// 2 bytes
	MOVH R16, (R1)
	ADD $2, R1, R1
	B decode_fast_loop

decode_overlap3_1byte:
	MOVB R16, (R1)
	ADD $1, R1, R1
	B decode_fast_loop

decode_fast_copy_overlap_4plus:
	// Offset 4+: general overlapping copy
	ADD R9, R8, R8

decode_overlap4_loop:
	CMP $4, R9
	BLT decode_overlap4_remainder
	MOVWU (R14), R16
	MOVW R16, (R1)
	ADD $4, R14, R14
	ADD $4, R1, R1
	SUB $4, R9, R9
	B decode_overlap4_loop

decode_overlap4_remainder:
	CBZ R9, decode_fast_loop

decode_overlap4_byte_loop:
	MOVBU (R14), R16
	MOVB R16, (R1)
	ADD $1, R14, R14
	ADD $1, R1, R1
	SUB $1, R9, R9
	CBNZ R9, decode_overlap4_byte_loop
	B decode_fast_loop

	// ============================================
	// SLOW LOOP - no margins, full bounds checking
	// ============================================
decode_remain_entry:
	// Entry point after fast loop

decode_remain_loop:
	// Check if we're done
	// Note: Go ARM64 CMP Rm, Rn computes Rn - Rm
	CMP R6, R3
	BHS decode_end              // if src >= srcEnd (R3 - R6 >= 0), done

	// Read tag byte
	MOVBU (R3), R10
	MOVD R10, R11
	LSR $2, R11, R11            // value = tag >> 2

	// Check tag type
	AND $0x03, R10, R16
	CBNZ R16, decode_remain_copy

	// ----------------------------------------
	// TAG 0: Literals or Repeat (slow path)
	// ----------------------------------------
	LSR $1, R11, R9             // length field
	CMP $29, R9
	BLT decode_remain_lit_short
	BEQ decode_remain_lit_1byte
	CMP $30, R9
	BEQ decode_remain_lit_2byte
	B decode_remain_lit_3byte

decode_remain_lit_short:
	ADD $1, R9, R9
	ADD $1, R3, R3
	B decode_remain_lit_check

decode_remain_lit_1byte:
	ADD $2, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVBU -1(R3), R9
	ADD $30, R9, R9
	B decode_remain_lit_check

decode_remain_lit_2byte:
	ADD $3, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVHU -2(R3), R9
	ADD $30, R9, R9
	B decode_remain_lit_check

decode_remain_lit_3byte:
	ADD $4, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVWU -4(R3), R9
	LSR $8, R9, R9
	ADD $30, R9, R9

decode_remain_lit_check:
	// Check repeat bit
	TBZ $0, R11, decode_remain_lit_copy

	// REPEAT - use stored offset
	B decode_remain_copy_exec

decode_remain_lit_copy:
	// Bounds check
	ADD R1, R9, R16
	CMP R5, R16
	BHI corrupt
	ADD R3, R9, R16
	CMP R6, R16
	BHI corrupt

	// Copy literals (slow path, byte by byte for simplicity)
decode_remain_lit_loop:
	CBZ R9, decode_remain_loop
	MOVBU (R3), R16
	MOVB R16, (R1)
	ADD $1, R3, R3
	ADD $1, R1, R1
	ADD $1, R8, R8
	SUB $1, R9, R9
	B decode_remain_lit_loop

	// ----------------------------------------
	// Copy operations (slow path)
	// ----------------------------------------
decode_remain_copy:
	CMP $2, R16
	BLT decode_remain_copy1
	BEQ decode_remain_copy2
	B decode_remain_copy3

decode_remain_copy1:
	ADD $2, R3, R3
	CMP R6, R3
	BHI corrupt

	MOVHU -2(R3), R16
	AND $0x0F, R11, R9
	LSR $6, R16, R7
	ADD $1, R7, R7

	CMP $15, R9
	BNE decode_remain_copy1_short

	ADD $1, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVBU -1(R3), R9
	ADD $18, R9, R9
	B decode_remain_copy_exec

decode_remain_copy1_short:
	ADD $4, R9, R9
	B decode_remain_copy_exec

decode_remain_copy2:
	ADD $3, R3, R3
	CMP R6, R3
	BHI corrupt

	MOVBU -3(R3), R9
	LSR $2, R9, R9
	MOVHU -2(R3), R7

	CMP $61, R9
	BGE decode_remain_copy2_ext

	ADD $4, R9, R9
	ADD $minCopy2Offset, R7, R7
	B decode_remain_copy_exec

decode_remain_copy2_ext:
	// Dispatch based on R9 (from CMP $61, R9 above)
	// Note: Don't modify flags before these branches!
	BEQ decode_remain_copy2_ext1    // R9 == 61
	CMP $62, R9
	BEQ decode_remain_copy2_ext2    // R9 == 62
	// Fall through for R9 == 63

	// 3 byte extension
	ADD $minCopy2Offset, R7, R7     // Add offset base
	ADD $3, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVWU -4(R3), R9
	LSR $8, R9, R9
	ADD $64, R9, R9
	B decode_remain_copy_exec

decode_remain_copy2_ext2:
	ADD $minCopy2Offset, R7, R7     // Add offset base
	ADD $2, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVHU -2(R3), R9
	ADD $64, R9, R9
	B decode_remain_copy_exec

decode_remain_copy2_ext1:
	ADD $minCopy2Offset, R7, R7     // Add offset base
	ADD $1, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVBU -1(R3), R9
	ADD $64, R9, R9
	B decode_remain_copy_exec

decode_remain_copy3:
	ADD $4, R3, R3
	CMP R6, R3
	BHI corrupt

	MOVWU -4(R3), R16
	TBZ $2, R16, decode_remain_copy2_fused

	// Copy3
	LSR $3, R16, R17
	AND $0x03, R17, R14         // litLen

	LSR $5, R16, R9
	AND $0x3F, R9, R9

	LSR $11, R16, R7
	ADD $minCopy3Offset, R7, R7

	CMP $61, R9
	BGE decode_remain_copy3_ext

	ADD $4, R9, R9
	B decode_remain_copy3_lits

decode_remain_copy3_ext:
	BEQ decode_remain_copy3_ext1
	CMP $62, R9
	BEQ decode_remain_copy3_ext2

	// 3 byte ext
	ADD $3, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVWU -4(R3), R9
	LSR $8, R9, R9
	ADD $64, R9, R9
	B decode_remain_copy3_lits

decode_remain_copy3_ext2:
	ADD $2, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVHU -2(R3), R9
	ADD $64, R9, R9
	B decode_remain_copy3_lits

decode_remain_copy3_ext1:
	ADD $1, R3, R3
	CMP R6, R3
	BHI corrupt
	MOVBU -1(R3), R9
	ADD $64, R9, R9

decode_remain_copy3_lits:
	CBZ R14, decode_remain_copy_exec

	// Bounds check and copy fused literals
	ADD R1, R14, R16
	CMP R5, R16
	BHI corrupt
	ADD R3, R14, R16
	CMP R6, R16
	BHI corrupt

decode_remain_copy3_lit_loop:
	CBZ R14, decode_remain_copy_exec
	MOVBU (R3), R16
	MOVB R16, (R1)
	ADD $1, R3, R3
	ADD $1, R1, R1
	ADD $1, R8, R8
	SUB $1, R14, R14
	B decode_remain_copy3_lit_loop

decode_remain_copy2_fused:
	// Fused Copy2
	LSR $3, R16, R14
	AND $0x03, R14, R14
	ADD $1, R14, R14

	LSR $5, R16, R9
	AND $0x07, R9, R9
	ADD $4, R9, R9

	LSR $8, R16, R7
	AND $0xFFFF, R7, R7
	ADD $minCopy2Offset, R7, R7

	SUB $1, R3, R3              // back up 1 byte (we consumed 4, need 3)

	// Bounds check and copy fused literals
	ADD R1, R14, R16
	CMP R5, R16
	BHI corrupt
	ADD R3, R14, R16
	CMP R6, R16
	BHI corrupt

decode_remain_copy2_fused_lit_loop:
	CBZ R14, decode_remain_copy_exec
	MOVBU (R3), R16
	MOVB R16, (R1)
	ADD $1, R3, R3
	ADD $1, R1, R1
	ADD $1, R8, R8
	SUB $1, R14, R14
	B decode_remain_copy2_fused_lit_loop

decode_remain_copy_exec:
	// Bounds check: offset <= dstPos
	// CMP R8, R7 computes R7 - R8; BHI branches when R7 > R8 (offset > dstPos)
	CMP R8, R7
	BHI corrupt

	ADD R1, R9, R16
	CMP R5, R16
	BHI corrupt

	// Calculate source
	SUB R7, R1, R14

	// Simple byte-by-byte copy for slow path
	ADD R9, R8, R8

decode_remain_copy_loop:
	CBZ R9, decode_remain_loop
	MOVBU (R14), R16
	MOVB R16, (R1)
	ADD $1, R14, R14
	ADD $1, R1, R1
	SUB $1, R9, R9
	B decode_remain_copy_loop

	// ============================================
	// END - Validation
	// ============================================
decode_end:
	// Validate we consumed all input and filled all output
	// dst should equal dstEnd (original dst base + dst len)
	// src should equal srcEnd

	CMP R5, R1
	BNE corrupt                 // dst != dstEnd
	CMP R6, R3
	BNE corrupt                 // src != srcEnd

	// Success
	MOVD ZR, ret+48(FP)
	RET

corrupt:
	MOVD $1, R0
	MOVD R0, ret+48(FP)
	RET
