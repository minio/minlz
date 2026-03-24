//go:build arm64 && !appengine && !noasm && gc && !purego

#include "textflag.h"

// func packBitsNEON(dst []byte, src []byte)
// Packs byte-per-entry src (0x00/0xFF) into bit-packed dst using NEON ADDP.
// len(dst) must equal len(src)/8. len(src) must be a multiple of 128.
// Each input byte must be exactly 0x00 or 0xFF.
TEXT ·packBitsNEON(SB), NOSPLIT, $0-48
	MOVD dst_base+0(FP), R0     // dst pointer
	MOVD src_base+24(FP), R1    // src pointer
	MOVD src_len+32(FP), R2     // src length
	LSR  $7, R2                  // R2 = len(src) / 128

	CBZ  R2, done

	// Load bit-position mask: {1,2,4,8,16,32,64,128} repeated twice.
	MOVD $0x8040201008040201, R3
	VDUP R3, V31.D2

loop:
	// Load 128 bytes (8 × 16).
	VLD1.P 64(R1), [V0.B16, V1.B16, V2.B16, V3.B16]
	VLD1.P 64(R1), [V4.B16, V5.B16, V6.B16, V7.B16]

	// AND each with bit-position mask.
	VAND V31.B16, V0.B16, V0.B16
	VAND V31.B16, V1.B16, V1.B16
	VAND V31.B16, V2.B16, V2.B16
	VAND V31.B16, V3.B16, V3.B16
	VAND V31.B16, V4.B16, V4.B16
	VAND V31.B16, V5.B16, V5.B16
	VAND V31.B16, V6.B16, V6.B16
	VAND V31.B16, V7.B16, V7.B16

	// Round 1: pairwise add adjacent registers.
	// 16 bytes each → 8 pair-sums from first, 8 from second.
	VADDP V1.B16, V0.B16, V0.B16
	VADDP V3.B16, V2.B16, V2.B16
	VADDP V5.B16, V4.B16, V4.B16
	VADDP V7.B16, V6.B16, V6.B16

	// Round 2: pairwise add again.
	VADDP V2.B16, V0.B16, V0.B16
	VADDP V6.B16, V4.B16, V4.B16

	// Round 3: final pairwise add → 16 packed bytes in V0.
	VADDP V4.B16, V0.B16, V0.B16

	// Store 16 output bytes.
	VST1.P [V0.B16], 16(R0)

	SUB  $1, R2
	CBNZ R2, loop

done:
	RET
