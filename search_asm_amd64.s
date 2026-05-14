//go:build !appengine && !noasm && gc && !purego

#include "textflag.h"

// func cpuidAVX2() bool
// Returns true if AVX2 is supported.
TEXT ·cpuidAVX2(SB), NOSPLIT, $0-1
	// Check OSXSAVE (CPUID.1:ECX bit 27) first.
	MOVL $1, AX
	XORL CX, CX
	CPUID
	BTL  $27, CX       // OSXSAVE?
	JCC  noavx2

	// Check OS has enabled AVX state (XGETBV XCR0 bits 1+2).
	XORL CX, CX
	XGETBV
	ANDL $0x06, AX
	CMPL AX, $0x06
	JNE  noavx2

	// Check AVX2 (CPUID.7:EBX bit 5).
	MOVL $7, AX
	XORL CX, CX
	CPUID
	BTL  $5, BX         // AVX2?
	JCC  noavx2

	MOVB $1, ret+0(FP)
	RET

noavx2:
	MOVB $0, ret+0(FP)
	RET

// func packBitsSSE2(dst []byte, src []byte)
// Packs byte-per-entry src (0x00/0xFF) into bit-packed dst using PMOVMSKB.
// len(dst) must equal len(src)/8. len(src) must be a multiple of 256.
// Requires: SSE2
TEXT ·packBitsSSE2(SB), NOSPLIT, $0-48
	MOVQ dst_base+0(FP), DI
	MOVQ src_base+24(FP), SI
	MOVQ src_len+32(FP), CX
	SHRQ $8, CX         // CX = len(src) / 256
	TESTQ CX, CX
	JZ   sse2done

sse2loop:
	MOVOU 0*16(SI), X0
	MOVOU 1*16(SI), X1
	MOVOU 2*16(SI), X2
	MOVOU 3*16(SI), X3
	MOVOU 4*16(SI), X4
	MOVOU 5*16(SI), X5
	MOVOU 6*16(SI), X6
	MOVOU 7*16(SI), X7

	PMOVMSKB X0, AX
	PMOVMSKB X1, BX
	PMOVMSKB X2, DX
	PMOVMSKB X3, R8
	PMOVMSKB X4, R9
	PMOVMSKB X5, R10
	PMOVMSKB X6, R11
	PMOVMSKB X7, R12

	MOVW AX, 0*2(DI)
	MOVW BX, 1*2(DI)
	MOVW DX, 2*2(DI)
	MOVW R8, 3*2(DI)
	MOVW R9, 4*2(DI)
	MOVW R10, 5*2(DI)
	MOVW R11, 6*2(DI)
	MOVW R12, 7*2(DI)

	MOVOU 8*16(SI), X0
	MOVOU 9*16(SI), X1
	MOVOU 10*16(SI), X2
	MOVOU 11*16(SI), X3
	MOVOU 12*16(SI), X4
	MOVOU 13*16(SI), X5
	MOVOU 14*16(SI), X6
	MOVOU 15*16(SI), X7

	PMOVMSKB X0, AX
	PMOVMSKB X1, BX
	PMOVMSKB X2, DX
	PMOVMSKB X3, R8
	PMOVMSKB X4, R9
	PMOVMSKB X5, R10
	PMOVMSKB X6, R11
	PMOVMSKB X7, R12

	MOVW AX, 8*2(DI)
	MOVW BX, 9*2(DI)
	MOVW DX, 10*2(DI)
	MOVW R8, 11*2(DI)
	MOVW R9, 12*2(DI)
	MOVW R10, 13*2(DI)
	MOVW R11, 14*2(DI)
	MOVW R12, 15*2(DI)

	ADDQ $256, SI
	ADDQ $32, DI
	DECQ CX
	JNZ  sse2loop

sse2done:
	RET

// func packBitsAVX2(dst []byte, src []byte)
// Packs byte-per-entry src (0x00/0xFF) into bit-packed dst using VPMOVMSKB.
// len(dst) must equal len(src)/8. len(src) must be a multiple of 256.
// Requires: AVX2
TEXT ·packBitsAVX2(SB), NOSPLIT, $0-48
	MOVQ dst_base+0(FP), DI
	MOVQ src_base+24(FP), SI
	MOVQ src_len+32(FP), CX
	SHRQ $8, CX         // CX = len(src) / 256
	TESTQ CX, CX
	JZ   done

loop:
	VMOVDQU 0*32(SI), Y0
	VMOVDQU 1*32(SI), Y1
	VMOVDQU 2*32(SI), Y2
	VMOVDQU 3*32(SI), Y3
	VMOVDQU 4*32(SI), Y4
	VMOVDQU 5*32(SI), Y5
	VMOVDQU 6*32(SI), Y6
	VMOVDQU 7*32(SI), Y7

	VPMOVMSKB Y0, AX
	VPMOVMSKB Y1, BX
	VPMOVMSKB Y2, DX
	VPMOVMSKB Y3, R8
	VPMOVMSKB Y4, R9
	VPMOVMSKB Y5, R10
	VPMOVMSKB Y6, R11
	VPMOVMSKB Y7, R12

	MOVL AX, 0*4(DI)
	MOVL BX, 1*4(DI)
	MOVL DX, 2*4(DI)
	MOVL R8, 3*4(DI)
	MOVL R9, 4*4(DI)
	MOVL R10, 5*4(DI)
	MOVL R11, 6*4(DI)
	MOVL R12, 7*4(DI)

	ADDQ $256, SI
	ADDQ $32, DI
	DECQ CX
	JNZ  loop

done:
	VZEROUPPER
	RET
