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
#include "go_asm.h"

// NEON kernels of TierNEON: contracts in neon_arm64.go, tables in neon_tables_arm64.go.
// The Go assembler has no CMHI or USHL; they are WORDs with fixed registers.

// func gatherNEONAsm(ranks *byte, src *byte, n int, s2r *[256]byte) uint8
// n > 0 is a multiple of 16. Upstream init_neon: s2r lives in 16 registers, one TBL4 maps
// symbols 0..63 and three TBX4 the other quarters.
TEXT ·gatherNEONAsm(SB), NOSPLIT, $0-33
	MOVD   ranks+0(FP), R0
	MOVD   src+8(FP), R1
	MOVD   n+16(FP), R2
	MOVD   s2r+24(FP), R3
	VLD1.P 64(R3), [V16.B16, V17.B16, V18.B16, V19.B16]
	VLD1.P 64(R3), [V20.B16, V21.B16, V22.B16, V23.B16]
	VLD1.P 64(R3), [V24.B16, V25.B16, V26.B16, V27.B16]
	VLD1   (R3), [V28.B16, V29.B16, V30.B16, V31.B16]
	VMOVI  $64, V15.B16
	VEOR   V14.B16, V14.B16, V14.B16

gather_loop:
	VLD1.P 16(R1), [V0.B16]
	VTBL   V0.B16, [V16.B16, V17.B16, V18.B16, V19.B16], V1.B16
	VSUB   V15.B16, V0.B16, V0.B16
	VTBX   V0.B16, [V20.B16, V21.B16, V22.B16, V23.B16], V1.B16
	VSUB   V15.B16, V0.B16, V0.B16
	VTBX   V0.B16, [V24.B16, V25.B16, V26.B16, V27.B16], V1.B16
	VSUB   V15.B16, V0.B16, V0.B16
	VTBX   V0.B16, [V28.B16, V29.B16, V30.B16, V31.B16], V1.B16
	VUMAX  V1.B16, V14.B16, V14.B16
	VST1.P [V1.B16], 16(R0)
	SUBS   $16, R2, R2
	BNE    gather_loop

	VEXT  $8, V14.B16, V14.B16, V0.B16
	VUMAX V0.B16, V14.B16, V14.B16
	VEXT  $4, V14.B16, V14.B16, V0.B16
	VUMAX V0.B16, V14.B16, V14.B16
	VEXT  $2, V14.B16, V14.B16, V0.B16
	VUMAX V0.B16, V14.B16, V14.B16
	VEXT  $1, V14.B16, V14.B16, V0.B16
	VUMAX V0.B16, V14.B16, V14.B16
	VMOV  V14.B[0], R4
	MOVB  R4, ret+32(FP)
	RET

// MASKS64 sets R9 to the partition masks of the 64 ranks in V0-V3 (byte c for ranks
// 8c..8c+7) and leaves them in V4's low bytes: compare with V31, keep the bit weights in V30
// and sum each 8-lane chunk with a pairwise-add tree (upstream masks64v_neon).
#define MASKS64 \
	WORD  $0x6e3f3404 /* cmhi v4.16b, v0.16b, v31.16b */; \
	WORD  $0x6e3f3425 /* cmhi v5.16b, v1.16b, v31.16b */; \
	WORD  $0x6e3f3446 /* cmhi v6.16b, v2.16b, v31.16b */; \
	WORD  $0x6e3f3467 /* cmhi v7.16b, v3.16b, v31.16b */; \
	VAND  V30.B16, V4.B16, V4.B16;                        \
	VAND  V30.B16, V5.B16, V5.B16;                        \
	VAND  V30.B16, V6.B16, V6.B16;                        \
	VAND  V30.B16, V7.B16, V7.B16;                        \
	VADDP V5.B16, V4.B16, V4.B16;                         \
	VADDP V7.B16, V6.B16, V6.B16;                         \
	VADDP V6.B16, V4.B16, V4.B16;                         \
	VADDP V4.B16, V4.B16, V4.B16;                         \
	FMOVD F4, R9

// MASK8 sets R9 and V4 to the partition mask of the 8 ranks in V0's low half, whose high
// half is zero.
#define MASK8 \
	WORD  $0x6e3f3404 /* cmhi v4.16b, v0.16b, v31.16b */; \
	VAND  V30.B16, V4.B16, V4.B16;                        \
	VADDV V4.B8, V4;                                      \
	FMOVS F4, R9

// P16(v, off, c) scatters the 16 ranks of v, at offset off of the step, c being the right
// count of the ranks before them; R9 holds the step's masks and R10 their popcounts. One TBL
// puts the left ranks in order at the front and the right ones reversed at the back, a second
// one reverses (upstream part64_full_neon). The stores are 16 bytes; the junk after each
// side is overwritten by the next group or ends inside the step's 64 ranks.
#define P16(v, off, c) \
	UBFX  $(off), R9, $8, R12;       \
	UBFX  $((off)+8), R9, $8, R13;   \
	UBFX  $(off), R10, $8, R15;      \
	ADD   R12<<4, R7, R12;           \
	ADD   R13<<5, R8, R13;           \
	ADD   R15, R13, R13;             \
	VLD1  (R12), [V8.B16];           \
	VLD1  (R13), [V9.B16];           \
	VORR  V9.B16, V8.B16, V8.B16;    \
	VTBL  V8.B16, [v.B16], V8.B16;   \
	VTBL  V29.B16, [V8.B16], V9.B16; \
	SUB   c, R5, R22;                \
	FMOVQ F8, off(R22);              \
	ADD   c, R6, R23;                \
	VST1  [V9.B16], (R23)

// func partFullNEONAsm(ranks *byte, right *byte, bm *byte, n int, thr uint8) int
// n > 0 is a multiple of 64.
TEXT ·partFullNEONAsm(SB), NOSPLIT, $0-48
	MOVD  ranks+0(FP), R0
	MOVD  right+8(FP), R1
	MOVD  bm+16(FP), R2
	MOVD  n+24(FP), R3
	MOVBU thr+32(FP), R4
	VDUP  R4, V31.B16
	VMOVQ $0x8040201008040201, $0x8040201008040201, V30
	VMOVQ $0x08090a0b0c0d0e0f, $0x0001020304050607, V29
	MOVD  $0x0101010101010101, R14
	MOVD  $·p16A(SB), R7
	MOVD  $·p16B(SB), R8
	MOVD  R0, R5
	MOVD  R1, R6

full_loop:
	// The left stores land in the step's own ranks, so all 64 are loaded first.
	VLD1.P 64(R0), [V0.B16, V1.B16, V2.B16, V3.B16]
	MASKS64
	MOVD.P R9, 8(R2)
	VCNT   V4.B8, V5.B8
	FMOVD  F5, R10
	MUL    R14, R10, R11
	UBFX   $8, R11, $8, R19
	UBFX   $24, R11, $8, R20
	UBFX   $40, R11, $8, R21
	P16(V0, 0, ZR)
	P16(V1, 16, R19)
	P16(V2, 32, R20)
	P16(V3, 48, R21)
	LSR    $56, R11, R11
	ADD    R11, R6, R6
	SUB    R11, R5, R5
	ADD    $64, R5, R5
	SUBS   $64, R3, R3
	BNE    full_loop

	SUB  R1, R6, R6
	MOVD R6, ret+40(FP)
	RET

// C8(v, tab, sh, c) stores the right ranks of 8 lanes of v (mask byte at bit sh of R9, ctab
// half tab) at R6+c; the junk after them is overwritten by the next chunk.
#define C8(v, tab, sh, c) \
	UBFX  $(sh), R9, $8, R12;    \
	FMOVD (tab)(R12<<3), F8;     \
	VTBL  V8.B8, [v.B16], V8.B8; \
	FMOVD F8, (R6)(c)

// func partRightNEONAsm(ranks *byte, right *byte, bm *byte, n int, thr uint8) int
// n > 0 is a multiple of 8. Upstream part_core_neon with EMIT_RIGHT.
TEXT ·partRightNEONAsm(SB), NOSPLIT, $0-48
	MOVD  ranks+0(FP), R0
	MOVD  right+8(FP), R1
	MOVD  bm+16(FP), R2
	MOVD  n+24(FP), R3
	MOVBU thr+32(FP), R4
	VDUP  R4, V31.B16
	VMOVQ $0x8040201008040201, $0x8040201008040201, V30
	MOVD  $0x0101010101010101, R14
	MOVD  $·ctab(SB), R7
	ADD   $2048, R7, R8
	MOVD  R1, R6

right_loop64:
	CMP    $64, R3
	BLT    right_loop8
	VLD1.P 64(R0), [V0.B16, V1.B16, V2.B16, V3.B16]
	MASKS64
	MOVD.P R9, 8(R2)
	VCNT   V4.B8, V5.B8
	FMOVD  F5, R10
	MUL    R14, R10, R11
	UBFX   $0, R11, $8, R19
	UBFX   $8, R11, $8, R20
	UBFX   $16, R11, $8, R21
	UBFX   $24, R11, $8, R22
	UBFX   $32, R11, $8, R23
	UBFX   $40, R11, $8, R24
	UBFX   $48, R11, $8, R25
	C8(V0, R7, 0, ZR)
	C8(V0, R8, 8, R19)
	C8(V1, R7, 16, R20)
	C8(V1, R8, 24, R21)
	C8(V2, R7, 32, R22)
	C8(V2, R8, 40, R23)
	C8(V3, R7, 48, R24)
	C8(V3, R8, 56, R25)
	ADD    R11>>56, R6, R6
	SUB    $64, R3, R3
	B      right_loop64

right_loop8:
	CBZ     R3, right_done
	FMOVD.P 8(R0), F0
	MASK8
	MOVB.P  R9, 1(R2)
	C8(V0, R7, 0, ZR)
	VCNT    V4.B8, V4.B8
	FMOVS   F4, R10
	ADD     R10, R6, R6
	SUB     $8, R3, R3
	B       right_loop8

right_done:
	SUB  R1, R6, R6
	MOVD R6, ret+40(FP)
	RET

// func partNoneNEONAsm(ranks *byte, bm *byte, n int, thr uint8) int
// n > 0 is a multiple of 8. Upstream part_core_neon without EMIT_RIGHT.
TEXT ·partNoneNEONAsm(SB), NOSPLIT, $0-40
	MOVD  ranks+0(FP), R0
	MOVD  bm+8(FP), R2
	MOVD  n+16(FP), R3
	MOVBU thr+24(FP), R4
	VDUP  R4, V31.B16
	VMOVQ $0x8040201008040201, $0x8040201008040201, V30
	MOVD  ZR, R6

none_loop64:
	CMP     $64, R3
	BLT     none_loop8
	VLD1.P  64(R0), [V0.B16, V1.B16, V2.B16, V3.B16]
	MASKS64
	MOVD.P  R9, 8(R2)
	VCNT    V4.B8, V5.B8
	VUADDLV V5.B8, V5
	FMOVS   F5, R10
	ADD     R10, R6, R6
	SUB     $64, R3, R3
	B       none_loop64

none_loop8:
	CBZ     R3, none_done
	FMOVD.P 8(R0), F0
	MASK8
	MOVB.P  R9, 1(R2)
	VCNT    V4.B8, V4.B8
	FMOVS   F4, R10
	ADD     R10, R6, R6
	SUB     $8, R3, R3
	B       none_loop8

none_done:
	MOVD R6, ret+32(FP)
	RET

// The vertical pack walks 16-lane groups: R6 is the group's offset in its block, R1 the
// block's 8 vectors of 16 values (stride R5 = ln) and R0 its d vectors of packed columns.

// VGROUP sets R7 to the group's first value vector and R8 to its first column.
#define VGROUP \
	ADD R6, R1, R7; \
	ADD R6, R0, R8

// VNEXT continues with the next group of the block, or the next block (R9 = d*ln column
// bytes) until R3 values are done.
#define VNEXT(loop) \
	ADD  $16, R6, R6;   \
	CMP  R5, R6;        \
	BLT  loop;          \
	MOVD ZR, R6;        \
	ADD  R9, R0, R0;    \
	ADD  R5<<3, R1, R1; \
	SUBS R5<<3, R3, R3; \
	BNE  loop;          \
	RET

// VDISPATCH branches to the kernel for d in register d.
#define VDISPATCH(d, l2, l3, l4, l5, l6, l7) \
	CMP $2, d; \
	BEQ l2;    \
	CMP $3, d; \
	BEQ l3;    \
	CMP $4, d; \
	BEQ l4;    \
	CMP $5, d; \
	BEQ l5;    \
	CMP $6, d; \
	BEQ l6;    \
	B   l7

// func vertPackNEONAsm(dst *byte, ranks *byte, n int, ln int, d uint8, base uint8)
// n > 0 is a multiple of 8*ln, 2 <= d <= 7 and every rank-base < 1<<d. Upstream
// vert_pack_neon_dN, vert512_pack_neon_dN: each column is built with VSLI in step order,
// which only inserts above the bits already there.
TEXT ·vertPackNEONAsm(SB), NOSPLIT, $0-34
	MOVD  dst+0(FP), R0
	MOVD  ranks+8(FP), R1
	MOVD  n+16(FP), R3
	MOVD  ln+24(FP), R5
	MOVBU d+32(FP), R4
	MOVBU base+33(FP), R10
	VDUP  R10, V31.B16
	MUL   R4, R5, R9
	MOVD  ZR, R6
	VDISPATCH(R4, vpack2, vpack3, vpack4, vpack5, vpack6, vpack7)

	// VP_LOAD loads the group's 8 steps minus the base into V0-V7.
#define VP_LOAD \
	VGROUP;                         \
	VLD1.P (R7)(R5), [V0.B16];      \
	VLD1.P (R7)(R5), [V1.B16];      \
	VLD1.P (R7)(R5), [V2.B16];      \
	VLD1.P (R7)(R5), [V3.B16];      \
	VLD1.P (R7)(R5), [V4.B16];      \
	VLD1.P (R7)(R5), [V5.B16];      \
	VLD1.P (R7)(R5), [V6.B16];      \
	VLD1.P (R7)(R5), [V7.B16];      \
	VSUB   V31.B16, V0.B16, V0.B16; \
	VSUB   V31.B16, V1.B16, V1.B16; \
	VSUB   V31.B16, V2.B16, V2.B16; \
	VSUB   V31.B16, V3.B16, V3.B16; \
	VSUB   V31.B16, V4.B16, V4.B16; \
	VSUB   V31.B16, V5.B16, V5.B16; \
	VSUB   V31.B16, V6.B16, V6.B16; \
	VSUB   V31.B16, V7.B16, V7.B16

#define COL(v) VST1.P [v.B16], (R8)(R5)

vpack2:
	VP_LOAD
	VSLI $2, V1.B16, V0.B16
	VSLI $4, V2.B16, V0.B16
	VSLI $6, V3.B16, V0.B16
	VSLI $2, V5.B16, V4.B16
	VSLI $4, V6.B16, V4.B16
	VSLI $6, V7.B16, V4.B16
	COL(V0)
	COL(V4)
	VNEXT(vpack2)

vpack3:
	VP_LOAD
	VUSHR $2, V2.B16, V8.B16
	VSLI  $1, V3.B16, V8.B16
	VSLI  $4, V4.B16, V8.B16
	VSLI  $7, V5.B16, V8.B16
	VUSHR $1, V5.B16, V9.B16
	VSLI  $2, V6.B16, V9.B16
	VSLI  $5, V7.B16, V9.B16
	VSLI  $3, V1.B16, V0.B16
	VSLI  $6, V2.B16, V0.B16
	COL(V0)
	COL(V8)
	COL(V9)
	VNEXT(vpack3)

vpack4:
	VP_LOAD
	VSLI $4, V1.B16, V0.B16
	VSLI $4, V3.B16, V2.B16
	VSLI $4, V5.B16, V4.B16
	VSLI $4, V7.B16, V6.B16
	COL(V0)
	COL(V2)
	COL(V4)
	COL(V6)
	VNEXT(vpack4)

vpack5:
	VP_LOAD
	VUSHR $3, V1.B16, V8.B16
	VSLI  $2, V2.B16, V8.B16
	VSLI  $7, V3.B16, V8.B16
	VUSHR $1, V3.B16, V9.B16
	VSLI  $4, V4.B16, V9.B16
	VUSHR $4, V4.B16, V10.B16
	VSLI  $1, V5.B16, V10.B16
	VSLI  $6, V6.B16, V10.B16
	VUSHR $2, V6.B16, V11.B16
	VSLI  $3, V7.B16, V11.B16
	VSLI  $5, V1.B16, V0.B16
	COL(V0)
	COL(V8)
	COL(V9)
	COL(V10)
	COL(V11)
	VNEXT(vpack5)

vpack6:
	VP_LOAD
	VUSHR $2, V1.B16, V8.B16
	VSLI  $4, V2.B16, V8.B16
	VUSHR $4, V2.B16, V9.B16
	VSLI  $2, V3.B16, V9.B16
	VUSHR $2, V5.B16, V10.B16
	VSLI  $4, V6.B16, V10.B16
	VUSHR $4, V6.B16, V11.B16
	VSLI  $2, V7.B16, V11.B16
	VSLI  $6, V1.B16, V0.B16
	VSLI  $6, V5.B16, V4.B16
	COL(V0)
	COL(V8)
	COL(V9)
	COL(V4)
	COL(V10)
	COL(V11)
	VNEXT(vpack6)

vpack7:
	VP_LOAD
	VUSHR $1, V1.B16, V8.B16
	VSLI  $6, V2.B16, V8.B16
	VUSHR $2, V2.B16, V9.B16
	VSLI  $5, V3.B16, V9.B16
	VUSHR $3, V3.B16, V10.B16
	VSLI  $4, V4.B16, V10.B16
	VUSHR $4, V4.B16, V11.B16
	VSLI  $3, V5.B16, V11.B16
	VUSHR $5, V5.B16, V12.B16
	VSLI  $2, V6.B16, V12.B16
	VUSHR $6, V6.B16, V13.B16
	VSLI  $1, V7.B16, V13.B16
	VSLI  $7, V1.B16, V0.B16
	COL(V0)
	COL(V8)
	COL(V9)
	COL(V10)
	COL(V11)
	COL(V12)
	COL(V13)
	VNEXT(vpack7)

// func natPackNEONAsm(dst *byte, ranks *byte, n int, dstLen int, d uint8, base uint8) int
// Packs 16-code groups of the natural layout while a 16-byte store fits in dst[:dstLen] and
// returns the number of codes done, a multiple of 16; n >= 16, 2 <= d <= 8 and every
// rank-base < 1<<d. D < 8 is upstream's converging pyramid (pivco_huffman_neon_pack.h) for
// every d: each USHL level shifts the halves of a lane pair towards each other until u64
// lane h holds codes 8h..8h+7, and a TBL compacts their 2d bytes.
TEXT ·natPackNEONAsm(SB), NOSPLIT, $0-48
	MOVD   dst+0(FP), R0
	MOVD   ranks+8(FP), R1
	MOVD   n+16(FP), R3
	MOVD   dstLen+24(FP), R4
	MOVBU  d+32(FP), R5
	MOVBU  base+33(FP), R6
	VDUP   R6, V31.B16
	MOVD   R3, R10
	CMP    $8, R5
	BEQ    pack8_loop64
	MOVD   $·natPackTab(SB), R9
	ADD    R5<<6, R9, R9
	ADD    R5<<4, R9, R9
	VLD1.P 64(R9), [V26.B16, V27.B16, V28.B16, V29.B16]
	VLD1   (R9), [V30.B16]
	LSL    $1, R5, R6

pack_loop:
	CMP    $16, R3
	BLT    pack_done
	CMP    $16, R4
	BLT    pack_done
	VLD1.P 16(R1), [V0.B16]
	VSUB   V31.B16, V0.B16, V0.B16
	WORD   $0x6e3b4400               // ushl v0.16b, v0.16b, v27.16b
	WORD   $0x6e7c4400               // ushl v0.8h, v0.8h, v28.8h
	WORD   $0x6ebd4400               // ushl v0.4s, v0.4s, v29.4s
	WORD   $0x6efe4400               // ushl v0.2d, v0.2d, v30.2d
	VTBL   V26.B16, [V0.B16], V0.B16
	VST1   [V0.B16], (R0)
	ADD    R6, R0, R0
	SUB    R6, R4, R4
	SUB    $16, R3, R3
	B      pack_loop

pack8_loop64:
	CMP    $64, R3
	BLT    pack8_loop16
	VLD1.P 64(R1), [V0.B16, V1.B16, V2.B16, V3.B16]
	VSUB   V31.B16, V0.B16, V0.B16
	VSUB   V31.B16, V1.B16, V1.B16
	VSUB   V31.B16, V2.B16, V2.B16
	VSUB   V31.B16, V3.B16, V3.B16
	VST1.P [V0.B16, V1.B16, V2.B16, V3.B16], 64(R0)
	SUB    $64, R3, R3
	B      pack8_loop64

pack8_loop16:
	CMP    $16, R3
	BLT    pack_done
	VLD1.P 16(R1), [V0.B16]
	VSUB   V31.B16, V0.B16, V0.B16
	VST1.P [V0.B16], 16(R0)
	SUB    $16, R3, R3
	B      pack8_loop16

pack_done:
	SUB  R3, R10, R10
	MOVD R10, ret+40(FP)
	RET

// Decoder: execNEONAsm runs a whole Program with the op checks of execGeneric and of the
// amd64 executors (_generate/pivco dec.go), so the per-op cost is a dispatch, not a call.
// Like theirs, its writes are exact and its reads of In stay inside In; only the natural
// unpack reads past an op's region. Register map:
//
//	R22 *Program   R23 current op   R24 end of the ops   R25 &Arena[0]
//	R15 max(len(Arena)-ArenaSlack, 0)
//	R0  output pointer   R1 In, then the op's bitmap or region   R4 K, then the count left
//	R10 len(In) and R11 Bm in the op prologue; the kernels use R2-R14, R19-R21 and V0-V31.

#define IN_LEN (Program_In+8)
#define ARENA_LEN (Program_Arena+8)
#define DST_LEN (Program_Dst+8)

// BITMAP_CHECK fails unless the ceil(K/8) bitmap bytes at Bm lie in In and points R1 at them.
#define BITMAP_CHECK \
	ADD $7, R4, R7;     \
	ADD R7>>3, R11, R7; \
	CMP R10, R7;        \
	BHI fail;           \
	ADD R11, R1, R1

// ARENA_CHECK(r) fails unless the K bytes at arena offset r lie below the slack and makes r
// a pointer.
#define ARENA_CHECK(r) \
	ADD R4, r, R7; \
	CMP R15, R7;   \
	BHI fail;      \
	ADD R25, r, r

// FLAT_CHECK fails unless the R7-byte region at Bm lies in In, R2S is set and R2S[P:P+2^D]
// (D in R12) inside it; it points R1 at the region and R2 at R2S[P]. LSL takes D mod 64, so
// the R2S bound holds only for the D range the caller checked first.
#define FLAT_CHECK \
	ADD   R11, R7, R7;          \
	CMP   R10, R7;              \
	BHI   fail;                 \
	ADD   R11, R1, R1;          \
	MOVD  Program_R2S(R22), R2; \
	CBZ   R2, fail;             \
	MOVBU Op_P(R23), R7;        \
	ADD   R7, R2, R2;           \
	MOVD  $1, R8;               \
	LSL   R12, R8, R8;          \
	ADD   R7, R8, R8;           \
	CMP   $const_R2SLen, R8;    \
	BHI   fail

// The merges are merge_vec_vec_neon's two-table merge with the SABD+TBL2 index replaced by
// a subtraction of two mergeTab rows (R6, R7): right lanes index the right child with a TBX,
// left lanes the left one with a TBL of 240 minus the index. Each 16 outputs are loaded
// before they are stored, which keeps the in-place layout safe (see mergeVV). In a 64-output
// step, chunk g starts 16g-c left and c right values in, c being the pfx byte of the step's
// bitmap word before it. The last K&15 outputs are one 16-output step on the bitmap bits
// below K, stored exactly; its 16-byte loads at the cursors reach 15 bytes past the K bytes
// of the child.

#define MERGE_SETUP \
	MOVD $·mergeTab(SB), R6;     \
	ADD  $4096, R6, R7;          \
	MOVD $0x0101010101010101, R8

// MERGE_IDX(sh) sets V1 to the merge index of the 16 outputs whose bitmap is at bit sh of R9.
#define MERGE_IDX(sh) \
	UBFX $(sh), R9, $8, R11;     \
	UBFX $((sh)+8), R9, $8, R12; \
	ADD  R11<<4, R6, R11;        \
	ADD  R12<<4, R7, R12;        \
	VLD1 (R11), [V1.B16];        \
	VLD1 (R12), [V2.B16];        \
	VSUB V2.B16, V1.B16, V1.B16

// VV16(off, c) merges 16 outputs to R0: lefts from R2+off-c, rights from R3+c.
#define VV16(off, c) \
	MERGE_IDX(off);                  \
	VSUB   V1.B16, V31.B16, V2.B16;  \
	SUB    c, R2, R13;               \
	ADD    c, R3, R14;               \
	FMOVQ  off(R13), F3;             \
	VLD1   (R14), [V4.B16];          \
	VTBL   V2.B16, [V3.B16], V5.B16; \
	VTBX   V1.B16, [V4.B16], V5.B16; \
	VST1.P [V5.B16], 16(R0)

// CV16(off, c) merges 16 outputs to R0: the constant V30 and rights from R3+c.
#define CV16(off, c) \
	MERGE_IDX(off);                  \
	ADD    c, R3, R14;               \
	VLD1   (R14), [V4.B16];          \
	VMOV   V30.B16, V5.B16;          \
	VTBX   V1.B16, [V4.B16], V5.B16; \
	VST1.P [V5.B16], 16(R0)

// STEP64 loads the next 64 bitmap bits to R9, the running popcounts of their bytes to R10
// and the right counts before chunks 1-3 to R19-R21.
#define STEP64 \
	MOVD    (R1), R9;          \
	FMOVD.P 8(R1), F0;         \
	VCNT    V0.B8, V0.B8;      \
	FMOVD   F0, R10;           \
	MUL     R8, R10, R10;      \
	UBFX    $8, R10, $8, R19;  \
	UBFX    $24, R10, $8, R20; \
	UBFX    $40, R10, $8, R21

// POP(reg) sets R10 to the popcount of the low 16 bits of reg.
#define POP(reg) \
	FMOVS   reg, F0;      \
	VCNT    V0.B8, V0.B8; \
	VUADDLV V0.B8, V0;    \
	FMOVS   F0, R10

// TAIL_BITS sets R9 to the bitmap bits at R1 below R4 = 1..15, reading exactly
// ceil(R4/8) bytes: the second byte is the first one again unless R4 > 8.
#define TAIL_BITS \
	CMP   $8, R4;          \
	CSINC LE, R1, R1, R11; \
	MOVBU (R1), R9;        \
	MOVBU (R11), R11;      \
	ORR   R11<<8, R9, R9;  \
	MOVD  $1, R11;         \
	LSL   R4, R11, R11;    \
	SUB   $1, R11, R11;    \
	AND   R11, R9, R9

// Field extraction for the vertical unpack: x = the d-bit field at bit r of column a,
// continued in column b when it crosses into it; V30 is the code mask.
#define LOW(a, x) VAND V30.B16, a.B16, x.B16
#define MID(r, a, x) VUSHR $(r), a.B16, x.B16; VAND V30.B16, x.B16, x.B16
#define TOP(r, a, x) VUSHR $(r), a.B16, x.B16
#define CROSS(r, a, b, x) VUSHR $(r), a.B16, x.B16; VSLI $(8-(r)), b.B16, x.B16; VAND V30.B16, x.B16, x.B16

// Code to symbol lookups through lut in V16-V23: TBL1 for d <= 4, TBL2, TBL4, and for d=7
// the OR of a TBL4 of the codes and one of the codes minus 64 (V29 = 64). Upstream prefers
// these independent lookups to a TBX4, which waits for the first TBL4.
#define LUT1(x) VTBL x.B16, [V16.B16], x.B16
#define LUT2(x) VTBL x.B16, [V16.B16, V17.B16], x.B16
#define LUT4(x) VTBL x.B16, [V16.B16, V17.B16, V18.B16, V19.B16], x.B16
#define LUT7(x) \
	VSUB V29.B16, x.B16, V28.B16;                                \
	VTBL V28.B16, [V20.B16, V21.B16, V22.B16, V23.B16], V28.B16; \
	LUT4(x);                                                     \
	VORR V28.B16, x.B16, x.B16

// The vertical unpack walks 16-lane groups: R6 is the group's offset in its block, R0 the
// block's 8 vectors of ln values (stride R5 = ln) and R1 its d vectors of packed columns.

// VU_GROUP sets R7 to the group's first output vector and R8 to its first column.
#define VU_GROUP \
	ADD R6, R0, R7; \
	ADD R6, R1, R8

// VU_NEXT continues with the next group of the block, or the next block (R9 = d*ln column
// bytes) until the K values are done.
#define VU_NEXT(loop) \
	ADD  $16, R6, R6;   \
	CMP  R5, R6;        \
	BLT  loop;          \
	MOVD ZR, R6;        \
	ADD  R9, R1, R1;    \
	ADD  R5<<3, R0, R0; \
	SUBS R5<<3, R4, R4; \
	BNE  loop;          \
	B    advance

// VU_OUT maps the 8 steps in V8-V15 and stores them.
#define VU_OUT(lut) \
	lut(V8);                    \
	lut(V9);                    \
	lut(V10);                   \
	lut(V11);                   \
	lut(V12);                   \
	lut(V13);                   \
	lut(V14);                   \
	lut(V15);                   \
	VST1.P [V8.B16], (R7)(R5);  \
	VST1.P [V9.B16], (R7)(R5);  \
	VST1.P [V10.B16], (R7)(R5); \
	VST1.P [V11.B16], (R7)(R5); \
	VST1.P [V12.B16], (R7)(R5); \
	VST1.P [V13.B16], (R7)(R5); \
	VST1.P [V14.B16], (R7)(R5); \
	VST1.P [V15.B16], (R7)(R5)

#define VU_COL(v) VLD1.P (R8)(R5), [v.B16]

// NAT_CODES(lut) maps the 16 natural codes in V0's low 2d bytes to V3: a TBL gathers the
// byte pair of each code into a u16 lane, USHL shifts the code down, UZP1 keeps the low
// bytes and lut maps them (upstream merge_flat_d7_neon, for every d).
#define NAT_CODES(lut) \
	VTBL  V24.B16, [V0.B16], V1.B16;                   \
	VTBL  V25.B16, [V0.B16], V2.B16;                   \
	WORD  $0x6e7a4421 /* ushl v1.8h, v1.8h, v26.8h */; \
	WORD  $0x6e7a4442 /* ushl v2.8h, v2.8h, v26.8h */; \
	VUZP1 V2.B16, V1.B16, V3.B16;                      \
	VAND  V27.B16, V3.B16, V3.B16;                     \
	lut(V3)

// NAT_NEXT moves on by 16 codes; R6 = 2d and R13 is the number of In bytes from R1 on.
#define NAT_NEXT \
	ADD R6, R1, R1;   \
	SUB R6, R13, R13; \
	SUB $16, R4, R4

// NAT_FITS(w) leaves for the tail unless 16 codes are left and a w-byte load fits in In.
#define NAT_FITS(w) \
	CMP $16, R4;   \
	BLT nat_tail;  \
	CMP $(w), R13; \
	BLT nat_tail

// func execNEONAsm(p *Program) int
TEXT ·execNEONAsm(SB), NOSPLIT, $0-16
	MOVD p+0(FP), R22
	MOVD Program_Ops(R22), R23
	MOVD Program_Ops+8(R22), R7
	MOVD $Op__size, R8
	MADD R8, R23, R7, R24
	MOVD Program_Arena(R22), R25
	MOVD ARENA_LEN(R22), R7
	SUBS $const_ArenaSlack, R7, R15
	CSEL LT, ZR, R15, R15
	B    next

advance:
	ADD $Op__size, R23, R23

next:
	CMP R24, R23
	BHS ok

	// Output: Out+K must fit Dst, or the arena less its slack.
	MOVBU Op_Flags(R23), R7
	MOVWU Op_K(R23), R4
	MOVWU Op_Out(R23), R0
	MOVD  R25, R8
	MOVD  R15, R9
	TST   $const_FlagDst, R7
	BEQ   out_arena
	MOVD  Program_Dst(R22), R8
	MOVD  DST_LEN(R22), R9

out_arena:
	ADD R4, R0, R7
	CMP R9, R7
	BHI fail
	ADD R8, R0, R0

	// Source: In; R10 = its length, R11 = Bm.
	MOVD  Program_In(R22), R1
	MOVD  IN_LEN(R22), R10
	MOVWU Op_Bm(R23), R11
	MOVBU Op_Kind(R23), R7
	CMP   $const_OpVV, R7
	BEQ   vv
	CMP   $const_OpCV, R7
	BEQ   cv
	CMP   $const_OpCC, R7
	BEQ   cc
	CMP   $const_OpV512, R7
	BEQ   v512
	CMP   $const_OpV128, R7
	BEQ   v128
	CMP   $const_OpNat, R7
	BEQ   nat
	CMP   $const_OpCopy, R7
	BEQ   copy
	B     fail

vv:
	// out[j] = bit j ? R[r++] : L[l++] (upstream merge_vec_vec_neon)
	BITMAP_CHECK
	MOVWU Op_L(R23), R2
	MOVWU Op_R(R23), R3
	ARENA_CHECK(R2)
	ARENA_CHECK(R3)
	MOVD  R3, R5
	MERGE_SETUP
	VMOVI $240, V31.B16

vv_loop64:
	CMP $64, R4
	BLT vv_loop16
	STEP64
	VV16(0, ZR)
	VV16(16, R19)
	VV16(32, R20)
	VV16(48, R21)
	LSR $56, R10, R10
	ADD R10, R3, R3
	SUB R10, R2, R2
	ADD $64, R2, R2
	SUB $64, R4, R4
	B   vv_loop64

vv_loop16:
	CMP     $16, R4
	BLT     vv_tail
	MOVHU.P 2(R1), R9
	VV16(0, ZR)
	POP(R9)
	ADD     R10, R3, R3
	SUB     R10, R2, R2
	ADD     $16, R2, R2
	SUB     $16, R4, R4
	B       vv_loop16

vv_tail:
	CBZ  R4, merge_check
	TAIL_BITS
	MERGE_IDX(0)
	VSUB V1.B16, V31.B16, V2.B16
	VLD1 (R2), [V5.B16]
	VLD1 (R3), [V4.B16]
	VTBL V2.B16, [V5.B16], V3.B16
	VTBX V1.B16, [V4.B16], V3.B16
	B    merge_pop

cv:
	// out[j] = bit j ? R[r++] : P (upstream merge_cst_vec_neon)
	BITMAP_CHECK
	MOVWU Op_R(R23), R3
	ARENA_CHECK(R3)
	MOVD  R3, R5
	MERGE_SETUP
	MOVBU Op_P(R23), R9
	VDUP  R9, V30.B16

cv_loop64:
	CMP $64, R4
	BLT cv_loop16
	STEP64
	CV16(0, ZR)
	CV16(16, R19)
	CV16(32, R20)
	CV16(48, R21)
	ADD R10>>56, R3, R3
	SUB $64, R4, R4
	B   cv_loop64

cv_loop16:
	CMP     $16, R4
	BLT     cv_tail
	MOVHU.P 2(R1), R9
	CV16(0, ZR)
	POP(R9)
	ADD     R10, R3, R3
	SUB     $16, R4, R4
	B       cv_loop16

cv_tail:
	CBZ  R4, merge_check
	TAIL_BITS
	MERGE_IDX(0)
	VLD1 (R3), [V4.B16]
	VMOV V30.B16, V3.B16
	VTBX V1.B16, [V4.B16], V3.B16

merge_pop:
	POP(R9)
	ADD R10, R3, R3

merge_check:
	// The right cursor must have advanced by exactly KR. The tail in V3 is stored after the
	// check, so a failing op leaves its last K&15 outputs unwritten.
	SUB   R5, R3, R3
	MOVWU Op_KR(R23), R7
	CMP   R7, R3
	BNE   fail
	B     tail_store

cc:
	// out[j] = bit j ? Q : P (upstream merge_cst_cst_neon): each bitmap bit is spread to its
	// output byte and selects Q over P.
	BITMAP_CHECK
	MOVBU Op_P(R23), R5
	MOVBU Op_Q(R23), R6
	VDUP  R5, V28.B16
	VDUP  R6, V29.B16
	VMOVQ $0x8040201008040201, $0x8040201008040201, V30
	VMOVQ $0, $0x0101010101010101, V27

cc_loop16:
	CMP     $16, R4
	BLT     cc_tail
	VLD1R.P 2(R1), [V0.H8]
	VTBL    V27.B16, [V0.B16], V1.B16
	VCMTST  V30.B16, V1.B16, V1.B16
	VBSL    V28.B16, V29.B16, V1.B16
	VST1.P  [V1.B16], 16(R0)
	SUB     $16, R4, R4
	B       cc_loop16

cc_tail:
	CBZ    R4, advance
	TAIL_BITS
	VDUP   R9, V0.H8
	VTBL   V27.B16, [V0.B16], V3.B16
	VCMTST V30.B16, V3.B16, V3.B16
	VBSL   V28.B16, V29.B16, V3.B16
	B      tail_store

v512:
	MOVD $64, R5
	TST  $511, R4
	BNE  fail
	B    vert

v128:
	MOVD $16, R5
	TST  $127, R4
	BNE  fail

vert:
	// Flat, vertical layout of ln = R5 lanes (upstream vert512_merge_neon, vert_merge_neon):
	// D in 2..7, region K*D/8 bytes. Step s of a group is cut from its columns with uniform
	// shifts and mapped through R2S[P:].
	MOVBU Op_D(R23), R12
	SUB   $2, R12, R7
	CMP   $5, R7
	BHI   fail
	MUL   R4, R12, R7
	LSR   $3, R7, R7
	FLAT_CHECK
	CBZ   R4, advance
	MUL   R12, R5, R9
	MOVD  ZR, R6
	MOVD  $1, R10
	LSL   R12, R10, R10
	SUB   $1, R10, R10
	VDUP  R10, V30.B16
	VLD1  (R2), [V16.B16, V17.B16, V18.B16, V19.B16]
	VDISPATCH(R12, vunpack2, vunpack3, vunpack4, vunpack5, vunpack6, vunpack7setup)

vunpack2:
	VU_GROUP
	VU_COL(V0)
	VU_COL(V1)
	LOW(V0, V8)
	MID(2, V0, V9)
	MID(4, V0, V10)
	TOP(6, V0, V11)
	LOW(V1, V12)
	MID(2, V1, V13)
	MID(4, V1, V14)
	TOP(6, V1, V15)
	VU_OUT(LUT1)
	VU_NEXT(vunpack2)

vunpack3:
	VU_GROUP
	VU_COL(V0)
	VU_COL(V1)
	VU_COL(V2)
	LOW(V0, V8)
	MID(3, V0, V9)
	CROSS(6, V0, V1, V10)
	MID(1, V1, V11)
	MID(4, V1, V12)
	CROSS(7, V1, V2, V13)
	MID(2, V2, V14)
	TOP(5, V2, V15)
	VU_OUT(LUT1)
	VU_NEXT(vunpack3)

vunpack4:
	VU_GROUP
	VU_COL(V0)
	VU_COL(V1)
	VU_COL(V2)
	VU_COL(V3)
	LOW(V0, V8)
	TOP(4, V0, V9)
	LOW(V1, V10)
	TOP(4, V1, V11)
	LOW(V2, V12)
	TOP(4, V2, V13)
	LOW(V3, V14)
	TOP(4, V3, V15)
	VU_OUT(LUT1)
	VU_NEXT(vunpack4)

vunpack5:
	VU_GROUP
	VU_COL(V0)
	VU_COL(V1)
	VU_COL(V2)
	VU_COL(V3)
	VU_COL(V4)
	LOW(V0, V8)
	CROSS(5, V0, V1, V9)
	MID(2, V1, V10)
	CROSS(7, V1, V2, V11)
	CROSS(4, V2, V3, V12)
	MID(1, V3, V13)
	CROSS(6, V3, V4, V14)
	TOP(3, V4, V15)
	VU_OUT(LUT2)
	VU_NEXT(vunpack5)

vunpack6:
	VU_GROUP
	VU_COL(V0)
	VU_COL(V1)
	VU_COL(V2)
	VU_COL(V3)
	VU_COL(V4)
	VU_COL(V5)
	LOW(V0, V8)
	CROSS(6, V0, V1, V9)
	CROSS(4, V1, V2, V10)
	TOP(2, V2, V11)
	LOW(V3, V12)
	CROSS(6, V3, V4, V13)
	CROSS(4, V4, V5, V14)
	TOP(2, V5, V15)
	VU_OUT(LUT4)
	VU_NEXT(vunpack6)

vunpack7setup:
	ADD   $64, R2, R2
	VLD1  (R2), [V20.B16, V21.B16, V22.B16, V23.B16]
	VMOVI $64, V29.B16

vunpack7:
	VU_GROUP
	VU_COL(V0)
	VU_COL(V1)
	VU_COL(V2)
	VU_COL(V3)
	VU_COL(V4)
	VU_COL(V5)
	VU_COL(V6)
	LOW(V0, V8)
	CROSS(7, V0, V1, V9)
	CROSS(6, V1, V2, V10)
	CROSS(5, V2, V3, V11)
	CROSS(4, V3, V4, V12)
	CROSS(3, V4, V5, V13)
	CROSS(2, V5, V6, V14)
	TOP(1, V6, V15)
	VU_OUT(LUT7)
	VU_NEXT(vunpack7)

nat:
	// Flat, natural layout: D in 2..8, region ceil(K*D/8) bytes. 16-code groups load a
	// window of 16 bytes (8 for d <= 4, whose groups span at most 8) while it fits in In.
	MOVBU  Op_D(R23), R12
	SUB    $2, R12, R7
	CMP    $6, R7
	BHI    fail
	MUL    R4, R12, R7
	ADD    $7, R7, R7
	LSR    $3, R7, R7
	FLAT_CHECK
	SUB    R11, R10, R13
	CMP    $8, R12
	BEQ    nat_d8
	MOVD   $·natUnpackTab(SB), R9
	ADD    R12<<6, R9, R9
	VLD1   (R9), [V24.B16, V25.B16, V26.B16, V27.B16]
	VLD1.P 64(R2), [V16.B16, V17.B16, V18.B16, V19.B16]
	LSL    $1, R12, R6
	CMP    $4, R12
	BLE    nat_loop4
	CMP    $5, R12
	BEQ    nat_loop5
	CMP    $6, R12
	BEQ    nat_loop6
	VLD1   (R2), [V20.B16, V21.B16, V22.B16, V23.B16]
	VMOVI  $64, V29.B16

nat_loop7:
	NAT_FITS(16)
	VLD1   (R1), [V0.B16]
	NAT_CODES(LUT7)
	VST1.P [V3.B16], 16(R0)
	NAT_NEXT
	B      nat_loop7

nat_loop6:
	NAT_FITS(16)
	VLD1   (R1), [V0.B16]
	NAT_CODES(LUT4)
	VST1.P [V3.B16], 16(R0)
	NAT_NEXT
	B      nat_loop6

nat_loop5:
	NAT_FITS(16)
	VLD1   (R1), [V0.B16]
	NAT_CODES(LUT2)
	VST1.P [V3.B16], 16(R0)
	NAT_NEXT
	B      nat_loop5

nat_loop4:
	NAT_FITS(8)
	FMOVD  (R1), F0
	NAT_CODES(LUT1)
	VST1.P [V3.B16], 16(R0)
	NAT_NEXT
	B      nat_loop4

nat_tail:
	// The last group, and those whose window would pass the end of In: a full window while
	// 16 bytes of In are left, else exactly the ceil(min(16, R4)*D/8) bytes the group uses.
	CBZ  R4, advance
	CMP  $16, R13
	BLT  nat_exact
	VLD1 (R1), [V0.B16]
	B    nat_codes

nat_exact:
	MOVD   $16, R7
	CMP    R7, R4
	CSEL   LT, R4, R7, R7
	MUL    R12, R7, R7
	ADD    $7, R7, R7
	LSR    $3, R7, R7
	MOVD   R1, R8
	MOVD   ZR, R9
	TBZ    $3, R7, nat_ld4
	MOVD.P 8(R8), R9

nat_ld4:
	// The 0..7 bytes after the first 8 gather in R10, R11 being their bit count.
	MOVD    ZR, R10
	MOVD    ZR, R11
	TBZ     $2, R7, nat_ld2
	MOVWU.P 4(R8), R10
	MOVD    $32, R11

nat_ld2:
	TBZ     $1, R7, nat_ld1
	MOVHU.P 2(R8), R14
	LSL     R11, R14, R14
	ORR     R14, R10, R10
	ADD     $16, R11, R11

nat_ld1:
	TBZ   $0, R7, nat_ld0
	MOVBU (R8), R14
	LSL   R11, R14, R14
	ORR   R14, R10, R10

nat_ld0:
	TST  $8, R7
	CSEL EQ, R10, R9, R9
	CSEL EQ, ZR, R10, R10
	VMOV R9, V0.D[0]
	VMOV R10, V0.D[1]

nat_codes:
	CMP $4, R12
	BLE nat_codes4
	CMP $5, R12
	BEQ nat_codes5
	CMP $6, R12
	BEQ nat_codes6
	NAT_CODES(LUT7)
	B   nat_store

nat_codes6:
	NAT_CODES(LUT4)
	B nat_store

nat_codes5:
	NAT_CODES(LUT2)
	B nat_store

nat_codes4:
	NAT_CODES(LUT1)

nat_store:
	CMP    $16, R4
	BLT    tail_store
	VST1.P [V3.B16], 16(R0)
	NAT_NEXT
	B      nat_tail

nat_d8:
	// 256-entry table: one lookup per code.
	CBZ     R4, advance
	MOVBU.P 1(R1), R7
	MOVBU   (R2)(R7), R7
	MOVB.P  R7, 1(R0)
	SUB     $1, R4, R4
	B       nat_d8

copy:
	// D = 8 flat: the codes are the symbols (upstream merge_flat_d8_neon).
	MOVBU Op_D(R23), R7
	CMP   $8, R7
	BNE   fail
	ADD   R4, R11, R7
	CMP   R10, R7
	BHI   fail
	ADD   R11, R1, R1

copy_64:
	CMP    $64, R4
	BLT    copy_16
	VLD1.P 64(R1), [V0.B16, V1.B16, V2.B16, V3.B16]
	VST1.P [V0.B16, V1.B16, V2.B16, V3.B16], 64(R0)
	SUB    $64, R4, R4
	B      copy_64

copy_16:
	CMP    $16, R4
	BLT    copy_8
	VLD1.P 16(R1), [V0.B16]
	VST1.P [V0.B16], 16(R0)
	SUB    $16, R4, R4
	B      copy_16

copy_8:
	TBZ    $3, R4, copy_4
	MOVD.P 8(R1), R7
	MOVD.P R7, 8(R0)

copy_4:
	TBZ     $2, R4, copy_2
	MOVWU.P 4(R1), R7
	MOVW.P  R7, 4(R0)

copy_2:
	TBZ     $1, R4, copy_1
	MOVHU.P 2(R1), R7
	MOVH.P  R7, 2(R0)

copy_1:
	TBZ   $0, R4, advance
	MOVBU (R1), R7
	MOVB  R7, (R0)
	B     advance

tail_store:
	// The low R4 = 0..15 bytes of V3.
	VMOV   V3.D[0], R7
	TBZ    $3, R4, tail_st4
	MOVD.P R7, 8(R0)
	VMOV   V3.D[1], R7

tail_st4:
	TBZ    $2, R4, tail_st2
	MOVW.P R7, 4(R0)
	LSR    $32, R7, R7

tail_st2:
	TBZ    $1, R4, tail_st1
	MOVH.P R7, 2(R0)
	LSR    $16, R7, R7

tail_st1:
	TBZ  $0, R4, advance
	MOVB R7, (R0)
	B    advance

fail:
	MOVD Program_Ops(R22), R7
	SUB  R7, R23, R7
	MOVD $Op__size, R8
	UDIV R8, R7, R7
	MOVD R7, ret+8(FP)
	RET

ok:
	MOVD $-1, R7
	MOVD R7, ret+8(FP)
	RET
