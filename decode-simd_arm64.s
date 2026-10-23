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

// The operation loops of SIMD blocks follow simdExecAVX2, see _generate/gen-simd.go.
// Literals and matches are copied 32 bytes at a time, as a pair of Q registers, so an operation
// writes up to simdSlack bytes past its end, and streams are read past their values (simdPad).

#define DST R0
#define LITP R1
#define TOKP R2
#define LLP R3
#define OFFP R4
#define RAW R5
#define ESCP R6
#define TOKEND R7
#define ST R8
#define DP R9
#define LAST R10
#define POS R11
#define TOK R12
#define LEN R13
#define OFFTAB R19
#define PERMTAB R20
#define C32 R21
#define LITS R22

// simdPermTabNEON holds 33 rows of 32 TBL indexes, indexed by min(offset, 32).
// Byte i of row r is i % r, and i in row 32. Row 0 is row 1: a delta reference at position 0 repeats dst[0], which is 0.
// It is not a Go variable, as Go init order cannot see that the assembly reads it.
#define PERM(r, a, b, c, d) \
DATA ·simdPermTabNEON+(r*32)(SB)/8, a \
DATA ·simdPermTabNEON+(r*32+8)(SB)/8, b \
DATA ·simdPermTabNEON+(r*32+16)(SB)/8, c \
DATA ·simdPermTabNEON+(r*32+24)(SB)/8, d

	PERM(0, $0, $0, $0, $0)
	PERM(1, $0, $0, $0, $0)
	PERM(2, $0x0100010001000100, $0x0100010001000100, $0x0100010001000100, $0x0100010001000100)
	PERM(3, $0x0100020100020100, $0x0002010002010002, $0x0201000201000201, $0x0100020100020100)
	PERM(4, $0x0302010003020100, $0x0302010003020100, $0x0302010003020100, $0x0302010003020100)
	PERM(5, $0x0201000403020100, $0x0004030201000403, $0x0302010004030201, $0x0100040302010004)
	PERM(6, $0x0100050403020100, $0x0302010005040302, $0x0504030201000504, $0x0100050403020100)
	PERM(7, $0x0006050403020100, $0x0100060504030201, $0x0201000605040302, $0x0302010006050403)
	PERM(8, $0x0706050403020100, $0x0706050403020100, $0x0706050403020100, $0x0706050403020100)
	PERM(9, $0x0706050403020100, $0x0605040302010008, $0x0504030201000807, $0x0403020100080706)
	PERM(10, $0x0706050403020100, $0x0504030201000908, $0x0302010009080706, $0x0100090807060504)
	PERM(11, $0x0706050403020100, $0x04030201000a0908, $0x01000a0908070605, $0x0908070605040302)
	PERM(12, $0x0706050403020100, $0x030201000b0a0908, $0x0b0a090807060504, $0x0706050403020100)
	PERM(13, $0x0706050403020100, $0x0201000c0b0a0908, $0x0a09080706050403, $0x0504030201000c0b)
	PERM(14, $0x0706050403020100, $0x01000d0c0b0a0908, $0x0908070605040302, $0x030201000d0c0b0a)
	PERM(15, $0x0706050403020100, $0x000e0d0c0b0a0908, $0x0807060504030201, $0x01000e0d0c0b0a09)
	PERM(16, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0706050403020100, $0x0f0e0d0c0b0a0908)
	PERM(17, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0605040302010010, $0x0e0d0c0b0a090807)
	PERM(18, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0504030201001110, $0x0d0c0b0a09080706)
	PERM(19, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0403020100121110, $0x0c0b0a0908070605)
	PERM(20, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0302010013121110, $0x0b0a090807060504)
	PERM(21, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0201001413121110, $0x0a09080706050403)
	PERM(22, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0100151413121110, $0x0908070605040302)
	PERM(23, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x0016151413121110, $0x0807060504030201)
	PERM(24, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x0706050403020100)
	PERM(25, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x0605040302010018)
	PERM(26, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x0504030201001918)
	PERM(27, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x04030201001a1918)
	PERM(28, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x030201001b1a1918)
	PERM(29, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x0201001c1b1a1918)
	PERM(30, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x01001d1c1b1a1918)
	PERM(31, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x001e1d1c1b1a1918)
	PERM(32, $0x0706050403020100, $0x0f0e0d0c0b0a0908, $0x1716151413121110, $0x1f1e1d1c1b1a1918)

GLOBL ·simdPermTabNEON(SB), RODATA|NOPTR, $1056

#define OPS_START \
	ADD  TOKP, TOKEND                   \
	MOVD (ST), DP                       \
	ADD  DST, DP                        \
	MOVD 8(ST), R14                     \
	ADD  LITS, R14, LITP                \
	MOVD 16(ST), LAST                   \
	MOVD ZR, POS                        \
	MOVD $·simdOffTabNEON(SB), OFFTAB   \
	MOVD $·simdPermTabNEON(SB), PERMTAB \
	CMP  TOKEND, TOKP                   \
	BHS  done

// OPS_REST runs the rest of an operation, after its first 32 literals are stored.
// TOK keeps its flag bits; the match length code is TOK>>2, and codes 33 and up (TOK >= 132) are escapes.
// A match is copied from r = min(LAST, d) back, from R14 = DP - r. r is LAST unless that reaches
// before dst, which branches to clamp. When r is below the match length code,
// the match overlaps itself and r selects a simdPermTabNEON row.
// Matches longer than 32 bytes with r below 32 set the error flag.
#define OPS_REST \
	TST      $2, TOK               \
	MOVBU    (LLP), LEN            \
	CSEL     EQ, ZR, LEN, LEN      \
	CINC     NE, LLP, LLP          \
	CMP      $33, LEN              \
	BEQ      litlong               \
	litback:                       \
	ADD      LEN, DP               \
	ADD      LEN, LITP             \
	MOVBU    (OFFP), R14           \
	MOVD     (OFFTAB)(R14<<3), R14 \
	LSR      $3, POS, R15          \
	MOVD     (RAW)(R15), R15       \
	AND      $7, POS, R16          \
	LSR      R16, R15, R15         \
	ANDW     R14, R15, R15         \
	UBFX     $32, R14, $8, R16     \
	ADD      R14>>40, R15, R14     \
	ADD      R16, POS, R15         \
	TST      $1, TOK               \
	CSEL     NE, LAST, R14, LAST   \
	CSEL     NE, POS, R15, POS     \
	CINC     EQ, OFFP, OFFP        \
	SUBS     LAST, DP, R14         \
	CCMP     HS, R14, DST, $0      \
	BLO      clamp                 \
	CMP      TOK>>2, LAST          \
	BLO      pattern               \
	FLDPQ    (R14), (F2, F3)       \
	FSTPQ    (F2, F3), (DP)        \
	check:                         \
	CMP      $132, TOK             \
	BHS      long                  \
	next:                          \
	ADD      TOK>>2, DP            \
	CMP      TOKEND, TOKP          \
	BLO      loop                  \
	done:                          \
	SUB      DST, DP               \
	MOVD     DP, (ST)              \
	SUB      LITS, LITP            \
	MOVD     LITP, 8(ST)           \
	MOVD     LAST, 16(ST)          \
	RET                            \
	pattern:                                    \
	FLDPQ      (R14), (F2, F3)                  \
	ADD        LAST<<5, PERMTAB, R15            \
	FLDPQ      (R15), (F4, F5)                  \
	VTBL       V4.B16, [V2.B16, V3.B16], V6.B16 \
	VTBL       V5.B16, [V2.B16, V3.B16], V7.B16 \
	FSTPQ      (F6, F7), (DP)                   \
	B          check                            \
	clamp:                                      \
	MOVD       DST, R14                         \
	SUB        DST, DP, R16                     \
	FLDPQ      (R14), (F2, F3)                  \
	CMP        TOK>>2, R16                      \
	BHS        clampcopy                        \
	ADD        R16<<5, PERMTAB, R15             \
	FLDPQ      (R15), (F4, F5)                  \
	VTBL       V4.B16, [V2.B16, V3.B16], V2.B16 \
	VTBL       V5.B16, [V2.B16, V3.B16], V3.B16 \
	clampcopy:                                  \
	FSTPQ      (F2, F3), (DP)                   \
	B          check                            \
	litlong:                                    \
	MOVBU.P    1(ESCP), R15                     \
	ADD        $33, R15, LEN                    \
	ADD        $32, R15                         \
	LSR        $5, R15                          \
	ADD        $32, LITP, R14                   \
	ADD        $32, DP, R16                     \
	litloop:                                    \
	FLDPQ.P    32(R14), (F2, F3)                \
	FSTPQ.P    (F2, F3), 32(R16)                \
	SUBS       $1, R15                          \
	BNE        litloop                          \
	B          litback                          \
	long:                                       \
	SUB        R14, DP, R16                     \
	CMP        $32, R16                         \
	BHS        longok                           \
	MOVD       $1, R15                          \
	MOVD       R15, 24(ST)                      \
	longok:                                     \
	MOVBU.P    1(ESCP), R15                     \
	ADD        $33, R15, TOK                    \
	LSL        $2, TOK                          \
	ADD        $32, R15                         \
	LSR        $5, R15                          \
	ADD        $32, R14                         \
	ADD        $32, DP, R16                     \
	longloop:                                   \
	FLDPQ.P    32(R14), (F2, F3)                \
	FSTPQ.P    (F2, F3), 32(R16)                \
	SUBS       $1, R15                          \
	BNE        longloop                         \
	B          next

// func simdExecNEON(dst *byte, lits *byte, tok *byte, ll *byte, offc *byte, raw *byte, esc *byte, n int, st *[4]int)
TEXT ·simdExecNEON(SB), NOSPLIT, $0-72
	MOVD    dst+0(FP), DST
	MOVD    lits+8(FP), LITS
	MOVD    tok+16(FP), TOKP
	MOVD    ll+24(FP), LLP
	MOVD    offc+32(FP), OFFP
	MOVD    raw+40(FP), RAW
	MOVD    esc+48(FP), ESCP
	MOVD    n+56(FP), TOKEND
	MOVD    st+64(FP), ST
	OPS_START
	PCALIGN $64

loop:
	MOVBU.P 1(TOKP), TOK
	FLDPQ   (LITP), (F0, F1)
	FSTPQ   (F0, F1), (DP)
	OPS_REST

// func simdExecDeltaNEON(dst *byte, lits *byte, tok *byte, ll *byte, offc *byte, raw *byte, esc *byte, n int, st *[4]int)
// The first 32 literals are added to their references, from r = min(LAST, d) back before the operation.
TEXT ·simdExecDeltaNEON(SB), NOSPLIT, $0-72
	MOVD    dst+0(FP), DST
	MOVD    lits+8(FP), LITS
	MOVD    tok+16(FP), TOKP
	MOVD    ll+24(FP), LLP
	MOVD    offc+32(FP), OFFP
	MOVD    raw+40(FP), RAW
	MOVD    esc+48(FP), ESCP
	MOVD    n+56(FP), TOKEND
	MOVD    st+64(FP), ST
	OPS_START
	PCALIGN $64

loop:
	MOVBU.P 1(TOKP), TOK
	FLDPQ   (LITP), (F0, F1)
	SUB     DST, DP, R16
	CMP     R16, LAST
	CSEL    LO, LAST, R16, R15
	SUB     R15, DP, R17
	FLDPQ   (R17), (F2, F3)
	CMP     $32, R15
	BLO     dpattern
	VADD    V2.B16, V0.B16, V0.B16
	VADD    V3.B16, V1.B16, V1.B16

dstore:
	FSTPQ (F0, F1), (DP)
	OPS_REST

dpattern:
	ADD   R15<<5, PERMTAB, R16
	FLDPQ (R16), (F4, F5)
	VTBL  V4.B16, [V2.B16, V3.B16], V6.B16
	VTBL  V5.B16, [V2.B16, V3.B16], V7.B16
	VADD  V6.B16, V0.B16, V0.B16
	VADD  V7.B16, V1.B16, V1.B16
	B     dstore

	// FLAT_REST is OPS_REST for chunks with the no-overlap flag, whose operations start at position 32 or later.
	// A match is copied from r = min(max(LAST, min(ml, 32)), d) back. When LAST is below the match length code,
	// raise branches to raise it, so the match never overlaps. Only a larger LAST can reach before dst,
	// which branches to clamp, and the position, at least 32, is also at least the match length.
#define FLAT_REST \
	TST         $2, TOK               \
	MOVBU       (LLP), LEN            \
	CSEL        EQ, ZR, LEN, LEN      \
	CINC        NE, LLP, LLP          \
	CMP         $33, LEN              \
	BEQ         litlong               \
	litback:                          \
	ADD         LEN, DP               \
	ADD         LEN, LITP             \
	MOVBU       (OFFP), R14           \
	MOVD        (OFFTAB)(R14<<3), R14 \
	LSR         $3, POS, R15          \
	MOVD        (RAW)(R15), R15       \
	AND         $7, POS, R16          \
	LSR         R16, R15, R15         \
	ANDW        R14, R15, R15         \
	UBFX        $32, R14, $8, R16     \
	ADD         R14>>40, R15, R14     \
	ADD         R16, POS, R15         \
	TST         $1, TOK               \
	CSEL        NE, LAST, R14, LAST   \
	CSEL        NE, POS, R15, POS     \
	CINC        EQ, OFFP, OFFP        \
	CMP         TOK>>2, LAST          \
	BLO         raise                 \
	clampcheck:                       \
	SUBS        LAST, DP, R14         \
	CCMP        HS, R14, DST, $0      \
	BLO         clamp                 \
	copy:                             \
	FLDPQ       (R14), (F2, F3)       \
	FSTPQ       (F2, F3), (DP)        \
	CMP         $132, TOK             \
	BHS         long                  \
	next:                             \
	ADD         TOK>>2, DP            \
	CMP         TOKEND, TOKP          \
	BLO         loop                  \
	done:                             \
	SUB         DST, DP               \
	MOVD        DP, (ST)              \
	SUB         LITS, LITP            \
	MOVD        LITP, 8(ST)           \
	MOVD        LAST, 16(ST)          \
	RET                               \
	raise:                      \
	LSR       $2, TOK, R16      \
	CMP       $33, R16          \
	BNE       raised            \
	CMP       $32, LAST         \
	BHS       clampcheck        \
	MOVD      $32, R16          \
	raised:                     \
	SUB       R16, DP, R14      \
	B         copy              \
	clamp:                      \
	MOVD      DST, R14          \
	B         copy              \
	litlong:                    \
	MOVBU.P   1(ESCP), R15      \
	ADD       $33, R15, LEN     \
	ADD       $32, R15          \
	LSR       $5, R15           \
	ADD       $32, LITP, R14    \
	ADD       $32, DP, R16      \
	litloop:                    \
	FLDPQ.P   32(R14), (F2, F3) \
	FSTPQ.P   (F2, F3), 32(R16) \
	SUBS      $1, R15           \
	BNE       litloop           \
	B         litback           \
	long:                       \
	MOVBU.P   1(ESCP), R15      \
	ADD       $33, R15, TOK     \
	LSL       $2, TOK           \
	ADD       $32, R15          \
	LSR       $5, R15           \
	ADD       $32, R14          \
	ADD       $32, DP, R16      \
	longloop:                   \
	FLDPQ.P   32(R14), (F2, F3) \
	FSTPQ.P   (F2, F3), 32(R16) \
	SUBS      $1, R15           \
	BNE       longloop          \
	B         next

// func simdExecFlatNEON(dst *byte, lits *byte, tok *byte, ll *byte, offc *byte, raw *byte, esc *byte, n int, pos int, st *[4]int)
TEXT ·simdExecFlatNEON(SB), NOSPLIT, $0-80
	MOVD    dst+0(FP), DST
	MOVD    lits+8(FP), LITS
	MOVD    tok+16(FP), TOKP
	MOVD    ll+24(FP), LLP
	MOVD    offc+32(FP), OFFP
	MOVD    raw+40(FP), RAW
	MOVD    esc+48(FP), ESCP
	MOVD    n+56(FP), TOKEND
	MOVD    st+72(FP), ST
	OPS_START
	MOVD    pos+64(FP), POS
	PCALIGN $64

loop:
	MOVBU.P 1(TOKP), TOK
	FLDPQ   (LITP), (F0, F1)
	FSTPQ   (F0, F1), (DP)
	FLAT_REST

// func simdExecFlatDeltaNEON(dst *byte, lits *byte, tok *byte, ll *byte, offc *byte, raw *byte, esc *byte, n int, pos int, st *[4]int)
// The delta references are raised to 32 too, and clamped to the position, so they never repeat.
TEXT ·simdExecFlatDeltaNEON(SB), NOSPLIT, $0-80
	MOVD    dst+0(FP), DST
	MOVD    lits+8(FP), LITS
	MOVD    tok+16(FP), TOKP
	MOVD    ll+24(FP), LLP
	MOVD    offc+32(FP), OFFP
	MOVD    raw+40(FP), RAW
	MOVD    esc+48(FP), ESCP
	MOVD    n+56(FP), TOKEND
	MOVD    st+72(FP), ST
	OPS_START
	MOVD    pos+64(FP), POS
	MOVD    $32, C32
	PCALIGN $64

loop:
	MOVBU.P 1(TOKP), TOK
	FLDPQ   (LITP), (F0, F1)
	SUB     DST, DP, R16
	CMP     $32, LAST
	CSEL    HS, LAST, C32, R15
	CMP     R16, R15
	CSEL    HI, R16, R15, R15
	SUB     R15, DP, R17
	FLDPQ   (R17), (F2, F3)
	VADD    V2.B16, V0.B16, V0.B16
	VADD    V3.B16, V1.B16, V1.B16
	FSTPQ   (F0, F1), (DP)
	FLAT_REST

	// The stream checks of chunks take 16 bytes at a time; n is a multiple of 16 above 0.
	// They keep the largest byte, or byte - 1, in V7, and sum bytes with UADDLV into D lanes.

#define STATS_START \
	VEOR  V7.B16, V7.B16, V7.B16    \
	VEOR  V16.B16, V16.B16, V16.B16 \
	VEOR  V17.B16, V17.B16, V17.B16 \
	VEOR  V18.B16, V18.B16, V18.B16 \
	VEOR  V19.B16, V19.B16, V19.B16 \
	VMOVI $1, V29.B16

// STATS_OK sets R2 to 1 if no byte of V7 is above lim, else 0.
#define STATS_OK(lim) \
	VMOVI lim, V6.B16            \
	VUMIN V6.B16, V7.B16, V6.B16 \
	VCMEQ V6.B16, V7.B16, V6.B16 \
	VMOV  V6.D[0], R2            \
	VMOV  V6.D[1], R3            \
	AND   R3, R2                 \
	CMN   $1, R2                 \
	CSET  EQ, R2

// func simdTokStatsNEON(b *byte, n int) (out, lit, rep, esc int, ok bool)
TEXT ·simdTokStatsNEON(SB), NOSPLIT, $0-49
	MOVD  b+0(FP), R0
	MOVD  n+8(FP), R1
	STATS_START
	VMOVI $2, V30.B16
	VMOVI $4, V28.B16

tokloop:
	VLD1.P 16(R0), [V0.B16]
	VUMAX  V0.B16, V7.B16, V7.B16
	VUSHR  $2, V0.B16, V1.B16
	VAND   V30.B16, V0.B16, V2.B16
	VAND   V29.B16, V0.B16, V3.B16

	// Match length code 33 has bits 7 and 2.
	VUSHR   $5, V0.B16, V4.B16
	VAND    V0.B16, V4.B16, V4.B16
	VAND    V28.B16, V4.B16, V4.B16
	VUADDLV V1.B16, V1
	VUADDLV V2.B16, V2
	VUADDLV V3.B16, V3
	VUADDLV V4.B16, V4
	VADD    V1.D2, V16.D2, V16.D2
	VADD    V2.D2, V17.D2, V17.D2
	VADD    V3.D2, V18.D2, V18.D2
	VADD    V4.D2, V19.D2, V19.D2
	SUBS    $16, R1
	BNE     tokloop
	VMOV    V16.D[0], R2
	MOVD    R2, out+16(FP)
	VMOV    V17.D[0], R2
	LSR     $1, R2
	MOVD    R2, lit+24(FP)
	VMOV    V18.D[0], R2
	MOVD    R2, rep+32(FP)
	VMOV    V19.D[0], R2
	LSR     $2, R2
	MOVD    R2, esc+40(FP)
	STATS_OK($135)
	MOVB    R2, ok+48(FP)
	RET

// func simdLLStatsNEON(b *byte, n int) (sum, esc int, ok bool)
TEXT ·simdLLStatsNEON(SB), NOSPLIT, $0-33
	MOVD  b+0(FP), R0
	MOVD  n+8(FP), R1
	STATS_START
	VMOVI $33, V30.B16

llloop:
	VLD1.P 16(R0), [V0.B16]

	// 0 wraps to 255, so 1-33 is at most 32.
	VSUB    V29.B16, V0.B16, V1.B16
	VUMAX   V1.B16, V7.B16, V7.B16
	VCMEQ   V30.B16, V0.B16, V2.B16
	VAND    V29.B16, V2.B16, V2.B16
	VUADDLV V0.B16, V1
	VUADDLV V2.B16, V2
	VADD    V1.D2, V16.D2, V16.D2
	VADD    V2.D2, V17.D2, V17.D2
	SUBS    $16, R1
	BNE     llloop
	VMOV    V16.D[0], R2
	MOVD    R2, sum+16(FP)
	VMOV    V17.D[0], R2
	MOVD    R2, esc+24(FP)
	STATS_OK($32)
	MOVB    R2, ok+32(FP)
	RET

// func simdOffStatsNEON(b *byte, n int) (bits int, ok bool)
// The raw bits of symbol s are max(s>>3, 1) - 1.
TEXT ·simdOffStatsNEON(SB), NOSPLIT, $0-25
	MOVD b+0(FP), R0
	MOVD n+8(FP), R1
	MOVD R1, R4
	STATS_START

offloop:
	VLD1.P  16(R0), [V0.B16]
	VSUB    V29.B16, V0.B16, V1.B16
	VUMAX   V1.B16, V7.B16, V7.B16
	VUSHR   $3, V0.B16, V2.B16
	VUMAX   V29.B16, V2.B16, V2.B16
	VUADDLV V2.B16, V2
	VADD    V2.D2, V16.D2, V16.D2
	SUBS    $16, R1
	BNE     offloop
	VMOV    V16.D[0], R2
	SUB     R4, R2
	MOVD    R2, bits+16(FP)
	STATS_OK($174)
	MOVB    R2, ok+24(FP)
	RET
