//go:build !appengine && !noasm && gc && !purego

package minlz

import "github.com/minio/minlz/internal/cpu"

//go:noescape
func packBitsSSE2(dst []byte, src []byte)

//go:noescape
func packBitsAVX2(dst []byte, src []byte)

func packBits(dst, src []byte) {
	if cpu.AVX2 {
		packBitsAVX2(dst, src)
		return
	}
	packBitsSSE2(dst, src)
}
