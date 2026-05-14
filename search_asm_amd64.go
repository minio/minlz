//go:build !appengine && !noasm && gc && !purego

package minlz

//go:noescape
func cpuidAVX2() bool

//go:noescape
func packBitsSSE2(dst []byte, src []byte)

//go:noescape
func packBitsAVX2(dst []byte, src []byte)

var hasAVX2 = cpuidAVX2()

func packBits(dst, src []byte) {
	if hasAVX2 {
		packBitsAVX2(dst, src)
		return
	}
	packBitsSSE2(dst, src)
}
