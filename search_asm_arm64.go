//go:build arm64 && !appengine && !noasm && gc && !purego

package minlz

//go:noescape
func packBitsNEON(dst []byte, src []byte)

func packBits(dst, src []byte) {
	packBitsNEON(dst, src)
}
