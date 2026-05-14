//go:build (!amd64 && !arm64) || appengine || !gc || noasm || purego

package minlz

func packBits(dst, src []byte) {
	packBitsGeneric(dst, src)
}
