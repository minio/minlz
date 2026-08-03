// Command enchash prints a digest of the encoder's output for a fixed input at
// each level. The encoders are supposed to be one algorithm expressed twice, so
// the digests must match across architectures; round-trip tests would not catch
// an arm64 encoder that emitted valid but different output.
package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"

	"github.com/minio/minlz"
)

func main() {
	src, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%s/%s  input=%d\n", runtime.GOOS, runtime.GOARCH, len(src))
	// Sizes chosen to reach different block encoders: each level dispatches on
	// input length, so one length exercises only one of them.
	for _, n := range []int{1 << 10, 5 << 10, 20 << 10, 100 << 10, 600 << 10} {
		if n > len(src) {
			break
		}
		for _, lvl := range []int{minlz.LevelFastest, minlz.LevelBalanced, minlz.LevelSmallest} {
			enc, err := minlz.Encode(nil, src[:n], lvl)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			sum := sha256.Sum256(enc)
			fmt.Printf("n=%-7d level=%d  len=%-8d sha=%x\n", n, lvl, len(enc), sum[:8])
		}
	}
}
