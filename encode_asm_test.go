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

package minlz

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// updateEncodeGolden regenerates the digests in encodeAsmGolden. Run it on
// amd64 -- that is the architecture the digests are defined to describe -- and
// paste the printed map back into this file:
//
//	GOARCH=amd64 go test -run TestEncodeAsmGolden -minlz.update-encode-golden ./
var updateEncodeGolden = flag.Bool("minlz.update-encode-golden", false,
	"print regenerated encodeAsmGolden digests instead of checking them")

// encodeAsmInput is one deterministic input shape. Sizes straddle 64<<10
// because the encoders dispatch to separate 64K variants below it.
type encodeAsmInput struct {
	name string
	size int
	gen  func(rng *rand.Rand, size int) []byte
}

func genEncRepeats(_ *rand.Rand, size int) []byte {
	pattern := []byte("the quick brown fox jumps over the lazy dog. ")
	out := make([]byte, 0, size+len(pattern))
	for len(out) < size {
		out = append(out, pattern...)
	}
	return out[:size]
}

func genEncText(rng *rand.Rand, size int) []byte {
	words := []string{
		"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
		"elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore",
	}
	out := make([]byte, 0, size+16)
	for len(out) < size {
		out = append(out, words[rng.Intn(len(words))]...)
		out = append(out, ' ')
	}
	return out[:size]
}

func genEncRandom(rng *rand.Rand, size int) []byte {
	out := make([]byte, size)
	rng.Read(out)
	return out
}

// genEncMixed alternates incompressible and compressible runs, which is what
// exercises the encoders' skip heuristics; neither uniform extreme does.
func genEncMixed(rng *rand.Rand, size int) []byte {
	const run = 4 << 10
	out := make([]byte, 0, size+2*run)
	for len(out) < size {
		noise := make([]byte, run)
		rng.Read(noise)
		out = append(out, noise...)
		out = append(out, genEncRepeats(rng, run)...)
	}
	return out[:size]
}

var encodeAsmInputs = []encodeAsmInput{
	{"repeats-1k", 1 << 10, genEncRepeats},
	{"repeats-256k", 256 << 10, genEncRepeats},
	{"text-32k", 32 << 10, genEncText},
	{"text-1m", 1 << 20, genEncText},
	{"random-64k", 64 << 10, genEncRandom},
	{"random-1m", 1 << 20, genEncRandom},
	{"mixed-256k", 256 << 10, genEncMixed},
	{"text-64k-minus-1", 64<<10 - 1, genEncText},
	{"text-64k-plus-1", 64<<10 + 1, genEncText},
}

var encodeAsmLevels = []struct {
	name  string
	level int
}{
	{"SuperFast", LevelSuperFast},
	{"Fastest", LevelFastest},
	{"Balanced", LevelBalanced},
	{"Smallest", LevelSmallest},
}

// TestEncodeAsmGolden pins the assembly encoders' output to digests recorded on
// amd64, so that any architecture producing different bytes fails here.
//
// This is what makes "arm64 encodes identically to amd64" a checked property
// rather than a claim. It matters because arm64's encoders are not written for
// the architecture: they are lowered from the same avo program that produces
// the amd64 ones, and a lowering bug can perfectly well yield output that is
// valid, decodes correctly, and is simply different. Round-trip tests pass in
// that case; this one does not.
//
// Note that comparing the assembly against the pure-Go encoders would not work:
// they are independent implementations and legitimately disagree on amd64
// today. The reference here is amd64's assembly, not Go's.
//
// If a legitimate encoder change makes this fail, regenerate with
// -minlz.update-encode-golden on amd64 rather than weakening the test.
func TestEncodeAsmGolden(t *testing.T) {
	if !hasAsm {
		t.Skip("no assembly encoders in this build; digests describe the assembly")
	}

	got := make(map[string]string, len(encodeAsmInputs)*len(encodeAsmLevels))
	for _, in := range encodeAsmInputs {
		for _, lv := range encodeAsmLevels {
			key := in.name + "/" + lv.name
			t.Run(key, func(t *testing.T) {
				src := in.gen(rand.New(rand.NewSource(1)), in.size)
				enc, err := Encode(nil, src, lv.level)
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				sum := sha256.Sum256(enc)
				got[key] = hex.EncodeToString(sum[:])

				if *updateEncodeGolden {
					return
				}
				want, ok := encodeAsmGolden[key]
				if !ok {
					t.Fatalf("no golden digest for %q; regenerate on amd64", key)
				}
				if got[key] != want {
					t.Errorf("encoded output differs from the amd64 reference\n got %s (%d bytes)\nwant %s",
						got[key], len(enc), want)
				}

				// Catches the case where every architecture is wrong the same
				// way, which a digest comparison alone would not.
				dec, err := Decode(nil, enc)
				if err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if !bytes.Equal(dec, src) {
					t.Fatalf("round trip differs from input at offset %d", matchLen(dec, src))
				}
			})
		}
	}

	if *updateEncodeGolden {
		keys := make([]string, 0, len(got))
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := &bytes.Buffer{}
		fmt.Fprintf(out, "var encodeAsmGolden = map[string]string{\n")
		for _, k := range keys {
			fmt.Fprintf(out, "\t%q: %q,\n", k, got[k])
		}
		fmt.Fprintf(out, "}\n")
		t.Log("regenerated digests:\n" + out.String())
	}
}

// encodeAsmGolden holds SHA-256 digests of the assembly encoders' output,
// recorded on amd64. Regenerate with -minlz.update-encode-golden; see
// TestEncodeAsmGolden.
var encodeAsmGolden = map[string]string{
	"mixed-256k/Balanced":        "88eb1b8821ea17746df83c4a477807a9d7b7080bc502466d2d6bd942a340b553",
	"mixed-256k/Fastest":         "2088948cf5d7865d07c32a0c0ac506789b7d08f85af427371749460fe63d260c",
	"mixed-256k/Smallest":        "e9a5609bf0982c7b843b255ed5b34d6516002f8b4291f3a861c5512529281b0b",
	"mixed-256k/SuperFast":       "c791b630dacc6738a8305bf5f5f3464d9b87c273d1720482d226b72c1cbe7044",
	"random-1m/Balanced":         "9db7cd2cb1862ffdf41fa8cb11a9b67d1774a52ec1f3fde5c25ab8571e512e80",
	"random-1m/Fastest":          "9db7cd2cb1862ffdf41fa8cb11a9b67d1774a52ec1f3fde5c25ab8571e512e80",
	"random-1m/Smallest":         "9db7cd2cb1862ffdf41fa8cb11a9b67d1774a52ec1f3fde5c25ab8571e512e80",
	"random-1m/SuperFast":        "9db7cd2cb1862ffdf41fa8cb11a9b67d1774a52ec1f3fde5c25ab8571e512e80",
	"random-64k/Balanced":        "07a6b7ac3df0b1f79dec447dddcb369b04d3a060fd56e221456793aee2bfbd93",
	"random-64k/Fastest":         "07a6b7ac3df0b1f79dec447dddcb369b04d3a060fd56e221456793aee2bfbd93",
	"random-64k/Smallest":        "07a6b7ac3df0b1f79dec447dddcb369b04d3a060fd56e221456793aee2bfbd93",
	"random-64k/SuperFast":       "07a6b7ac3df0b1f79dec447dddcb369b04d3a060fd56e221456793aee2bfbd93",
	"repeats-1k/Balanced":        "0482adbd31ccf6e8f254881c382d962ead21337a09ef23c18125ea655f2528bd",
	"repeats-1k/Fastest":         "964eb2bd11e018d68395a283a07d65e130b177956710cb408aaa92b6d5af9455",
	"repeats-1k/Smallest":        "0482adbd31ccf6e8f254881c382d962ead21337a09ef23c18125ea655f2528bd",
	"repeats-1k/SuperFast":       "997c39d7860da6da0bcb8d831699dff7fa71d1e5f0986e8c3519c98a587caa3c",
	"repeats-256k/Balanced":      "c3d0852c7f7c6d62e008af056ae1c4366d5b8a860a96ffed41a6c47d8be84424",
	"repeats-256k/Fastest":       "c3d0852c7f7c6d62e008af056ae1c4366d5b8a860a96ffed41a6c47d8be84424",
	"repeats-256k/Smallest":      "57cc0ed085f598119fb2bd335c1607da3b85fd1d5595bde16edc42913e67193d",
	"repeats-256k/SuperFast":     "57cc0ed085f598119fb2bd335c1607da3b85fd1d5595bde16edc42913e67193d",
	"text-1m/Balanced":           "52d3ff88576103c4b479d8b984f8d6388ef7328a2edc68bd70c23c65337acefb",
	"text-1m/Fastest":            "db0e947b1af146ebd54b1201cb61d440fadacd5b2492f84f7c3be061aefa663f",
	"text-1m/Smallest":           "6096d1b5acdc117201fa5be502a5b61f7e53d476eb16287c3ad890e0c3d2c298",
	"text-1m/SuperFast":          "00d894b3599a7447cae2cc2a643cf241707aa75bc23ea5e35ce11f9a150300a2",
	"text-32k/Balanced":          "9f17bfdc675ade76a1865c64e2ccadadcf04514e810038520eb1f56c1ae18882",
	"text-32k/Fastest":           "5c95a5cbabccb2ae2034d865b044c7a749ef439016912597315ff9c241eb0101",
	"text-32k/Smallest":          "490cc695c4ae91643196549586dade8b565934ad77f73a9f2f6baefcd716a6bc",
	"text-32k/SuperFast":         "d308d5b7b84180e1a0781f08bef2eab61da262656a6c1f7cb972d6dd3506a40a",
	"text-64k-minus-1/Balanced":  "cdfec76bf9cd97695555fbc22e7937492b1ae6f765cd50d69220ba263059ddcf",
	"text-64k-minus-1/Fastest":   "7c0faaa524b4a0c62a14be36210aec3460b60a30497859687afbbcd80f5904b0",
	"text-64k-minus-1/Smallest":  "c22078911e504bd3ccfcaa274a298a8c0fff529326e117fac13b7550babe7135",
	"text-64k-minus-1/SuperFast": "c2667cf412b18157a9d0e500d4bab685e19eb2b0f9608d8140757af5a4f70224",
	"text-64k-plus-1/Balanced":   "4c99619b199a3bdbafd28685b715d4c4da240a7b4eab54aa55b8a0a6fb3a6b60",
	"text-64k-plus-1/Fastest":    "ec9e4627c2cb5a69731cf088b3b10e248b94bdac2da8f27585829cf754ed001c",
	"text-64k-plus-1/Smallest":   "3d4fec4dfdf4a1a5352b98a810b25454212b38a3190f683be6ed5e8c0cfe8d49",
	"text-64k-plus-1/SuperFast":  "efbf13d66ad6482bf8a3e7d75f8dfce0016ffcf55f669b0087158c84f063fbde",
}
