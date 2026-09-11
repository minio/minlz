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

// encodeAsmInput is one deterministic input shape. The assembly dispatch in
// encode_asm.go picks a differently-sized encoder at each of 1K, 4K, 16K, 64K,
// 512K and 2M, so the sizes below sit on both sides of every one of those
// boundaries: each of the seven lowered variants per family gets at least one
// digest, and off-by-one in the dispatch itself would move an input to a
// different variant and change its digest.
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
	// 1K class.
	{"repeats-1k", 1 << 10, genEncRepeats},
	// 4K class.
	{"text-1k-plus-1", 1<<10 + 1, genEncText},
	{"text-4k", 4 << 10, genEncText},
	// 16K class.
	{"mixed-4k-plus-1", 4<<10 + 1, genEncMixed},
	{"text-16k", 16 << 10, genEncText},
	// 64K class. random-64k encodes to a stored block at every level, which
	// pins the "nothing to gain" exit rather than an encoder.
	{"text-16k-plus-1", 16<<10 + 1, genEncText},
	{"text-32k", 32 << 10, genEncText},
	{"text-64k-minus-1", 64<<10 - 1, genEncText},
	{"random-64k", 64 << 10, genEncRandom},
	// 512K class.
	{"text-64k-plus-1", 64<<10 + 1, genEncText},
	{"repeats-256k", 256 << 10, genEncRepeats},
	{"mixed-256k", 256 << 10, genEncMixed},
	{"mixed-512k", 512 << 10, genEncMixed},
	// 2M class.
	{"text-512k-plus-1", 512<<10 + 1, genEncText},
	{"text-1m", 1 << 20, genEncText},
	{"mixed-2m", 2 << 20, genEncMixed},
	// Largest class, up to the 8M block limit.
	{"mixed-2m-plus-1", 2<<20 + 1, genEncMixed},
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
	"mixed-2m-plus-1/Balanced":   "ff0bc3d149572df2288370f113449f2a72f732c75e83fb3d7b1d98df8bc9744f",
	"mixed-2m-plus-1/Fastest":    "9faef2f77223b35d770f4eb8c58bac081af3cecb9cd22f399d8c1e6354d15676",
	"mixed-2m-plus-1/Smallest":   "40be3fcec7096111a53027124e6dd5da7085f389cc86d5c56f3292ec1d5db461",
	"mixed-2m-plus-1/SuperFast":  "6e7206fea57164befa6298c17d5211dc0735c32a89e8d460166833a41ba3c8ae",
	"mixed-2m/Balanced":          "3bb0957e9fb47a8d003f2a09038feab8bf91993692063cec9fc00c6d3b87a11e",
	"mixed-2m/Fastest":           "089783e04b4175e26b5caf86664699cb59fd2b4e67cd8e150d46a7a342d70c27",
	"mixed-2m/Smallest":          "12383e2d0e98ebcbd02daa488b06b11cf8c6e6939664dad0bafabbaf81c7c5d3",
	"mixed-2m/SuperFast":         "7211bdd7b5318b5d630435190225495916c83814ef7d67df240c90706770b4a0",
	"mixed-4k-plus-1/Balanced":   "d92e19219bf5eb51eed40815fc9acb3de11b3e0a02dae641afd8d10e2819315f",
	"mixed-4k-plus-1/Fastest":    "d92e19219bf5eb51eed40815fc9acb3de11b3e0a02dae641afd8d10e2819315f",
	"mixed-4k-plus-1/Smallest":   "d92e19219bf5eb51eed40815fc9acb3de11b3e0a02dae641afd8d10e2819315f",
	"mixed-4k-plus-1/SuperFast":  "d92e19219bf5eb51eed40815fc9acb3de11b3e0a02dae641afd8d10e2819315f",
	"mixed-512k/Balanced":        "8896ff57577dea25d32d65b4a7045536b79df45c0b0ef2b12279b499df289610",
	"mixed-512k/Fastest":         "d31b18b26622609d0d44208f9c689f8f8bf48bee24a18ed1086a5d7ce3b4b774",
	"mixed-512k/Smallest":        "f46e44a05b1f56988f06a33220a76977a451e658e625e7612fd91c5c8618d92c",
	"mixed-512k/SuperFast":       "305fb72ce261ae717844f3ffaa5e423dbdfe084e771944c76de354342be7a950",
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
	"text-16k-plus-1/Balanced":   "1248ee486bdaa1ebc8aa154a7e8b8217adac1332a1de26423c894e8febac7dd2",
	"text-16k-plus-1/Fastest":    "d7ff7038b200007baf6a21e8316663eb8121a23a0eea87b9f417aeacb09dd1ec",
	"text-16k-plus-1/Smallest":   "d2f63012bf40743eb513c1009a3fb598cf4fd06fca7488014cdbd5b87169eb25",
	"text-16k-plus-1/SuperFast":  "e1efcd5a906f983afafa8f14774a225d64fa4019277d9d3a2f5e258e1b1692cc",
	"text-16k/Balanced":          "927c03f7a52ab2fa0497e2729a75710fdcbabf91fec8391e250f12271af37ba4",
	"text-16k/Fastest":           "5229cfe90ce627ac3431e6a63d9253432f0ffc217319aa8d263825cbcfa41afd",
	"text-16k/Smallest":          "01f2178a6a736fe1587bf4a49114fda87164812848c5b635c112e39530a0e216",
	"text-16k/SuperFast":         "bc49a0717e5aba747d9e6acd437f5041b3e99bb6d56d46493a894f8916654cd9",
	"text-1k-plus-1/Balanced":    "788291af88972f6cfd390f0def8d2ccb28781ba2466ccd44f4de97934a26c8bf",
	"text-1k-plus-1/Fastest":     "3fd425693515c343ba50b50c55b6c94266909abfda96bad24bbc5bee3f1bd41d",
	"text-1k-plus-1/Smallest":    "0eb95f5f8c422759d384cd1bed9b38399945c3c552ac30a7de08cdd0f664996d",
	"text-1k-plus-1/SuperFast":   "107763edcd24c08c5b5cdea4da352f0b96f8c228564edf97adc206f715241e5d",
	"text-1m/Balanced":           "52d3ff88576103c4b479d8b984f8d6388ef7328a2edc68bd70c23c65337acefb",
	"text-1m/Fastest":            "db0e947b1af146ebd54b1201cb61d440fadacd5b2492f84f7c3be061aefa663f",
	"text-1m/Smallest":           "6096d1b5acdc117201fa5be502a5b61f7e53d476eb16287c3ad890e0c3d2c298",
	"text-1m/SuperFast":          "00d894b3599a7447cae2cc2a643cf241707aa75bc23ea5e35ce11f9a150300a2",
	"text-32k/Balanced":          "9f17bfdc675ade76a1865c64e2ccadadcf04514e810038520eb1f56c1ae18882",
	"text-32k/Fastest":           "5c95a5cbabccb2ae2034d865b044c7a749ef439016912597315ff9c241eb0101",
	"text-32k/Smallest":          "490cc695c4ae91643196549586dade8b565934ad77f73a9f2f6baefcd716a6bc",
	"text-32k/SuperFast":         "d308d5b7b84180e1a0781f08bef2eab61da262656a6c1f7cb972d6dd3506a40a",
	"text-4k/Balanced":           "29c090bda31bccf2fb787f37c1d86f7a51f13dad2b48a5bbc2d9a7804178f1d9",
	"text-4k/Fastest":            "c1d052ce98af5ab30512a33e8e8f4f1678c9fecc8055f1e15e7b203aef1d39ec",
	"text-4k/Smallest":           "ac63af233502931a3778a80746b6b4e273b44762dbcb201c5e0a49ecd1e45a62",
	"text-4k/SuperFast":          "51d0f51e7875c2e024248401392b8ab13e3fb552509314bc4086495156698e42",
	"text-512k-plus-1/Balanced":  "b33b5689af334ba1322f7bb2dbd92681df1d39c6c7ea270fd9286b0ca5ed8fc3",
	"text-512k-plus-1/Fastest":   "90b7423aa6d7e167ba74d981a843dbfbca5bfc78b4ffff8516e7cc4973aa342c",
	"text-512k-plus-1/Smallest":  "fd86cf4e79c6430615d4e5c6be27e075f3a98a009837166099ef6bef3746d058",
	"text-512k-plus-1/SuperFast": "a7eec24ebab564f8ce24607d7b49a430f4f9e6b3e5fe55089cebe276d1bcbc81",
	"text-64k-minus-1/Balanced":  "cdfec76bf9cd97695555fbc22e7937492b1ae6f765cd50d69220ba263059ddcf",
	"text-64k-minus-1/Fastest":   "7c0faaa524b4a0c62a14be36210aec3460b60a30497859687afbbcd80f5904b0",
	"text-64k-minus-1/Smallest":  "c22078911e504bd3ccfcaa274a298a8c0fff529326e117fac13b7550babe7135",
	"text-64k-minus-1/SuperFast": "c2667cf412b18157a9d0e500d4bab685e19eb2b0f9608d8140757af5a4f70224",
	"text-64k-plus-1/Balanced":   "4c99619b199a3bdbafd28685b715d4c4da240a7b4eab54aa55b8a0a6fb3a6b60",
	"text-64k-plus-1/Fastest":    "ec9e4627c2cb5a69731cf088b3b10e248b94bdac2da8f27585829cf754ed001c",
	"text-64k-plus-1/Smallest":   "3d4fec4dfdf4a1a5352b98a810b25454212b38a3190f683be6ed5e8c0cfe8d49",
	"text-64k-plus-1/SuperFast":  "efbf13d66ad6482bf8a3e7d75f8dfce0016ffcf55f669b0087158c84f063fbde",
}
