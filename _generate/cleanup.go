// Copyright 2025 MinIO Inc.
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

//go:build custom
// +build custom

package main

import (
	"bytes"
	"flag"
	"log"
	"os"

	"github.com/klauspost/asmfmt"
)

func main() {
	flag.Parse()
	args := flag.Args()
	for _, file := range args {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalln(err)
		}
		data = bytes.Replace(data, []byte("\t// #"), []byte("#"), -1)
		data, err = asmfmt.Format(bytes.NewBuffer(data))
		if err != nil {
			log.Fatalln(err)
		}
		err = os.WriteFile(file, data, os.ModePerm)
		if err != nil {
			log.Fatalln(err)
		}
	}
}
