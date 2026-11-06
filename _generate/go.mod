module github.com/minio/minlz/_generate

go 1.25.0

tool github.com/klauspost/asmfmt/cmd/asmfmt

require (
	github.com/honeycombio/avo v0.6.1-0.20261002035755-423abca816c4
	github.com/minio/minlz v0.0.0-00010101000000-000000000000
)

require (
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
)

replace github.com/minio/minlz => ../
