module github.com/minio/minlz/_generate

go 1.25.0

require github.com/honeycombio/avo v0.6.1-0.20261002035755-423abca816c4

// The pivco generator loads the types of internal/pivco/kernel at run time; go mod tidy
// drops this requirement, as nothing imports minlz.
require github.com/minio/minlz v0.0.0-00010101000000-000000000000

require (
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
)

replace github.com/minio/minlz => ../
