// Package integration holds end-to-end tests that exercise the consumer against
// real infrastructure (LocalStack Kinesis + Valkey/Redis).
//
// All test and helper code is guarded by the "integration" build tag, so the
// default build and the hermetic unit suite (make test) never run it. Use
// `make integration`, which brings up the dependencies via docker compose and
// runs `go test -tags integration ./...`.
//
// This untagged file exists only so the package always has buildable Go source.
package integration
