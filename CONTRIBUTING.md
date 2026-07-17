Thanks for taking the time to contribute! This project is intended to be a clean, native Go library for consuming Kinesis streams. For now, I’ll be working on it independently.

## Development setup

This repository is a Go workspace with four modules: core, the Valkey backend
under `pkg/backend/valkey`, the example under `examples/valkey`, and the
integration suite under `test/integration`. Use the `make` targets, which run
across the first three — a bare `go test ./...` only covers the module you are
standing in. The infra-backed `test/integration` suite runs via
`make integration`.

Install the shared git hooks once after cloning:

```bash
make hooks   # sets core.hooksPath=.githooks
```

- `pre-commit`: `make fmt-check` + `make vet` + `make build` + `make test` — the fast, offline local gate.
- `pre-push`: `make test` again as a final safety net across all modules.

CI (the unit workflow) runs the same gate plus four heavier checks the local
hook leaves out: `make lint` (staticcheck across every module),
`make integration-build` (compile-check of the tagged integration suite),
`make test-race` (race detector over the concurrency-heavy
`pkg/consumer` and `pkg/metrics/statsd` packages), and `make vulncheck`
(govulncheck vulnerability scan of both published modules; fails on any
finding reachable from their call graphs). Run them locally before opening a
PR if you want to match CI:

```bash
make lint               # staticcheck; fetches a pinned linter on first run
make integration-build  # compiles test/integration without any infrastructure
make test-race          # go test -race on the concurrency-heavy packages
make vulncheck          # govulncheck; needs network for the vulnerability DB
```

govulncheck also scans the standard library of the Go toolchain it runs on.
If `make vulncheck` reports findings under "Standard library", your local
toolchain is stale — rerun on the newest patch release (for example
`GOTOOLCHAIN=go1.26.5 make vulncheck`, or upgrade your Go install). CI always
resolves the newest patched toolchain (`check-latest`), so a stale local
toolchain is never the version of record.

Useful targets: `make test`, `make build`, `make vet`, `make fmt-check`, `make lint`, `make integration-build`, `make test-race`, `make vulncheck`, `make tidy`.
