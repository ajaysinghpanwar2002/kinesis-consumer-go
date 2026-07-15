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

- `pre-commit`: `make fmt-check` + `make vet` + `make build` + `make test` (the full gate).
- `pre-push`: `make test` again as a final safety net across all modules.

Useful targets: `make test`, `make build`, `make vet`, `make fmt-check`, `make tidy`.
