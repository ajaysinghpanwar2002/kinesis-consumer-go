# Kinesis Consumer Go

A pure Go library for consuming Kinesis streams with shard leasing, shard-aware checkpointing, and reshard-aware ordering.
No Java, no MultiLangDaemon.

## Why this library

- Native Go, single process. No JVM sidecar or MultiLangDaemon.
- Modular packaging: import only the core, plus an optional backend module.
- Not tied to DynamoDB: coordination state lives behind pluggable checkpoint-store
  and lease-manager interfaces. A Valkey backend is built in; DynamoDB and Redis are
  planned.
- Reshard-aware ordering via SHARD_END markers and parent gating.
- Tunable batching, retries, polling, and per-shard concurrency.
- Pluggable poison-record handling: fail-fast, skip, or send-to-DLQ.

## Key features

- Shard leasing with heartbeats and fair-share rebalancing.
- Shard-aware checkpointing and reshard gating.
- Record or batch handlers with configurable batching and retry.
- Pluggable DLQ publisher interface for poison records.
- Optional graceful drain mode on shutdown (finish in-flight work, checkpoint, release lease).
- Pluggable checkpoint stores and lease managers.
- Opt-in structured logging via `log/slog` (silent by default).
- Opt-in metrics with a dependency-free UDP statsd reporter and packaged
  Telegraf, InfluxDB, and Grafana assets (silent by default).
- LocalStack + Valkey workflow for local testing.

## Install

```bash
go get github.com/ajaysinghpanwar2002/kinesis-consumer-go
```

## Documentation

- [Getting started](docs/getting-started.md) — install, a minimal copy-paste-runnable consumer, multi-worker coordination, and graceful shutdown.
- [Features and capabilities](docs/features.md) — a complete, source-accurate inventory of what the library does (and what it does not yet do).
- [Configuration reference](docs/configuration.md) — every `Config` field and `With*` option with defaults, effects, and validation rules.
- [Handler failure policy, DLQ, and shard concurrency](docs/handler-behavior.md)
- [Logging](docs/logging.md) — enabling `WithLogger`, the complete structured event catalog with levels and attributes, and production guidance.
- [Metrics](docs/metrics.md) — enabling `WithMetrics`, the complete metric and tag catalog, statsd wire conventions, and the Telegraf/InfluxDB/Grafana path.
- [Integration test suite](docs/testing.md) — a verifiable ledger of every integration scenario and the behavior it proves.

## Examples

- [`examples/valkey`](examples/valkey) — a runnable consumer backed by Valkey for
  both checkpoints and leasing. It passes the Valkey store to `consumer.New` and
  does not call `WithLeaseManager`: because the store implements `lease.Provider`,
  the consumer turns on shard leasing automatically. Run two copies against the
  same stream and Valkey to see shards spread across workers. It needs a real (or
  LocalStack) Kinesis endpoint and a reachable Valkey server, so it is its own
  module and is not part of the test suite:

  ```bash
  cd examples/valkey
  go run . -stream-name my-stream -valkey-addr localhost:6379
  ```

## Development

The repository is a Go workspace with four modules: the core library, the
Valkey backend under `pkg/backend/valkey`, the example under `examples/valkey`,
and the integration suite under `test/integration`. Because `go test ./...`
does not cross module boundaries, the unit `make` targets iterate the first
three so nothing is silently skipped; the infra-backed `test/integration` suite
runs separately via `make integration`.

Run the unit test suite across all modules:

```bash
make test
```

Compile all packages (including the example):

```bash
make build
```

Other targets: `make vet`, `make fmt-check`, `make tidy`.

Run the same core test suite in Docker:

```bash
make docker-test
```

### Git hooks

Shared hooks live in `.githooks/` and are installed by pointing git at them:

```bash
make hooks   # sets core.hooksPath=.githooks
```

- `pre-commit` runs the full gate: `make fmt-check`, `make vet`, `make build`, `make test`.
- `pre-push` re-runs `make test` as a final safety net (e.g. for commits made with `--no-verify`).

Unit tests are hermetic (they use an in-memory Redis and no network).

## Integration test suite

Beyond the hermetic unit tests, the library is covered by an integration suite
that runs against **real LocalStack Kinesis + real Valkey** (not mocks) via
`make integration`. It exercises 30+ scenarios across attribution, checkpoint
resume, uncooperative failover, failure policies and DLQ, batch and concurrent
processing, start positions, resharding, rebalance fairness, and key isolation.

Each hardening scenario carries a load-bearing assertion, was mutation-verified
(a deliberate bug was shown to make it fail, then reverted), and was run
repeatedly to rule out flakiness. See [docs/testing.md](docs/testing.md) for the
full ledger — every row maps to a real test you can read and run.

## Comparison: AWS KCL vs MultiLangDaemon vs this library

| Aspect | AWS KCL (Java) | KCL MultiLangDaemon | Kinesis Consumer Go |
| --- | --- | --- | --- |
| Runtime | Java | App + Java daemon | Go only |
| Process model | Single JVM | Multi-process | Single process |
| Coordination store | DynamoDB (default) | DynamoDB (default) | Valkey built-in; pluggable interface (DynamoDB/Redis planned) |
| Packaging | Java deps | Java jars + wrapper | Core + optional modules |
| Language support | Java | Any via daemon | Go |

Community Go alternatives exist, but most are either lightweight consumers or wrappers around the Java KCL. This project targets a native Go experience with KCL-like coordination, without the daemon.

## Community native Go alternatives (non-AWS)

| Library | Focus | Notes |
| --- | --- | --- |
| [vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2) | KCL-like API | Native Go; aims to mirror KCL interface. |
| [gokini](https://github.com/patrobinson/gokini) | Minimal deps | Explicitly no MultiLangDaemon. |
| [kinsumer](https://github.com/twitchscience/kinsumer) | Native consumer | Smaller API surface, not full KCL parity. |
| [kinesumer](https://github.com/daangn/kinesumer) | Consumer group client | Uses DynamoDB state store. |
| [kinesis-consumer](https://github.com/harlow/kinesis-consumer) | Lightweight wrapper | Pluggable checkpoint backends. |

These are community-maintained projects; scope and activity vary.

## License

This project is licensed under the MIT License. See `LICENSE`.
