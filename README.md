# Kinesis Consumer Go

A pure Go library for consuming Kinesis streams with shard leasing, shard-aware checkpointing, and reshard-aware ordering.
No Java, no MultiLangDaemon.

## Why this library

- Native Go, single process. No JVM sidecar or MultiLangDaemon.
- Modular packaging: import only core, plus optional backend or metrics modules.
- Not tied to DynamoDB; DynamoDB, Valkey, and Redis backends are optional.
- Reshard-aware ordering via SHARD_END markers and parent gating.
- Tunable batching, retries, polling, and per-shard concurrency.
- Structured logs with `slog` and optional StatsD metrics + Grafana dashboards.
- Pluggable poison-record handling: fail-fast, skip, or send-to-DLQ.

## Key features

- Shard leasing with heartbeats and fair-share rebalancing.
- Shard-aware checkpointing and reshard gating.
- Record or batch handlers with configurable batching and retry.
- Pluggable DLQ publisher interface for poison records.
- Optional graceful drain mode on shutdown (finish in-flight work, checkpoint, release lease).
- Pluggable checkpoint stores and lease managers.
- LocalStack + Valkey/Redis workflow for local testing.

## Install

```bash
go get github.com/pratilipi/kinesis-consumer-go
```

## Documentation

- [Handler failure policy, DLQ, and shard concurrency](docs/handler-behavior.md)

## Comparison: AWS KCL vs MultiLangDaemon vs this library

| Aspect | AWS KCL (Java) | KCL MultiLangDaemon | Kinesis Consumer Go |
| --- | --- | --- | --- |
| Runtime | Java | App + Java daemon | Go only |
| Process model | Single JVM | Multi-process | Single process |
| Coordination store | DynamoDB (default) | DynamoDB (default) | DynamoDB/Valkey/Redis built-in, pluggable |
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
