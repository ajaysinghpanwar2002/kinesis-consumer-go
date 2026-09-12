# Changelog

All notable changes to this project are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) as
described in [docs/releasing.md](docs/releasing.md).

Both published modules — the core library
(`github.com/ajaysinghpanwar2002/kinesis-consumer-go`) and the Valkey backend
(`github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey`) — are
released together and share the version numbers below.

## [Unreleased]

### Added

- Explicit acknowledgment mode: `WithExplicitHandler` and
  `WithExplicitBatchHandler` deliver `Delivery` handles whose `Ack` is what
  advances the checkpoint, so an application that completes work asynchronously
  can tie progress to what is durable rather than to what was delivered.
  Checkpoints cover the contiguous acknowledged prefix and flush on
  `WithCheckpointInterval` (default one second) as well as the existing record
  count. Graceful drain waits for outstanding acknowledgments and flushes before
  releasing a lease, and a closed shard is completed only after them. The mode
  requires a fenced checkpoint store paired with its lease manager and is always
  bounded by admission limits. See
  [docs/explicit-processing.md](docs/explicit-processing.md).
- `WithInFlightLimits` bounds admitted records and payload bytes per shard and
  per instance, plus fetched/staged pages, and splits batches into ordered
  prefixes that fit. See [docs/in-flight-limits.md](docs/in-flight-limits.md).
- Generation-fenced recovery for the built-in backends: per-shard sessions that
  validate ownership atomically with every read and write, write-once inclusive
  replay positions, recovery-anchor verification, and terminal completion
  markers. See [docs/fenced-recovery.md](docs/fenced-recovery.md).
- Exported `ErrStaleDelivery` and `ErrOversizedRecord` sentinels, and
  `*OversizedRecordError`, for `errors.Is`/`errors.As` matching.

## [0.1.0] - 2026-07-20

Initial public release. The library is pre-1.0: the API is frozen for this
line but may still change in a future minor version (see
[docs/releasing.md](docs/releasing.md)).

### Added

- Kinesis stream consumer with shard leasing, heartbeats, and fair-share
  rebalancing.
- Shard-aware, advance-only (monotonic) checkpointing with `SHARD_END`
  reshard gating and parent-shard ordering.
- Record and batch handlers with configurable batching, retries, and polling.
- Production-safe fail-fast poison-record handling by default, with explicit
  skip or send-to-DLQ options; DLQ publication has independent bounded retries,
  per-attempt deadlines, and stable idempotency keys for downstream deduplication.
- Optional graceful drain on shutdown (finish in-flight work, checkpoint,
  release the lease).
- Pluggable `checkpoint.Store` and `lease.Manager` interfaces with documented
  contracts, plus a built-in Valkey backend (standalone and cluster).
- Expiry-aware, per-group/stream Valkey lease and worker indexes with atomic
  owner checks, single-slot cluster routing, and snapshot command counts
  independent of unrelated database keys.
- Opt-in structured logging via `log/slog` (silent by default) and opt-in
  metrics via a dependency-free UDP statsd reporter with packaged Telegraf,
  InfluxDB, and Grafana assets (silent by default).
- `KinesisAPI` interface accepted by `consumer.New` so callers can supply test
  doubles or instrumented wrappers.
- Exported `ErrDrainTimeout` and `ErrNoShards` sentinels for `errors.Is`
  matching on `Start`'s errors.

[Unreleased]: https://github.com/ajaysinghpanwar2002/kinesis-consumer-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ajaysinghpanwar2002/kinesis-consumer-go/releases/tag/v0.1.0
