# Features and Capabilities

A complete, source-accurate inventory of what `kinesis-consumer-go` does today.
The library is a single-process, pure-Go Kinesis consumer with KCL-style shard
leasing and checkpointing, and no MultiLangDaemon or JVM sidecar.

This page is a capability ledger. For the deep behavior of handlers, retries,
failure policies, and shard concurrency, see
[handler-behavior.md](handler-behavior.md). For what is intentionally **not**
built yet, see [Not yet built](#not-yet-built) at the end — the list is honest
so you can plan around the gaps.

## At a glance

| Area | Capability |
| --- | --- |
| Stream identity | Consume by `StreamName` or `StreamARN` |
| Start position | `LATEST` (default), `TRIM_HORIZON`, `AT_TIMESTAMP`; resumes from checkpoint when one exists |
| Shard discovery | Initial listing + periodic resync; live reshard/refresh without restart |
| Reshard ordering | Parent/child gating via `SHARD_END` completion markers |
| Coordination | Shard leases with acquire/renew/release + worker heartbeats |
| Rebalancing | Fair-share across workers with jitter, cooldown, bounded moves, overage shedding |
| Checkpointing | Per-page, periodic (`checkpointEvery`), on-drain, and catch-up flush |
| Handlers | Per-record or per-page batch handler |
| Reliability | Configurable retry + failure policy (skip / fail-fast / send-to-DLQ) |
| DLQ | Pluggable `DLQPublisher` with rich poison-record metadata |
| Throughput | Per-shard record-handler concurrency |
| Shutdown | Optional graceful drain (finish in-flight work, checkpoint, release leases) |
| Backends | Built-in Valkey; pluggable `checkpoint.Store` + `lease.Manager` interfaces |
| Multi-tenancy | Key-prefix isolation for checkpoint, lease, and worker keys |
| Logging | Opt-in structured `log/slog` events for lifecycle, leases, rebalance, and record outcomes ([logging.md](logging.md)) |
| Metrics | Opt-in counters, gauges, and timings through `metrics.Reporter`, with a statsd/Telegraf/InfluxDB/Grafana path ([metrics.md](metrics.md)) |

## Public entry point

```go
consumer.New(cfg consumer.Config, client *kinesis.Client, store checkpoint.Store, handler consumer.HandlerFunc, opts ...consumer.Option) (*consumer.Consumer, error)
```

`Start(ctx)` runs the consumer and blocks until `ctx` is cancelled (or a fatal
error surfaces). The Kinesis client and checkpoint store are required; the
handler may be nil only when a batch handler is supplied via `WithBatchHandler`.

## Stream identity and start position

- **Identity:** set `Config.StreamName` or `Config.StreamARN` (one is required).
  All coordination keys are keyed by whichever is set, consistently across
  discovery, checkpointing, and leasing.
- **Start position** (`Config.StartPosition`, used only when no checkpoint
  exists for a shard):
  - `StartLatest` — the default; only records produced after the consumer
    positions on the shard.
  - `StartTrimHorizon` — from the oldest retained record.
  - `StartAtTimestamp` — from `Config.StartTimestamp` (required for this mode).
- **Resume:** when a checkpoint exists for a shard, the consumer resumes strictly
  after the checkpointed sequence number, ignoring `StartPosition`.

## Shard discovery and resharding

- **Discovery:** shards are listed at startup and re-listed on a periodic shard
  sync tick (`WithPolling`'s second argument, default 1 minute).
- **Live reshard/refresh:** a long-running consumer discovers new child shards
  produced by a split/merge on its sync tick and begins consuming them without a
  restart.
- **Parent/child gating:** children are held until their parent shard is read to
  completion. Completion is recorded as a `SHARD_END` checkpoint marker, and
  children only become eligible once every parent is complete — preserving
  per-key ordering across a reshard.

## Coordination: leasing, heartbeats, rebalancing

Leasing turns on automatically when the checkpoint store also implements
`lease.Provider` (the Valkey store does), or explicitly via `WithLeaseManager`.
Run multiple consumer processes against the same stream and store and shards
spread across them.

- **Leases:** each shard is owned by exactly one worker via an acquire / renew /
  release lifecycle backed by owner-checked operations, so ownership transfers
  stay safe against concurrent workers.
- **Worker heartbeats:** each worker periodically records liveness
  (`WithHeartbeat`, default 5s interval / 20s TTL). Heartbeat state sizes each
  worker's fair share; a worker whose heartbeat lapses is treated as inactive.
- **Fair-share rebalancing** (`WithRebalance`): workers converge toward an even
  shard split. Rebalancing is damped to avoid thrash:
  - a randomized interval (min + jitter, default 10s + 10s),
  - a per-shard cooldown after a move (default 10s) that blocks immediate
    re-acquire,
  - a bound on the number of moves per rebalance tick (default 2).
  - The mechanism combines both directions: an under-share worker (below its
    fair-share low bound) first acquires unowned shards, then, if still short,
    claims a shard from an over-share donor (a worker holding more than the high
    bound); separately, an over-share worker sheds its excess shards.
- **Reclaim of failed workers:** a shard held by an inactive worker is reclaimed
  after its lease key's TTL lapses (the shard becomes unowned and is acquired on
  a later tick).

## Checkpointing

- **Granularity:** progress is saved per successfully processed `GetRecords`
  page. The page is checkpointed only after the whole page's handler work
  succeeds.
- **Periodic:** `WithBatching`'s `checkpointEvery` (default 100) throttles how
  often a checkpoint is written across processed records.
- **On drain:** graceful shutdown flushes the latest processed position before
  the worker exits.
- **Catch-up flush:** when a worker catches up to the shard tip, it flushes its
  pending position so a later restart or failover resumes from there.
- **Delivery guarantee:** at-least-once. A crash between handler success and
  checkpoint can re-deliver the in-flight page, so handlers should tolerate
  duplicates.

## Handlers, retries, and failure policy

- **Record handler:** `HandlerFunc(ctx, Record)` called once per record
  (`Record` is the AWS SDK `types.Record`).
- **Batch handler:** `WithBatchHandler(BatchHandlerFunc)` called once per
  `GetRecords` page instead of per record. Batch mode has no per-record
  isolation — one failing record fails the whole page.
- **GetRecords page size:** `WithBatching`'s `batchSize` (default 100) bounds the
  Kinesis `GetRecords` `Limit`.
- **Retries:** `WithRetry(maxAttempts, backoff)` (default 3 attempts, 1s base)
  with linear backoff (`sleep = attempt * backoff`). Context cancellation /
  deadline is terminal and bypasses the failure policy.
- **Failure policy** (`WithFailurePolicy`, applied after retries are exhausted):
  - `FailurePolicySkip` — default; treat as handled and continue.
  - `FailurePolicyFailFast` — stop the worker and surface the error through
    `Start`.
  - `FailurePolicySendToDLQ` — publish to the configured `DLQPublisher` and
    continue (requires `WithDLQPublisher`; `New` errors without one).

See [handler-behavior.md](handler-behavior.md) for the full failure-policy and
DLQ semantics.

## Dead-letter queue

- `WithDLQPublisher(DLQPublisher)` plugs in any destination implementing
  `Publish(ctx, PoisonRecord) error`.
- `PoisonRecord` carries stream name/ARN, shard ID, sequence number, partition
  key, approximate arrival timestamp, a copied payload, the handler error text,
  handler kind (`record` or `batch`), attempt count, failure time, and batch
  index fields.
- For a failed batch, every record in the page is published as its own
  `PoisonRecord` with batch metadata (there is no per-record isolation in batch
  mode).

## Per-shard concurrency

- `WithShardConcurrency(n)` (default 1) runs up to `n` record-handler calls
  concurrently within one shard page. `n > 1` improves throughput but breaks
  strict per-shard ordering; keep `n == 1` when order matters. Applies to record
  handlers only. See [handler-behavior.md](handler-behavior.md).

## Graceful shutdown

- `WithGracefulDrain(timeout)` lets workers finish in-flight work, checkpoint,
  and release their leases before `Start` returns on cancellation (a zero
  timeout waits indefinitely). Without it, shutdown is prompt and relies on the
  at-least-once guarantee + failover for any in-flight page.

## Backends and pluggability

- **Interfaces (core module):**
  - `checkpoint.Store` — `Get` / `Save` / `Delete` per (stream, shard).
  - `lease.Manager` — `Acquire` / `Claim` / `Renew`+`Release` (via `Lease`) /
    `Heartbeat` / `List` / `Workers`.
  - `lease.Provider` — lets a store supply its own lease manager so a single
    dependency backs both checkpoints and leases.
- **Built-in Valkey backend** (`pkg/backend/valkey`, separate module): a
  `checkpoint.Store` plus a lease `Manager`. The store implements
  `lease.Provider`, so passing only the Valkey store to `consumer.New` enables
  leasing automatically with no `WithLeaseManager` call. Ownership-safe
  transfers use owner-checked Lua scripts.
- **In-memory backends** (`checkpoint.MemoryStore`, `lease.MemoryManager`): for
  tests and single-process experimentation.

## Multi-tenant key isolation

All coordination state lives under configurable key prefixes, so multiple
streams or tenants can share one Valkey without colliding:

- Checkpoints: `<checkpointPrefix>:<stream>:<shard>` (default prefix
  `kinesis-checkpoint`; override with `WithKeyPrefix`).
- Leases: `<leasePrefix>:<stream>:<shard>`, where `leasePrefix` defaults to
  `<checkpointPrefix>-lease`.
- Worker heartbeats: `<leasePrefix>-worker:<stream>:<owner>`.

## Observability

- **Structured logging:** `WithLogger` enables `log/slog` lifecycle, lease,
  rebalance, checkpoint, drain, and failure-policy events. The default discard
  logger is silent. See [logging.md](logging.md) for the event catalog and
  production guidance.
- **Metrics:** `WithMetrics` enables the full counter, gauge, and timing catalog
  through `metrics.Reporter`. The default `metrics.Nop{}` reporter is silent.
  The core module includes a dependency-free UDP statsd implementation, plus
  sample Telegraf, InfluxDB, and Grafana assets. See
  [metrics.md](metrics.md) for names, tags, semantics, wiring, and cardinality
  guidance.

## Configuration defaults

Every knob has a working default, so a consumer runs with no options at all.

| Option | Default | Effect |
| --- | --- | --- |
| `Config.StartPosition` | `LATEST` | Start position when no checkpoint exists |
| `WithBatching(batchSize, checkpointEvery)` | 100, 100 | GetRecords page size; checkpoint throttle |
| `WithPolling(pollInterval, shardSyncInterval)` | 1s, 1m | GetRecords poll cadence; shard resync cadence |
| `WithRetry(maxAttempts, backoff)` | 3, 1s | Handler retry attempts and linear base backoff |
| `WithShardConcurrency(n)` | 1 | Concurrent record-handler calls per shard |
| `WithFailurePolicy(policy)` | `skip` | Post-retry poison handling |
| `WithHeartbeat(interval, ttl)` | 5s, 20s | Worker liveness cadence and lease/worker TTL |
| `WithRebalance(min, jitter, cooldown, maxMoves)` | 10s, 10s, 10s, 2 | Rebalance timing and move bounds |
| `WithGracefulDrain(timeout)` | off | Drain in-flight work on shutdown |
| `WithLogger(logger)` | discard (silent) | Structured `log/slog` lifecycle/lease/rebalance/record events |
| `WithMetrics(reporter)` | `metrics.Nop{}` (silent) | Counters, gauges, and timings through `metrics.Reporter` |
| Lease release timeout | 5s | Bound on releasing a lease during shutdown |

## Not yet built

Kept honest so the docs are a reliable contract:

- **Additional backends:** only Valkey is built in. DynamoDB and Redis are
  planned behind the same `checkpoint.Store` / `lease.Manager` interfaces but are
  not implemented.
