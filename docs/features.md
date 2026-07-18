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
| Stream identity | Consume by exactly one of `StreamName` / `StreamARN`, isolated by required `ConsumerGroup` |
| Start position | `LATEST` (default), `TRIM_HORIZON`, `AT_TIMESTAMP`; resumes from checkpoint when one exists; reshard children always continue from `TRIM_HORIZON` |
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
| Multi-tenancy | Consumer-group plus key-prefix isolation for checkpoint, lease, and worker keys |
| Logging | Opt-in structured `log/slog` events for lifecycle, leases, rebalance, and record outcomes ([logging.md](logging.md)) |
| Metrics | Opt-in counters, gauges, and timings through `metrics.Reporter`, with a statsd/Telegraf/InfluxDB/Grafana path ([metrics.md](metrics.md)) |

## Public entry point

```go
consumer.New(cfg consumer.Config, client consumer.KinesisAPI, store checkpoint.Store, handler consumer.HandlerFunc, opts ...consumer.Option) (*consumer.Consumer, error)
```

`Start(ctx)` runs the consumer and blocks until `ctx` is cancelled (or a fatal
error surfaces). The Kinesis client and checkpoint store are required; the
handler may be nil only when a batch handler is supplied via `WithBatchHandler`.

`KinesisAPI` is the three-method interface (`ListShards`, `GetShardIterator`,
`GetRecords`) the consumer depends on. A `*kinesis.Client` satisfies it
directly, so you normally pass `kinesis.NewFromConfig(...)`; supply your own
implementation when you want a test double or an instrumented wrapper.

`Start` returns two exported sentinel errors you can branch on with
`errors.Is`: `consumer.ErrNoShards` when the stream has no shards to consume,
and `consumer.ErrDrainTimeout` when a `WithGracefulDrain` drain does not finish
in time (the returned error still carries the stream name / elapsed timeout in
its message).

## Stream identity and start position

- **Identity:** set exactly one of `Config.StreamName` or `Config.StreamARN`,
  plus required `Config.ConsumerGroup`. AWS calls use the configured name or
  ARN; checkpoint, lease, and heartbeat keys use
  `<consumer-group>:<canonical-stream-name>`. Workers in one group share
  progress and ownership, while different groups consume independently.
- **Start position** (`Config.StartPosition`, used only for a **parentless**
  shard with no checkpoint):
  - `StartLatest` — the default; only records produced after the consumer
    positions on the shard.
  - `StartTrimHorizon` — from the oldest retained record.
  - `StartAtTimestamp` — from `Config.StartTimestamp` (required for this mode).
- **Reshard children:** a checkpoint-less shard with a parent in the known
  shard listing always resumes from `TRIM_HORIZON`, regardless of
  `StartPosition`. Parent/child gating guarantees the child only starts after
  every parent reached `SHARD_END`, so this is an exact continuation — records
  produced into the child between the reshard and pickup are never skipped
  (with `StartLatest` they otherwise would be). `StartPosition` still applies
  when the parents have aged out of retention and never appeared in this
  consumer's listings (e.g. a fresh group bootstrapping on a long-ago-resharded
  stream).
- **Resume:** when a checkpoint exists for a shard, the consumer resumes strictly
  after the checkpointed sequence number, ignoring `StartPosition`.

## Shard discovery and resharding

- **Discovery:** shards are listed at startup and re-listed on a periodic shard
  sync tick (`WithPolling`'s second argument, default 1 minute).
- **Live reshard/refresh:** a long-running consumer discovers new child shards
  produced by a split/merge on its sync tick and begins consuming them without a
  restart.
- **Survivable sync failures:** a failed sync pass (Kinesis listing, checkpoint
  readiness reads, lease listing/acquisition) keeps the last known shard map and
  every running worker; discovery retries with capped backoff + jitter. Fatal
  authorization/configuration errors stop the consumer immediately, and
  persistent failure stops it with `ErrShardSyncStale` once the
  `WithShardSyncMaxStaleness` bound (default 10x the sync interval) is
  exceeded. Sync health — consecutive failures, last success, last error — is
  exposed via `Consumer.Health()`.
- **Parent/child gating:** children are held until their parent shard is read to
  completion. Completion is recorded as a `SHARD_END` checkpoint marker, and
  children only become eligible once every parent is complete — preserving
  per-key ordering across a reshard.

## Coordination: leasing, heartbeats, rebalancing

Leasing turns on automatically when the checkpoint store also implements
`lease.Provider` (the Valkey store does), or explicitly via `WithLeaseManager`.
Run multiple consumer processes against the same stream and store and shards
spread across them.

- **Leases:** each shard is owned by one worker in steady state via an
  acquire / renew / release lifecycle backed by owner-checked operations, so
  ownership transfers stay safe against concurrent workers. Exclusivity is
  time-bounded, not absolute — see
  [Ownership transfer windows](#ownership-transfer-windows).
- **Worker heartbeats:** each worker periodically records liveness
  (`WithHeartbeat`, default 5s interval / 20s TTL). Heartbeat state sizes each
  worker's fair share; a worker whose heartbeat lapses is treated as inactive.
  Failed sends are survivable while the indexed entry from the last successful
  send is live, but once failures persist to one heartbeat interval before
  that entry expires, the consumer stops with `ErrHeartbeatStale` — before
  peers can treat it as dead and claim its shards away while it is still
  processing. Heartbeat health — consecutive failures, last success, last
  error — is exposed via `Consumer.Health()`.
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
  - Startup and shard-sync acquisition are bounded by the same fair share and
    cooldown: a worker fills toward its high bound from unowned shards only, so
    a cold-starting worker does not grab the whole stream just to shed it back,
    and a shard this worker just shed is not re-acquired before its cooldown
    expires.
- **Reclaim of failed workers:** a shard held by an inactive worker is reclaimed
  after its indexed lease expiration lapses (the shard becomes unowned and is acquired on
  a later tick).

### Ownership transfer windows

Every ownership transfer opens a short window during which two workers may
process the same shard. This is a deliberate part of the at-least-once
contract — the library bounds the windows instead of trying to eliminate
them:

- **Rebalance claim:** the taker claims the lease and starts immediately; the
  donor notices at its next lease renew (within one heartbeat interval,
  default 5s) plus whatever in-flight handler work it finishes first.
- **Failed-worker reclaim:** a crashed or partitioned worker's shard is
  reclaimed once its lease TTL lapses (default 20s). If the old worker is
  still running but cannot reach the lease backend, its renew attempts are
  retried as transient failures until the TTL budget is exhausted, then the
  worker stops; if it reaches the backend after the reclaim, the next renew
  fails with `ErrNotOwned` and the worker stops immediately.
- **Hung renew (zombie):** a worker whose renew call hangs without erroring is
  fenced by a local lease-validity watchdog within the lease TTL plus two
  heartbeat intervals (default 30s).

Each bound above is the time until the old worker is *told* to stop; add the
time its in-flight handler work takes to observe cancellation for the full
window.

During any of these windows the effect is bounded duplicate delivery, never
data loss or checkpoint rollback: checkpoint saves are advance-only, so a
fenced-out worker's late write cannot regress progress recorded by the new
owner or overwrite a `SHARD_END` completion marker. Handlers must tolerate
duplicates (see the delivery guarantee under
[Checkpointing](#checkpointing)); if your workload needs cross-worker mutual
exclusion or exactly-once effects, enforce them downstream (idempotent
writes, conditional updates keyed by sequence number).

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
  checkpoint can re-deliver the in-flight page, and an ownership transfer can
  briefly deliver the same records to two workers (see
  [Ownership transfer windows](#ownership-transfer-windows)), so handlers
  must tolerate duplicates.

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
  - `FailurePolicyFailFast` — default; stop the worker, surface the error
    through `Start`, and do not checkpoint the failed page.
  - `FailurePolicySkip` — explicit, intentionally lossy opt-in. Record mode
    drops the failed record but continues the page; batch mode drops the entire
    failed `GetRecords` page. The completed page is then eligible for a
    checkpoint, so the dropped data is not replayed on resume.
  - `FailurePolicySendToDLQ` — publish to the configured `DLQPublisher` and
    continue (requires `WithDLQPublisher`; `New` errors without one).

See [handler-behavior.md](handler-behavior.md) for the full failure-policy and
DLQ semantics.

## Dead-letter queue

- `WithDLQPublisher(DLQPublisher)` plugs in any destination implementing
  `Publish(ctx, PoisonRecord) error`.
- `WithDLQRetry(maxAttempts, backoff)` (default 3 attempts, 1s linear-backoff
  base) controls DLQ delivery independently from `WithRetry`.
- `WithDLQAttemptTimeout(timeout)` (default 10s) bounds each individual
  `Publish` attempt, including a publisher that ignores its context.
- `PoisonRecord` carries a stable idempotency key, stream name/ARN, consumer
  group, shard ID, sequence number, partition key, approximate arrival
  timestamp, a copied payload, the handler error text, handler kind (`record`
  or `batch`), attempt count, failure time, and batch index fields.
- For a failed batch, every record in the page is published as its own
  `PoisonRecord` with batch metadata (there is no per-record isolation in batch
  mode).
- Delivery is at-least-once, not atomic fan-out. If publishing record N fails,
  the successfully published prefix is not checkpointed as progress; a replay
  republishes it. Downstream systems must deduplicate on `IdempotencyKey`.

## Per-shard concurrency

- `WithShardConcurrency(n)` (default 1) runs up to `n` record-handler calls
  concurrently within one shard page. `n > 1` improves throughput but breaks
  strict per-shard ordering; keep `n == 1` when order matters. Applies to record
  handlers only. See [handler-behavior.md](handler-behavior.md).

## Graceful shutdown

- `WithGracefulDrain(timeout)` lets workers finish in-flight work, checkpoint,
  and release their leases before `Start` returns on cancellation (a zero
  timeout waits indefinitely). At a nonzero deadline, the consumer signals all
  workers to stop and returns `ErrDrainTimeout` without waiting again. Without
  graceful drain, cancellation likewise signals stop and returns promptly.
- Go cannot forcibly terminate a callback. A handler or DLQ publisher that
  ignores its canceled context may keep running after `Start` returns, but its
  late result is discarded before checkpointing and its worker releases the
  lease when the callback eventually returns. Consumer extensions must honor
  context promptly and be concurrency-safe; synchronous logging and metrics
  hooks, which have no context, must always return promptly.

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

All coordination state lives under consumer-group and canonical-stream
segments plus configurable key prefixes, so multiple applications and streams
can share one Valkey without colliding:

- Checkpoints: `<checkpointPrefix>:<group>:<stream>:<shard>` (default prefix
  `kinesis-checkpoint`; override with `WithKeyPrefix`).
- Lease owners and expirations: per-identity hash/sorted-set structures under
  `<leasePrefix>:v2:{<identity64>}:lease-*`, where `leasePrefix` defaults to
  `kinesis-lease` (a custom checkpoint prefix derives an adjacent lease prefix
  unless explicitly overridden).
- Worker heartbeat expirations:
  `<leasePrefix>:v2:{<identity64>}:workers`.

`<identity64>` is unpadded base64url of `<group>:<stream>`. Its Redis Cluster
hash tag keeps one identity's coordination keys in one slot. Atomic snapshot
scripts remove expired/inconsistent entries and read only the target identity;
they never perform a database-wide or cluster-node SCAN.

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
| `Config.StartPosition` | `LATEST` | Start position for parentless shards with no checkpoint (reshard children always use `TRIM_HORIZON`) |
| `WithBatching(batchSize, checkpointEvery)` | 100, 100 | GetRecords page size; checkpoint throttle |
| `WithPolling(pollInterval, shardSyncInterval)` | 1s, 1m | GetRecords poll cadence; shard resync cadence |
| `WithRetry(maxAttempts, backoff)` | 3, 1s | Handler retry attempts and linear base backoff |
| `WithShardConcurrency(n)` | 1 | Concurrent record-handler calls per shard |
| `WithFailurePolicy(policy)` | `fail-fast` | Post-retry poison handling |
| `WithDLQRetry(maxAttempts, backoff)` | 3, 1s | DLQ-only publish retry attempts and linear base backoff |
| `WithDLQAttemptTimeout(timeout)` | 10s | Per-attempt DLQ publish deadline |
| `WithHeartbeat(interval, ttl)` | 5s, 20s | Worker liveness cadence and lease/worker TTL; persistent heartbeat failure stops the consumer one interval before the TTL lapses |
| `WithShardSyncMaxStaleness(maxStaleness)` | 10x sync interval (10m) | How long a failing shard sync may go before the consumer stops |
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
- **KPL aggregated-record deaggregation — a correctness trap, read this if you
  use the KPL.** Records produced by the Kinesis Producer Library (KPL) with
  aggregation enabled are **not** deaggregated. The handler receives the raw
  Kinesis record — a single opaque protobuf blob that packs many user records —
  and never sees the individual records inside it. Worse, because checkpointing
  advances past every successfully processed page (see
  [Checkpointing](#checkpointing)), the consumer silently checkpoints past that
  data as if it had been delivered, so the packed records are effectively lost
  with no error. If your producers use KPL aggregation, either disable
  aggregation on the producer or deaggregate inside your handler before
  processing; do not rely on this library to unpack them.
- **Enhanced fan-out (`SubscribeToShard`):** not implemented. The consumer
  reads exclusively by polling `GetRecords`; there is no push-based HTTP/2
  subscription giving each consumer a dedicated per-shard throughput pipe.
- **Polling throughput ceiling:** because reads are polling-only, every
  consumer of a shard shares that shard's read budget — 2 MB/s and 5
  `GetRecords` calls/s — so co-located consumers on the same stream contend for
  it, and delivery latency is bounded below by the poll interval
  (`WithPolling`, default 1s). Enhanced fan-out's dedicated 2 MB/s-per-consumer
  pipe and lower push latency are not available. Tune `WithPolling` and
  `WithBatching` for your latency/throughput needs, and avoid piling many
  independent consumers onto one stream.
