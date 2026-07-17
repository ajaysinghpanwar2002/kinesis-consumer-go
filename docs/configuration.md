# Configuration Reference

Every configuration surface: the required `Config`, the consumer `With*` options,
and the Valkey backend options — with defaults, effects, and validation rules.

For a narrative overview see [features.md](features.md); to get running see
[getting-started.md](getting-started.md); for handler/DLQ/concurrency depth see
[handler-behavior.md](handler-behavior.md). Observability references are in
[logging.md](logging.md) and [metrics.md](metrics.md).

## `consumer.Config`

Passed by value as the first argument to `consumer.New`. Identifies the stream,
the logical consumer group, and the initial read position.

| Field | Type | Default | Effect |
| --- | --- | --- | --- |
| `StreamName` | `string` | — | Stream to consume by name. Set exactly one of this and `StreamARN`. |
| `StreamARN` | `string` | — | Stream to consume by ARN. Set exactly one of this and `StreamName`; AWS calls use the ARN while coordination and telemetry use its canonical stream-name resource. |
| `ConsumerGroup` | `string` | — | Required logical application identity. Workers with the same group share checkpoints and shard ownership; different groups consume the same stream independently. |
| `StartPosition` | `StartPosition` | `StartLatest` | Where to start when a shard has **no checkpoint** (see below). |
| `StartTimestamp` | `*time.Time` | `nil` | Required when `StartPosition == StartAtTimestamp`; ignored otherwise. |

Validation (`New` returns an error if violated): exactly one of `StreamName` /
`StreamARN` must be set; an ARN must be a complete Kinesis `stream/<name>` ARN
with partition, region, and 12-digit account ID;
`ConsumerGroup` and the canonical stream name must be 1–128 characters using
only letters, numbers, `.`, `_`, and `-`; `StartPosition` must be one of the
three values below; `StartAtTimestamp` requires a non-nil `StartTimestamp`.

### Start positions

| Value | Meaning |
| --- | --- |
| `StartLatest` | Default. Only records produced after the consumer positions on the shard. |
| `StartTrimHorizon` | From the oldest retained record in the shard. |
| `StartAtTimestamp` | From `StartTimestamp` (required). |

**Resume overrides start position.** When a checkpoint already exists for a
shard, the consumer resumes strictly *after* the checkpointed sequence number
(internally an `AFTER_SEQUENCE_NUMBER` iterator) regardless of `StartPosition`.
`StartPosition` only governs the first run of a shard with no stored progress.

## Consumer options (`consumer.With*`)

Passed as the trailing variadic arguments to `consumer.New`. Every option has a
working default, so `New` with no options is valid.

| Option | Parameters | Default | Effect | Constraints |
| --- | --- | --- | --- | --- |
| `WithBatching` | `batchSize int32, checkpointEvery int` | `100, 100` | `batchSize` bounds the GetRecords `Limit` (page size); a checkpoint is written at most every `checkpointEvery` processed records. | `batchSize` in `[1, 10000]`; `checkpointEvery >= 1` |
| `WithPolling` | `pollInterval, shardSyncInterval time.Duration` | `1s, 1m` | `pollInterval` is the sleep between GetRecords polls when idle; `shardSyncInterval` is how often the shard list is refreshed (drives live reshard discovery). | `pollInterval > 0`; `shardSyncInterval >= 1s` |
| `WithIdleTimeBetweenReads` | `d time.Duration` | `200ms` | Minimum delay between successive GetRecords calls on one shard (KCL `idleTimeBetweenReadsInMillis` equivalent) — paces catch-up reads under the Kinesis 5 reads/sec/shard limit. `0` disables pacing. Retryable read errors (throttling, 5xx, network) additionally back off in-place (500ms doubling to a 10s cap) instead of stopping the consumer. | `>= 0` |
| `WithRetry` | `maxAttempts int, backoff time.Duration` | `3, 1s` | Handler retry attempts and linear base backoff (`sleep = attempt * backoff`) before the failure policy applies. | `maxAttempts >= 1`; `backoff > 0` |
| `WithShardConcurrency` | `concurrency int` | `1` | Concurrent record-handler calls within one shard page. `> 1` improves throughput but breaks strict per-shard ordering. Record handlers only. | `>= 1` |
| `WithFailurePolicy` | `policy FailurePolicy` | `FailurePolicyFailFast` | What happens to a record/batch after retries are exhausted (see values below). | must be a valid policy |
| `WithDLQPublisher` | `publisher DLQPublisher` | none | Destination for poison records; **required** when the policy is `FailurePolicySendToDLQ`. | non-nil |
| `WithDLQRetry` | `maxAttempts int, backoff time.Duration` | `3, 1s` | DLQ publish attempts and linear base backoff (`sleep = attempt * backoff`). Independent from handler and coordination retries configured through `WithRetry`. | `maxAttempts >= 1`; `backoff > 0` |
| `WithDLQAttemptTimeout` | `timeout time.Duration` | `10s` | Deadline applied separately to every `DLQPublisher.Publish` attempt. A context-ignoring call is abandoned at the deadline and its late result cannot enable checkpointing. | `timeout > 0` |
| `WithBatchHandler` | `handler BatchHandlerFunc` | none | Switches to one handler call per GetRecords page instead of per record. Mutually exclusive with the positional record handler in `New`: pass one or the other — the positional handler must be nil when this is set, and `New` errors if both are provided. | non-nil |
| `WithHeartbeat` | `interval, ttl time.Duration` | `5s, 20s` | Worker liveness heartbeat cadence and the lease/worker key TTL. Failed heartbeat sends are survivable while the worker key from the last successful send is live; once failures persist to `ttl - interval` past that success, `Start` returns the causal error wrapped in `ErrHeartbeatStale` — one full interval before peers can observe the key as expired and claim this worker's shards away. Heartbeat health (consecutive failures, last success, last error) is exposed via `Consumer.Health()`. | both whole-millisecond values `>= 1ms`; `interval < ttl`; `ttl + interval` must fit in `time.Duration`. Otherwise Valkey would truncate the configured TTL, or the lease watchdog deadline could overflow. Recommended: `ttl >= 3x interval` (the default is 4x) so a lease survives transient renew hiccups. |
| `WithRebalance` | `minInterval, jitter, cooldown time.Duration, maxMoves int` | `10s, 10s, 10s, 2` | Rebalance timing (`minInterval + [0,jitter)` between ticks), per-shard cooldown after a move, and the max shard moves per tick. | `minInterval > 0`; `jitter >= 0`; `cooldown > 0`; `maxMoves >= 1` |
| `WithShardSyncMaxStaleness` | `maxStaleness time.Duration` | `10x shardSyncInterval` (10m) | Bounds how long the consumer runs on a stale shard map. Failed shard-sync passes are survivable (existing workers keep delivering; discovery retries with capped backoff + jitter), but once the time since the last successful sync exceeds this bound, `Start` returns the causal error wrapped in `ErrShardSyncStale`. Fatal Kinesis authorization/configuration errors stop the consumer immediately regardless. Sync health (consecutive failures, last success, last error) is exposed via `Consumer.Health()`. | `> 0`; `>= shardSyncInterval` |
| `WithGracefulDrain` | `timeout time.Duration` | off | On ctx cancel, workers finish in-flight work, checkpoint, and release leases before `Start` returns. `0` waits indefinitely. At a nonzero deadline, remaining workers are signaled to stop and `Start` returns `ErrDrainTimeout` without joining an uncooperative callback. Drains longer than `heartbeatTTL` are safe: the worker keeps heartbeating for the whole drain, and a shard a peer claims mid-drain counts as drained (not an error). One worker's failure lets the others finish before the deadline or error is returned. | `timeout >= 0` |
| `WithLeaseManager` | `manager lease.Manager` | auto | Supplies an explicit lease manager. Usually unnecessary — a store implementing `lease.Provider` (the Valkey store) enables leasing automatically. | non-nil |
| `WithLogger` | `logger *slog.Logger` | discard (silent) | Structured logger for consumer lifecycle, lease, rebalance, and record-processing events; see [logging.md](logging.md) for the event catalog. | non-nil |
| `WithMetrics` | `reporter metrics.Reporter` | `metrics.Nop{}` (silent) | Emits the consumer's counters, gauges, and timings through the supplied reporter; see [metrics.md](metrics.md) for the catalog and packaged statsd pipeline. | non-nil |

### Failure policy values

| Value | Behavior |
| --- | --- |
| `FailurePolicyFailFast` | Default. Stop the shard worker, surface the error through `Start`, and do not checkpoint the failed page. |
| `FailurePolicySkip` | Explicit, intentionally lossy opt-in. Record mode drops the failed record; batch mode drops the entire failed GetRecords page. Processing continues and the page can be checkpointed, so the dropped data is not replayed on resume. |
| `FailurePolicySendToDLQ` | Publish to the configured `DLQPublisher` and continue (requires `WithDLQPublisher`; `New` errors without one). |

See [handler-behavior.md](handler-behavior.md) for the full failure-policy and
DLQ semantics, including `PoisonRecord` metadata.

### Extension lifecycle contract

Handlers, batch handlers, DLQ publishers, Kinesis wrappers, checkpoint stores,
lease managers, and leases can be called concurrently and must honor every
supplied context promptly. A callback that ignores cancellation cannot be
forcibly terminated. The consumer abandons a DLQ call at its attempt deadline,
and shutdown can similarly abandon handlers; those extension goroutines may
finish later, but their late results are discarded before checkpointing. A DLQ
retry can overlap an abandoned attempt of the same `PoisonRecord.IdempotencyKey`,
so publishers must be concurrency-safe and downstream delivery must deduplicate.

Custom `slog.Handler` and `metrics.Reporter` implementations must also be
concurrency-safe and return promptly. Those synchronous observability APIs do
not receive a context, so a blocking implementation can delay processing or
shutdown.

## Valkey backend options

The Valkey checkpoint store doubles as the lease provider. Construct it with
`valkeycheckpoint.New(addr, opts...)`; passing the returned `*Store` to
`consumer.New` enables leasing automatically.

| Option | Parameters | Default | Effect |
| --- | --- | --- | --- |
| `WithKeyPrefix` | `prefix string` | `kinesis-checkpoint` | Prefix for checkpoint keys. Also the base for the derived lease prefix when customized. |
| `WithLeasePrefix` | `prefix string` | `kinesis-lease` (default checkpoint prefix) / `<checkpointPrefix>-lease` (custom) | Prefix for lease keys. The default checkpoint prefix maps to the shared standalone default so both lease-manager construction paths coordinate in one namespace; a custom checkpoint prefix derives `<prefix>-lease`. |
| `WithPingTimeout` | `timeout time.Duration` | `5s` | Timeout for the connectivity check performed in `New`. |
| `WithDB` | `db int` | `0` | Valkey database index (standalone only; not supported with cluster). |
| `WithTLS` | — | off | Connect over TLS with default settings (system CA roots). |
| `WithTLSConfig` | `tlsConfig *tls.Config` | — | Connect over TLS with the caller's configuration (custom `RootCAs`, `ServerName`, client certificates, …). The config is cloned immediately, so later caller mutation does not affect the store. Nil is rejected. |
| `WithAuth` | `username, password string` | no auth | Authenticate with static credentials. The password is required; an empty username authenticates the default user (password-only deployments). Mutually exclusive with `WithCredentialsProvider`. |
| `WithCredentialsProvider` | `fn CredentialsFn` | no auth | Authenticate with dynamically supplied credentials. `fn` is invoked on each connection attempt (initial dial and every reconnect), so rotated credentials are picked up without rebuilding the store. It must be safe for concurrent use; nil is rejected. Mutually exclusive with `WithAuth`. |
| `WithCluster` | — | off | Treat the endpoint as a Valkey cluster. |

Secrets are never logged: the library does not log connection configs, and
formatting one explicitly (`%v`, `%+v`, `%#v`) redacts the password and
reduces the credentials provider to a presence marker.

If you build a standalone lease manager explicitly (for `WithLeaseManager`),
`valkeylease.NewManager(addr, opts...)` accepts the same connection options plus
`WithMaxLeases(n)` to bound how many leases one manager will hold. A store
constructed with auth and TLS options propagates them to the lease manager it
provides, so the auto-created manager is secured identically.

## Key scheme

All coordination state is namespaced by consumer group and canonical stream
name, so unrelated applications can consume the same stream through one
Valkey without sharing progress or ownership:

| Key | Format | Default prefix |
| --- | --- | --- |
| Checkpoint | `<checkpointPrefix>:<group>:<stream>:<shard>` | `kinesis-checkpoint` |
| Lease | `<leasePrefix>:<group>:<stream>:<shard>` | `kinesis-lease` |
| Worker heartbeat | `<leasePrefix>-worker:<group>:<stream>:<owner>` | `kinesis-lease-worker` |

`<group>` is `ConsumerGroup`. `<stream>` is the canonical Kinesis stream name:
`StreamName` directly, or the name extracted from the `stream/<name>` resource
of `StreamARN`. Name-only and ARN-only configurations for the same group and
stream therefore share one coordination namespace.

The default lease prefix is the same whether the manager is store-provided or
built standalone with `valkeylease.NewManager`, so default-configured workers
always coordinate in one namespace. If you customize the checkpoint prefix,
the store derives `<checkpointPrefix>-lease` — any standalone manager in that
deployment must be given the matching prefix explicitly, or its workers will
lease in a separate namespace and process every shard twice.

### Migration note: consumer-group key scheme

The consumer-group segment is a pre-v1 breaking key-format change. There is no
legacy dual-read or automatic checkpoint migration because no published tag
used the old scheme. Old checkpoints are not visible under the new keys, and
old and new workers do not coordinate. Stop every old worker before upgrading,
then restart all workers with the same intended `ConsumerGroup`; do not perform
a rolling mixed-version deployment. Copy or rename checkpoint keys out of band
before restart only when retaining the old progress is required.

### Historical migration note: default lease prefix change

Store-provided lease managers with default prefixes previously wrote lease
and worker-heartbeat keys under `kinesis-checkpoint-lease(-worker)`; they now
use `kinesis-lease(-worker)`, matching the standalone default. When upgrading
a deployment that used the old default:

- Checkpoints are unaffected; lease and heartbeat keys are ephemeral TTL
  state that simply regrows under the new prefix.
- Do **not** roll the upgrade gradually: old and new workers would
  coordinate in different namespaces and dual-process every shard until the
  last old worker stops. Stop all workers, upgrade, then restart.
- To defer the migration, pin the old namespace explicitly with
  `WithLeasePrefix("kinesis-checkpoint-lease")`.
