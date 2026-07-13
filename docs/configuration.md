# Configuration Reference

Every configuration surface: the required `Config`, the consumer `With*` options,
and the Valkey backend options — with defaults, effects, and validation rules.

For a narrative overview see [features.md](features.md); to get running see
[getting-started.md](getting-started.md); for handler/DLQ/concurrency depth see
[handler-behavior.md](handler-behavior.md).

## `consumer.Config`

Passed by value as the first argument to `consumer.New`. Identifies the stream
and the initial read position.

| Field | Type | Default | Effect |
| --- | --- | --- | --- |
| `StreamName` | `string` | — | Stream to consume. Either this or `StreamARN` is required. |
| `StreamARN` | `string` | — | Stream ARN; an alternative to `StreamName`. All coordination keys use whichever is set. |
| `StartPosition` | `StartPosition` | `StartLatest` | Where to start when a shard has **no checkpoint** (see below). |
| `StartTimestamp` | `*time.Time` | `nil` | Required when `StartPosition == StartAtTimestamp`; ignored otherwise. |

Validation (`New` returns an error if violated): at least one of `StreamName` /
`StreamARN` must be set; `StartPosition` must be one of the three values below;
`StartAtTimestamp` requires a non-nil `StartTimestamp`.

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
| `WithBatching` | `batchSize int32, checkpointEvery int` | `100, 100` | `batchSize` bounds the GetRecords `Limit` (page size); a checkpoint is written at most every `checkpointEvery` processed records. | both `>= 1` |
| `WithPolling` | `pollInterval, shardSyncInterval time.Duration` | `1s, 1m` | `pollInterval` is the sleep between GetRecords polls when idle; `shardSyncInterval` is how often the shard list is refreshed (drives live reshard discovery). | `pollInterval > 0`; `shardSyncInterval >= 1s` |
| `WithRetry` | `maxAttempts int, backoff time.Duration` | `3, 1s` | Handler retry attempts and linear base backoff (`sleep = attempt * backoff`) before the failure policy applies. | `maxAttempts >= 1`; `backoff > 0` |
| `WithShardConcurrency` | `concurrency int` | `1` | Concurrent record-handler calls within one shard page. `> 1` improves throughput but breaks strict per-shard ordering. Record handlers only. | `>= 1` |
| `WithFailurePolicy` | `policy FailurePolicy` | `FailurePolicySkip` | What happens to a record/batch after retries are exhausted (see values below). | must be a valid policy |
| `WithDLQPublisher` | `publisher DLQPublisher` | none | Destination for poison records; **required** when the policy is `FailurePolicySendToDLQ`. | non-nil |
| `WithBatchHandler` | `handler BatchHandlerFunc` | none | Switches to one handler call per GetRecords page instead of per record. When set, the positional handler in `New` may be nil. | non-nil |
| `WithHeartbeat` | `interval, ttl time.Duration` | `5s, 20s` | Worker liveness heartbeat cadence and the lease/worker key TTL. | both `> 0` |
| `WithRebalance` | `minInterval, jitter, cooldown time.Duration, maxMoves int` | `10s, 10s, 10s, 2` | Rebalance timing (`minInterval + [0,jitter)` between ticks), per-shard cooldown after a move, and the max shard moves per tick. | `minInterval > 0`; `jitter >= 0`; `cooldown > 0`; `maxMoves >= 1` |
| `WithGracefulDrain` | `timeout time.Duration` | off | On ctx cancel, workers finish in-flight work, checkpoint, and release leases before `Start` returns. `0` waits indefinitely. | `timeout >= 0` |
| `WithLeaseManager` | `manager lease.Manager` | auto | Supplies an explicit lease manager. Usually unnecessary — a store implementing `lease.Provider` (the Valkey store) enables leasing automatically. | non-nil |
| `WithLogger` | `logger *slog.Logger` | discard (silent) | Structured logger for consumer lifecycle, lease, rebalance, and record-processing events; see [logging.md](logging.md) for the event catalog. | non-nil |

### Failure policy values

| Value | Behavior |
| --- | --- |
| `FailurePolicySkip` | Default. Treat the poison record/batch as handled and continue. |
| `FailurePolicyFailFast` | Stop the shard worker and surface the error through `Start`. |
| `FailurePolicySendToDLQ` | Publish to the configured `DLQPublisher` and continue (requires `WithDLQPublisher`; `New` errors without one). |

See [handler-behavior.md](handler-behavior.md) for the full failure-policy and
DLQ semantics, including `PoisonRecord` metadata.

## Valkey backend options

The Valkey checkpoint store doubles as the lease provider. Construct it with
`valkeycheckpoint.New(addr, opts...)`; passing the returned `*Store` to
`consumer.New` enables leasing automatically.

| Option | Parameters | Default | Effect |
| --- | --- | --- | --- |
| `WithKeyPrefix` | `prefix string` | `kinesis-checkpoint` | Prefix for checkpoint keys. Also the base for the derived lease prefix. |
| `WithLeasePrefix` | `prefix string` | `<checkpointPrefix>-lease` | Prefix for lease keys. Defaults to the checkpoint prefix + `-lease`. |
| `WithPingTimeout` | `timeout time.Duration` | `5s` | Timeout for the connectivity check performed in `New`. |
| `WithDB` | `db int` | `0` | Valkey database index (standalone only; not supported with cluster). |
| `WithTLS` | — | off | Connect over TLS. |
| `WithCluster` | — | off | Treat the endpoint as a Valkey cluster. |

If you build a standalone lease manager explicitly (for `WithLeaseManager`),
`valkeylease.NewManager(addr, opts...)` accepts the same connection options plus
`WithMaxLeases(n)` to bound how many leases one manager will hold.

## Key scheme

All coordination state is namespaced so multiple streams/tenants can share one
Valkey:

| Key | Format | Default prefix |
| --- | --- | --- |
| Checkpoint | `<checkpointPrefix>:<stream>:<shard>` | `kinesis-checkpoint` |
| Lease | `<leasePrefix>:<stream>:<shard>` | `<checkpointPrefix>-lease` |
| Worker heartbeat | `<leasePrefix>-worker:<stream>:<owner>` | `<checkpointPrefix>-lease-worker` |

`<stream>` is the `StreamName` or `StreamARN`, whichever the `Config` sets.
