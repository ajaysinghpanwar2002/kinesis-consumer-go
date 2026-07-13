# Logging

The consumer emits structured logs through the standard library's `log/slog`.
Logging is **silent by default**: without `WithLogger` the consumer uses a
non-nil discard logger, so the library writes nothing and call sites never
nil-check. Every message, level, and attribute on this page is verified
against the log sites in `pkg/consumer`.

## Enabling logging

Pass any `*slog.Logger` to `consumer.New`:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

c, err := consumer.New(cfg, handler,
    consumer.WithLogger(logger),
)
```

`WithLogger(nil)` is rejected by `New` with a validation error. The logger is
shared by the consumer's heartbeat, rebalance, and shard-worker goroutines, so
the handler you supply must be safe for concurrent use (all `slog` handlers in
the standard library are).

## Level philosophy

| Level | Meaning here |
| --- | --- |
| Error | Reserved for one event: the consumer stopped because `Start` is returning an error. |
| Warn | Data-affecting or otherwise-invisible failures: dropped/DLQ'd records, a failed worker, a lease release failure that the error path would swallow. |
| Info | Rare lifecycle and ownership transitions: consumer and worker start/stop, rebalance moves, shard completion. |
| Debug | Steady-state mechanics: lease acquire/release, checkpoint saves, rebalance plans. |

A healthy consumer at Info level logs a few consumer-lifecycle lines plus one
`shard worker started`/`stopped` pair per owned shard, an occasional rebalance
move when workers join or leave, and a `shard completed` line per reshard —
nothing per record, per poll, or per rebalance tick.

## Event catalog

### Consumer lifecycle

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `consumer starting` | Info | `stream` | `Start(ctx)` begins. |
| `consumer stopped` | Info | `stream` | `Start` returns nil (clean cancellation). |
| `consumer stopped` | Error | `stream`, `error` | `Start` returns an error; this is the terminal event. |

### Shard workers and leases

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `shard worker started` | Info | `shard` | A registered shard-worker goroutine begins processing. |
| `shard worker stopped` | Info | `shard` | The worker returned cleanly (no `error` attribute). |
| `shard worker stopped` | Warn | `shard`, `error` | The worker returned an error: a processing failure, a lease-renewal failure, or — when nothing else failed first — a lease-release failure. |
| `shard lease acquired` | Debug | `shard`, `owner` | Initial/refresh acquisition succeeded (`acquireShardLeases`). |
| `shard lease released` | Debug | `shard` | The bounded release at worker exit succeeded. |
| `shard lease release failed` | Warn | `shard`, `error` | The bounded release failed or timed out. Logged here because the caller discards the release error when the worker already failed, so this Warn can be the only record of it. |

Lease renewal failures are not logged separately: `runShardWorker` returns
them, and the worker-stop Warn is the aggregation point.

### Rebalancing

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `rebalance plan` | Debug | `shards`, `workers`, `low`, `high`, `owned`, `actions` | A rebalance pass produced at least one planned action. `low`/`high` are the fair-share bounds; `owned` is this consumer's count entering the pass. |
| `rebalance shard acquired` | Info | `shard` | An acquire-unowned action obtained the lease. |
| `rebalance acquire skipped` | Debug | `shard` | The acquire found the lease already held (lost the race; not an error). |
| `rebalance shard claimed` | Info | `shard`, `donor` | A claim action took the shard from an over-fair-share donor. |
| `rebalance claim skipped` | Debug | `shard`, `donor` | The claim found the donor no longer owns the lease (not an error). |
| `rebalance shard shed` | Info | `shard`, `owned`, `high` | This consumer stopped a worker to shed ownership above the fair-share `high` bound. |

Quiet passes — a balanced snapshot with no actions and no shedding — log
nothing, and per-candidate cooldown skips are not logged; cooldown influence
is visible through the move events plus the configured cooldown period.

### Record processing and checkpointing

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `records skipped after handler failure` | Warn | `shard`, `handler`, `records`, `attempts`, `error` | The `skip` failure policy dropped a failed record/page after retries were exhausted. This is the only trace of dropped data. |
| `poison records published to dlq` | Warn | `shard`, `handler`, `records`, `attempts`, `error` | The `send-to-dlq` policy published every poison record in the failed group. |
| `shard checkpoint saved` | Debug | `shard`, `sequence` | Any successful checkpoint save: periodic (`checkpointEvery`), caught-up flush, or drain. |
| `shard drain checkpoint flushed` | Debug | `shard`, `sequence`, `records` | The graceful-drain flush persisted `records` pending records at shutdown. |
| `shard completed` | Info | `shard`, `checkpoint` | The `SHARD_END` completion checkpoint was persisted for a closed shard (reshard). |

The failure-policy Warns fire once per failed group — a single record for the
record handler, the whole page for a batch handler — and carry the final
`attempts` count, so they double as the retry-exhaustion signal. Individual
retry attempts are not logged.

## What is deliberately not logged

Operation-specific failures are not logged at the site where they occur; they
propagate as errors and are reported at the lifecycle boundaries they cross.
A single failure can therefore appear at more than one boundary — a failing
worker logs `shard worker stopped` at Warn and, if it stops the consumer, the
terminal `consumer stopped` Error repeats the cause. The exception is
`shard lease release failed`, which is logged at its site because the caller
can swallow the release error entirely.

- **Checkpoint save failures, DLQ publish failures, and fail-fast handler
  errors** have no dedicated event; they surface through the worker and
  consumer stop events above.
- **Per-attempt handler retries** and **expired-iterator recovery** are
  silent.
- **Rebalance and snapshot read errors** keep their existing return paths.

## Production guidance

- Run at **Info**. Steady state is near-silent; every line at Info marks a
  real lifecycle or ownership change.
- Turn on **Debug** when diagnosing lease churn, checkpoint cadence, replay
  after failover, or rebalance convergence — Debug volume scales with
  activity (per lease/checkpoint/plan), not per record.
- Alert on **Error** (consumer terminated) unconditionally.
- Consider alerting on these Warns:
  - `records skipped after handler failure` — data is being dropped.
  - `poison records published to dlq` — poison inflow; watch the DLQ.
  - `shard worker stopped` (Warn form) and `shard lease release failed` —
    repeated occurrences indicate a failing backend or handler.
- All events carry machine-parseable attributes (`shard`, `stream`, `owner`,
  `donor`, counts, `error`), so JSON handlers can index them directly.
