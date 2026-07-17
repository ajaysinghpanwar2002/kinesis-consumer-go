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

c, err := consumer.New(cfg, client, store, handler,
    consumer.WithLogger(logger),
)
```

`WithLogger(nil)` is rejected by `New` with a validation error. The logger is
shared by the consumer's heartbeat, rebalance, and shard-worker goroutines, so
the handler you supply must be safe for concurrent use (all `slog` handlers in
the standard library are). It must also return promptly: log calls are
synchronous and carry no cancellation context, so a blocking handler can delay
processing or shutdown.

`New` derives the consumer logger with two bounded identity attributes that are
present on every event: canonical `stream` and configured `consumer_group`.
When `StreamARN` is used, `stream` is the ARN's stream-name resource rather than
the full ARN. Attribute lists in the catalog below name fields added by the
individual event; the two identity attributes are implicit everywhere.

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
| `consumer starting` | Info | — | `Start(ctx)` begins. |
| `consumer stopped` | Info | — | `Start` returns nil (clean cancellation). |
| `consumer stopped` | Error | `error` | `Start` returns an error; this is the terminal event. |

### Shard workers and leases

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `shard worker started` | Info | `shard` | A registered shard-worker goroutine begins processing. |
| `shard worker stopped` | Info | `shard` | The worker returned cleanly (no `error` attribute). |
| `shard worker stopped` | Warn | `shard`, `error` | The worker returned an error: a processing failure, a lease-renewal failure, or — when nothing else failed first — a lease-release failure. |
| `shard lease acquired` | Debug | `shard`, `owner` | Initial/refresh acquisition succeeded (`acquireShardLeases`). |
| `shard lease released` | Debug | `shard` | The bounded release at worker exit succeeded. |
| `shard lease lost; stopping worker` | Info | `shard` | A lease renew returned `ErrNotOwned` because a peer claimed the shard. The affected worker stops cleanly (the peer resumes from the last checkpoint), no release is attempted, and the consumer plus unrelated shard workers remain live. |
| `shard lease release failed` | Warn | `shard`, `error` | The bounded release failed or timed out. Logged here because the caller discards the release error when the worker already failed, so this Warn can be the only record of it. |
| `shard lease renew failed; will retry` | Warn | `shard`, `since_last_renew`, `ttl`, `error` | A transient renew failure. The loop retries on subsequent heartbeat ticks while the TTL budget since the last successful renew lasts. `ErrNotOwned` and budget exhaustion are not retried; `ErrNotOwned` becomes the clean shard-local completion logged by the Info event above, while budget exhaustion surfaces through the worker-stop Warn. |
| `shard lease validity expired; stopping worker` | Warn | `shard`, `since_last_renew`, `ttl` | The local lease-validity watchdog fired: no successful renew within the TTL on the local clock and no error from the renew loop either — the backend `Renew` call is presumed hung. The worker is stopped (fenced), because the backend lease has lapsed and a peer may already own the shard; continuing would risk dual processing. Surfaces through the worker-stop Warn like other terminal renew failures. |
| `worker heartbeat failed` | Warn | `owner`, `consecutive_failures`, `staleness`, `error` | A worker-liveness heartbeat send failed (live context). Failures are survivable while the last successful indexed heartbeat entry is live; `staleness` is the time since that success. Once failures persist to one heartbeat interval before that entry expires — the point after which peers may treat this worker as dead and claim its shards away — the consumer stops with `ErrHeartbeatStale` instead of dual-processing, surfacing through the terminal `consumer stopped` Error. |

`ErrNotOwned` is a shard-local ownership transition in every mode: it logs the
Info event above and the worker stops cleanly without failing `Start`.
TTL-budget exhaustion remains a terminal renewal failure aggregated by the
worker-stop Warn. Retried transient failures log in place because they would
otherwise be invisible, and lease-validity watchdog expiry logs in place
because a hung `Renew` produces no error to carry the diagnosis.

### Rebalancing

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `rebalance plan` | Debug | `shards`, `workers`, `low`, `high`, `owned`, `actions` | A rebalance pass produced at least one planned action. `low`/`high` are the fair-share bounds; `owned` is this consumer's count entering the pass. |
| `rebalance shard acquired` | Info | `shard` | An acquire-unowned action obtained the lease. |
| `rebalance acquire skipped` | Debug | `shard` | The acquire found the lease already held (lost the race; not an error). |
| `rebalance shard claimed` | Info | `shard`, `donor` | A claim action took the shard from an over-fair-share donor. |
| `rebalance claim skipped` | Debug | `shard`, `donor` | The claim found the donor no longer owns the lease (not an error). |
| `rebalance shard shed` | Info | `shard`, `owned`, `high` | This consumer stopped a worker to shed ownership above the fair-share `high` bound. |
| `rebalance pass failed` | Warn | `error` | A rebalance pass returned an error (live context) and was skipped; the next tick retries. Shard-sync failures log separately (below) because they carry the staleness bound. |
| `shard sync failed` | Warn | `error`, `consecutive_failures`, `staleness`, `retry_delay` | A periodic shard-sync pass failed (live context). Existing workers keep delivering; discovery retries after `retry_delay` (capped exponential backoff + jitter, never later than the sync interval). `staleness` is the time since the last successful sync — once it exceeds `WithShardSyncMaxStaleness`, the consumer stops with `ErrShardSyncStale` instead of logging this Warn. Fatal authorization/configuration errors stop the consumer immediately without this Warn. |

Quiet passes — a balanced snapshot with no actions and no shedding — log
nothing, and per-candidate cooldown skips are not logged; cooldown influence
is visible through the move events plus the configured cooldown period.

### Record processing and checkpointing

| Message | Level | Attributes | When |
| --- | --- | --- | --- |
| `get records failed; backing off` | Warn | `shard`, `consecutive_failures`, `backoff`, `error` | A retryable GetRecords failure (throttling, 5xx, network) — the pass backs off in-place (500ms doubling to a 10s cap) and retries the same iterator instead of stopping the consumer. |
| `records skipped after handler failure` | Warn | `shard`, `handler`, `records`, `attempts`, `error` | Explicit `skip` policy intentionally dropped a failed record (record mode) or whole page (batch mode) after retries were exhausted. The page can then be checkpointed past the dropped data. This Warn is emitted only when `WithLogger` enables logging; the default logger discards it. |
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
terminal `consumer stopped` Error repeats the cause. The exceptions are the
events whose error the caller swallows or retries away, so the site-level
Warn is the only record: `shard lease release failed`,
`shard lease renew failed; will retry`, `get records failed; backing off`,
`worker heartbeat failed`, `rebalance pass failed`, and `shard sync failed`
(until their staleness bounds turn the heartbeat and shard-sync Warns into
terminal errors).

- **Checkpoint save failures, DLQ publish failures, and fail-fast handler
  errors** have no dedicated event; they surface through the worker and
  consumer stop events above.
- **Per-attempt handler retries** and **expired-iterator recovery** are
  silent.
- **Shard-sync (shard discovery) errors** warn in place (`shard sync failed`)
  and retry with backoff; only fatal authorization/configuration errors or
  exceeding the staleness bound stop the consumer, surfacing through the
  terminal `consumer stopped` Error. Rebalance-pass failures likewise warn in
  place (`rebalance pass failed`) and retry at the next tick.

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
  - `get records failed; backing off` — sustained occurrences mean the
    consumer is throttled or Kinesis is degraded; check `WithBatching`,
    `WithIdleTimeBetweenReads`, and competing consumers on the stream.
  - `shard lease renew failed; will retry` — sustained occurrences mean the
    lease backend is flaky or overloaded; workers stop (and shards fail over)
    once the TTL budget is exhausted.
  - `shard lease validity expired; stopping worker` — a lease-backend call
    hung past the TTL; the worker was fenced to prevent dual processing.
    Check backend connectivity/timeouts.
  - `worker heartbeat failed` — sustained occurrences mean worker liveness is
    degraded: peers still see the last successful heartbeat, but the consumer
    stops with `ErrHeartbeatStale` one heartbeat interval before that
    heartbeat's TTL lapses (before peers can claim its shards away). The
    `staleness` attribute (also exposed via `Consumer.Health`) shows how
    close it is.
  - `rebalance pass failed` — sustained occurrences mean shard distribution
    has stopped converging.
  - `shard sync failed` — sustained occurrences mean shard discovery is
    degraded: existing shards keep flowing, but resharding is invisible and
    the consumer stops with `ErrShardSyncStale` once the
    `WithShardSyncMaxStaleness` bound is exceeded. The `staleness` attribute
    (also exposed via `Consumer.Health`) shows how close it is.
- All events carry machine-parseable attributes (`shard`, `stream`, `owner`,
  `donor`, counts, `error`), so JSON handlers can index them directly.
