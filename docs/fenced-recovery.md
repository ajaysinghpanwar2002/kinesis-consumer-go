# Fenced consumer recovery

The consumer binds a fenced checkpoint session to each shard it owns, derives
its shard iterator from persisted recovery state, and validates ownership
atomically with every progress write. This page describes what that changes for
a running consumer. For the storage contracts themselves see
[fenced memory](fenced-memory.md) and [fenced Valkey](fenced-valkey.md).

## When fencing engages

A shard worker binds a session when its checkpoint store implements
`checkpoint.FencedStore` and its shard lease implements `lease.FencedLease`.
Both built-in pairs qualify:

```go
manager := lease.NewMemoryManager()
store := checkpoint.NewMemoryStoreWithLeaseManager(manager)
cons, err := consumer.New(cfg, client, store, handler, consumer.WithLeaseManager(manager))
```

The Valkey backend qualifies the same way — pass its `LeaseManager()` provider,
or let the consumer create the manager from the store.

Three combinations keep the original unfenced behavior instead:

- a store that does not implement `checkpoint.FencedStore`;
- a lease manager whose leases are not `lease.FencedLease`;
- a fenced-capable store bound to a lease from another backend or namespace,
  which `Bind` reports as `lease.ErrLeaseMismatch`. `checkpoint.NewMemoryStore`
  has no lease manager and always lands here.

Custom stores and lease managers written against the original interfaces
therefore keep working unchanged. Mixing halves of two backends is a
configuration mistake rather than an error, because it is exactly what an
unfenced deployment already looks like; the consumer logs a warning naming the
shard and carries on. Pair the two halves — or use the store's `LeaseManager()`
provider — to get fencing.

Any other binding failure stops the shard. A fenced backend whose recovery state
cannot be read is never downgraded to the unfenced path, because that path would
re-anchor the shard at the configured start position and skip records.

## Where a shard resumes

An unfenced worker reads the stored checkpoint and starts after it, or falls
back to the configured start position when there is none. A fenced worker asks
its session instead, which distinguishes "nothing recorded yet" from "recorded
but never checkpointed":

| Recovery state | Resume point |
| --- | --- |
| Fresh | The configured start position (`TRIM_HORIZON` for a reshard child). |
| Initial | The recorded sequence, **inclusive** — the record itself is replayed. |
| Checkpoint | Strictly after the recorded sequence. |
| Completed | The shard is finished; the worker stops and children unblock. |

The inclusive case is the reason for the extra state: the first record a shard
ever yields is recorded before it is handled, so a crash between the two must
replay it rather than skip it.

## First-record protection and its boundary

Before any record of a shard's first page is handled, the consumer persists that
page's first sequence number as an inclusive replay position. The write is
once-only: a position recorded earlier by a predecessor or an earlier attempt
wins, and the worker restarts from that position instead of the page it just
fetched.

Protection begins with that write, not with the worker. A crash between
anchoring at `LATEST` and persisting the first fetched sequence loses the
observation window — but no record can have been handled during it, so nothing
that was delivered is lost. This is the documented startup boundary; it is why
`TRIM_HORIZON` remains the safer start position for a stream that must not skip
its earliest records.

## Anchor verification

Whenever a fenced worker resumes from a recorded sequence — on startup and when
refreshing an expired iterator — it first proves that sequence is still the
record the shard yields at that position. It reads inclusively, at the anchor,
and requires the first returned record to match.

- A later first record means the anchor was trimmed. The shard halts;
  the consumer never substitutes whatever is available now, because doing so
  would silently skip everything in between.
- A missing stream or shard, or a sequence outside the shard, halts the shard.
- Empty pages prove nothing on their own — Kinesis can need several reads to
  reach records — so verification keeps reading, paced against the shard's read
  limit.
- Transient failures (throttling, server faults, network errors) are retried.
- Verification is bounded to 30 seconds and to the caller's context. An
  exhausted budget halts the shard; a cancelled context is a shutdown, not a
  recovery failure.

That read is also the read the shard continues from. The page which proved the
anchor is handled directly: an initial replay keeps the anchor, an exclusive
checkpoint continuation drops it, and the rest of the page is delivered either
way. Deriving a second iterator instead would leave a window in which retention
trims the anchor between the proof and the delivery, and the new iterator would
then start at a later record with nothing left to notice.

An exclusive continuation can empty that page: with a batch size of one it
always does, because the page holds the anchor and nothing else. Such a page is
empty because of what was removed from it, not because the shard has run out of
records, so the shard reads on from it immediately instead of treating it as the
tip and waiting out a poll interval.

Verification costs one `GetShardIterator` and at least one `GetRecords` per
resumption — the same calls an unverified resumption would make, because the
verifying read is not thrown away. Extra reads happen only when Kinesis answers
with empty pages. It runs when a worker picks up a shard, not per page.

## Expired iterators

Within one worker generation an expired iterator is refreshed from the last
sequence that worker fetched, so nothing between the last checkpoint and the
expiry is replayed. A worker that has not fetched anything yet, and any
successor, recovers from persisted state instead.

## Transient backend failures

Binding a session, reading recovery state, and persisting the initial replay
position retry transient backend failures under the consumer's existing retry
settings (`WithRetry`), like every other checkpoint read and write. The built-in
store clients report network errors rather than absorbing them, and startup
recovery is a path whose errors are deliberately fatal, so one dropped
connection must not end the run. All three operations are safe to repeat: the
initial write is once-only and returns the position already recorded.

Lost ownership, unusable recovery state, and a mismatched fenced pair are not
retried. They are answers, not blips.

Those retries run under a lease that is already being renewed: a shard worker
starts its renewal loop before it binds, so a backoff cannot leave an acquired
lease unrenewed long enough to expire. Renewal also cancels the worker when
ownership moves, so a bind in progress stops rather than completing against a
lease that is gone.

## Ownership and failures

Every fenced read and write validates owner, generation, and backend expiry
atomically with the operation, so a worker whose lease has moved on cannot
persist progress. Losing the lease that way is a handoff, not a failure: the
worker stops cleanly, the peer resumes from the last persisted position, and the
consumer keeps running. The session is invalidated permanently when the worker
exits, so a callback that ignored its cancellation cannot land a late write.

Unusable recovery state — missing or inconsistent metadata, an invalid recovery
value, an unverifiable anchor, or progress observed to regress — returns
`checkpoint.ErrRecoveryState`. Match it with `errors.Is`. The consumer halts
rather than reinitializing, counts `kinesis_consumer.recovery_failures`, logs
the shard, and surfaces the error from `Start`. Investigate the backend before
restarting: the safe reset is to stop consumers and clear a disposable
namespace, never to let the consumer re-anchor itself.
