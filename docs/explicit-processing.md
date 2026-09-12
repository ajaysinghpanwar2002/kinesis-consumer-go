# Explicit acknowledgment

Explicit mode replaces the constructor handler with one that receives a
`Delivery` instead of a `Record`. Returning from the handler does not
acknowledge anything, so an application that writes downstream asynchronously —
batching, buffering, or handing work to its own pool — can keep the checkpoint
tied to what is actually durable rather than to what was merely delivered.

Automatic record and batch handlers are unchanged and remain the default.

```go
cons, err := consumer.New(cfg, kinesisClient, store, nil,
    consumer.WithExplicitHandler(func(ctx context.Context, d consumer.Delivery) error {
        return queue.Submit(ctx, d)
    }),
    consumer.WithCheckpointInterval(time.Second),
    consumer.WithGracefulDrain(30*time.Second),
)
```

`WithExplicitBatchHandler` delivers `[]Delivery` per capacity-limited prefix.
Exactly one handler mode is allowed, and the constructor handler must be nil.

## Requirements

- A checkpoint store with fenced sessions, paired with the lease manager that
  authorizes its writes. `New` rejects a store without fenced sessions; a lease
  manager that is not a pair with the store is rejected by the first shard that
  binds. Automatic mode still downgrades that pairing to unfenced processing —
  explicit mode cannot, because an acknowledgment that is not fenced proves
  nothing about who owns the shard.
- Bounded admission. Without `WithInFlightLimits` the explicit defaults apply:
  1,000 records and 10 MiB per shard, 10,000 records and 64 MiB per instance,
  and four fetch slots. See [admission limits](in-flight-limits.md).
- `WithShardConcurrency` is rejected. It parallelizes automatic callbacks;
  explicit deliveries are already acknowledged concurrently and out of order.

## Acknowledging

`Delivery.Ack` accepts completion. Copies of a `Delivery` share one
acknowledgment state, calls may be concurrent and out of order, and repeated
calls succeed while the delivery is valid.

Success means accepted, **not persisted**. The checkpoint advances only through
a contiguous completed prefix, so one unacknowledged record behind a long run of
acknowledged ones holds the checkpoint where it is — while still releasing the
admission capacity of everything acknowledged after it.

`Ack` returns `ErrStaleDelivery` for a zero `Delivery`, for an unfinished
delivery from an attempt that failed and was retried, and for any delivery whose
worker has stopped. A backend ownership check that fails returns its error and
leaves the acknowledgment unaccepted.

Failures follow the configured policy. A failed or panicking handler retries
only the deliveries that were not acknowledged during the failed attempt, in
their original order, with fresh handles; the old handles become stale first, so
a late acknowledgment from the failed attempt cannot land. `FailurePolicySkip`
completes the remaining failed deliveries and `FailurePolicySendToDLQ` completes
them after publication, but neither can advance a checkpoint across an earlier
unacknowledged record.

## Checkpointing

A per-shard writer persists the contiguous acknowledged prefix. It writes on
`WithCheckpointInterval` (one second by default), on the `WithBatching` record
count, on a drain, and on shard completion, using the existing bounded
checkpoint retry policy and checkpoint metrics. Writes are serialized, so no
trigger can race another or regress persisted progress.

Checkpoint work is independent of admission: acknowledgments release capacity
while a checkpoint write is still in flight. Exhausted checkpoint writes stop
the shard and the failure is returned from `Start`.

## Shutdown

Shutdown order matters, because the consumer's drain and the application's
completion workers wait on each other. See `Example_explicitShutdown` in the
package documentation for the whole program.

1. Cancelling the context passed to `Start` asks the consumer to drain. It stops
   fetching and admission, discards staged records it never admitted, and leaves
   them replayable.
2. `Start` keeps running. Lease heartbeats, acknowledgment handling, and the
   checkpoint writer stay live, and the contexts of handlers that already
   returned successfully are not cancelled.
3. The drain waits for every admitted delivery to be acknowledged, then flushes
   the acknowledged prefix before releasing the shard's lease.
4. `Start` returns. Nothing is owed an acknowledgment any more, so this is when
   the application stops its own workers and closes their dependencies.

Give completion workers a lifetime that is not the shutdown signal. Stopping
them when the signal arrives leaves the drain waiting for acknowledgments that
can no longer happen, until its timeout.

`WithGracefulDrain`'s timeout bounds that wait; zero waits indefinitely. On
timeout — and on an immediate stop, shedding, or lease loss — processing
contexts are cancelled, outstanding handles become stale, and blocked work is
woken. Unacknowledged records replay.

A closed shard is marked complete only after every outstanding acknowledgment
has been accepted; the marker carries that final sequence, so writing it is what
makes the shard's progress durable. Children stay blocked on the marker, so a
reshard cannot deliver a child's records before the parent's are persisted.

## Related documentation

- [Admission limits and staging](in-flight-limits.md)
- [Fenced recovery](fenced-recovery.md) and the
  [Valkey durability contract](fenced-valkey.md)
- [Handler behavior](handler-behavior.md)
