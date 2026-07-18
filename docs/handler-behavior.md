# Handler Failure Policy, DLQ, and Shard Concurrency

This page documents the current public handler behavior in `pkg/consumer`.

## Handler Modes

`consumer.New` requires either a record handler or a batch handler configured
with `consumer.WithBatchHandler`.

```go
handler := func(ctx context.Context, record consumer.Record) error {
    return nil
}
```

When a record handler is used, the consumer calls it once per Kinesis record. If
`consumer.WithBatchHandler` is configured, the consumer calls the batch handler
once per `GetRecords` response instead. The two modes are mutually exclusive:
`consumer.New` returns an error when both a record handler and
`WithBatchHandler` are provided.

```go
opts := []consumer.Option{
    consumer.WithBatchHandler(func(ctx context.Context, records []consumer.Record) error {
        return nil
    }),
}
```

## Retries

`consumer.WithRetry(maxAttempts, backoff)` controls handler retries. The default
is 3 attempts with a 1 second base backoff.

Between failed attempts, the consumer waits using linear backoff:

```text
sleep = attempt * backoff
```

Only consumer-side context cancellation is terminal. If the shard context the
consumer passed to the handler is done (consumer shutdown, lease loss, or a
concurrent-page abort), the consumer stops immediately and does not apply the
failure policy.

Handlers must honor that context promptly. Go cannot forcibly terminate a
callback: after a nonzero graceful-drain deadline or an ordinary non-graceful
cancellation, `Start` returns even if a callback is still running. If that
callback later reports success, the consumer discards the late result before
checkpointing; the record remains eligible for at-least-once redelivery. A zero
graceful-drain timeout intentionally waits indefinitely. DLQ calls have their
own bounded-attempt behavior described below.

A handler that merely *returns* an error matching `context.Canceled` or
`context.DeadlineExceeded` — for example an `http.Client` timeout or a
database query deadline inside the handler — is treated like any other handler
failure: it is retried and, if retries are exhausted, the failure policy
applies. It does not stop the consumer.

## Panics

A panic in a record or batch handler does not kill the process. The consumer
recovers it at the attempt boundary, logs it at error level with the panic
value and stack trace, and converts it into an ordinary handler error wrapping
`consumer.ErrHandlerPanic`. From there it follows exactly the flow above: the
attempt counts as a failure, remaining retries run (a transiently panicking
handler can still succeed), and exhausted retries hand the records to the
configured failure policy — fail-fast surfaces the panic error through
`Start` (match it with `errors.Is(err, consumer.ErrHandlerPanic)`), skip drops
the records, and send-to-DLQ publishes poison records whose handler-error text
carries the panic value.

A handler that panics with an error value (`panic(err)`) keeps that error
matchable: the converted error wraps both `ErrHandlerPanic` and the original
error. This applies in all three modes — record, batch, and
`WithShardConcurrency > 1` — including the concurrent page workers, which are
additionally guarded so an unexpected library-side panic fails the page
instead of the process.

## Failure Policies

After handler retries are exhausted, the configured failure policy decides what
happens to the poison record or failed batch.

| Policy | Behavior |
| --- | --- |
| `consumer.FailurePolicyFailFast` | Default. Returns the handler failure, stops the shard worker, surfaces the error through `Start`, and leaves the failed page uncheckpointed for replay. |
| `consumer.FailurePolicySkip` | Explicit, intentionally lossy opt-in. Treats the failure as handled and allows the completed page checkpoint to advance. |
| `consumer.FailurePolicySendToDLQ` | Publishes poison records through the configured `DLQPublisher` and continues if publishing succeeds. |

Skip has different loss granularity in the two handler modes. With a record
handler, only the failed record is dropped; the consumer continues processing
the other records in the page and can checkpoint past the page. With a batch
handler, one error fails the indivisible batch, so Skip drops the entire failed
`GetRecords` page—including records that were not themselves poison—and can
checkpoint past all of it. In both modes, a restart resumes after the advanced
checkpoint and does not recover the dropped data. Use Skip only when continued
progress is worth that deliberate durability tradeoff.

`FailurePolicySendToDLQ` requires `consumer.WithDLQPublisher`. `consumer.New`
returns an error if send-to-DLQ is configured without a publisher.

```go
type publisher struct{}

func (publisher) Publish(ctx context.Context, record consumer.PoisonRecord) error {
    return nil
}

opts := []consumer.Option{
    consumer.WithFailurePolicy(consumer.FailurePolicySendToDLQ),
    consumer.WithDLQPublisher(publisher{}),
    consumer.WithDLQRetry(3, time.Second),
    consumer.WithDLQAttemptTimeout(10 * time.Second),
}
```

DLQ delivery has settings independent from handler and coordination retries:

- `WithDLQRetry(maxAttempts, backoff)` defaults to 3 attempts and a 1 second
  linear-backoff base. Before attempt N, the consumer sleeps
  `(N-1) * backoff`.
- `WithDLQAttemptTimeout(timeout)` defaults to 10 seconds and gives each
  `Publish` call its own child context and deadline.
- Parent cancellation stops delivery without another attempt. A per-attempt
  deadline is retryable while attempts remain.

If all DLQ attempts fail, the consumer returns an error that includes both the
handler failure and the final DLQ publish failure. The failed source page is not
checkpointed.

When publishing succeeds, the consumer checkpoints past the page immediately
after the page completes, rather than waiting for the next due checkpoint
(`WithBatching`'s `checkpointEvery`). Until that checkpoint is saved, a crash
replays the page: its poison records would be republished to the DLQ *and*
redelivered to the handler on the replay, so a record can be observed on both
paths. The immediate flush narrows that both-paths duplicate window to the gap
between the publish and the save; it cannot close it — DLQ delivery and source
processing remain at-least-once, so downstream systems must deduplicate on
`IdempotencyKey`.

## Poison Record Metadata

The DLQ publisher receives a `consumer.PoisonRecord`. The record includes:

- A stable `IdempotencyKey` for downstream deduplication.
- Stream name and ARN when configured, plus the required consumer group.
- Shard ID, sequence number, partition key, approximate arrival timestamp, and a
  copied payload.
- The original handler error text.
- Handler kind: `record` or `batch`.
- Attempt count and failure timestamp.
- Batch size and index fields.

For record handlers, one poison record is published for the failed record. For
batch handlers, every record in the failed batch is published with batch
metadata.

`IdempotencyKey` has the form `kcg-dlq-v1:<sha256>` and hashes unambiguously
length-delimited values for consumer group, canonical stream name, shard ID,
sequence number, and handler kind. It is therefore stable across DLQ attempts,
source replay, Consumer rebuilds, equivalent name/ARN stream configuration,
failure timestamps, batch position, handler error text, and payload changes.
Record and batch handler failures intentionally have different keys.

DLQ publishing is part of handler processing. A publisher must honor `ctx`
promptly, return useful errors, and be safe for concurrent and repeated calls.
When a publisher ignores its context, the consumer stops waiting at the attempt
deadline. The extension goroutine may continue, but its late success cannot make
the page checkpointable; a retry can overlap it and use the same idempotency
key.

DLQ delivery is at-least-once, never exactly-once or atomic across a failed
batch. If record N fails after records 1 through N-1 were published, no source
checkpoint is saved for the page. A restart replays the page and republishes the
successful prefix. A publisher can also commit a record and lose its response,
causing an ordinary retry. Downstream DLQ systems must deduplicate on
`IdempotencyKey`; the library does not claim atomic fan-out. Source replays after
a crash and brief dual delivery during ownership transfers add the same
duplicate possibility (see [features.md](features.md)).

## Shard Concurrency and Ordering

`consumer.WithShardConcurrency(n)` controls record-handler concurrency within
one shard. The default is 1.

With `n == 1`, record handlers run sequentially in Kinesis batch order for that
shard. This preserves per-shard processing order.

With `n > 1`, the consumer runs up to `n` record handler calls at the same time
for a shard page. This can improve throughput, but it breaks strict per-shard
ordering because later records may complete before earlier records.

On the first record-handler error, the consumer cancels the derived page context,
stops assigning new records, waits for already-started handlers to return, and
then returns the first error.

`WithShardConcurrency` applies only to record handlers. Batch handlers are still
called once per `GetRecords` response and are not split across worker goroutines
by the consumer.

When `WithShardConcurrency(n > 1)` is configured, record handlers can run
concurrently and `DLQPublisher.Publish` can also be called concurrently if
multiple records exhaust retries. Handler and publisher implementations must be
concurrency-safe.

Keep `WithShardConcurrency(1)` when strict per-shard ordering matters.
