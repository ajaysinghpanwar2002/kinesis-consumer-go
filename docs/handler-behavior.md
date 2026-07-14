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
once per `GetRecords` response instead and ignores any record handler passed to
`New`.

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

A handler that merely *returns* an error matching `context.Canceled` or
`context.DeadlineExceeded` — for example an `http.Client` timeout or a
database query deadline inside the handler — is treated like any other handler
failure: it is retried and, if retries are exhausted, the failure policy
applies. It does not stop the consumer.

## Failure Policies

After handler retries are exhausted, the configured failure policy decides what
happens to the poison record or failed batch.

| Policy | Behavior |
| --- | --- |
| `consumer.FailurePolicySkip` | Default. Treats poison records as handled and continues. |
| `consumer.FailurePolicyFailFast` | Returns the handler failure. The shard worker stops and the error is surfaced through `Start`. |
| `consumer.FailurePolicySendToDLQ` | Publishes poison records through the configured `DLQPublisher` and continues if publishing succeeds. |

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
}
```

If DLQ publishing fails, the consumer returns an error that includes both the
handler failure and the DLQ publish failure.

## Poison Record Metadata

The DLQ publisher receives a `consumer.PoisonRecord`. The record includes:

- Stream name and ARN when configured.
- Shard ID, sequence number, partition key, approximate arrival timestamp, and a
  copied payload.
- The original handler error text.
- Handler kind: `record` or `batch`.
- Attempt count and failure timestamp.
- Batch size and index fields.

For record handlers, one poison record is published for the failed record. For
batch handlers, every record in the failed batch is published with batch
metadata.

DLQ publishing is part of handler processing. A publisher should honor `ctx`,
return useful errors, and be safe for repeated calls. The consumer is
at-least-once, so downstream DLQ handling should tolerate duplicates.

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
