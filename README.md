# Kinesis Consumer Go

Reusable Kinesis consumer with pluggable checkpoint storage and a simple handler API.

## Install

```bash
go get github.com/pratilipi/kinesis-consumer-go/consumer
go get github.com/pratilipi/kinesis-consumer-go/checkpoint
```

## Quick start

```go
ctx := context.Background()
awsCfg, _ := awsconfig.LoadDefaultConfig(ctx)
client := kinesis.NewFromConfig(awsCfg)

store := checkpoint.NewRedisStore(redisClient, "kinesis:checkpoint", 30*24*time.Hour)
handler := func(ctx context.Context, record types.Record) error {
    // your business logic here
    return nil
}

leaseMgr := lease.NewRedisManager(redisClient, "kinesis:lease")

c, _ := consumer.New(consumer.Config{
    StreamName:      "my-stream",
    StartPosition:   consumer.StartLatest,
    ShardConcurrency: 4, // process up to 4 records in parallel per shard
    CheckpointEvery: 100,
}, client, store, handler, consumer.WithLeaseManager(leaseMgr, "", 0, 0, 0))

_ = c.Start(ctx) // blocks until ctx is cancelled or an error occurs
```

## Configuration

- `StreamName` or `StreamARN`: identify the stream (one is required).
- `StartPosition`: `LATEST`, `TRIM_HORIZON`, or `AT_TIMESTAMP` (with `StartTimestamp`).
- `BatchSize`: max records per `GetRecords` (default 100).
- `ShardConcurrency`: number of handler goroutines per shard (default 1). Values >1 process records in parallel within a shard (ordering is no longer guaranteed).
- `PollInterval`: sleep when no records (default 1s).
- `Retry`: `MaxAttempts` and `Backoff` for handler retries (defaults: 3, 1s).
- `CheckpointEvery`: checkpoint after N processed records (default 100).
- `Logger`: optional `*slog.Logger` (defaults to no-op).

### Batch handler (optional)

If you prefer to work with batches directly, provide a batch handler via the `WithBatchHandler` option:

```go
batchHandler := func(ctx context.Context, records []types.Record) error {
    // handle a full batch returned by GetRecords
    return nil
}

c, _ := consumer.New(cfg, client, store, nil, consumer.WithBatchHandler(batchHandler))
```

### Shard leasing (optional)

Use shard leasing to coordinate multiple consumer processes so only one handles a shard at a time. Provide a `lease.Manager` implementation (Redis/Valkey is built in):

```go
leaseMgr := lease.NewRedisManager(redisClient, "kinesis:lease")
// owner and timings are optional; defaults are applied when 0/"" is passed.
c, _ := consumer.New(cfg, client, store, handler, consumer.WithLeaseManager(
    leaseMgr,
    "",             // owner (auto-generated if empty)
    30*time.Second, // lease TTL
    10*time.Second, // renew interval
    5*time.Second,  // retry interval when a shard is busy
))
```

## Checkpoint stores

- In-memory: `checkpoint.NewMemoryStore()` (tests/local).
- Redis/Valkey: `checkpoint.NewRedisStore(redisClient, prefix, ttl)`.
- Implement `checkpoint.Store` for other backends (e.g., MySQL) without touching consumer logic.

## Example

See `examples/basic` for a runnable main that wires AWS config, Redis checkpointing, and a simple handler.
