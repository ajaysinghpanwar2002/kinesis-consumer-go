# Kinesis Consumer Go

Small, reusable Kinesis consumer with automatic shard tracking, pluggable checkpoint storage, optional shard leasing, and both record-level and batch handlers.

## What you get
- Auto-discovery of shards with parent/child ordering: children start only after parents are fully processed and checkpointed.
- At-least-once delivery with pluggable checkpoints (memory, Redis, or your own store) and shard-completion markers.
- Optional shard leasing (Redis/Valkey provided) so multiple consumer processes do not double-process shards.
- Configurable retries/backoff, per-shard parallelism, and batch vs per-record handling.
- Background shard sync to pick up stream resharding without restarts; graceful shutdown via context cancellation.

## Install

```bash
go get github.com/pratilipi/kinesis-consumer-go/consumer
go get github.com/pratilipi/kinesis-consumer-go/checkpoint
go get github.com/pratilipi/kinesis-consumer-go/lease    # only if you want leasing
```

## How it works (mental model)
- On start, shards are listed and a tracker keeps state for active and completed shards. Parents must be marked complete before children run.
- For each shard, the consumer resumes from the checkpointed sequence number (or a start position), requests records with `GetRecords`, and calls your handler.
- Handler errors are retried up to `Retry.MaxAttempts` with a linear backoff (`attempt * Retry.Backoff`).
- Checkpoints are written every `CheckpointEvery` records and when a shard ends. Closed shards are marked with `SHARD_END[:seq]` to unblock children.
- A background ticker re-lists shards every `ShardSyncInterval` to discover new shards.
- If a `lease.Manager` is provided, shards are processed only while the lease is owned and renewed; losing the lease stops work on that shard.

## Quick start (per-record handler)
1) Configure AWS credentials/region (standard AWS SDK v2) and create a `kinesis.Client`.  
2) Pick a checkpoint store (Redis for shared durability; in-memory for tests).  
3) Optionally set up a lease manager if multiple consumers will read the same stream.  
4) Write a handler and start the consumer; cancel the context to stop.

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

awsCfg, _ := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
kinesisClient := kinesis.NewFromConfig(awsCfg)

redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
store := checkpoint.NewRedisStore(redisClient, "kinesis:checkpoint", 30*24*time.Hour)
leaseMgr := lease.NewRedisManager(redisClient, "kinesis:lease") // optional

handler := func(ctx context.Context, rec types.Record) error {
    // your business logic here
    return nil
}

cfg := consumer.Config{
    StreamName:       "my-stream",                 // or StreamARN
    StartPosition:    consumer.StartLatest,        // StartTrimHorizon | StartAtTimestamp (+StartTimestamp)
    ShardConcurrency: 2,                           // per-shard goroutines; >1 drops ordering within a shard
    BatchSize:        500,                         // GetRecords limit
    CheckpointEvery:  100,                         // checkpoint frequency
    Retry:            consumer.RetryConfig{MaxAttempts: 3, Backoff: time.Second},
}

cons, err := consumer.New(cfg, kinesisClient, store, handler,
    consumer.WithLeaseManager(leaseMgr, "", 0, 0, 0)) // omit option to run without leasing
if err != nil {
    slog.Error("create consumer", slog.Any("err", err))
    return
}

if err := cons.Start(ctx); err != nil {
    slog.Error("consumer stopped", slog.Any("err", err))
}
```

## Batch handler mode
If you prefer to process the entire `GetRecords` response yourself, supply a batch handler (the per-record handler is ignored):

```go
batchHandler := func(ctx context.Context, records []types.Record) error {
    // process or fan out the batch
    return nil
}

cons, err := consumer.New(cfg, kinesisClient, store, nil, consumer.WithBatchHandler(batchHandler))
```

## Configuration reference
- `StreamName` or `StreamARN` (required): identifies the stream.
- `StartPosition`: `LATEST` (default), `TRIM_HORIZON`, or `AT_TIMESTAMP` (requires `StartTimestamp`).
- `BatchSize` (default 100): max records per `GetRecords`.
- `ShardConcurrency` (default 1): per-shard goroutines for the **record** handler; values >1 process records in parallel and break ordering.
- `PollInterval` (default 1s): sleep when no records are returned.
- `ShardSyncInterval` (default 1m): how often to list shards and start new ones.
- `Retry` (defaults: max attempts 3, backoff 1s): linear backoff for handler retries.
- `CheckpointEvery` (default 100): write checkpoints after this many processed records.
- `Logger`: optional `*slog.Logger` (defaults to a no-op logger).

## Checkpoint stores
- In-memory: `checkpoint.NewMemoryStore()` (tests/local only; no persistence).
- Redis/Valkey: `checkpoint.NewRedisStore(redisClient, prefix, ttl)` (default prefix `kinesis:checkpoint`, TTL 30d).
- Custom: implement `checkpoint.Store` to plug in any backend.

## Shard leasing (optional)
Provide a `lease.Manager` (Redis implementation included) to ensure only one consumer owns a shard at a time. Configure TTL/renew/retry timing via `WithLeaseManager`; an owner ID is auto-generated if empty.

## Run the example
- `examples/basic`: minimal consumer wiring (AWS client, Redis checkpoint/lease, metrics, optional pprof).  
  Run with env vars like `STREAM_NAME`, `AWS_REGION`, `AWS_ENDPOINT` (e.g., LocalStack), `REDIS_ADDR`, `START_POSITION`, `START_TIMESTAMP_RFC3339`, `SHARD_CONCURRENCY`, `CHECKPOINT_EVERY`, `BATCH_SIZE`.
- `cmd/producer`: helper to load a stream for benchmarking; configurable via `STREAM_NAME`, `AWS_REGION`, `AWS_ENDPOINT`, `SHARD_COUNT`, `BATCH_SIZE`, `WORKERS`, `PAYLOAD_BYTES`.
