package consumer

import "context"

// HandlerFunc processes one Kinesis record. It must honor ctx promptly and be
// safe for concurrent calls: different shards run concurrently, and
// WithShardConcurrency can also parallelize calls within one shard. If a
// handler ignores cancellation, Start may return while that call is still
// running; its late result is discarded and cannot advance a checkpoint.
type HandlerFunc func(ctx context.Context, record Record) error

// BatchHandlerFunc processes records from one GetRecords response together. It
// must honor ctx promptly and be safe for concurrent calls from different
// shards. If it ignores cancellation, Start may return while that call is still
// running; its late result is discarded and cannot advance a checkpoint.
type BatchHandlerFunc func(ctx context.Context, records []Record) error
