package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// KinesisAPI is the subset of the AWS SDK v2 Kinesis client that the consumer
// depends on. New accepts this interface rather than a concrete
// *kinesis.Client so callers can supply a test double or an instrumented
// wrapper (for tracing, throttling, fault injection, and so on).
//
// A *kinesis.Client — the Client alias, returned by kinesis.New /
// kinesis.NewFromConfig — satisfies KinesisAPI directly, so existing callers
// pass their client unchanged. The parameter types mirror the SDK exactly,
// including the *kinesis.Options functional options, so a wrapper can forward
// calls verbatim.
type KinesisAPI interface {
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

// The concrete AWS SDK client satisfies the interface the consumer accepts.
var _ KinesisAPI = (*Client)(nil)
