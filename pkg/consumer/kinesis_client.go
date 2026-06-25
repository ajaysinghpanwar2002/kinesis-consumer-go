package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type kinesisAPI interface {
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}
