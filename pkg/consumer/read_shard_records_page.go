package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (c *Consumer) readShardRecordsPage(ctx context.Context, shardID string) (*kinesis.GetRecordsOutput, error) {
	iterator, err := c.getShardIterator(ctx, shardID)
	if err != nil {
		return nil, fmt.Errorf("read shard records page %s: %w", shardID, err)
	}
	if iterator == "" {
		return nil, fmt.Errorf("read shard records page %s: empty shard iterator", shardID)
	}

	out, err := c.getRecords(ctx, iterator)
	if err != nil {
		return nil, fmt.Errorf("read shard records page %s: %w", shardID, err)
	}
	return out, nil
}
