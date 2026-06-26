package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (c *Consumer) pollShardRecordsPages(ctx context.Context, shardID string) ([]*kinesis.GetRecordsOutput, error) {
	iterator, err := c.getShardIterator(ctx, shardID)
	if err != nil {
		return nil, fmt.Errorf("poll shard records pages %s: %w", shardID, err)
	}
	if iterator == "" {
		return nil, fmt.Errorf("poll shard records pages %s: empty shard iterator", shardID)
	}

	var pages []*kinesis.GetRecordsOutput
	for iterator != "" {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return pages, nil
			}
			return nil, fmt.Errorf("poll shard records pages %s: %w", shardID, ctx.Err())
		default:
		}

		out, err := c.getRecords(ctx, iterator)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return pages, nil
			}
			return nil, fmt.Errorf("poll shard records pages %s: %w", shardID, err)
		}
		pages = append(pages, out)
		if out == nil {
			return pages, nil
		}
		iterator = aws.ToString(out.NextShardIterator)
	}
	return pages, nil
}
