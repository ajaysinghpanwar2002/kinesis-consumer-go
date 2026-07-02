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
		if c.isDraining() {
			return pages, nil
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
		if c.isDraining() {
			return pages, nil
		}
		if len(out.Records) == 0 {
			// An empty page means we have caught up to the shard tip for now.
			// Return so the pass can process/checkpoint the pages gathered so far
			// and the next pass resumes from the checkpoint. Without this, an open
			// shard (whose NextShardIterator is always non-empty) would loop here
			// forever and never hand any pages to the processor.
			return pages, nil
		}
		iterator = aws.ToString(out.NextShardIterator)
	}
	return pages, nil
}
