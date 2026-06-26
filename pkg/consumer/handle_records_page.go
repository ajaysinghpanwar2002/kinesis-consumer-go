package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (c *Consumer) handleRecordsPage(ctx context.Context, shardID string, out *kinesis.GetRecordsOutput) error {
	if out == nil || len(out.Records) == 0 {
		return nil
	}

	if c.batchHandler != nil {
		if err := c.batchHandler(ctx, out.Records); err != nil {
			return fmt.Errorf("handle records page %s: batch handler: %w", shardID, err)
		}
		return nil
	}

	if c.handler == nil {
		return fmt.Errorf("handle records page %s: record handler is nil", shardID)
	}
	for _, record := range out.Records {
		if err := c.handler(ctx, record); err != nil {
			return fmt.Errorf("handle records page %s: record handler: %w", shardID, err)
		}
	}
	return nil
}
