package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (c *Consumer) processRecordsPage(ctx context.Context, shardID string, out *kinesis.GetRecordsOutput) (string, int, error) {
	if out == nil || len(out.Records) == 0 {
		return "", 0, nil
	}

	if err := c.handleRecordsPage(ctx, shardID, out); err != nil {
		return "", 0, fmt.Errorf("process records page %s: %w", shardID, err)
	}

	lastRecord := out.Records[len(out.Records)-1]
	return aws.ToString(lastRecord.SequenceNumber), len(out.Records), nil
}
