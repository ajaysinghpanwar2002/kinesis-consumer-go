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
	// Cancellation cannot forcibly stop a user callback. If one ignores ctx
	// and reports success after Start has already signaled this worker to stop,
	// discard that late result before it can reach checkpoint code.
	if ctx.Err() != nil || shardWorkerStopRequested(ctx) {
		err := ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		return "", 0, fmt.Errorf("process records page %s: %w", shardID, err)
	}

	lastRecord := out.Records[len(out.Records)-1]
	return aws.ToString(lastRecord.SequenceNumber), len(out.Records), nil
}

func (c *Consumer) processRecordsPageWithCheckpoint(ctx context.Context, shardID string, out *kinesis.GetRecordsOutput, processedSinceCheckpoint int) (string, int, error) {
	lastSeq, processed, err := c.processRecordsPage(ctx, shardID, out)
	if err != nil {
		return "", processedSinceCheckpoint, fmt.Errorf("process records page with checkpoint %s: %w", shardID, err)
	}
	if processed == 0 {
		return "", processedSinceCheckpoint, nil
	}

	accumulated := processedSinceCheckpoint + processed
	count, err := c.saveShardCheckpointIfDue(ctx, shardID, lastSeq, accumulated)
	if err != nil {
		return lastSeq, count, fmt.Errorf("process records page checkpoint %s: %w", shardID, err)
	}
	return lastSeq, count, nil
}
