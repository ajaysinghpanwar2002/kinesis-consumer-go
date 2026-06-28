package consumer

import (
	"context"
	"errors"
	"fmt"
)

func (c *Consumer) processShardRecordsLoop(ctx context.Context, shardID string) (string, int, error) {
	lastSeq := ""
	processedSinceCheckpoint := 0

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, processedSinceCheckpoint, nil
			}
			return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, ctx.Err())
		default:
		}

		passLastSeq, count, err := c.processShardRecordsPass(ctx, shardID, processedSinceCheckpoint)
		if passLastSeq != "" {
			lastSeq = passLastSeq
		}
		processedSinceCheckpoint = count
		if err != nil {
			return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, err)
		}
	}
}
