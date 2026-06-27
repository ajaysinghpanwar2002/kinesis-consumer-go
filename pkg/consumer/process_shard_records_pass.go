package consumer

import (
	"context"
	"fmt"
)

func (c *Consumer) processShardRecordsPass(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
	pages, err := c.pollShardRecordsPages(ctx, shardID)
	if err != nil {
		return "", processedSinceCheckpoint, fmt.Errorf("process shard records pass %s: %w", shardID, err)
	}

	lastSeq, count, err := c.processRecordsPagesWithCheckpoint(ctx, shardID, pages, processedSinceCheckpoint)
	if err != nil {
		return lastSeq, count, fmt.Errorf("process shard records pass %s: %w", shardID, err)
	}
	return lastSeq, count, nil
}
