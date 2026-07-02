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
	if c.isDraining() {
		return lastSeq, count, nil
	}
	if shardRecordsPagesEnded(pages) {
		if err := c.saveShardCompletionCheckpoint(ctx, shardID, lastSeq); err != nil {
			return lastSeq, count, fmt.Errorf("process shard records pass completion checkpoint %s: %w", shardID, err)
		}
		return lastSeq, count, fmt.Errorf("process shard records pass %s: %w", shardID, errShardCompleted)
	}

	// The pass caught up to the shard tip. Flush any processed-but-not-yet-
	// checkpointed records so the next pass, which re-derives its iterator from the
	// stored checkpoint, resumes past them instead of replaying them. Skip when the
	// context is done (shutdown): the loop's drain path owns that checkpoint, and a
	// canceled context would fail the save.
	if count > 0 && lastSeq != "" && ctx.Err() == nil {
		if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
			return lastSeq, count, fmt.Errorf("process shard records pass flush checkpoint %s: %w", shardID, err)
		}
		count = 0
	}
	return lastSeq, count, nil
}
