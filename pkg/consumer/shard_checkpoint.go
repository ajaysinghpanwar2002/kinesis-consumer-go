package consumer

import (
	"context"
	"fmt"
)

func (c *Consumer) saveShardCheckpoint(ctx context.Context, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return nil
	}
	if err := c.store.Save(ctx, c.streamKey(), shardID, sequenceNumber); err != nil {
		return fmt.Errorf("save shard checkpoint %s %s: %w", shardID, sequenceNumber, err)
	}
	return nil
}

func (c *Consumer) saveShardCheckpointIfDue(ctx context.Context, shardID, sequenceNumber string, processedSinceCheckpoint int) (int, error) {
	if processedSinceCheckpoint < c.tuning.checkpointEvery {
		return processedSinceCheckpoint, nil
	}
	if sequenceNumber == "" {
		return processedSinceCheckpoint, nil
	}
	if err := c.saveShardCheckpoint(ctx, shardID, sequenceNumber); err != nil {
		return processedSinceCheckpoint, fmt.Errorf("save due shard checkpoint %s: %w", shardID, err)
	}
	return processedSinceCheckpoint % c.tuning.checkpointEvery, nil
}
