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
