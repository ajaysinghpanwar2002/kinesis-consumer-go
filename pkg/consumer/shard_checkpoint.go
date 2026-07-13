package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func (c *Consumer) readShardCheckpoint(ctx context.Context, shardID string) (string, error) {
	seq, err := c.store.Get(ctx, c.streamKey(), shardID)
	if err != nil {
		return "", fmt.Errorf("read shard checkpoint %s: %w", shardID, err)
	}
	return seq, nil
}

func (c *Consumer) saveShardCheckpoint(ctx context.Context, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return nil
	}
	start := time.Now()
	if err := c.store.Save(ctx, c.streamKey(), shardID, sequenceNumber); err != nil {
		c.reporter.Counter(metricCheckpointFailures, 1, c.shardTags(shardID))
		return fmt.Errorf("save shard checkpoint %s %s: %w", shardID, sequenceNumber, err)
	}
	c.reporter.Timing(metricCheckpointSaveDuration, time.Since(start), c.shardTags(shardID))
	c.reporter.Counter(metricCheckpointsSaved, 1, c.shardTags(shardID))
	c.logger.Debug("shard checkpoint saved", slog.String("shard", shardID), slog.String("sequence", sequenceNumber))
	return nil
}

func (c *Consumer) saveShardCompletionCheckpoint(ctx context.Context, shardID, sequenceNumber string) error {
	checkpoint := shardCompletionValue(sequenceNumber)
	start := time.Now()
	if err := c.store.Save(ctx, c.streamKey(), shardID, checkpoint); err != nil {
		c.reporter.Counter(metricCheckpointFailures, 1, c.shardTags(shardID))
		return fmt.Errorf("save shard completion checkpoint %s %s: %w", shardID, checkpoint, err)
	}
	c.reporter.Timing(metricCheckpointSaveDuration, time.Since(start), c.shardTags(shardID))
	c.reporter.Counter(metricCheckpointsSaved, 1, c.shardTags(shardID))
	c.reporter.Counter(metricShardsCompleted, 1, c.shardTags(shardID))
	c.logger.Info("shard completed", slog.String("shard", shardID), slog.String("checkpoint", checkpoint))
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

func (c *Consumer) checkpointOnDrain(ctx context.Context, shardID, sequenceNumber string, processedSinceCheckpoint int) error {
	if processedSinceCheckpoint <= 0 || sequenceNumber == "" {
		return nil
	}
	if err := c.saveShardCheckpoint(ctx, shardID, sequenceNumber); err != nil {
		return fmt.Errorf("save drain shard checkpoint %s: %w", shardID, err)
	}
	c.logger.Debug("shard drain checkpoint flushed",
		slog.String("shard", shardID),
		slog.String("sequence", sequenceNumber),
		slog.Int("records", processedSinceCheckpoint),
	)
	return nil
}
