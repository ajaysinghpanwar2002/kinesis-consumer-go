package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func (c *Consumer) readShardCheckpoint(ctx context.Context, shardID string) (string, error) {
	seq, err := c.store.Get(ctx, c.coordinationKey(), shardID)
	if err != nil {
		return "", fmt.Errorf("read shard checkpoint %s: %w", shardID, err)
	}
	return seq, nil
}

// saveCheckpointValueWithRetry writes one checkpoint value through the store,
// retrying failures with the shared bounded retry policy (retryMaxAttempts /
// retryBackoff) so a brief store blip on a due checkpoint does not escalate
// into a consumer-fatal worker error. Every failed attempt counts a
// checkpoint failure so absorbed blips stay visible on dashboards. The
// backoff wait aborts as soon as ctx is done and returns the ctx error, so a
// shutdown mid-retry surfaces as the shutdown rather than a spurious fatal
// save error. At-least-once semantics are unchanged: on final failure the
// caller still surfaces the error and records replay from the previous
// checkpoint.
func (c *Consumer) saveCheckpointValueWithRetry(ctx context.Context, shardID, value string) error {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.store.Save(ctx, c.coordinationKey(), shardID, value)
		if err == nil {
			return nil
		}
		c.reporter.Counter(metricCheckpointFailures, 1, c.shardTags(shardID))
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return err
		}
	}

	return lastErr
}

func (c *Consumer) saveShardCheckpoint(ctx context.Context, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return nil
	}
	start := time.Now()
	if err := c.saveCheckpointValueWithRetry(ctx, shardID, sequenceNumber); err != nil {
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
	if err := c.saveCheckpointValueWithRetry(ctx, shardID, checkpoint); err != nil {
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
