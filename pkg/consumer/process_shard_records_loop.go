package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"
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

		passStartCount := processedSinceCheckpoint
		passLastSeq, count, err := c.runShardRecordsPass(ctx, shardID, processedSinceCheckpoint)
		if passLastSeq != "" {
			lastSeq = passLastSeq
		}
		processedSinceCheckpoint = count
		if err != nil {
			return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, err)
		}
		if passLastSeq == "" && count == passStartCount {
			if err := c.sleep(ctx, c.tuning.pollInterval); err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return lastSeq, processedSinceCheckpoint, nil
				}
				return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, err)
			}
		}
	}
}

func (c *Consumer) runShardRecordsPass(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
	process := c.processShardRecordsPass
	if c.processShardRecordsPassFn != nil {
		process = c.processShardRecordsPassFn
	}
	return process(ctx, shardID, processedSinceCheckpoint)
}

func (c *Consumer) sleep(ctx context.Context, d time.Duration) error {
	if c.sleepFn != nil {
		return c.sleepFn(ctx, d)
	}
	return sleepWithContext(ctx, d)
}
