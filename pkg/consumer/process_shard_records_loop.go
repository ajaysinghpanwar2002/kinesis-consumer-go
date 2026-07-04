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
	// iterator is held across passes so a StartLatest position is not re-anchored
	// to the moving shard tip on every pass (LIB-2). It is loop-local on purpose:
	// a new worker taking over after rebalance/failover starts fresh (empty) and
	// re-derives from the stored checkpoint.
	iterator := ""

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, processedSinceCheckpoint, nil
			}
			return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, ctx.Err())
		default:
		}
		if c.isDraining() {
			if err := c.checkpointOnDrain(ctx, shardID, lastSeq, processedSinceCheckpoint); err != nil {
				return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, err)
			}
			return lastSeq, processedSinceCheckpoint, nil
		}

		passStartCount := processedSinceCheckpoint
		passLastSeq, count, nextIterator, err := c.runShardRecordsPass(ctx, shardID, processedSinceCheckpoint, iterator)
		iterator = nextIterator
		if passLastSeq != "" {
			lastSeq = passLastSeq
		}
		processedSinceCheckpoint = count
		if err != nil {
			if errors.Is(err, errShardCompleted) {
				return lastSeq, processedSinceCheckpoint, nil
			}
			return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, err)
		}
		if c.isDraining() {
			if err := c.checkpointOnDrain(ctx, shardID, lastSeq, processedSinceCheckpoint); err != nil {
				return lastSeq, processedSinceCheckpoint, fmt.Errorf("process shard records loop %s: %w", shardID, err)
			}
			return lastSeq, processedSinceCheckpoint, nil
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

func (c *Consumer) runShardRecordsPass(ctx context.Context, shardID string, processedSinceCheckpoint int, iterator string) (string, int, string, error) {
	process := c.processShardRecordsPass
	if c.processShardRecordsPassFn != nil {
		process = c.processShardRecordsPassFn
	}
	return process(ctx, shardID, processedSinceCheckpoint, iterator)
}

func (c *Consumer) sleep(ctx context.Context, d time.Duration) error {
	if c.sleepFn != nil {
		return c.sleepFn(ctx, d)
	}
	return sleepWithContext(ctx, d)
}
