package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// processShardRecordsPass reads and processes records from a shard one page at a
// time until it catches up to the tip, the shard closes, the consumer starts
// draining, or the context is done. Pages are processed as they are fetched, so
// memory stays bounded to a single page regardless of how large the backlog is.
//
// It returns the last processed sequence number, the running
// processed-since-checkpoint count for the next pass, and an error. A closed
// shard returns errShardCompleted after persisting a completion checkpoint.
func (c *Consumer) processShardRecordsPass(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
	iterator, err := c.getShardIterator(ctx, shardID)
	if err != nil {
		return "", processedSinceCheckpoint, fmt.Errorf("process shard records pass %s: %w", shardID, err)
	}
	if iterator == "" {
		return "", processedSinceCheckpoint, fmt.Errorf("process shard records pass %s: empty shard iterator", shardID)
	}

	lastSeq := ""
	count := processedSinceCheckpoint

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, count, nil
			}
			return lastSeq, count, fmt.Errorf("process shard records pass %s: %w", shardID, ctx.Err())
		default:
		}
		if c.isDraining() {
			return lastSeq, count, nil
		}

		out, err := c.getRecords(ctx, iterator)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, count, nil
			}
			return lastSeq, count, fmt.Errorf("process shard records pass %s: %w", shardID, err)
		}

		pageLastSeq, pageCount, err := c.processRecordsPageWithCheckpoint(ctx, shardID, out, count)
		if pageLastSeq != "" {
			lastSeq = pageLastSeq
		}
		count = pageCount
		if err != nil {
			return lastSeq, count, fmt.Errorf("process shard records pass %s: %w", shardID, err)
		}

		if c.isDraining() {
			// The loop's drain path owns the drain checkpoint.
			return lastSeq, count, nil
		}

		if pageEndsShard(out) {
			if err := c.saveShardCompletionCheckpoint(ctx, shardID, lastSeq); err != nil {
				return lastSeq, count, fmt.Errorf("process shard records pass completion checkpoint %s: %w", shardID, err)
			}
			return lastSeq, count, fmt.Errorf("process shard records pass %s: %w", shardID, errShardCompleted)
		}

		if out == nil || len(out.Records) == 0 {
			// Caught up to the shard tip. Flush any processed-but-not-yet-
			// checkpointed records so the next pass, which re-derives its iterator
			// from the stored checkpoint, resumes past them instead of replaying
			// them. Skip when the context is done (shutdown): the loop's drain path
			// owns that checkpoint, and a canceled context would fail the save.
			if count > 0 && lastSeq != "" && ctx.Err() == nil {
				if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
					return lastSeq, count, fmt.Errorf("process shard records pass flush checkpoint %s: %w", shardID, err)
				}
				count = 0
			}
			return lastSeq, count, nil
		}

		iterator = aws.ToString(out.NextShardIterator)
	}
}
