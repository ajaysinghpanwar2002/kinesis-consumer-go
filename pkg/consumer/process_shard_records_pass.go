package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// processShardRecordsPass reads and processes records from a shard one page at a
// time until it catches up to the tip, the shard closes, the consumer starts
// draining, or the context is done. Pages are processed as they are fetched, so
// memory stays bounded to a single page regardless of how large the backlog is.
//
// The caller (processShardRecordsLoop) owns the shard iterator across passes and
// threads it in via the iterator argument. When iterator is empty (the first pass
// of a worker, or after an expired-iterator reset) the pass derives one from the
// stored checkpoint — or, with no checkpoint, from the configured StartPosition.
// When the pass catches up it returns the last NextShardIterator so the loop can
// keep polling from exactly there. This matters for StartLatest: re-deriving a
// fresh LATEST iterator every pass would re-anchor to the moving shard tip and
// silently skip records produced during the poll gap (LIB-2). A fixed anchor
// (TRIM_HORIZON / AT_TIMESTAMP) re-reads harmlessly, but LATEST does not.
//
// It returns the last processed sequence number, the running
// processed-since-checkpoint count for the next pass, the shard iterator to
// resume from, and an error. A closed shard returns errShardCompleted after
// persisting a completion checkpoint.
func (c *Consumer) processShardRecordsPass(ctx context.Context, shardID string, processedSinceCheckpoint int, iterator string) (string, int, string, error) {
	lastSeq := ""
	count := processedSinceCheckpoint

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, count, iterator, nil
			}
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, ctx.Err())
		default:
		}
		if c.isDraining() {
			return lastSeq, count, iterator, nil
		}

		if iterator == "" {
			derived, err := c.getShardIterator(ctx, shardID)
			if err != nil {
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
			}
			if derived == "" {
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: empty shard iterator", shardID)
			}
			iterator = derived
		}

		getRecordsStart := time.Now()
		out, err := c.getRecords(ctx, iterator)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, count, iterator, nil
			}
			var expired *types.ExpiredIteratorException
			if errors.As(err, &expired) {
				// The held iterator outlived its ~5-minute TTL (e.g. a large
				// pollInterval or a slow handler stretched the gap between reads).
				// Drop it and re-derive from the stored checkpoint on the next
				// iteration instead of failing the shard.
				iterator = ""
				continue
			}
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
		}
		c.reporter.Timing(metricGetRecordsDuration, time.Since(getRecordsStart), c.shardTags(shardID))
		c.reporter.Counter(metricPagesFetched, 1, c.shardTags(shardID))
		if out.MillisBehindLatest != nil {
			c.reporter.Gauge(metricMillisBehindLatest, float64(*out.MillisBehindLatest), c.shardTags(shardID))
		}

		pageLastSeq, pageCount, err := c.processRecordsPageWithCheckpoint(ctx, shardID, out, count)
		if pageLastSeq != "" {
			lastSeq = pageLastSeq
		}
		count = pageCount
		if err != nil {
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
		}

		if c.isDraining() {
			// The loop's drain path owns the drain checkpoint.
			return lastSeq, count, iterator, nil
		}

		if pageEndsShard(out) {
			if err := c.saveShardCompletionCheckpoint(ctx, shardID, lastSeq); err != nil {
				return lastSeq, count, "", fmt.Errorf("process shard records pass completion checkpoint %s: %w", shardID, err)
			}
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, errShardCompleted)
		}

		iterator = aws.ToString(out.NextShardIterator)

		// getRecords never returns a nil output (it normalizes nil to an empty
		// GetRecordsOutput), and NextShardIterator was just dereferenced above, so
		// an empty page is the only caught-up signal to check here.
		if len(out.Records) == 0 {
			// Caught up to the shard tip. Flush any processed-but-not-yet-
			// checkpointed records so a FAILOVER/RESTART (a fresh worker re-enters
			// with an empty iterator and re-derives from the checkpoint) resumes
			// past them instead of replaying. The in-process next pass resumes from
			// the returned iterator, but the flush must stay for the cross-worker
			// case. Skip when the context is done (shutdown): the loop's drain path
			// owns that checkpoint, and a canceled context would fail the save.
			if count > 0 && lastSeq != "" && ctx.Err() == nil {
				if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
					return lastSeq, count, "", fmt.Errorf("process shard records pass flush checkpoint %s: %w", shardID, err)
				}
				count = 0
			}
			return lastSeq, count, iterator, nil
		}
	}
}
