package consumer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type dlqPageFlushKey struct{}

// dlqPageFlushState is the page-scoped signal that the failure policy
// published this page's poison records to the DLQ, installed by
// processRecordsPageWithCheckpoint and set by applyFailurePolicy. The policy
// runs deep inside the per-record retry path and does not know page
// boundaries, while the flush must happen at page level: a checkpoint marks
// the whole prefix processed, and with WithShardConcurrency > 1 records
// complete out of order, so flushing mid-page would be unsafe. The atomic
// makes concurrent per-record DLQ publishes within one page safe.
type dlqPageFlushState struct {
	published atomic.Bool
}

// markDLQPagePublished records a successful DLQ publish on the current page.
// A no-op when no page flag is installed (e.g. direct policy-level tests).
func markDLQPagePublished(ctx context.Context) {
	if state, ok := ctx.Value(dlqPageFlushKey{}).(*dlqPageFlushState); ok {
		state.published.Store(true)
	}
}

func (c *Consumer) processRecordsPage(ctx context.Context, shardID string, out *kinesis.GetRecordsOutput) (string, int, error) {
	if out == nil || len(out.Records) == 0 {
		return "", 0, nil
	}

	if err := c.handleRecordsPage(ctx, shardID, out); err != nil {
		return "", 0, fmt.Errorf("process records page %s: %w", shardID, err)
	}
	// Cancellation cannot forcibly stop a user callback. If one ignores ctx
	// and reports success after Start has already signaled this worker to stop,
	// discard that late result before it can reach checkpoint code.
	if ctx.Err() != nil || shardWorkerStopRequested(ctx) {
		err := ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		return "", 0, fmt.Errorf("process records page %s: %w", shardID, err)
	}

	// Health().Processing.LastRecordProcessed: the page finished processing —
	// handler success, or a skip/DLQ policy outcome that lets the page
	// checkpoint advance.
	c.processingHealth.recordProcessed(time.Now())

	lastRecord := out.Records[len(out.Records)-1]
	return aws.ToString(lastRecord.SequenceNumber), len(out.Records), nil
}

func (c *Consumer) processRecordsPageWithCheckpoint(ctx context.Context, shardID string, out *kinesis.GetRecordsOutput, processedSinceCheckpoint int) (string, int, error) {
	dlqFlush := &dlqPageFlushState{}
	lastSeq, processed, err := c.processRecordsPage(context.WithValue(ctx, dlqPageFlushKey{}, dlqFlush), shardID, out)
	if err != nil {
		return "", processedSinceCheckpoint, fmt.Errorf("process records page with checkpoint %s: %w", shardID, err)
	}
	if processed == 0 {
		return "", processedSinceCheckpoint, nil
	}

	accumulated := processedSinceCheckpoint + processed
	if dlqFlush.published.Load() {
		// The page's poison records were published to the DLQ. Checkpoint past
		// the page immediately instead of waiting for the next due checkpoint:
		// until the checkpoint advances, a crash replays the page and delivers
		// its records both to the DLQ and fully processed downstream. The flush
		// narrows that both-paths duplicate window to the publish→save gap; it
		// cannot close it (at-least-once).
		if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
			return lastSeq, accumulated, fmt.Errorf("process records page dlq checkpoint %s: %w", shardID, err)
		}
		return lastSeq, 0, nil
	}
	count, err := c.saveShardCheckpointIfDue(ctx, shardID, lastSeq, accumulated)
	if err != nil {
		return lastSeq, count, fmt.Errorf("process records page checkpoint %s: %w", shardID, err)
	}
	return lastSeq, count, nil
}
