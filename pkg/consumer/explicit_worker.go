package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// processExplicitPage hands one fetched page to this shard's explicit
// processor, transferring the fetch slot with it. The processor owns admission,
// delivery, retries, and acknowledgment tracking from here; the pass keeps
// owning iterator derivation, anchor verification, and read pacing.
//
// It reports the last fetched sequence only when the whole page was admitted.
// An expired-iterator refresh resumes from that sequence, so a page that was
// staged and then discarded must not advance it past records nothing delivered.
func (c *Consumer) processExplicitPage(
	admissionCtx context.Context,
	processor *explicitProcessor,
	out *kinesis.GetRecordsOutput,
	slot *admissionReservation,
) (string, error) {
	lastSeq := ""
	if n := len(out.Records); n > 0 {
		lastSeq = aws.ToString(out.Records[n-1].SequenceNumber)
	}
	if err := processor.processPage(admissionCtx, out.Records, slot); err != nil {
		if c.isDraining() && errors.Is(err, context.Canceled) {
			// Shutdown stops admission before it stops anything else, so the
			// rest of the page is discarded unhandled. Discarding is the point:
			// marking it complete would checkpoint past records this worker
			// never delivered, and admitting it would defeat the drain.
			return "", nil
		}
		return "", err
	}
	if lastSeq != "" {
		// The whole page was admitted, so this is where the shard now stands
		// for the rest of this acquisition, whatever the checkpoint says.
		processor.recordFetched(lastSeq)
		// Health().Processing.LastRecordProcessed: every delivery in the page
		// reached its handler and the handler returned. Acknowledgment and
		// persistence are reported separately.
		c.processingHealth.recordProcessed(time.Now())
	}
	return lastSeq, nil
}

// completeShard persists a closed shard's terminal completion marker.
//
// Explicit mode waits for every outstanding acknowledgment first and writes
// through its single checkpoint writer, so a closed shard's children are
// released only once this shard's own progress is durable.
func (c *Consumer) completeShard(ctx context.Context, shardID, lastSeq string) error {
	if processor := explicitProcessorFrom(ctx); processor != nil {
		return processor.complete(ctx, lastSeq)
	}
	return c.saveShardCompletionCheckpoint(ctx, shardID, lastSeq)
}

// finishShardDrain ends one shard's graceful drain. Automatic mode flushes the
// records it processed since its last checkpoint. Explicit mode instead waits
// for every admitted delivery to be acknowledged and then flushes the
// contiguous completed prefix.
//
// Both run under the live worker context, not the admission context that drain
// already cancelled, so the flush lands while the lease is still held and
// before the worker releases it.
func (c *Consumer) finishShardDrain(ctx context.Context, shardID, lastSeq string, processedSinceCheckpoint int) error {
	if processor := explicitProcessorFrom(ctx); processor != nil {
		return processor.drain(ctx)
	}
	return c.checkpointOnDrain(ctx, shardID, lastSeq, processedSinceCheckpoint)
}
