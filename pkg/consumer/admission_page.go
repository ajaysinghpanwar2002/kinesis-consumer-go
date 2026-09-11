package consumer

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type admittedRecordsKey struct{}

// admissionContext stops fetches and capacity waits during drain while the
// worker context remains live for admitted callbacks and checkpoints.
func (c *Consumer) admissionContext(ctx context.Context) (context.Context, func()) {
	if c.admission == nil {
		return ctx, func() {}
	}
	readCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.admission.stopCtx, cancel)
	if c.admission.stopCtx.Err() != nil {
		cancel()
	}
	return readCtx, func() { stop(); cancel() }
}

func (c *Consumer) ownRecordsOutput(out *kinesis.GetRecordsOutput) *kinesis.GetRecordsOutput {
	if c.admission == nil {
		return out
	}
	owned := *out
	owned.Records = append([]Record(nil), out.Records...)
	return &owned
}

// processBoundedPage checkpoints each completed ordered prefix before waiting
// for more capacity. The reservation is released by processRecordsPage before
// any checkpoint I/O. Only the staging slice is cleared; callbacks get their
// own slice and retain ownership of the payload buffers they receive.
func (c *Consumer) processBoundedPage(ctx, admissionCtx context.Context, shardID string, out *kinesis.GetRecordsOutput, count int, slot *admissionReservation, previousSeq string) (string, int, error) {
	if c.admission == nil {
		return c.processRecordsPageWithCheckpoint(ctx, shardID, out, count)
	}
	defer clear(out.Records)
	sizes := make([]int, len(out.Records))
	for i := range out.Records {
		sizes[i] = len(out.Records[i].Data)
	}
	lastSeq := previousSeq
	for offset := 0; offset < len(out.Records); {
		if admissionCtx.Err() != nil || c.isDraining() {
			if c.isDraining() {
				return lastSeq, count, nil
			}
			return lastSeq, count, admissionCtx.Err()
		}
		limit := min(c.admission.limits.MaxBytesPerShard, c.admission.limits.MaxBytesPerInstance)
		if sizes[offset] > limit {
			return lastSeq, count, &OversizedRecordError{ShardID: shardID, SequenceNumber: aws.ToString(out.Records[offset].SequenceNumber), Bytes: sizes[offset], Limit: limit}
		}
		reservation, err := c.admission.acquireBeforeWait(admissionCtx, shardID, sizes[offset:], false, func() error {
			if count > 0 && lastSeq != "" {
				if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
					return err
				}
				count = 0
			}
			return nil
		})
		if err != nil {
			if c.isDraining() && errors.Is(err, context.Canceled) {
				return lastSeq, count, nil
			}
			return lastSeq, count, err
		}
		n := len(reservation.sizes)
		prefix := &kinesis.GetRecordsOutput{Records: append([]Record(nil), out.Records[offset:offset+n]...)}
		clear(out.Records[offset : offset+n])
		offset += n
		if offset == len(out.Records) {
			slot.release()
		}
		seq, nextCount, err := c.processRecordsPageWithCheckpoint(context.WithValue(ctx, admittedRecordsKey{}, reservation), shardID, prefix, count)
		if seq != "" {
			lastSeq = seq
		}
		count = nextCount
		if err != nil {
			return lastSeq, count, err
		}
	}
	return lastSeq, count, nil
}

func (c *Consumer) handlePageRecord(ctx context.Context, shardID string, records []Record, i int) error {
	if reservation, ok := ctx.Value(admittedRecordsKey{}).(*admissionReservation); ok {
		defer reservation.releaseRecord(i)
		defer func() { records[i] = Record{} }()
	}
	return c.handleRecordWithRetry(ctx, shardID, records[i])
}
