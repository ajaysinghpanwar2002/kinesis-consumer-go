package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// FailurePolicy controls behavior when a handler keeps failing after retries.
type FailurePolicy string

const (
	FailurePolicyFailFast  FailurePolicy = "fail-fast"
	FailurePolicySkip      FailurePolicy = "skip"
	FailurePolicySendToDLQ FailurePolicy = "send-to-dlq"
)

const (
	handlerKindRecord = "record"
	handlerKindBatch  = "batch"
)

// PoisonRecord is sent to a DLQ when a record is considered poison.
type PoisonRecord struct {
	StreamName                 string     `json:"stream_name,omitempty"`
	StreamARN                  string     `json:"stream_arn,omitempty"`
	ShardID                    string     `json:"shard_id"`
	SequenceNumber             string     `json:"sequence_number,omitempty"`
	PartitionKey               string     `json:"partition_key,omitempty"`
	ApproximateArrival         *time.Time `json:"approximate_arrival_timestamp,omitempty"`
	Payload                    []byte     `json:"payload,omitempty"`
	Error                      string     `json:"error"`
	Handler                    string     `json:"handler"`
	Attempts                   int        `json:"attempts"`
	FailedAt                   time.Time  `json:"failed_at"`
	BatchSize                  int        `json:"batch_size"`
	RecordIndexInBatch         int        `json:"record_index_in_batch"`
	RecordSequenceInBatchOrder int        `json:"record_sequence_in_batch_order"`
}

// DLQPublisher publishes poison records to a dead-letter destination.
type DLQPublisher interface {
	Publish(ctx context.Context, record PoisonRecord) error
}

func (p FailurePolicy) validate() error {
	switch p {
	case FailurePolicyFailFast, FailurePolicySkip, FailurePolicySendToDLQ:
		return nil
	case "":
		return errors.New("failure policy cannot be empty")
	default:
		return fmt.Errorf("invalid failure policy %q", p)
	}
}

func (c *Consumer) retryMaxAttempts() int {
	if c == nil || c.tuning.retryMaxAttempts < 1 {
		return 1
	}
	return c.tuning.retryMaxAttempts
}

func (c *Consumer) retryBackoff(attempt int) time.Duration {
	if c == nil || c.tuning.retryBackoff <= 0 || attempt < 1 {
		return 0
	}
	return time.Duration(attempt) * c.tuning.retryBackoff
}

func (c *Consumer) effectiveFailurePolicy() FailurePolicy {
	if c == nil || c.failurePolicy == "" {
		return FailurePolicySkip
	}
	return c.failurePolicy
}

func (c *Consumer) handleRecordWithRetry(ctx context.Context, shardID string, record Record) error {
	if c == nil || c.handler == nil {
		return errors.New("record handler is nil")
	}

	attempts, err := c.retryHandler(ctx, shardID, handlerKindRecord, func() error {
		return c.handler(ctx, record)
	})
	if err == nil {
		c.reporter.Counter(metricRecordsProcessed, 1,
			c.shardTags(shardID, metrics.Tag{Key: metricTagHandler, Value: handlerKindRecord}))
		return nil
	}
	if terminalErr, ok := terminalContextError(ctx); ok {
		return terminalErr
	}

	failFastErr := fmt.Errorf("record handler failed after %d attempts: %w", attempts, err)
	return c.applyFailurePolicy(ctx, shardID, handlerKindRecord, []Record{record}, attempts, err, failFastErr)
}

func (c *Consumer) handleBatchWithRetry(ctx context.Context, shardID string, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if c == nil || c.batchHandler == nil {
		return errors.New("batch handler is nil")
	}

	attempts, err := c.retryHandler(ctx, shardID, handlerKindBatch, func() error {
		return c.batchHandler(ctx, records)
	})
	if err == nil {
		c.reporter.Counter(metricRecordsProcessed, int64(len(records)),
			c.shardTags(shardID, metrics.Tag{Key: metricTagHandler, Value: handlerKindBatch}))
		return nil
	}
	if terminalErr, ok := terminalContextError(ctx); ok {
		return terminalErr
	}

	failFastErr := fmt.Errorf("batch handler failed after %d attempts: %w", attempts, err)
	return c.applyFailurePolicy(ctx, shardID, handlerKindBatch, records, attempts, err, failFastErr)
}

// terminalContextError reports whether the consumer's own shard context is
// done. Only that is terminal: a handler *returning* an error that matches
// context.Canceled or context.DeadlineExceeded (e.g. from an internal HTTP or
// DB timeout) is an ordinary handler failure and must follow the
// retry → failure-policy path, not stop the consumer.
func terminalContextError(ctx context.Context) (error, bool) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr, true
	}
	return nil, false
}

func (c *Consumer) retryHandler(ctx context.Context, shardID, handlerKind string, fn func() error) (int, error) {
	maxAttempts := c.retryMaxAttempts()
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			c.reporter.Counter(metricHandlerRetries, 1,
				c.shardTags(shardID, metrics.Tag{Key: metricTagHandler, Value: handlerKind}))
		}
		attemptStart := time.Now()
		err := fn()
		c.reporter.Timing(metricHandlerDuration, time.Since(attemptStart),
			c.shardTags(shardID, metrics.Tag{Key: metricTagHandler, Value: handlerKind}))
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return attempt, lastErr
			}
			if attempt == maxAttempts {
				return attempt, lastErr
			}
			if err := sleepWithContext(ctx, c.retryBackoff(attempt)); err != nil {
				return attempt, err
			}
			continue
		}
		return attempt, nil
	}
	return maxAttempts, lastErr
}

func (c *Consumer) applyFailurePolicy(
	ctx context.Context,
	shardID string,
	handlerKind string,
	records []Record,
	attempts int,
	cause error,
	failFastErr error,
) error {
	switch c.effectiveFailurePolicy() {
	case FailurePolicySkip:
		c.reporter.Counter(metricRecordsSkipped, int64(len(records)),
			c.shardTags(shardID, metrics.Tag{Key: metricTagPolicy, Value: string(FailurePolicySkip)}))
		c.logger.Warn("records skipped after handler failure",
			slog.String("shard", shardID),
			slog.String("handler", handlerKind),
			slog.Int("records", len(records)),
			slog.Int("attempts", attempts),
			slog.Any("error", cause),
		)
		return nil
	case FailurePolicySendToDLQ:
		if c == nil || c.dlqPublisher == nil {
			return failFastErr
		}
		for i, record := range records {
			poison := c.newPoisonRecord(shardID, handlerKind, attempts, cause, record, len(records), i)
			if err := c.dlqPublisher.Publish(ctx, poison); err != nil {
				return fmt.Errorf("%w; dlq publish: %w", failFastErr, err)
			}
			c.reporter.Counter(metricDLQRecordsPublished, 1, c.shardTags(shardID))
		}
		c.logger.Warn("poison records published to dlq",
			slog.String("shard", shardID),
			slog.String("handler", handlerKind),
			slog.Int("records", len(records)),
			slog.Int("attempts", attempts),
			slog.Any("error", cause),
		)
		return nil
	default:
		return failFastErr
	}
}

func (c *Consumer) newPoisonRecord(
	shardID string,
	handlerKind string,
	attempts int,
	cause error,
	record types.Record,
	batchSize int,
	batchIndex int,
) PoisonRecord {
	var approx *time.Time
	if record.ApproximateArrivalTimestamp != nil {
		t := *record.ApproximateArrivalTimestamp
		approx = &t
	}

	payload := make([]byte, len(record.Data))
	copy(payload, record.Data)

	var streamName, streamARN string
	if c != nil {
		streamName = c.cfg.StreamName
		streamARN = c.cfg.StreamARN
	}

	return PoisonRecord{
		StreamName:                 streamName,
		StreamARN:                  streamARN,
		ShardID:                    shardID,
		SequenceNumber:             aws.ToString(record.SequenceNumber),
		PartitionKey:               aws.ToString(record.PartitionKey),
		ApproximateArrival:         approx,
		Payload:                    payload,
		Error:                      cause.Error(),
		Handler:                    handlerKind,
		Attempts:                   attempts,
		FailedAt:                   time.Now().UTC(),
		BatchSize:                  batchSize,
		RecordIndexInBatch:         batchIndex,
		RecordSequenceInBatchOrder: batchIndex,
	}
}
