package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// FailurePolicy controls behavior when a handler keeps failing after retries.
// The default is FailurePolicyFailFast.
type FailurePolicy string

const (
	// FailurePolicyFailFast stops the consumer without checkpointing the failed
	// page and surfaces the handler error through Start.
	FailurePolicyFailFast FailurePolicy = "fail-fast"
	// FailurePolicySkip intentionally drops failed record-handler records or an
	// entire failed batch-handler page and allows the page checkpoint to advance.
	FailurePolicySkip FailurePolicy = "skip"
	// FailurePolicySendToDLQ publishes failed records before allowing the page
	// checkpoint to advance. Granularity follows the handler mode: a record
	// handler publishes only the failed record, while a batch handler fails
	// the whole indivisible page, so every record in that GetRecords page —
	// including ones that were not themselves poison — is published with
	// batch metadata. It then checkpoints past the page immediately once it
	// completes (instead of waiting for the next due checkpoint) to keep the
	// crash-replay window small. The residual window cannot be closed: a crash
	// between a successful publish and that checkpoint replays the page, so a
	// record can end up both in the DLQ and fully processed downstream.
	// Deduplicate on PoisonRecord.IdempotencyKey.
	FailurePolicySendToDLQ FailurePolicy = "send-to-dlq"
)

const (
	handlerKindRecord = "record"
	handlerKindBatch  = "batch"
)

const (
	defaultDLQRetryAttempts  = 3
	defaultDLQRetryBackoff   = time.Second
	defaultDLQAttemptTimeout = 10 * time.Second
	idempotencyKeyPrefix     = "kcg-dlq-v1:"
)

// PoisonRecord is sent to a DLQ when a record is considered poison.
type PoisonRecord struct {
	// IdempotencyKey is stable for one source record and handler mode across
	// DLQ retries, source replays, and Consumer rebuilds. DLQ delivery remains
	// at-least-once; downstream publishers and consumers should deduplicate on
	// this key rather than treating a Publish call as exactly-once.
	IdempotencyKey             string     `json:"idempotency_key"`
	StreamName                 string     `json:"stream_name,omitempty"`
	StreamARN                  string     `json:"stream_arn,omitempty"`
	ConsumerGroup              string     `json:"consumer_group"`
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

// DLQPublisher publishes poison records to a dead-letter destination. Publish
// must honor ctx promptly, be safe for concurrent calls (including overlapping
// retries of the same IdempotencyKey), and tolerate duplicate keys. If Publish
// ignores cancellation or its attempt timeout, the call may continue after the
// consumer abandons it; a late success cannot advance the source checkpoint.
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
		return FailurePolicyFailFast
	}
	return c.failurePolicy
}

func (c *Consumer) effectiveDLQRetryAttempts() int {
	if c == nil || c.dlqRetryAttempts < 1 {
		return 1
	}
	return c.dlqRetryAttempts
}

func (c *Consumer) effectiveDLQRetryBackoff(attempt int) time.Duration {
	if c == nil || c.dlqRetryBackoff <= 0 || attempt < 1 {
		return 0
	}
	return time.Duration(attempt) * c.dlqRetryBackoff
}

func (c *Consumer) effectiveDLQAttemptTimeout() time.Duration {
	if c == nil || c.dlqAttemptTimeout <= 0 {
		return defaultDLQAttemptTimeout
	}
	return c.dlqAttemptTimeout
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
		err := c.callHandlerAttempt(shardID, handlerKind, fn)
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

// callHandlerAttempt invokes one handler attempt, converting a panic into an
// ordinary handler error so it flows into retry → failure policy instead of
// unwinding through the shard worker goroutine and killing the process (which
// would bypass retry/skip/DLQ, stop every shard on this worker, leak leases
// until TTL, and crash-loop on the same poison record after restart since
// nothing checkpointed past it).
func (c *Consumer) callHandlerAttempt(shardID, handlerKind string, fn func() error) (err error) {
	defer func() {
		// recover() alone decides: a nil return means normal (or Goexit)
		// unwinding and must not be converted.
		if r := recover(); r != nil {
			err = c.recoveredPanicError(shardID, handlerKind, r)
		}
	}()
	return fn()
}

// recoveredPanicError logs a recovered panic with its stack — the only place
// the offending handler frame is visible — and returns it as an error
// wrapping ErrHandlerPanic. A panic(err) value is wrapped too, so callers can
// still match the original error with errors.Is/errors.As.
//
// The error is constructed before, and independently of, the log call: this
// helper runs while a recovery guard is already unwinding, so a panicking
// slog.Handler must not be allowed to escape and kill the very process the
// conversion exists to protect. The log is best-effort and self-recovering.
func (c *Consumer) recoveredPanicError(shardID, handlerKind string, panicValue any) error {
	var err error
	if panicErr, ok := panicValue.(error); ok {
		err = fmt.Errorf("%w: %w", ErrHandlerPanic, panicErr)
	} else {
		err = fmt.Errorf("%w: %v", ErrHandlerPanic, panicValue)
	}
	func() {
		defer func() {
			// Swallow only a logger panic; the converted error already
			// carries the panic value, so nothing is lost but the log line.
			_ = recover()
		}()
		c.logger.Error("handler panic recovered",
			slog.String("shard", shardID),
			slog.String("handler", handlerKind),
			slog.Any("panic", panicValue),
			slog.String("stack", string(debug.Stack())),
		)
	}()
	return err
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
			dlqAttempts, err := c.publishPoisonRecordWithRetry(ctx, poison)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return fmt.Errorf("%w; dlq publish failed after %d attempts: %w", failFastErr, dlqAttempts, err)
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
		// Request an immediate page checkpoint flush so the published records
		// are not replayed (and re-delivered on both paths) by a crash before
		// the next due checkpoint.
		markDLQPagePublished(ctx)
		return nil
	default:
		return failFastErr
	}
}

func (c *Consumer) publishPoisonRecordWithRetry(ctx context.Context, poison PoisonRecord) (int, error) {
	maxAttempts := c.effectiveDLQRetryAttempts()
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempt - 1, err
		}
		if attempt > 1 {
			if err := sleepWithContext(ctx, c.effectiveDLQRetryBackoff(attempt-1)); err != nil {
				return attempt - 1, err
			}
		}

		lastErr = c.publishPoisonRecordAttempt(ctx, poison)
		if lastErr == nil {
			return attempt, nil
		}
		if err := ctx.Err(); err != nil {
			return attempt, err
		}
	}
	return maxAttempts, lastErr
}

func (c *Consumer) publishPoisonRecordAttempt(ctx context.Context, poison PoisonRecord) error {
	attemptCtx, cancel := context.WithTimeout(ctx, c.effectiveDLQAttemptTimeout())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- c.dlqPublisher.Publish(attemptCtx, poison)
	}()

	select {
	case err := <-result:
		// Attempt timeout or parent cancellation wins over a racing late
		// success. The caller's page-level fence provides the same protection
		// against cancellation after this return.
		if attemptErr := attemptCtx.Err(); attemptErr != nil {
			return attemptErr
		}
		return err
	case <-attemptCtx.Done():
		return attemptCtx.Err()
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

	var streamName, streamARN, consumerGroup, canonicalStreamName string
	if c != nil {
		streamName = c.cfg.StreamName
		streamARN = c.cfg.StreamARN
		consumerGroup = c.cfg.ConsumerGroup
		canonicalStreamName = c.canonicalStreamName()
	}

	return PoisonRecord{
		IdempotencyKey:             poisonRecordIdempotencyKey(consumerGroup, canonicalStreamName, shardID, aws.ToString(record.SequenceNumber), handlerKind),
		StreamName:                 streamName,
		StreamARN:                  streamARN,
		ConsumerGroup:              consumerGroup,
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

func poisonRecordIdempotencyKey(consumerGroup, streamName, shardID, sequenceNumber, handlerKind string) string {
	hash := sha256.New()
	var length [8]byte
	for _, field := range []string{consumerGroup, streamName, shardID, sequenceNumber, handlerKind} {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(field))
	}
	return idempotencyKeyPrefix + hex.EncodeToString(hash.Sum(nil))
}
