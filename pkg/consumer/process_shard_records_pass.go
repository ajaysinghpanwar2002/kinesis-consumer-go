package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

// Backoff for retryable GetRecords errors (throttling, server faults, network
// blips): capped exponential, reset by the next successful read. Retries are
// unbounded by design — throttling is an operating condition, not a consumer
// failure — and each retry re-checks ctx/drain at the top of the pass loop.
const (
	getRecordsBackoffBase = 500 * time.Millisecond
	getRecordsBackoffMax  = 10 * time.Second
)

func getRecordsBackoff(failures int) time.Duration {
	if failures < 1 {
		return 0
	}
	backoff := getRecordsBackoffBase
	for i := 1; i < failures; i++ {
		backoff *= 2
		if backoff >= getRecordsBackoffMax {
			return getRecordsBackoffMax
		}
	}
	return backoff
}

// retryableGetRecordsError classifies GetRecords errors the pass survives by
// backing off in-place: Kinesis throttling, server (5xx) faults, and network
// errors. Client faults (validation, auth, missing resources) stay fatal.
// Callers must gate on ctx.Err() == nil — context.DeadlineExceeded satisfies
// net.Error and must not be spun on when the consumer context itself is dead.
func retryableGetRecordsError(err error) bool {
	var throughputExceeded *types.ProvisionedThroughputExceededException
	var limitExceeded *types.LimitExceededException
	var kmsThrottled *types.KMSThrottlingException
	if errors.As(err, &throughputExceeded) || errors.As(err, &limitExceeded) || errors.As(err, &kmsThrottled) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorFault() == smithy.FaultServer {
			return true
		}
		switch apiErr.ErrorCode() {
		case "ThrottlingException", "InternalFailure", "ServiceUnavailable", "RequestTimeout":
			return true
		}
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// getRecordsErrorKind maps a failed GetRecords error to the fixed-enum `kind`
// tag on the get_records_failures counter: throttle, expired, or other.
func getRecordsErrorKind(err error) string {
	var expired *types.ExpiredIteratorException
	if errors.As(err, &expired) {
		return metricKindExpired
	}
	var throughputExceeded *types.ProvisionedThroughputExceededException
	var limitExceeded *types.LimitExceededException
	var kmsThrottled *types.KMSThrottlingException
	if errors.As(err, &throughputExceeded) || errors.As(err, &limitExceeded) || errors.As(err, &kmsThrottled) {
		return metricKindThrottle
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ThrottlingException" {
		return metricKindThrottle
	}
	return metricKindOther
}

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
	readFailures := 0
	var lastReadAt time.Time

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

		// Pace successive reads: the Kinesis limit is 5 reads/sec/shard, and a
		// catch-up loop with zero delay between non-empty pages manufactures
		// the throttling that would otherwise kill the pass.
		if wait := c.tuning.idleTimeBetweenReads - time.Since(lastReadAt); !lastReadAt.IsZero() && wait > 0 {
			if err := c.sleep(ctx, wait); err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return lastSeq, count, iterator, nil
				}
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
			}
		}

		getRecordsStart := time.Now()
		out, err := c.getRecords(ctx, iterator)
		lastReadAt = time.Now()
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return lastSeq, count, iterator, nil
			}
			// Count every failed read (per attempt) by kind; shutdown
			// cancellation above is not a failure.
			c.reporter.Counter(metricGetRecordsFailures, 1,
				c.shardTags(shardID, metrics.Tag{Key: metricTagKind, Value: getRecordsErrorKind(err)}))
			var expired *types.ExpiredIteratorException
			if errors.As(err, &expired) {
				// The held iterator outlived its ~5-minute TTL (e.g. a large
				// pollInterval or a slow handler stretched the gap between reads).
				// Drop it and re-derive from the stored checkpoint on the next
				// iteration instead of failing the shard. Re-derivation reads only
				// the *stored* checkpoint, so flush unsaved in-memory progress
				// first: with StartLatest and no checkpoint yet, a re-derived
				// LATEST iterator would re-anchor to the current tip and silently
				// skip everything since the last processed page (LIB-2); with a
				// checkpoint, stale progress replays needlessly.
				if count > 0 && lastSeq != "" && ctx.Err() == nil {
					if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
						return lastSeq, count, "", fmt.Errorf("process shard records pass expired-iterator checkpoint %s: %w", shardID, err)
					}
					count = 0
				}
				iterator = ""
				continue
			}
			if ctx.Err() == nil && retryableGetRecordsError(err) {
				// Throttling, a server fault, or a network blip: survive it
				// in-place instead of failing the shard (which would stop the
				// whole consumer). The same iterator stays valid for the retry.
				readFailures++
				backoff := getRecordsBackoff(readFailures)
				c.logger.Warn("get records failed; backing off",
					slog.String("shard", shardID),
					slog.Int("consecutive_failures", readFailures),
					slog.Duration("backoff", backoff),
					slog.Any("error", err),
				)
				if err := c.sleep(ctx, backoff); err != nil {
					if errors.Is(ctx.Err(), context.Canceled) {
						return lastSeq, count, iterator, nil
					}
					return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
				}
				continue
			}
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
		}
		readFailures = 0
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

		// getRecords rejects nil outputs, and NextShardIterator was just
		// dereferenced above, so an empty page is the only caught-up signal to
		// check here.
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
