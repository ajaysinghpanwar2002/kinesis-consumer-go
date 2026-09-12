package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
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

// getRecordsRetryDelay is getRecordsBackoff plus up to 50% jitter (nil rng
// disables jitter for determinism), capped at getRecordsBackoffMax —
// matching the shard-sync retry path (shardSyncRetryDelay) so shard readers
// throttled by the same event don't retry in lockstep and re-manufacture the
// throttling that triggered the backoff.
func getRecordsRetryDelay(failures int, rng *rand.Rand) time.Duration {
	delay := getRecordsBackoff(failures)
	if rng != nil && delay > 0 {
		delay += time.Duration(rng.Int63n(int64(delay/2) + 1))
	}
	if delay > getRecordsBackoffMax {
		delay = getRecordsBackoffMax
	}
	return delay
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
// stored checkpoint — or, with no checkpoint, from TRIM_HORIZON for a reshard
// child with a known parent and from the configured StartPosition otherwise
// (see getShardIterator). A fenced worker resuming from a recovery anchor gets
// back the page that proved the anchor readable instead of an iterator, and
// handles it before reading again.
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
	admissionCtx, stopAdmission := c.admissionContext(ctx)
	defer stopAdmission()
	var slot *admissionReservation
	defer func() { slot.release() }()
	lastSeq := ""
	count := processedSinceCheckpoint
	// Slot waits during both polling and iterator recovery must flush with the
	// live worker context; drain only cancels the admission context.
	flushBeforeWait := func() error {
		if count > 0 && lastSeq != "" {
			if err := c.saveShardCheckpoint(ctx, shardID, lastSeq); err != nil {
				return err
			}
			count = 0
		}
		return nil
	}
	readFailures := 0
	session := shardSessionFrom(ctx)
	processor := explicitProcessorFrom(ctx)
	// resumeSequence is where an expired iterator refreshes from. It is not
	// lastSeq: lastSeq is this pass's own progress, and the loop reads an empty
	// one as "nothing happened, wait a poll interval". Passes are not the unit
	// of ownership though — the acquisition is — so in explicit mode, where
	// progress persists long after it is fetched, the position comes from the
	// processor. Seeding lastSeq with it instead would make every idle pass
	// look productive and drop the poll interval entirely.
	resumeSequence := func() string {
		if processor != nil {
			return processor.lastFetched()
		}
		return lastSeq
	}
	var lastReadAt time.Time
	// A page already read on this shard's behalf — recovery reads the anchor
	// itself — that still has to be handled. It is only ever held together with
	// an empty iterator, so a pass that ends before handling it leaves the next
	// one to derive the position again from unchanged recovery state, rather
	// than resuming past records it never delivered.
	var pending *pendingShardPage
	defer func() {
		if pending != nil {
			pending.slot.release()
		}
	}()
	// Lazily seeded on the first retryable read failure; healthy passes never
	// pay for it.
	var backoffRng *rand.Rand

	for {
		slot.release()
		slot = nil
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

		if iterator == "" && pending == nil {
			derived, derivedPage, err := c.getShardIterator(admissionCtx, shardID)
			if err != nil {
				if c.isDraining() && errors.Is(err, context.Canceled) {
					return lastSeq, count, iterator, nil
				}
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
			}
			if derived == "" && derivedPage == nil {
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: empty shard iterator", shardID)
			}
			iterator, pending = derived, derivedPage
		}

		var out *kinesis.GetRecordsOutput
		// Set when this page held nothing but the anchor an exclusive
		// continuation dropped. It is empty because of what was removed from
		// it, so it says nothing about the shard being caught up.
		emptiedPage := false
		if pending != nil {
			// Recovery already read this page to prove its anchor. Handling it
			// here, instead of reading again from a fresh iterator, is what
			// keeps the resume point exact.
			out = pending.output
			slot = pending.slot
			lastReadAt = pending.readAt
			emptiedPage = pending.emptied
			c.reporter.Timing(metricGetRecordsDuration, pending.took, c.shardTags(shardID))
			pending = nil
		} else {
			// Pace successive reads: the Kinesis limit is 5 reads/sec/shard, and a
			// catch-up loop with zero delay between non-empty pages manufactures
			// the throttling that would otherwise kill the pass.
			if wait := c.tuning.idleTimeBetweenReads - time.Since(lastReadAt); !lastReadAt.IsZero() && wait > 0 {
				if err := c.sleep(admissionCtx, wait); err != nil {
					if errors.Is(ctx.Err(), context.Canceled) || c.isDraining() {
						return lastSeq, count, iterator, nil
					}
					return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
				}
			}

			var slotErr error
			slot, slotErr = c.admission.acquireBeforeWait(admissionCtx, shardID, nil, true, flushBeforeWait)
			if slotErr != nil {
				if errors.Is(slotErr, context.Canceled) && (c.isDraining() || errors.Is(ctx.Err(), context.Canceled)) {
					return lastSeq, count, iterator, nil
				}
				return lastSeq, count, "", slotErr
			}
			getRecordsStart := time.Now()
			fetched, err := c.getRecords(admissionCtx, iterator)
			lastReadAt = time.Now()
			if err != nil {
				slot.release()
				slot = nil
				if errors.Is(ctx.Err(), context.Canceled) || c.isDraining() {
					return lastSeq, count, iterator, nil
				}
				// Count every failed read (per attempt) by kind; shutdown
				// cancellation above is not a failure.
				c.reporter.Counter(metricGetRecordsFailures, 1,
					c.shardTags(shardID, metrics.Tag{Key: metricTagKind, Value: getRecordsErrorKind(err)}))
				var expired *types.ExpiredIteratorException
				if errors.As(err, &expired) {
					// The held iterator outlived its ~5-minute TTL (e.g. a large
					// pollInterval or a slow handler stretched the gap between
					// reads).
					if resume := resumeSequence(); session != nil && resume != "" {
						// Still the same worker generation, so resume from what
						// this worker last fetched. Re-reading persisted state
						// would replay every record since the last checkpoint; a
						// successor, which has no local position, still recovers
						// from persisted state.
						_, refreshed, refreshErr := c.anchoredShardRead(admissionCtx, shardID, resume, false, flushBeforeWait)
						if refreshErr != nil {
							if c.isDraining() && errors.Is(refreshErr, context.Canceled) {
								return lastSeq, count, iterator, nil
							}
							return lastSeq, count, "", fmt.Errorf("process shard records pass expired-iterator refresh %s: %w", shardID, refreshErr)
						}
						iterator, pending = "", refreshed
						continue
					}
					// Re-derive on the next iteration instead of failing the
					// shard. Unfenced re-derivation reads only the *stored*
					// checkpoint, so flush unsaved in-memory progress first: with
					// StartLatest and no checkpoint yet, a re-derived LATEST
					// iterator would re-anchor to the current tip and silently skip
					// everything since the last processed page (LIB-2); with a
					// checkpoint, stale progress replays needlessly. A fenced
					// worker that has fetched nothing has no progress to flush and
					// recovers its exact position from the session instead.
					if session == nil && count > 0 && lastSeq != "" && ctx.Err() == nil {
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
					if backoffRng == nil {
						backoffRng = rand.New(rand.NewSource(time.Now().UnixNano()))
					}
					backoff := getRecordsRetryDelay(readFailures, backoffRng)
					c.logger.Warn("get records failed; backing off",
						slog.String("shard", shardID),
						slog.Int("consecutive_failures", readFailures),
						slog.Duration("backoff", backoff),
						slog.Any("error", err),
					)
					if err := c.sleep(admissionCtx, backoff); err != nil {
						if errors.Is(ctx.Err(), context.Canceled) || c.isDraining() {
							return lastSeq, count, iterator, nil
						}
						return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
					}
					continue
				}
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
			}
			c.reporter.Timing(metricGetRecordsDuration, time.Since(getRecordsStart), c.shardTags(shardID))
			out = c.ownRecordsOutput(fetched)
		}
		readFailures = 0
		// Health().Processing.LastReadSuccess: every successful read counts,
		// including empty tip pages — the signal is "the delivery loop is
		// turning", not "records arrived".
		c.processingHealth.recordRead(lastReadAt)
		c.reporter.Counter(metricPagesFetched, 1, c.shardTags(shardID))
		if out.MillisBehindLatest != nil {
			c.reporter.Gauge(metricMillisBehindLatest, float64(*out.MillisBehindLatest), c.shardTags(shardID))
		}

		if session != nil && len(out.Records) > 0 && session.needsInitialPosition() {
			// First-record protection starts here: the shard's first observed
			// sequence becomes its inclusive replay position before any record
			// of this page can be admitted.
			resume, initErr := c.initializeShardRecovery(ctx, shardID, session, out.Records[0])
			if initErr != nil {
				return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, initErr)
			}
			if resume {
				iterator = ""
				continue
			}
		}

		var pageLastSeq string
		var err error
		if processor != nil {
			// The processor owns admission, delivery, and checkpointing for
			// this shard, so the pass keeps no processed-since-checkpoint count.
			pageLastSeq, err = c.processExplicitPage(admissionCtx, processor, out, slot)
		} else {
			pageLastSeq, count, err = c.processBoundedPage(ctx, admissionCtx, shardID, out, count, slot, lastSeq)
		}
		if pageLastSeq != "" {
			lastSeq = pageLastSeq
		}
		slot.release()
		slot = nil
		if err != nil {
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, err)
		}

		if c.isDraining() {
			// The loop's drain path owns the drain checkpoint.
			return lastSeq, count, iterator, nil
		}

		if pageEndsShard(out) {
			if err := c.completeShard(ctx, shardID, lastSeq); err != nil {
				return lastSeq, count, "", fmt.Errorf("process shard records pass completion checkpoint %s: %w", shardID, err)
			}
			return lastSeq, count, "", fmt.Errorf("process shard records pass %s: %w", shardID, errShardCompleted)
		}

		iterator = aws.ToString(out.NextShardIterator)

		// getRecords rejects nil outputs, and NextShardIterator was just
		// dereferenced above, so an empty page is the only caught-up signal to
		// check here.
		if len(out.Records) == 0 {
			if emptiedPage {
				// Not the shard tip: this page carried only the anchor, and
				// the exclusive continuation dropped it. With one record per
				// page — WithBatching(1, ...) — that is every checkpoint
				// resumption. Reading it as "caught up" would sleep a poll
				// interval before every resumption, and a poll interval longer
				// than the iterator's ~5-minute life would expire the iterator
				// each time, so the shard would re-verify the same anchor
				// forever and never deliver anything. Read on from this page
				// instead.
				continue
			}
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
