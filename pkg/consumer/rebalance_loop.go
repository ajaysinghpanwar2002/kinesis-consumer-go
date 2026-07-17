package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
)

func newRebalanceDelayFunc(min, jitter time.Duration) func() time.Duration {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func() time.Duration {
		return rebalanceDelay(min, jitter, rng)
	}
}

func rebalanceDelay(min, jitter time.Duration, rng *rand.Rand) time.Duration {
	if jitter <= 0 || rng == nil {
		return min
	}
	return min + time.Duration(rng.Int63n(int64(jitter)))
}

// Backoff for failed shard-sync passes: capped exponential, reset by the next
// successful pass. A failed pass retries sooner than the regular sync
// interval so a blip recovers quickly, but never sooner than the backoff says
// (protecting a struggling dependency) and never later than the regular
// interval (the normal cadence is the ceiling).
const (
	shardSyncRetryBackoffBase = time.Second
	shardSyncRetryBackoffMax  = 30 * time.Second
)

// shardSyncRetryDelay computes the delay before retrying a failed sync pass:
// exponential in the consecutive-failure count from
// shardSyncRetryBackoffBase, plus up to 50% jitter (nil rng disables jitter
// for determinism), capped at both shardSyncRetryBackoffMax and interval.
func shardSyncRetryDelay(consecutiveFailures int, interval time.Duration, rng *rand.Rand) time.Duration {
	delay := shardSyncRetryBackoffBase
	for i := 1; i < consecutiveFailures; i++ {
		delay *= 2
		if delay >= shardSyncRetryBackoffMax {
			delay = shardSyncRetryBackoffMax
			break
		}
	}
	if rng != nil {
		delay += time.Duration(rng.Int63n(int64(delay/2) + 1))
	}
	if delay > shardSyncRetryBackoffMax {
		delay = shardSyncRetryBackoffMax
	}
	if delay > interval {
		delay = interval
	}
	return delay
}

// fatalShardSyncError reports whether a failed sync pass points at broken
// authorization or configuration that no retry can repair, so the run must
// stop now instead of riding the staleness bound. Kinesis client faults
// qualify, except codes that are transient by nature (throttling, limits,
// timeouts, expired pagination tokens). Everything else — server faults,
// network errors, checkpoint/lease backend failures — is survivable and
// bounded only by WithShardSyncMaxStaleness.
func fatalShardSyncError(err error) bool {
	var accessDenied *types.AccessDeniedException
	var notFound *types.ResourceNotFoundException
	var invalidArg *types.InvalidArgumentException
	var validation *types.ValidationException
	if errors.As(err, &accessDenied) || errors.As(err, &notFound) ||
		errors.As(err, &invalidArg) || errors.As(err, &validation) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorFault() == smithy.FaultClient {
		switch apiErr.ErrorCode() {
		case "ThrottlingException", "LimitExceededException",
			"ProvisionedThroughputExceededException", "RequestTimeout",
			"RequestTimeoutException", "ExpiredNextTokenException":
			return false
		}
		return true
	}
	return false
}

func (c *Consumer) refreshAndRebalanceShardWorkersLoop(
	ctx context.Context,
	shardSyncInterval time.Duration,
	nextRebalanceDelay func() time.Duration,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
	now func() time.Time,
) error {
	if now == nil {
		now = time.Now
	}
	if nextRebalanceDelay == nil {
		nextRebalanceDelay = newRebalanceDelayFunc(
			c.tuning.rebalanceIntervalMin,
			c.tuning.rebalanceIntervalJitter,
		)
	}

	// The staleness clock anchors at the loop start: in a real run Start's
	// initial discovery has just succeeded, and anchor only fills a zero
	// LastSuccess so an earlier recorded success is preserved.
	c.syncHealth.anchor(now())
	syncRng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// A timer instead of a ticker so a failed pass can be retried on the
	// backoff schedule rather than waiting out a full sync interval.
	syncTimer := time.NewTimer(shardSyncInterval)
	defer syncTimer.Stop()

	rebalanceTimer := time.NewTimer(nextRebalanceDelay())
	defer rebalanceTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-syncTimer.C:
			if err := c.refreshAndStartReadyShardWorkers(
				ctx,
				knownShards,
				completionState,
				cooldown,
				now(),
				workers,
				workerWG,
				workerErrCh,
				stopRun,
			); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					// Shutdown, not a sync failure: keep the loop's exit
					// semantics and never count or log it as unhealthy.
					if errors.Is(ctxErr, context.Canceled) {
						return nil
					}
					return ctxErr
				}
				if fatalShardSyncError(err) {
					return err
				}
				// Survivable: the last known shard map and every running
				// worker stay untouched; only discovery of *new* shards is
				// delayed, bounded by the staleness policy below.
				failures, lastSuccess := c.syncHealth.recordFailure(err)
				c.reporter.Counter(metricShardSyncFailures, 1, c.streamTags())
				staleness := now().Sub(lastSuccess)
				if c.tuning.shardSyncMaxStaleness > 0 && staleness > c.tuning.shardSyncMaxStaleness {
					return fmt.Errorf("%w for %s (max %s): %w",
						ErrShardSyncStale, staleness.Round(time.Millisecond), c.tuning.shardSyncMaxStaleness, err)
				}
				retryDelay := shardSyncRetryDelay(failures, shardSyncInterval, syncRng)
				c.logger.Warn("shard sync failed",
					slog.Any("error", err),
					slog.Int("consecutive_failures", failures),
					slog.Duration("staleness", staleness),
					slog.Duration("retry_delay", retryDelay),
				)
				syncTimer.Reset(retryDelay)
				continue
			}
			c.syncHealth.recordSuccess(now())
			syncTimer.Reset(shardSyncInterval)
		case <-rebalanceTimer.C:
			if _, err := c.rebalanceShardsOnce(
				ctx,
				knownShards,
				completionState,
				cooldown,
				workers,
				workerWG,
				workerErrCh,
				stopRun,
				now(),
			); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					if errors.Is(ctxErr, context.Canceled) {
						return nil
					}
					return ctxErr
				}
				// A failed pass retries at the next tick, so it is survivable —
				// but never silent: a half-broken lease backend would otherwise
				// stop all rebalancing with zero diagnostics. Shard-sync
				// failures (above) are likewise survivable but tracked
				// separately: they carry the staleness bound that eventually
				// stops the run, because invisible resharding — not a skipped
				// rebalance — is what makes a stale view dangerous.
				c.reporter.Counter(metricRebalancePassFailures, 1, c.streamTags())
				c.logger.Warn("rebalance pass failed", slog.Any("error", err))
			}
			rebalanceTimer.Reset(nextRebalanceDelay())
		}
	}
}
