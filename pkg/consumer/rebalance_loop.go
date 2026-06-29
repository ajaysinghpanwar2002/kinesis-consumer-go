package consumer

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
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

	shardSyncTicker := time.NewTicker(shardSyncInterval)
	defer shardSyncTicker.Stop()

	rebalanceTimer := time.NewTimer(nextRebalanceDelay())
	defer rebalanceTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-shardSyncTicker.C:
			if err := c.refreshAndStartReadyShardWorkers(
				ctx,
				knownShards,
				completionState,
				workers,
				workerWG,
				workerErrCh,
				stopRun,
			); err != nil {
				return err
			}
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
			}
			rebalanceTimer.Reset(nextRebalanceDelay())
		}
	}
}
