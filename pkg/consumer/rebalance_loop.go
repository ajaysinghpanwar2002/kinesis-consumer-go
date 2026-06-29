package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func (c *Consumer) refreshAndRebalanceShardWorkersLoop(
	ctx context.Context,
	shardSyncInterval time.Duration,
	rebalanceInterval time.Duration,
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

	shardSyncTicker := time.NewTicker(shardSyncInterval)
	defer shardSyncTicker.Stop()

	rebalanceTicker := time.NewTicker(rebalanceInterval)
	defer rebalanceTicker.Stop()

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
		case <-rebalanceTicker.C:
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
		}
	}
}
