package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) startRegisteredShardWorker(
	ctx context.Context,
	shardID string,
	shardLease lease.Lease,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) {
	workerBaseCtx := ctx
	if c.gracefulDrain {
		workerBaseCtx = context.Background()
	}
	workerCtx, stopWorker := context.WithCancel(workerBaseCtx)
	workers.add(shardID, stopWorker)

	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done(shardID)

		if err := c.runShardWorker(workerCtx, shardID, shardLease); err != nil {
			select {
			case workerErrCh <- err:
			default:
			}
			if stopRun != nil {
				stopRun()
			}
		}
	}()
}

func (c *Consumer) startRegisteredShardWorkers(
	ctx context.Context,
	shardLeases map[string]lease.Lease,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) {
	for shardID, shardLease := range shardLeases {
		c.startRegisteredShardWorker(ctx, shardID, shardLease, workers, workerWG, workerErrCh, stopRun)
	}
}

func (c *Consumer) acquireAndStartReadyShardWorkers(
	ctx context.Context,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) error {
	readyShardIDs, err := completionState.readyShardIDs(ctx, c, knownShards)
	if err != nil {
		return err
	}

	acquireShardIDs := make([]string, 0, len(readyShardIDs))
	for _, shardID := range readyShardIDs {
		if workers.has(shardID) {
			continue
		}
		acquireShardIDs = append(acquireShardIDs, shardID)
	}

	shardLeases, err := c.acquireShardLeases(ctx, acquireShardIDs)
	if err != nil {
		return err
	}
	c.startRegisteredShardWorkers(ctx, shardLeases, workers, workerWG, workerErrCh, stopRun)
	return nil
}

func (c *Consumer) refreshAndStartReadyShardWorkers(
	ctx context.Context,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) error {
	if err := c.refreshKnownShards(ctx, knownShards); err != nil {
		return err
	}
	return c.acquireAndStartReadyShardWorkers(
		ctx,
		knownShards,
		completionState,
		workers,
		workerWG,
		workerErrCh,
		stopRun,
	)
}

func (c *Consumer) refreshAndStartReadyShardWorkersLoop(
	ctx context.Context,
	interval time.Duration,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
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
		}
	}
}
