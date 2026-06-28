package consumer

import (
	"context"
	"sync"

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
	workerCtx, stopWorker := context.WithCancel(ctx)
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
