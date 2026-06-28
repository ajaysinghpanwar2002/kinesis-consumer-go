package consumer

import (
	"context"
	"sync"

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
