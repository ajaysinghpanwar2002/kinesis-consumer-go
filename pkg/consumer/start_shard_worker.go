package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
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
	workerGen := workers.add(shardID, stopWorker)

	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done(shardID, workerGen)

		c.reporter.Counter(metricWorkerStarts, 1, c.shardTags(shardID))
		c.logger.Info("shard worker started", slog.String("shard", shardID))

		err := c.runShardWorker(workerCtx, shardID, shardLease)
		if err != nil {
			c.reporter.Counter(metricWorkerStops, 1,
				c.shardTags(shardID, metrics.Tag{Key: metricTagOutcome, Value: metricOutcomeError}))
			// The worker's aggregated error (renew or process failure) is logged
			// here per-shard. workerErrCh only keeps the first error (non-blocking
			// send), so this Warn is the sole record of any subsequent concurrent
			// worker failure during shutdown. Consumer-level Error stays in Start.
			c.logger.Warn("shard worker stopped", slog.String("shard", shardID), slog.Any("error", err))
			select {
			case workerErrCh <- err:
			default:
			}
			if stopRun != nil {
				stopRun()
			}
			return
		}
		c.reporter.Counter(metricWorkerStops, 1,
			c.shardTags(shardID, metrics.Tag{Key: metricTagOutcome, Value: metricOutcomeClean}))
		c.logger.Info("shard worker stopped", slog.String("shard", shardID))
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

// acquireAndStartReadyShardWorkers fills this worker toward its fair-share
// high bound from unowned ready shards. It applies the same ownership
// snapshot, fair-share cap, and post-move cooldown as the rebalance planner:
// greedy acquisition past high would immediately be shed again (cold-start
// thundering herd), and ignoring cooldown lets the very worker that shed a
// shard re-acquire it at its next sync tick. The cooldown map needs no lock:
// the initial acquisition runs before the orchestration goroutine starts,
// and afterwards sync and rebalance ticks share that goroutine's select loop.
func (c *Consumer) acquireAndStartReadyShardWorkers(
	ctx context.Context,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	cooldown map[string]time.Time,
	now time.Time,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) error {
	readyShardIDs, err := completionState.readyShardIDs(ctx, c, knownShards)
	if err != nil {
		return err
	}
	if len(readyShardIDs) == 0 {
		return nil
	}

	leaseOwners, err := c.listRebalanceLeaseOwnersWithRetry(ctx)
	if err != nil {
		return err
	}
	workerOwners, err := c.listRebalanceWorkerOwnersWithRetry(ctx)
	if err != nil {
		return err
	}

	snapshot := buildRebalanceOwnershipSnapshot(readyShardIDs, leaseOwners, workerOwners, c.leaseOwner)
	acquireShardIDs := selectSyncAcquireShards(snapshot, c.leaseOwner, cooldown, workers, now)

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
	cooldown map[string]time.Time,
	now time.Time,
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
		cooldown,
		now,
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
	cooldown map[string]time.Time,
	now func() time.Time,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) error {
	if now == nil {
		now = time.Now
	}

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
				cooldown,
				now(),
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
