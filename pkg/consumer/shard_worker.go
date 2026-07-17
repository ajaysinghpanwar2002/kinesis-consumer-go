package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) runShardWorker(ctx context.Context, shardID string, shardLease lease.Lease) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var leaseLost atomic.Bool
	renewCtx, stopRenew := context.WithCancel(workerCtx)
	renewErrCh := make(chan error, 1)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		if err := c.renewShardLeaseLoopWithWatchdog(renewCtx, shardID, shardLease); err != nil {
			if errors.Is(err, lease.ErrNotOwned) {
				// A peer claimed the shard. Ownership loss is local to this
				// worker: the peer resumes from the last checkpoint, so stop
				// processing promptly without failing the whole consumer run.
				leaseLost.Store(true)
				c.reporter.Counter(metricLeaseLost, 1, c.shardTags(shardID))
				c.logger.Info("shard lease lost; stopping worker",
					slog.String("shard", shardID))
				cancel()
				return
			}
			select {
			case renewErrCh <- err:
			default:
			}
			cancel()
		}
	}()

	processErrCh := make(chan error, 1)
	processDone := make(chan struct{})
	go func() {
		defer close(processDone)
		if err := c.runShardRecordsLoop(workerCtx, shardID); err != nil {
			select {
			case processErrCh <- err:
			default:
			}
		}
		cancel()
	}()

	var err error
	select {
	case err = <-renewErrCh:
		cancel()
	case err = <-processErrCh:
		cancel()
	case <-workerCtx.Done():
		if !errors.Is(workerCtx.Err(), context.Canceled) {
			err = workerCtx.Err()
		}
		select {
		case renewErr := <-renewErrCh:
			err = renewErr
		default:
		}
		select {
		case processErr := <-processErrCh:
			if err == nil {
				err = processErr
			}
		default:
		}
	}

	stopRenew()
	<-renewDone
	<-processDone

	if err == nil {
		select {
		case err = <-renewErrCh:
		default:
		}
	}
	if err == nil {
		select {
		case err = <-processErrCh:
		default:
		}
	}

	if leaseLost.Load() {
		// The lease belongs to a peer now — there is nothing to release, and a
		// release attempt would only fail ErrNotOwned and pollute the failure
		// counters on a clean shard-local handoff. Processing may report the
		// cancellation we issued to stop it; that is part of the handoff, not an
		// independent worker failure. Preserve every other processing error.
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	// A caller/run stop can make a context-aware handler or DLQ attempt return
	// context.Canceled before Start's runCtx.Done branch wins its select. That
	// cancellation is the requested worker stop, not an independent fatal
	// worker error. Only normalize it when the parent worker context is also
	// canceled; a handler that returns context.Canceled while its context is
	// live still follows retries/failure policy and remains a real error.
	if errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
		err = nil
	}
	if releaseErr := c.releaseShardLeaseWithTimeout(shardID, shardLease); releaseErr != nil && err == nil {
		err = releaseErr
	}
	return err
}

func (c *Consumer) runShardRecordsLoop(ctx context.Context, shardID string) error {
	process := c.processShardRecordsLoop
	if c.processShardRecordsLoopFn != nil {
		process = c.processShardRecordsLoopFn
	}

	_, _, err := process(ctx, shardID)
	return err
}
