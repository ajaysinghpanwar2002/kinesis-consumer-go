package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) runShardWorker(ctx context.Context, shardID string, shardLease lease.Lease) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var leaseLostDuringDrain atomic.Bool
	renewCtx, stopRenew := context.WithCancel(workerCtx)
	renewErrCh := make(chan error, 1)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		if err := c.renewShardLeaseLoopWithWatchdog(renewCtx, shardID, shardLease); err != nil {
			if c.isDraining() && errors.Is(err, lease.ErrNotOwned) {
				// A peer claimed the shard mid-drain. That completes this
				// shard's drain: the peer resumes from the last checkpoint, so
				// stop processing promptly and report a clean stop instead of
				// failing the whole drain.
				leaseLostDuringDrain.Store(true)
				c.logger.Info("shard lease lost during drain; treating shard as drained",
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

	if leaseLostDuringDrain.Load() {
		// The lease belongs to a peer now — there is nothing to release, and a
		// release attempt would only fail ErrNotOwned and pollute the failure
		// counters on a clean drain.
		return err
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
