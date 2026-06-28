package consumer

import (
	"context"
	"errors"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) runShardWorker(ctx context.Context, shardID string, shardLease lease.Lease) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	renewCtx, stopRenew := context.WithCancel(workerCtx)
	renewErrCh := make(chan error, 1)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		if err := c.renewShardLeaseLoop(renewCtx, shardID, shardLease); err != nil {
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

	if releaseErr := c.releaseShardLease(context.Background(), shardID, shardLease); releaseErr != nil && err == nil {
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
