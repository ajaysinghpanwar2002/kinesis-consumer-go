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

	var err error
	select {
	case err = <-renewErrCh:
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
	}

	stopRenew()
	<-renewDone

	if err == nil {
		select {
		case err = <-renewErrCh:
		default:
		}
	}

	if releaseErr := c.releaseShardLease(context.Background(), shardID, shardLease); releaseErr != nil && err == nil {
		err = releaseErr
	}
	return err
}
