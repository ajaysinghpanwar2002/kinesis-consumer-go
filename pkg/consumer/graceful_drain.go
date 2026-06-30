package consumer

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var errGracefulDrainTimeout = errors.New("graceful drain timed out")

func drainShardWorkers(
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	timeout time.Duration,
) error {
	return drainShardWorkersOrError(workers, workerWG, timeout, nil)
}

func drainShardWorkersOrError(
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	timeout time.Duration,
	workerErrCh <-chan error,
) error {
	if workerWG == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		workerWG.Wait()
		close(done)
	}()

	if timeout <= 0 {
		return waitForShardDrain(workers, workerWG, done, nil, 0, workerErrCh)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	return waitForShardDrain(workers, workerWG, done, timer.C, timeout, workerErrCh)
}

func (c *Consumer) drainShardWorkers(
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh <-chan error,
) error {
	if c == nil {
		return drainShardWorkersOrError(workers, workerWG, 0, workerErrCh)
	}
	c.draining.Store(true)
	defer c.draining.Store(false)

	return drainShardWorkersOrError(workers, workerWG, c.drainTimeout, workerErrCh)
}

func (c *Consumer) isDraining() bool {
	if c == nil {
		return false
	}
	return c.draining.Load()
}

func waitForShardDrain(
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	done <-chan struct{},
	timeout <-chan time.Time,
	timeoutAfter time.Duration,
	workerErrCh <-chan error,
) error {
	for {
		select {
		case <-done:
			if err := pendingShardDrainError(workerErrCh); err != nil {
				if workers != nil {
					workers.stopAll()
				}
				workerWG.Wait()
				return err
			}
			return nil
		case err, ok := <-workerErrCh:
			if !ok {
				workerErrCh = nil
				continue
			}
			if err == nil {
				continue
			}
			if workers != nil {
				workers.stopAll()
			}
			workerWG.Wait()
			return err
		case <-timeout:
			if workers != nil {
				workers.stopAll()
			}
			workerWG.Wait()
			return fmt.Errorf("%w after %s", errGracefulDrainTimeout, timeoutAfter)
		}
	}
}

func pendingShardDrainError(workerErrCh <-chan error) error {
	select {
	case err, ok := <-workerErrCh:
		if !ok {
			return nil
		}
		return err
	default:
		return nil
	}
}
