package consumer

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

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
		return waitForShardDrain(workers, done, nil, 0, workerErrCh)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	return waitForShardDrain(workers, done, timer.C, timeout, workerErrCh)
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

	start := time.Now()
	defer func() {
		c.reporter.Timing(metricDrainDuration, time.Since(start), c.streamTags())
	}()

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
	done <-chan struct{},
	timeout <-chan time.Time,
	timeoutAfter time.Duration,
	workerErrCh <-chan error,
) error {
	// One worker's failure must not abort the other shards' drain: force-
	// stopping them here would discard their drain checkpoints. Remember the
	// first error, let the healthy workers finish (the timeout still bounds
	// the wait), and surface it once the drain completes.
	var firstErr error
	for {
		select {
		case <-done:
			if firstErr == nil {
				firstErr = pendingShardDrainError(workerErrCh)
			}
			return firstErr
		case err, ok := <-workerErrCh:
			if !ok {
				workerErrCh = nil
				continue
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-timeout:
			if firstErr == nil {
				// Both channels can already be ready when select chooses the
				// timeout. Preserve an error buffered before force-stop, but do
				// not wait for errors produced by cancellation below.
				firstErr = pendingShardDrainError(workerErrCh)
			}
			if workers != nil {
				workers.stopAll()
			}
			// The goroutine waiting on workerWG above remains as the asynchronous
			// reaper. Do not wait again here: a handler or extension that ignores
			// its canceled context must not defeat the configured drain deadline.
			timeoutErr := fmt.Errorf("%w after %s", ErrDrainTimeout, timeoutAfter)
			if firstErr != nil {
				return errors.Join(firstErr, timeoutErr)
			}
			return timeoutErr
		}
	}
}

// stopAndReapShardWorkers signals every worker to stop and arranges for their
// eventual cleanup without joining them on the Start call stack. Callers must
// stop the orchestration goroutine first so no workerWG.Add can race with Wait.
func stopAndReapShardWorkers(workers *shardWorkerSet, workerWG *sync.WaitGroup) {
	if workers != nil {
		workers.stopAll()
	}
	if workerWG == nil {
		return
	}
	go func() {
		workerWG.Wait()
	}()
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
