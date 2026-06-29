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
	if workerWG == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		workerWG.Wait()
		close(done)
	}()

	if timeout <= 0 {
		<-done
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		if workers != nil {
			workers.stopAll()
		}
		workerWG.Wait()
		return fmt.Errorf("%w after %s", errGracefulDrainTimeout, timeout)
	}
}
