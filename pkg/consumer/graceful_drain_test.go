package consumer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
)

func TestDrainShardWorkersWaitsForNaturalExitWithoutCancel(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelled int32
	gen := workers.add("shard-a", func() { atomic.AddInt32(&cancelled, 1) })

	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a", gen)
		time.Sleep(5 * time.Millisecond)
	}()

	if err := drainShardWorkers(workers, &workerWG, 100*time.Millisecond); err != nil {
		t.Fatalf("drainShardWorkers() error = %v, want nil", err)
	}
	if got := atomic.LoadInt32(&cancelled); got != 0 {
		t.Fatalf("cancel calls = %d, want 0", got)
	}
	if workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = true after natural drain, want false")
	}
}

func TestDrainShardWorkersZeroTimeoutWaitsIndefinitely(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelled int32
	gen := workers.add("shard-a", func() { atomic.AddInt32(&cancelled, 1) })

	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a", gen)
		time.Sleep(5 * time.Millisecond)
	}()

	if err := drainShardWorkers(workers, &workerWG, 0); err != nil {
		t.Fatalf("drainShardWorkers() error = %v, want nil", err)
	}
	if got := atomic.LoadInt32(&cancelled); got != 0 {
		t.Fatalf("cancel calls = %d, want 0", got)
	}
	if workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = true after natural drain, want false")
	}
}

func TestDrainShardWorkersTimeoutForcesStopAndReturnsTimeout(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelled int32
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	workers.add("shard-a", func() {
		atomic.AddInt32(&cancelled, 1)
		workerWG.Done()
	})

	err := drainShardWorkers(workers, &workerWG, 10*time.Millisecond)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("drainShardWorkers() error = %v, want %v", err, ErrDrainTimeout)
	}
	if err == nil || err.Error() != "graceful drain timed out after 10ms" {
		t.Fatalf("drainShardWorkers() error = %v, want timeout message", err)
	}
	if got := atomic.LoadInt32(&cancelled); got != 1 {
		t.Fatalf("cancel calls = %d, want 1", got)
	}
	if workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = true after forced stop, want false")
	}
}

func TestDrainShardWorkersNoopsWithoutWorkerGroup(t *testing.T) {
	t.Parallel()

	if err := drainShardWorkers(nil, nil, time.Millisecond); err != nil {
		t.Fatalf("drainShardWorkers() error = %v, want nil", err)
	}
}

func TestConsumerDrainShardWorkersSetsDrainStateOnlyWhileWaiting(t *testing.T) {
	t.Parallel()

	c := &Consumer{drainTimeout: time.Second, reporter: metrics.Nop{}}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	gen := workers.add("shard-a", func() {})

	observedDraining := make(chan struct{})
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a", gen)

		for !c.isDraining() {
			time.Sleep(time.Millisecond)
		}
		close(observedDraining)
	}()

	if err := c.drainShardWorkers(workers, &workerWG, nil); err != nil {
		t.Fatalf("drainShardWorkers() error = %v, want nil", err)
	}
	select {
	case <-observedDraining:
	default:
		t.Fatal("worker did not observe draining state")
	}
	if c.isDraining() {
		t.Fatal("isDraining() = true after drain returned, want false")
	}
}

func TestDrainShardWorkersReturnsWorkerErrorAfterHealthyWorkersFinish(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	var wg sync.WaitGroup
	workerErrCh := make(chan error, 2)
	healthyFinished := make(chan struct{})

	wg.Add(2)
	go func() { // failing worker: errors and exits immediately
		defer wg.Done()
		workerErrCh <- errBoom
	}()
	go func() { // healthy worker: still draining when the error arrives
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		close(healthyFinished)
	}()

	err := drainShardWorkersOrError(nil, &wg, time.Second, workerErrCh)
	if !errors.Is(err, errBoom) {
		t.Fatalf("drainShardWorkersOrError() error = %v, want wraps %v", err, errBoom)
	}
	select {
	case <-healthyFinished:
	default:
		t.Fatal("drain returned before the healthy worker finished")
	}
}

func TestDrainShardWorkersTimeoutAfterWorkerErrorJoinsBoth(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	var wg sync.WaitGroup
	workers := newShardWorkerSet()
	workerErrCh := make(chan error, 2)

	stopSlow := make(chan struct{})
	workers.add("slow-shard", func() { close(stopSlow) })
	wg.Add(2)
	go func() { // failing worker
		defer wg.Done()
		workerErrCh <- errBoom
	}()
	go func() { // worker that never finishes on its own: only stopAll ends it
		defer wg.Done()
		<-stopSlow
	}()

	err := drainShardWorkersOrError(workers, &wg, 20*time.Millisecond, workerErrCh)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("drainShardWorkersOrError() error = %v, want wraps %v", err, ErrDrainTimeout)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("drainShardWorkersOrError() error = %v, want also wraps %v", err, errBoom)
	}
}

func TestWaitForShardDrainTimeoutCollectsErrorBufferedDuringForceStop(t *testing.T) {
	t.Parallel()

	// Deterministic timeout-first coverage: workerErrCh is empty when the
	// select runs (only the timeout branch is ready), and the worker error
	// appears only when the force-stop makes the worker report its failure.
	// The timeout branch must still pick it up and join it with the timeout
	// error instead of dropping it.
	errBoom := errors.New("boom")
	var wg sync.WaitGroup
	workers := newShardWorkerSet()
	workerErrCh := make(chan error, 1)
	stop := make(chan struct{})
	workers.add("slow-shard", func() {
		workerErrCh <- errBoom // the force-stopped worker reports its failure
		close(stop)
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-stop
	}()

	timeoutCh := make(chan time.Time, 1)
	timeoutCh <- time.Now()     // only the timeout branch is selectable
	done := make(chan struct{}) // the drain never finishes on its own

	err := waitForShardDrain(workers, &wg, done, timeoutCh, 20*time.Millisecond, workerErrCh)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("waitForShardDrain() error = %v, want wraps %v", err, ErrDrainTimeout)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("waitForShardDrain() error = %v, want also wraps %v", err, errBoom)
	}
}
