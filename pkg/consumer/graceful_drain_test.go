package consumer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
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

func TestStopAndReapShardWorkersJoinsCooperativeWorkersWithinBudget(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	stop := make(chan struct{})
	gen := workers.add("shard-a", func() { close(stop) })

	var workerWG sync.WaitGroup
	var finished atomic.Bool
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a", gen)
		<-stop
		finished.Store(true)
	}()

	stopAndReapShardWorkers(workers, &workerWG, time.Second)

	if !finished.Load() {
		t.Fatal("stopAndReapShardWorkers returned before the cooperative worker finished")
	}
}

func TestStopAndReapShardWorkersAbandonsHungWorkerAfterBudget(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var stopped int32
	gen := workers.add("shard-a", func() { atomic.AddInt32(&stopped, 1) })

	var workerWG sync.WaitGroup
	finish := make(chan struct{})
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a", gen)
		<-finish // deliberately ignore the stop signal
	}()

	returned := make(chan struct{})
	go func() {
		stopAndReapShardWorkers(workers, &workerWG, 10*time.Millisecond)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("stopAndReapShardWorkers blocked on a worker that ignores its stop signal")
	}
	if got := atomic.LoadInt32(&stopped); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}

	close(finish)
	workerWG.Wait()
}

func TestStopAndReapShardWorkersZeroTimeoutDoesNotWait(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	gen := workers.add("shard-a", func() {})

	var workerWG sync.WaitGroup
	finish := make(chan struct{})
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a", gen)
		<-finish
	}()

	returned := make(chan struct{})
	go func() {
		stopAndReapShardWorkers(workers, &workerWG, 0)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("stopAndReapShardWorkers with zero timeout did not return immediately")
	}

	close(finish)
	workerWG.Wait()
}

func TestStopAndReapShardWorkersNoopsWithoutWorkerGroup(t *testing.T) {
	t.Parallel()

	stopAndReapShardWorkers(nil, nil, time.Millisecond)
}

func TestWaitForShardDrainTimeoutReturnsWithoutWaitingForForcedWorkerExit(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	workers := newShardWorkerSet()
	stop := make(chan struct{})
	finish := make(chan struct{})
	workers.add("slow-shard", func() {
		close(stop)
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-finish // deliberately ignore the stop signal
	}()

	timeoutCh := make(chan time.Time, 1)
	timeoutCh <- time.Now()     // only the timeout branch is selectable
	done := make(chan struct{}) // the drain never finishes on its own

	returned := make(chan error, 1)
	go func() {
		returned <- waitForShardDrain(workers, done, timeoutCh, 20*time.Millisecond, nil)
	}()

	var err error
	select {
	case err = <-returned:
	case <-time.After(100 * time.Millisecond):
		close(finish)
		t.Fatal("waitForShardDrain blocked waiting for a force-stopped worker")
	}
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("waitForShardDrain() error = %v, want wraps %v", err, ErrDrainTimeout)
	}
	select {
	case <-stop:
	default:
		t.Fatal("worker stop was not signaled before timeout returned")
	}
	close(finish)
	wg.Wait()
}
