package consumer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainShardWorkersWaitsForNaturalExitWithoutCancel(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelled int32
	workers.add("shard-a", func() { atomic.AddInt32(&cancelled, 1) })

	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a")
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
	workers.add("shard-a", func() { atomic.AddInt32(&cancelled, 1) })

	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		defer workers.done("shard-a")
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
	if !errors.Is(err, errGracefulDrainTimeout) {
		t.Fatalf("drainShardWorkers() error = %v, want %v", err, errGracefulDrainTimeout)
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
