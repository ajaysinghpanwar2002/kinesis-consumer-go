package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func TestStartRegisteredShardWorkerRegistersAndRemovesOnStop(t *testing.T) {
	t.Parallel()

	c := newTestRegisteredShardWorkerConsumer(nil)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	shardLease := &recordingReleaseLease{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.startRegisteredShardWorker(ctx, "shard-1", shardLease, workers, &workerWG, workerErrCh, cancel)

	if !workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = false, want true")
	}
	if !workers.stop("shard-1") {
		t.Fatal("workers.stop(shard-1) = false, want true")
	}
	waitWorkerGroupDone(t, &workerWG)

	if workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = true after worker exit, want false")
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
	select {
	case err := <-workerErrCh:
		t.Fatalf("workerErrCh received %v, want no error", err)
	default:
	}
}

func TestStartRegisteredShardWorkerReportsErrorAndCancelsRun(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := newTestRegisteredShardWorkerConsumer(errBoom)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.startRegisteredShardWorker(ctx, "shard-1", fakeShardLease{}, workers, &workerWG, workerErrCh, cancel)
	waitWorkerGroupDone(t, &workerWG)

	select {
	case err := <-workerErrCh:
		if !errors.Is(err, errBoom) {
			t.Fatalf("workerErrCh error = %v, want wraps %v", err, errBoom)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker error")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("run context error = %v, want %v", ctx.Err(), context.Canceled)
	}
	if workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = true after error exit, want false")
	}
}

func TestStartRegisteredShardWorkersStartsEveryAcquiredLease(t *testing.T) {
	t.Parallel()

	c := newTestRegisteredShardWorkerConsumer(nil)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)
	shardLeaseA := &recordingReleaseLease{}
	shardLeaseB := &recordingReleaseLease{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.startRegisteredShardWorkers(
		ctx,
		map[string]lease.Lease{
			"shard-a": shardLeaseA,
			"shard-b": shardLeaseB,
		},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)

	if !workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = false, want true")
	}
	if !workers.has("shard-b") {
		t.Fatal("workers.has(shard-b) = false, want true")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)

	if workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = true after stopAll, want false")
	}
	if workers.has("shard-b") {
		t.Fatal("workers.has(shard-b) = true after stopAll, want false")
	}
	if shardLeaseA.calls != 1 {
		t.Fatalf("shard-a Release calls = %d, want 1", shardLeaseA.calls)
	}
	if shardLeaseB.calls != 1 {
		t.Fatalf("shard-b Release calls = %d, want 1", shardLeaseB.calls)
	}
	select {
	case err := <-workerErrCh:
		t.Fatalf("workerErrCh received %v, want no error", err)
	default:
	}
}

func TestStartRegisteredShardWorkersEmptyInputStartsNothing(t *testing.T) {
	t.Parallel()

	c := newTestRegisteredShardWorkerConsumer(nil)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.startRegisteredShardWorkers(
		ctx,
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	waitWorkerGroupDone(t, &workerWG)

	select {
	case err := <-workerErrCh:
		t.Fatalf("workerErrCh received %v, want no error", err)
	default:
	}
}

func TestStartRegisteredShardWorkersReportsWorkerErrorAndCancelsRun(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := newTestRegisteredShardWorkerConsumer(errBoom)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.startRegisteredShardWorkers(
		ctx,
		map[string]lease.Lease{"shard-1": fakeShardLease{}},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	waitWorkerGroupDone(t, &workerWG)

	select {
	case err := <-workerErrCh:
		if !errors.Is(err, errBoom) {
			t.Fatalf("workerErrCh error = %v, want wraps %v", err, errBoom)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker error")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("run context error = %v, want %v", ctx.Err(), context.Canceled)
	}
	if workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = true after error exit, want false")
	}
}

func newTestRegisteredShardWorkerConsumer(processErr error) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatInterval = time.Millisecond
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		cfg:    Config{StreamName: "stream"},
		tuning: tuning,
		processShardRecordsLoopFn: func(ctx context.Context, shardID string) (string, int, error) {
			_ = shardID
			if processErr != nil {
				return "", 0, processErr
			}
			<-ctx.Done()
			return "", 0, nil
		},
	}
}

func waitWorkerGroupDone(t *testing.T, workerWG *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		workerWG.Wait()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker goroutine")
	}
}
