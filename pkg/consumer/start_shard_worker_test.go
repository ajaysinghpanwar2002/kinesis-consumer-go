package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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
