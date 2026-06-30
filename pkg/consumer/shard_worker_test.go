package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func TestRunShardWorkerReleasesLeaseAfterContextCancellation(t *testing.T) {
	t.Parallel()

	processStarted := make(chan string, 1)
	shardLease := &recordingReleaseLease{}
	c := newTestShardWorkerConsumer(time.Hour, 30*time.Millisecond)
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		processStarted <- shardID
		<-ctx.Done()
		return "", 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runShardWorker(ctx, c, "shard-1", shardLease)

	if got := <-processStarted; got != "shard-1" {
		t.Fatalf("processed shard = %q, want shard-1", got)
	}
	cancel()
	waitShardWorkerDone(t, done, nil)

	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
	if shardLease.ctx == nil {
		t.Fatal("Release context = nil, want context")
	}
	if _, ok := shardLease.ctx.Deadline(); !ok {
		t.Fatal("Release context has no deadline")
	}
}

func TestRunShardWorkerStopsRenewalBeforeRelease(t *testing.T) {
	t.Parallel()

	shardLease := &orderedRenewReleaseLease{
		events: make(chan string, 3),
	}
	c := newTestShardWorkerConsumer(time.Millisecond, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := runShardWorker(ctx, c, "shard-1", shardLease)

	waitEvent(t, shardLease.events, "renew-start")
	cancel()
	waitShardWorkerDone(t, done, nil)

	waitEvent(t, shardLease.events, "renew-done")
	waitEvent(t, shardLease.events, "release")
}

func TestRunShardWorkerCompletesAfterProcessingCompletion(t *testing.T) {
	t.Parallel()

	processReturn := make(chan struct{})
	processedShard := make(chan string, 1)
	shardLease := &countingOrderedRenewReleaseLease{
		events: make(chan string, 3),
	}
	c := newTestShardWorkerConsumer(time.Millisecond, 30*time.Millisecond)
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx
		processedShard <- shardID
		<-processReturn
		return "seq-1", 0, nil
	}

	done := runShardWorker(context.Background(), c, "shard-1", shardLease)

	if got := <-processedShard; got != "shard-1" {
		t.Fatalf("processed shard = %q, want shard-1", got)
	}
	waitEvent(t, shardLease.events, "renew-start")
	close(processReturn)
	waitShardWorkerDone(t, done, nil)

	waitEvent(t, shardLease.events, "renew-done")
	waitEvent(t, shardLease.events, "release")
	if got := shardLease.releaseCalls(); got != 1 {
		t.Fatalf("Release calls = %d, want 1", got)
	}
}

func TestRunShardWorkerStopsRenewalAndReleasesLeaseAfterProcessingError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	processErr := fmt.Errorf("process shard records loop shard-1: %w", errBoom)
	processReturn := make(chan struct{})
	shardLease := &orderedRenewReleaseLease{
		events: make(chan string, 3),
	}
	c := newTestShardWorkerConsumer(time.Millisecond, 30*time.Millisecond)
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx
		_ = shardID
		<-processReturn
		return "", 0, processErr
	}

	done := runShardWorker(context.Background(), c, "shard-1", shardLease)

	waitEvent(t, shardLease.events, "renew-start")
	close(processReturn)
	err := waitShardWorkerDone(t, done, errBoom)
	if err.Error() != "process shard records loop shard-1: boom" {
		t.Fatalf("runShardWorker() error = %v, want %q", err, "process shard records loop shard-1: boom")
	}

	waitEvent(t, shardLease.events, "renew-done")
	waitEvent(t, shardLease.events, "release")
}

func TestRunShardWorkerReturnsRenewalError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	processStarted := make(chan struct{})
	processCanceled := make(chan struct{})
	shardLease := &recordingRenewLease{err: errBoom}
	c := newTestShardWorkerConsumer(time.Millisecond, 30*time.Millisecond)
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		close(processStarted)
		<-ctx.Done()
		close(processCanceled)
		return "", 0, nil
	}

	done := runShardWorker(context.Background(), c, "shard-1", shardLease)
	<-processStarted
	err := waitShardWorkerDone(t, done, errBoom)
	if !errors.Is(err, errBoom) {
		t.Fatalf("runShardWorker() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "renew shard lease shard-1: boom" {
		t.Fatalf("runShardWorker() error = %v, want %q", err, "renew shard lease shard-1: boom")
	}
	select {
	case <-processCanceled:
	default:
		t.Fatal("processing was not canceled after renewal error")
	}
}

func TestRunShardWorkerIgnoresContextCancellation(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	c := newTestShardWorkerConsumer(time.Hour, 30*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.runShardWorker(ctx, "shard-1", shardLease)
	if err != nil {
		t.Fatalf("runShardWorker() error = %v, want nil", err)
	}
}

func TestRunShardWorkerReturnsReleaseTimeout(t *testing.T) {
	t.Parallel()

	processReturn := make(chan struct{})
	shardLease := &blockingReleaseLease{}
	c := newTestShardWorkerConsumer(time.Hour, 30*time.Millisecond)
	c.tuning.shardLeaseReleaseTimeout = time.Millisecond
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx
		_ = shardID
		<-processReturn
		return "", 0, nil
	}

	done := runShardWorker(context.Background(), c, "shard-1", shardLease)
	close(processReturn)

	err := waitShardWorkerDone(t, done, context.DeadlineExceeded)
	if err == nil || err.Error() != "release shard lease shard-1 timed out: context deadline exceeded" {
		t.Fatalf("runShardWorker() error = %v, want release timeout", err)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func newTestShardWorkerConsumer(heartbeatInterval, heartbeatTTL time.Duration) *Consumer {
	return &Consumer{
		tuning: tuningConfig{
			heartbeatInterval:        heartbeatInterval,
			heartbeatTTL:             heartbeatTTL,
			shardLeaseReleaseTimeout: 30 * time.Millisecond,
		},
		processShardRecordsLoopFn: func(ctx context.Context, shardID string) (string, int, error) {
			_ = shardID
			<-ctx.Done()
			return "", 0, nil
		},
	}
}

func runShardWorker(ctx context.Context, c *Consumer, shardID string, shardLease lease.Lease) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- c.runShardWorker(ctx, shardID, shardLease)
	}()
	return done
}

func waitShardWorkerDone(t *testing.T, done <-chan error, want error) error {
	t.Helper()

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("runShardWorker() error = %v, want %v", err, want)
		}
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runShardWorker to return")
	}
	return nil
}

type countingOrderedRenewReleaseLease struct {
	events chan string
	calls  int
}

func (l *countingOrderedRenewReleaseLease) Renew(ctx context.Context, _ time.Duration) error {
	l.events <- "renew-start"
	<-ctx.Done()
	l.events <- "renew-done"
	return ctx.Err()
}

func (l *countingOrderedRenewReleaseLease) Release(context.Context) error {
	l.events <- "release"
	l.calls++
	return nil
}

func (l *countingOrderedRenewReleaseLease) releaseCalls() int {
	return l.calls
}
