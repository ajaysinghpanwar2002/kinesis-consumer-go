package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
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

func TestAcquireAndStartReadyShardWorkersSkipsRunningWorkersBeforeAcquire(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: shardLease, acquired: true},
		},
	}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	workers := newShardWorkerSet()
	workers.add("shard-running", func() {})
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-ready", "shard-running"),
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want nil", err)
	}

	assertAcquireShardOrder(t, manager.calls, []string{"shard-ready"})
	if !workers.has("shard-ready") {
		t.Fatal("workers.has(shard-ready) = false, want true")
	}
	if !workers.has("shard-running") {
		t.Fatal("workers.has(shard-running) = false, want true")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestAcquireAndStartReadyShardWorkersDoesNotStartUnacquiredShards(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: false},
		},
	}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-1"),
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want nil", err)
	}

	assertAcquireShardOrder(t, manager.calls, []string{"shard-1"})
	if workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = true, want false")
	}
	waitWorkerGroupDone(t, &workerWG)
}

func TestAcquireAndStartReadyShardWorkersReturnsReadyShardError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	c.store = &readinessCheckpointStore{
		getErrs: map[string]error{"shard-1": errBoom},
	}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-1"),
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want wraps %v", err, errBoom)
	}
	want := "check ready shard shard-1 completion: check shard completion shard-1: read shard checkpoint shard-1: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want %q", err, want)
	}
	assertAcquireShardOrder(t, manager.calls, nil)
}

func TestAcquireAndStartReadyShardWorkersReturnsAcquireError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{err: errBoom}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	c.tuning.retryMaxAttempts = 1
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-1"),
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want wraps %v", err, errBoom)
	}
	want := "acquire shard leases shard-1: acquire shard lease shard-1: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want %q", err, want)
	}
	if workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = true, want false")
	}
}

func TestRefreshAndStartReadyShardWorkersRefreshesKnownShardsAndStartsNewReadyShards(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: shardLease, acquired: true},
		},
	}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	c.client = &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{
				testShard("running", "parent", "adjacent"),
				testShard("new", "", ""),
			}},
		},
	}
	knownShards := map[string]types.Shard{
		"running": testShard("running", "", ""),
	}
	workers := newShardWorkerSet()
	workers.add("running", func() {})
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.refreshAndStartReadyShardWorkers(
		ctx,
		knownShards,
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want nil", err)
	}

	assertAcquireShardOrder(t, manager.calls, []string{"new"})
	if got := aws.ToString(knownShards["running"].ParentShardId); got != "parent" {
		t.Fatalf("knownShards[running].ParentShardId = %q, want parent", got)
	}
	if got := aws.ToString(knownShards["running"].AdjacentParentShardId); got != "adjacent" {
		t.Fatalf("knownShards[running].AdjacentParentShardId = %q, want adjacent", got)
	}
	if got := aws.ToString(knownShards["new"].ShardId); got != "new" {
		t.Fatalf("knownShards[new].ShardId = %q, want new", got)
	}
	if !workers.has("running") {
		t.Fatal("workers.has(running) = false, want true")
	}
	if !workers.has("new") {
		t.Fatal("workers.has(new) = false, want true")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestRefreshAndStartReadyShardWorkersReturnsRefreshErrorWithoutAcquire(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	c.client = &fakeKinesisClient{err: errBoom}
	knownShards := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.refreshAndStartReadyShardWorkers(
		ctx,
		knownShards,
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want %q", err, "list shards: boom")
	}
	assertAcquireShardOrder(t, manager.calls, nil)
	if len(knownShards) != 1 {
		t.Fatalf("len(knownShards) = %d, want 1", len(knownShards))
	}
	if got := aws.ToString(knownShards["existing"].ShardId); got != "existing" {
		t.Fatalf("knownShards[existing].ShardId = %q, want existing", got)
	}
}

func TestRefreshAndStartReadyShardWorkersReturnsAcquireErrorAfterRefresh(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{err: errBoom}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	c.tuning.retryMaxAttempts = 1
	c.client = &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{testShard("new", "", "")}},
		},
	}
	knownShards := map[string]types.Shard{}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.refreshAndStartReadyShardWorkers(
		ctx,
		knownShards,
		newShardCompletionState(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want wraps %v", err, errBoom)
	}
	want := "acquire shard leases new: acquire shard lease new: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want %q", err, want)
	}
	assertAcquireShardOrder(t, manager.calls, []string{"new"})
	if got := aws.ToString(knownShards["new"].ShardId); got != "new" {
		t.Fatalf("knownShards[new].ShardId = %q, want new", got)
	}
	if workers.has("new") {
		t.Fatal("workers.has(new) = true, want false")
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

func newTestReadyShardWorkerConsumer(manager *recordingAcquireManager, processErr error) *Consumer {
	c := newTestRegisteredShardWorkerConsumer(processErr)
	c.store = &readinessCheckpointStore{getErrs: map[string]error{}}
	c.leaseManager = manager
	c.leaseOwner = "owner"
	c.tuning.heartbeatTTL = 30 * time.Millisecond
	return c
}

func readyShardWorkerMap(shardIDs ...string) map[string]types.Shard {
	shards := make(map[string]types.Shard, len(shardIDs))
	for _, shardID := range shardIDs {
		shards[shardID] = types.Shard{ShardId: aws.String(shardID)}
	}
	return shards
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
