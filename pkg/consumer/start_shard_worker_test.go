package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
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

func TestStartRegisteredShardWorkerGracefulDrainDetachesWorkerContextFromRunContext(t *testing.T) {
	t.Parallel()

	c := newTestRegisteredShardWorkerConsumer(nil)
	c.gracefulDrain = true
	workerStarted := make(chan struct{})
	workerCtxCanceled := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		close(workerStarted)
		<-ctx.Done()
		close(workerCtxCanceled)
		return "", 0, nil
	}

	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancelRun := context.WithCancel(context.Background())
	c.startRegisteredShardWorker(ctx, "shard-1", fakeShardLease{}, workers, &workerWG, workerErrCh, cancelRun)

	<-workerStarted
	cancelRun()

	select {
	case <-workerCtxCanceled:
		t.Fatal("worker context canceled by run context in graceful drain mode")
	case <-time.After(20 * time.Millisecond):
	}

	if !workers.stop("shard-1") {
		t.Fatal("workers.stop(shard-1) = false, want true")
	}
	waitWorkerGroupDone(t, &workerWG)

	select {
	case <-workerCtxCanceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker context cancellation after explicit stop")
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

func TestStartRegisteredShardWorkerLeaseLossIsLocalAndKeepsPeerWorkerLive(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newTestRegisteredShardWorkerConsumer(nil)
	c.reporter = reporter
	processStarted := make(chan string, 2)
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		processStarted <- shardID
		<-ctx.Done()
		if shardID == "shard-lost" {
			return "", 0, ctx.Err()
		}
		return "", 0, nil
	}

	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)
	lostLease := &renewErrReleaseRecorderLease{renewErr: lease.ErrNotOwned}
	healthyLease := &recordingReleaseLease{}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	c.startRegisteredShardWorker(runCtx, "shard-lost", lostLease, workers, &workerWG, workerErrCh, cancelRun)
	c.startRegisteredShardWorker(runCtx, "shard-healthy", healthyLease, workers, &workerWG, workerErrCh, cancelRun)

	started := map[string]bool{}
	for len(started) < 2 {
		select {
		case shardID := <-processStarted:
			started[shardID] = true
		case <-time.After(time.Second):
			t.Fatalf("workers did not both start; started=%v", started)
		}
	}

	deadline := time.Now().Add(time.Second)
	for workers.has("shard-lost") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if workers.has("shard-lost") {
		t.Fatal("lost worker registration remained after lease loss")
	}
	if !workers.has("shard-healthy") {
		t.Fatal("unrelated healthy worker stopped after peer worker lost its lease")
	}
	if runCtx.Err() != nil {
		t.Fatalf("run context error = %v, want live after shard-local lease loss", runCtx.Err())
	}
	select {
	case err := <-workerErrCh:
		t.Fatalf("workerErrCh received %v, want no run-wide error", err)
	default:
	}
	if calls := lostLease.releaseCalls.Load(); calls != 0 {
		t.Fatalf("lost lease Release calls = %d, want 0", calls)
	}
	if calls := healthyLease.calls; calls != 0 {
		t.Fatalf("healthy lease Release calls = %d while worker remains live, want 0", calls)
	}

	lost := reporter.countersNamed(metricLeaseLost)
	if len(lost) != 1 || lost[0].value != 1 {
		t.Fatalf("lease_lost calls = %+v, want one increment", lost)
	}
	assertCounterTags(t, lost[0], map[string]string{
		"stream": "stream",
		"shard":  "shard-lost",
	})
	stops := reporter.countersNamed(metricWorkerStops)
	if len(stops) != 1 {
		t.Fatalf("worker_stops calls before final cleanup = %d, want 1", len(stops))
	}
	assertCounterTags(t, stops[0], map[string]string{
		"stream":  "stream",
		"shard":   "shard-lost",
		"outcome": "clean",
	})

	cancelRun()
	waitWorkerGroupDone(t, &workerWG)
	if workers.has("shard-healthy") {
		t.Fatal("healthy worker registration remained after final cancellation")
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
		nil,
		time.Now(),
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

// syncOwnershipManager wraps the recording acquire manager with fixed
// List/Workers answers so tests can shape the sync path's ownership snapshot.
type syncOwnershipManager struct {
	*recordingAcquireManager
	leaseOwners  map[string]string
	workerOwners []string
}

func (m *syncOwnershipManager) List(context.Context, string) (map[string]string, error) {
	return m.leaseOwners, nil
}

func (m *syncOwnershipManager) Workers(context.Context, string) ([]string, error) {
	return m.workerOwners, nil
}

func TestAcquireAndStartReadyShardWorkersStopsAtFairShareHigh(t *testing.T) {
	t.Parallel()

	// 4 ready shards and one live peer worker -> high = ceil(4/2) = 2. The
	// old greedy path took all four (cold-start thundering herd) and the
	// peer had to claim them back two per rebalance tick.
	recording := &recordingAcquireManager{
		results: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
			{lease: &recordingReleaseLease{}, acquired: true},
		},
	}
	c := newTestReadyShardWorkerConsumer(recording, nil)
	c.leaseManager = &syncOwnershipManager{
		recordingAcquireManager: recording,
		workerOwners:            []string{"peer"},
	}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-1", "shard-2", "shard-3", "shard-4"),
		newShardCompletionState(),
		nil,
		time.Now(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want nil", err)
	}

	assertAcquireShardOrder(t, recording.calls, []string{"shard-1", "shard-2"})
	if workers.has("shard-3") || workers.has("shard-4") {
		t.Fatal("worker started beyond the fair-share high bound")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
}

func TestAcquireAndStartReadyShardWorkersHonorsCooldown(t *testing.T) {
	t.Parallel()

	// A shard freshly shed by this worker is in cooldown: the sync tick must
	// not re-acquire it (the shed/sync ping-pong), but may take it again once
	// the cooldown has expired.
	recording := &recordingAcquireManager{
		results: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
	}
	c := newTestReadyShardWorkerConsumer(recording, nil)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	cooldown := map[string]time.Time{"shard-1": now.Add(10 * time.Second)}

	err := c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-1"),
		newShardCompletionState(),
		cooldown,
		now,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("acquireAndStartReadyShardWorkers() error = %v, want nil", err)
	}
	assertAcquireShardOrder(t, recording.calls, nil)
	if workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = true, want false (shard in cooldown)")
	}

	err = c.acquireAndStartReadyShardWorkers(
		ctx,
		readyShardWorkerMap("shard-1"),
		newShardCompletionState(),
		cooldown,
		now.Add(11*time.Second),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("acquireAndStartReadyShardWorkers() after cooldown error = %v, want nil", err)
	}
	assertAcquireShardOrder(t, recording.calls, []string{"shard-1"})
	if !workers.has("shard-1") {
		t.Fatal("workers.has(shard-1) = false after cooldown expiry, want true")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
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
		nil,
		time.Now(),
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
		nil,
		time.Now(),
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
		nil,
		time.Now(),
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
		nil,
		time.Now(),
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

func TestRefreshAndStartReadyShardWorkersPrunesAgedOutShardStateOnCommit(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	// The fresh listing no longer contains aged-out: Kinesis lists closed
	// shards only until retention expires, so the pass must drop its entries
	// from the shared shard map, the completion cache, and the cooldown map.
	c.client = &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{testShard("live", "", "")}},
		},
	}
	knownShards := map[string]types.Shard{
		"aged-out": testShard("aged-out", "", ""),
		"live":     testShard("live", "", ""),
	}
	completionState := newShardCompletionState()
	completionState.markCompleted("aged-out")
	completionState.markCompleted("live")
	cooldown := map[string]time.Time{
		"aged-out": time.Now().Add(time.Minute),
		"live":     time.Now().Add(time.Minute),
	}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.refreshAndStartReadyShardWorkers(
		ctx,
		knownShards,
		completionState,
		cooldown,
		time.Now(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want nil", err)
	}

	if _, ok := knownShards["aged-out"]; ok {
		t.Fatal("knownShards retains aged-out after a successful pass, want pruned")
	}
	if _, ok := knownShards["live"]; !ok {
		t.Fatal("knownShards missing live, want retained")
	}
	if _, ok := cooldown["aged-out"]; ok {
		t.Fatal("cooldown retains aged-out, want pruned")
	}
	if completionState.isCompleted("aged-out") {
		t.Fatal("completion cache retains aged-out, want pruned")
	}
	if !completionState.isCompleted("live") {
		t.Fatal("completion cache missing live, want retained")
	}
}

func TestRefreshAndStartReadyShardWorkersUnblocksChildOfAgedOutNeverCompletedParent(t *testing.T) {
	t.Parallel()

	// The parent aged out of retention without ever being checkpointed as
	// completed (no consumer finished it before its records expired). It is
	// still in the known-shard map from earlier listings, so without pruning
	// the child would stay parent-gated forever; the fresh listing without the
	// parent must unblock the child on this very pass.
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: shardLease, acquired: true},
		},
	}
	c := newTestReadyShardWorkerConsumer(manager, nil)
	c.client = &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{testShard("child", "parent", "")}},
		},
	}
	knownShards := map[string]types.Shard{
		"parent": testShard("parent", "", ""),
		"child":  testShard("child", "parent", ""),
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
		nil,
		time.Now(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("refreshAndStartReadyShardWorkers() error = %v, want nil", err)
	}

	assertAcquireShardOrder(t, manager.calls, []string{"child"})
	if !workers.has("child") {
		t.Fatal("workers.has(child) = false, want a worker for the unblocked child")
	}
	if _, ok := knownShards["parent"]; ok {
		t.Fatal("knownShards retains the aged-out parent, want pruned")
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
		nil,
		time.Now(),
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
		nil,
		time.Now(),
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
	// The pass failed after listing, so its discovery must not commit: the
	// shared map still reflects the last successful sync.
	if _, ok := knownShards["new"]; ok {
		t.Fatal("failed refresh pass committed shard new into the shared shard map")
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
		cfg:      Config{StreamName: "stream"},
		tuning:   tuning,
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
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
