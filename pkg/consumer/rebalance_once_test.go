package consumer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

type recordingRebalanceOnceManager struct {
	fakeLeaseManager

	leaseOwners    map[string]string
	workerOwners   []string
	listErrs       []error
	workerErrs     []error
	listCalls      []string
	workerCalls    []string
	executionCalls []rebalanceExecutionCall
	acquireResults []acquireResult
	claimResults   []claimResult
	listCh         chan string
	workerCh       chan string
	executionCh    chan rebalanceExecutionCall
}

func (m *recordingRebalanceOnceManager) List(_ context.Context, streamName string) (map[string]string, error) {
	m.listCalls = append(m.listCalls, streamName)
	if m.listCh != nil {
		select {
		case m.listCh <- streamName:
		default:
		}
	}
	if len(m.listErrs) > 0 {
		err := m.listErrs[0]
		m.listErrs = m.listErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return m.leaseOwners, nil
}

func (m *recordingRebalanceOnceManager) Workers(_ context.Context, streamName string) ([]string, error) {
	m.workerCalls = append(m.workerCalls, streamName)
	if m.workerCh != nil {
		select {
		case m.workerCh <- streamName:
		default:
		}
	}
	if len(m.workerErrs) > 0 {
		err := m.workerErrs[0]
		m.workerErrs = m.workerErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return m.workerOwners, nil
}

func (m *recordingRebalanceOnceManager) Acquire(_ context.Context, _ string, shardID, _ string, _ time.Duration) (lease.Lease, bool, error) {
	result := acquireResult{}
	if len(m.acquireResults) > 0 {
		result = m.acquireResults[0]
		m.acquireResults = m.acquireResults[1:]
	}
	m.executionCalls = append(m.executionCalls, rebalanceExecutionCall{
		kind:    rebalancePlanAcquireUnowned,
		shardID: shardID,
	})
	if m.executionCh != nil {
		select {
		case m.executionCh <- rebalanceExecutionCall{kind: rebalancePlanAcquireUnowned, shardID: shardID}:
		default:
		}
	}
	return result.lease, result.acquired, result.err
}

func (m *recordingRebalanceOnceManager) Claim(_ context.Context, _ string, shardID, expectedOwner, _ string, _ time.Duration) (lease.Lease, bool, error) {
	result := claimResult{}
	if len(m.claimResults) > 0 {
		result = m.claimResults[0]
		m.claimResults = m.claimResults[1:]
	}
	m.executionCalls = append(m.executionCalls, rebalanceExecutionCall{
		kind:    rebalancePlanClaimDonor,
		shardID: shardID,
		donor:   expectedOwner,
	})
	if m.executionCh != nil {
		select {
		case m.executionCh <- rebalanceExecutionCall{kind: rebalancePlanClaimDonor, shardID: shardID, donor: expectedOwner}:
		default:
		}
	}
	return result.lease, result.claimed, result.err
}

func TestRebalanceShardsOnceNoReadyShardsSkipsSnapshotsAndExecution(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.rebalanceShardsOnce(
		ctx,
		nil,
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	if len(result.readyShardIDs) != 0 {
		t.Fatalf("readyShardIDs = %v, want empty", result.readyShardIDs)
	}
	if len(result.plan.actions) != 0 {
		t.Fatalf("plan.actions = %v, want empty", result.plan.actions)
	}
	if result.started != 0 {
		t.Fatalf("started = %d, want 0", result.started)
	}
	assertShardList(t, result.movedShardIDs, nil)
	if len(manager.listCalls) != 0 {
		t.Fatalf("List calls = %d, want 0", len(manager.listCalls))
	}
	if len(manager.workerCalls) != 0 {
		t.Fatalf("Workers calls = %d, want 0", len(manager.workerCalls))
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, nil)
	waitWorkerGroupDone(t, &workerWG)
}

func TestRebalanceShardsOnceBuildsPlanFromSnapshotsAndExecutesIt(t *testing.T) {
	t.Parallel()

	acquiredLease := &recordingReleaseLease{}
	claimedLease := &recordingReleaseLease{}
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		workerOwners: []string{"self", "donor-a"},
		acquireResults: []acquireResult{
			{lease: acquiredLease, acquired: true},
		},
		claimResults: []claimResult{
			{lease: claimedLease, claimed: true},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)
	now := time.Date(2026, 6, 29, 1, 2, 3, 0, time.UTC)
	cooldown := map[string]time.Time{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-d", "", ""),
			testShard("shard-a", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-b", "", ""),
		),
		newShardCompletionState(),
		cooldown,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		now,
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	assertShardList(t, result.readyShardIDs, []string{"shard-a", "shard-b", "shard-c", "shard-d"})
	assertRebalancePlanActions(t, result.plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
		{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
	})
	if result.plan.initialCount != 0 || result.plan.projectedCount != 2 {
		t.Fatalf("plan initial/projected = %d/%d, want 0/2", result.plan.initialCount, result.plan.projectedCount)
	}
	if result.started != 2 {
		t.Fatalf("started = %d, want 2", result.started)
	}
	assertShardList(t, result.movedShardIDs, []string{"shard-a", "shard-b"})
	assertRebalanceCooldown(t, cooldown, map[string]time.Time{
		"shard-a": now.Add(c.tuning.shardCooldownPeriod),
		"shard-b": now.Add(c.tuning.shardCooldownPeriod),
	})
	if !slices.Equal(manager.listCalls, []string{"stream"}) {
		t.Fatalf("List calls = %v, want [stream]", manager.listCalls)
	}
	if !slices.Equal(manager.workerCalls, []string{"stream"}) {
		t.Fatalf("Workers calls = %v, want [stream]", manager.workerCalls)
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
		{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
	})

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if acquiredLease.calls != 1 {
		t.Fatalf("acquired lease Release calls = %d, want 1", acquiredLease.calls)
	}
	if claimedLease.calls != 1 {
		t.Fatalf("claimed lease Release calls = %d, want 1", claimedLease.calls)
	}
}

func TestRebalanceShardsOnceRetriesSnapshotReadsBeforePlanning(t *testing.T) {
	t.Parallel()

	errFirst := errors.New("first")
	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"self"},
		listErrs:     []error{errFirst, nil},
		workerErrs:   []error{errFirst, nil},
		acquireResults: []acquireResult{
			{acquired: false},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.tuning.retryMaxAttempts = 2
	c.tuning.retryBackoff = 0
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	cooldown := map[string]time.Time{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(testShard("shard-a", "", "")),
		newShardCompletionState(),
		cooldown,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	assertRebalancePlanActions(t, result.plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	if result.started != 0 {
		t.Fatalf("started = %d, want 0", result.started)
	}
	assertShardList(t, result.movedShardIDs, nil)
	assertRebalanceCooldown(t, cooldown, nil)
	if len(manager.listCalls) != 2 {
		t.Fatalf("List calls = %d, want 2", len(manager.listCalls))
	}
	if len(manager.workerCalls) != 2 {
		t.Fatalf("Workers calls = %d, want 2", len(manager.workerCalls))
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	waitWorkerGroupDone(t, &workerWG)
}

func TestRebalanceShardsOnceCooldownSkipsRepeatedTarget(t *testing.T) {
	t.Parallel()

	acquiredLease := &recordingReleaseLease{}
	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"self"},
		acquireResults: []acquireResult{
			{lease: acquiredLease, acquired: true},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	now := time.Date(2026, 6, 29, 3, 4, 5, 0, time.UTC)
	cooldown := map[string]time.Time{}
	shards := shardMapForReadiness(testShard("shard-a", "", ""))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := c.rebalanceShardsOnce(
		ctx,
		shards,
		newShardCompletionState(),
		cooldown,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		now,
	)
	if err != nil {
		t.Fatalf("first rebalanceShardsOnce() error = %v, want nil", err)
	}
	assertRebalancePlanActions(t, first.plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	assertShardList(t, first.movedShardIDs, []string{"shard-a"})
	assertRebalanceCooldown(t, cooldown, map[string]time.Time{
		"shard-a": now.Add(c.tuning.shardCooldownPeriod),
	})

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if acquiredLease.calls != 1 {
		t.Fatalf("acquired lease Release calls = %d, want 1", acquiredLease.calls)
	}

	second, err := c.rebalanceShardsOnce(
		ctx,
		shards,
		newShardCompletionState(),
		cooldown,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		now,
	)
	if err != nil {
		t.Fatalf("second rebalanceShardsOnce() error = %v, want nil", err)
	}
	assertRebalancePlanActions(t, second.plan.actions, nil)
	assertShardList(t, second.movedShardIDs, nil)
	assertRebalanceExecutionCalls(t, manager.executionCalls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
}

func TestRebalanceShardsOnceShedsLocalOverageAndRecordsCooldown(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-a": "self",
			"shard-b": "self",
			"shard-c": "self",
			"shard-d": "worker-a",
		},
		workerOwners: []string{"self", "worker-a"},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var cancelledA int32
	var cancelledB int32
	var cancelledC int32
	workers.add("shard-a", func() { atomic.AddInt32(&cancelledA, 1) })
	workers.add("shard-b", func() { atomic.AddInt32(&cancelledB, 1) })
	workers.add("shard-c", func() { atomic.AddInt32(&cancelledC, 1) })
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	now := time.Date(2026, 6, 29, 4, 5, 6, 0, time.UTC)
	cooldown := map[string]time.Time{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		cooldown,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		now,
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	assertRebalancePlanActions(t, result.plan.actions, nil)
	if result.plan.initialCount != 3 || result.plan.projectedCount != 3 {
		t.Fatalf("plan initial/projected = %d/%d, want 3/3", result.plan.initialCount, result.plan.projectedCount)
	}
	assertShardList(t, result.movedShardIDs, []string{"shard-a"})
	assertRebalanceCooldown(t, cooldown, map[string]time.Time{
		"shard-a": now.Add(c.tuning.shardCooldownPeriod),
	})
	// The shed worker keeps its registration as the stale-worker fence until
	// its deferred done runs; it is stopping, no longer running.
	if !workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = false after shed, want true until the worker finishes")
	}
	if workers.running("shard-a") {
		t.Fatal("workers.running(shard-a) = true after shed, want false")
	}
	if !workers.has("shard-b") {
		t.Fatal("workers.has(shard-b) = false after shed, want true")
	}
	if !workers.has("shard-c") {
		t.Fatal("workers.has(shard-c) = false after shed, want true")
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&cancelledB); got != 0 {
		t.Fatalf("shard-b cancel calls = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&cancelledC); got != 0 {
		t.Fatalf("shard-c cancel calls = %d, want 0", got)
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, nil)
}

// TestStuckShedWorkerFencesShardUntilItsLeaseReleaseCompletes reproduces the
// 2026-07-17 review's stale-handle hazard end to end: a shed worker whose
// callback ignores cancellation outlives both the heartbeat TTL (its lease
// expires, the listing shows the shard unowned) and any cooldown (the
// cooldown map is empty here — long expired). Without the local fence this
// consumer would re-acquire the shard on the next pass and the stuck
// worker's later owner-checked Release would delete that successor lease
// (same owner). With the fence, re-acquisition is impossible until the old
// worker has finished — its Release strictly precedes any successor Acquire.
func TestStuckShedWorkerFencesShardUntilItsLeaseReleaseCompletes(t *testing.T) {
	t.Parallel()

	oldLease := &recordingReleaseLease{released: make(chan struct{})}
	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{}, // old lease expired: shard unowned
		workerOwners: []string{"self", "peer-1"},
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)

	finishWorker := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx // deliberately uncooperative: ignores cancellation
		_ = shardID
		<-finishWorker
		return "", 0, nil
	}

	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.startRegisteredShardWorker(ctx, "shard-a", oldLease, workers, &workerWG, workerErrCh, cancel)
	if !workers.stop("shard-a") { // the shed
		t.Fatal("stop(shard-a) = false, want true")
	}

	// First pass: shard-a is unowned, below fair-share high, and out of
	// cooldown — eligible by every duration-based rule. The fence alone must
	// keep this consumer from re-acquiring it.
	knownShards := shardMapForReadiness(testShard("shard-a", "", ""))
	result, err := c.rebalanceShardsOnce(
		ctx,
		knownShards,
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	if result.started != 0 {
		t.Fatalf("started = %d, want 0 while the old worker still runs", result.started)
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, nil)
	select {
	case <-oldLease.released:
		t.Fatal("old lease released while the worker was still stuck")
	default:
	}

	// The callback finally returns: the worker releases its lease and its
	// deferred done lifts the fence.
	close(finishWorker)
	waitForRecordingRelease(t, oldLease)
	waitForTrue(t, func() bool { return !workers.has("shard-a") }, "worker fence to lift")

	// Second pass: the shard is re-acquired only now, strictly after the old
	// worker's Release — the stale handle can no longer touch the new lease.
	result, err = c.rebalanceShardsOnce(
		ctx,
		knownShards,
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	if result.started != 1 {
		t.Fatalf("started = %d, want 1 after the old worker finished", result.started)
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	if oldLease.calls != 1 {
		t.Fatalf("old lease Release calls = %d, want 1 (before the re-acquire)", oldLease.calls)
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
}

func TestRebalanceShardsOnceReturnsReadyShardErrorWithoutSnapshots(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{}
	c := newTestRebalanceOnceConsumer(manager)
	c.store = &readinessCheckpointStore{
		checkpoints: map[string]string{},
		getErrs:     map[string]error{"shard-a": errBoom},
	}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(testShard("shard-a", "", "")),
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("rebalanceShardsOnce() error = %v, want wraps %v", err, errBoom)
	}
	want := "check ready shard shard-a completion: check shard completion shard-a: read shard checkpoint shard-a: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("rebalanceShardsOnce() error = %v, want %q", err, want)
	}
	if len(manager.listCalls) != 0 {
		t.Fatalf("List calls = %d, want 0", len(manager.listCalls))
	}
}

func TestRebalanceShardsOnceReturnsLeaseOwnerSnapshotError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{
		listErrs: []error{errBoom},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(testShard("shard-a", "", "")),
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("rebalanceShardsOnce() error = %v, want wraps %v", err, errBoom)
	}
	want := "list rebalance lease owners: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("rebalanceShardsOnce() error = %v, want %q", err, want)
	}
	if len(manager.workerCalls) != 0 {
		t.Fatalf("Workers calls = %d, want 0", len(manager.workerCalls))
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, nil)
}

func TestRebalanceShardsOnceReturnsWorkerOwnerSnapshotError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{},
		workerErrs:  []error{errBoom},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(testShard("shard-a", "", "")),
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("rebalanceShardsOnce() error = %v, want wraps %v", err, errBoom)
	}
	want := "list rebalance worker owners: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("rebalanceShardsOnce() error = %v, want %q", err, want)
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, nil)
}

func TestRebalanceShardsOnceReturnsExecutionErrorWithPlan(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"self"},
		acquireResults: []acquireResult{
			{err: errBoom},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(testShard("shard-a", "", "")),
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("rebalanceShardsOnce() error = %v, want wraps %v", err, errBoom)
	}
	want := "execute rebalance acquire shard shard-a: acquire shard lease shard-a: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("rebalanceShardsOnce() error = %v, want %q", err, want)
	}
	assertRebalancePlanActions(t, result.plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	if result.started != 0 {
		t.Fatalf("started = %d, want 0", result.started)
	}
	assertRebalanceExecutionCalls(t, manager.executionCalls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
}

func TestRebalanceShardsOnceRecordsCooldownForSuccessfulMovesBeforeExecutionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	acquiredLease := &recordingReleaseLease{}
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		workerOwners: []string{"self", "donor-a"},
		acquireResults: []acquireResult{
			{lease: acquiredLease, acquired: true},
		},
		claimResults: []claimResult{
			{err: errBoom},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	now := time.Date(2026, 6, 29, 2, 3, 4, 0, time.UTC)
	cooldown := map[string]time.Time{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		cooldown,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		now,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("rebalanceShardsOnce() error = %v, want wraps %v", err, errBoom)
	}
	if result.started != 1 {
		t.Fatalf("started = %d, want 1", result.started)
	}
	assertShardList(t, result.movedShardIDs, []string{"shard-a"})
	assertRebalanceCooldown(t, cooldown, map[string]time.Time{
		"shard-a": now.Add(c.tuning.shardCooldownPeriod),
	})

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if acquiredLease.calls != 1 {
		t.Fatalf("acquired lease Release calls = %d, want 1", acquiredLease.calls)
	}
}

func newTestRebalanceOnceConsumer(manager lease.Manager) *Consumer {
	c := newTestRegisteredShardWorkerConsumer(nil)
	c.store = &readinessCheckpointStore{
		checkpoints: map[string]string{},
		getErrs:     map[string]error{},
	}
	c.leaseManager = manager
	c.leaseOwner = "self"
	c.tuning.retryMaxAttempts = 1
	c.tuning.retryBackoff = 0
	c.tuning.heartbeatTTL = 30 * time.Millisecond
	c.tuning.maxMovesPerRebalance = 2
	return c
}

func assertRebalanceCooldown(t *testing.T, got, want map[string]time.Time) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("cooldown = %v, want %v", got, want)
	}
	for shardID, wantUntil := range want {
		gotUntil, ok := got[shardID]
		if !ok {
			t.Fatalf("cooldown missing shard %q; got %v", shardID, got)
		}
		if !gotUntil.Equal(wantUntil) {
			t.Fatalf("cooldown[%s] = %v, want %v", shardID, gotUntil, wantUntil)
		}
	}
}

func TestRebalanceShardsOnceLogsPlanAcquireAndClaim(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		workerOwners: []string{"self", "donor-a"},
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
		claimResults: []claimResult{
			{lease: &recordingReleaseLease{}, claimed: true},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.logger = slog.New(handler)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Date(2026, 7, 13, 1, 2, 3, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)

	records := handler.snapshot()

	plan, ok := findRecord(records, "rebalance plan")
	if !ok {
		t.Fatalf("no 'rebalance plan' record, got %+v", records)
	}
	if plan.level != slog.LevelDebug {
		t.Fatalf("plan level = %v, want Debug", plan.level)
	}
	wantPlanAttrs := map[string]string{
		"shards": "4", "workers": "2", "low": "2", "high": "2", "owned": "0", "actions": "2",
	}
	for key, want := range wantPlanAttrs {
		if plan.attrs[key] != want {
			t.Fatalf("plan attr %q = %q, want %q (attrs %+v)", key, plan.attrs[key], want, plan.attrs)
		}
	}

	acquired, ok := findRecord(records, "rebalance shard acquired")
	if !ok {
		t.Fatalf("no 'rebalance shard acquired' record, got %+v", records)
	}
	if acquired.level != slog.LevelInfo {
		t.Fatalf("acquired level = %v, want Info", acquired.level)
	}
	if acquired.attrs["shard"] != "shard-a" {
		t.Fatalf("acquired shard attr = %q, want %q", acquired.attrs["shard"], "shard-a")
	}

	claimed, ok := findRecord(records, "rebalance shard claimed")
	if !ok {
		t.Fatalf("no 'rebalance shard claimed' record, got %+v", records)
	}
	if claimed.level != slog.LevelInfo {
		t.Fatalf("claimed level = %v, want Info", claimed.level)
	}
	if claimed.attrs["shard"] != "shard-b" {
		t.Fatalf("claimed shard attr = %q, want %q", claimed.attrs["shard"], "shard-b")
	}
	if claimed.attrs["donor"] != "donor-a" {
		t.Fatalf("claimed donor attr = %q, want %q", claimed.attrs["donor"], "donor-a")
	}
}

func TestRebalanceShardsOnceLogsContestedAcquireAndClaim(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		workerOwners: []string{"self", "donor-a"},
		acquireResults: []acquireResult{
			{acquired: false},
		},
		claimResults: []claimResult{
			{claimed: false},
		},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.logger = slog.New(handler)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Date(2026, 7, 13, 1, 2, 3, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	waitWorkerGroupDone(t, &workerWG)

	records := handler.snapshot()

	acquireSkipped, ok := findRecord(records, "rebalance acquire skipped")
	if !ok {
		t.Fatalf("no 'rebalance acquire skipped' record, got %+v", records)
	}
	if acquireSkipped.level != slog.LevelDebug {
		t.Fatalf("acquire skipped level = %v, want Debug", acquireSkipped.level)
	}
	if acquireSkipped.attrs["shard"] != "shard-a" {
		t.Fatalf("acquire skipped shard attr = %q, want %q", acquireSkipped.attrs["shard"], "shard-a")
	}

	claimSkipped, ok := findRecord(records, "rebalance claim skipped")
	if !ok {
		t.Fatalf("no 'rebalance claim skipped' record, got %+v", records)
	}
	if claimSkipped.level != slog.LevelDebug {
		t.Fatalf("claim skipped level = %v, want Debug", claimSkipped.level)
	}
	if claimSkipped.attrs["shard"] != "shard-b" {
		t.Fatalf("claim skipped shard attr = %q, want %q", claimSkipped.attrs["shard"], "shard-b")
	}
	if claimSkipped.attrs["donor"] != "donor-a" {
		t.Fatalf("claim skipped donor attr = %q, want %q", claimSkipped.attrs["donor"], "donor-a")
	}

	if _, ok := findRecord(records, "rebalance shard acquired"); ok {
		t.Fatalf("unexpected 'rebalance shard acquired' record, got %+v", records)
	}
	if _, ok := findRecord(records, "rebalance shard claimed"); ok {
		t.Fatalf("unexpected 'rebalance shard claimed' record, got %+v", records)
	}
}

func TestRebalanceShardsOnceLogsShed(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-a": "self",
			"shard-b": "self",
			"shard-c": "self",
			"shard-d": "worker-a",
		},
		workerOwners: []string{"self", "worker-a"},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.logger = slog.New(handler)
	workers := newShardWorkerSet()
	workers.add("shard-a", func() {})
	workers.add("shard-b", func() {})
	workers.add("shard-c", func() {})
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Date(2026, 7, 13, 2, 3, 4, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	waitWorkerGroupDone(t, &workerWG)

	records := handler.snapshot()

	shed, ok := findRecord(records, "rebalance shard shed")
	if !ok {
		t.Fatalf("no 'rebalance shard shed' record, got %+v", records)
	}
	if shed.level != slog.LevelInfo {
		t.Fatalf("shed level = %v, want Info", shed.level)
	}
	if shed.attrs["shard"] != "shard-a" {
		t.Fatalf("shed shard attr = %q, want %q", shed.attrs["shard"], "shard-a")
	}
	if shed.attrs["owned"] != "3" {
		t.Fatalf("shed owned attr = %q, want %q", shed.attrs["owned"], "3")
	}
	if shed.attrs["high"] != "2" {
		t.Fatalf("shed high attr = %q, want %q", shed.attrs["high"], "2")
	}

	// The shed pass has no plan actions, so no plan record must be emitted.
	if _, ok := findRecord(records, "rebalance plan"); ok {
		t.Fatalf("unexpected 'rebalance plan' record on action-free pass, got %+v", records)
	}
}

func TestRebalanceShardsOnceQuietPassLogsNothing(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-a": "self",
			"shard-b": "worker-a",
		},
		workerOwners: []string{"self", "worker-a"},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.logger = slog.New(handler)
	workers := newShardWorkerSet()
	workers.add("shard-a", func() {})
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
		),
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Date(2026, 7, 13, 3, 4, 5, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	waitWorkerGroupDone(t, &workerWG)

	if records := handler.snapshot(); len(records) != 0 {
		t.Fatalf("quiet balanced pass emitted records: %+v", records)
	}
}
