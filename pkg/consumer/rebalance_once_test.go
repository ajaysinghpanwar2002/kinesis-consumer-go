package consumer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
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
		nil,
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
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	assertRebalancePlanActions(t, result.plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	if result.started != 0 {
		t.Fatalf("started = %d, want 0", result.started)
	}
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
