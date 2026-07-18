package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

type rebalanceExecutionCall struct {
	kind    rebalancePlanActionKind
	shardID string
	donor   string
}

type recordingRebalanceExecutionManager struct {
	fakeLeaseManager

	calls          []rebalanceExecutionCall
	acquireResults []acquireResult
	claimResults   []claimResult
}

func (m *recordingRebalanceExecutionManager) Acquire(_ context.Context, _ string, shardID, _ string, _ time.Duration) (lease.Lease, bool, error) {
	result := acquireResult{}
	if len(m.acquireResults) > 0 {
		result = m.acquireResults[0]
		m.acquireResults = m.acquireResults[1:]
	}
	m.calls = append(m.calls, rebalanceExecutionCall{
		kind:    rebalancePlanAcquireUnowned,
		shardID: shardID,
	})
	return result.lease, result.acquired, result.err
}

func (m *recordingRebalanceExecutionManager) Claim(_ context.Context, _ string, shardID, expectedOwner, _ string, _ time.Duration) (lease.Lease, bool, error) {
	result := claimResult{}
	if len(m.claimResults) > 0 {
		result = m.claimResults[0]
		m.claimResults = m.claimResults[1:]
	}
	m.calls = append(m.calls, rebalanceExecutionCall{
		kind:    rebalancePlanClaimDonor,
		shardID: shardID,
		donor:   expectedOwner,
	})
	return result.lease, result.claimed, result.err
}

func TestExecuteRebalancePlanStartsAcquiredAndClaimedWorkersInOrder(t *testing.T) {
	t.Parallel()

	acquiredLease := &recordingReleaseLease{}
	claimedLease := &recordingReleaseLease{}
	manager := &recordingRebalanceExecutionManager{
		acquireResults: []acquireResult{{lease: acquiredLease, acquired: true}},
		claimResults:   []claimResult{{lease: claimedLease, claimed: true}},
	}
	c := newTestRebalanceExecutionConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.executeRebalancePlan(
		ctx,
		rebalancePlan{actions: []rebalancePlanAction{
			{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
			{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
		}},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("executeRebalancePlan() error = %v, want nil", err)
	}
	if result.started != 2 {
		t.Fatalf("executeRebalancePlan() started = %d, want 2", result.started)
	}
	assertShardList(t, result.movedShardIDs, []string{"shard-a", "shard-b"})
	assertRebalanceExecutionCalls(t, manager.calls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
		{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
	})
	if !workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = false, want true")
	}
	if !workers.has("shard-b") {
		t.Fatal("workers.has(shard-b) = false, want true")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if acquiredLease.calls != 1 {
		t.Fatalf("acquired lease Release calls = %d, want 1", acquiredLease.calls)
	}
	if claimedLease.calls != 1 {
		t.Fatalf("claimed lease Release calls = %d, want 1", claimedLease.calls)
	}
	select {
	case err := <-workerErrCh:
		t.Fatalf("workerErrCh received %v, want no error", err)
	default:
	}
}

func TestExecuteRebalancePlanDoesNotStartMissesOrNilLeases(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceExecutionManager{
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: false},
			{acquired: true},
		},
		claimResults: []claimResult{
			{lease: &recordingReleaseLease{}, claimed: false},
			{claimed: true},
		},
	}
	c := newTestRebalanceExecutionConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.executeRebalancePlan(
		ctx,
		rebalancePlan{actions: []rebalancePlanAction{
			{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
			{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
			{kind: rebalancePlanAcquireUnowned, shardID: "shard-c"},
			{kind: rebalancePlanClaimDonor, shardID: "shard-d", donor: "donor-b"},
		}},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("executeRebalancePlan() error = %v, want nil", err)
	}
	if result.started != 0 {
		t.Fatalf("executeRebalancePlan() started = %d, want 0", result.started)
	}
	assertShardList(t, result.movedShardIDs, nil)
	assertRebalanceExecutionCalls(t, manager.calls, []rebalanceExecutionCall{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
		{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-c"},
		{kind: rebalancePlanClaimDonor, shardID: "shard-d", donor: "donor-b"},
	})
	waitWorkerGroupDone(t, &workerWG)
}

func TestExecuteRebalancePlanSkipsAlreadyRunningWorkers(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceExecutionManager{
		acquireResults: []acquireResult{{lease: &recordingReleaseLease{}, acquired: true}},
		claimResults:   []claimResult{{lease: &recordingReleaseLease{}, claimed: true}},
	}
	c := newTestRebalanceExecutionConsumer(manager)
	workers := newShardWorkerSet()
	workers.add("shard-a", func() {})
	workers.add("shard-b", func() {})
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.executeRebalancePlan(
		ctx,
		rebalancePlan{actions: []rebalancePlanAction{
			{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
			{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
		}},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if err != nil {
		t.Fatalf("executeRebalancePlan() error = %v, want nil", err)
	}
	if result.started != 0 {
		t.Fatalf("executeRebalancePlan() started = %d, want 0", result.started)
	}
	assertShardList(t, result.movedShardIDs, nil)
	assertRebalanceExecutionCalls(t, manager.calls, nil)
	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
}

func TestExecuteLocalRebalanceShedShardsStopsRunningWorkersInOrder(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelledA int32
	var cancelledB int32
	workers.add("shard-a", func() { atomic.AddInt32(&cancelledA, 1) })
	workers.add("shard-b", func() { atomic.AddInt32(&cancelledB, 1) })

	got := executeLocalRebalanceShedShards(
		[]string{"shard-a", "missing", "", "shard-b"},
		workers,
	)
	assertShardList(t, got, []string{"shard-a", "shard-b"})
	// Shed workers keep their registration (the local stale-worker fence)
	// until their deferred done runs; they are stopping, not running.
	if !workers.has("shard-a") || !workers.has("shard-b") {
		t.Fatal("workers.has after shed = false, want true until the workers finish")
	}
	if workers.running("shard-a") || workers.running("shard-b") {
		t.Fatal("workers.running after shed = true, want false")
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&cancelledB); got != 1 {
		t.Fatalf("shard-b cancel calls = %d, want 1", got)
	}
}

func TestExecuteLocalRebalanceShedShardsNoopsWithoutWorkers(t *testing.T) {
	t.Parallel()

	got := executeLocalRebalanceShedShards([]string{"shard-a"}, nil)
	assertShardList(t, got, nil)
}

func TestExecuteRebalancePlanReturnsAcquireError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceExecutionManager{
		acquireResults: []acquireResult{{err: errBoom}},
	}
	c := newTestRebalanceExecutionConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.executeRebalancePlan(
		ctx,
		rebalancePlan{actions: []rebalancePlanAction{
			{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
		}},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("executeRebalancePlan() error = %v, want wraps %v", err, errBoom)
	}
	want := "execute rebalance acquire shard shard-a: acquire shard lease shard-a: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("executeRebalancePlan() error = %v, want %q", err, want)
	}
	if result.started != 0 {
		t.Fatalf("executeRebalancePlan() started = %d, want 0", result.started)
	}
	assertShardList(t, result.movedShardIDs, nil)
}

func TestExecuteRebalancePlanReturnsStartedCountBeforeClaimError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	acquiredLease := &recordingReleaseLease{}
	manager := &recordingRebalanceExecutionManager{
		acquireResults: []acquireResult{{lease: acquiredLease, acquired: true}},
		claimResults:   []claimResult{{err: errBoom}},
	}
	c := newTestRebalanceExecutionConsumer(manager)
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := c.executeRebalancePlan(
		ctx,
		rebalancePlan{actions: []rebalancePlanAction{
			{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
			{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
		}},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("executeRebalancePlan() error = %v, want wraps %v", err, errBoom)
	}
	want := "execute rebalance claim shard shard-b from donor-a: claim shard lease shard-b from donor-a: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("executeRebalancePlan() error = %v, want %q", err, want)
	}
	if result.started != 1 {
		t.Fatalf("executeRebalancePlan() started = %d, want 1", result.started)
	}
	assertShardList(t, result.movedShardIDs, []string{"shard-a"})
	if !workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = false, want true")
	}

	workers.stopAll()
	waitWorkerGroupDone(t, &workerWG)
	if acquiredLease.calls != 1 {
		t.Fatalf("acquired lease Release calls = %d, want 1", acquiredLease.calls)
	}
}

func TestExecuteRebalancePlanReturnsInvalidActionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action rebalancePlanAction
		want   string
	}{
		{
			name:   "missing shard",
			action: rebalancePlanAction{kind: rebalancePlanAcquireUnowned},
			want:   "execute rebalance acquire_unowned action: missing shard ID",
		},
		{
			name:   "missing donor",
			action: rebalancePlanAction{kind: rebalancePlanClaimDonor, shardID: "shard-a"},
			want:   "execute rebalance claim action for shard shard-a: missing donor",
		},
		{
			name:   "unknown kind",
			action: rebalancePlanAction{kind: "unknown", shardID: "shard-a"},
			want:   "execute rebalance action for shard shard-a: unknown action kind \"unknown\"",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestRebalanceExecutionConsumer(&recordingRebalanceExecutionManager{})
			workers := newShardWorkerSet()
			var workerWG sync.WaitGroup
			workerErrCh := make(chan error, 1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			result, err := c.executeRebalancePlan(
				ctx,
				rebalancePlan{actions: []rebalancePlanAction{tt.action}},
				workers,
				&workerWG,
				workerErrCh,
				cancel,
			)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("executeRebalancePlan() error = %v, want %q", err, tt.want)
			}
			if result.started != 0 {
				t.Fatalf("executeRebalancePlan() started = %d, want 0", result.started)
			}
			assertShardList(t, result.movedShardIDs, nil)
		})
	}
}

func newTestRebalanceExecutionConsumer(manager lease.Manager) *Consumer {
	c := newTestRegisteredShardWorkerConsumer(nil)
	c.leaseManager = manager
	c.leaseOwner = "owner"
	c.tuning.retryMaxAttempts = 1
	c.tuning.heartbeatTTL = 30 * time.Millisecond
	return c
}

func assertRebalanceExecutionCalls(t *testing.T, got, want []rebalanceExecutionCall) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("rebalance execution calls = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rebalance execution call %d = %#v, want %#v", i, got[i], want[i])
		}
	}
}
