package consumer

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestRefreshAndRebalanceShardWorkersLoopRunsRebalanceOnTick(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"self"},
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
		executionCh: make(chan rebalanceExecutionCall, 1),
	}
	c := newTestRebalanceOnceConsumer(manager)
	knownShards := readyShardWorkerMap("shard-a")
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := runRefreshAndRebalanceShardWorkersLoop(
		ctx,
		c,
		time.Hour,
		fixedRebalanceDelay(time.Millisecond),
		knownShards,
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)

	call := waitRebalanceExecutionCall(t, manager.executionCh)
	if call.kind != rebalancePlanAcquireUnowned || call.shardID != "shard-a" {
		t.Fatalf("rebalance execution call = %#v, want acquire shard-a", call)
	}

	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)
}

func TestRefreshAndRebalanceShardWorkersLoopShedsLocalOverageOnRebalanceTick(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-a": "self",
			"shard-b": "self",
			"shard-c": "self",
			"shard-d": "worker-a",
		},
		workerOwners: []string{"self", "worker-a"},
		listCh:       make(chan string, 1),
	}
	c := newTestRebalanceOnceConsumer(manager)
	knownShards := readyShardWorkerMap("shard-a", "shard-b", "shard-c", "shard-d")
	workers := newShardWorkerSet()
	var cancelledA int32
	var cancelledB int32
	var cancelledC int32
	workers.add("shard-a", func() { atomic.AddInt32(&cancelledA, 1) })
	workers.add("shard-b", func() { atomic.AddInt32(&cancelledB, 1) })
	workers.add("shard-c", func() { atomic.AddInt32(&cancelledC, 1) })
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	cooldown := map[string]time.Time{}
	now := time.Date(2026, 6, 29, 7, 8, 9, 0, time.UTC)
	delays := make(chan time.Duration, 2)
	delayCalls := 0
	nextDelay := func() time.Duration {
		delayCalls++
		delay := time.Hour
		if delayCalls == 1 {
			delay = time.Millisecond
		}
		delays <- delay
		return delay
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.refreshAndRebalanceShardWorkersLoop(
			ctx,
			time.Hour,
			nextDelay,
			knownShards,
			newShardCompletionState(),
			cooldown,
			workers,
			&workerWG,
			workerErrCh,
			cancel,
			func() time.Time { return now },
		)
	}()

	if got := waitRebalanceDelay(t, delays); got != time.Millisecond {
		t.Fatalf("initial rebalance delay = %v, want %v", got, time.Millisecond)
	}
	waitRebalanceListCall(t, manager.listCh)
	if got := waitRebalanceDelay(t, delays); got != time.Hour {
		t.Fatalf("next rebalance delay = %v, want %v", got, time.Hour)
	}
	if workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = true after scheduled shed, want false")
	}
	if !workers.has("shard-b") {
		t.Fatal("workers.has(shard-b) = false after scheduled shed, want true")
	}
	if !workers.has("shard-c") {
		t.Fatal("workers.has(shard-c) = false after scheduled shed, want true")
	}
	if got := atomic.LoadInt32(&cancelledB); got != 0 {
		t.Fatalf("shard-b cancel calls = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&cancelledC); got != 0 {
		t.Fatalf("shard-c cancel calls = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls = %d, want 1", got)
	}
	assertRebalanceCooldown(t, cooldown, map[string]time.Time{
		"shard-a": now.Add(c.tuning.shardCooldownPeriod),
	})
	assertRebalanceExecutionCalls(t, manager.executionCalls, nil)

	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)
}

func TestRefreshAndRebalanceShardWorkersLoopSkipsRebalanceErrorAndContinues(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"self"},
		listErrs:     []error{errBoom, nil},
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
		listCh:      make(chan string, 2),
		executionCh: make(chan rebalanceExecutionCall, 1),
	}
	c := newTestRebalanceOnceConsumer(manager)
	knownShards := readyShardWorkerMap("shard-a")
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := runRefreshAndRebalanceShardWorkersLoop(
		ctx,
		c,
		time.Hour,
		fixedRebalanceDelay(time.Millisecond),
		knownShards,
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)

	waitRebalanceListCall(t, manager.listCh)
	call := waitRebalanceExecutionCall(t, manager.executionCh)
	if call.kind != rebalancePlanAcquireUnowned || call.shardID != "shard-a" {
		t.Fatalf("rebalance execution call = %#v, want acquire shard-a", call)
	}

	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)
}

func TestRefreshAndRebalanceShardWorkersLoopReturnsRefreshError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{}
	c := newTestRebalanceOnceConsumer(manager)
	c.client = &fakeKinesisClient{err: errBoom}
	knownShards := map[string]types.Shard{}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runRefreshAndRebalanceShardWorkersLoop(
		ctx,
		c,
		time.Millisecond,
		fixedRebalanceDelay(time.Hour),
		knownShards,
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)

	err := waitRefreshAndRebalanceShardWorkersLoopDone(t, done, errBoom)
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("refreshAndRebalanceShardWorkersLoop() error = %v, want %q", err, "list shards: boom")
	}
}

func TestStartRunsRuntimeRebalanceAfterInitialAcquisitionMiss(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"owner"},
		acquireResults: []acquireResult{
			{acquired: false},
			{lease: &recordingReleaseLease{}, acquired: true},
		},
		listCh:      make(chan string, 1),
		executionCh: make(chan rebalanceExecutionCall, 2),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-a")}}},
			},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Hour
	c.tuning.rebalanceIntervalMin = time.Millisecond
	c.tuning.rebalanceIntervalJitter = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	initial := waitRebalanceExecutionCall(t, manager.executionCh)
	if initial.kind != rebalancePlanAcquireUnowned || initial.shardID != "shard-a" {
		t.Fatalf("initial acquire call = %#v, want acquire shard-a", initial)
	}
	waitRebalanceListCall(t, manager.listCh)
	runtime := waitRebalanceExecutionCall(t, manager.executionCh)
	if runtime.kind != rebalancePlanAcquireUnowned || runtime.shardID != "shard-a" {
		t.Fatalf("runtime rebalance call = %#v, want acquire shard-a", runtime)
	}

	cancel()
	waitStartDone(t, done, nil)
}

func TestRefreshAndRebalanceShardWorkersLoopReschedulesRebalanceAfterTick(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners:  map[string]string{},
		workerOwners: []string{"self"},
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
		executionCh: make(chan rebalanceExecutionCall, 1),
	}
	c := newTestRebalanceOnceConsumer(manager)
	knownShards := readyShardWorkerMap("shard-a")
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	delays := make(chan time.Duration, 2)
	delayCalls := 0
	nextDelay := func() time.Duration {
		delayCalls++
		delay := time.Hour
		if delayCalls == 1 {
			delay = time.Millisecond
		}
		delays <- delay
		return delay
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runRefreshAndRebalanceShardWorkersLoop(
		ctx,
		c,
		time.Hour,
		nextDelay,
		knownShards,
		newShardCompletionState(),
		nil,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	)

	if got := waitRebalanceDelay(t, delays); got != time.Millisecond {
		t.Fatalf("initial rebalance delay = %v, want %v", got, time.Millisecond)
	}
	waitRebalanceExecutionCall(t, manager.executionCh)
	if got := waitRebalanceDelay(t, delays); got != time.Hour {
		t.Fatalf("next rebalance delay = %v, want %v", got, time.Hour)
	}

	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)
}

func TestRebalanceDelayNoJitterReturnsMin(t *testing.T) {
	t.Parallel()

	min := 10 * time.Second
	if got := rebalanceDelay(min, 0, rand.New(rand.NewSource(1))); got != min {
		t.Fatalf("rebalanceDelay() = %v, want %v", got, min)
	}
	if got := rebalanceDelay(min, -time.Second, rand.New(rand.NewSource(1))); got != min {
		t.Fatalf("rebalanceDelay() = %v, want %v", got, min)
	}
	if got := rebalanceDelay(min, time.Second, nil); got != min {
		t.Fatalf("rebalanceDelay() = %v, want %v", got, min)
	}
}

func TestRebalanceDelayAddsBoundedJitter(t *testing.T) {
	t.Parallel()

	min := 10 * time.Second
	jitter := 5 * time.Second
	rng := rand.New(rand.NewSource(1))
	sawJitter := false
	for i := 0; i < 50; i++ {
		got := rebalanceDelay(min, jitter, rng)
		if got < min || got >= min+jitter {
			t.Fatalf("rebalanceDelay() = %v, want in [%v, %v)", got, min, min+jitter)
		}
		if got > min {
			sawJitter = true
		}
	}
	if !sawJitter {
		t.Fatal("rebalanceDelay() never added jitter")
	}
}

func runRefreshAndRebalanceShardWorkersLoop(
	ctx context.Context,
	c *Consumer,
	shardSyncInterval time.Duration,
	nextRebalanceDelay func() time.Duration,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- c.refreshAndRebalanceShardWorkersLoop(
			ctx,
			shardSyncInterval,
			nextRebalanceDelay,
			knownShards,
			completionState,
			cooldown,
			workers,
			workerWG,
			workerErrCh,
			stopRun,
			time.Now,
		)
	}()
	return done
}

func fixedRebalanceDelay(delay time.Duration) func() time.Duration {
	return func() time.Duration {
		return delay
	}
}

func waitRefreshAndRebalanceShardWorkersLoopDone(t *testing.T, done <-chan error, want error) error {
	t.Helper()

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("refreshAndRebalanceShardWorkersLoop() error = %v, want %v", err, want)
		}
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refreshAndRebalanceShardWorkersLoop to return")
		return nil
	}
}

func waitRebalanceDelay(t *testing.T, delays <-chan time.Duration) time.Duration {
	t.Helper()

	select {
	case delay := <-delays:
		return delay
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rebalance delay")
		return 0
	}
}

func waitRebalanceExecutionCall(t *testing.T, calls <-chan rebalanceExecutionCall) rebalanceExecutionCall {
	t.Helper()

	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rebalance execution call")
		return rebalanceExecutionCall{}
	}
}

func waitRebalanceListCall(t *testing.T, calls <-chan string) string {
	t.Helper()

	select {
	case streamName := <-calls:
		return streamName
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rebalance list call")
		return ""
	}
}
