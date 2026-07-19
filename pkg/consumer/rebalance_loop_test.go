package consumer

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
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
	// The shed worker keeps its registration as the stale-worker fence until
	// its deferred done runs; it is stopping, no longer running.
	if !workers.has("shard-a") {
		t.Fatal("workers.has(shard-a) = false after scheduled shed, want true until the worker finishes")
	}
	if workers.running("shard-a") {
		t.Fatal("workers.running(shard-a) = true after scheduled shed, want false")
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
	logHandler := newCapturingHandler()
	reporter := &recordingReporter{}
	c.logger = slog.New(logHandler)
	c.reporter = reporter
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

	// The failed first pass is survivable but never silent: one failure
	// counter and one warn, then the next tick succeeded (asserted above).
	failures := reporter.countersNamed(metricRebalancePassFailures)
	if len(failures) != 1 {
		t.Fatalf("rebalance_pass_failures calls = %d, want 1", len(failures))
	}
	assertCounterTags(t, failures[0], map[string]string{"stream": "stream"})
	var warns []capturedRecord
	for _, rec := range logHandler.snapshot() {
		if rec.message == "rebalance pass failed" {
			warns = append(warns, rec)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("rebalance pass warn logs = %d, want 1", len(warns))
	}
	if warns[0].level != slog.LevelWarn {
		t.Fatalf("rebalance pass log level = %v, want %v", warns[0].level, slog.LevelWarn)
	}
	if warns[0].attrs["error"] == "" {
		t.Fatal("rebalance pass log error attribute missing")
	}
}

func TestRefreshAndRebalanceShardWorkersLoopSurvivesTransientRefreshError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{}
	c := newTestRebalanceOnceConsumer(manager)
	logHandler := newCapturingHandler()
	reporter := &recordingReporter{}
	c.logger = slog.New(logHandler)
	c.reporter = reporter
	client := &fakeKinesisClient{
		errs:       []error{errBoom, nil},
		listCallCh: make(chan kinesis.ListShardsInput, 3),
	}
	c.client = client
	knownShards := map[string]types.Shard{}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
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

	// The first refresh fails; reaching a second ListShards call proves the
	// loop retried instead of returning, and a third call proves the second
	// (recovery) pass fully completed and recorded its success.
	waitListShardsCall(t, client.listCallCh)
	waitListShardsCall(t, client.listCallCh)
	waitListShardsCall(t, client.listCallCh)
	health := c.Health().ShardSync
	if health.ConsecutiveFailures != 0 || health.LastError != nil || health.LastSuccess.IsZero() {
		t.Fatalf("recovered health = %+v, want 0 consecutive failures, nil error, non-zero last success", health)
	}

	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)

	// The failed pass is survivable but never silent: one failure counter and
	// one structured warn carrying the retry diagnostics.
	failures := reporter.countersNamed(metricShardSyncFailures)
	if len(failures) != 1 {
		t.Fatalf("shard_sync_failures calls = %d, want 1", len(failures))
	}
	assertCounterTags(t, failures[0], map[string]string{"stream": "stream"})
	var warns []capturedRecord
	for _, rec := range logHandler.snapshot() {
		if rec.message == "shard sync failed" {
			warns = append(warns, rec)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("shard sync warn logs = %d, want 1", len(warns))
	}
	if warns[0].level != slog.LevelWarn {
		t.Fatalf("shard sync log level = %v, want %v", warns[0].level, slog.LevelWarn)
	}
	for _, attr := range []string{"error", "consecutive_failures", "staleness", "retry_delay"} {
		if warns[0].attrs[attr] == "" {
			t.Fatalf("shard sync log attribute %q missing", attr)
		}
	}
}

func TestRefreshAndRebalanceShardWorkersLoopKeepsShardMapAcrossFailedPasses(t *testing.T) {
	t.Parallel()

	// A pass that lists successfully but then fails (readiness read error)
	// must not leak its partial discovery into the shared shard map: the last
	// successfully synced map — here just shard-old — is what rebalance ticks
	// keep seeing.
	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{
		workerOwners: []string{"self"},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.store = &flakyReadinessStore{failShard: "shard-new", failErr: errBoom, remaining: 1 << 30}
	// Every pass must keep failing until the test has observed two of them:
	// once the scripted outputs run out the fake client returns empty listings,
	// whose passes read nothing and SUCCEED — resetting ConsecutiveFailures to
	// 0 and making the wait condition only transiently true (the historic
	// ~1-in-20 flake in this test). Script enough failing listings to outlast
	// any plausible scheduling delay.
	failingListing := &kinesis.ListShardsOutput{
		Shards: []types.Shard{{ShardId: aws.String("shard-new")}},
	}
	outs := make([]*kinesis.ListShardsOutput, 4096)
	for i := range outs {
		outs[i] = failingListing
	}
	c.client = &fakeKinesisClient{outs: outs}
	knownShards := readyShardWorkerMap("shard-old")
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
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

	waitForTrue(t, func() bool {
		return c.Health().ShardSync.ConsecutiveFailures >= 2
	}, "two failing sync passes")
	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)

	if _, ok := knownShards["shard-new"]; ok {
		t.Fatal("failed sync passes leaked shard-new into the shared shard map")
	}
	if _, ok := knownShards["shard-old"]; !ok {
		t.Fatal("shard-old missing: the last known shard map was not preserved")
	}
}

func TestRefreshAndRebalanceShardWorkersLoopCommitsShardMapAfterRecoveredPass(t *testing.T) {
	t.Parallel()

	// Once a pass completes end to end, its discovery commits: after one
	// failed pass, the recovered pass rediscovers shard-new and it lands in
	// the shared shard map.
	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{
		workerOwners: []string{"self"},
	}
	c := newTestRebalanceOnceConsumer(manager)
	c.store = &flakyReadinessStore{failShard: "shard-new", failErr: errBoom, remaining: 1}
	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{{ShardId: aws.String("shard-new")}}},
			{Shards: []types.Shard{{ShardId: aws.String("shard-new")}}},
		},
		listCallCh: make(chan kinesis.ListShardsInput, 3),
	}
	c.client = client
	knownShards := map[string]types.Shard{}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
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

	// Pass 1 lists and fails at readiness; pass 2 relists and commits. A
	// third ListShards call proves pass 2 fully completed.
	waitListShardsCall(t, client.listCallCh)
	waitListShardsCall(t, client.listCallCh)
	waitListShardsCall(t, client.listCallCh)
	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)

	if _, ok := knownShards["shard-new"]; !ok {
		t.Fatal("recovered sync pass did not commit shard-new to the shared shard map")
	}
	health := c.Health().ShardSync
	if health.ConsecutiveFailures != 0 || health.LastError != nil {
		t.Fatalf("recovered health = %+v, want healthy", health)
	}
}

func TestRefreshAndRebalanceShardWorkersLoopReturnsFatalClientFaultRefreshError(t *testing.T) {
	t.Parallel()

	authErr := &smithy.GenericAPIError{
		Code:    "AccessDeniedException",
		Message: "denied",
		Fault:   smithy.FaultClient,
	}
	manager := &recordingRebalanceOnceManager{}
	c := newTestRebalanceOnceConsumer(manager)
	c.client = &fakeKinesisClient{err: authErr}
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

	err := waitRefreshAndRebalanceShardWorkersLoopDone(t, done, authErr)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "AccessDeniedException" {
		t.Fatalf("refreshAndRebalanceShardWorkersLoop() error = %v, want access-denied client fault", err)
	}
}

func TestRefreshAndRebalanceShardWorkersLoopReturnsStaleErrorPastMaxStaleness(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingRebalanceOnceManager{}
	c := newTestRebalanceOnceConsumer(manager)
	c.client = &fakeKinesisClient{err: errBoom}
	c.tuning.shardSyncMaxStaleness = time.Minute
	knownShards := map[string]types.Shard{}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	// The first now() call anchors the staleness clock; every later call is
	// two minutes on, so the first failed pass is already past the bound.
	base := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	var nowCalls atomic.Int32
	stalledNow := func() time.Time {
		if nowCalls.Add(1) == 1 {
			return base
		}
		return base.Add(2 * time.Minute)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- c.refreshAndRebalanceShardWorkersLoop(
			ctx,
			time.Millisecond,
			fixedRebalanceDelay(time.Hour),
			knownShards,
			newShardCompletionState(),
			nil,
			workers,
			&workerWG,
			workerErrCh,
			cancel,
			stalledNow,
		)
	}()

	err := waitRefreshAndRebalanceShardWorkersLoopDone(t, done, ErrShardSyncStale)
	if !errors.Is(err, errBoom) {
		t.Fatalf("refreshAndRebalanceShardWorkersLoop() error = %v, want preserves cause %v", err, errBoom)
	}
	want := "shard discovery stale for 2m0s (max 1m0s): list shards: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("refreshAndRebalanceShardWorkersLoop() error = %q, want %q", err, want)
	}
}

func TestRefreshAndRebalanceShardWorkersLoopShutdownDuringFailingSyncReturnsNil(t *testing.T) {
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

	waitForTrue(t, func() bool {
		return c.Health().ShardSync.ConsecutiveFailures >= 1
	}, "a failing sync pass to be recorded")

	// Ordinary shutdown while discovery is unhealthy is still a clean stop,
	// never misreported as a sync failure.
	cancel()
	waitRefreshAndRebalanceShardWorkersLoopDone(t, done, nil)
	waitWorkerGroupDone(t, &workerWG)
}

func waitListShardsCall(t *testing.T, calls <-chan kinesis.ListShardsInput) {
	t.Helper()

	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ListShards call")
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
