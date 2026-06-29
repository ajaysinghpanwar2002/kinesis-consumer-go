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
		time.Millisecond,
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
		time.Millisecond,
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
		time.Hour,
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

func runRefreshAndRebalanceShardWorkersLoop(
	ctx context.Context,
	c *Consumer,
	shardSyncInterval time.Duration,
	rebalanceInterval time.Duration,
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
			rebalanceInterval,
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
