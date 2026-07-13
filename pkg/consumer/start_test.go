package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"

	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
)

func TestStartPropagatesShardListingError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := newTestStartConsumer(
		&fakeKinesisClient{err: errBoom},
		newRecordingHeartbeatManager(),
	)

	err := c.Start(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("Start() error = %v, want %q", err, "list shards: boom")
	}
}

func TestStartReturnsErrorWhenNoShardsDiscovered(t *testing.T) {
	t.Parallel()

	c := newTestStartConsumer(
		&fakeKinesisClient{outs: []*kinesis.ListShardsOutput{{}}},
		newRecordingHeartbeatManager(),
	)

	err := c.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want error")
	}
	if err.Error() != "no shards found for stream stream" {
		t.Fatalf("Start() error = %q, want %q", err.Error(), "no shards found for stream stream")
	}
}

func TestStartSendsWorkerHeartbeat(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestStartConsumer(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := runStart(ctx, c)

	call := waitHeartbeat(t, manager)
	cancel()
	waitStartDone(t, done, nil)

	assertHeartbeatCall(t, call, "stream", "owner", 30*time.Millisecond)
}

func TestStartAttemptsAcquisitionForDiscoveredShardIDs(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{acquired: false},
			{acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	_ = waitAcquireCall(t, manager)
	assertAcquireShardOrder(t, manager.calls, []string{"shard-1", "shard-2"})

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartDoesNotAcquireCompletedShard(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 1),
	}
	store := &readinessCheckpointStore{
		checkpoints: map[string]string{"shard-1": "SHARD_END:sequence-1"},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.store = store

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	assertNoAcquireCall(t, manager)
	cancel()
	waitStartDone(t, done, nil)
	if got := shardGetCalls(store, "shard-1"); got != 1 {
		t.Fatalf("shard-1 checkpoint Get calls = %d, want 1", got)
	}
}

func TestStartDoesNotAcquireChildWithIncompleteKnownParent(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("parent")},
					{ShardId: aws.String("child"), ParentShardId: aws.String("parent")},
				}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	assertAcquireShardOrder(t, manager.calls, []string{"parent"})
	assertNoAcquireCall(t, manager)

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartOverwritesDuplicateShardMetadataBeforeReadyFiltering(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("child")},
					{ShardId: aws.String("parent")},
					{ShardId: aws.String("child"), ParentShardId: aws.String("parent")},
				}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	assertAcquireShardOrder(t, manager.calls, []string{"parent"})
	assertNoAcquireCall(t, manager)

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartAcquiresChildWithCompletedParent(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 1),
		results: []acquireResult{
			{acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("parent")},
					{ShardId: aws.String("child"), ParentShardId: aws.String("parent")},
				}},
			},
		},
		manager,
	)
	c.store = &readinessCheckpointStore{
		checkpoints: map[string]string{"parent": "SHARD_END"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	assertAcquireShardOrder(t, manager.calls, []string{"child"})

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartAcquiresNewlyDiscoveredShardDuringRefresh(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 3),
		results: []acquireResult{
			{lease: shardLease, acquired: true},
			{acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
			},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	first := waitAcquireCall(t, manager)
	if first.shardID != "shard-1" {
		t.Fatalf("first acquired shard = %q, want shard-1", first.shardID)
	}
	second := waitAcquireCall(t, manager)
	if second.shardID != "shard-2" {
		t.Fatalf("refresh acquired shard = %q, want shard-2", second.shardID)
	}

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartAcquiresChildWhenParentCheckpointCompletesDuringRefresh(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 3),
		results: []acquireResult{
			{lease: shardLease, acquired: true},
			{acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("parent")},
					{ShardId: aws.String("child"), ParentShardId: aws.String("parent")},
				}},
				{Shards: []types.Shard{
					{ShardId: aws.String("parent")},
					{ShardId: aws.String("child"), ParentShardId: aws.String("parent")},
				}},
			},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Millisecond
	store := newMutableReadinessCheckpointStore()
	c.store = store

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	first := waitAcquireCall(t, manager)
	if first.shardID != "parent" {
		t.Fatalf("first acquired shard = %q, want parent", first.shardID)
	}
	store.set("parent", "SHARD_END:sequence-1")

	second := waitAcquireCall(t, manager)
	if second.shardID != "child" {
		t.Fatalf("refresh acquired shard = %q, want child", second.shardID)
	}

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartReturnsPeriodicRefreshListError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: shardLease, acquired: true},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
			errs: []error{nil, errBoom},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Millisecond

	err := c.Start(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("Start() error = %v, want %q", err, "list shards: boom")
	}
}

func TestStartReturnsPeriodicRefreshAcquisitionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 3),
		results: []acquireResult{
			{lease: shardLease, acquired: true},
			{err: errBoom},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
			},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Millisecond
	c.tuning.retryMaxAttempts = 1

	err := c.Start(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errBoom)
	}
	want := "acquire shard leases shard-2: acquire shard lease shard-2: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("Start() error = %v, want %q", err, want)
	}
}

func TestStartSuppressesContextCanceledFromInFlightPeriodicRefresh(t *testing.T) {
	t.Parallel()

	client := &cancelingRefreshKinesisClient{
		refreshStarted: make(chan struct{}),
	}
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh:   make(chan acquireCall, 1),
		result:   shardLease,
		acquired: true,
	}
	c := newTestStartConsumerWithLeaseManager(client, manager)
	c.tuning.shardSyncInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	waitRefreshListStarted(t, client.refreshStarted)
	cancel()
	waitStartDone(t, done, nil)
}

func TestStartSkipsEmptyShardIDDuringReadyFiltering(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("")}}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	assertNoAcquireCall(t, manager)
	cancel()
	waitStartDone(t, done, nil)
}

func TestStartReturnsReadyShardCheckpointReadError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.store = &readinessCheckpointStore{
		getErrs: map[string]error{"shard-1": errBoom},
	}

	err := c.Start(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errBoom)
	}
	want := "check ready shard shard-1 completion: check shard completion shard-1: read shard checkpoint shard-1: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("Start() error = %v, want %q", err, want)
	}
	assertAcquireShardOrder(t, manager.calls, nil)
}

func TestStartReturnsAcquisitionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{
		err: errBoom,
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.tuning.retryMaxAttempts = 1

	err := c.Start(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "acquire shard leases shard-1: acquire shard lease shard-1: boom" {
		t.Fatalf("Start() error = %v, want %q", err, "acquire shard leases shard-1: acquire shard lease shard-1: boom")
	}
}

func TestStartReleasesAcquiredLeasesOnContextCancellation(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	cancel()
	waitStartDone(t, done, nil)

	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
	if shardLease.ctx == nil {
		t.Fatal("Release context = nil, want context")
	}
}

func TestStartStopsAllInitialWorkersOnContextCancellation(t *testing.T) {
	t.Parallel()

	shardLeaseA := &recordingReleaseLease{}
	shardLeaseB := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{lease: shardLeaseA, acquired: true},
			{lease: shardLeaseB, acquired: true},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-a")},
					{ShardId: aws.String("shard-b")},
				}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	_ = waitAcquireCall(t, manager)
	cancel()
	waitStartDone(t, done, nil)

	if shardLeaseA.calls != 1 {
		t.Fatalf("shard-a Release calls = %d, want 1", shardLeaseA.calls)
	}
	if shardLeaseB.calls != 1 {
		t.Fatalf("shard-b Release calls = %d, want 1", shardLeaseB.calls)
	}
}

func TestStartGracefulDrainWaitsForNaturalWorkerExitOnContextCancellation(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.gracefulDrain = true
	c.drainTimeout = time.Second

	workerStarted := make(chan struct{})
	finishWorker := make(chan struct{})
	workerCtxCanceled := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		close(workerStarted)
		select {
		case <-ctx.Done():
			close(workerCtxCanceled)
			return "", 0, nil
		case <-finishWorker:
			return "", 0, nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	<-workerStarted
	cancel()

	assertStartStillRunning(t, done)
	select {
	case <-workerCtxCanceled:
		t.Fatal("worker context canceled during graceful drain")
	default:
	}

	close(finishWorker)
	waitStartDone(t, done, nil)

	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestStartGracefulDrainCheckpointsPendingProgressOnContextCancellation(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	store := &fakeCheckpointSaveStore{}
	c.store = store
	c.gracefulDrain = true
	c.drainTimeout = time.Second
	c.processShardRecordsLoopFn = nil

	firstPassDone := make(chan struct{})
	passCalls := 0
	c.processShardRecordsPassFn = func(ctx context.Context, shardID string, processedSinceCheckpoint int, iterator string) (string, int, string, error) {
		_ = shardID
		passCalls++
		if passCalls == 1 {
			if processedSinceCheckpoint != 0 {
				return "", processedSinceCheckpoint, "", errors.New("first pass received nonzero checkpoint count")
			}
			close(firstPassDone)
			return "sequence-1", 1, "", nil
		}
		if processedSinceCheckpoint != 1 {
			return "", processedSinceCheckpoint, "", errors.New("drain pass received unexpected checkpoint count")
		}
		for !c.isDraining() {
			select {
			case <-ctx.Done():
				return "", processedSinceCheckpoint, "", ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		return "", processedSinceCheckpoint, "", nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	<-firstPassDone
	cancel()

	waitStartDone(t, done, nil)
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	call := store.saveCalls[0]
	if call.shardID != "shard-1" {
		t.Fatalf("shardID = %q, want %q", call.shardID, "shard-1")
	}
	if call.sequenceNumber != "sequence-1" {
		t.Fatalf("sequenceNumber = %q, want %q", call.sequenceNumber, "sequence-1")
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestStartGracefulDrainTimeoutForceStopsWorkers(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.gracefulDrain = true
	c.drainTimeout = 10 * time.Millisecond

	workerStarted := make(chan struct{})
	workerCtxCanceled := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		close(workerStarted)
		<-ctx.Done()
		close(workerCtxCanceled)
		return "", 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	<-workerStarted
	cancel()

	waitStartDone(t, done, errGracefulDrainTimeout)
	select {
	case <-workerCtxCanceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forced worker stop")
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestStartGracefulDrainStillForceStopsWorkersOnFatalError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLeaseA := &recordingReleaseLease{}
	shardLeaseB := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{lease: shardLeaseA, acquired: true},
			{lease: shardLeaseB, acquired: true},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-a")},
					{ShardId: aws.String("shard-b")},
				}},
			},
		},
		manager,
	)
	c.gracefulDrain = true
	c.drainTimeout = time.Second

	shardAStarted := make(chan struct{})
	shardBStarted := make(chan struct{})
	returnShardError := make(chan struct{})
	shardBStopped := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		switch shardID {
		case "shard-a":
			close(shardAStarted)
			<-returnShardError
			return "", 0, errBoom
		case "shard-b":
			close(shardBStarted)
			<-ctx.Done()
			close(shardBStopped)
			return "", 0, nil
		default:
			return "", 0, errors.New("unexpected shard")
		}
	}

	done := runStart(context.Background(), c)

	_ = waitAcquireCall(t, manager)
	_ = waitAcquireCall(t, manager)
	<-shardAStarted
	<-shardBStarted
	close(returnShardError)

	waitStartDone(t, done, errBoom)
	select {
	case <-shardBStopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for non-failing worker to be force-stopped")
	}
	if shardLeaseA.calls != 1 {
		t.Fatalf("shard-a Release calls = %d, want 1", shardLeaseA.calls)
	}
	if shardLeaseB.calls != 1 {
		t.Fatalf("shard-b Release calls = %d, want 1", shardLeaseB.calls)
	}
}

func TestStartGracefulDrainForceStopsWorkersOnErrorDuringDrain(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLeaseA := &recordingReleaseLease{}
	shardLeaseB := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{lease: shardLeaseA, acquired: true},
			{lease: shardLeaseB, acquired: true},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-a")},
					{ShardId: aws.String("shard-b")},
				}},
			},
		},
		manager,
	)
	c.gracefulDrain = true
	c.drainTimeout = time.Second

	shardAStarted := make(chan struct{})
	shardBStarted := make(chan struct{})
	returnShardError := make(chan struct{})
	shardBStopped := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		switch shardID {
		case "shard-a":
			close(shardAStarted)
			<-returnShardError
			return "", 0, errBoom
		case "shard-b":
			close(shardBStarted)
			<-ctx.Done()
			close(shardBStopped)
			return "", 0, nil
		default:
			return "", 0, errors.New("unexpected shard")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	_ = waitAcquireCall(t, manager)
	<-shardAStarted
	<-shardBStarted
	cancel()

	assertStartStillRunning(t, done)
	close(returnShardError)

	waitStartDone(t, done, errBoom)
	select {
	case <-shardBStopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remaining worker to be force-stopped")
	}
	if shardLeaseA.calls != 1 {
		t.Fatalf("shard-a Release calls = %d, want 1", shardLeaseA.calls)
	}
	if shardLeaseB.calls != 1 {
		t.Fatalf("shard-b Release calls = %d, want 1", shardLeaseB.calls)
	}
}

func TestStartGracefulDrainReturnsFinalWorkerErrorDuringDrain(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.gracefulDrain = true
	c.drainTimeout = time.Second

	workerStarted := make(chan struct{})
	returnShardError := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx
		_ = shardID
		close(workerStarted)
		<-returnShardError
		return "", 0, errBoom
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	<-workerStarted
	cancel()

	assertStartStillRunning(t, done)
	close(returnShardError)

	waitStartDone(t, done, errBoom)
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestStartDoesNotReleaseNotAcquiredShards(t *testing.T) {
	t.Parallel()

	acquiredLease := &recordingReleaseLease{}
	notAcquiredLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{lease: acquiredLease, acquired: true},
			{lease: notAcquiredLease, acquired: false},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
			},
		},
		manager,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	_ = waitAcquireCall(t, manager)
	cancel()
	waitStartDone(t, done, nil)

	if acquiredLease.calls != 1 {
		t.Fatalf("acquired Release calls = %d, want 1", acquiredLease.calls)
	}
	if notAcquiredLease.calls != 0 {
		t.Fatalf("not acquired Release calls = %d, want 0", notAcquiredLease.calls)
	}
}

func TestStartRenewsAcquiredLease(t *testing.T) {
	t.Parallel()

	shardLease := &recordingRenewLease{ch: make(chan renewCall, 1)}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.tuning.heartbeatInterval = time.Millisecond
	c.tuning.heartbeatTTL = 45 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	call := waitRenewCall(t, shardLease)
	cancel()
	waitStartDone(t, done, nil)

	if call.ttl != 45*time.Millisecond {
		t.Fatalf("Renew ttl = %v, want %v", call.ttl, 45*time.Millisecond)
	}
	if call.ctx == nil {
		t.Fatal("Renew context = nil, want context")
	}
}

func TestStartReturnsRenewalError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingRenewLease{err: errBoom}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.tuning.heartbeatInterval = time.Millisecond

	err := c.Start(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "renew shard lease shard-1: boom" {
		t.Fatalf("Start() error = %v, want %q", err, "renew shard lease shard-1: boom")
	}
}

func TestStartWaitsForRenewalLoopsBeforeRelease(t *testing.T) {
	t.Parallel()

	shardLease := &orderedRenewReleaseLease{
		events: make(chan string, 3),
	}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.tuning.heartbeatInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	waitEvent(t, shardLease.events, "renew-start")
	cancel()
	waitStartDone(t, done, nil)

	waitEvent(t, shardLease.events, "renew-done")
	waitEvent(t, shardLease.events, "release")
}

func TestStartDoesNotRenewNotAcquiredShard(t *testing.T) {
	t.Parallel()

	notAcquiredLease := &recordingRenewLease{ch: make(chan renewCall, 1)}
	manager := &recordingAcquireManager{
		result:   notAcquiredLease,
		acquired: false,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.tuning.heartbeatInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	select {
	case <-notAcquiredLease.ch:
		t.Fatal("Renew called for not-acquired shard")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartBlocksUntilContextCancellation(t *testing.T) {
	t.Parallel()

	c := newTestStartConsumer(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		newRecordingHeartbeatManager(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	assertStartStillRunning(t, done)
	cancel()
	waitStartDone(t, done, nil)
}

func TestStartReturnsContextDeadlineExceeded(t *testing.T) {
	t.Parallel()

	c := newTestStartConsumer(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		newRecordingHeartbeatManager(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := c.Start(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func newTestStartConsumer(client kinesisAPI, manager *recordingHeartbeatManager) *Consumer {
	c := newTestHeartbeatConsumer(manager)
	c.client = client
	c.store = &fakeCheckpointSaveStore{}
	return c
}

func newTestStartConsumerWithLeaseManager(client kinesisAPI, manager lease.Manager) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatInterval = 10 * time.Millisecond
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		cfg: Config{
			StreamName: "stream",
		},
		client:       client,
		store:        &fakeCheckpointSaveStore{},
		leaseManager: manager,
		leaseOwner:   "owner",
		tuning:       tuning,
		logger:       slog.New(slog.DiscardHandler),
		reporter:     metrics.Nop{},
		processShardRecordsLoopFn: func(ctx context.Context, shardID string) (string, int, error) {
			_ = shardID
			<-ctx.Done()
			return "", 0, nil
		},
	}
}

func runStart(ctx context.Context, c *Consumer) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- c.Start(ctx)
	}()
	return done
}

func assertStartStillRunning(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("Start() returned before context cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func assertNoAcquireCall(t *testing.T, manager *recordingAcquireManager) {
	t.Helper()

	select {
	case call := <-manager.callCh:
		t.Fatalf("unexpected Acquire call for shard %q", call.shardID)
	case <-time.After(20 * time.Millisecond):
	}
}

func waitStartDone(t *testing.T, done <-chan error, want error) {
	t.Helper()

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("Start() error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Start to return")
	}
}

type orderedRenewReleaseLease struct {
	events chan string
}

func (l *orderedRenewReleaseLease) Renew(ctx context.Context, _ time.Duration) error {
	l.events <- "renew-start"
	<-ctx.Done()
	l.events <- "renew-done"
	return ctx.Err()
}

func (l *orderedRenewReleaseLease) Release(context.Context) error {
	l.events <- "release"
	return nil
}

func waitEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()

	select {
	case got := <-events:
		if got != want {
			t.Fatalf("event = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for event %q", want)
	}
}

type cancelingRefreshKinesisClient struct {
	fakeKinesisClient

	listCalls      int
	refreshOnce    sync.Once
	refreshStarted chan struct{}
}

func (c *cancelingRefreshKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	_ = optFns

	if params != nil {
		c.calls = append(c.calls, *params)
	}
	c.listCalls++
	if c.listCalls == 1 {
		return &kinesis.ListShardsOutput{
			Shards: []types.Shard{{ShardId: aws.String("shard-1")}},
		}, nil
	}

	c.refreshOnce.Do(func() {
		close(c.refreshStarted)
	})
	<-ctx.Done()
	return nil, ctx.Err()
}

func waitRefreshListStarted(t *testing.T, refreshStarted <-chan struct{}) {
	t.Helper()

	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh ListShards call")
	}
}

type mutableReadinessCheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]string
}

func newMutableReadinessCheckpointStore() *mutableReadinessCheckpointStore {
	return &mutableReadinessCheckpointStore{
		checkpoints: make(map[string]string),
	}
}

func (s *mutableReadinessCheckpointStore) Get(ctx context.Context, streamName, shardID string) (string, error) {
	_ = ctx
	_ = streamName

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoints[shardID], nil
}

func (s *mutableReadinessCheckpointStore) Save(context.Context, string, string, string) error {
	return nil
}

func (s *mutableReadinessCheckpointStore) Delete(context.Context, string, string) error {
	return nil
}

func (s *mutableReadinessCheckpointStore) set(shardID, checkpoint string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[shardID] = checkpoint
}
