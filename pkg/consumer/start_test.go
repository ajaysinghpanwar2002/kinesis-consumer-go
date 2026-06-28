package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
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

func newTestStartConsumer(client *fakeKinesisClient, manager *recordingHeartbeatManager) *Consumer {
	c := newTestHeartbeatConsumer(manager)
	c.client = client
	c.store = &fakeCheckpointSaveStore{}
	return c
}

func newTestStartConsumerWithLeaseManager(client *fakeKinesisClient, manager lease.Manager) *Consumer {
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
