package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
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
	if !errors.Is(err, ErrNoShards) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrNoShards)
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

	assertHeartbeatCall(t, call, "group:stream", "owner", 30*time.Millisecond)
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

func TestStartSurvivesTransientPeriodicRefreshListError(t *testing.T) {
	t.Parallel()

	// One failed periodic refresh must not stop delivery: the shard-1 worker
	// keeps running through the failure, and the next successful refresh
	// discovers and acquires shard-2 (recovery).
	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	recoveredLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 3),
		results: []acquireResult{
			{lease: shardLease, acquired: true},
			{lease: recoveredLease, acquired: true},
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
			errs: []error{nil, errBoom, nil},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Millisecond

	var workerForceStopped atomic.Bool
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		<-ctx.Done()
		workerForceStopped.Store(true)
		return "", 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	first := waitAcquireCall(t, manager)
	if first.shardID != "shard-1" {
		t.Fatalf("first acquired shard = %q, want shard-1", first.shardID)
	}
	// The failed refresh happens between these two acquisitions; reaching the
	// recovered shard-2 acquisition proves the loop survived it.
	second := waitAcquireCall(t, manager)
	if second.shardID != "shard-2" {
		t.Fatalf("recovered acquired shard = %q, want shard-2", second.shardID)
	}
	if workerForceStopped.Load() {
		t.Fatal("shard-1 worker was stopped by a transient refresh failure")
	}
	health := c.Health().ShardSync
	if health.LastSuccess.IsZero() {
		t.Fatal("Health().ShardSync.LastSuccess = zero, want anchored")
	}

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartSurvivesTransientPeriodicRefreshAcquisitionError(t *testing.T) {
	t.Parallel()

	// A failed sync lease acquisition is a lease-backend blip, not a fatal
	// run error: the next sync attempt retries the same shard and succeeds.
	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	recoveredLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 4),
		results: []acquireResult{
			{lease: shardLease, acquired: true},
			{err: errBoom},
			{lease: recoveredLease, acquired: true},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				// The failing pass consumes the second listing and its
				// candidate map is discarded, so the recovery pass must
				// rediscover shard-2 from a fresh listing.
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
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

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	first := waitAcquireCall(t, manager)
	if first.shardID != "shard-1" {
		t.Fatalf("first acquired shard = %q, want shard-1", first.shardID)
	}
	failed := waitAcquireCall(t, manager)
	if failed.shardID != "shard-2" {
		t.Fatalf("failing acquired shard = %q, want shard-2", failed.shardID)
	}
	recovered := waitAcquireCall(t, manager)
	if recovered.shardID != "shard-2" {
		t.Fatalf("recovered acquired shard = %q, want shard-2", recovered.shardID)
	}

	cancel()
	waitStartDone(t, done, nil)
}

// failAfterFirstListClient serves one successful ListShards, then fails every
// later call with a fixed error — a persistently broken periodic refresh.
type failAfterFirstListClient struct {
	fakeKinesisClient

	calls atomic.Int32
	out   *kinesis.ListShardsOutput
	err   error
}

func (c *failAfterFirstListClient) ListShards(context.Context, *kinesis.ListShardsInput, ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	if c.calls.Add(1) == 1 {
		return c.out, nil
	}
	return nil, c.err
}

// flakyReadinessStore fails checkpoint Gets for one shard a fixed number of
// times, then behaves like an empty store.
type flakyReadinessStore struct {
	mu        sync.Mutex
	failShard string
	failErr   error
	remaining int
}

func (s *flakyReadinessStore) Get(_ context.Context, _, shardID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if shardID == s.failShard && s.remaining > 0 {
		s.remaining--
		return "", s.failErr
	}
	return "", nil
}

func (*flakyReadinessStore) Save(context.Context, string, string, string) error {
	return nil
}

func (*flakyReadinessStore) Delete(context.Context, string, string) error {
	return nil
}

func TestStartSurvivesTransientRefreshCheckpointReadError(t *testing.T) {
	t.Parallel()

	// A transient checkpoint failure during the readiness check of a refresh
	// pass must not stop delivery; the retried pass recovers and acquires the
	// newly discovered shard.
	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	recoveredLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 3),
		results: []acquireResult{
			{lease: shardLease, acquired: true},
			{lease: recoveredLease, acquired: true},
		},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				// The failing pass consumes the second listing and its
				// candidate map is discarded, so the recovery pass must
				// rediscover shard-2 from a fresh listing.
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
			},
		},
		manager,
	)
	c.store = &flakyReadinessStore{failShard: "shard-2", failErr: errBoom, remaining: 1}
	c.tuning.shardSyncInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	first := waitAcquireCall(t, manager)
	if first.shardID != "shard-1" {
		t.Fatalf("first acquired shard = %q, want shard-1", first.shardID)
	}
	second := waitAcquireCall(t, manager)
	if second.shardID != "shard-2" {
		t.Fatalf("recovered acquired shard = %q, want shard-2", second.shardID)
	}

	cancel()
	waitStartDone(t, done, nil)
}

// flakyListLeaseManager grants leases like recordingAcquireManager but fails
// lease listing with a queued error sequence.
type flakyListLeaseManager struct {
	*recordingAcquireManager

	mu       sync.Mutex
	listErrs []error
}

func (m *flakyListLeaseManager) List(context.Context, string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.listErrs) > 0 {
		err := m.listErrs[0]
		m.listErrs = m.listErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func TestStartSurvivesTransientRefreshLeaseListError(t *testing.T) {
	t.Parallel()

	// A transient lease-backend listing failure during a refresh pass must
	// not stop delivery; the retried pass recovers and acquires the newly
	// discovered shard.
	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	recoveredLease := &recordingReleaseLease{}
	manager := &flakyListLeaseManager{
		recordingAcquireManager: &recordingAcquireManager{
			callCh: make(chan acquireCall, 3),
			results: []acquireResult{
				{lease: shardLease, acquired: true},
				{lease: recoveredLease, acquired: true},
			},
		},
		// The initial acquisition's List succeeds; the first refresh pass
		// fails once; everything after recovers.
		listErrs: []error{nil, errBoom},
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				// The failing pass consumes the second listing and its
				// candidate map is discarded, so the recovery pass must
				// rediscover shard-2 from a fresh listing.
				{Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{ShardId: aws.String("shard-2")},
				}},
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

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	first := waitAcquireCall(t, manager.recordingAcquireManager)
	if first.shardID != "shard-1" {
		t.Fatalf("first acquired shard = %q, want shard-1", first.shardID)
	}
	second := waitAcquireCall(t, manager.recordingAcquireManager)
	if second.shardID != "shard-2" {
		t.Fatalf("recovered acquired shard = %q, want shard-2", second.shardID)
	}

	cancel()
	waitStartDone(t, done, nil)
}

func TestStartReturnsFatalPeriodicRefreshAuthError(t *testing.T) {
	t.Parallel()

	// Broken authorization is not survivable: no retry can repair it, so the
	// run stops immediately instead of riding the staleness bound.
	authErr := &types.AccessDeniedException{Message: aws.String("denied")}
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
			errs: []error{nil, authErr},
		},
		manager,
	)
	c.tuning.shardSyncInterval = time.Millisecond

	err := c.Start(context.Background())
	var gotAuth *types.AccessDeniedException
	if !errors.As(err, &gotAuth) {
		t.Fatalf("Start() error = %v, want wraps *types.AccessDeniedException", err)
	}
}

func TestStartReturnsShardSyncStaleErrorAfterPersistentRefreshFailure(t *testing.T) {
	t.Parallel()

	// Discovery that keeps failing past the staleness bound must eventually
	// surface the causal error through Start.
	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: shardLease, acquired: true},
		},
	}
	// The initial listing succeeds; every periodic refresh after it fails.
	client := &failAfterFirstListClient{
		out: &kinesis.ListShardsOutput{
			Shards: []types.Shard{{ShardId: aws.String("shard-1")}},
		},
		err: errBoom,
	}
	c := newTestStartConsumerWithLeaseManager(client, manager)
	c.tuning.shardSyncInterval = time.Millisecond
	c.tuning.shardSyncMaxStaleness = 50 * time.Millisecond

	err := c.Start(context.Background())
	if !errors.Is(err, ErrShardSyncStale) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrShardSyncStale)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want preserves cause %v", err, errBoom)
	}
	health := c.Health().ShardSync
	if health.ConsecutiveFailures < 1 {
		t.Fatalf("Health().ShardSync.ConsecutiveFailures = %d, want >= 1", health.ConsecutiveFailures)
	}
	if !errors.Is(health.LastError, errBoom) {
		t.Fatalf("Health().ShardSync.LastError = %v, want wraps %v", health.LastError, errBoom)
	}
}

func TestStartReturnsHeartbeatStaleErrorAfterPersistentHeartbeatFailure(t *testing.T) {
	t.Parallel()

	// Heartbeats that keep failing on a live run must stop it with the
	// causal error before peers can treat this worker as dead.
	errBoom := errors.New("heartbeat boom")
	manager := newRecordingHeartbeatManager()
	manager.err = errBoom
	c := newTestStartConsumer(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)

	err := c.Start(context.Background())
	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want preserves cause %v", err, errBoom)
	}
	health := c.Health().Heartbeat
	if health.ConsecutiveFailures < 1 {
		t.Fatalf("Health().Heartbeat.ConsecutiveFailures = %d, want >= 1", health.ConsecutiveFailures)
	}
	if !errors.Is(health.LastError, errBoom) {
		t.Fatalf("Health().Heartbeat.LastError = %v, want wraps %v", health.LastError, errBoom)
	}
}

func TestStartReturnsHeartbeatStaleErrorDuringInitialShardListing(t *testing.T) {
	t.Parallel()

	// Validity can lapse while Start is still inside its initial shard
	// listing; the cancellation it causes must surface as the heartbeat
	// error, not a bare context.Canceled.
	errBoom := errors.New("heartbeat boom")
	manager := newRecordingHeartbeatManager()
	manager.err = errBoom
	client := &blockingListShardsClient{listStarted: make(chan struct{})}
	c := newTestStartConsumer(client, manager)

	err := c.Start(context.Background())
	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want preserves cause %v", err, errBoom)
	}
}

// switchableHeartbeatAcquireManager acquires like its embedded
// recordingAcquireManager while letting the test flip heartbeat sends
// between success and failure mid-run.
type switchableHeartbeatAcquireManager struct {
	*recordingAcquireManager

	hbMu  sync.Mutex
	hbErr error
}

func (m *switchableHeartbeatAcquireManager) Heartbeat(context.Context, string, string, time.Duration) error {
	m.hbMu.Lock()
	defer m.hbMu.Unlock()
	return m.hbErr
}

func (m *switchableHeartbeatAcquireManager) setHeartbeatErr(err error) {
	m.hbMu.Lock()
	defer m.hbMu.Unlock()
	m.hbErr = err
}

func TestStartReturnsHeartbeatStaleErrorWhenWorkerEchoesCancellation(t *testing.T) {
	t.Parallel()

	// The heartbeat-triggered cancellation propagates into running workers,
	// which can push the resulting context.Canceled into workerErrCh and win
	// the exit race against runCtx.Done(). The echo must not mask the causal
	// heartbeat error.
	errBoom := errors.New("heartbeat boom")
	shardLease := &recordingReleaseLease{}
	manager := &switchableHeartbeatAcquireManager{
		recordingAcquireManager: &recordingAcquireManager{
			result:   shardLease,
			acquired: true,
		},
	}
	manager.setHeartbeatErr(errBoom)
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		<-ctx.Done()
		return "", 0, ctx.Err()
	}

	err := c.Start(context.Background())
	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want preserves cause %v", err, errBoom)
	}
}

func TestStartReturnsHeartbeatStaleErrorWhenWorkerCleanupFails(t *testing.T) {
	t.Parallel()

	// A heartbeat-triggered shutdown can also surface as a non-cancellation
	// worker error: here processing exits cleanly but the lease release fails
	// against the same dead backend. The cleanup error must not mask the
	// causal heartbeat error either.
	errBoom := errors.New("heartbeat boom")
	errRelease := errors.New("release failed")
	shardLease := &recordingReleaseLease{err: errRelease}
	manager := &switchableHeartbeatAcquireManager{
		recordingAcquireManager: &recordingAcquireManager{
			result:   shardLease,
			acquired: true,
		},
	}
	manager.setHeartbeatErr(errBoom)
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)

	err := c.Start(context.Background())
	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Start() error = %v, want preserves cause %v", err, errBoom)
	}
	if errors.Is(err, errRelease) {
		t.Fatalf("Start() error = %v, want the causal heartbeat error, not the release consequence", err)
	}
}

func TestStartGracefulDrainIgnoresHeartbeatValidityLossDuringDrain(t *testing.T) {
	t.Parallel()

	// Once the caller's ctx is cancelled and a graceful drain is running,
	// heartbeat-validity loss is part of shutdown, not a run failure: the
	// drain result must stay nil.
	errBoom := errors.New("heartbeat boom")
	shardLease := &recordingReleaseLease{}
	manager := &switchableHeartbeatAcquireManager{
		recordingAcquireManager: &recordingAcquireManager{
			result:   shardLease,
			acquired: true,
			callCh:   make(chan acquireCall, 1),
		},
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
	c.drainTimeout = 5 * time.Second

	workerStarted := make(chan struct{})
	finishWorker := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx
		_ = shardID
		close(workerStarted)
		<-finishWorker
		return "", 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager.recordingAcquireManager)
	<-workerStarted
	cancel()
	waitForTrue(t, c.isDraining, "drain to begin")

	// Fail every heartbeat and hold the drain until the validity deadline
	// (lastSuccess + ttl - interval) is well past: the loop keeps beating
	// through the drain, so its staleness signal must have fired by then.
	manager.setHeartbeatErr(errBoom)
	waitForTrue(t, func() bool {
		health := c.Health().Heartbeat
		return health.LastError != nil && time.Since(health.LastSuccess) >= 2*c.tuning.heartbeatTTL
	}, "heartbeat validity to lapse during drain")

	close(finishWorker)
	waitStartDone(t, done, nil)

	if failures := c.Health().Heartbeat.ConsecutiveFailures; failures < 1 {
		t.Fatalf("Health().Heartbeat.ConsecutiveFailures = %d, want >= 1", failures)
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

	shardLease := &recordingReleaseLease{released: make(chan struct{})}
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
	waitForRecordingRelease(t, shardLease)

	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
	if shardLease.ctx == nil {
		t.Fatal("Release context = nil, want context")
	}
}

func TestStartStopsAllInitialWorkersOnContextCancellation(t *testing.T) {
	t.Parallel()

	shardLeaseA := &recordingReleaseLease{released: make(chan struct{})}
	shardLeaseB := &recordingReleaseLease{released: make(chan struct{})}
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
	waitForRecordingRelease(t, shardLeaseA)
	waitForRecordingRelease(t, shardLeaseB)

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

	shardLease := &recordingReleaseLease{released: make(chan struct{})}
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

	waitStartDone(t, done, ErrDrainTimeout)
	select {
	case <-workerCtxCanceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forced worker stop")
	}
	waitForRecordingRelease(t, shardLease)
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

type boundedShutdownCheckpointStore struct {
	saveCh chan string
}

func (s *boundedShutdownCheckpointStore) Get(context.Context, string, string) (string, error) {
	return "", nil
}

func (s *boundedShutdownCheckpointStore) Save(_ context.Context, _, _ string, sequenceNumber string) error {
	select {
	case s.saveCh <- sequenceNumber:
	default:
	}
	return nil
}

func (*boundedShutdownCheckpointStore) Delete(context.Context, string, string) error {
	return nil
}

type boundedShutdownLease struct {
	released chan struct{}
	once     sync.Once
}

func (*boundedShutdownLease) Renew(context.Context, time.Duration) error {
	return nil
}

func (l *boundedShutdownLease) Release(context.Context) error {
	l.once.Do(func() { close(l.released) })
	return nil
}

type blockingDLQPublisher struct {
	started  chan<- context.Context
	release  <-chan struct{}
	returned chan<- struct{}
}

func (p blockingDLQPublisher) Publish(ctx context.Context, _ PoisonRecord) error {
	p.started <- ctx
	<-p.release // deliberately ignore ctx
	close(p.returned)
	return nil
}

func TestStartCancellationIsBoundedByUncooperativeCallbacks(t *testing.T) {
	const (
		drainTimeout     = 20 * time.Millisecond
		schedulingBudget = 250 * time.Millisecond
	)

	tests := []struct {
		name     string
		graceful bool
		dlq      bool
	}{
		{name: "graceful drain handler", graceful: true},
		{name: "graceful drain dlq", graceful: true, dlq: true},
		{name: "non-graceful handler"},
		{name: "non-graceful dlq", dlq: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackStarted := make(chan context.Context, 1)
			callbackRelease := make(chan struct{})
			callbackReturned := make(chan struct{})
			var releaseCallback sync.Once
			release := func() {
				releaseCallback.Do(func() { close(callbackRelease) })
			}
			t.Cleanup(release)

			client := &fakeKinesisClient{
				outs: []*kinesis.ListShardsOutput{
					{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				},
				getShardIteratorOut: &kinesis.GetShardIteratorOutput{
					ShardIterator: aws.String("iterator-1"),
				},
				getRecordsOut: &kinesis.GetRecordsOutput{
					NextShardIterator: aws.String("iterator-2"),
					Records: []types.Record{{
						SequenceNumber: aws.String("sequence-1"),
						PartitionKey:   aws.String("partition-1"),
					}},
				},
			}
			shardLease := &boundedShutdownLease{released: make(chan struct{})}
			manager := &recordingAcquireManager{
				result:   shardLease,
				acquired: true,
				callCh:   make(chan acquireCall, 1),
			}
			store := &boundedShutdownCheckpointStore{saveCh: make(chan string, 1)}
			c := newTestStartConsumerWithLeaseManager(client, manager)
			c.store = store
			c.processShardRecordsLoopFn = nil
			c.tuning.checkpointEvery = 1
			c.tuning.retryMaxAttempts = 1
			c.gracefulDrain = tt.graceful
			c.drainTimeout = drainTimeout
			// Force-stop joins workers for up to this budget so cooperative
			// ones can release their leases; keep it small so the hung
			// callback's abandonment stays inside schedulingBudget.
			c.tuning.shardLeaseReleaseTimeout = drainTimeout

			if tt.dlq {
				c.handler = func(context.Context, Record) error {
					return errors.New("handler failed")
				}
				c.failurePolicy = FailurePolicySendToDLQ
				c.dlqPublisher = blockingDLQPublisher{
					started:  callbackStarted,
					release:  callbackRelease,
					returned: callbackReturned,
				}
			} else {
				c.handler = func(ctx context.Context, _ Record) error {
					callbackStarted <- ctx
					<-callbackRelease // deliberately ignore ctx
					close(callbackReturned)
					return nil
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := runStart(ctx, c)
			_ = waitAcquireCall(t, manager)

			var callbackCtx context.Context
			select {
			case callbackCtx = <-callbackStarted:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for callback to start")
			}

			cancelledAt := time.Now()
			cancel()

			var startErr error
			select {
			case startErr = <-done:
			case <-time.After(schedulingBudget):
				t.Fatalf("Start did not return within %s of cancellation", schedulingBudget)
			}
			if tt.graceful {
				if !errors.Is(startErr, ErrDrainTimeout) {
					t.Fatalf("Start() error = %v, want wraps %v", startErr, ErrDrainTimeout)
				}
			} else if startErr != nil {
				t.Fatalf("Start() error = %v, want nil", startErr)
			}
			if elapsed := time.Since(cancelledAt); elapsed > schedulingBudget {
				t.Fatalf("Start returned after %s, want at most %s", elapsed, schedulingBudget)
			}

			select {
			case <-callbackCtx.Done():
			default:
				t.Fatal("callback context was not canceled before Start returned")
			}
			select {
			case <-callbackReturned:
				t.Fatal("callback returned before the test released it")
			default:
			}
			select {
			case sequence := <-store.saveCh:
				t.Fatalf("checkpoint %q saved while callback was still blocked", sequence)
			default:
			}

			// Let the detached worker finish. Its late success must be discarded
			// before checkpointing, but normal worker cleanup still releases the
			// lease and reaps the goroutine.
			release()
			select {
			case <-callbackReturned:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for callback to return")
			}
			select {
			case <-shardLease.released:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for late worker cleanup to release its lease")
			}
			select {
			case sequence := <-store.saveCh:
				t.Fatalf("late callback result saved checkpoint %q", sequence)
			default:
			}
		})
	}
}

func TestStartGracefulDrainStillForceStopsWorkersOnFatalError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLeaseA := &recordingReleaseLease{released: make(chan struct{})}
	shardLeaseB := &recordingReleaseLease{released: make(chan struct{})}
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
	waitForRecordingRelease(t, shardLeaseA)
	waitForRecordingRelease(t, shardLeaseB)
	if shardLeaseA.calls != 1 {
		t.Fatalf("shard-a Release calls = %d, want 1", shardLeaseA.calls)
	}
	if shardLeaseB.calls != 1 {
		t.Fatalf("shard-b Release calls = %d, want 1", shardLeaseB.calls)
	}
}

func TestStartGracefulDrainLetsHealthyWorkersFinishAfterErrorDuringDrain(t *testing.T) {
	t.Parallel()

	// One worker failing mid-drain must not force-stop the others: they finish
	// their drain (checkpoint + release) and the error surfaces afterwards.
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
	finishShardB := make(chan struct{})
	var shardBForceStopped atomic.Bool
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		switch shardID {
		case "shard-a":
			close(shardAStarted)
			<-returnShardError
			return "", 0, errBoom
		case "shard-b":
			close(shardBStarted)
			select {
			case <-ctx.Done():
				shardBForceStopped.Store(true)
			case <-finishShardB:
			}
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
	// shard-a has errored; the drain must keep waiting for shard-b.
	assertStartStillRunning(t, done)

	close(finishShardB)
	waitStartDone(t, done, errBoom)
	if shardBForceStopped.Load() {
		t.Fatal("healthy worker was force-stopped instead of finishing its drain")
	}
	if shardLeaseA.calls != 1 {
		t.Fatalf("shard-a Release calls = %d, want 1", shardLeaseA.calls)
	}
	if shardLeaseB.calls != 1 {
		t.Fatalf("shard-b Release calls = %d, want 1", shardLeaseB.calls)
	}
}

// drainTakeoverLease renews cleanly until the test flips lost, then fails
// ErrNotOwned — modeling a peer claiming the lease mid-drain.
type drainTakeoverLease struct {
	lost           atomic.Bool
	notOwnedRenews atomic.Int32
	releaseCalls   atomic.Int32
}

func (l *drainTakeoverLease) Renew(context.Context, time.Duration) error {
	if l.lost.Load() {
		l.notOwnedRenews.Add(1)
		return lease.ErrNotOwned
	}
	return nil
}

func (l *drainTakeoverLease) Release(context.Context) error {
	l.releaseCalls.Add(1)
	return nil
}

func TestStartGracefulDrainSurvivesPeerClaimDuringDrain(t *testing.T) {
	t.Parallel()

	// SHUT-1 acceptance: a drain outliving the heartbeat TTL loses one shard to
	// a peer (renew fails ErrNotOwned). That shard counts as drained; the other
	// shard finishes cleanly; Start returns nil on a clean Ctrl-C.
	lostLease := &drainTakeoverLease{}
	healthyLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		callCh: make(chan acquireCall, 2),
		results: []acquireResult{
			{lease: lostLease, acquired: true},
			{lease: healthyLease, acquired: true},
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
	c.drainTimeout = 5 * time.Second
	c.tuning.heartbeatInterval = time.Millisecond

	started := make(chan struct{}, 2)
	finishHealthy := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = shardID
		started <- struct{}{}
		select {
		case <-ctx.Done():
		case <-finishHealthy:
		}
		return "", 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	_ = waitAcquireCall(t, manager)
	<-started
	<-started
	cancel()

	// Flip the takeover only once the drain has actually begun, so the
	// ErrNotOwned is seen while draining.
	waitForTrue(t, c.isDraining, "drain to begin")
	lostLease.lost.Store(true)
	waitForTrue(t, func() bool { return lostLease.notOwnedRenews.Load() >= 1 }, "peer takeover to be observed")

	close(finishHealthy)
	waitStartDone(t, done, nil)

	if calls := lostLease.releaseCalls.Load(); calls != 0 {
		t.Fatalf("lost lease Release calls = %d, want 0 (peer owns it)", calls)
	}
	if healthyLease.calls != 1 {
		t.Fatalf("healthy lease Release calls = %d, want 1", healthyLease.calls)
	}
}

// heartbeatCountingAcquireManager counts Heartbeat sends so tests can observe
// worker-liveness continuing across a drain.
type heartbeatCountingAcquireManager struct {
	*recordingAcquireManager
	heartbeats atomic.Int32
}

func (m *heartbeatCountingAcquireManager) Heartbeat(context.Context, string, string, time.Duration) error {
	m.heartbeats.Add(1)
	return nil
}

func TestStartGracefulDrainKeepsHeartbeatingDuringDrain(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	manager := &heartbeatCountingAcquireManager{
		recordingAcquireManager: &recordingAcquireManager{
			result:   shardLease,
			acquired: true,
			callCh:   make(chan acquireCall, 1),
		},
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
	c.drainTimeout = 5 * time.Second
	c.tuning.heartbeatInterval = time.Millisecond

	workerStarted := make(chan struct{})
	finishWorker := make(chan struct{})
	c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
		_ = ctx
		_ = shardID
		close(workerStarted)
		<-finishWorker
		return "", 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager.recordingAcquireManager)
	<-workerStarted
	cancel()
	waitForTrue(t, c.isDraining, "drain to begin")

	// Worker liveness must keep being published while the drain runs, or peers
	// drop this worker from their snapshots after heartbeatTTL and claim its
	// shards mid-drain.
	base := manager.heartbeats.Load()
	waitForTrue(t, func() bool { return manager.heartbeats.Load() >= base+3 }, "heartbeats during drain")

	close(finishWorker)
	waitStartDone(t, done, nil)
}

func waitForTrue(t *testing.T, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
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

	acquiredLease := &recordingReleaseLease{released: make(chan struct{})}
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
	waitForRecordingRelease(t, acquiredLease)

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
	// Persistent renew failures are retried within the ttl budget first, then
	// surface as a ttl-exhaustion error wrapping the cause.
	if err == nil || !strings.Contains(err.Error(), "not renewed within ttl") {
		t.Fatalf("Start() error = %v, want ttl-exhaustion wrapping %q", err, "renew shard lease shard-1: boom")
	}
	if !strings.Contains(err.Error(), "renew shard lease shard-1: boom") {
		t.Fatalf("Start() error = %v, want cause %q preserved", err, "renew shard lease shard-1: boom")
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
	// A wide interval keeps the per-call renew deadline from firing before
	// the cancellation this test drives (the fake blocks until ctx-done).
	c.tuning.heartbeatInterval = 20 * time.Millisecond
	c.tuning.heartbeatTTL = 100 * time.Millisecond

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

func newTestStartConsumer(client KinesisAPI, manager *recordingHeartbeatManager) *Consumer {
	c := newTestHeartbeatConsumer(manager)
	c.client = client
	c.store = &fakeCheckpointSaveStore{}
	return c
}

func newTestStartConsumerWithLeaseManager(client KinesisAPI, manager lease.Manager) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatInterval = 10 * time.Millisecond
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		cfg: Config{
			StreamName:    "stream",
			ConsumerGroup: "group",
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

func waitForRecordingRelease(t *testing.T, shardLease *recordingReleaseLease) {
	t.Helper()

	select {
	case <-shardLease.released:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shard lease release")
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
