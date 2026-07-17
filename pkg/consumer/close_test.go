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

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// closableLeaseManager is a lease manager with a Close method, standing in for
// a backend manager (like the Valkey one) that owns its own client. It tracks
// lifecycle violations: any manager call after Close, or a Close that fires
// while a call is still in flight.
type closableLeaseManager struct {
	fakeLeaseManager

	closeErr error

	mu               sync.Mutex
	closeCalls       int
	heartbeats       int
	inFlight         int
	usedWhileClosed  bool
	closedWhileInUse bool
}

func (m *closableLeaseManager) Heartbeat(context.Context, string, string, time.Duration) error {
	m.mu.Lock()
	if m.closeCalls > 0 {
		m.usedWhileClosed = true
	}
	m.inFlight++
	m.heartbeats++
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.inFlight--
		m.mu.Unlock()
	}()
	return nil
}

func (m *closableLeaseManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inFlight > 0 {
		m.closedWhileInUse = true
	}
	m.closeCalls++
	return m.closeErr
}

func (m *closableLeaseManager) closeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalls
}

func (m *closableLeaseManager) heartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeats
}

func (m *closableLeaseManager) assertLifecycleClean(t *testing.T) {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.usedWhileClosed {
		t.Fatal("lease manager was used after Close")
	}
	if m.closedWhileInUse {
		t.Fatal("lease manager was closed while a call was in flight")
	}
}

// closableAcquireManager grants leases like recordingAcquireManager while
// tracking Close/heartbeat lifecycle like closableLeaseManager.
type closableAcquireManager struct {
	*recordingAcquireManager
	closable closableLeaseManager
}

func (m *closableAcquireManager) Heartbeat(ctx context.Context, streamName, owner string, ttl time.Duration) error {
	return m.closable.Heartbeat(ctx, streamName, owner, ttl)
}

func (m *closableAcquireManager) Close() error {
	return m.closable.Close()
}

func newCloseTestConsumer(t *testing.T, store checkpoint.Store, opts ...Option) *Consumer {
	t.Helper()

	c, err := New(
		Config{StreamName: "stream", ConsumerGroup: "group"},
		&fakeKinesisClient{},
		store,
		func(context.Context, Record) error { return nil },
		opts...,
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	return c
}

func TestCloseClosesAutoCreatedLeaseManager(t *testing.T) {
	t.Parallel()

	manager := &closableLeaseManager{}
	c := newCloseTestConsumer(t, fakeProviderStore{manager: manager})

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if got := manager.closeCount(); got != 1 {
		t.Fatalf("lease manager Close calls = %d, want 1", got)
	}
}

func TestCloseIsIdempotentAndConcurrencySafe(t *testing.T) {
	t.Parallel()

	manager := &closableLeaseManager{}
	c := newCloseTestConsumer(t, fakeProviderStore{manager: manager})

	const closers = 8
	errs := make(chan error, closers)
	var wg sync.WaitGroup
	for range closers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- c.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close() error = %v, want nil", err)
		}
	}

	if err := c.Close(); err != nil {
		t.Fatalf("repeated Close() error = %v, want nil", err)
	}
	if got := manager.closeCount(); got != 1 {
		t.Fatalf("lease manager Close calls = %d, want 1", got)
	}
}

func TestCloseDoesNotCloseExplicitLeaseManager(t *testing.T) {
	t.Parallel()

	explicit := &closableLeaseManager{}
	// A provider store that fails proves WithLeaseManager wins without the
	// provider ever being consulted (New would fail otherwise).
	store := fakeProviderStore{err: errors.New("provider must not be consulted")}
	c := newCloseTestConsumer(t, store, WithLeaseManager(explicit))

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if got := explicit.closeCount(); got != 0 {
		t.Fatalf("explicit lease manager Close calls = %d, want 0", got)
	}
}

func TestCloseWithNonClosableAutoCreatedManagerReturnsNil(t *testing.T) {
	t.Parallel()

	c := newCloseTestConsumer(t, fakeProviderStore{manager: fakeLeaseManager{}})

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("repeated Close() error = %v, want nil", err)
	}
}

func TestCloseReturnsWrappedLeaseManagerCloseError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &closableLeaseManager{closeErr: errBoom}
	c := newCloseTestConsumer(t, fakeProviderStore{manager: manager})

	err := c.Close()
	if !errors.Is(err, errBoom) {
		t.Fatalf("Close() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "close lease manager: boom" {
		t.Fatalf("Close() error = %v, want %q", err, "close lease manager: boom")
	}

	// A repeated Close performs no second cleanup and reports the same result.
	if again := c.Close(); !errors.Is(again, errBoom) {
		t.Fatalf("repeated Close() error = %v, want the first result %v", again, err)
	}
	if got := manager.closeCount(); got != 1 {
		t.Fatalf("lease manager Close calls = %d, want 1", got)
	}
}

func TestStartAfterCloseReturnsErrConsumerClosed(t *testing.T) {
	t.Parallel()

	manager := &closableLeaseManager{}
	client := &fakeKinesisClient{}
	c, err := New(
		Config{StreamName: "stream", ConsumerGroup: "group"},
		client,
		fakeProviderStore{manager: manager},
		func(context.Context, Record) error { return nil },
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	startErr := c.Start(context.Background())
	if !errors.Is(startErr, ErrConsumerClosed) {
		t.Fatalf("Start() error = %v, want %v", startErr, ErrConsumerClosed)
	}
	if got := len(client.calls); got != 0 {
		t.Fatalf("ListShards calls after Close = %d, want 0", got)
	}
	if got := manager.heartbeatCount(); got != 0 {
		t.Fatalf("Heartbeat calls after Close = %d, want 0", got)
	}
	manager.assertLifecycleClean(t)
}

func TestCloseDuringStartStopsRunAndClosesManagerAfterLastUse(t *testing.T) {
	tests := []struct {
		name     string
		graceful bool
	}{
		{name: "drain disabled"},
		// With graceful drain configured, Close still force-stops the run:
		// drain only serves caller-ctx cancellation.
		{name: "drain enabled", graceful: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			manager := &closableAcquireManager{
				recordingAcquireManager: &recordingAcquireManager{
					result:   &recordingReleaseLease{},
					acquired: true,
					callCh:   make(chan acquireCall, 1),
				},
			}
			c, err := New(
				Config{StreamName: "stream", ConsumerGroup: "group"},
				&fakeKinesisClient{
					outs: []*kinesis.ListShardsOutput{
						{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
					},
				},
				fakeProviderStore{manager: manager},
				func(context.Context, Record) error { return nil },
			)
			if err != nil {
				t.Fatalf("New() error = %v, want nil", err)
			}
			c.tuning.heartbeatInterval = time.Millisecond
			c.gracefulDrain = tt.graceful
			c.drainTimeout = time.Minute

			workerStarted := make(chan struct{})
			workerCtxCanceled := make(chan struct{})
			c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
				_ = shardID
				close(workerStarted)
				<-ctx.Done()
				close(workerCtxCanceled)
				return "", 0, nil
			}

			done := runStart(context.Background(), c)
			_ = waitAcquireCall(t, manager.recordingAcquireManager)
			<-workerStarted

			closeDone := make(chan error, 1)
			go func() {
				closeDone <- c.Close()
			}()

			// Close force-stops the run — Start must return ErrConsumerClosed
			// without waiting on the never-cancelled caller ctx (and without a
			// drain even when one is configured).
			waitStartDone(t, done, ErrConsumerClosed)
			select {
			case <-workerCtxCanceled:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for worker context cancellation")
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("Close() error = %v, want nil", err)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for Close to return")
			}

			if got := manager.closable.closeCount(); got != 1 {
				t.Fatalf("lease manager Close calls = %d, want 1", got)
			}
			// The heartbeat loop was fully stopped before the manager was
			// closed; give any stray loop a moment to prove it is gone.
			time.Sleep(20 * time.Millisecond)
			manager.closable.assertLifecycleClean(t)
		})
	}
}

func TestCloseWaitsForInProgressGracefulDrain(t *testing.T) {
	t.Parallel()

	manager := &closableAcquireManager{
		recordingAcquireManager: &recordingAcquireManager{
			result:   &recordingReleaseLease{},
			acquired: true,
			callCh:   make(chan acquireCall, 1),
		},
	}
	c, err := New(
		Config{StreamName: "stream", ConsumerGroup: "group"},
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		fakeProviderStore{manager: manager},
		func(context.Context, Record) error { return nil },
		WithGracefulDrain(time.Minute),
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

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

	// Begin a graceful drain, then Close mid-drain: Close must wait for the
	// drain instead of cutting it short.
	cancel()
	waitForTrue(t, c.isDraining, "drain to begin")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close() returned %v while the drain was still running", err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := manager.closable.closeCount(); got != 0 {
		t.Fatalf("lease manager Close calls during drain = %d, want 0", got)
	}

	close(finishWorker)
	waitStartDone(t, done, nil)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Close to return after the drain")
	}
	if got := manager.closable.closeCount(); got != 1 {
		t.Fatalf("lease manager Close calls = %d, want 1", got)
	}
	manager.closable.assertLifecycleClean(t)
}

// blockingListShardsClient parks the initial ListShards call until its
// context is cancelled, so a test can hold Start inside shard listing.
type blockingListShardsClient struct {
	fakeKinesisClient

	listStarted chan struct{}
	once        sync.Once
}

func (c *blockingListShardsClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	_ = params
	_ = optFns
	c.once.Do(func() { close(c.listStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// blockingAcquireManager parks the initial Acquire call until its context is
// cancelled, so a test can hold Start inside initial lease acquisition.
type blockingAcquireManager struct {
	closableLeaseManager

	acquireStarted chan struct{}
	once           sync.Once
}

func (m *blockingAcquireManager) Acquire(ctx context.Context, _, _, _ string, _ time.Duration) (lease.Lease, bool, error) {
	m.once.Do(func() { close(m.acquireStarted) })
	<-ctx.Done()
	return nil, false, ctx.Err()
}

func TestCloseDuringInitialShardListingReturnsErrConsumerClosed(t *testing.T) {
	t.Parallel()

	manager := &closableLeaseManager{}
	client := &blockingListShardsClient{listStarted: make(chan struct{})}
	c, err := New(
		Config{StreamName: "stream", ConsumerGroup: "group"},
		client,
		fakeProviderStore{manager: manager},
		func(context.Context, Record) error { return nil },
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	done := runStart(context.Background(), c)
	<-client.listStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()

	assertCloseInterruptedStart(t, done)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if got := manager.closeCount(); got != 1 {
		t.Fatalf("lease manager Close calls = %d, want 1", got)
	}
	manager.assertLifecycleClean(t)
}

func TestCloseDuringInitialAcquisitionReturnsErrConsumerClosed(t *testing.T) {
	t.Parallel()

	manager := &blockingAcquireManager{acquireStarted: make(chan struct{})}
	c, err := New(
		Config{StreamName: "stream", ConsumerGroup: "group"},
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		fakeProviderStore{manager: manager},
		func(context.Context, Record) error { return nil },
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	done := runStart(context.Background(), c)
	<-manager.acquireStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()

	assertCloseInterruptedStart(t, done)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if got := manager.closeCount(); got != 1 {
		t.Fatalf("lease manager Close calls = %d, want 1", got)
	}
	manager.assertLifecycleClean(t)
}

// assertCloseInterruptedStart waits for Start to return and requires the
// normalized ErrConsumerClosed, not a leaked wrapped context.Canceled.
func assertCloseInterruptedStart(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("Start() error = %v, want %v", err, ErrConsumerClosed)
		}
		if errors.Is(err, context.Canceled) {
			t.Fatalf("Start() error = %v, want normalized without context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Start to return")
	}
}

// makingProviderStore hands out a fresh closable manager per LeaseManager
// call, so repeated consumer lifecycles can be audited manager-by-manager.
type makingProviderStore struct {
	fakeCheckpointStore

	mu      sync.Mutex
	created []*closableAcquireManager
}

func (s *makingProviderStore) LeaseManager() (lease.Manager, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := &closableAcquireManager{
		recordingAcquireManager: &recordingAcquireManager{},
	}
	s.created = append(s.created, m)
	return m, nil
}

func (s *makingProviderStore) lastCreated() *closableAcquireManager {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.created[len(s.created)-1]
}

func TestRepeatedLifecycleCyclesCloseEveryAutoCreatedManager(t *testing.T) {
	t.Parallel()

	const cycles = 5
	store := &makingProviderStore{}

	for i := 0; i < cycles; i++ {
		c, err := New(
			Config{StreamName: "stream", ConsumerGroup: "group"},
			&fakeKinesisClient{
				outs: []*kinesis.ListShardsOutput{
					{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
				},
			},
			store,
			func(context.Context, Record) error { return nil },
		)
		if err != nil {
			t.Fatalf("cycle %d: New() error = %v, want nil", i, err)
		}
		workerStarted := make(chan struct{})
		c.processShardRecordsLoopFn = func(ctx context.Context, shardID string) (string, int, error) {
			_ = shardID
			close(workerStarted)
			<-ctx.Done()
			return "", 0, nil
		}
		manager := store.lastCreated()
		manager.result = &recordingReleaseLease{}
		manager.acquired = true

		ctx, cancel := context.WithCancel(context.Background())
		done := runStart(ctx, c)
		<-workerStarted
		cancel()
		waitStartDone(t, done, nil)

		if err := c.Close(); err != nil {
			t.Fatalf("cycle %d: Close() error = %v, want nil", i, err)
		}
	}

	store.mu.Lock()
	created := append([]*closableAcquireManager(nil), store.created...)
	store.mu.Unlock()
	if len(created) != cycles {
		t.Fatalf("auto-created managers = %d, want %d", len(created), cycles)
	}
	for i, m := range created {
		if got := m.closable.closeCount(); got != 1 {
			t.Fatalf("cycle %d: lease manager Close calls = %d, want 1", i, got)
		}
		m.closable.assertLifecycleClean(t)
	}
}
