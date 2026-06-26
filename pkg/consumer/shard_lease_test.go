package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

type acquireCall struct {
	streamName string
	shardID    string
	owner      string
	ttl        time.Duration
}

type recordingAcquireManager struct {
	fakeLeaseManager

	call     acquireCall
	calls    []acquireCall
	callCh   chan acquireCall
	result   lease.Lease
	acquired bool
	err      error
	results  []acquireResult
}

type acquireResult struct {
	lease    lease.Lease
	acquired bool
	err      error
}

func (m *recordingAcquireManager) Acquire(_ context.Context, streamName, shardID, owner string, ttl time.Duration) (lease.Lease, bool, error) {
	call := acquireCall{
		streamName: streamName,
		shardID:    shardID,
		owner:      owner,
		ttl:        ttl,
	}
	m.call = call
	m.calls = append(m.calls, call)

	if m.callCh != nil {
		select {
		case m.callCh <- call:
		default:
		}
	}

	if len(m.results) > 0 {
		result := m.results[0]
		m.results = m.results[1:]
		return result.lease, result.acquired, result.err
	}

	return m.result, m.acquired, m.err
}

type fakeShardLease struct{}

func (fakeShardLease) Renew(context.Context, time.Duration) error {
	return nil
}

func (fakeShardLease) Release(context.Context) error {
	return nil
}

type recordingReleaseLease struct {
	ctx   context.Context
	calls int
	err   error
}

func (l *recordingReleaseLease) Renew(context.Context, time.Duration) error {
	return nil
}

func (l *recordingReleaseLease) Release(ctx context.Context) error {
	l.ctx = ctx
	l.calls++
	return l.err
}

type recordingRenewLease struct {
	ctx   context.Context
	ttl   time.Duration
	calls int
	err   error
}

func (l *recordingRenewLease) Renew(ctx context.Context, ttl time.Duration) error {
	l.ctx = ctx
	l.ttl = ttl
	l.calls++
	return l.err
}

func (l *recordingRenewLease) Release(context.Context) error {
	return nil
}

func TestAcquireShardLeaseSuccess(t *testing.T) {
	t.Parallel()

	wantLease := fakeShardLease{}
	manager := &recordingAcquireManager{
		result:   wantLease,
		acquired: true,
	}
	c := newTestAcquireConsumer(manager)

	gotLease, acquired, err := c.acquireShardLease(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("acquireShardLease() error = %v, want nil", err)
	}
	if !acquired {
		t.Fatal("acquireShardLease() acquired = false, want true")
	}
	if gotLease != wantLease {
		t.Fatalf("acquireShardLease() lease = %v, want %v", gotLease, wantLease)
	}
	assertAcquireCall(t, manager.call, "stream", "shard-1", "owner", 30*time.Millisecond)
}

func TestAcquireShardLeaseNotAcquired(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{}
	c := newTestAcquireConsumer(manager)

	gotLease, acquired, err := c.acquireShardLease(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("acquireShardLease() error = %v, want nil", err)
	}
	if acquired {
		t.Fatal("acquireShardLease() acquired = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("acquireShardLease() lease = %v, want nil", gotLease)
	}
	assertAcquireCall(t, manager.call, "stream", "shard-1", "owner", 30*time.Millisecond)
}

func TestAcquireShardLeaseUsesARNStreamKey(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	manager := &recordingAcquireManager{}
	c := newTestAcquireConsumer(manager)
	c.cfg = Config{StreamARN: streamARN}

	_, _, err := c.acquireShardLease(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("acquireShardLease() error = %v, want nil", err)
	}
	assertAcquireCall(t, manager.call, streamARN, "shard-1", "owner", 30*time.Millisecond)
}

func TestAcquireShardLeaseWrapsError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{err: errBoom}
	c := newTestAcquireConsumer(manager)

	gotLease, acquired, err := c.acquireShardLease(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("acquireShardLease() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "acquire shard lease shard-1: boom" {
		t.Fatalf("acquireShardLease() error = %v, want %q", err, "acquire shard lease shard-1: boom")
	}
	if acquired {
		t.Fatal("acquireShardLease() acquired = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("acquireShardLease() lease = %v, want nil", gotLease)
	}
}

func TestAcquireShardLeaseWithRetrySuccessWithoutRetry(t *testing.T) {
	t.Parallel()

	wantLease := fakeShardLease{}
	manager := &recordingAcquireManager{
		result:   wantLease,
		acquired: true,
	}
	c := newTestAcquireConsumer(manager)

	gotLease, acquired, err := c.acquireShardLeaseWithRetry(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("acquireShardLeaseWithRetry() error = %v, want nil", err)
	}
	if !acquired {
		t.Fatal("acquireShardLeaseWithRetry() acquired = false, want true")
	}
	if gotLease != wantLease {
		t.Fatalf("acquireShardLeaseWithRetry() lease = %v, want %v", gotLease, wantLease)
	}
	if len(manager.calls) != 1 {
		t.Fatalf("Acquire calls = %d, want 1", len(manager.calls))
	}
}

func TestAcquireShardLeaseWithRetryRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	wantLease := fakeShardLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{err: errBoom},
			{err: errBoom},
			{lease: wantLease, acquired: true},
		},
	}
	c := newTestAcquireConsumer(manager)
	c.tuning.retryMaxAttempts = 3
	c.tuning.retryBackoff = 0

	gotLease, acquired, err := c.acquireShardLeaseWithRetry(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("acquireShardLeaseWithRetry() error = %v, want nil", err)
	}
	if !acquired {
		t.Fatal("acquireShardLeaseWithRetry() acquired = false, want true")
	}
	if gotLease != wantLease {
		t.Fatalf("acquireShardLeaseWithRetry() lease = %v, want %v", gotLease, wantLease)
	}
	if len(manager.calls) != 3 {
		t.Fatalf("Acquire calls = %d, want 3", len(manager.calls))
	}
}

func TestAcquireShardLeaseWithRetryDoesNotRetryNotAcquired(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{}
	c := newTestAcquireConsumer(manager)
	c.tuning.retryMaxAttempts = 3
	c.tuning.retryBackoff = 0

	gotLease, acquired, err := c.acquireShardLeaseWithRetry(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("acquireShardLeaseWithRetry() error = %v, want nil", err)
	}
	if acquired {
		t.Fatal("acquireShardLeaseWithRetry() acquired = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("acquireShardLeaseWithRetry() lease = %v, want nil", gotLease)
	}
	if len(manager.calls) != 1 {
		t.Fatalf("Acquire calls = %d, want 1", len(manager.calls))
	}
}

func TestAcquireShardLeaseWithRetryReturnsLastErrorAfterExhaustion(t *testing.T) {
	t.Parallel()

	errFirst := errors.New("first")
	errLast := errors.New("last")
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{err: errFirst},
			{err: errLast},
		},
	}
	c := newTestAcquireConsumer(manager)
	c.tuning.retryMaxAttempts = 2
	c.tuning.retryBackoff = 0

	gotLease, acquired, err := c.acquireShardLeaseWithRetry(context.Background(), "shard-1")
	if !errors.Is(err, errLast) {
		t.Fatalf("acquireShardLeaseWithRetry() error = %v, want wraps %v", err, errLast)
	}
	if err == nil || err.Error() != "acquire shard lease shard-1: last" {
		t.Fatalf("acquireShardLeaseWithRetry() error = %v, want %q", err, "acquire shard lease shard-1: last")
	}
	if acquired {
		t.Fatal("acquireShardLeaseWithRetry() acquired = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("acquireShardLeaseWithRetry() lease = %v, want nil", gotLease)
	}
	if len(manager.calls) != 2 {
		t.Fatalf("Acquire calls = %d, want 2", len(manager.calls))
	}
}

func TestAcquireShardLeaseWithRetryStopsWhenContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{
		err:    errBoom,
		callCh: make(chan acquireCall, 1),
	}
	c := newTestAcquireConsumer(manager)
	c.tuning.retryMaxAttempts = 2
	c.tuning.retryBackoff = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := c.acquireShardLeaseWithRetry(ctx, "shard-1")
		done <- err
	}()

	waitAcquireCall(t, manager)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("acquireShardLeaseWithRetry() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquireShardLeaseWithRetry to return")
	}
	if len(manager.calls) != 1 {
		t.Fatalf("Acquire calls = %d, want 1", len(manager.calls))
	}
}

func TestRenewShardLeaseNilLeaseNoop(t *testing.T) {
	t.Parallel()

	err := (&Consumer{}).renewShardLease(context.Background(), "shard-1", nil)
	if err != nil {
		t.Fatalf("renewShardLease() error = %v, want nil", err)
	}
}

func TestRenewShardLeaseSuccess(t *testing.T) {
	t.Parallel()

	shardLease := &recordingRenewLease{}
	c := newTestLeaseConsumer(30 * time.Millisecond)

	err := c.renewShardLease(context.Background(), "shard-1", shardLease)
	if err != nil {
		t.Fatalf("renewShardLease() error = %v, want nil", err)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Renew calls = %d, want 1", shardLease.calls)
	}
}

func TestRenewShardLeaseForwardsContextAndTTL(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	shardLease := &recordingRenewLease{}
	c := newTestLeaseConsumer(45 * time.Millisecond)

	err := c.renewShardLease(ctx, "shard-1", shardLease)
	if err != nil {
		t.Fatalf("renewShardLease() error = %v, want nil", err)
	}
	if shardLease.ctx != ctx {
		t.Fatalf("Renew context = %v, want %v", shardLease.ctx, ctx)
	}
	if shardLease.ttl != 45*time.Millisecond {
		t.Fatalf("Renew ttl = %v, want %v", shardLease.ttl, 45*time.Millisecond)
	}
}

func TestRenewShardLeaseWrapsError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingRenewLease{err: errBoom}
	c := newTestLeaseConsumer(30 * time.Millisecond)

	err := c.renewShardLease(context.Background(), "shard-1", shardLease)
	if !errors.Is(err, errBoom) {
		t.Fatalf("renewShardLease() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "renew shard lease shard-1: boom" {
		t.Fatalf("renewShardLease() error = %v, want %q", err, "renew shard lease shard-1: boom")
	}
	if shardLease.calls != 1 {
		t.Fatalf("Renew calls = %d, want 1", shardLease.calls)
	}
}

func TestReleaseShardLeaseNilLeaseNoop(t *testing.T) {
	t.Parallel()

	err := (&Consumer{}).releaseShardLease(context.Background(), "shard-1", nil)
	if err != nil {
		t.Fatalf("releaseShardLease() error = %v, want nil", err)
	}
}

func TestReleaseShardLeaseSuccess(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}

	err := (&Consumer{}).releaseShardLease(context.Background(), "shard-1", shardLease)
	if err != nil {
		t.Fatalf("releaseShardLease() error = %v, want nil", err)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func TestReleaseShardLeaseForwardsContext(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	shardLease := &recordingReleaseLease{}

	err := (&Consumer{}).releaseShardLease(ctx, "shard-1", shardLease)
	if err != nil {
		t.Fatalf("releaseShardLease() error = %v, want nil", err)
	}
	if shardLease.ctx != ctx {
		t.Fatalf("Release context = %v, want %v", shardLease.ctx, ctx)
	}
}

func TestReleaseShardLeaseWrapsError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingReleaseLease{err: errBoom}

	err := (&Consumer{}).releaseShardLease(context.Background(), "shard-1", shardLease)
	if !errors.Is(err, errBoom) {
		t.Fatalf("releaseShardLease() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "release shard lease shard-1: boom" {
		t.Fatalf("releaseShardLease() error = %v, want %q", err, "release shard lease shard-1: boom")
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func newTestLeaseConsumer(heartbeatTTL time.Duration) *Consumer {
	return &Consumer{
		tuning: tuningConfig{
			heartbeatTTL: heartbeatTTL,
		},
	}
}

func newTestAcquireConsumer(manager *recordingAcquireManager) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		cfg: Config{
			StreamName: "stream",
		},
		leaseManager: manager,
		leaseOwner:   "owner",
		tuning:       tuning,
	}
}

func waitAcquireCall(t *testing.T, manager *recordingAcquireManager) acquireCall {
	t.Helper()

	select {
	case call := <-manager.callCh:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Acquire call")
		return acquireCall{}
	}
}

func assertAcquireCall(t *testing.T, call acquireCall, streamName, shardID, owner string, ttl time.Duration) {
	t.Helper()

	if call.streamName != streamName {
		t.Fatalf("Acquire streamName = %q, want %q", call.streamName, streamName)
	}
	if call.shardID != shardID {
		t.Fatalf("Acquire shardID = %q, want %q", call.shardID, shardID)
	}
	if call.owner != owner {
		t.Fatalf("Acquire owner = %q, want %q", call.owner, owner)
	}
	if call.ttl != ttl {
		t.Fatalf("Acquire ttl = %v, want %v", call.ttl, ttl)
	}
}
