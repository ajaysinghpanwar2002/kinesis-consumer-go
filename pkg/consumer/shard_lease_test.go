package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"

	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
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

type claimCall struct {
	ctx           context.Context
	streamName    string
	shardID       string
	expectedOwner string
	newOwner      string
	ttl           time.Duration
}

type recordingClaimManager struct {
	fakeLeaseManager

	call    claimCall
	calls   []claimCall
	callCh  chan claimCall
	result  lease.Lease
	claimed bool
	err     error
	results []claimResult
}

type claimResult struct {
	lease   lease.Lease
	claimed bool
	err     error
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

func (m *recordingClaimManager) Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (lease.Lease, bool, error) {
	call := claimCall{
		ctx:           ctx,
		streamName:    streamName,
		shardID:       shardID,
		expectedOwner: expectedOwner,
		newOwner:      newOwner,
		ttl:           ttl,
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
		return result.lease, result.claimed, result.err
	}

	return m.result, m.claimed, m.err
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

type blockingReleaseLease struct {
	ctx   context.Context
	calls int
}

func (l *blockingReleaseLease) Renew(context.Context, time.Duration) error {
	return nil
}

func (l *blockingReleaseLease) Release(ctx context.Context) error {
	l.ctx = ctx
	l.calls++
	<-ctx.Done()
	return ctx.Err()
}

type recordingRenewLease struct {
	ctx   context.Context
	ttl   time.Duration
	calls int
	err   error
	ch    chan renewCall
}

type renewCall struct {
	ctx context.Context
	ttl time.Duration
}

func (l *recordingRenewLease) Renew(ctx context.Context, ttl time.Duration) error {
	l.ctx = ctx
	l.ttl = ttl
	l.calls++
	if l.ch != nil {
		select {
		case l.ch <- renewCall{ctx: ctx, ttl: ttl}:
		default:
		}
	}
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

func TestClaimShardLeaseSuccess(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	wantLease := fakeShardLease{}
	manager := &recordingClaimManager{
		result:  wantLease,
		claimed: true,
	}
	c := newTestClaimConsumer(manager)

	gotLease, claimed, err := c.claimShardLease(ctx, "shard-1", "donor")
	if err != nil {
		t.Fatalf("claimShardLease() error = %v, want nil", err)
	}
	if !claimed {
		t.Fatal("claimShardLease() claimed = false, want true")
	}
	if gotLease != wantLease {
		t.Fatalf("claimShardLease() lease = %v, want %v", gotLease, wantLease)
	}
	assertClaimCall(t, manager.call, ctx, "stream", "shard-1", "donor", "owner", 30*time.Millisecond)
}

func TestClaimShardLeaseNotClaimed(t *testing.T) {
	t.Parallel()

	manager := &recordingClaimManager{}
	c := newTestClaimConsumer(manager)

	gotLease, claimed, err := c.claimShardLease(context.Background(), "shard-1", "donor")
	if err != nil {
		t.Fatalf("claimShardLease() error = %v, want nil", err)
	}
	if claimed {
		t.Fatal("claimShardLease() claimed = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("claimShardLease() lease = %v, want nil", gotLease)
	}
	assertClaimCall(t, manager.call, nil, "stream", "shard-1", "donor", "owner", 30*time.Millisecond)
}

func TestClaimShardLeaseUsesARNStreamKey(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	manager := &recordingClaimManager{}
	c := newTestClaimConsumer(manager)
	c.cfg = Config{StreamARN: streamARN}

	_, _, err := c.claimShardLease(context.Background(), "shard-1", "donor")
	if err != nil {
		t.Fatalf("claimShardLease() error = %v, want nil", err)
	}
	assertClaimCall(t, manager.call, nil, streamARN, "shard-1", "donor", "owner", 30*time.Millisecond)
}

func TestClaimShardLeaseWrapsError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingClaimManager{err: errBoom}
	c := newTestClaimConsumer(manager)

	gotLease, claimed, err := c.claimShardLease(context.Background(), "shard-1", "donor")
	if !errors.Is(err, errBoom) {
		t.Fatalf("claimShardLease() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "claim shard lease shard-1 from donor: boom" {
		t.Fatalf("claimShardLease() error = %v, want %q", err, "claim shard lease shard-1 from donor: boom")
	}
	if claimed {
		t.Fatal("claimShardLease() claimed = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("claimShardLease() lease = %v, want nil", gotLease)
	}
}

func TestClaimShardLeaseWithRetrySuccessWithoutRetry(t *testing.T) {
	t.Parallel()

	wantLease := fakeShardLease{}
	manager := &recordingClaimManager{
		result:  wantLease,
		claimed: true,
	}
	c := newTestClaimConsumer(manager)

	gotLease, claimed, err := c.claimShardLeaseWithRetry(context.Background(), "shard-1", "donor")
	if err != nil {
		t.Fatalf("claimShardLeaseWithRetry() error = %v, want nil", err)
	}
	if !claimed {
		t.Fatal("claimShardLeaseWithRetry() claimed = false, want true")
	}
	if gotLease != wantLease {
		t.Fatalf("claimShardLeaseWithRetry() lease = %v, want %v", gotLease, wantLease)
	}
	if len(manager.calls) != 1 {
		t.Fatalf("Claim calls = %d, want 1", len(manager.calls))
	}
}

func TestClaimShardLeaseWithRetryRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	wantLease := fakeShardLease{}
	manager := &recordingClaimManager{
		results: []claimResult{
			{err: errBoom},
			{err: errBoom},
			{lease: wantLease, claimed: true},
		},
	}
	c := newTestClaimConsumer(manager)
	c.tuning.retryMaxAttempts = 3
	c.tuning.retryBackoff = 0

	gotLease, claimed, err := c.claimShardLeaseWithRetry(context.Background(), "shard-1", "donor")
	if err != nil {
		t.Fatalf("claimShardLeaseWithRetry() error = %v, want nil", err)
	}
	if !claimed {
		t.Fatal("claimShardLeaseWithRetry() claimed = false, want true")
	}
	if gotLease != wantLease {
		t.Fatalf("claimShardLeaseWithRetry() lease = %v, want %v", gotLease, wantLease)
	}
	if len(manager.calls) != 3 {
		t.Fatalf("Claim calls = %d, want 3", len(manager.calls))
	}
}

func TestClaimShardLeaseWithRetryDoesNotRetryNotClaimed(t *testing.T) {
	t.Parallel()

	manager := &recordingClaimManager{}
	c := newTestClaimConsumer(manager)
	c.tuning.retryMaxAttempts = 3
	c.tuning.retryBackoff = 0

	gotLease, claimed, err := c.claimShardLeaseWithRetry(context.Background(), "shard-1", "donor")
	if err != nil {
		t.Fatalf("claimShardLeaseWithRetry() error = %v, want nil", err)
	}
	if claimed {
		t.Fatal("claimShardLeaseWithRetry() claimed = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("claimShardLeaseWithRetry() lease = %v, want nil", gotLease)
	}
	if len(manager.calls) != 1 {
		t.Fatalf("Claim calls = %d, want 1", len(manager.calls))
	}
}

func TestClaimShardLeaseWithRetryReturnsLastErrorAfterExhaustion(t *testing.T) {
	t.Parallel()

	errFirst := errors.New("first")
	errLast := errors.New("last")
	manager := &recordingClaimManager{
		results: []claimResult{
			{err: errFirst},
			{err: errLast},
		},
	}
	c := newTestClaimConsumer(manager)
	c.tuning.retryMaxAttempts = 2
	c.tuning.retryBackoff = 0

	gotLease, claimed, err := c.claimShardLeaseWithRetry(context.Background(), "shard-1", "donor")
	if !errors.Is(err, errLast) {
		t.Fatalf("claimShardLeaseWithRetry() error = %v, want wraps %v", err, errLast)
	}
	if err == nil || err.Error() != "claim shard lease shard-1 from donor: last" {
		t.Fatalf("claimShardLeaseWithRetry() error = %v, want %q", err, "claim shard lease shard-1 from donor: last")
	}
	if claimed {
		t.Fatal("claimShardLeaseWithRetry() claimed = true, want false")
	}
	if gotLease != nil {
		t.Fatalf("claimShardLeaseWithRetry() lease = %v, want nil", gotLease)
	}
	if len(manager.calls) != 2 {
		t.Fatalf("Claim calls = %d, want 2", len(manager.calls))
	}
}

func TestClaimShardLeaseWithRetryStopsWhenContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingClaimManager{
		err:    errBoom,
		callCh: make(chan claimCall, 1),
	}
	c := newTestClaimConsumer(manager)
	c.tuning.retryMaxAttempts = 2
	c.tuning.retryBackoff = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := c.claimShardLeaseWithRetry(ctx, "shard-1", "donor")
		done <- err
	}()

	waitClaimCall(t, manager)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("claimShardLeaseWithRetry() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for claimShardLeaseWithRetry to return")
	}
	if len(manager.calls) != 1 {
		t.Fatalf("Claim calls = %d, want 1", len(manager.calls))
	}
}

func TestAcquireShardLeasesEmptyShardListReturnsEmptyMap(t *testing.T) {
	t.Parallel()

	manager := &recordingAcquireManager{}
	c := newTestAcquireConsumer(manager)

	got, err := c.acquireShardLeases(context.Background(), nil)
	if err != nil {
		t.Fatalf("acquireShardLeases() error = %v, want nil", err)
	}
	if got == nil {
		t.Fatal("acquireShardLeases() leases = nil, want empty map")
	}
	if len(got) != 0 {
		t.Fatalf("acquireShardLeases() len = %d, want 0", len(got))
	}
	if len(manager.calls) != 0 {
		t.Fatalf("Acquire calls = %d, want 0", len(manager.calls))
	}
}

func TestAcquireShardLeasesReturnsAcquiredLeasesByShardID(t *testing.T) {
	t.Parallel()

	lease1 := fakeShardLease{}
	lease2 := &recordingRenewLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: lease1, acquired: true},
			{lease: lease2, acquired: true},
		},
	}
	c := newTestAcquireConsumer(manager)

	got, err := c.acquireShardLeases(context.Background(), []string{"shard-1", "shard-2"})
	if err != nil {
		t.Fatalf("acquireShardLeases() error = %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("acquireShardLeases() len = %d, want 2", len(got))
	}
	if got["shard-1"] != lease1 {
		t.Fatalf("acquireShardLeases()[shard-1] = %v, want %v", got["shard-1"], lease1)
	}
	if got["shard-2"] != lease2 {
		t.Fatalf("acquireShardLeases()[shard-2] = %v, want %v", got["shard-2"], lease2)
	}
	assertAcquireShardOrder(t, manager.calls, []string{"shard-1", "shard-2"})
}

func TestAcquireShardLeasesSkipsNotAcquiredAndNilLeases(t *testing.T) {
	t.Parallel()

	wantLease := fakeShardLease{}
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: wantLease, acquired: true},
			{acquired: false},
			{acquired: true},
		},
	}
	c := newTestAcquireConsumer(manager)

	got, err := c.acquireShardLeases(context.Background(), []string{"shard-1", "shard-2", "shard-3"})
	if err != nil {
		t.Fatalf("acquireShardLeases() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("acquireShardLeases() len = %d, want 1", len(got))
	}
	if got["shard-1"] != wantLease {
		t.Fatalf("acquireShardLeases()[shard-1] = %v, want %v", got["shard-1"], wantLease)
	}
	if _, ok := got["shard-2"]; ok {
		t.Fatal("acquireShardLeases()[shard-2] exists, want skipped")
	}
	if _, ok := got["shard-3"]; ok {
		t.Fatal("acquireShardLeases()[shard-3] exists, want skipped")
	}
	assertAcquireShardOrder(t, manager.calls, []string{"shard-1", "shard-2", "shard-3"})
}

func TestAcquireShardLeasesStopsOnAcquisitionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	manager := &recordingAcquireManager{
		results: []acquireResult{
			{lease: fakeShardLease{}, acquired: true},
			{err: errBoom},
			{lease: fakeShardLease{}, acquired: true},
		},
	}
	c := newTestAcquireConsumer(manager)
	c.tuning.retryMaxAttempts = 1

	got, err := c.acquireShardLeases(context.Background(), []string{"shard-1", "shard-2", "shard-3"})
	if !errors.Is(err, errBoom) {
		t.Fatalf("acquireShardLeases() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "acquire shard leases shard-2: acquire shard lease shard-2: boom" {
		t.Fatalf("acquireShardLeases() error = %v, want %q", err, "acquire shard leases shard-2: acquire shard lease shard-2: boom")
	}
	if got != nil {
		t.Fatalf("acquireShardLeases() leases = %v, want nil", got)
	}
	assertAcquireShardOrder(t, manager.calls, []string{"shard-1", "shard-2"})
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

func TestRenewShardLeaseLoopStopsOnContextCancellationWithoutRenewing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shardLease := &recordingRenewLease{}
	c := newTestLeaseLoopConsumer(time.Hour, 30*time.Millisecond)

	err := c.renewShardLeaseLoop(ctx, "shard-1", shardLease)
	if err != nil {
		t.Fatalf("renewShardLeaseLoop() error = %v, want nil", err)
	}
	if shardLease.calls != 0 {
		t.Fatalf("Renew calls = %d, want 0", shardLease.calls)
	}
}

func TestRenewShardLeaseLoopRenewsOnTick(t *testing.T) {
	t.Parallel()

	shardLease := &recordingRenewLease{ch: make(chan renewCall, 1)}
	c := newTestLeaseLoopConsumer(time.Millisecond, 45*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := runRenewShardLeaseLoop(ctx, c, "shard-1", shardLease)

	call := waitRenewCall(t, shardLease)
	cancel()
	waitRenewLoopDone(t, done, nil)

	if call.ctx != ctx {
		t.Fatalf("Renew context = %v, want %v", call.ctx, ctx)
	}
	if call.ttl != 45*time.Millisecond {
		t.Fatalf("Renew ttl = %v, want %v", call.ttl, 45*time.Millisecond)
	}
}

func TestRenewShardLeaseLoopStopsOnRenewalError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	shardLease := &recordingRenewLease{err: errBoom}
	c := newTestLeaseLoopConsumer(time.Millisecond, 30*time.Millisecond)

	err := c.renewShardLeaseLoop(context.Background(), "shard-1", shardLease)
	if !errors.Is(err, errBoom) {
		t.Fatalf("renewShardLeaseLoop() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "renew shard lease shard-1: boom" {
		t.Fatalf("renewShardLeaseLoop() error = %v, want %q", err, "renew shard lease shard-1: boom")
	}
	if shardLease.calls != 1 {
		t.Fatalf("Renew calls = %d, want 1", shardLease.calls)
	}
}

func TestRenewShardLeaseLoopReturnsContextDeadlineExceeded(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	shardLease := &recordingRenewLease{}
	c := newTestLeaseLoopConsumer(time.Hour, 30*time.Millisecond)

	err := c.renewShardLeaseLoop(ctx, "shard-1", shardLease)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("renewShardLeaseLoop() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if shardLease.calls != 0 {
		t.Fatalf("Renew calls = %d, want 0", shardLease.calls)
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

func TestReleaseShardLeaseWithTimeoutSuccessUsesDeadlineContext(t *testing.T) {
	t.Parallel()

	shardLease := &recordingReleaseLease{}
	c := &Consumer{
		reporter: metrics.Nop{},
		tuning:   tuningConfig{shardLeaseReleaseTimeout: 25 * time.Millisecond},
		logger:   slog.New(slog.DiscardHandler),
	}

	err := c.releaseShardLeaseWithTimeout("shard-1", shardLease)
	if err != nil {
		t.Fatalf("releaseShardLeaseWithTimeout() error = %v, want nil", err)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
	if shardLease.ctx == nil {
		t.Fatal("Release context = nil, want context")
	}
	if _, ok := shardLease.ctx.Deadline(); !ok {
		t.Fatal("Release context has no deadline")
	}
}

func TestReleaseShardLeaseWithTimeoutReturnsDeadlineExceeded(t *testing.T) {
	t.Parallel()

	shardLease := &blockingReleaseLease{}
	c := &Consumer{
		reporter: metrics.Nop{},
		tuning:   tuningConfig{shardLeaseReleaseTimeout: time.Millisecond},
		logger:   slog.New(slog.DiscardHandler),
	}

	err := c.releaseShardLeaseWithTimeout("shard-1", shardLease)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("releaseShardLeaseWithTimeout() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if err == nil || err.Error() != "release shard lease shard-1 timed out: context deadline exceeded" {
		t.Fatalf("releaseShardLeaseWithTimeout() error = %v, want timeout message", err)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Release calls = %d, want 1", shardLease.calls)
	}
}

func newTestLeaseConsumer(heartbeatTTL time.Duration) *Consumer {
	return &Consumer{
		reporter: metrics.Nop{},
		tuning: tuningConfig{
			heartbeatTTL: heartbeatTTL,
		},
	}
}

func newTestLeaseLoopConsumer(heartbeatInterval, heartbeatTTL time.Duration) *Consumer {
	return &Consumer{
		reporter: metrics.Nop{},
		tuning: tuningConfig{
			heartbeatInterval: heartbeatInterval,
			heartbeatTTL:      heartbeatTTL,
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
		logger:       slog.New(slog.DiscardHandler),
		reporter:     metrics.Nop{},
	}
}

func newTestClaimConsumer(manager *recordingClaimManager) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		reporter: metrics.Nop{},
		cfg: Config{
			StreamName: "stream",
		},
		leaseManager: manager,
		leaseOwner:   "owner",
		tuning:       tuning,
	}
}

func runRenewShardLeaseLoop(ctx context.Context, c *Consumer, shardID string, shardLease lease.Lease) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- c.renewShardLeaseLoop(ctx, shardID, shardLease)
	}()
	return done
}

func waitRenewCall(t *testing.T, shardLease *recordingRenewLease) renewCall {
	t.Helper()

	select {
	case call := <-shardLease.ch:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Renew call")
		return renewCall{}
	}
}

func waitRenewLoopDone(t *testing.T, done <-chan error, want error) {
	t.Helper()

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("renewShardLeaseLoop() error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for renewShardLeaseLoop to return")
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

func waitClaimCall(t *testing.T, manager *recordingClaimManager) claimCall {
	t.Helper()

	select {
	case call := <-manager.callCh:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Claim call")
		return claimCall{}
	}
}

func assertAcquireShardOrder(t *testing.T, calls []acquireCall, shardIDs []string) {
	t.Helper()

	if len(calls) != len(shardIDs) {
		t.Fatalf("Acquire calls = %d, want %d", len(calls), len(shardIDs))
	}
	for i, shardID := range shardIDs {
		if calls[i].shardID != shardID {
			t.Fatalf("Acquire call %d shardID = %q, want %q", i, calls[i].shardID, shardID)
		}
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

func assertClaimCall(t *testing.T, call claimCall, ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) {
	t.Helper()

	if ctx != nil && call.ctx != ctx {
		t.Fatalf("Claim context = %v, want %v", call.ctx, ctx)
	}
	if call.streamName != streamName {
		t.Fatalf("Claim streamName = %q, want %q", call.streamName, streamName)
	}
	if call.shardID != shardID {
		t.Fatalf("Claim shardID = %q, want %q", call.shardID, shardID)
	}
	if call.expectedOwner != expectedOwner {
		t.Fatalf("Claim expectedOwner = %q, want %q", call.expectedOwner, expectedOwner)
	}
	if call.newOwner != newOwner {
		t.Fatalf("Claim newOwner = %q, want %q", call.newOwner, newOwner)
	}
	if call.ttl != ttl {
		t.Fatalf("Claim ttl = %v, want %v", call.ttl, ttl)
	}
}
