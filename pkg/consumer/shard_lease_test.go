package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
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
	errs  []error // per-call error queue (nil entry = success); overrides err while non-empty
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
	if len(l.errs) > 0 {
		err := l.errs[0]
		l.errs = l.errs[1:]
		return err
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

func TestRenewShardLeaseBoundsCallWithHeartbeatIntervalDeadline(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	shardLease := &recordingRenewLease{}
	c := newTestLeaseLoopConsumer(50*time.Millisecond, 200*time.Millisecond)

	before := time.Now()
	err := c.renewShardLease(ctx, "shard-1", shardLease)
	if err != nil {
		t.Fatalf("renewShardLease() error = %v, want nil", err)
	}
	deadline, ok := shardLease.ctx.Deadline()
	if !ok {
		t.Fatal("Renew context has no deadline, want heartbeat-interval bound")
	}
	if until := deadline.Sub(before); until < 50*time.Millisecond || until > 150*time.Millisecond {
		t.Fatalf("Renew deadline = %v from call start, want ~%v (the heartbeat interval)", until, 50*time.Millisecond)
	}
	if got := shardLease.ctx.Value(contextKey{}); got != "value" {
		t.Fatalf("Renew context value = %v, want %q (must derive from the caller context)", got, "value")
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

	err := c.renewShardLeaseLoop(ctx, "shard-1", shardLease, newLeaseRenewTracker())
	if err != nil {
		t.Fatalf("renewShardLeaseLoop() error = %v, want nil", err)
	}
	if shardLease.calls != 0 {
		t.Fatalf("Renew calls = %d, want 0", shardLease.calls)
	}
}

func TestRenewShardLeaseLoopRenewsOnTick(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	shardLease := &recordingRenewLease{ch: make(chan renewCall, 1)}
	c := newTestLeaseLoopConsumer(time.Millisecond, 45*time.Millisecond)

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
	done := runRenewShardLeaseLoop(ctx, c, "shard-1", shardLease)

	call := waitRenewCall(t, shardLease)
	cancel()
	waitRenewLoopDone(t, done, nil)

	// The per-call context derives from the loop context (values propagate)
	// and carries the heartbeat-interval deadline.
	if got := call.ctx.Value(contextKey{}); got != "value" {
		t.Fatalf("Renew context value = %v, want %q", got, "value")
	}
	if _, ok := call.ctx.Deadline(); !ok {
		t.Fatal("Renew context has no deadline, want heartbeat-interval bound")
	}
	if call.ttl != 45*time.Millisecond {
		t.Fatalf("Renew ttl = %v, want %v", call.ttl, 45*time.Millisecond)
	}
}

func TestRenewShardLeaseLoopStopsAfterTTLBudgetExhausted(t *testing.T) {
	t.Parallel()

	// Persistent transient failures: the loop retries on each tick while the
	// lease's TTL budget lasts, then stops — the backend lease has lapsed and a
	// peer may own the shard.
	errBoom := errors.New("boom")
	shardLease := &recordingRenewLease{err: errBoom}
	c := newTestLeaseLoopConsumer(time.Millisecond, 30*time.Millisecond)

	start := time.Now()
	err := c.renewShardLeaseLoop(context.Background(), "shard-1", shardLease, newLeaseRenewTracker())
	if !errors.Is(err, errBoom) {
		t.Fatalf("renewShardLeaseLoop() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || !strings.Contains(err.Error(), "not renewed within ttl") {
		t.Fatalf("renewShardLeaseLoop() error = %v, want ttl-exhaustion message", err)
	}
	if shardLease.calls < 2 {
		t.Fatalf("Renew calls = %d, want >= 2 (transient failures retried)", shardLease.calls)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("loop stopped after %v, want >= the 30ms ttl budget", elapsed)
	}
}

func TestRenewShardLeaseLoopRetriesTransientFailureAndRecovers(t *testing.T) {
	t.Parallel()

	// One dropped renew must not stop the worker: the failure is retried on the
	// next tick and the loop keeps running once renewal succeeds again.
	errBoom := errors.New("boom")
	shardLease := &recordingRenewLease{
		errs: []error{errBoom, nil, nil, nil},
		ch:   make(chan renewCall, 4),
	}
	logHandler := newCapturingHandler()
	reporter := &recordingReporter{}
	c := newTestLeaseLoopConsumer(time.Millisecond, time.Hour)
	c.logger = slog.New(logHandler)
	c.reporter = reporter

	ctx, cancel := context.WithCancel(context.Background())
	done := runRenewShardLeaseLoop(ctx, c, "shard-1", shardLease)

	// The failed call, then at least one successful retry.
	waitRenewCall(t, shardLease)
	waitRenewCall(t, shardLease)
	cancel()
	waitRenewLoopDone(t, done, nil)

	if shardLease.calls < 2 {
		t.Fatalf("Renew calls = %d, want >= 2 (failure retried)", shardLease.calls)
	}
	// Both counters fire across the failure-then-recovery sequence.
	if failures := reporter.countersNamed(metricLeaseRenewalFailures); len(failures) != 1 {
		t.Fatalf("lease_renewal_failures calls = %d, want 1 (the single transient failure)", len(failures))
	}
	if renewals := reporter.countersNamed(metricLeaseRenewals); len(renewals) < 1 {
		t.Fatalf("lease_renewals calls = %d, want >= 1 (recovered)", len(renewals))
	}
	var warns []capturedRecord
	for _, rec := range logHandler.snapshot() {
		if rec.message == "shard lease renew failed; will retry" {
			warns = append(warns, rec)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("renew retry warn logs = %d, want 1", len(warns))
	}
	if warns[0].level != slog.LevelWarn {
		t.Fatalf("renew retry log level = %v, want %v", warns[0].level, slog.LevelWarn)
	}
	if warns[0].attrs["shard"] != "shard-1" {
		t.Fatalf("renew retry log shard = %q, want shard-1", warns[0].attrs["shard"])
	}
	if warns[0].attrs["since_last_renew"] == "" {
		t.Fatal("renew retry log since_last_renew attribute missing")
	}
	if warns[0].attrs["ttl"] != time.Hour.String() {
		t.Fatalf("renew retry log ttl = %q, want %q", warns[0].attrs["ttl"], time.Hour.String())
	}
	if warns[0].attrs["error"] == "" {
		t.Fatal("renew retry log error attribute missing")
	}
}

func TestRenewShardLeaseLoopStopsPromptlyOnErrNotOwned(t *testing.T) {
	t.Parallel()

	shardLease := &recordingRenewLease{err: lease.ErrNotOwned}
	// A long TTL proves the loop does NOT burn the ttl budget on ErrNotOwned —
	// it stops on the first tick.
	c := newTestLeaseLoopConsumer(time.Millisecond, time.Hour)

	err := c.renewShardLeaseLoop(context.Background(), "shard-1", shardLease, newLeaseRenewTracker())
	if !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("renewShardLeaseLoop() error = %v, want wraps %v", err, lease.ErrNotOwned)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Renew calls = %d, want 1 (no retry once the lease is lost)", shardLease.calls)
	}
}

func TestRenewShardLeaseLoopReturnsContextDeadlineExceeded(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	shardLease := &recordingRenewLease{}
	c := newTestLeaseLoopConsumer(time.Hour, 30*time.Millisecond)

	err := c.renewShardLeaseLoop(ctx, "shard-1", shardLease, newLeaseRenewTracker())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("renewShardLeaseLoop() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if shardLease.calls != 0 {
		t.Fatalf("Renew calls = %d, want 0", shardLease.calls)
	}
}

// hangingRenewLease ignores its context entirely: Renew blocks until unblock
// is closed. It models a backend call stuck in a network black hole.
type hangingRenewLease struct {
	renewCalls   atomic.Int32
	releaseCalls atomic.Int32
	unblock      chan struct{}
}

func (l *hangingRenewLease) Renew(context.Context, time.Duration) error {
	l.renewCalls.Add(1)
	<-l.unblock
	return nil
}

func (l *hangingRenewLease) Release(context.Context) error {
	l.releaseCalls.Add(1)
	return nil
}

// ctxWaitingRenewLease blocks until its (per-call) context is done and
// returns its error — a hung but context-respecting backend.
type ctxWaitingRenewLease struct {
	calls atomic.Int32
}

func (l *ctxWaitingRenewLease) Renew(ctx context.Context, _ time.Duration) error {
	l.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func (l *ctxWaitingRenewLease) Release(context.Context) error {
	return nil
}

func TestRenewShardLeaseLoopWithWatchdogFencesRenewHungIgnoringContext(t *testing.T) {
	t.Parallel()

	shardLease := &hangingRenewLease{unblock: make(chan struct{})}
	t.Cleanup(func() { close(shardLease.unblock) })
	logHandler := newCapturingHandler()
	c := newTestLeaseLoopConsumer(10*time.Millisecond, 40*time.Millisecond)
	c.logger = slog.New(logHandler)

	start := time.Now()
	err := c.renewShardLeaseLoopWithWatchdog(context.Background(), "shard-1", shardLease)
	elapsed := time.Since(start)

	if err == nil || !strings.Contains(err.Error(), "validity expired") {
		t.Fatalf("renewShardLeaseLoopWithWatchdog() error = %v, want lease-validity expiry", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("watchdog fenced after %v, want >= the 40ms ttl", elapsed)
	}
	if calls := shardLease.renewCalls.Load(); calls != 1 {
		t.Fatalf("Renew calls = %d, want 1 (the single hung attempt)", calls)
	}
	var warns []capturedRecord
	for _, rec := range logHandler.snapshot() {
		if rec.message == "shard lease validity expired; stopping worker" {
			warns = append(warns, rec)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("validity-expired warn logs = %d, want 1", len(warns))
	}
	if warns[0].level != slog.LevelWarn {
		t.Fatalf("validity-expired log level = %v, want %v", warns[0].level, slog.LevelWarn)
	}
	if warns[0].attrs["shard"] != "shard-1" {
		t.Fatalf("validity-expired log shard = %q, want shard-1", warns[0].attrs["shard"])
	}
	if warns[0].attrs["since_last_renew"] == "" {
		t.Fatal("validity-expired log since_last_renew attribute missing")
	}
	if warns[0].attrs["ttl"] != (40 * time.Millisecond).String() {
		t.Fatalf("validity-expired log ttl = %q, want %q", warns[0].attrs["ttl"], (40 * time.Millisecond).String())
	}
}

func TestRenewShardLeaseLoopWithWatchdogBoundsCtxRespectingHangToTTLBudget(t *testing.T) {
	t.Parallel()

	// Each hung-but-context-respecting attempt fails at its per-call deadline
	// (one heartbeat interval), follows the transient-retry path, and the
	// loop's own TTL-budget check surfaces the causal error — the watchdog
	// backstop stays out of it.
	shardLease := &ctxWaitingRenewLease{}
	reporter := &recordingReporter{}
	c := newTestLeaseLoopConsumer(20*time.Millisecond, 70*time.Millisecond)
	c.reporter = reporter

	err := c.renewShardLeaseLoopWithWatchdog(context.Background(), "shard-1", shardLease)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("renewShardLeaseLoopWithWatchdog() error = %v, want wraps %v", err, context.DeadlineExceeded)
	}
	if err == nil || !strings.Contains(err.Error(), "not renewed within ttl") {
		t.Fatalf("renewShardLeaseLoopWithWatchdog() error = %v, want ttl-exhaustion wrapping the deadline failure", err)
	}
	if failures := reporter.countersNamed(metricLeaseRenewalFailures); len(failures) < 2 {
		t.Fatalf("lease_renewal_failures calls = %d, want >= 2 (bounded attempts retried)", len(failures))
	}
	if renewals := reporter.countersNamed(metricLeaseRenewals); len(renewals) != 0 {
		t.Fatalf("lease_renewals calls = %d, want 0", len(renewals))
	}
}

func TestRenewShardLeaseLoopWithWatchdogQuietWhileRenewalsSucceed(t *testing.T) {
	t.Parallel()

	shardLease := &recordingRenewLease{ch: make(chan renewCall, 1)}
	logHandler := newCapturingHandler()
	c := newTestLeaseLoopConsumer(time.Millisecond, 20*time.Millisecond)
	c.logger = slog.New(logHandler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.renewShardLeaseLoopWithWatchdog(ctx, "shard-1", shardLease) }()

	// Several full TTL windows of successful renews must keep the watchdog
	// re-arming instead of firing.
	time.Sleep(3 * 20 * time.Millisecond)
	cancel()
	waitRenewLoopDone(t, done, nil)

	for _, rec := range logHandler.snapshot() {
		if rec.message == "shard lease validity expired; stopping worker" {
			t.Fatal("watchdog fired while renewals were succeeding")
		}
	}
}

func TestRenewShardLeaseLoopWithWatchdogPassesThroughErrNotOwned(t *testing.T) {
	t.Parallel()

	shardLease := &recordingRenewLease{err: lease.ErrNotOwned}
	c := newTestLeaseLoopConsumer(time.Millisecond, time.Hour)

	err := c.renewShardLeaseLoopWithWatchdog(context.Background(), "shard-1", shardLease)
	if !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("renewShardLeaseLoopWithWatchdog() error = %v, want wraps %v", err, lease.ErrNotOwned)
	}
	if shardLease.calls != 1 {
		t.Fatalf("Renew calls = %d, want 1 (no retry once the lease is lost)", shardLease.calls)
	}
}

func TestRenewShardLeaseLoopWithWatchdogReturnsNilOnCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shardLease := &recordingRenewLease{}
	c := newTestLeaseLoopConsumer(time.Millisecond, time.Hour)

	if err := c.renewShardLeaseLoopWithWatchdog(ctx, "shard-1", shardLease); err != nil {
		t.Fatalf("renewShardLeaseLoopWithWatchdog() error = %v, want nil", err)
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
		logger:   slog.New(slog.DiscardHandler),
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
		done <- c.renewShardLeaseLoop(ctx, shardID, shardLease, newLeaseRenewTracker())
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
