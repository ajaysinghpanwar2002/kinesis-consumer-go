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
	result   lease.Lease
	acquired bool
	err      error
}

func (m *recordingAcquireManager) Acquire(_ context.Context, streamName, shardID, owner string, ttl time.Duration) (lease.Lease, bool, error) {
	m.call = acquireCall{
		streamName: streamName,
		shardID:    shardID,
		owner:      owner,
		ttl:        ttl,
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
