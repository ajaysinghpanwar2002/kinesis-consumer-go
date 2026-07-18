package lease

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	consumerlease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/alicebob/miniredis/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func newTestManager(t *testing.T, opts ...Option) (*Manager, *miniredis.Miniredis) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)

	opts = append([]Option{WithKeyPrefix("lease-test")}, opts...)
	mgr, err := NewManager(server.Addr(), opts...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return mgr, server
}

func seedIndexedLease(t *testing.T, mgr *Manager, streamName, shardID, owner string, ttl time.Duration) {
	t.Helper()
	keys := mgr.keys(streamName)
	res, err := mgr.client.Do(context.Background(), mgr.client.B().Eval().Script(backend.LeaseAcquireScript).Numkeys(2).
		Key(keys.LeaseOwners, keys.LeaseExpirations).
		Arg(shardID, owner, strconv.FormatInt(ttl.Milliseconds(), 10)).Build()).ToInt64()
	if err != nil || res != 1 {
		t.Fatalf("seed indexed lease = (%d, %v), want (1, nil)", res, err)
	}
}

func TestManagerAcquireClaimReleaseRenew(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t)
	ctx := context.Background()

	lease, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire error=%v ok=%v, want ok true", err, ok)
	}

	// A second worker cannot acquire an already-owned shard.
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-b", time.Minute); err != nil || ok {
		t.Fatalf("Acquire second ok=%v err=%v, want ok false", ok, err)
	}

	owners, err := mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if owners["shard-1"] != "owner-a" {
		t.Fatalf("owner = %q, want owner-a", owners["shard-1"])
	}

	// Claiming with the wrong expected owner fails.
	if _, ok, err := mgr.Claim(ctx, "stream", "shard-1", "owner-b", "owner-c", time.Minute); err != nil || ok {
		t.Fatalf("Claim wrong owner ok=%v err=%v, want ok false", ok, err)
	}

	// Claiming with the correct expected owner transfers ownership.
	claimed, ok, err := mgr.Claim(ctx, "stream", "shard-1", "owner-a", "owner-b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim error=%v ok=%v, want ok true", err, ok)
	}

	owners, err = mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if owners["shard-1"] != "owner-b" {
		t.Fatalf("owner = %q, want owner-b", owners["shard-1"])
	}

	if err := claimed.Renew(ctx, time.Minute); err != nil {
		t.Fatalf("Renew error: %v", err)
	}
	if err := claimed.Release(ctx); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	// Releasing or renewing a lease we no longer own reports ErrNotOwned.
	if err := claimed.Release(ctx); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("second Release error = %v, want ErrNotOwned", err)
	}
	if err := claimed.Renew(ctx, time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("Renew after release error = %v, want ErrNotOwned", err)
	}
	// The original owner was superseded by the claim, so its Release also fails.
	if err := lease.Release(ctx); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("old lease Release error = %v, want ErrNotOwned", err)
	}
}

func TestManagerConsumerGroupCoordinationKeysAreIsolated(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	ctx := context.Background()

	leaseA, acquired, err := mgr.Acquire(ctx, "group-a:orders", "shard-1", "owner-a", time.Minute)
	if err != nil || !acquired || leaseA == nil {
		t.Fatalf("Acquire group A = (%v, %v, %v), want non-nil, true, nil", leaseA, acquired, err)
	}
	leaseB, acquired, err := mgr.Acquire(ctx, "group-b:orders", "shard-1", "owner-b", time.Minute)
	if err != nil || !acquired || leaseB == nil {
		t.Fatalf("Acquire group B = (%v, %v, %v), want non-nil, true, nil", leaseB, acquired, err)
	}
	if peerLease, peerAcquired, err := mgr.Acquire(ctx, "group-a:orders", "shard-1", "owner-a-peer", time.Minute); err != nil || peerAcquired || peerLease != nil {
		t.Fatalf("same-group peer Acquire = (%v, %v, %v), want nil, false, nil", peerLease, peerAcquired, err)
	}

	if err := mgr.Heartbeat(ctx, "group-a:orders", "worker-a", time.Minute); err != nil {
		t.Fatalf("Heartbeat group A: %v", err)
	}
	if err := mgr.Heartbeat(ctx, "group-b:orders", "worker-b", time.Minute); err != nil {
		t.Fatalf("Heartbeat group B: %v", err)
	}

	if owners, err := mgr.List(ctx, "group-a:orders"); err != nil || len(owners) != 1 || owners["shard-1"] != "owner-a" {
		t.Fatalf("List group A = (%v, %v), want shard-1 owner-a", owners, err)
	}
	if owners, err := mgr.List(ctx, "group-b:orders"); err != nil || len(owners) != 1 || owners["shard-1"] != "owner-b" {
		t.Fatalf("List group B = (%v, %v), want shard-1 owner-b", owners, err)
	}
	if workers, err := mgr.Workers(ctx, "group-a:orders"); err != nil || !slices.Equal(workers, []string{"worker-a"}) {
		t.Fatalf("Workers group A = (%v, %v), want [worker-a]", workers, err)
	}
	if workers, err := mgr.Workers(ctx, "group-b:orders"); err != nil || !slices.Equal(workers, []string{"worker-b"}) {
		t.Fatalf("Workers group B = (%v, %v), want [worker-b]", workers, err)
	}

	keysA := backend.LeaseCoordinationKeys("lease-test", "group-a:orders")
	keysB := backend.LeaseCoordinationKeys("lease-test", "group-b:orders")
	if got := server.HGet(keysA.LeaseOwners, "shard-1"); got != "owner-a" {
		t.Fatalf("group A indexed lease owner = %q, want owner-a", got)
	}
	if got := server.HGet(keysB.LeaseOwners, "shard-1"); got != "owner-b" {
		t.Fatalf("group B indexed lease owner = %q, want owner-b", got)
	}
	if _, err := server.ZScore(keysA.Workers, "worker-a"); err != nil {
		t.Fatalf("group A indexed worker missing: %v", err)
	}
	if _, err := server.ZScore(keysB.Workers, "worker-b"); err != nil {
		t.Fatalf("group B indexed worker missing: %v", err)
	}

	if err := leaseA.Release(ctx); err != nil {
		t.Fatalf("Release group A: %v", err)
	}
	if err := leaseB.Release(ctx); err != nil {
		t.Fatalf("Release group B: %v", err)
	}
}

func TestManagerMaxLeasesGate(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	lease1, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// The single slot is taken, so a different shard cannot be acquired.
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire shard-2 = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}

	// Releasing the held lease frees the slot for another shard.
	if err := lease1.Release(ctx); err != nil {
		t.Fatalf("Release shard-1: %v", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after release ok=%v err=%v, want ok true", ok, err)
	}
}

func TestConsumerTransactionalAcquireRollbackFreesMaxLeasesSlot(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	errAcquire := errors.New("injected second acquire failure")
	failingManager := &failNthAcquireManager{
		Manager: mgr,
		failOn:  2,
		err:     errAcquire,
	}
	client := twoShardKinesisClient{}
	c, err := consumer.New(
		consumer.Config{StreamName: "stream", ConsumerGroup: "group", StartPosition: consumer.StartTrimHorizon},
		client,
		checkpoint.NewMemoryStore(),
		func(context.Context, consumer.Record) error { return nil },
		consumer.WithLeaseManager(failingManager),
		consumer.WithRetry(1, time.Millisecond),
		consumer.WithHeartbeat(10*time.Millisecond, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}

	err = c.Start(context.Background())
	if !errors.Is(err, errAcquire) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errAcquire)
	}
	if failingManager.calls != 2 {
		t.Fatalf("Acquire calls = %d, want 2", failingManager.calls)
	}

	probe, acquired, err := mgr.Acquire(context.Background(), "group:stream", "shard-3", "probe-owner", time.Minute)
	if err != nil || !acquired || probe == nil {
		t.Fatalf("Acquire after transactional rollback = (%v, %v, %v), want non-nil, true, nil", probe, acquired, err)
	}
	if err := probe.Release(context.Background()); err != nil {
		t.Fatalf("release probe lease: %v", err)
	}
}

type failNthAcquireManager struct {
	consumerlease.Manager
	failOn int
	calls  int
	err    error
}

func (m *failNthAcquireManager) Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (consumerlease.Lease, bool, error) {
	m.calls++
	if m.calls == m.failOn {
		return nil, false, m.err
	}
	return m.Manager.Acquire(ctx, streamName, shardID, owner, ttl)
}

type twoShardKinesisClient struct{}

func (twoShardKinesisClient) ListShards(context.Context, *kinesis.ListShardsInput, ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return &kinesis.ListShardsOutput{Shards: []types.Shard{
		{ShardId: aws.String("shard-1")},
		{ShardId: aws.String("shard-2")},
	}}, nil
}

func (twoShardKinesisClient) GetShardIterator(context.Context, *kinesis.GetShardIteratorInput, ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{}, nil
}

func (twoShardKinesisClient) GetRecords(context.Context, *kinesis.GetRecordsInput, ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	return &kinesis.GetRecordsOutput{}, nil
}

// The following three tests discriminate the release-frees-slot behavior under
// a bounded tracker (MaxLeases=1): each reserves the single slot, then hits a
// path where the backend op fails (or ownership is lost), and asserts a
// different shard can still be acquired afterward. They fail if the
// corresponding releaseSlot()/once.Do(done) call is removed, unlike the
// unlimited-tracker tests where release is a no-op.

func TestManagerAcquireFailureFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	// Another worker already owns shard-1, so the bounded manager's atomic
	// acquire fails after it has reserved its single slot; that slot must be
	// freed.
	seedIndexedLease(t, mgr, "stream", "shard-1", "owner-x", time.Minute)
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire already-owned shard-1 = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after failed acquire ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerClaimFailureFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	// Claim against the wrong expected owner returns res==0 after reserving the
	// single slot; that slot must be freed.
	seedIndexedLease(t, mgr, "stream", "shard-1", "owner-x", time.Minute)
	if l, ok, err := mgr.Claim(ctx, "stream", "shard-1", "wrong-owner", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Claim wrong owner = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after failed claim ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerRenewNotOwnedFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)

	lease, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Second)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// The lease TTL lapses in Valkey, but the in-memory slot stays held until a
	// renew observes the loss of ownership and releases it.
	server.SetTime(anchor.Add(2 * time.Second))
	if err := lease.Renew(ctx, time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("Renew after expiry = %v, want ErrNotOwned", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after renew-not-owned ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerRedundantSelfAcquireKeepsLiveLeaseSlot(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	held, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// A redundant self-acquire of the already-held shard is refused at the slot
	// tracker and must not disturb the live lease's reservation.
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("redundant self-Acquire = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}

	// Regression for the undercount bug: the failed redundant acquire used to
	// free the slot backing the live lease, letting shard-2 through at
	// MaxLeases capacity.
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire shard-2 at capacity = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}

	// The live lease's release still frees the slot.
	if err := held.Release(ctx); err != nil {
		t.Fatalf("Release shard-1: %v", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after release ok=%v err=%v, want ok true", ok, err)
	}
}

func TestManagerRedundantSelfClaimKeepsLiveLeaseSlot(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	held, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// A redundant self-claim of the already-held shard is refused at the slot
	// tracker before touching the backend, keeping the live lease's slot
	// intact.
	if l, ok, err := mgr.Claim(ctx, "stream", "shard-1", "owner-a", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("redundant self-Claim = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire shard-2 at capacity = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}

	if err := held.Release(ctx); err != nil {
		t.Fatalf("Release shard-1: %v", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after release ok=%v err=%v, want ok true", ok, err)
	}
}

func TestManagerConcurrentSameShardAcquireRespectsMax(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	type result struct {
		lease consumerlease.Lease
		ok    bool
		err   error
	}
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < 2; i++ {
		go func() {
			start.Wait()
			l, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
			results <- result{lease: l, ok: ok, err: err}
		}()
	}
	start.Done()

	var winner consumerlease.Lease
	wins := 0
	for i := 0; i < 2; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("concurrent Acquire error: %v", res.err)
		}
		if res.ok {
			wins++
			winner = res.lease
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent same-shard Acquire wins = %d, want exactly 1", wins)
	}

	// The tracker never exceeded max: the single slot is still occupied by the
	// winner, so another shard is refused.
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire shard-2 at capacity = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}

	// After the winner releases, the shard is acquirable again.
	if err := winner.Release(ctx); err != nil {
		t.Fatalf("Release winner: %v", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("re-Acquire shard-1 after release ok=%v err=%v, want ok true", ok, err)
	}
}

func TestManagerHeartbeatWorkers(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t)
	ctx := context.Background()

	if err := mgr.Heartbeat(ctx, "stream", "worker-a", time.Minute); err != nil {
		t.Fatalf("Heartbeat error: %v", err)
	}
	if err := mgr.Heartbeat(ctx, "stream", "worker-b", time.Minute); err != nil {
		t.Fatalf("Heartbeat error: %v", err)
	}

	owners, err := mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	slices.Sort(owners)
	want := []string{"worker-a", "worker-b"}
	if !slices.Equal(owners, want) {
		t.Fatalf("workers = %v, want %v", owners, want)
	}
}

func TestManagerHeartbeatExpiresWorker(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	ctx := context.Background()
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)

	if err := mgr.Heartbeat(ctx, "stream", "worker-a", time.Second); err != nil {
		t.Fatalf("Heartbeat error: %v", err)
	}

	owners, err := mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	if !slices.Contains(owners, "worker-a") {
		t.Fatalf("workers = %v, want worker-a", owners)
	}

	server.SetTime(anchor.Add(2 * time.Second))
	owners, err = mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	if slices.Contains(owners, "worker-a") {
		t.Fatalf("worker-a still present after TTL expiry")
	}
}

func TestManagerLeaseExpires(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	ctx := context.Background()
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)

	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Second); err != nil || !ok {
		t.Fatalf("Acquire error=%v ok=%v, want ok true", err, ok)
	}

	owners, err := mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if owners["shard-1"] != "owner-a" {
		t.Fatalf("owner = %q, want owner-a", owners["shard-1"])
	}

	server.SetTime(anchor.Add(2 * time.Second))
	owners, err = mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if _, ok := owners["shard-1"]; ok {
		t.Fatalf("shard-1 still present after TTL expiry")
	}
}

func TestNewManagerRejectsEmptyAddr(t *testing.T) {
	t.Parallel()

	if _, err := NewManager(""); err == nil {
		t.Fatal("NewManager(\"\") error = nil, want error")
	}
}

func TestNewManagerPingFailure(t *testing.T) {
	t.Parallel()

	// Reserve a port with miniredis, then close it so the address is
	// unreachable, forcing NewManager's ping to fail promptly.
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	addr := server.Addr()
	server.Close()

	mgr, err := NewManager(addr, WithPingTimeout(200*time.Millisecond))
	if err == nil {
		_ = mgr.Close()
		t.Fatal("NewManager on unreachable addr error = nil, want error")
	}
}

// TestManagerRejectsSubMillisecondTTL pins the born-expired-lease guard: the
// Lua scripts work in whole milliseconds, so a sub-millisecond TTL truncates
// to zero — a lease no peer ever sees as live that would still consume a
// MaxLeases slot. All TTL-taking operations reject it, and a rejected Acquire
// must not leak its slot reservation.
func TestManagerRejectsSubMillisecondTTL(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)
	mgr, err := NewManager(server.Addr(), WithKeyPrefix("lease-test"), WithMaxLeases(1))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	ctx := context.Background()

	if _, _, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", 500*time.Microsecond); err == nil {
		t.Fatal("Acquire(500µs) error = nil, want ttl validation error")
	}
	if _, _, err := mgr.Claim(ctx, "stream", "shard-1", "owner-a", "owner-b", 0); err == nil {
		t.Fatal("Claim(0) error = nil, want ttl validation error")
	}
	if err := mgr.Heartbeat(ctx, "stream", "owner-a", time.Millisecond-1); err == nil {
		t.Fatal("Heartbeat(<1ms) error = nil, want ttl validation error")
	}

	// The rejected Acquire must not have consumed the single MaxLeases slot.
	acquired, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire after rejected ttl = (ok=%t, err=%v), want (true, nil)", ok, err)
	}

	// Renew validates too, and the failed validation does not touch the lease.
	if err := acquired.Renew(ctx, 500*time.Microsecond); err == nil {
		t.Fatal("Renew(500µs) error = nil, want ttl validation error")
	}
	owners, err := mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if owners["shard-1"] != "owner-a" {
		t.Fatalf("owners = %v, want shard-1 owned by owner-a (lease untouched by rejected renew)", owners)
	}
	if err := acquired.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
}
