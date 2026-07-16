package lease

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	consumerlease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
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
		consumer.Config{StreamName: "stream", StartPosition: consumer.StartTrimHorizon},
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

	probe, acquired, err := mgr.Acquire(context.Background(), "stream", "shard-3", "probe-owner", time.Minute)
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

	mgr, server := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	// Another worker already owns shard-1, so the bounded manager's SET NX
	// fails after it has reserved its single slot; that slot must be freed.
	if err := server.Set(backend.LeaseKey("lease-test", "stream", "shard-1"), "owner-x"); err != nil {
		t.Fatalf("preset lease: %v", err)
	}
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire already-owned shard-1 = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after failed acquire ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerClaimFailureFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	// Claim against the wrong expected owner returns res==0 after reserving the
	// single slot; that slot must be freed.
	if err := server.Set(backend.LeaseKey("lease-test", "stream", "shard-1"), "owner-x"); err != nil {
		t.Fatalf("preset lease: %v", err)
	}
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

	lease, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Second)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// The lease TTL lapses in Valkey, but the in-memory slot stays held until a
	// renew observes the loss of ownership and releases it.
	server.FastForward(2 * time.Second)
	if err := lease.Renew(ctx, time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("Renew after expiry = %v, want ErrNotOwned", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after renew-not-owned ok=%v err=%v, want ok true (slot must be freed)", ok, err)
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

	server.FastForward(2 * time.Second)
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

	server.FastForward(2 * time.Second)
	owners, err = mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if _, ok := owners["shard-1"]; ok {
		t.Fatalf("shard-1 still present after TTL expiry")
	}
}

// multiNodeClient wraps a real valkey.Client but reports a fixed set of nodes
// from Nodes(). It lets tests exercise scanKeys' cross-node fan-out without a
// real cluster (miniredis is standalone-only, and valkey-go@v1.0.65 ships no
// mock package). Every node in the map is itself a real client connected to
// its own miniredis, so the per-node SCAN cursor walk and the dedup run
// against real data.
type multiNodeClient struct {
	valkey.Client
	nodes map[string]valkey.Client
}

func (c *multiNodeClient) Nodes() map[string]valkey.Client {
	return c.nodes
}

// newRawClient opens a standalone client to addr matching how NewManager
// builds its client (single connection, no client-side cache).
func newRawClient(t *testing.T, addr string) valkey.Client {
	t.Helper()

	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
	})
	if err != nil {
		t.Fatalf("valkey.NewClient(%q): %v", addr, err)
	}
	t.Cleanup(c.Close)
	return c
}

// newMultiNodeManager builds a Manager whose client reports n distinct nodes,
// each backed by its own miniredis server, and returns the manager alongside
// those servers so tests can seed keys per node.
func newMultiNodeManager(t *testing.T, n int) (*Manager, []*miniredis.Miniredis) {
	t.Helper()

	servers := make([]*miniredis.Miniredis, n)
	nodes := make(map[string]valkey.Client, n)
	var base valkey.Client
	for i := range servers {
		server, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis start: %v", err)
		}
		t.Cleanup(server.Close)
		servers[i] = server

		client := newRawClient(t, server.Addr())
		if base == nil {
			base = client
		}
		nodes[server.Addr()] = client
	}

	mgr := &Manager{
		client:     &multiNodeClient{Client: base, nodes: nodes},
		keyPrefix:  "lease-test",
		workPrefix: "lease-test-worker",
		slots:      backend.NewSlotTracker(0),
	}
	return mgr, servers
}

func TestScanKeysFansOutAcrossNodes(t *testing.T) {
	t.Parallel()

	mgr, servers := newMultiNodeManager(t, 2)
	ctx := context.Background()

	key1 := backend.LeaseKey("lease-test", "stream", "shard-1")
	key2 := backend.LeaseKey("lease-test", "stream", "shard-2")
	if err := servers[0].Set(key1, "owner-a"); err != nil {
		t.Fatalf("seed node 0: %v", err)
	}
	if err := servers[1].Set(key2, "owner-b"); err != nil {
		t.Fatalf("seed node 1: %v", err)
	}

	keys, err := mgr.scanKeys(ctx, "lease-test:stream:*")
	if err != nil {
		t.Fatalf("scanKeys error: %v", err)
	}
	slices.Sort(keys)
	want := []string{key1, key2}
	slices.Sort(want)
	if !slices.Equal(keys, want) {
		t.Fatalf("scanKeys = %v, want the union across both nodes %v", keys, want)
	}
}

func TestScanKeysDeduplicatesAcrossNodes(t *testing.T) {
	t.Parallel()

	mgr, servers := newMultiNodeManager(t, 2)
	ctx := context.Background()

	// shared mimics a key visible on both a primary and its replica; unique
	// lives on only one node.
	shared := backend.LeaseKey("lease-test", "stream", "shard-1")
	unique := backend.LeaseKey("lease-test", "stream", "shard-2")
	if err := servers[0].Set(shared, "owner-a"); err != nil {
		t.Fatalf("seed node 0 shared: %v", err)
	}
	if err := servers[1].Set(shared, "owner-a"); err != nil {
		t.Fatalf("seed node 1 shared: %v", err)
	}
	if err := servers[1].Set(unique, "owner-b"); err != nil {
		t.Fatalf("seed node 1 unique: %v", err)
	}

	keys, err := mgr.scanKeys(ctx, "lease-test:stream:*")
	if err != nil {
		t.Fatalf("scanKeys error: %v", err)
	}
	slices.Sort(keys)
	want := []string{shared, unique}
	slices.Sort(want)
	if !slices.Equal(keys, want) {
		t.Fatalf("scanKeys = %v, want deduplicated union %v", keys, want)
	}
}

func TestWorkersFansOutAcrossNodes(t *testing.T) {
	t.Parallel()

	mgr, servers := newMultiNodeManager(t, 2)
	ctx := context.Background()

	if err := servers[0].Set(backend.WorkerKey("lease-test-worker", "stream", "worker-a"), "worker-a"); err != nil {
		t.Fatalf("seed node 0: %v", err)
	}
	if err := servers[1].Set(backend.WorkerKey("lease-test-worker", "stream", "worker-b"), "worker-b"); err != nil {
		t.Fatalf("seed node 1: %v", err)
	}

	owners, err := mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	slices.Sort(owners)
	want := []string{"worker-a", "worker-b"}
	if !slices.Equal(owners, want) {
		t.Fatalf("workers = %v, want owners from both nodes %v", owners, want)
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
