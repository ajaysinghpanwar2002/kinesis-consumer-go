//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// TestShedCleanupReleaseErrNotOwnedKeepsDonorLive drives Finding 2's targeted
// path deterministically through a real shard-local shed and proves the donor
// consumer — and its unrelated shard worker — stay live.
//
// Setup:
//   - Consumer A owns both shards. A large heartbeat interval keeps its shard
//     renew loops from firing during the test, so a shed shard's ownership loss
//     can surface only through the cleanup Release, never the renew loop.
//   - A phantom second worker is registered (a bare Heartbeat, no consumer), so
//     A is over its fair share and its short rebalance loop sheds exactly one
//     shard while keeping the other.
//   - A's lease manager is instrumented so that, at the first cleanup Release
//     (the shed), it first performs a real peer Claim of that same lease and
//     then delegates to the real Release. That reproduces the exact race — a
//     peer claimed the lease just before the donor's cleanup release — making
//     the real backend return ErrNotOwned, deterministically.
//
// Pre-fix, that cleanup ErrNotOwned was wrapped as a worker error and cancelled
// the whole run. The consumer must instead absorb it as a shard-local handoff:
// A's Start stays live and its surviving shard keeps delivering records.
func TestShedCleanupReleaseErrNotOwnedKeepsDonorLive(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-shed-cleanup")
	const shardCount = 2
	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-shed-cleanup-kp"))
	t.Cleanup(func() { _ = store.Close() })

	base := newLeaseManager(t, store)
	injecting := &shedClaimInjectingManager{Manager: base, peerOwner: "peer-intruder"}

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}

	collector := newAttributingCollector()
	consumerA := newShedProbeConsumer(t, stream, client, store, injecting, collector.handlerFor("a"))
	errA, stopA := runObservedConsumer(t, consumerA)
	t.Cleanup(stopA)

	ownerA := waitForSingleLeaseOwner(t, injecting, stream, shardCount, 45*time.Second)

	// Register a phantom peer worker so A is over its fair share and will shed
	// one shard. The phantom never runs a consumer; it only tips the fair-share
	// math so the shed fires.
	if err := base.Heartbeat(ctx, integrationCoordinationIdentity(stream), "phantom-worker", 90*time.Second); err != nil {
		t.Fatalf("register phantom worker: %v", err)
	}

	// The shed's cleanup Release must return ErrNotOwned exactly once (only the
	// cleanup path calls Release; the renew-loss path returns before releasing).
	waitForReleaseErrNotOwned(t, injecting, 1, 30*time.Second)

	// The donor's Start must still be running after absorbing the shed handoff.
	assertStartLiveFor(t, errA, time.Second)

	// The surviving shard's worker must keep processing. Find the shard A still
	// owns and put records only there, then confirm A delivers them.
	survivor := survivorShard(t, injecting, stream, ownerA)
	post := makePayloads("shed-cleanup-post", 20)
	putRecordsAcrossShards(ctx, t, client, stream, map[string]string{survivor: hashKeys[survivor]}, post)
	if got := collector.waitForProcessedBy("a", post, 1, 30*time.Second); got < 1 {
		t.Fatalf("donor processed %d records on its surviving shard %s, want at least 1", got, survivor)
	}

	assertStartLiveFor(t, errA, 300*time.Millisecond)
}

// survivorShard returns the single shard still owned by owner after the shed.
func survivorShard(t *testing.T, manager lease.Manager, stream, owner string) string {
	t.Helper()
	owners, err := manager.List(context.Background(), integrationCoordinationIdentity(stream))
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	for shardID, o := range owners {
		if o == owner {
			return shardID
		}
	}
	t.Fatalf("owner %s no longer owns any shard after the shed; owners=%v", owner, owners)
	return ""
}

// waitForReleaseErrNotOwned blocks until the instrumented manager has observed
// at least want cleanup Release calls returning ErrNotOwned.
func waitForReleaseErrNotOwned(t *testing.T, m *shedClaimInjectingManager, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if got := int(m.releaseErrNotOwned.Load()); got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cleanup Release returning ErrNotOwned occurred %d times within %s, want >= %d (shed path not exercised)", m.releaseErrNotOwned.Load(), timeout, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertStartLiveFor fails if the consumer's Start returns within duration.
func assertStartLiveFor(t *testing.T, errCh <-chan error, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case err := <-errCh:
		t.Fatalf("consumer Start returned during live observation: %v", err)
	case <-timer.C:
	}
}

// newShedProbeConsumer builds a consumer that sheds on a short rebalance interval
// but whose large heartbeat interval keeps shard renewals from firing during the
// test, so a shed shard's ownership loss surfaces only via cleanup Release.
func newShedProbeConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	leaseManager lease.Manager,
	handler consumer.HandlerFunc,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithLeaseManager(leaseManager),
		consumer.WithBatching(10, 1),
		consumer.WithPolling(100*time.Millisecond, time.Second),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(200*time.Millisecond, 0, 500*time.Millisecond, 2),
		consumer.WithHeartbeat(30*time.Second, 90*time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create shed-probe consumer for %s: %v", stream, err)
	}
	return cons
}

// shedClaimInjectingManager wraps a lease.Manager. At the first cleanup Release,
// each wrapped lease first performs a real peer Claim of its own lease and then
// delegates to the real Release, so the backend returns ErrNotOwned — the
// deterministic reproduction of a peer claiming the lease during the donor's
// shed cleanup. It counts the ErrNotOwned releases to prove the path executed.
type shedClaimInjectingManager struct {
	lease.Manager
	peerOwner          string
	claimOnce          sync.Once
	releaseErrNotOwned atomic.Int32
}

func (m *shedClaimInjectingManager) wrap(l lease.Lease, coordKey, shardID, owner string, ttl time.Duration) lease.Lease {
	if l == nil {
		return nil
	}
	return &shedClaimInjectingLease{Lease: l, mgr: m, coordKey: coordKey, shardID: shardID, owner: owner, ttl: ttl}
}

func (m *shedClaimInjectingManager) Acquire(ctx context.Context, stream, shardID, owner string, ttl time.Duration) (lease.Lease, bool, error) {
	l, ok, err := m.Manager.Acquire(ctx, stream, shardID, owner, ttl)
	return m.wrap(l, stream, shardID, owner, ttl), ok, err
}

func (m *shedClaimInjectingManager) Claim(ctx context.Context, stream, shardID, expectedOwner, newOwner string, ttl time.Duration) (lease.Lease, bool, error) {
	l, ok, err := m.Manager.Claim(ctx, stream, shardID, expectedOwner, newOwner, ttl)
	return m.wrap(l, stream, shardID, newOwner, ttl), ok, err
}

type shedClaimInjectingLease struct {
	lease.Lease
	mgr      *shedClaimInjectingManager
	coordKey string
	shardID  string
	owner    string
	ttl      time.Duration
}

func (l *shedClaimInjectingLease) Release(ctx context.Context) error {
	// On the first cleanup Release (the shed), simulate a peer that claimed this
	// lease just before the donor released it, so the real owner-checked delete
	// returns ErrNotOwned — the exact Finding 2 race, made deterministic.
	l.mgr.claimOnce.Do(func() {
		_, _, _ = l.mgr.Manager.Claim(ctx, l.coordKey, l.shardID, l.owner, l.mgr.peerOwner, l.ttl)
	})
	err := l.Lease.Release(ctx)
	if errors.Is(err, lease.ErrNotOwned) {
		l.mgr.releaseErrNotOwned.Add(1)
	}
	return err
}
