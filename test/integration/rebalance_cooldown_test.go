//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// TestRebalanceCooldownPreventsImmediateReacquire proves scenario #23 (IT-20):
// a shard that this worker just moved away is not immediately moved back while
// its local rebalance cooldown is active.
//
// The cooldown is intentionally a LOCAL anti-thrash guard, not a distributed
// lease attribute. This test therefore exercises the clean local shed path:
// A owns all 3 shards, B joins, A sheds one shard, and B acquires it. When B is
// then stopped, the shed shard becomes unowned and A is the only live worker, so
// A would be allowed to reacquire it (3 shards / 1 worker -> high=3) if the local
// cooldown were ignored. The assertion keeps that shard unowned across several
// A rebalance ticks, then verifies A reacquires it once the cooldown has expired.
func TestRebalanceCooldownPreventsImmediateReacquire(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-cooldown")
	const shardCount = 3
	rebalanceInterval := 100 * time.Millisecond
	cooldown := 12 * time.Second

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-cooldown-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}
	payloads := makePayloads("cooldown", 30)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, payloads)

	coll := newCollector()

	consumerA := newCooldownProbeConsumer(t, stream, client, store, coll.handler(), rebalanceInterval, 30*time.Second, cooldown)
	_, stopA := runConsumer(t, consumerA)
	defer stopA()

	ownerA := waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	// B's rebalance interval is deliberately long: this keeps B off the live-claim
	// path so the movement under test is A's clean local shed plus B's shard-sync
	// acquisition of the unowned lease.
	consumerB := newCooldownProbeConsumer(t, stream, client, store, coll.handler(), 30*time.Second, time.Second, cooldown)
	_, stopB := runConsumer(t, consumerB)
	defer stopB()

	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)
	owners := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, time.Second, 45*time.Second)

	counts := leaseOwnerCounts(owners)
	if counts[ownerA] != 2 || len(counts) != 2 {
		t.Fatalf("want A to hold 2 shards after shed, got ownerA=%q owners=%v counts=%v", ownerA, owners, counts)
	}
	movedShard := ""
	movedOwner := ""
	for shardID, owner := range owners {
		if owner != ownerA {
			movedShard = shardID
			movedOwner = owner
			break
		}
	}
	if movedShard == "" || movedOwner == "" {
		t.Fatalf("could not identify A's shed shard: ownerA=%q owners=%v", ownerA, owners)
	}
	if counts[movedOwner] != 1 {
		t.Fatalf("want moved owner %q to hold exactly 1 shard, owners=%v counts=%v", movedOwner, owners, counts)
	}
	t.Logf("cooldown setup: ownerA=%s movedShard=%s movedOwner=%s owners=%v", ownerA, movedShard, movedOwner, owners)

	// Stop B so it releases the moved shard. Once B's worker heartbeat expires, A
	// is the only live worker and would be able to acquire the unowned shard if its
	// local cooldown entry were not honored.
	stopB()
	waitForOnlyLeaseWorker(t, leaseManager, stream, ownerA, 15*time.Second)
	waitForShardOwner(t, leaseManager, stream, movedShard, "", 10*time.Second)

	// A's shard-sync interval is long, so only the rebalance path can reacquire the
	// unowned shard during this window. The window is much longer than A's 100ms
	// rebalance interval but shorter than the 12s cooldown, giving multiple
	// rebalance ticks a chance to make the wrong move.
	assertShardOwnerFor(t, leaseManager, stream, movedShard, "", 1500*time.Millisecond)

	// Cooldown is temporary: after it expires, A should pick up the unowned shard
	// and return to owning the full 3-shard stream.
	waitForShardOwner(t, leaseManager, stream, movedShard, ownerA, cooldown+5*time.Second)
	finalOwners := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 1, 500*time.Millisecond, 15*time.Second)
	finalCounts := leaseOwnerCounts(finalOwners)
	if len(finalCounts) != 1 || finalCounts[ownerA] != shardCount {
		t.Fatalf("want A to own all %d shards after cooldown expires, owners=%v counts=%v", shardCount, finalOwners, finalCounts)
	}

	if missing := coll.waitFor(payloads, 60*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(payloads), missing)
	}
}

func newCooldownProbeConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	rebalanceInterval time.Duration,
	shardSyncInterval time.Duration,
	cooldown time.Duration,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, shardSyncInterval),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(rebalanceInterval, 0, cooldown, 1),
		consumer.WithHeartbeat(100*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create cooldown probe consumer for %s: %v", stream, err)
	}
	return cons
}

func waitForOnlyLeaseWorker(t *testing.T, manager lease.Manager, stream, owner string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	var lastErr error
	for {
		workers, err := manager.Workers(context.Background(), stream)
		if err == nil {
			last = workers
			if len(workers) == 1 && workers[0] == owner {
				return
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not report only worker %q within %s; workers=%v lastErr=%v", stream, owner, timeout, last, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForShardOwner(t *testing.T, manager lease.Manager, stream, shardID, owner string, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]string
	var lastErr error
	for {
		owners, err := manager.List(context.Background(), stream)
		if err == nil {
			last = owners
			if owners[shardID] == owner {
				return owners
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s shard %s owner did not become %q within %s; last=%v lastErr=%v", stream, shardID, owner, timeout, last, lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func assertShardOwnerFor(t *testing.T, manager lease.Manager, stream, shardID, owner string, stableFor time.Duration) {
	t.Helper()
	deadline := time.Now().Add(stableFor)
	samples := 0
	for time.Now().Before(deadline) {
		owners, err := manager.List(context.Background(), stream)
		if err != nil {
			t.Fatalf("list owners while checking shard %s stability: %v", shardID, err)
		}
		if got := owners[shardID]; got != owner {
			t.Fatalf("stream %s shard %s owner changed during %s window: got %q want %q owners=%v", stream, shardID, stableFor, got, owner, owners)
		}
		samples++
		time.Sleep(50 * time.Millisecond)
	}
	if samples < 2 {
		t.Fatalf("sampled shard %s owner only %d times during %s", shardID, samples, stableFor)
	}
}
