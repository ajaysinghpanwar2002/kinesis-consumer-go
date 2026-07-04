//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestRebalanceMoreConsumersThanShards proves scenario #21 (IT-18): when there
// are MORE consumers than shards, the rebalancer (a) caps ownership at the fair
// share so at most `shardCount` consumers own a shard (each holds exactly one),
// (b) keeps the surplus consumers idle but still HEARTBEATING so they remain
// visible in Workers(), (c) reaches a STABLE ownership split with no thrash, and
// (d) still delivers every record at least once.
//
// Mechanism (observed in source; the exact path is NOT guaranteed to execute on
// every run, but every path converges to the same 1+1 end state the assertions
// check). Initial acquisition (acquireShardLeases) grabs every unowned ready
// shard via SET NX, ignoring fair share, so TYPICALLY the first consumer to start
// grabs BOTH shards and the others start with zero. With 2 shards and 4 workers
// the fair share is low=0, high=ceil(2/4)=1. From a 2-shard owner the split then
// happens via the SHED path: the owner sheds its excess (excess=2-high=1) by
// releasing one lease (selectLocalRebalanceShedShards -> workers.stop), and an
// idle consumer acquires that now-unowned shard. Other benign races reach the
// same end state without the shed (two consumers each SET NX a different shard at
// startup -> 1+1 immediately; or a consumer bootstrapping while only 3 workers
// are visible sees high=ceil(2/3)=1 and can claim from a 2-shard owner). Once the
// split is 1+1, pickRebalanceDonor skips any owner with count<=high, so there is
// no eligible donor and ownership is stable (no thrash). Every consumer heartbeats
// via workerHeartbeatLoop, started before shard acquisition (consumer.go:43-47),
// so the two ownership-less consumers still register in Workers().
//
// Delivery is asserted AT LEAST once, not exactly once: the split happens via a
// mid-flight shed (workers.stop cancels a running worker), and the library only
// guarantees at-least-once across a rebalance handoff — a record in the handler
// at shed time can be redelivered from the last checkpoint. This is the same
// reason the two-consumer TestMultiConsumerRebalancesLeases asserts at-least-once.
func TestRebalanceMoreConsumersThanShards(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-morecons")
	const shardCount = 2
	const consumerCount = 4

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-morecons-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	// Produce across BOTH shards up front so every owner has real work.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}
	payloads := makePayloads("morecons", 40)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, payloads)

	// Start all 4 consumers on the shared store, each with the same collector.
	coll := newCollector()
	for i := 0; i < consumerCount; i++ {
		cons := newRebalancingConsumer(t, stream, client, store, coll.handler(), 200*time.Millisecond)
		_, stop := runConsumer(t, cons)
		defer stop()
	}

	// All 4 consumers must register as active workers via their heartbeat, even
	// though only 2 can own a shard.
	waitForLeaseWorkers(t, leaseManager, stream, consumerCount, 45*time.Second)

	// Wait for a STABLE 2-shard split across at least 2 owners. Convergence here is
	// the real proof of the fair-share cap: with 2 shards, reaching 2 distinct
	// owners means each holds exactly one. If a single consumer greedily kept both,
	// this would time out. 4 LocalStack consumers can be slow to settle, so the
	// budget is generous.
	owners := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, 3*time.Second, 90*time.Second)

	// (a) Fair-share cap: exactly 2 distinct owners, each with 1 shard; no owner
	// holds both. The load-bearing proof of the cap is that waitForStableLeaseOwners
	// above CONVERGED (a greedy single owner of both shards would make it time out);
	// this is a defensive restatement of that post-condition.
	counts := leaseOwnerCounts(owners)
	if len(counts) != 2 {
		t.Fatalf("want exactly 2 distinct shard owners, got %d: owners=%v counts=%v", len(counts), owners, counts)
	}
	for owner, n := range counts {
		if n != 1 {
			t.Fatalf("owner %q holds %d shards, want exactly 1 (fair share high=1); owners=%v", owner, n, owners)
		}
	}

	// (b) LOAD-BEARING idle-heartbeat: all 4 consumers appear in Workers() even
	// though only 2 own a shard, proving the surplus consumers keep heartbeating
	// independent of ownership.
	workers, err := leaseManager.Workers(ctx, stream)
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	if len(workers) != consumerCount {
		t.Fatalf("want %d active workers (all consumers heartbeating), got %d: %v", consumerCount, len(workers), workers)
	}

	// (c) Stability: re-read after a further settle and confirm the owner map is
	// unchanged. waitForStableLeaseOwners already required stability for a window;
	// this extra check guards against a late thrash move after that window.
	time.Sleep(2 * time.Second)
	after, err := leaseManager.List(ctx, stream)
	if err != nil {
		t.Fatalf("re-list owners for stability check: %v", err)
	}
	if !sameOwnerMap(owners, after) {
		t.Fatalf("ownership thrashed after stabilizing: before=%v after=%v", owners, after)
	}

	// (d) Delivery: every record delivered at least once. Exactly-once is not
	// asserted because the fair-share split redistributes a shard via a mid-flight
	// shed, across which the library guarantees only at-least-once.
	if missing := coll.waitFor(payloads, 90*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(payloads), missing)
	}

	t.Logf("more-consumers-than-shards ok: owners=%v workers=%d", owners, len(workers))
}
