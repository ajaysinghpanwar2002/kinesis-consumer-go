//go:build integration

package integration

import (
	"context"
	"sort"
	"testing"
	"time"
)

// TestRebalanceUnevenShardCount proves scenario #22 (IT-19): with an uneven
// shard-to-consumer ratio (5 shards, 2 consumers) the rebalancer converges to a
// STABLE FAIR distribution — each owner holds between low and high shards — and
// stays there (no thrash), while delivering every record at least once.
//
// Fair share (confirmed against rebalance_snapshot.go): activeWorkers=2, open=5
// -> low=5/2=2, high=ceil(5/2)=3. The only distribution with both owners in [2,3]
// summing to 5 is {2,3}.
//
// Why the bound assertion is genuinely load-bearing here (unlike the trivial 1/1
// of the 2-shard IT-18): a 1+4 or 5+0 split would still satisfy
// waitForStableLeaseOwners(shardCount=5, minOwners=2) — that wait only requires
// all shards owned across >=2 owners, NOT fairness. So the explicit
// "every owner count in [low,high]" check below is what actually proves the
// fair-share cap, not the wait.
//
// STAGGERED START (deterministic): consumer A is started first and allowed to grab
// ALL 5 shards (waitForSingleLeaseOwner). Only then is B started. This proves the
// rebalancer ACTIVELY redistributes from a known 5/0 start (rather than leaving it
// ambiguous whether a concurrent startup merely happened to land fair), and it
// makes the mutation deterministic.
//
// Mechanism from the 5/0 start (verified against source AND empirically during the
// IT-19 mutation exploration). Redistribution here happens via the SHED path: the
// 5-shard owner has excess=5-high=2 and selectLocalRebalanceShedShards releases up
// to maxMoves=2 shards per tick (workers.stop cleanly stops its own worker and
// releases the lease, harness newRebalancingConsumer uses
// WithRebalance(interval,0,500ms,2)); the freshly-unowned shards (500ms cooldown)
// are then acquired by the other consumer. A CLAIM path also exists in code
// (buildLocalRebalancePlan first loop when projectedCount<low, low=2>0 here), but
// claiming a lease from a STILL-LIVE worker makes the donor's renewShardLeaseLoop
// fail with "lease not owned by caller" and crash that consumer — so the clean,
// stable redistribution observed in practice is the shed+acquire path, not claim.
// (This was confirmed by a mutation that disabled shed: forcing the claim path
// destabilized the cluster instead of yielding a clean split.)
//
// The result converges to {3,2}. Once at 3/2, pickRebalanceDonor skips both owners
// (each count<=high=3) so there is no donor, and neither is below low, so ownership
// is stable.
//
// Delivery is asserted AT LEAST once, not exactly once: redistribution moves shards
// mid-flight (shed workers.stop / claim), across which the library guarantees only
// at-least-once — the same reason TestMultiConsumerRebalancesLeases and the 2-shard
// IT-18 assert at-least-once.
func TestRebalanceUnevenShardCount(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-uneven")
	const shardCount = 5
	const consumerCount = 2
	// The EXPECTED fair bounds, computed here as the test's own ground truth and
	// deliberately independent of the library's snapshot values (rebalance_snapshot.go).
	// This independence is what makes a library miscomputation detectable: if the
	// library's low/high drifted, the produced split would fall outside these fixed
	// bounds and assertion (a) would fire.
	const low = 2  // shardCount / consumerCount
	const high = 3 // ceil(shardCount / consumerCount)

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-uneven-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	// Produce across ALL shards up front so A has real work while it owns them all.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}
	initialPayloads := makePayloads("uneven-initial", 50)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, initialPayloads)

	coll := newCollector()

	// A first: let it grab all 5 shards before B joins.
	consumerA := newRebalancingConsumer(t, stream, client, store, coll.handler(), 200*time.Millisecond)
	_, stopA := runConsumer(t, consumerA)
	defer stopA()
	waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	// B second: now the rebalancer must actively move shards from A to B.
	consumerB := newRebalancingConsumer(t, stream, client, store, coll.handler(), 200*time.Millisecond)
	_, stopB := runConsumer(t, consumerB)
	defer stopB()
	waitForLeaseWorkers(t, leaseManager, stream, consumerCount, 45*time.Second)

	// Wait for a STABLE distribution: all 5 shards owned across at least 2 owners,
	// unchanged for the stability window. Moving 2 shards off A plus the 500ms shed
	// cooldown means a few rebalance ticks, so the budget is generous.
	owners := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, 3*time.Second, 90*time.Second)

	// (a) LOAD-BEARING fair bound: exactly 2 distinct owners, every owner's count in
	// [low, high]. With 5 shards this forces {2,3}. A 1+4 or 5+0 split would pass the
	// wait above but fail here — this is the real proof of the fair-share cap.
	counts := leaseOwnerCounts(owners)
	if len(counts) != consumerCount {
		t.Fatalf("want exactly %d distinct shard owners, got %d: owners=%v counts=%v", consumerCount, len(counts), owners, counts)
	}
	got := make([]int, 0, len(counts))
	total := 0
	for owner, n := range counts {
		if n < low || n > high {
			t.Fatalf("owner %q holds %d shards, want within fair bound [%d,%d]; owners=%v", owner, n, low, high, owners)
		}
		got = append(got, n)
		total += n
	}
	if total != shardCount {
		t.Fatalf("owner counts sum to %d, want %d (all shards owned); owners=%v", total, shardCount, owners)
	}
	sort.Ints(got)
	t.Logf("uneven-shard fair split ok: counts=%v owners=%v", got, owners)

	// Produce a handoff batch AFTER the split so records flow through the FINAL
	// ownership (including B's newly-acquired shards), not just A's pre-split run.
	handoffPayloads := makePayloads("uneven-handoff", 50)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, handoffPayloads)

	// (b) Stability: re-read after a further settle and confirm the owner map is
	// unchanged (no late thrash after the stability window).
	time.Sleep(2 * time.Second)
	after, err := leaseManager.List(ctx, integrationCoordinationIdentity(stream))
	if err != nil {
		t.Fatalf("re-list owners for stability check: %v", err)
	}
	if !sameOwnerMap(owners, after) {
		t.Fatalf("ownership thrashed after stabilizing: before=%v after=%v", owners, after)
	}

	// (c) Delivery: every record delivered at least once. Exactly-once is not
	// asserted because redistribution moves shards mid-flight, across which the
	// library guarantees only at-least-once.
	allPayloads := append(append([]string(nil), initialPayloads...), handoffPayloads...)
	if missing := coll.waitFor(allPayloads, 90*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(allPayloads), missing)
	}
}
