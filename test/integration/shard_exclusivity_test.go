//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"
)

// TestShardExclusivitySteadyState proves that once shard leases settle to a
// fair-share equilibrium (4 shards across 2 consumers = 2 each, at which point
// the rebalance planner emits no further moves), each shard is processed by
// exactly one consumer. This is the robust, non-flaky form of "a shard is not
// processed by two consumers": exclusivity is enforced by the lease in steady
// state, because a consumer only runs a worker for a shard whose lease it holds.
//
// It deliberately does NOT assert anything about the bounded handoff window
// during a rebalance move — the processing loop does not re-check ownership, so
// a consumer that just lost a lease keeps processing until its next renewal
// fails, and at-least-once delivery permits that transient overlap.
func TestShardExclusivitySteadyState(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-exclusive")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const shardCount = 4

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("expected %d shard hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}
	shardIDs := make([]string, 0, len(hashKeys))
	for id := range hashKeys {
		shardIDs = append(shardIDs, id)
	}
	sort.Strings(shardIDs)

	coll := newAttributingCollector()

	consumerA := newRebalancingConsumer(t, stream, client, store, coll.handlerFor("A"), 200*time.Millisecond)
	_, stopA := runConsumer(t, consumerA)
	defer stopA()
	waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	consumerB := newRebalancingConsumer(t, stream, client, store, coll.handlerFor("B"), 200*time.Millisecond)
	_, stopB := runConsumer(t, consumerB)
	defer stopB()
	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)

	// At 2/2 equilibrium the planner emits no moves, so ownership is stable and
	// this normally succeeds on the first attempt. The retry only guards a rare
	// mid-window rebalance bounce; each attempt uses a fresh, unique batch.
	const maxAttempts = 4
	var lastIssue string
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		before := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, 2*time.Second, 60*time.Second)

		// One distinct payload per shard, each pinned to its shard via ExplicitHashKey.
		batch := make(map[string]string, shardCount) // shardID -> payload
		for i, id := range shardIDs {
			payload := fmt.Sprintf("exclusive-attempt%d-shard%d", attempt, i)
			batch[id] = payload
			putRecordsToShard(ctx, t, client, stream, hashKeys[id], []string{payload})
		}

		allPayloads := make([]string, 0, len(batch))
		for _, p := range batch {
			allPayloads = append(allPayloads, p)
		}
		if missing := coll.waitForAll(allPayloads, 45*time.Second); missing != nil {
			lastIssue = fmt.Sprintf("measurement batch not fully delivered; missing %v", missing)
			continue
		}

		// Ownership must not have moved during the produce/deliver window, or the
		// steady-state claim does not apply to this batch.
		after, err := leaseManager.List(ctx, integrationCoordinationIdentity(stream))
		if err != nil || !sameOwnerMap(before, after) {
			lastIssue = fmt.Sprintf("ownership changed during measurement window (before=%v after=%v err=%v)", before, after, err)
			continue
		}

		// Steady-state exclusivity: each shard's measurement payload was processed
		// by exactly one distinct consumer. Duplicates by the same consumer are
		// allowed (at-least-once); two distinct consumers on one shard is a defect.
		processors := make(map[string]bool)
		for id, payload := range batch {
			consumers := coll.consumersFor(payload)
			for _, cid := range consumers {
				processors[cid] = true
			}
			switch {
			case len(consumers) == 0:
				t.Errorf("shard %s payload %q was not attributed to any consumer", id, payload)
			case len(consumers) > 1:
				t.Errorf("shard %s processed by multiple consumers %v in steady state (payload %q)", id, consumers, payload)
			}
		}

		// Both consumers must have processed at least one shard, or the exclusivity
		// check above would pass trivially with one idle consumer (split-brain on
		// the peer). At 2/2 equilibrium each consumer owns two shards, so the batch
		// must be split across both.
		if len(processors) < 2 {
			all := make([]string, 0, len(processors))
			for cid := range processors {
				all = append(all, cid)
			}
			sort.Strings(all)
			t.Errorf("expected both consumers to process shards in steady state; only %v processed the batch", all)
		}
		return
	}
	t.Fatalf("could not obtain a stable steady-state measurement in %d attempts: %s", maxAttempts, lastIssue)
}
