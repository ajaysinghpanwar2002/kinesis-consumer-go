//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestConsumerBProcessesAfterJoining proves that a second consumer, once it
// joins and acquires shard leases, performs real record processing itself — not
// merely that "some consumer" delivered each payload. Each processed record is
// attributed to a specific consumer via per-consumer handlers, so the test can
// assert that consumer B processed at least one record produced after it joined.
func TestConsumerBProcessesAfterJoining(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-attribution")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const shardCount = 4

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	coll := newAttributingCollector()

	// Consumer A starts alone and should acquire all shards.
	consumerA := newRebalancingConsumer(t, stream, client, store, coll.handlerFor("A"), 200*time.Millisecond)
	_, stopA := runConsumer(t, consumerA)
	defer stopA()
	waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	// Initial batch, produced while only A is running.
	initialPayloads := makePayloads("attr-initial", 40)
	putRecords(ctx, t, client, stream, initialPayloads)

	// Consumer B joins; ownership should distribute across two live workers.
	consumerB := newRebalancingConsumer(t, stream, client, store, coll.handlerFor("B"), 200*time.Millisecond)
	_, stopB := runConsumer(t, consumerB)
	defer stopB()
	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)
	waitForDistributedLeaseOwners(t, leaseManager, stream, shardCount, 2, 90*time.Second)

	// Handoff batch, produced after B has joined and taken shards. B owns at
	// least one shard, and these payloads are spread across all shards via
	// distinct partition keys, so B must process at least one of them itself.
	handoffPayloads := makePayloads("attr-handoff", 60)
	putRecords(ctx, t, client, stream, handoffPayloads)

	if got := coll.waitForProcessedBy("B", handoffPayloads, 1, 90*time.Second); got < 1 {
		t.Fatalf("consumer B did not process any handoff records after joining; payloads processed by B: %d", got)
	}

	// End-to-end sanity: every payload is delivered at least once across the two
	// consumers (at-least-once; no exact-count assumptions).
	allPayloads := append(append([]string(nil), initialPayloads...), handoffPayloads...)
	if missing := coll.waitForAll(allPayloads, 90*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(allPayloads), missing)
	}
}
