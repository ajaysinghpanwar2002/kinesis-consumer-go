//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestMultiConsumerRebalancesLeases exercises real lease redistribution across
// two consumers sharing the same Kinesis stream and Valkey backend. Delivery is
// asserted at least once because ownership can move while records are in flight.
func TestMultiConsumerRebalancesLeases(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-rebalance")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const shardCount = 4

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	initialPayloads := makePayloads("rebalance-initial", 40)
	putRecords(ctx, t, client, stream, initialPayloads)

	coll := newCollector()
	handler := coll.handler()
	consumerA := newRebalancingConsumer(t, stream, client, store, handler, 200*time.Millisecond)
	_, stopA := runConsumer(t, consumerA)
	defer stopA()

	firstOwner := waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	consumerB := newRebalancingConsumer(t, stream, client, store, handler, 200*time.Millisecond)
	_, stopB := runConsumer(t, consumerB)
	defer stopB()
	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)

	handoffPayloads := makePayloads("rebalance-handoff", 60)
	putRecords(ctx, t, client, stream, handoffPayloads)

	owners := waitForDistributedLeaseOwners(t, leaseManager, stream, shardCount, 2, 90*time.Second)
	counts := leaseOwnerCounts(owners)
	if counts[firstOwner] == shardCount {
		t.Fatalf("expected owner %q to shed at least one shard; owners=%v", firstOwner, owners)
	}

	allPayloads := append(append([]string(nil), initialPayloads...), handoffPayloads...)
	if missing := coll.waitFor(allPayloads, 90*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(allPayloads), missing)
	}
}
