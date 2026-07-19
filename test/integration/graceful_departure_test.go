//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// newGracefulDepartureConsumer builds a rebalancing consumer with a deliberately
// LONG heartbeat TTL (short interval so renew/rebalance stay responsive). The
// long TTL is what makes the deregistration effect observable: without
// clean-shutdown deregistration a departed worker's liveness entry would linger
// for the full TTL, so any assertion that resolves far sooner can only pass
// because the worker actively removed its entry.
func newGracefulDepartureConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(500*time.Millisecond, 0, 500*time.Millisecond, 2),
		// TTL 60s (>= 3x interval) far exceeds the assertion windows below, so a
		// lingering ghost worker would fail those assertions instead of expiring
		// in time to mask a missing deregistration.
		consumer.WithHeartbeat(time.Second, 60*time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create graceful-departure consumer for %s: %v", stream, err)
	}
	return cons
}

// TestGracefulWorkerDepartureFreesShardsImmediately proves finding 4: when a
// worker shuts down cleanly, it deregisters its own liveness entry so surviving
// peers recompute fair share from its absence immediately, instead of counting
// the departed worker until its heartbeat TTL expires and leaving the released
// shards idle for roughly one TTL.
//
// Setup: 4 shards, 2 consumers on a shared store. They converge to a stable
// split across both owners (e.g. 2+2). One consumer is then gracefully stopped
// (ctx cancel + wait for Start to return), which drains it, releases its leases,
// and — as the heartbeat loop stops — deregisters its worker entry.
//
// The two load-bearing assertions both resolve in windows far below the 60s
// heartbeat TTL, so neither could pass by TTL expiry:
//   - the live-worker set drops to exactly the one survivor, and
//   - that survivor acquires ALL 4 shards. Without deregistration the survivor's
//     fair-share high would stay ceil(4/2)=2 (the ghost still counted), so it
//     would refuse to acquire the released shards until the ghost expired ~60s
//     later.
func TestGracefulWorkerDepartureFreesShardsImmediately(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-graceful-depart")
	const shardCount = 4

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-graceful-depart-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	// Produce across every shard so all four owners have real work.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}
	payloads := makePayloads("graceful-depart", 80)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, payloads)

	coll := newCollector()

	// Consumer A stays for the whole test (the survivor).
	consA := newGracefulDepartureConsumer(t, stream, client, store, coll.handler())
	_, stopA := runConsumer(t, consA)
	defer stopA()

	// Consumer B departs mid-test.
	consB := newGracefulDepartureConsumer(t, stream, client, store, coll.handler())
	_, stopB := runConsumer(t, consB)
	defer stopB() // once-guarded: the explicit stopB() below makes this a no-op

	// Both consumers register and ownership settles across both of them.
	waitForLeaseWorkers(t, leaseManager, stream, 2, 45*time.Second)
	before := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, 3*time.Second, 90*time.Second)
	if counts := leaseOwnerCounts(before); len(counts) != 2 {
		t.Fatalf("want 4 shards split across exactly 2 owners before departure, got counts=%v owners=%v", counts, before)
	}

	// Gracefully depart consumer B. stopB blocks until B.Start returns, which is
	// after B has drained, released its leases, and deregistered its worker
	// entry — so the effects below are already committed when it returns.
	stopB()

	// (a) The departed worker leaves the live set within a window far below the
	// 60s TTL. A lingering ghost (no deregistration) would keep this at 2 until
	// ~60s and time out here.
	survivors := waitForWorkerCount(t, leaseManager, stream, 1, 15*time.Second)

	// (b) The survivor acquires ALL 4 shards within a window far below the TTL.
	// This is the practical payoff: fair-share recomputation is not blocked by a
	// stale worker, so the cleanly-released shards are picked up immediately
	// rather than sitting idle for ~one TTL.
	finalOwner := waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 25*time.Second)
	if survivors[0] != finalOwner {
		t.Fatalf("sole live worker %q is not the sole shard owner %q", survivors[0], finalOwner)
	}

	// (c) Every record is delivered at least once (exactly-once is not asserted:
	// the departure redistributes shards across a handoff, which is at-least-once).
	if missing := coll.waitFor(payloads, 90*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(payloads), missing)
	}

	t.Logf("graceful departure ok: survivor=%q owns all %d shards", finalOwner, shardCount)
}
