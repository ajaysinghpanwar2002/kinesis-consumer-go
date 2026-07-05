//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	valkeycheckpoint "github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
)

// TestKeyPrefixIsolationAcrossStreams proves scenario #25 (IT-22): two consumers
// running against two different streams, each configured with a distinct Valkey
// key prefix, share one Valkey instance without bleeding checkpoints, shard
// leases, or worker heartbeats into each other.
//
// LOAD-BEARING NOTE. Because the two consumers read from two DIFFERENT Kinesis
// streams, delivery isolation ("A never sees B's records") is provided by Kinesis,
// not by the library's key prefix — so that check alone would prove nothing about
// the prefix. The prefix is the SOLE isolating variable only in the foreign-prefix
// cross-reads below: reading one stream's keys through the OTHER consumer's prefix.
// The lease/checkpoint key is <prefix>:<stream>:<shard>; reading stream A's keys
// through prefix B (same stream segment, same shard segment, different prefix) MUST
// find nothing, and that emptiness is proof of prefix isolation only after the
// positive side (the key exists under its own prefix) has been established. Hence
// every empty cross-read is guarded by first waiting for the corresponding key to
// appear under its own prefix.
//
// Verified by two library-side mutations, each of which trips exactly one
// isolation assertion while leaving delivery intact (different streams don't
// collide even without a prefix):
//   - drop the prefix in backend.CheckpointKey -> storeB.Get(streamA, shard) finds
//     A's checkpoint -> the checkpoint cross-read fires.
//   - make Manager.List's scan pattern prefix-agnostic -> leaseB.List(streamA)
//     finds A's leases -> the lease cross-read fires.
func TestKeyPrefixIsolationAcrossStreams(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	// Two independent streams on one Kinesis + one Valkey.
	streamA := uniqueName("kcg-it-iso-a")
	streamB := uniqueName("kcg-it-iso-b")
	const shardCount = 2
	createStream(ctx, t, client, streamA, shardCount)
	createStream(ctx, t, client, streamB, shardCount)
	waitStreamActive(ctx, t, client, streamA, 90*time.Second)
	waitStreamActive(ctx, t, client, streamB, 90*time.Second)

	// Distinct checkpoint prefixes. The lease prefix (<prefix>-lease) and worker
	// prefix (<prefix>-worker) are derived from these, so all three key namespaces
	// differ between the two consumers.
	storeA := newStore(t, uniqueName("kcg-it-iso-kp-a"))
	t.Cleanup(func() { _ = storeA.Close() })
	storeB := newStore(t, uniqueName("kcg-it-iso-kp-b"))
	t.Cleanup(func() { _ = storeB.Close() })
	leaseA := newLeaseManager(t, storeA)
	leaseB := newLeaseManager(t, storeB)

	// Disjoint payload sets, seeded across every shard of each stream so every
	// shard produces a checkpoint under its own prefix.
	hashA := shardHashKeys(ctx, t, client, streamA)
	hashB := shardHashKeys(ctx, t, client, streamB)
	payloadsA := makePayloads("iso-a", 30)
	payloadsB := makePayloads("iso-b", 30)
	putRecordsAcrossShards(ctx, t, client, streamA, hashA, payloadsA)
	putRecordsAcrossShards(ctx, t, client, streamB, hashB, payloadsB)

	collA := newCollector()
	collB := newCollector()
	consumerA := newConsumer(t, streamA, client, storeA, collA.handler())
	consumerB := newConsumer(t, streamB, client, storeB, collB.handler())
	_, stopA := runConsumer(t, consumerA)
	defer stopA()
	_, stopB := runConsumer(t, consumerB)
	defer stopB()

	// Each consumer delivers its OWN stream in full.
	if missing := collA.waitFor(payloadsA, 90*time.Second); missing != nil {
		t.Fatalf("consumer A did not deliver all of stream A; missing %d/%d: %v", len(missing), len(payloadsA), missing)
	}
	if missing := collB.waitFor(payloadsB, 90*time.Second); missing != nil {
		t.Fatalf("consumer B did not deliver all of stream B; missing %d/%d: %v", len(missing), len(payloadsB), missing)
	}

	// Kinesis-level sanity (NOT the prefix proof): neither consumer sees the
	// other's payloads, because they read different streams entirely.
	for _, p := range payloadsB {
		if got := collA.count(p); got != 0 {
			t.Fatalf("consumer A saw stream B payload %q %d times; streams must not cross-deliver", p, got)
		}
	}
	for _, p := range payloadsA {
		if got := collB.count(p); got != 0 {
			t.Fatalf("consumer B saw stream A payload %q %d times; streams must not cross-deliver", p, got)
		}
	}

	// ---- LEASE + WORKER isolation (asserted while both consumers still run, so
	// leases and heartbeats are live). ----

	// Guard: each prefix sees its OWN stream's leases (all shards, one owner).
	ownerA := waitForSingleLeaseOwner(t, leaseA, streamA, shardCount, 45*time.Second)
	ownerB := waitForSingleLeaseOwner(t, leaseB, streamB, shardCount, 45*time.Second)

	// LOAD-BEARING: the other prefix cannot see those leases.
	if owners, err := leaseB.List(ctx, streamA); err != nil || len(owners) != 0 {
		t.Fatalf("prefix B leaked stream A leases: List(streamA)=%v err=%v; want empty", owners, err)
	}
	if owners, err := leaseA.List(ctx, streamB); err != nil || len(owners) != 0 {
		t.Fatalf("prefix A leaked stream B leases: List(streamB)=%v err=%v; want empty", owners, err)
	}

	// Guard + LOAD-BEARING for the distinct -worker namespace: each prefix sees
	// its own worker heartbeat, neither sees the other's.
	if workers := waitForLeaseWorkers(t, leaseA, streamA, 1, 30*time.Second); len(workers) == 0 {
		t.Fatalf("prefix A saw no worker for stream A; guard for the worker cross-read is vacuous")
	}
	_ = waitForLeaseWorkers(t, leaseB, streamB, 1, 30*time.Second)
	if workers, err := leaseB.Workers(ctx, streamA); err != nil || len(workers) != 0 {
		t.Fatalf("prefix B leaked stream A workers: Workers(streamA)=%v err=%v; want empty", workers, err)
	}
	if workers, err := leaseA.Workers(ctx, streamB); err != nil || len(workers) != 0 {
		t.Fatalf("prefix A leaked stream B workers: Workers(streamB)=%v err=%v; want empty", workers, err)
	}
	t.Logf("lease isolation held: ownerA=%s ownerB=%s", ownerA, ownerB)

	// ---- CHECKPOINT isolation (per shard). ----
	for _, shard := range listShardIDs(ctx, t, client, streamA) {
		// Guard: A's checkpoint exists under prefix A (poll to avoid racing the
		// per-page save that lands after the handler returns).
		waitForCheckpoint(t, storeA, streamA, shard, 30*time.Second)
		// LOAD-BEARING: prefix B cannot see A's checkpoint for the same shard.
		if v, err := storeB.Get(ctx, streamA, shard); err != nil || v != "" {
			t.Fatalf("prefix B leaked stream A checkpoint for shard %s: Get=%q err=%v; want empty", shard, v, err)
		}
	}
	for _, shard := range listShardIDs(ctx, t, client, streamB) {
		waitForCheckpoint(t, storeB, streamB, shard, 30*time.Second)
		if v, err := storeA.Get(ctx, streamB, shard); err != nil || v != "" {
			t.Fatalf("prefix A leaked stream B checkpoint for shard %s: Get=%q err=%v; want empty", shard, v, err)
		}
	}
}

// waitForCheckpoint blocks until a non-empty checkpoint exists for the shard under
// the store's own prefix, returning it. It fails the test on timeout so a
// subsequent foreign-prefix empty read is proof of isolation rather than proof
// that nothing has been persisted yet.
func waitForCheckpoint(t *testing.T, store *valkeycheckpoint.Store, stream, shard string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		v, err := store.Get(context.Background(), stream, shard)
		if err == nil && v != "" {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint for %s/%s did not appear under its own prefix within %s (last err: %v)", stream, shard, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
