//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestCheckpointResumeAfterRebalance proves that checkpoints written ACROSS an
// ownership transfer are honored by a fresh consumer. Two consumers settle to a
// 2/2 split, so a post-split batch is checkpointed by whichever owner holds each
// shard (A on A's shards, B on B's). A fresh consumer C then resumes and must NOT
// replay that post-split batch — which it could only do if it read the sequence
// checkpoints written by BOTH owners (AFTER_SEQUENCE_NUMBER resume across the
// transfer).
//
// The load-bearing assertion is on batch2 (the post-split batch): if B's
// post-rebalance checkpointing were dropped, C would resume B's shards from A's
// end-of-batch1 point and replay ~half of batch2. A naive "did not replay the
// whole stream" bar would miss that; bounding batch2 replay to <= shardCount
// catches it, while healthy runs replay ~0 (checkpointEvery=1 → resume strictly
// after the last processed record).
func TestCheckpointResumeAfterRebalance(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-resume")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const shardCount = 4

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("expected %d shard hash keys, got %d", shardCount, len(hashKeys))
	}

	setup := newCollector()

	// Phase 1: consumer A alone owns all shards and processes batch1.
	batch1 := makePayloads("resume-b1", 40)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, batch1)

	consumerA := newRebalancingConsumer(t, stream, client, store, setup.handler(), 200*time.Millisecond)
	_, stopA := runConsumer(t, consumerA)
	waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)
	if missing := setup.waitFor(batch1, 60*time.Second); missing != nil {
		stopA()
		t.Fatalf("consumer A did not process batch1; missing %d/%d", len(missing), len(batch1))
	}

	// Phase 2: consumer B joins; ownership settles to 2/2, so batch2 is
	// checkpointed by both owners across the transfer.
	consumerB := newRebalancingConsumer(t, stream, client, store, setup.handler(), 200*time.Millisecond)
	_, stopB := runConsumer(t, consumerB)
	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)
	waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, 2*time.Second, 60*time.Second)

	batch2 := makePayloads("resume-b2", 40)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, batch2)
	if missing := setup.waitFor(batch2, 60*time.Second); missing != nil {
		stopA()
		stopB()
		t.Fatalf("A+B did not process batch2; missing %d/%d", len(missing), len(batch2))
	}

	// Stop both; graceful drain flushes in-flight checkpoints.
	stopA()
	stopB()

	// Starvation sanity only (A alone satisfies this — NOT evidence of the
	// transfer): every shard has some persisted resume point.
	for _, id := range listShardIDs(ctx, t, client, stream) {
		v, err := store.Get(ctx, stream, id)
		if err != nil {
			t.Fatalf("read checkpoint for shard %s: %v", id, err)
		}
		if v == "" {
			t.Fatalf("shard %s has no persisted checkpoint after A+B processed the stream", id)
		}
	}

	// Phase 3: a fresh consumer C resumes from the persisted checkpoints.
	batch3 := makePayloads("resume-b3", 20)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, batch3)

	collC := newCollector()
	consumerC := newConsumer(t, stream, client, store, collC.handler())
	_, stopC := runConsumer(t, consumerC)
	defer stopC()

	if missing := collC.waitFor(batch3, 90*time.Second); missing != nil {
		t.Fatalf("resumed consumer C did not deliver new records; missing %d/%d: %v", len(missing), len(batch3), missing)
	}

	replayedBatch1 := replayCount(collC, batch1)
	replayedBatch2 := replayCount(collC, batch2)
	t.Logf("consumer C replay after resume: batch1=%d/%d batch2=%d/%d", replayedBatch1, len(batch1), replayedBatch2, len(batch2))

	// Transfer-specific bar: batch2 was checkpointed by whichever owner held each
	// shard after the split. A dropped post-rebalance checkpoint on B's shards
	// would replay ~len(batch2)/2 records; a healthy resume replays ~0.
	if replayedBatch2 > shardCount {
		t.Fatalf("consumer C replayed %d/%d post-split records (bound %d); checkpoints across the ownership transfer were not honored", replayedBatch2, len(batch2), shardCount)
	}
	// Old-stream sanity: C did not replay the entire pre-rebalance batch.
	if replayedBatch1 == len(batch1) {
		t.Fatalf("consumer C replayed all %d pre-rebalance records; checkpoints were not honored", len(batch1))
	}
}

// replayCount returns how many of the given payloads consumer C observed at least
// once (i.e. re-read after resuming).
func replayCount(c *collector, payloads []string) int {
	n := 0
	for _, p := range payloads {
		if c.count(p) > 0 {
			n++
		}
	}
	return n
}
