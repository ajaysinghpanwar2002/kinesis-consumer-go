//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestSingleConsumerEndToEnd exercises the full path against real infrastructure:
// produce records, consume them all with a single consumer (at-least-once),
// confirm checkpoints are persisted, then restart and confirm the new consumer
// resumes from the checkpoint (sees freshly produced records without replaying
// the whole stream).
func TestSingleConsumerEndToEnd(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-single")
	keyPrefix := uniqueName("kcg-it-ckpt")

	createStream(ctx, t, client, stream, 2)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	// Phase A: produce and consume everything.
	payloadsA := makePayloads("a", 20)
	putRecords(ctx, t, client, stream, payloadsA)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

	collA := newCollector()
	consA := newConsumer(t, stream, client, store, collA.handler())
	_, stopA := runConsumer(t, consA)

	if missing := collA.waitFor(payloadsA, 90*time.Second); missing != nil {
		stopA()
		t.Fatalf("consumer did not deliver all records; missing %d/%d: %v", len(missing), len(payloadsA), missing)
	}
	// Graceful drain on stop checkpoints in-flight progress before returning.
	stopA()

	// Checkpoints must be persisted for at least one shard.
	shards := listShardIDs(ctx, t, client, stream)
	checkpointed := 0
	for _, id := range shards {
		v, err := store.Get(ctx, integrationCoordinationIdentity(stream), id)
		if err != nil {
			t.Fatalf("read checkpoint for shard %s: %v", id, err)
		}
		if v != "" {
			checkpointed++
		}
	}
	if checkpointed == 0 {
		t.Fatalf("expected at least one shard checkpoint to be persisted across %d shards", len(shards))
	}

	// Phase B: produce more and confirm a fresh consumer resumes from checkpoints.
	payloadsB := makePayloads("b", 10)
	putRecords(ctx, t, client, stream, payloadsB)

	collB := newCollector()
	consB := newConsumer(t, stream, client, store, collB.handler())
	_, stopB := runConsumer(t, consB)
	defer stopB()

	if missing := collB.waitFor(payloadsB, 90*time.Second); missing != nil {
		t.Fatalf("resumed consumer did not deliver new records; missing %d/%d: %v", len(missing), len(payloadsB), missing)
	}

	// Resume means the restarted consumer did not replay the entire stream from
	// TRIM_HORIZON. A little boundary reprocessing is allowed (at-least-once), but
	// it must not have re-seen every Phase A record.
	replayedA := 0
	for _, p := range payloadsA {
		if collB.count(p) > 0 {
			replayedA++
		}
	}
	if replayedA == len(payloadsA) {
		t.Fatalf("resumed consumer replayed all %d Phase A records; checkpoints were not honored", len(payloadsA))
	}
}
