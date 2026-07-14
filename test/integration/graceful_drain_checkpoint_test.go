//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// TestGracefulDrainCheckpointsInFlightProgress proves the graceful-shutdown
// contract (#8): a consumer stopped mid-stream flushes its in-flight progress, so
// a fresh consumer resumes exactly where it left off and does NOT replay the
// records it already processed. This is the complement of IT-4, which showed that
// WITHOUT a drain the replay is bounded by checkpointEvery.
//
// The setup isolates the drain flush as the ONLY possible checkpoint writer:
//   - checkpointEvery is set larger than the whole stream, so the periodic
//     "due" checkpoint (saveShardCheckpointIfDue) never fires.
//   - C1 is stopped mid-stream (backlog remains), so it never catches up to the
//     tip and the catch-up flush never fires either.
//
// With both of those off, a non-empty checkpoint — C2 resuming at C1's frontier
// with zero replay — can only have come from checkpointOnDrain. The companion
// mutation (documented below) reruns the same scenario with an ungraceful stop
// and observes C2 replay the whole prefix, confirming the drain is the cause.
func TestGracefulDrainCheckpointsInFlightProgress(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-drain")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const (
		shardCount = 1
		total      = 300
		batchSize  = 10
		// Larger than the stream, so the periodic due-save path never triggers and
		// the only writer left is the drain flush.
		checkpointEvery = 1_000_000
		// Stop well before the tip so the catch-up flush cannot fire; a slow handler
		// guarantees a large backlog remains at cancel time.
		stopAtLeast   = 30
		perRecordWork = 20 * time.Millisecond
	)

	// Test precondition: checkpointEvery must exceed the whole stream, or the
	// periodic due-save path would fire and the drain flush would no longer be the
	// only possible checkpoint writer.
	if checkpointEvery <= total {
		t.Fatalf("test invariant: checkpointEvery (%d) must exceed total (%d) so the due-save path stays disabled", checkpointEvery, total)
	}

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("expected %d shard hash key, got %d", shardCount, len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	payloads := makePayloads("drain", total)
	ordinalOf := make(map[string]int, total)
	for i, p := range payloads {
		ordinalOf[p] = i
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, payloads)

	// Phase 1: C1 processes slowly, then is cancelled GRACEFULLY mid-stream.
	collC1 := newCollector()
	record := collC1.handler()
	slowHandler := func(hctx context.Context, rec consumer.Record) error {
		if err := record(hctx, rec); err != nil {
			return err
		}
		// Slow the throughput so a large backlog remains when we cancel. The worker
		// context is detached during graceful drain, so this sleep is not cut short
		// by the drain; the in-flight page finishes and is checkpointed.
		select {
		case <-time.After(perRecordWork):
		case <-hctx.Done():
		}
		return nil
	}

	consumerC1 := newDrainConsumer(t, stream, client, store, slowHandler, batchSize, checkpointEvery)
	_, stopC1 := runConsumer(t, consumerC1)

	// Wait until C1 has processed at least a couple of pages, then stop it. Because
	// records are delivered in order on a single shard, seeing ordinals 0..stopAtLeast-1
	// means the frontier is at least stopAtLeast.
	if missing := collC1.waitFor(payloads[:stopAtLeast], 90*time.Second); missing != nil {
		stopC1()
		t.Fatalf("C1 did not reach its stop threshold; missing %d/%d", len(missing), stopAtLeast)
	}

	// Graceful stop: cancel and wait for Start to return. The drain waits for the
	// in-flight page to finish and flushes the processed-so-far checkpoint.
	stopC1()

	c1Ordinals := deliveredOrdinals(collC1, payloads, ordinalOf)
	frontier := len(c1Ordinals)
	if frontier == 0 {
		t.Fatal("C1 delivered nothing")
	}
	// On a single shard, delivery is in order, so C1's delivered set is 0..frontier-1.
	if !equalOrdinals(c1Ordinals, rangeOrdinals(0, frontier)) {
		t.Fatalf("C1 delivered a non-contiguous prefix %v; want exactly 0..%d", summarizeOrdinals(c1Ordinals), frontier-1)
	}
	if frontier >= total {
		t.Fatalf("C1 frontier=%d reached the tip (%d): the stop was not mid-stream, so the drain flush is not isolated from the catch-up flush", frontier, total)
	}
	if frontier < 2*batchSize {
		t.Fatalf("C1 frontier=%d spanned fewer than two pages (batchSize=%d); increase the stop threshold", frontier, batchSize)
	}

	// Phase 2: a fresh C2 resumes from the persisted drain checkpoint.
	collC2 := newCollector()
	consumerC2 := newConsumer(t, stream, client, store, collC2.handler())
	_, stopC2 := runConsumer(t, consumerC2)
	defer stopC2()

	// C2 must deliver everything from the frontier to the tip.
	if missing := collC2.waitFor(payloads[frontier:], 90*time.Second); missing != nil {
		t.Fatalf("resumed C2 did not deliver the remainder; missing %d/%d: %v", len(missing), total-frontier, missing)
	}

	c2Ordinals := deliveredOrdinals(collC2, payloads, ordinalOf)
	replay := 0
	for _, o := range c2Ordinals {
		if o < frontier {
			replay++
		}
	}
	t.Logf("C1 frontier=%d, C2 delivered %d ordinals (first=%d, last=%d), replay=%d (checkpointEvery=%d)",
		frontier, len(c2Ordinals), c2Ordinals[0], c2Ordinals[len(c2Ordinals)-1], replay, checkpointEvery)

	// The drain checkpoint was at ordinal frontier-1, so C2 must resume strictly
	// after it: zero replay. With the due-save path disabled and no catch-up, any
	// replay would mean the drain failed to flush in-flight progress.
	if replay != 0 {
		t.Fatalf("C2 replayed %d records at/below the drain frontier %d; graceful drain did not checkpoint in-flight progress", replay, frontier)
	}
	// C2 delivered exactly the remainder, contiguously to the tip (no loss).
	if !equalOrdinals(c2Ordinals, rangeOrdinals(frontier, total)) {
		t.Fatalf("C2 delivered %v; want exactly %d..%d", summarizeOrdinals(c2Ordinals), frontier, total-1)
	}

	// Completeness: every record delivered at least once across C1 and C2.
	union := make(map[int]bool, total)
	for _, o := range c1Ordinals {
		union[o] = true
	}
	for _, o := range c2Ordinals {
		union[o] = true
	}
	if len(union) != total {
		var missing []int
		for i := 0; i < total; i++ {
			if !union[i] {
				missing = append(missing, i)
			}
		}
		t.Fatalf("records lost across the graceful stop/resume: %d/%d missing: %v", len(missing), total, missing)
	}
}
