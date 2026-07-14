//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// TestGracefulDrainHandsOffToLiveSuccessor proves the drain-on-departure handoff
// contract (#4): when a consumer processing a shard is gracefully stopped, it
// flushes its in-flight progress and releases the lease, and an ALREADY-RUNNING
// peer takes the shard over and resumes EXACTLY at the frontier — with zero
// replay of the records the departing owner already processed.
//
// This is the multi-consumer, live-successor form of IT-5. IT-5 proved a single
// consumer's graceful drain checkpoints in-flight progress, verified by a FRESH
// sequential consumer started AFTER the first stopped. Here the successor B is a
// CONCURRENT consumer that is already running while A owns the shard, and it
// takes over via lease RELEASE→ACQUIRE (A drains → deletes the lease key → B's
// shardSync tick acquires the now-unowned shard via SET NX). That release-driven
// path is the new coverage, and it also lets the test assert a split-brain
// property: B provably cannot process the shard until A releases it.
//
// NOTE on the scenario title's "rebalance + drain interaction": a rebalance SHED
// does NOT drain (isDraining() is a consumer-wide flag set only on full-consumer
// shutdown; a shed/claim-loss just cancels the worker), so the handoff here is
// driven by A's graceful DEPARTURE, not by the fair-share planner. The
// planner-driven shed-with-slow-handler case is a separate scenario.
//
// The setup isolates the drain flush as the ONLY possible checkpoint writer, the
// same way IT-5 does:
//   - checkpointEvery is set larger than the whole stream, so the periodic "due"
//     checkpoint (saveShardCheckpointIfDue) never fires.
//   - A is stopped mid-stream (backlog remains), so it never catches up to the tip
//     and the catch-up flush never fires either.
//
// With both off, B resuming at A's frontier with zero replay can only have come
// from checkpointOnDrain. The companion mutation (documented below) swaps A to an
// ungraceful stop and observes B replay the whole prefix, confirming the drain is
// the cause.
func TestGracefulDrainHandsOffToLiveSuccessor(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-handoff")
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

	payloads := makePayloads("handoff", total)
	ordinalOf := make(map[string]int, total)
	for i, p := range payloads {
		ordinalOf[p] = i
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, payloads)

	// Consumer A: processes slowly with graceful drain enabled. The worker context
	// is detached during drain, so a slow in-flight page finishes and is
	// checkpointed rather than cut short by the cancel.
	collA := newCollector()
	recordA := collA.handler()
	slowHandler := func(hctx context.Context, rec consumer.Record) error {
		if err := recordA(hctx, rec); err != nil {
			return err
		}
		select {
		case <-time.After(perRecordWork):
		case <-hctx.Done():
		}
		return nil
	}
	consumerA := newDrainConsumer(t, stream, client, store, slowHandler, batchSize, checkpointEvery)
	_, stopA := runConsumer(t, consumerA)

	// Establish A as the shard owner BEFORE starting B. With a single shard both
	// consumers would otherwise race for the lease at startup, and if B (a fast
	// handler) won it would drain the stream while A processed nothing. Waiting for
	// A to deliver its first page proves A holds and is renewing the lease; from
	// then on B cannot SET NX it (and, being neither a donor nor holding an unowned
	// shard, issues no claim), so B idles until A releases on drain.
	const establishCount = batchSize
	if missing := collA.waitFor(payloads[:establishCount], 90*time.Second); missing != nil {
		stopA()
		t.Fatalf("A did not establish ownership; missing %d/%d", len(missing), establishCount)
	}

	// Consumer B: a live peer, running before A stops, that will take over via
	// lease RELEASE→ACQUIRE once A drains.
	collB := newCollector()
	consumerB := newConsumer(t, stream, client, store, collB.handler())
	_, stopB := runConsumer(t, consumerB)
	defer stopB()

	// Let A process further while B is live (B repeatedly tries and fails to acquire
	// the held lease). Single-shard delivery is in order, so seeing ordinals
	// 0..stopAtLeast-1 means A's frontier is >= stopAtLeast.
	if missing := collA.waitFor(payloads[:stopAtLeast], 90*time.Second); missing != nil {
		stopA()
		t.Fatalf("A did not reach its stop threshold; missing %d/%d", len(missing), stopAtLeast)
	}

	// Split-brain check: A still holds the lease (we have not cancelled it yet), so
	// B must not have processed anything on this shard. This proves the handoff is
	// release-driven with no concurrent processing, not merely eventually-consistent.
	if b := deliveredOrdinals(collB, payloads, ordinalOf); len(b) != 0 {
		stopA()
		t.Fatalf("B delivered %v while A still held the lease; the shard was not exclusively owned", summarizeOrdinals(b))
	}

	// Graceful stop of A: cancel and wait for Start to return. A drains the
	// in-flight page, flushes the processed-so-far checkpoint, and releases the lease.
	stopA()

	aOrdinals := deliveredOrdinals(collA, payloads, ordinalOf)
	frontier := len(aOrdinals)
	if frontier == 0 {
		t.Fatal("A delivered nothing")
	}
	// On a single shard, delivery is in order, so A's delivered set is 0..frontier-1.
	if !equalOrdinals(aOrdinals, rangeOrdinals(0, frontier)) {
		t.Fatalf("A delivered a non-contiguous prefix %v; want exactly 0..%d", summarizeOrdinals(aOrdinals), frontier-1)
	}
	if frontier >= total {
		t.Fatalf("A frontier=%d reached the tip (%d): the stop was not mid-stream, so the drain flush is not isolated from the catch-up flush", frontier, total)
	}
	if frontier < 2*batchSize {
		t.Fatalf("A frontier=%d spanned fewer than two pages (batchSize=%d); increase the stop threshold", frontier, batchSize)
	}

	// B (already running) acquires the released shard and resumes from the drain
	// checkpoint. It must deliver everything from the frontier to the tip.
	if missing := collB.waitFor(payloads[frontier:], 90*time.Second); missing != nil {
		t.Fatalf("live successor B did not deliver the remainder; missing %d/%d: %v", len(missing), total-frontier, missing)
	}

	bOrdinals := deliveredOrdinals(collB, payloads, ordinalOf)
	replay := 0
	for _, o := range bOrdinals {
		if o < frontier {
			replay++
		}
	}
	t.Logf("A frontier=%d, B delivered %d ordinals (first=%d, last=%d), replay=%d (checkpointEvery=%d)",
		frontier, len(bOrdinals), bOrdinals[0], bOrdinals[len(bOrdinals)-1], replay, checkpointEvery)

	// The drain checkpoint was at ordinal frontier-1, so B must resume strictly
	// after it: zero replay. With the due-save path disabled and no catch-up, any
	// replay would mean the drain failed to flush in-flight progress.
	if replay != 0 {
		t.Fatalf("B replayed %d records at/below the drain frontier %d; graceful drain did not hand off in-flight progress", replay, frontier)
	}
	// B delivered exactly the remainder, contiguously to the tip (no loss).
	if !equalOrdinals(bOrdinals, rangeOrdinals(frontier, total)) {
		t.Fatalf("B delivered %v; want exactly %d..%d", summarizeOrdinals(bOrdinals), frontier, total-1)
	}

	// Completeness: every record delivered at least once across A and B.
	union := make(map[int]bool, total)
	for _, o := range aOrdinals {
		union[o] = true
	}
	for _, o := range bOrdinals {
		union[o] = true
	}
	if len(union) != total {
		var missing []int
		for i := 0; i < total; i++ {
			if !union[i] {
				missing = append(missing, i)
			}
		}
		t.Fatalf("records lost across the graceful handoff: %d/%d missing: %v", len(missing), total, missing)
	}
}
