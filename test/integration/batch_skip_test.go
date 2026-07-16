//go:build integration

package integration

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// errBatchSkipBoom is the permanent BATCH-handler error that causes a whole page
// to be skipped (dropped) under the skip failure policy.
var errBatchSkipBoom = errors.New("integration: permanent batch handler failure for skip")

// TestBatchSkipDropsWholePage proves scenario #19 (IT-11c): the SKIP failure
// policy in BATCH mode has PAGE granularity. Because a batch handler processes a
// whole GetRecords page at once, one poison record fails the entire page — and
// under WithFailurePolicy(FailurePolicySkip) the whole page is SILENTLY DROPPED
// (no DLQ, no delivery of ANY record in the page, poison or not) and the page is
// checkpointed past. The non-poison COLLATERAL in that page is permanently lost:
// C1 never delivers it and a resuming C2 never replays it.
//
// This is the data-loss-shaped counterpart to IT-11b (batch DLQ, which at least
// captures the collateral) and the sharp contrast to IT-9a (record-mode skip,
// which drops ONLY the single poison record and delivers the rest of the page).
// The load-bearing content is the whole-page silent drop + checkpoint-past, i.e.
// durable loss of the non-poison collateral.
//
// WithShardConcurrency is intentionally NOT set: the batch path returns before the
// concurrency branch (handle_records_page.go:17 vs :27), so it is a no-op here.
func TestBatchSkipDropsWholePage(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("bskip")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}

	const (
		batchSize     = 10
		total1        = 30 // 3 pages of 10 → the poison lands in the middle page
		poisonIdx     = 15 // page [10..19]
		maxAttempts   = 2  // the poison page exhausts these, then the page is skipped
		total2        = 20
		deliverBudget = 60 * time.Second
	)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != 1 {
		t.Fatalf("want exactly 1 shard hash key, got %d", len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	batch1 := makePayloads("bskip-b1", total1)
	poison := batch1[poisonIdx]
	ordinalOf := make(map[string]int, total1)
	for i, p := range batch1 {
		ordinalOf[p] = i
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	store := newStore(t, uniqueName("bskip-kp"))

	// C1: batch mode with SKIP. The batch handler fails the whole page if it
	// contains the poison (so that page is skipped/dropped), otherwise records every
	// record of the page as delivered.
	successColl := newBatchCollector()
	recordPage := successColl.handler()
	var poisonPageCalls atomic.Int64
	batchHandler := func(hctx context.Context, records []consumer.Record) error {
		for _, rec := range records {
			if string(rec.Data) == poison {
				poisonPageCalls.Add(1)
				return errBatchSkipBoom
			}
		}
		return recordPage(hctx, records)
	}
	consC1, err := consumer.New(consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}, client, store, nil,
		consumer.WithBatchHandler(batchHandler),
		consumer.WithFailurePolicy(consumer.FailurePolicySkip),
		consumer.WithRetry(maxAttempts, 10*time.Millisecond),
		consumer.WithBatching(batchSize, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create batch skip consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consC1)

	// Continuation: the page AFTER the poison page (ordinals 20..29) is delivered,
	// proving the consumer skipped past the failed page rather than halting.
	afterPage := batch1[20:30]
	if missing := successColl.waitFor(afterPage, deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("C1 did not deliver the page after the poison page (no skip-continuation): missing %d: %v", len(missing), missing)
	}
	// The page BEFORE the poison page (ordinals 0..9) is delivered too.
	if missing := successColl.waitFor(batch1[0:10], deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("C1 did not deliver the page before the poison page: missing %d: %v", len(missing), missing)
	}
	stopC1()

	// The skip path was actually exercised (the page really failed, was retried, and
	// then skipped) — not merely "some records happened not to arrive".
	if calls := poisonPageCalls.Load(); calls < 1 {
		t.Fatalf("poison page handler was invoked %d times; want >= 1 (the skip path must have run)", calls)
	}
	t.Logf("poison page failed %d time(s) before being skipped (maxAttempts=%d)", poisonPageCalls.Load(), maxAttempts)

	// LOAD-BEARING (batch-distinct): derive the DROPPED page as the batch1 records
	// C1 never delivered, and prove it is exactly one whole page — the poison plus
	// its non-poison collateral, all silently dropped. Record-mode skip (IT-9a)
	// would drop only the single poison record; this drops all len(page).
	//
	// Absence == dropped is sound here (not merely "delivered late") because: the
	// single shard in batch mode is processed strictly in page order (the batch path
	// returns before any concurrency), and the waitFor(afterPage) above is a
	// sequencing barrier — once the page AFTER the poison page has been delivered,
	// the poison page has already been processed and its skip decision made. So a
	// batch1 record still absent from successColl at this point is absent because it
	// was dropped, not because it is still in flight.
	var dropped []int
	for _, p := range batch1 {
		if successColl.count(p) == 0 {
			dropped = append(dropped, ordinalOf[p])
		}
	}
	sort.Ints(dropped)
	if len(dropped) != batchSize {
		t.Fatalf("C1 dropped %d records (%v); want exactly one whole page of %d (batch skip drops the entire page)", len(dropped), dropped, batchSize)
	}
	minOrd := dropped[0]
	for i, k := range dropped {
		if k != minOrd+i {
			t.Fatalf("dropped records are not one contiguous page: %v", dropped)
		}
	}
	if poisonIdx < minOrd || poisonIdx >= minOrd+batchSize {
		t.Fatalf("dropped page %d..%d does not contain the poison (ordinal %d)", minOrd, minOrd+batchSize-1, poisonIdx)
	}
	droppedPage := batch1[minOrd : minOrd+batchSize]
	t.Logf("batch skip dropped whole page ordinals %d..%d (poison at %d); %d records delivered around it", minOrd, minOrd+batchSize-1, poisonIdx, total1-batchSize)

	// The pages around the dropped page were delivered exactly once each.
	delivered := append(append([]string(nil), batch1[0:10]...), batch1[20:30]...)
	for _, p := range delivered {
		if n := successColl.count(p); n != 1 {
			t.Fatalf("non-dropped record %q delivered %d times; want exactly 1", p, n)
		}
	}

	// Checkpoint-past + resume: skip returned nil, so the dropped page was
	// checkpointed. Produce batch2 and start a fresh plain batch-mode C2; it must
	// deliver batch2 and replay NONE of batch1 — including the dropped page. Because
	// C2's handler never errors, if the page had NOT been checkpointed C2 would
	// re-read and deliver the whole dropped page → replay jumps.
	batch2 := makePayloads("bskip-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	collC2 := newBatchCollector()
	consC2 := newBatchConsumer(t, stream, client, store, collC2.handler(), batchSize)
	_, stopC2 := runConsumer(t, consC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, deliverBudget); len(missing) != 0 {
		t.Fatalf("resumed C2 did not deliver batch2: missing %d/%d: %v", len(missing), total2, missing)
	}
	if replay := collC2.replayCount(batch1); replay != 0 {
		t.Fatalf("C2 replayed %d/%d batch1 records; the skipped page must have been checkpointed past", replay, total1)
	}

	// DURABLE DATA LOSS (the point of this slice): the dropped page — including its
	// non-poison collateral — was delivered by NOBODY. C1 skipped it and C2 resumed
	// past it, so with the skip policy in batch mode those records are permanently
	// lost with no DLQ record.
	for _, p := range droppedPage {
		if n := successColl.count(p) + collC2.count(p); n != 0 {
			t.Fatalf("dropped-page record %q (ordinal %d) was delivered %d times; under batch skip the whole page is silently lost", p, ordinalOf[p], n)
		}
	}

	// Completeness of the SURVIVORS: every record outside the dropped page (the
	// around-page batch1 records + all of batch2) was delivered at least once.
	for _, p := range delivered {
		if successColl.count(p)+collC2.count(p) == 0 {
			t.Fatalf("surviving record %q was never delivered", p)
		}
	}
	for _, p := range batch2 {
		if collC2.count(p) == 0 {
			t.Fatalf("batch2 record %q was never delivered", p)
		}
	}
	t.Logf("batch skip ok: whole poison page (%d records) silently dropped + checkpointed past, %d survivors delivered, C2 replay 0", batchSize, total1-batchSize+total2)
}
