//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// errBatchDLQBoom is the permanent BATCH-handler error that sends a whole page to
// the DLQ.
var errBatchDLQBoom = errors.New("integration: permanent batch handler failure for dlq")

// TestBatchDLQFansOutWholePage proves scenario #19 (IT-11b): the DLQ failure
// policy in BATCH mode. Because a batch handler processes a whole GetRecords page
// at once, there is NO per-record isolation: one poison record fails the ENTIRE
// page, so under WithFailurePolicy(SendToDLQ) every record in that page — the
// poison AND its non-poison COLLATERAL — is published to the DLQ as its own
// PoisonRecord, then the consumer continues past the page and checkpoints past it.
//
// This is the batch-distinct content vs IT-10a (record-mode DLQ, which isolates a
// single poison record). The load-bearing facts:
//   - fan-out COUNT == len(page), NOT len(page)*attempts (the publish loop runs
//     once, after retries exhaust — the batch analog of IT-10a's capture-count==1);
//   - each PoisonRecord carries batch metadata: BatchSize==len(page),
//     RecordIndexInBatch spanning 0..len(page)-1, Handler=="batch";
//   - the non-poison collateral in the page is never delivered to the success
//     handler (it shared the poison's fate);
//   - the page is checkpointed past, so a fresh consumer does not replay it.
//
// WithShardConcurrency is intentionally NOT set: the batch path returns before the
// concurrency branch (handle_records_page.go:17 vs :27), so it is a no-op here.
func TestBatchDLQFansOutWholePage(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("bdlq")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}
	shardID := shardIDs[0]

	const (
		batchSize     = 10
		total1        = 30 // 3 pages of 10 → the poison lands in the middle page
		poisonIdx     = 15 // page [10..19]
		maxAttempts   = 2  // the poison page exhausts these, then the page is DLQ'd
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

	batch1 := makePayloads("bdlq-b1", total1)
	poison := batch1[poisonIdx]
	ordinalOf := make(map[string]int, total1)
	for i, p := range batch1 {
		ordinalOf[p] = i
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	store := newStore(t, uniqueName("bdlq-kp"))

	// C1: batch mode. The batch handler fails the WHOLE page if it contains the
	// poison (so that page's records are all DLQ'd), otherwise records every record
	// of the page as delivered.
	successColl := newBatchCollector()
	recordPage := successColl.handler()
	batchHandler := func(hctx context.Context, records []consumer.Record) error {
		for _, rec := range records {
			if string(rec.Data) == poison {
				return errBatchDLQBoom
			}
		}
		return recordPage(hctx, records)
	}
	dlq := &capturingDLQ{}
	consC1, err := consumer.New(consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}, client, store, nil,
		consumer.WithBatchHandler(batchHandler),
		consumer.WithFailurePolicy(consumer.FailurePolicySendToDLQ),
		consumer.WithDLQPublisher(dlq),
		consumer.WithRetry(maxAttempts, 10*time.Millisecond),
		consumer.WithBatching(batchSize, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create batch dlq consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consC1)

	// The page AFTER the poison page must be delivered — proving the consumer
	// continued past the failed (DLQ'd) page. Wait on ordinals 20..29.
	afterPage := batch1[20:30]
	if missing := successColl.waitFor(afterPage, deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("C1 did not deliver the page after the poison page (no continuation): missing %d: %v", len(missing), missing)
	}
	// The page BEFORE the poison page (ordinals 0..9) is delivered too.
	if missing := successColl.waitFor(batch1[0:10], deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("C1 did not deliver the page before the poison page: missing %d: %v", len(missing), missing)
	}

	// Give the poison page's DLQ publishes time to land (they precede the after-page
	// checkpoint, so by the time afterPage is delivered they have already happened;
	// this is belt-and-suspenders before the graceful stop).
	stopC1()

	// (1) LOAD-BEARING fan-out COUNT: exactly one PoisonRecord per record in the
	// poison page — len(page), NOT len(page)*maxAttempts. The publish loop runs once
	// after retries exhaust; a per-attempt publish would give 20, a re-poll 20.
	published := dlq.snapshot()
	if len(published) != batchSize {
		t.Fatalf("DLQ captured %d records; want exactly %d (one per record in the failed page, not per-attempt)", len(published), batchSize)
	}

	// (2) Per-record batch metadata. Index by RecordIndexInBatch; every position
	// 0..batchSize-1 must appear exactly once, and index order must match sequence
	// order within the page (record at index i is the i-th record of the page).
	byIndex := make(map[int]consumer.PoisonRecord, len(published))
	ordinals := make([]int, 0, len(published))
	sawPoison := false
	for _, pr := range published {
		if _, dup := byIndex[pr.RecordIndexInBatch]; dup {
			t.Fatalf("duplicate RecordIndexInBatch %d in DLQ fan-out", pr.RecordIndexInBatch)
		}
		byIndex[pr.RecordIndexInBatch] = pr

		if pr.BatchSize != batchSize {
			t.Errorf("poison BatchSize = %d; want %d (the whole page)", pr.BatchSize, batchSize)
		}
		if pr.Handler != "batch" {
			t.Errorf("poison Handler = %q; want %q", pr.Handler, "batch")
		}
		if pr.Attempts != maxAttempts {
			t.Errorf("poison Attempts = %d; want %d (all retries exhausted)", pr.Attempts, maxAttempts)
		}
		if pr.RecordIndexInBatch != pr.RecordSequenceInBatchOrder {
			t.Errorf("poison index/order mismatch: %d != %d", pr.RecordIndexInBatch, pr.RecordSequenceInBatchOrder)
		}
		if pr.StreamName != stream {
			t.Errorf("poison StreamName = %q; want %q", pr.StreamName, stream)
		}
		if pr.StreamARN != "" {
			t.Errorf("poison StreamARN = %q; want empty (only StreamName configured)", pr.StreamARN)
		}
		if pr.ShardID != shardID {
			t.Errorf("poison ShardID = %q; want %q", pr.ShardID, shardID)
		}
		if pr.Error != errBatchDLQBoom.Error() {
			t.Errorf("poison Error = %q; want the raw batch handler error %q", pr.Error, errBatchDLQBoom.Error())
		}
		if pr.SequenceNumber == "" {
			t.Errorf("poison SequenceNumber is empty; want the Kinesis-assigned sequence")
		}

		// Payload must be a batch1 member; tie payload<->partition key<->ordinal.
		k, ok := ordinalOf[string(pr.Payload)]
		if !ok {
			t.Fatalf("DLQ payload %q is not a batch1 record", pr.Payload)
		}
		if wantPK := fmt.Sprintf("pk-%d", k); pr.PartitionKey != wantPK {
			t.Errorf("poison PartitionKey = %q; want %q (payload ordinal %d)", pr.PartitionKey, wantPK, k)
		}
		if k == poisonIdx {
			sawPoison = true
		}
		ordinals = append(ordinals, k)
	}
	for i := 0; i < batchSize; i++ {
		if _, ok := byIndex[i]; !ok {
			t.Fatalf("DLQ fan-out missing RecordIndexInBatch %d; want every position 0..%d", i, batchSize-1)
		}
	}
	if !sawPoison {
		t.Fatalf("the actual poison record (ordinal %d) was not among the DLQ'd page", poisonIdx)
	}
	// The DLQ'd records form one contiguous page, and index order == sequence order.
	sort.Ints(ordinals)
	minOrd := ordinals[0]
	for i, k := range ordinals {
		if k != minOrd+i {
			t.Fatalf("DLQ'd ordinals are not one contiguous page: %v", ordinals)
		}
	}
	for i := 0; i < batchSize; i++ {
		if got := string(byIndex[i].Payload); got != batch1[minOrd+i] {
			t.Fatalf("RecordIndexInBatch %d payload = %q; want ordinal %d (%q) — index order must match page sequence order", i, got, minOrd+i, batch1[minOrd+i])
		}
	}
	poisonPage := batch1[minOrd : minOrd+batchSize]
	t.Logf("batch DLQ fan-out: page ordinals %d..%d (all %d records) published, poison at ordinal %d", minOrd, minOrd+batchSize-1, batchSize, poisonIdx)

	// (3) COLLATERAL: not one record of the poison page reached the success handler —
	// the whole page shared the poison's fate (this is the batch-specific behavior).
	for _, p := range poisonPage {
		if n := successColl.count(p); n != 0 {
			t.Fatalf("poison-page record %q (ordinal %d) was delivered %d times; a failed batch delivers NONE of its records", p, ordinalOf[p], n)
		}
	}

	// (4) The pages around the poison page were delivered exactly once each.
	delivered := append(append([]string(nil), batch1[0:10]...), batch1[20:30]...)
	for _, p := range delivered {
		if n := successColl.count(p); n != 1 {
			t.Fatalf("non-poison-page record %q delivered %d times; want exactly 1", p, n)
		}
	}

	// (5) Checkpoint-past + resume: the DLQ'd page returned nil (send-to-dlq
	// continues), so it was checkpointed. Produce batch2 and start a fresh plain
	// batch-mode C2; it must deliver batch2 and replay NONE of batch1 (including the
	// poison page). If the page were not checkpointed, C2 (whose handler never
	// errors) would re-read and "deliver" the whole poison page → replay jumps.
	batch2 := makePayloads("bdlq-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	collC2 := newBatchCollector()
	consC2 := newBatchConsumer(t, stream, client, store, collC2.handler(), batchSize)
	_, stopC2 := runConsumer(t, consC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, deliverBudget); len(missing) != 0 {
		t.Fatalf("resumed C2 did not deliver batch2: missing %d/%d: %v", len(missing), total2, missing)
	}
	if replay := collC2.replayCount(batch1); replay != 0 {
		t.Fatalf("C2 replayed %d/%d batch1 records; the DLQ'd page must have been checkpointed past", replay, total1)
	}

	// (6) Completeness: the surviving records (non-poison-page batch1 + batch2) were
	// each delivered by some consumer; the poison page went to the DLQ, delivered by
	// nobody.
	for _, p := range delivered {
		if successColl.count(p)+collC2.count(p) == 0 {
			t.Fatalf("record %q was never delivered", p)
		}
	}
	for _, p := range batch2 {
		if collC2.count(p) == 0 {
			t.Fatalf("batch2 record %q was never delivered", p)
		}
	}
	for _, p := range poisonPage {
		if collC2.count(p) != 0 {
			t.Fatalf("C2 delivered poison-page record %q; it should have been checkpointed past", p)
		}
	}
	t.Logf("batch DLQ ok: %d records delivered around the page, %d DLQ'd, C2 resumed with 0 replay", len(delivered), batchSize)
}
