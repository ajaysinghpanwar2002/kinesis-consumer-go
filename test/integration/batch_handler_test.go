//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	valkeycheckpoint "github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// batchCollector records what a BATCH handler observes: every payload's
// occurrence count (like collector) PLUS the size of each batch call, so a test
// can prove delivery is page-shaped (multi-record calls) rather than
// one-call-per-record.
type batchCollector struct {
	mu        sync.Mutex
	seen      map[string]int
	callSizes []int
}

func newBatchCollector() *batchCollector { return &batchCollector{seen: make(map[string]int)} }

// handler returns a BatchHandlerFunc that records each call's size and every
// record's data. It never errors (IT-11a covers delivery/checkpoint only; batch
// failure policy is IT-11b).
func (c *batchCollector) handler() consumer.BatchHandlerFunc {
	return func(_ context.Context, records []consumer.Record) error {
		c.mu.Lock()
		c.callSizes = append(c.callSizes, len(records))
		for _, rec := range records {
			c.seen[string(rec.Data)]++
		}
		c.mu.Unlock()
		return nil
	}
}

func (c *batchCollector) count(payload string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[payload]
}

func (c *batchCollector) missing(payloads []string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, p := range payloads {
		if c.seen[p] == 0 {
			out = append(out, p)
		}
	}
	return out
}

func (c *batchCollector) waitFor(payloads []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		missing := c.missing(payloads)
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return missing
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sizes returns a copy of the recorded batch-call sizes.
func (c *batchCollector) sizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, len(c.callSizes))
	copy(out, c.callSizes)
	return out
}

// replayCount returns how many of the given payloads this collector saw at least
// once (used to measure cross-consumer replay).
func (c *batchCollector) replayCount(payloads []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, p := range payloads {
		if c.seen[p] > 0 {
			n++
		}
	}
	return n
}

// newBatchConsumer builds a consumer in BATCH mode: the positional record
// handler is nil and delivery goes through WithBatchHandler. Batching/polling are
// tuned short and graceful drain is enabled, mirroring newConsumer.
func newBatchConsumer(t *testing.T, stream string, client *kinesis.Client, store *valkeycheckpoint.Store, batch consumer.BatchHandlerFunc, batchSize int32) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, nil,
		consumer.WithBatchHandler(batch),
		consumer.WithBatching(batchSize, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create batch consumer for %s: %v", stream, err)
	}
	return cons
}

// TestBatchHandlerDeliversPagesAndResumes proves scenario #19 (IT-11a): a
// consumer configured with WithBatchHandler (a) delivers records to the BATCH
// handler once per GetRecords page — page-shaped, multi-record calls, not
// one-call-per-record — (b) checkpoints per page so a fresh batch-mode consumer
// resumes PAST completed pages, and (c) loses nothing.
//
// The checkpoint/resume half runs the same processRecordsPageWithCheckpoint path
// IT-3/IT-4/IT-9a already exercised (only the page-dispatch differs), so it is
// confirmatory here; the genuinely new content is the batch SHAPE assertion. That
// shape rests on the source fact that handle_records_page.go dispatches the WHOLE
// page to the batch handler once, plus the observed call-size distribution — it
// cannot be mutation-tested in-process (batch mode can't be made per-record
// without a library change). It is self-checking, though: with a NIL positional
// handler, if batch dispatch were broken the page path would hit "record handler
// is nil" and the test would fail loudly rather than silently pass.
func TestBatchHandlerDeliversPagesAndResumes(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("batch")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}

	const (
		batchSize     = 10
		batch1N       = 30 // 3 full pages of 10 → guarantees multi-record calls
		batch2N       = 20
		deliverBudget = 60 * time.Second
	)

	// Pin every record to the single shard so pages are dense and delivery order
	// == production order.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != 1 {
		t.Fatalf("want exactly 1 shard hash key, got %d", len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	batch1 := makePayloads("b1", batch1N)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	store := newStore(t, uniqueName("batch-kp"))

	// C1: batch mode, consumes all of batch1. checkpointEvery=1 (via
	// newBatchConsumer's WithBatching) due-saves at every page boundary, so after
	// C1 catches up the checkpoint sits at batch1's tip.
	collC1 := newBatchCollector()
	consC1 := newBatchConsumer(t, stream, client, store, collC1.handler(), batchSize)
	_, stopC1 := runConsumer(t, consC1)

	if missing := collC1.waitFor(batch1, deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("C1 (batch mode) did not deliver all of batch1: missing %d/%d: %v", len(missing), batch1N, missing)
	}

	// (a) Batch SHAPE — the novel, batch-specific content. Delivery is page-shaped:
	//   - at least one call carried >1 record (multi-record, NOT per-record);
	//   - every call is bounded by batchSize (the LIB-1 GetRecords Limit contract);
	//   - there are far fewer calls than records (batched, not one-call-per-record).
	sizes := collC1.sizes()
	if len(sizes) == 0 {
		stopC1()
		t.Fatal("C1 batch handler was never called")
	}
	maxSize, multiRecordCalls := 0, 0
	for _, s := range sizes {
		if s > maxSize {
			maxSize = s
		}
		if s > 1 {
			multiRecordCalls++
		}
		if s > batchSize {
			stopC1()
			t.Fatalf("batch call size %d exceeds batchSize %d; pages must be bounded by the GetRecords Limit (sizes=%v)", s, batchSize, sizes)
		}
	}
	if multiRecordCalls == 0 {
		stopC1()
		t.Fatalf("no batch call carried more than one record; delivery is not page-shaped (sizes=%v)", sizes)
	}
	if len(sizes) >= batch1N {
		stopC1()
		t.Fatalf("batch handler was called %d times for %d records; delivery looks per-record, not batched (sizes=%v)", len(sizes), batch1N, sizes)
	}
	t.Logf("batch shape: %d calls for %d records, max call size=%d, sizes=%v", len(sizes), batch1N, maxSize, sizes)

	// Exactly-once within this healthy, error-free run (batch retry re-delivers a
	// whole page, but no page errors here).
	for _, p := range batch1 {
		if got := collC1.count(p); got != 1 {
			stopC1()
			t.Fatalf("C1 delivered %q %d times, want exactly 1 (no error → no page re-delivery)", p, got)
		}
	}

	// Graceful stop: drain flush + lease release. Checkpoint is at batch1's tip.
	stopC1()

	// Produce batch2 AFTER C1 stopped, so it is only ever seen by C2.
	batch2 := makePayloads("b2", batch2N)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	// C2: a fresh batch-mode consumer on the SAME store. It must resume past
	// batch1 (per-page checkpoint honored) and deliver batch2.
	collC2 := newBatchCollector()
	consC2 := newBatchConsumer(t, stream, client, store, collC2.handler(), batchSize)
	_, stopC2 := runConsumer(t, consC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, deliverBudget); len(missing) != 0 {
		t.Fatalf("C2 (batch mode) did not deliver all of batch2: missing %d/%d: %v", len(missing), batch2N, missing)
	}

	// (b) LOAD-BEARING: C2 resumed from the per-page checkpoint, so it replays NONE
	// of batch1. If the per-page checkpoint were not written/honored, C2 would
	// restart from TRIM_HORIZON and replay all of batch1 (mutation: point C2 at a
	// fresh empty store → replay jumps to batch1N).
	if replay := collC2.replayCount(batch1); replay != 0 {
		t.Fatalf("C2 replayed %d/%d batch1 records; want 0 (per-page checkpoint not honored — resumed from the beginning?)", replay, batch1N)
	}

	// C2's calls are also batch-shaped (sanity; batch2N=20 > batchSize).
	c2Sizes := collC2.sizes()
	c2Multi := false
	for _, s := range c2Sizes {
		if s > batchSize {
			t.Fatalf("C2 batch call size %d exceeds batchSize %d (sizes=%v)", s, batchSize, c2Sizes)
		}
		if s > 1 {
			c2Multi = true
		}
	}
	if !c2Multi {
		t.Fatalf("C2 delivered no multi-record batch (sizes=%v)", c2Sizes)
	}

	// (c) Completeness: every record delivered by C1 or C2 at least once.
	for _, p := range batch1 {
		if collC1.count(p)+collC2.count(p) == 0 {
			t.Fatalf("batch1 record %q was never delivered", p)
		}
	}
	for _, p := range batch2 {
		if collC1.count(p)+collC2.count(p) == 0 {
			t.Fatalf("batch2 record %q was never delivered", p)
		}
	}
	t.Logf("batch resume ok: C1 delivered all %d batch1 records, C2 resumed and delivered all %d batch2 records with 0 replay", batch1N, batch2N)
}
