//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// concurrencyProbe wraps a plain collector but instruments the handler to track
// how many handler invocations are in flight at once, keeping the running MAX.
// A per-record sleep holds each invocation open long enough for its siblings to
// overlap, so a WithShardConcurrency(N>1) consumer drives the max above 1 while a
// sequential (N==1) consumer never does.
type concurrencyProbe struct {
	coll        *collector
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	perRecord   time.Duration
}

func newConcurrencyProbe(perRecord time.Duration) *concurrencyProbe {
	return &concurrencyProbe{coll: newCollector(), perRecord: perRecord}
}

// handler returns a HandlerFunc that records the max concurrent in-flight count,
// sleeps to force overlap, then records the payload via the wrapped collector.
func (p *concurrencyProbe) handler() consumer.HandlerFunc {
	inner := p.coll.handler()
	return func(ctx context.Context, rec consumer.Record) error {
		cur := p.inFlight.Add(1)
		for {
			m := p.maxInFlight.Load()
			if cur <= m || p.maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(p.perRecord)
		err := inner(ctx, rec)
		p.inFlight.Add(-1)
		return err
	}
}

// TestShardConcurrencyDeliversAllAndCheckpointsPerPage proves scenario #20
// (IT-12): with WithShardConcurrency(N>1) in record-handler mode, a shard's page
// is processed by a pool of goroutines pulling records round-robin, so records
// WITHIN a page run CONCURRENTLY and out of order (handle_records_page.go:48).
// Despite that concurrency, in a healthy (poison-free) run delivery is
// exactly-once and the checkpoint still advances per SUCCESSFULLY-handled page,
// so a fresh consumer resumes past completed pages and replays nothing.
//
// The novel, load-bearing content is the concurrency observation (maxInFlight>1);
// exactly-once is self-checking (the round-robin `next` counter cannot hand the
// same record to two workers, and no page errors here so there is no retry to
// double a sibling); and the resume-replay==0 half re-exercises the per-page
// checkpoint path IT-3/IT-4/IT-11a already proved — its value here is that,
// paired with completeness, it pins the checkpoint to the PAGE boundary even
// though records completed out of order (replay==0 catches "checkpoint too low",
// completeness catches "too high").
//
// Honesty note: this proves the checkpoint DOES advance per successfully-handled
// page. It does NOT prove the negative (a partial-page failure must NOT advance
// the checkpoint) — that atomicity case is a distinct behavior (see IT-12b in
// TASK_PLAN.md), intentionally not covered here.
func TestShardConcurrencyDeliversAllAndCheckpointsPerPage(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("shardconc")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}

	const (
		concurrency   = 4
		batchSize     = 10
		total1        = 30 // 3 dense pages of 10 → each page has plenty of records for the pool
		total2        = 20
		perRecord     = 50 * time.Millisecond
		deliverBudget = 60 * time.Second
	)

	// Pin every record to the single shard so pages are dense (a full page gives
	// the pool >1 record to run in parallel).
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != 1 {
		t.Fatalf("want exactly 1 shard hash key, got %d", len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	batch1 := makePayloads("sc-b1", total1)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	store := newStore(t, uniqueName("shardconc-kp"))

	// C1: record mode with WithShardConcurrency(4). The probe handler forces
	// per-record overlap and records the max concurrent in-flight count.
	probe := newConcurrencyProbe(perRecord)
	consC1, err := consumer.New(consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}, client, store, probe.handler(),
		consumer.WithShardConcurrency(concurrency),
		consumer.WithBatching(batchSize, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create concurrent consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consC1)

	if missing := probe.coll.waitFor(batch1, deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("C1 did not deliver all of batch1: missing %d/%d: %v", len(missing), total1, missing)
	}

	// (1) LOAD-BEARING concurrency observation: at least two handler invocations
	// were in flight simultaneously. With WithShardConcurrency(1) the sequential
	// path (handle_records_page.go:33) runs and this stays at 1 → the assertion
	// fires (that is the mutation that makes this test IT-12, not IT-11a). Assert
	// >1 (not ==concurrency) so a short final page can't make it flaky.
	maxObserved := probe.maxInFlight.Load()
	if maxObserved <= 1 {
		stopC1()
		t.Fatalf("max concurrent in-flight handler calls = %d; want > 1 (WithShardConcurrency(%d) must process a page's records concurrently)", maxObserved, concurrency)
	}
	t.Logf("shard concurrency observed: max %d handler calls in flight at once (WithShardConcurrency(%d), page size %d)", maxObserved, concurrency, batchSize)

	// (2) Exactly-once: concurrent, out-of-order processing must not drop or
	// double-deliver any record in this healthy (no-error → no page retry) run.
	for _, p := range batch1 {
		if got := probe.coll.count(p); got != 1 {
			stopC1()
			t.Fatalf("C1 delivered %q %d times, want exactly 1 (concurrency must not drop/duplicate; no error → no page re-delivery)", p, got)
		}
	}

	// Graceful stop: drain flush + lease release. Checkpoint sits at batch1's tip.
	stopC1()

	// Produce batch2 AFTER C1 stopped so it is only ever seen by C2.
	batch2 := makePayloads("sc-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	// C2: a fresh plain (concurrency 1) consumer on the SAME store. It must resume
	// past batch1 (per-page checkpoint honored) and deliver batch2.
	collC2 := newCollector()
	consC2 := newConsumer(t, stream, client, store, collC2.handler())
	_, stopC2 := runConsumer(t, consC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, deliverBudget); len(missing) != 0 {
		t.Fatalf("resumed C2 did not deliver batch2: missing %d/%d: %v", len(missing), total2, missing)
	}

	// (3) LOAD-BEARING checkpoint-per-page: C2 replays NONE of batch1. Even though
	// C1 processed each page's records out of order, the checkpoint advanced only
	// after each whole page succeeded, so the tip is page-aligned and C2 resumes
	// past it. Mutation: a fresh empty store for C2 → replay jumps to total1.
	replay := 0
	for _, p := range batch1 {
		if collC2.count(p) > 0 {
			replay++
		}
	}
	if replay != 0 {
		t.Fatalf("C2 replayed %d/%d batch1 records; want 0 (per-page checkpoint not honored — resumed from the beginning?)", replay, total1)
	}

	// (4) Completeness: every record delivered by C1 or C2 at least once.
	for _, p := range batch1 {
		if probe.coll.count(p)+collC2.count(p) == 0 {
			t.Fatalf("batch1 record %q was never delivered", p)
		}
	}
	for _, p := range batch2 {
		if collC2.count(p) == 0 {
			t.Fatalf("batch2 record %q was never delivered", p)
		}
	}
	t.Logf("shard concurrency ok: C1 delivered all %d batch1 records concurrently (max %d in flight), C2 resumed with 0 replay and delivered all %d batch2 records", total1, maxObserved, total2)
}
