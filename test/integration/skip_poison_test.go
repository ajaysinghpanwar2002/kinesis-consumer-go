//go:build integration

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// errPoisonSkip is the permanent handler error used to drive the skip policy on a
// designated poison record. Package-level so both consumers share it.
var errPoisonSkip = errors.New("integration: permanent poison-record failure")

// newSkipHandler returns a handler that records every non-poison record to coll
// and permanently fails (returns errPoisonSkip) on the poison payload, counting
// how many times the poison was reached.
func newSkipHandler(coll *collector, poison string, poisonCalls *atomic.Int64) consumer.HandlerFunc {
	record := coll.handler()
	return func(hctx context.Context, rec consumer.Record) error {
		if string(rec.Data) == poison {
			poisonCalls.Add(1)
			return errPoisonSkip
		}
		return record(hctx, rec)
	}
}

// TestSkipPoisonContinuesAndCheckpoints proves the skip failure policy (#15): a
// record whose handler permanently fails, under WithFailurePolicy(FailurePolicySkip),
// is SKIPPED — the consumer continues processing later records (it is not blocked
// on the poison) AND the checkpoint advances past the poison's page, so a fresh
// consumer resuming does not replay it.
//
// Structure mirrors IT-4's resume proof: C1 processes batch1 (with a mid-stream
// poison), is stopped, batch2 is produced, and a fresh C2 resumes from the
// persisted checkpoint. C2 replaying zero of batch1 is the load-bearing evidence
// that the skip checkpointed past the poison.
//
// Companion mutations (documented in TASK_PLAN, verified out of band):
//   - policy -> FailFast: C1 errors on the poison, so the post-poison delivery wait
//     times out (continuation claim).
//   - C2 pointed at a fresh empty store: C2 replays batch1 from TRIM_HORIZON, so the
//     replay==0 assertion fires (checkpoint claim).
func TestSkipPoisonContinuesAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-skip")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const (
		shardCount = 1
		total1     = 30 // three pages of 10
		poisonIdx  = 15 // mid-stream, with records after it in the same and later pages
		total2     = 20
	)

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

	// Pin every record to the single shard so delivery order == production order and
	// the poison is reached deterministically.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("expected %d shard hash key, got %d", shardCount, len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	batch1 := makePayloads("skip-b1", total1)
	poison := batch1[poisonIdx]
	nonPoison1 := make([]string, 0, total1-1)
	for i, p := range batch1 {
		if i != poisonIdx {
			nonPoison1 = append(nonPoison1, p)
		}
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	// Phase 1: C1 processes batch1 with the skip policy, skipping the poison.
	collC1 := newCollector()
	var poisonCallsC1 atomic.Int64
	cfg := consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon}
	consumerC1, err := consumer.New(cfg, client, store, newSkipHandler(collC1, poison, &poisonCallsC1),
		consumer.WithFailurePolicy(consumer.FailurePolicySkip),
		consumer.WithRetry(1, time.Millisecond),
		consumer.WithBatching(10, 1),
		consumer.WithShardConcurrency(1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create skip consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consumerC1)

	// Continuation: every non-poison record, including those AFTER the poison, is
	// delivered. If the poison blocked the stream this would time out.
	if missing := collC1.waitFor(nonPoison1, 90*time.Second); missing != nil {
		stopC1()
		t.Fatalf("C1 did not deliver all non-poison batch1 records; missing %d/%d: %v", len(missing), len(nonPoison1), missing)
	}

	// The skip path was actually exercised (the poison was reached and failed),
	// rather than the poison never being read.
	if got := poisonCallsC1.Load(); got < 1 {
		stopC1()
		t.Fatalf("poison handler was never invoked (calls=%d); the skip path was not exercised", got)
	}

	// Graceful stop: all three pages completed before the stop, so the checkpoint is
	// at batch1's tip (each completed page checkpointed at checkpointEvery=1).
	stopC1()

	// Exactly-once for non-poison batch1; the poison is never delivered (the handler
	// fails before recording it).
	for _, p := range nonPoison1 {
		if n := collC1.count(p); n != 1 {
			t.Fatalf("C1 delivered non-poison record %q %d times; want exactly 1", p, n)
		}
	}
	if n := collC1.count(poison); n != 0 {
		t.Fatalf("C1 delivered the poison record %d times; a skipped record must not be delivered", n)
	}

	// Phase 2: produce later records, then a fresh C2 resumes from the checkpoint.
	batch2 := makePayloads("skip-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	collC2 := newCollector()
	var poisonCallsC2 atomic.Int64
	consumerC2 := newConsumer(t, stream, client, store, newSkipHandler(collC2, poison, &poisonCallsC2))
	_, stopC2 := runConsumer(t, consumerC2)
	defer stopC2()

	// C2 delivered the later records: it resumed and is alive.
	if missing := collC2.waitFor(batch2, 90*time.Second); missing != nil {
		t.Fatalf("resumed C2 did not deliver batch2; missing %d/%d: %v", len(missing), len(batch2), missing)
	}

	// LOAD-BEARING: C2 replayed none of batch1 — the checkpoint advanced past the
	// poison to batch1's tip. Any replay would mean the skip failed to checkpoint past
	// the poison's page.
	replay := 0
	for _, p := range nonPoison1 {
		if collC2.count(p) > 0 {
			replay++
		}
	}
	t.Logf("C1 delivered %d/%d non-poison batch1 (poisonCalls=%d), C2 delivered batch2 with replay=%d of batch1 (C2 poisonCalls=%d)",
		len(nonPoison1), total1, poisonCallsC1.Load(), replay, poisonCallsC2.Load())
	if replay != 0 {
		t.Fatalf("C2 replayed %d batch1 records; the skip must have checkpointed past the poison", replay)
	}
	if n := collC2.count(poison); n != 0 {
		t.Fatalf("C2 delivered the poison record %d times; it should never be delivered", n)
	}

	// Completeness: every non-poison batch1 record and every batch2 record was
	// delivered at least once across C1 and C2; the poison was delivered by nobody.
	all := append(append([]string(nil), nonPoison1...), batch2...)
	for _, p := range all {
		if collC1.count(p)+collC2.count(p) == 0 {
			t.Fatalf("record %q was never delivered by either consumer", p)
		}
	}
}
