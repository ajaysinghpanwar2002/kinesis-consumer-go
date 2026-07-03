//go:build integration

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// errTransient is a plain (non-context) error a handler returns to simulate a
// transient failure that a retry can recover from. It must not be a context error
// or retryHandler would short-circuit instead of retrying.
var errTransient = errors.New("integration: transient handler failure")

// newRetryHandler returns a handler that fails the first failTimes attempts on the
// designated flaky payload and succeeds afterwards, recording every successfully
// handled record to coll. It counts how many times the flaky record's handler was
// invoked (including the successful attempt).
func newRetryHandler(coll *collector, flaky string, failTimes int, flakyAttempts *atomic.Int64) consumer.HandlerFunc {
	record := coll.handler()
	return func(hctx context.Context, rec consumer.Record) error {
		if string(rec.Data) == flaky {
			if int(flakyAttempts.Add(1)) <= failTimes {
				return errTransient
			}
			// Fall through on the (failTimes+1)-th attempt: success.
		}
		return record(hctx, rec)
	}
}

// TestRetryEventuallySucceeds proves the retry behavior (#18): a handler that fails
// a few times and then succeeds is retried (WithRetry), and on the successful
// attempt the record is processed — delivered once, not skipped or sent to a DLQ —
// and its page is checkpointed so a resuming consumer does not replay it.
//
// Scope note: the library is at-least-once, not globally exactly-once. The "once"
// claims here are scoped to this single run — the retry loop does not double-deliver
// the eventually-successful record, and the resumed consumer does not replay the
// already-checkpointed page.
//
// maxAttempts (5) is set strictly greater than failTimes+1 (3), so
// flakyAttempts==3 proves the retry loop STOPPED early on the first success rather
// than burning all attempts. WithFailurePolicy(FailFast) is a tripwire: no record
// should ever exhaust its retries here, so a FailFast error would mean retry is
// broken (loud, fast failure) rather than a silent skip.
//
// Companion mutations (documented in TASK_PLAN, verified out of band):
//   - C2 pointed at a fresh empty store: C2 replays batch1, so replay>0 (the
//     "then checkpointed" half).
//   - WithRetry(1, .): the flaky record exhausts its single attempt -> FailFast ->
//     C1 Start errors and the flaky record is never delivered (the retry tripwire).
func TestRetryEventuallySucceeds(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-retry")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const (
		shardCount  = 1
		total1      = 30
		flakyIdx    = 15
		failTimes   = 2
		maxAttempts = 5 // strictly > failTimes+1, so attempts==failTimes+1 proves early stop
		total2      = 20
	)

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

	batch1 := makePayloads("retry-b1", total1)
	flaky := batch1[flakyIdx]
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	// Phase 1: C1 processes batch1; the flaky record fails failTimes then succeeds.
	collC1 := newCollector()
	var flakyAttempts atomic.Int64
	cfg := consumer.Config{StreamName: stream, StartPosition: consumer.StartTrimHorizon}
	consumerC1, err := consumer.New(cfg, client, store, newRetryHandler(collC1, flaky, failTimes, &flakyAttempts),
		consumer.WithFailurePolicy(consumer.FailurePolicyFailFast), // tripwire: fires only if retry fails to recover
		consumer.WithRetry(maxAttempts, 10*time.Millisecond),
		consumer.WithBatching(10, 1),
		consumer.WithShardConcurrency(1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create retry consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consumerC1)

	// Every record, including the flaky one, is delivered. If retry did not recover
	// the flaky record, FailFast would error C1 and this would time out.
	if missing := collC1.waitFor(batch1, 90*time.Second); missing != nil {
		stopC1()
		t.Fatalf("C1 did not deliver all batch1 records; missing %d/%d: %v", len(missing), total1, missing)
	}
	stopC1()

	// (1) The flaky record was retried exactly failTimes times and then succeeded,
	// stopping early (attempts < maxAttempts).
	if got := flakyAttempts.Load(); got != failTimes+1 {
		t.Fatalf("flaky record handler was invoked %d times; want exactly %d (failTimes=%d then one success, stopping before maxAttempts=%d)",
			got, failTimes+1, failTimes, maxAttempts)
	}
	// (2) The eventually-successful record was delivered exactly once (retry did not
	// double-deliver it in this run).
	if n := collC1.count(flaky); n != 1 {
		t.Fatalf("flaky record delivered %d times; want exactly 1 (retry must not double-deliver on success)", n)
	}
	// (3) Every batch1 record delivered exactly once.
	for _, p := range batch1 {
		if n := collC1.count(p); n != 1 {
			t.Fatalf("C1 delivered record %q %d times; want exactly 1", p, n)
		}
	}

	// Phase 2: produce later records, then a fresh C2 resumes from the checkpoint.
	batch2 := makePayloads("retry-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	collC2 := newCollector()
	consumerC2 := newConsumer(t, stream, client, store, collC2.handler())
	_, stopC2 := runConsumer(t, consumerC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, 90*time.Second); missing != nil {
		t.Fatalf("resumed C2 did not deliver batch2; missing %d/%d: %v", len(missing), total2, missing)
	}

	// (4) LOAD-BEARING: C2 replayed none of batch1 — the successful flaky record's
	// page was checkpointed, so the resume starts past it.
	replay := 0
	for _, p := range batch1 {
		if collC2.count(p) > 0 {
			replay++
		}
	}
	t.Logf("C1 flaky attempts=%d (delivered %d), C2 delivered batch2 with replay=%d of batch1",
		flakyAttempts.Load(), collC1.count(flaky), replay)
	if replay != 0 {
		t.Fatalf("C2 replayed %d batch1 records; the successful record's page must have been checkpointed", replay)
	}

	// (5)+(6) Completeness: every batch1 and batch2 record delivered at least once
	// across C1 and C2.
	all := append(append([]string(nil), batch1...), batch2...)
	for _, p := range all {
		if collC1.count(p)+collC2.count(p) == 0 {
			t.Fatalf("record %q was never delivered by either consumer", p)
		}
	}
}
