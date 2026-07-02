//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// TestCheckpointFrequencyBoundsReplayAfterCrash proves the checkpoint-frequency
// contract (#7): with checkpointEvery spanning MULTIPLE GetRecords pages, an
// ungracefully-stopped consumer persists a checkpoint that LAGS its processing
// frontier by more than one page, and a fresh consumer resumes EXACTLY after that
// checkpoint — replaying only a bounded set of records (not the whole stream, not
// nothing).
//
// Setup (single shard, records delivered strictly in order):
//   - batchSize = 10  → each GetRecords page returns exactly 10 records, so a
//     checkpointEvery of 30 spans three pages.
//   - checkpointEvery = 30 → `saveShardCheckpointIfDue` persists a checkpoint only
//     at the page boundary where the running count crosses a multiple of 30
//     (ordinals 29, 59, 89 … for 10-record pages).
//   - C1 uses NO graceful drain, so cancelling it does NOT flush the residual
//     processed-but-not-checkpointed records; the persisted checkpoint is the last
//     DUE save.
//
// C1's handler records the first `stopAfter` records and then PARKS on ctx.Done
// (returning a terminal context error), so C1 delivers a deterministic prefix and
// stops mid-stream, mid-page. A fresh C2 then resumes from the persisted
// checkpoint and runs to the tip.
func TestCheckpointFrequencyBoundsReplayAfterCrash(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-ckptfreq")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const (
		shardCount      = 1
		total           = 120
		batchSize       = 10
		checkpointEvery = 30
		// Park after delivering ordinals 0..88. The page holding ordinal 89 never
		// completes, and the count since the last due save (at ordinal 59) never
		// reaches 30 again, so the persisted checkpoint stays at ordinal 59 while
		// C1's frontier is 88 — a lag of ~one checkpoint interval, spanning pages.
		stopAfter = 89
	)

	// Test precondition: the checkpoint interval must span more than one page, or
	// there is no multi-page lag to observe and the replay lower bound below would
	// not distinguish this from per-page checkpointing.
	if checkpointEvery <= batchSize {
		t.Fatalf("test invariant: checkpointEvery (%d) must exceed batchSize (%d) so a checkpoint interval spans multiple pages", checkpointEvery, batchSize)
	}

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

	// Pin every record to the single shard so delivery order == production order.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("expected %d shard hash key, got %d", shardCount, len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	payloads := makePayloads("freq", total)
	ordinalOf := make(map[string]int, total)
	for i, p := range payloads {
		ordinalOf[p] = i
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, payloads)

	// Phase 1: C1 processes the first stopAfter records, then parks mid-page.
	collC1 := newCollector()
	var handled atomic.Int64
	record := collC1.handler()
	parkingHandler := func(hctx context.Context, rec consumer.Record) error {
		if int(handled.Add(1)) <= stopAfter {
			return record(hctx, rec)
		}
		<-hctx.Done()
		return hctx.Err()
	}

	consumerC1 := newNoDrainConsumer(t, stream, client, store, parkingHandler, batchSize, checkpointEvery)
	_, stopC1 := runConsumer(t, consumerC1)

	// Wait until C1 has delivered exactly the prefix (ordinals 0..stopAfter-1); the
	// next record parks, so the count cannot advance past it.
	if missing := collC1.waitFor(payloads[:stopAfter], 90*time.Second); missing != nil {
		stopC1()
		t.Fatalf("C1 did not deliver its prefix; missing %d/%d", len(missing), stopAfter)
	}

	// Ungraceful stop: cancel with no drain, so the residual is NOT flushed. This
	// also releases C1's lease (runShardWorker releases on exit), so C2 can acquire.
	stopC1()

	c1Ordinals := deliveredOrdinals(collC1, payloads, ordinalOf)
	wantC1 := rangeOrdinals(0, stopAfter) // 0..stopAfter-1
	if !equalOrdinals(c1Ordinals, wantC1) {
		t.Fatalf("C1 delivered ordinals %v, want exactly 0..%d", summarizeOrdinals(c1Ordinals), stopAfter-1)
	}

	// Phase 2: a fresh C2 resumes from the persisted checkpoint and runs to the tip.
	collC2 := newCollector()
	consumerC2 := newConsumer(t, stream, client, store, collC2.handler())
	_, stopC2 := runConsumer(t, consumerC2)
	defer stopC2()

	// C2 must at least deliver the tail that lies strictly above any possible
	// checkpoint position. For 10-record pages the last due save is at ordinal 59;
	// since ordinal 89's page never completes, no ordinal-89 save occurs, so the
	// checkpoint is bounded well below stopAfter-batchSize regardless of exact page
	// size. Waiting on this tail confirms C2 resumed and ran to the tip; the precise
	// resume position is then pinned by the contiguity + replay-bound assertions.
	tail := payloads[stopAfter-batchSize:] // ordinals (stopAfter-batchSize)..total-1
	if missing := collC2.waitFor(tail, 90*time.Second); missing != nil {
		t.Fatalf("resumed C2 did not deliver the tail; missing %d/%d: %v", len(missing), len(tail), missing)
	}

	c2Ordinals := deliveredOrdinals(collC2, payloads, ordinalOf)
	if len(c2Ordinals) == 0 {
		t.Fatal("C2 delivered nothing")
	}
	firstC2 := c2Ordinals[0]
	lastC2 := c2Ordinals[len(c2Ordinals)-1]

	// Precise resume: C2 delivered a contiguous run from its first ordinal to the
	// tip, with no gaps and nothing below firstC2. The implied checkpoint ordinal is
	// firstC2-1.
	if lastC2 != total-1 {
		t.Fatalf("C2 last delivered ordinal = %d, want %d (tip)", lastC2, total-1)
	}
	if !equalOrdinals(c2Ordinals, rangeOrdinals(firstC2, total)) {
		t.Fatalf("C2 delivered a non-contiguous run %v; want exactly %d..%d", summarizeOrdinals(c2Ordinals), firstC2, total-1)
	}

	// Frequency + bounded-replay contract. replay = the records C1 already delivered
	// that C2 re-delivered = C1 frontier (stopAfter) minus firstC2.
	replay := stopAfter - firstC2
	checkpointOrdinal := firstC2 - 1
	t.Logf("checkpoint ordinal=%d, C1 frontier=%d, firstC2=%d, replay=%d (batchSize=%d, checkpointEvery=%d)",
		checkpointOrdinal, stopAfter-1, firstC2, replay, batchSize, checkpointEvery)

	// Lower bound: replay >= batchSize means the checkpoint lagged the frontier by
	// MORE than one page — impossible unless checkpointEvery spans multiple pages
	// (per-page checkpointing would leave at most one partial page unflushed).
	if replay < batchSize {
		t.Fatalf("replay=%d < batchSize=%d: checkpoint did not lag beyond one page, so checkpointEvery>1 was not exercised", replay, batchSize)
	}
	// Upper bound: replay < checkpointEvery means the resume was bounded to the
	// records after the last persisted checkpoint — not the whole stream.
	if replay >= checkpointEvery {
		t.Fatalf("replay=%d >= checkpointEvery=%d: more than one checkpoint interval was replayed (checkpoint not honored)", replay, checkpointEvery)
	}

	// Completeness: every record was delivered at least once across C1 and C2.
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
		t.Fatalf("records lost across the crash/resume: %d/%d missing: %v", len(missing), total, missing)
	}
}

// deliveredOrdinals returns the sorted ordinals of the payloads a collector saw at
// least once.
func deliveredOrdinals(c *collector, payloads []string, ordinalOf map[string]int) []int {
	var out []int
	for _, p := range payloads {
		if c.count(p) > 0 {
			out = append(out, ordinalOf[p])
		}
	}
	sort.Ints(out)
	return out
}

// rangeOrdinals returns [lo, hi) as a sorted slice.
func rangeOrdinals(lo, hi int) []int {
	out := make([]int, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, i)
	}
	return out
}

func equalOrdinals(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// summarizeOrdinals renders ordinals compactly for diagnostics: the first few
// plus the total count and last ordinal.
func summarizeOrdinals(o []int) string {
	if len(o) == 0 {
		return "[] (n=0)"
	}
	head := o
	suffix := ""
	if len(head) > 8 {
		head = head[:8]
		suffix = "…"
	}
	return fmt.Sprintf("%v%s (n=%d, last=%d)", head, suffix, len(o), o[len(o)-1])
}
