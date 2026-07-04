//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// TestStartLatestSkipsBacklogDeliversNew proves scenario #9 (IT-13a): a FRESH
// consumer (no checkpoint) configured with StartPosition: StartLatest does NOT
// deliver records that already existed in the shard when it started, and DOES
// deliver records produced after its LATEST iterator is live.
//
// StartPosition only applies on a first run with no checkpoint — getShardIterator
// reads the checkpoint first and uses AFTER_SEQUENCE_NUMBER when one exists — so
// this uses a fresh store. LATEST anchors to iterator-acquisition time, so
// positioning is confirmed deterministically with probe records (produce one,
// wait, repeat until delivered) rather than a bare sleep that would race the
// discovery+lease+GetShardIterator gap. (The library holds the iterator across
// poll passes — see LIB-2 — so once positioned it does not re-anchor and drop
// new records; before that fix this positioning was flaky.)
//
// The load-bearing assertion is the NEGATIVE one: the pre-existing backlog is
// delivered 0 times. The positive (all "after" delivered) is self-checking — a
// broken LATEST would simply time out. Mutation: flip StartPosition to
// StartTrimHorizon with the same fresh store → the backlog gets delivered → the
// backlog-count==0 assertion fires. That is the one proof this test distinguishes
// LATEST from the TRIM_HORIZON hardcoded in every other integration test.
//
// Deliberately out of scope: StartLatest with an existing checkpoint (the
// AFTER_SEQUENCE_NUMBER path takes precedence, covered by IT-3/IT-4), multi-shard
// LATEST, and AT_TIMESTAMP (→ IT-13b).
func TestStartLatestSkipsBacklogDeliversNew(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("startlatest")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}

	const (
		backlogN       = 30
		afterN         = 20
		positionBudget = 30 * time.Second
		deliverBudget  = 60 * time.Second
	)

	// Pin every record to the single shard so there is exactly one iterator to
	// reason about and delivery order == production order.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != 1 {
		t.Fatalf("want exactly 1 shard hash key, got %d", len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	// PRECONDITION for the negative assertion: the backlog must be durably in the
	// shard BEFORE the LATEST iterator is acquired. putRecordsToShard is synchronous
	// and the consumer is started only after it returns, so this holds — keep this
	// ordering strict (do not move production after runConsumer).
	backlog := makePayloads("sl-before", backlogN)
	putRecordsToShard(ctx, t, client, stream, hashKey, backlog)

	// Fresh store: with a checkpoint present, getShardIterator would use
	// AFTER_SEQUENCE_NUMBER and StartPosition would be ignored entirely.
	store := newStore(t, uniqueName("startlatest-kp"))

	coll := newCollector()
	consC1, err := consumer.New(consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartLatest,
	}, client, store, coll.handler(),
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create StartLatest consumer: %v", err)
	}
	_, stopC1 := runConsumer(t, consC1)
	defer stopC1()

	// (1) Position at LATEST deterministically: keep producing a fresh probe record
	// until one is delivered. The first caught probe proves the LATEST iterator is
	// live and catching NEW records — probes produced before the iterator was
	// acquired are simply missed (a disjoint set we assert nothing about).
	positioned := false
	probes := 0
	positionDeadline := time.Now().Add(positionBudget)
	for time.Now().Before(positionDeadline) {
		probe := fmt.Sprintf("sl-probe-%d", probes)
		probes++
		putRecordsToShard(ctx, t, client, stream, hashKey, []string{probe})
		if missing := coll.waitFor([]string{probe}, 2*time.Second); missing == nil {
			positioned = true
			break
		}
	}
	if !positioned {
		t.Fatalf("consumer never delivered a probe within %s; LATEST iterator not established", positionBudget)
	}
	t.Logf("positioned at LATEST after %d probe(s)", probes)

	// (2) Produce the "after" batch now that the iterator is confirmed live, and
	// wait for all of it. Self-checking: if LATEST never advanced, this times out.
	after := makePayloads("sl-after", afterN)
	putRecordsToShard(ctx, t, client, stream, hashKey, after)
	if missing := coll.waitFor(after, deliverBudget); len(missing) != 0 {
		t.Fatalf("StartLatest consumer did not deliver the post-start batch: missing %d/%d: %v", len(missing), afterN, missing)
	}

	// (3) LOAD-BEARING: none of the pre-existing backlog was delivered. By now the
	// consumer has streamed well past the LATEST start point (all "after" arrived),
	// so any backlog record was skipped, not merely late. Mutation (StartTrimHorizon)
	// would deliver the whole backlog here.
	var leaked []string
	for _, p := range backlog {
		if coll.count(p) != 0 {
			leaked = append(leaked, p)
		}
	}
	if len(leaked) != 0 {
		t.Fatalf("StartLatest delivered %d/%d pre-existing backlog records (want 0); LATEST must skip the backlog: %v", len(leaked), backlogN, leaked)
	}

	// (4) Completeness of the post-start batch: every "after" record delivered at
	// least once.
	for _, p := range after {
		if coll.count(p) == 0 {
			t.Fatalf("post-start record %q was never delivered", p)
		}
	}
	t.Logf("StartLatest ok: skipped all %d backlog records, delivered all %d post-start records (%d probes)", backlogN, afterN, probes)
}
