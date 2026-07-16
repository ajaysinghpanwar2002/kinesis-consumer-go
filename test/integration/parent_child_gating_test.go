//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// TestParentChildGatingHoldsChildrenUntilParentCompletes proves scenario #12
// (IT-17): a child shard that is ALREADY DISCOVERED and has records waiting is
// NOT processed while its parent is present-and-incomplete, and IS processed once
// the parent reaches SHARD_END. This is the readiness gate
// (readyShardIDs/parentsReady, shard_readiness.go) that IT-16 (discovery) did not
// stress.
//
// Two design choices make the negative assertion load-bearing for GATING rather
// than for discovery or timing:
//
//  1. Split BEFORE starting the consumer, so both children are in the consumer's
//     INITIAL listShards (consumer.go:53-62) from t=0. "No child delivered" then
//     cannot be explained by "child not yet discovered" — the child is known from
//     the first readiness pass and is held back solely by the parent-incomplete
//     gate. (IT-16 deliberately did the opposite — start-before-split — to isolate
//     discovery; IT-17 inverts it to isolate gating.)
//
//  2. A blocking parent handler: it blocks on parent-tagged records until the test
//     releases it (and returns on ctx.Done as a shutdown safety net, recording
//     nothing). While blocked, the parent worker is stuck on its first record, so
//     the parent never checkpoints and never reaches SHARD_END, so the children can
//     NEVER become ready. The negative window is therefore deterministic: children
//     are gated because the parent is provably incomplete, not because the test
//     looked too early.
func TestParentChildGatingHoldsChildrenUntilParentCompletes(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("parentchildgating")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	initial := closedShardListShards(ctx, t, client, stream)
	if len(initial) != 1 {
		t.Fatalf("want exactly 1 initial shard, got %d: %v", len(initial), closedShardSummary(initial))
	}
	parent := initial[0]
	parentID := aws.ToString(parent.ShardId)
	parentHashKey := aws.ToString(parent.HashKeyRange.StartingHashKey)
	splitKey := midpointHashKey(t, parent.HashKeyRange)

	const parentTag = "gating-parent"
	parentPayloads := makePayloads(parentTag, 12)
	putRecordsToShard(ctx, t, client, stream, parentHashKey, parentPayloads)

	// Split BEFORE the consumer starts, so the children are in its initial shard
	// list and are gated from the first readiness pass — not merely undiscovered.
	if _, err := client.SplitShard(ctx, &kinesis.SplitShardInput{
		StreamName:         aws.String(stream),
		ShardToSplit:       aws.String(parentID),
		NewStartingHashKey: aws.String(splitKey),
	}); err != nil {
		t.Fatalf("split parent shard %s at %s: %v", parentID, splitKey, err)
	}
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	shards := waitForClosedParentWithChildren(ctx, t, client, stream, parentID, 90*time.Second)
	children := childShardsForParent(shards, parentID)
	if len(children) != 2 {
		t.Fatalf("want 2 child shards for parent %s, got %d: %v", parentID, len(children), closedShardSummary(shards))
	}

	// Produce the child records up front so they are waiting to be read the moment
	// the gate would open. Their absence during the negative window is thus caused
	// by the gate, not by an empty child shard.
	childPayloadsByShard := make(map[string][]string, len(children))
	var childPayloads []string
	for _, child := range children {
		childID := aws.ToString(child.ShardId)
		payloads := makePayloads("gating-child-"+childID, 6)
		childPayloadsByShard[childID] = payloads
		childPayloads = append(childPayloads, payloads...)
		putRecordsToShard(ctx, t, client, stream, aws.ToString(child.HashKeyRange.StartingHashKey), payloads)
	}

	store := newStore(t, uniqueName("gating-kp"))
	t.Cleanup(func() { _ = store.Close() })

	// A blocking parent handler: parent records block on release; child records are
	// recorded immediately. On ctx.Done (shutdown) the handler returns without
	// erroring the shard so runConsumer's stop does not hang.
	release := make(chan struct{})
	coll := newCollector()
	base := coll.handler()
	handler := func(hctx context.Context, rec consumer.Record) error {
		if strings.HasPrefix(string(rec.Data), parentTag) {
			select {
			case <-release:
			case <-hctx.Done():
				return nil
			}
		}
		return base(hctx, rec)
	}

	cons := newConsumer(t, stream, client, store, handler)
	_, stop := runConsumer(t, cons)
	defer stop()
	// Ensure a blocked parent handler is released even if an assertion fails, so
	// the deferred stop can drain instead of timing out.
	var released bool
	defer func() {
		if !released {
			close(release)
		}
	}()

	// NEGATIVE WINDOW: with the parent blocked, let several shardSync ticks pass and
	// confirm the gate holds.
	time.Sleep(5 * time.Second)

	// (a) LOAD-BEARING negative: no child record delivered while the parent is
	// present-and-incomplete, even though the children are discovered (initial list)
	// and have records waiting. Checked first so the pre-seed-parent-completion
	// mutation (which makes children ready) trips this load-bearing assertion
	// directly.
	for _, p := range childPayloads {
		if got := coll.count(p); got != 0 {
			t.Fatalf("child record %q delivered %d times while parent %s was incomplete; readiness gate did not hold", p, got, parentID)
		}
	}

	// (b) Guard: the parent is provably incomplete (no SHARD_END checkpoint yet), so
	// (a) is not vacuously true — the children were gated by a real incomplete
	// parent, not by a race or a prematurely completed parent.
	if seq, err := store.Get(ctx, integrationCoordinationIdentity(stream), parentID); err != nil {
		t.Fatalf("read parent checkpoint during gate window: %v", err)
	} else if strings.HasPrefix(seq, "SHARD_END") {
		t.Fatalf("parent %s already SHARD_END (%q) during the gate window; blocking handler failed to hold it incomplete", parentID, seq)
	}

	// Open the gate: the parent can now drain and complete, which should let the
	// children become ready.
	released = true
	close(release)

	// (c) parent records all delivered; (e) LOAD-BEARING positive: children are
	// delivered once the gate opens.
	if missing := coll.waitFor(parentPayloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("parent records not all delivered after release; missing %d/%d: %v", len(missing), len(parentPayloads), missing)
	}
	// (d) parent reaches SHARD_END.
	parentCheckpoint := waitForShardEndCheckpoint(ctx, t, store, stream, parentID, 60*time.Second)
	if !strings.HasPrefix(parentCheckpoint, "SHARD_END") {
		t.Fatalf("parent checkpoint = %q, want a SHARD_END completion marker", parentCheckpoint)
	}
	if missing := coll.waitFor(childPayloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("child records not delivered after parent completion; missing %d/%d: %v", len(missing), len(childPayloads), missing)
	}

	// (f) Exactly-once for the union.
	for _, p := range parentPayloads {
		if got := coll.count(p); got != 1 {
			t.Fatalf("parent record %q delivered %d times, want exactly 1", p, got)
		}
	}
	for _, p := range childPayloads {
		if got := coll.count(p); got != 1 {
			t.Fatalf("child record %q delivered %d times, want exactly 1", p, got)
		}
	}

	t.Logf("parent/child gating ok: parent=%s checkpoint=%s children=%v childPayloads=%v",
		parentID,
		parentCheckpoint,
		shardIDs(children),
		childPayloadsByShard,
	)
}
