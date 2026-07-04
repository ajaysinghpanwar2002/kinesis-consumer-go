//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// TestLiveConsumerDiscoversChildrenOnReshard proves scenario #11 (IT-16): a
// SINGLE consumer that is running continuously across a SplitShard discovers the
// newly-created child shards on its periodic shardSync tick and processes their
// records WITHOUT any restart, once the closed parent reaches SHARD_END.
//
// This is the live-refresh complement to IT-15, which proved only the
// restart/fresh-consumer path. The load-bearing distinction is that the SAME
// live consumer instance that read the parent goes on to read the children.
//
// Why child delivery here is inherently proof of live refresh: the library's
// only child-discovery path is refreshKnownShards (shard_map.go), called solely
// from refreshAndStartReadyShardWorkers on the shardSync tick. mergeKnownShards
// has exactly two callers — the initial listShards at Start and that refresh —
// and the GetRecords ChildShards field is never read for discovery. Because the
// consumer is started BEFORE the split, its initial shard list contains only the
// parent, so any child record it later delivers MUST have arrived through a
// shardSync refresh tick.
//
// Assertion order follows the IT-15->IT-16 delta: the parent is held OPEN->CLOSED
// by the running consumer (a lifecycle IT-15 never exercised, since IT-15 started
// its consumer after the split). SHARD_END is asserted FIRST, right after the
// parent is confirmed closed and before child records are produced, so that a
// hang bisects cleanly between "parent never completed across the close" and
// "children never discovered".
func TestLiveConsumerDiscoversChildrenOnReshard(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("liveresharddiscovery")
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

	parentPayloads := makePayloads("livereshard-parent", 12)
	putRecordsToShard(ctx, t, client, stream, parentHashKey, parentPayloads)

	// One consumer, started BEFORE the split. Its initial shard listing sees only
	// the parent; children can reach it only via a later shardSync refresh tick.
	store := newStore(t, uniqueName("livereshard-kp"))
	t.Cleanup(func() { _ = store.Close() })

	coll := newCollector()
	cons := newConsumer(t, stream, client, store, coll.handler())
	_, stop := runConsumer(t, cons)
	defer stop()

	// (1) Baseline: the live consumer delivers the parent records before the split.
	if missing := coll.waitFor(parentPayloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("live consumer did not deliver parent records before split; missing %d/%d: %v", len(missing), len(parentPayloads), missing)
	}

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

	// (3) LOAD-BEARING (asserted first, before child records exist): the running
	// consumer, holding the parent lease across OPEN->CLOSED, reads the parent to
	// exhaustion and persists a completion checkpoint. Isolating this before child
	// delivery means a failure here cannot be confused with a child-discovery
	// failure.
	//
	// The completion value is the bare "SHARD_END" here, NOT "SHARD_END:<seq>".
	// That is the meaningful IT-15->IT-16 distinction: IT-15's fresh consumer read
	// the parent AFTER the close, so the pass that ended the shard carried records
	// and recorded a last sequence ("SHARD_END:<seq>"). This live consumer read all
	// parent records BEFORE the split, so the pass that detects the close reads an
	// EMPTY final page (lastSeq == "") and saveShardCompletionCheckpoint writes the
	// bare marker (shardCompletionValue("") == "SHARD_END", shard_completion.go).
	// Both forms satisfy isShardCompletedCheckpoint (HasPrefix "SHARD_END") and gate
	// the children identically, so the load-bearing invariant is the prefix, not the
	// suffix.
	parentCheckpoint := waitForShardEndCheckpoint(ctx, t, store, stream, parentID, 60*time.Second)
	if !strings.HasPrefix(parentCheckpoint, "SHARD_END") {
		t.Fatalf("parent checkpoint = %q, want a SHARD_END completion marker", parentCheckpoint)
	}

	// Produce records pinned to each child only after the parent has completed, so
	// child delivery is unambiguously the result of live discovery + readiness.
	childPayloadsByShard := make(map[string][]string, len(children))
	var childPayloads []string
	for _, child := range children {
		childID := aws.ToString(child.ShardId)
		payloads := makePayloads("livereshard-child-"+childID, 6)
		childPayloadsByShard[childID] = payloads
		childPayloads = append(childPayloads, payloads...)
		putRecordsToShard(ctx, t, client, stream, aws.ToString(child.HashKeyRange.StartingHashKey), payloads)
	}

	// (2) LOAD-BEARING: the SAME still-running consumer delivers all child records.
	// No restart and no second consumer — child delivery here proves the shardSync
	// refresh discovered the children and started workers for them live.
	if missing := coll.waitFor(childPayloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("live consumer did not deliver child records after reshard; missing %d/%d: %v", len(missing), len(childPayloads), missing)
	}

	// (4) Exactly-once for the union. A duplicate parent record would signal a
	// re-read at the OPEN->CLOSED boundary; a duplicate child would signal a
	// double worker start after discovery.
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

	t.Logf("live reshard discovery ok: parent=%s checkpoint=%s children=%v childPayloads=%v",
		parentID,
		parentCheckpoint,
		shardIDs(children),
		childPayloadsByShard,
	)
}
