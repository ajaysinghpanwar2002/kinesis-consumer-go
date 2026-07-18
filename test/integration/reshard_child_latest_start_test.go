//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// newLatestConsumer mirrors newConsumer but starts from StartLatest — the
// library default and the position under test here — and accepts any
// consumer.KinesisAPI so tests can wrap the client. Every other harness
// constructor uses StartTrimHorizon, which is exactly why the child-start
// data-loss window this test covers was invisible to the rest of the suite.
func newLatestConsumer(t *testing.T, stream string, client consumer.KinesisAPI, store *valkeycheckpoint.Store, handler consumer.HandlerFunc) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartLatest,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create latest consumer for %s: %v", stream, err)
	}
	return cons
}

// discoveryGatingClient wraps the real Kinesis client and, until opened,
// filters every ListShards response down to the single pre-split parent
// shard. The library's only child-discovery path is the shard listing
// (mergeKnownShards via the initial listing and the shard-sync refresh), so
// while the gate is closed the consumer provably cannot know the children
// exist, cannot start workers for them, and cannot anchor their iterators —
// pinning the split->pickup window open for as long as the test needs.
// GetRecords and GetShardIterator pass through untouched, so the parent
// worker still drains to SHARD_END and completes normally behind the gate.
type discoveryGatingClient struct {
	*kinesis.Client
	parentID string
	open     atomic.Bool
}

func (g *discoveryGatingClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	out, err := g.Client.ListShards(ctx, params, optFns...)
	if err != nil || out == nil || g.open.Load() {
		return out, err
	}
	filtered := make([]types.Shard, 0, 1)
	for _, shard := range out.Shards {
		if aws.ToString(shard.ShardId) == g.parentID {
			filtered = append(filtered, shard)
		}
	}
	out.Shards = filtered
	return out, nil
}

// TestResharedChildrenDeliverWindowRecordsWithStartLatest proves the Slice 1
// fix: with the default StartLatest position, records produced into a child
// shard BETWEEN the split and the moment the consumer picks the child up must
// still be delivered. Before the fix, a checkpoint-less child fell through to
// cfg.StartPosition, anchored at the child's tip at worker-start time, and
// every record written in that window (split -> parent drain -> SHARD_END
// gate -> shard-sync tick -> acquisition; routinely 10-60s live) was silently
// lost and checkpointed past. The fix anchors any checkpoint-less shard with
// a known parent at TRIM_HORIZON, so the child resumes exactly where its
// parents left off.
//
// The window here is DETERMINISTIC, not timing-based: a discoveryGatingClient
// hides the children from every shard listing the consumer sees until all
// window records are produced AND verified undelivered, so this test fails
// against the old LATEST behavior no matter how fast discovery runs. The
// existing TestLiveConsumerDiscoversChildrenOnReshard cannot catch the bug at
// all: it runs on StartTrimHorizon, where a child anchoring at StartPosition
// is harmless by construction.
func TestResharedChildrenDeliverWindowRecordsWithStartLatest(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("reshardlatest")
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

	store := newStore(t, uniqueName("reshardlatest-kp"))
	t.Cleanup(func() { _ = store.Close() })

	gate := &discoveryGatingClient{Client: client, parentID: parentID}
	coll := newCollector()
	cons := newLatestConsumer(t, stream, gate, store, coll.handler())
	_, stop := runConsumer(t, cons)
	defer stop()

	// With StartLatest the parent worker anchors at the shard tip at some
	// unknown instant after Start. Probe until a record produced NOW is
	// delivered — from then on the consumer is provably reading the parent
	// live, so the split below happens mid-run.
	anchored := false
	var probes []string
	for i := 0; i < 60 && !anchored; i++ {
		probe := fmt.Sprintf("reshardlatest-probe-%d", i)
		probes = append(probes, probe)
		putRecordsToShard(ctx, t, client, stream, parentHashKey, []string{probe})
		anchored = len(coll.waitFor([]string{probe}, time.Second)) == 0
	}
	if !anchored {
		t.Fatalf("consumer never delivered a live probe record from the parent; produced %d probes", len(probes))
	}

	if _, err := client.SplitShard(ctx, &kinesis.SplitShardInput{
		StreamName:         aws.String(stream),
		ShardToSplit:       aws.String(parentID),
		NewStartingHashKey: aws.String(splitKey),
	}); err != nil {
		t.Fatalf("split parent shard %s at %s: %v", parentID, splitKey, err)
	}
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	// The children exist on the stream (seen through the UNGATED client), but
	// the consumer's listings still show only the parent.
	shards := waitForClosedParentWithChildren(ctx, t, client, stream, parentID, 90*time.Second)
	children := childShardsForParent(shards, parentID)
	if len(children) != 2 {
		t.Fatalf("want 2 child shards for parent %s, got %d: %v", parentID, len(children), closedShardSummary(shards))
	}

	// Window batch 1: produced while the parent may not even have completed.
	var windowPayloads []string
	produceToChildren := func(tag string) {
		for _, child := range children {
			childID := aws.ToString(child.ShardId)
			payloads := makePayloads("reshardlatest-"+tag+"-"+childID, 6)
			windowPayloads = append(windowPayloads, payloads...)
			putRecordsToShard(ctx, t, client, stream, aws.ToString(child.HashKeyRange.StartingHashKey), payloads)
		}
	}
	produceToChildren("window1")

	// The parent drains to SHARD_END behind the gate (GetRecords is untouched)
	// and completes, so once the gate opens the children are immediately
	// eligible — the remaining pickup delay is discovery alone.
	parentCheckpoint := waitForShardEndCheckpoint(ctx, t, store, stream, parentID, 60*time.Second)
	if !strings.HasPrefix(parentCheckpoint, "SHARD_END") {
		t.Fatalf("parent checkpoint = %q, want a SHARD_END completion marker", parentCheckpoint)
	}

	// Window batch 2: parent completed, children still invisible — the tail
	// end of the window.
	produceToChildren("window2")

	// LOAD-BEARING PRECONDITION: every window record was produced while the
	// consumer provably could not have started a child worker (the children
	// have never appeared in any listing it received), so nothing can have
	// been delivered yet. This is what makes the test deterministic: the
	// records verifiably precede child pickup.
	if delivered := len(windowPayloads) - len(coll.missing(windowPayloads)); delivered != 0 {
		t.Fatalf("%d window records delivered while child discovery was gated; the window was not held open", delivered)
	}

	gate.open.Store(true)

	// LOAD-BEARING: after discovery is released, every record produced into
	// the window is delivered. With the pre-fix behavior the child workers
	// anchor LATEST at pickup time — strictly after every window record was
	// written — so none of these payloads could ever arrive.
	if missing := coll.waitFor(windowPayloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("records produced between split and child pickup were lost with StartLatest; missing %d/%d: %v",
			len(missing), len(windowPayloads), missing)
	}

	// In this controlled sequence (single consumer, no failover) the window
	// records should also arrive exactly once, matching the suite's other
	// reshard scenarios; a duplicate would signal a double worker start.
	for _, p := range windowPayloads {
		if got := coll.count(p); got != 1 {
			t.Fatalf("window record %q delivered %d times, want exactly 1", p, got)
		}
	}

	t.Logf("reshard StartLatest window ok: parent=%s checkpoint=%s children=%v probes=%d windowRecords=%d",
		parentID, parentCheckpoint, shardIDs(children), len(probes), len(windowPayloads))
}
