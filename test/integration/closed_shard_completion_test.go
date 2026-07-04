//go:build integration

package integration

import (
	"context"
	"math/big"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// TestClosedShardCompletionCheckpointSkipsParentOnRestart proves scenario #13
// (IT-15): when Kinesis closes a shard after resharding, the consumer reads the
// parent to exhaustion, persists a SHARD_END:<seq> completion checkpoint, and a
// fresh consumer on the same store skips that completed parent while processing
// the child shards.
//
// The LocalStack feasibility probe for this slice showed SplitShard is reliable:
// ListShards reports the closed parent plus two children, and raw GetRecords on
// the parent eventually returns an empty NextShardIterator. This test exercises
// the consumer path on top of that signal.
func TestClosedShardCompletionCheckpointSkipsParentOnRestart(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("closedshard")
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

	parentPayloads := makePayloads("closed-parent", 12)
	putRecordsToShard(ctx, t, client, stream, parentHashKey, parentPayloads)

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

	store := newStore(t, uniqueName("closedshard-kp"))
	t.Cleanup(func() { _ = store.Close() })

	// C1 starts after the split. Ready-shard filtering should let it process the
	// closed parent while holding the children back until the parent has SHARD_END.
	collA := newCollector()
	consumerA := newConsumer(t, stream, client, store, collA.handler())
	_, stopA := runConsumer(t, consumerA)
	defer stopA()
	if missing := collA.waitFor(parentPayloads, 60*time.Second); len(missing) != 0 {
		stopA()
		t.Fatalf("consumer A did not deliver all parent records before shard completion; missing %d/%d: %v", len(missing), len(parentPayloads), missing)
	}

	parentCheckpoint := waitForShardEndCheckpoint(ctx, t, store, stream, parentID, 60*time.Second)
	if !strings.HasPrefix(parentCheckpoint, "SHARD_END:") {
		stopA()
		t.Fatalf("parent checkpoint = %q, want SHARD_END:<last-sequence>", parentCheckpoint)
	}
	stopA()

	// Write only to the children after C1 is stopped. Child delivery by C2 is the
	// positive barrier that makes the no-parent-replay assertion final.
	childPayloadsByShard := make(map[string][]string, len(children))
	var childPayloads []string
	for _, child := range children {
		childID := aws.ToString(child.ShardId)
		payloads := makePayloads("closed-child-"+childID, 6)
		childPayloadsByShard[childID] = payloads
		childPayloads = append(childPayloads, payloads...)
		putRecordsToShard(ctx, t, client, stream, aws.ToString(child.HashKeyRange.StartingHashKey), payloads)
	}

	collB := newCollector()
	consumerB := newConsumer(t, stream, client, store, collB.handler())
	_, stopB := runConsumer(t, consumerB)
	defer stopB()

	if missing := collB.waitFor(childPayloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("consumer B did not deliver all child records after parent completion; missing %d/%d: %v", len(missing), len(childPayloads), missing)
	}

	replayedParent := replayCount(collB, parentPayloads)
	if replayedParent != 0 {
		t.Fatalf("consumer B replayed %d/%d parent records even though parent checkpoint is %q", replayedParent, len(parentPayloads), parentCheckpoint)
	}
	for _, p := range childPayloads {
		if collB.count(p) == 0 {
			t.Fatalf("child record %q was never delivered", p)
		}
	}

	t.Logf("closed shard completion ok: parent=%s checkpoint=%s children=%v childPayloads=%v",
		parentID,
		parentCheckpoint,
		shardIDs(children),
		childPayloadsByShard,
	)
}

func closedShardListShards(ctx context.Context, t *testing.T, client *kinesis.Client, stream string) []types.Shard {
	t.Helper()

	out, err := client.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String(stream)})
	if err != nil {
		t.Fatalf("list shards for %s: %v", stream, err)
	}
	return out.Shards
}

func waitForClosedParentWithChildren(ctx context.Context, t *testing.T, client *kinesis.Client, stream, parentID string, timeout time.Duration) []types.Shard {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last []types.Shard
	for time.Now().Before(deadline) {
		shards := closedShardListShards(ctx, t, client, stream)
		last = shards

		var foundParent bool
		var parentClosed bool
		for _, shard := range shards {
			if aws.ToString(shard.ShardId) != parentID {
				continue
			}
			foundParent = true
			parentClosed = shard.SequenceNumberRange != nil && aws.ToString(shard.SequenceNumberRange.EndingSequenceNumber) != ""
			break
		}
		if foundParent && parentClosed && len(childShardsForParent(shards, parentID)) >= 2 {
			return shards
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("stream %s did not list closed parent %s with children within %s; last shards=%v", stream, parentID, timeout, closedShardSummary(last))
	return nil
}

func childShardsForParent(shards []types.Shard, parentID string) []types.Shard {
	children := make([]types.Shard, 0, 2)
	for _, shard := range shards {
		if aws.ToString(shard.ParentShardId) == parentID || aws.ToString(shard.AdjacentParentShardId) == parentID {
			children = append(children, shard)
		}
	}
	sort.Slice(children, func(i, j int) bool {
		return aws.ToString(children[i].ShardId) < aws.ToString(children[j].ShardId)
	})
	return children
}

func waitForShardEndCheckpoint(ctx context.Context, t *testing.T, store interface {
	Get(context.Context, string, string) (string, error)
}, stream, shardID string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last string
	var lastErr error
	for time.Now().Before(deadline) {
		value, err := store.Get(ctx, stream, shardID)
		if err != nil {
			lastErr = err
		} else {
			last = value
			if strings.HasPrefix(value, "SHARD_END") {
				return value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("checkpoint for shard %s did not become SHARD_END within %s; last=%q lastErr=%v", shardID, timeout, last, lastErr)
	return ""
}

func midpointHashKey(t *testing.T, hashRange *types.HashKeyRange) string {
	t.Helper()

	if hashRange == nil {
		t.Fatal("shard has nil HashKeyRange")
	}
	start, ok := new(big.Int).SetString(aws.ToString(hashRange.StartingHashKey), 10)
	if !ok {
		t.Fatalf("parse starting hash key %q", aws.ToString(hashRange.StartingHashKey))
	}
	end, ok := new(big.Int).SetString(aws.ToString(hashRange.EndingHashKey), 10)
	if !ok {
		t.Fatalf("parse ending hash key %q", aws.ToString(hashRange.EndingHashKey))
	}
	mid := new(big.Int).Add(start, end)
	mid.Div(mid, big.NewInt(2))
	if mid.Cmp(start) <= 0 || mid.Cmp(end) >= 0 {
		t.Fatalf("computed split key %s is not inside hash range [%s, %s]", mid.String(), start.String(), end.String())
	}
	return mid.String()
}

func shardIDs(shards []types.Shard) []string {
	ids := make([]string, 0, len(shards))
	for _, shard := range shards {
		ids = append(ids, aws.ToString(shard.ShardId))
	}
	sort.Strings(ids)
	return ids
}

func closedShardSummary(shards []types.Shard) []string {
	out := make([]string, 0, len(shards))
	for _, shard := range shards {
		id := aws.ToString(shard.ShardId)
		start := ""
		end := ""
		if shard.SequenceNumberRange != nil {
			start = aws.ToString(shard.SequenceNumberRange.StartingSequenceNumber)
			end = aws.ToString(shard.SequenceNumberRange.EndingSequenceNumber)
		}
		out = append(out, id+" parent="+aws.ToString(shard.ParentShardId)+" adjacent="+aws.ToString(shard.AdjacentParentShardId)+" seqStart="+start+" seqEnd="+end)
	}
	sort.Strings(out)
	return out
}
