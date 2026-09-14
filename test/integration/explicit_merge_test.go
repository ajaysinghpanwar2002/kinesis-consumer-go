//go:build integration

package integration

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// One completed parent is insufficient: the merged child remains blocked until
// the other parent's asynchronous acknowledgment produces a completion marker.
func TestExplicitMergeWaitsForBothPersistedParents(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()
	stream := uniqueName("explicit-merge")
	createStream(ctx, t, client, stream, 2)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)
	parents := closedShardListShards(ctx, t, client, stream)
	if len(parents) != 2 {
		t.Fatalf("parents=%d", len(parents))
	}
	sort.Slice(parents, func(i, j int) bool { return aws.ToString(parents[i].ShardId) < aws.ToString(parents[j].ShardId) })
	parentIDs := []string{aws.ToString(parents[0].ShardId), aws.ToString(parents[1].ShardId)}
	for i, p := range parents {
		putRecordsToShard(ctx, t, client, stream, aws.ToString(p.HashKeyRange.StartingHashKey), []string{"parent-" + parentIDs[i]})
	}
	if _, err := client.MergeShards(ctx, &kinesis.MergeShardsInput{StreamName: aws.String(stream), ShardToMerge: parents[0].ShardId, AdjacentShardToMerge: parents[1].ShardId}); err != nil {
		t.Fatal(err)
	}
	waitStreamActive(ctx, t, client, stream, 60*time.Second)
	shards := closedShardListShards(ctx, t, client, stream)
	var childID, childHash string
	for _, s := range shards {
		if s.ParentShardId != nil && s.AdjacentParentShardId != nil {
			childID = aws.ToString(s.ShardId)
			childHash = aws.ToString(s.HashKeyRange.StartingHashKey)
		}
	}
	if childID == "" {
		t.Fatal("merged child not listed with both parents")
	}
	payloads := makePayloads("merged-child", 3)
	putRecordsToShard(ctx, t, client, stream, childHash, payloads)
	store := newStore(t, uniqueName("merge-state"))
	defer store.Close()
	pending := make(chan consumer.Delivery, 1)
	coll := newCollector()
	record := coll.handler()
	c, err := consumer.New(consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon}, client, store, nil,
		consumer.WithExplicitHandler(func(ctx context.Context, d consumer.Delivery) error {
			if err := record(ctx, d.Record); err != nil {
				return err
			}
			if d.ShardID == parentIDs[0] {
				select {
				case pending <- d:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return d.Ack(ctx)
		}), consumer.WithBatching(10, 1), consumer.WithCheckpointInterval(10*time.Millisecond), consumer.WithPolling(100*time.Millisecond, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, stop := runConsumer(t, c)
	defer stop()
	var delayed consumer.Delivery
	select {
	case delayed = <-pending:
	case <-time.After(30 * time.Second):
		t.Fatal("delayed parent not delivered")
	}
	waitForShardEndCheckpoint(ctx, t, store, stream, parentIDs[1], 30*time.Second)
	// The child was in the initial listing; exercise several readiness passes
	// with one persisted parent marker and one successfully returned callback.
	time.Sleep(2200 * time.Millisecond)
	for _, p := range payloads {
		if coll.count(p) != 0 {
			t.Fatalf("child delivered before both parents completed: %s", p)
		}
	}
	if seq, err := store.Get(ctx, integrationCoordinationIdentity(stream), parentIDs[0]); err != nil || strings.HasPrefix(seq, "SHARD_END") {
		t.Fatalf("delayed parent checkpoint=%q, %v", seq, err)
	}
	if err := delayed.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	waitForShardEndCheckpoint(ctx, t, store, stream, parentIDs[0], 30*time.Second)
	if missing := coll.waitFor(payloads, 30*time.Second); len(missing) != 0 {
		t.Fatalf("merged child missing %v", missing)
	}
}
