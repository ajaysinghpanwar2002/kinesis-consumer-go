//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// streamARN returns the stream's ARN via DescribeStreamSummary, so a test can
// configure a consumer by ARN instead of by name.
func streamARN(ctx context.Context, t *testing.T, client *kinesis.Client, name string) string {
	t.Helper()
	out, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("describe stream summary for %s: %v", name, err)
	}
	if out.StreamDescriptionSummary == nil || aws.ToString(out.StreamDescriptionSummary.StreamARN) == "" {
		t.Fatalf("stream %s has no ARN in DescribeStreamSummary", name)
	}
	return aws.ToString(out.StreamDescriptionSummary.StreamARN)
}

// TestStreamARNConsumesAndResumes proves scenario #10 (IT-14): a consumer
// configured with ONLY Config.StreamARN (StreamName empty) works end-to-end —
// it discovers shards, gets iterators, consumes every record, checkpoints, and a
// fresh ARN-only consumer resumes past the checkpoint.
//
// StreamARN drives ListShards (list_shards.go:58) and GetShardIterator
// (get_shard_iterator.go:77) when set; GetRecords rides the iterator; and
// streamKey() (heartbeat.go:8) returns the ARN when StreamName is empty, so the
// ARN is also the checkpoint/lease coordination namespace. Producing is done by
// StreamName (test-side, independent of the consumer's identity config).
//
// The load-bearing fact is that ARN-only delivery works at all: if StreamARN were
// ignored, shard discovery / iterator acquisition would fail and nothing would be
// delivered (mutation: a valid-format but nonexistent ARN → no delivery). The
// resume half additionally proves the checkpoint is keyed consistently under the
// ARN.
func TestStreamARNConsumesAndResumes(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("streamarn")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}

	const (
		batch1N       = 30
		batch2N       = 20
		deliverBudget = 60 * time.Second
	)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != 1 {
		t.Fatalf("want exactly 1 shard hash key, got %d", len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	arn := streamARN(ctx, t, client, stream)
	t.Logf("consuming by ARN: %s", arn)

	batch1 := makePayloads("arn-b1", batch1N)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	store := newStore(t, uniqueName("streamarn-kp"))

	// C1: identified ONLY by StreamARN (StreamName empty).
	collC1 := newCollector()
	consC1, err := consumer.New(consumer.Config{
		StreamARN:     arn,
		StartPosition: consumer.StartTrimHorizon,
	}, client, store, collC1.handler(),
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create ARN-only consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consC1)

	// (1) LOAD-BEARING: ARN-only C1 delivers all of batch1. This only works if
	// StreamARN is honored by ListShards + GetShardIterator.
	if missing := collC1.waitFor(batch1, deliverBudget); len(missing) != 0 {
		stopC1()
		t.Fatalf("ARN-only C1 did not deliver all of batch1: missing %d/%d: %v", len(missing), batch1N, missing)
	}
	for _, p := range batch1 {
		if got := collC1.count(p); got != 1 {
			stopC1()
			t.Fatalf("C1 delivered %q %d times, want exactly 1", p, got)
		}
	}

	// Graceful stop: drain flush + lease release; checkpoint sits at batch1's tip
	// (keyed under streamKey() == the ARN).
	stopC1()

	// Produce batch2 after C1 stopped, so it is only ever seen by C2.
	batch2 := makePayloads("arn-b2", batch2N)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	// C2: a fresh ARN-only consumer on the SAME store.
	collC2 := newCollector()
	consC2, err := consumer.New(consumer.Config{
		StreamARN:     arn,
		StartPosition: consumer.StartTrimHorizon,
	}, client, store, collC2.handler(),
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create ARN-only consumer C2: %v", err)
	}
	_, stopC2 := runConsumer(t, consC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, deliverBudget); len(missing) != 0 {
		t.Fatalf("resumed ARN-only C2 did not deliver batch2: missing %d/%d: %v", len(missing), batch2N, missing)
	}

	// (2) LOAD-BEARING resume: C2 replays NONE of batch1 — the checkpoint written by
	// C1 under the ARN key was read back by C2 under the same ARN key. If the ARN
	// were not the consistent coordination namespace, C2 would restart from
	// TRIM_HORIZON and replay all of batch1.
	replay := 0
	for _, p := range batch1 {
		if collC2.count(p) > 0 {
			replay++
		}
	}
	if replay != 0 {
		t.Fatalf("C2 replayed %d/%d batch1 records; want 0 (checkpoint must be keyed consistently under the ARN)", replay, batch1N)
	}

	// (3) Completeness.
	for _, p := range batch1 {
		if collC1.count(p)+collC2.count(p) == 0 {
			t.Fatalf("batch1 record %q was never delivered", p)
		}
	}
	for _, p := range batch2 {
		if collC2.count(p) == 0 {
			t.Fatalf("batch2 record %q was never delivered", p)
		}
	}
	t.Logf("ARN path ok: C1 delivered all %d batch1 records by ARN, C2 resumed by ARN with 0 replay and delivered all %d batch2 records", batch1N, batch2N)
}
