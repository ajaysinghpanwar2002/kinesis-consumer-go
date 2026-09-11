//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

func TestInFlightLimitsSplitBatchesAcrossShardsAndResume(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()
	stream := uniqueName("in-flight")
	createStream(ctx, t, client, stream, 2)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)
	hashes := shardHashKeys(ctx, t, client, stream)
	var all []string
	for shard, hash := range hashes {
		payloads := makePayloads(shard, 12)
		putRecordsToShard(ctx, t, client, stream, hash, payloads)
		all = append(all, payloads...)
	}
	store := newStore(t, uniqueName("in-flight-kp"))
	coll := newBatchCollector()
	var mu sync.Mutex
	activeRecords, activeBytes := 0, 0
	handler := func(ctx context.Context, records []consumer.Record) error {
		bytes := 0
		for _, r := range records {
			bytes += len(r.Data)
		}
		mu.Lock()
		activeRecords += len(records)
		activeBytes += bytes
		if activeRecords > 3 || activeBytes > 512 || len(records) > 2 || bytes > 256 {
			t.Errorf("limits exceeded: active=%d/%d batch=%d/%d", activeRecords, activeBytes, len(records), bytes)
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		err := coll.handler()(ctx, records)
		mu.Lock()
		activeRecords -= len(records)
		activeBytes -= bytes
		mu.Unlock()
		return err
	}
	cons, err := consumer.New(consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon}, client, store, nil,
		consumer.WithBatchHandler(handler), consumer.WithBatching(10, 1), consumer.WithPolling(200*time.Millisecond, time.Second), consumer.WithGracefulDrain(10*time.Second),
		consumer.WithInFlightLimits(consumer.InFlightLimits{MaxRecordsPerShard: 2, MaxRecordsPerInstance: 3, MaxBytesPerShard: 256, MaxBytesPerInstance: 512, MaxFetchSlots: 1}))
	if err != nil {
		t.Fatal(err)
	}
	_, stop := runConsumer(t, cons)
	if missing := coll.waitFor(all, 60*time.Second); len(missing) > 0 {
		stop()
		t.Fatalf("missing %v", missing)
	}
	stop()
	for _, p := range all {
		if coll.count(p) != 1 {
			t.Errorf("payload %s delivered %d times", p, coll.count(p))
		}
	}
	// A successor traverses the fenced anchor verification path before consuming
	// new data, with the same one-slot budget as normal reads.
	next := makePayloads("next", 3)
	for _, hash := range hashes {
		putRecordsToShard(ctx, t, client, stream, hash, next)
		break
	}
	resumed := newBatchCollector()
	successor, err := consumer.New(consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup}, client, store, nil,
		consumer.WithBatchHandler(resumed.handler()), consumer.WithBatching(10, 1), consumer.WithPolling(200*time.Millisecond, time.Second), consumer.WithGracefulDrain(10*time.Second),
		consumer.WithInFlightLimits(consumer.InFlightLimits{MaxRecordsPerShard: 1, MaxFetchSlots: 1}))
	if err != nil {
		t.Fatal(err)
	}
	_, stopSuccessor := runConsumer(t, successor)
	if missing := resumed.waitFor(next, 60*time.Second); len(missing) > 0 {
		stopSuccessor()
		t.Fatalf("successor missing %v", missing)
	}
	stopSuccessor()
	if n := resumed.replayCount(all); n != 0 {
		t.Fatalf("replayed %d checkpointed records", n)
	}
}
