package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestMemoryBackendsRunConsumerSmoke(t *testing.T) {
	store := checkpoint.NewMemoryStore()
	leaseManager := lease.NewMemoryManager()
	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
		},
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records: []types.Record{
					{
						SequenceNumber: aws.String("sequence-1"),
						PartitionKey:   aws.String("partition-1"),
						Data:           []byte("payload-1"),
					},
				},
			},
		},
	}

	processed := make(chan types.Record, 1)
	opt, err := applyOptions([]Option{
		WithLeaseManager(leaseManager),
		WithHeartbeat(10*time.Millisecond, 2*time.Second),
		WithBatching(10, 1),
		WithPolling(time.Millisecond, time.Second),
		WithGracefulDrain(time.Second),
	})
	if err != nil {
		t.Fatalf("applyOptions: %v", err)
	}

	c := &Consumer{
		cfg:           Config{StreamName: "stream"},
		client:        client,
		store:         store,
		handler:       func(_ context.Context, record Record) error { processed <- record; return nil },
		failurePolicy: opt.failurePolicy,
		leaseManager:  opt.lease.manager,
		leaseOwner:    "owner",
		gracefulDrain: opt.shutdown.gracefulDrain,
		drainTimeout:  opt.shutdown.gracefulDrainTimeout,
		tuning:        opt.tuning,
		logger:        opt.logger,
		reporter:      opt.reporter,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)
	startDone := false
	t.Cleanup(func() {
		cancel()
		if startDone {
			return
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Start() cleanup error = %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Error("timed out waiting for Start cleanup")
		}
	})

	record := waitProcessedRecord(t, processed)
	if got := aws.ToString(record.SequenceNumber); got != "sequence-1" {
		t.Fatalf("processed sequence = %q, want sequence-1", got)
	}

	waitForNoLiveLease(t, leaseManager, "stream", "shard-1")
	cancel()
	waitStartDone(t, done, nil)
	startDone = true

	checkpointValue, err := store.Get(context.Background(), "stream", "shard-1")
	if err != nil {
		t.Fatalf("checkpoint Get: %v", err)
	}
	if checkpointValue != "SHARD_END:sequence-1" {
		t.Fatalf("checkpoint = %q, want SHARD_END:sequence-1", checkpointValue)
	}

	owners, err := leaseManager.Workers(context.Background(), "stream")
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if len(owners) != 1 || owners[0] != "owner" {
		t.Fatalf("Workers = %v, want [owner]", owners)
	}

	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
}

func waitProcessedRecord(t *testing.T, processed <-chan types.Record) types.Record {
	t.Helper()

	select {
	case record := <-processed:
		return record
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for processed record")
		return types.Record{}
	}
}

func waitForNoLiveLease(t *testing.T, manager lease.Manager, streamName, shardID string) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		owners, err := manager.List(context.Background(), streamName)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if _, ok := owners[shardID]; !ok {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for lease %s to be released; live leases: %v", shardID, owners)
		case <-ticker.C:
		}
	}
}
