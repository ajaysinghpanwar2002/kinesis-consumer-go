package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestProcessShardRecordsLoopRunsPassAndReturnsOnCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var handled []string
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
		afterGetRecords: cancel,
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(handled) != 1 || handled[0] != "sequence-1" {
		t.Fatalf("handled records = %v, want [sequence-1]", handled)
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
}

func TestProcessShardRecordsLoopCarriesCheckpointCountBetweenPasses(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	getRecordsCalls := 0
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-2")}}},
		},
		afterGetRecords: func() {
			getRecordsCalls++
			if getRecordsCalls == 2 {
				cancel()
			}
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessShardRecordsLoopCarriesLatestNonEmptySequence(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	getRecordsCalls := 0
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
			{},
		},
		afterGetRecords: func() {
			getRecordsCalls++
			if getRecordsCalls == 2 {
				cancel()
			}
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestProcessShardRecordsLoopWrapsPassError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getShardIteratorErr: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	lastSeq, count, err := c.processShardRecordsLoop(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsLoop() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process shard records loop shard-1: process shard records pass shard-1: poll shard records pages shard-1: get shard iterator shard-1: boom" {
		t.Fatalf("processShardRecordsLoop() error = %v, want %q", err, "process shard records loop shard-1: process shard records pass shard-1: poll shard records pages shard-1: get shard iterator shard-1: boom")
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessShardRecordsLoopTreatsContextCancellationAsNormalShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if len(client.getShardIteratorCalls) != 0 {
		t.Fatalf("GetShardIterator calls = %d, want 0", len(client.getShardIteratorCalls))
	}
}
