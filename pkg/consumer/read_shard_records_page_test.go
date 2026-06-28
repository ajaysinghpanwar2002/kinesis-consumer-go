package consumer

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestReadShardRecordsPageGetsIteratorThenRecords(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOut: &kinesis.GetRecordsOutput{
			Records: []types.Record{
				{
					Data:           []byte("payload"),
					PartitionKey:   aws.String("partition-1"),
					SequenceNumber: aws.String("sequence-1"),
				},
			},
			NextShardIterator: aws.String("iterator-2"),
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	out, err := c.readShardRecordsPage(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("readShardRecordsPage() error = %v, want nil", err)
	}
	if !slices.Equal(client.callOrder, []string{"GetShardIterator", "GetRecords"}) {
		t.Fatalf("call order = %v, want %v", client.callOrder, []string{"GetShardIterator", "GetRecords"})
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	if aws.ToString(client.getShardIteratorCalls[0].ShardId) != "shard-1" {
		t.Fatalf("ShardId = %q, want %q", aws.ToString(client.getShardIteratorCalls[0].ShardId), "shard-1")
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
	if aws.ToString(client.getRecordsCalls[0].ShardIterator) != "iterator-1" {
		t.Fatalf("ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[0].ShardIterator), "iterator-1")
	}
	if len(out.Records) != 1 {
		t.Fatalf("len(Records) = %d, want 1", len(out.Records))
	}
	if string(out.Records[0].Data) != "payload" {
		t.Fatalf("Record Data = %q, want %q", string(out.Records[0].Data), "payload")
	}
	if aws.ToString(out.NextShardIterator) != "iterator-2" {
		t.Fatalf("NextShardIterator = %q, want %q", aws.ToString(out.NextShardIterator), "iterator-2")
	}
}

func TestReadShardRecordsPageReturnsEmptyIteratorError(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	_, err := c.readShardRecordsPage(context.Background(), "shard-1")
	if err == nil || err.Error() != "read shard records page shard-1: empty shard iterator" {
		t.Fatalf("readShardRecordsPage() error = %v, want %q", err, "read shard records page shard-1: empty shard iterator")
	}
	if len(client.getRecordsCalls) != 0 {
		t.Fatalf("GetRecords calls = %d, want 0", len(client.getRecordsCalls))
	}
}

func TestReadShardRecordsPageWrapsIteratorError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getShardIteratorErr: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	_, err := c.readShardRecordsPage(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("readShardRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "read shard records page shard-1: get shard iterator shard-1: boom" {
		t.Fatalf("readShardRecordsPage() error = %v, want %q", err, "read shard records page shard-1: get shard iterator shard-1: boom")
	}
	if len(client.getRecordsCalls) != 0 {
		t.Fatalf("GetRecords calls = %d, want 0", len(client.getRecordsCalls))
	}
}

func TestReadShardRecordsPageWrapsRecordsError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsErr: errBoom,
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	_, err := c.readShardRecordsPage(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("readShardRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "read shard records page shard-1: get records iterator-1: boom" {
		t.Fatalf("readShardRecordsPage() error = %v, want %q", err, "read shard records page shard-1: get records iterator-1: boom")
	}
}
