package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestPollShardRecordsPagesAdvancesToNextIterator(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-1"), Data: []byte("payload-1")},
				},
				NextShardIterator: aws.String("iterator-2"),
			},
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-2"), Data: []byte("payload-2")},
				},
			},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	pages, err := c.pollShardRecordsPages(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("pollShardRecordsPages() error = %v, want nil", err)
	}
	if len(pages) != 2 {
		t.Fatalf("len(pages) = %d, want 2", len(pages))
	}
	if len(client.getRecordsCalls) != 2 {
		t.Fatalf("GetRecords calls = %d, want 2", len(client.getRecordsCalls))
	}
	if aws.ToString(client.getRecordsCalls[0].ShardIterator) != "iterator-1" {
		t.Fatalf("first ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[0].ShardIterator), "iterator-1")
	}
	if aws.ToString(client.getRecordsCalls[1].ShardIterator) != "iterator-2" {
		t.Fatalf("second ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[1].ShardIterator), "iterator-2")
	}
	if string(pages[1].Records[0].Data) != "payload-2" {
		t.Fatalf("second page record data = %q, want %q", string(pages[1].Records[0].Data), "payload-2")
	}
}

func TestPollShardRecordsPagesContinuesAfterEmptyPageWithNextIterator(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{NextShardIterator: aws.String("iterator-2")},
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-1"), Data: []byte("payload")},
				},
			},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	pages, err := c.pollShardRecordsPages(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("pollShardRecordsPages() error = %v, want nil", err)
	}
	if len(pages) != 2 {
		t.Fatalf("len(pages) = %d, want 2", len(pages))
	}
	if len(pages[0].Records) != 0 {
		t.Fatalf("len(first page Records) = %d, want 0", len(pages[0].Records))
	}
	if aws.ToString(client.getRecordsCalls[1].ShardIterator) != "iterator-2" {
		t.Fatalf("second ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[1].ShardIterator), "iterator-2")
	}
}

func TestPollShardRecordsPagesStopsOnIteratorExhaustion(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	pages, err := c.pollShardRecordsPages(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("pollShardRecordsPages() error = %v, want nil", err)
	}
	if len(pages) != 1 {
		t.Fatalf("len(pages) = %d, want 1", len(pages))
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
}

func TestPollShardRecordsPagesStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{NextShardIterator: aws.String("iterator-2")},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}
	cancel()

	pages, err := c.pollShardRecordsPages(ctx, "shard-1")
	if err != nil {
		t.Fatalf("pollShardRecordsPages() error = %v, want nil", err)
	}
	if len(pages) != 0 {
		t.Fatalf("len(pages) = %d, want 0", len(pages))
	}
	if len(client.getRecordsCalls) != 0 {
		t.Fatalf("GetRecords calls = %d, want 0", len(client.getRecordsCalls))
	}
}

func TestPollShardRecordsPagesWrapsRecordsError(t *testing.T) {
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

	_, err := c.pollShardRecordsPages(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("pollShardRecordsPages() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "poll shard records pages shard-1: get records iterator-1: boom" {
		t.Fatalf("pollShardRecordsPages() error = %v, want %q", err, "poll shard records pages shard-1: get records iterator-1: boom")
	}
}
