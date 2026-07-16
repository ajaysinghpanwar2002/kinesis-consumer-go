package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestGetRecordsForwardsContextAndShardIterator(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	client := &fakeKinesisClient{}
	c := &Consumer{
		client: client,
	}

	if _, err := c.getRecords(ctx, "iterator-1"); err != nil {
		t.Fatalf("getRecords() error = %v, want nil", err)
	}
	if client.getRecordsCtx != ctx {
		t.Fatalf("GetRecords context = %v, want %v", client.getRecordsCtx, ctx)
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
	if aws.ToString(client.getRecordsCalls[0].ShardIterator) != "iterator-1" {
		t.Fatalf("ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[0].ShardIterator), "iterator-1")
	}
}

func TestGetRecordsUsesConfiguredStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:123456789012:stream/orders"
	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamARN: streamARN},
		client: client,
	}

	if _, err := c.getRecords(context.Background(), "iterator-1"); err != nil {
		t.Fatalf("getRecords() error = %v, want nil", err)
	}
	if got := aws.ToString(client.getRecordsCalls[0].StreamARN); got != streamARN {
		t.Fatalf("StreamARN = %q, want %q", got, streamARN)
	}
}

func TestGetRecordsSetsBatchSizeLimit(t *testing.T) {
	t.Parallel()

	t.Run("configured batch size caps the page limit", func(t *testing.T) {
		t.Parallel()
		client := &fakeKinesisClient{}
		c := &Consumer{
			client: client,
			tuning: tuningConfig{batchSize: 25},
		}

		if _, err := c.getRecords(context.Background(), "iterator-1"); err != nil {
			t.Fatalf("getRecords() error = %v, want nil", err)
		}
		if len(client.getRecordsCalls) != 1 {
			t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
		}
		if got := client.getRecordsCalls[0].Limit; got == nil || *got != 25 {
			t.Fatalf("Limit = %v, want 25", aws.ToInt32(got))
		}
	})

	t.Run("zero batch size sends no limit", func(t *testing.T) {
		t.Parallel()
		client := &fakeKinesisClient{}
		c := &Consumer{
			client: client,
		}

		if _, err := c.getRecords(context.Background(), "iterator-1"); err != nil {
			t.Fatalf("getRecords() error = %v, want nil", err)
		}
		if len(client.getRecordsCalls) != 1 {
			t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
		}
		if got := client.getRecordsCalls[0].Limit; got != nil {
			t.Fatalf("Limit = %d, want nil (guard against invalid Limit=0)", *got)
		}
	})
}

func TestGetRecordsReturnsRecordsAndNextIterator(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
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
		client: client,
	}

	out, err := c.getRecords(context.Background(), "iterator-1")
	if err != nil {
		t.Fatalf("getRecords() error = %v, want nil", err)
	}
	if len(out.Records) != 1 {
		t.Fatalf("len(Records) = %d, want 1", len(out.Records))
	}
	if string(out.Records[0].Data) != "payload" {
		t.Fatalf("Record Data = %q, want %q", string(out.Records[0].Data), "payload")
	}
	if aws.ToString(out.Records[0].PartitionKey) != "partition-1" {
		t.Fatalf("PartitionKey = %q, want %q", aws.ToString(out.Records[0].PartitionKey), "partition-1")
	}
	if aws.ToString(out.Records[0].SequenceNumber) != "sequence-1" {
		t.Fatalf("SequenceNumber = %q, want %q", aws.ToString(out.Records[0].SequenceNumber), "sequence-1")
	}
	if aws.ToString(out.NextShardIterator) != "iterator-2" {
		t.Fatalf("NextShardIterator = %q, want %q", aws.ToString(out.NextShardIterator), "iterator-2")
	}
}

func TestGetRecordsHandlesNilOutput(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{}
	c := &Consumer{
		client: client,
	}

	out, err := c.getRecords(context.Background(), "iterator-1")
	if err != nil {
		t.Fatalf("getRecords() error = %v, want nil", err)
	}
	if out == nil {
		t.Fatal("getRecords() output = nil, want empty output")
	}
	if len(out.Records) != 0 {
		t.Fatalf("len(Records) = %d, want 0", len(out.Records))
	}
	if out.NextShardIterator != nil {
		t.Fatalf("NextShardIterator = %q, want nil", aws.ToString(out.NextShardIterator))
	}
}

func TestGetRecordsWrapsClientError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getRecordsErr: errBoom}
	c := &Consumer{
		client: client,
	}

	_, err := c.getRecords(context.Background(), "iterator-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("getRecords() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "get records iterator-1: boom" {
		t.Fatalf("getRecords() error = %v, want %q", err, "get records iterator-1: boom")
	}
}
