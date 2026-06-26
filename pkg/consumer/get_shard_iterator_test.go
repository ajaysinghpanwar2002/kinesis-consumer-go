package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestGetShardIteratorForwardsContextStreamShardAndIteratorType(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	got, err := c.getShardIterator(ctx, "shard-1")
	if err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if got != "iterator-1" {
		t.Fatalf("getShardIterator() = %q, want %q", got, "iterator-1")
	}
	if client.getShardIteratorCtx != ctx {
		t.Fatalf("GetShardIterator context = %v, want %v", client.getShardIteratorCtx, ctx)
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	call := client.getShardIteratorCalls[0]
	if aws.ToString(call.StreamName) != "stream" {
		t.Fatalf("StreamName = %q, want %q", aws.ToString(call.StreamName), "stream")
	}
	if call.StreamARN != nil {
		t.Fatalf("StreamARN = %q, want nil", aws.ToString(call.StreamARN))
	}
	if aws.ToString(call.ShardId) != "shard-1" {
		t.Fatalf("ShardId = %q, want %q", aws.ToString(call.ShardId), "shard-1")
	}
	if call.ShardIteratorType != types.ShardIteratorTypeLatest {
		t.Fatalf("ShardIteratorType = %v, want %v", call.ShardIteratorType, types.ShardIteratorTypeLatest)
	}
}

func TestGetShardIteratorUsesStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamARN: streamARN},
		client: client,
	}

	if _, err := c.getShardIterator(context.Background(), "shard-1"); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	call := client.getShardIteratorCalls[0]
	if aws.ToString(call.StreamARN) != streamARN {
		t.Fatalf("StreamARN = %q, want %q", aws.ToString(call.StreamARN), streamARN)
	}
	if call.StreamName != nil {
		t.Fatalf("StreamName = %q, want nil", aws.ToString(call.StreamName))
	}
}

func TestGetShardIteratorWrapsClientError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getShardIteratorErr: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	_, err := c.getShardIterator(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("getShardIterator() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "get shard iterator shard-1: boom" {
		t.Fatalf("getShardIterator() error = %v, want %q", err, "get shard iterator shard-1: boom")
	}
}
