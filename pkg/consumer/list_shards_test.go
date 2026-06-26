package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type fakeKinesisClient struct {
	calls []kinesis.ListShardsInput
	outs  []*kinesis.ListShardsOutput
	err   error

	getShardIteratorCtx   context.Context
	getShardIteratorCalls []kinesis.GetShardIteratorInput
	getShardIteratorOut   *kinesis.GetShardIteratorOutput
	getShardIteratorErr   error
}

func (c *fakeKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	_ = ctx
	_ = optFns

	if params != nil {
		c.calls = append(c.calls, *params)
	}
	if c.err != nil {
		return nil, c.err
	}
	if len(c.outs) == 0 {
		return &kinesis.ListShardsOutput{}, nil
	}

	out := c.outs[0]
	c.outs = c.outs[1:]
	return out, nil
}

func (c *fakeKinesisClient) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	_ = optFns

	c.getShardIteratorCtx = ctx
	if params != nil {
		c.getShardIteratorCalls = append(c.getShardIteratorCalls, *params)
	}
	if c.getShardIteratorErr != nil {
		return nil, c.getShardIteratorErr
	}
	if c.getShardIteratorOut == nil {
		return &kinesis.GetShardIteratorOutput{}, nil
	}
	return c.getShardIteratorOut, nil
}

func TestListShardsPaginationSkipsNilShardIDs(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{
				Shards: []types.Shard{
					{ShardId: aws.String("shard-1")},
					{},
				},
				NextToken: aws.String("next"),
			},
			{
				Shards: []types.Shard{
					{ShardId: aws.String("shard-2")},
				},
			},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	shards, err := c.listShards(context.Background())
	if err != nil {
		t.Fatalf("listShards() error = %v, want nil", err)
	}
	if len(shards) != 2 {
		t.Fatalf("len(shards) = %d, want 2", len(shards))
	}
	if aws.ToString(shards[0].ShardId) != "shard-1" {
		t.Fatalf("shards[0].ShardId = %q, want shard-1", aws.ToString(shards[0].ShardId))
	}
	if aws.ToString(shards[1].ShardId) != "shard-2" {
		t.Fatalf("shards[1].ShardId = %q, want shard-2", aws.ToString(shards[1].ShardId))
	}
	if len(client.calls) != 2 {
		t.Fatalf("ListShards calls = %d, want 2", len(client.calls))
	}
	if aws.ToString(client.calls[0].StreamName) != "stream" {
		t.Fatalf("first StreamName = %q, want stream", aws.ToString(client.calls[0].StreamName))
	}
	if client.calls[0].NextToken != nil {
		t.Fatalf("first NextToken = %q, want nil", aws.ToString(client.calls[0].NextToken))
	}
	if aws.ToString(client.calls[1].NextToken) != "next" {
		t.Fatalf("second NextToken = %q, want next", aws.ToString(client.calls[1].NextToken))
	}
}

func TestListShardsUsesStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamARN: streamARN},
		client: client,
	}

	if _, err := c.listShards(context.Background()); err != nil {
		t.Fatalf("listShards() error = %v, want nil", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("ListShards calls = %d, want 1", len(client.calls))
	}
	if aws.ToString(client.calls[0].StreamARN) != streamARN {
		t.Fatalf("StreamARN = %q, want %q", aws.ToString(client.calls[0].StreamARN), streamARN)
	}
	if client.calls[0].StreamName != nil {
		t.Fatalf("StreamName = %q, want nil", aws.ToString(client.calls[0].StreamName))
	}
}

func TestListShardsWrapsClientError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{err: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	_, err := c.listShards(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("listShards() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("listShards() error = %v, want %q", err, "list shards: boom")
	}
}
