package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

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
		store:  &fakeCheckpointSaveStore{},
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
	if call.StartingSequenceNumber != nil {
		t.Fatalf("StartingSequenceNumber = %q, want nil", aws.ToString(call.StartingSequenceNumber))
	}
}

func TestGetShardIteratorUsesStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamARN: streamARN},
		client: client,
		store:  &fakeCheckpointSaveStore{},
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

func TestGetShardIteratorUsesConfiguredStartPositionWithoutCheckpoint(t *testing.T) {
	t.Parallel()

	timestamp := time.Unix(1700000000, 0).UTC()
	tests := []struct {
		name          string
		cfg           Config
		wantType      types.ShardIteratorType
		wantTimestamp *time.Time
	}{
		{
			name: "default latest",
			cfg: Config{
				StreamName: "stream",
			},
			wantType: types.ShardIteratorTypeLatest,
		},
		{
			name: "explicit latest",
			cfg: Config{
				StreamName:    "stream",
				StartPosition: StartLatest,
			},
			wantType: types.ShardIteratorTypeLatest,
		},
		{
			name: "trim horizon",
			cfg: Config{
				StreamName:    "stream",
				StartPosition: StartTrimHorizon,
			},
			wantType: types.ShardIteratorTypeTrimHorizon,
		},
		{
			name: "at timestamp",
			cfg: Config{
				StreamName:     "stream",
				StartPosition:  StartAtTimestamp,
				StartTimestamp: &timestamp,
			},
			wantType:      types.ShardIteratorTypeAtTimestamp,
			wantTimestamp: &timestamp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeKinesisClient{}
			c := &Consumer{
				cfg:    tt.cfg,
				client: client,
				store:  &fakeCheckpointSaveStore{},
			}

			if _, err := c.getShardIterator(context.Background(), "shard-1"); err != nil {
				t.Fatalf("getShardIterator() error = %v, want nil", err)
			}
			if len(client.getShardIteratorCalls) != 1 {
				t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
			}
			call := client.getShardIteratorCalls[0]
			if call.ShardIteratorType != tt.wantType {
				t.Fatalf("ShardIteratorType = %v, want %v", call.ShardIteratorType, tt.wantType)
			}
			if call.StartingSequenceNumber != nil {
				t.Fatalf("StartingSequenceNumber = %q, want nil", aws.ToString(call.StartingSequenceNumber))
			}
			if tt.wantTimestamp == nil {
				if call.Timestamp != nil {
					t.Fatalf("Timestamp = %v, want nil", *call.Timestamp)
				}
				return
			}
			if call.Timestamp == nil || !call.Timestamp.Equal(*tt.wantTimestamp) {
				t.Fatalf("Timestamp = %v, want %v", call.Timestamp, tt.wantTimestamp)
			}
		})
	}
}

func TestGetShardIteratorUsesTrimHorizonForChildWithKnownParentWithoutCheckpoint(t *testing.T) {
	t.Parallel()

	timestamp := time.Unix(1700000000, 0).UTC()
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "default latest",
			cfg:  Config{StreamName: "stream"},
		},
		{
			name: "explicit latest",
			cfg:  Config{StreamName: "stream", StartPosition: StartLatest},
		},
		{
			name: "trim horizon",
			cfg:  Config{StreamName: "stream", StartPosition: StartTrimHorizon},
		},
		{
			name: "at timestamp",
			cfg: Config{
				StreamName:     "stream",
				StartPosition:  StartAtTimestamp,
				StartTimestamp: &timestamp,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeKinesisClient{}
			c := &Consumer{
				cfg:    tt.cfg,
				client: client,
				store:  &fakeCheckpointSaveStore{},
			}
			c.parentage.record(map[string]types.Shard{
				"parent":  shardWithParents("parent", "", ""),
				"child-1": shardWithParents("child-1", "parent", ""),
			})

			if _, err := c.getShardIterator(context.Background(), "child-1"); err != nil {
				t.Fatalf("getShardIterator() error = %v, want nil", err)
			}
			if len(client.getShardIteratorCalls) != 1 {
				t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
			}
			call := client.getShardIteratorCalls[0]
			if call.ShardIteratorType != types.ShardIteratorTypeTrimHorizon {
				t.Fatalf("ShardIteratorType = %v, want %v", call.ShardIteratorType, types.ShardIteratorTypeTrimHorizon)
			}
			if call.StartingSequenceNumber != nil {
				t.Fatalf("StartingSequenceNumber = %q, want nil", aws.ToString(call.StartingSequenceNumber))
			}
			if call.Timestamp != nil {
				t.Fatalf("Timestamp = %v, want nil", *call.Timestamp)
			}
		})
	}
}

func TestGetShardIteratorKeepsStartPositionWhenParentsAgedOut(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}
	// The child's parent IDs reference shards absent from every listing this
	// consumer saw (aged out of retention): StartPosition semantics apply.
	c.parentage.record(map[string]types.Shard{
		"child-1": shardWithParents("child-1", "expired-parent", ""),
	})

	if _, err := c.getShardIterator(context.Background(), "child-1"); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	call := client.getShardIteratorCalls[0]
	if call.ShardIteratorType != types.ShardIteratorTypeLatest {
		t.Fatalf("ShardIteratorType = %v, want %v", call.ShardIteratorType, types.ShardIteratorTypeLatest)
	}
}

func TestGetShardIteratorPrefersCheckpointOverParentage(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{checkpoint: "sequence-1"},
	}
	c.parentage.record(map[string]types.Shard{
		"parent":  shardWithParents("parent", "", ""),
		"child-1": shardWithParents("child-1", "parent", ""),
	})

	if _, err := c.getShardIterator(context.Background(), "child-1"); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	call := client.getShardIteratorCalls[0]
	if call.ShardIteratorType != types.ShardIteratorTypeAfterSequenceNumber {
		t.Fatalf("ShardIteratorType = %v, want %v", call.ShardIteratorType, types.ShardIteratorTypeAfterSequenceNumber)
	}
	if aws.ToString(call.StartingSequenceNumber) != "sequence-1" {
		t.Fatalf("StartingSequenceNumber = %q, want %q", aws.ToString(call.StartingSequenceNumber), "sequence-1")
	}
}

func TestRefreshKnownShardsFeedsParentageIntoIteratorDerivation(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{
				Shards: []types.Shard{
					shardWithParents("parent", "", ""),
					shardWithParents("child-1", "parent", ""),
				},
			},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	known := make(map[string]types.Shard)
	if err := c.refreshKnownShards(context.Background(), known); err != nil {
		t.Fatalf("refreshKnownShards() error = %v, want nil", err)
	}

	// Both the first derivation and any expired-iterator re-derivation go
	// through getShardIterator with only the shard ID: the refresh recording
	// must be what carries the parent info to it.
	if _, err := c.getShardIterator(context.Background(), "child-1"); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	if got := client.getShardIteratorCalls[0].ShardIteratorType; got != types.ShardIteratorTypeTrimHorizon {
		t.Fatalf("child ShardIteratorType = %v, want %v", got, types.ShardIteratorTypeTrimHorizon)
	}

	client.getShardIteratorCalls = nil
	if _, err := c.getShardIterator(context.Background(), "parent"); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	if got := client.getShardIteratorCalls[0].ShardIteratorType; got != types.ShardIteratorTypeLatest {
		t.Fatalf("parent ShardIteratorType = %v, want %v", got, types.ShardIteratorTypeLatest)
	}
}

func TestGetShardIteratorReturnsCompletedForShardEndCheckpoint(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{checkpoint: "SHARD_END:sequence-1"},
	}

	got, err := c.getShardIterator(context.Background(), "shard-1")
	if !errors.Is(err, errShardCompleted) {
		t.Fatalf("getShardIterator() error = %v, want %v", err, errShardCompleted)
	}
	if err == nil || err.Error() != "get shard iterator shard-1: shard already completed" {
		t.Fatalf("getShardIterator() error = %v, want %q", err, "get shard iterator shard-1: shard already completed")
	}
	if got != "" {
		t.Fatalf("getShardIterator() = %q, want empty", got)
	}
	if len(client.getShardIteratorCalls) != 0 {
		t.Fatalf("GetShardIterator calls = %d, want 0", len(client.getShardIteratorCalls))
	}
}

func TestGetShardIteratorUsesCheckpointSequence(t *testing.T) {
	t.Parallel()

	timestamp := time.Unix(1700000000, 0).UTC()
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
	}
	c := &Consumer{
		cfg: Config{
			StreamName:     "stream",
			StartPosition:  StartAtTimestamp,
			StartTimestamp: &timestamp,
		},
		client: client,
		store:  &fakeCheckpointSaveStore{checkpoint: "sequence-1"},
	}

	got, err := c.getShardIterator(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if got != "iterator-1" {
		t.Fatalf("getShardIterator() = %q, want %q", got, "iterator-1")
	}
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1", len(client.getShardIteratorCalls))
	}
	call := client.getShardIteratorCalls[0]
	if call.ShardIteratorType != types.ShardIteratorTypeAfterSequenceNumber {
		t.Fatalf("ShardIteratorType = %v, want %v", call.ShardIteratorType, types.ShardIteratorTypeAfterSequenceNumber)
	}
	if aws.ToString(call.StartingSequenceNumber) != "sequence-1" {
		t.Fatalf("StartingSequenceNumber = %q, want %q", aws.ToString(call.StartingSequenceNumber), "sequence-1")
	}
	if call.Timestamp != nil {
		t.Fatalf("Timestamp = %v, want nil", *call.Timestamp)
	}
	if aws.ToString(call.StreamName) != "stream" {
		t.Fatalf("StreamName = %q, want %q", aws.ToString(call.StreamName), "stream")
	}
	if call.StreamARN != nil {
		t.Fatalf("StreamARN = %q, want nil", aws.ToString(call.StreamARN))
	}
	if aws.ToString(call.ShardId) != "shard-1" {
		t.Fatalf("ShardId = %q, want %q", aws.ToString(call.ShardId), "shard-1")
	}
}

func TestGetShardIteratorWrapsCheckpointReadError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{getErr: errBoom},
	}

	_, err := c.getShardIterator(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("getShardIterator() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "get shard iterator checkpoint shard-1: read shard checkpoint shard-1: boom" {
		t.Fatalf("getShardIterator() error = %v, want %q", err, "get shard iterator checkpoint shard-1: read shard checkpoint shard-1: boom")
	}
	if len(client.getShardIteratorCalls) != 0 {
		t.Fatalf("GetShardIterator calls = %d, want 0", len(client.getShardIteratorCalls))
	}
}

func TestGetShardIteratorWrapsClientError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getShardIteratorErr: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	_, err := c.getShardIterator(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("getShardIterator() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "get shard iterator shard-1: boom" {
		t.Fatalf("getShardIterator() error = %v, want %q", err, "get shard iterator shard-1: boom")
	}
}

func TestGetShardIteratorRejectsNilOutputWithoutCheckpointMutation(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	client := &fakeKinesisClient{getShardIteratorNil: true}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
	}

	iterator, err := c.getShardIterator(context.Background(), "shard-1")
	if !errors.Is(err, errNilKinesisOutput) {
		t.Fatalf("getShardIterator() error = %v, want wraps %v", err, errNilKinesisOutput)
	}
	if err == nil || err.Error() != "get shard iterator shard-1: kinesis protocol error: nil output without error" {
		t.Fatalf("getShardIterator() error = %v, want nil-output protocol error", err)
	}
	if iterator != "" {
		t.Fatalf("getShardIterator() = %q, want empty", iterator)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("checkpoint Save calls = %d, want 0", len(store.saveCalls))
	}
}
