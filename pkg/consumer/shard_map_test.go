package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestMergeKnownShardsLeavesMapUnchangedForEmptyInput(t *testing.T) {
	t.Parallel()

	existing := testShard("existing", "", "")
	shards := map[string]types.Shard{"existing": existing}

	mergeKnownShards(shards, nil)

	if len(shards) != 1 {
		t.Fatalf("len(shards) = %d, want 1", len(shards))
	}
	if got := shards["existing"]; aws.ToString(got.ShardId) != "existing" {
		t.Fatalf("shards[existing].ShardId = %q, want existing", aws.ToString(got.ShardId))
	}
}

func TestMergeKnownShardsSkipsNilAndEmptyShardIDs(t *testing.T) {
	t.Parallel()

	shards := map[string]types.Shard{}

	mergeKnownShards(shards, []types.Shard{
		{},
		{ShardId: aws.String("")},
		testShard("shard-1", "", ""),
	})

	if len(shards) != 1 {
		t.Fatalf("len(shards) = %d, want 1", len(shards))
	}
	if _, ok := shards[""]; ok {
		t.Fatal("shards contains empty key, want skipped")
	}
	if got := shards["shard-1"]; aws.ToString(got.ShardId) != "shard-1" {
		t.Fatalf("shards[shard-1].ShardId = %q, want shard-1", aws.ToString(got.ShardId))
	}
}

func TestMergeKnownShardsOverwritesExistingMetadata(t *testing.T) {
	t.Parallel()

	shards := map[string]types.Shard{
		"child": testShard("child", "", ""),
	}

	mergeKnownShards(shards, []types.Shard{
		testShard("child", "parent", "adjacent"),
	})

	got := shards["child"]
	if aws.ToString(got.ParentShardId) != "parent" {
		t.Fatalf("ParentShardId = %q, want parent", aws.ToString(got.ParentShardId))
	}
	if aws.ToString(got.AdjacentParentShardId) != "adjacent" {
		t.Fatalf("AdjacentParentShardId = %q, want adjacent", aws.ToString(got.AdjacentParentShardId))
	}
}

func TestRefreshKnownShardsMergesListedShards(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{
				testShard("existing", "parent", "adjacent"),
				testShard("new", "", ""),
				{ShardId: aws.String("")},
			}},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	known := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}

	if err := c.refreshKnownShards(context.Background(), known); err != nil {
		t.Fatalf("refreshKnownShards() error = %v, want nil", err)
	}

	if len(known) != 2 {
		t.Fatalf("len(known) = %d, want 2", len(known))
	}
	if got := known["existing"]; aws.ToString(got.ParentShardId) != "parent" {
		t.Fatalf("known[existing].ParentShardId = %q, want parent", aws.ToString(got.ParentShardId))
	}
	if got := known["existing"]; aws.ToString(got.AdjacentParentShardId) != "adjacent" {
		t.Fatalf("known[existing].AdjacentParentShardId = %q, want adjacent", aws.ToString(got.AdjacentParentShardId))
	}
	if got := known["new"]; aws.ToString(got.ShardId) != "new" {
		t.Fatalf("known[new].ShardId = %q, want new", aws.ToString(got.ShardId))
	}
	if _, ok := known[""]; ok {
		t.Fatal("known contains empty key, want skipped")
	}
	if len(client.calls) != 1 {
		t.Fatalf("ListShards calls = %d, want 1", len(client.calls))
	}
}

func TestRefreshKnownShardsLeavesMapUnchangedForEmptyList(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{{}},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	known := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}

	if err := c.refreshKnownShards(context.Background(), known); err != nil {
		t.Fatalf("refreshKnownShards() error = %v, want nil", err)
	}

	if len(known) != 1 {
		t.Fatalf("len(known) = %d, want 1", len(known))
	}
	if got := known["existing"]; aws.ToString(got.ShardId) != "existing" {
		t.Fatalf("known[existing].ShardId = %q, want existing", aws.ToString(got.ShardId))
	}
}

func TestRefreshKnownShardsReturnsListErrorWithoutMutatingMap(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{err: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	known := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}

	err := c.refreshKnownShards(context.Background(), known)
	if !errors.Is(err, errBoom) {
		t.Fatalf("refreshKnownShards() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("refreshKnownShards() error = %v, want %q", err, "list shards: boom")
	}
	if len(known) != 1 {
		t.Fatalf("len(known) = %d, want 1", len(known))
	}
	if got := known["existing"]; aws.ToString(got.ShardId) != "existing" {
		t.Fatalf("known[existing].ShardId = %q, want existing", aws.ToString(got.ShardId))
	}
}
