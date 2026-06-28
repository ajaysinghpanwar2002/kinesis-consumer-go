package consumer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
