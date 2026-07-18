package consumer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func shardWithParents(shardID, parentID, adjacentParentID string) types.Shard {
	shard := types.Shard{ShardId: aws.String(shardID)}
	if parentID != "" {
		shard.ParentShardId = aws.String(parentID)
	}
	if adjacentParentID != "" {
		shard.AdjacentParentShardId = aws.String(adjacentParentID)
	}
	return shard
}

func TestShardParentageMarksChildOnlyWhenParentIsListed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		shards  map[string]types.Shard
		shardID string
		want    bool
	}{
		{
			name: "split child with listed parent",
			shards: map[string]types.Shard{
				"parent": shardWithParents("parent", "", ""),
				"child":  shardWithParents("child", "parent", ""),
			},
			shardID: "child",
			want:    true,
		},
		{
			name: "merge child with only adjacent parent listed",
			shards: map[string]types.Shard{
				"parent-b": shardWithParents("parent-b", "", ""),
				"child":    shardWithParents("child", "parent-a", "parent-b"),
			},
			shardID: "child",
			want:    true,
		},
		{
			name: "parentless shard",
			shards: map[string]types.Shard{
				"shard-1": shardWithParents("shard-1", "", ""),
			},
			shardID: "shard-1",
			want:    false,
		},
		{
			name: "parent IDs reference shards aged out of the listing",
			shards: map[string]types.Shard{
				"child": shardWithParents("child", "expired-a", "expired-b"),
			},
			shardID: "child",
			want:    false,
		},
		{
			name: "unknown shard",
			shards: map[string]types.Shard{
				"parent": shardWithParents("parent", "", ""),
				"child":  shardWithParents("child", "parent", ""),
			},
			shardID: "other",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var p shardParentage
			p.record(tt.shards)
			if got := p.hasKnownParents(tt.shardID); got != tt.want {
				t.Fatalf("hasKnownParents(%q) = %v, want %v", tt.shardID, got, tt.want)
			}
		})
	}
}

func TestShardParentageIsMonotonicAcrossRecordings(t *testing.T) {
	t.Parallel()

	var p shardParentage
	p.record(map[string]types.Shard{
		"parent": shardWithParents("parent", "", ""),
		"child":  shardWithParents("child", "parent", ""),
	})
	if !p.hasKnownParents("child") {
		t.Fatalf("hasKnownParents(%q) = false after parent was listed, want true", "child")
	}

	// A later listing without the parent (e.g. after slice-14-style pruning or
	// retention aging) must not demote the child back to StartPosition.
	p.record(map[string]types.Shard{
		"child": shardWithParents("child", "parent", ""),
	})
	if !p.hasKnownParents("child") {
		t.Fatalf("hasKnownParents(%q) = false after parent aged out, want it to stay true", "child")
	}
}

func TestShardParentageZeroValueIsSafe(t *testing.T) {
	t.Parallel()

	var p shardParentage
	if p.hasKnownParents("shard-1") {
		t.Fatal("hasKnownParents on zero value = true, want false")
	}
	p.record(nil)
	if p.hasKnownParents("shard-1") {
		t.Fatal("hasKnownParents after empty record = true, want false")
	}
}
