package consumer

import (
	"context"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func (s *shardCompletionState) readyShardIDs(ctx context.Context, c *Consumer, shardMap map[string]types.Shard) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ready := make([]string, 0, len(shardMap))
	for shardID, shard := range shardMap {
		if shardID == "" {
			continue
		}

		completed, err := s.shardCompleted(ctx, c, shardID)
		if err != nil {
			return nil, fmt.Errorf("check ready shard %s completion: %w", shardID, err)
		}
		if completed {
			continue
		}

		parentsReady, err := s.parentsReady(ctx, c, shard, shardMap)
		if err != nil {
			return nil, err
		}
		if parentsReady {
			ready = append(ready, shardID)
		}
	}

	sort.Strings(ready)
	return ready, nil
}

func (s *shardCompletionState) parentsReady(
	ctx context.Context,
	c *Consumer,
	shard types.Shard,
	shardMap map[string]types.Shard,
) (bool, error) {
	for _, parentID := range shardParents(shard) {
		completed, err := s.shardCompleted(ctx, c, parentID)
		if err != nil {
			return false, fmt.Errorf("check shard %s parent %s completion: %w", shardIDValue(shard), parentID, err)
		}
		if completed {
			continue
		}
		if _, ok := shardMap[parentID]; ok {
			return false, nil
		}
	}
	return true, nil
}

func shardParents(shard types.Shard) []string {
	parents := make([]string, 0, 2)
	if shard.ParentShardId != nil && *shard.ParentShardId != "" {
		parents = append(parents, *shard.ParentShardId)
	}
	if shard.AdjacentParentShardId != nil && *shard.AdjacentParentShardId != "" {
		parents = append(parents, *shard.AdjacentParentShardId)
	}
	return parents
}

func shardIDValue(shard types.Shard) string {
	if shard.ShardId == nil {
		return ""
	}
	return *shard.ShardId
}
