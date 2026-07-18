package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func mergeKnownShards(dst map[string]types.Shard, shards []types.Shard) {
	for _, shard := range shards {
		shardID := shardIDValue(shard)
		if shardID == "" {
			continue
		}
		dst[shardID] = shard
	}
}

func (c *Consumer) refreshKnownShards(ctx context.Context, known map[string]types.Shard) error {
	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	mergeKnownShards(known, shards)
	// Record parentage even when the caller later discards this refresh's
	// candidate map on failure: parent links are immutable facts from the
	// listing, so keeping them early is always safe.
	c.parentage.record(known)
	return nil
}
