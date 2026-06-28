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
	return nil
}
