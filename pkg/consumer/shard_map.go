package consumer

import "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

func mergeKnownShards(dst map[string]types.Shard, shards []types.Shard) {
	for _, shard := range shards {
		shardID := shardIDValue(shard)
		if shardID == "" {
			continue
		}
		dst[shardID] = shard
	}
}
