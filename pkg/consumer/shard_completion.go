package consumer

import (
	"errors"
	"strings"
)

const shardCompletedPrefix = "SHARD_END"

var errShardCompleted = errors.New("shard already completed")

func isShardCompletedCheckpoint(seq string) bool {
	return strings.HasPrefix(seq, shardCompletedPrefix)
}

func shardCompletionValue(seq string) string {
	if seq == "" {
		return shardCompletedPrefix
	}
	return shardCompletedPrefix + ":" + seq
}
