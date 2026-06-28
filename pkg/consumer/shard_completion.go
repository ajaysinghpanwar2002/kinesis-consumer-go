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
