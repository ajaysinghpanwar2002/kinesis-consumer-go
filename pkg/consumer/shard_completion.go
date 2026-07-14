package consumer

import (
	"errors"
	"strings"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
)

const shardCompletedPrefix = checkpoint.CompletedPrefix

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
