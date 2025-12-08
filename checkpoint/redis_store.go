package checkpoint

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const shardCompletedPrefix = "SHARD_END"

type RedisStore struct {
	client redis.Cmdable
	prefix string
	ttl    time.Duration
}

func NewRedisStore(client redis.Cmdable, prefix string, ttl time.Duration) *RedisStore {
	if prefix == "" {
		prefix = "kinesis:checkpoint"
	}
	if ttl == 0 {
		ttl = 30 * 24 * time.Hour
	}
	return &RedisStore{
		client: client,
		prefix: prefix,
		ttl:    ttl,
	}
}

func (s *RedisStore) Get(ctx context.Context, streamName, shardID string) (string, error) {
	val, err := s.client.Get(ctx, s.key(streamName, shardID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

func (s *RedisStore) Save(ctx context.Context, streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return nil
	}

	ttl := s.ttl
	if strings.HasPrefix(sequenceNumber, shardCompletedPrefix) {
		// Persist shard completion markers so child shards are not blocked
		// when the store TTL is configured.
		ttl = 0
	}

	return s.client.Set(ctx, s.key(streamName, shardID), sequenceNumber, ttl).Err()
}

func (s *RedisStore) Delete(ctx context.Context, streamName, shardID string) error {
	return s.client.Del(ctx, s.key(streamName, shardID)).Err()
}

func (s *RedisStore) key(streamName, shardID string) string {
	return s.prefix + ":" + streamName + ":" + shardID
}
