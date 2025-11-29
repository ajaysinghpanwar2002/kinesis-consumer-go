package lease

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisManager implements shard leasing using Redis/Valkey.
type RedisManager struct {
	client redis.Cmdable
	prefix string
}

func NewRedisManager(client redis.Cmdable, prefix string) *RedisManager {
	if prefix == "" {
		prefix = "kinesis:lease"
	}
	return &RedisManager{
		client: client,
		prefix: prefix,
	}
}

func (m *RedisManager) Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (Lease, bool, error) {
	key := m.key(streamName, shardID)
	ok, err := m.client.SetNX(ctx, key, owner, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return &redisLease{
		client: m.client,
		key:    key,
		owner:  owner,
	}, true, nil
}

type redisLease struct {
	client redis.Cmdable
	key    string
	owner  string
}

func (l *redisLease) Renew(ctx context.Context, ttl time.Duration) error {
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`
	res, err := l.client.Eval(ctx, script, []string{l.key}, l.owner, ttl.Milliseconds()).Result()
	if err != nil {
		return err
	}
	if res == int64(0) {
		return ErrNotOwned
	}
	return nil
}

func (l *redisLease) Release(ctx context.Context) error {
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`
	res, err := l.client.Eval(ctx, script, []string{l.key}, l.owner).Result()
	if err != nil {
		return err
	}
	if res == int64(0) {
		return ErrNotOwned
	}
	return nil
}

func (m *RedisManager) key(streamName, shardID string) string {
	return m.prefix + ":" + streamName + ":" + shardID
}
