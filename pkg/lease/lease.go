package lease

import (
	"context"
	"errors"
	"time"
)

var ErrNotOwned = errors.New("lease not owned by caller")

// Manager coordinates exclusive shard ownership across consumer workers.
type Manager interface {
	Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (Lease, bool, error)
	List(ctx context.Context, streamName string) (map[string]string, error)
	Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (Lease, bool, error)
	Heartbeat(ctx context.Context, streamName, owner string, ttl time.Duration) error
	Workers(ctx context.Context, streamName string) ([]string, error)
}

// Provider supplies a lease manager from another dependency such as a checkpoint store.
type Provider interface {
	LeaseManager() (Manager, error)
}

// Lease represents ownership of one shard.
type Lease interface {
	Renew(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
}
