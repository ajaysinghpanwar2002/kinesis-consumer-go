package lease

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotOwned = errors.New("lease not owned by caller")
)

// Manager coordinates exclusive access to shards across consumers.
type Manager interface {
	// Acquire tries to claim a shard for an owner. If acquired is false, the shard
	// is already owned by another consumer.
	Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (Lease, bool, error)
}

// Lease represents an acquired shard lease that can be renewed or released.
type Lease interface {
	// Renew extends the lease TTL. Implementations should return ErrNotOwned if
	// the caller no longer owns the lease.
	Renew(ctx context.Context, ttl time.Duration) error
	// Release relinquishes the lease. Implementations should return ErrNotOwned
	// when the caller is not the owner.
	Release(ctx context.Context) error
}
