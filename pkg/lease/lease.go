package lease

import (
	"context"
	"errors"
	"time"
)

var ErrNotOwned = errors.New("lease not owned by caller")

// Manager coordinates shard ownership across consumer workers.
//
// Ownership is exclusive in steady state but not absolute: transfers open
// short windows during which the previous owner may still be processing.
// A rebalance claim moves the lease before the donor observes it at its
// next renew; a crashed worker's lease is reclaimed after its TTL lapses;
// and a worker whose Renew hangs is fenced by the consumer's local
// lease-validity watchdog within the TTL plus two renew intervals. These
// windows are part of the consumer's at-least-once delivery contract:
// they can duplicate records, and advance-only checkpointing keeps them
// from ever regressing progress. A Manager implementation must therefore
// guarantee atomic transitions (if-absent Acquire, owner-CAS Claim,
// owner-checked Renew/Release), not that two workers can never briefly
// process the same shard.
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
