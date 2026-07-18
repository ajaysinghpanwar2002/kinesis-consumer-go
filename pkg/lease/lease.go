package lease

import (
	"context"
	"errors"
	"time"
)

var ErrNotOwned = errors.New("lease not owned by caller")

// Manager coordinates shard ownership across consumer workers. The streamName
// argument is the consumer's opaque coordination identity, currently
// "<consumerGroup>:<canonicalStreamName>"; implementations must preserve it
// verbatim so different groups remain isolated.
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
//
// The owner argument is an opaque token identifying the calling worker; the
// same token is the ownership identity that Claim matches against and that
// Renew/Release are bound to. Every ttl argument must be at least 1ms: the
// reference backends measure TTL in milliseconds, so a sub-millisecond ttl has
// no valid representation (the consumer only ever passes a validated ttl that
// satisfies this). Acquire and Claim share a contended convention: when the
// requested transition cannot be made because another live lease stands in
// the way, they return (nil, false, nil) — false with no error — and reserve
// a non-nil error for genuine backend failures.
//
// Clock model: the backend's clock owns stored expiry. Whether a lease or
// worker entry is live is determined against backend time, so consumers with
// skewed wall clocks still agree on which entries are live. Consumer-local
// monotonic timers play a complementary role: they enforce bounded local
// safety budgets measured in locally elapsed time. The heartbeat-staleness
// fence stops the consumer before peers can treat it as dead; the renew
// watchdog bounds how long a worker whose Renew hangs can keep processing
// (within the TTL plus two renew intervals — a window that can extend past
// backend-side expiry and is part of the documented transfer windows). That
// division of labor is safe only under bounded clock-*rate* skew between the
// two clocks — TTLs are sized in seconds while typical rate drift is parts
// per million, so an entry must not expire meaningfully earlier or later
// than its nominal ttl as measured by either clock.
//
// List and Workers are individually consistent snapshots, but a combined
// (List, Workers) read is two calls and is not atomic across them: leases and
// worker liveness can change between the calls, so a lease may reference an
// owner absent from Workers and vice versa. Callers (the consumer's rebalance
// pass) must tolerate such mutually inconsistent snapshots; damping and
// re-reading on later ticks make them converge.
//
// Implementations must be safe for concurrent calls and return promptly when
// ctx is done. A backend that ignores cancellation can delay consumer shutdown
// or late worker cleanup.
type Manager interface {
	// Acquire atomically creates the shard lease for owner if and only if no
	// live lease currently exists (if-absent-set), returning the held Lease
	// and true on success. It returns (nil, false, nil) when a live lease is
	// already held by anyone (including owner itself).
	Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (Lease, bool, error)

	// List returns shardID -> owner for every currently-live lease on the
	// stream. Expired leases must be excluded — an implementation must never
	// surface a lease whose TTL has lapsed — so callers can treat the result
	// as the authoritative live-ownership snapshot.
	List(ctx context.Context, streamName string) (map[string]string, error)

	// Claim atomically transfers the shard lease from expectedOwner to
	// newOwner, valid only against a lease that is both still live and still
	// owned by expectedOwner (owner-compare-and-set), returning the new Lease
	// and true on success. It returns (nil, false, nil) when no live lease
	// exists or its owner is not expectedOwner.
	Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (Lease, bool, error)

	// Heartbeat records or refreshes owner as a live worker on the stream for
	// ttl. Worker liveness is tracked separately from shard leases; it is what
	// List's callers use (via Workers) to build the rebalance snapshot, so a
	// worker must heartbeat independently of whether it currently holds any
	// lease.
	Heartbeat(ctx context.Context, streamName, owner string, ttl time.Duration) error

	// Workers returns the owners currently alive on the stream. Expired worker
	// entries must be excluded, mirroring List.
	Workers(ctx context.Context, streamName string) ([]string, error)
}

// Provider supplies a lease manager from another dependency such as a checkpoint store.
type Provider interface {
	LeaseManager() (Manager, error)
}

// Lease represents ownership of one shard, bound to the owner token that
// acquired or claimed it. Renew and Release are authorized only for that
// owner: once the underlying lease has expired or been taken over, both report
// ErrNotOwned rather than acting on another worker's lease.
//
// Implementations must be safe for concurrent calls and return promptly when
// ctx is done.
type Lease interface {
	// Renew extends the lease TTL by ttl, but only while the caller is still
	// the recorded owner. It returns ErrNotOwned once ownership has been lost
	// — whether by TTL expiry or takeover by another worker — which the
	// consumer treats as the signal to stop the shard worker. Other non-nil
	// errors are backend failures.
	Renew(ctx context.Context, ttl time.Duration) error

	// Release deletes the lease, but only while the caller is still the owner
	// (owner-checked delete). It returns ErrNotOwned when ownership has already
	// been lost — a routine outcome after a takeover or expiry, not a failure
	// callers should treat as fatal.
	Release(ctx context.Context) error
}
