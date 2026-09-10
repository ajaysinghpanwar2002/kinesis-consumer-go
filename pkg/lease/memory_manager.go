package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

// MemoryManager is an in-memory Manager implementation intended for tests and
// local development. It coordinates shard ownership (exclusive in steady
// state — see the Manager contract for the transfer windows) and worker
// liveness in process memory, with TTL expiry driven by an injectable clock.
//
// Its semantics mirror the Redis reference backend: a lease can be acquired
// only when no live lease exists, claimed only from a still-live expected
// owner, and renewed or released only by its current owner. Expired entries
// read as absent.
//
// Callers must pass a positive ttl. A non-positive ttl yields an entry that is
// already expired at creation; the consumer validates its lease TTL, so this
// edge is unreachable on the real path.
type MemoryManager struct {
	mu      sync.Mutex
	leases  map[string]map[string]leaseEntry // stream -> shard -> entry
	workers map[string]map[string]time.Time  // stream -> owner -> expiry
	now     func() time.Time
}

type leaseEntry struct {
	owner      string
	generation string
	expiry     time.Time
}

var (
	_ Manager      = (*MemoryManager)(nil)
	_ Deregisterer = (*MemoryManager)(nil)
)

// NewMemoryManager returns an empty in-memory lease manager backed by the real
// clock.
func NewMemoryManager() *MemoryManager {
	return &MemoryManager{
		leases:  make(map[string]map[string]leaseEntry),
		workers: make(map[string]map[string]time.Time),
		now:     time.Now,
	}
}

// expired reports whether an entry with the given expiry is expired at t.
// An entry is expired once now has reached its expiry instant.
func expired(expiry, t time.Time) bool {
	return !expiry.After(t)
}

// Acquire claims a shard for owner when no live lease exists. It returns
// (nil, false, nil) when the shard is already owned by a live lease.
func (m *MemoryManager) Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	now := m.now()
	if entry, ok := m.leases[streamName][shardID]; ok && !expired(entry.expiry, now) {
		return nil, false, nil
	}

	generation := newGeneration()
	m.setLeaseLocked(streamName, shardID, owner, generation, now.Add(ttl))
	return m.newLease(streamName, shardID, owner, generation), true, nil
}

// Claim transfers a shard from expectedOwner to newOwner when a live lease is
// still held by expectedOwner. It returns (nil, false, nil) otherwise.
func (m *MemoryManager) Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	now := m.now()
	entry, ok := m.leases[streamName][shardID]
	if !ok || expired(entry.expiry, now) || entry.owner != expectedOwner {
		return nil, false, nil
	}

	generation := newGeneration()
	m.setLeaseLocked(streamName, shardID, newOwner, generation, now.Add(ttl))
	return m.newLease(streamName, shardID, newOwner, generation), true, nil
}

// List returns the current live shard owners for a stream.
func (m *MemoryManager) List(_ context.Context, streamName string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	shards := m.leases[streamName]
	result := make(map[string]string, len(shards))
	for shardID, entry := range shards {
		if expired(entry.expiry, now) {
			delete(shards, shardID)
			continue
		}
		result[shardID] = entry.owner
	}
	return result, nil
}

// Heartbeat records or refreshes owner as a live worker for the stream.
func (m *MemoryManager) Heartbeat(_ context.Context, streamName, owner string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	owners := m.workers[streamName]
	if owners == nil {
		owners = make(map[string]time.Time)
		m.workers[streamName] = owners
	}
	owners[owner] = m.now().Add(ttl)
	return nil
}

// Workers returns the live worker owners for a stream, sorted for deterministic
// output.
func (m *MemoryManager) Workers(_ context.Context, streamName string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	owners := m.workers[streamName]
	result := make([]string, 0, len(owners))
	for owner, expiry := range owners {
		if expired(expiry, now) {
			delete(owners, owner)
			continue
		}
		result = append(result, owner)
	}
	sort.Strings(result)
	return result, nil
}

// Deregister removes owner from the stream's live-worker set. Removing an owner
// that is already absent is a no-op success, mirroring the reference backend.
func (m *MemoryManager) Deregister(_ context.Context, streamName, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if owners := m.workers[streamName]; owners != nil {
		delete(owners, owner)
	}
	return nil
}

func (m *MemoryManager) setLeaseLocked(streamName, shardID, owner, generation string, expiry time.Time) {
	shards := m.leases[streamName]
	if shards == nil {
		shards = make(map[string]leaseEntry)
		m.leases[streamName] = shards
	}
	shards[shardID] = leaseEntry{owner: owner, generation: generation, expiry: expiry}
}

func newGeneration() string {
	var token [32]byte
	_, _ = rand.Read(token[:]) // crypto/rand.Read cannot fail on supported Go versions.
	return hex.EncodeToString(token[:])
}

func (m *MemoryManager) newLease(streamName, shardID, owner, generation string) *memoryLease {
	return &memoryLease{mgr: m, stream: streamName, shard: shardID, owner: owner, generation: generation}
}

// WithLease executes fn while holding the ownership lock after validating a
// matching memory lease. It is the memory checkpoint backend's atomic write
// boundary. fn must not call the manager or lease, or retain access after return.
// A callback error does not invalidate ownership.
func (m *MemoryManager) WithLease(ctx context.Context, held FencedLease, stream, shard string, fn func() error) error {
	l, ok := held.(*memoryLease)
	if !ok || l == nil || l.mgr != m || l.stream != stream || l.shard != shard {
		return ErrLeaseMismatch
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	entry, ok := m.leases[stream][shard]
	if l.invalid || !ok || entry.owner != l.owner || entry.generation != l.generation || expired(entry.expiry, m.now()) {
		l.invalid = true
		return ErrNotOwned
	}
	return fn()
}

type memoryLease struct {
	mgr                              *MemoryManager
	stream, shard, owner, generation string
	invalid                          bool // guarded by mgr.mu
}

var _ FencedLease = (*memoryLease)(nil)

func (l *memoryLease) Generation() string { return l.generation }

func (l *memoryLease) Validate(ctx context.Context) error {
	return l.mgr.WithLease(ctx, l, l.stream, l.shard, func() error { return nil })
}

func (l *memoryLease) Invalidate() {
	l.mgr.mu.Lock()
	defer l.mgr.mu.Unlock()
	l.invalid = true
}

func (l *memoryLease) Renew(ctx context.Context, ttl time.Duration) error {
	return l.mgr.WithLease(ctx, l, l.stream, l.shard, func() error {
		l.mgr.setLeaseLocked(l.stream, l.shard, l.owner, l.generation, l.mgr.now().Add(ttl))
		return nil
	})
}

func (l *memoryLease) Release(ctx context.Context) error {
	return l.mgr.WithLease(ctx, l, l.stream, l.shard, func() error {
		delete(l.mgr.leases[l.stream], l.shard)
		l.invalid = true
		return nil
	})
}
