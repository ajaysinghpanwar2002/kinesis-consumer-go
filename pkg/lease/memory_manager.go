package lease

import (
	"context"
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
	owner  string
	expiry time.Time
}

var _ Manager = (*MemoryManager)(nil)

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
func (m *MemoryManager) Acquire(_ context.Context, streamName, shardID, owner string, ttl time.Duration) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if entry, ok := m.leases[streamName][shardID]; ok && !expired(entry.expiry, now) {
		return nil, false, nil
	}

	m.setLeaseLocked(streamName, shardID, owner, now.Add(ttl))
	return m.newLease(streamName, shardID, owner), true, nil
}

// Claim transfers a shard from expectedOwner to newOwner when a live lease is
// still held by expectedOwner. It returns (nil, false, nil) otherwise.
func (m *MemoryManager) Claim(_ context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	entry, ok := m.leases[streamName][shardID]
	if !ok || expired(entry.expiry, now) || entry.owner != expectedOwner {
		return nil, false, nil
	}

	m.setLeaseLocked(streamName, shardID, newOwner, now.Add(ttl))
	return m.newLease(streamName, shardID, newOwner), true, nil
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

func (m *MemoryManager) setLeaseLocked(streamName, shardID, owner string, expiry time.Time) {
	shards := m.leases[streamName]
	if shards == nil {
		shards = make(map[string]leaseEntry)
		m.leases[streamName] = shards
	}
	shards[shardID] = leaseEntry{owner: owner, expiry: expiry}
}

func (m *MemoryManager) newLease(streamName, shardID, owner string) *memoryLease {
	return &memoryLease{mgr: m, stream: streamName, shard: shardID, owner: owner}
}

// renew extends the lease TTL when owner still holds a live lease.
func (m *MemoryManager) renew(streamName, shardID, owner string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	entry, ok := m.leases[streamName][shardID]
	if !ok || expired(entry.expiry, now) || entry.owner != owner {
		return ErrNotOwned
	}
	m.setLeaseLocked(streamName, shardID, owner, now.Add(ttl))
	return nil
}

// release deletes the lease when owner still holds a live lease.
func (m *MemoryManager) release(streamName, shardID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.leases[streamName][shardID]
	if !ok || expired(entry.expiry, m.now()) || entry.owner != owner {
		return ErrNotOwned
	}
	delete(m.leases[streamName], shardID)
	return nil
}

// memoryLease is a single owned shard lease bound to its MemoryManager.
type memoryLease struct {
	mgr    *MemoryManager
	stream string
	shard  string
	owner  string
}

var _ Lease = (*memoryLease)(nil)

// Renew extends the lease TTL if the caller still owns it, else ErrNotOwned.
func (l *memoryLease) Renew(_ context.Context, ttl time.Duration) error {
	return l.mgr.renew(l.stream, l.shard, l.owner, ttl)
}

// Release removes the lease if the caller still owns it, else ErrNotOwned.
func (l *memoryLease) Release(_ context.Context) error {
	return l.mgr.release(l.stream, l.shard, l.owner)
}
