package checkpoint

import (
	"context"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store implementation intended for tests and
// local development. It keeps per-shard sequence numbers in a map guarded by a
// read/write mutex and never returns an error.
//
// The key scheme streamName + ":" + shardID is ambiguous in the abstract, but
// AWS stream and shard names cannot contain ':', so no real collision is
// reachable.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty in-memory checkpoint store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]string)}
}

// Get returns the stored sequence number for the shard, or ("", nil) when no
// checkpoint has been saved. An empty result means "no checkpoint".
func (m *MemoryStore) Get(_ context.Context, streamName, shardID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[m.key(streamName, shardID)], nil
}

// Save persists the sequence number for the shard if it advances the
// checkpoint, per the Store contract; a stale or duplicate value is silently
// discarded. The value is stored verbatim, including SHARD_END completion
// markers.
func (m *MemoryStore) Save(_ context.Context, streamName, shardID, sequenceNumber string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(streamName, shardID)
	if checkpointAdvances(m.data[key], sequenceNumber) {
		m.data[key] = sequenceNumber
	}
	return nil
}

// checkpointAdvances implements the Store contract's advance-only rule. It
// must stay in lockstep with the Valkey backend's CheckpointSaveScript. (The
// script additionally rejects a corrupt stored value with an error; this map
// has no external writers, so no equivalent check exists here.)
func checkpointAdvances(current, next string) bool {
	if current == "" {
		return true
	}
	if next == current {
		return false
	}
	if strings.HasPrefix(current, CompletedPrefix) {
		return false
	}
	if strings.HasPrefix(next, CompletedPrefix) {
		return true
	}
	if len(next) != len(current) {
		return len(next) > len(current)
	}
	return next > current
}

// Delete removes any stored checkpoint for the shard. It is a no-op when no
// checkpoint exists.
func (m *MemoryStore) Delete(_ context.Context, streamName, shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, m.key(streamName, shardID))
	return nil
}

func (m *MemoryStore) key(streamName, shardID string) string {
	return streamName + ":" + shardID
}
