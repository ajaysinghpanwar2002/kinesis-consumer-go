package checkpoint

import (
	"context"
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

// Save persists the sequence number for the shard, overwriting any previous
// value. The value is stored verbatim, including SHARD_END completion markers.
func (m *MemoryStore) Save(_ context.Context, streamName, shardID, sequenceNumber string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[m.key(streamName, shardID)] = sequenceNumber
	return nil
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
