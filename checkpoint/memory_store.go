package checkpoint

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]string),
	}
}

func (m *MemoryStore) Get(_ context.Context, streamName, shardID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[m.key(streamName, shardID)], nil
}

func (m *MemoryStore) Save(_ context.Context, streamName, shardID, sequenceNumber string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[m.key(streamName, shardID)] = sequenceNumber
	return nil
}

func (m *MemoryStore) Delete(_ context.Context, streamName, shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, m.key(streamName, shardID))
	return nil
}

func (m *MemoryStore) key(streamName, shardID string) string {
	return streamName + ":" + shardID
}
