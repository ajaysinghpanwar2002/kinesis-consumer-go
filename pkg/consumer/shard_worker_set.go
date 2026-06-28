package consumer

import (
	"context"
	"sync"
)

type shardWorkerSet struct {
	mu      sync.RWMutex
	workers map[string]context.CancelFunc
}

func newShardWorkerSet() *shardWorkerSet {
	return &shardWorkerSet{
		workers: make(map[string]context.CancelFunc),
	}
}

func (s *shardWorkerSet) add(shardID string, cancel context.CancelFunc) {
	if shardID == "" || cancel == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[shardID] = cancel
}

func (s *shardWorkerSet) has(shardID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.workers[shardID]
	return ok
}

func (s *shardWorkerSet) done(shardID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, shardID)
}

func (s *shardWorkerSet) stop(shardID string) bool {
	s.mu.Lock()
	cancel := s.workers[shardID]
	delete(s.workers, shardID)
	s.mu.Unlock()

	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (s *shardWorkerSet) stopAll() {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.workers))
	for _, cancel := range s.workers {
		cancels = append(cancels, cancel)
	}
	s.workers = make(map[string]context.CancelFunc)
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}
