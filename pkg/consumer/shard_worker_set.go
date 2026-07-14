package consumer

import (
	"context"
	"sync"
)

type shardWorkerRegistration struct {
	gen    uint64
	cancel context.CancelFunc
}

type shardWorkerSet struct {
	mu      sync.RWMutex
	nextGen uint64
	workers map[string]shardWorkerRegistration
}

func newShardWorkerSet() *shardWorkerSet {
	return &shardWorkerSet{
		workers: make(map[string]shardWorkerRegistration),
	}
}

// add registers the worker and returns its generation, which the worker must
// pass back to done. Generations start at 1; the 0 returned for ignored
// registrations never matches a stored entry.
func (s *shardWorkerSet) add(shardID string, cancel context.CancelFunc) uint64 {
	if shardID == "" || cancel == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextGen++
	s.workers[shardID] = shardWorkerRegistration{gen: s.nextGen, cancel: cancel}
	return s.nextGen
}

func (s *shardWorkerSet) has(shardID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.workers[shardID]
	return ok
}

// done removes the registration only if it still belongs to the caller's
// generation. A worker's deferred done runs after lease release (up to
// several seconds after its stop), so the shard may already be registered to
// a replacement worker — deleting unconditionally would orphan that
// replacement from has/stop/stopAll.
func (s *shardWorkerSet) done(shardID string, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reg, ok := s.workers[shardID]; ok && reg.gen == gen {
		delete(s.workers, shardID)
	}
}

func (s *shardWorkerSet) stop(shardID string) bool {
	s.mu.Lock()
	reg, ok := s.workers[shardID]
	delete(s.workers, shardID)
	s.mu.Unlock()

	if !ok {
		return false
	}
	reg.cancel()
	return true
}

func (s *shardWorkerSet) stopAll() {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.workers))
	for _, reg := range s.workers {
		cancels = append(cancels, reg.cancel)
	}
	s.workers = make(map[string]shardWorkerRegistration)
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}
