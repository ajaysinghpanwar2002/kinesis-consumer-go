package consumer

import (
	"context"
	"sync"
)

type shardWorkerRegistration struct {
	gen      uint64
	cancel   context.CancelFunc
	stopping bool
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

// has reports whether any worker — running or stopping — is registered for
// the shard. Acquisition and claim eligibility must use this: a stopped
// worker keeps its registration until it has fully finished (lease released,
// deferred done run), and re-engaging the shard before then would let the
// old worker's stale lease handle release the successor's lease (the
// owner-checked release passes because both handles carry the same owner).
// No cooldown can bound that window — a callback may ignore its canceled
// context for arbitrarily long.
func (s *shardWorkerSet) has(shardID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.workers[shardID]
	return ok
}

// running reports whether a worker is registered for the shard and has not
// been stopped. Shed selection uses this so an already-stopping worker (a
// stuck callback awaiting its asynchronous reaper) cannot be re-picked and
// burn a move-budget slot every rebalance tick.
func (s *shardWorkerSet) running(shardID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reg, ok := s.workers[shardID]
	return ok && !reg.stopping
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

// stop cancels the shard's worker but keeps its registration, marked
// stopping, until the worker's deferred done removes it. Deleting here would
// drop the local stale-worker fence (see has) the moment the stop signal is
// sent, long before the worker has actually finished. Returns true only on
// the first stop of a running worker, so callers cannot double-count a move
// for a worker that is still winding down.
func (s *shardWorkerSet) stop(shardID string) bool {
	s.mu.Lock()
	reg, ok := s.workers[shardID]
	if ok && !reg.stopping {
		reg.stopping = true
		s.workers[shardID] = reg
	} else {
		ok = false
	}
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
