package backend

import "sync"

// SlotTracker bounds how many leases a single lease manager will hold at once.
// A max of 0 or less means unlimited. It is safe for concurrent use.
type SlotTracker struct {
	max   int
	mu    sync.Mutex
	owned map[string]struct{}
}

// NewSlotTracker returns a tracker that allows up to max concurrent
// reservations. A max of 0 or less means unlimited.
func NewSlotTracker(max int) *SlotTracker {
	return &SlotTracker{
		max:   max,
		owned: make(map[string]struct{}),
	}
}

// Max returns the configured reservation limit, or 0 for a nil tracker.
func (s *SlotTracker) Max() int {
	if s == nil {
		return 0
	}
	return s.max
}

// Reserve attempts to reserve a slot for key. It returns a release function and
// true when the reservation succeeds. When the tracker is nil or unlimited
// (max <= 0), it always succeeds with a no-op release and never tracks keys,
// so duplicate keys are indistinguishable there. When the limit is reached, it
// returns (nil, false).
//
// Reserving a key that is already reserved returns (nil, false): the existing
// reservation is owned by exactly one release closure, and handing out a
// second closure for the same map entry would let a failed redundant
// acquisition free the slot backing a still-live lease.
//
// The release function is intentionally not idempotent-guarded here; callers
// that need at-most-once release wrap it themselves.
func (s *SlotTracker) Reserve(key string) (func(), bool) {
	if s == nil || s.max <= 0 {
		return func() {}, true
	}

	s.mu.Lock()
	if _, exists := s.owned[key]; exists {
		s.mu.Unlock()
		return nil, false
	}
	if len(s.owned) >= s.max {
		s.mu.Unlock()
		return nil, false
	}
	s.owned[key] = struct{}{}
	s.mu.Unlock()

	return func() {
		s.mu.Lock()
		delete(s.owned, key)
		s.mu.Unlock()
	}, true
}
