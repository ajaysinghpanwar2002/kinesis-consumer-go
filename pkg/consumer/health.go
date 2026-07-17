package consumer

import (
	"sync"
	"time"
)

// Health is a point-in-time snapshot of the consumer's internal liveness
// signals, read via Consumer.Health. It reports observations, not a verdict:
// callers decide their own alerting thresholds (typically on
// ShardSync.ConsecutiveFailures and the age of ShardSync.LastSuccess).
type Health struct {
	ShardSync ShardSyncHealth
}

// ShardSyncHealth describes the periodic shard-discovery loop: the sync pass
// that lists shards, checks checkpoint readiness, and acquires unowned
// leases. Failed passes are survivable (existing shard workers keep
// delivering) until the staleness bound configured by
// WithShardSyncMaxStaleness is exceeded.
type ShardSyncHealth struct {
	// ConsecutiveFailures counts sync passes that have failed since the last
	// successful pass; 0 while discovery is healthy.
	ConsecutiveFailures int
	// LastSuccess is when a sync pass last completed successfully. It is the
	// anchor of the staleness bound. Zero until the consumer's first
	// successful discovery (Start's initial listing counts).
	LastSuccess time.Time
	// LastError is the most recent sync failure, nil while discovery is
	// healthy.
	LastError error
}

// Health returns a snapshot of the consumer's liveness signals. It is safe
// for concurrent use with a running Start and returns zero values for a
// consumer that has not started.
func (c *Consumer) Health() Health {
	return Health{
		ShardSync: c.syncHealth.snapshot(),
	}
}

// shardSyncHealthState is the mutable, locked backing store behind
// Health().ShardSync, written only by the orchestration loop.
type shardSyncHealthState struct {
	mu                  sync.Mutex
	consecutiveFailures int
	lastSuccess         time.Time
	lastErr             error
}

// anchor seeds lastSuccess for the staleness clock without claiming a new
// success: it only fills a zero lastSuccess, so a real run measures from
// Start's just-completed initial discovery.
func (s *shardSyncHealthState) anchor(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSuccess.IsZero() {
		s.lastSuccess = now
	}
}

func (s *shardSyncHealthState) recordSuccess(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveFailures = 0
	s.lastSuccess = now
	s.lastErr = nil
}

// recordFailure notes a failed sync pass and returns the updated consecutive
// count alongside the staleness anchor for the caller's bound check.
func (s *shardSyncHealthState) recordFailure(err error) (consecutiveFailures int, lastSuccess time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveFailures++
	s.lastErr = err
	return s.consecutiveFailures, s.lastSuccess
}

func (s *shardSyncHealthState) snapshot() ShardSyncHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ShardSyncHealth{
		ConsecutiveFailures: s.consecutiveFailures,
		LastSuccess:         s.lastSuccess,
		LastError:           s.lastErr,
	}
}
