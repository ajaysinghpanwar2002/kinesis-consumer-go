package consumer

import (
	"sync"
	"time"
)

// Health is a point-in-time snapshot of the consumer's internal liveness
// signals, read via Consumer.Health. It reports observations, not a verdict:
// callers decide their own alerting thresholds (typically on each signal's
// ConsecutiveFailures and the age of its LastSuccess).
type Health struct {
	ShardSync ShardSyncHealth
	Heartbeat HeartbeatHealth
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

// HeartbeatHealth describes the worker-liveness heartbeat loop: the periodic
// send that keeps this worker visible to peers. Failed sends are survivable
// (the worker key written by the last successful send is still live) until
// one heartbeat interval before that key's TTL lapses, at which point the
// run stops with ErrHeartbeatStale rather than dual-process shards that
// peers are about to claim.
type HeartbeatHealth struct {
	// ConsecutiveFailures counts heartbeat sends that have failed since the
	// last successful send; 0 while liveness is healthy.
	ConsecutiveFailures int
	// LastSuccess is when a heartbeat last succeeded. It is the anchor of
	// the validity deadline: the run stops once failures persist to
	// LastSuccess + heartbeat TTL - heartbeat interval. Zero until Start's
	// heartbeat loop begins (its start seeds the anchor before the first
	// send).
	LastSuccess time.Time
	// LastError is the most recent heartbeat failure, nil while liveness is
	// healthy.
	LastError error
}

// Health returns a snapshot of the consumer's liveness signals. It is safe
// for concurrent use with a running Start and returns zero values for a
// consumer that has not started.
func (c *Consumer) Health() Health {
	syncFailures, syncLastSuccess, syncErr := c.syncHealth.snapshot()
	hbFailures, hbLastSuccess, hbErr := c.heartbeatHealth.snapshot()
	return Health{
		ShardSync: ShardSyncHealth{
			ConsecutiveFailures: syncFailures,
			LastSuccess:         syncLastSuccess,
			LastError:           syncErr,
		},
		Heartbeat: HeartbeatHealth{
			ConsecutiveFailures: hbFailures,
			LastSuccess:         hbLastSuccess,
			LastError:           hbErr,
		},
	}
}

// healthSignalState is the mutable, locked backing store behind one Health
// signal (shard sync, heartbeat), written only by the loop that owns the
// signal.
type healthSignalState struct {
	mu                  sync.Mutex
	consecutiveFailures int
	lastSuccess         time.Time
	lastErr             error
}

// anchor seeds lastSuccess for the staleness clock without claiming a new
// success: it only fills a zero lastSuccess, so a real run measures from the
// owning loop's start (or, for shard sync, Start's just-completed initial
// discovery).
func (s *healthSignalState) anchor(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSuccess.IsZero() {
		s.lastSuccess = now
	}
}

func (s *healthSignalState) recordSuccess(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveFailures = 0
	s.lastSuccess = now
	s.lastErr = nil
}

// recordFailure notes a failed pass and returns the updated consecutive
// count alongside the staleness anchor for the caller's bound check.
func (s *healthSignalState) recordFailure(err error) (consecutiveFailures int, lastSuccess time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveFailures++
	s.lastErr = err
	return s.consecutiveFailures, s.lastSuccess
}

func (s *healthSignalState) snapshot() (consecutiveFailures int, lastSuccess time.Time, lastErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consecutiveFailures, s.lastSuccess, s.lastErr
}
