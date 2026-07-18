package consumer

import (
	"sync"
	"time"
)

func processingHealthSnapshot(s *processingProgressState) ProcessingHealth {
	lastRead, lastProcessed := s.snapshot()
	return ProcessingHealth{
		LastReadSuccess:     lastRead,
		LastRecordProcessed: lastProcessed,
	}
}

// Health is a point-in-time snapshot of the consumer's internal liveness
// signals, read via Consumer.Health. It reports observations, not a verdict:
// callers decide their own alerting thresholds (typically on each signal's
// ConsecutiveFailures and the age of its LastSuccess).
type Health struct {
	ShardSync  ShardSyncHealth
	Heartbeat  HeartbeatHealth
	Processing ProcessingHealth
}

// ProcessingHealth describes record-delivery progress across all owned
// shards. It exists so a wedged-but-renewing consumer — one whose leases and
// heartbeats stay healthy while no records move (for example a handler stuck
// ignoring its context) — is visible to probes. Both signals are
// consumer-global: any owned shard advances them, so one wedged shard among
// several healthy ones will not show here (use the per-shard metrics for
// that granularity).
type ProcessingHealth struct {
	// LastReadSuccess is when a GetRecords call last succeeded on any owned
	// shard, including empty pages at the shard tip. It is the delivery
	// loop's liveness signal — a wedged handler blocks its shard's next read,
	// while an idle-but-healthy consumer at the tip keeps polling — so probes
	// should alert on the age of this timestamp. Zero until the first
	// successful read (and always zero for a consumer that owns no shards).
	LastReadSuccess time.Time
	// LastRecordProcessed is when a page of records last finished processing
	// — handler success, or a skip/DLQ failure-policy outcome that lets the
	// page checkpoint advance. On a healthy consumer with no incoming traffic
	// this goes stale while LastReadSuccess stays fresh; the pair
	// distinguishes "no traffic" from "not processing". Zero until the first
	// processed page.
	LastRecordProcessed time.Time
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
		Processing: processingHealthSnapshot(&c.processingHealth),
	}
}

// processingProgressState backs Health().Processing. Shard workers write it
// concurrently from their read/process paths, so each record keeps only the
// greatest timestamp: a descheduled writer resuming with an older time must
// not regress a newer one and falsely report stale processing. Mutex-guarded
// time.Time values (like healthSignalState) preserve the monotonic clock
// component for callers computing signal age; the lock is negligible next to
// the GetRecords network call each write follows. Zero values mean never.
type processingProgressState struct {
	mu            sync.Mutex
	lastRead      time.Time
	lastProcessed time.Time
}

func (s *processingProgressState) recordRead(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.After(s.lastRead) {
		s.lastRead = now
	}
}

func (s *processingProgressState) recordProcessed(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.After(s.lastProcessed) {
		s.lastProcessed = now
	}
}

func (s *processingProgressState) snapshot() (lastRead, lastProcessed time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRead, s.lastProcessed
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
