package consumer

import (
	"sync"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

// CheckpointHealth describes backend write attempts, including retry failures.
// Any successful write resets ConsecutiveFailures and LastError.
type CheckpointHealth struct {
	ConsecutiveFailures int
	LastSuccess         time.Time
	LastProgress        time.Time
	LastError           error
	// LastFailure retains the most recent failure even after a successful retry.
	LastFailure error
}

// RecoveryHealth retains recovery failures after workers exit, excluding
// requested cancellation and ordinary ownership loss.
type RecoveryHealth struct {
	Failures  int
	LastError error
}

// ShardHealth describes the current acquisition. Sequence numbers remain strings.
// AcceptedSequence is the contiguous completed prefix, not the greatest Ack.
// PersistedRecords counts records covered by this acquisition's confirmed writes.
type ShardHealth struct {
	AcceptedSequence            string
	PersistedSequence           string
	PersistedRecords            uint64
	Completed                   bool
	Checkpoint                  CheckpointHealth
	TimeSinceCheckpointProgress time.Duration
	Pressure                    PressureHealth
}

type shardObservation struct {
	health   ShardHealth
	tracker  *ackTracker
	accepted uint64
	started  time.Time
	progress time.Time
}
type observationState struct {
	mu sync.Mutex
	// emitMu serializes sampling/emission with removal so a stale sample cannot
	// restore a worker's gauges after cleanup. Reporter calls hold no state lock.
	emitMu                sync.Mutex
	shards                map[string]*shardObservation
	started               time.Time
	lastProgress          time.Time
	lastCheckpointFailure error
}

func (s *observationState) begin(shard string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shards == nil {
		s.shards = make(map[string]*shardObservation)
	}
	s.shards[shard] = &shardObservation{started: time.Now()}
}
func (s *observationState) setTracker(shard string, t *ackTracker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.shards[shard]; state != nil {
		state.tracker = t
	}
}
func (s *observationState) accept(shard, sequence string, count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.shards[shard]; state != nil {
		state.health.AcceptedSequence = sequence
		state.accepted += count
	}
}
func (s *observationState) checkpointResult(shard string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.lastCheckpointFailure = err
	}
	if state := s.shards[shard]; state != nil {
		h := &state.health.Checkpoint
		if err != nil {
			h.ConsecutiveFailures++
			h.LastError = err
			h.LastFailure = err
		} else {
			h.ConsecutiveFailures = 0
			h.LastError = nil
			h.LastSuccess = time.Now()
		}
	}
}
func (c *Consumer) recordPersisted(shard, sequence string, complete bool) {
	s := &c.observation
	s.mu.Lock()
	var delta uint64
	if state := s.shards[shard]; state != nil {
		state.health.Completed = complete
		if state.tracker == nil {
			delta = state.accepted - state.health.PersistedRecords
			if delta > 0 {
				state.progress = time.Now()
				state.health.Checkpoint.LastProgress = state.progress
				s.lastProgress = state.progress
			}
			state.health.PersistedRecords = state.accepted
			state.health.PersistedSequence = sequence
		}
	}
	s.mu.Unlock()
	if delta > 0 {
		c.reporter.Counter(metricRecordsCheckpointed, int64(delta), c.shardTags(shard))
	}
}
func (c *Consumer) recordExplicitPersisted(shard, sequence string, completed, delta uint64) {
	s := &c.observation
	s.mu.Lock()
	if state := s.shards[shard]; state != nil {
		if sequence != "" {
			state.health.PersistedSequence = sequence
		}
		state.health.PersistedRecords = completed
		if delta > 0 {
			state.progress = time.Now()
			state.health.Checkpoint.LastProgress = state.progress
			s.lastProgress = state.progress
		}
	}
	s.mu.Unlock()
	if delta > 0 {
		c.reporter.Counter(metricRecordsCheckpointed, int64(delta), c.shardTags(shard))
	}
}
func (c *Consumer) observationSnapshot(now time.Time) (map[string]ShardHealth, PressureHealth) {
	s := &c.observation
	s.mu.Lock()
	defer s.mu.Unlock()
	pressures, total := c.admission.pressure(now)
	shards := make(map[string]ShardHealth, len(s.shards))
	for shard, state := range s.shards {
		h := state.health
		if state.tracker != nil {
			if sequence, _ := state.tracker.progress(); sequence != "" {
				h.AcceptedSequence = sequence
			}
		}
		anchor := state.progress
		if anchor.IsZero() {
			anchor = state.started
		}
		h.TimeSinceCheckpointProgress = max(0, now.Sub(anchor))
		h.Pressure = pressures[shard]
		shards[shard] = h
	}
	return shards, total
}

func (c *Consumer) emitPressure(p PressureHealth, tags []metrics.Tag) {
	for name, value := range map[string]float64{
		metricUnacknowledgedRecords: float64(p.UnacknowledgedRecords), metricUnacknowledgedBytes: float64(p.UnacknowledgedBytes),
		metricOldestUnacknowledgedAge: p.OldestUnacknowledgedAge.Seconds(), metricStagedBytes: float64(p.StagedBytes),
		metricFetchSlots: float64(p.FetchSlots), metricPausedShards: float64(p.PausedShards),
		metricPauseDuration: p.PauseDuration.Seconds(), metricPausedSeconds: p.PausedDuration.Seconds(),
	} {
		c.reporter.Gauge(name, value, tags)
	}
	for _, reason := range pauseReasons {
		reasonTags := append(append([]metrics.Tag(nil), tags...), metrics.Tag{Key: metricTagReason, Value: reason})
		c.reporter.Gauge(metricPauseDuration, p.Pauses[reason].Current.Seconds(), reasonTags)
		c.reporter.Gauge(metricPausedSeconds, p.Pauses[reason].Total.Seconds(), reasonTags)
	}
}
func (c *Consumer) emitObservations() {
	c.observation.emitMu.Lock()
	defer c.observation.emitMu.Unlock()
	shards, total := c.observationSnapshot(time.Now())
	c.emitPressure(total, c.streamTags())
	_, age := c.observation.progressSnapshot(time.Now())
	c.reporter.Gauge(metricCheckpointProgressAge, age.Seconds(), c.streamTags())
	for shard, h := range shards {
		c.emitPressure(h.Pressure, c.shardTags(shard))
		c.reporter.Gauge(metricCheckpointProgressAge, h.TimeSinceCheckpointProgress.Seconds(), c.shardTags(shard))
	}
}
func (c *Consumer) finishObservations(shard string) {
	s := &c.observation
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	s.mu.Lock()
	delete(s.shards, shard)
	s.mu.Unlock()
	if c.admission != nil {
		c.admission.mu.Lock()
		delete(c.admission.pauses, shard)
		c.admission.mu.Unlock()
	}
	c.emitPressure(PressureHealth{}, c.shardTags(shard))
	c.reporter.Gauge(metricCheckpointProgressAge, 0, c.shardTags(shard))
	c.reporter.Gauge(metricMillisBehindLatest, 0, c.shardTags(shard))
}

// Reporting lives through graceful drain and has no dependency on worker I/O.
func (c *Consumer) startObservations() func() {
	c.observation.mu.Lock()
	c.observation.started = time.Now()
	c.observation.mu.Unlock()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.emitObservations()
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		c.emitPressure(PressureHealth{}, c.streamTags())
		c.reporter.Gauge(metricCheckpointProgressAge, 0, c.streamTags())
	}
}

// Recovery seeds known progress without claiming a new write or covered record.
func (s *observationState) recovered(shard, sequence string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.shards[shard]; state != nil && state.health.PersistedSequence == "" {
		state.health.PersistedSequence = sequence
		state.health.AcceptedSequence = sequence
	}
}

func (s *observationState) progressSnapshot(now time.Time) (time.Time, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	anchor := s.lastProgress
	if anchor.IsZero() {
		anchor = s.started
	}
	if anchor.IsZero() {
		return s.lastProgress, 0
	}
	return s.lastProgress, max(0, now.Sub(anchor))
}

func (s *observationState) checkpointFailure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCheckpointFailure
}
