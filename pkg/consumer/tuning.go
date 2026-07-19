package consumer

import (
	"errors"
	"time"
)

const maxKinesisGetRecordsBatchSize int32 = 10_000

type tuningConfig struct {
	shardConcurrency         int
	batchSize                int32
	pollInterval             time.Duration
	idleTimeBetweenReads     time.Duration
	shardSyncInterval        time.Duration
	shardSyncMaxStaleness    time.Duration
	retryMaxAttempts         int
	retryBackoff             time.Duration
	checkpointEvery          int
	rebalanceIntervalMin     time.Duration
	rebalanceIntervalJitter  time.Duration
	heartbeatInterval        time.Duration
	heartbeatTTL             time.Duration
	shardLeaseReleaseTimeout time.Duration
	shardCooldownPeriod      time.Duration
	maxMovesPerRebalance     int
}

func defaultTuning() tuningConfig {
	return tuningConfig{
		shardConcurrency:         1,
		batchSize:                100,
		pollInterval:             time.Second,
		idleTimeBetweenReads:     200 * time.Millisecond,
		shardSyncInterval:        time.Minute,
		retryMaxAttempts:         3,
		retryBackoff:             time.Second,
		checkpointEvery:          100,
		rebalanceIntervalMin:     10 * time.Second,
		rebalanceIntervalJitter:  10 * time.Second,
		heartbeatInterval:        5 * time.Second,
		heartbeatTTL:             20 * time.Second,
		shardLeaseReleaseTimeout: 5 * time.Second,
		shardCooldownPeriod:      10 * time.Second,
		maxMovesPerRebalance:     2,
	}
}

func (t tuningConfig) validate() error {
	if err := validateBatchSize(t.batchSize); err != nil {
		return err
	}
	if t.shardConcurrency < 1 {
		return errors.New("shardConcurrency must be >= 1")
	}
	if t.pollInterval <= 0 {
		return errors.New("pollInterval must be > 0")
	}
	if t.idleTimeBetweenReads < 0 {
		return errors.New("idle time between reads cannot be negative")
	}
	if t.shardSyncInterval < time.Second {
		return errors.New("shardSyncInterval must be >= 1s")
	}
	// Zero means "derive the default in New" (and, for direct construction,
	// "staleness policy disabled"); an explicit value below the sync interval
	// would declare the shard map stale before a single failed pass could
	// even be retried.
	if t.shardSyncMaxStaleness != 0 && t.shardSyncMaxStaleness < t.shardSyncInterval {
		return errors.New("shard sync max staleness must be >= shardSyncInterval")
	}
	if t.retryMaxAttempts < 1 {
		return errors.New("retry max attempts must be >= 1")
	}
	if t.retryBackoff <= 0 {
		return errors.New("retry backoff must be > 0")
	}
	if t.checkpointEvery < 1 {
		return errors.New("checkpointEvery must be >= 1")
	}
	if t.rebalanceIntervalMin <= 0 {
		return errors.New("rebalance min interval must be > 0")
	}
	if t.rebalanceIntervalJitter < 0 {
		return errors.New("rebalance jitter cannot be negative")
	}
	if err := validateHeartbeatDurations(t.heartbeatInterval, t.heartbeatTTL); err != nil {
		return err
	}
	if t.shardLeaseReleaseTimeout <= 0 {
		return errors.New("shard lease release timeout must be > 0")
	}
	if t.shardCooldownPeriod <= 0 {
		return errors.New("shard cooldown must be > 0")
	}
	if t.maxMovesPerRebalance < 1 {
		return errors.New("maxMovesPerRebalance must be >= 1")
	}
	return nil
}

func validateBatchSize(batchSize int32) error {
	if batchSize < 1 || batchSize > maxKinesisGetRecordsBatchSize {
		return errors.New("batch size must be between 1 and 10000")
	}
	return nil
}

func validateHeartbeatDurations(interval, ttl time.Duration) error {
	if interval < time.Millisecond {
		return errors.New("heartbeat interval must be >= 1ms")
	}
	if interval%time.Millisecond != 0 {
		return errors.New("heartbeat interval must be a whole number of milliseconds")
	}
	if ttl < time.Millisecond {
		return errors.New("heartbeat ttl must be >= 1ms")
	}
	if ttl%time.Millisecond != 0 {
		return errors.New("heartbeat ttl must be a whole number of milliseconds")
	}
	// A heartbeat/renew call can itself run for up to one interval, and
	// staleness is only checked after it returns. After a success at t=0 the
	// next call can start near t=interval and return near t=2*interval, so it
	// must finish before the documented safety deadline ttl-interval — which
	// requires ttl >= 3*interval. A looser ratio lets a lease expire (and a peer
	// reclaim the shard) while the old worker is still inside an in-flight call,
	// widening the split-processing window beyond the contract. Compared
	// overflow-safe as interval > ttl/3, which is exactly equivalent to
	// 3*interval > ttl for the positive, whole-millisecond durations validated
	// above and never multiplies, so 3*interval cannot wrap time.Duration. This
	// subsumes interval < ttl (ttl >= 3*interval implies ttl > interval).
	if interval > ttl/3 {
		return errors.New("heartbeat ttl must be >= 3x heartbeat interval")
	}
	// renewShardLeaseLoopWithWatchdog derives its first watchdog deadline by
	// adding these durations. Check before adding so an accepted configuration
	// cannot wrap negative and later panic in time.NewTimer.
	if ttl > time.Duration(1<<63-1)-interval {
		return errors.New("heartbeat ttl + interval overflows time.Duration")
	}
	return nil
}
