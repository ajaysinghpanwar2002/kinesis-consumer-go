package consumer

import (
	"errors"
	"time"
)

type tuningConfig struct {
	shardConcurrency         int
	batchSize                int32
	pollInterval             time.Duration
	idleTimeBetweenReads     time.Duration
	shardSyncInterval        time.Duration
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
	if t.batchSize < 1 {
		return errors.New("batch size must be >= 1")
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
	if t.heartbeatInterval <= 0 {
		return errors.New("heartbeat interval must be > 0")
	}
	if t.heartbeatTTL <= 0 {
		return errors.New("heartbeat ttl must be > 0")
	}
	// A renew tick must land well inside the TTL, or every lease expires
	// before its renewal and peers systematically claim shards away from a
	// live worker (dual ownership).
	if t.heartbeatInterval >= t.heartbeatTTL {
		return errors.New("heartbeat interval must be < heartbeat ttl (recommend ttl >= 3x interval)")
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
