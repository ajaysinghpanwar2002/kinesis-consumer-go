package consumer

import (
	"errors"
	"log/slog"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

// Option configures optional consumer behavior.
type Option func(*options) error

type options struct {
	batchHandler      BatchHandlerFunc
	failurePolicy     FailurePolicy
	dlqPublisher      DLQPublisher
	dlqRetryAttempts  int
	dlqRetryBackoff   time.Duration
	dlqAttemptTimeout time.Duration
	lease             leaseOptions
	shutdown          shutdownOptions
	tuning            tuningConfig
	logger            *slog.Logger
	reporter          metrics.Reporter
}

type leaseOptions struct {
	manager lease.Manager
}

type shutdownOptions struct {
	gracefulDrain        bool
	gracefulDrainTimeout time.Duration
}

func defaultOptions() options {
	return options{
		failurePolicy:     FailurePolicyFailFast,
		dlqRetryAttempts:  defaultDLQRetryAttempts,
		dlqRetryBackoff:   defaultDLQRetryBackoff,
		dlqAttemptTimeout: defaultDLQAttemptTimeout,
		tuning:            defaultTuning(),
		// Discard by default so the library stays silent unless the caller
		// opts in via WithLogger. A non-nil logger is always present so call
		// sites never need a nil check.
		logger: slog.New(slog.DiscardHandler),
		// No-op by default so the library emits nothing unless the caller opts
		// in via WithMetrics. A non-nil reporter is always present so emission
		// sites never need a nil check.
		reporter: metrics.Nop{},
	}
}

func applyOptions(opts []Option) (options, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return options{}, err
		}
	}
	return cfg, nil
}

// WithBatchHandler switches the consumer to call the provided batch handler
// once per GetRecords response instead of invoking the per-record handler.
func WithBatchHandler(handler BatchHandlerFunc) Option {
	if handler == nil {
		return func(*options) error {
			return errors.New("batch handler cannot be nil")
		}
	}
	return func(cfg *options) error {
		cfg.batchHandler = handler
		return nil
	}
}

// WithRetry overrides retry attempts and backoff for handlers.
func WithRetry(maxAttempts int, backoff time.Duration) Option {
	return func(cfg *options) error {
		if maxAttempts < 1 {
			return errors.New("maxAttempts must be >= 1")
		}
		if backoff <= 0 {
			return errors.New("backoff must be > 0")
		}
		cfg.tuning.retryMaxAttempts = maxAttempts
		cfg.tuning.retryBackoff = backoff
		return nil
	}
}

// WithPolling overrides GetRecords polling intervals.
func WithPolling(pollInterval, shardSyncInterval time.Duration) Option {
	return func(cfg *options) error {
		if pollInterval <= 0 {
			return errors.New("pollInterval must be > 0")
		}
		if shardSyncInterval < time.Second {
			return errors.New("shardSyncInterval must be >= 1s")
		}
		cfg.tuning.pollInterval = pollInterval
		cfg.tuning.shardSyncInterval = shardSyncInterval
		return nil
	}
}

// WithBatching adjusts GetRecords batch size and checkpoint frequency.
func WithBatching(batchSize int32, checkpointEvery int) Option {
	return func(cfg *options) error {
		if err := validateBatchSize(batchSize); err != nil {
			return err
		}
		if checkpointEvery < 1 {
			return errors.New("checkpointEvery must be >= 1")
		}
		cfg.tuning.batchSize = batchSize
		cfg.tuning.checkpointEvery = checkpointEvery
		return nil
	}
}

// WithShardConcurrency sets per-shard goroutines used for record handler
// processing. Values above 1 trade ordering for throughput: records within a
// page are handled concurrently, so strict per-shard (and therefore
// per-partition-key) processing order is no longer preserved — later records
// may complete before earlier ones. Keep the default of 1 when partition-key
// ordering matters. Applies only to record handlers; batch handlers always
// receive whole pages sequentially.
func WithShardConcurrency(concurrency int) Option {
	return func(cfg *options) error {
		if concurrency < 1 {
			return errors.New("shardConcurrency must be >= 1")
		}
		cfg.tuning.shardConcurrency = concurrency
		return nil
	}
}

// WithLeaseManager enables shard leasing for multi-consumer coordination.
//
// A manager supplied here stays caller-owned: the consumer never closes it,
// not even from Consumer.Close. Omit this option with a store that implements
// lease.Provider to let the consumer create — and own — the manager itself.
func WithLeaseManager(manager lease.Manager) Option {
	return func(cfg *options) error {
		if manager == nil {
			return errors.New("lease manager cannot be nil")
		}
		cfg.lease.manager = manager
		return nil
	}
}

// WithRebalance tunes how shard leases rebalance across workers.
func WithRebalance(minInterval, jitter, cooldown time.Duration, maxMoves int) Option {
	return func(cfg *options) error {
		if minInterval <= 0 {
			return errors.New("rebalance minInterval must be > 0")
		}
		if jitter < 0 {
			return errors.New("rebalance jitter cannot be negative")
		}
		if cooldown <= 0 {
			return errors.New("rebalance cooldown must be > 0")
		}
		if maxMoves < 1 {
			return errors.New("rebalance maxMoves must be >= 1")
		}
		cfg.tuning.rebalanceIntervalMin = minInterval
		cfg.tuning.rebalanceIntervalJitter = jitter
		cfg.tuning.shardCooldownPeriod = cooldown
		cfg.tuning.maxMovesPerRebalance = maxMoves
		return nil
	}
}

// WithIdleTimeBetweenReads sets the minimum delay between successive
// GetRecords calls on one shard (the KCL idleTimeBetweenReadsInMillis
// equivalent). It paces catch-up reads under the Kinesis 5 reads/sec/shard
// limit; 0 disables pacing.
func WithIdleTimeBetweenReads(d time.Duration) Option {
	return func(cfg *options) error {
		if d < 0 {
			return errors.New("idle time between reads cannot be negative")
		}
		cfg.tuning.idleTimeBetweenReads = d
		return nil
	}
}

// WithShardSyncMaxStaleness bounds how long the consumer keeps running on a
// stale shard map. Periodic shard-sync failures (Kinesis listing, checkpoint
// readiness reads, lease listing) are survivable: existing workers keep
// delivering and discovery is retried with capped backoff. But once the time
// since the last successful sync exceeds this bound, resharding may have
// become invisible, so Start returns the causal error wrapped in
// ErrShardSyncStale.
//
// The default is 10x the shard sync interval (10m with default polling). The
// value must be at least the shard sync interval configured via WithPolling.
func WithShardSyncMaxStaleness(maxStaleness time.Duration) Option {
	return func(cfg *options) error {
		if maxStaleness <= 0 {
			return errors.New("shard sync max staleness must be > 0")
		}
		cfg.tuning.shardSyncMaxStaleness = maxStaleness
		return nil
	}
}

// WithHeartbeat configures the worker heartbeat interval and TTL. Both values
// must be whole milliseconds because the built-in Valkey backend stores TTLs
// with millisecond precision.
func WithHeartbeat(interval, ttl time.Duration) Option {
	return func(cfg *options) error {
		if err := validateHeartbeatDurations(interval, ttl); err != nil {
			return err
		}
		cfg.tuning.heartbeatInterval = interval
		cfg.tuning.heartbeatTTL = ttl
		return nil
	}
}

// WithFailurePolicy sets behavior when a handler keeps failing after retries.
// The default is FailurePolicyFailFast. FailurePolicySkip is an intentionally
// lossy opt-in that checkpoints past failed records or batches.
func WithFailurePolicy(policy FailurePolicy) Option {
	return func(cfg *options) error {
		if err := policy.validate(); err != nil {
			return err
		}
		cfg.failurePolicy = policy
		return nil
	}
}

// WithDLQPublisher configures an optional poison-record DLQ publisher.
func WithDLQPublisher(publisher DLQPublisher) Option {
	return func(cfg *options) error {
		if publisher == nil {
			return errors.New("dlq publisher cannot be nil")
		}
		cfg.dlqPublisher = publisher
		return nil
	}
}

// WithDLQRetry overrides poison-record publish attempts and linear backoff.
// These settings are independent from WithRetry, which controls handler and
// coordination retries. The default is 3 attempts with a 1 second base
// backoff; the sleep before attempt n is (n-1)*backoff.
func WithDLQRetry(maxAttempts int, backoff time.Duration) Option {
	return func(cfg *options) error {
		if maxAttempts < 1 {
			return errors.New("dlq maxAttempts must be >= 1")
		}
		if backoff <= 0 {
			return errors.New("dlq backoff must be > 0")
		}
		cfg.dlqRetryAttempts = maxAttempts
		cfg.dlqRetryBackoff = backoff
		return nil
	}
}

// WithDLQAttemptTimeout bounds each call to DLQPublisher.Publish. The default
// is 10 seconds. When a publisher ignores its context, the consumer abandons
// that attempt at the deadline; the publisher call may continue in a detached
// goroutine, but its late result cannot make the source page checkpointable.
func WithDLQAttemptTimeout(timeout time.Duration) Option {
	return func(cfg *options) error {
		if timeout <= 0 {
			return errors.New("dlq attempt timeout must be > 0")
		}
		cfg.dlqAttemptTimeout = timeout
		return nil
	}
}

// WithGracefulDrain enables shutdown drain mode.
//
// On shutdown cancellation, workers are allowed to finish in-flight work before
// Start returns. A zero timeout waits indefinitely.
func WithGracefulDrain(timeout time.Duration) Option {
	return func(cfg *options) error {
		if timeout < 0 {
			return errors.New("graceful drain timeout cannot be negative")
		}
		cfg.shutdown.gracefulDrain = true
		cfg.shutdown.gracefulDrainTimeout = timeout
		return nil
	}
}

// WithLogger sets the structured logger used for consumer lifecycle events.
//
// The default is a discard logger, so the library is silent unless a logger is
// provided here. Its slog.Handler must be safe for concurrent calls and return
// promptly; logging is synchronous and cannot be canceled by the consumer.
func WithLogger(logger *slog.Logger) Option {
	return func(cfg *options) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}
		cfg.logger = logger
		return nil
	}
}

// WithMetrics sets the reporter used for consumer metrics.
//
// The default is a no-op reporter, so the library emits no metrics unless a
// reporter is provided here.
func WithMetrics(reporter metrics.Reporter) Option {
	return func(cfg *options) error {
		if reporter == nil {
			return errors.New("metrics reporter cannot be nil")
		}
		cfg.reporter = reporter
		return nil
	}
}
