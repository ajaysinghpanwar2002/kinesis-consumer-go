package consumer

import (
	"errors"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/lease"
)

// Option configures optional consumer features.
type Option func(*consumerOptions) error

type consumerOptions struct {
	batchHandler       BatchHandlerFunc
	leaseManager       lease.Manager
	leaseOwner         string
	leaseTTL           time.Duration
	leaseRenewInterval time.Duration
	leaseRetryInterval time.Duration
}

func applyOptions(opts []Option) (consumerOptions, error) {
	var cfg consumerOptions
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return consumerOptions{}, err
		}
	}
	return cfg, nil
}

// WithBatchHandler switches the consumer to call the provided batch handler
// once per GetRecords call instead of invoking the per-record handler.
func WithBatchHandler(handler BatchHandlerFunc) Option {
	if handler == nil {
		return func(*consumerOptions) error {
			return errors.New("batch handler cannot be nil")
		}
	}
	return func(cfg *consumerOptions) error {
		cfg.batchHandler = handler
		return nil
	}
}

// WithLeaseManager enables shard leasing for multi-consumer coordination.
// Provide a lease.Manager implementation plus optional owner/TTL timings.
// If owner is empty, a default will be generated. ttl: lease lifetime;
// renewInterval: how often to extend the lease; retryInterval: how often to
// retry when a shard is owned elsewhere.
func WithLeaseManager(manager lease.Manager, owner string, ttl, renewInterval, retryInterval time.Duration) Option {
	return func(cfg *consumerOptions) error {
		if manager == nil {
			return errors.New("lease manager cannot be nil")
		}
		if ttl <= 0 {
			ttl = 30 * time.Second
		}
		if renewInterval <= 0 || renewInterval >= ttl {
			renewInterval = ttl / 2
			if renewInterval < time.Second {
				renewInterval = time.Second
			}
		}
		if retryInterval <= 0 {
			retryInterval = 5 * time.Second
		}

		cfg.leaseManager = manager
		cfg.leaseOwner = owner
		cfg.leaseTTL = ttl
		cfg.leaseRenewInterval = renewInterval
		cfg.leaseRetryInterval = retryInterval
		return nil
	}
}
