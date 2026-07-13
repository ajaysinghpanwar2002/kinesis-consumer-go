package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) acquireShardLease(ctx context.Context, shardID string) (lease.Lease, bool, error) {
	start := time.Now()
	shardLease, acquired, err := c.leaseManager.Acquire(
		ctx,
		c.streamKey(),
		shardID,
		c.leaseOwner,
		c.tuning.heartbeatTTL,
	)
	if err != nil {
		return nil, false, fmt.Errorf("acquire shard lease %s: %w", shardID, err)
	}
	c.reporter.Timing(metricLeaseAcquireDuration, time.Since(start), c.shardTags(shardID))
	return shardLease, acquired, nil
}

func (c *Consumer) acquireShardLeaseWithRetry(ctx context.Context, shardID string) (lease.Lease, bool, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		shardLease, acquired, err := c.acquireShardLease(ctx, shardID)
		if err == nil {
			return shardLease, acquired, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return nil, false, err
		}
	}

	return nil, false, lastErr
}

func (c *Consumer) claimShardLease(ctx context.Context, shardID, expectedOwner string) (lease.Lease, bool, error) {
	shardLease, claimed, err := c.leaseManager.Claim(
		ctx,
		c.streamKey(),
		shardID,
		expectedOwner,
		c.leaseOwner,
		c.tuning.heartbeatTTL,
	)
	if err != nil {
		return nil, false, fmt.Errorf("claim shard lease %s from %s: %w", shardID, expectedOwner, err)
	}
	return shardLease, claimed, nil
}

func (c *Consumer) claimShardLeaseWithRetry(ctx context.Context, shardID, expectedOwner string) (lease.Lease, bool, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		shardLease, claimed, err := c.claimShardLease(ctx, shardID, expectedOwner)
		if err == nil {
			return shardLease, claimed, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return nil, false, err
		}
	}

	return nil, false, lastErr
}

func (c *Consumer) acquireShardLeases(ctx context.Context, shardIDs []string) (map[string]lease.Lease, error) {
	leases := make(map[string]lease.Lease, len(shardIDs))
	for _, shardID := range shardIDs {
		shardLease, acquired, err := c.acquireShardLeaseWithRetry(ctx, shardID)
		if err != nil {
			return nil, fmt.Errorf("acquire shard leases %s: %w", shardID, err)
		}
		if !acquired || shardLease == nil {
			continue
		}
		leases[shardID] = shardLease
		c.reporter.Counter(metricLeaseAcquired, 1, c.shardTags(shardID))
		c.logger.Debug("shard lease acquired", slog.String("shard", shardID), slog.String("owner", c.leaseOwner))
	}
	return leases, nil
}

func (c *Consumer) renewShardLease(ctx context.Context, shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	if err := shardLease.Renew(ctx, c.tuning.heartbeatTTL); err != nil {
		return fmt.Errorf("renew shard lease %s: %w", shardID, err)
	}
	return nil
}

func (c *Consumer) renewShardLeaseLoop(ctx context.Context, shardID string, shardLease lease.Lease) error {
	ticker := time.NewTicker(c.tuning.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			if err := c.renewShardLease(ctx, shardID, shardLease); err != nil {
				if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					// Shutdown cancellation, not a renewal failure: count neither.
					if errors.Is(ctx.Err(), context.Canceled) {
						return nil
					}
					return ctx.Err()
				}
				c.reporter.Counter(metricLeaseRenewalFailures, 1, c.shardTags(shardID))
				return err
			}
			c.reporter.Counter(metricLeaseRenewals, 1, c.shardTags(shardID))
		}
	}
}

func (c *Consumer) releaseShardLease(ctx context.Context, shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	if err := shardLease.Release(ctx); err != nil {
		return fmt.Errorf("release shard lease %s: %w", shardID, err)
	}
	return nil
}

func (c *Consumer) releaseShardLeaseWithTimeout(shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	releaseCtx, cancel := context.WithTimeout(context.Background(), c.shardLeaseReleaseTimeout())
	defer cancel()

	if err := shardLease.Release(releaseCtx); err != nil {
		releaseErr := fmt.Errorf("release shard lease %s: %w", shardID, err)
		if errors.Is(err, context.DeadlineExceeded) {
			releaseErr = fmt.Errorf("release shard lease %s timed out: %w", shardID, err)
		}
		// Logged here because the caller (shard_worker.go) discards this error
		// when the worker already failed, so it would otherwise be invisible.
		c.reporter.Counter(metricLeaseReleaseFailures, 1, c.shardTags(shardID))
		c.logger.Warn("shard lease release failed", slog.String("shard", shardID), slog.Any("error", releaseErr))
		return releaseErr
	}
	c.reporter.Counter(metricLeaseReleased, 1, c.shardTags(shardID))
	c.logger.Debug("shard lease released", slog.String("shard", shardID))
	return nil
}

func (c *Consumer) shardLeaseReleaseTimeout() time.Duration {
	if c == nil || c.tuning.shardLeaseReleaseTimeout <= 0 {
		return 5 * time.Second
	}
	return c.tuning.shardLeaseReleaseTimeout
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
