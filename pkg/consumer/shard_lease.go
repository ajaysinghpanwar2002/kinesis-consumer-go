package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) acquireShardLease(ctx context.Context, shardID string) (lease.Lease, bool, error) {
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
					if errors.Is(ctx.Err(), context.Canceled) {
						return nil
					}
					return ctx.Err()
				}
				return err
			}
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
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("release shard lease %s timed out: %w", shardID, err)
		}
		return fmt.Errorf("release shard lease %s: %w", shardID, err)
	}
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
