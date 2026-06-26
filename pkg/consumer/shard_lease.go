package consumer

import (
	"context"
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

func (c *Consumer) releaseShardLease(ctx context.Context, shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	if err := shardLease.Release(ctx); err != nil {
		return fmt.Errorf("release shard lease %s: %w", shardID, err)
	}
	return nil
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
