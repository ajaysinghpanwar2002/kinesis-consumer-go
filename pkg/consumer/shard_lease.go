package consumer

import (
	"context"
	"fmt"

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
