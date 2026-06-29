package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type rebalanceRunResult struct {
	readyShardIDs []string
	plan          rebalancePlan
	started       int
}

func (c *Consumer) rebalanceShardsOnce(
	ctx context.Context,
	knownShards map[string]types.Shard,
	completionState *shardCompletionState,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
	now time.Time,
) (rebalanceRunResult, error) {
	var result rebalanceRunResult

	readyShardIDs, err := completionState.readyShardIDs(ctx, c, knownShards)
	if err != nil {
		return result, err
	}
	result.readyShardIDs = readyShardIDs
	if len(readyShardIDs) == 0 {
		return result, nil
	}

	leaseOwners, err := c.listRebalanceLeaseOwnersWithRetry(ctx)
	if err != nil {
		return result, err
	}
	workerOwners, err := c.listRebalanceWorkerOwnersWithRetry(ctx)
	if err != nil {
		return result, err
	}

	result.plan = buildLocalRebalancePlan(
		readyShardIDs,
		leaseOwners,
		workerOwners,
		c.leaseOwner,
		cooldown,
		workers,
		now,
		c.tuning.maxMovesPerRebalance,
	)
	result.started, err = c.executeRebalancePlan(ctx, result.plan, workers, workerWG, workerErrCh, stopRun)
	if err != nil {
		return result, err
	}
	return result, nil
}

func (c *Consumer) listRebalanceLeaseOwners(ctx context.Context) (map[string]string, error) {
	leaseOwners, err := c.leaseManager.List(ctx, c.streamKey())
	if err != nil {
		return nil, fmt.Errorf("list rebalance lease owners: %w", err)
	}
	return leaseOwners, nil
}

func (c *Consumer) listRebalanceLeaseOwnersWithRetry(ctx context.Context) (map[string]string, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		leaseOwners, err := c.listRebalanceLeaseOwners(ctx)
		if err == nil {
			return leaseOwners, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return nil, err
		}
	}

	return nil, lastErr
}

func (c *Consumer) listRebalanceWorkerOwners(ctx context.Context) ([]string, error) {
	workerOwners, err := c.leaseManager.Workers(ctx, c.streamKey())
	if err != nil {
		return nil, fmt.Errorf("list rebalance worker owners: %w", err)
	}
	return workerOwners, nil
}

func (c *Consumer) listRebalanceWorkerOwnersWithRetry(ctx context.Context) ([]string, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		workerOwners, err := c.listRebalanceWorkerOwners(ctx)
		if err == nil {
			return workerOwners, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return nil, err
		}
	}

	return nil, lastErr
}
