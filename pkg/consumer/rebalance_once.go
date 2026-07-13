package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type rebalanceRunResult struct {
	readyShardIDs []string
	plan          rebalancePlan
	started       int
	movedShardIDs []string
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
	if len(result.plan.actions) > 0 {
		c.logger.Debug("rebalance plan",
			slog.Int("shards", result.plan.snapshot.open),
			slog.Int("workers", result.plan.snapshot.activeWorkers),
			slog.Int("low", result.plan.snapshot.low),
			slog.Int("high", result.plan.snapshot.high),
			slog.Int("owned", result.plan.initialCount),
			slog.Int("actions", len(result.plan.actions)),
		)
	}
	executionResult, err := c.executeRebalancePlan(ctx, result.plan, workers, workerWG, workerErrCh, stopRun)
	result.started = executionResult.started
	result.movedShardIDs = executionResult.movedShardIDs
	recordRebalanceCooldown(cooldown, result.movedShardIDs, now, c.tuning.shardCooldownPeriod)
	if err != nil {
		return result, err
	}

	remainingMoves := c.tuning.maxMovesPerRebalance - len(result.movedShardIDs)
	shedShardIDs := selectLocalRebalanceShedShards(
		result.plan.snapshot,
		c.leaseOwner,
		cooldown,
		workers,
		now,
		remainingMoves,
	)
	stoppedShardIDs := executeLocalRebalanceShedShards(shedShardIDs, workers)
	for _, shardID := range stoppedShardIDs {
		c.logger.Info("rebalance shard shed",
			slog.String("shard", shardID),
			slog.Int("owned", result.plan.snapshot.ownerCounts[c.leaseOwner]),
			slog.Int("high", result.plan.snapshot.high),
		)
	}
	result.movedShardIDs = append(result.movedShardIDs, stoppedShardIDs...)
	recordRebalanceCooldown(cooldown, stoppedShardIDs, now, c.tuning.shardCooldownPeriod)
	return result, nil
}

func recordRebalanceCooldown(
	cooldown map[string]time.Time,
	shardIDs []string,
	now time.Time,
	period time.Duration,
) {
	if cooldown == nil || period <= 0 {
		return
	}
	until := now.Add(period)
	for _, shardID := range shardIDs {
		if shardID == "" {
			continue
		}
		cooldown[shardID] = until
	}
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
