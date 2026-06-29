package consumer

import (
	"context"
	"fmt"
	"sync"
)

type rebalanceExecutionResult struct {
	started       int
	movedShardIDs []string
}

func (c *Consumer) executeRebalancePlan(
	ctx context.Context,
	plan rebalancePlan,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) (rebalanceExecutionResult, error) {
	var result rebalanceExecutionResult
	for _, action := range plan.actions {
		actionStarted, err := c.executeRebalancePlanAction(ctx, action, workers, workerWG, workerErrCh, stopRun)
		if err != nil {
			return result, err
		}
		if actionStarted {
			result.started++
			result.movedShardIDs = append(result.movedShardIDs, action.shardID)
		}
	}
	return result, nil
}

func executeLocalRebalanceShedShards(
	shardIDs []string,
	workers *shardWorkerSet,
) []string {
	if workers == nil {
		return nil
	}

	stopped := make([]string, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardID == "" {
			continue
		}
		if workers.stop(shardID) {
			stopped = append(stopped, shardID)
		}
	}
	return stopped
}

func (c *Consumer) executeRebalancePlanAction(
	ctx context.Context,
	action rebalancePlanAction,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) (bool, error) {
	if action.shardID == "" {
		return false, fmt.Errorf("execute rebalance %s action: missing shard ID", action.kind)
	}
	if workers.has(action.shardID) {
		return false, nil
	}

	switch action.kind {
	case rebalancePlanAcquireUnowned:
		return c.executeRebalanceAcquireAction(ctx, action.shardID, workers, workerWG, workerErrCh, stopRun)
	case rebalancePlanClaimDonor:
		if action.donor == "" {
			return false, fmt.Errorf("execute rebalance claim action for shard %s: missing donor", action.shardID)
		}
		return c.executeRebalanceClaimAction(ctx, action.shardID, action.donor, workers, workerWG, workerErrCh, stopRun)
	default:
		return false, fmt.Errorf("execute rebalance action for shard %s: unknown action kind %q", action.shardID, action.kind)
	}
}

func (c *Consumer) executeRebalanceAcquireAction(
	ctx context.Context,
	shardID string,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) (bool, error) {
	shardLease, acquired, err := c.acquireShardLeaseWithRetry(ctx, shardID)
	if err != nil {
		return false, fmt.Errorf("execute rebalance acquire shard %s: %w", shardID, err)
	}
	if !acquired || shardLease == nil {
		return false, nil
	}

	c.startRegisteredShardWorker(ctx, shardID, shardLease, workers, workerWG, workerErrCh, stopRun)
	return true, nil
}

func (c *Consumer) executeRebalanceClaimAction(
	ctx context.Context,
	shardID string,
	donor string,
	workers *shardWorkerSet,
	workerWG *sync.WaitGroup,
	workerErrCh chan<- error,
	stopRun context.CancelFunc,
) (bool, error) {
	shardLease, claimed, err := c.claimShardLeaseWithRetry(ctx, shardID, donor)
	if err != nil {
		return false, fmt.Errorf("execute rebalance claim shard %s from %s: %w", shardID, donor, err)
	}
	if !claimed || shardLease == nil {
		return false, nil
	}

	c.startRegisteredShardWorker(ctx, shardID, shardLease, workers, workerWG, workerErrCh, stopRun)
	return true, nil
}
