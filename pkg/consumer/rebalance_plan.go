package consumer

import "time"

type rebalancePlanActionKind string

const (
	rebalancePlanAcquireUnowned rebalancePlanActionKind = "acquire_unowned"
	rebalancePlanClaimDonor     rebalancePlanActionKind = "claim_donor"
)

type rebalancePlanAction struct {
	kind    rebalancePlanActionKind
	shardID string
	donor   string
}

type rebalancePlan struct {
	snapshot       rebalanceOwnershipSnapshot
	initialCount   int
	projectedCount int
	actions        []rebalancePlanAction
}

func buildLocalRebalancePlan(
	readyShardIDs []string,
	leaseOwners map[string]string,
	workerOwners []string,
	self string,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	now time.Time,
	maxMoves int,
) rebalancePlan {
	snapshot := buildRebalanceOwnershipSnapshot(readyShardIDs, leaseOwners, workerOwners, self)
	plan := rebalancePlan{
		snapshot:       snapshot,
		initialCount:   snapshot.ownerCounts[self],
		projectedCount: snapshot.ownerCounts[self],
	}
	if maxMoves <= 0 {
		return plan
	}

	ownerCounts := copyOwnerCounts(snapshot.ownerCounts)
	ownerShards := copyOwnerShards(snapshot.ownerShards)
	unowned := append([]string(nil), snapshot.unowned...)

	for plan.projectedCount < snapshot.low && len(plan.actions) < maxMoves {
		shardID := pickRebalanceShard(unowned, cooldown, workers, now)
		if shardID != "" {
			unowned = removeRebalanceShard(unowned, shardID)
			plan.actions = append(plan.actions, rebalancePlanAction{
				kind:    rebalancePlanAcquireUnowned,
				shardID: shardID,
			})
			plan.projectedCount++
			continue
		}

		// A starved worker may claim from any donor above `low`, not just
		// above `high`: requiring `count > high` leaves dead zones like
		// 2/2/0 (4 shards / 3 workers, high=2) frozen forever. Convergence
		// is safe — the taker only claims while below `low` and the donor
		// never drops below `low` (the threshold is strict).
		if !planClaimFromDonor(&plan, ownerCounts, ownerShards, cooldown, workers, now, snapshot.low, self, maxMoves) {
			break
		}
	}

	for plan.projectedCount < snapshot.high && len(plan.actions) < maxMoves {
		shardID := pickRebalanceShard(unowned, cooldown, workers, now)
		if shardID == "" {
			break
		}
		unowned = removeRebalanceShard(unowned, shardID)
		plan.actions = append(plan.actions, rebalancePlanAction{
			kind:    rebalancePlanAcquireUnowned,
			shardID: shardID,
		})
		plan.projectedCount++
	}

	return plan
}

func planClaimFromDonor(
	plan *rebalancePlan,
	ownerCounts map[string]int,
	ownerShards map[string][]string,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	now time.Time,
	donorThreshold int,
	self string,
	maxMoves int,
) bool {
	for len(plan.actions) < maxMoves {
		donor := pickRebalanceDonor(ownerCounts, donorThreshold, self)
		if donor == "" {
			return false
		}

		shardID := pickRebalanceShard(ownerShards[donor], cooldown, workers, now)
		if shardID == "" {
			delete(ownerCounts, donor)
			continue
		}

		ownerShards[donor] = removeRebalanceShard(ownerShards[donor], shardID)
		ownerCounts[donor]--
		if ownerCounts[donor] <= 0 {
			delete(ownerCounts, donor)
		}
		plan.actions = append(plan.actions, rebalancePlanAction{
			kind:    rebalancePlanClaimDonor,
			shardID: shardID,
			donor:   donor,
		})
		plan.projectedCount++
		return true
	}

	return false
}

func copyOwnerCounts(ownerCounts map[string]int) map[string]int {
	copied := make(map[string]int, len(ownerCounts))
	for owner, count := range ownerCounts {
		copied[owner] = count
	}
	return copied
}

func copyOwnerShards(ownerShards map[string][]string) map[string][]string {
	copied := make(map[string][]string, len(ownerShards))
	for owner, shards := range ownerShards {
		copied[owner] = append([]string(nil), shards...)
	}
	return copied
}
