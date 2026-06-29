package consumer

import "time"

func pickRebalanceDonor(
	ownerCounts map[string]int,
	high int,
	self string,
) string {
	bestOwner := ""
	bestCount := 0

	for owner, count := range ownerCounts {
		if owner == self || count <= high {
			continue
		}
		if bestOwner == "" || count > bestCount || (count == bestCount && owner < bestOwner) {
			bestOwner = owner
			bestCount = count
		}
	}

	return bestOwner
}

func pickRebalanceShard(
	candidates []string,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	now time.Time,
) string {
	for _, shardID := range candidates {
		if rebalanceShardInCooldown(shardID, cooldown, now) {
			continue
		}
		if workers != nil && workers.has(shardID) {
			continue
		}
		return shardID
	}

	return ""
}

func rebalanceShardInCooldown(
	shardID string,
	cooldown map[string]time.Time,
	now time.Time,
) bool {
	until, ok := cooldown[shardID]
	return ok && now.Before(until)
}

func removeRebalanceShard(
	shards []string,
	target string,
) []string {
	for i, shardID := range shards {
		if shardID == target {
			return append(shards[:i], shards[i+1:]...)
		}
	}

	return shards
}

func selectLocalRebalanceShedShards(
	snapshot rebalanceOwnershipSnapshot,
	self string,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	now time.Time,
	maxMoves int,
) []string {
	if workers == nil || maxMoves <= 0 {
		return nil
	}

	excess := snapshot.ownerCounts[self] - snapshot.high
	if excess <= 0 {
		return nil
	}

	limit := excess
	if maxMoves < limit {
		limit = maxMoves
	}

	shards := make([]string, 0, limit)
	for _, shardID := range snapshot.ourShards {
		if len(shards) >= limit {
			break
		}
		if shardID == "" {
			continue
		}
		if rebalanceShardInCooldown(shardID, cooldown, now) {
			continue
		}
		if !workers.has(shardID) {
			continue
		}
		shards = append(shards, shardID)
	}

	return shards
}
