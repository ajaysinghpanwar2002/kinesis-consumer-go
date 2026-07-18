package consumer

import "time"

// pickRebalanceDonor selects the peer to claim a shard from: the owner with
// the most shards (lexicographically smallest owner on ties), considering
// only owners strictly above threshold — a donor at the threshold would be
// pushed below it by the claim.
func pickRebalanceDonor(
	ownerCounts map[string]int,
	threshold int,
	self string,
) string {
	bestOwner := ""
	bestCount := 0

	for owner, count := range ownerCounts {
		if owner == self || count <= threshold {
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

// selectSyncAcquireShards picks the unowned shards the startup/shard-sync
// path may acquire: at most up to the fair-share high bound (greedy
// acquisition past it would just be shed again), skipping shards in
// post-move cooldown (or the same worker that shed a shard re-acquires it
// at the next sync tick) and shards with a live local worker.
func selectSyncAcquireShards(
	snapshot rebalanceOwnershipSnapshot,
	self string,
	cooldown map[string]time.Time,
	workers *shardWorkerSet,
	now time.Time,
) []string {
	budget := snapshot.high - snapshot.ownerCounts[self]
	if budget <= 0 {
		return nil
	}

	unowned := append([]string(nil), snapshot.unowned...)
	picked := make([]string, 0, budget)
	for len(picked) < budget {
		shardID := pickRebalanceShard(unowned, cooldown, workers, now)
		if shardID == "" {
			break
		}
		unowned = removeRebalanceShard(unowned, shardID)
		picked = append(picked, shardID)
	}
	return picked
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
		// Only running workers are shed candidates: a stopping worker was
		// already shed and would burn a move-budget slot (its stop is a
		// no-op) every tick it spends stuck in an uncooperative callback.
		if !workers.running(shardID) {
			continue
		}
		shards = append(shards, shardID)
	}

	return shards
}
