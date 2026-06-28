package consumer

import "sort"

type rebalanceOwnershipSnapshot struct {
	activeWorkers int
	open          int
	low           int
	high          int
	ownerCounts   map[string]int
	ownerShards   map[string][]string
	unowned       []string
	ourShards     []string
}

func buildRebalanceOwnershipSnapshot(
	readyShardIDs []string,
	leaseOwners map[string]string,
	workerOwners []string,
	self string,
) rebalanceOwnershipSnapshot {
	ownerSet := make(map[string]struct{}, len(workerOwners)+1)
	for _, owner := range workerOwners {
		ownerSet[owner] = struct{}{}
	}
	ownerSet[self] = struct{}{}

	activeWorkers := len(ownerSet)
	if activeWorkers == 0 {
		activeWorkers = 1
	}

	snapshot := rebalanceOwnershipSnapshot{
		activeWorkers: activeWorkers,
		open:          len(readyShardIDs),
		ownerCounts:   make(map[string]int),
		ownerShards:   make(map[string][]string),
	}
	snapshot.low = snapshot.open / snapshot.activeWorkers
	snapshot.high = (snapshot.open + snapshot.activeWorkers - 1) / snapshot.activeWorkers

	for _, shardID := range readyShardIDs {
		owner := leaseOwners[shardID]
		if owner == "" {
			snapshot.unowned = append(snapshot.unowned, shardID)
			continue
		}

		snapshot.ownerCounts[owner]++
		snapshot.ownerShards[owner] = append(snapshot.ownerShards[owner], shardID)
		if owner == self {
			snapshot.ourShards = append(snapshot.ourShards, shardID)
		}
	}

	sort.Strings(snapshot.unowned)
	sort.Strings(snapshot.ourShards)
	for owner := range snapshot.ownerShards {
		sort.Strings(snapshot.ownerShards[owner])
	}

	return snapshot
}
