package consumer

import (
	"slices"
	"testing"
	"time"
)

func TestPickRebalanceDonorDeterministicTieBreak(t *testing.T) {
	t.Parallel()

	ownerCounts := map[string]int{
		"worker-b": 3,
		"worker-a": 3,
		"self":     1,
	}

	got := pickRebalanceDonor(ownerCounts, 2, "self")
	if got != "worker-a" {
		t.Fatalf("pickRebalanceDonor = %q, want %q", got, "worker-a")
	}
}

func TestPickRebalanceDonorNoneEligible(t *testing.T) {
	t.Parallel()

	ownerCounts := map[string]int{
		"worker-a": 2,
		"worker-b": 1,
		"self":     4,
	}

	got := pickRebalanceDonor(ownerCounts, 2, "self")
	if got != "" {
		t.Fatalf("pickRebalanceDonor = %q, want empty", got)
	}
}

func TestPickRebalanceDonorHighestCountWins(t *testing.T) {
	t.Parallel()

	ownerCounts := map[string]int{
		"worker-a": 3,
		"worker-b": 5,
		"self":     1,
	}

	got := pickRebalanceDonor(ownerCounts, 2, "self")
	if got != "worker-b" {
		t.Fatalf("pickRebalanceDonor = %q, want %q", got, "worker-b")
	}
}

func TestPickRebalanceDonorSkipsSelfEvenIfHighest(t *testing.T) {
	t.Parallel()

	ownerCounts := map[string]int{
		"worker-a": 3,
		"self":     10,
	}

	got := pickRebalanceDonor(ownerCounts, 2, "self")
	if got != "worker-a" {
		t.Fatalf("pickRebalanceDonor = %q, want %q", got, "worker-a")
	}
}

func TestRebalanceShardInCooldown(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 1, 2, 3, 0, time.UTC)
	cooldown := map[string]time.Time{
		"active":  now.Add(time.Minute),
		"expired": now.Add(-time.Minute),
	}

	if !rebalanceShardInCooldown("active", cooldown, now) {
		t.Fatal("rebalanceShardInCooldown active = false, want true")
	}
	if rebalanceShardInCooldown("expired", cooldown, now) {
		t.Fatal("rebalanceShardInCooldown expired = true, want false")
	}
	if rebalanceShardInCooldown("missing", cooldown, now) {
		t.Fatal("rebalanceShardInCooldown missing = true, want false")
	}
}

func TestPickRebalanceShardSkipsCooldownAndRunning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 1, 2, 3, 0, time.UTC)
	cooldown := map[string]time.Time{
		"shard-1": now.Add(time.Minute),
	}
	workers := newShardWorkerSet()
	workers.add("shard-2", func() {})

	got := pickRebalanceShard([]string{"shard-1", "shard-2", "shard-3"}, cooldown, workers, now)
	if got != "shard-3" {
		t.Fatalf("pickRebalanceShard = %q, want %q", got, "shard-3")
	}
}

func TestPickRebalanceShardAllowsExpiredCooldownAndNilWorkers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 1, 2, 3, 0, time.UTC)
	cooldown := map[string]time.Time{
		"shard-1": now.Add(-time.Minute),
	}

	got := pickRebalanceShard([]string{"shard-1", "shard-2"}, cooldown, nil, now)
	if got != "shard-1" {
		t.Fatalf("pickRebalanceShard = %q, want %q", got, "shard-1")
	}
}

func TestPickRebalanceShardEmptyWhenNoCandidate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 1, 2, 3, 0, time.UTC)
	cooldown := map[string]time.Time{
		"shard-2": now.Add(time.Minute),
	}
	workers := newShardWorkerSet()
	workers.add("shard-1", func() {})

	got := pickRebalanceShard([]string{"shard-1", "shard-2"}, cooldown, workers, now)
	if got != "" {
		t.Fatalf("pickRebalanceShard = %q, want empty", got)
	}
}

func TestRemoveRebalanceShardPreservesOrder(t *testing.T) {
	t.Parallel()

	shards := []string{"shard-a", "shard-b", "shard-c"}
	got := removeRebalanceShard(shards, "shard-b")
	want := []string{"shard-a", "shard-c"}
	if !slices.Equal(got, want) {
		t.Fatalf("removeRebalanceShard = %v, want %v", got, want)
	}
}

func TestRemoveRebalanceShardMissingReturnsOriginalList(t *testing.T) {
	t.Parallel()

	shards := []string{"shard-a", "shard-b", "shard-c"}
	got := removeRebalanceShard(shards, "missing")
	if !slices.Equal(got, shards) {
		t.Fatalf("removeRebalanceShard missing = %v, want %v", got, shards)
	}
}
