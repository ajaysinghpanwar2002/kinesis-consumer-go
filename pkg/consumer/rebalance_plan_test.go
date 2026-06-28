package consumer

import (
	"slices"
	"testing"
	"time"
)

func TestBuildLocalRebalancePlanAcquiresUnownedThenClaimsDonorToLow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{"shard-d", "shard-a", "shard-c", "shard-b"},
		map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		[]string{"self", "donor-a"},
		"self",
		nil,
		nil,
		now,
		3,
	)

	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
		{kind: rebalancePlanClaimDonor, shardID: "shard-b", donor: "donor-a"},
	})
	if plan.initialCount != 0 {
		t.Fatalf("initialCount = %d, want 0", plan.initialCount)
	}
	if plan.projectedCount != 2 {
		t.Fatalf("projectedCount = %d, want 2", plan.projectedCount)
	}
	if plan.snapshot.low != 2 || plan.snapshot.high != 2 {
		t.Fatalf("low/high = %d/%d, want 2/2", plan.snapshot.low, plan.snapshot.high)
	}
}

func TestBuildLocalRebalancePlanAcquiresRemainingUnownedUpToHigh(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{"shard-e", "shard-b", "shard-d", "shard-a", "shard-c"},
		map[string]string{
			"shard-a": "self",
			"shard-b": "self",
			"shard-c": "donor-a",
		},
		[]string{"self", "donor-a"},
		"self",
		nil,
		nil,
		now,
		3,
	)

	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-d"},
	})
	if plan.initialCount != 2 {
		t.Fatalf("initialCount = %d, want 2", plan.initialCount)
	}
	if plan.projectedCount != 3 {
		t.Fatalf("projectedCount = %d, want 3", plan.projectedCount)
	}
	if plan.snapshot.low != 2 || plan.snapshot.high != 3 {
		t.Fatalf("low/high = %d/%d, want 2/3", plan.snapshot.low, plan.snapshot.high)
	}
}

func TestBuildLocalRebalancePlanHonorsMaxMovesAcrossPhases(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b", "shard-c", "shard-d"},
		map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		[]string{"self", "donor-a"},
		"self",
		nil,
		nil,
		now,
		1,
	)

	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanAcquireUnowned, shardID: "shard-a"},
	})
	if plan.projectedCount != 1 {
		t.Fatalf("projectedCount = %d, want 1", plan.projectedCount)
	}
}

func TestBuildLocalRebalancePlanSkipsCooldownAndRunningCandidates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	cooldown := map[string]time.Time{
		"shard-a": now.Add(time.Minute),
		"shard-c": now.Add(time.Minute),
	}
	workers := newShardWorkerSet()
	workers.add("shard-b", func() {})
	workers.add("shard-d", func() {})

	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b", "shard-c", "shard-d", "shard-e", "shard-f"},
		map[string]string{
			"shard-c": "donor-a",
			"shard-d": "donor-a",
			"shard-e": "donor-a",
			"shard-f": "donor-a",
		},
		[]string{"self", "donor-a"},
		"self",
		cooldown,
		workers,
		now,
		1,
	)

	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanClaimDonor, shardID: "shard-e", donor: "donor-a"},
	})
	if plan.projectedCount != 1 {
		t.Fatalf("projectedCount = %d, want 1", plan.projectedCount)
	}
}

func TestBuildLocalRebalancePlanFallsBackToNextDonor(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	cooldown := map[string]time.Time{
		"shard-a": now.Add(time.Minute),
		"shard-b": now.Add(time.Minute),
		"shard-c": now.Add(time.Minute),
	}

	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b", "shard-c", "shard-d", "shard-e", "shard-f", "shard-g", "shard-h"},
		map[string]string{
			"shard-a": "donor-a",
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-b",
			"shard-e": "donor-b",
			"shard-f": "donor-b",
			"shard-g": "donor-b",
			"shard-h": "donor-b",
		},
		[]string{"self", "donor-a", "donor-b"},
		"self",
		cooldown,
		nil,
		now,
		1,
	)

	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanClaimDonor, shardID: "shard-d", donor: "donor-b"},
	})
}

func TestBuildLocalRebalancePlanNoopsWhenBalancedAndNoUnowned(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b", "shard-c", "shard-d"},
		map[string]string{
			"shard-a": "self",
			"shard-b": "self",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		[]string{"self", "donor-a"},
		"self",
		nil,
		nil,
		now,
		2,
	)

	if len(plan.actions) != 0 {
		t.Fatalf("actions = %v, want empty", plan.actions)
	}
	if plan.initialCount != 2 || plan.projectedCount != 2 {
		t.Fatalf("initial/projected count = %d/%d, want 2/2", plan.initialCount, plan.projectedCount)
	}
}

func TestBuildLocalRebalancePlanNoopsWhenMaxMovesIsZero(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b"},
		map[string]string{},
		[]string{"self"},
		"self",
		nil,
		nil,
		now,
		0,
	)

	if len(plan.actions) != 0 {
		t.Fatalf("actions = %v, want empty", plan.actions)
	}
	if plan.projectedCount != 0 {
		t.Fatalf("projectedCount = %d, want 0", plan.projectedCount)
	}
}

func assertRebalancePlanActions(t *testing.T, got, want []rebalancePlanAction) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("actions = %#v, want %#v", got, want)
	}
}
