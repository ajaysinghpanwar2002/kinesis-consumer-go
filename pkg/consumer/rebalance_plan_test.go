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

func TestBuildLocalRebalancePlanStarvedWorkerClaimsFromAtHighDonor(t *testing.T) {
	t.Parallel()

	// The 2/2/0 dead zone: 4 shards over 3 workers (low=1, high=2). No donor
	// exceeds high, so requiring count > high froze the starved worker at 0
	// forever. It must claim one shard from a donor at exactly high.
	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b", "shard-c", "shard-d"},
		map[string]string{
			"shard-a": "donor-a",
			"shard-b": "donor-a",
			"shard-c": "donor-b",
			"shard-d": "donor-b",
		},
		[]string{"self", "donor-a", "donor-b"},
		"self",
		nil,
		nil,
		now,
		2,
	)

	if plan.snapshot.low != 1 || plan.snapshot.high != 2 {
		t.Fatalf("low/high = %d/%d, want 1/2", plan.snapshot.low, plan.snapshot.high)
	}
	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanClaimDonor, shardID: "shard-a", donor: "donor-a"},
	})
	if plan.projectedCount != 1 {
		t.Fatalf("projectedCount = %d, want 1 (exactly low, no over-claim)", plan.projectedCount)
	}
}

func TestBuildLocalRebalancePlanStarvedWorkerReachesLowWithTenShardsFourWorkers(t *testing.T) {
	t.Parallel()

	// The 3/3/3/1 dead zone: 10 shards over 4 workers (low=2, high=3). Every
	// donor sits at exactly high, so the worker at 1 could never reach its
	// fair-share floor of 2. It must claim exactly one shard and stop at low.
	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	plan := buildLocalRebalancePlan(
		[]string{
			"shard-01", "shard-02", "shard-03", "shard-04", "shard-05",
			"shard-06", "shard-07", "shard-08", "shard-09", "shard-10",
		},
		map[string]string{
			"shard-01": "donor-a",
			"shard-02": "donor-a",
			"shard-03": "donor-a",
			"shard-04": "donor-b",
			"shard-05": "donor-b",
			"shard-06": "donor-b",
			"shard-07": "donor-c",
			"shard-08": "donor-c",
			"shard-09": "donor-c",
			"shard-10": "self",
		},
		[]string{"self", "donor-a", "donor-b", "donor-c"},
		"self",
		nil,
		nil,
		now,
		2,
	)

	if plan.snapshot.low != 2 || plan.snapshot.high != 3 {
		t.Fatalf("low/high = %d/%d, want 2/3", plan.snapshot.low, plan.snapshot.high)
	}
	assertRebalancePlanActions(t, plan.actions, []rebalancePlanAction{
		{kind: rebalancePlanClaimDonor, shardID: "shard-01", donor: "donor-a"},
	})
	if plan.projectedCount != 2 {
		t.Fatalf("projectedCount = %d, want 2 (exactly low)", plan.projectedCount)
	}
}

func TestBuildLocalRebalancePlanNeverClaimsFromDonorAtLow(t *testing.T) {
	t.Parallel()

	// Donor eligibility is strictly count > low: a claim from a donor at low
	// would just move the starvation. 4 shards over 3 workers (low=1), both
	// donors at exactly low, and the only unowned shard in cooldown — the
	// starved worker must plan nothing rather than drain a donor below low.
	now := time.Date(2026, 6, 28, 4, 5, 6, 0, time.UTC)
	cooldown := map[string]time.Time{
		"shard-x": now.Add(time.Minute),
		"shard-y": now.Add(time.Minute),
	}
	plan := buildLocalRebalancePlan(
		[]string{"shard-a", "shard-b", "shard-x", "shard-y"},
		map[string]string{
			"shard-a": "donor-a",
			"shard-b": "donor-b",
		},
		[]string{"self", "donor-a", "donor-b"},
		"self",
		cooldown,
		nil,
		now,
		2,
	)

	if plan.snapshot.low != 1 || plan.snapshot.high != 2 {
		t.Fatalf("low/high = %d/%d, want 1/2", plan.snapshot.low, plan.snapshot.high)
	}
	assertRebalancePlanActions(t, plan.actions, nil)
	if plan.projectedCount != 0 {
		t.Fatalf("projectedCount = %d, want 0", plan.projectedCount)
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
