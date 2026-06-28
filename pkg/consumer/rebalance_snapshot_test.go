package consumer

import (
	"maps"
	"slices"
	"testing"
)

func TestBuildRebalanceOwnershipSnapshotClassifiesReadyShardOwnership(t *testing.T) {
	t.Parallel()

	got := buildRebalanceOwnershipSnapshot(
		[]string{"shard-4", "shard-1", "shard-3", "shard-2"},
		map[string]string{
			"shard-1": "self",
			"shard-2": "",
			"shard-3": "worker-b",
			"shard-4": "self",
			"stale":   "worker-c",
		},
		[]string{"worker-b", "self"},
		"self",
	)

	if got.activeWorkers != 2 {
		t.Fatalf("activeWorkers = %d, want 2", got.activeWorkers)
	}
	if got.open != 4 {
		t.Fatalf("open = %d, want 4", got.open)
	}
	if got.low != 2 || got.high != 2 {
		t.Fatalf("low/high = %d/%d, want 2/2", got.low, got.high)
	}
	wantCounts := map[string]int{
		"self":     2,
		"worker-b": 1,
	}
	if !maps.Equal(got.ownerCounts, wantCounts) {
		t.Fatalf("ownerCounts = %v, want %v", got.ownerCounts, wantCounts)
	}
	assertShardList(t, got.unowned, []string{"shard-2"})
	assertShardList(t, got.ourShards, []string{"shard-1", "shard-4"})
	assertOwnerShards(t, got.ownerShards, map[string][]string{
		"self":     {"shard-1", "shard-4"},
		"worker-b": {"shard-3"},
	})
	if _, ok := got.ownerCounts["worker-c"]; ok {
		t.Fatal("ownerCounts includes stale non-ready shard owner worker-c")
	}
}

func TestBuildRebalanceOwnershipSnapshotIncludesSelfWhenWorkersEmpty(t *testing.T) {
	t.Parallel()

	got := buildRebalanceOwnershipSnapshot(
		[]string{"shard-1", "shard-2"},
		map[string]string{},
		nil,
		"self",
	)

	if got.activeWorkers != 1 {
		t.Fatalf("activeWorkers = %d, want 1", got.activeWorkers)
	}
	if got.low != 2 || got.high != 2 {
		t.Fatalf("low/high = %d/%d, want 2/2", got.low, got.high)
	}
	assertShardList(t, got.unowned, []string{"shard-1", "shard-2"})
	if len(got.ownerCounts) != 0 {
		t.Fatalf("ownerCounts = %v, want empty", got.ownerCounts)
	}
}

func TestBuildRebalanceOwnershipSnapshotIncludesSelfWhenWorkersListIsStale(t *testing.T) {
	t.Parallel()

	got := buildRebalanceOwnershipSnapshot(
		[]string{"shard-1", "shard-2", "shard-3", "shard-4", "shard-5"},
		map[string]string{
			"shard-1": "self",
			"shard-2": "worker-b",
			"shard-3": "worker-c",
		},
		[]string{"worker-b", "worker-c"},
		"self",
	)

	if got.activeWorkers != 3 {
		t.Fatalf("activeWorkers = %d, want 3", got.activeWorkers)
	}
	if got.low != 1 || got.high != 2 {
		t.Fatalf("low/high = %d/%d, want 1/2", got.low, got.high)
	}
	assertShardList(t, got.unowned, []string{"shard-4", "shard-5"})
	assertShardList(t, got.ourShards, []string{"shard-1"})
}

func TestBuildRebalanceOwnershipSnapshotDeduplicatesWorkersForFairShare(t *testing.T) {
	t.Parallel()

	got := buildRebalanceOwnershipSnapshot(
		[]string{"shard-1", "shard-2", "shard-3"},
		map[string]string{},
		[]string{"worker-b", "worker-b", "self"},
		"self",
	)

	if got.activeWorkers != 2 {
		t.Fatalf("activeWorkers = %d, want 2", got.activeWorkers)
	}
	if got.low != 1 || got.high != 2 {
		t.Fatalf("low/high = %d/%d, want 1/2", got.low, got.high)
	}
}

func TestBuildRebalanceOwnershipSnapshotSortsDeterministicShardLists(t *testing.T) {
	t.Parallel()

	got := buildRebalanceOwnershipSnapshot(
		[]string{"z", "b", "a", "c"},
		map[string]string{
			"z": "worker-b",
			"b": "self",
			"a": "",
			"c": "worker-b",
		},
		[]string{"worker-b"},
		"self",
	)

	assertShardList(t, got.unowned, []string{"a"})
	assertShardList(t, got.ourShards, []string{"b"})
	assertOwnerShards(t, got.ownerShards, map[string][]string{
		"self":     {"b"},
		"worker-b": {"c", "z"},
	})
}

func assertShardList(t *testing.T, got, want []string) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("shards = %v, want %v", got, want)
	}
}

func assertOwnerShards(t *testing.T, got, want map[string][]string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("ownerShards = %v, want %v", got, want)
	}
	for owner, wantShards := range want {
		assertShardList(t, got[owner], wantShards)
	}
}
