package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestMergeKnownShardsLeavesMapUnchangedForEmptyInput(t *testing.T) {
	t.Parallel()

	existing := testShard("existing", "", "")
	shards := map[string]types.Shard{"existing": existing}

	mergeKnownShards(shards, nil)

	if len(shards) != 1 {
		t.Fatalf("len(shards) = %d, want 1", len(shards))
	}
	if got := shards["existing"]; aws.ToString(got.ShardId) != "existing" {
		t.Fatalf("shards[existing].ShardId = %q, want existing", aws.ToString(got.ShardId))
	}
}

func TestMergeKnownShardsSkipsNilAndEmptyShardIDs(t *testing.T) {
	t.Parallel()

	shards := map[string]types.Shard{}

	mergeKnownShards(shards, []types.Shard{
		{},
		{ShardId: aws.String("")},
		testShard("shard-1", "", ""),
	})

	if len(shards) != 1 {
		t.Fatalf("len(shards) = %d, want 1", len(shards))
	}
	if _, ok := shards[""]; ok {
		t.Fatal("shards contains empty key, want skipped")
	}
	if got := shards["shard-1"]; aws.ToString(got.ShardId) != "shard-1" {
		t.Fatalf("shards[shard-1].ShardId = %q, want shard-1", aws.ToString(got.ShardId))
	}
}

func TestMergeKnownShardsOverwritesExistingMetadata(t *testing.T) {
	t.Parallel()

	shards := map[string]types.Shard{
		"child": testShard("child", "", ""),
	}

	mergeKnownShards(shards, []types.Shard{
		testShard("child", "parent", "adjacent"),
	})

	got := shards["child"]
	if aws.ToString(got.ParentShardId) != "parent" {
		t.Fatalf("ParentShardId = %q, want parent", aws.ToString(got.ParentShardId))
	}
	if aws.ToString(got.AdjacentParentShardId) != "adjacent" {
		t.Fatalf("AdjacentParentShardId = %q, want adjacent", aws.ToString(got.AdjacentParentShardId))
	}
}

func TestRefreshKnownShardsMergesListedShards(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{
				testShard("existing", "parent", "adjacent"),
				testShard("new", "", ""),
				{ShardId: aws.String("")},
			}},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	known := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}

	if _, err := c.refreshKnownShards(context.Background(), known); err != nil {
		t.Fatalf("refreshKnownShards() error = %v, want nil", err)
	}

	if len(known) != 2 {
		t.Fatalf("len(known) = %d, want 2", len(known))
	}
	if got := known["existing"]; aws.ToString(got.ParentShardId) != "parent" {
		t.Fatalf("known[existing].ParentShardId = %q, want parent", aws.ToString(got.ParentShardId))
	}
	if got := known["existing"]; aws.ToString(got.AdjacentParentShardId) != "adjacent" {
		t.Fatalf("known[existing].AdjacentParentShardId = %q, want adjacent", aws.ToString(got.AdjacentParentShardId))
	}
	if got := known["new"]; aws.ToString(got.ShardId) != "new" {
		t.Fatalf("known[new].ShardId = %q, want new", aws.ToString(got.ShardId))
	}
	if _, ok := known[""]; ok {
		t.Fatal("known contains empty key, want skipped")
	}
	if len(client.calls) != 1 {
		t.Fatalf("ListShards calls = %d, want 1", len(client.calls))
	}
}

func TestRefreshKnownShardsLeavesMapUnchangedForEmptyList(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{{}},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	known := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}

	if _, err := c.refreshKnownShards(context.Background(), known); err != nil {
		t.Fatalf("refreshKnownShards() error = %v, want nil", err)
	}

	if len(known) != 1 {
		t.Fatalf("len(known) = %d, want 1", len(known))
	}
	if got := known["existing"]; aws.ToString(got.ShardId) != "existing" {
		t.Fatalf("known[existing].ShardId = %q, want existing", aws.ToString(got.ShardId))
	}
}

func TestRefreshKnownShardsReturnsListErrorWithoutMutatingMap(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{err: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	known := map[string]types.Shard{
		"existing": testShard("existing", "", ""),
	}

	_, err := c.refreshKnownShards(context.Background(), known)
	if !errors.Is(err, errBoom) {
		t.Fatalf("refreshKnownShards() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "list shards: boom" {
		t.Fatalf("refreshKnownShards() error = %v, want %q", err, "list shards: boom")
	}
	if len(known) != 1 {
		t.Fatalf("len(known) = %d, want 1", len(known))
	}
	if got := known["existing"]; aws.ToString(got.ShardId) != "existing" {
		t.Fatalf("known[existing].ShardId = %q, want existing", aws.ToString(got.ShardId))
	}
}

func TestRefreshKnownShardsReturnsListedShardIDSet(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		outs: []*kinesis.ListShardsOutput{
			{Shards: []types.Shard{
				testShard("shard-1", "", ""),
				testShard("shard-2", "", ""),
				{ShardId: aws.String("")},
			}},
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}
	// A shard known from an earlier listing but absent from this one must not
	// appear in the returned listed set: that set is the pruning authority.
	known := map[string]types.Shard{
		"aged-out": testShard("aged-out", "", ""),
	}

	listed, err := c.refreshKnownShards(context.Background(), known)
	if err != nil {
		t.Fatalf("refreshKnownShards() error = %v, want nil", err)
	}

	if len(listed) != 2 {
		t.Fatalf("len(listed) = %d, want 2 (%v)", len(listed), listed)
	}
	for _, shardID := range []string{"shard-1", "shard-2"} {
		if _, ok := listed[shardID]; !ok {
			t.Fatalf("listed missing %q; got %v", shardID, listed)
		}
	}
	if _, ok := listed["aged-out"]; ok {
		t.Fatal("listed contains aged-out, want only shards from the fresh listing")
	}
}

func TestPruneStaleShardStateDropsStateForUnlistedShards(t *testing.T) {
	t.Parallel()

	known := map[string]types.Shard{
		"aged-out": testShard("aged-out", "", ""),
		"live":     testShard("live", "", ""),
	}
	listed := map[string]struct{}{"live": {}}
	completionState := newShardCompletionState()
	completionState.markCompleted("aged-out")
	completionState.markCompleted("live")
	cooldown := map[string]time.Time{
		"aged-out": time.Now().Add(time.Minute),
		"live":     time.Now().Add(time.Minute),
	}
	parentage := &shardParentage{hasParents: map[string]struct{}{
		"aged-out": {},
		"live":     {},
	}}

	pruneStaleShardState(known, listed, completionState, cooldown, parentage, newShardWorkerSet())

	if _, ok := known["aged-out"]; ok {
		t.Fatal("known retains aged-out, want pruned")
	}
	if _, ok := known["live"]; !ok {
		t.Fatal("known missing live, want retained")
	}
	if _, ok := cooldown["aged-out"]; ok {
		t.Fatal("cooldown retains aged-out, want pruned")
	}
	if _, ok := cooldown["live"]; !ok {
		t.Fatal("cooldown missing live, want retained")
	}
	if completionState.isCompleted("aged-out") {
		t.Fatal("completion cache retains aged-out, want pruned")
	}
	if !completionState.isCompleted("live") {
		t.Fatal("completion cache missing live, want retained")
	}
	if parentage.hasKnownParents("aged-out") {
		t.Fatal("parentage retains aged-out, want pruned")
	}
	if !parentage.hasKnownParents("live") {
		t.Fatal("parentage missing live, want retained")
	}
}

func TestPruneStaleShardStateRetainsUnlistedShardsWithRegisteredWorkers(t *testing.T) {
	t.Parallel()

	known := map[string]types.Shard{
		"working": testShard("working", "", ""),
	}
	completionState := newShardCompletionState()
	completionState.markCompleted("working")
	cooldown := map[string]time.Time{"working": time.Now().Add(time.Minute)}
	parentage := &shardParentage{hasParents: map[string]struct{}{"working": {}}}
	workers := newShardWorkerSet()
	workers.add("working", func() {})

	// The listing must be non-empty (an empty one is ignored wholesale) and
	// must not contain the shard, so only the worker exemption can retain it.
	pruneStaleShardState(known, map[string]struct{}{"unrelated": {}}, completionState, cooldown, parentage, workers)

	if _, ok := known["working"]; !ok {
		t.Fatal("known missing working, want retained while its worker is registered")
	}
	if _, ok := cooldown["working"]; !ok {
		t.Fatal("cooldown missing working, want retained while its worker is registered")
	}
	if !completionState.isCompleted("working") {
		t.Fatal("completion cache missing working, want retained while its worker is registered")
	}
	if !parentage.hasKnownParents("working") {
		t.Fatal("parentage missing working, want retained while its worker is registered")
	}
}

func TestPruneStaleShardStateRetainsCompletionOfParentsStillReferencedByListedShards(t *testing.T) {
	t.Parallel()

	// The completed parent has aged out of the listing but its child is still
	// listed: the parent's cached completion must survive, or readiness would
	// re-read the parent's checkpoint from the store on every pass until the
	// child ages out too.
	known := map[string]types.Shard{
		"parent": testShard("parent", "", ""),
		"child":  testShard("child", "parent", ""),
	}
	listed := map[string]struct{}{"child": {}}
	completionState := newShardCompletionState()
	completionState.markCompleted("parent")

	pruneStaleShardState(known, listed, completionState, nil, nil, newShardWorkerSet())

	if _, ok := known["parent"]; ok {
		t.Fatal("known retains parent, want pruned (only the completion memo survives)")
	}
	if !completionState.isCompleted("parent") {
		t.Fatal("completion cache dropped parent while child still references it")
	}

	// Once the child ages out as well (a later listing has moved on to other
	// shards), nothing references the parent and its completion memo goes too.
	pruneStaleShardState(known, map[string]struct{}{"unrelated": {}}, completionState, nil, nil, newShardWorkerSet())

	if completionState.isCompleted("parent") {
		t.Fatal("completion cache retains parent after its last referencing child aged out")
	}
}

func TestPruneStaleShardStateIgnoresEmptyListings(t *testing.T) {
	t.Parallel()

	// An empty listing is degenerate (an existing stream always lists at least
	// one shard) and must never be authority for mass-deleting local state.
	known := map[string]types.Shard{
		"live": testShard("live", "", ""),
	}
	completionState := newShardCompletionState()
	completionState.markCompleted("live")
	cooldown := map[string]time.Time{"live": time.Now().Add(time.Minute)}
	parentage := &shardParentage{hasParents: map[string]struct{}{"live": {}}}

	pruneStaleShardState(known, map[string]struct{}{}, completionState, cooldown, parentage, newShardWorkerSet())

	if _, ok := known["live"]; !ok {
		t.Fatal("known missing live after empty listing, want untouched")
	}
	if _, ok := cooldown["live"]; !ok {
		t.Fatal("cooldown missing live after empty listing, want untouched")
	}
	if !completionState.isCompleted("live") {
		t.Fatal("completion cache missing live after empty listing, want untouched")
	}
	if !parentage.hasKnownParents("live") {
		t.Fatal("parentage missing live after empty listing, want untouched")
	}
}
