package consumer

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type readinessCheckpointStore struct {
	checkpoints map[string]string
	getCalls    []checkpointGetCall
	getErrs     map[string]error
}

func (s *readinessCheckpointStore) Get(ctx context.Context, streamName, shardID string) (string, error) {
	s.getCalls = append(s.getCalls, checkpointGetCall{
		ctx:        ctx,
		streamName: streamName,
		shardID:    shardID,
	})
	if err := s.getErrs[shardID]; err != nil {
		return "", err
	}
	return s.checkpoints[shardID], nil
}

func (s *readinessCheckpointStore) Save(context.Context, string, string, string) error {
	return nil
}

func (s *readinessCheckpointStore) Delete(context.Context, string, string) error {
	return nil
}

func TestReadyShardIDsFiltersCompletedAndParentGatedShards(t *testing.T) {
	t.Parallel()

	store := &readinessCheckpointStore{
		checkpoints: map[string]string{
			"completed-store": "SHARD_END",
		},
		getErrs: map[string]error{},
	}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()
	state.markCompleted("completed-local")
	shards := shardMapForReadiness(
		testShard("parent", "", ""),
		testShard("child-blocked", "parent", ""),
		testShard("child-missing", "missing-parent", ""),
		testShard("ready", "", ""),
		testShard("completed-local", "", ""),
		testShard("completed-store", "", ""),
	)

	got, err := state.readyShardIDs(context.Background(), c, shards)
	if err != nil {
		t.Fatalf("readyShardIDs() error = %v, want nil", err)
	}
	want := []string{"child-missing", "parent", "ready"}
	if !slices.Equal(got, want) {
		t.Fatalf("readyShardIDs() = %v, want %v", got, want)
	}
	if !state.isCompleted("completed-store") {
		t.Fatal("completed-store was not marked completed from checkpoint")
	}
	if gotCalls := shardGetCalls(store, "completed-local"); gotCalls != 0 {
		t.Fatalf("completed-local Get calls = %d, want 0", gotCalls)
	}
}

func TestReadyShardIDsAllowsChildWhenKnownParentCompletedInStore(t *testing.T) {
	t.Parallel()

	store := &readinessCheckpointStore{
		checkpoints: map[string]string{
			"parent": "SHARD_END:sequence-1",
		},
		getErrs: map[string]error{},
	}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()
	shards := shardMapForReadiness(
		testShard("parent", "", ""),
		testShard("child", "parent", ""),
	)

	got, err := state.readyShardIDs(context.Background(), c, shards)
	if err != nil {
		t.Fatalf("readyShardIDs() error = %v, want nil", err)
	}
	want := []string{"child"}
	if !slices.Equal(got, want) {
		t.Fatalf("readyShardIDs() = %v, want %v", got, want)
	}
	if !state.isCompleted("parent") {
		t.Fatal("parent was not marked completed from checkpoint")
	}
}

func TestReadyShardIDsSkipsEmptyShardID(t *testing.T) {
	t.Parallel()

	store := &readinessCheckpointStore{
		checkpoints: map[string]string{},
		getErrs:     map[string]error{},
	}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()
	shards := map[string]types.Shard{
		"":        testShard("", "", ""),
		"shard-1": testShard("shard-1", "", ""),
	}

	got, err := state.readyShardIDs(context.Background(), c, shards)
	if err != nil {
		t.Fatalf("readyShardIDs() error = %v, want nil", err)
	}
	want := []string{"shard-1"}
	if !slices.Equal(got, want) {
		t.Fatalf("readyShardIDs() = %v, want %v", got, want)
	}
	if gotCalls := shardGetCalls(store, ""); gotCalls != 0 {
		t.Fatalf("empty shard Get calls = %d, want 0", gotCalls)
	}
}

func TestParentsReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		shard       types.Shard
		shards      map[string]types.Shard
		checkpoints map[string]string
		localDone   []string
		want        bool
	}{
		{
			name:  "no parents ready",
			shard: testShard("shard-1", "", ""),
			want:  true,
		},
		{
			name:  "missing parent allowed",
			shard: testShard("child", "parent", ""),
			shards: shardMapForReadiness(
				testShard("child", "parent", ""),
			),
			want: true,
		},
		{
			name:  "incomplete known parent blocks",
			shard: testShard("child", "parent", ""),
			shards: shardMapForReadiness(
				testShard("child", "parent", ""),
				testShard("parent", "", ""),
			),
			want: false,
		},
		{
			name:  "local completed known parent allows",
			shard: testShard("child", "parent", ""),
			shards: shardMapForReadiness(
				testShard("child", "parent", ""),
				testShard("parent", "", ""),
			),
			localDone: []string{"parent"},
			want:      true,
		},
		{
			name:  "store completed known parent allows",
			shard: testShard("child", "parent", ""),
			shards: shardMapForReadiness(
				testShard("child", "parent", ""),
				testShard("parent", "", ""),
			),
			checkpoints: map[string]string{"parent": "SHARD_END:sequence-1"},
			want:        true,
		},
		{
			name:  "adjacent incomplete known parent blocks",
			shard: testShard("child", "", "adjacent"),
			shards: shardMapForReadiness(
				testShard("child", "", "adjacent"),
				testShard("adjacent", "", ""),
			),
			want: false,
		},
		{
			name:  "missing adjacent parent allowed",
			shard: testShard("child", "", "adjacent"),
			shards: shardMapForReadiness(
				testShard("child", "", "adjacent"),
			),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &readinessCheckpointStore{
				checkpoints: tt.checkpoints,
				getErrs:     map[string]error{},
			}
			c := &Consumer{
				cfg:   Config{StreamName: "stream"},
				store: store,
			}
			state := newShardCompletionState()
			for _, shardID := range tt.localDone {
				state.markCompleted(shardID)
			}

			got, err := state.parentsReady(context.Background(), c, tt.shard, tt.shards)
			if err != nil {
				t.Fatalf("parentsReady() error = %v, want nil", err)
			}
			if got != tt.want {
				t.Fatalf("parentsReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadyShardIDsWrapsShardCompletionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &readinessCheckpointStore{
		checkpoints: map[string]string{},
		getErrs:     map[string]error{"shard-1": errBoom},
	}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()

	_, err := state.readyShardIDs(context.Background(), c, shardMapForReadiness(testShard("shard-1", "", "")))
	if !errors.Is(err, errBoom) {
		t.Fatalf("readyShardIDs() error = %v, want wraps %v", err, errBoom)
	}
	want := "check ready shard shard-1 completion: check shard completion shard-1: read shard checkpoint shard-1: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("readyShardIDs() error = %v, want %q", err, want)
	}
	if state.isCompleted("shard-1") {
		t.Fatal("shard-1 marked complete after read error, want incomplete")
	}
}

func TestReadyShardIDsWrapsParentCompletionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &readinessCheckpointStore{
		checkpoints: map[string]string{},
		getErrs:     map[string]error{"parent": errBoom},
	}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()
	shards := shardMapForReadiness(
		testShard("child", "parent", ""),
	)

	_, err := state.readyShardIDs(context.Background(), c, shards)
	if !errors.Is(err, errBoom) {
		t.Fatalf("readyShardIDs() error = %v, want wraps %v", err, errBoom)
	}
	want := "check shard child parent parent completion: check shard completion parent: read shard checkpoint parent: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("readyShardIDs() error = %v, want %q", err, want)
	}
	if state.isCompleted("parent") {
		t.Fatal("parent marked complete after read error, want incomplete")
	}
}

func TestReadyShardIDsReturnsCanceledContextBeforeStoreRead(t *testing.T) {
	t.Parallel()

	store := &readinessCheckpointStore{
		checkpoints: map[string]string{},
		getErrs:     map[string]error{},
	}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := state.readyShardIDs(ctx, c, shardMapForReadiness(testShard("shard-1", "", "")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("readyShardIDs() error = %v, want %v", err, context.Canceled)
	}
	if len(store.getCalls) != 0 {
		t.Fatalf("Get calls = %d, want 0", len(store.getCalls))
	}
}

func testShard(id, parent, adjacent string) types.Shard {
	shard := types.Shard{}
	if id != "" {
		shard.ShardId = aws.String(id)
	}
	if parent != "" {
		shard.ParentShardId = aws.String(parent)
	}
	if adjacent != "" {
		shard.AdjacentParentShardId = aws.String(adjacent)
	}
	return shard
}

func shardMapForReadiness(shards ...types.Shard) map[string]types.Shard {
	out := make(map[string]types.Shard, len(shards))
	for _, shard := range shards {
		if shard.ShardId == nil {
			out[""] = shard
			continue
		}
		out[*shard.ShardId] = shard
	}
	return out
}

func shardGetCalls(store *readinessCheckpointStore, shardID string) int {
	var count int
	for _, call := range store.getCalls {
		if call.shardID == shardID {
			count++
		}
	}
	return count
}
