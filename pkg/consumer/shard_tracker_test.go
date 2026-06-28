package consumer

import (
	"context"
	"errors"
	"testing"
)

func TestShardCompletionStateMarkAndIsCompleted(t *testing.T) {
	t.Parallel()

	state := newShardCompletionState()

	state.markCompleted("")
	if state.isCompleted("") {
		t.Fatal("isCompleted(empty) = true, want false")
	}
	if state.isCompleted("shard-1") {
		t.Fatal("isCompleted(shard-1) before mark = true, want false")
	}

	state.markCompleted("shard-1")
	if !state.isCompleted("shard-1") {
		t.Fatal("isCompleted(shard-1) after mark = false, want true")
	}
	if state.isCompleted("shard-2") {
		t.Fatal("isCompleted(shard-2) = true, want false")
	}
}

func TestShardCompletionStateReadsShardEndCheckpointAndMarksCompleted(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{checkpoint: "SHARD_END:sequence-1"}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()

	completed, err := state.shardCompleted(context.Background(), c, "shard-1")
	if err != nil {
		t.Fatalf("shardCompleted() error = %v, want nil", err)
	}
	if !completed {
		t.Fatal("shardCompleted() = false, want true")
	}
	if !state.isCompleted("shard-1") {
		t.Fatal("isCompleted(shard-1) = false, want true after SHARD_END checkpoint")
	}
	if len(store.getCalls) != 1 {
		t.Fatalf("Get calls = %d, want 1", len(store.getCalls))
	}
}

func TestShardCompletionStateLocalCompletionSkipsCheckpointRead(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{getErr: errors.New("boom")}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()
	state.markCompleted("shard-1")

	completed, err := state.shardCompleted(context.Background(), c, "shard-1")
	if err != nil {
		t.Fatalf("shardCompleted() error = %v, want nil", err)
	}
	if !completed {
		t.Fatal("shardCompleted() = false, want true")
	}
	if len(store.getCalls) != 0 {
		t.Fatalf("Get calls = %d, want 0", len(store.getCalls))
	}
}

func TestShardCompletionStateOrdinaryCheckpointIsNotCompleted(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{checkpoint: "sequence-1"}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()

	completed, err := state.shardCompleted(context.Background(), c, "shard-1")
	if err != nil {
		t.Fatalf("shardCompleted() error = %v, want nil", err)
	}
	if completed {
		t.Fatal("shardCompleted() = true, want false")
	}
	if state.isCompleted("shard-1") {
		t.Fatal("isCompleted(shard-1) = true, want false after ordinary checkpoint")
	}
	if len(store.getCalls) != 1 {
		t.Fatalf("Get calls = %d, want 1", len(store.getCalls))
	}
}

func TestShardCompletionStateEmptyShardIDIsIgnored(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{checkpoint: "SHARD_END"}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()

	completed, err := state.shardCompleted(context.Background(), c, "")
	if err != nil {
		t.Fatalf("shardCompleted() error = %v, want nil", err)
	}
	if completed {
		t.Fatal("shardCompleted(empty) = true, want false")
	}
	if len(store.getCalls) != 0 {
		t.Fatalf("Get calls = %d, want 0", len(store.getCalls))
	}
}

func TestShardCompletionStateWrapsCheckpointReadErrorAndDoesNotMarkCompleted(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{getErr: errBoom}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}
	state := newShardCompletionState()

	completed, err := state.shardCompleted(context.Background(), c, "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("shardCompleted() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "check shard completion shard-1: read shard checkpoint shard-1: boom" {
		t.Fatalf("shardCompleted() error = %v, want %q", err, "check shard completion shard-1: read shard checkpoint shard-1: boom")
	}
	if completed {
		t.Fatal("shardCompleted() = true, want false")
	}
	if state.isCompleted("shard-1") {
		t.Fatal("isCompleted(shard-1) = true, want false after read error")
	}
}
