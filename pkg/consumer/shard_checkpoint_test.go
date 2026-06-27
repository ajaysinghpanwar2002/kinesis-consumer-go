package consumer

import (
	"context"
	"errors"
	"testing"
)

type checkpointSaveCall struct {
	ctx            context.Context
	streamName     string
	shardID        string
	sequenceNumber string
}

type fakeCheckpointSaveStore struct {
	saveCalls []checkpointSaveCall
	saveErr   error
}

func (s *fakeCheckpointSaveStore) Get(context.Context, string, string) (string, error) {
	return "", nil
}

func (s *fakeCheckpointSaveStore) Save(ctx context.Context, streamName, shardID, sequenceNumber string) error {
	s.saveCalls = append(s.saveCalls, checkpointSaveCall{
		ctx:            ctx,
		streamName:     streamName,
		shardID:        shardID,
		sequenceNumber: sequenceNumber,
	})
	if s.saveErr != nil {
		return s.saveErr
	}
	return nil
}

func (s *fakeCheckpointSaveStore) Delete(context.Context, string, string) error {
	return nil
}

func TestSaveShardCheckpointSavesStreamShardAndSequence(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	call := store.saveCalls[0]
	if call.streamName != "stream" {
		t.Fatalf("streamName = %q, want %q", call.streamName, "stream")
	}
	if call.shardID != "shard-1" {
		t.Fatalf("shardID = %q, want %q", call.shardID, "shard-1")
	}
	if call.sequenceNumber != "sequence-1" {
		t.Fatalf("sequenceNumber = %q, want %q", call.sequenceNumber, "sequence-1")
	}
}

func TestSaveShardCheckpointUsesStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:   Config{StreamARN: streamARN},
		store: store,
	}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].streamName != streamARN {
		t.Fatalf("streamName = %q, want %q", store.saveCalls[0].streamName, streamARN)
	}
}

func TestSaveShardCheckpointTreatsEmptySequenceAsNoop(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", ""); err != nil {
		t.Fatalf("saveShardCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestSaveShardCheckpointForwardsContext(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}

	if err := c.saveShardCheckpoint(ctx, "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].ctx != ctx {
		t.Fatalf("Save context = %v, want %v", store.saveCalls[0].ctx, ctx)
	}
}

func TestSaveShardCheckpointWrapsStoreError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		cfg:   Config{StreamName: "stream"},
		store: store,
	}

	err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("saveShardCheckpoint() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "save shard checkpoint shard-1 sequence-1: boom" {
		t.Fatalf("saveShardCheckpoint() error = %v, want %q", err, "save shard checkpoint shard-1 sequence-1: boom")
	}
}

func TestSaveShardCheckpointIfDueSkipsBelowThreshold(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
	}

	count, err := c.saveShardCheckpointIfDue(context.Background(), "shard-1", "sequence-1", 2)
	if err != nil {
		t.Fatalf("saveShardCheckpointIfDue() error = %v, want nil", err)
	}
	if count != 2 {
		t.Fatalf("saveShardCheckpointIfDue() count = %d, want 2", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestSaveShardCheckpointIfDueSavesAtThreshold(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
	}

	count, err := c.saveShardCheckpointIfDue(context.Background(), "shard-1", "sequence-1", 3)
	if err != nil {
		t.Fatalf("saveShardCheckpointIfDue() error = %v, want nil", err)
	}
	if count != 0 {
		t.Fatalf("saveShardCheckpointIfDue() count = %d, want 0", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-1" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-1")
	}
}

func TestSaveShardCheckpointIfDueSavesAboveThreshold(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
	}

	count, err := c.saveShardCheckpointIfDue(context.Background(), "shard-1", "sequence-5", 5)
	if err != nil {
		t.Fatalf("saveShardCheckpointIfDue() error = %v, want nil", err)
	}
	if count != 2 {
		t.Fatalf("saveShardCheckpointIfDue() count = %d, want 2", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-5" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-5")
	}
}

func TestSaveShardCheckpointIfDueTreatsEmptySequenceAsNoop(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
	}

	count, err := c.saveShardCheckpointIfDue(context.Background(), "shard-1", "", 3)
	if err != nil {
		t.Fatalf("saveShardCheckpointIfDue() error = %v, want nil", err)
	}
	if count != 3 {
		t.Fatalf("saveShardCheckpointIfDue() count = %d, want 3", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestSaveShardCheckpointIfDueWrapsSaveError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
	}

	count, err := c.saveShardCheckpointIfDue(context.Background(), "shard-1", "sequence-1", 3)
	if !errors.Is(err, errBoom) {
		t.Fatalf("saveShardCheckpointIfDue() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "save due shard checkpoint shard-1: save shard checkpoint shard-1 sequence-1: boom" {
		t.Fatalf("saveShardCheckpointIfDue() error = %v, want %q", err, "save due shard checkpoint shard-1: save shard checkpoint shard-1 sequence-1: boom")
	}
	if count != 3 {
		t.Fatalf("saveShardCheckpointIfDue() count = %d, want 3", count)
	}
}
