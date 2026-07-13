package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

type checkpointGetCall struct {
	ctx        context.Context
	streamName string
	shardID    string
}

type checkpointSaveCall struct {
	ctx            context.Context
	streamName     string
	shardID        string
	sequenceNumber string
}

type fakeCheckpointSaveStore struct {
	getCalls   []checkpointGetCall
	checkpoint string
	getErr     error
	saveCalls  []checkpointSaveCall
	saveErr    error
}

func (s *fakeCheckpointSaveStore) Get(ctx context.Context, streamName, shardID string) (string, error) {
	s.getCalls = append(s.getCalls, checkpointGetCall{
		ctx:        ctx,
		streamName: streamName,
		shardID:    shardID,
	})
	if s.getErr != nil {
		return "", s.getErr
	}
	return s.checkpoint, nil
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

func TestReadShardCheckpointReadsStreamShardAndSequence(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{checkpoint: "sequence-1"}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	seq, err := c.readShardCheckpoint(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("readShardCheckpoint() error = %v, want nil", err)
	}
	if seq != "sequence-1" {
		t.Fatalf("readShardCheckpoint() sequence = %q, want %q", seq, "sequence-1")
	}
	if len(store.getCalls) != 1 {
		t.Fatalf("Get calls = %d, want 1", len(store.getCalls))
	}
	call := store.getCalls[0]
	if call.streamName != "stream" {
		t.Fatalf("streamName = %q, want %q", call.streamName, "stream")
	}
	if call.shardID != "shard-1" {
		t.Fatalf("shardID = %q, want %q", call.shardID, "shard-1")
	}
}

func TestReadShardCheckpointUsesStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	store := &fakeCheckpointSaveStore{checkpoint: "sequence-1"}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamARN: streamARN},
		store:  store,
	}

	seq, err := c.readShardCheckpoint(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("readShardCheckpoint() error = %v, want nil", err)
	}
	if seq != "sequence-1" {
		t.Fatalf("readShardCheckpoint() sequence = %q, want %q", seq, "sequence-1")
	}
	if len(store.getCalls) != 1 {
		t.Fatalf("Get calls = %d, want 1", len(store.getCalls))
	}
	if store.getCalls[0].streamName != streamARN {
		t.Fatalf("streamName = %q, want %q", store.getCalls[0].streamName, streamARN)
	}
}

func TestReadShardCheckpointReturnsEmptyCheckpoint(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	seq, err := c.readShardCheckpoint(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("readShardCheckpoint() error = %v, want nil", err)
	}
	if seq != "" {
		t.Fatalf("readShardCheckpoint() sequence = %q, want empty", seq)
	}
}

func TestReadShardCheckpointForwardsContext(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	store := &fakeCheckpointSaveStore{checkpoint: "sequence-1"}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	if _, err := c.readShardCheckpoint(ctx, "shard-1"); err != nil {
		t.Fatalf("readShardCheckpoint() error = %v, want nil", err)
	}
	if len(store.getCalls) != 1 {
		t.Fatalf("Get calls = %d, want 1", len(store.getCalls))
	}
	if store.getCalls[0].ctx != ctx {
		t.Fatalf("Get context = %v, want %v", store.getCalls[0].ctx, ctx)
	}
}

func TestReadShardCheckpointWrapsStoreError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{getErr: errBoom}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	seq, err := c.readShardCheckpoint(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("readShardCheckpoint() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "read shard checkpoint shard-1: boom" {
		t.Fatalf("readShardCheckpoint() error = %v, want %q", err, "read shard checkpoint shard-1: boom")
	}
	if seq != "" {
		t.Fatalf("readShardCheckpoint() sequence = %q, want empty", seq)
	}
}

func TestSaveShardCheckpointSavesStreamShardAndSequence(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamARN: streamARN},
		store:  store,
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("saveShardCheckpoint() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "save shard checkpoint shard-1 sequence-1: boom" {
		t.Fatalf("saveShardCheckpoint() error = %v, want %q", err, "save shard checkpoint shard-1 sequence-1: boom")
	}
}

func TestSaveShardCompletionCheckpointSavesStreamShardAndMarker(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want nil", err)
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
	if call.sequenceNumber != "SHARD_END:sequence-1" {
		t.Fatalf("sequenceNumber = %q, want %q", call.sequenceNumber, "SHARD_END:sequence-1")
	}
}

func TestSaveShardCompletionCheckpointSavesEmptySequenceMarker(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", ""); err != nil {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "SHARD_END" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "SHARD_END")
	}
}

func TestSaveShardCompletionCheckpointUsesStreamARN(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamARN: streamARN},
		store:  store,
	}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].streamName != streamARN {
		t.Fatalf("streamName = %q, want %q", store.saveCalls[0].streamName, streamARN)
	}
}

func TestSaveShardCompletionCheckpointForwardsContext(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	if err := c.saveShardCompletionCheckpoint(ctx, "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want nil", err)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].ctx != ctx {
		t.Fatalf("Save context = %v, want %v", store.saveCalls[0].ctx, ctx)
	}
}

func TestSaveShardCompletionCheckpointWrapsStoreError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "save shard completion checkpoint shard-1 SHARD_END:sequence-1: boom" {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want %q", err, "save shard completion checkpoint shard-1 SHARD_END:sequence-1: boom")
	}
}

func TestSaveShardCheckpointIfDueSkipsBelowThreshold(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
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
		logger: slog.New(slog.DiscardHandler),
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
		logger: slog.New(slog.DiscardHandler),
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
		logger: slog.New(slog.DiscardHandler),
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
		logger: slog.New(slog.DiscardHandler),
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

func TestCheckpointOnDrainSavesPendingSequence(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	if err := c.checkpointOnDrain(context.Background(), "shard-1", "sequence-1", 2); err != nil {
		t.Fatalf("checkpointOnDrain() error = %v, want nil", err)
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

func TestCheckpointOnDrainNoopsWithoutPendingSequence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		sequenceNumber           string
		processedSinceCheckpoint int
	}{
		{name: "no processed records", sequenceNumber: "sequence-1", processedSinceCheckpoint: 0},
		{name: "empty sequence", sequenceNumber: "", processedSinceCheckpoint: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &fakeCheckpointSaveStore{}
			c := &Consumer{
				logger: slog.New(slog.DiscardHandler),
				cfg:    Config{StreamName: "stream"},
				store:  store,
			}

			if err := c.checkpointOnDrain(context.Background(), "shard-1", tt.sequenceNumber, tt.processedSinceCheckpoint); err != nil {
				t.Fatalf("checkpointOnDrain() error = %v, want nil", err)
			}
			if len(store.saveCalls) != 0 {
				t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
			}
		})
	}
}

func TestCheckpointOnDrainWrapsSaveError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		store:  store,
	}

	err := c.checkpointOnDrain(context.Background(), "shard-1", "sequence-1", 2)
	if !errors.Is(err, errBoom) {
		t.Fatalf("checkpointOnDrain() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "save drain shard checkpoint shard-1: save shard checkpoint shard-1 sequence-1: boom" {
		t.Fatalf("checkpointOnDrain() error = %v, want %q", err, "save drain shard checkpoint shard-1: save shard checkpoint shard-1 sequence-1: boom")
	}
}

func TestSaveShardCheckpointLogsSave(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	c := &Consumer{
		logger: slog.New(handler),
		cfg:    Config{StreamName: "stream"},
		store:  &fakeCheckpointSaveStore{},
	}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCheckpoint() error = %v, want nil", err)
	}

	records := handler.snapshot()
	saved, ok := findRecord(records, "shard checkpoint saved")
	if !ok {
		t.Fatalf("no 'shard checkpoint saved' record, got %+v", records)
	}
	if saved.level != slog.LevelDebug {
		t.Fatalf("saved level = %v, want Debug", saved.level)
	}
	if saved.attrs["shard"] != "shard-1" {
		t.Fatalf("saved shard attr = %q, want %q", saved.attrs["shard"], "shard-1")
	}
	if saved.attrs["sequence"] != "sequence-1" {
		t.Fatalf("saved sequence attr = %q, want %q", saved.attrs["sequence"], "sequence-1")
	}
}

func TestSaveShardCheckpointFailureLogsNothing(t *testing.T) {
	t.Parallel()

	errSave := errors.New("save boom")
	handler := newCapturingHandler()
	c := &Consumer{
		logger: slog.New(handler),
		cfg:    Config{StreamName: "stream"},
		store:  &fakeCheckpointSaveStore{saveErr: errSave},
	}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1"); !errors.Is(err, errSave) {
		t.Fatalf("saveShardCheckpoint() error = %v, want wraps %v", err, errSave)
	}

	if records := handler.snapshot(); len(records) != 0 {
		t.Fatalf("failed save emitted records: %+v", records)
	}
}

func TestCheckpointOnDrainLogsFlush(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	c := &Consumer{
		logger: slog.New(handler),
		cfg:    Config{StreamName: "stream"},
		store:  &fakeCheckpointSaveStore{},
	}

	if err := c.checkpointOnDrain(context.Background(), "shard-1", "sequence-9", 4); err != nil {
		t.Fatalf("checkpointOnDrain() error = %v, want nil", err)
	}

	records := handler.snapshot()
	flushed, ok := findRecord(records, "shard drain checkpoint flushed")
	if !ok {
		t.Fatalf("no 'shard drain checkpoint flushed' record, got %+v", records)
	}
	if flushed.level != slog.LevelDebug {
		t.Fatalf("flushed level = %v, want Debug", flushed.level)
	}
	if flushed.attrs["shard"] != "shard-1" {
		t.Fatalf("flushed shard attr = %q, want %q", flushed.attrs["shard"], "shard-1")
	}
	if flushed.attrs["sequence"] != "sequence-9" {
		t.Fatalf("flushed sequence attr = %q, want %q", flushed.attrs["sequence"], "sequence-9")
	}
	if flushed.attrs["records"] != "4" {
		t.Fatalf("flushed records attr = %q, want %q", flushed.attrs["records"], "4")
	}
}

func TestCheckpointOnDrainNoPendingLogsNothing(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	c := &Consumer{
		logger: slog.New(handler),
		cfg:    Config{StreamName: "stream"},
		store:  &fakeCheckpointSaveStore{},
	}

	if err := c.checkpointOnDrain(context.Background(), "shard-1", "sequence-9", 0); err != nil {
		t.Fatalf("checkpointOnDrain() error = %v, want nil", err)
	}

	if records := handler.snapshot(); len(records) != 0 {
		t.Fatalf("no-op drain emitted records: %+v", records)
	}
}

func TestSaveShardCompletionCheckpointLogsCompletion(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	c := &Consumer{
		logger: slog.New(handler),
		cfg:    Config{StreamName: "stream"},
		store:  &fakeCheckpointSaveStore{},
	}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want nil", err)
	}

	records := handler.snapshot()
	completed, ok := findRecord(records, "shard completed")
	if !ok {
		t.Fatalf("no 'shard completed' record, got %+v", records)
	}
	if completed.level != slog.LevelInfo {
		t.Fatalf("completed level = %v, want Info", completed.level)
	}
	if completed.attrs["shard"] != "shard-1" {
		t.Fatalf("completed shard attr = %q, want %q", completed.attrs["shard"], "shard-1")
	}
	if completed.attrs["checkpoint"] != "SHARD_END:sequence-1" {
		t.Fatalf("completed checkpoint attr = %q, want %q", completed.attrs["checkpoint"], "SHARD_END:sequence-1")
	}
}
