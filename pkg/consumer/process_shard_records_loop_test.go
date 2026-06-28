package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestProcessShardRecordsLoopRunsPassAndReturnsOnCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var handled []string
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
		afterGetRecords: cancel,
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(handled) != 1 || handled[0] != "sequence-1" {
		t.Fatalf("handled records = %v, want [sequence-1]", handled)
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
}

func TestProcessShardRecordsLoopCarriesCheckpointCountBetweenPasses(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	passCalls := 0
	c := &Consumer{
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			passCalls++
			if passCalls == 1 {
				if processedSinceCheckpoint != 0 {
					t.Fatalf("first processedSinceCheckpoint = %d, want 0", processedSinceCheckpoint)
				}
				return "sequence-1", 1, nil
			}
			if processedSinceCheckpoint != 1 {
				t.Fatalf("second processedSinceCheckpoint = %d, want 1", processedSinceCheckpoint)
			}
			cancel()
			return "sequence-2", 2, nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if passCalls != 2 {
		t.Fatalf("pass calls = %d, want 2", passCalls)
	}
}

func TestProcessShardRecordsLoopCarriesLatestNonEmptySequence(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	getRecordsCalls := 0
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
			{},
		},
		afterGetRecords: func() {
			getRecordsCalls++
			if getRecordsCalls == 2 {
				cancel()
			}
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestProcessShardRecordsLoopWrapsPassError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getShardIteratorErr: errBoom}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
	}

	lastSeq, count, err := c.processShardRecordsLoop(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsLoop() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process shard records loop shard-1: process shard records pass shard-1: poll shard records pages shard-1: get shard iterator shard-1: boom" {
		t.Fatalf("processShardRecordsLoop() error = %v, want %q", err, "process shard records loop shard-1: process shard records pass shard-1: poll shard records pages shard-1: get shard iterator shard-1: boom")
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessShardRecordsLoopTreatsShardCompletedAsNormalCompletion(t *testing.T) {
	t.Parallel()

	passCalls := 0
	sleepCalls := 0
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			passCalls++
			if passCalls == 1 {
				return "sequence-1", processedSinceCheckpoint + 2, nil
			}
			if processedSinceCheckpoint != 2 {
				t.Fatalf("processedSinceCheckpoint = %d, want 2", processedSinceCheckpoint)
			}
			return "", processedSinceCheckpoint, fmt.Errorf("poll shard records pages shard-1: %w", errShardCompleted)
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			_ = d
			sleepCalls++
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(context.Background(), "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if passCalls != 2 {
		t.Fatalf("pass calls = %d, want 2", passCalls)
	}
	if sleepCalls != 0 {
		t.Fatalf("sleep calls = %d, want 0", sleepCalls)
	}
}

func TestProcessShardRecordsLoopTreatsContextCancellationAsNormalShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
	}
	c := &Consumer{
		cfg:    Config{StreamName: "stream"},
		client: client,
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if len(client.getShardIteratorCalls) != 0 {
		t.Fatalf("GetShardIterator calls = %d, want 0", len(client.getShardIteratorCalls))
	}
}

func TestProcessShardRecordsLoopSleepsAfterNoProgressPass(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	passCalls := 0
	var slept time.Duration
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			passCalls++
			if passCalls == 2 {
				cancel()
			}
			return "", processedSinceCheckpoint, nil
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			slept = d
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if passCalls != 2 {
		t.Fatalf("pass calls = %d, want 2", passCalls)
	}
	if slept != 25*time.Millisecond {
		t.Fatalf("slept = %v, want %v", slept, 25*time.Millisecond)
	}
}

func TestProcessShardRecordsLoopDoesNotSleepAfterProgressPass(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	passCalls := 0
	sleepCalls := 0
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			passCalls++
			if passCalls == 1 {
				return "sequence-1", processedSinceCheckpoint + 1, nil
			}
			cancel()
			return "", processedSinceCheckpoint, nil
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			_ = d
			sleepCalls++
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if sleepCalls != 1 {
		t.Fatalf("sleep calls = %d, want 1", sleepCalls)
	}
}

func TestProcessShardRecordsLoopDoesNotSleepAfterCheckpointCountProgress(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	passCalls := 0
	sleepCalls := 0
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			passCalls++
			if passCalls == 1 {
				return "", processedSinceCheckpoint + 1, nil
			}
			cancel()
			return "", processedSinceCheckpoint, nil
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			_ = d
			sleepCalls++
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if sleepCalls != 1 {
		t.Fatalf("sleep calls = %d, want 1", sleepCalls)
	}
}

func TestProcessShardRecordsLoopCancellationDuringPauseReturnsNil(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			return "", processedSinceCheckpoint, nil
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = d
			cancel()
			return context.Canceled
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if err != nil {
		t.Fatalf("processShardRecordsLoop() error = %v, want nil", err)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessShardRecordsLoopDeadlineDuringPauseReturnsError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			return "", processedSinceCheckpoint, nil
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			_ = d
			return context.DeadlineExceeded
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(ctx, "shard-1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("processShardRecordsLoop() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if err == nil || err.Error() != "process shard records loop shard-1: context deadline exceeded" {
		t.Fatalf("processShardRecordsLoop() error = %v, want %q", err, "process shard records loop shard-1: context deadline exceeded")
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessShardRecordsLoopSkipsSleepAfterPassError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	sleepCalls := 0
	c := &Consumer{
		tuning: tuningConfig{pollInterval: 25 * time.Millisecond},
		processShardRecordsPassFn: func(ctx context.Context, shardID string, processedSinceCheckpoint int) (string, int, error) {
			_ = ctx
			_ = shardID
			return "", processedSinceCheckpoint, errBoom
		},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			_ = d
			sleepCalls++
			return nil
		},
	}

	lastSeq, count, err := c.processShardRecordsLoop(context.Background(), "shard-1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsLoop() error = %v, want wraps %v", err, errBoom)
	}
	if sleepCalls != 0 {
		t.Fatalf("sleep calls = %d, want 0", sleepCalls)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}
