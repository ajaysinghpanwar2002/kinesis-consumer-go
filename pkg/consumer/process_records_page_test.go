package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

func TestProcessRecordsPageReturnsLastSequenceAndCount(t *testing.T) {
	t.Parallel()

	var handled []string
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
			{SequenceNumber: aws.String("sequence-3")},
		},
	}

	lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
	if err != nil {
		t.Fatalf("processRecordsPage() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	if len(handled) != 3 {
		t.Fatalf("handled records = %d, want 3", len(handled))
	}
}

func TestProcessRecordsPageTreatsNilAndEmptyPagesAsZeroProgress(t *testing.T) {
	t.Parallel()

	calls := 0
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		handler: func(ctx context.Context, record Record) error {
			calls++
			return nil
		},
	}

	for _, out := range []*kinesis.GetRecordsOutput{nil, {}} {
		lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
		if err != nil {
			t.Fatalf("processRecordsPage() error = %v, want nil", err)
		}
		if lastSeq != "" {
			t.Fatalf("lastSeq = %q, want empty", lastSeq)
		}
		if count != 0 {
			t.Fatalf("count = %d, want 0", count)
		}
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

func TestProcessRecordsPageDefaultPolicyReturnsNoProgressWhenHandlerFails(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			if aws.ToString(record.SequenceNumber) == "sequence-2" {
				return errBoom
			}
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("processRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	want := "process records page shard-1: handle records page shard-1: record handler: record handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("processRecordsPage() error = %v, want %q", err, want)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessRecordsPageDefaultPolicyWrapsBatchHandlerError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			_ = records
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("processRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	want := "process records page shard-1: handle records page shard-1: batch handler: batch handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("processRecordsPage() error = %v, want %q", err, want)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessRecordsPageWithCheckpointCarriesCountBelowThreshold(t *testing.T) {
	t.Parallel()

	var handled []string
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 5},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 1)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	if len(handled) != 2 {
		t.Fatalf("handled records = %d, want 2", len(handled))
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessRecordsPageWithCheckpointSavesAtThreshold(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 1)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-2" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-2")
	}
}

func TestProcessRecordsPageWithCheckpointRejectsLateSuccessAfterContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 1},
		handler: func(context.Context, Record) error {
			cancel()
			return nil // deliberately report success after cancellation
		},
	}
	out := &kinesis.GetRecordsOutput{Records: []types.Record{{
		SequenceNumber: aws.String("sequence-1"),
	}}}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(ctx, "shard-1", out, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want wraps %v", err, context.Canceled)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessRecordsPageWithCheckpointCarriesModuloRemainder(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
			{SequenceNumber: aws.String("sequence-3")},
		},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 2)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-3" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-3")
	}
}

func TestProcessRecordsPageWithCheckpointTreatsEmptyPageAsNoop(t *testing.T) {
	t.Parallel()

	calls := 0
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			calls++
			return nil
		},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", &kinesis.GetRecordsOutput{}, 2)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessRecordsPageWithCheckpointWrapsPageProcessingError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		reporter:      metrics.Nop{},
		cfg:           Config{StreamName: "stream"},
		store:         store,
		failurePolicy: FailurePolicyFailFast,
		tuning:        tuningConfig{checkpointEvery: 1},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 2)
	if !errors.Is(err, errBoom) {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want wraps %v", err, errBoom)
	}
	want := "process records page with checkpoint shard-1: process records page shard-1: handle records page shard-1: record handler: record handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want %q", err, want)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessRecordsPageWithCheckpointWrapsCheckpointError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 1},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 2)
	if !errors.Is(err, errBoom) {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process records page checkpoint shard-1: save due shard checkpoint shard-1: save shard checkpoint shard-1 sequence-1: boom" {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want %q", err, "process records page checkpoint shard-1: save due shard checkpoint shard-1: save shard checkpoint shard-1 sequence-1: boom")
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}
