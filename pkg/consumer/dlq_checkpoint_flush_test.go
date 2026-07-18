package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

// newDLQFlushTestConsumer builds a consumer whose checkpointEvery is far
// larger than any test page, so the only way a checkpoint can be saved is the
// immediate post-DLQ flush under test.
func newDLQFlushTestConsumer(store *fakeCheckpointSaveStore, publisher DLQPublisher) *Consumer {
	return &Consumer{
		cfg:               Config{StreamName: "stream", ConsumerGroup: "group"},
		streamName:        "stream",
		logger:            slog.New(slog.DiscardHandler),
		reporter:          metrics.Nop{},
		store:             store,
		failurePolicy:     FailurePolicySendToDLQ,
		dlqPublisher:      publisher,
		dlqRetryAttempts:  1,
		dlqAttemptTimeout: time.Second,
		tuning:            tuningConfig{checkpointEvery: 100, retryMaxAttempts: 1},
	}
}

func TestProcessRecordsPageWithCheckpointFlushesImmediatelyAfterRecordDLQPublish(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{}
	dlq := &scriptedDLQPublisher{}
	c := newDLQFlushTestConsumer(store, dlq)
	c.handler = func(_ context.Context, record Record) error {
		if aws.ToString(record.SequenceNumber) == "sequence-2" {
			return errBoom
		}
		return nil
	}
	out := poisonTestOutput("sequence-1", "sequence-2", "sequence-3")

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 7)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (flush resets the since-checkpoint count)", count)
	}
	if published := dlq.snapshot(); len(published) != 1 || published[0].SequenceNumber != "sequence-2" {
		t.Fatalf("DLQ published = %+v, want exactly the poison record sequence-2", published)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1 (immediate flush despite checkpointEvery=100)", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-3" {
		t.Fatalf("flushed sequenceNumber = %q, want the page's last sequence %q", store.saveCalls[0].sequenceNumber, "sequence-3")
	}
}

func TestProcessRecordsPageWithCheckpointFlushesImmediatelyAfterBatchDLQPublish(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{}
	dlq := &scriptedDLQPublisher{}
	c := newDLQFlushTestConsumer(store, dlq)
	c.batchHandler = func(context.Context, []Record) error { return errBoom }
	out := poisonTestOutput("sequence-1", "sequence-2")

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 4)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (flush resets the since-checkpoint count)", count)
	}
	if published := dlq.snapshot(); len(published) != 2 {
		t.Fatalf("DLQ published %d records, want the whole failed page (2)", len(published))
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1 (immediate flush despite checkpointEvery=100)", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-2" {
		t.Fatalf("flushed sequenceNumber = %q, want the page's last sequence %q", store.saveCalls[0].sequenceNumber, "sequence-2")
	}
}

func TestProcessRecordsPageWithCheckpointFlushesAfterConcurrentRecordDLQPublish(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{}
	dlq := &scriptedDLQPublisher{}
	c := newDLQFlushTestConsumer(store, dlq)
	c.tuning.shardConcurrency = 2
	c.handler = func(_ context.Context, record Record) error {
		if aws.ToString(record.SequenceNumber) == "sequence-2" {
			return errBoom
		}
		return nil
	}
	out := poisonTestOutput("sequence-1", "sequence-2", "sequence-3", "sequence-4")

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 0)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-4" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-4")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (flush resets the since-checkpoint count)", count)
	}
	if published := dlq.snapshot(); len(published) != 1 || published[0].SequenceNumber != "sequence-2" {
		t.Fatalf("DLQ published = %+v, want exactly the poison record sequence-2", published)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1 (the flush must reach through the derived worker context)", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-4" {
		t.Fatalf("flushed sequenceNumber = %q, want the page's last sequence %q", store.saveCalls[0].sequenceNumber, "sequence-4")
	}
}

func TestProcessRecordsPageWithCheckpointWrapsDLQFlushError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("handler boom")
	errSave := errors.New("save boom")
	store := &fakeCheckpointSaveStore{saveErr: errSave}
	dlq := &scriptedDLQPublisher{}
	c := newDLQFlushTestConsumer(store, dlq)
	c.handler = func(context.Context, Record) error { return errBoom }
	out := poisonTestOutput("sequence-1")

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 2)
	if !errors.Is(err, errSave) {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want wraps %v", err, errSave)
	}
	want := "process records page dlq checkpoint shard-1: save shard checkpoint shard-1 sequence-1: save boom"
	if err == nil || err.Error() != want {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want %q", err, want)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3 (accumulated count carried on flush failure)", count)
	}
}

func TestProcessRecordsPageWithCheckpointDoesNotFlushWhenDLQPublishFails(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	errPublish := errors.New("dlq down")
	store := &fakeCheckpointSaveStore{}
	dlq := &scriptedDLQPublisher{errors: []error{errPublish}}
	c := newDLQFlushTestConsumer(store, dlq)
	c.handler = func(context.Context, Record) error { return errBoom }
	out := poisonTestOutput("sequence-1")

	_, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 2)
	if !errors.Is(err, errPublish) {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want wraps %v", err, errPublish)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2 (unchanged on page failure)", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0 (a failed publish must not checkpoint the page)", len(store.saveCalls))
	}
}

func TestProcessRecordsPageWithCheckpointSkipPolicyDoesNotForceFlush(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	store := &fakeCheckpointSaveStore{}
	c := newDLQFlushTestConsumer(store, nil)
	c.failurePolicy = FailurePolicySkip
	c.dlqPublisher = nil
	c.handler = func(_ context.Context, record Record) error {
		if aws.ToString(record.SequenceNumber) == "sequence-2" {
			return errBoom
		}
		return nil
	}
	out := poisonTestOutput("sequence-1", "sequence-2", "sequence-3")

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 7)
	if err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 10 {
		t.Fatalf("count = %d, want 10 (skip keeps the due-checkpoint cadence)", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0 (skip must not force a flush)", len(store.saveCalls))
	}
}
