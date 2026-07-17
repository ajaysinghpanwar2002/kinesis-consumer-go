package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type scriptedDLQPublisher struct {
	mu      sync.Mutex
	errors  []error
	records []PoisonRecord
}

func (p *scriptedDLQPublisher) Publish(_ context.Context, record PoisonRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	call := len(p.records)
	p.records = append(p.records, record)
	if call < len(p.errors) {
		return p.errors[call]
	}
	return nil
}

func (p *scriptedDLQPublisher) snapshot() []PoisonRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PoisonRecord(nil), p.records...)
}

func newDLQPolicyTestConsumer(publisher DLQPublisher) *Consumer {
	return &Consumer{
		cfg:               Config{StreamName: "orders", ConsumerGroup: "billing"},
		streamName:        "orders",
		logger:            slog.New(slog.DiscardHandler),
		reporter:          metrics.Nop{},
		failurePolicy:     FailurePolicySendToDLQ,
		dlqPublisher:      publisher,
		dlqRetryAttempts:  3,
		dlqRetryBackoff:   time.Millisecond,
		dlqAttemptTimeout: time.Second,
		tuning:            tuningConfig{retryMaxAttempts: 1},
	}
}

func poisonTestOutput(sequenceNumbers ...string) *kinesis.GetRecordsOutput {
	records := make([]types.Record, 0, len(sequenceNumbers))
	for _, sequenceNumber := range sequenceNumbers {
		records = append(records, types.Record{
			SequenceNumber: aws.String(sequenceNumber),
			PartitionKey:   aws.String("partition-" + sequenceNumber),
			Data:           []byte("payload-" + sequenceNumber),
		})
	}
	return &kinesis.GetRecordsOutput{Records: records}
}

func TestDLQRetrySucceedsWithoutRepeatingHandler(t *testing.T) {
	t.Parallel()

	errUnavailable := errors.New("dlq unavailable")
	publisher := &scriptedDLQPublisher{errors: []error{errUnavailable, errUnavailable}}
	c := newDLQPolicyTestConsumer(publisher)
	handlerCalls := 0
	c.handler = func(context.Context, Record) error {
		handlerCalls++
		return errors.New("poison")
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", poisonTestOutput("sequence-1")); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1; DLQ retries must not repeat the handler", handlerCalls)
	}
	records := publisher.snapshot()
	if len(records) != 3 {
		t.Fatalf("DLQ calls = %d, want 3", len(records))
	}
	for i := 1; i < len(records); i++ {
		if records[i].IdempotencyKey != records[0].IdempotencyKey {
			t.Fatalf("retry %d key = %q, want %q", i+1, records[i].IdempotencyKey, records[0].IdempotencyKey)
		}
	}
}

func TestDLQRetryExhaustionPreservesHandlerAndPublishCauses(t *testing.T) {
	t.Parallel()

	errHandler := errors.New("handler poison")
	errPublish := errors.New("dlq unavailable")
	publisher := &scriptedDLQPublisher{errors: []error{errPublish, errPublish, errPublish}}
	c := newDLQPolicyTestConsumer(publisher)
	c.handler = func(context.Context, Record) error { return errHandler }

	err := c.handleRecordsPage(context.Background(), "shard-1", poisonTestOutput("sequence-1"))
	if !errors.Is(err, errHandler) || !errors.Is(err, errPublish) {
		t.Fatalf("handleRecordsPage() error = %v, want both handler and DLQ causes", err)
	}
	if records := publisher.snapshot(); len(records) != 3 {
		t.Fatalf("DLQ calls = %d, want 3", len(records))
	}
}

type blockingDLQAttemptPublisher struct {
	started chan context.Context
	release chan struct{}
}

func (p *blockingDLQAttemptPublisher) Publish(ctx context.Context, _ PoisonRecord) error {
	p.started <- ctx
	<-p.release
	return nil
}

func TestDLQAttemptTimeoutBoundsContextIgnoringPublisher(t *testing.T) {
	publisher := &blockingDLQAttemptPublisher{
		started: make(chan context.Context, 1),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(publisher.release) })
	c := newDLQPolicyTestConsumer(publisher)
	c.dlqRetryAttempts = 1
	c.dlqAttemptTimeout = 20 * time.Millisecond
	c.handler = func(context.Context, Record) error { return errors.New("poison") }

	startedAt := time.Now()
	err := c.handleRecordsPage(context.Background(), "shard-1", poisonTestOutput("sequence-1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handleRecordsPage() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("timed-out Publish blocked for %v, want < 1s", elapsed)
	}

	var attemptCtx context.Context
	select {
	case attemptCtx = <-publisher.started:
	default:
		t.Fatal("Publish did not start")
	}
	if !errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("Publish context error = %v, want deadline exceeded", attemptCtx.Err())
	}
}

type successAfterDeadlineDLQPublisher struct{}

func (successAfterDeadlineDLQPublisher) Publish(ctx context.Context, _ PoisonRecord) error {
	<-ctx.Done()
	return nil
}

func TestDLQAttemptRejectsSuccessReturnedAfterDeadline(t *testing.T) {
	c := newDLQPolicyTestConsumer(successAfterDeadlineDLQPublisher{})
	c.dlqAttemptTimeout = time.Millisecond
	poison := c.newPoisonRecord(
		"shard-1",
		handlerKindRecord,
		1,
		errors.New("poison"),
		types.Record{SequenceNumber: aws.String("sequence-1")},
		1,
		0,
	)

	// The deadline and nil result are deliberately made ready together. Repeat
	// enough times to exercise either select branch: both must reject the late
	// result, or a timed-out poison record could incorrectly be checkpointed.
	for attempt := range 100 {
		if err := c.publishPoisonRecordAttempt(context.Background(), poison); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt %d error = %v, want deadline exceeded", attempt+1, err)
		}
	}
}

type cancelAwareDLQPublisher struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
}

func (p *cancelAwareDLQPublisher) Publish(ctx context.Context, _ PoisonRecord) error {
	p.mu.Lock()
	p.calls++
	if p.calls == 1 {
		close(p.started)
	}
	p.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (p *cancelAwareDLQPublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func TestDLQParentCancellationStopsWithoutRetry(t *testing.T) {
	publisher := &cancelAwareDLQPublisher{started: make(chan struct{})}
	c := newDLQPolicyTestConsumer(publisher)
	c.dlqRetryBackoff = time.Hour
	c.handler = func(context.Context, Record) error { return errors.New("poison") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.handleRecordsPage(ctx, "shard-1", poisonTestOutput("sequence-1"))
	}()
	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("Publish did not start")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handleRecordsPage() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handleRecordsPage() did not stop on parent cancellation")
	}
	if calls := publisher.callCount(); calls != 1 {
		t.Fatalf("DLQ calls = %d, want 1", calls)
	}
}

type failSequenceDLQPublisher struct {
	mu           sync.Mutex
	failSequence string
	records      []PoisonRecord
}

func (p *failSequenceDLQPublisher) Publish(_ context.Context, record PoisonRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, record)
	if record.SequenceNumber == p.failSequence {
		return errors.New("nth record failed")
	}
	return nil
}

func (p *failSequenceDLQPublisher) snapshot() []PoisonRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PoisonRecord(nil), p.records...)
}

func TestBatchDLQNthRecordFailureAndRestartUseStableKeys(t *testing.T) {
	t.Parallel()

	out := poisonTestOutput("sequence-1", "sequence-2", "sequence-3")
	firstPublisher := &failSequenceDLQPublisher{failSequence: "sequence-2"}
	first := newDLQPolicyTestConsumer(firstPublisher)
	first.dlqRetryAttempts = 2
	first.batchHandler = func(context.Context, []Record) error { return errors.New("batch poison") }

	if err := first.handleRecordsPage(context.Background(), "shard-1", out); err == nil {
		t.Fatal("first handleRecordsPage() error = nil, want nth-record DLQ failure")
	}
	firstRecords := firstPublisher.snapshot()
	if len(firstRecords) != 3 {
		t.Fatalf("first-run DLQ calls = %d, want 3 (sequence-1 once, sequence-2 twice)", len(firstRecords))
	}
	if firstRecords[0].SequenceNumber != "sequence-1" ||
		firstRecords[1].SequenceNumber != "sequence-2" ||
		firstRecords[2].SequenceNumber != "sequence-2" {
		t.Fatalf("first-run sequences = %q/%q/%q, want sequence-1/sequence-2/sequence-2",
			firstRecords[0].SequenceNumber, firstRecords[1].SequenceNumber, firstRecords[2].SequenceNumber)
	}
	if firstRecords[1].IdempotencyKey != firstRecords[2].IdempotencyKey {
		t.Fatalf("nth-record retry keys differ: %q != %q", firstRecords[1].IdempotencyKey, firstRecords[2].IdempotencyKey)
	}

	restartPublisher := &scriptedDLQPublisher{}
	restarted := newDLQPolicyTestConsumer(restartPublisher)
	restarted.batchHandler = func(context.Context, []Record) error { return errors.New("different text after restart") }
	if err := restarted.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("restarted handleRecordsPage() error = %v, want nil", err)
	}
	restartedRecords := restartPublisher.snapshot()
	if len(restartedRecords) != 3 {
		t.Fatalf("restart DLQ calls = %d, want 3", len(restartedRecords))
	}
	if restartedRecords[0].IdempotencyKey != firstRecords[0].IdempotencyKey {
		t.Fatalf("successful-prefix restart key = %q, want %q", restartedRecords[0].IdempotencyKey, firstRecords[0].IdempotencyKey)
	}
	if restartedRecords[1].IdempotencyKey != firstRecords[1].IdempotencyKey {
		t.Fatalf("nth-record restart key = %q, want %q", restartedRecords[1].IdempotencyKey, firstRecords[1].IdempotencyKey)
	}
}

func TestPoisonRecordIdempotencyKeyUsesCanonicalIdentityInRecordAndBatchModes(t *testing.T) {
	t.Parallel()

	record := types.Record{SequenceNumber: aws.String("sequence-1"), Data: []byte("payload")}
	cause := errors.New("first failure text")
	nameConsumer := &Consumer{
		cfg:        Config{StreamName: "orders", ConsumerGroup: "billing"},
		streamName: "orders",
	}
	arnConsumer := &Consumer{
		cfg: Config{
			StreamARN:     "arn:aws:kinesis:us-east-1:123456789012:stream/orders",
			ConsumerGroup: "billing",
		},
		streamName: "orders",
	}

	for _, handlerKind := range []string{handlerKindRecord, handlerKindBatch} {
		fromName := nameConsumer.newPoisonRecord("shard-1", handlerKind, 1, cause, record, 1, 0)
		fromARN := arnConsumer.newPoisonRecord("shard-1", handlerKind, 7, errors.New("changed failure text"), record, 9, 8)
		if fromName.IdempotencyKey == "" {
			t.Fatalf("%s IdempotencyKey is empty", handlerKind)
		}
		if fromName.IdempotencyKey != fromARN.IdempotencyKey {
			t.Fatalf("%s name/ARN keys differ: %q != %q", handlerKind, fromName.IdempotencyKey, fromARN.IdempotencyKey)
		}
	}

	recordKey := nameConsumer.newPoisonRecord("shard-1", handlerKindRecord, 1, cause, record, 1, 0).IdempotencyKey
	batchKey := nameConsumer.newPoisonRecord("shard-1", handlerKindBatch, 1, cause, record, 1, 0).IdempotencyKey
	if recordKey == batchKey {
		t.Fatalf("record and batch keys are equal: %q", recordKey)
	}

	base := poisonRecordIdempotencyKey("billing", "orders", "shard-1", "sequence-1", handlerKindRecord)
	variants := []string{
		poisonRecordIdempotencyKey("shipping", "orders", "shard-1", "sequence-1", handlerKindRecord),
		poisonRecordIdempotencyKey("billing", "returns", "shard-1", "sequence-1", handlerKindRecord),
		poisonRecordIdempotencyKey("billing", "orders", "shard-2", "sequence-1", handlerKindRecord),
		poisonRecordIdempotencyKey("billing", "orders", "shard-1", "sequence-2", handlerKindRecord),
		poisonRecordIdempotencyKey("billing", "orders", "shard-1", "sequence-1", handlerKindBatch),
	}
	for i, variant := range variants {
		if variant == base {
			t.Fatalf("identity variant %d did not change key %q", i, base)
		}
	}
}
