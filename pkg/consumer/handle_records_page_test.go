package consumer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type recordingPoisonPublisher struct {
	records []PoisonRecord
	err     error
}

func (p *recordingPoisonPublisher) Publish(ctx context.Context, record PoisonRecord) error {
	_ = ctx
	if p.err != nil {
		return p.err
	}
	p.records = append(p.records, record)
	return nil
}

func TestHandleRecordsPageInvokesRecordHandlerInOrder(t *testing.T) {
	t.Parallel()

	var got []string
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			got = append(got, aws.ToString(record.SequenceNumber))
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

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if len(got) != 3 {
		t.Fatalf("handled records = %d, want 3", len(got))
	}
	for i, want := range []string{"sequence-1", "sequence-2", "sequence-3"} {
		if got[i] != want {
			t.Fatalf("handled record %d = %q, want %q", i, got[i], want)
		}
	}
}

func TestHandleRecordsPageInvokesBatchHandlerOnce(t *testing.T) {
	t.Parallel()

	var got [][]string
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		tuning: tuningConfig{shardConcurrency: 4},
		handler: func(ctx context.Context, record Record) error {
			t.Fatal("record handler called with batch handler configured")
			return nil
		},
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			var sequences []string
			for _, record := range records {
				sequences = append(sequences, aws.ToString(record.SequenceNumber))
			}
			got = append(got, sequences)
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("batch handler calls = %d, want 1", len(got))
	}
	if len(got[0]) != 2 {
		t.Fatalf("batch record count = %d, want 2", len(got[0]))
	}
	if got[0][0] != "sequence-1" || got[0][1] != "sequence-2" {
		t.Fatalf("batch records = %v, want [sequence-1 sequence-2]", got[0])
	}
}

func TestHandleRecordsPageLimitsRecordConcurrency(t *testing.T) {
	t.Parallel()

	started := make(chan string, 4)
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		tuning: tuningConfig{shardConcurrency: 2},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			now := active.Add(1)
			for {
				max := maxActive.Load()
				if now <= max || maxActive.CompareAndSwap(max, now) {
					break
				}
			}
			defer active.Add(-1)

			started <- aws.ToString(record.SequenceNumber)
			<-release
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
			{SequenceNumber: aws.String("sequence-3")},
			{SequenceNumber: aws.String("sequence-4")},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleRecordsPage(context.Background(), "shard-1", out)
	}()

	waitForStartedRecord(t, started)
	waitForStartedRecord(t, started)
	select {
	case seq := <-started:
		t.Fatalf("record %s started while two handlers were blocked", seq)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("handleRecordsPage() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handleRecordsPage() did not return")
	}
	if maxActive.Load() != 2 {
		t.Fatalf("max active handlers = %d, want 2", maxActive.Load())
	}
	if len(started) != 2 {
		t.Fatalf("remaining started records = %d, want 2", len(started))
	}
}

func TestHandleRecordsPageConcurrentRecordErrorCancelsAndStopsNewRecords(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	seq2Started := make(chan struct{})
	var seq3Calls atomic.Int32
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicyFailFast,
		tuning:        tuningConfig{shardConcurrency: 2, retryMaxAttempts: 1},
		handler: func(ctx context.Context, record Record) error {
			switch aws.ToString(record.SequenceNumber) {
			case "sequence-1":
				select {
				case <-seq2Started:
				case <-time.After(time.Second):
					t.Fatal("sequence-2 did not start")
				}
				return errBoom
			case "sequence-2":
				close(seq2Started)
				<-ctx.Done()
				return ctx.Err()
			case "sequence-3":
				seq3Calls.Add(1)
				return nil
			default:
				return nil
			}
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
			{SequenceNumber: aws.String("sequence-3")},
		},
	}

	err := c.handleRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	want := "handle records page shard-1: record handler: record handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("handleRecordsPage() error = %v, want %q", err, want)
	}
	if seq3Calls.Load() != 0 {
		t.Fatalf("sequence-3 calls = %d, want 0", seq3Calls.Load())
	}
}

func TestHandleRecordsPageConcurrentContextCancellationStopsWithoutFailurePolicy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	publisher := &recordingPoisonPublisher{}
	var calls atomic.Int32
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  publisher,
		tuning:        tuningConfig{shardConcurrency: 2, retryMaxAttempts: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			calls.Add(1)
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	err := c.handleRecordsPage(ctx, "shard-1", out)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handleRecordsPage() error = %v, want %v", err, context.Canceled)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler calls = %d, want 0", calls.Load())
	}
	if len(publisher.records) != 0 {
		t.Fatalf("published records = %d, want 0", len(publisher.records))
	}
}

func TestHandleRecordsPageTreatsEmptyPageAsNoop(t *testing.T) {
	t.Parallel()

	calls := 0
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		handler: func(ctx context.Context, record Record) error {
			calls++
			return nil
		},
		batchHandler: func(ctx context.Context, records []Record) error {
			calls++
			return nil
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", &kinesis.GetRecordsOutput{}); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if err := c.handleRecordsPage(context.Background(), "shard-1", nil); err != nil {
		t.Fatalf("handleRecordsPage(nil) error = %v, want nil", err)
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

func TestHandleRecordsPageWrapsRecordHandlerError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	calls := 0
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicyFailFast,
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			calls++
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
			{SequenceNumber: aws.String("sequence-3")},
		},
	}

	err := c.handleRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	want := "handle records page shard-1: record handler: record handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("handleRecordsPage() error = %v, want %q", err, want)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestHandleRecordsPageWrapsBatchHandlerError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicyFailFast,
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			_ = records
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	err := c.handleRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	want := "handle records page shard-1: batch handler: batch handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("handleRecordsPage() error = %v, want %q", err, want)
	}
}

func TestHandleRecordsPageRetriesRecordHandlerUntilSuccess(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	attempts := map[string]int{}
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicyFailFast,
		tuning:        tuningConfig{retryMaxAttempts: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			seq := aws.ToString(record.SequenceNumber)
			attempts[seq]++
			if seq == "sequence-1" && attempts[seq] < 3 {
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

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if attempts["sequence-1"] != 3 {
		t.Fatalf("sequence-1 attempts = %d, want 3", attempts["sequence-1"])
	}
	if attempts["sequence-2"] != 1 {
		t.Fatalf("sequence-2 attempts = %d, want 1", attempts["sequence-2"])
	}
}

func TestHandleRecordsPageRetriesBatchHandlerUntilSuccess(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	attempts := 0
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicyFailFast,
		tuning:        tuningConfig{retryMaxAttempts: 2},
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			_ = records
			attempts++
			if attempts == 1 {
				return errBoom
			}
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestHandleRecordsPageSkipPolicyTreatsPoisonRecordAsHandled(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	attempts := 0
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicySkip,
		tuning:        tuningConfig{retryMaxAttempts: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			attempts++
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestHandleRecordsPageSendToDLQPublishesPoisonRecord(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	arrival := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	publisher := &recordingPoisonPublisher{}
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		cfg:           Config{StreamName: "stream", StreamARN: "arn:stream"},
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  publisher,
		tuning:        tuningConfig{retryMaxAttempts: 2},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{
			SequenceNumber:              aws.String("sequence-1"),
			PartitionKey:                aws.String("partition-1"),
			ApproximateArrivalTimestamp: &arrival,
			Data:                        []byte("payload"),
		}},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if len(publisher.records) != 1 {
		t.Fatalf("published records = %d, want 1", len(publisher.records))
	}
	got := publisher.records[0]
	if got.StreamName != "stream" || got.StreamARN != "arn:stream" || got.ShardID != "shard-1" {
		t.Fatalf("poison stream/shard = %q/%q/%q, want stream/arn:stream/shard-1", got.StreamName, got.StreamARN, got.ShardID)
	}
	if got.SequenceNumber != "sequence-1" || got.PartitionKey != "partition-1" {
		t.Fatalf("poison sequence/partition = %q/%q, want sequence-1/partition-1", got.SequenceNumber, got.PartitionKey)
	}
	if got.ApproximateArrival == nil || !got.ApproximateArrival.Equal(arrival) {
		t.Fatalf("poison arrival = %v, want %v", got.ApproximateArrival, arrival)
	}
	if !slices.Equal(got.Payload, []byte("payload")) {
		t.Fatalf("poison payload = %q, want payload", string(got.Payload))
	}
	out.Records[0].Data[0] = 'P'
	if string(got.Payload) != "payload" {
		t.Fatalf("poison payload mutated to %q, want copied payload", string(got.Payload))
	}
	if got.Error != "boom" || got.Handler != handlerKindRecord || got.Attempts != 2 {
		t.Fatalf("poison error/handler/attempts = %q/%q/%d, want boom/record/2", got.Error, got.Handler, got.Attempts)
	}
	if got.BatchSize != 1 || got.RecordIndexInBatch != 0 || got.RecordSequenceInBatchOrder != 0 {
		t.Fatalf("poison batch fields = %d/%d/%d, want 1/0/0", got.BatchSize, got.RecordIndexInBatch, got.RecordSequenceInBatchOrder)
	}
	if got.FailedAt.IsZero() {
		t.Fatal("poison FailedAt is zero, want timestamp")
	}
}

func TestHandleRecordsPageSendToDLQPublishesEveryBatchRecord(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	publisher := &recordingPoisonPublisher{}
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  publisher,
		tuning:        tuningConfig{retryMaxAttempts: 1},
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			_ = records
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if len(publisher.records) != 2 {
		t.Fatalf("published records = %d, want 2", len(publisher.records))
	}
	for i, got := range publisher.records {
		if got.Handler != handlerKindBatch {
			t.Fatalf("record %d handler = %q, want %q", i, got.Handler, handlerKindBatch)
		}
		if got.BatchSize != 2 || got.RecordIndexInBatch != i || got.RecordSequenceInBatchOrder != i {
			t.Fatalf("record %d batch fields = %d/%d/%d, want 2/%d/%d", i, got.BatchSize, got.RecordIndexInBatch, got.RecordSequenceInBatchOrder, i, i)
		}
	}
}

func TestHandleRecordsPageSendToDLQPublishErrorFails(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	errDLQ := errors.New("dlq unavailable")
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  &recordingPoisonPublisher{err: errDLQ},
		tuning:        tuningConfig{retryMaxAttempts: 1},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	err := c.handleRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if !errors.Is(err, errDLQ) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errDLQ)
	}
	want := "handle records page shard-1: record handler: record handler failed after 1 attempts: boom; dlq publish: dlq unavailable"
	if err == nil || err.Error() != want {
		t.Fatalf("handleRecordsPage() error = %v, want %q", err, want)
	}
}

func TestHandleRecordsPageContextCancellationBypassesFailurePolicy(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	publisher := &recordingPoisonPublisher{}
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  publisher,
		tuning:        tuningConfig{retryMaxAttempts: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			attempts++
			cancel()
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	err := c.handleRecordsPage(ctx, "shard-1", out)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handleRecordsPage() error = %v, want %v", err, context.Canceled)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if len(publisher.records) != 0 {
		t.Fatalf("published records = %d, want 0", len(publisher.records))
	}
}

func TestHandleRecordsPageHandlerContextErrorsBypassFailurePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run("record "+tt.name, func(t *testing.T) {
			t.Parallel()

			attempts := 0
			publisher := &recordingPoisonPublisher{}
			c := &Consumer{
				logger:        slog.New(slog.DiscardHandler),
				failurePolicy: FailurePolicySendToDLQ,
				dlqPublisher:  publisher,
				tuning:        tuningConfig{retryMaxAttempts: 3},
				handler: func(ctx context.Context, record Record) error {
					_ = ctx
					_ = record
					attempts++
					return tt.err
				},
			}
			out := &kinesis.GetRecordsOutput{
				Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
			}

			err := c.handleRecordsPage(context.Background(), "shard-1", out)
			if !errors.Is(err, tt.err) {
				t.Fatalf("handleRecordsPage() error = %v, want %v", err, tt.err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
			if len(publisher.records) != 0 {
				t.Fatalf("published records = %d, want 0", len(publisher.records))
			}
		})

		t.Run("batch "+tt.name, func(t *testing.T) {
			t.Parallel()

			attempts := 0
			publisher := &recordingPoisonPublisher{}
			c := &Consumer{
				logger:        slog.New(slog.DiscardHandler),
				failurePolicy: FailurePolicySendToDLQ,
				dlqPublisher:  publisher,
				tuning:        tuningConfig{retryMaxAttempts: 3},
				batchHandler: func(ctx context.Context, records []Record) error {
					_ = ctx
					_ = records
					attempts++
					return tt.err
				},
			}
			out := &kinesis.GetRecordsOutput{
				Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
			}

			err := c.handleRecordsPage(context.Background(), "shard-1", out)
			if !errors.Is(err, tt.err) {
				t.Fatalf("handleRecordsPage() error = %v, want %v", err, tt.err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
			if len(publisher.records) != 0 {
				t.Fatalf("published records = %d, want 0", len(publisher.records))
			}
		})
	}
}

func waitForStartedRecord(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case seq := <-started:
		return seq
	case <-time.After(time.Second):
		t.Fatal("record handler did not start")
		return ""
	}
}

func TestHandleRecordsPageSkipPolicyLogsSkippedRecords(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	handler := newCapturingHandler()
	c := &Consumer{
		logger:        slog.New(handler),
		failurePolicy: FailurePolicySkip,
		tuning:        tuningConfig{retryMaxAttempts: 2},
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			_ = records
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}

	records := handler.snapshot()
	skipped, ok := findRecord(records, "records skipped after handler failure")
	if !ok {
		t.Fatalf("no 'records skipped after handler failure' record, got %+v", records)
	}
	if skipped.level != slog.LevelWarn {
		t.Fatalf("skipped level = %v, want Warn", skipped.level)
	}
	wantAttrs := map[string]string{
		"shard": "shard-1", "handler": handlerKindBatch, "records": "2", "attempts": "2",
	}
	for key, want := range wantAttrs {
		if skipped.attrs[key] != want {
			t.Fatalf("skipped attr %q = %q, want %q (attrs %+v)", key, skipped.attrs[key], want, skipped.attrs)
		}
	}
	if !strings.Contains(skipped.attrs["error"], errBoom.Error()) {
		t.Fatalf("skipped error attr = %q, want to contain %q", skipped.attrs["error"], errBoom.Error())
	}
	if _, ok := findRecord(records, "poison records published to dlq"); ok {
		t.Fatalf("unexpected dlq record under skip policy, got %+v", records)
	}
}

func TestHandleRecordsPageSendToDLQLogsPublishedPoisonRecords(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	handler := newCapturingHandler()
	c := &Consumer{
		logger:        slog.New(handler),
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  &recordingPoisonPublisher{},
		tuning:        tuningConfig{retryMaxAttempts: 1},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}

	records := handler.snapshot()
	published, ok := findRecord(records, "poison records published to dlq")
	if !ok {
		t.Fatalf("no 'poison records published to dlq' record, got %+v", records)
	}
	if published.level != slog.LevelWarn {
		t.Fatalf("published level = %v, want Warn", published.level)
	}
	wantAttrs := map[string]string{
		"shard": "shard-1", "handler": handlerKindRecord, "records": "1", "attempts": "1",
	}
	for key, want := range wantAttrs {
		if published.attrs[key] != want {
			t.Fatalf("published attr %q = %q, want %q (attrs %+v)", key, published.attrs[key], want, published.attrs)
		}
	}
	if !strings.Contains(published.attrs["error"], errBoom.Error()) {
		t.Fatalf("published error attr = %q, want to contain %q", published.attrs["error"], errBoom.Error())
	}
	if _, ok := findRecord(records, "records skipped after handler failure"); ok {
		t.Fatalf("unexpected skip record under dlq policy, got %+v", records)
	}
}

func TestHandleRecordsPageSendToDLQPublishErrorLogsNothing(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	errDLQ := errors.New("dlq unavailable")
	handler := newCapturingHandler()
	c := &Consumer{
		logger:        slog.New(handler),
		failurePolicy: FailurePolicySendToDLQ,
		dlqPublisher:  &recordingPoisonPublisher{err: errDLQ},
		tuning:        tuningConfig{retryMaxAttempts: 1},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); !errors.Is(err, errDLQ) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errDLQ)
	}

	if records := handler.snapshot(); len(records) != 0 {
		t.Fatalf("failed dlq publish emitted records: %+v", records)
	}
}

func TestHandleRecordsPageFailFastLogsNothing(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	handler := newCapturingHandler()
	c := &Consumer{
		logger:        slog.New(handler),
		failurePolicy: FailurePolicyFailFast,
		tuning:        tuningConfig{retryMaxAttempts: 1},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}

	if records := handler.snapshot(); len(records) != 0 {
		t.Fatalf("fail-fast emitted records: %+v", records)
	}
}
