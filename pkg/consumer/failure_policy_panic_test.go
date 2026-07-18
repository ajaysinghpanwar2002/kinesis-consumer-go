package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func newPanicTestConsumer(reporter metrics.Reporter, logger *slog.Logger) *Consumer {
	if reporter == nil {
		reporter = metrics.Nop{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Consumer{
		cfg:      Config{StreamName: "stream", ConsumerGroup: "group"},
		logger:   logger,
		reporter: reporter,
		tuning:   tuningConfig{retryMaxAttempts: 3, retryBackoff: time.Millisecond},
	}
}

func TestRecordHandlerPanicIsRetriedThenFailsFast(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	logHandler := newCapturingHandler()
	c := newPanicTestConsumer(reporter, slog.New(logHandler))
	attempts := 0
	c.handler = func(context.Context, Record) error {
		attempts++
		panic("kaboom")
	}

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	err := c.handleRecordWithRetry(context.Background(), "shard-1", record)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordWithRetry() error = %v, want wraps ErrHandlerPanic", err)
	}
	if !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("handleRecordWithRetry() error = %v, want panic value in message", err)
	}
	// A panic is an ordinary handler failure: every configured attempt runs.
	if attempts != 3 {
		t.Fatalf("handler attempts = %d, want 3 (panics are retried like errors)", attempts)
	}
	if retries := reporter.countersNamed(metricHandlerRetries); len(retries) != 2 {
		t.Fatalf("handler_retries calls = %d, want 2 (attempts beyond the first)", len(retries))
	}
	// handler_duration measures every attempt, panicking ones included.
	if durations := reporter.timingsNamed(metricHandlerDuration); len(durations) != 3 {
		t.Fatalf("handler_duration calls = %d, want 3 (one per attempt)", len(durations))
	}

	// The recovery is never silent: each panicking attempt logs the panic
	// value and the stack that identifies the offending handler frame.
	var panicLogs []capturedRecord
	for _, rec := range logHandler.snapshot() {
		if rec.message == "handler panic recovered" {
			panicLogs = append(panicLogs, rec)
		}
	}
	if len(panicLogs) != 3 {
		t.Fatalf("handler panic recovered logs = %d, want 3 (one per attempt)", len(panicLogs))
	}
	first := panicLogs[0]
	if first.level != slog.LevelError {
		t.Fatalf("panic log level = %v, want %v", first.level, slog.LevelError)
	}
	if first.attrs["panic"] != "kaboom" {
		t.Fatalf("panic log value = %q, want %q", first.attrs["panic"], "kaboom")
	}
	if first.attrs["shard"] != "shard-1" || first.attrs["handler"] != handlerKindRecord {
		t.Fatalf("panic log shard/handler = %q/%q, want shard-1/record", first.attrs["shard"], first.attrs["handler"])
	}
	if !strings.Contains(first.attrs["stack"], "failure_policy_panic_test") {
		t.Fatalf("panic log stack does not reach the panicking handler frame:\n%s", first.attrs["stack"])
	}
}

func TestRecordHandlerPanicRecoversAndSucceedsOnRetry(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newPanicTestConsumer(reporter, nil)
	attempts := 0
	c.handler = func(context.Context, Record) error {
		attempts++
		if attempts == 1 {
			panic("transient kaboom")
		}
		return nil
	}

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	if err := c.handleRecordWithRetry(context.Background(), "shard-1", record); err != nil {
		t.Fatalf("handleRecordWithRetry() error = %v, want nil (recovered on retry)", err)
	}
	if attempts != 2 {
		t.Fatalf("handler attempts = %d, want 2", attempts)
	}
	if processed := reporter.countersNamed(metricRecordsProcessed); len(processed) != 1 {
		t.Fatalf("records_processed calls = %d, want 1 (eventual success)", len(processed))
	}
}

func TestRecordHandlerPanicWithErrorValueStaysMatchable(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := newPanicTestConsumer(nil, nil)
	c.tuning.retryMaxAttempts = 1
	c.handler = func(context.Context, Record) error { panic(errBoom) }

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	err := c.handleRecordWithRetry(context.Background(), "shard-1", record)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordWithRetry() error = %v, want wraps ErrHandlerPanic", err)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordWithRetry() error = %v, want panic(err) value matchable via errors.Is", err)
	}
}

// panicPayload is a non-error custom type used as a panic value; its fields
// must survive into the converted error text and the structured panic log.
type panicPayload struct {
	Code   int
	Detail string
}

func TestRecordHandlerPanicWithCustomValueIsInspectable(t *testing.T) {
	t.Parallel()

	logHandler := newCapturingHandler()
	c := newPanicTestConsumer(nil, slog.New(logHandler))
	c.tuning.retryMaxAttempts = 1
	c.handler = func(context.Context, Record) error {
		panic(panicPayload{Code: 42, Detail: "custom kaboom"})
	}

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	err := c.handleRecordWithRetry(context.Background(), "shard-1", record)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordWithRetry() error = %v, want wraps ErrHandlerPanic", err)
	}
	for _, want := range []string{"42", "custom kaboom"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("handleRecordWithRetry() error = %v, want custom panic value field %q in message", err, want)
		}
	}
	panicLog, ok := findRecord(logHandler.snapshot(), "handler panic recovered")
	if !ok {
		t.Fatal("no 'handler panic recovered' log record captured")
	}
	for _, want := range []string{"42", "custom kaboom"} {
		if !strings.Contains(panicLog.attrs["panic"], want) {
			t.Fatalf("panic log value = %q, want custom panic value field %q", panicLog.attrs["panic"], want)
		}
	}
}

// panickingLogHandler is a slog.Handler whose Handle panics — the hostile
// logger case the recovery path must survive.
type panickingLogHandler struct{}

func (panickingLogHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panickingLogHandler) Handle(context.Context, slog.Record) error { panic("logger kaboom") }
func (h panickingLogHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h panickingLogHandler) WithGroup(string) slog.Handler           { return h }

func TestPanicRecoverySurvivesPanickingLogger(t *testing.T) {
	t.Parallel()

	// The conversion runs while a recovery guard is already unwinding; a
	// panicking slog.Handler must not escape it — in concurrent mode that
	// would unwind a bare worker goroutine and kill the process. The handler
	// panic must still come back as a converted error, minus the log line.
	c := newPanicTestConsumer(nil, slog.New(panickingLogHandler{}))
	c.tuning.retryMaxAttempts = 1
	c.tuning.shardConcurrency = 2
	c.handler = func(_ context.Context, record Record) error {
		if aws.ToString(record.SequenceNumber) == "sequence-1" {
			panic("kaboom under hostile logger")
		}
		return nil
	}

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
	}
	err := c.handleRecordsConcurrently(context.Background(), "shard-1", records)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordsConcurrently() error = %v, want wraps ErrHandlerPanic despite panicking logger", err)
	}
	if !strings.Contains(err.Error(), "kaboom under hostile logger") {
		t.Fatalf("handleRecordsConcurrently() error = %v, want handler panic value, not the logger's", err)
	}
}

func TestBatchHandlerPanicFollowsSkipPolicy(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newPanicTestConsumer(reporter, nil)
	c.failurePolicy = FailurePolicySkip
	c.batchHandler = func(context.Context, []Record) error { panic("batch kaboom") }

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
	}
	if err := c.handleBatchWithRetry(context.Background(), "shard-1", records); err != nil {
		t.Fatalf("handleBatchWithRetry() error = %v, want nil (skip policy treats panic as handled)", err)
	}
	skipped := reporter.countersNamed(metricRecordsSkipped)
	if len(skipped) != 1 || skipped[0].value != 2 {
		t.Fatalf("records_skipped = %v, want one call skipping the whole page (2 records)", skipped)
	}
}

func TestBatchHandlerPanicPublishesPoisonRecordsToDLQ(t *testing.T) {
	t.Parallel()

	publisher := &scriptedDLQPublisher{}
	c := newDLQPolicyTestConsumer(publisher)
	c.batchHandler = func(context.Context, []Record) error { panic("dlq kaboom") }

	records := []Record{
		{SequenceNumber: aws.String("sequence-1"), PartitionKey: aws.String("pk-1")},
		{SequenceNumber: aws.String("sequence-2"), PartitionKey: aws.String("pk-2")},
	}
	if err := c.handleBatchWithRetry(context.Background(), "shard-1", records); err != nil {
		t.Fatalf("handleBatchWithRetry() error = %v, want nil (published to DLQ)", err)
	}
	published := publisher.snapshot()
	if len(published) != 2 {
		t.Fatalf("poison records published = %d, want 2 (whole failed batch)", len(published))
	}
	for _, poison := range published {
		if !strings.Contains(poison.Error, "handler panicked") ||
			!strings.Contains(poison.Error, "dlq kaboom") {
			t.Fatalf("poison Error = %q, want the panic conversion and value", poison.Error)
		}
	}
}

func TestConcurrentRecordHandlerPanicFailsPageNotProcess(t *testing.T) {
	t.Parallel()

	c := newPanicTestConsumer(nil, nil)
	c.tuning.retryMaxAttempts = 1
	c.tuning.shardConcurrency = 3

	var handled atomic.Int64
	c.handler = func(_ context.Context, record Record) error {
		if aws.ToString(record.SequenceNumber) == "sequence-2" {
			panic("concurrent kaboom")
		}
		handled.Add(1)
		return nil
	}

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
		{SequenceNumber: aws.String("sequence-3")},
	}
	err := c.handleRecordsConcurrently(context.Background(), "shard-1", records)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordsConcurrently() error = %v, want wraps ErrHandlerPanic", err)
	}
	if !strings.Contains(err.Error(), "concurrent kaboom") {
		t.Fatalf("handleRecordsConcurrently() error = %v, want panic value in message", err)
	}
}

func TestConcurrentWorkerGoroutinePanicIsConvertedNotFatal(t *testing.T) {
	t.Parallel()

	// The attempt-level recover keeps handler panics out of the worker
	// goroutines entirely, so exercise the goroutine-body guard with a REAL
	// escaped panic: a nil reporter makes retryHandler's Timing call — which
	// runs after the guarded attempt returns — dereference a nil interface.
	// That is exactly the library-bug class the guard exists for; without it
	// this panic unwinds through the bare worker goroutine and kills the
	// whole test process.
	c := &Consumer{
		cfg:    Config{StreamName: "stream", ConsumerGroup: "group"},
		logger: slog.New(slog.DiscardHandler),
		tuning: tuningConfig{retryMaxAttempts: 1, retryBackoff: time.Millisecond, shardConcurrency: 2},
	}
	c.handler = func(context.Context, Record) error { return nil }

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
	}
	err := c.handleRecordsConcurrently(context.Background(), "shard-1", records)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordsConcurrently() error = %v, want the escaped panic converted to wrap ErrHandlerPanic", err)
	}
	if !strings.Contains(err.Error(), "nil pointer") && !strings.Contains(err.Error(), "invalid memory address") {
		t.Fatalf("handleRecordsConcurrently() error = %v, want the runtime panic value in the message", err)
	}
}

func TestConcurrentPanicStopsSiblingWorkersViaCancel(t *testing.T) {
	t.Parallel()

	// After the panicking record fails the page, the shared page context is
	// canceled and remaining unstarted records are never assigned: the page
	// fails like any first handler error, while the process survives.
	c := newPanicTestConsumer(nil, nil)
	c.tuning.retryMaxAttempts = 1
	c.tuning.shardConcurrency = 2

	var mu sync.Mutex
	started := map[string]bool{}
	c.handler = func(ctx context.Context, record Record) error {
		seq := aws.ToString(record.SequenceNumber)
		mu.Lock()
		started[seq] = true
		mu.Unlock()
		switch seq {
		case "sequence-1":
			// Hold this worker until the page context is canceled — which can
			// only happen via the sibling's panic conversion. This makes the
			// ordering deterministic: with 2 workers, one is parked here and
			// the other is panicking, so no worker is free to take records 3
			// or 4 before the cancel stops assignment.
			<-ctx.Done()
			return ctx.Err()
		case "sequence-2":
			panic("cancel kaboom")
		}
		return nil
	}

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
		{SequenceNumber: aws.String("sequence-3")},
		{SequenceNumber: aws.String("sequence-4")},
	}
	err := c.handleRecordsConcurrently(context.Background(), "shard-1", records)
	if !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("handleRecordsConcurrently() error = %v, want wraps ErrHandlerPanic (panic is the page's first error)", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !started["sequence-1"] || !started["sequence-2"] {
		t.Fatalf("started = %v, want both initial records started", started)
	}
	if started["sequence-3"] || started["sequence-4"] {
		t.Fatalf("started = %v, want assignment to stop after the panic canceled the page", started)
	}
}
