package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
)

// recordingReporter is a metrics.Reporter test double that records every
// counter call. It is mutex-guarded because some consumer paths emit from
// multiple goroutines.
type recordingReporter struct {
	mu       sync.Mutex
	counters []counterCall
}

type counterCall struct {
	name  string
	value int64
	tags  map[string]string
}

func (r *recordingReporter) Counter(name string, value int64, tags []metrics.Tag) {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = append(r.counters, counterCall{name: name, value: value, tags: tagMap})
}

func (r *recordingReporter) Gauge(string, float64, []metrics.Tag)        {}
func (r *recordingReporter) Timing(string, time.Duration, []metrics.Tag) {}
func (r *recordingReporter) Histogram(string, float64, []metrics.Tag)    {}

// countersNamed returns all recorded counter calls with the given name.
func (r *recordingReporter) countersNamed(name string) []counterCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []counterCall
	for _, call := range r.counters {
		if call.name == name {
			out = append(out, call)
		}
	}
	return out
}

func assertCounterTags(t *testing.T, call counterCall, want map[string]string) {
	t.Helper()
	if len(call.tags) != len(want) {
		t.Fatalf("%s tags = %v, want %v", call.name, call.tags, want)
	}
	for key, value := range want {
		if call.tags[key] != value {
			t.Fatalf("%s tag %q = %q, want %q", call.name, key, call.tags[key], value)
		}
	}
}

func newMetricsTestConsumer(reporter *recordingReporter) *Consumer {
	return &Consumer{
		cfg:      Config{StreamName: "stream"},
		logger:   slog.New(slog.DiscardHandler),
		reporter: reporter,
	}
}

func TestRecordHandlerSuccessCountsRecordsProcessed(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.handler = func(context.Context, Record) error { return nil }

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	if err := c.handleRecordWithRetry(context.Background(), "shard-1", record); err != nil {
		t.Fatalf("handleRecordWithRetry() error = %v, want nil", err)
	}

	processed := reporter.countersNamed(metricRecordsProcessed)
	if len(processed) != 1 {
		t.Fatalf("records_processed calls = %d, want 1", len(processed))
	}
	if processed[0].value != 1 {
		t.Fatalf("records_processed value = %d, want 1", processed[0].value)
	}
	assertCounterTags(t, processed[0], map[string]string{
		"stream":  "stream",
		"shard":   "shard-1",
		"handler": "record",
	})
}

func TestBatchHandlerSuccessCountsBatchSizeOnce(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.batchHandler = func(context.Context, []Record) error { return nil }

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
		{SequenceNumber: aws.String("sequence-3")},
	}
	if err := c.handleBatchWithRetry(context.Background(), "shard-1", records); err != nil {
		t.Fatalf("handleBatchWithRetry() error = %v, want nil", err)
	}

	processed := reporter.countersNamed(metricRecordsProcessed)
	if len(processed) != 1 {
		t.Fatalf("records_processed calls = %d, want 1 (one increment per batch)", len(processed))
	}
	if processed[0].value != 3 {
		t.Fatalf("records_processed value = %d, want 3", processed[0].value)
	}
	assertCounterTags(t, processed[0], map[string]string{
		"stream":  "stream",
		"shard":   "shard-1",
		"handler": "batch",
	})
}

func TestRetriesCountHandlerRetriesBeyondFirstAttempt(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.tuning = tuningConfig{retryMaxAttempts: 3, retryBackoff: time.Millisecond}
	attempts := 0
	c.handler = func(context.Context, Record) error {
		attempts++
		if attempts < 3 {
			return errors.New("boom")
		}
		return nil
	}

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	if err := c.handleRecordWithRetry(context.Background(), "shard-1", record); err != nil {
		t.Fatalf("handleRecordWithRetry() error = %v, want nil", err)
	}

	retries := reporter.countersNamed(metricHandlerRetries)
	if len(retries) != 2 {
		t.Fatalf("handler_retries calls = %d, want 2 (attempts beyond the first)", len(retries))
	}
	for _, call := range retries {
		if call.value != 1 {
			t.Fatalf("handler_retries value = %d, want 1", call.value)
		}
		assertCounterTags(t, call, map[string]string{
			"stream":  "stream",
			"shard":   "shard-1",
			"handler": "record",
		})
	}
	if processed := reporter.countersNamed(metricRecordsProcessed); len(processed) != 1 {
		t.Fatalf("records_processed calls = %d, want 1 (eventual success)", len(processed))
	}
}

func TestSkipPolicyCountsRecordsSkippedNotProcessed(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.failurePolicy = FailurePolicySkip
	c.batchHandler = func(context.Context, []Record) error { return errors.New("boom") }

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
	}
	if err := c.handleBatchWithRetry(context.Background(), "shard-1", records); err != nil {
		t.Fatalf("handleBatchWithRetry() error = %v, want nil (skip policy)", err)
	}

	skipped := reporter.countersNamed(metricRecordsSkipped)
	if len(skipped) != 1 {
		t.Fatalf("records_skipped calls = %d, want 1", len(skipped))
	}
	if skipped[0].value != 2 {
		t.Fatalf("records_skipped value = %d, want 2", skipped[0].value)
	}
	assertCounterTags(t, skipped[0], map[string]string{
		"stream": "stream",
		"shard":  "shard-1",
		"policy": "skip",
	})
	if processed := reporter.countersNamed(metricRecordsProcessed); len(processed) != 0 {
		t.Fatalf("records_processed calls = %d, want 0 (records were skipped)", len(processed))
	}
}

func TestDLQPolicyCountsPublishedRecords(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.failurePolicy = FailurePolicySendToDLQ
	c.dlqPublisher = &recordingPoisonPublisher{}
	c.batchHandler = func(context.Context, []Record) error { return errors.New("boom") }

	records := []Record{
		{SequenceNumber: aws.String("sequence-1")},
		{SequenceNumber: aws.String("sequence-2")},
	}
	if err := c.handleBatchWithRetry(context.Background(), "shard-1", records); err != nil {
		t.Fatalf("handleBatchWithRetry() error = %v, want nil (dlq policy)", err)
	}

	published := reporter.countersNamed(metricDLQRecordsPublished)
	if len(published) != 2 {
		t.Fatalf("dlq_records_published calls = %d, want 2 (one per record)", len(published))
	}
	for _, call := range published {
		if call.value != 1 {
			t.Fatalf("dlq_records_published value = %d, want 1", call.value)
		}
		assertCounterTags(t, call, map[string]string{
			"stream": "stream",
			"shard":  "shard-1",
		})
	}
	if processed := reporter.countersNamed(metricRecordsProcessed); len(processed) != 0 {
		t.Fatalf("records_processed calls = %d, want 0 (records went to dlq)", len(processed))
	}
}

func TestDLQPublishFailureCountsNothingPublished(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.failurePolicy = FailurePolicySendToDLQ
	c.dlqPublisher = &recordingPoisonPublisher{err: errors.New("dlq unavailable")}
	c.handler = func(context.Context, Record) error { return errors.New("boom") }

	record := types.Record{SequenceNumber: aws.String("sequence-1")}
	if err := c.handleRecordWithRetry(context.Background(), "shard-1", record); err == nil {
		t.Fatal("handleRecordWithRetry() error = nil, want dlq publish error")
	}

	if published := reporter.countersNamed(metricDLQRecordsPublished); len(published) != 0 {
		t.Fatalf("dlq_records_published calls = %d, want 0 (publish failed)", len(published))
	}
}

func TestSaveShardCheckpointCountsSave(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.store = &fakeCheckpointSaveStore{}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCheckpoint() error = %v, want nil", err)
	}

	saved := reporter.countersNamed(metricCheckpointsSaved)
	if len(saved) != 1 {
		t.Fatalf("checkpoints_saved calls = %d, want 1", len(saved))
	}
	if saved[0].value != 1 {
		t.Fatalf("checkpoints_saved value = %d, want 1", saved[0].value)
	}
	assertCounterTags(t, saved[0], map[string]string{
		"stream": "stream",
		"shard":  "shard-1",
	})
	if failures := reporter.countersNamed(metricCheckpointFailures); len(failures) != 0 {
		t.Fatalf("checkpoint_failures calls = %d, want 0", len(failures))
	}
}

func TestSaveShardCheckpointCountsFailure(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.store = &fakeCheckpointSaveStore{saveErr: errors.New("save boom")}

	if err := c.saveShardCheckpoint(context.Background(), "shard-1", "sequence-1"); err == nil {
		t.Fatal("saveShardCheckpoint() error = nil, want save error")
	}

	failures := reporter.countersNamed(metricCheckpointFailures)
	if len(failures) != 1 {
		t.Fatalf("checkpoint_failures calls = %d, want 1", len(failures))
	}
	assertCounterTags(t, failures[0], map[string]string{
		"stream": "stream",
		"shard":  "shard-1",
	})
	if saved := reporter.countersNamed(metricCheckpointsSaved); len(saved) != 0 {
		t.Fatalf("checkpoints_saved calls = %d, want 0", len(saved))
	}
}

func TestSaveShardCompletionCheckpointCountsSaveAndFailure(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.store = &fakeCheckpointSaveStore{}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1"); err != nil {
		t.Fatalf("saveShardCompletionCheckpoint() error = %v, want nil", err)
	}
	if saved := reporter.countersNamed(metricCheckpointsSaved); len(saved) != 1 {
		t.Fatalf("checkpoints_saved calls = %d, want 1", len(saved))
	}

	failing := &recordingReporter{}
	c = newMetricsTestConsumer(failing)
	c.store = &fakeCheckpointSaveStore{saveErr: errors.New("save boom")}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1"); err == nil {
		t.Fatal("saveShardCompletionCheckpoint() error = nil, want save error")
	}
	if failures := failing.countersNamed(metricCheckpointFailures); len(failures) != 1 {
		t.Fatalf("checkpoint_failures calls = %d, want 1", len(failures))
	}
}

func TestProcessShardRecordsPassCountsPagesFetched(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	getRecordsCalls := 0
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("iterator-2"),
			},
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-2")}},
				NextShardIterator: aws.String("iterator-3"),
			},
		},
		afterGetRecords: func() {
			getRecordsCalls++
			if getRecordsCalls == 2 {
				cancel()
			}
		},
	}
	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.client = client
	c.store = &fakeCheckpointSaveStore{}
	c.tuning = tuningConfig{checkpointEvery: 10}
	c.handler = func(context.Context, Record) error { return nil }

	if _, _, _, err := c.processShardRecordsPass(ctx, "shard-1", 0, ""); err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}

	pages := reporter.countersNamed(metricPagesFetched)
	if len(pages) != 2 {
		t.Fatalf("pages_fetched calls = %d, want 2", len(pages))
	}
	for _, call := range pages {
		if call.value != 1 {
			t.Fatalf("pages_fetched value = %d, want 1", call.value)
		}
		assertCounterTags(t, call, map[string]string{
			"stream": "stream",
			"shard":  "shard-1",
		})
	}
}
