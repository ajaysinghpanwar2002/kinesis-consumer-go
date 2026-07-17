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

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

// recordingReporter is a metrics.Reporter test double that records every
// counter, gauge, and timing call. It is mutex-guarded because some consumer
// paths emit from multiple goroutines.
type recordingReporter struct {
	mu       sync.Mutex
	counters []counterCall
	gauges   []gaugeCall
	timings  []timingCall
}

type counterCall struct {
	name  string
	value int64
	tags  map[string]string
}

type gaugeCall struct {
	name  string
	value float64
	tags  map[string]string
}

type timingCall struct {
	name  string
	value time.Duration
	tags  map[string]string
}

func metricTagMap(tags []metrics.Tag) map[string]string {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}
	return tagMap
}

func (r *recordingReporter) Counter(name string, value int64, tags []metrics.Tag) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = append(r.counters, counterCall{name: name, value: value, tags: metricTagMap(tags)})
}

func (r *recordingReporter) Gauge(name string, value float64, tags []metrics.Tag) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges = append(r.gauges, gaugeCall{name: name, value: value, tags: metricTagMap(tags)})
}

func (r *recordingReporter) Timing(name string, value time.Duration, tags []metrics.Tag) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timings = append(r.timings, timingCall{name: name, value: value, tags: metricTagMap(tags)})
}

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

// gaugesNamed returns all recorded gauge calls with the given name.
func (r *recordingReporter) gaugesNamed(name string) []gaugeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []gaugeCall
	for _, call := range r.gauges {
		if call.name == name {
			out = append(out, call)
		}
	}
	return out
}

// timingsNamed returns all recorded timing calls with the given name.
func (r *recordingReporter) timingsNamed(name string) []timingCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []timingCall
	for _, call := range r.timings {
		if call.name == name {
			out = append(out, call)
		}
	}
	return out
}

func assertTagMap(t *testing.T, name string, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s tags = %v, want %v", name, got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s tag %q = %q, want %q", name, key, got[key], value)
		}
	}
}

func assertCounterTags(t *testing.T, call counterCall, want map[string]string) {
	t.Helper()
	assertTagMap(t, call.name, call.tags, want)
}

func newMetricsTestConsumer(reporter *recordingReporter) *Consumer {
	return &Consumer{
		cfg:      Config{StreamName: "stream", ConsumerGroup: "group"},
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
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"handler":        "record",
	})

	durations := reporter.timingsNamed(metricHandlerDuration)
	if len(durations) != 1 {
		t.Fatalf("handler_duration calls = %d, want 1 (one per attempt)", len(durations))
	}
	assertTagMap(t, durations[0].name, durations[0].tags, map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"handler":        "record",
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
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"handler":        "batch",
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
			"stream":         "stream",
			"consumer_group": "group",
			"shard":          "shard-1",
			"handler":        "record",
		})
	}
	if processed := reporter.countersNamed(metricRecordsProcessed); len(processed) != 1 {
		t.Fatalf("records_processed calls = %d, want 1 (eventual success)", len(processed))
	}
	// handler_duration measures every attempt, failed ones included.
	if durations := reporter.timingsNamed(metricHandlerDuration); len(durations) != 3 {
		t.Fatalf("handler_duration calls = %d, want 3 (one per attempt)", len(durations))
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
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"policy":         "skip",
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
			"stream":         "stream",
			"consumer_group": "group",
			"shard":          "shard-1",
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
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
	if failures := reporter.countersNamed(metricCheckpointFailures); len(failures) != 0 {
		t.Fatalf("checkpoint_failures calls = %d, want 0", len(failures))
	}
	durations := reporter.timingsNamed(metricCheckpointSaveDuration)
	if len(durations) != 1 {
		t.Fatalf("checkpoint_save_duration calls = %d, want 1", len(durations))
	}
	assertTagMap(t, durations[0].name, durations[0].tags, map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
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
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
	if saved := reporter.countersNamed(metricCheckpointsSaved); len(saved) != 0 {
		t.Fatalf("checkpoints_saved calls = %d, want 0", len(saved))
	}
	if durations := reporter.timingsNamed(metricCheckpointSaveDuration); len(durations) != 0 {
		t.Fatalf("checkpoint_save_duration calls = %d, want 0 (save failed)", len(durations))
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
	completed := reporter.countersNamed(metricShardsCompleted)
	if len(completed) != 1 {
		t.Fatalf("shards_completed calls = %d, want 1", len(completed))
	}
	assertCounterTags(t, completed[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})

	failing := &recordingReporter{}
	c = newMetricsTestConsumer(failing)
	c.store = &fakeCheckpointSaveStore{saveErr: errors.New("save boom")}

	if err := c.saveShardCompletionCheckpoint(context.Background(), "shard-1", "sequence-1"); err == nil {
		t.Fatal("saveShardCompletionCheckpoint() error = nil, want save error")
	}
	if failures := failing.countersNamed(metricCheckpointFailures); len(failures) != 1 {
		t.Fatalf("checkpoint_failures calls = %d, want 1", len(failures))
	}
	if completed := failing.countersNamed(metricShardsCompleted); len(completed) != 0 {
		t.Fatalf("shards_completed calls = %d, want 0 (save failed)", len(completed))
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
				Records:            []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator:  aws.String("iterator-2"),
				MillisBehindLatest: aws.Int64(1500),
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

	if _, _, _, err := c.processShardRecordsPass(ctx, "shard-1", 0, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, context.Canceled)
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
			"stream":         "stream",
			"consumer_group": "group",
			"shard":          "shard-1",
		})
	}

	if durations := reporter.timingsNamed(metricGetRecordsDuration); len(durations) != 2 {
		t.Fatalf("get_records_duration calls = %d, want 2", len(durations))
	}

	// Only the first page carries MillisBehindLatest; the nil second page must
	// emit no gauge.
	behind := reporter.gaugesNamed(metricMillisBehindLatest)
	if len(behind) != 1 {
		t.Fatalf("millis_behind_latest calls = %d, want 1", len(behind))
	}
	if behind[0].value != 1500 {
		t.Fatalf("millis_behind_latest value = %v, want 1500", behind[0].value)
	}
	assertTagMap(t, behind[0].name, behind[0].tags, map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
}

func TestAcquireShardLeasesCountsAcquiredAndTiming(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	manager := &recordingAcquireManager{result: fakeShardLease{}, acquired: true}
	c := newMetricsTestConsumer(reporter)
	c.leaseManager = manager
	c.leaseOwner = "owner"
	c.tuning = tuningConfig{retryMaxAttempts: 1, heartbeatTTL: 30 * time.Millisecond}

	if _, err := c.acquireShardLeases(context.Background(), []string{"shard-1"}); err != nil {
		t.Fatalf("acquireShardLeases() error = %v, want nil", err)
	}

	acquired := reporter.countersNamed(metricLeaseAcquired)
	if len(acquired) != 1 {
		t.Fatalf("lease_acquired calls = %d, want 1", len(acquired))
	}
	if acquired[0].value != 1 {
		t.Fatalf("lease_acquired value = %d, want 1", acquired[0].value)
	}
	assertCounterTags(t, acquired[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})

	durations := reporter.timingsNamed(metricLeaseAcquireDuration)
	if len(durations) != 1 {
		t.Fatalf("lease_acquire_duration calls = %d, want 1", len(durations))
	}
	assertTagMap(t, durations[0].name, durations[0].tags, map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
}

func TestAcquireShardLeasesNotAcquiredCountsNoLeaseAcquired(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	manager := &recordingAcquireManager{}
	c := newMetricsTestConsumer(reporter)
	c.leaseManager = manager
	c.leaseOwner = "owner"
	c.tuning = tuningConfig{retryMaxAttempts: 1, heartbeatTTL: 30 * time.Millisecond}

	if _, err := c.acquireShardLeases(context.Background(), []string{"shard-1"}); err != nil {
		t.Fatalf("acquireShardLeases() error = %v, want nil", err)
	}

	if acquired := reporter.countersNamed(metricLeaseAcquired); len(acquired) != 0 {
		t.Fatalf("lease_acquired calls = %d, want 0 (lease held elsewhere)", len(acquired))
	}
	// The manager call itself succeeded, so its duration is still measured.
	if durations := reporter.timingsNamed(metricLeaseAcquireDuration); len(durations) != 1 {
		t.Fatalf("lease_acquire_duration calls = %d, want 1", len(durations))
	}
}

func TestReleaseShardLeaseWithTimeoutCountsRelease(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)

	if err := c.releaseShardLeaseWithTimeout("shard-1", &recordingReleaseLease{}); err != nil {
		t.Fatalf("releaseShardLeaseWithTimeout() error = %v, want nil", err)
	}

	released := reporter.countersNamed(metricLeaseReleased)
	if len(released) != 1 {
		t.Fatalf("lease_released calls = %d, want 1", len(released))
	}
	assertCounterTags(t, released[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
	if failures := reporter.countersNamed(metricLeaseReleaseFailures); len(failures) != 0 {
		t.Fatalf("lease_release_failures calls = %d, want 0", len(failures))
	}
}

func TestReleaseShardLeaseWithTimeoutCountsReleaseFailure(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)

	err := c.releaseShardLeaseWithTimeout("shard-1", &recordingReleaseLease{err: errors.New("release boom")})
	if err == nil {
		t.Fatal("releaseShardLeaseWithTimeout() error = nil, want release error")
	}

	failures := reporter.countersNamed(metricLeaseReleaseFailures)
	if len(failures) != 1 {
		t.Fatalf("lease_release_failures calls = %d, want 1", len(failures))
	}
	assertCounterTags(t, failures[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
	if released := reporter.countersNamed(metricLeaseReleased); len(released) != 0 {
		t.Fatalf("lease_released calls = %d, want 0", len(released))
	}
}

func TestRenewShardLeaseLoopCountsRenewals(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.tuning = tuningConfig{heartbeatInterval: time.Millisecond, heartbeatTTL: 30 * time.Millisecond}
	shardLease := &recordingRenewLease{ch: make(chan renewCall, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.renewShardLeaseLoop(ctx, "shard-1", shardLease, newLeaseRenewTracker()) }()

	select {
	case <-shardLease.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Renew call")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("renewShardLeaseLoop() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for renew loop to stop")
	}

	renewals := reporter.countersNamed(metricLeaseRenewals)
	if len(renewals) == 0 {
		t.Fatal("lease_renewals calls = 0, want at least 1")
	}
	assertCounterTags(t, renewals[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
	if failures := reporter.countersNamed(metricLeaseRenewalFailures); len(failures) != 0 {
		t.Fatalf("lease_renewal_failures calls = %d, want 0", len(failures))
	}
}

func TestRenewShardLeaseLoopCountsRenewalFailure(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.tuning = tuningConfig{heartbeatInterval: time.Millisecond, heartbeatTTL: 30 * time.Millisecond}
	shardLease := &recordingRenewLease{err: errors.New("renew boom")}

	if err := c.renewShardLeaseLoop(context.Background(), "shard-1", shardLease, newLeaseRenewTracker()); err == nil {
		t.Fatal("renewShardLeaseLoop() error = nil, want renew error")
	}

	// Transient failures are retried within the ttl budget and every failed
	// attempt counts — one counter per renew call, including the
	// budget-exhausting final attempt.
	failures := reporter.countersNamed(metricLeaseRenewalFailures)
	if len(failures) < 2 {
		t.Fatalf("lease_renewal_failures calls = %d, want >= 2 (per-attempt counting)", len(failures))
	}
	if len(failures) != shardLease.calls {
		t.Fatalf("lease_renewal_failures calls = %d, want %d (one per renew attempt)", len(failures), shardLease.calls)
	}
	assertCounterTags(t, failures[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})
	if renewals := reporter.countersNamed(metricLeaseRenewals); len(renewals) != 0 {
		t.Fatalf("lease_renewals calls = %d, want 0", len(renewals))
	}
}

func TestRebalanceShardsOnceEmitsMovesGaugesAndPassDuration(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-b": "donor-a",
			"shard-c": "donor-a",
			"shard-d": "donor-a",
		},
		workerOwners: []string{"self", "donor-a"},
		acquireResults: []acquireResult{
			{lease: &recordingReleaseLease{}, acquired: true},
		},
		claimResults: []claimResult{
			{lease: &recordingReleaseLease{}, claimed: true},
		},
	}
	reporter := &recordingReporter{}
	c := newTestRebalanceOnceConsumer(manager)
	c.cfg.ConsumerGroup = "group"
	c.reporter = reporter
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}
	workers.stopAll()
	workerWG.Wait()

	moves := reporter.countersNamed(metricRebalanceMoves)
	if len(moves) != 2 {
		t.Fatalf("rebalance_moves calls = %d, want 2 (acquire + claim)", len(moves))
	}
	assertCounterTags(t, moves[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-a",
		"kind":           "acquire",
	})
	assertCounterTags(t, moves[1], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-b",
		"kind":           "claim",
	})
	if skips := reporter.countersNamed(metricRebalanceSkips); len(skips) != 0 {
		t.Fatalf("rebalance_skips calls = %d, want 0", len(skips))
	}

	// 4 open shards over 2 active workers: fair share is exactly 2 per worker;
	// this consumer owned 0 shards when the snapshot was taken.
	wantGauges := map[string]float64{
		metricOwnedShards:   0,
		metricActiveWorkers: 2,
		metricFairShareLow:  2,
		metricFairShareHigh: 2,
	}
	for name, want := range wantGauges {
		gauges := reporter.gaugesNamed(name)
		if len(gauges) != 1 {
			t.Fatalf("%s calls = %d, want 1", name, len(gauges))
		}
		if gauges[0].value != want {
			t.Fatalf("%s value = %v, want %v", name, gauges[0].value, want)
		}
		assertTagMap(t, name, gauges[0].tags, map[string]string{"stream": "stream", "consumer_group": "group"})
	}

	durations := reporter.timingsNamed(metricRebalancePassDuration)
	if len(durations) != 1 {
		t.Fatalf("rebalance_pass_duration calls = %d, want 1", len(durations))
	}
	assertTagMap(t, durations[0].name, durations[0].tags, map[string]string{"stream": "stream", "consumer_group": "group"})
}

func TestRebalanceShardsOnceCountsShedMoves(t *testing.T) {
	t.Parallel()

	manager := &recordingRebalanceOnceManager{
		leaseOwners: map[string]string{
			"shard-a": "self",
			"shard-b": "self",
			"shard-c": "self",
			"shard-d": "worker-a",
		},
		workerOwners: []string{"self", "worker-a"},
	}
	reporter := &recordingReporter{}
	c := newTestRebalanceOnceConsumer(manager)
	c.cfg.ConsumerGroup = "group"
	c.reporter = reporter
	workers := newShardWorkerSet()
	workers.add("shard-a", func() {})
	workers.add("shard-b", func() {})
	workers.add("shard-c", func() {})
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.rebalanceShardsOnce(
		ctx,
		shardMapForReadiness(
			testShard("shard-a", "", ""),
			testShard("shard-b", "", ""),
			testShard("shard-c", "", ""),
			testShard("shard-d", "", ""),
		),
		newShardCompletionState(),
		map[string]time.Time{},
		workers,
		&workerWG,
		workerErrCh,
		cancel,
		time.Now(),
	)
	if err != nil {
		t.Fatalf("rebalanceShardsOnce() error = %v, want nil", err)
	}

	moves := reporter.countersNamed(metricRebalanceMoves)
	if len(moves) != 1 {
		t.Fatalf("rebalance_moves calls = %d, want 1 (shed)", len(moves))
	}
	assertCounterTags(t, moves[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-a",
		"kind":           "shed",
	})
}

func TestExecuteRebalanceActionsCountSkips(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.leaseManager = &recordingAcquireManager{}
	c.leaseOwner = "owner"
	c.tuning = tuningConfig{retryMaxAttempts: 1, heartbeatTTL: 30 * time.Millisecond}
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	started, err := c.executeRebalanceAcquireAction(context.Background(), "shard-1", workers, &workerWG, workerErrCh, nil)
	if err != nil || started {
		t.Fatalf("executeRebalanceAcquireAction() = %v, %v, want false, nil", started, err)
	}

	claimConsumer := newMetricsTestConsumer(reporter)
	claimConsumer.leaseManager = &recordingClaimManager{}
	claimConsumer.leaseOwner = "owner"
	claimConsumer.tuning = tuningConfig{retryMaxAttempts: 1, heartbeatTTL: 30 * time.Millisecond}

	started, err = claimConsumer.executeRebalanceClaimAction(context.Background(), "shard-2", "donor-a", workers, &workerWG, workerErrCh, nil)
	if err != nil || started {
		t.Fatalf("executeRebalanceClaimAction() = %v, %v, want false, nil", started, err)
	}

	skips := reporter.countersNamed(metricRebalanceSkips)
	if len(skips) != 2 {
		t.Fatalf("rebalance_skips calls = %d, want 2", len(skips))
	}
	assertCounterTags(t, skips[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"kind":           "acquire",
	})
	assertCounterTags(t, skips[1], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-2",
		"kind":           "claim",
	})
	if moves := reporter.countersNamed(metricRebalanceMoves); len(moves) != 0 {
		t.Fatalf("rebalance_moves calls = %d, want 0", len(moves))
	}
}

func TestShardWorkerCountsStartAndCleanStop(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newTestRegisteredShardWorkerConsumer(nil)
	c.cfg.ConsumerGroup = "group"
	c.reporter = reporter
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	c.startRegisteredShardWorker(context.Background(), "shard-1", fakeShardLease{}, workers, &workerWG, workerErrCh, nil)
	workers.stopAll()
	workerWG.Wait()

	starts := reporter.countersNamed(metricWorkerStarts)
	if len(starts) != 1 {
		t.Fatalf("worker_starts calls = %d, want 1", len(starts))
	}
	assertCounterTags(t, starts[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
	})

	stops := reporter.countersNamed(metricWorkerStops)
	if len(stops) != 1 {
		t.Fatalf("worker_stops calls = %d, want 1", len(stops))
	}
	assertCounterTags(t, stops[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"outcome":        "clean",
	})
}

func TestShardWorkerCountsErrorStop(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newTestRegisteredShardWorkerConsumer(errors.New("process boom"))
	c.cfg.ConsumerGroup = "group"
	c.reporter = reporter
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	workerErrCh := make(chan error, 1)

	c.startRegisteredShardWorker(context.Background(), "shard-1", fakeShardLease{}, workers, &workerWG, workerErrCh, nil)
	workerWG.Wait()

	stops := reporter.countersNamed(metricWorkerStops)
	if len(stops) != 1 {
		t.Fatalf("worker_stops calls = %d, want 1", len(stops))
	}
	assertCounterTags(t, stops[0], map[string]string{
		"stream":         "stream",
		"consumer_group": "group",
		"shard":          "shard-1",
		"outcome":        "error",
	})
}

func TestDrainShardWorkersEmitsDrainDuration(t *testing.T) {
	t.Parallel()

	reporter := &recordingReporter{}
	c := newMetricsTestConsumer(reporter)
	c.drainTimeout = time.Second
	var workerWG sync.WaitGroup

	if err := c.drainShardWorkers(newShardWorkerSet(), &workerWG, nil); err != nil {
		t.Fatalf("drainShardWorkers() error = %v, want nil", err)
	}

	durations := reporter.timingsNamed(metricDrainDuration)
	if len(durations) != 1 {
		t.Fatalf("drain_duration calls = %d, want 1", len(durations))
	}
	assertTagMap(t, durations[0].name, durations[0].tags, map[string]string{"stream": "stream", "consumer_group": "group"})
}
