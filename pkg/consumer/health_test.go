package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

func TestHealthZeroValueConsumer(t *testing.T) {
	t.Parallel()

	c := &Consumer{}
	health := c.Health()
	if health.ShardSync.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0", health.ShardSync.ConsecutiveFailures)
	}
	if !health.ShardSync.LastSuccess.IsZero() {
		t.Fatalf("LastSuccess = %v, want zero", health.ShardSync.LastSuccess)
	}
	if health.ShardSync.LastError != nil {
		t.Fatalf("LastError = %v, want nil", health.ShardSync.LastError)
	}
	if health.Heartbeat.ConsecutiveFailures != 0 {
		t.Fatalf("Heartbeat.ConsecutiveFailures = %d, want 0", health.Heartbeat.ConsecutiveFailures)
	}
	if !health.Heartbeat.LastSuccess.IsZero() {
		t.Fatalf("Heartbeat.LastSuccess = %v, want zero", health.Heartbeat.LastSuccess)
	}
	if health.Heartbeat.LastError != nil {
		t.Fatalf("Heartbeat.LastError = %v, want nil", health.Heartbeat.LastError)
	}
}

func TestHealthShardSyncTransitions(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := &Consumer{}
	anchor := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	// anchor fills a zero LastSuccess without claiming a success...
	c.syncHealth.anchor(anchor)
	if got := c.Health().ShardSync.LastSuccess; !got.Equal(anchor) {
		t.Fatalf("LastSuccess after anchor = %v, want %v", got, anchor)
	}
	// ...and never overwrites a recorded one.
	c.syncHealth.anchor(anchor.Add(time.Hour))
	if got := c.Health().ShardSync.LastSuccess; !got.Equal(anchor) {
		t.Fatalf("LastSuccess after second anchor = %v, want unchanged %v", got, anchor)
	}

	failures, lastSuccess := c.syncHealth.recordFailure(errBoom)
	if failures != 1 || !lastSuccess.Equal(anchor) {
		t.Fatalf("recordFailure = (%d, %v), want (1, %v)", failures, lastSuccess, anchor)
	}
	failures, _ = c.syncHealth.recordFailure(errBoom)
	if failures != 2 {
		t.Fatalf("second recordFailure count = %d, want 2", failures)
	}
	health := c.Health().ShardSync
	if health.ConsecutiveFailures != 2 || !errors.Is(health.LastError, errBoom) {
		t.Fatalf("failing health = %+v, want 2 consecutive failures wrapping %v", health, errBoom)
	}

	recovered := anchor.Add(time.Minute)
	c.syncHealth.recordSuccess(recovered)
	health = c.Health().ShardSync
	if health.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures after success = %d, want 0", health.ConsecutiveFailures)
	}
	if !health.LastSuccess.Equal(recovered) {
		t.Fatalf("LastSuccess after success = %v, want %v", health.LastSuccess, recovered)
	}
	if health.LastError != nil {
		t.Fatalf("LastError after success = %v, want nil", health.LastError)
	}
}

func TestHealthMapsSignalsToTheirFields(t *testing.T) {
	t.Parallel()

	// The two signals share one state type; the snapshot must not cross
	// wires between them.
	errBeat := errors.New("beat")
	c := &Consumer{}
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	c.heartbeatHealth.recordFailure(errBeat)
	c.heartbeatHealth.recordFailure(errBeat)
	c.syncHealth.recordSuccess(now)

	health := c.Health()
	if health.Heartbeat.ConsecutiveFailures != 2 || !errors.Is(health.Heartbeat.LastError, errBeat) {
		t.Fatalf("Heartbeat = %+v, want 2 consecutive failures wrapping %v", health.Heartbeat, errBeat)
	}
	if health.ShardSync.ConsecutiveFailures != 0 || health.ShardSync.LastError != nil {
		t.Fatalf("ShardSync = %+v, want healthy", health.ShardSync)
	}
	if !health.ShardSync.LastSuccess.Equal(now) {
		t.Fatalf("ShardSync.LastSuccess = %v, want %v", health.ShardSync.LastSuccess, now)
	}
}

func TestHealthIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	c := &Consumer{}
	errBoom := errors.New("boom")
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.syncHealth.recordFailure(errBoom)
				c.syncHealth.recordSuccess(now)
				c.heartbeatHealth.recordFailure(errBoom)
				c.heartbeatHealth.recordSuccess(now)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = c.Health()
			}
		}()
	}
	wg.Wait()
}

func TestShardSyncRetryDelay(t *testing.T) {
	t.Parallel()

	const wide = time.Hour

	tests := []struct {
		name     string
		failures int
		interval time.Duration
		want     time.Duration
	}{
		{name: "first failure", failures: 1, interval: wide, want: time.Second},
		{name: "second failure doubles", failures: 2, interval: wide, want: 2 * time.Second},
		{name: "fifth failure", failures: 5, interval: wide, want: 16 * time.Second},
		{name: "capped at max", failures: 6, interval: wide, want: 30 * time.Second},
		{name: "far past cap", failures: 60, interval: wide, want: 30 * time.Second},
		{name: "capped at sync interval", failures: 1, interval: 200 * time.Millisecond, want: 200 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shardSyncRetryDelay(tt.failures, tt.interval, nil); got != tt.want {
				t.Fatalf("shardSyncRetryDelay(%d, %v, nil) = %v, want %v", tt.failures, tt.interval, got, tt.want)
			}
		})
	}
}

func TestShardSyncRetryDelayJitterBounds(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1))
	sawJitter := false
	for i := 0; i < 50; i++ {
		got := shardSyncRetryDelay(1, time.Hour, rng)
		if got < time.Second || got > time.Second+500*time.Millisecond {
			t.Fatalf("shardSyncRetryDelay() = %v, want in [1s, 1.5s]", got)
		}
		if got > time.Second {
			sawJitter = true
		}
	}
	if !sawJitter {
		t.Fatal("shardSyncRetryDelay() never added jitter")
	}
	// Jitter must respect the sync-interval ceiling too.
	for i := 0; i < 50; i++ {
		if got := shardSyncRetryDelay(1, 1200*time.Millisecond, rng); got > 1200*time.Millisecond {
			t.Fatalf("shardSyncRetryDelay() = %v, want <= interval 1.2s", got)
		}
	}
}

func TestFatalShardSyncError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "access denied", err: &types.AccessDeniedException{}, want: true},
		{name: "resource not found", err: &types.ResourceNotFoundException{}, want: true},
		{name: "invalid argument", err: &types.InvalidArgumentException{}, want: true},
		{name: "validation", err: &types.ValidationException{}, want: true},
		{name: "generic client fault", err: &smithy.GenericAPIError{Code: "MissingAuthenticationToken", Fault: smithy.FaultClient}, want: true},
		{name: "client-fault throttling", err: &smithy.GenericAPIError{Code: "ThrottlingException", Fault: smithy.FaultClient}, want: false},
		{name: "client-fault expired token", err: &smithy.GenericAPIError{Code: "ExpiredNextTokenException", Fault: smithy.FaultClient}, want: false},
		{name: "throughput exceeded", err: &types.ProvisionedThroughputExceededException{}, want: false},
		{name: "limit exceeded", err: &types.LimitExceededException{}, want: false},
		{name: "server fault", err: &smithy.GenericAPIError{Code: "InternalFailure", Fault: smithy.FaultServer}, want: false},
		{name: "plain backend error", err: errors.New("valkey: connection refused"), want: false},
		{name: "wrapped access denied", err: fmt.Errorf("list shards: %w", &types.AccessDeniedException{}), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := fatalShardSyncError(tt.err); got != tt.want {
				t.Fatalf("fatalShardSyncError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestHealthProcessingZeroUntilFirstActivity(t *testing.T) {
	t.Parallel()

	c := &Consumer{}
	h := c.Health()
	if !h.Processing.LastReadSuccess.IsZero() {
		t.Fatalf("LastReadSuccess = %v, want zero before any read", h.Processing.LastReadSuccess)
	}
	if !h.Processing.LastRecordProcessed.IsZero() {
		t.Fatalf("LastRecordProcessed = %v, want zero before any processed page", h.Processing.LastRecordProcessed)
	}
}

func TestHealthProcessingReportsRecordedTimestamps(t *testing.T) {
	t.Parallel()

	c := &Consumer{}
	readAt := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	processedAt := readAt.Add(time.Second)
	c.processingHealth.recordRead(readAt)
	c.processingHealth.recordProcessed(processedAt)

	h := c.Health()
	if !h.Processing.LastReadSuccess.Equal(readAt) {
		t.Fatalf("LastReadSuccess = %v, want %v", h.Processing.LastReadSuccess, readAt)
	}
	if !h.Processing.LastRecordProcessed.Equal(processedAt) {
		t.Fatalf("LastRecordProcessed = %v, want %v", h.Processing.LastRecordProcessed, processedAt)
	}
}

// TestProcessShardRecordsPassAdvancesReadHealthOnEmptyTipPage pins the
// LastReadSuccess hook and its semantics: an empty caught-up page counts as a
// successful read (the delivery loop is turning) without counting as
// processed records.
func TestProcessShardRecordsPassAdvancesReadHealthOnEmptyTipPage(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{NextShardIterator: aws.String("tip-iterator")}, // empty: caught up
		},
	}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    &fakeCheckpointSaveStore{},
		tuning:   tuningConfig{checkpointEvery: 10},
		handler:  func(context.Context, Record) error { return nil },
	}

	if _, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator"); err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	h := c.Health()
	if h.Processing.LastReadSuccess.IsZero() {
		t.Fatal("LastReadSuccess is zero, want advanced by the empty tip page read")
	}
	if !h.Processing.LastRecordProcessed.IsZero() {
		t.Fatalf("LastRecordProcessed = %v, want zero (no records were processed)", h.Processing.LastRecordProcessed)
	}
}

// TestProcessRecordsPageAdvancesProcessedHealth pins the LastRecordProcessed
// hook: a page that finishes processing advances the timestamp.
func TestProcessRecordsPageAdvancesProcessedHealth(t *testing.T) {
	t.Parallel()

	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    &fakeCheckpointSaveStore{},
		tuning:   tuningConfig{checkpointEvery: 10},
		handler:  func(context.Context, Record) error { return nil },
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	if _, _, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", out, 0); err != nil {
		t.Fatalf("processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if c.Health().Processing.LastRecordProcessed.IsZero() {
		t.Fatal("LastRecordProcessed is zero, want advanced by the processed page")
	}
}

// TestProcessingProgressKeepsGreatestTimestamp pins the out-of-order-writer
// guarantee: a descheduled worker resuming with an older timestamp must not
// regress a newer one — the signals report the LAST progress, so a regression
// would falsely age the consumer despite recent activity.
func TestProcessingProgressKeepsGreatestTimestamp(t *testing.T) {
	t.Parallel()

	var state processingProgressState
	newer := time.Date(2026, 7, 18, 10, 0, 1, 0, time.UTC)
	older := newer.Add(-time.Second)

	state.recordRead(newer)
	state.recordRead(older) // late writer with a stale capture
	state.recordProcessed(newer)
	state.recordProcessed(older)

	lastRead, lastProcessed := state.snapshot()
	if !lastRead.Equal(newer) {
		t.Fatalf("lastRead = %v, want %v (older write must not regress it)", lastRead, newer)
	}
	if !lastProcessed.Equal(newer) {
		t.Fatalf("lastProcessed = %v, want %v (older write must not regress it)", lastProcessed, newer)
	}
}
