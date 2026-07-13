package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// capturingHandler is a minimal slog.Handler that records every emitted record.
// It is mutex-guarded because Start spawns heartbeat and orchestration
// goroutines that share the consumer's logger.
type capturingHandler struct {
	mu      *sync.Mutex
	records *[]capturedRecord
	attrs   []slog.Attr
}

type capturedRecord struct {
	level   slog.Level
	message string
	attrs   map[string]string
}

func newCapturingHandler() *capturingHandler {
	return &capturingHandler{
		mu:      &sync.Mutex{},
		records: &[]capturedRecord{},
	}
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, rec slog.Record) error {
	attrs := make(map[string]string)
	for _, a := range h.attrs {
		attrs[a.Key] = a.Value.String()
	}
	rec.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, capturedRecord{
		level:   rec.Level,
		message: rec.Message,
		attrs:   attrs,
	})
	return nil
}

func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)
	return &capturingHandler{mu: h.mu, records: h.records, attrs: merged}
}

func (h *capturingHandler) WithGroup(string) slog.Handler { return h }

func (h *capturingHandler) snapshot() []capturedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]capturedRecord, len(*h.records))
	copy(out, *h.records)
	return out
}

// findRecord returns the first captured record whose message matches, or false.
func findRecord(records []capturedRecord, message string) (capturedRecord, bool) {
	for _, rec := range records {
		if rec.message == message {
			return rec, true
		}
	}
	return capturedRecord{}, false
}

func TestStartLogsCleanLifecycle(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	c := newTestStartConsumer(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		newRecordingHeartbeatManager(),
	)
	c.logger = slog.New(handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	assertStartStillRunning(t, done)
	cancel()
	waitStartDone(t, done, nil)

	records := handler.snapshot()

	starting, ok := findRecord(records, "consumer starting")
	if !ok {
		t.Fatalf("no 'consumer starting' record, got %+v", records)
	}
	if starting.level != slog.LevelInfo {
		t.Fatalf("starting level = %v, want Info", starting.level)
	}
	if starting.attrs["stream"] != "stream" {
		t.Fatalf("starting stream attr = %q, want %q", starting.attrs["stream"], "stream")
	}

	stopped, ok := findRecord(records, "consumer stopped")
	if !ok {
		t.Fatalf("no 'consumer stopped' record, got %+v", records)
	}
	if stopped.level != slog.LevelInfo {
		t.Fatalf("stopped level = %v, want Info (clean stop)", stopped.level)
	}
	if stopped.attrs["stream"] != "stream" {
		t.Fatalf("stopped stream attr = %q, want %q", stopped.attrs["stream"], "stream")
	}
	if _, hasErr := stopped.attrs["error"]; hasErr {
		t.Fatalf("clean stop record carries an error attr: %+v", stopped.attrs)
	}
}

func TestStartLogsFatalError(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	// No shards discovered is a fatal path: Start returns an error, which must
	// be logged at Error level by the terminal handler.
	c := newTestStartConsumer(
		&fakeKinesisClient{outs: []*kinesis.ListShardsOutput{{}}},
		newRecordingHeartbeatManager(),
	)
	c.logger = slog.New(handler)

	err := c.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want no-shards error")
	}

	records := handler.snapshot()

	if _, ok := findRecord(records, "consumer starting"); !ok {
		t.Fatalf("no 'consumer starting' record, got %+v", records)
	}

	stopped, ok := findRecord(records, "consumer stopped")
	if !ok {
		t.Fatalf("no 'consumer stopped' record, got %+v", records)
	}
	if stopped.level != slog.LevelError {
		t.Fatalf("stopped level = %v, want Error (fatal path)", stopped.level)
	}
	if stopped.attrs["error"] != err.Error() {
		t.Fatalf("stopped error attr = %q, want %q", stopped.attrs["error"], err.Error())
	}
	if stopped.attrs["stream"] != "stream" {
		t.Fatalf("stopped stream attr = %q, want %q", stopped.attrs["stream"], "stream")
	}
}

func TestStartLogsWorkerAndLeaseLifecycle(t *testing.T) {
	t.Parallel()

	handler := newCapturingHandler()
	shardLease := &recordingReleaseLease{}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.logger = slog.New(handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := runStart(ctx, c)

	_ = waitAcquireCall(t, manager)
	cancel()
	waitStartDone(t, done, nil)

	records := handler.snapshot()

	acquired, ok := findRecord(records, "shard lease acquired")
	if !ok {
		t.Fatalf("no 'shard lease acquired' record, got %+v", records)
	}
	if acquired.level != slog.LevelDebug {
		t.Fatalf("acquired level = %v, want Debug", acquired.level)
	}
	if acquired.attrs["shard"] != "shard-1" {
		t.Fatalf("acquired shard attr = %q, want %q", acquired.attrs["shard"], "shard-1")
	}
	if acquired.attrs["owner"] != "owner" {
		t.Fatalf("acquired owner attr = %q, want %q", acquired.attrs["owner"], "owner")
	}

	started, ok := findRecord(records, "shard worker started")
	if !ok {
		t.Fatalf("no 'shard worker started' record, got %+v", records)
	}
	if started.level != slog.LevelInfo {
		t.Fatalf("started level = %v, want Info", started.level)
	}
	if started.attrs["shard"] != "shard-1" {
		t.Fatalf("started shard attr = %q, want %q", started.attrs["shard"], "shard-1")
	}

	stopped, ok := findRecord(records, "shard worker stopped")
	if !ok {
		t.Fatalf("no 'shard worker stopped' record, got %+v", records)
	}
	if stopped.level != slog.LevelInfo {
		t.Fatalf("stopped level = %v, want Info (clean stop)", stopped.level)
	}
	if _, hasErr := stopped.attrs["error"]; hasErr {
		t.Fatalf("clean worker stop carries an error attr: %+v", stopped.attrs)
	}

	released, ok := findRecord(records, "shard lease released")
	if !ok {
		t.Fatalf("no 'shard lease released' record, got %+v", records)
	}
	if released.level != slog.LevelDebug {
		t.Fatalf("released level = %v, want Debug", released.level)
	}
	if released.attrs["shard"] != "shard-1" {
		t.Fatalf("released shard attr = %q, want %q", released.attrs["shard"], "shard-1")
	}
}

// TestStartLogsWorkerFailureAndSwallowedReleaseError covers the swallow path:
// when a worker fails, the release error at shard_worker.go is discarded (the
// worker's own error wins), so the release-failed Warn is the ONLY record of
// the release failure, while worker-stopped reports the original worker error.
func TestStartLogsWorkerFailureAndSwallowedReleaseError(t *testing.T) {
	t.Parallel()

	errProcess := errors.New("process boom")
	errRelease := errors.New("release boom")
	handler := newCapturingHandler()
	shardLease := &recordingReleaseLease{err: errRelease}
	manager := &recordingAcquireManager{
		result:   shardLease,
		acquired: true,
		callCh:   make(chan acquireCall, 1),
	}
	c := newTestStartConsumerWithLeaseManager(
		&fakeKinesisClient{
			outs: []*kinesis.ListShardsOutput{
				{Shards: []types.Shard{{ShardId: aws.String("shard-1")}}},
			},
		},
		manager,
	)
	c.logger = slog.New(handler)
	c.processShardRecordsLoopFn = func(context.Context, string) (string, int, error) {
		return "", 0, errProcess
	}

	err := c.Start(context.Background())
	if !errors.Is(err, errProcess) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errProcess)
	}

	records := handler.snapshot()

	stopped, ok := findRecord(records, "shard worker stopped")
	if !ok {
		t.Fatalf("no 'shard worker stopped' record, got %+v", records)
	}
	if stopped.level != slog.LevelWarn {
		t.Fatalf("stopped level = %v, want Warn (worker failed)", stopped.level)
	}
	if !strings.Contains(stopped.attrs["error"], errProcess.Error()) {
		t.Fatalf("stopped error attr = %q, want to contain %q", stopped.attrs["error"], errProcess.Error())
	}

	releaseFailed, ok := findRecord(records, "shard lease release failed")
	if !ok {
		t.Fatalf("no 'shard lease release failed' record, got %+v", records)
	}
	if releaseFailed.level != slog.LevelWarn {
		t.Fatalf("release failed level = %v, want Warn", releaseFailed.level)
	}
	if !strings.Contains(releaseFailed.attrs["error"], errRelease.Error()) {
		t.Fatalf("release failed error attr = %q, want to contain %q", releaseFailed.attrs["error"], errRelease.Error())
	}
	if releaseFailed.attrs["shard"] != "shard-1" {
		t.Fatalf("release failed shard attr = %q, want %q", releaseFailed.attrs["shard"], "shard-1")
	}
}
