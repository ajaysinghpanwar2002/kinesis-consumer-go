package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
)

type heartbeatCall struct {
	streamName string
	owner      string
	ttl        time.Duration
}

type recordingHeartbeatManager struct {
	fakeLeaseManager

	mu    sync.Mutex
	calls []heartbeatCall
	err   error
	ch    chan heartbeatCall
}

func newRecordingHeartbeatManager() *recordingHeartbeatManager {
	return &recordingHeartbeatManager{
		ch: make(chan heartbeatCall, 10),
	}
}

func (m *recordingHeartbeatManager) Heartbeat(_ context.Context, streamName, owner string, ttl time.Duration) error {
	call := heartbeatCall{
		streamName: streamName,
		owner:      owner,
		ttl:        ttl,
	}

	m.mu.Lock()
	m.calls = append(m.calls, call)
	m.mu.Unlock()

	select {
	case m.ch <- call:
	default:
	}

	return m.err
}

func TestStreamKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "uses stream name before arn",
			cfg: Config{
				StreamName: "stream-name",
				StreamARN:  "stream-arn",
			},
			want: "stream-name",
		},
		{
			name: "falls back to arn",
			cfg: Config{
				StreamARN: "stream-arn",
			},
			want: "stream-arn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := Consumer{cfg: tt.cfg}
			if got := c.streamKey(); got != tt.want {
				t.Fatalf("streamKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWorkerHeartbeatLoopSendsImmediateHeartbeat(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestHeartbeatConsumer(manager)

	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c)

	call := waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)

	assertHeartbeatCall(t, call, "stream", "owner", 30*time.Millisecond)
}

func TestWorkerHeartbeatLoopSendsRepeatedHeartbeats(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestHeartbeatConsumer(manager)

	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c)

	_ = waitHeartbeat(t, manager)
	call := waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)

	assertHeartbeatCall(t, call, "stream", "owner", 30*time.Millisecond)
}

func TestWorkerHeartbeatLoopStopsOnCancel(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestHeartbeatConsumer(manager)
	c.tuning.heartbeatInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c)

	_ = waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)
}

func TestWorkerHeartbeatLoopWarnsAndCountsFailures(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("heartbeat boom")
	manager := newRecordingHeartbeatManager()
	manager.err = errBoom
	logHandler := newCapturingHandler()
	reporter := &recordingReporter{}
	c := newTestHeartbeatConsumer(manager)
	c.logger = slog.New(logHandler)
	c.reporter = reporter
	c.tuning.heartbeatInterval = time.Hour // only the immediate send fires

	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c)

	_ = waitHeartbeat(t, manager)
	// Wait for the emission before cancelling: a cancel that lands between the
	// failed Heartbeat return and its ctx.Err() check is treated as shutdown
	// and deliberately emits nothing.
	deadline := time.Now().Add(time.Second)
	for len(reporter.countersNamed(metricHeartbeatFailures)) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	waitDone(t, done)

	failures := reporter.countersNamed(metricHeartbeatFailures)
	if len(failures) != 1 {
		t.Fatalf("heartbeat_failures calls = %d, want 1", len(failures))
	}
	assertCounterTags(t, failures[0], map[string]string{"stream": "stream"})

	var warns []capturedRecord
	for _, rec := range logHandler.snapshot() {
		if rec.message == "worker heartbeat failed" {
			warns = append(warns, rec)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("heartbeat warn logs = %d, want 1", len(warns))
	}
	if warns[0].level != slog.LevelWarn {
		t.Fatalf("heartbeat log level = %v, want %v", warns[0].level, slog.LevelWarn)
	}
	if warns[0].attrs["owner"] != "owner" {
		t.Fatalf("heartbeat log owner = %q, want owner", warns[0].attrs["owner"])
	}
	if warns[0].attrs["error"] == "" {
		t.Fatal("heartbeat log error attribute missing")
	}
}

func TestWorkerHeartbeatLoopSuccessEmitsNoFailures(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	reporter := &recordingReporter{}
	c := newTestHeartbeatConsumer(manager)
	c.reporter = reporter

	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c)

	_ = waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)

	if failures := reporter.countersNamed(metricHeartbeatFailures); len(failures) != 0 {
		t.Fatalf("heartbeat_failures calls = %d, want 0", len(failures))
	}
}

func newTestHeartbeatConsumer(manager *recordingHeartbeatManager) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatInterval = 10 * time.Millisecond
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		cfg: Config{
			StreamName: "stream",
		},
		leaseManager: manager,
		leaseOwner:   "owner",
		tuning:       tuning,
		logger:       slog.New(slog.DiscardHandler),
		reporter:     metrics.Nop{},
	}
}

func runHeartbeatLoop(ctx context.Context, c *Consumer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.workerHeartbeatLoop(ctx)
	}()
	return done
}

func waitHeartbeat(t *testing.T, manager *recordingHeartbeatManager) heartbeatCall {
	t.Helper()

	select {
	case call := <-manager.ch:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat")
		return heartbeatCall{}
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat loop to stop")
	}
}

func assertHeartbeatCall(t *testing.T, call heartbeatCall, streamName, owner string, ttl time.Duration) {
	t.Helper()

	if call.streamName != streamName {
		t.Fatalf("heartbeat streamName = %q, want %q", call.streamName, streamName)
	}
	if call.owner != owner {
		t.Fatalf("heartbeat owner = %q, want %q", call.owner, owner)
	}
	if call.ttl != ttl {
		t.Fatalf("heartbeat ttl = %v, want %v", call.ttl, ttl)
	}
}
