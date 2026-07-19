package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
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
	err := m.err
	m.mu.Unlock()

	select {
	case m.ch <- call:
	default:
	}

	return err
}

func (m *recordingHeartbeatManager) setErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func TestConsumerIdentityHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		consumer         *Consumer
		wantStream       string
		wantCoordination string
	}{
		{
			name: "uses resolved constructor identity",
			consumer: &Consumer{
				cfg:                  Config{StreamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/orders", ConsumerGroup: "billing"},
				streamName:           "orders",
				coordinationIdentity: "billing:orders",
			},
			wantStream:       "orders",
			wantCoordination: "billing:orders",
		},
		{
			name: "direct fixture fallback",
			consumer: &Consumer{
				cfg: Config{StreamName: "orders", ConsumerGroup: "billing"},
			},
			wantStream:       "orders",
			wantCoordination: "billing:orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.consumer.canonicalStreamName(); got != tt.wantStream {
				t.Fatalf("canonicalStreamName() = %q, want %q", got, tt.wantStream)
			}
			if got := tt.consumer.coordinationKey(); got != tt.wantCoordination {
				t.Fatalf("coordinationKey() = %q, want %q", got, tt.wantCoordination)
			}
		})
	}
}

func TestWorkerHeartbeatLoopSendsImmediateHeartbeat(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestHeartbeatConsumer(manager)

	recorder := newStopRunRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, recorder.stopRun)

	call := waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)

	assertHeartbeatCall(t, call, "group:stream", "owner", 30*time.Millisecond)
	assertNoStopRun(t, recorder)
}

func TestWorkerHeartbeatLoopSendsRepeatedHeartbeats(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestHeartbeatConsumer(manager)

	recorder := newStopRunRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, recorder.stopRun)

	_ = waitHeartbeat(t, manager)
	call := waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)

	assertHeartbeatCall(t, call, "group:stream", "owner", 30*time.Millisecond)
	assertNoStopRun(t, recorder)
}

func TestWorkerHeartbeatLoopStopsOnCancel(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	c := newTestHeartbeatConsumer(manager)
	c.tuning.heartbeatInterval = time.Hour
	c.tuning.heartbeatTTL = 2 * time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, newStopRunRecorder().stopRun)

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
	// Keep the validity deadline far away: this test is about the failure
	// diagnostics, not the staleness stop.
	c.tuning.heartbeatTTL = 3 * time.Hour

	recorder := newStopRunRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, recorder.stopRun)

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
	assertCounterTags(t, failures[0], map[string]string{"stream": "stream", "consumer_group": "group"})

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
	assertNoStopRun(t, recorder)

	health := c.Health().Heartbeat
	if health.ConsecutiveFailures != 1 {
		t.Fatalf("Health().Heartbeat.ConsecutiveFailures = %d, want 1", health.ConsecutiveFailures)
	}
	if !errors.Is(health.LastError, errBoom) {
		t.Fatalf("Health().Heartbeat.LastError = %v, want wraps %v", health.LastError, errBoom)
	}
	if health.LastSuccess.IsZero() {
		t.Fatal("Health().Heartbeat.LastSuccess is zero, want anchored at loop start")
	}
}

func TestWorkerHeartbeatLoopSuccessEmitsNoFailures(t *testing.T) {
	t.Parallel()

	manager := newRecordingHeartbeatManager()
	reporter := &recordingReporter{}
	c := newTestHeartbeatConsumer(manager)
	c.reporter = reporter

	recorder := newStopRunRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, recorder.stopRun)

	_ = waitHeartbeat(t, manager)
	cancel()
	waitDone(t, done)

	if failures := reporter.countersNamed(metricHeartbeatFailures); len(failures) != 0 {
		t.Fatalf("heartbeat_failures calls = %d, want 0", len(failures))
	}
	assertNoStopRun(t, recorder)
}

func TestWorkerHeartbeatLoopStopsRunWhenValidityLapses(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("heartbeat boom")
	manager := newRecordingHeartbeatManager()
	manager.err = errBoom
	c := newTestHeartbeatConsumer(manager)
	c.tuning.heartbeatInterval = 5 * time.Millisecond
	c.tuning.heartbeatTTL = 25 * time.Millisecond

	recorder := newStopRunRecorder()
	start := time.Now()
	done := runHeartbeatLoop(context.Background(), c, recorder.stopRun)

	err := waitStopRun(t, recorder)
	elapsed := time.Since(start)
	// The loop exits on its own after signaling; no context cancel needed.
	waitDone(t, done)

	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("stopRun error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("stopRun error = %v, want preserves cause %v", err, errBoom)
	}
	// Failures must persist for ttl - interval before validity lapses; an
	// earlier stop would turn a transient blip into a run failure.
	if margin := c.tuning.heartbeatTTL - c.tuning.heartbeatInterval; elapsed < margin {
		t.Fatalf("stopRun after %v, want no earlier than %v", elapsed, margin)
	}

	health := c.Health().Heartbeat
	if health.ConsecutiveFailures < 1 {
		t.Fatalf("Health().Heartbeat.ConsecutiveFailures = %d, want >= 1", health.ConsecutiveFailures)
	}
	if !errors.Is(health.LastError, errBoom) {
		t.Fatalf("Health().Heartbeat.LastError = %v, want wraps %v", health.LastError, errBoom)
	}
}

func TestWorkerHeartbeatLoopRecoveryResetsValidityDeadline(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("heartbeat boom")
	manager := newRecordingHeartbeatManager()
	manager.err = errBoom
	c := newTestHeartbeatConsumer(manager)
	c.tuning.heartbeatInterval = 5 * time.Millisecond
	c.tuning.heartbeatTTL = 40 * time.Millisecond

	recorder := newStopRunRecorder()
	done := runHeartbeatLoop(context.Background(), c, recorder.stopRun)

	// Let two sends fail inside the safety deadline, then recover.
	_ = waitHeartbeat(t, manager)
	_ = waitHeartbeat(t, manager)
	manager.setErr(nil)

	deadline := time.Now().Add(time.Second)
	for c.Health().Heartbeat.ConsecutiveFailures != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	health := c.Health().Heartbeat
	if health.ConsecutiveFailures != 0 || health.LastError != nil {
		t.Fatalf("Health().Heartbeat after recovery = %+v, want reset", health)
	}
	assertNoStopRun(t, recorder)
	lastSuccess := health.LastSuccess

	// Fail permanently: validity must be measured from the renewed success,
	// not the loop-start anchor the earlier failures counted against.
	errAgain := errors.New("heartbeat down again")
	manager.setErr(errAgain)

	err := waitStopRun(t, recorder)
	stopTime := time.Now()
	waitDone(t, done)

	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("stopRun error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	if !errors.Is(err, errAgain) {
		t.Fatalf("stopRun error = %v, want preserves cause %v", err, errAgain)
	}
	if margin := c.tuning.heartbeatTTL - c.tuning.heartbeatInterval; stopTime.Sub(lastSuccess) < margin {
		t.Fatalf("stopRun %v after last success, want no earlier than %v", stopTime.Sub(lastSuccess), margin)
	}
}

// blockingFailingHeartbeatManager parks Heartbeat until the loop context is
// cancelled and then fails, modeling a shutdown that lands mid-send.
type blockingFailingHeartbeatManager struct {
	fakeLeaseManager

	started chan struct{}
	once    sync.Once
	err     error
}

func (m *blockingFailingHeartbeatManager) Heartbeat(ctx context.Context, _, _ string, _ time.Duration) error {
	m.once.Do(func() { close(m.started) })
	<-ctx.Done()
	return m.err
}

func TestWorkerHeartbeatLoopShutdownIsNotReportedAsValidityLoss(t *testing.T) {
	t.Parallel()

	manager := &blockingFailingHeartbeatManager{
		started: make(chan struct{}),
		err:     errors.New("backend gone"),
	}
	reporter := &recordingReporter{}
	c := newTestHeartbeatConsumer(newRecordingHeartbeatManager())
	c.leaseManager = manager
	c.reporter = reporter
	// The interval doubles as the per-call send timeout; keep it far larger
	// than any plausible test-process stall (a mere 5s could lapse on a loaded
	// CI machine, unparking the send with a timeout error and flaking the
	// negative assertions below) so only the shutdown cancel ever unparks the
	// send, while ttl-interval keeps the validity deadline tiny.
	c.tuning.heartbeatInterval = time.Hour
	c.tuning.heartbeatTTL = time.Hour + 50*time.Millisecond

	recorder := newStopRunRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, recorder.stopRun)

	<-manager.started
	// Let the validity deadline lapse while the send is parked, then cancel:
	// the failure surfaces only after cancellation and must stay silent.
	time.Sleep(100 * time.Millisecond)
	cancel()
	waitDone(t, done)

	assertNoStopRun(t, recorder)
	if failures := reporter.countersNamed(metricHeartbeatFailures); len(failures) != 0 {
		t.Fatalf("heartbeat_failures calls = %d, want 0", len(failures))
	}
}

// hangingHeartbeatManager models a black-holed connection through a
// context-respecting client: every Heartbeat parks until its call context is
// done and returns that context's error.
type hangingHeartbeatManager struct {
	fakeLeaseManager
}

func (hangingHeartbeatManager) Heartbeat(ctx context.Context, _, _ string, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestWorkerHeartbeatLoopHangingSendStillTripsStaleness(t *testing.T) {
	t.Parallel()

	// Regression: without the per-call send timeout, a hanging Heartbeat
	// blocks the loop forever — no failure is ever recorded and the
	// ErrHeartbeatStale stop below never fires.
	reporter := &recordingReporter{}
	c := newTestHeartbeatConsumer(newRecordingHeartbeatManager())
	c.leaseManager = hangingHeartbeatManager{}
	c.reporter = reporter
	c.tuning.heartbeatInterval = 5 * time.Millisecond
	c.tuning.heartbeatTTL = 25 * time.Millisecond

	recorder := newStopRunRecorder()
	start := time.Now()
	done := runHeartbeatLoop(context.Background(), c, recorder.stopRun)

	err := waitStopRun(t, recorder)
	elapsed := time.Since(start)
	waitDone(t, done)

	if !errors.Is(err, ErrHeartbeatStale) {
		t.Fatalf("stopRun error = %v, want wraps %v", err, ErrHeartbeatStale)
	}
	// The failure cause must be the per-call deadline, not shutdown.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stopRun error = %v, want preserves cause %v", err, context.DeadlineExceeded)
	}
	if margin := c.tuning.heartbeatTTL - c.tuning.heartbeatInterval; elapsed < margin {
		t.Fatalf("stopRun after %v, want no earlier than %v", elapsed, margin)
	}
	if failures := reporter.countersNamed(metricHeartbeatFailures); len(failures) == 0 {
		t.Fatal("heartbeat_failures calls = 0, want >= 1 (timeouts must count as failures)")
	}

	health := c.Health().Heartbeat
	if health.ConsecutiveFailures < 1 {
		t.Fatalf("Health().Heartbeat.ConsecutiveFailures = %d, want >= 1", health.ConsecutiveFailures)
	}
	if !errors.Is(health.LastError, context.DeadlineExceeded) {
		t.Fatalf("Health().Heartbeat.LastError = %v, want wraps %v", health.LastError, context.DeadlineExceeded)
	}
}

// slowHeartbeatManager delays each Heartbeat, then succeeds, unless the call
// context expires first.
type slowHeartbeatManager struct {
	fakeLeaseManager

	delay time.Duration
	sent  chan struct{}
}

func (m *slowHeartbeatManager) Heartbeat(ctx context.Context, _, _ string, _ time.Duration) error {
	select {
	case <-time.After(m.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case m.sent <- struct{}{}:
	default:
	}
	return nil
}

func TestWorkerHeartbeatLoopSlowSendWithinIntervalSucceeds(t *testing.T) {
	t.Parallel()

	// A send slower than the network ideal but well inside the interval must
	// not be cut off by the per-call timeout.
	manager := &slowHeartbeatManager{delay: 20 * time.Millisecond, sent: make(chan struct{}, 1)}
	reporter := &recordingReporter{}
	c := newTestHeartbeatConsumer(newRecordingHeartbeatManager())
	c.leaseManager = manager
	c.reporter = reporter
	c.tuning.heartbeatInterval = time.Hour
	c.tuning.heartbeatTTL = 2 * time.Hour

	recorder := newStopRunRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := runHeartbeatLoop(ctx, c, recorder.stopRun)

	select {
	case <-manager.sent:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for slow heartbeat to complete")
	}
	cancel()
	waitDone(t, done)

	if failures := reporter.countersNamed(metricHeartbeatFailures); len(failures) != 0 {
		t.Fatalf("heartbeat_failures calls = %d, want 0", len(failures))
	}
	assertNoStopRun(t, recorder)
	if c.Health().Heartbeat.LastSuccess.IsZero() {
		t.Fatal("Health().Heartbeat.LastSuccess is zero, want recorded success")
	}
}

func newTestHeartbeatConsumer(manager *recordingHeartbeatManager) *Consumer {
	tuning := defaultTuning()
	tuning.heartbeatInterval = 10 * time.Millisecond
	tuning.heartbeatTTL = 30 * time.Millisecond

	return &Consumer{
		cfg: Config{
			StreamName:    "stream",
			ConsumerGroup: "group",
		},
		leaseManager: manager,
		leaseOwner:   "owner",
		tuning:       tuning,
		logger:       slog.New(slog.DiscardHandler),
		reporter:     metrics.Nop{},
	}
}

// stopRunRecorder captures the first stopRun invocation from the heartbeat
// loop so tests can await it or assert it never happened.
type stopRunRecorder struct {
	ch chan error
}

func newStopRunRecorder() *stopRunRecorder {
	return &stopRunRecorder{ch: make(chan error, 1)}
}

func (r *stopRunRecorder) stopRun(err error) {
	select {
	case r.ch <- err:
	default:
	}
}

func waitStopRun(t *testing.T, r *stopRunRecorder) error {
	t.Helper()

	select {
	case err := <-r.ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat stopRun")
		return nil
	}
}

// assertNoStopRun must be called after the loop has stopped, so no signal can
// still be in flight.
func assertNoStopRun(t *testing.T, r *stopRunRecorder) {
	t.Helper()

	select {
	case err := <-r.ch:
		t.Fatalf("stopRun called with %v, want no call", err)
	default:
	}
}

func runHeartbeatLoop(ctx context.Context, c *Consumer, stopRun func(error)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.workerHeartbeatLoop(ctx, stopRun)
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
