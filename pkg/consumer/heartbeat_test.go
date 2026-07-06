package consumer

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
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

	return nil
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
