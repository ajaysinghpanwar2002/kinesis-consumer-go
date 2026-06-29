package consumer

import (
	"context"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	t.Parallel()

	cfg := defaultOptions()
	wantTuning := defaultTuning()

	if cfg.batchHandler != nil {
		t.Fatalf("batchHandler = %v, want nil", cfg.batchHandler)
	}
	if cfg.shutdown.gracefulDrain {
		t.Fatal("gracefulDrain = true, want false")
	}
	if cfg.shutdown.gracefulDrainTimeout != 0 {
		t.Fatalf("gracefulDrainTimeout = %v, want 0", cfg.shutdown.gracefulDrainTimeout)
	}
	if cfg.tuning != wantTuning {
		t.Fatalf("tuning = %+v, want %+v", cfg.tuning, wantTuning)
	}
}

func TestApplyOptionsSkipsNilOptions(t *testing.T) {
	t.Parallel()

	cfg, err := applyOptions([]Option{nil})
	if err != nil {
		t.Fatalf("applyOptions() error = %v, want nil", err)
	}
	if cfg.tuning != defaultTuning() {
		t.Fatalf("tuning = %+v, want default tuning", cfg.tuning)
	}
}

func TestOptionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
		want string
	}{
		{
			name: "nil batch handler",
			opt:  WithBatchHandler(nil),
			want: "batch handler cannot be nil",
		},
		{
			name: "retry max attempts",
			opt:  WithRetry(0, time.Second),
			want: "maxAttempts must be >= 1",
		},
		{
			name: "retry backoff",
			opt:  WithRetry(1, 0),
			want: "backoff must be > 0",
		},
		{
			name: "poll interval",
			opt:  WithPolling(0, time.Second),
			want: "pollInterval must be > 0",
		},
		{
			name: "shard sync interval",
			opt:  WithPolling(time.Second, 500*time.Millisecond),
			want: "shardSyncInterval must be >= 1s",
		},
		{
			name: "batch size",
			opt:  WithBatching(0, 1),
			want: "batchSize must be >= 1",
		},
		{
			name: "checkpoint every",
			opt:  WithBatching(1, 0),
			want: "checkpointEvery must be >= 1",
		},
		{
			name: "shard concurrency",
			opt:  WithShardConcurrency(0),
			want: "shardConcurrency must be >= 1",
		},
		{
			name: "lease manager",
			opt:  WithLeaseManager(nil),
			want: "lease manager cannot be nil",
		},
		{
			name: "rebalance min interval",
			opt:  WithRebalance(0, 0, time.Second, 1),
			want: "rebalance minInterval must be > 0",
		},
		{
			name: "rebalance jitter",
			opt:  WithRebalance(time.Second, -1, time.Second, 1),
			want: "rebalance jitter cannot be negative",
		},
		{
			name: "rebalance cooldown",
			opt:  WithRebalance(time.Second, 0, 0, 1),
			want: "rebalance cooldown must be > 0",
		},
		{
			name: "rebalance max moves",
			opt:  WithRebalance(time.Second, 0, time.Second, 0),
			want: "rebalance maxMoves must be >= 1",
		},
		{
			name: "heartbeat interval",
			opt:  WithHeartbeat(0, time.Second),
			want: "heartbeat interval must be > 0",
		},
		{
			name: "heartbeat ttl",
			opt:  WithHeartbeat(time.Second, 0),
			want: "heartbeat ttl must be > 0",
		},
		{
			name: "graceful drain timeout",
			opt:  WithGracefulDrain(-1 * time.Second),
			want: "graceful drain timeout cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := applyOptions([]Option{tt.opt})
			if err == nil {
				t.Fatalf("applyOptions() error = nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Fatalf("applyOptions() error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestOptionsApplyValues(t *testing.T) {
	t.Parallel()

	called := false
	handler := func(context.Context, []Record) error {
		called = true
		return nil
	}
	leaseMgr := fakeLeaseManager{}

	cfg, err := applyOptions([]Option{
		WithBatchHandler(handler),
		WithLeaseManager(leaseMgr),
		WithRetry(2, 3*time.Second),
		WithPolling(4*time.Second, 5*time.Second),
		WithBatching(6, 7),
		WithShardConcurrency(8),
		WithRebalance(9*time.Second, 10*time.Second, 11*time.Second, 12),
		WithHeartbeat(13*time.Second, 14*time.Second),
		WithGracefulDrain(15 * time.Second),
	})
	if err != nil {
		t.Fatalf("applyOptions() error = %v, want nil", err)
	}

	if cfg.batchHandler == nil {
		t.Fatalf("batchHandler = nil, want handler")
	}
	if err := cfg.batchHandler(context.Background(), nil); err != nil {
		t.Fatalf("batchHandler() error = %v, want nil", err)
	}
	if !called {
		t.Fatal("batchHandler was not called")
	}
	if cfg.lease.manager != leaseMgr {
		t.Fatalf("lease manager was not applied")
	}
	if !cfg.shutdown.gracefulDrain {
		t.Fatal("gracefulDrain = false, want true")
	}
	if cfg.shutdown.gracefulDrainTimeout != 15*time.Second {
		t.Fatalf("gracefulDrainTimeout = %v, want %v", cfg.shutdown.gracefulDrainTimeout, 15*time.Second)
	}

	if cfg.tuning.retryMaxAttempts != 2 {
		t.Fatalf("retryMaxAttempts = %d, want 2", cfg.tuning.retryMaxAttempts)
	}
	if cfg.tuning.retryBackoff != 3*time.Second {
		t.Fatalf("retryBackoff = %v, want %v", cfg.tuning.retryBackoff, 3*time.Second)
	}
	if cfg.tuning.pollInterval != 4*time.Second {
		t.Fatalf("pollInterval = %v, want %v", cfg.tuning.pollInterval, 4*time.Second)
	}
	if cfg.tuning.shardSyncInterval != 5*time.Second {
		t.Fatalf("shardSyncInterval = %v, want %v", cfg.tuning.shardSyncInterval, 5*time.Second)
	}
	if cfg.tuning.batchSize != 6 {
		t.Fatalf("batchSize = %d, want 6", cfg.tuning.batchSize)
	}
	if cfg.tuning.checkpointEvery != 7 {
		t.Fatalf("checkpointEvery = %d, want 7", cfg.tuning.checkpointEvery)
	}
	if cfg.tuning.shardConcurrency != 8 {
		t.Fatalf("shardConcurrency = %d, want 8", cfg.tuning.shardConcurrency)
	}
	if cfg.tuning.rebalanceIntervalMin != 9*time.Second {
		t.Fatalf("rebalanceIntervalMin = %v, want %v", cfg.tuning.rebalanceIntervalMin, 9*time.Second)
	}
	if cfg.tuning.rebalanceIntervalJitter != 10*time.Second {
		t.Fatalf("rebalanceIntervalJitter = %v, want %v", cfg.tuning.rebalanceIntervalJitter, 10*time.Second)
	}
	if cfg.tuning.shardCooldownPeriod != 11*time.Second {
		t.Fatalf("shardCooldownPeriod = %v, want %v", cfg.tuning.shardCooldownPeriod, 11*time.Second)
	}
	if cfg.tuning.maxMovesPerRebalance != 12 {
		t.Fatalf("maxMovesPerRebalance = %d, want 12", cfg.tuning.maxMovesPerRebalance)
	}
	if cfg.tuning.heartbeatInterval != 13*time.Second {
		t.Fatalf("heartbeatInterval = %v, want %v", cfg.tuning.heartbeatInterval, 13*time.Second)
	}
	if cfg.tuning.heartbeatTTL != 14*time.Second {
		t.Fatalf("heartbeatTTL = %v, want %v", cfg.tuning.heartbeatTTL, 14*time.Second)
	}
}

func TestWithGracefulDrainZeroTimeoutEnablesIndefiniteDrain(t *testing.T) {
	t.Parallel()

	cfg, err := applyOptions([]Option{
		WithGracefulDrain(0),
	})
	if err != nil {
		t.Fatalf("applyOptions() error = %v, want nil", err)
	}
	if !cfg.shutdown.gracefulDrain {
		t.Fatal("gracefulDrain = false, want true")
	}
	if cfg.shutdown.gracefulDrainTimeout != 0 {
		t.Fatalf("gracefulDrainTimeout = %v, want 0", cfg.shutdown.gracefulDrainTimeout)
	}
}
