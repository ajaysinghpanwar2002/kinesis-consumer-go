package consumer

import (
	"testing"
	"time"
)

func TestDefaultTuning(t *testing.T) {
	t.Parallel()

	cfg := defaultTuning()

	if cfg.shardConcurrency != 1 {
		t.Fatalf("shardConcurrency = %d, want 1", cfg.shardConcurrency)
	}
	if cfg.batchSize != 100 {
		t.Fatalf("batchSize = %d, want 100", cfg.batchSize)
	}
	if cfg.pollInterval != time.Second {
		t.Fatalf("pollInterval = %v, want %v", cfg.pollInterval, time.Second)
	}
	if cfg.shardSyncInterval != time.Minute {
		t.Fatalf("shardSyncInterval = %v, want %v", cfg.shardSyncInterval, time.Minute)
	}
	if cfg.retryMaxAttempts != 3 {
		t.Fatalf("retryMaxAttempts = %d, want 3", cfg.retryMaxAttempts)
	}
	if cfg.retryBackoff != time.Second {
		t.Fatalf("retryBackoff = %v, want %v", cfg.retryBackoff, time.Second)
	}
	if cfg.checkpointEvery != 100 {
		t.Fatalf("checkpointEvery = %d, want 100", cfg.checkpointEvery)
	}
	if cfg.rebalanceIntervalMin != 10*time.Second {
		t.Fatalf("rebalanceIntervalMin = %v, want %v", cfg.rebalanceIntervalMin, 10*time.Second)
	}
	if cfg.rebalanceIntervalJitter != 10*time.Second {
		t.Fatalf("rebalanceIntervalJitter = %v, want %v", cfg.rebalanceIntervalJitter, 10*time.Second)
	}
	if cfg.heartbeatInterval != 5*time.Second {
		t.Fatalf("heartbeatInterval = %v, want %v", cfg.heartbeatInterval, 5*time.Second)
	}
	if cfg.heartbeatTTL != 20*time.Second {
		t.Fatalf("heartbeatTTL = %v, want %v", cfg.heartbeatTTL, 20*time.Second)
	}
	if cfg.shardLeaseReleaseTimeout != 5*time.Second {
		t.Fatalf("shardLeaseReleaseTimeout = %v, want %v", cfg.shardLeaseReleaseTimeout, 5*time.Second)
	}
	if cfg.shardCooldownPeriod != 10*time.Second {
		t.Fatalf("shardCooldownPeriod = %v, want %v", cfg.shardCooldownPeriod, 10*time.Second)
	}
	if cfg.maxMovesPerRebalance != 2 {
		t.Fatalf("maxMovesPerRebalance = %d, want 2", cfg.maxMovesPerRebalance)
	}
}

func TestTuningValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*tuningConfig)
		want string
	}{
		{
			name: "default tuning",
		},
		{
			name: "batch size",
			edit: func(cfg *tuningConfig) { cfg.batchSize = 0 },
			want: "batch size must be >= 1",
		},
		{
			name: "shard concurrency",
			edit: func(cfg *tuningConfig) { cfg.shardConcurrency = 0 },
			want: "shardConcurrency must be >= 1",
		},
		{
			name: "poll interval",
			edit: func(cfg *tuningConfig) { cfg.pollInterval = 0 },
			want: "pollInterval must be > 0",
		},
		{
			name: "shard sync interval",
			edit: func(cfg *tuningConfig) { cfg.shardSyncInterval = 500 * time.Millisecond },
			want: "shardSyncInterval must be >= 1s",
		},
		{
			name: "retry attempts",
			edit: func(cfg *tuningConfig) { cfg.retryMaxAttempts = 0 },
			want: "retry max attempts must be >= 1",
		},
		{
			name: "retry backoff",
			edit: func(cfg *tuningConfig) { cfg.retryBackoff = 0 },
			want: "retry backoff must be > 0",
		},
		{
			name: "checkpoint every",
			edit: func(cfg *tuningConfig) { cfg.checkpointEvery = 0 },
			want: "checkpointEvery must be >= 1",
		},
		{
			name: "rebalance min interval",
			edit: func(cfg *tuningConfig) { cfg.rebalanceIntervalMin = 0 },
			want: "rebalance min interval must be > 0",
		},
		{
			name: "rebalance jitter",
			edit: func(cfg *tuningConfig) { cfg.rebalanceIntervalJitter = -1 },
			want: "rebalance jitter cannot be negative",
		},
		{
			name: "heartbeat interval",
			edit: func(cfg *tuningConfig) { cfg.heartbeatInterval = 0 },
			want: "heartbeat interval must be > 0",
		},
		{
			name: "heartbeat ttl",
			edit: func(cfg *tuningConfig) { cfg.heartbeatTTL = 0 },
			want: "heartbeat ttl must be > 0",
		},
		{
			name: "heartbeat interval equals ttl",
			edit: func(cfg *tuningConfig) {
				cfg.heartbeatInterval = 10 * time.Second
				cfg.heartbeatTTL = 10 * time.Second
			},
			want: "heartbeat interval must be < heartbeat ttl (recommend ttl >= 3x interval)",
		},
		{
			name: "heartbeat interval exceeds ttl",
			edit: func(cfg *tuningConfig) {
				cfg.heartbeatInterval = 30 * time.Second
				cfg.heartbeatTTL = 5 * time.Second
			},
			want: "heartbeat interval must be < heartbeat ttl (recommend ttl >= 3x interval)",
		},
		{
			name: "heartbeat interval just below ttl",
			edit: func(cfg *tuningConfig) {
				cfg.heartbeatInterval = 10*time.Second - time.Millisecond
				cfg.heartbeatTTL = 10 * time.Second
			},
		},
		{
			name: "shard lease release timeout",
			edit: func(cfg *tuningConfig) { cfg.shardLeaseReleaseTimeout = 0 },
			want: "shard lease release timeout must be > 0",
		},
		{
			name: "shard cooldown",
			edit: func(cfg *tuningConfig) { cfg.shardCooldownPeriod = 0 },
			want: "shard cooldown must be > 0",
		},
		{
			name: "max moves",
			edit: func(cfg *tuningConfig) { cfg.maxMovesPerRebalance = 0 },
			want: "maxMovesPerRebalance must be >= 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultTuning()
			if tt.edit != nil {
				tt.edit(&cfg)
			}

			err := cfg.validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("validate() error = nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Fatalf("validate() error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}
