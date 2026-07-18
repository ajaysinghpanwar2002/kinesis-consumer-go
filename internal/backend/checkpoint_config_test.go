package backend

import (
	"reflect"
	"testing"
	"time"
)

func TestDefaultCheckpointConfig(t *testing.T) {
	t.Parallel()

	got := DefaultCheckpointConfig("localhost:6379")
	want := CheckpointConfig{Addr: "localhost:6379"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultCheckpointConfig = %+v, want %+v", got, want)
	}
}

func TestFinalizeCheckpointConfigErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     CheckpointConfig
		wantErr string
	}{
		{
			name:    "empty addr",
			cfg:     CheckpointConfig{},
			wantErr: "valkey address is required",
		},
		{
			name:    "negative db",
			cfg:     CheckpointConfig{Addr: "addr", DB: -1},
			wantErr: "valkey db must be >= 0",
		},
		{
			name:    "cluster with non-zero db",
			cfg:     CheckpointConfig{Addr: "addr", UseCluster: true, DB: 1},
			wantErr: "valkey db is not supported with cluster mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := FinalizeCheckpointConfig(tt.cfg, "valkey")
			if err == nil {
				t.Fatalf("FinalizeCheckpointConfig error = nil, want %q", tt.wantErr)
			}
			if err.Error() != tt.wantErr {
				t.Fatalf("FinalizeCheckpointConfig error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestFinalizeCheckpointConfigDefaultsAndPreservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  CheckpointConfig
		want CheckpointConfig
	}{
		{
			name: "applies all defaults",
			cfg:  CheckpointConfig{Addr: "addr"},
			want: CheckpointConfig{
				Addr:        "addr",
				KeyPrefix:   "kinesis-checkpoint",
				LeasePrefix: "kinesis-lease",
				PingTimeout: 5 * time.Second,
			},
		},
		{
			name: "derives lease prefix from custom key prefix",
			cfg:  CheckpointConfig{Addr: "addr", KeyPrefix: "custom"},
			want: CheckpointConfig{
				Addr:        "addr",
				KeyPrefix:   "custom",
				LeasePrefix: "custom-lease",
				PingTimeout: 5 * time.Second,
			},
		},
		{
			name: "preserves explicit lease prefix",
			cfg:  CheckpointConfig{Addr: "addr", KeyPrefix: "custom", LeasePrefix: "explicit-lease"},
			want: CheckpointConfig{
				Addr:        "addr",
				KeyPrefix:   "custom",
				LeasePrefix: "explicit-lease",
				PingTimeout: 5 * time.Second,
			},
		},
		{
			name: "accepts cluster with zero db",
			cfg:  CheckpointConfig{Addr: "addr", UseCluster: true},
			want: CheckpointConfig{
				Addr:        "addr",
				UseCluster:  true,
				KeyPrefix:   "kinesis-checkpoint",
				LeasePrefix: "kinesis-lease",
				PingTimeout: 5 * time.Second,
			},
		},
		{
			name: "preserves positive ping timeout and non-zero db",
			cfg:  CheckpointConfig{Addr: "addr", DB: 2, PingTimeout: time.Second},
			want: CheckpointConfig{
				Addr:        "addr",
				DB:          2,
				KeyPrefix:   "kinesis-checkpoint",
				LeasePrefix: "kinesis-lease",
				PingTimeout: time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := FinalizeCheckpointConfig(tt.cfg, "valkey")
			if err != nil {
				t.Fatalf("FinalizeCheckpointConfig error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("FinalizeCheckpointConfig = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestCheckpointKey pins the stored v2 key format; changing any literal here
// is a breaking stored-format change and needs a version bump plus a
// migration note in docs/configuration.md.
func TestCheckpointKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		stream string
		shard  string
		want   string
	}{
		{
			name:   "default prefix",
			prefix: "kinesis-checkpoint",
			stream: "stream",
			shard:  "shard-1",
			want:   "kinesis-checkpoint:v2:c3RyZWFt:c2hhcmQtMQ",
		},
		{
			name:   "coordination identity encodes with its group delimiter",
			prefix: "kinesis-checkpoint",
			stream: "group-a:orders",
			shard:  "shard-1",
			want:   "kinesis-checkpoint:v2:Z3JvdXAtYTpvcmRlcnM:c2hhcmQtMQ",
		},
		{
			name:   "prefix hash-tag delimiters are escaped like lease keys",
			prefix: "ten{ant}%",
			stream: "stream",
			shard:  "shard-1",
			want:   "ten%7Bant%7D%25:v2:c3RyZWFt:c2hhcmQtMQ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := CheckpointKey(tt.prefix, tt.stream, tt.shard); got != tt.want {
				t.Fatalf("CheckpointKey(%q, %q, %q) = %q, want %q", tt.prefix, tt.stream, tt.shard, got, tt.want)
			}
		})
	}
}

// TestCheckpointKeyIsInjective is the collision regression for the pre-v2
// raw-join format: these (stream, shard) pairs join to the identical string
// "a:b:c", so the old scheme mapped them to one key.
func TestCheckpointKeyIsInjective(t *testing.T) {
	t.Parallel()

	a := CheckpointKey("kinesis-checkpoint", "a:b", "c")
	b := CheckpointKey("kinesis-checkpoint", "a", "b:c")
	if a == b {
		t.Fatalf("CheckpointKey collision: (%q,%q) and (%q,%q) both map to %q", "a:b", "c", "a", "b:c", a)
	}
}

func TestDefaultLeasePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		checkpointPrefix string
		want             string
	}{
		{name: "empty falls back to standalone default", checkpointPrefix: "", want: "kinesis-lease"},
		{name: "default checkpoint prefix maps to the shared standalone default", checkpointPrefix: "kinesis-checkpoint", want: "kinesis-lease"},
		{name: "custom checkpoint prefix derives an adjacent lease prefix", checkpointPrefix: "custom", want: "custom-lease"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := DefaultLeasePrefix(tt.checkpointPrefix); got != tt.want {
				t.Fatalf("DefaultLeasePrefix(%q) = %q, want %q", tt.checkpointPrefix, got, tt.want)
			}
		})
	}
}

// TestDefaultPrefixesAgreeAcrossConstructionPaths pins COORD-4: a default
// standalone lease manager and a default store-provided one must coordinate
// in the same key namespace, or two such workers each acquire every shard.
func TestDefaultPrefixesAgreeAcrossConstructionPaths(t *testing.T) {
	t.Parallel()

	leaseCfg, err := FinalizeLeaseConfig(DefaultLeaseConfig("addr"), "valkey")
	if err != nil {
		t.Fatalf("FinalizeLeaseConfig error = %v, want nil", err)
	}
	checkpointCfg, err := FinalizeCheckpointConfig(DefaultCheckpointConfig("addr"), "valkey")
	if err != nil {
		t.Fatalf("FinalizeCheckpointConfig error = %v, want nil", err)
	}

	if leaseCfg.KeyPrefix != checkpointCfg.LeasePrefix {
		t.Fatalf("standalone default lease prefix %q != store-derived default lease prefix %q; the construction paths coordinate in different namespaces",
			leaseCfg.KeyPrefix, checkpointCfg.LeasePrefix)
	}
}

func TestSetCheckpointKeyPrefix(t *testing.T) {
	t.Parallel()

	var cfg CheckpointConfig
	if err := SetCheckpointKeyPrefix(&cfg, ""); err == nil {
		t.Fatal("SetCheckpointKeyPrefix(\"\") error = nil, want error")
	}
	if err := SetCheckpointKeyPrefix(&cfg, "prefix"); err != nil {
		t.Fatalf("SetCheckpointKeyPrefix error = %v, want nil", err)
	}
	if cfg.KeyPrefix != "prefix" {
		t.Fatalf("KeyPrefix = %q, want prefix", cfg.KeyPrefix)
	}
}

func TestSetCheckpointLeasePrefix(t *testing.T) {
	t.Parallel()

	var cfg CheckpointConfig
	if err := SetCheckpointLeasePrefix(&cfg, ""); err == nil {
		t.Fatal("SetCheckpointLeasePrefix(\"\") error = nil, want error")
	}
	if err := SetCheckpointLeasePrefix(&cfg, "lease-prefix"); err != nil {
		t.Fatalf("SetCheckpointLeasePrefix error = %v, want nil", err)
	}
	if cfg.LeasePrefix != "lease-prefix" {
		t.Fatalf("LeasePrefix = %q, want lease-prefix", cfg.LeasePrefix)
	}
}

func TestSetCheckpointPingTimeout(t *testing.T) {
	t.Parallel()

	var cfg CheckpointConfig
	if err := SetCheckpointPingTimeout(&cfg, 0); err == nil {
		t.Fatal("SetCheckpointPingTimeout(0) error = nil, want error")
	}
	if err := SetCheckpointPingTimeout(&cfg, -time.Second); err == nil {
		t.Fatal("SetCheckpointPingTimeout(-1s) error = nil, want error")
	}
	if err := SetCheckpointPingTimeout(&cfg, 2*time.Second); err != nil {
		t.Fatalf("SetCheckpointPingTimeout error = %v, want nil", err)
	}
	if cfg.PingTimeout != 2*time.Second {
		t.Fatalf("PingTimeout = %v, want 2s", cfg.PingTimeout)
	}
}

func TestSetCheckpointDB(t *testing.T) {
	t.Parallel()

	var cfg CheckpointConfig
	if err := SetCheckpointDB(&cfg, -1); err == nil {
		t.Fatal("SetCheckpointDB(-1) error = nil, want error")
	}
	if err := SetCheckpointDB(&cfg, 3); err != nil {
		t.Fatalf("SetCheckpointDB error = %v, want nil", err)
	}
	if cfg.DB != 3 {
		t.Fatalf("DB = %d, want 3", cfg.DB)
	}
}
