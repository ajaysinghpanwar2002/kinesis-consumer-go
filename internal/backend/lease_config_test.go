package backend

import (
	"reflect"
	"testing"
	"time"
)

func TestDefaultLeaseConfig(t *testing.T) {
	cfg := DefaultLeaseConfig("localhost:6379")
	if cfg.Addr != "localhost:6379" {
		t.Fatalf("Addr = %q, want %q", cfg.Addr, "localhost:6379")
	}
	if !reflect.DeepEqual(cfg, LeaseConfig{Addr: "localhost:6379"}) {
		t.Fatalf("DefaultLeaseConfig set unexpected fields: %+v", cfg)
	}
}

func TestFinalizeLeaseConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  LeaseConfig
		want string
	}{
		{
			name: "empty addr",
			cfg:  LeaseConfig{},
			want: "valkey address is required",
		},
		{
			name: "negative db",
			cfg:  LeaseConfig{Addr: "x", DB: -1},
			want: "valkey db must be >= 0",
		},
		{
			name: "cluster with non-zero db",
			cfg:  LeaseConfig{Addr: "x", UseCluster: true, DB: 1},
			want: "valkey db is not supported with cluster mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FinalizeLeaseConfig(tt.cfg, "valkey")
			if err == nil {
				t.Fatalf("FinalizeLeaseConfig(%+v) error = nil, want %q", tt.cfg, tt.want)
			}
			if err.Error() != tt.want {
				t.Fatalf("error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestFinalizeLeaseConfigDefaultsAndPreservation(t *testing.T) {
	tests := []struct {
		name string
		in   LeaseConfig
		want LeaseConfig
	}{
		{
			name: "defaults applied",
			in:   LeaseConfig{Addr: "x"},
			want: LeaseConfig{Addr: "x", KeyPrefix: "kinesis-lease", PingTimeout: 5 * time.Second},
		},
		{
			name: "explicit values preserved",
			in: LeaseConfig{
				Addr:        "x",
				UseTLS:      true,
				KeyPrefix:   "custom",
				PingTimeout: 2 * time.Second,
				DB:          3,
				MaxLeases:   7,
			},
			want: LeaseConfig{
				Addr:        "x",
				UseTLS:      true,
				KeyPrefix:   "custom",
				PingTimeout: 2 * time.Second,
				DB:          3,
				MaxLeases:   7,
			},
		},
		{
			name: "cluster with db zero accepted",
			in:   LeaseConfig{Addr: "x", UseCluster: true},
			want: LeaseConfig{Addr: "x", UseCluster: true, KeyPrefix: "kinesis-lease", PingTimeout: 5 * time.Second},
		},
		{
			name: "negative max leases left unvalidated (unlimited)",
			in:   LeaseConfig{Addr: "x", MaxLeases: -1},
			want: LeaseConfig{Addr: "x", KeyPrefix: "kinesis-lease", PingTimeout: 5 * time.Second, MaxLeases: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FinalizeLeaseConfig(tt.in, "valkey")
			if err != nil {
				t.Fatalf("FinalizeLeaseConfig(%+v) error = %v", tt.in, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("FinalizeLeaseConfig(%+v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestLeaseKey(t *testing.T) {
	if got := LeaseKey("kinesis-lease", "stream", "shard-1"); got != "kinesis-lease:stream:shard-1" {
		t.Fatalf("LeaseKey = %q, want %q", got, "kinesis-lease:stream:shard-1")
	}
}

func TestWorkerKey(t *testing.T) {
	if got := WorkerKey("kinesis-lease-worker", "stream", "owner-1"); got != "kinesis-lease-worker:stream:owner-1" {
		t.Fatalf("WorkerKey = %q, want %q", got, "kinesis-lease-worker:stream:owner-1")
	}
}

func TestSetLeaseKeyPrefix(t *testing.T) {
	var cfg LeaseConfig
	if err := SetLeaseKeyPrefix(&cfg, "custom"); err != nil {
		t.Fatalf("SetLeaseKeyPrefix valid error = %v", err)
	}
	if cfg.KeyPrefix != "custom" {
		t.Fatalf("KeyPrefix = %q, want %q", cfg.KeyPrefix, "custom")
	}
	if err := SetLeaseKeyPrefix(&cfg, ""); err == nil {
		t.Fatal("SetLeaseKeyPrefix(\"\") error = nil, want error")
	}
}

func TestSetLeasePingTimeout(t *testing.T) {
	var cfg LeaseConfig
	if err := SetLeasePingTimeout(&cfg, time.Second); err != nil {
		t.Fatalf("SetLeasePingTimeout valid error = %v", err)
	}
	if cfg.PingTimeout != time.Second {
		t.Fatalf("PingTimeout = %v, want %v", cfg.PingTimeout, time.Second)
	}
	for _, bad := range []time.Duration{0, -time.Second} {
		if err := SetLeasePingTimeout(&cfg, bad); err == nil {
			t.Fatalf("SetLeasePingTimeout(%v) error = nil, want error", bad)
		}
	}
}

func TestSetLeaseDB(t *testing.T) {
	var cfg LeaseConfig
	if err := SetLeaseDB(&cfg, 2); err != nil {
		t.Fatalf("SetLeaseDB valid error = %v", err)
	}
	if cfg.DB != 2 {
		t.Fatalf("DB = %d, want 2", cfg.DB)
	}
	if err := SetLeaseDB(&cfg, -1); err == nil {
		t.Fatal("SetLeaseDB(-1) error = nil, want error")
	}
}

func TestSetLeaseMaxLeases(t *testing.T) {
	var cfg LeaseConfig
	if err := SetLeaseMaxLeases(&cfg, 5); err != nil {
		t.Fatalf("SetLeaseMaxLeases valid error = %v", err)
	}
	if cfg.MaxLeases != 5 {
		t.Fatalf("MaxLeases = %d, want 5", cfg.MaxLeases)
	}
	// Zero is allowed and means unlimited.
	if err := SetLeaseMaxLeases(&cfg, 0); err != nil {
		t.Fatalf("SetLeaseMaxLeases(0) error = %v, want nil", err)
	}
	if cfg.MaxLeases != 0 {
		t.Fatalf("MaxLeases = %d, want 0", cfg.MaxLeases)
	}
	if err := SetLeaseMaxLeases(&cfg, -1); err == nil {
		t.Fatal("SetLeaseMaxLeases(-1) error = nil, want error")
	}
}
