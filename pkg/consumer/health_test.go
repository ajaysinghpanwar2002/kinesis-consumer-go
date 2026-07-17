package consumer

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
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
