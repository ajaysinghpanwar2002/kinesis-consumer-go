package lease

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pratilipi/kinesis-consumer-go/internal/backend"
	consumerlease "github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

func newTestManager(t *testing.T, opts ...Option) (*Manager, *miniredis.Miniredis) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)

	opts = append([]Option{WithKeyPrefix("lease-test")}, opts...)
	mgr, err := NewManager(server.Addr(), opts...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return mgr, server
}

func TestManagerAcquireClaimReleaseRenew(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t)
	ctx := context.Background()

	lease, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire error=%v ok=%v, want ok true", err, ok)
	}

	// A second worker cannot acquire an already-owned shard.
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-b", time.Minute); err != nil || ok {
		t.Fatalf("Acquire second ok=%v err=%v, want ok false", ok, err)
	}

	owners, err := mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if owners["shard-1"] != "owner-a" {
		t.Fatalf("owner = %q, want owner-a", owners["shard-1"])
	}

	// Claiming with the wrong expected owner fails.
	if _, ok, err := mgr.Claim(ctx, "stream", "shard-1", "owner-b", "owner-c", time.Minute); err != nil || ok {
		t.Fatalf("Claim wrong owner ok=%v err=%v, want ok false", ok, err)
	}

	// Claiming with the correct expected owner transfers ownership.
	claimed, ok, err := mgr.Claim(ctx, "stream", "shard-1", "owner-a", "owner-b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim error=%v ok=%v, want ok true", err, ok)
	}

	owners, err = mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if owners["shard-1"] != "owner-b" {
		t.Fatalf("owner = %q, want owner-b", owners["shard-1"])
	}

	if err := claimed.Renew(ctx, time.Minute); err != nil {
		t.Fatalf("Renew error: %v", err)
	}
	if err := claimed.Release(ctx); err != nil {
		t.Fatalf("Release error: %v", err)
	}
	// Releasing or renewing a lease we no longer own reports ErrNotOwned.
	if err := claimed.Release(ctx); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("second Release error = %v, want ErrNotOwned", err)
	}
	if err := claimed.Renew(ctx, time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("Renew after release error = %v, want ErrNotOwned", err)
	}
	// The original owner was superseded by the claim, so its Release also fails.
	if err := lease.Release(ctx); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("old lease Release error = %v, want ErrNotOwned", err)
	}
}

func TestManagerMaxLeasesGate(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	lease1, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// The single slot is taken, so a different shard cannot be acquired.
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire shard-2 = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}

	// Releasing the held lease frees the slot for another shard.
	if err := lease1.Release(ctx); err != nil {
		t.Fatalf("Release shard-1: %v", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after release ok=%v err=%v, want ok true", ok, err)
	}
}

// The following three tests discriminate the release-frees-slot behavior under
// a bounded tracker (MaxLeases=1): each reserves the single slot, then hits a
// path where the backend op fails (or ownership is lost), and asserts a
// different shard can still be acquired afterward. They fail if the
// corresponding releaseSlot()/once.Do(done) call is removed, unlike the
// unlimited-tracker tests where release is a no-op.

func TestManagerAcquireFailureFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	// Another worker already owns shard-1, so the bounded manager's SET NX
	// fails after it has reserved its single slot; that slot must be freed.
	if err := server.Set(backend.LeaseKey("lease-test", "stream", "shard-1"), "owner-x"); err != nil {
		t.Fatalf("preset lease: %v", err)
	}
	if l, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Acquire already-owned shard-1 = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after failed acquire ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerClaimFailureFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	// Claim against the wrong expected owner returns res==0 after reserving the
	// single slot; that slot must be freed.
	if err := server.Set(backend.LeaseKey("lease-test", "stream", "shard-1"), "owner-x"); err != nil {
		t.Fatalf("preset lease: %v", err)
	}
	if l, ok, err := mgr.Claim(ctx, "stream", "shard-1", "wrong-owner", "owner-a", time.Minute); err != nil || ok || l != nil {
		t.Fatalf("Claim wrong owner = (%v, %v, %v), want (nil, false, nil)", l, ok, err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after failed claim ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerRenewNotOwnedFreesSlot(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t, WithMaxLeases(1))
	ctx := context.Background()

	lease, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Second)
	if err != nil || !ok {
		t.Fatalf("Acquire shard-1 error=%v ok=%v, want ok true", err, ok)
	}

	// The lease TTL lapses in Valkey, but the in-memory slot stays held until a
	// renew observes the loss of ownership and releases it.
	server.FastForward(2 * time.Second)
	if err := lease.Renew(ctx, time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("Renew after expiry = %v, want ErrNotOwned", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-2", "owner-a", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire shard-2 after renew-not-owned ok=%v err=%v, want ok true (slot must be freed)", ok, err)
	}
}

func TestManagerHeartbeatWorkers(t *testing.T) {
	t.Parallel()

	mgr, _ := newTestManager(t)
	ctx := context.Background()

	if err := mgr.Heartbeat(ctx, "stream", "worker-a", time.Minute); err != nil {
		t.Fatalf("Heartbeat error: %v", err)
	}
	if err := mgr.Heartbeat(ctx, "stream", "worker-b", time.Minute); err != nil {
		t.Fatalf("Heartbeat error: %v", err)
	}

	owners, err := mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	slices.Sort(owners)
	want := []string{"worker-a", "worker-b"}
	if !slices.Equal(owners, want) {
		t.Fatalf("workers = %v, want %v", owners, want)
	}
}

func TestManagerHeartbeatExpiresWorker(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	ctx := context.Background()

	if err := mgr.Heartbeat(ctx, "stream", "worker-a", time.Second); err != nil {
		t.Fatalf("Heartbeat error: %v", err)
	}

	owners, err := mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	if !slices.Contains(owners, "worker-a") {
		t.Fatalf("workers = %v, want worker-a", owners)
	}

	server.FastForward(2 * time.Second)
	owners, err = mgr.Workers(ctx, "stream")
	if err != nil {
		t.Fatalf("Workers error: %v", err)
	}
	if slices.Contains(owners, "worker-a") {
		t.Fatalf("worker-a still present after TTL expiry")
	}
}

func TestManagerLeaseExpires(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	ctx := context.Background()

	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-a", time.Second); err != nil || !ok {
		t.Fatalf("Acquire error=%v ok=%v, want ok true", err, ok)
	}

	owners, err := mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if owners["shard-1"] != "owner-a" {
		t.Fatalf("owner = %q, want owner-a", owners["shard-1"])
	}

	server.FastForward(2 * time.Second)
	owners, err = mgr.List(ctx, "stream")
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if _, ok := owners["shard-1"]; ok {
		t.Fatalf("shard-1 still present after TTL expiry")
	}
}

func TestNewManagerRejectsEmptyAddr(t *testing.T) {
	t.Parallel()

	if _, err := NewManager(""); err == nil {
		t.Fatal("NewManager(\"\") error = nil, want error")
	}
}

func TestNewManagerPingFailure(t *testing.T) {
	t.Parallel()

	// Reserve a port with miniredis, then close it so the address is
	// unreachable, forcing NewManager's ping to fail promptly.
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	addr := server.Addr()
	server.Close()

	mgr, err := NewManager(addr, WithPingTimeout(200*time.Millisecond))
	if err == nil {
		_ = mgr.Close()
		t.Fatal("NewManager on unreachable addr error = nil, want error")
	}
}
