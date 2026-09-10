package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryGenerationFencesSameOwner(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryManager()
	now := time.Now()
	m.now = func() time.Time { return now }
	first, _, _ := m.Acquire(ctx, "stream", "shard", "owner", time.Second)
	original := m.leases["stream"]["shard"]
	now = now.Add(time.Second)
	second, ok, err := m.Acquire(ctx, "stream", "shard", "owner", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	if first.(FencedLease).Generation() == second.(FencedLease).Generation() {
		t.Fatal("reused generation")
	}
	for _, operation := range []func() error{
		func() error { return first.Renew(ctx, time.Second) },
		func() error { return first.Release(ctx) },
		func() error { return first.(FencedLease).Validate(ctx) },
	} {
		if err := operation(); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("old handle: %v", err)
		}
	}
	// Even restoring the original live backend entry cannot resurrect a locally
	// invalidated handle. An unobserved historical restore is outside the contract.
	now = original.expiry.Add(-time.Millisecond)
	m.leases["stream"]["shard"] = original
	if err := first.(FencedLease).Validate(ctx); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("resurrected: %v", err)
	}
}

func TestMemoryUnobservedHistoricalRestoreBoundary(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryManager()
	first, _, _ := m.Acquire(ctx, "s", "a", "one", time.Hour)
	original := m.leases["s"]["a"]
	next, _, _ := m.Claim(ctx, "s", "a", "one", "two", time.Hour)
	if first.(FencedLease).Generation() == next.(FencedLease).Generation() {
		t.Fatal("transfer reused generation")
	}
	m.leases["s"]["a"] = original
	if err := first.(FencedLease).Validate(ctx); err != nil {
		t.Fatalf("historical restore boundary changed: %v", err)
	}
	first.(FencedLease).Invalidate()
	if err := first.(FencedLease).Validate(ctx); !errors.Is(err, ErrNotOwned) {
		t.Fatal(err)
	}
}

func TestMemoryOwnershipLockSerializesTransfer(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryManager()
	held, _, _ := m.Acquire(ctx, "s", "a", "one", time.Hour)
	entered, finish := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := m.WithLease(ctx, held.(FencedLease), "s", "a", func() error { close(entered); <-finish; return nil })
		if err != nil {
			t.Error(err)
		}
	}()
	<-entered
	transferred := make(chan struct{})
	go func() {
		defer wg.Done()
		_, ok, err := m.Claim(ctx, "s", "a", "one", "two", time.Hour)
		if err != nil || !ok {
			t.Errorf("claim: %v %v", ok, err)
		}
		close(transferred)
	}()
	select {
	case <-transferred:
		t.Fatal("transfer bypassed ownership lock")
	default:
	}
	close(finish)
	wg.Wait()
	called := false
	err := m.WithLease(ctx, held.(FencedLease), "s", "a", func() error { called = true; return nil })
	if !errors.Is(err, ErrNotOwned) || called {
		t.Fatalf("stale callback: %v, called %v", err, called)
	}
}

func TestMemoryFencedExpiryCancellationAndMismatch(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryManager()
	now := time.Now()
	m.now = func() time.Time { return now }
	held, _, _ := m.Acquire(ctx, "s", "a", "owner", time.Second)
	f := held.(FencedLease)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := f.Validate(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := f.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.WithLease(ctx, f, "s", "other", func() error { t.Fatal("called"); return nil }); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := f.Validate(ctx); !errors.Is(err, ErrNotOwned) {
		t.Fatal(err)
	}
	now = now.Add(-time.Second)
	if err := f.Validate(ctx); !errors.Is(err, ErrNotOwned) {
		t.Fatal("expiry invalidation was not permanent", err)
	}
}
