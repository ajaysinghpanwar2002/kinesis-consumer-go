package lease

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	testStream = "stream-a"
	testShard  = "shard-1"
	testTTL    = 10 * time.Second
)

// clock is a deterministic, manually advanced clock for TTL tests.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestManager returns a manager wired to a controllable clock starting at a
// fixed instant.
func newTestManager() (*MemoryManager, *clock) {
	c := &clock{t: time.Unix(1_000, 0)}
	m := NewMemoryManager()
	m.now = c.now
	return m, c
}

func mustAcquire(t *testing.T, m *MemoryManager, shard, owner string) Lease {
	t.Helper()
	l, acquired, err := m.Acquire(context.Background(), testStream, shard, owner, testTTL)
	if err != nil {
		t.Fatalf("Acquire(%s,%s): unexpected error %v", shard, owner, err)
	}
	if !acquired || l == nil {
		t.Fatalf("Acquire(%s,%s) = (%v, %v), want acquired lease", shard, owner, l, acquired)
	}
	return l
}

func TestMemoryManagerAcquireFreeShard(t *testing.T) {
	m, _ := newTestManager()
	mustAcquire(t, m, testShard, "owner-1")

	owners, err := m.List(context.Background(), testStream)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if owners[testShard] != "owner-1" {
		t.Fatalf("List[%s] = %q, want %q", testShard, owners[testShard], "owner-1")
	}
}

func TestMemoryManagerAcquireDeniedWhileLive(t *testing.T) {
	m, c := newTestManager()
	mustAcquire(t, m, testShard, "owner-1")

	// A different owner cannot acquire a live lease.
	if l, acquired, err := m.Acquire(context.Background(), testStream, testShard, "owner-2", testTTL); err != nil || acquired || l != nil {
		t.Fatalf("Acquire by owner-2 = (%v, %v, %v), want (nil, false, nil)", l, acquired, err)
	}

	// Neither can the current owner re-acquire its own live lease (matches SetNX).
	if l, acquired, err := m.Acquire(context.Background(), testStream, testShard, "owner-1", testTTL); err != nil || acquired || l != nil {
		t.Fatalf("re-Acquire by owner-1 = (%v, %v, %v), want (nil, false, nil)", l, acquired, err)
	}

	// Advancing to exactly the expiry instant counts as expired.
	c.advance(testTTL)
	if l, acquired, err := m.Acquire(context.Background(), testStream, testShard, "owner-2", testTTL); err != nil || !acquired || l == nil {
		t.Fatalf("Acquire after expiry = (%v, %v, %v), want acquired lease", l, acquired, err)
	}
}

func TestMemoryManagerAcquireTakeoverAfterExpiry(t *testing.T) {
	m, c := newTestManager()
	mustAcquire(t, m, testShard, "owner-1")

	c.advance(testTTL + time.Second) // strictly past expiry

	l2 := mustAcquire(t, m, testShard, "owner-2")

	owners, err := m.List(context.Background(), testStream)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if owners[testShard] != "owner-2" {
		t.Fatalf("List[%s] = %q, want owner-2 after takeover", testShard, owners[testShard])
	}

	// The new owner can renew the taken-over lease. (The old owner losing its
	// handle after expiry is covered by TestMemoryManagerRenew.)
	if err := l2.Renew(context.Background(), testTTL); err != nil {
		t.Fatalf("owner-2 Renew: %v", err)
	}
}

func TestMemoryManagerClaim(t *testing.T) {
	t.Run("success from live owner", func(t *testing.T) {
		m, _ := newTestManager()
		orig := mustAcquire(t, m, testShard, "owner-1")

		l, claimed, err := m.Claim(context.Background(), testStream, testShard, "owner-1", "owner-2", testTTL)
		if err != nil || !claimed || l == nil {
			t.Fatalf("Claim = (%v, %v, %v), want claimed lease", l, claimed, err)
		}

		owners, _ := m.List(context.Background(), testStream)
		if owners[testShard] != "owner-2" {
			t.Fatalf("List[%s] = %q, want owner-2 after claim", testShard, owners[testShard])
		}

		// The prior owner's handle no longer owns the lease.
		if err := orig.Renew(context.Background(), testTTL); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("prior owner Renew after claim = %v, want ErrNotOwned", err)
		}
	})

	t.Run("fails on owner mismatch", func(t *testing.T) {
		m, _ := newTestManager()
		mustAcquire(t, m, testShard, "owner-1")

		if l, claimed, err := m.Claim(context.Background(), testStream, testShard, "owner-3", "owner-2", testTTL); err != nil || claimed || l != nil {
			t.Fatalf("Claim with wrong expected owner = (%v, %v, %v), want (nil, false, nil)", l, claimed, err)
		}
	})

	t.Run("fails on expired entry", func(t *testing.T) {
		m, c := newTestManager()
		mustAcquire(t, m, testShard, "owner-1")
		c.advance(testTTL + time.Second)

		if l, claimed, err := m.Claim(context.Background(), testStream, testShard, "owner-1", "owner-2", testTTL); err != nil || claimed || l != nil {
			t.Fatalf("Claim of expired lease = (%v, %v, %v), want (nil, false, nil)", l, claimed, err)
		}
	})

	t.Run("fails on unknown shard", func(t *testing.T) {
		m, _ := newTestManager()
		if l, claimed, err := m.Claim(context.Background(), testStream, "missing", "owner-1", "owner-2", testTTL); err != nil || claimed || l != nil {
			t.Fatalf("Claim of unknown shard = (%v, %v, %v), want (nil, false, nil)", l, claimed, err)
		}
	})
}

func TestMemoryManagerRenew(t *testing.T) {
	t.Run("extends lease past original expiry", func(t *testing.T) {
		m, c := newTestManager()
		l := mustAcquire(t, m, testShard, "owner-1")

		c.advance(testTTL - time.Second) // still live
		if err := l.Renew(context.Background(), testTTL); err != nil {
			t.Fatalf("Renew: %v", err)
		}

		// Past the original expiry but within the renewed window: still live.
		c.advance(2 * time.Second)
		owners, _ := m.List(context.Background(), testStream)
		if owners[testShard] != "owner-1" {
			t.Fatalf("List after renew = %q, want owner-1 still live", owners[testShard])
		}
	})

	t.Run("ErrNotOwned after expiry", func(t *testing.T) {
		m, c := newTestManager()
		l := mustAcquire(t, m, testShard, "owner-1")
		c.advance(testTTL + time.Second)

		if err := l.Renew(context.Background(), testTTL); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("Renew after expiry = %v, want ErrNotOwned", err)
		}
	})
}

func TestMemoryManagerRelease(t *testing.T) {
	t.Run("release then absent", func(t *testing.T) {
		m, _ := newTestManager()
		l := mustAcquire(t, m, testShard, "owner-1")

		if err := l.Release(context.Background()); err != nil {
			t.Fatalf("Release: %v", err)
		}
		owners, _ := m.List(context.Background(), testStream)
		if _, ok := owners[testShard]; ok {
			t.Fatalf("List still contains %s after release", testShard)
		}

		// Second release finds nothing owned.
		if err := l.Release(context.Background()); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("second Release = %v, want ErrNotOwned", err)
		}
	})

	t.Run("ErrNotOwned on expired lease", func(t *testing.T) {
		m, c := newTestManager()
		l := mustAcquire(t, m, testShard, "owner-1")
		c.advance(testTTL + time.Second)

		if err := l.Release(context.Background()); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("Release of expired lease = %v, want ErrNotOwned", err)
		}
	})
}

func TestMemoryManagerListReflectsLiveLeasesOnly(t *testing.T) {
	m, c := newTestManager()
	mustAcquire(t, m, "shard-1", "owner-1")
	c.advance(testTTL / 2)
	mustAcquire(t, m, "shard-2", "owner-2") // acquired later, expires later

	// Advance so shard-1 has expired but shard-2 is still live.
	c.advance(testTTL / 2)

	owners, err := m.List(context.Background(), testStream)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, ok := owners["shard-1"]; ok {
		t.Fatalf("List contains expired shard-1: %v", owners)
	}
	if owners["shard-2"] != "owner-2" {
		t.Fatalf("List[shard-2] = %q, want owner-2", owners["shard-2"])
	}
}

func TestMemoryManagerWorkers(t *testing.T) {
	t.Run("sorted live owners and dropout after ttl", func(t *testing.T) {
		m, c := newTestManager()
		ctx := context.Background()
		for _, owner := range []string{"owner-c", "owner-a", "owner-b"} {
			if err := m.Heartbeat(ctx, testStream, owner, testTTL); err != nil {
				t.Fatalf("Heartbeat %s: %v", owner, err)
			}
		}

		got, err := m.Workers(ctx, testStream)
		if err != nil {
			t.Fatalf("Workers: %v", err)
		}
		want := []string{"owner-a", "owner-b", "owner-c"}
		if len(got) != len(want) {
			t.Fatalf("Workers = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("Workers = %v, want %v", got, want)
			}
		}

		// Refresh only owner-a, then expire the original window.
		c.advance(testTTL / 2)
		if err := m.Heartbeat(ctx, testStream, "owner-a", testTTL); err != nil {
			t.Fatalf("Heartbeat refresh: %v", err)
		}
		c.advance(testTTL / 2) // owner-b and owner-c now expired; owner-a still live

		got, err = m.Workers(ctx, testStream)
		if err != nil {
			t.Fatalf("Workers: %v", err)
		}
		if len(got) != 1 || got[0] != "owner-a" {
			t.Fatalf("Workers after partial expiry = %v, want [owner-a]", got)
		}
	})

	t.Run("empty for unknown stream", func(t *testing.T) {
		m, _ := newTestManager()
		got, err := m.Workers(context.Background(), "other-stream")
		if err != nil {
			t.Fatalf("Workers: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("Workers = %v, want empty", got)
		}
	})
}

func TestMemoryManagerDeregister(t *testing.T) {
	t.Run("removes a live worker while leaving others", func(t *testing.T) {
		m, _ := newTestManager()
		ctx := context.Background()
		for _, owner := range []string{"owner-a", "owner-b"} {
			if err := m.Heartbeat(ctx, testStream, owner, testTTL); err != nil {
				t.Fatalf("Heartbeat %s: %v", owner, err)
			}
		}

		if err := m.Deregister(ctx, testStream, "owner-a"); err != nil {
			t.Fatalf("Deregister: %v", err)
		}

		got, err := m.Workers(ctx, testStream)
		if err != nil {
			t.Fatalf("Workers: %v", err)
		}
		if len(got) != 1 || got[0] != "owner-b" {
			t.Fatalf("Workers after Deregister = %v, want [owner-b]", got)
		}
	})

	t.Run("absent owner is a no-op success", func(t *testing.T) {
		m, _ := newTestManager()
		ctx := context.Background()
		if err := m.Heartbeat(ctx, testStream, "owner-a", testTTL); err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}

		// Never-registered owner, then the same owner twice: all no-ops.
		if err := m.Deregister(ctx, testStream, "ghost"); err != nil {
			t.Fatalf("Deregister absent owner: %v", err)
		}
		if err := m.Deregister(ctx, testStream, "owner-a"); err != nil {
			t.Fatalf("Deregister owner-a: %v", err)
		}
		if err := m.Deregister(ctx, testStream, "owner-a"); err != nil {
			t.Fatalf("Deregister owner-a again: %v", err)
		}
		if err := m.Deregister(ctx, "unknown-stream", "owner-a"); err != nil {
			t.Fatalf("Deregister unknown stream: %v", err)
		}

		got, err := m.Workers(ctx, testStream)
		if err != nil {
			t.Fatalf("Workers: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("Workers after deregistering all = %v, want empty", got)
		}
	})
}

// TestMemoryManagerConcurrentAccess is meaningful under the race detector:
// run with `go test -race ./pkg/lease/`.
func TestMemoryManagerConcurrentAccess(t *testing.T) {
	m := NewMemoryManager() // real clock is fine; we only care about data races
	ctx := context.Background()

	const goroutines = 16
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			shard := "shard-" + strconv.Itoa(g)
			owner := "owner-" + strconv.Itoa(g)
			for i := 0; i < iterations; i++ {
				_ = m.Heartbeat(ctx, testStream, owner, testTTL)
				if l, acquired, _ := m.Acquire(ctx, testStream, shard, owner, testTTL); acquired {
					_ = l.Renew(ctx, testTTL)
					_ = l.Release(ctx)
				}
				_, _, _ = m.Claim(ctx, testStream, shard, owner, owner+"-b", testTTL)
				_, _ = m.List(ctx, testStream)
				_, _ = m.Workers(ctx, testStream)
			}
		}(g)
	}
	wg.Wait()
}
