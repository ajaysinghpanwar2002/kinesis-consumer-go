package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func newSession(t *testing.T) (*MemoryStore, *lease.MemoryManager, lease.FencedLease, Session) {
	t.Helper()
	m := lease.NewMemoryManager()
	store := NewMemoryStoreWithLeaseManager(m)
	held, ok, err := m.Acquire(context.Background(), "s", "a", "owner", time.Hour)
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	f := held.(lease.FencedLease)
	session, err := store.Bind(context.Background(), "s", "a", f)
	if err != nil {
		t.Fatal(err)
	}
	return store, m, f, session
}

func TestMemoryRecoveryLifecycle(t *testing.T) {
	ctx := context.Background()
	store, m, held, s := newSession(t)
	p, err := s.Recovery(ctx)
	if err != nil || p.Kind != RecoveryFresh {
		t.Fatalf("fresh: %+v %v", p, err)
	}
	p, err = s.Initialize(ctx, "99")
	if err != nil || p != (RecoveryPosition{RecoveryInitial, "99"}) {
		t.Fatalf("initial: %+v %v", p, err)
	}
	p, err = s.Initialize(ctx, "100")
	if err != nil || p.Sequence != "99" {
		t.Fatalf("overwritten initial: %+v %v", p, err)
	}
	if err := s.Save(ctx, "98"); !errors.Is(err, ErrRecoveryState) {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "100"); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "99"); err != nil {
		t.Fatal(err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = m.List(ctx, "s")
	_ = m.Deregister(ctx, "s", "owner")
	next, _, _ := m.Acquire(ctx, "s", "a", "owner", time.Hour)
	successor, err := store.Bind(ctx, "s", "a", next.(lease.FencedLease))
	if err != nil {
		t.Fatal(err)
	}
	p, err = successor.Initialize(ctx, "200")
	if err != nil || p != (RecoveryPosition{RecoveryCheckpoint, "100"}) {
		t.Fatalf("successor: %+v %v", p, err)
	}
	if err := s.Save(ctx, CompletedPrefix); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatal(err)
	}
	if err := successor.Save(ctx, "SHARD_END:100"); err != nil {
		t.Fatal(err)
	}
	if err := successor.Save(ctx, "999"); err != nil {
		t.Fatal(err)
	}
	p, err = successor.Recovery(ctx)
	if err != nil || p != (RecoveryPosition{RecoveryCompleted, "SHARD_END:100"}) {
		t.Fatalf("completion: %+v %v", p, err)
	}
}

func TestMemoryConcurrentInitialization(t *testing.T) {
	ctx := context.Background()
	store, _, held, s := newSession(t)
	var wg sync.WaitGroup
	positions := make(chan RecoveryPosition, 32)
	for i := 1; i <= 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := store.Bind(ctx, "s", "a", held)
			if err != nil {
				t.Error(err)
				return
			}
			p, err := session.Initialize(ctx, fmt.Sprint(i))
			if err != nil {
				t.Error(err)
			}
			positions <- p
		}(i)
	}
	wg.Wait()
	close(positions)
	want, err := s.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for got := range positions {
		if got != want {
			t.Fatalf("%+v != %+v", got, want)
		}
	}
}

func TestMemoryRecoveryCorruption(t *testing.T) {
	cases := map[string]func(*MemoryStore, string){
		"missing initial":             func(m *MemoryStore, k string) { delete(m.initial, k) },
		"invalid initial":             func(m *MemoryStore, k string) { m.initial[k] = "01" },
		"missing registry":            func(m *MemoryStore, k string) { delete(m.registry, k) },
		"unknown registry":            func(m *MemoryStore, k string) { m.registry[k] = "unknown" },
		"unexpected checkpoint":       func(m *MemoryStore, k string) { m.data[k] = "100" },
		"checkpoint cannot fall back": func(m *MemoryStore, k string) { m.registry[k] = RecoveryCheckpoint },
		"missing completion":          func(m *MemoryStore, k string) { m.registry[k] = RecoveryCompleted; delete(m.initial, k) },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			m, _, held, s := newSession(t)
			if _, err := s.Initialize(ctx, "99"); err != nil {
				t.Fatal(err)
			}
			corrupt(m, m.key("s", "a"))
			if _, err := m.Bind(ctx, "s", "a", held); !errors.Is(err, ErrRecoveryState) {
				t.Fatalf("fresh session: %v", err)
			}
			if _, err := s.Initialize(ctx, "100"); !errors.Is(err, ErrRecoveryState) {
				t.Fatalf("initialize: %v", err)
			}
			if err := s.Save(ctx, "100"); !errors.Is(err, ErrRecoveryState) {
				t.Fatalf("save: %v", err)
			}
		})
	}
}

func TestMemoryCheckpointLossAndRegression(t *testing.T) {
	for _, value := range []string{"", "9", "garbage", "SHARD_END:garbage"} {
		t.Run(value, func(t *testing.T) {
			ctx := context.Background()
			m, _, _, s := newSession(t)
			if err := s.Save(ctx, "100"); err != nil {
				t.Fatal(err)
			}
			key := m.key("s", "a")
			if value == "" {
				delete(m.data, key)
			} else {
				m.data[key] = value
			}
			if _, err := s.Recovery(ctx); !errors.Is(err, ErrRecoveryState) {
				t.Fatal(err)
			}
		})
	}
}

func TestMemorySessionMismatchAndInvalidation(t *testing.T) {
	ctx := context.Background()
	m, manager, held, s := newSession(t)
	other, _, _ := lease.NewMemoryManager().Acquire(ctx, "s", "a", "owner", time.Hour)
	for _, candidate := range []lease.FencedLease{nil, other.(lease.FencedLease)} {
		if _, err := m.Bind(ctx, "s", "a", candidate); !errors.Is(err, lease.ErrLeaseMismatch) {
			t.Fatal(err)
		}
	}
	if _, err := m.Bind(ctx, "s", "wrong", held); !errors.Is(err, lease.ErrLeaseMismatch) {
		t.Fatal(err)
	}
	if _, err := NewMemoryStore().Bind(ctx, "s", "a", held); !errors.Is(err, lease.ErrLeaseMismatch) {
		t.Fatal(err)
	}
	s.Invalidate()
	if err := s.Save(ctx, "1"); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatal(err)
	}
	if err := held.Validate(ctx); err != nil {
		t.Fatal("session invalidation released lease", err)
	}
	next, _, _ := manager.Claim(ctx, "s", "a", "owner", "owner", time.Hour)
	if _, err := m.Bind(ctx, "s", "a", held); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatal(err)
	}
	if _, err := m.Bind(ctx, "s", "a", next.(lease.FencedLease)); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryRecoverySurvivesLeaseAndWorkerExpiry(t *testing.T) {
	for _, kind := range []RecoveryKind{RecoveryInitial, RecoveryCheckpoint, RecoveryCompleted} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			m := lease.NewMemoryManager()
			store := NewMemoryStoreWithLeaseManager(m)
			held, _, _ := m.Acquire(ctx, "s", "a", "owner", time.Hour)
			_ = m.Heartbeat(ctx, "s", "owner", time.Hour)
			s, err := store.Bind(ctx, "s", "a", held.(lease.FencedLease))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Initialize(ctx, "100"); err != nil {
				t.Fatal(err)
			}
			if kind == RecoveryCheckpoint {
				if err := s.Save(ctx, "101"); err != nil {
					t.Fatal(err)
				}
			}
			if kind == RecoveryCompleted {
				if err := s.Save(ctx, CompletedPrefix); err != nil {
					t.Fatal(err)
				}
			}
			want, err := s.Recovery(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := held.Renew(ctx, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			_ = m.Heartbeat(ctx, "s", "owner", time.Millisecond)
			time.Sleep(time.Millisecond * 5)
			leases, _ := m.List(ctx, "s")
			workers, _ := m.Workers(ctx, "s")
			if len(leases) != 0 || len(workers) != 0 {
				t.Fatal("entries did not expire")
			}
			next, _, _ := m.Acquire(ctx, "s", "a", "next", time.Hour)
			successor, err := store.Bind(ctx, "s", "a", next.(lease.FencedLease))
			if err != nil {
				t.Fatal(err)
			}
			got, err := successor.Recovery(ctx)
			if err != nil || got != want {
				t.Fatalf("lost recovery: %+v %v", got, err)
			}
		})
	}
}

func TestMemorySaveRacingTransfer(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		store, m, _, s := newSession(t)
		if _, err := s.Initialize(ctx, "1"); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		result := make(chan error, 1)
		go func() { <-start; result <- s.Save(ctx, "2") }()
		close(start)
		next, ok, err := m.Claim(ctx, "s", "a", "owner", "next", time.Hour)
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		saveErr := <-result
		if saveErr != nil && !errors.Is(saveErr, lease.ErrNotOwned) {
			t.Fatal(saveErr)
		}
		successor, err := store.Bind(ctx, "s", "a", next.(lease.FencedLease))
		if err != nil {
			t.Fatal(err)
		}
		p, err := successor.Recovery(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if saveErr == nil && p != (RecoveryPosition{RecoveryCheckpoint, "2"}) {
			t.Fatalf("accepted save missing: %+v", p)
		}
		if saveErr != nil && p != (RecoveryPosition{RecoveryInitial, "1"}) {
			t.Fatalf("rejected save persisted: %+v", p)
		}
		if err := s.Save(ctx, CompletedPrefix); !errors.Is(err, lease.ErrNotOwned) {
			t.Fatal(err)
		}
	}
}

func TestMemoryRecoveryStartupAndTotalLossBoundary(t *testing.T) {
	ctx := context.Background()
	store, _, held, s := newSession(t)
	// A fetched sequence has no protection until Initialize commits it.
	fresh, err := store.Bind(ctx, "s", "a", held)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := fresh.Recovery(ctx); err != nil || p.Kind != RecoveryFresh {
		t.Fatalf("before initialization: %+v %v", p, err)
	}
	if _, err := s.Initialize(ctx, "10"); err != nil {
		t.Fatal(err)
	}
	key := store.key("s", "a")
	delete(store.initial, key)
	delete(store.registry, key)
	if _, err := s.Recovery(ctx); !errors.Is(err, ErrRecoveryState) {
		t.Fatal("observed loss not detected", err)
	}
	// With all evidence lost, a new session cannot distinguish a fresh namespace.
	afterLoss, err := store.Bind(ctx, "s", "a", held)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := afterLoss.Recovery(ctx); err != nil || p.Kind != RecoveryFresh {
		t.Fatalf("total loss boundary: %+v %v", p, err)
	}
}

func TestMemoryFencedInvalidValuesAndCancellation(t *testing.T) {
	ctx := context.Background()
	_, _, _, s := newSession(t)
	for _, invalid := range []string{"", "01", "-1", "1.0", "SHARD_ENDoops", "SHARD_END:", "SHARD_END:01"} {
		if _, err := s.Initialize(ctx, invalid); !errors.Is(err, ErrRecoveryState) {
			t.Fatalf("initial %q: %v", invalid, err)
		}
		if err := s.Save(ctx, invalid); !errors.Is(err, ErrRecoveryState) {
			t.Fatalf("save %q: %v", invalid, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Initialize(canceled, "1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Save(canceled, "2"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p, err := s.Recovery(ctx); err != nil || p.Kind != RecoveryFresh {
		t.Fatalf("canceled mutation: %+v %v", p, err)
	}
}
