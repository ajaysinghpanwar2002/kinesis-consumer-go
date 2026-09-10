package checkpoint

import (
	"context"
	"errors"
	"fmt"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	core "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func bindTest(t *testing.T, s *Store, m lease.Manager, shard string) (lease.FencedLease, core.Session) {
	t.Helper()
	held, ok, err := m.Acquire(context.Background(), "stream", shard, "owner", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	fenced := held.(lease.FencedLease)
	session, err := s.Bind(context.Background(), "stream", shard, fenced)
	if err != nil {
		t.Fatal(err)
	}
	return fenced, session
}
func managerTest(t *testing.T, s *Store) lease.Manager {
	t.Helper()
	m, err := s.LeaseManager()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.(interface{ Close() error }).Close() })
	return m
}

func TestFencedRecoveryTransitions(t *testing.T) {
	s, server := newTestStore(t)
	m := managerTest(t, s)
	ctx := context.Background()
	held, session := bindTest(t, s, m, "shard")
	p, err := session.Recovery(ctx)
	if err != nil || p.Kind != core.RecoveryFresh {
		t.Fatalf("fresh %v %v", p, err)
	}
	p, err = session.Initialize(ctx, "100000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	first := p
	p, err = session.Initialize(ctx, "999999999999999999999999999999999999999")
	if err != nil || p != first {
		t.Fatalf("write once %v %v", p, err)
	}
	if err = session.Save(ctx, "1"); !errors.Is(err, core.ErrRecoveryState) {
		t.Fatalf("before initial %v", err)
	}
	next := "100000000000000000000000000000000000002"
	if err = session.Save(ctx, next); err != nil {
		t.Fatal(err)
	}
	if server.Exists(s.key("stream", "shard") + ":initial") {
		t.Fatal("initial retained")
	}
	if err = session.Save(ctx, first.Sequence); err != nil {
		t.Fatal(err)
	}
	p, err = session.Recovery(ctx)
	if err != nil || p.Sequence != next || p.Kind != core.RecoveryCheckpoint {
		t.Fatalf("checkpoint %v %v", p, err)
	}
	if err = session.Save(ctx, "SHARD_END:"+next); err != nil {
		t.Fatal(err)
	}
	if err = session.Save(ctx, "999999999999999999999999999999999999999"); err != nil {
		t.Fatal(err)
	}
	if err = held.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_, successor := bindTest(t, s, m, "shard")
	p, err = successor.Recovery(ctx)
	if err != nil || p.Kind != core.RecoveryCompleted {
		t.Fatalf("completion %v %v", p, err)
	}
}

func TestFencedMetadataLossAndRollback(t *testing.T) {
	for _, phase := range []string{"initial", "checkpoint", "completed"} {
		for _, damage := range []string{"value", "registry", "wrongtype", "ttl", "rollback", "extra"} {
			t.Run(phase+"/"+damage, func(t *testing.T) {
				s, server := newTestStore(t)
				m := managerTest(t, s)
				ctx := context.Background()
				_, session := bindTest(t, s, m, "shard")
				if _, err := session.Initialize(ctx, "10"); err != nil {
					t.Fatal(err)
				}
				key := s.key("stream", "shard") + ":initial"
				if phase != "initial" {
					key = s.key("stream", "shard")
					v := "20"
					if phase == "completed" {
						v = "SHARD_END:20"
					}
					if err := session.Save(ctx, v); err != nil {
						t.Fatal(err)
					}
				}
				registry := backend.RecoveryRegistryKey(s.keyPrefix, "stream")
				switch damage {
				case "value":
					server.Del(key)
				case "registry":
					server.HDel(registry, "shard")
				case "wrongtype":
					server.Del(key)
					server.HSet(key, "bad", "type")
				case "ttl":
					server.SetTTL(key, time.Hour)
				case "rollback":
					if err := server.Set(key, "9"); err != nil {
						t.Fatal(err)
					}
				case "extra":
					other := s.key("stream", "shard")
					if phase != "initial" {
						other += ":initial"
					}
					if err := server.Set(other, "10"); err != nil {
						t.Fatal(err)
					}
				}
				for _, op := range []func() error{func() error { _, e := session.Recovery(ctx); return e }, func() error { _, e := session.Initialize(ctx, "30"); return e }, func() error { return session.Save(ctx, "40") }} {
					if err := op(); !errors.Is(err, core.ErrRecoveryState) {
						t.Fatalf("damage accepted: %v", err)
					}
				}
			})
		}
	}
}

func TestFencedMetadataOutlivesEveryLeaseAndWorker(t *testing.T) {
	s, server := newTestStore(t)
	m := managerTest(t, s)
	ctx := context.Background()
	for _, kind := range []string{"initial", "checkpoint", "completed"} {
		_, session := bindTest(t, s, m, kind)
		if _, err := session.Initialize(ctx, "10"); err != nil {
			t.Fatal(err)
		}
		if kind != "initial" {
			v := "20"
			if kind == "completed" {
				v = "SHARD_END:20"
			}
			if err := session.Save(ctx, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.Heartbeat(ctx, "stream", "owner", time.Minute); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Minute)
	if _, err := m.List(ctx, "stream"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Workers(ctx, "stream"); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"initial", "checkpoint", "completed"} {
		_, session := bindTest(t, s, m, kind)
		p, err := session.Recovery(ctx)
		if err != nil || string(p.Kind) != kind {
			t.Fatalf("lost %s: %v %v", kind, p, err)
		}
	}
}

func TestFencedConcurrentInitializationAndTransfer(t *testing.T) {
	s, _ := newTestStore(t)
	m := managerTest(t, s)
	ctx := context.Background()
	held, session := bindTest(t, s, m, "shard")
	var wg sync.WaitGroup
	positions := make(chan core.RecoveryPosition, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other, err := s.Bind(ctx, "stream", "shard", held)
			if err != nil {
				t.Error(err)
				return
			}
			p, err := other.Initialize(ctx, fmt.Sprint(100+i))
			if err != nil {
				t.Error(err)
				return
			}
			positions <- p
		}()
	}
	wg.Wait()
	close(positions)
	var first core.RecoveryPosition
	for p := range positions {
		if first.Kind == core.RecoveryFresh {
			first = p
		}
		if p != first {
			t.Errorf("initial changed: %v %v", first, p)
		}
	}
	replacement, ok, err := m.Claim(ctx, "stream", "shard", "owner", "owner", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	if replacement.(lease.FencedLease).Generation() == held.Generation() {
		t.Fatal("generation reused")
	}
	if err = session.Save(ctx, "999"); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("stale save %v", err)
	}
	if err = held.Validate(ctx); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("stale validation %v", err)
	}
	successor, err := s.Bind(ctx, "stream", "shard", replacement.(lease.FencedLease))
	if err != nil {
		t.Fatal(err)
	}
	p, err := successor.Recovery(ctx)
	if err != nil || p != first {
		t.Fatalf("successor %v %v", p, err)
	}
	successor.Invalidate()
	if err = successor.Save(ctx, "999"); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("invalid session %v", err)
	}
	if err = replacement.(lease.FencedLease).Validate(ctx); err != nil {
		t.Fatalf("session invalidated lease: %v", err)
	}
}

func TestFencedBindMismatchAndLegacyProgress(t *testing.T) {
	s, _ := newTestStore(t)
	m := managerTest(t, s)
	ctx := context.Background()
	held, _ := bindTest(t, s, m, "shard")
	if _, err := s.Bind(ctx, "stream", "other", held); !errors.Is(err, lease.ErrLeaseMismatch) {
		t.Fatalf("mismatch %v", err)
	}
	other, _ := newTestStore(t)
	if _, err := other.Bind(ctx, "stream", "shard", held); !errors.Is(err, lease.ErrLeaseMismatch) {
		t.Fatalf("backend mismatch %v", err)
	}
	if err := s.Save(ctx, "stream", "shard", "20"); err != nil {
		t.Fatal(err)
	}
	session, err := s.Bind(ctx, "stream", "shard", held)
	if err != nil {
		t.Fatal(err)
	}
	p, err := session.Recovery(ctx)
	if err != nil || p.Kind != core.RecoveryCheckpoint || p.Sequence != "20" {
		t.Fatalf("legacy %v %v", p, err)
	}
	if err = s.Delete(ctx, "stream", "shard"); err != nil {
		t.Fatal(err)
	}
	if _, err = session.Recovery(ctx); !errors.Is(err, core.ErrRecoveryState) {
		t.Fatalf("reset not detected %v", err)
	}
}

func TestIncompatibleLayoutPreserved(t *testing.T) {
	for _, key := range []string{"kinesis-checkpoint:v2:c3RyZWFt:c2hhcmQ", "kinesis-checkpoint:v3:orders:shard-000", "kinesis-checkpoint:group:stream:shard", "kinesis-lease:v2:{c3RyZWFt}:lease-owners", "kinesis-lease-worker:stream:owner"} {
		t.Run(key, func(t *testing.T) {
			s, server := newTestStore(t)
			if err := server.Set(key, "10"); err != nil {
				t.Fatal(err)
			}
			other, err := New(server.Addr())
			if other != nil {
				other.Close()
			}
			if !errors.Is(err, ErrIncompatibleLayout) {
				t.Fatalf("layout accepted %v", err)
			}
			if got, _ := server.Get(key); got != "10" {
				t.Fatal("old state changed")
			}
			_ = s
		})
	}
}

func TestFencedSaveRacingTransfer(t *testing.T) {
	s, _ := newTestStore(t)
	m := managerTest(t, s)
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		shard := fmt.Sprint(i)
		held, session := bindTest(t, s, m, shard)
		if _, err := session.Initialize(ctx, "10"); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		saved := make(chan error, 1)
		go func() { <-start; saved <- session.Save(ctx, "20") }()
		close(start)
		replacement, ok, err := m.Claim(ctx, "stream", shard, "owner", "next", time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim %v %v", ok, err)
		}
		if err = <-saved; err != nil && !errors.Is(err, lease.ErrNotOwned) {
			t.Fatal(err)
		}
		successor, err := s.Bind(ctx, "stream", shard, replacement.(lease.FencedLease))
		if err != nil {
			t.Fatal(err)
		}
		before, err := successor.Recovery(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if before.Sequence != "10" && before.Sequence != "20" {
			t.Fatalf("invalid race outcome %v", before)
		}
		if err = session.Save(ctx, "SHARD_END:30"); !errors.Is(err, lease.ErrNotOwned) {
			t.Fatalf("post-transfer write %v", err)
		}
		after, err := successor.Recovery(ctx)
		if err != nil || after != before {
			t.Fatalf("stale writer changed progress: %v %v", after, err)
		}
		if err = held.Renew(ctx, time.Minute); !errors.Is(err, lease.ErrNotOwned) {
			t.Fatalf("stale renew %v", err)
		}
	}
}

func TestFencedCanceledAndTransientOperations(t *testing.T) {
	s, server := newTestStore(t)
	m := managerTest(t, s)
	ctx := context.Background()
	held, session := bindTest(t, s, m, "shard")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := session.Initialize(canceled, "10"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled initialize %v", err)
	}
	server.SetError("ERR injected backend failure")
	if _, err := session.Initialize(ctx, "10"); err == nil {
		t.Fatal("backend failure accepted")
	}
	server.SetError("")
	if err := held.Validate(ctx); err != nil {
		t.Fatalf("transient error invalidated lease %v", err)
	}
	p, err := session.Recovery(ctx)
	if err != nil || p.Kind != core.RecoveryFresh {
		t.Fatalf("failed operations wrote %v %v", p, err)
	}
	if _, err = session.Initialize(ctx, "20"); err != nil {
		t.Fatal(err)
	}
}

func TestReopenNestedConfiguredPrefixes(t *testing.T) {
	for _, prefixes := range [][2]string{{"tenant", "tenant:leases"}, {"tenant:checkpoints", "tenant"}, {"tenant{a}%", "tenant{a}%:leases"}} {
		t.Run(prefixes[0]+"/"+prefixes[1], func(t *testing.T) {
			s, server := newTestStore(t, WithKeyPrefix(prefixes[0]), WithLeasePrefix(prefixes[1]))
			m := managerTest(t, s)
			held, session := bindTest(t, s, m, "shard")
			if _, err := session.Initialize(context.Background(), "10"); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(server.Addr(), WithKeyPrefix(prefixes[0]), WithLeasePrefix(prefixes[1]))
			if err != nil {
				t.Fatalf("reopen own state: %v", err)
			}
			defer reopened.Close()
			_ = managerTest(t, reopened)
			successor, err := reopened.Bind(context.Background(), "stream", "shard", held)
			if err != nil {
				t.Fatal(err)
			}
			p, err := successor.Recovery(context.Background())
			if err != nil || p.Kind != core.RecoveryInitial || p.Sequence != "10" {
				t.Fatalf("reopened recovery %v %v", p, err)
			}
		})
	}
}

// A real blocked server request holds both session and lease serialization.
// Each queued call must honor its own deadline without waiting for that request.
func TestFencedQueuedOperationsRespectCancellation(t *testing.T) {
	for _, operation := range []string{"renew", "release", "validate", "recovery", "initialize", "save"} {
		t.Run(operation, func(t *testing.T) {
			s, server := newTestStore(t)
			m := managerTest(t, s)
			held, session := bindTest(t, s, m, "shard")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := session.Initialize(ctx, "10"); err != nil {
				t.Fatal(err)
			}

			entered, resume := make(chan struct{}), make(chan struct{})
			var blockOnce, resumeOnce sync.Once
			unblock := func() { resumeOnce.Do(func() { close(resume) }) }
			defer unblock()
			server.Server().SetPreHook(func(_ *miniredisserver.Peer, cmd string, _ ...string) bool {
				if cmd == "EVALSHA" || cmd == "EVAL" {
					blockOnce.Do(func() { close(entered); <-resume })
				}
				return false
			})
			active := make(chan error, 1)
			go func() { active <- session.Save(ctx, "20") }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("active request did not reach backend")
			}

			queuedCtx, queuedCancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer queuedCancel()
			queued := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "renew":
					err = held.Renew(queuedCtx, time.Minute)
				case "release":
					err = held.Release(queuedCtx)
				case "validate":
					err = held.Validate(queuedCtx)
				case "recovery":
					_, err = session.Recovery(queuedCtx)
				case "initialize":
					_, err = session.Initialize(queuedCtx, "30")
				case "save":
					err = session.Save(queuedCtx, "30")
				}
				queued <- err
			}()
			select {
			case err := <-queued:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("queued call: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled call is still waiting for the active backend request")
			}
			select {
			case err := <-active:
				t.Fatalf("active request ended before backend resumed: %v", err)
			default:
			}
			unblock()
			if err := <-active; err != nil {
				t.Fatal(err)
			}
			if err := held.Validate(ctx); err != nil {
				t.Fatalf("canceled waiter invalidated the lease: %v", err)
			}
			position, err := session.Recovery(ctx)
			if err != nil || position.Kind != core.RecoveryCheckpoint || position.Sequence != "20" {
				t.Fatalf("canceled waiter changed progress: %v %v", position, err)
			}
		})
	}
}
