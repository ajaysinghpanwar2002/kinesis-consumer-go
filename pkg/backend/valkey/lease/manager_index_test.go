package lease

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	consumerlease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func TestManagerListRepairsExpiredAndOrphanedLeaseIndexEntries(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)
	ctx := context.Background()

	if _, ok, err := mgr.Acquire(ctx, "group:stream", "live", "owner-live", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire live = (ok=%v, err=%v), want (true, nil)", ok, err)
	}

	keys := backend.LeaseCoordinationKeys("lease-test", "group:stream")
	server.HSet(keys.LeaseOwners,
		"expired", "owner-expired",
		"owner-only", "owner-orphan",
	)
	if _, err := server.ZAdd(keys.LeaseExpirations, float64(anchor.UnixMilli()), "expired"); err != nil {
		t.Fatalf("seed expired index: %v", err)
	}
	if _, err := server.ZAdd(keys.LeaseExpirations, float64(anchor.Add(time.Minute).UnixMilli()), "expiration-only"); err != nil {
		t.Fatalf("seed expiration-only index: %v", err)
	}

	owners, err := mgr.List(ctx, "group:stream")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := map[string]string{"live": "owner-live"}
	if !mapsEqual(owners, want) {
		t.Fatalf("List = %v, want %v", owners, want)
	}

	for _, shardID := range []string{"expired", "owner-only", "expiration-only"} {
		if got := server.HGet(keys.LeaseOwners, shardID); got != "" {
			t.Errorf("stale owner %q remains %q", shardID, got)
		}
		if zsetContains(server, keys.LeaseExpirations, shardID) {
			t.Errorf("stale expiration %q remains", shardID)
		}
	}
}

func TestManagerWorkersRepairsExpiredEntries(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)
	ctx := context.Background()

	if err := mgr.Heartbeat(ctx, "group:stream", "live", time.Minute); err != nil {
		t.Fatalf("Heartbeat live: %v", err)
	}
	keys := backend.LeaseCoordinationKeys("lease-test", "group:stream")
	if _, err := server.ZAdd(keys.Workers, float64(anchor.UnixMilli()), "expired"); err != nil {
		t.Fatalf("seed expired worker: %v", err)
	}

	workers, err := mgr.Workers(ctx, "group:stream")
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if !slices.Equal(workers, []string{"live"}) {
		t.Fatalf("Workers = %v, want [live]", workers)
	}
	if zsetContains(server, keys.Workers, "expired") {
		t.Fatal("expired worker remains in index after snapshot")
	}
}

func TestManagerAggregateIndexesExpireAfterLatestMember(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)
	ctx := context.Background()

	if _, ok, err := mgr.Acquire(ctx, "group:stream", "short", "owner-short", time.Second); err != nil || !ok {
		t.Fatalf("Acquire short = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	if _, ok, err := mgr.Acquire(ctx, "group:stream", "long", "owner-long", 5*time.Second); err != nil || !ok {
		t.Fatalf("Acquire long = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	if err := mgr.Heartbeat(ctx, "group:stream", "owner-short", time.Second); err != nil {
		t.Fatalf("Heartbeat short: %v", err)
	}
	if err := mgr.Heartbeat(ctx, "group:stream", "owner-long", 5*time.Second); err != nil {
		t.Fatalf("Heartbeat long: %v", err)
	}

	keys := backend.LeaseCoordinationKeys("lease-test", "group:stream")
	for name, key := range map[string]string{
		"lease owners":      keys.LeaseOwners,
		"lease expirations": keys.LeaseExpirations,
		"workers":           keys.Workers,
	} {
		if got := server.TTL(key); got != 5*time.Second {
			t.Errorf("%s TTL = %s, want 5s (latest member)", name, got)
		}
	}

	server.FastForward(6 * time.Second)
	for name, key := range map[string]string{
		"lease owners":      keys.LeaseOwners,
		"lease expirations": keys.LeaseExpirations,
		"workers":           keys.Workers,
	} {
		if server.Exists(key) {
			t.Errorf("%s aggregate key remains after latest expiration", name)
		}
	}
}

func TestManagerExpiredLeaseCannotBeClaimedAndCanBeReacquired(t *testing.T) {
	t.Parallel()

	mgr, server := newTestManager(t)
	anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	server.SetTime(anchor)
	ctx := context.Background()

	oldLease, ok, err := mgr.Acquire(ctx, "group:stream", "shard-1", "owner-a", time.Second)
	if err != nil || !ok {
		t.Fatalf("Acquire owner-a = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	server.SetTime(anchor.Add(2 * time.Second))

	if claimed, ok, err := mgr.Claim(ctx, "group:stream", "shard-1", "owner-a", "owner-b", time.Minute); err != nil || ok || claimed != nil {
		t.Fatalf("Claim expired = (%v, %v, %v), want (nil, false, nil)", claimed, ok, err)
	}
	newLease, ok, err := mgr.Acquire(ctx, "group:stream", "shard-1", "owner-b", time.Minute)
	if err != nil || !ok || newLease == nil {
		t.Fatalf("reacquire expired = (%v, %v, %v), want (lease, true, nil)", newLease, ok, err)
	}
	if err := oldLease.Release(ctx); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("old Release = %v, want ErrNotOwned", err)
	}
	if owners, err := mgr.List(ctx, "group:stream"); err != nil || owners["shard-1"] != "owner-b" {
		t.Fatalf("List after reacquire = (%v, %v), want shard-1 owner-b", owners, err)
	}
}

func TestManagerTargetedMutationsRepairExpirationOnlyEntry(t *testing.T) {
	t.Run("acquire", func(t *testing.T) {
		mgr, server := newTestManager(t)
		lease, keys := makeExpirationOnlyLease(t, mgr, server)

		acquired, ok, err := mgr.Acquire(context.Background(), "group:stream", "shard-1", "owner-b", time.Minute)
		if err != nil || !ok || acquired == nil {
			t.Fatalf("Acquire over expiration-only entry = (%v, %v, %v), want (lease, true, nil)", acquired, ok, err)
		}
		if got := server.HGet(keys.LeaseOwners, "shard-1"); got != "owner-b" {
			t.Fatalf("repaired owner = %q, want owner-b", got)
		}
		if err := lease.Release(context.Background()); !errors.Is(err, consumerlease.ErrNotOwned) {
			t.Fatalf("orphaned old Release = %v, want ErrNotOwned", err)
		}
	})

	t.Run("claim", func(t *testing.T) {
		mgr, server := newTestManager(t)
		_, keys := makeExpirationOnlyLease(t, mgr, server)

		claimed, ok, err := mgr.Claim(context.Background(), "group:stream", "shard-1", "owner-a", "owner-b", time.Minute)
		if err != nil || ok || claimed != nil {
			t.Fatalf("Claim expiration-only entry = (%v, %v, %v), want (nil, false, nil)", claimed, ok, err)
		}
		if zsetContains(server, keys.LeaseExpirations, "shard-1") {
			t.Fatal("Claim left expiration-only entry behind")
		}
	})

	t.Run("renew", func(t *testing.T) {
		mgr, server := newTestManager(t)
		lease, keys := makeExpirationOnlyLease(t, mgr, server)

		if err := lease.Renew(context.Background(), time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
			t.Fatalf("Renew expiration-only entry = %v, want ErrNotOwned", err)
		}
		if zsetContains(server, keys.LeaseExpirations, "shard-1") {
			t.Fatal("Renew left expiration-only entry behind")
		}
	})

	t.Run("release", func(t *testing.T) {
		mgr, server := newTestManager(t)
		lease, keys := makeExpirationOnlyLease(t, mgr, server)

		if err := lease.Release(context.Background()); !errors.Is(err, consumerlease.ErrNotOwned) {
			t.Fatalf("Release expiration-only entry = %v, want ErrNotOwned", err)
		}
		if zsetContains(server, keys.LeaseExpirations, "shard-1") {
			t.Fatal("Release left expiration-only entry behind")
		}
	})
}

func makeExpirationOnlyLease(
	t *testing.T,
	mgr *Manager,
	server *miniredis.Miniredis,
) (consumerlease.Lease, backend.CoordinationKeys) {
	t.Helper()
	lease, ok, err := mgr.Acquire(context.Background(), "group:stream", "shard-1", "owner-a", time.Minute)
	if err != nil || !ok || lease == nil {
		t.Fatalf("seed Acquire = (%v, %v, %v), want (lease, true, nil)", lease, ok, err)
	}
	keys := backend.LeaseCoordinationKeys("lease-test", "group:stream")
	server.HDel(keys.LeaseOwners, "shard-1")
	if !zsetContains(server, keys.LeaseExpirations, "shard-1") {
		t.Fatal("seed expiration-only entry is missing")
	}
	return lease, keys
}

type observedCommand struct {
	name string
	slot uint16
}

type observingClient struct {
	valkey.Client

	doCalls    atomic.Int64
	nodesCalls atomic.Int64
	mu         sync.Mutex
	commands   []observedCommand
}

func (c *observingClient) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	parts := cmd.Commands()
	name := ""
	if len(parts) > 0 {
		name = strings.ToUpper(parts[0])
	}
	c.mu.Lock()
	c.commands = append(c.commands, observedCommand{name: name, slot: cmd.Slot()})
	c.mu.Unlock()
	c.doCalls.Add(1)
	return c.Client.Do(ctx, cmd)
}

func (c *observingClient) Nodes() map[string]valkey.Client {
	c.nodesCalls.Add(1)
	return c.Client.Nodes()
}

func (c *observingClient) snapshotCommands() []observedCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.commands)
}

func observeManager(mgr *Manager) *observingClient {
	observer := &observingClient{Client: mgr.client}
	mgr.client = observer
	return observer
}

func TestManagerCommandsRouteOneCoordinationIdentityToOneClusterSlot(t *testing.T) {
	t.Parallel()

	// A prefix containing hash-tag delimiters must not steal the identity tag
	// or make the lease hash and expiration set cross slots.
	mgr, _ := newTestManager(t, WithKeyPrefix("lease{unsafe}prefix"))
	observer := observeManager(mgr)
	ctx := context.Background()
	identity := "group}:{other}:stream"

	leaseA, ok, err := mgr.Acquire(ctx, identity, "shard-1", "owner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	claimed, ok, err := mgr.Claim(ctx, identity, "shard-1", "owner-a", "owner-b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	if err := leaseA.Renew(ctx, time.Minute); !errors.Is(err, consumerlease.ErrNotOwned) {
		t.Fatalf("old Renew = %v, want ErrNotOwned", err)
	}
	if err := claimed.Renew(ctx, time.Minute); err != nil {
		t.Fatalf("claimed Renew: %v", err)
	}
	if _, err := mgr.List(ctx, identity); err != nil {
		t.Fatalf("List: %v", err)
	}
	if err := mgr.Heartbeat(ctx, identity, "owner-b", time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if _, err := mgr.Workers(ctx, identity); err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if err := claimed.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	commands := observer.snapshotCommands()
	if len(commands) == 0 {
		t.Fatal("no commands observed")
	}
	wantSlot := commands[0].slot
	for i, command := range commands {
		if command.name != "EVAL" && command.name != "EVALSHA" {
			t.Errorf("command %d = %q, want EVAL/EVALSHA (no SCAN/GET discovery)", i, command.name)
		}
		if command.slot != wantSlot {
			t.Errorf("command %d slot = %d, want shared coordination slot %d", i, command.slot, wantSlot)
		}
	}
	if got := observer.nodesCalls.Load(); got != 0 {
		t.Fatalf("Nodes calls = %d, want 0", got)
	}
}

func TestManagerSnapshotCommandCountIsIndependentOfDatabaseSize(t *testing.T) {
	for _, size := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("shards_and_workers_%d", size), func(t *testing.T) {
			mgr, server := newTestManager(t)
			anchor := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
			server.SetTime(anchor)
			seedSnapshotMembers(t, mgr, size)
			seedUnrelatedKeys(t, server, 2000)
			// Warm the snapshot scripts before observing: the first EVALSHA per
			// script pays a one-time NOSCRIPT→EVAL fallback that is not part of
			// the steady-state per-snapshot command count.
			if _, err := mgr.List(context.Background(), "target:stream"); err != nil {
				t.Fatalf("warm List: %v", err)
			}
			if _, err := mgr.Workers(context.Background(), "target:stream"); err != nil {
				t.Fatalf("warm Workers: %v", err)
			}
			observer := observeManager(mgr)

			assertSnapshotCommandCount(t, mgr, observer, size)
		})
	}
}

func BenchmarkManagerSnapshotScaling(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("shards_and_workers_%d", size), func(b *testing.B) {
			server, err := miniredis.Run()
			if err != nil {
				b.Fatalf("miniredis start: %v", err)
			}
			b.Cleanup(server.Close)
			server.SetTime(time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC))

			mgr, err := NewManager(server.Addr(), WithKeyPrefix("lease-benchmark"))
			if err != nil {
				b.Fatalf("NewManager: %v", err)
			}
			b.Cleanup(func() { _ = mgr.Close() })
			seedSnapshotMembers(b, mgr, size)
			seedUnrelatedKeys(b, server, 5000)
			observer := observeManager(mgr)

			b.ResetTimer()
			for range b.N {
				owners, err := mgr.List(context.Background(), "target:stream")
				if err != nil || len(owners) != size {
					b.Fatalf("List = (%d owners, %v), want (%d, nil)", len(owners), err, size)
				}
				workers, err := mgr.Workers(context.Background(), "target:stream")
				if err != nil || len(workers) != size {
					b.Fatalf("Workers = (%d workers, %v), want (%d, nil)", len(workers), err, size)
				}
			}
			b.StopTimer()

			wantCommands := int64(2 * b.N)
			if got := observer.doCalls.Load(); got != wantCommands {
				b.Fatalf("Valkey commands = %d, want %d", got, wantCommands)
			}
			if got := observer.nodesCalls.Load(); got != 0 {
				b.Fatalf("Nodes calls = %d, want 0", got)
			}
			b.ReportMetric(2, "valkey_commands/op")
		})
	}
}

type testOrBenchmark interface {
	Helper()
	Fatalf(string, ...any)
}

func seedSnapshotMembers(tb testOrBenchmark, mgr *Manager, size int) {
	tb.Helper()
	ctx := context.Background()
	for i := range size {
		shardID := fmt.Sprintf("shard-%04d", i)
		owner := fmt.Sprintf("owner-%04d", i)
		if _, ok, err := mgr.Acquire(ctx, "target:stream", shardID, owner, time.Hour); err != nil || !ok {
			tb.Fatalf("seed Acquire %d = (ok=%v, err=%v), want (true, nil)", i, ok, err)
		}
		if err := mgr.Heartbeat(ctx, "target:stream", owner, time.Hour); err != nil {
			tb.Fatalf("seed Heartbeat %d: %v", i, err)
		}
	}
}

func seedUnrelatedKeys(tb testOrBenchmark, server *miniredis.Miniredis, count int) {
	tb.Helper()
	for i := range count {
		if err := server.Set(fmt.Sprintf("unrelated:%06d", i), "noise"); err != nil {
			tb.Fatalf("seed unrelated key %d: %v", i, err)
		}
	}
}

func assertSnapshotCommandCount(t *testing.T, mgr *Manager, observer *observingClient, size int) {
	t.Helper()
	owners, err := mgr.List(context.Background(), "target:stream")
	if err != nil || len(owners) != size {
		t.Fatalf("List = (%d owners, %v), want (%d, nil)", len(owners), err, size)
	}
	workers, err := mgr.Workers(context.Background(), "target:stream")
	if err != nil || len(workers) != size {
		t.Fatalf("Workers = (%d workers, %v), want (%d, nil)", len(workers), err, size)
	}
	if got := observer.doCalls.Load(); got != 2 {
		t.Fatalf("snapshot Valkey commands = %d, want 2", got)
	}
	if got := observer.nodesCalls.Load(); got != 0 {
		t.Fatalf("snapshot Nodes calls = %d, want 0", got)
	}
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func zsetContains(server *miniredis.Miniredis, key, member string) bool {
	members, err := server.ZMembers(key)
	return err == nil && slices.Contains(members, member)
}
