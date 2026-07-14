package checkpoint

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/internal/backend"
	consumerlease "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func newTestStore(t *testing.T, opts ...Option) (*Store, *miniredis.Miniredis) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(server.Close)

	store, err := New(server.Addr(), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return store, server
}

func TestStoreGetSaveDelete(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	ctx := context.Background()

	if got, err := store.Get(ctx, "stream", "shard-1"); err != nil || got != "" {
		t.Fatalf("Get missing = (%q, %v), want (\"\", nil)", got, err)
	}

	if err := store.Save(ctx, "stream", "shard-1", "seq-1"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got, err := store.Get(ctx, "stream", "shard-1"); err != nil || got != "seq-1" {
		t.Fatalf("Get after save = (%q, %v), want (\"seq-1\", nil)", got, err)
	}

	if err := store.Save(ctx, "stream", "shard-1", "SHARD_END:seq-9"); err != nil {
		t.Fatalf("Save overwrite: %v", err)
	}
	if got, err := store.Get(ctx, "stream", "shard-1"); err != nil || got != "SHARD_END:seq-9" {
		t.Fatalf("Get after overwrite = (%q, %v), want (\"SHARD_END:seq-9\", nil)", got, err)
	}

	if err := store.Delete(ctx, "stream", "shard-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got, err := store.Get(ctx, "stream", "shard-1"); err != nil || got != "" {
		t.Fatalf("Get after delete = (%q, %v), want (\"\", nil)", got, err)
	}
}

func TestStoreSaveIsAdvanceOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		saves []string
		want  string
	}{
		{
			// Lexicographically "99" > "100"; only length-first numeric
			// ordering keeps the later, larger sequence.
			name:  "shorter numeric regression discarded",
			saves: []string{"100", "99"},
			want:  "100",
		},
		{
			name:  "equal-length regression discarded",
			saves: []string{"101", "100"},
			want:  "101",
		},
		{
			name:  "longer numeric advance persists",
			saves: []string{"99", "100"},
			want:  "100",
		},
		{
			name:  "equal-length advance persists",
			saves: []string{"100", "101"},
			want:  "101",
		},
		{
			name:  "same value is an idempotent no-op",
			saves: []string{"100", "100"},
			want:  "100",
		},
		{
			name:  "shard end never overwritten by a sequence",
			saves: []string{"SHARD_END:100", "200"},
			want:  "SHARD_END:100",
		},
		{
			name:  "shard end never overwritten by another completion",
			saves: []string{"SHARD_END:100", "SHARD_END:200"},
			want:  "SHARD_END:100",
		},
		{
			name:  "completion overwrites a plain sequence",
			saves: []string{"100", "SHARD_END:100"},
			want:  "SHARD_END:100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, _ := newTestStore(t)
			ctx := context.Background()
			for i, seq := range tt.saves {
				if err := store.Save(ctx, "stream", "shard-1", seq); err != nil {
					t.Fatalf("Save #%d (%q): %v", i+1, seq, err)
				}
			}
			if got, err := store.Get(ctx, "stream", "shard-1"); err != nil || got != tt.want {
				t.Fatalf("Get = (%q, %v), want (%q, nil)", got, err, tt.want)
			}
		})
	}
}

func TestStoreDeleteAllowsRewind(t *testing.T) {
	t.Parallel()

	// Delete is the documented rewind path: a missing key makes the next
	// save unconditional even for a lower sequence.
	store, _ := newTestStore(t)
	ctx := context.Background()

	if err := store.Save(ctx, "stream", "shard-1", "500"); err != nil {
		t.Fatalf("Save 500: %v", err)
	}
	if err := store.Delete(ctx, "stream", "shard-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.Save(ctx, "stream", "shard-1", "300"); err != nil {
		t.Fatalf("Save 300 after delete: %v", err)
	}
	if got, err := store.Get(ctx, "stream", "shard-1"); err != nil || got != "300" {
		t.Fatalf("Get = (%q, %v), want (\"300\", nil)", got, err)
	}
}

func TestStoreDeleteMissingIsNoOp(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	if err := store.Delete(context.Background(), "stream", "absent"); err != nil {
		t.Fatalf("Delete missing = %v, want nil", err)
	}
}

func TestStoreWithKeyPrefix(t *testing.T) {
	t.Parallel()

	store, server := newTestStore(t, WithKeyPrefix("custom-prefix"))
	if err := store.Save(context.Background(), "stream", "shard-1", "seq-1"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	key := backend.CheckpointKey("custom-prefix", "stream", "shard-1")
	if got, err := server.Get(key); err != nil || got != "seq-1" {
		t.Fatalf("server.Get(%q) = (%q, %v), want (\"seq-1\", nil)", key, got, err)
	}
}

func TestStoreWithDBIsolation(t *testing.T) {
	t.Parallel()

	store, server := newTestStore(t, WithDB(1))
	if err := store.Save(context.Background(), "stream", "shard-1", "seq-1"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	key := backend.CheckpointKey("kinesis-checkpoint", "stream", "shard-1")
	if _, err := server.DB(0).Get(key); err == nil {
		t.Fatalf("expected checkpoint absent from DB 0, but found it")
	}
	if got, err := server.DB(1).Get(key); err != nil || got != "seq-1" {
		t.Fatalf("DB(1).Get(%q) = (%q, %v), want (\"seq-1\", nil)", key, got, err)
	}
}

// leaseManagerFromProvider gets a lease manager through the lease.Provider
// interface so the test exercises the same path consumer.New uses, and closes
// the manager's separate client on cleanup.
func leaseManagerFromProvider(t *testing.T, store *Store) consumerlease.Manager {
	t.Helper()

	var provider consumerlease.Provider = store
	mgr, err := provider.LeaseManager()
	if err != nil {
		t.Fatalf("LeaseManager: %v", err)
	}
	t.Cleanup(func() {
		if closer, ok := mgr.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				t.Errorf("lease manager Close: %v", err)
			}
		}
	})
	return mgr
}

func TestStoreLeaseManagerDefaultPrefix(t *testing.T) {
	t.Parallel()

	store, server := newTestStore(t)
	mgr := leaseManagerFromProvider(t, store)

	ctx := context.Background()
	lease, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-1", time.Minute)
	if err != nil || !ok || lease == nil {
		t.Fatalf("Acquire = (%v, %v, %v), want (lease, true, nil)", lease, ok, err)
	}

	// The default checkpoint prefix maps to the shared standalone lease
	// default, so leases are written under "kinesis-lease" — the same
	// namespace a default standalone manager uses. Assert the raw key to
	// prove the store wired its lease prefix into the manager.
	key := backend.LeaseKey("kinesis-lease", "stream", "shard-1")
	if got, err := server.Get(key); err != nil || got != "owner-1" {
		t.Fatalf("server.Get(%q) = (%q, %v), want (\"owner-1\", nil)", key, got, err)
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := server.Get(key); err == nil {
		t.Fatalf("lease key %q still present after Release", key)
	}
}

func TestStoreLeaseManagerWithLeasePrefix(t *testing.T) {
	t.Parallel()

	store, server := newTestStore(t, WithLeasePrefix("custom-lease"))
	mgr := leaseManagerFromProvider(t, store)

	if _, ok, err := mgr.Acquire(context.Background(), "stream", "shard-1", "owner-1", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire = (ok=%v, err=%v), want (true, nil)", ok, err)
	}

	key := backend.LeaseKey("custom-lease", "stream", "shard-1")
	if got, err := server.Get(key); err != nil || got != "owner-1" {
		t.Fatalf("server.Get(%q) = (%q, %v), want (\"owner-1\", nil)", key, got, err)
	}

	// The default lease key must not be used when a prefix is set.
	derived := backend.LeaseKey("kinesis-lease", "stream", "shard-1")
	if _, err := server.Get(derived); err == nil {
		t.Fatalf("unexpected lease at default-derived key %q", derived)
	}
}

func TestStoreLeaseManagerNamespaceIndependentFromCheckpoints(t *testing.T) {
	t.Parallel()

	store, server := newTestStore(t)
	mgr := leaseManagerFromProvider(t, store)

	ctx := context.Background()
	if err := store.Save(ctx, "stream", "shard-1", "seq-1"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, ok, err := mgr.Acquire(ctx, "stream", "shard-1", "owner-1", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire = (ok=%v, err=%v), want (true, nil)", ok, err)
	}

	checkpointKey := backend.CheckpointKey("kinesis-checkpoint", "stream", "shard-1")
	if got, err := server.Get(checkpointKey); err != nil || got != "seq-1" {
		t.Fatalf("server.Get(%q) = (%q, %v), want (\"seq-1\", nil)", checkpointKey, got, err)
	}
	leaseKey := backend.LeaseKey("kinesis-lease", "stream", "shard-1")
	if got, err := server.Get(leaseKey); err != nil || got != "owner-1" {
		t.Fatalf("server.Get(%q) = (%q, %v), want (\"owner-1\", nil)", leaseKey, got, err)
	}
}

func TestNewRejectsEmptyAddr(t *testing.T) {
	t.Parallel()

	if _, err := New(""); err == nil {
		t.Fatal("New(\"\") error = nil, want error")
	}
}

func TestNewPingFailure(t *testing.T) {
	t.Parallel()

	// Reserve a port with miniredis, then close it so the address is
	// unreachable, forcing New's ping to fail promptly under a small timeout.
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	addr := server.Addr()
	server.Close()

	store, err := New(addr, WithPingTimeout(200*time.Millisecond))
	if err == nil {
		_ = store.Close()
		t.Fatal("New on unreachable addr error = nil, want error")
	}
}
