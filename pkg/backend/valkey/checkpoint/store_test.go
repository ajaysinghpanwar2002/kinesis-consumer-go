package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pratilipi/kinesis-consumer-go/internal/backend"
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
