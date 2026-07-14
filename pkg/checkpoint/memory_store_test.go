package checkpoint

import (
	"context"
	"strconv"
	"sync"
	"testing"
)

func TestMemoryStoreGetSaveDelete(t *testing.T) {
	tests := []struct {
		name string
		// setup mutates a fresh store before the assertion.
		setup func(t *testing.T, s *MemoryStore)
		// stream/shard identify the checkpoint under test.
		stream string
		shard  string
		want   string
	}{
		{
			name:   "missing key returns empty",
			setup:  func(*testing.T, *MemoryStore) {},
			stream: "stream-a",
			shard:  "shard-1",
			want:   "",
		},
		{
			name: "save then get",
			setup: func(t *testing.T, s *MemoryStore) {
				if err := s.Save(context.Background(), "stream-a", "shard-1", "seq-1"); err != nil {
					t.Fatalf("Save: %v", err)
				}
			},
			stream: "stream-a",
			shard:  "shard-1",
			want:   "seq-1",
		},
		{
			name: "overwrite keeps latest",
			setup: func(t *testing.T, s *MemoryStore) {
				ctx := context.Background()
				if err := s.Save(ctx, "stream-a", "shard-1", "seq-1"); err != nil {
					t.Fatalf("Save seq-1: %v", err)
				}
				if err := s.Save(ctx, "stream-a", "shard-1", "seq-2"); err != nil {
					t.Fatalf("Save seq-2: %v", err)
				}
			},
			stream: "stream-a",
			shard:  "shard-1",
			want:   "seq-2",
		},
		{
			name: "save shard-end marker verbatim",
			setup: func(t *testing.T, s *MemoryStore) {
				if err := s.Save(context.Background(), "stream-a", "shard-1", "SHARD_END:seq-9"); err != nil {
					t.Fatalf("Save: %v", err)
				}
			},
			stream: "stream-a",
			shard:  "shard-1",
			want:   "SHARD_END:seq-9",
		},
		{
			name: "delete removes checkpoint",
			setup: func(t *testing.T, s *MemoryStore) {
				ctx := context.Background()
				if err := s.Save(ctx, "stream-a", "shard-1", "seq-1"); err != nil {
					t.Fatalf("Save: %v", err)
				}
				if err := s.Delete(ctx, "stream-a", "shard-1"); err != nil {
					t.Fatalf("Delete: %v", err)
				}
			},
			stream: "stream-a",
			shard:  "shard-1",
			want:   "",
		},
		{
			name: "delete missing key is a no-op",
			setup: func(t *testing.T, s *MemoryStore) {
				if err := s.Delete(context.Background(), "stream-a", "shard-1"); err != nil {
					t.Fatalf("Delete: %v", err)
				}
			},
			stream: "stream-a",
			shard:  "shard-1",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStore()
			tt.setup(t, s)
			got, err := s.Get(context.Background(), tt.stream, tt.shard)
			if err != nil {
				t.Fatalf("Get: unexpected error %v", err)
			}
			if got != tt.want {
				t.Fatalf("Get = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMemoryStoreSaveIsAdvanceOnly mirrors the Valkey store's advance-only
// matrix so the two implementations of the Store contract cannot drift.
func TestMemoryStoreSaveIsAdvanceOnly(t *testing.T) {
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
			ctx := context.Background()
			s := NewMemoryStore()
			for i, seq := range tt.saves {
				if err := s.Save(ctx, "stream-a", "shard-1", seq); err != nil {
					t.Fatalf("Save #%d (%q): %v", i+1, seq, err)
				}
			}
			got, err := s.Get(ctx, "stream-a", "shard-1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Get = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMemoryStoreDeleteAllowsRewind(t *testing.T) {
	// Delete is the documented rewind path: a missing key makes the next
	// save unconditional even for a lower sequence.
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.Save(ctx, "stream-a", "shard-1", "500"); err != nil {
		t.Fatalf("Save 500: %v", err)
	}
	if err := s.Delete(ctx, "stream-a", "shard-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Save(ctx, "stream-a", "shard-1", "300"); err != nil {
		t.Fatalf("Save 300 after delete: %v", err)
	}
	if got, err := s.Get(ctx, "stream-a", "shard-1"); err != nil || got != "300" {
		t.Fatalf("Get = (%q, %v), want (\"300\", nil)", got, err)
	}
}

func TestMemoryStoreKeyIsolation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.Save(ctx, "stream-a", "shard-1", "a1"); err != nil {
		t.Fatalf("Save a1: %v", err)
	}
	if err := s.Save(ctx, "stream-b", "shard-1", "b1"); err != nil {
		t.Fatalf("Save b1: %v", err)
	}
	if err := s.Save(ctx, "stream-a", "shard-2", "a2"); err != nil {
		t.Fatalf("Save a2: %v", err)
	}

	// Deleting one key must not disturb the others.
	if err := s.Delete(ctx, "stream-a", "shard-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	cases := []struct {
		stream string
		shard  string
		want   string
	}{
		{"stream-a", "shard-1", ""},   // deleted
		{"stream-b", "shard-1", "b1"}, // different stream, same shard
		{"stream-a", "shard-2", "a2"}, // same stream, different shard
	}
	for _, c := range cases {
		got, err := s.Get(ctx, c.stream, c.shard)
		if err != nil {
			t.Fatalf("Get %s/%s: %v", c.stream, c.shard, err)
		}
		if got != c.want {
			t.Fatalf("Get %s/%s = %q, want %q", c.stream, c.shard, got, c.want)
		}
	}
}

// TestMemoryStoreConcurrentAccess is meaningful under the race detector:
// run with `go test -race ./pkg/checkpoint/`.
func TestMemoryStoreConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	const goroutines = 16
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			shard := "shard-" + strconv.Itoa(g)
			for i := 0; i < iterations; i++ {
				_ = s.Save(ctx, "stream-a", shard, strconv.Itoa(i))
				_, _ = s.Get(ctx, "stream-a", shard)
				if i%2 == 0 {
					_ = s.Delete(ctx, "stream-a", shard)
				}
			}
		}(g)
	}
	wg.Wait()
}
