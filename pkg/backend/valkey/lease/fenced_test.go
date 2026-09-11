package lease

import (
	"context"
	"errors"
	valkey "github.com/valkey-io/valkey-go"
	"slices"
	"sync"
	"testing"
	"time"

	core "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func TestFencedLeaseNeverRevivesAfterObservedLoss(t *testing.T) {
	for _, loss := range []string{"invalidate", "expiry", "transfer", "release"} {
		t.Run(loss, func(t *testing.T) {
			m, server := newTestManager(t)
			ctx := context.Background()
			held, ok, err := m.Acquire(ctx, "stream", "shard", "owner", time.Minute)
			if err != nil || !ok {
				t.Fatal(err)
			}
			l := held.(core.FencedLease)
			keys := m.keys("stream")
			oldExpiry, err := server.ZScore(keys.LeaseExpirations, "shard")
			if err != nil {
				t.Fatal(err)
			}
			switch loss {
			case "invalidate":
				l.Invalidate()
			case "expiry":
				server.FastForward(2 * time.Minute)
			case "transfer":
				if _, ok, err = m.Claim(ctx, "stream", "shard", "owner", "owner", time.Minute); err != nil || !ok {
					t.Fatal(err)
				}
			case "release":
				if err = l.Release(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err = l.Validate(ctx); !errors.Is(err, core.ErrNotOwned) {
				t.Fatalf("loss %v", err)
			}
			// Restore an old, internally consistent live lease. Local invalidation must win.
			server.HSet(keys.LeaseOwners, "shard", "owner")
			server.HSet(keys.LeaseGenerations, "shard", l.Generation())
			server.ZAdd(keys.LeaseExpirations, oldExpiry, "shard")
			for _, op := range []func() error{func() error { return l.Validate(ctx) }, func() error { return l.Renew(ctx, time.Minute) }, func() error { return l.Release(ctx) }} {
				if err := op(); !errors.Is(err, core.ErrNotOwned) {
					t.Fatalf("revived %v", err)
				}
			}
		})
	}
}

func TestUnobservedHistoricalLeaseIsOutsideGuarantee(t *testing.T) {
	m, server := newTestManager(t)
	ctx := context.Background()
	held, _, err := m.Acquire(ctx, "stream", "shard", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	old := held.(core.FencedLease)
	keys := m.keys("stream")
	replacement, ok, err := m.Claim(ctx, "stream", "shard", "owner", "next", time.Minute)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if replacement.(core.FencedLease).Generation() == old.Generation() {
		t.Fatal("generation reused")
	}
	server.HSet(keys.LeaseOwners, "shard", "owner")
	server.HSet(keys.LeaseGenerations, "shard", old.Generation())
	// No operation observed the transfer: current authoritative state cannot reveal rollback.
	if err = old.Validate(ctx); err != nil {
		t.Fatalf("documented historical snapshot case: %v", err)
	}
	old.Invalidate()
}

// blockBindingClient delays an actual Binding.Exec command while it owns the
// mutation mutex. Validation uses the same client and lease, not a fake lease.
type blockBindingClient struct {
	valkey.Client
	entered chan struct{}
	unblock chan struct{}
}

func (c *blockBindingClient) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	if slices.Contains(cmd.Commands(), "blocked-checkpoint") {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		select {
		case <-c.unblock:
		case <-ctx.Done():
		}
	}
	return c.Client.Do(ctx, cmd)
}

func TestValidationIndependentOfBlockedBinding(t *testing.T) {
	for _, lose := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "lost"}[lose], func(t *testing.T) {
			m, server := newTestManager(t)
			ctx := context.Background()
			held, _, err := m.Acquire(ctx, "stream", "shard", "owner", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			l := held.(*valkeyLease)
			client := &blockBindingClient{Client: l.client, entered: make(chan struct{}, 1), unblock: make(chan struct{})}
			l.client = client
			var unblock sync.Once
			defer unblock.Do(func() { close(client.unblock) })
			script := valkey.NewLuaScript(OwnershipScript + "return {'ok'}")
			saved := make(chan error, 1)
			go func() {
				_, err := (&Binding{held: l}).Exec(ctx, script, nil, []string{"blocked-checkpoint"})
				saved <- err
			}()
			<-client.entered
			keys := m.keys("stream")
			if lose {
				server.HSet(keys.LeaseGenerations, "shard", "different-generation")
			}
			validationCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			err = l.Validate(validationCtx)
			if lose {
				if !errors.Is(err, core.ErrNotOwned) {
					t.Fatalf("ownership loss blocked by checkpoint: %v", err)
				}
				server.HSet(keys.LeaseGenerations, "shard", l.Generation())
				if err := l.Validate(validationCtx); !errors.Is(err, core.ErrNotOwned) {
					t.Fatalf("revived: %v", err)
				}
			} else if err != nil {
				t.Fatalf("validation blocked by checkpoint: %v", err)
			}
			// Allow both EVALSHA and its initial NOSCRIPT fallback to finish.
			unblock.Do(func() { close(client.unblock) })
			if err := <-saved; lose && !errors.Is(err, core.ErrNotOwned) {
				t.Fatalf("late binding success revived acquisition: %v", err)
			} else if !lose && err != nil {
				t.Fatal(err)
			}
		})
	}
}
