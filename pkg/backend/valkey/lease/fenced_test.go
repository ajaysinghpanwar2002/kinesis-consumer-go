package lease

import (
	"context"
	"errors"
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
