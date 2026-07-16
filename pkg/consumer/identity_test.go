package consumer

import (
	"context"
	"testing"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func TestConsumerGroupIdentityIsolatesMemoryCoordination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := checkpoint.NewMemoryStore()
	manager := lease.NewMemoryManager()

	groupA := newIdentityTestConsumer(t, store, manager, "group-a")
	groupB := newIdentityTestConsumer(t, store, manager, "group-b")
	groupAPeer := newIdentityTestConsumer(t, store, manager, "group-a")

	if got := groupA.coordinationKey(); got != "group-a:orders" {
		t.Fatalf("group A coordinationKey() = %q, want %q", got, "group-a:orders")
	}
	if got := groupB.coordinationKey(); got != "group-b:orders" {
		t.Fatalf("group B coordinationKey() = %q, want %q", got, "group-b:orders")
	}
	if got := groupAPeer.coordinationKey(); got != groupA.coordinationKey() {
		t.Fatalf("same-group coordination keys differ: %q != %q", got, groupA.coordinationKey())
	}

	if err := groupA.saveShardCheckpoint(ctx, "shard-1", "10"); err != nil {
		t.Fatalf("save group A checkpoint: %v", err)
	}
	if err := groupB.saveShardCheckpoint(ctx, "shard-1", "20"); err != nil {
		t.Fatalf("save group B checkpoint: %v", err)
	}
	if got, err := groupAPeer.readShardCheckpoint(ctx, "shard-1"); err != nil || got != "10" {
		t.Fatalf("same-group checkpoint = (%q, %v), want (10, nil)", got, err)
	}
	if got, err := groupB.readShardCheckpoint(ctx, "shard-1"); err != nil || got != "20" {
		t.Fatalf("different-group checkpoint = (%q, %v), want (20, nil)", got, err)
	}

	leaseA, acquired, err := groupA.acquireShardLease(ctx, "shard-1")
	if err != nil || !acquired || leaseA == nil {
		t.Fatalf("group A Acquire = (%v, %v, %v), want non-nil, true, nil", leaseA, acquired, err)
	}
	leaseB, acquired, err := groupB.acquireShardLease(ctx, "shard-1")
	if err != nil || !acquired || leaseB == nil {
		t.Fatalf("group B Acquire = (%v, %v, %v), want non-nil, true, nil", leaseB, acquired, err)
	}
	if peerLease, peerAcquired, err := groupAPeer.acquireShardLease(ctx, "shard-1"); err != nil || peerAcquired || peerLease != nil {
		t.Fatalf("same-group peer Acquire = (%v, %v, %v), want nil, false, nil", peerLease, peerAcquired, err)
	}

	if owners, err := manager.List(ctx, "group-a:orders"); err != nil || len(owners) != 1 || owners["shard-1"] != groupA.leaseOwner {
		t.Fatalf("group A owners = (%v, %v), want only %q", owners, err, groupA.leaseOwner)
	}
	if owners, err := manager.List(ctx, "group-b:orders"); err != nil || len(owners) != 1 || owners["shard-1"] != groupB.leaseOwner {
		t.Fatalf("group B owners = (%v, %v), want only %q", owners, err, groupB.leaseOwner)
	}

	if err := leaseA.Release(ctx); err != nil {
		t.Fatalf("release group A lease: %v", err)
	}
	if err := leaseB.Release(ctx); err != nil {
		t.Fatalf("release group B lease: %v", err)
	}
}

func newIdentityTestConsumer(t *testing.T, store checkpoint.Store, manager lease.Manager, group string) *Consumer {
	t.Helper()
	c, err := New(
		Config{StreamName: "orders", ConsumerGroup: group},
		&fakeKinesisClient{},
		store,
		func(context.Context, Record) error { return nil },
		WithLeaseManager(manager),
	)
	if err != nil {
		t.Fatalf("New(%s): %v", group, err)
	}
	return c
}
