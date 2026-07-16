//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func TestConsumerGroupsIsolateAndShareCoordination(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()
	stream := uniqueName("consumer-groups")
	const shardCount = 2
	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	payloads := makePayloads("consumer-groups", 30)
	putRecordsAcrossShards(ctx, t, client, stream, shardHashKeys(ctx, t, client, stream), payloads)

	store := newStore(t, uniqueName("consumer-groups-kp"))
	t.Cleanup(func() { _ = store.Close() })
	manager := newLeaseManager(t, store)

	const (
		groupA      = "orders-blue"
		groupB      = "orders-green"
		sharedGroup = "orders-shared"
	)

	collectorA := newCollector()
	collectorB := newCollector()
	consumerA := newConsumerForGroup(t, stream, groupA, client, store, collectorA.handler())
	consumerB := newConsumerForGroup(t, stream, groupB, client, store, collectorB.handler())
	_, stopA := runConsumer(t, consumerA)
	_, stopB := runConsumer(t, consumerB)

	if missing := collectorA.waitFor(payloads, 90*time.Second); len(missing) != 0 {
		stopA()
		stopB()
		t.Fatalf("group A did not independently consume the stream; missing %d/%d: %v", len(missing), len(payloads), missing)
	}
	if missing := collectorB.waitFor(payloads, 90*time.Second); len(missing) != 0 {
		stopA()
		stopB()
		t.Fatalf("group B did not independently consume the stream; missing %d/%d: %v", len(missing), len(payloads), missing)
	}

	assertGroupCoordinationState(t, ctx, store, manager, groupA, stream, shardCount, 1)
	assertGroupCoordinationState(t, ctx, store, manager, groupB, stream, shardCount, 1)
	if owners, err := manager.List(ctx, coordinationIdentity("orders-unused", stream)); err != nil || len(owners) != 0 {
		stopA()
		stopB()
		t.Fatalf("unused group leaked leases: owners=%v err=%v", owners, err)
	}

	stopA()
	stopB()

	sharedCollector := newAttributingCollector()
	sharedA := newConsumerForGroup(t, stream, sharedGroup, client, store, sharedCollector.handlerFor("A"))
	sharedB := newConsumerForGroup(t, stream, sharedGroup, client, store, sharedCollector.handlerFor("B"))
	_, stopSharedA := runConsumer(t, sharedA)
	defer stopSharedA()
	_, stopSharedB := runConsumer(t, sharedB)
	defer stopSharedB()

	if missing := sharedCollector.waitForAll(payloads, 90*time.Second); len(missing) != 0 {
		t.Fatalf("same-group workers did not collectively consume the stream; missing %d/%d: %v", len(missing), len(payloads), missing)
	}
	waitForGroupDistribution(t, manager, sharedGroup, stream, shardCount, 2, 60*time.Second)
	assertGroupCoordinationState(t, ctx, store, manager, sharedGroup, stream, shardCount, 2)
}

func newConsumerForGroup(
	t *testing.T,
	stream string,
	group string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
) *consumer.Consumer {
	t.Helper()
	c, err := consumer.New(
		consumer.Config{StreamName: stream, ConsumerGroup: group, StartPosition: consumer.StartTrimHorizon},
		client,
		store,
		handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(200*time.Millisecond, 0, 500*time.Millisecond, 2),
		consumer.WithHeartbeat(100*time.Millisecond, 2*time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create consumer for %s/%s: %v", group, stream, err)
	}
	return c
}

func assertGroupCoordinationState(
	t *testing.T,
	ctx context.Context,
	store *valkeycheckpoint.Store,
	manager lease.Manager,
	group string,
	stream string,
	shardCount int,
	wantWorkers int,
) {
	t.Helper()
	identity := coordinationIdentity(group, stream)
	owners, err := manager.List(ctx, identity)
	if err != nil || len(owners) != shardCount {
		t.Fatalf("group %s owners = (%v, %v), want %d live shard leases", group, owners, err, shardCount)
	}
	workers, err := manager.Workers(ctx, identity)
	if err != nil || len(workers) != wantWorkers {
		t.Fatalf("group %s workers = (%v, %v), want %d", group, workers, err, wantWorkers)
	}
	for _, shardID := range listShardIDs(ctx, t, newKinesisClient(), stream) {
		deadline := time.Now().Add(30 * time.Second)
		var checkpoint string
		var err error
		for time.Now().Before(deadline) {
			checkpoint, err = store.Get(ctx, identity, shardID)
			if err == nil && checkpoint != "" {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil || checkpoint == "" {
			t.Fatalf("group %s checkpoint for %s = (%q, %v), want non-empty", group, shardID, checkpoint, err)
		}
	}
}

func waitForGroupDistribution(t *testing.T, manager lease.Manager, group, stream string, shardCount, wantOwners int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	identity := coordinationIdentity(group, stream)
	var last map[string]string
	for time.Now().Before(deadline) {
		owners, err := manager.List(context.Background(), identity)
		if err == nil {
			last = owners
			unique := make(map[string]struct{})
			for _, owner := range owners {
				unique[owner] = struct{}{}
			}
			if len(owners) == shardCount && len(unique) >= wantOwners {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("group %s did not distribute %d shards across %d owners within %s; last=%v", group, shardCount, wantOwners, timeout, last)
}
