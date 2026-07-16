//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// TestLiveClaimStopsOnlyTheDonorShard proves that a real Valkey Claim does not
// turn the donor's subsequent ErrNotOwned renewal into a run-wide failure.
// Consumer A's rebalance interval is intentionally much longer than the test,
// so it cannot shed: after A owns every shard, B can obtain its fair share only
// by claiming live leases from A. Both original Start calls must then remain
// live across many 100ms renew/rebalance periods and continue delivering.
func TestLiveClaimStopsOnlyTheDonorShard(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-live-claim")
	const shardCount = 4
	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-live-claim-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}

	collector := newAttributingCollector()
	consumerA := newClaimProbeConsumer(t, stream, client, store, collector.handlerFor("a"), time.Hour)
	errA, stopA := runObservedConsumer(t, consumerA)
	t.Cleanup(stopA)
	ownerA := waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	consumerB := newClaimProbeConsumer(t, stream, client, store, collector.handlerFor("b"), 100*time.Millisecond)
	errB, stopB := runObservedConsumer(t, consumerB)
	t.Cleanup(stopB)
	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)

	owners := waitForStableLeaseOwners(t, leaseManager, stream, shardCount, 2, time.Second, 45*time.Second)
	counts := leaseOwnerCounts(owners)
	if counts[ownerA] == 0 || counts[ownerA] == shardCount {
		t.Fatalf("claim path did not leave donor %q with a proper subset of shards: owners=%v counts=%v", ownerA, owners, counts)
	}

	// Fifteen heartbeat renewals and roughly fifteen B-side rebalance periods
	// follow the takeover. The pre-fix donor returned ErrNotOwned and its Start
	// call completed with an error during this window.
	assertStartsLiveFor(t, errA, errB, 1500*time.Millisecond)

	postClaim := makePayloads("live-claim-post", 40)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, postClaim)
	if missing := collector.waitForAll(postClaim, 60*time.Second); missing != nil {
		t.Fatalf("post-claim records were not all delivered; missing %d/%d: %v", len(missing), len(postClaim), missing)
	}
	if got := collector.waitForProcessedBy("a", postClaim, 1, 15*time.Second); got < 1 {
		t.Fatalf("donor consumer processed %d post-claim records, want at least 1", got)
	}
	if got := collector.waitForProcessedBy("b", postClaim, 1, 15*time.Second); got < 1 {
		t.Fatalf("claiming consumer processed %d post-claim records, want at least 1", got)
	}
	assertStartsLiveFor(t, errA, errB, 300*time.Millisecond)
}

func newClaimProbeConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	rebalanceInterval time.Duration,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(100*time.Millisecond, time.Second),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(rebalanceInterval, 0, 500*time.Millisecond, 2),
		consumer.WithHeartbeat(100*time.Millisecond, 2*time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create claim-probe consumer for %s: %v", stream, err)
	}
	return cons
}

func runObservedConsumer(t *testing.T, cons *consumer.Consumer) (<-chan error, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errCh <- cons.Start(ctx)
	}()

	var once sync.Once
	return errCh, func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Errorf("consumer Start did not return within 30s of cancel")
				return
			}
			select {
			case err := <-errCh:
				if err != nil && !isCanceled(err) {
					t.Errorf("consumer Start returned error: %v", err)
				}
			default:
			}
		})
	}
}

func assertStartsLiveFor(t *testing.T, errA, errB <-chan error, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case err := <-errA:
		t.Fatalf("donor Start returned during live-claim observation: %v", err)
	case err := <-errB:
		t.Fatalf("claiming Start returned during live-claim observation: %v", err)
	case <-timer.C:
	}
}
