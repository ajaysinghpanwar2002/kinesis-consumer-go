//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

// TestRebalanceMaxMovesPerTick proves scenario #24 (IT-21): maxMoves=1 is a
// per-rebalance-pass movement budget, so rebalancing from a known 4/0 start to
// the fair 2/2 split happens gradually rather than all at once.
//
// This deliberately exercises the clean local-SHED path. A starts first and owns
// all 4 shards. B joins as an active worker but has a long rebalance interval, so
// it does not claim from live A. A's rebalance ticks are therefore responsible for
// moving shards away: with 4 shards / 2 workers, high=2, but maxMoves=1 means A
// can shed only one shard in the first pass. B then acquires that unowned shard
// via shard sync, producing a required intermediate 3/1 split. A later sheds one
// more shard on a later tick, and B acquires it, reaching 2/2.
//
// The load-bearing assertion is that the test must observe and hold the 3/1
// split before 2/2 is allowed. If the shed path ignored maxMoves and dropped both
// excess shards in one pass, B's shard sync would acquire both and the test would
// reach 2/2 before observing 3/1.
func TestRebalanceMaxMovesPerTick(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-maxmoves")
	const shardCount = 4
	rebalanceInterval := 3 * time.Second
	cooldown := 500 * time.Millisecond

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-maxmoves-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("want %d shards with hash keys, got %d: %v", shardCount, len(hashKeys), hashKeys)
	}
	initialPayloads := makePayloads("maxmoves-initial", 40)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, initialPayloads)

	coll := newCollector()

	consumerA := newMaxMovesProbeConsumer(t, stream, client, store, coll.handler(), rebalanceInterval, 30*time.Second, cooldown, 1)
	_, stopA := runConsumer(t, consumerA)
	defer stopA()

	ownerA := waitForSingleLeaseOwner(t, leaseManager, stream, shardCount, 45*time.Second)

	// B uses a long rebalance interval to keep it off the live-claim path. Its
	// normal shard-sync interval lets it pick up A's shed, unowned shards between
	// A's widely-spaced rebalance ticks.
	consumerB := newMaxMovesProbeConsumer(t, stream, client, store, coll.handler(), 30*time.Second, time.Second, cooldown, 1)
	_, stopB := runConsumer(t, consumerB)
	defer stopB()

	waitForLeaseWorkers(t, leaseManager, stream, 2, 15*time.Second)

	intermediate := waitForThreeOneSplitBeforeTwoTwo(t, leaseManager, stream, ownerA, shardCount, 20*time.Second)
	assertOwnerSplitFor(t, leaseManager, stream, ownerA, shardCount, 3, 1, time.Second)
	t.Logf("max-moves intermediate split observed: ownerA=%s owners=%v", ownerA, intermediate)

	finalOwners := waitForOwnerSplit(t, leaseManager, stream, ownerA, shardCount, 2, 2, 30*time.Second)
	assertOwnerSplitFor(t, leaseManager, stream, ownerA, shardCount, 2, 2, time.Second)
	t.Logf("max-moves final split observed: ownerA=%s owners=%v", ownerA, finalOwners)

	// Produce after convergence so the final 2/2 ownership also handles fresh work.
	handoffPayloads := makePayloads("maxmoves-handoff", 40)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, handoffPayloads)

	allPayloads := append(append([]string(nil), initialPayloads...), handoffPayloads...)
	if missing := coll.waitFor(allPayloads, 90*time.Second); missing != nil {
		t.Fatalf("consumers did not deliver all records at least once; missing %d/%d: %v", len(missing), len(allPayloads), missing)
	}
}

func newMaxMovesProbeConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	rebalanceInterval time.Duration,
	shardSyncInterval time.Duration,
	cooldown time.Duration,
	maxMoves int,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, shardSyncInterval),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(rebalanceInterval, 0, cooldown, maxMoves),
		consumer.WithHeartbeat(100*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create max-moves probe consumer for %s: %v", stream, err)
	}
	return cons
}

func waitForThreeOneSplitBeforeTwoTwo(
	t *testing.T,
	manager lease.Manager,
	stream string,
	ownerA string,
	shardCount int,
	timeout time.Duration,
) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]string
	var lastErr error
	for {
		owners, err := manager.List(context.Background(), stream)
		if err == nil {
			last = owners
			if len(owners) == shardCount {
				counts := leaseOwnerCounts(owners)
				if ownerSplitMatches(counts, ownerA, 3, 1) {
					return owners
				}
				if ownerSplitMatches(counts, ownerA, 2, 2) {
					t.Fatalf("stream %s reached final 2/2 split before the required 3/1 intermediate split; owners=%v counts=%v", stream, owners, counts)
				}
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not reach 3/1 before 2/2 within %s; last=%v lastErr=%v", stream, timeout, last, lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForOwnerSplit(
	t *testing.T,
	manager lease.Manager,
	stream string,
	ownerA string,
	shardCount int,
	wantA int,
	wantOther int,
	timeout time.Duration,
) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]string
	var lastErr error
	for {
		owners, err := manager.List(context.Background(), stream)
		if err == nil {
			last = owners
			if len(owners) == shardCount {
				counts := leaseOwnerCounts(owners)
				if ownerSplitMatches(counts, ownerA, wantA, wantOther) {
					return owners
				}
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not reach split A=%d other=%d within %s; last=%v lastErr=%v", stream, wantA, wantOther, timeout, last, lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func assertOwnerSplitFor(
	t *testing.T,
	manager lease.Manager,
	stream string,
	ownerA string,
	shardCount int,
	wantA int,
	wantOther int,
	stableFor time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(stableFor)
	samples := 0
	for time.Now().Before(deadline) {
		owners, err := manager.List(context.Background(), stream)
		if err != nil {
			t.Fatalf("list owners while checking split stability: %v", err)
		}
		counts := leaseOwnerCounts(owners)
		if len(owners) != shardCount || !ownerSplitMatches(counts, ownerA, wantA, wantOther) {
			t.Fatalf("stream %s split changed during %s window: want A=%d other=%d, owners=%v counts=%v", stream, stableFor, wantA, wantOther, owners, counts)
		}
		samples++
		time.Sleep(50 * time.Millisecond)
	}
	if samples < 2 {
		t.Fatalf("sampled stream %s split only %d times during %s", stream, samples, stableFor)
	}
}

func ownerSplitMatches(counts map[string]int, ownerA string, wantA int, wantOther int) bool {
	if counts[ownerA] != wantA || len(counts) != 2 {
		return false
	}
	for owner, count := range counts {
		if owner != ownerA {
			return count == wantOther
		}
	}
	return false
}
