//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// TestStaleLeaseReclaimedAfterTTLExpiry proves the uncooperative-failover
// invariant for scenario #6: a live consumer takes over a shard held by an
// inactive worker only AFTER that worker's stale lease lapses, and never steals
// a still-valid lease.
//
// A crashed worker leaves exactly this state in Valkey: a lease key that is
// still valid until its TTL expires, with NO worker heartbeat renewing it. We
// plant that state directly (via the lease manager's Acquire, which writes the
// same SET NX PX key a real worker would and no heartbeat), then start a live
// consumer B.
//
// FINDING (verified against source): the library reclaims a stale lease via
// LEASE-KEY TTL EXPIRY, not via heartbeat expiry. Once the key lapses the shard
// becomes unowned and B acquires it on its ~1s shardSync tick. Heartbeat state
// only sizes fair-share rebalancing; with a single shard and a single live
// worker high==open==1, so a dead owner's within-share lease is never a claim
// donor (count 1 is not > high 1) — takeover is provably via Acquire-after-
// expiry, never Claim. The mid-window snapshot below asserts the dead owner is
// absent from Workers() (no live heartbeat -> "considered inactive") while its
// lease is still present, tying both halves of the scenario together.
func TestStaleLeaseReclaimedAfterTTLExpiry(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("stale-lease")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}
	shardID := shardIDs[0]

	payloads := makePayloads("batch", 30)
	putRecords(ctx, t, client, stream, payloads)

	store := newStore(t, uniqueName("stale-lease-kp"))
	mgr := newLeaseManager(t, store)

	// Timing knobs. midWindow is safely below staleTTL so the snapshot lands
	// while the stale lease is definitely still present; minReclaimWait is a
	// margin below staleTTL so a genuine early theft (B ignoring ownership)
	// trips the assertion while ordinary scheduling slack does not.
	const (
		staleTTL       = 6 * time.Second
		midWindow      = 2500 * time.Millisecond
		minReclaimWait = 5 * time.Second
		deliverBudget  = 25 * time.Second
		deadOwner      = "dead-owner"
	)

	// Plant the stale lease. plantedAt anchors the reclaim timeline; the key
	// expires at approximately plantedAt+staleTTL. The returned lease is
	// intentionally discarded — we never Renew/Release it, so it lapses on its
	// own like an abandoned worker's lease.
	plantedAt := time.Now()
	if _, ok, err := mgr.Acquire(ctx, stream, shardID, deadOwner, staleTTL); err != nil || !ok {
		t.Fatalf("plant stale lease: ok=%v err=%v, want ok true", ok, err)
	}

	base := newCollector()
	deliver := base.handler()
	var (
		mu        sync.Mutex
		firstSeen time.Time
	)
	handler := func(ctx context.Context, rec consumer.Record) error {
		mu.Lock()
		if firstSeen.IsZero() {
			firstSeen = time.Now()
		}
		mu.Unlock()
		return deliver(ctx, rec)
	}

	cons := newConsumer(t, stream, client, store, handler)
	_, stop := runConsumer(t, cons)
	defer stop()

	// Mid-window snapshot (below the TTL): the stale lease is still owned by the
	// dead worker, the dead worker has no heartbeat (absent from Workers ->
	// "considered inactive"), and B has delivered nothing. Together these prove
	// the silence is caused by the still-valid stale lease within this run, not
	// merely slow startup.
	time.Sleep(midWindow)

	owners, err := mgr.List(ctx, stream)
	if err != nil {
		t.Fatalf("mid-window List: %v", err)
	}
	if owners[shardID] != deadOwner {
		t.Fatalf("mid-window lease owner for %s = %q, want %q (stale lease should still be present)", shardID, owners[shardID], deadOwner)
	}

	workers, err := mgr.Workers(ctx, stream)
	if err != nil {
		t.Fatalf("mid-window Workers: %v", err)
	}
	for _, w := range workers {
		if w == deadOwner {
			t.Fatalf("dead worker %q present in Workers %v; it has no heartbeat and must be considered inactive", deadOwner, workers)
		}
	}

	mu.Lock()
	early := firstSeen
	mu.Unlock()
	if !early.IsZero() {
		t.Fatalf("consumer delivered a record %s after start, before the stale lease expired; it must not steal a valid lease", early.Sub(plantedAt))
	}

	// After the stale lease lapses, B acquires the now-unowned shard on its next
	// shardSync tick and delivers every record.
	if missing := base.waitFor(payloads, deliverBudget); len(missing) != 0 {
		t.Fatalf("after stale lease expiry, %d/%d payloads never delivered: %v", len(missing), len(payloads), missing)
	}

	mu.Lock()
	got := firstSeen
	mu.Unlock()
	if got.IsZero() {
		t.Fatal("first delivery time not recorded despite completeness")
	}
	if waited := got.Sub(plantedAt); waited < minReclaimWait {
		t.Fatalf("first delivery %s after planting the stale lease, want >= %s (reclaim must wait for the lease to lapse)", waited, minReclaimWait)
	}
}
