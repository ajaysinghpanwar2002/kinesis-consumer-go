//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

// crashLeaseManager wraps a real lease.Manager but hands out leases whose
// Release is a no-op. It simulates an ungraceful crash at the coordination
// layer: a worker that dies never releases its lease, so the lease key lingers
// in Valkey until its TTL lapses. Renew still delegates to the real lease, so
// the lease stays alive (renewed) for as long as the consumer keeps running —
// exactly like a healthy worker — and only becomes stale once the consumer
// stops renewing (the "crash").
//
// This reproduces the full crash RESIDUE the failover mechanism observes: a
// lingering lease key (expiring on its last-renew TTL), a worker heartbeat key
// (expiring on its own TTL because the heartbeat loop stops), and a checkpoint
// frozen at the last committed record. It does NOT exercise OS-level process
// death (goroutines unwind cleanly rather than dying mid-instruction), which is
// invisible to a coordination-layer failover test.
type crashLeaseManager struct {
	lease.Manager
}

func (m crashLeaseManager) Acquire(ctx context.Context, streamName, shardID, owner string, ttl time.Duration) (lease.Lease, bool, error) {
	l, ok, err := m.Manager.Acquire(ctx, streamName, shardID, owner, ttl)
	if l != nil {
		l = noReleaseLease{l}
	}
	return l, ok, err
}

func (m crashLeaseManager) Claim(ctx context.Context, streamName, shardID, expectedOwner, newOwner string, ttl time.Duration) (lease.Lease, bool, error) {
	l, ok, err := m.Manager.Claim(ctx, streamName, shardID, expectedOwner, newOwner, ttl)
	if l != nil {
		l = noReleaseLease{l}
	}
	return l, ok, err
}

// noReleaseLease delegates everything (notably Renew) to the wrapped lease but
// swallows Release, so a "crashed" worker never deletes its lease key.
type noReleaseLease struct {
	lease.Lease
}

func (noReleaseLease) Release(context.Context) error { return nil }

// TestUngracefulCrashFailoverResumesFromCheckpoint proves scenario #5: when a
// worker crashes mid-stream (leaving its lease unreleased), a fresh successor
// reclaims the shard only after the stale lease lapses and RESUMES from the
// crashed worker's checkpoint — re-delivering only the in-flight, uncheckpointed
// page (bounded replay), losing nothing.
//
// The headline is no-data-loss across the crash boundary (completeness + bounded
// replay), which is what distinguishes this from IT-6 (reclaim timing with a
// synthetic lease): here A did real work and B picks up exactly after A's real
// checkpoint. See crashLeaseManager for how the crash residue is simulated
// without OS-level process death or any library change.
func TestUngracefulCrashFailoverResumesFromCheckpoint(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("crash")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}
	shardID := shardIDs[0]

	const (
		total           = 60
		batchSize       = 10
		crashAfter      = 13 // wait for ordinals 0..12: page 1 done + into page 2
		slowPerRecord   = 25 * time.Millisecond
		unionBudget     = 30 * time.Second
		establishBudget = 30 * time.Second
	)
	payloads := makePayloads("batch", total)
	ordinalOf := make(map[string]int, total)
	for i, p := range payloads {
		ordinalOf[p] = i
	}
	putRecords(ctx, t, client, stream, payloads)

	store := newStore(t, uniqueName("crash-kp"))

	// Consumer A: processes slowly and, via the crash wrapper, never releases its
	// lease. A short heartbeat TTL (3s) makes the lingering lease lapse quickly
	// after the crash so B can reclaim within the test budget. No graceful drain,
	// so cancelling A is an ungraceful stop (no drain flush) — the only records A
	// commits are the periodic per-page checkpoints (checkpointEvery=1 saves at
	// each completed page's last record).
	crashMgr := crashLeaseManager{newLeaseManager(t, store)}
	collA := newCollector()
	slowA := collA.handler()
	handlerA := func(ctx context.Context, rec consumer.Record) error {
		time.Sleep(slowPerRecord)
		return slowA(ctx, rec)
	}
	consA, err := consumer.New(consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}, client, store, handlerA,
		consumer.WithBatching(batchSize, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithHeartbeat(200*time.Millisecond, 3*time.Second),
		consumer.WithLeaseManager(crashMgr),
	)
	if err != nil {
		t.Fatalf("create consumer A: %v", err)
	}

	cancelA, stopA := runConsumer(t, consA)
	defer stopA() // harmless if already stopped (runConsumer guards with sync.Once)

	// Let A establish ownership and process a prefix spanning >1 page so it has a
	// real checkpoint to resume from.
	if missing := collA.waitFor(payloads[:crashAfter], establishBudget); len(missing) != 0 {
		t.Fatalf("consumer A did not deliver the first %d records before crash: missing %v", crashAfter, missing)
	}

	// Capture A's lease owner token so we can prove the SAME lease survives the
	// crash (not released, not yet reclaimed).
	before, err := crashMgr.List(ctx, stream)
	if err != nil {
		t.Fatalf("List before crash: %v", err)
	}
	aOwner := before[shardID]
	if aOwner == "" {
		t.Fatalf("consumer A does not own shard %s before crash; owners=%v", shardID, before)
	}

	// Crash A: cancel and wait for Start to return. With the no-op-Release wrapper
	// this leaves the lease key in place.
	cancelA()
	stopA()

	// PRECONDITION (fail-fast, not the load-bearing check): the crashed worker's
	// lease is still present under the SAME owner token — it was not released.
	after, err := crashMgr.List(ctx, stream)
	if err != nil {
		t.Fatalf("List after crash: %v", err)
	}
	if after[shardID] != aOwner {
		t.Fatalf("after crash, shard %s owner = %q, want %q still present (a crashed worker must not release its lease)", shardID, after[shardID], aOwner)
	}

	// Consumer B: a fresh successor using the real (store-provided) lease manager.
	// It cannot acquire the shard until A's stale lease lapses (~3s), then resumes
	// from A's checkpoint.
	collB := newCollector()
	consB := newConsumer(t, stream, client, store, collB.handler())
	_, stopB := runConsumer(t, consB)
	defer stopB()

	// Every record must be delivered by A or B (no loss across the crash).
	if missing := waitForUnion(collA, collB, payloads, unionBudget); len(missing) != 0 {
		t.Fatalf("records lost across the crash: %d/%d never delivered by A or B: %v", len(missing), total, missing)
	}

	aOrds := deliveredOrdinals(collA, payloads, ordinalOf)
	bOrds := deliveredOrdinals(collB, payloads, ordinalOf)
	if len(aOrds) == 0 {
		t.Fatal("consumer A delivered nothing")
	}
	if len(bOrds) == 0 {
		t.Fatal("consumer B delivered nothing; it never reclaimed the shard")
	}

	fa := aOrds[len(aOrds)-1]
	if !equalOrdinals(aOrds, rangeOrdinals(0, fa+1)) {
		t.Fatalf("consumer A delivered non-contiguous ordinals %s, want a contiguous prefix 0..%d", summarizeOrdinals(aOrds), fa)
	}
	if fa >= total-1 {
		t.Fatalf("consumer A delivered the whole stream (fa=%d); not a mid-stream crash — slow the handler or add records", fa)
	}
	if fa < batchSize {
		t.Fatalf("consumer A crashed before completing a page (fa=%d < batchSize=%d); no real checkpoint to resume from", fa, batchSize)
	}

	lastB := bOrds[len(bOrds)-1]
	if lastB != total-1 {
		t.Fatalf("consumer B last delivered ordinal = %d, want %d (the tip)", lastB, total-1)
	}
	firstB := bOrds[0]
	if !equalOrdinals(bOrds, rangeOrdinals(firstB, total)) {
		t.Fatalf("consumer B delivered non-contiguous ordinals %s, want a contiguous run %d..%d", summarizeOrdinals(bOrds), firstB, total-1)
	}

	// LOAD-BEARING: B resumed from A's checkpoint, so it re-delivers at most the
	// in-flight (uncheckpointed) page. If B had restarted from TRIM_HORIZON it
	// would re-deliver A's entire prefix (replay == fa+1 > batchSize).
	replay := intersectionCount(aOrds, bOrds)
	if replay > batchSize {
		t.Fatalf("replay across crash = %d ordinals, want <= one page (%d); B did not resume from A's checkpoint (restarted from the beginning?). A=%s B=%s",
			replay, batchSize, summarizeOrdinals(aOrds), summarizeOrdinals(bOrds))
	}
	t.Logf("crash/resume ok: A delivered 0..%d, B delivered %d..%d, replay=%d (<=batchSize=%d)", fa, firstB, lastB, replay, batchSize)
}

// waitForUnion blocks until every payload has been delivered by collA or collB
// at least once, or the timeout elapses, returning the still-missing payloads.
func waitForUnion(collA, collB *collector, payloads []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		var missing []string
		for _, p := range payloads {
			if collA.count(p) == 0 && collB.count(p) == 0 {
				missing = append(missing, p)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return missing
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// intersectionCount returns how many ordinals appear in both sorted slices.
func intersectionCount(a, b []int) int {
	seen := make(map[int]struct{}, len(a))
	for _, v := range a {
		seen[v] = struct{}{}
	}
	n := 0
	for _, v := range b {
		if _, ok := seen[v]; ok {
			n++
		}
	}
	return n
}
