//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestRestartOverReleasedLeaseResumesFromCheckpoint proves scenario #27 (IT-24):
// the clean stop -> restart operator path. Consumer A runs, processes all of
// batch1, and is stopped normally; a fresh consumer B then starts on the SAME
// store/stream and makes progress despite the coordination state A left behind.
//
// NOVEL / LOAD-BEARING (the lease half — no prior test asserts this): after A's
// clean stop, its shard leases are RELEASED (leaseManager.List is empty) while its
// checkpoint is still PRESENT. This is the mechanism by which leftover coordination
// state does not block a restart: the released-lease keys are immediately
// re-acquirable by a new owner, and the checkpoint is there to resume from. The
// lease read goes through the derived lease prefix (<prefix>-lease), so it is NOT
// polluted by checkpoint keys (the opposite of the bare-prefix gotcha in IT-22/IT-23).
//
// The release is unconditional at worker shutdown (shard_worker.go:82, independent
// of graceful drain) and runs before Start returns; runConsumer's stop() cancels
// AND joins until Start returns, so by the time stop() returns the releases and the
// checkpoint flush have completed — the post-stop assertions are deterministic, no
// timing margin to tune.
//
// CONFIRMATION (demoted, not the headline — this half overlaps IT-3/IT-5): B resumes
// over the leftover checkpoint, replaying 0 of batch1 and delivering batch2, with no
// record lost across the restart. B gets a different owner token than A (a realistic
// restart-on-a-new-host), so B acquiring A's released-lease key is a genuine
// fresh-owner Acquire, not a same-owner no-op.
//
// DISTINCT from: IT-5 (mid-stream drain), IT-5b (live concurrent successor), IT-6
// (planted stale lease reclaimed after TTL), IT-7 (crash residue). IT-24 is the
// sequential full-stop -> fresh-start path, and the post-stop "lease released +
// checkpoint present" state check is unique to it.
//
// Verified by two orthogonal mutations (each reverted):
//   - M-store (fresh store for B) -> replay(batch1) != 0 -> the resume assertion
//     fires; leases untouched.
//   - M-lease (suppress the release at shard_worker.go:82) -> A's lease lingers ->
//     the post-stop List-empty assertion fires; replay stays 0 (release is
//     orthogonal to checkpointing). NOTE: M-lease proves the release MECHANISM, not
//     a B-is-blocked counterfactual (that timing path is IT-7).
func TestRestartOverReleasedLeaseResumesFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-restart")
	const shardCount = 2
	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, uniqueName("kcg-it-restart-kp"))
	t.Cleanup(func() { _ = store.Close() })
	leaseManager := newLeaseManager(t, store)

	hashKeys := shardHashKeys(ctx, t, client, stream)
	batch1 := makePayloads("restart-a", 30)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, batch1)

	// Consumer A: clean run with auto-provided leasing (only the store, no
	// WithLeaseManager) and default graceful drain.
	collA := newCollector()
	consumerA := newConsumer(t, stream, client, store, collA.handler())
	_, stopA := runConsumer(t, consumerA)
	defer stopA() // idempotent (sync.Once); explicit call below does the real work.

	// Process ALL of batch1, then stop A fully.
	if missing := collA.waitFor(batch1, 90*time.Second); missing != nil {
		t.Fatalf("consumer A did not deliver all of batch1; missing %d/%d: %v", len(missing), len(batch1), missing)
	}
	stopA() // cancels and JOINS until Start returns -> releases + checkpoint flush done.

	// LOAD-BEARING: A released every lease on its clean stop (no leftover lease can
	// block B). Read through the derived lease prefix so checkpoint keys don't leak in.
	if owners, err := leaseManager.List(ctx, stream); err != nil || len(owners) != 0 {
		t.Fatalf("consumer A did not release its leases on clean stop: List=%v err=%v; want empty", owners, err)
	}

	// GUARD (non-vacuous): a resumable checkpoint IS left behind for every shard, so
	// the released-lease assertion above is a real restart precondition, not a
	// "nothing happened" state.
	for _, shard := range listShardIDs(ctx, t, client, stream) {
		v, err := store.Get(ctx, stream, shard)
		if err != nil || v == "" {
			t.Fatalf("no leftover checkpoint for shard %s after A's stop: Get=%q err=%v", shard, v, err)
		}
	}

	// New work produced strictly after A stopped.
	batch2 := makePayloads("restart-b", 20)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, batch2)

	// Fresh consumer B on the SAME store/stream (different owner token).
	collB := newCollector()
	consumerB := newConsumer(t, stream, client, store, collB.handler())
	_, stopB := runConsumer(t, consumerB)
	defer stopB()

	// CONFIRMATION: B makes progress (delivers the new work) over the leftover state.
	if missing := collB.waitFor(batch2, 90*time.Second); missing != nil {
		t.Fatalf("restarted consumer B did not deliver batch2; missing %d/%d: %v", len(missing), len(batch2), missing)
	}
	// replay==0: B resumed from the leftover checkpoint rather than reprocessing
	// A's already-checkpointed records.
	var replayed []string
	for _, p := range batch1 {
		if collB.count(p) != 0 {
			replayed = append(replayed, p)
		}
	}
	if len(replayed) != 0 {
		t.Fatalf("consumer B replayed %d of batch1 over the leftover checkpoint: %v", len(replayed), replayed)
	}

	// Completeness: no record lost across the restart.
	allPayloads := append(append([]string(nil), batch1...), batch2...)
	if missing := waitForUnion(collA, collB, allPayloads, 30*time.Second); missing != nil {
		t.Fatalf("records lost across restart; missing %d/%d: %v", len(missing), len(allPayloads), missing)
	}
}
