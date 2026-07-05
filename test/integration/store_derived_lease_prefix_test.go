//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	valkeylease "github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey/lease"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

// TestStoreDerivedLeasePrefix proves scenario #26 (IT-23): a consumer given ONLY
// the Valkey store (no WithLeaseManager) acquires shard leases automatically
// through the live leaseSettings -> lease.Provider -> store.LeaseManager() path,
// and those leases land under the LITERAL derived prefix "<checkpointPrefix>-lease".
//
// WHY THIS IS NOT REDUNDANT (the one thing that justifies the slice):
//   - The unit test TestDefaultLeasePrefix checks the derivation FORMULA in
//     isolation; it never exercises the consumer's live auto-provisioning path.
//   - Every rebalance test and IT-22 read leases through store.LeaseManager(),
//     which RE-DERIVES the prefix through the same DefaultLeasePrefix — so a broken
//     derivation would still self-agree (writer and reader move together) and pass.
//
// The only probe that catches a derivation drift is one that independently encodes
// the "-lease" contract as a HARDCODED literal and checks it end-to-end against
// real Valkey. Hence the probe below is a fresh manager built with an explicit
// prefix, NOT store.LeaseManager().
//
// POSITIVE-ONLY by design. There is no clean negative: a probe at the bare
// checkpoint prefix "<checkpointPrefix>" would falsely match the CHECKPOINT keys
// (checkpoints are "<prefix>:<stream>:<shard>", exactly what the "<prefix>:<stream>:*"
// lease scan matches), and a nonexistent-prefix probe is vacuous. The negative is
// logically subsumed by the positive under the single-derivation coupling.
//
// Scope note: this slice's value is deliberately thin — it proves the derivation is
// wired through the live Provider path, nothing more.
//
// Mutation-verified: changing the library DefaultLeasePrefix suffix ("-lease" ->
// "-leasex") relocates the consumer's leases to "<prefix>-leasex:*"; delivery still
// succeeds (leasing works, keys just moved), but the "<prefix>-lease" probe below
// never converges and the derived-prefix wait fails.
func TestStoreDerivedLeasePrefix(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-derive")
	const shardCount = 2
	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	checkpointPrefix := uniqueName("kcg-it-derive-kp")
	store := newStore(t, checkpointPrefix)
	t.Cleanup(func() { _ = store.Close() })

	hashKeys := shardHashKeys(ctx, t, client, stream)
	payloads := makePayloads("derive", 20)
	putRecordsAcrossShards(ctx, t, client, stream, hashKeys, payloads)

	// Consumer with ONLY the store -> leasing is auto-provided from the store
	// (leaseSettings falls back to store.(lease.Provider).LeaseManager()).
	coll := newCollector()
	consumer := newConsumer(t, stream, client, store, coll.handler())
	_, stop := runConsumer(t, consumer)
	defer stop()

	// Guard: the consumer actually ran (and therefore actually leased) via the auto
	// path. Delivery is the observable proof the Provider-derived manager is live.
	if missing := coll.waitFor(payloads, 90*time.Second); missing != nil {
		t.Fatalf("consumer did not deliver all records; missing %d/%d: %v", len(missing), len(payloads), missing)
	}

	// LOAD-BEARING: an independent manager at the hardcoded literal derived prefix
	// sees all shard leases owned by one owner. If the auto path used any prefix
	// other than "<checkpointPrefix>-lease", this never converges.
	derived := newLeaseManagerAtPrefix(t, checkpointPrefix+"-lease")
	owner := waitForSingleLeaseOwner(t, derived, stream, shardCount, 15*time.Second)
	t.Logf("auto-provided leases found under %q-lease, owned by %s", checkpointPrefix, owner)
}

// newLeaseManagerAtPrefix builds a standalone Valkey lease manager at an explicit
// key prefix, independent of any checkpoint store's derivation, and registers its
// Close.
func newLeaseManagerAtPrefix(t *testing.T, prefix string) lease.Manager {
	t.Helper()
	manager, err := valkeylease.NewManager(valkeyAddr(), valkeylease.WithKeyPrefix(prefix))
	if err != nil {
		t.Fatalf("create lease manager at prefix %q: %v", prefix, err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}
