# Fenced Valkey storage

The Valkey lease manager implements `lease.FencedLease`; its checkpoint store
implements `checkpoint.FencedStore`. Consumer integration is a later slice:
the consumer still calls legacy checkpoint methods. Use sessions directly to
exercise the fenced backend contracts.

```go
store, err := valkeycheckpoint.New(addr)
if err != nil {
    return err
}
defer store.Close()
manager, err := store.LeaseManager()
if err != nil {
    return err
}
defer manager.(interface{ Close() error }).Close()
held, acquired, err := manager.Acquire(ctx, identity, shard, owner, ttl)
if err != nil {
    return err
}
if !acquired {
    return lease.ErrNotOwned
}
session, err := store.Bind(ctx, identity, shard, held.(lease.FencedLease))
if err != nil {
    return err
}
position, err := session.Recovery(ctx)
// Check err before interpreting position.Kind and position.Sequence.
```

`Bind` requires the same endpoint string, database, cluster mode, lease prefix,
coordination identity, and shard. Endpoint aliases are conservatively rejected.
The store's provider constructs matching dependencies. Session scripts execute
through the lease's client, on the authoritative primary, sharing its local
invalidation lock. Neither client enables replica reads or client-side caching.
Close dependencies only after sessions and lease activity have stopped. Calls
queued behind another lease or session operation honor their own context
cancellation. A canceled queued call makes no changes, including a queued
`Release`; retry release with a live context when cleanup is still needed.

Every acquisition and transfer uses a fresh random 256-bit generation, including
same-owner transfers and reacquisitions. Validation, renewal, release, and session
operations check owner, generation, and server-clock expiry. Observed ownership
loss permanently invalidates a lease. Session invalidation is permanent for that
session and does not release or invalidate its lease. A command already sent may
finish despite cancellation or a lost response; retry initialization and progress
operations against the same persisted state.

`Initialize` atomically stores the first fetched decimal sequence and a registry
entry once. Concurrent callers and successors reuse it. Persist this inclusive
replay position before any first-page admission decision, including oversized
record rejection. A crash before this write can lose the first `LATEST`
observation window; no records may have been admitted then. `Save` advances a
checkpoint exclusively, removes the initial value, and changes the registry in
the same script. Decimal strings are compared without floating-point conversion;
older checkpoints are no-ops and completion is terminal.

Initial positions, checkpoints, registry entries, and completion markers have no
lease TTL. Release, cleanup, and expiry of all leases and workers leave them
intact. Recovery scripts validate required values, their types and lifetimes,
and progress regression observed by the session before any write. Missing,
invalid, or inconsistent metadata returns `checkpoint.ErrRecoveryState`. Halt
and reconcile; never fall back to `LATEST` or silently reinitialize. Losing both
values and every initialization record can look like a new namespace. Iterator
anchor verification and consumer error propagation belong to consumer integration.

## Durability assumptions

The strict contract requires authoritative-primary reads and writes, persistent
non-evicting coordination state, no rollback of acknowledged ownership or progress,
and no simultaneous authoritative primaries. A conservative profile is:

```conf
maxmemory-policy noeviction
appendonly yes
appendfsync always
```

Use controlled restart/failover that preserves acknowledged state. AOF with
`appendfsync always` synchronizes each write; these settings alone do not make
arbitrary replica promotion safe. See [Valkey persistence](https://valkey.io/topics/persistence/).

Scripts fence against current authoritative state. Replication is asynchronous;
acknowledged writes can be lost during failover even with `WAIT`. Neither scripts
nor `WAIT` establishes rollback-free ownership history. Automatic failover that
can restore stale generations or lose progress is outside the strict guarantee.
See [Valkey replication](https://valkey.io/topics/replication/).

Ordinary replay after an application crash differs from backend rollback. Halt on
observable regression or inconsistent recovery metadata. An unobserved historical
snapshot may restore an old live lease; a fresh process cannot detect an internally
consistent snapshot without external evidence. Random generations prevent reuse
on new acquisitions, but cannot repair restored old ownership. For known state
loss, stop consumers and reconcile before restarting. No failover coordinator or
automatic reset is provided. A “persisted checkpoint” means a backend-confirmed
write under these assumptions; memory storage is not process-crash durable.

## Layout and offline resets

The [v3 key table](configuration.md#key-scheme) colocates ownership,
progress, and the separate per-identity registry in one cluster slot. Constructors
scan all nodes for older keys under configured prefixes and return
`ErrIncompatibleLayout` without deleting state. Startup therefore requires SCAN
permission and enough `WithPingTimeout` budget to inspect the database. Runtime
lease snapshots still operate only on their identity's aggregate keys. Stop all
old consumers before opening v3 clients; concurrent mixed-version writers and
migration tooling are unsupported. Do not copy old values into v3 keys without
reconciling the registry and recovery semantics.

For a disposable development/test environment, stop all consumers, inspect the
exact keys under the checkpoint and lease prefixes on every primary, and delete
only those reviewed keys one at a time with `UNLINK`. Include historical raw/v2
keys and old worker prefixes if present. Recreating a dedicated disposable
container and its volume also resets the namespace. Do not reset shared or
production state to bypass a recovery error. For a single disposable v3 shard,
`Store.Delete(ctx, identity, shard)` removes checkpoint, initial value, and its
registry entry atomically while consumers are stopped. Legacy `Save` and `Delete`
are unfenced administrative methods; do not mix them with live fenced sessions.

## Validation

`make valkey-integration` creates three Valkey 8 cluster primaries, exercises
fenced scripts across identities (including empty and brace-containing identities),
and rejects old keys in a cluster. A separate server uses the profile above;
a forced SIGKILL and restart verifies initial, checkpoint, and completed recovery
from AOF after ownership is released. Containers and volumes are isolated and
removed by the tests. This validates controlled restart, not replica promotion.
Unit and race tests cover transfers, permanent invalidation, initialization races,
metadata expiry/loss/corruption, observed regression, and the unsupported
unobserved historical-lease case. `make integration` covers existing consumer
behavior with LocalStack and Valkey.
