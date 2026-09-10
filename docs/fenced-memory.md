# Fenced memory storage

`lease.FencedLease` and `checkpoint.FencedStore` are optional capabilities for
binding progress to a single lease acquisition. This foundation implements them
in memory. Consumer activation and Valkey support follow in later slices.
Existing consumer execution still uses the legacy store interface.

Construct matching dependencies explicitly:

```go
manager := lease.NewMemoryManager()
store := checkpoint.NewMemoryStoreWithLeaseManager(manager)
held, acquired, err := manager.Acquire(ctx, stream, shard, owner, ttl)
if err != nil {
    return err
}
if !acquired {
    return lease.ErrNotOwned
}
session, err := store.Bind(ctx, stream, shard, held.(lease.FencedLease))
if err != nil {
    return err
}
position, err := session.Recovery(ctx)
// Check err, then use position.Kind and position.Sequence for recovery.
```

`Bind` rejects a different manager, stream, or shard. `NewMemoryStore` retains
legacy use; it has no matching lease manager and rejects binding. Legacy `Save`
and `Delete` are unfenced administrative operations; do not mix them with active
fenced sessions. To reset a disposable namespace, stop consumers and use `Delete`,
which removes the checkpoint, initial position, and registry entry together.

Each acquisition and transfer creates a cryptographically random 256-bit token,
including transfers to the same owner. Validation and fenced reads/writes check
owner, generation, and backend expiry while holding the manager's ownership lock.
Lease invalidation is permanent; session invalidation is also permanent but does
not invalidate other sessions or release the lease. Canceled operations do not
write recovery state. An operation already inside its atomic critical section
may complete while cancellation arrives.

`Recovery` returns one of four states:

| Kind | Value and continuation |
| --- | --- |
| Fresh | No initialization evidence; use the configured start position. |
| Initial | First fetched sequence; replay inclusively. |
| Checkpoint | Saved sequence; resume exclusively after it. |
| Completed | Terminal `SHARD_END` or `SHARD_END:<sequence>` marker. |

Call `Initialize` before admitting any first-page record, including rejecting an
oversized record. It atomically records an inclusive position and its registry
entry once. Concurrent calls and successors reuse existing recovery. A crash
before initialization can lose the first `LATEST` observation window; no record
may have been admitted yet. `Save` atomically supersedes the initial position with
a checkpoint and updates the registry. Older checkpoints are no-ops and completion
is terminal. Sequence numbers remain decimal strings without leading zeros.

Lease release, expiry, worker expiry, and cleanup leave recovery metadata intact.
The registry separately identifies which value is required. Missing or invalid
required metadata, inconsistent values, and regression observed by a session
return `checkpoint.ErrRecoveryState`. Halt on that error; do not fall back to
`LATEST` or silently initialize again. Iterator anchor verification is a later
consumer integration requirement.

Memory storage is for tests and local development; all state disappears with the
process. Losing both recovery values and every initialization record can look like
a fresh namespace to a new session. Random generations prevent token reuse on new
acquisitions, but cannot make backend rollback safe: an unobserved historical
snapshot can restore an old live lease. A locally invalidated handle stays invalid,
but a fresh process cannot always detect a consistent historical snapshot. The
strict backend contract requires no rollback of acknowledged ownership/progress
history and no simultaneous authoritative primaries.
