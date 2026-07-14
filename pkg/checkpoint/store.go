package checkpoint

import "context"

// CompletedPrefix marks a checkpoint value as a shard-completion record.
// The consumer writes completion checkpoints as "SHARD_END" or
// "SHARD_END:<finalSequenceNumber>"; any value with this prefix is terminal.
const CompletedPrefix = "SHARD_END"

// Store persists per-shard consumer progress.
//
// Save is advance-only: an implementation must apply a save only when it
// advances the checkpoint, and silently discard (return nil) one that would
// regress it. In order:
//
//   - no existing value → write
//   - equal to the current value → no-op success (idempotent)
//   - current value has CompletedPrefix → terminal, never overwritten
//   - new value has CompletedPrefix → write (completion always advances)
//   - otherwise compare as Kinesis sequence numbers: unsigned decimal
//     strings without leading zeros, so longer is greater and equal lengths
//     compare lexicographically (the numbers exceed 53-bit float precision —
//     numeric parsing is not an option)
//
// Discarding stale writes silently — rather than erroring — matters because
// the losing writer is typically the donor side of a shard handoff whose
// final flush lands after the taker's first save; failing that save would
// turn routine handoffs into worker errors. Fencing a stale worker's
// processing is the lease layer's job; the checkpoint layer only guarantees
// committed progress is never lost.
//
// There is no in-band rewind: an intentional replay is performed by calling
// Delete (a missing key makes the next Save unconditional).
type Store interface {
	Get(ctx context.Context, streamName, shardID string) (string, error)
	Save(ctx context.Context, streamName, shardID, sequenceNumber string) error
	Delete(ctx context.Context, streamName, shardID string) error
}
