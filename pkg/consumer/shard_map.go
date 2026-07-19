package consumer

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func mergeKnownShards(dst map[string]types.Shard, shards []types.Shard) {
	for _, shard := range shards {
		shardID := shardIDValue(shard)
		if shardID == "" {
			continue
		}
		dst[shardID] = shard
	}
}

// refreshKnownShards merges a fresh listing into known and returns the set of
// shard IDs the listing actually contained, so the caller can prune state for
// shards the stream no longer reports.
func (c *Consumer) refreshKnownShards(ctx context.Context, known map[string]types.Shard) (map[string]struct{}, error) {
	shards, err := c.listShards(ctx)
	if err != nil {
		return nil, err
	}
	mergeKnownShards(known, shards)
	// Record parentage even when the caller later discards this refresh's
	// candidate map on failure: parent links are immutable facts from the
	// listing, so keeping them early is always safe.
	c.parentage.record(known)

	listed := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		if shardID := shardIDValue(shard); shardID != "" {
			listed[shardID] = struct{}{}
		}
	}
	return listed, nil
}

// pruneStaleShardState drops consumer-local state for shards that have aged
// out of the stream: Kinesis lists every shard — open or closed — until its
// retention expires, so a shard absent from a successful full listing is gone
// for good and every listing that follows agrees. Without pruning, a
// long-lived consumer of a stream that reshards accumulates entries in the
// known-shard map, the completion cache, the rebalance cooldown map, and the
// parentage set forever — and an aged-out parent that was never checkpointed
// as completed (its records expired before any consumer finished it) would
// stay in the known-shard map and gate its children's readiness forever.
//
// A shard with a still-registered worker is exempt even when unlisted: its
// entries stay visible to shed/stop bookkeeping until the worker has fully
// finished, and the next listing prunes them.
//
// Completion-cache entries are additionally retained while any known shard
// still names the completed shard as a parent: readiness consults the parent's
// completion every pass, and dropping the cached entry early would re-read the
// parent's checkpoint from the store on every pass until the child ages out
// too.
func pruneStaleShardState(
	known map[string]types.Shard,
	listed map[string]struct{},
	completionState *shardCompletionState,
	cooldown map[string]time.Time,
	parentage *shardParentage,
	workers *shardWorkerSet,
) {
	// An empty listing is never treated as pruning authority: a stream that
	// exists always lists at least one shard (Start refuses empty streams, and
	// a deleted stream surfaces as a listing error, not an empty page), so an
	// empty result says something is degenerate about the source, and mass-
	// deleting all local state on its word is needless risk.
	if len(listed) == 0 {
		return
	}

	retained := func(shardID string) bool {
		if _, ok := listed[shardID]; ok {
			return true
		}
		return workers != nil && workers.has(shardID)
	}

	for shardID := range known {
		if !retained(shardID) {
			delete(known, shardID)
		}
	}
	for shardID := range cooldown {
		if !retained(shardID) {
			delete(cooldown, shardID)
		}
	}

	referencedParents := make(map[string]struct{})
	for _, shard := range known {
		for _, parentID := range shardParents(shard) {
			referencedParents[parentID] = struct{}{}
		}
	}
	if completionState != nil {
		completionState.prune(func(shardID string) bool {
			if retained(shardID) {
				return true
			}
			_, ok := referencedParents[shardID]
			return ok
		})
	}
	if parentage != nil {
		parentage.prune(retained)
	}
}
