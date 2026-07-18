package consumer

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// shardParentage remembers which shards have at least one parent present in
// the known shard listing. A checkpoint-less shard with a known parent is a
// reshard child being picked up for the first time: its iterator must anchor
// at TRIM_HORIZON so processing continues exactly where the completed parents
// left off, not at the configured StartPosition (default StartLatest, which
// would anchor at the child's tip at worker-start time and silently drop
// every record written between the reshard and pickup).
//
// Entries are monotonic: parents are created before their children and a
// shard's parent IDs never change, so once a shard is listed alongside one of
// its parents that fact stays true. Shards whose parent IDs reference shards
// aged out of retention (absent from every listing this consumer saw) are
// treated as parentless and keep StartPosition semantics.
//
// Written from the orchestration paths (the initial listing at Start and the
// shard-sync refresh); read concurrently by shard workers deriving iterators.
type shardParentage struct {
	mu         sync.RWMutex
	hasParents map[string]struct{}
}

// record updates parent knowledge from a merged shard listing (shard ID ->
// shard). A shard is marked only when one of its parents is itself present in
// the same listing.
func (p *shardParentage) record(shards map[string]types.Shard) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for shardID, shard := range shards {
		if shardID == "" {
			continue
		}
		if _, ok := p.hasParents[shardID]; ok {
			continue
		}
		for _, parentID := range shardParents(shard) {
			if _, ok := shards[parentID]; !ok {
				continue
			}
			if p.hasParents == nil {
				p.hasParents = make(map[string]struct{})
			}
			p.hasParents[shardID] = struct{}{}
			break
		}
	}
}

// hasKnownParents reports whether the shard has ever been listed with at
// least one of its parents present in the same known-shard map.
func (p *shardParentage) hasKnownParents(shardID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.hasParents[shardID]
	return ok
}
