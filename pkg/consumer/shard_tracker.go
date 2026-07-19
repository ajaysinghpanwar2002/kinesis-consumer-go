package consumer

import (
	"context"
	"fmt"
	"sync"
)

type shardCompletionState struct {
	mu        sync.RWMutex
	completed map[string]struct{}
}

func newShardCompletionState() *shardCompletionState {
	return &shardCompletionState{
		completed: make(map[string]struct{}),
	}
}

func (s *shardCompletionState) markCompleted(shardID string) {
	if shardID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed[shardID] = struct{}{}
}

// prune drops cached completion entries whose shard the retain predicate no
// longer wants (see pruneStaleShardState). The cache is re-derivable from the
// checkpoint store, so pruning can never lose the completion fact — only the
// memo of it.
func (s *shardCompletionState) prune(retain func(shardID string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for shardID := range s.completed {
		if !retain(shardID) {
			delete(s.completed, shardID)
		}
	}
}

func (s *shardCompletionState) isCompleted(shardID string) bool {
	if shardID == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.completed[shardID]
	return ok
}

func (s *shardCompletionState) shardCompleted(ctx context.Context, c *Consumer, shardID string) (bool, error) {
	if shardID == "" {
		return false, nil
	}
	if s.isCompleted(shardID) {
		return true, nil
	}

	seq, err := c.readShardCheckpoint(ctx, shardID)
	if err != nil {
		return false, fmt.Errorf("check shard completion %s: %w", shardID, err)
	}
	if !isShardCompletedCheckpoint(seq) {
		return false, nil
	}

	s.markCompleted(shardID)
	return true, nil
}
