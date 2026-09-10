package checkpoint

import (
	"context"
	"fmt"
	"strings"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

var _ FencedStore = (*MemoryStore)(nil)

// NewMemoryStoreWithLeaseManager creates a store whose fenced sessions use
// manager's ownership lock. Pass this same manager to the consumer. The original
// NewMemoryStore constructor supports legacy operations only; Bind rejects it.
// Neither constructor provides process-crash durability.
func NewMemoryStoreWithLeaseManager(manager *lease.MemoryManager) *MemoryStore {
	m := NewMemoryStore()
	m.manager = manager
	return m
}

// Bind checks backend/shard identity and current ownership before binding.
func (m *MemoryStore) Bind(ctx context.Context, stream, shard string, held lease.FencedLease) (Session, error) {
	if m.manager == nil {
		return nil, lease.ErrLeaseMismatch
	}
	s := &memorySession{store: m, held: held, stream: stream, shard: shard}
	if _, err := s.Recovery(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

type memorySession struct {
	store         *MemoryStore
	held          lease.FencedLease
	stream, shard string
	invalid       bool // guarded by store.mu; when both locks are needed, ownership comes first
	observed      RecoveryPosition
}

func (s *memorySession) run(ctx context.Context, fn func(string) error) error {
	return s.store.manager.WithLease(ctx, s.held, s.stream, s.shard, func() error {
		s.store.mu.Lock()
		defer s.store.mu.Unlock()
		if s.invalid {
			return lease.ErrNotOwned
		}
		return fn(s.store.key(s.stream, s.shard))
	})
}

func (s *memorySession) Invalidate() {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.invalid = true
}

func validSequence(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func validCompletion(value string) bool {
	return value == CompletedPrefix || (strings.HasPrefix(value, CompletedPrefix+":") && validSequence(strings.TrimPrefix(value, CompletedPrefix+":")))
}

func (s *memorySession) recoveryLocked(key string) (RecoveryPosition, error) {
	m := s.store
	kind, registered := m.registry[key]
	initial, hasInitial := m.initial[key]
	saved, hasSaved := m.data[key]
	p := RecoveryPosition{Kind: kind}
	valid := false
	switch kind {
	case RecoveryFresh:
		valid = !registered && !hasInitial && !hasSaved
	case RecoveryInitial:
		p.Sequence = initial
		valid = hasInitial && !hasSaved && validSequence(initial)
	case RecoveryCheckpoint:
		p.Sequence = saved
		valid = hasSaved && !hasInitial && validSequence(saved)
	case RecoveryCompleted:
		p.Sequence = saved
		valid = hasSaved && !hasInitial && validCompletion(saved)
	}
	if !valid || regressed(s.observed, p) {
		return RecoveryPosition{}, fmt.Errorf("%w: shard %q", ErrRecoveryState, s.shard)
	}
	s.observed = p
	return p, nil
}

func regressed(previous, next RecoveryPosition) bool {
	switch previous.Kind {
	case RecoveryInitial:
		return next.Kind == RecoveryFresh || (next.Kind == RecoveryInitial && next.Sequence != previous.Sequence) || (next.Kind == RecoveryCheckpoint && checkpointAdvances(next.Sequence, previous.Sequence))
	case RecoveryCheckpoint:
		return next.Kind == RecoveryFresh || next.Kind == RecoveryInitial || (next.Kind == RecoveryCheckpoint && checkpointAdvances(next.Sequence, previous.Sequence))
	case RecoveryCompleted:
		return next != previous
	}
	return false
}

func (s *memorySession) Recovery(ctx context.Context) (RecoveryPosition, error) {
	var position RecoveryPosition
	err := s.run(ctx, func(key string) error {
		var err error
		position, err = s.recoveryLocked(key)
		return err
	})
	return position, err
}

func (s *memorySession) Initialize(ctx context.Context, sequence string) (RecoveryPosition, error) {
	var position RecoveryPosition
	err := s.run(ctx, func(key string) error {
		var err error
		position, err = s.recoveryLocked(key)
		if err != nil {
			return err
		}
		if position.Kind != RecoveryFresh {
			return nil
		}
		if !validSequence(sequence) {
			return fmt.Errorf("%w: invalid initial sequence", ErrRecoveryState)
		}
		s.store.initial[key] = sequence
		s.store.registry[key] = RecoveryInitial
		position = RecoveryPosition{Kind: RecoveryInitial, Sequence: sequence}
		s.observed = position
		return nil
	})
	return position, err
}

func (s *memorySession) Save(ctx context.Context, sequence string) error {
	return s.run(ctx, func(key string) error {
		current, err := s.recoveryLocked(key)
		if err != nil {
			return err
		}
		kind := RecoveryCheckpoint
		if validCompletion(sequence) {
			kind = RecoveryCompleted
		} else if !validSequence(sequence) {
			return fmt.Errorf("%w: invalid checkpoint", ErrRecoveryState)
		}
		if current.Kind == RecoveryCompleted {
			return nil
		}
		if current.Kind == RecoveryInitial && kind == RecoveryCheckpoint && checkpointAdvances(sequence, current.Sequence) {
			return fmt.Errorf("%w: checkpoint precedes initial position", ErrRecoveryState)
		}
		if current.Kind == RecoveryCheckpoint && !checkpointAdvances(current.Sequence, sequence) {
			return nil
		}
		s.store.data[key] = sequence
		s.store.registry[key] = kind
		delete(s.store.initial, key)
		s.observed = RecoveryPosition{Kind: kind, Sequence: sequence}
		return nil
	})
}
