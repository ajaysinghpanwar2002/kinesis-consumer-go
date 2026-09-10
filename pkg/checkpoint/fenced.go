package checkpoint

import (
	"context"
	"errors"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// ErrRecoveryState identifies missing, invalid, inconsistent, or observably
// regressed recovery metadata. Callers must halt rather than reinitialize.
var ErrRecoveryState = errors.New("invalid recovery state")

// RecoveryKind identifies the required persistent recovery value.
type RecoveryKind string

const (
	RecoveryFresh      RecoveryKind = ""
	RecoveryInitial    RecoveryKind = "initial"
	RecoveryCheckpoint RecoveryKind = "checkpoint"
	RecoveryCompleted  RecoveryKind = "completed"
)

// RecoveryPosition is inclusive for Initial and exclusive for Checkpoint.
// Completed contains a terminal Store marker. Fresh has an empty sequence.
type RecoveryPosition struct {
	Kind     RecoveryKind
	Sequence string
}

// FencedStore is an optional Store capability. Bind must reject leases from an
// incompatible backend or namespace. Legacy Store methods remain unfenced;
// consumers using fencing must perform all progress operations through Session.
type FencedStore interface {
	Store
	Bind(context.Context, string, string, lease.FencedLease) (Session, error)
}

// Session binds recovery operations to one lease acquisition. Every operation
// validates owner, generation, and backend expiry atomically with reading or
// writing state. Lease loss and local invalidation are permanent. Backend errors
// must not accept writes. Implementations must support concurrent calls.
//
// Initialization records live independently of lease/worker TTLs and identify
// which recovery value is required. Partial metadata loss is an error. Loss of
// both metadata and all initialization evidence may look like a fresh namespace.
// Backend rollback to an internally consistent historical snapshot cannot always
// be detected by a fresh process; strict fencing requires no rollback of
// acknowledged ownership/progress history or simultaneous authoritative primaries.
type Session interface {
	// Recovery validates the registry/value relationship, including any progress
	// regression observable within this session.
	Recovery(context.Context) (RecoveryPosition, error)
	// Initialize atomically writes the first fetched sequence, once, before any
	// admission (including oversized-record rejection). Existing recovery wins.
	// Protection starts after this write: a crash before it can lose the initial
	// LATEST observation window, during which no record may have been admitted.
	Initialize(context.Context, string) (RecoveryPosition, error)
	// Save atomically advances the checkpoint and its required-metadata registry.
	// Equal/older sequences are no-ops; completion is terminal. A checkpoint
	// supersedes the inclusive initial position and may not precede it.
	Save(context.Context, string) error
	// Invalidate permanently invalidates this session without releasing the lease.
	Invalidate()
}
