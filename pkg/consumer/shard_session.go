package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// shardSessionKey carries a shard worker's fenced recovery session down its own
// processing path. Like shardWorkerStopKey it is worker-scoped state, not a
// dependency: the session is created from the lease that authorizes exactly
// this worker's writes, lives and dies with runShardWorker, and every call site
// that reads it (iterator derivation, initialization, checkpoint saves) already
// receives that worker's context. Threading it as a parameter instead would
// change the signature of every processing hook without making the lifetime any
// clearer.
type shardSessionKey struct{}

// shardSession is the consumer's view of one fenced checkpoint session. Every
// operation validates owner, generation, and backend expiry atomically with the
// read or write, so a worker whose lease moved on can neither read recovery
// state nor persist progress.
//
// fresh caches the last observed recovery kind so the pass knows whether the
// first fetched sequence still has to be persisted as an inclusive replay
// position. It is written and read only from the shard's processing goroutine,
// but guarded anyway: it is cheap next to the backend calls around it.
type shardSession struct {
	c       *Consumer
	session checkpoint.Session
	shardID string

	mu    sync.Mutex
	fresh bool
}

func withShardSession(ctx context.Context, session *shardSession) context.Context {
	if session == nil {
		return ctx
	}
	return context.WithValue(ctx, shardSessionKey{}, session)
}

// shardSessionFrom returns the calling shard worker's fenced session, or nil
// when the consumer is running unfenced (a legacy store or lease manager).
func shardSessionFrom(ctx context.Context) *shardSession {
	session, _ := ctx.Value(shardSessionKey{}).(*shardSession)
	return session
}

// bindShardSession binds the shard's checkpoint session to the lease
// acquisition that authorizes it. It returns (nil, nil) when fencing does not
// apply, which keeps automatic mode working exactly as before on the three
// legacy combinations: a store without the FencedStore capability, a lease
// manager whose leases are not FencedLease, and a fenced-capable store bound to
// a lease from another backend or namespace (ErrLeaseMismatch) — including a
// checkpoint.NewMemoryStore built without its lease manager. Explicit handler
// mode will instead require a matching pair.
//
// Every other Bind failure is returned: a fenced backend that is present but
// whose recovery state is unreadable or inconsistent must halt the shard, never
// silently fall back to an unfenced path that could re-anchor at LATEST.
func (c *Consumer) bindShardSession(ctx context.Context, shardID string, shardLease lease.Lease) (*shardSession, error) {
	store, ok := c.store.(checkpoint.FencedStore)
	if !ok {
		return nil, nil
	}
	held, ok := shardLease.(lease.FencedLease)
	if !ok {
		c.logger.Debug("lease manager does not support fencing; shard runs unfenced",
			slog.String("shard", shardID))
		return nil, nil
	}

	var bound checkpoint.Session
	err := c.retryFencedRecovery(ctx, shardID, "bind shard session", func(ctx context.Context) error {
		session, bindErr := store.Bind(ctx, c.coordinationKey(), shardID, held)
		if bindErr != nil {
			return bindErr
		}
		bound = session
		return nil
	})
	if err != nil {
		if errors.Is(err, lease.ErrLeaseMismatch) {
			c.logger.Warn("checkpoint store and lease manager are not a fenced pair; shard runs unfenced",
				slog.String("shard", shardID))
			return nil, nil
		}
		// Both built-in stores validate recovery state inside Bind, so this is
		// where corrupt metadata surfaces at startup.
		return nil, c.observeRecoveryFailure(shardID, fmt.Errorf("bind shard session %s: %w", shardID, err))
	}
	session := &shardSession{c: c, session: bound, shardID: shardID}
	c.logger.Debug("fenced shard session bound",
		slog.String("shard", shardID), slog.String("generation", held.Generation()))
	return session, nil
}

// observeRecoveryFailure counts unusable recovery state so an operator sees
// halted shards on dashboards, not only in the error returned from Start. It is
// called at each boundary where such an error first surfaces — session binding,
// session operations, and anchor verification — so every failure is counted
// exactly once.
func (c *Consumer) observeRecoveryFailure(shardID string, err error) error {
	if errors.Is(err, checkpoint.ErrRecoveryState) {
		c.reporter.Counter(metricRecoveryFailures, 1, c.shardTags(shardID))
		c.logger.Error("shard recovery state is unusable",
			slog.String("shard", shardID), slog.Any("error", err))
	}
	return err
}

// retryFencedRecovery runs one fenced recovery call under the shared bounded
// retry policy (retryMaxAttempts / retryBackoff), for the same reason
// readShardCheckpoint — the call this path replaces on a fenced worker — has
// one: the built-in store clients report network errors instead of absorbing
// them, so without consumer-owned retries a single dropped connection during
// startup recovery, the one pass whose errors are deliberately fatal, would end
// the whole run.
//
// Only transient failures are retried. Lost ownership, unusable recovery state,
// and a mismatched fenced pair are answers, not blips: they are returned on the
// first attempt. Binding, recovery reads, and initialization are all safe to
// repeat against the same persisted state — initialization is write-once and
// returns the existing position — so a retry can only re-observe what is
// already there. The backoff wait aborts as soon as ctx is done and returns the
// ctx error, so a shutdown mid-retry surfaces as the shutdown.
func (c *Consumer) retryFencedRecovery(ctx context.Context, shardID, operation string, call func(context.Context) error) error {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := call(ctx)
		if err == nil {
			return nil
		}
		if permanentRecoveryError(err) || ctx.Err() != nil {
			return err
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		c.logger.Warn("fenced recovery call failed; will retry",
			slog.String("shard", shardID),
			slog.String("operation", operation),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)
		if sleepErr := sleepWithContext(ctx, c.tuning.retryBackoff); sleepErr != nil {
			return sleepErr
		}
	}

	return lastErr
}

// permanentRecoveryError reports failures no retry can resolve: ownership has
// moved on, the shard's persisted recovery state is unusable, or the store and
// lease manager are not a fenced pair.
func permanentRecoveryError(err error) bool {
	return permanentCheckpointError(err) || errors.Is(err, lease.ErrLeaseMismatch)
}

func (s *shardSession) observe(err error) error {
	return s.c.observeRecoveryFailure(s.shardID, err)
}

func (s *shardSession) setFresh(fresh bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fresh = fresh
}

// needsInitialPosition reports whether the shard still has to persist an
// inclusive replay position before its first record may be admitted.
func (s *shardSession) needsInitialPosition() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fresh
}

func (s *shardSession) recovery(ctx context.Context) (checkpoint.RecoveryPosition, error) {
	var position checkpoint.RecoveryPosition
	err := s.c.retryFencedRecovery(ctx, s.shardID, "read shard recovery", func(ctx context.Context) error {
		read, readErr := s.session.Recovery(ctx)
		if readErr != nil {
			return readErr
		}
		position = read
		return nil
	})
	if err != nil {
		return checkpoint.RecoveryPosition{}, s.observe(err)
	}
	s.setFresh(position.Kind == checkpoint.RecoveryFresh)
	return position, nil
}

// initialize persists sequence as the shard's inclusive replay position. An
// existing recovery position wins, and is returned instead, so concurrent
// calls, retries, and successors all reuse the first one ever written.
func (s *shardSession) initialize(ctx context.Context, sequence string) (checkpoint.RecoveryPosition, error) {
	var position checkpoint.RecoveryPosition
	err := s.c.retryFencedRecovery(ctx, s.shardID, "initialize shard recovery", func(ctx context.Context) error {
		written, writeErr := s.session.Initialize(ctx, sequence)
		if writeErr != nil {
			return writeErr
		}
		position = written
		return nil
	})
	if err != nil {
		return checkpoint.RecoveryPosition{}, s.observe(err)
	}
	s.setFresh(position.Kind == checkpoint.RecoveryFresh)
	return position, nil
}

func (s *shardSession) save(ctx context.Context, value string) error {
	if err := s.session.Save(ctx, value); err != nil {
		return s.observe(err)
	}
	// A persisted checkpoint supersedes any initial replay position, so the
	// shard can never need one again.
	s.setFresh(false)
	return nil
}

// invalidate permanently stops this session from reading or writing recovery
// state. It does not release the lease: the worker's own release path owns
// that, and a session invalidated because ownership already moved has nothing
// to release.
func (s *shardSession) invalidate() {
	s.session.Invalidate()
}

// initializeShardRecovery persists the first sequence this shard ever yielded
// as its inclusive replay position, before any record of that page is handled.
// It reports whether the caller must resume from an existing position instead:
// the write is once-only, so a position recorded earlier — by a predecessor, an
// earlier attempt, or a concurrent one — wins, and this page may not be
// admitted from where it happens to start.
//
// The window before this write is the documented startup boundary: a crash
// here loses the LATEST observation, and no record may have been admitted
// during it.
func (c *Consumer) initializeShardRecovery(ctx context.Context, shardID string, session *shardSession, first Record) (bool, error) {
	sequence := aws.ToString(first.SequenceNumber)
	if sequence == "" {
		return false, fmt.Errorf("initialize shard recovery %s: %w: first record has no sequence number",
			shardID, checkpoint.ErrRecoveryState)
	}

	position, err := session.initialize(ctx, sequence)
	if err != nil {
		return false, fmt.Errorf("initialize shard recovery %s: %w", shardID, err)
	}
	if position.Kind == checkpoint.RecoveryInitial && position.Sequence == sequence {
		c.logger.Debug("shard initial replay position persisted",
			slog.String("shard", shardID), slog.String("sequence", sequence))
		return false, nil
	}

	c.logger.Info("shard already has a recovery position; resuming from it",
		slog.String("shard", shardID),
		slog.String("kind", string(position.Kind)),
		slog.String("sequence", position.Sequence),
	)
	return true, nil
}
