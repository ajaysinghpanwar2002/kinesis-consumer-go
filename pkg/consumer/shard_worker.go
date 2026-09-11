package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) runShardWorker(ctx context.Context, shardID string, shardLease lease.Lease) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var leaseLost atomic.Bool
	// The session is bound below, once renewal is already running, so the renew
	// loop reaches it through a slot rather than a plain variable.
	var sessionSlot shardSessionSlot
	renewCtx, stopRenew := context.WithCancel(workerCtx)
	renewErrCh := make(chan error, 1)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		if err := c.renewShardLeaseLoopWithWatchdog(renewCtx, shardID, shardLease); err != nil {
			if errors.Is(err, lease.ErrNotOwned) {
				// A peer claimed the shard. Ownership loss is local to this
				// worker: the peer resumes from the last checkpoint, so stop
				// processing promptly without failing the whole consumer run.
				leaseLost.Store(true)
				// Fence the session immediately so an in-flight write cannot
				// land while the worker is winding down — and so one bound
				// after this point is fenced the moment it arrives.
				sessionSlot.fence()
				c.reporter.Counter(metricLeaseLost, 1, c.shardTags(shardID))
				c.logger.Info("shard lease lost; stopping worker",
					slog.String("shard", shardID))
				cancel()
				return
			}
			select {
			case renewErrCh <- err:
			default:
			}
			cancel()
		}
	}()

	// Binding runs under a lease that is already being renewed. It retries
	// transient backend failures, and each backoff is dead time in which
	// nothing else would extend the lease this worker just acquired: a
	// heartbeat TTL shorter than the retry budget would otherwise let one blip
	// expire an owned lease before the shard ever read a record. Renewal also
	// cancels workerCtx when ownership moves, so a bind in progress stops
	// instead of finishing against a lease that is gone.
	session, bindErr := c.bindShardSession(workerCtx, shardID, shardLease)
	if bindErr != nil {
		stopRenew()
		<-renewDone
		return c.stopWorkerAfterFailedBind(ctx, shardID, shardLease, bindErr, &leaseLost, renewErrCh)
	}
	sessionSlot.set(session)

	processCtx := workerCtx
	if session != nil {
		// The session outlives neither this worker nor its lease: invalidating
		// it on the way out permanently stops any late write from a callback
		// that ignored its cancellation.
		defer session.invalidate()
		processCtx = withShardSession(workerCtx, session)
	}

	processErrCh := make(chan error, 1)
	processDone := make(chan struct{})
	go func() {
		defer close(processDone)
		if err := c.runShardRecordsLoop(processCtx, shardID); err != nil {
			select {
			case processErrCh <- err:
			default:
			}
		}
		cancel()
	}()

	var err error
	select {
	case err = <-renewErrCh:
		cancel()
	case err = <-processErrCh:
		cancel()
	case <-workerCtx.Done():
		if !errors.Is(workerCtx.Err(), context.Canceled) {
			err = workerCtx.Err()
		}
		select {
		case renewErr := <-renewErrCh:
			err = renewErr
		default:
		}
		select {
		case processErr := <-processErrCh:
			if err == nil {
				err = processErr
			}
		default:
		}
	}

	stopRenew()
	<-renewDone
	<-processDone

	if err == nil {
		select {
		case err = <-renewErrCh:
		default:
		}
	}
	if err == nil {
		select {
		case err = <-processErrCh:
		default:
		}
	}

	if errors.Is(err, lease.ErrNotOwned) {
		// A fenced recovery read or checkpoint write found the lease gone. It
		// is the same shard-local handoff the renew loop reports, so stop
		// cleanly instead of failing the consumer run — including when both
		// paths observe the takeover and this error arrives after the renew
		// loop already set the fence. The fence only decides who counts and
		// logs it, never whether the error is normalized.
		if !leaseLost.Swap(true) {
			c.reporter.Counter(metricLeaseLost, 1, c.shardTags(shardID))
			c.logger.Info("shard ownership lost during processing; stopping worker",
				slog.String("shard", shardID))
		}
		err = nil
	}

	if leaseLost.Load() {
		// The lease belongs to a peer now — there is nothing to release, and a
		// release attempt would only fail ErrNotOwned and pollute the failure
		// counters on a clean shard-local handoff. Processing may report the
		// cancellation we issued to stop it; that is part of the handoff, not an
		// independent worker failure. Preserve every other processing error.
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	// A caller/run stop can make a context-aware handler or DLQ attempt return
	// context.Canceled before Start's runCtx.Done branch wins its select. That
	// cancellation is the requested worker stop, not an independent fatal
	// worker error. Only normalize it when this worker was actually told to
	// stop: the parent worker context is canceled, or the stop fence is set —
	// a shed stopper sets the fence before calling cancel, so a descheduled
	// stopper (fence visible, cancellation not yet delivered) can make the
	// page path return context.Canceled while ctx.Err() is still nil, and
	// without the fence check that routine shed would escalate into a fatal
	// run error. A handler that returns context.Canceled while its context is
	// live and unfenced still follows retries/failure policy and remains a
	// real error.
	if errors.Is(err, context.Canceled) &&
		(errors.Is(ctx.Err(), context.Canceled) || shardWorkerStopRequested(ctx)) {
		err = nil
	}
	if releaseErr := c.releaseShardLeaseWithTimeout(shardID, shardLease); releaseErr != nil && err == nil {
		err = releaseErr
	}
	return err
}

func (c *Consumer) runShardRecordsLoop(ctx context.Context, shardID string) error {
	process := c.processShardRecordsLoop
	if c.processShardRecordsLoopFn != nil {
		process = c.processShardRecordsLoopFn
	}

	_, _, err := process(ctx, shardID)
	return err
}

// stopWorkerAfterFailedBind winds up a worker whose lease renewal started but
// whose session never bound. Ownership loss — reported by the bind itself or by
// the renew loop that cancelled it — is a clean shard-local handoff: nothing to
// release, nothing to process. A renewal failure that cancelled the bind is the
// real error, not the interrupted call it produced.
func (c *Consumer) stopWorkerAfterFailedBind(
	ctx context.Context,
	shardID string,
	shardLease lease.Lease,
	bindErr error,
	leaseLost *atomic.Bool,
	renewErrCh <-chan error,
) error {
	if errors.Is(bindErr, lease.ErrNotOwned) && !leaseLost.Swap(true) {
		c.reporter.Counter(metricLeaseLost, 1, c.shardTags(shardID))
		c.logger.Info("shard lease lost before binding its recovery session; stopping worker",
			slog.String("shard", shardID))
	}
	if leaseLost.Load() {
		return nil
	}

	select {
	case renewErr := <-renewErrCh:
		bindErr = renewErr
	default:
		if errors.Is(bindErr, context.Canceled) &&
			(errors.Is(ctx.Err(), context.Canceled) || shardWorkerStopRequested(ctx)) {
			bindErr = nil
		}
	}
	if releaseErr := c.releaseShardLeaseWithTimeout(shardID, shardLease); releaseErr != nil && bindErr == nil {
		bindErr = releaseErr
	}
	return bindErr
}

// shardSessionSlot hands the bound session to the renew loop, which now starts
// before binding does. Ordering is the point: fencing an unbound slot marks it,
// so a session that binds after ownership has already moved is invalidated the
// moment it lands rather than slipping past the fence.
type shardSessionSlot struct {
	mu      sync.Mutex
	session *shardSession
	fenced  bool
}

func (s *shardSessionSlot) set(session *shardSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.session = session
	if s.fenced && session != nil {
		session.invalidate()
	}
}

func (s *shardSessionSlot) fence() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fenced = true
	if s.session != nil {
		s.session.invalidate()
	}
}
