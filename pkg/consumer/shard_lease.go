package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func (c *Consumer) acquireShardLease(ctx context.Context, shardID string) (lease.Lease, bool, error) {
	start := time.Now()
	shardLease, acquired, err := c.leaseManager.Acquire(
		ctx,
		c.coordinationKey(),
		shardID,
		c.leaseOwner,
		c.tuning.heartbeatTTL,
	)
	if err != nil {
		return nil, false, fmt.Errorf("acquire shard lease %s: %w", shardID, err)
	}
	c.reporter.Timing(metricLeaseAcquireDuration, time.Since(start), c.shardTags(shardID))
	return shardLease, acquired, nil
}

func (c *Consumer) acquireShardLeaseWithRetry(ctx context.Context, shardID string) (lease.Lease, bool, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		shardLease, acquired, err := c.acquireShardLease(ctx, shardID)
		if err == nil {
			return shardLease, acquired, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return nil, false, err
		}
	}

	return nil, false, lastErr
}

func (c *Consumer) claimShardLease(ctx context.Context, shardID, expectedOwner string) (lease.Lease, bool, error) {
	shardLease, claimed, err := c.leaseManager.Claim(
		ctx,
		c.coordinationKey(),
		shardID,
		expectedOwner,
		c.leaseOwner,
		c.tuning.heartbeatTTL,
	)
	if err != nil {
		return nil, false, fmt.Errorf("claim shard lease %s from %s: %w", shardID, expectedOwner, err)
	}
	return shardLease, claimed, nil
}

func (c *Consumer) claimShardLeaseWithRetry(ctx context.Context, shardID, expectedOwner string) (lease.Lease, bool, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		shardLease, claimed, err := c.claimShardLease(ctx, shardID, expectedOwner)
		if err == nil {
			return shardLease, claimed, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return nil, false, err
		}
	}

	return nil, false, lastErr
}

func (c *Consumer) acquireShardLeases(ctx context.Context, shardIDs []string) (map[string]lease.Lease, error) {
	leases := make(map[string]lease.Lease, len(shardIDs))
	for _, shardID := range shardIDs {
		shardLease, acquired, err := c.acquireShardLeaseWithRetry(ctx, shardID)
		if err != nil {
			acquireErr := fmt.Errorf("acquire shard leases %s: %w", shardID, err)
			if rollbackErr := c.rollbackShardLeases(leases); rollbackErr != nil {
				return nil, errors.Join(acquireErr, rollbackErr)
			}
			return nil, acquireErr
		}
		if !acquired || shardLease == nil {
			continue
		}
		leases[shardID] = shardLease
		c.reporter.Counter(metricLeaseAcquired, 1, c.shardTags(shardID))
		c.logger.Debug("shard lease acquired", slog.String("shard", shardID), slog.String("owner", c.leaseOwner))
	}
	return leases, nil
}

func (c *Consumer) rollbackShardLeases(shardLeases map[string]lease.Lease) error {
	var rollbackErrs []error
	for shardID, shardLease := range shardLeases {
		if err := c.releaseShardLeaseWithTimeout(shardID, shardLease); err != nil {
			rollbackErrs = append(rollbackErrs, err)
		}
	}
	return errors.Join(rollbackErrs...)
}

func (c *Consumer) renewShardLease(ctx context.Context, shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	// Bound the backend call so a hung-but-context-respecting Renew surfaces
	// as a transient DeadlineExceeded within one heartbeat interval — leaving
	// room for retries inside the TTL budget — instead of blocking the renew
	// loop past the TTL with no error at all.
	if c.tuning.heartbeatInterval > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.tuning.heartbeatInterval)
		defer cancel()
	}

	if err := shardLease.Renew(ctx, c.tuning.heartbeatTTL); err != nil {
		return fmt.Errorf("renew shard lease %s: %w", shardID, err)
	}
	return nil
}

// leaseRenewTracker shares the time of the last successful renew — read on
// the local monotonic clock — between the renew loop and its watchdog.
type leaseRenewTracker struct {
	last atomic.Pointer[time.Time]
}

func newLeaseRenewTracker() *leaseRenewTracker {
	t := &leaseRenewTracker{}
	t.touch()
	return t
}

func (t *leaseRenewTracker) touch() {
	now := time.Now()
	t.last.Store(&now)
}

func (t *leaseRenewTracker) sinceLastRenewed() time.Duration {
	return time.Since(*t.last.Load())
}

// renewShardLeaseLoopWithWatchdog runs the renew loop alongside a local
// lease-validity watchdog. The loop fences failures that return errors; the
// watchdog fences the ones that don't: a Renew hung past the TTL (a backend
// that ignores its context) means the backend lease has lapsed and a peer
// may already own the shard, so the worker must stop even though no renew
// attempt ever reported an error.
//
// The watchdog is a backstop: it arms one heartbeat interval *after* the TTL
// (the loop's own TTL-budget check stays the primary enforcer and carries the
// causal error; arming at exactly the TTL would race it) and on expiry grants
// one more interval of grace for a bounded in-flight attempt to report. The
// worker is therefore fenced within heartbeatTTL + 2*heartbeatInterval even
// when Renew never returns.
func (c *Consumer) renewShardLeaseLoopWithWatchdog(ctx context.Context, shardID string, shardLease lease.Lease) error {
	renewed := newLeaseRenewTracker()
	expiry := c.tuning.heartbeatTTL + c.tuning.heartbeatInterval

	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()

	// Buffered so an abandoned loop goroutine can still deliver its result
	// and exit after the watchdog has returned.
	loopErrCh := make(chan error, 1)
	go func() {
		loopErrCh <- c.renewShardLeaseLoop(loopCtx, shardID, shardLease, renewed)
	}()

	timer := time.NewTimer(expiry)
	defer timer.Stop()

	for {
		select {
		case err := <-loopErrCh:
			return err
		case <-timer.C:
			if remaining := expiry - renewed.sinceLastRenewed(); remaining > 0 {
				timer.Reset(remaining)
				continue
			}
			// TTL blown on the local clock with no word from the loop: Renew
			// is presumably hung inside the backend call. Give a bounded
			// in-flight attempt one interval to report its causal error, then
			// fence without waiting further.
			grace := time.NewTimer(c.tuning.heartbeatInterval)
			select {
			case err := <-loopErrCh:
				grace.Stop()
				return err
			case <-grace.C:
			}
			if remaining := expiry - renewed.sinceLastRenewed(); remaining > 0 {
				// A late renew landed during the grace window after all.
				timer.Reset(remaining)
				continue
			}
			sinceRenewed := renewed.sinceLastRenewed()
			c.logger.Warn("shard lease validity expired; stopping worker",
				slog.String("shard", shardID),
				slog.Duration("since_last_renew", sinceRenewed),
				slog.Duration("ttl", c.tuning.heartbeatTTL),
			)
			return fmt.Errorf("shard lease %s validity expired: no successful renew for %v (ttl %v)",
				shardID, sinceRenewed, c.tuning.heartbeatTTL)
		}
	}
}

func (c *Consumer) renewShardLeaseLoop(ctx context.Context, shardID string, shardLease lease.Lease, renewed *leaseRenewTracker) error {
	ticker := time.NewTicker(c.tuning.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			err := c.renewShardLease(ctx, shardID, shardLease)
			if err == nil {
				renewed.touch()
				c.reporter.Counter(metricLeaseRenewals, 1, c.shardTags(shardID))
				continue
			}
			if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				// Shutdown cancellation, not a renewal failure: count neither.
				// (A per-call deadline expiry leaves ctx.Err() nil and takes the
				// transient path below.)
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}
				return ctx.Err()
			}
			c.reporter.Counter(metricLeaseRenewalFailures, 1, c.shardTags(shardID))
			if errors.Is(err, lease.ErrNotOwned) {
				// The lease is held by someone else (takeover, or expiry plus
				// reclaim): retrying cannot get it back — stop promptly.
				return err
			}
			// Transient backend failure. The lease stays ours at the backend
			// until heartbeatTTL since the last successful renew, so retry on
			// subsequent ticks within that budget — one dropped renew must not
			// restart the whole consumer.
			sinceRenewed := renewed.sinceLastRenewed()
			if sinceRenewed >= c.tuning.heartbeatTTL {
				// Budget exhausted: the backend lease has lapsed and a peer may
				// already own the shard; continuing risks dual processing.
				return fmt.Errorf("shard lease %s not renewed within ttl %v: %w", shardID, c.tuning.heartbeatTTL, err)
			}
			c.logger.Warn("shard lease renew failed; will retry",
				slog.String("shard", shardID),
				slog.Duration("since_last_renew", sinceRenewed),
				slog.Duration("ttl", c.tuning.heartbeatTTL),
				slog.Any("error", err),
			)
		}
	}
}

func (c *Consumer) releaseShardLease(ctx context.Context, shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	if err := shardLease.Release(ctx); err != nil {
		return fmt.Errorf("release shard lease %s: %w", shardID, err)
	}
	return nil
}

func (c *Consumer) releaseShardLeaseWithTimeout(shardID string, shardLease lease.Lease) error {
	if shardLease == nil {
		return nil
	}

	releaseCtx, cancel := context.WithTimeout(context.Background(), c.shardLeaseReleaseTimeout())
	defer cancel()

	if err := shardLease.Release(releaseCtx); err != nil {
		if errors.Is(err, lease.ErrNotOwned) {
			// A peer already owns this shard: the lease was taken over or expired
			// and reclaimed before this cleanup release ran (a routine rebalance
			// race, e.g. a shed donor whose renew loop was cancelled before it
			// observed the loss). Ownership loss is shard-local — the peer resumes
			// from the last checkpoint — so treat it as a successful handoff, not a
			// consumer-fatal release failure. Count it as a lost lease (mirroring
			// the renew-loop handoff path) instead of polluting the release-failure
			// counter, and return nil so the worker stops cleanly. Checked before
			// the deadline branch below so a genuine release timeout stays fatal.
			c.reporter.Counter(metricLeaseLost, 1, c.shardTags(shardID))
			c.logger.Info("shard lease already claimed by peer at release; treating as handoff",
				slog.String("shard", shardID))
			return nil
		}
		releaseErr := fmt.Errorf("release shard lease %s: %w", shardID, err)
		if errors.Is(err, context.DeadlineExceeded) {
			releaseErr = fmt.Errorf("release shard lease %s timed out: %w", shardID, err)
		}
		// Logged here because the caller (shard_worker.go) discards this error
		// when the worker already failed, so it would otherwise be invisible.
		c.reporter.Counter(metricLeaseReleaseFailures, 1, c.shardTags(shardID))
		c.logger.Warn("shard lease release failed", slog.String("shard", shardID), slog.Any("error", releaseErr))
		return releaseErr
	}
	c.reporter.Counter(metricLeaseReleased, 1, c.shardTags(shardID))
	c.logger.Debug("shard lease released", slog.String("shard", shardID))
	return nil
}

func (c *Consumer) shardLeaseReleaseTimeout() time.Duration {
	if c == nil || c.tuning.shardLeaseReleaseTimeout <= 0 {
		return 5 * time.Second
	}
	return c.tuning.shardLeaseReleaseTimeout
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
