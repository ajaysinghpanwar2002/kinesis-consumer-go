package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func (c *Consumer) canonicalStreamName() string {
	if c.streamName != "" {
		return c.streamName
	}
	streamName, _ := resolveCanonicalStreamName(c.cfg)
	return streamName
}

func (c *Consumer) coordinationKey() string {
	if c.coordinationIdentity != "" {
		return c.coordinationIdentity
	}
	streamName := c.canonicalStreamName()
	if c.cfg.ConsumerGroup == "" {
		return streamName
	}
	return c.cfg.ConsumerGroup + ":" + streamName
}

// workerHeartbeatLoop keeps this worker visible to peers and enforces
// heartbeat validity: once sends have kept failing until one heartbeat
// interval before the last successful send's TTL lapses — the point after
// which peers may treat this worker as dead and claim its shards away —
// it reports the causal failure through stopRun (wrapped in
// ErrHeartbeatStale) and returns. stopRun must cancel the run.
func (c *Consumer) workerHeartbeatLoop(ctx context.Context, stopRun func(error)) {
	// Anchor the validity clock at loop start so a backend that never
	// accepts a single heartbeat is still bounded.
	c.heartbeatHealth.anchor(time.Now())

	// send performs one heartbeat and reports whether validity is lost.
	send := func() (stale bool) {
		// Bound the backend call so a hung-but-context-respecting Heartbeat
		// surfaces as a transient DeadlineExceeded within one heartbeat
		// interval — flowing into the failure/staleness accounting below —
		// instead of blocking this loop forever and silently disabling the
		// ErrHeartbeatStale stop while peers claim the shards away. Mirrors
		// renewShardLease.
		sendCtx := ctx
		if c.tuning.heartbeatInterval > 0 {
			var cancel context.CancelFunc
			sendCtx, cancel = context.WithTimeout(ctx, c.tuning.heartbeatInterval)
			defer cancel()
		}
		err := c.leaseManager.Heartbeat(sendCtx, c.coordinationKey(), c.leaseOwner, c.tuning.heartbeatTTL)
		if err == nil {
			c.heartbeatHealth.recordSuccess(time.Now())
			return false
		}
		if ctx.Err() != nil {
			// Shutdown cancellation, not a liveness failure.
			return false
		}
		// A silently failing heartbeat makes peers treat this worker as dead
		// and steadily claim its shards away — surface it on the victim.
		failures, lastSuccess := c.heartbeatHealth.recordFailure(err)
		c.reporter.Counter(metricHeartbeatFailures, 1, c.streamTags())

		// The worker key written by the last successful send expires at
		// lastSuccess+ttl; stopping at lastSuccess+ttl-interval keeps one
		// full heartbeat interval between this run stopping and peers being
		// able to observe the key as expired (interval < ttl is validated).
		deadline := lastSuccess.Add(c.tuning.heartbeatTTL - c.tuning.heartbeatInterval)
		staleness := time.Since(lastSuccess)
		stale = !time.Now().Before(deadline)
		c.logger.Warn("worker heartbeat failed",
			slog.String("owner", c.leaseOwner),
			slog.Int("consecutive_failures", failures),
			slog.Duration("staleness", staleness),
			slog.Any("error", err),
		)
		if stale {
			stopRun(fmt.Errorf("%w for %s (ttl %s, interval %s): %w",
				ErrHeartbeatStale, staleness.Round(time.Millisecond),
				c.tuning.heartbeatTTL, c.tuning.heartbeatInterval, err))
		}
		return stale
	}

	if send() {
		return
	}

	ticker := time.NewTicker(c.tuning.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if send() {
				return
			}
		}
	}
}
