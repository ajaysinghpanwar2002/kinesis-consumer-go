package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
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
			c.deregisterWorkerOnCleanShutdown()
			return
		case <-ticker.C:
			if send() {
				return
			}
		}
	}
}

// deregisterWorkerOnCleanShutdown best-effort removes this worker's liveness
// entry once the heartbeat loop stops on a clean shutdown (its context being
// cancelled), so surviving peers recompute fair share from this worker's
// absence immediately instead of counting it (via the lease manager's Workers)
// for up to one heartbeat TTL — the gap that would otherwise leave cleanly
// released shards idle across a rolling deploy or scale-down.
//
// It runs only on the clean ctx-cancelled exit, never the staleness exit: a
// stale heartbeat means the backend is already unreachable, so the entry will
// expire by TTL and a Deregister call would only fail against the same backend.
// It is a no-op when the lease manager does not implement lease.Deregisterer;
// entries then expire by TTL, exactly as after a crash.
//
// The heartbeat context is already cancelled by the time this runs, so it uses
// a fresh context bounded by the shard-lease-release timeout (the same cleanup
// budget worker lease release uses). Start's heartbeat-stop defer waits on the
// loop goroutine's completion before Start returns, so this finishes before an
// owned lease manager is closed. Failures are logged and swallowed: shutdown
// must neither block nor fail on a best-effort cleanup.
func (c *Consumer) deregisterWorkerOnCleanShutdown() {
	deregisterer, ok := c.leaseManager.(lease.Deregisterer)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.shardLeaseReleaseTimeout())
	defer cancel()

	if err := deregisterer.Deregister(ctx, c.coordinationKey(), c.leaseOwner); err != nil {
		c.logger.Warn("worker deregister on shutdown failed",
			slog.String("owner", c.leaseOwner),
			slog.Any("error", err))
		return
	}
	c.reporter.Counter(metricWorkerDeregistered, 1, c.streamTags())
	c.logger.Debug("worker deregistered on shutdown", slog.String("owner", c.leaseOwner))
}
