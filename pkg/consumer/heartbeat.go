package consumer

import (
	"context"
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

func (c *Consumer) workerHeartbeatLoop(ctx context.Context) {
	send := func() {
		err := c.leaseManager.Heartbeat(ctx, c.coordinationKey(), c.leaseOwner, c.tuning.heartbeatTTL)
		if err == nil || ctx.Err() != nil {
			// Success, or shutdown cancellation (not a liveness failure).
			return
		}
		// A silently failing heartbeat makes peers treat this worker as dead
		// and steadily claim its shards away — surface it on the victim.
		c.reporter.Counter(metricHeartbeatFailures, 1, c.streamTags())
		c.logger.Warn("worker heartbeat failed",
			slog.String("owner", c.leaseOwner),
			slog.Any("error", err),
		)
	}

	send()

	ticker := time.NewTicker(c.tuning.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}
