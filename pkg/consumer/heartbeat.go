package consumer

import (
	"context"
	"time"
)

func (c *Consumer) streamKey() string {
	if c.cfg.StreamName != "" {
		return c.cfg.StreamName
	}
	return c.cfg.StreamARN
}

func (c *Consumer) workerHeartbeatLoop(ctx context.Context) {
	send := func() {
		_ = c.leaseManager.Heartbeat(ctx, c.streamKey(), c.leaseOwner, c.tuning.heartbeatTTL)
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
