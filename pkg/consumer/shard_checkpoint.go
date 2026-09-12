package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// readShardCheckpoint reads one checkpoint value through the store, retrying
// failures with the shared bounded retry policy (retryMaxAttempts /
// retryBackoff), mirroring saveCheckpointValueWithRetry: the store client
// reports network errors instead of absorbing them, so without consumer-owned
// retries a single dropped connection on a readiness read during Start's
// initial acquisition — the one pass whose errors are deliberately fatal —
// would kill the whole run. The backoff wait aborts as soon as ctx is done and
// returns the ctx error, so a shutdown mid-retry surfaces as the shutdown.
func (c *Consumer) readShardCheckpoint(ctx context.Context, shardID string) (string, error) {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		seq, err := c.store.Get(ctx, c.coordinationKey(), shardID)
		if err == nil {
			return seq, nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		c.logger.Warn("shard checkpoint read failed; will retry",
			slog.String("shard", shardID),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf("read shard checkpoint %s: %w", shardID, lastErr)
}

// saveCheckpointValueWithRetry writes one checkpoint value through
// saveCheckpointValue, retrying failures with the shared bounded retry policy
// (retryMaxAttempts / retryBackoff) so a brief store blip does not escalate
// into a consumer-fatal worker error. Every failed attempt counts a
// checkpoint failure so absorbed blips stay visible on dashboards. The
// backoff wait aborts as soon as ctx is done and returns the ctx error, so a
// shutdown mid-retry surfaces as the shutdown rather than a spurious fatal
// save error. At-least-once semantics are unchanged: on final failure the
// caller still surfaces the error and records replay from the previous
// checkpoint.
func (c *Consumer) saveCheckpointValueWithRetry(ctx context.Context, shardID, value string) error {
	maxAttempts := c.tuning.retryMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.saveCheckpointValue(ctx, shardID, value)
		if err == nil {
			c.checkpointHealth.recordSuccess(time.Now())
			c.observation.checkpointResult(shardID, nil)
			return nil
		}
		if ctx.Err() == nil {
			c.checkpointHealth.recordFailure(err)
			c.observation.checkpointResult(shardID, err)
		}
		if permanentCheckpointError(err) {
			// Ownership has moved on, or the shard's recovery state is
			// unusable. Neither can be retried into success, and neither is
			// the transient store blip this counter tracks: the handoff is
			// counted as a lost lease by the worker, and the recovery failure
			// by the session that reported it.
			return err
		}
		c.reporter.Counter(metricCheckpointFailures, 1, c.shardTags(shardID))
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.tuning.retryBackoff); err != nil {
			return err
		}
	}

	return lastErr
}

// saveCheckpointValue writes one checkpoint value through the shard's fenced
// session when it has one — so ownership is validated atomically with the
// write and the value cannot land after the lease has moved — and through the
// plain store otherwise.
func (c *Consumer) saveCheckpointValue(ctx context.Context, shardID, value string) error {
	if session := shardSessionFrom(ctx); session != nil {
		return session.save(ctx, value)
	}
	return c.store.Save(ctx, c.coordinationKey(), shardID, value)
}

// permanentCheckpointError reports failures that no retry can resolve: the
// lease is gone, or the shard's persisted recovery state is inconsistent.
func permanentCheckpointError(err error) bool {
	return errors.Is(err, lease.ErrNotOwned) || errors.Is(err, checkpoint.ErrRecoveryState)
}

func (c *Consumer) saveShardCheckpoint(ctx context.Context, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return nil
	}
	start := time.Now()
	if err := c.saveCheckpointValueWithRetry(ctx, shardID, sequenceNumber); err != nil {
		return fmt.Errorf("save shard checkpoint %s %s: %w", shardID, sequenceNumber, err)
	}
	c.reporter.Timing(metricCheckpointSaveDuration, time.Since(start), c.shardTags(shardID))
	c.reporter.Counter(metricCheckpointsSaved, 1, c.shardTags(shardID))
	c.recordPersisted(shardID, sequenceNumber, false)
	c.logger.Debug("shard checkpoint saved", slog.String("shard", shardID), slog.String("sequence", sequenceNumber))
	return nil
}

func (c *Consumer) saveShardCompletionCheckpoint(ctx context.Context, shardID, sequenceNumber string) error {
	checkpoint := shardCompletionValue(sequenceNumber)
	start := time.Now()
	if err := c.saveCheckpointValueWithRetry(ctx, shardID, checkpoint); err != nil {
		return fmt.Errorf("save shard completion checkpoint %s %s: %w", shardID, checkpoint, err)
	}
	c.reporter.Timing(metricCheckpointSaveDuration, time.Since(start), c.shardTags(shardID))
	c.reporter.Counter(metricCheckpointsSaved, 1, c.shardTags(shardID))
	c.reporter.Counter(metricShardsCompleted, 1, c.shardTags(shardID))
	c.recordPersisted(shardID, sequenceNumber, true)
	c.logger.Info("shard completed", slog.String("shard", shardID), slog.String("checkpoint", checkpoint))
	return nil
}

func (c *Consumer) saveShardCheckpointIfDue(ctx context.Context, shardID, sequenceNumber string, processedSinceCheckpoint int) (int, error) {
	if processedSinceCheckpoint < c.tuning.checkpointEvery {
		return processedSinceCheckpoint, nil
	}
	if sequenceNumber == "" {
		return processedSinceCheckpoint, nil
	}
	if err := c.saveShardCheckpoint(ctx, shardID, sequenceNumber); err != nil {
		return processedSinceCheckpoint, fmt.Errorf("save due shard checkpoint %s: %w", shardID, err)
	}
	// The save covers every record processed so far — the checkpoint is the
	// page-end sequence number — so nothing remains uncheckpointed. A modulo
	// remainder here would be phantom carry-over that makes the next
	// checkpoint fire early.
	return 0, nil
}

func (c *Consumer) checkpointOnDrain(ctx context.Context, shardID, sequenceNumber string, processedSinceCheckpoint int) error {
	if processedSinceCheckpoint <= 0 || sequenceNumber == "" {
		return nil
	}
	if err := c.saveShardCheckpoint(ctx, shardID, sequenceNumber); err != nil {
		return fmt.Errorf("save drain shard checkpoint %s: %w", shardID, err)
	}
	c.logger.Debug("shard drain checkpoint flushed",
		slog.String("shard", shardID),
		slog.String("sequence", sequenceNumber),
		slog.Int("records", processedSinceCheckpoint),
	)
	return nil
}
