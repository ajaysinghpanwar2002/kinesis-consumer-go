package consumer

import (
	"errors"
	"io"
	"log/slog"
	"time"
)

type StartPosition string

const (
	StartLatest      StartPosition = "LATEST"
	StartTrimHorizon StartPosition = "TRIM_HORIZON"
	StartAtTimestamp StartPosition = "AT_TIMESTAMP"
)

type RetryConfig struct {
	MaxAttempts int
	Backoff     time.Duration
}

type Config struct {
	StreamName        string
	StreamARN         string
	StartPosition     StartPosition
	StartTimestamp    *time.Time
	ShardConcurrency  int
	BatchSize         int32
	PollInterval      time.Duration
	ShardSyncInterval time.Duration
	Retry             RetryConfig
	CheckpointEvery   int
	Logger            *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.StartPosition == "" {
		c.StartPosition = StartLatest
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.ShardConcurrency == 0 {
		c.ShardConcurrency = 1
	}
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
	if c.ShardSyncInterval == 0 {
		c.ShardSyncInterval = time.Minute
	}
	if c.Retry.MaxAttempts == 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.Backoff == 0 {
		c.Retry.Backoff = time.Second
	}
	if c.CheckpointEvery == 0 {
		c.CheckpointEvery = 100
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return c
}

func (c Config) validate() error {
	if c.StreamName == "" && c.StreamARN == "" {
		return errors.New("stream name or ARN is required")
	}
	if c.StartPosition == StartAtTimestamp && c.StartTimestamp == nil {
		return errors.New("start timestamp is required when using AT_TIMESTAMP")
	}
	if c.BatchSize < 1 {
		return errors.New("batch size must be >= 1")
	}
	if c.ShardConcurrency < 1 {
		return errors.New("shardConcurrency must be >= 1")
	}
	if c.ShardSyncInterval < time.Second {
		return errors.New("shardSyncInterval must be >= 1s")
	}
	if c.Retry.MaxAttempts < 1 {
		return errors.New("retry max attempts must be >= 1")
	}
	if c.CheckpointEvery < 1 {
		return errors.New("checkpointEvery must be >= 1")
	}
	return nil
}
