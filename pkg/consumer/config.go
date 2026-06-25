package consumer

import (
	"errors"
	"fmt"
	"time"
)

// StartPosition controls where consumption starts when no checkpoint exists.
type StartPosition string

const (
	StartLatest      StartPosition = "LATEST"
	StartTrimHorizon StartPosition = "TRIM_HORIZON"
	StartAtTimestamp StartPosition = "AT_TIMESTAMP"
)

// Config contains the required stream identity and initial position settings.
type Config struct {
	StreamName     string
	StreamARN      string
	StartPosition  StartPosition
	StartTimestamp *time.Time
}

func (c Config) withDefaults() Config {
	if c.StartPosition == "" {
		c.StartPosition = StartLatest
	}
	return c
}

func (c Config) validate() error {
	if c.StreamName == "" && c.StreamARN == "" {
		return errors.New("stream name or ARN is required")
	}

	switch c.StartPosition {
	case "", StartLatest, StartTrimHorizon, StartAtTimestamp:
	default:
		return fmt.Errorf("invalid start position %q", c.StartPosition)
	}

	if c.StartPosition == StartAtTimestamp && c.StartTimestamp == nil {
		return errors.New("start timestamp is required when using AT_TIMESTAMP")
	}

	return nil
}
