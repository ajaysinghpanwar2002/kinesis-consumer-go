package consumer

import "time"

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
