package consumer

import (
	"context"
	"errors"
	"time"
)

// ErrExplicitModeUnavailable identifies explicit configuration before worker
// lifecycle integration is available. New rejects this mode in this release.
var ErrExplicitModeUnavailable = errors.New("explicit handler mode awaits lifecycle integration")

type explicitHandlers struct {
	record func(context.Context, Delivery) error
	batch  func(context.Context, []Delivery) error
}

func (h explicitHandlers) enabled() bool { return h.record != nil || h.batch != nil }

func (h explicitHandlers) validate(record HandlerFunc, batch BatchHandlerFunc) error {
	count := 0
	for _, present := range []bool{record != nil, batch != nil, h.record != nil, h.batch != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return errors.New("provide exactly one automatic or explicit handler mode")
	}
	return nil
}

// WithExplicitHandler configures explicit record processing with a nil
// constructor handler. Returning nil does not acknowledge a delivery.
// New currently returns ErrExplicitModeUnavailable until lifecycle support lands.
func WithExplicitHandler(handler func(context.Context, Delivery) error) Option {
	return func(o *options) error {
		if handler == nil {
			return errors.New("explicit handler cannot be nil")
		}
		o.explicit.record = handler
		return nil
	}
}

// WithExplicitBatchHandler configures explicit batch processing. Each delivery
// must be acknowledged independently. New currently rejects explicit mode with
// ErrExplicitModeUnavailable until lifecycle support lands.
func WithExplicitBatchHandler(handler func(context.Context, []Delivery) error) Option {
	return func(o *options) error {
		if handler == nil {
			return errors.New("explicit batch handler cannot be nil")
		}
		o.explicit.batch = handler
		return nil
	}
}

// WithCheckpointInterval sets the explicit-mode checkpoint interval. It must be
// positive; the default is one second. Automatic mode uses count-based flushing
// and rejects this option.
func WithCheckpointInterval(interval time.Duration) Option {
	return func(o *options) error {
		if interval <= 0 {
			return errors.New("checkpoint interval must be positive")
		}
		o.checkpointInterval = interval
		return nil
	}
}
