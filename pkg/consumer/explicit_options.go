package consumer

import (
	"context"
	"errors"
	"time"
)

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

// WithExplicitHandler configures explicit record processing. The constructor
// handler must be nil and no other handler option may be set. Returning nil
// does not acknowledge a delivery and does not cancel its completion context:
// the application acknowledges each Delivery when its own work is durable,
// from the handler or from a worker that outlives it.
//
// Explicit mode requires a checkpoint store with fenced sessions and a lease
// manager that is a fenced pair with it. It is always bounded: without
// WithInFlightLimits the documented explicit defaults apply.
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
// in the slice carries its own acknowledgment handle and must be acknowledged
// independently; the slice itself belongs to the application. The requirements
// of WithExplicitHandler apply unchanged.
func WithExplicitBatchHandler(handler func(context.Context, []Delivery) error) Option {
	return func(o *options) error {
		if handler == nil {
			return errors.New("explicit batch handler cannot be nil")
		}
		o.explicit.batch = handler
		return nil
	}
}

// WithCheckpointInterval sets how often explicit mode flushes the contiguous
// acknowledged prefix. It must be positive; the default is one second.
// Automatic mode uses count-based flushing and rejects this option.
func WithCheckpointInterval(interval time.Duration) Option {
	return func(o *options) error {
		if interval <= 0 {
			return errors.New("checkpoint interval must be positive")
		}
		o.checkpointInterval = interval
		return nil
	}
}
