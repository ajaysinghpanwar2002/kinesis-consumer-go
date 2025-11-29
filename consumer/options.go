package consumer

import "errors"

// Option configures optional consumer features.
type Option func(*consumerOptions) error

type consumerOptions struct {
	batchHandler BatchHandlerFunc
}

func applyOptions(opts []Option) (consumerOptions, error) {
	var cfg consumerOptions
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return consumerOptions{}, err
		}
	}
	return cfg, nil
}

// WithBatchHandler switches the consumer to call the provided batch handler
// once per GetRecords call instead of invoking the per-record handler.
func WithBatchHandler(handler BatchHandlerFunc) Option {
	if handler == nil {
		return func(*consumerOptions) error {
			return errors.New("batch handler cannot be nil")
		}
	}
	return func(cfg *consumerOptions) error {
		cfg.batchHandler = handler
		return nil
	}
}
