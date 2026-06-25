package consumer

import (
	"errors"

	"github.com/pratilipi/kinesis-consumer-go/pkg/checkpoint"
)

// Consumer owns Kinesis shard consumption for one worker process.
type Consumer struct {
	cfg          Config
	client       kinesisAPI
	store        checkpoint.Store
	handler      HandlerFunc
	batchHandler BatchHandlerFunc
	tuning       tuningConfig
}

// New validates consumer dependencies and returns a configured Consumer.
func New(cfg Config, client *Client, store checkpoint.Store, handler HandlerFunc, opts ...Option) (*Consumer, error) {
	if err := validateConstructorInputs(client, store); err != nil {
		return nil, err
	}

	opt, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	handler, batchHandler, err := resolveHandlers(handler, opt)
	if err != nil {
		return nil, err
	}

	resolvedCfg, err := finalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	if err := opt.tuning.validate(); err != nil {
		return nil, err
	}

	return &Consumer{
		cfg:          resolvedCfg,
		client:       client,
		store:        store,
		handler:      handler,
		batchHandler: batchHandler,
		tuning:       opt.tuning,
	}, nil
}

func validateConstructorInputs(client *Client, store checkpoint.Store) error {
	if client == nil {
		return errors.New("kinesis client is required")
	}
	if store == nil {
		return errors.New("checkpoint store is required")
	}
	return nil
}

func resolveHandlers(handler HandlerFunc, opt options) (HandlerFunc, BatchHandlerFunc, error) {
	if handler == nil && opt.batchHandler == nil {
		return nil, nil, errors.New("handler is required (provide WithBatchHandler for batch processing)")
	}
	return handler, opt.batchHandler, nil
}

func finalizeConfig(cfg Config) (Config, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
