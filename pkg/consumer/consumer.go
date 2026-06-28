package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pratilipi/kinesis-consumer-go/pkg/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
)

// Consumer owns Kinesis shard consumption for one worker process.
type Consumer struct {
	cfg          Config
	client       kinesisAPI
	store        checkpoint.Store
	handler      HandlerFunc
	batchHandler BatchHandlerFunc
	leaseManager lease.Manager
	leaseOwner   string
	tuning       tuningConfig

	processShardRecordsPassFn func(context.Context, string, int) (string, int, error)
	processShardRecordsLoopFn func(context.Context, string) (string, int, error)
	sleepFn                   func(context.Context, time.Duration) error
}

// Start begins the consumer lifecycle and blocks until the context is done.
func (c *Consumer) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		c.workerHeartbeatLoop(runCtx)
	}()
	defer func() {
		cancel()
		<-heartbeatDone
	}()

	shards, err := c.listShards(runCtx)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards found for stream %s", c.streamKey())
	}

	shardMap := make(map[string]types.Shard, len(shards))
	mergeKnownShards(shardMap, shards)

	workerErrCh := make(chan error, len(shardMap))
	workerDone := make(chan struct{})
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	completionState := newShardCompletionState()
	if err := c.acquireAndStartReadyShardWorkers(
		runCtx,
		shardMap,
		completionState,
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	); err != nil {
		return err
	}
	go func() {
		defer close(workerDone)
		workerWG.Wait()
	}()

	select {
	case err := <-workerErrCh:
		cancel()
		workers.stopAll()
		<-workerDone
		return err
	case <-runCtx.Done():
		workers.stopAll()
	}
	<-workerDone

	select {
	case err := <-workerErrCh:
		return err
	default:
	}
	if errors.Is(runCtx.Err(), context.Canceled) {
		return nil
	}
	return runCtx.Err()
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

	leaseManager, leaseOwner, err := leaseSettings(store, opt)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		cfg:          resolvedCfg,
		client:       client,
		store:        store,
		handler:      handler,
		batchHandler: batchHandler,
		leaseManager: leaseManager,
		leaseOwner:   leaseOwner,
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

func leaseSettings(store checkpoint.Store, opt options) (lease.Manager, string, error) {
	manager := opt.lease.manager
	if manager == nil {
		if provider, ok := store.(lease.Provider); ok {
			auto, err := provider.LeaseManager()
			if err != nil {
				return nil, "", fmt.Errorf("create lease manager from store: %w", err)
			}
			manager = auto
		}
	}
	if manager == nil {
		return nil, "", errors.New("lease manager is required; use a store that provides leasing or WithLeaseManager")
	}

	return manager, defaultOwner(), nil
}

func defaultOwner() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}
