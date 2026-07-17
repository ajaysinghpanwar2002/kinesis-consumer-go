package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// Consumer owns Kinesis shard consumption for one worker process.
type Consumer struct {
	cfg                  Config
	client               KinesisAPI
	store                checkpoint.Store
	handler              HandlerFunc
	batchHandler         BatchHandlerFunc
	failurePolicy        FailurePolicy
	dlqPublisher         DLQPublisher
	leaseManager         lease.Manager
	leaseOwner           string
	streamName           string
	coordinationIdentity string
	gracefulDrain        bool
	drainTimeout         time.Duration
	draining             atomic.Bool
	tuning               tuningConfig
	logger               *slog.Logger
	reporter             metrics.Reporter

	processShardRecordsPassFn func(context.Context, string, int, string) (string, int, string, error)
	processShardRecordsLoopFn func(context.Context, string) (string, int, error)
	sleepFn                   func(context.Context, time.Duration) error
}

// Start begins the consumer lifecycle and blocks until ctx is cancelled — a
// clean shutdown that returns nil after any graceful drain — or a fatal error
// stops the consumer and is returned.
//
// A Consumer is single-use. Start installs no re-entry guard: calling it more
// than once concurrently runs duplicate heartbeat and rebalance loops under
// the same owner identity and double-processes shards. Reusing a Consumer for
// a second run after Start returns is likewise unsupported; construct a fresh
// Consumer with New to run again.
func (c *Consumer) Start(ctx context.Context) (err error) {
	c.logger.Info("consumer starting")
	// Registered first so it runs last, after the lifecycle-cleanup defers
	// below, and observes the final returned error. Every fatal path
	// (including no shards found) lands in the error branch.
	defer func() {
		if err != nil {
			c.logger.Error("consumer stopped", slog.Any("error", err))
			return
		}
		c.logger.Info("consumer stopped")
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The worker-liveness heartbeat gets its own lifetime, ending only when
	// Start returns — NOT when runCtx is cancelled. During a graceful drain
	// (which begins after runCtx is done) workers are still checkpointing and
	// renewing; if the heartbeat died with runCtx, peers would drop this
	// worker from their rebalance snapshots after heartbeatTTL and claim its
	// shards away mid-drain.
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		c.workerHeartbeatLoop(heartbeatCtx)
	}()
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
	}()

	shards, err := c.listShards(runCtx)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("%w for stream %s", ErrNoShards, c.canonicalStreamName())
	}

	shardMap := make(map[string]types.Shard, len(shards))
	mergeKnownShards(shardMap, shards)

	workerErrCh := make(chan error, len(shardMap))
	workers := newShardWorkerSet()
	var workerWG sync.WaitGroup
	completionState := newShardCompletionState()
	rebalanceCooldown := make(map[string]time.Time)
	if err := c.acquireAndStartReadyShardWorkers(
		runCtx,
		shardMap,
		completionState,
		rebalanceCooldown,
		time.Now(),
		workers,
		&workerWG,
		workerErrCh,
		cancel,
	); err != nil {
		return err
	}

	orchestrationDone := make(chan struct{})
	go func() {
		defer close(orchestrationDone)
		if err := c.refreshAndRebalanceShardWorkersLoop(
			runCtx,
			c.tuning.shardSyncInterval,
			newRebalanceDelayFunc(c.tuning.rebalanceIntervalMin, c.tuning.rebalanceIntervalJitter),
			shardMap,
			completionState,
			rebalanceCooldown,
			workers,
			&workerWG,
			workerErrCh,
			cancel,
			time.Now,
		); err != nil {
			if errors.Is(err, context.Canceled) {
				if errors.Is(runCtx.Err(), context.Canceled) {
					return
				}
				if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
					err = runCtx.Err()
				}
			}
			select {
			case workerErrCh <- err:
			default:
			}
			cancel()
		}
	}()

	select {
	case err := <-workerErrCh:
		cancel()
		<-orchestrationDone
		stopAndReapShardWorkers(workers, &workerWG)
		return err
	case <-runCtx.Done():
		cancel()
		<-orchestrationDone
		if ctx.Err() == nil {
			stopAndReapShardWorkers(workers, &workerWG)
			select {
			case err := <-workerErrCh:
				return err
			default:
			}
			return runCtx.Err()
		}
		if c.gracefulDrain {
			if err := c.drainShardWorkers(workers, &workerWG, workerErrCh); err != nil {
				return err
			}
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		} else {
			stopAndReapShardWorkers(workers, &workerWG)
		}
	}

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

// New validates consumer dependencies and returns a configured Consumer. The
// client is any KinesisAPI implementation (a *kinesis.Client, or a test
// double / instrumented wrapper); it must be non-nil.
func New(cfg Config, client KinesisAPI, store checkpoint.Store, handler HandlerFunc, opts ...Option) (*Consumer, error) {
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

	resolvedCfg, streamName, err := finalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	if err := opt.tuning.validate(); err != nil {
		return nil, err
	}
	if opt.failurePolicy == FailurePolicySendToDLQ && opt.dlqPublisher == nil {
		return nil, errors.New("dlq publisher is required when failure policy is send-to-dlq")
	}

	leaseManager, leaseOwner, err := leaseSettings(store, opt)
	if err != nil {
		return nil, err
	}

	coordinationIdentity := resolvedCfg.ConsumerGroup + ":" + streamName
	logger := opt.logger.With(
		slog.String("stream", streamName),
		slog.String("consumer_group", resolvedCfg.ConsumerGroup),
	)

	return &Consumer{
		cfg:                  resolvedCfg,
		client:               client,
		store:                store,
		handler:              handler,
		batchHandler:         batchHandler,
		failurePolicy:        opt.failurePolicy,
		dlqPublisher:         opt.dlqPublisher,
		leaseManager:         leaseManager,
		leaseOwner:           leaseOwner,
		streamName:           streamName,
		coordinationIdentity: coordinationIdentity,
		gracefulDrain:        opt.shutdown.gracefulDrain,
		drainTimeout:         opt.shutdown.gracefulDrainTimeout,
		tuning:               opt.tuning,
		logger:               logger,
		reporter:             opt.reporter,
	}, nil
}

func validateConstructorInputs(client KinesisAPI, store checkpoint.Store) error {
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
	if handler != nil && opt.batchHandler != nil {
		return nil, nil, errors.New("provide either a record handler or WithBatchHandler, not both")
	}
	return handler, opt.batchHandler, nil
}

func finalizeConfig(cfg Config) (Config, string, error) {
	cfg = cfg.withDefaults()
	streamName, err := cfg.validateAndResolveStreamName()
	if err != nil {
		return Config{}, "", err
	}
	return cfg, streamName, nil
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
