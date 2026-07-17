package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	// ownedLeaseManagerCloser is set only when New auto-created the lease
	// manager from the store's lease.Provider and that manager is closable;
	// it is what Close releases. A manager supplied via WithLeaseManager stays
	// caller-owned and is never recorded here.
	ownedLeaseManagerCloser io.Closer

	// syncHealth backs Health().ShardSync (written by the orchestration
	// loop); heartbeatHealth backs Health().Heartbeat (written by the
	// heartbeat loop).
	syncHealth      healthSignalState
	heartbeatHealth healthSignalState

	// lifecycleMu guards the closed/start/run state shared between Start and
	// Close. startClaimed is permanent: endRun clears only the active handles,
	// never permission to reuse this Consumer.
	lifecycleMu  sync.Mutex
	closed       bool
	startClaimed bool
	closeDone    chan struct{}
	closeErr     error
	runCancel    context.CancelFunc
	runDone      chan struct{}

	processShardRecordsPassFn func(context.Context, string, int, string) (string, int, string, error)
	processShardRecordsLoopFn func(context.Context, string) (string, int, error)
	sleepFn                   func(context.Context, time.Duration) error
}

// Start begins the consumer lifecycle and blocks until ctx is cancelled — a
// clean shutdown that returns nil after any graceful drain — or a fatal error
// stops the consumer and is returned.
//
// A Consumer is single-use. The first Start call claims it permanently;
// concurrent or sequential calls after that return ErrConsumerAlreadyStarted,
// even after the first run has returned. Construct a fresh Consumer with New
// to run again.
//
// Start returns ErrConsumerClosed when the Consumer has been closed — whether
// Close was called before Start or Close stopped this run mid-flight.
//
// Start returns the causal failure wrapped in ErrHeartbeatStale when the
// worker-liveness heartbeat keeps failing until one heartbeat interval
// before the last successful heartbeat's TTL lapses: past that point peers
// may treat this worker as dead and claim its shards away, so the run stops
// inside the safety margin instead of dual-processing.
func (c *Consumer) Start(ctx context.Context) (err error) {
	runCtx, cancel, runDone, beginErr := c.beginRun(ctx)
	if beginErr != nil {
		return beginErr
	}

	c.logger.Info("consumer starting")
	// Registered before the remaining lifecycle-cleanup defers so it runs last
	// and observes the final returned error. Every fatal path (including no
	// shards found) lands in the error branch.
	defer func() {
		if err != nil {
			c.logger.Error("consumer stopped", slog.Any("error", err))
			return
		}
		c.logger.Info("consumer stopped")
	}()

	// endRun signals Close that this run has fully finished. It must run after
	// the heartbeat-stop defer below (deferred calls run LIFO), so Close can
	// never release the lease manager while the heartbeat loop might still use
	// it.
	defer c.endRun(runDone)
	defer cancel()

	// The worker-liveness heartbeat gets its own lifetime, ending only when
	// Start returns — NOT when runCtx is cancelled. During a graceful drain
	// (which begins after runCtx is done) workers are still checkpointing and
	// renewing; if the heartbeat died with runCtx, peers would drop this
	// worker from their rebalance snapshots after heartbeatTTL and claim its
	// shards away mid-drain.
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	// heartbeatFatal carries a heartbeat-validity loss (ErrHeartbeatStale) to
	// the run-exit paths below. It is dedicated (not workerErrCh, which does
	// not exist yet when the loop starts) and published before the cancel so
	// the woken run always observes the causal error.
	heartbeatFatal := make(chan error, 1)
	go func() {
		defer close(heartbeatDone)
		c.workerHeartbeatLoop(heartbeatCtx, func(hbErr error) {
			select {
			case heartbeatFatal <- hbErr:
			default:
			}
			cancel()
		})
	}()
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
	}()
	takeHeartbeatFatal := func() error {
		select {
		case hbErr := <-heartbeatFatal:
			return hbErr
		default:
			return nil
		}
	}
	// normalizeInternalCancel maps a context.Canceled failure caused by an
	// internal run cancellation (caller ctx still live) back to its cause:
	// heartbeat-validity loss first, then Close (via closeInterruptedStartup).
	normalizeInternalCancel := func(err error) error {
		if errors.Is(err, context.Canceled) && ctx.Err() == nil {
			if hbErr := takeHeartbeatFatal(); hbErr != nil {
				return hbErr
			}
		}
		return c.closeInterruptedStartup(ctx, err)
	}

	shards, err := c.listShards(runCtx)
	if err != nil {
		return normalizeInternalCancel(err)
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
		return normalizeInternalCancel(err)
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
		// A published heartbeat-validity loss is the root cause of whatever
		// the workers it cancelled reported while stopping — the cancellation
		// echoed back as context.Canceled, or cleanup like a lease release
		// failing against the same dead backend. Return the causal heartbeat
		// error, never a consequence, whichever branch wins the exit race.
		if hbErr := takeHeartbeatFatal(); hbErr != nil {
			return hbErr
		}
		return normalizeInternalCancel(err)
	case <-runCtx.Done():
		cancel()
		<-orchestrationDone
		if ctx.Err() == nil {
			stopAndReapShardWorkers(workers, &workerWG)
			// Heartbeat-validity loss is checked before worker errors: the
			// stale heartbeat is the root cause and the force-stopped
			// workers' failures its consequence.
			if hbErr := takeHeartbeatFatal(); hbErr != nil {
				return hbErr
			}
			select {
			case err := <-workerErrCh:
				return err
			default:
			}
			// The run was cancelled internally with the caller's ctx still
			// live: either Close stopped the run, or an orchestration error
			// (drained above) did.
			if c.isClosed() {
				return ErrConsumerClosed
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

// Close releases the resources the Consumer created itself: the lease manager
// auto-created from the checkpoint store's lease.Provider, when there is one.
// A lease manager supplied via WithLeaseManager (and the checkpoint store,
// which is always caller-supplied) remains caller-owned and is never closed.
//
// If Start is running, Close stops it first: the run is cancelled promptly
// (workers are force-stopped without a graceful drain) and Close blocks until
// Start has returned — heartbeat and orchestration loops stopped — before the
// lease manager is closed; that Start call returns ErrConsumerClosed. If the
// caller's ctx was already cancelled and a graceful drain is in progress,
// Close waits for the drain to finish rather than cutting it short. After
// Close, the first Start call returns ErrConsumerClosed. Once a Start call has
// claimed the Consumer, later Start calls return ErrConsumerAlreadyStarted,
// including after Close.
//
// A worker abandoned by the bounded shutdown (stuck in an uncooperative
// callback that ignores its context) may outlive Close; its late lease
// Release then fails against the closed manager, which is harmless — the
// lease simply expires by TTL, as it would after a crash.
//
// Close is idempotent and safe for concurrent use: cleanup runs exactly once,
// and every call blocks until that cleanup completes and returns its result —
// non-nil only when closing the owned lease manager fails.
func (c *Consumer) Close() error {
	c.lifecycleMu.Lock()
	if c.closed {
		done := c.closeDone
		c.lifecycleMu.Unlock()
		<-done
		return c.closeErr
	}
	c.closed = true
	c.closeDone = make(chan struct{})
	cancel, runDone := c.runCancel, c.runDone
	c.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
		<-runDone
	}

	var err error
	if c.ownedLeaseManagerCloser != nil {
		if closeErr := c.ownedLeaseManagerCloser.Close(); closeErr != nil {
			err = fmt.Errorf("close lease manager: %w", closeErr)
		}
	}
	// closeErr must be written before closeDone is closed so waiters in the
	// closed branch above read it safely.
	c.closeErr = err
	close(c.closeDone)
	return err
}

// beginRun permanently claims the Consumer's single Start and registers its
// active run so Close can stop it. A previous Start takes precedence over the
// closed state so reuse has one stable result even after Close.
func (c *Consumer) beginRun(ctx context.Context) (context.Context, context.CancelFunc, chan struct{}, error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.startClaimed {
		return nil, nil, nil, ErrConsumerAlreadyStarted
	}
	if c.closed {
		return nil, nil, nil, ErrConsumerClosed
	}
	c.startClaimed = true
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	c.runCancel = cancel
	c.runDone = runDone
	return runCtx, cancel, runDone, nil
}

// endRun deregisters the run and unblocks a Close waiting for it to finish.
func (c *Consumer) endRun(runDone chan struct{}) {
	c.lifecycleMu.Lock()
	c.runCancel = nil
	c.runDone = nil
	c.lifecycleMu.Unlock()
	close(runDone)
}

// closeInterruptedStartup normalizes a startup failure caused by Close
// cancelling the run to ErrConsumerClosed, matching what Start returns when
// Close interrupts it after startup. Genuine dependency failures (any
// non-Canceled error), caller-ctx cancellation, and cancellations with no
// Close in flight pass through unchanged. Close publishes its closed state
// before cancelling the run, so a Close-triggered cancellation always
// observes isClosed here.
func (c *Consumer) closeInterruptedStartup(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) && ctx.Err() == nil && c.isClosed() {
		return ErrConsumerClosed
	}
	return err
}

func (c *Consumer) isClosed() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.closed
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
	// Derived (not fixed in defaultTuning) so a WithPolling override scales
	// the staleness bound with it instead of racing a fixed default.
	if opt.tuning.shardSyncMaxStaleness == 0 {
		opt.tuning.shardSyncMaxStaleness = 10 * opt.tuning.shardSyncInterval
	}
	if opt.failurePolicy == FailurePolicySendToDLQ && opt.dlqPublisher == nil {
		return nil, errors.New("dlq publisher is required when failure policy is send-to-dlq")
	}

	leaseManager, leaseOwner, ownedCloser, err := leaseSettings(store, opt)
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

		ownedLeaseManagerCloser: ownedCloser,
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

// leaseSettings resolves the lease manager, preferring an explicit
// WithLeaseManager and falling back to one auto-created from the store's
// lease.Provider. Only in the fallback case does the Consumer own the
// manager's lifetime: when that auto-created manager is closable it is
// returned as the owned closer for Close to release.
func leaseSettings(store checkpoint.Store, opt options) (lease.Manager, string, io.Closer, error) {
	manager := opt.lease.manager
	var owned io.Closer
	if manager == nil {
		if provider, ok := store.(lease.Provider); ok {
			auto, err := provider.LeaseManager()
			if err != nil {
				return nil, "", nil, fmt.Errorf("create lease manager from store: %w", err)
			}
			manager = auto
			if closer, ok := auto.(io.Closer); ok {
				owned = closer
			}
		}
	}
	if manager == nil {
		return nil, "", nil, errors.New("lease manager is required; use a store that provides leasing or WithLeaseManager")
	}

	return manager, defaultOwner(), owned, nil
}

func defaultOwner() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}
