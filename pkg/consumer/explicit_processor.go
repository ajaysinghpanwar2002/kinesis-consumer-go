package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// explicitProcessorKey carries a shard worker's explicit processor down its own
// processing path, alongside the fenced session it owns. Like shardSessionKey
// it is worker-scoped state rather than a dependency.
type explicitProcessorKey struct{}

func withExplicitProcessor(ctx context.Context, p *explicitProcessor) context.Context {
	if p == nil {
		return ctx
	}
	return context.WithValue(ctx, explicitProcessorKey{}, p)
}

// explicitProcessorFrom returns the calling shard worker's explicit processor,
// or nil when the consumer runs automatic handlers.
func explicitProcessorFrom(ctx context.Context) *explicitProcessor {
	p, _ := ctx.Value(explicitProcessorKey{}).(*explicitProcessor)
	return p
}

// checkpointRequest asks the single checkpoint writer for an out-of-band write:
// an eligible-progress flush, or the shard's terminal completion marker.
type checkpointRequest struct {
	complete bool
	sequence string
	result   chan error
}

// explicitProcessor is a worker-scoped processing component. runShardWorker
// owns its wiring: one page caller, one checkpoint runner, and cancellation
// only when completion work must stop. Successful callback return keeps ctx live.
// No payload lives here: pages and active attempts own their own buffers.
type explicitProcessor struct {
	c        *Consumer
	handlers explicitHandlers
	session  *shardSession
	tracker  *ackTracker
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
	flush    chan checkpointRequest
	done     chan struct{}
	err      error // written by runCheckpoints, read only after done closes

	// lost records that an ownership check answered ErrNotOwned. An
	// asynchronous Ack can observe a takeover before lease renewal does, and
	// the stop it triggers cancels renewal before renewal can report it, so
	// without this the worker would see only the cancellation.
	lost atomic.Bool

	// fetched is the last sequence this acquisition fetched and fully
	// admitted. An expired iterator resumes from it rather than from persisted
	// recovery, which lags behind unacknowledged and unflushed progress.
	fetchedMu sync.Mutex
	fetched   string
}

func newExplicitProcessor(ctx context.Context, c *Consumer, shard string, held lease.Lease, handlers explicitHandlers, interval time.Duration) (*explicitProcessor, error) {
	if err := handlers.validate(nil, nil); err != nil {
		return nil, err
	}
	if interval < 0 {
		return nil, errors.New("checkpoint interval must be positive")
	}
	if interval == 0 {
		interval = time.Second
	}
	if c.admission == nil {
		return nil, errors.New("explicit processing requires admission limits")
	}
	store, ok := c.store.(checkpoint.FencedStore)
	if !ok {
		return nil, errors.New("explicit processing requires a fenced checkpoint store")
	}
	fenced, ok := held.(lease.FencedLease)
	if !ok {
		return nil, errors.New("explicit processing requires a fenced lease")
	}
	var bound checkpoint.Session
	err := c.retryFencedRecovery(ctx, shard, "bind explicit session", func(ctx context.Context) error {
		var err error
		bound, err = store.Bind(ctx, c.coordinationKey(), shard, fenced)
		return err
	})
	if err != nil {
		return nil, c.observeRecoveryFailure(shard, err)
	}
	session := &shardSession{c: c, session: bound, shardID: shard}
	if _, err := session.recovery(ctx); err != nil {
		session.invalidate()
		return nil, err
	}
	ctx, cancel := context.WithCancel(withShardSession(ctx, session))
	p := &explicitProcessor{c: c, handlers: handlers, session: session, ctx: ctx, cancel: cancel, interval: interval, flush: make(chan checkpointRequest), done: make(chan struct{})}
	p.tracker = newAckTracker(shard, func(ctx context.Context) error {
		// Processing is cancelled the instant the worker is stopped — immediate
		// shutdown, drain deadline, shedding, lease loss — and this check is
		// what makes handles stale from that instant rather than once the
		// asynchronous stop finishes invalidating them. Nothing accepted after
		// it could be persisted anyway: the checkpoint runner is stopping too.
		if p.ctx.Err() != nil {
			return ErrStaleDelivery
		}
		err := fenced.Validate(ctx)
		if errors.Is(err, lease.ErrNotOwned) {
			p.lost.Store(true)
			p.stop()
		}
		return err
	})
	go p.runCheckpoints()
	return p, nil
}

// ownershipLost reports whether an ownership check found the lease gone. The
// worker reads it after joining, so the handoff is reported as a handoff rather
// than as the cancellation it produced.
func (p *explicitProcessor) ownershipLost() bool { return p.lost.Load() }

// recordFetched publishes the last sequence a fully admitted page carried.
// Only the shard's processing goroutine writes it, but it is guarded anyway:
// the cost is nothing next to the calls around it.
func (p *explicitProcessor) recordFetched(sequence string) {
	p.fetchedMu.Lock()
	defer p.fetchedMu.Unlock()
	p.fetched = sequence
}

// lastFetched returns the sequence an expired iterator resumes after, or an
// empty string before this acquisition has admitted a whole page.
func (p *explicitProcessor) lastFetched() string {
	p.fetchedMu.Lock()
	defer p.fetchedMu.Unlock()
	return p.fetched
}

// stop is immediate invalidation, not graceful drain. The worker must flush
// while its ownership context is live before calling this on a successful drain.
func (p *explicitProcessor) stop() {
	// Publish cancellation before handles become stale, so internal policy
	// acknowledgments always observe the stop that invalidated them.
	p.cancel()
	p.tracker.invalidate()
	// Session invalidation may wait for a save holding the backend lock.
	p.session.invalidate()
}

// processPage consumes an owned staging slice. The caller holds a fetch slot
// until this function releases it, and must discard the page after any error.
// admissionCtx may stop fetching/admission without canceling completion work.
func (p *explicitProcessor) processPage(admissionCtx context.Context, records []Record, slot *admissionReservation) error {
	defer clear(records)
	defer slot.release()
	if len(records) == 0 {
		return nil
	}
	if p.session.needsInitialPosition() {
		resume, err := p.c.initializeShardRecovery(p.ctx, p.session.shardID, p.session, records[0])
		if err != nil {
			return err
		}
		if resume {
			return fmt.Errorf("explicit page must resume existing recovery position: %w", checkpoint.ErrRecoveryState)
		}
	}
	sizes := make([]int, len(records))
	for i, record := range records {
		sizes[i] = len(record.Data)
	}
	for offset := 0; offset < len(records); {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		limit := min(p.c.admission.limits.MaxBytesPerShard, p.c.admission.limits.MaxBytesPerInstance)
		if sizes[offset] > limit {
			return &OversizedRecordError{ShardID: p.session.shardID, SequenceNumber: aws.ToString(records[offset].SequenceNumber), Bytes: sizes[offset], Limit: limit}
		}
		// Either cancellation source must wake admission, including checkpoint failure.
		waitCtx, cancel := context.WithCancel(admissionCtx)
		stop := context.AfterFunc(p.ctx, cancel)
		reservation, err := p.c.admission.acquireBeforeWait(waitCtx, p.session.shardID, sizes[offset:], false, nil)
		stop()
		cancel()
		if err != nil {
			return err
		}
		deliveries := make([]Delivery, len(reservation.sizes))
		for i := range deliveries {
			deliveries[i] = p.tracker.appendReserved(records[offset+i], func() { reservation.releaseRecord(i) })
			records[offset+i] = Record{}
		}
		offset += len(deliveries)
		if offset == len(records) {
			slot.release()
		}
		err = p.processDeliveries(deliveries)
		clear(deliveries)
		if err != nil {
			p.stop()
			return err
		}
	}
	return nil
}

func (p *explicitProcessor) processDeliveries(deliveries []Delivery) error {
	if p.handlers.batch != nil {
		return p.processAttempt(deliveries)
	}
	for i := range deliveries {
		if err := p.processAttempt(deliveries[i : i+1]); err != nil {
			return err
		}
		deliveries[i] = Delivery{}
	}
	return nil
}

func (p *explicitProcessor) processAttempt(deliveries []Delivery) error {
	pending := append([]Delivery(nil), deliveries...)
	clear(deliveries)
	defer func() { clear(pending) }()
	kind := handlerKindRecord
	if p.handlers.batch != nil {
		kind = handlerKindBatch
	}
	var cause error
	tags := p.c.shardTags(p.session.shardID, metrics.Tag{Key: metricTagHandler, Value: kind})
	attempts := p.c.retryMaxAttempts()
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		if attempt > 1 {
			p.c.reporter.Counter(metricHandlerRetries, 1, tags)
		}
		// The application owns its slice and may retain or alter it. Keep the
		// original identities separately for atomic retry invalidation.
		attemptStart := time.Now()
		cause = p.callAttempt(kind, pending)
		p.c.reporter.Timing(metricHandlerDuration, time.Since(attemptStart), tags)
		if cause == nil {
			// Delivered and returned successfully. Acknowledgment is the
			// application's separate decision and is counted by checkpoints.
			p.c.reporter.Counter(metricRecordsProcessed, int64(len(pending)), tags)
			return p.ctx.Err()
		}
		next := p.tracker.retry(pending)
		clear(pending)
		pending = next
		if err := p.ctx.Err(); err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		if attempt < attempts {
			if err := sleepWithContext(p.ctx, p.c.retryBackoff(attempt)); err != nil {
				return err
			}
		}
	}
	records := make([]Record, len(pending))
	for i := range pending {
		records[i] = pending[i].Record
	}
	defer clear(records)
	err := p.c.applyFailurePolicy(p.ctx, p.session.shardID, kind, records, attempts, cause, fmt.Errorf("explicit handler failed after %d attempts: %w", attempts, cause))
	if err != nil {
		return err
	}
	for _, d := range pending {
		if err := d.Ack(p.ctx); err != nil {
			// A worker stop can invalidate policy handles after Skip/DLQ
			// succeeds. Report that cancellation through the worker's stop
			// path; application errors and live-session stale errors remain
			// failures.
			if errors.Is(err, ErrStaleDelivery) && p.ctx.Err() != nil {
				return p.ctx.Err()
			}
			return err
		}
	}
	if p.c.effectiveFailurePolicy() == FailurePolicySendToDLQ {
		return p.flushCheckpoint(p.ctx)
	}
	return nil
}

// runCheckpoints is the only writer, so an interval, count trigger, or flush
// cannot race another save or regress the persisted count. Bounded retries and
// existing checkpoint metrics are provided by saveShardCheckpoint.
func (p *explicitProcessor) runCheckpoints() {
	defer close(p.done)
	defer p.stop()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	var persisted uint64
	save := func(force bool) error {
		sequence, completed := p.tracker.progress()
		if completed == persisted || (!force && completed-persisted < uint64(max(1, p.c.tuning.checkpointEvery))) {
			return nil
		}
		if err := p.c.saveShardCheckpoint(p.ctx, p.session.shardID, sequence); err != nil {
			return err
		}
		persisted = completed
		return nil
	}
	for {
		var err error
		select {
		case <-p.ctx.Done():
			return
		case <-p.tracker.changed:
			err = save(false)
		case <-ticker.C:
			err = save(true)
		case request := <-p.flush:
			if request.complete {
				err = p.saveCompletion(request.sequence)
			} else {
				err = save(true)
			}
			request.result <- err
		}
		if err != nil {
			// Immediate stop cancels backend I/O deliberately. Preserve actual
			// exhausted writes, but do not report that cancellation as failure.
			if p.ctx.Err() == nil {
				p.err = err
			}
			return
		}
	}
}

func (p *explicitProcessor) flushCheckpoint(ctx context.Context) error {
	return p.requestCheckpoint(ctx, checkpointRequest{})
}

// requestCheckpoint hands one out-of-band write to the checkpoint runner, which
// is the only writer, so it cannot race an interval or count-triggered save.
func (p *explicitProcessor) requestCheckpoint(ctx context.Context, request checkpointRequest) error {
	request.result = make(chan error, 1)
	select {
	case p.flush <- request:
	case <-p.done:
		if p.err != nil {
			return p.err
		}
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// saveCompletion persists the shard's terminal completion marker. Every
// admitted delivery is acknowledged by the time the caller asks for it, so the
// tracker's contiguous prefix is the final sequence; the caller's last fetched
// sequence covers a shard that ended without this worker admitting anything.
func (p *explicitProcessor) saveCompletion(sequence string) error {
	if completed, _ := p.tracker.progress(); completed != "" {
		sequence = completed
	}
	return p.c.saveShardCompletionCheckpoint(p.ctx, p.session.shardID, sequence)
}

// drain finishes a graceful shutdown for one shard: fetching and admission have
// already stopped, so wait for every admitted delivery to be acknowledged and
// then flush the contiguous completed prefix while ownership is still live. The
// caller's context is the worker context, which a drain deadline, immediate
// stop, shedding, or lease loss cancels — releasing the wait and leaving the
// unfinished records replayable.
func (p *explicitProcessor) drain(ctx context.Context) error {
	if err := p.tracker.waitIdle(ctx); err != nil {
		return err
	}
	return p.flushCheckpoint(ctx)
}

// complete ends a closed shard. Staged records are already admitted or
// discarded and callbacks have returned, so only outstanding acknowledgments
// remain; the completion marker is persisted after them, never across them, so
// children stay blocked until this shard's progress is durable.
func (p *explicitProcessor) complete(ctx context.Context, lastSequence string) error {
	if err := p.tracker.waitIdle(ctx); err != nil {
		return err
	}
	return p.requestCheckpoint(ctx, checkpointRequest{complete: true, sequence: lastSequence})
}

// A callback that ignores cancellation may outlive this call, but owns only
// its application slice; stop invalidates its handles before processing exits.
func (p *explicitProcessor) callAttempt(kind string, pending []Delivery) error {
	result := make(chan error, 1)
	given := append([]Delivery(nil), pending...)
	go func() {
		result <- p.c.callHandlerAttempt(p.session.shardID, kind, func() error {
			if p.handlers.batch != nil {
				return p.handlers.batch(p.ctx, given)
			}
			return p.handlers.record(p.ctx, given[0])
		})
	}()
	select {
	case err := <-result:
		if p.ctx.Err() != nil {
			return p.ctx.Err()
		}
		return err
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// wait reports a checkpoint failure after the runner has invalidated the
// session and woken processing/admission. The worker must propagate this error.
func (p *explicitProcessor) wait() error {
	<-p.done
	return p.err
}
