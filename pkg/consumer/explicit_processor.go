package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// explicitProcessor is a worker-scoped processing component. Slice 7 will own
// its worker wiring: one page caller, one checkpoint runner, and cancellation
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
	flush    chan chan error
	done     chan struct{}
	err      error // written by runCheckpoints, read only after done closes
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
	p := &explicitProcessor{c: c, handlers: handlers, session: session, ctx: ctx, cancel: cancel, interval: interval, flush: make(chan chan error), done: make(chan struct{})}
	p.tracker = newAckTracker(shard, func(ctx context.Context) error {
		err := fenced.Validate(ctx)
		if errors.Is(err, lease.ErrNotOwned) {
			p.stop()
		}
		return err
	})
	go p.runCheckpoints()
	return p, nil
}

// stop is immediate invalidation, not graceful drain. The worker must flush
// while its ownership context is live before calling this on a successful drain.
func (p *explicitProcessor) stop() {
	p.tracker.invalidate()
	// Session invalidation may wait for a save holding the backend lock.
	p.cancel()
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
	attempts := p.c.retryMaxAttempts()
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		// The application owns its slice and may retain or alter it. Keep the
		// original identities separately for atomic retry invalidation.
		cause = p.callAttempt(kind, pending)
		if cause == nil {
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
		case result := <-p.flush:
			err = save(true)
			result <- err
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
	result := make(chan error, 1)
	select {
	case p.flush <- result:
	case <-p.done:
		if p.err != nil {
			return p.err
		}
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
