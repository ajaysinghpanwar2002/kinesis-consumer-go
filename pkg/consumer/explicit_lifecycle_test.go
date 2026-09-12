package consumer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// explicitDeliveries collects deliveries the way an application completion
// worker does: the handler returns without acknowledging, and the test
// acknowledges later, from outside the callback that produced the delivery.
type explicitDeliveries struct {
	mu         sync.Mutex
	deliveries []Delivery
	contexts   []context.Context
	onDeliver  func(Delivery)
}

func (d *explicitDeliveries) handle(ctx context.Context, delivery Delivery) error {
	d.mu.Lock()
	d.deliveries = append(d.deliveries, delivery)
	d.contexts = append(d.contexts, ctx)
	onDeliver := d.onDeliver
	d.mu.Unlock()
	if onDeliver != nil {
		onDeliver(delivery)
	}
	return nil
}

func (d *explicitDeliveries) snapshot() []Delivery {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.deliveries)
}

func (d *explicitDeliveries) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.deliveries)
}

func (d *explicitDeliveries) sequences() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.deliveries))
	for _, delivery := range d.deliveries {
		out = append(out, aws.ToString(delivery.Record.SequenceNumber))
	}
	return out
}

// contextsLive reports whether every handler context is still usable. A
// successful explicit handler must not have its context cancelled out from
// under the asynchronous acknowledgment it is still waiting on.
func (d *explicitDeliveries) contextsLive() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ctx := range d.contexts {
		if ctx.Err() != nil {
			return false
		}
	}
	return true
}

func (d *explicitDeliveries) ackAll(ctx context.Context) error {
	for _, delivery := range d.snapshot() {
		if err := delivery.Ack(ctx); err != nil {
			return err
		}
	}
	return nil
}

func newExplicitTestConsumer(
	t *testing.T,
	stream *fakeStream,
	store checkpoint.Store,
	manager lease.Manager,
	handler func(context.Context, Delivery) error,
	opts ...Option,
) *Consumer {
	t.Helper()
	return newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, nil,
		append([]Option{WithExplicitHandler(handler)}, opts...)...)
}

func storedCheckpoint(t *testing.T, store *checkpoint.MemoryStore) string {
	t.Helper()
	stored, err := store.Get(context.Background(), testCoordKey, testShardID)
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	return stored
}

// awaitAdmissionStop blocks until shutdown has stopped admission. Waiting on
// the drain flag alone is not enough: capacity released a moment earlier can
// still be granted to a shard blocked mid-page, which is exactly the staging a
// drain must discard.
func awaitAdmissionStop(cons *Consumer) error {
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		if cons.admission != nil && cons.admission.stopCtx.Err() != nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("admission was never stopped")
}

func TestExplicitGracefulDrainWaitsForAcknowledgmentsThenFlushes(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collected := &explicitDeliveries{}
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle,
		WithGracefulDrain(5*time.Second))

	stop := startConsumer(t, cons)
	waitFor(t, "three deliveries", func() bool { return collected.count() >= 3 })

	// Acknowledge only once the drain is under way. The flush has to wait for
	// work admitted before shutdown, and the completion contexts of handlers
	// that already returned successfully must still be live while it does.
	acked := make(chan error, 1)
	go func() {
		if err := awaitAdmissionStop(cons); err != nil {
			acked <- err
			return
		}
		if !collected.contextsLive() {
			acked <- errors.New("drain cancelled a completion context still owed an acknowledgment")
			return
		}
		acked <- collected.ackAll(context.Background())
	}()

	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil after a graceful drain", err)
	}
	if err := <-acked; err != nil {
		t.Fatal(err)
	}

	if stored := storedCheckpoint(t, store); stored != "102" {
		t.Fatalf("checkpoint = %q, want the acknowledged prefix flushed before the lease is released", stored)
	}
}

func TestExplicitGracefulDrainDiscardsUnadmittedStaging(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collected := &explicitDeliveries{}
	// One record per shard, so the page stalls after its first delivery and
	// the remaining two stay staged and unadmitted until shutdown drops them.
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle,
		WithGracefulDrain(5*time.Second),
		WithInFlightLimits(InFlightLimits{MaxRecordsPerShard: 1}))

	stop := startConsumer(t, cons)
	waitFor(t, "the first delivery", func() bool { return collected.count() >= 1 })

	acked := make(chan error, 1)
	go func() {
		if err := awaitAdmissionStop(cons); err != nil {
			acked <- err
			return
		}
		acked <- collected.ackAll(context.Background())
	}()

	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil after a graceful drain", err)
	}
	if err := <-acked; err != nil {
		t.Fatal(err)
	}

	if got := collected.sequences(); !slices.Equal(got, []string{"100"}) {
		t.Fatalf("delivered = %v, want only the admitted record", got)
	}
	if stored := storedCheckpoint(t, store); stored != "100" {
		t.Fatalf("checkpoint = %q, want no advance past records the drain discarded", stored)
	}
}

func TestExplicitDrainTimeoutInvalidatesOutstandingDeliveries(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	// Nothing is ever acknowledged, so the drain can only end at its deadline.
	// stopAll is also what shedding and Close use to stop a worker.
	collected := &explicitDeliveries{}
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle,
		WithGracefulDrain(50*time.Millisecond))

	stop := startConsumer(t, cons)
	waitFor(t, "three deliveries", func() bool { return collected.count() >= 3 })

	if err := stop(); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Start() error = %v, want wraps %v", err, ErrDrainTimeout)
	}
	for _, delivery := range collected.snapshot() {
		if err := delivery.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
			t.Fatalf("Ack() error = %v, want %v: a timed-out drain invalidates its handles", err, ErrStaleDelivery)
		}
	}
	if stored := storedCheckpoint(t, store); stored != "" {
		t.Fatalf("checkpoint = %q, want nothing persisted for records that were never acknowledged", stored)
	}
	if position := readRecovery(t, store, manager); position.Kind != checkpoint.RecoveryInitial || position.Sequence != "100" {
		t.Fatalf("recovery position = %+v, want every record left replayable", position)
	}
}

func TestExplicitImmediateStopLeavesRecordsReplayable(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collected := &explicitDeliveries{}
	// No WithGracefulDrain: shutdown cancels processing instead of draining it.
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "three deliveries", func() bool { return collected.count() >= 3 })

	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	for _, delivery := range collected.snapshot() {
		if err := delivery.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
			t.Fatalf("Ack() error = %v, want %v after an immediate stop", err, ErrStaleDelivery)
		}
	}
	if stored := storedCheckpoint(t, store); stored != "" {
		t.Fatalf("checkpoint = %q, want no progress from an immediate stop", stored)
	}
}

func TestExplicitShardCompletionWaitsForAcknowledgments(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 2)...)
	stream.closed = true
	collected := &explicitDeliveries{}
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "both deliveries", func() bool { return collected.count() >= 2 })

	delivered := collected.snapshot()
	if err := delivered[0].Ack(context.Background()); err != nil {
		t.Fatalf("Ack() error = %v, want nil", err)
	}
	// The first acknowledgment checkpoints, which proves the shard worker is
	// still running and waiting rather than already finished. Completion may
	// not be persisted while the second record is still outstanding: children
	// of a closed shard resume from that marker.
	waitFor(t, "the first acknowledgment to be checkpointed", func() bool {
		return storedCheckpoint(t, store) == "100"
	})

	if err := delivered[1].Ack(context.Background()); err != nil {
		t.Fatalf("Ack() error = %v, want nil", err)
	}
	waitFor(t, "the completion marker", func() bool {
		return storedCheckpoint(t, store) == shardCompletionValue("101")
	})
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
}

func TestExplicitLeaseLossInvalidatesDeliveriesAndStopsCleanly(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 2)...)

	var cons *Consumer
	var stolen sync.Once
	collected := &explicitDeliveries{onDeliver: func(Delivery) {
		stolen.Do(func() {
			if _, claimed, err := manager.Claim(
				context.Background(), testCoordKey, testShardID, cons.leaseOwner, "peer", time.Minute,
			); err != nil || !claimed {
				t.Errorf("Claim() = (%v, %v), want a successful takeover", claimed, err)
			}
		})
	}}
	cons = newExplicitTestConsumer(t, stream, store, manager, collected.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "the outstanding delivery to be invalidated", func() bool {
		delivered := collected.snapshot()
		return len(delivered) > 0 && errors.Is(delivered[0].Ack(context.Background()), ErrStaleDelivery)
	})
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil: losing a shard is a handoff, not a consumer failure", err)
	}
	if stored := storedCheckpoint(t, store); stored != "" {
		t.Fatalf("checkpoint = %q, want no write from a worker whose lease moved on", stored)
	}
}

func TestExplicitExhaustedCheckpointWritesStopTheRun(t *testing.T) {
	memory, manager := fencedBackends()
	faults := &explicitFaultSession{failures: 1 << 30}
	stream := newFakeStream(testShardID, sequences(100, 2)...)
	collected := &explicitDeliveries{onDeliver: func(delivery Delivery) {
		_ = delivery.Ack(context.Background())
	}}
	cons := newExplicitTestConsumer(t, stream,
		&explicitFaultStore{FencedStore: memory, session: faults}, manager, collected.handle)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cons.Start(ctx); !errors.Is(err, errExplicitCheckpointUnavailable) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errExplicitCheckpointUnavailable)
	}
}

func TestNewRejectsExplicitModeWithoutFencedDependencies(t *testing.T) {
	handler := func(context.Context, Delivery) error { return nil }
	manager := lease.NewMemoryManager()
	stream := newFakeStream(testShardID)

	t.Run("unfenced store", func(t *testing.T) {
		_, err := New(trimHorizonConfig(), stream, fakeCheckpointStore{}, nil,
			WithLeaseManager(manager), WithExplicitHandler(handler))
		if err == nil {
			t.Fatal("New() error = nil, want a rejection: explicit acknowledgment requires fenced sessions")
		}
	})

	t.Run("shard concurrency", func(t *testing.T) {
		_, err := New(trimHorizonConfig(), stream, checkpoint.NewMemoryStoreWithLeaseManager(manager), nil,
			WithLeaseManager(manager), WithExplicitHandler(handler), WithShardConcurrency(2))
		if err == nil {
			t.Fatal("New() error = nil, want a rejection: shard concurrency is an automatic-mode setting")
		}
	})

	// This is also the dependency wiring the package's shutdown example uses: a
	// memory fenced pair completed by WithLeaseManager, because the memory
	// store does not provide the consumer's lease manager itself.
	t.Run("explicit admission defaults", func(t *testing.T) {
		cons, err := New(trimHorizonConfig(), stream, checkpoint.NewMemoryStoreWithLeaseManager(manager), nil,
			WithLeaseManager(manager), WithExplicitHandler(handler))
		if err != nil {
			t.Fatalf("New() error = %v, want nil", err)
		}
		want := InFlightLimits{
			MaxRecordsPerShard:    1000,
			MaxBytesPerShard:      10 << 20,
			MaxRecordsPerInstance: 10000,
			MaxBytesPerInstance:   64 << 20,
			MaxFetchSlots:         4,
		}
		if cons.admission == nil || cons.admission.limits != want {
			t.Fatalf("admission limits = %+v, want the documented explicit defaults %+v", cons.admission, want)
		}
	})
}

func TestExplicitModeRejectsLeaseManagerThatIsNotAFencedPair(t *testing.T) {
	storeManager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStoreWithLeaseManager(storeManager)
	consumerManager := lease.NewMemoryManager()
	stream := newFakeStream(testShardID, sequences(100, 1)...)

	cons := newExplicitTestConsumer(t, stream, store, consumerManager,
		func(context.Context, Delivery) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Automatic mode downgrades this pairing to unfenced processing. Explicit
	// mode cannot: without fencing an acknowledgment proves nothing.
	if err := cons.Start(ctx); !errors.Is(err, lease.ErrLeaseMismatch) {
		t.Fatalf("Start() error = %v, want wraps %v", err, lease.ErrLeaseMismatch)
	}
}

func TestExplicitGracefulDrainCompletesTimerDrivenBatches(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)

	// An application batcher that only ever flushes on its timer, started
	// before the consumer and stopped after it. The drain has to accommodate
	// that cadence: records admitted before shutdown are acknowledged by a
	// worker whose lifetime is not the shutdown signal.
	pending := make(chan Delivery, 16)
	batcherCtx, stopBatcher := context.WithCancel(context.Background())
	defer stopBatcher()
	batcherDone := make(chan struct{})
	go func() {
		defer close(batcherDone)
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		var batch []Delivery
		for {
			select {
			case delivery := <-pending:
				batch = append(batch, delivery)
			case <-ticker.C:
				for _, delivery := range batch {
					if err := delivery.Ack(context.Background()); err != nil {
						t.Errorf("Ack() error = %v, want nil", err)
					}
				}
				batch = nil
			case <-batcherCtx.Done():
				return
			}
		}
	}()

	var submitted atomic.Int64
	cons := newExplicitTestConsumer(t, stream, store, manager,
		func(ctx context.Context, delivery Delivery) error {
			select {
			case pending <- delivery:
				submitted.Add(1)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		WithGracefulDrain(5*time.Second))

	stop := startConsumer(t, cons)
	waitFor(t, "three deliveries to reach the batcher", func() bool { return submitted.Load() >= 3 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	stopBatcher()
	<-batcherDone

	if stored := storedCheckpoint(t, store); stored != "102" {
		t.Fatalf("checkpoint = %q, want the drain to wait for the batcher's next timed flush", stored)
	}
}

func TestExplicitGracefulDrainDoesNotCompleteAClosedShard(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	stream.closed = true
	collected := &explicitDeliveries{}
	// The page stalls after its first delivery, so the shard is closed but its
	// remaining records were never admitted. Finishing the shard would
	// checkpoint past records nothing delivered.
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle,
		WithGracefulDrain(5*time.Second),
		WithInFlightLimits(InFlightLimits{MaxRecordsPerShard: 1}))

	stop := startConsumer(t, cons)
	waitFor(t, "the first delivery", func() bool { return collected.count() >= 1 })

	acked := make(chan error, 1)
	go func() {
		if err := awaitAdmissionStop(cons); err != nil {
			acked <- err
			return
		}
		acked <- collected.ackAll(context.Background())
	}()

	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil after a graceful drain", err)
	}
	if err := <-acked; err != nil {
		t.Fatal(err)
	}

	if stored := storedCheckpoint(t, store); stored != "100" {
		t.Fatalf("checkpoint = %q, want the acknowledged prefix and no completion marker", stored)
	}
}

func TestExplicitAcknowledgedLeaseLossStopsOnlyTheShard(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collected := &explicitDeliveries{}
	reporter := &recordingReporter{}
	// One record per shard, so the page is blocked waiting for capacity when
	// the takeover happens: the acknowledgment's ownership check is the only
	// thing that can notice it, and the blocked wait is what reports the
	// cancellation it causes. Renewal is slowed so it cannot notice first.
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle,
		WithHeartbeat(time.Second, 3*time.Second),
		WithMetrics(reporter),
		WithInFlightLimits(InFlightLimits{MaxRecordsPerShard: 1}))

	// Start is driven directly rather than through startConsumer: cancelling
	// the run would itself normalize the worker's failure, and the point here
	// is that the run survives the takeover without being cancelled at all.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- cons.Start(ctx) }()

	waitFor(t, "the first delivery", func() bool { return collected.count() >= 1 })
	if _, claimed, err := manager.Claim(
		context.Background(), testCoordKey, testShardID, cons.leaseOwner, "peer", time.Minute,
	); err != nil || !claimed {
		t.Fatalf("Claim() = (%v, %v), want a successful takeover", claimed, err)
	}
	if err := collected.snapshot()[0].Ack(context.Background()); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("Ack() error = %v, want %v", err, lease.ErrNotOwned)
	}

	waitFor(t, "the shard worker to stop", func() bool {
		return len(reporter.countersNamed(metricWorkerStops)) > 0
	})
	stops := reporter.countersNamed(metricWorkerStops)
	if got := stops[0].tags[metricTagOutcome]; got != metricOutcomeClean {
		t.Fatalf("worker stop outcome = %q, want %q: a takeover observed by an acknowledgment is a handoff", got, metricOutcomeClean)
	}
	select {
	case err := <-started:
		t.Fatalf("Start() returned %v; losing one shard must not stop the consumer", err)
	default:
	}

	cancel()
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
	if stored := storedCheckpoint(t, store); stored != "" {
		t.Fatalf("checkpoint = %q, want no write from a worker whose lease moved on", stored)
	}
}

// Pause after failure policy succeeds, before its internal acknowledgments.
type explicitPolicyReporter struct {
	recordingReporter
	metric  string
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (r *explicitPolicyReporter) Counter(name string, value int64, tags []metrics.Tag) {
	r.recordingReporter.Counter(name, value, tags)
	if name == r.metric {
		r.once.Do(func() {
			close(r.entered)
			<-r.resume
		})
	}
}

func TestExplicitLeaseLossDuringFailurePolicyStopsOnlyTheShard(t *testing.T) {
	for _, policy := range []FailurePolicy{FailurePolicySkip, FailurePolicySendToDLQ} {
		t.Run(string(policy), func(t *testing.T) {
			store, manager := fencedBackends()
			stream := newFakeStream(testShardID, sequences(100, 2)...)
			collected := &explicitDeliveries{}
			reporter := &explicitPolicyReporter{
				metric: metricRecordsSkipped, entered: make(chan struct{}), resume: make(chan struct{}),
			}
			if policy == FailurePolicySendToDLQ {
				reporter.metric = metricDLQRecordsPublished
			}
			resume := sync.OnceFunc(func() { close(reporter.resume) })
			defer resume()
			cons := newExplicitTestConsumer(t, stream, store, manager,
				func(ctx context.Context, d Delivery) error {
					if aws.ToString(d.Record.SequenceNumber) == "100" {
						return collected.handle(ctx, d)
					}
					return errors.New("poison record")
				},
				WithHeartbeat(time.Minute, 3*time.Minute),
				WithMetrics(reporter), WithRetry(1, time.Millisecond),
				WithFailurePolicy(policy),
				WithDLQPublisher(explicitDLQ(func(context.Context, PoisonRecord) error { return nil })))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan error, 1)
			go func() { started <- cons.Start(ctx) }()
			select {
			case <-reporter.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("failure policy did not finish")
			}
			if _, claimed, err := manager.Claim(ctx, testCoordKey, testShardID, cons.leaseOwner, "peer", time.Minute); err != nil || !claimed {
				t.Fatalf("Claim() = (%v, %v), want successful takeover", claimed, err)
			}
			if err := collected.snapshot()[0].Ack(ctx); !errors.Is(err, lease.ErrNotOwned) {
				t.Fatalf("Ack() = %v, want ownership loss", err)
			}
			// The processor is now canceled. Let the policy attempt its Ack
			// using the handles invalidated by the other delivery's Ack.
			resume()
			waitFor(t, "the shard worker to stop", func() bool {
				return len(reporter.countersNamed(metricWorkerStops)) > 0
			})
			if got := reporter.countersNamed(metricWorkerStops)[0].tags[metricTagOutcome]; got != metricOutcomeClean {
				t.Fatalf("worker stop outcome = %q, want clean handoff", got)
			}
			select {
			case err := <-started:
				t.Fatalf("Start() returned %v during shard handoff", err)
			default:
			}
			cancel()
			select {
			case err := <-started:
				if err != nil {
					t.Fatalf("Start() = %v after cancellation", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not return after cancellation")
			}
			if got := storedCheckpoint(t, store); got != "" {
				t.Fatalf("checkpoint = %q, want unfinished work left replayable", got)
			}
		})
	}
}

func TestExplicitExpiredIteratorResumesFromLastFetchedSequence(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	// Expire every iterator after the page read and the empty tip read that
	// ends the pass, so the failure lands on the NEXT pass's first read, when
	// the pass holds no position of its own yet.
	stream.expireAfterRead = 2
	// Nothing is acknowledged, so persisted recovery stays at the initial
	// position for record 100 while the worker has already fetched through 102.
	collected := &explicitDeliveries{}
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "three deliveries", func() bool { return collected.count() >= 3 })
	waitFor(t, "the post-expiry recovery read", func() bool {
		return len(anchorRequests(stream)) > 0
	})
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	anchors := anchorRequests(stream)
	if anchors[0] != "102" {
		t.Fatalf("recovery anchor = %q, want the last sequence this worker fetched (102); "+
			"persisted recovery still points at 100 because nothing was acknowledged", anchors[0])
	}
	if got := collected.sequences(); !slices.Equal(got, []string{"100", "101", "102"}) {
		t.Fatalf("delivered = %v, want each record delivered once", got)
	}
}

// anchorRequests returns the sequences the consumer asked to read AT, which is
// how both recovery resumption and expired-iterator refresh derive a position.
func anchorRequests(stream *fakeStream) []string {
	var anchors []string
	for _, request := range stream.iteratorRequests() {
		if request.ShardIteratorType == types.ShardIteratorTypeAtSequenceNumber {
			anchors = append(anchors, aws.ToString(request.StartingSequenceNumber))
		}
	}
	return anchors
}

func TestExplicitIdlePassesKeepPollingPaced(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collected := &explicitDeliveries{}
	// Once a page has been admitted the shard sits at the tip with nothing to
	// do. Each idle pass must still wait a poll interval before reading again:
	// an acquisition-wide recovery cursor reported as per-pass progress would
	// make every pass look productive and read the shard as fast as it can.
	const pollInterval = 50 * time.Millisecond
	cons := newExplicitTestConsumer(t, stream, store, manager, collected.handle,
		WithPolling(pollInterval, time.Second))

	stop := startConsumer(t, cons)
	waitFor(t, "three deliveries", func() bool { return collected.count() >= 3 })

	before := stream.getRecordsCalls()
	idleFor := 10 * pollInterval
	time.Sleep(idleFor)
	reads := stream.getRecordsCalls() - before
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	// Paced reads over ten intervals are about ten, with scheduling slack. An
	// unpaced loop makes thousands.
	if reads > 40 {
		t.Fatalf("idle reads = %d over %v with a %v poll interval; the poll interval is not being applied",
			reads, idleFor, pollInterval)
	}
}
