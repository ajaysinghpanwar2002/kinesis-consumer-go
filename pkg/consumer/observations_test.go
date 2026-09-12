package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

func TestPressureTracksStagingPausesAndPeriodicAges(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerShard: 1, MaxBytesPerShard: 2, MaxFetchSlots: 1})
	r := &recordingReporter{}
	c.reporter = r
	c.observation.begin("a")
	c.observation.begin("b")
	slot, err := c.admission.acquire(context.Background(), "a", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer slot.release()
	slot.stage(testPage(2, 2).Records)
	admitted, err := c.admission.acquire(context.Background(), "a", []int{2, 2}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer admitted.release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	go func() {
		reservation, err := c.admission.acquire(ctx, "a", []int{2}, false)
		reservation.release()
		done <- err
	}()
	go func() {
		reservation, err := c.admission.acquire(ctx, "b", nil, true)
		reservation.release()
		done <- err
	}()
	waitForTrue(t, func() bool { return c.Health().Pressure.PausedShards == 2 }, "both paused shards")
	first := c.Health()
	p := first.Pressure
	if p.UnacknowledgedRecords != 1 || p.UnacknowledgedBytes != 2 || p.StagedBytes != 2 || p.FetchSlots != 1 {
		t.Fatalf("pressure=%+v", p)
	}
	if len(first.Shards["a"].Pressure.Pauses) != 3 || first.Shards["a"].Pressure.Pauses["count"].Current <= 0 || first.Shards["a"].Pressure.Pauses["bytes"].Current <= 0 || first.Shards["b"].Pressure.Pauses["fetch_slots"].Current <= 0 {
		t.Fatalf("reasons=%+v", first.Shards)
	}
	stop := c.startObservations()
	defer stop()
	// Two independent ticks must refresh the age while no work completes.
	waitForTrue(t, func() bool { return len(r.gaugesNamed(metricOldestUnacknowledgedAge)) >= 3 }, "first periodic pressure update")
	deadline := time.Now().Add(3 * time.Second)
	for len(r.gaugesNamed(metricOldestUnacknowledgedAge)) < 6 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	calls := r.gaugesNamed(metricOldestUnacknowledgedAge)
	var ages []float64
	for _, call := range calls {
		if call.tags[metricTagShard] == "a" {
			ages = append(ages, call.value)
		}
	}
	if len(ages) < 2 || ages[1] <= ages[0] {
		t.Fatalf("periodic ages=%v", ages)
	}
	later := c.Health()
	if later.Pressure.PauseDuration <= p.PauseDuration || later.Pressure.PausedDuration >= later.Shards["a"].Pressure.PausedDuration+later.Shards["b"].Pressure.PausedDuration {
		t.Fatalf("consumer pause must measure union: %+v", later)
	}
	// Returned maps are independent snapshots.
	delete(first.Shards, "a")
	first.Pressure.Pauses["count"] = PauseHealth{}
	if c.Health().Pressure.Pauses["count"].Current == 0 {
		t.Fatal("snapshot mutated live state")
	}
	cancel()
	<-done
	<-done
	admitted.release()
	slot.release()
	final := c.Health().Pressure
	if final.PausedShards != 0 || final.PauseDuration != 0 || final.UnacknowledgedRecords != 0 || final.StagedBytes != 0 || final.FetchSlots != 0 || final.PausedDuration == 0 {
		t.Fatalf("released pressure=%+v", final)
	}
	c.finishObservations("a")
	c.finishObservations("b")
	c.emitObservations()
	for _, name := range []string{metricUnacknowledgedRecords, metricStagedBytes, metricOldestUnacknowledgedAge, metricCheckpointProgressAge, metricPauseDuration, metricPausedSeconds} {
		for _, shard := range []string{"a", "b"} {
			var last float64
			for _, call := range r.gaugesNamed(name) {
				if call.tags[metricTagShard] == shard {
					last = call.value
				}
			}
			if last != 0 {
				t.Fatalf("%s/%s restored after exit: %v", name, shard, last)
			}
		}
	}
}

func TestExplicitHealthSeparatesAcceptedAndPersistedProgress(t *testing.T) {
	var deliveries []Delivery
	p, _, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{batch: func(_ context.Context, d []Delivery) error { deliveries = d; return nil }}, 100, time.Hour)
	p.c.observation.begin("a")
	p.c.observation.setTracker("a", p.tracker)
	r := &recordingReporter{}
	p.c.reporter = r
	if err := p.processPage(context.Background(), testPage(2, 3, 4).Records, nil); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{2, 1} {
		if err := deliveries[i].Ack(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	h := p.c.Health().Shards["a"]
	if h.AcceptedSequence != "" || h.PersistedSequence != "" || h.Pressure.UnacknowledgedRecords != 1 || h.Pressure.UnacknowledgedBytes != 2 {
		t.Fatalf("gap health=%+v", h)
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.countersNamed(metricRecordsCheckpointed)) != 0 {
		t.Fatal("counted records beyond gap")
	}
	if err := deliveries[0].Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	h = p.c.Health().Shards["a"]
	if h.AcceptedSequence != "102" || h.PersistedSequence != "" || h.Pressure.UnacknowledgedRecords != 0 {
		t.Fatalf("accepted health=%+v", h)
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.complete(context.Background(), "102"); err != nil {
		t.Fatal(err)
	}
	h = p.c.Health().Shards["a"]
	if h.PersistedSequence != "102" || h.PersistedRecords != 3 || !h.Completed || h.Checkpoint.LastSuccess.IsZero() {
		t.Fatalf("persisted health=%+v", h)
	}
	var covered int64
	for _, call := range r.countersNamed(metricRecordsCheckpointed) {
		covered += call.value
	}
	if covered != 3 {
		t.Fatalf("completion double-counted records: %d", covered)
	}
}

func TestCheckpointHealthUsesCandidateBeforeBlockedWrite(t *testing.T) {
	var deliveries []Delivery
	p, _, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{batch: func(_ context.Context, d []Delivery) error { deliveries = d; return nil }}, 100, time.Hour)
	p.c.observation.begin("a")
	p.c.observation.setTracker("a", p.tracker)
	fault := &explicitFaultSession{Session: p.session.session, entered: make(chan struct{}, 1), unblock: make(chan struct{})}
	p.session.session = fault
	defer func() {
		select {
		case <-fault.unblock:
		default:
			close(fault.unblock)
		}
	}()
	if err := p.processPage(context.Background(), testPage(1, 1).Records, nil); err != nil {
		t.Fatal(err)
	}
	if err := deliveries[0].Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.flushCheckpoint(context.Background()) }()
	select {
	case <-fault.entered:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	if err := deliveries[1].Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := p.c.Health().Shards["a"]
	if h.AcceptedSequence != "101" || h.PersistedRecords != 0 {
		t.Fatalf("blocked health=%+v", h)
	}
	close(fault.unblock)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h = p.c.Health().Shards["a"]
	if h.PersistedSequence != "100" || h.PersistedRecords != 1 {
		t.Fatalf("write counted later Ack: %+v", h)
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h = p.c.Health().Shards["a"]; h.PersistedRecords != 2 {
		t.Fatalf("second write=%+v", h)
	}
}

func TestHealthRetainsFailuresAfterWorkerCleanup(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{})
	c.observation.begin("a")
	failure := errors.New("checkpoint unavailable")
	c.store = &fakeCheckpointSaveStore{saveErr: failure}
	if err := c.saveShardCheckpoint(context.Background(), "a", "100"); !errors.Is(err, failure) {
		t.Fatalf("save=%v", err)
	}
	h := c.Health()
	if h.Checkpoint.ConsecutiveFailures != 1 || !errors.Is(h.Checkpoint.LastError, failure) || h.Shards["a"].Checkpoint.ConsecutiveFailures != 1 {
		t.Fatalf("failure health=%+v", h)
	}
	c.observeRecoveryFailure(context.Background(), "a", checkpoint.ErrRecoveryState)
	c.finishObservations("a")
	h = c.Health()
	if len(h.Shards) != 0 || !errors.Is(h.Checkpoint.LastError, failure) || !errors.Is(h.Recovery.LastError, checkpoint.ErrRecoveryState) || h.Recovery.Failures != 1 {
		t.Fatalf("post-exit=%+v", h)
	}
	c.store = &fakeCheckpointSaveStore{}
	if err := c.saveShardCheckpoint(context.Background(), "b", "101"); err != nil {
		t.Fatal(err)
	}
	h = c.Health()
	if h.Checkpoint.ConsecutiveFailures != 0 || h.Checkpoint.LastError != nil || h.Checkpoint.LastSuccess.IsZero() || !errors.Is(h.Checkpoint.LastFailure, failure) {
		t.Fatalf("recovered health=%+v", h)
	}
}

func TestAutomaticHealthCountsOnlyPersistedPages(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerShard: 1})
	c.observation.begin("a")
	c.handler = func(context.Context, Record) error { return nil }
	c.tuning.checkpointEvery = 100
	slot, err := c.admission.acquire(context.Background(), "a", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer slot.release()
	sequence, count, err := c.processBoundedPage(context.Background(), context.Background(), "a", testPage(1, 1), 0, slot, "")
	if err != nil {
		t.Fatal(err)
	}
	h := c.Health().Shards["a"]
	if h.AcceptedSequence != "101" || h.PersistedRecords != 0 || h.Pressure.UnacknowledgedRecords != 0 {
		t.Fatalf("before persistence=%+v", h)
	}
	if err := c.checkpointOnDrain(context.Background(), "a", sequence, count); err != nil {
		t.Fatal(err)
	}
	h = c.Health().Shards["a"]
	if h.PersistedRecords != 2 || h.PersistedSequence != "101" {
		t.Fatalf("drained=%+v", h)
	}
}

func TestExplicitCheckpointOutageIsNotRecoveryFailure(t *testing.T) {
	var delivery Delivery
	p, _, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{record: func(_ context.Context, d Delivery) error { delivery = d; return nil }}, 100, time.Hour)
	p.c.observation.begin("a")
	p.c.observation.setTracker("a", p.tracker)
	p.session.session = &explicitFaultSession{Session: p.session.session, failures: 1}
	if err := p.processPage(context.Background(), testPage(1).Records, nil); err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.flushCheckpoint(context.Background()); !errors.Is(err, errExplicitCheckpointUnavailable) {
		t.Fatalf("flush=%v", err)
	}
	if err := p.wait(); !errors.Is(err, errExplicitCheckpointUnavailable) {
		t.Fatalf("runner=%v", err)
	}
	p.c.finishObservations("a")
	h := p.c.Health()
	if !errors.Is(h.Checkpoint.LastFailure, errExplicitCheckpointUnavailable) || h.Recovery.Failures != 0 || h.Pressure.UnacknowledgedRecords != 0 {
		t.Fatalf("outage health=%+v", h)
	}
}

func TestRecoveryHealthExcludesCallerDeadline(t *testing.T) {
	for _, operation := range []string{"bind", "read", "initialize", "anchor"} {
		t.Run(operation, func(t *testing.T) {
			f := newFencedFixture(t, newFakeStream(testShardID, "100"))
			defer f.held.Release(context.Background())
			ctx, cancel := context.WithDeadline(f.ctx, time.Now().Add(-time.Second))
			defer cancel()
			var err error
			switch operation {
			case "bind":
				_, err = f.consumer.bindShardSession(ctx, testShardID, f.held)
			case "read":
				_, err = f.session.recovery(ctx)
			case "initialize":
				_, err = f.session.initialize(ctx, "100")
			case "anchor":
				_, err = f.consumer.verifyShardAnchorPage(ctx, testShardID, "100", nil)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("operation error=%v", err)
			}
			if h := f.consumer.Health().Recovery; h.Failures != 0 || h.LastError != nil {
				t.Fatalf("caller deadline polluted recovery health: %+v", h)
			}
		})
	}
}

type timedOutRecoverySession struct{ checkpoint.Session }

func (s timedOutRecoverySession) Recovery(context.Context) (checkpoint.RecoveryPosition, error) {
	return checkpoint.RecoveryPosition{}, context.DeadlineExceeded
}

func TestRecoveryHealthRetainsBackendTimeoutWithLiveCaller(t *testing.T) {
	f := newFencedFixture(t, newFakeStream(testShardID, "100"))
	defer f.held.Release(context.Background())
	f.session.session = timedOutRecoverySession{f.session.session}
	_, err := f.session.recovery(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("operation error=%v", err)
	}
	if h := f.consumer.Health().Recovery; h.Failures != 1 || !errors.Is(h.LastError, context.DeadlineExceeded) {
		t.Fatalf("backend timeout missing from health: %+v", h)
	}
}

type deadlineBindStore struct {
	checkpoint.FencedStore
	entered chan struct{}
}

func (s *deadlineBindStore) Bind(ctx context.Context, _ string, _ string, _ lease.FencedLease) (checkpoint.Session, error) {
	close(s.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestStartDeadlineDuringBindDoesNotReportRecoveryFailure(t *testing.T) {
	store, manager := fencedBackends()
	blocked := &deadlineBindStore{FencedStore: store, entered: make(chan struct{})}
	c := newTestConsumer(t, newFakeStream(testShardID, "100"), blocked, manager)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin binding")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not stop at its deadline")
	}
	if h := c.Health(); h.Recovery.Failures != 0 || h.Recovery.LastError != nil || len(h.Shards) != 0 {
		t.Fatalf("requested shutdown polluted health: %+v", h)
	}
}

func TestRecoverySlotWaitIgnoresAdmissionStopBeforeContextCancellation(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxFetchSlots: 1})
	slot, err := c.admission.acquire(context.Background(), "other", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer slot.release()
	// During drain, the controller's stop is synchronous but cancellation of
	// admissionContext is asynchronous. Keep the supplied context live to hold
	// that interleaving deterministically while verification waits for a slot.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.verifyShardAnchorPage(ctx, "a", "100", nil); done <- err }()
	waitForTrue(t, func() bool { _, _, queued := admissionState(c.admission); return queued == 1 }, "anchor waiting for slot")
	c.admission.stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("verify=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission stop did not wake verification")
	}
	if ctx.Err() != nil {
		t.Fatal("test did not retain the live-context interleaving")
	}
	if h := c.Health().Recovery; h.Failures != 0 || h.LastError != nil {
		t.Fatalf("drain polluted recovery health: %+v", h)
	}
}
