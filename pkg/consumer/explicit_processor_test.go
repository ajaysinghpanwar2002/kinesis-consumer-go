package consumer

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func explicitTestProcessor(t *testing.T, limits InFlightLimits, handlers explicitHandlers, every int, interval time.Duration) (*explicitProcessor, *checkpoint.MemoryStore, lease.Lease) {
	t.Helper()
	c := boundedTestConsumer(t, limits)
	store, manager := fencedBackends()
	c.store = store
	c.tuning.checkpointEvery = every
	held, ok, err := manager.Acquire(context.Background(), c.coordinationKey(), "a", "owner", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire: %v, %v", ok, err)
	}
	p, err := newExplicitProcessor(context.Background(), c, "a", held, handlers, interval)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.stop()
		if err := p.wait(); err != nil {
			t.Logf("checkpoint runner: %v", err)
		}
		_ = held.Release(context.Background())
	})
	return p, store, held
}

func TestExplicitDelayedAckGapAndInterval(t *testing.T) {
	var delivered []Delivery
	var callbackCtx context.Context
	p, store, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{batch: func(ctx context.Context, d []Delivery) error { callbackCtx = ctx; delivered = d; return nil }}, 100, 5*time.Millisecond)
	records := testPage(2, 3, 4).Records
	if err := p.processPage(context.Background(), records, nil); err != nil {
		t.Fatal(err)
	}
	if callbackCtx.Err() != nil {
		t.Fatal("successful callback canceled completion context")
	}
	for _, r := range records {
		if r.Data != nil {
			t.Fatal("staging retained payload")
		}
	}
	if err := delivered[2].Ack(callbackCtx); err != nil {
		t.Fatal(err)
	}
	if err := delivered[1].Ack(callbackCtx); err != nil {
		t.Fatal(err)
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a"); got != "" {
		t.Fatalf("checkpoint crossed gap: %s", got)
	}
	total, _, _ := admissionState(p.c.admission)
	if total.records != 1 || total.bytes != 2 {
		t.Fatalf("capacity=%+v", total)
	}
	if len(delivered[1].Record.Data) != 3 {
		t.Fatal("mutated application slice")
	}
	if err := delivered[0].Ack(callbackCtx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "interval checkpoint", func() bool {
		got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a")
		return got == "102"
	})
	if err := delivered[0].Ack(callbackCtx); err != nil {
		t.Fatal(err)
	}
}

func TestExplicitPartialRetryAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			var first []Delivery
			var attempts [][]string
			p, store, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{batch: func(ctx context.Context, d []Delivery) error {
				var seq []string
				for _, v := range d {
					seq = append(seq, aws.ToString(v.Record.SequenceNumber))
				}
				attempts = append(attempts, seq)
				if len(attempts) == 1 {
					first = append([]Delivery(nil), d...)
					if err := d[1].Ack(ctx); err != nil {
						return err
					}
					// Mutating application identities must not alter the retry list.
					d[0] = Delivery{}
					if panics {
						panic("failed")
					}
					return errors.New("failed")
				}
				if err := first[0].Ack(ctx); !errors.Is(err, ErrStaleDelivery) {
					t.Errorf("old attempt: %v", err)
				}
				if err := first[1].Ack(ctx); err != nil {
					t.Errorf("accepted handle: %v", err)
				}
				for _, v := range d {
					if err := v.Ack(ctx); err != nil {
						return err
					}
				}
				return nil
			}}, 100, time.Hour)
			p.c.tuning.retryMaxAttempts = 2
			if err := p.processPage(context.Background(), testPage(1, 1, 1).Records, nil); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(attempts, [][]string{{"100", "101", "102"}, {"100", "102"}}) {
				t.Fatalf("attempts=%v", attempts)
			}
			if err := p.flushCheckpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a"); got != "102" {
				t.Fatalf("checkpoint=%s", got)
			}
		})
	}
}

type explicitDLQ func(context.Context, PoisonRecord) error

func (f explicitDLQ) Publish(ctx context.Context, r PoisonRecord) error { return f(ctx, r) }

func TestExplicitFailureOutcomes(t *testing.T) {
	for _, policy := range []FailurePolicy{FailurePolicyFailFast, FailurePolicySkip, FailurePolicySendToDLQ} {
		for _, dlqFails := range []bool{false, true} {
			t.Run(string(policy)+map[bool]string{false: "/ok", true: "/dlq-fails"}[dlqFails], func(t *testing.T) {
				var old Delivery
				boom := errors.New("handler failure")
				p, store, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{record: func(_ context.Context, d Delivery) error { old = d; return boom }}, 100, time.Hour)
				p.c.failurePolicy = policy
				var publishes int
				p.c.dlqPublisher = explicitDLQ(func(context.Context, PoisonRecord) error {
					publishes++
					if dlqFails {
						return errors.New("publish failed")
					}
					return nil
				})
				err := p.processPage(context.Background(), testPage(2).Records, nil)
				wantError := policy == FailurePolicyFailFast || (policy == FailurePolicySendToDLQ && dlqFails)
				if (err != nil) != wantError {
					t.Fatalf("err=%v", err)
				}
				if err := old.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
					t.Fatalf("old handle=%v", err)
				}
				if !wantError {
					if err := p.flushCheckpoint(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a")
				if (got == "100") == wantError {
					t.Fatalf("checkpoint=%s error=%v", got, err)
				}
				if policy == FailurePolicySendToDLQ && publishes != 1 {
					t.Fatalf("publishes=%d", publishes)
				}
			})
		}
	}
}

func TestExplicitCapacityWaitAllowsAckAndCheckpoint(t *testing.T) {
	delivered := make(chan Delivery, 2)
	p, store, _ := explicitTestProcessor(t, InFlightLimits{MaxRecordsPerShard: 1}, explicitHandlers{record: func(_ context.Context, d Delivery) error { delivered <- d; return nil }}, 1, time.Hour)
	done := make(chan error, 1)
	go func() { done <- p.processPage(context.Background(), testPage(2, 3).Records, nil) }()
	first := <-delivered
	if err := first.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := <-delivered
	waitFor(t, "count checkpoint", func() bool {
		got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a")
		return got == "100"
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := second.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExplicitOversizedInitialAndOwnershipLoss(t *testing.T) {
	var calls atomic.Int32
	p, _, held := explicitTestProcessor(t, InFlightLimits{MaxBytesPerShard: 1}, explicitHandlers{record: func(context.Context, Delivery) error { calls.Add(1); return nil }}, 1, time.Hour)
	if err := p.processPage(context.Background(), testPage(2).Records, nil); !errors.Is(err, ErrOversizedRecord) {
		t.Fatal(err)
	}
	pos, err := p.session.recovery(context.Background())
	if err != nil || pos.Kind != checkpoint.RecoveryInitial || pos.Sequence != "100" {
		t.Fatalf("position=%+v err=%v", pos, err)
	}
	if calls.Load() != 0 {
		t.Fatal("oversized record delivered")
	}
	d := p.tracker.append(testPage(1).Records[0])
	if err := held.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(context.Background()); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatal(err)
	}
	if err := d.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
		t.Fatal(err)
	}
}

func TestExplicitStopAbandonsUncooperativeCallback(t *testing.T) {
	started := make(chan Delivery, 1)
	release := make(chan struct{})
	defer close(release)
	p, _, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{record: func(_ context.Context, d Delivery) error { started <- d; <-release; return nil }}, 1, time.Hour)
	done := make(chan error, 1)
	go func() { done <- p.processPage(context.Background(), testPage(2).Records, nil) }()
	d := <-started
	p.stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("callback blocked stop")
	}
	if err := d.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
		t.Fatal(err)
	}
	total, _, _ := admissionState(p.c.admission)
	if total.records != 0 {
		t.Fatalf("capacity leaked: %+v", total)
	}
}

func TestExplicitConfigurationGate(t *testing.T) {
	record := func(context.Context, Delivery) error { return nil }
	batch := func(context.Context, []Delivery) error { return nil }
	cases := []struct {
		name string
		opts []Option
		want error
	}{
		{"record", []Option{WithExplicitHandler(record)}, ErrExplicitModeUnavailable},
		{"batch", []Option{WithExplicitBatchHandler(batch), WithCheckpointInterval(time.Second)}, ErrExplicitModeUnavailable},
		{"both", []Option{WithExplicitHandler(record), WithExplicitBatchHandler(batch)}, nil},
		{"nil", []Option{WithExplicitHandler(nil)}, nil},
		{"interval", []Option{WithCheckpointInterval(0)}, nil},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			o, err := applyOptions(tt.opts)
			if err == nil {
				_, _, err = resolveHandlers(nil, o)
			}
			if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

type explicitFaultStore struct {
	checkpoint.FencedStore
	session *explicitFaultSession
}

func (s *explicitFaultStore) Bind(ctx context.Context, key, shard string, held lease.FencedLease) (checkpoint.Session, error) {
	bound, err := s.FencedStore.Bind(ctx, key, shard, held)
	if err != nil {
		return nil, err
	}
	s.session.Session = bound
	return s.session, nil
}

type explicitFaultSession struct {
	checkpoint.Session
	failures int32
	saves    atomic.Int32
	entered  chan struct{}
	unblock  chan struct{}
}

func (s *explicitFaultSession) Save(ctx context.Context, seq string) error {
	attempt := s.saves.Add(1)
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.unblock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if attempt <= s.failures {
		return errors.New("checkpoint unavailable")
	}
	return s.Session.Save(ctx, seq)
}

func TestExplicitCheckpointRetriesAndFailureWakeAdmission(t *testing.T) {
	for _, failures := range []int32{1, 3} {
		t.Run(map[int32]string{1: "recovers", 3: "exhausted"}[failures], func(t *testing.T) {
			c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerShard: 1})
			c.tuning.retryMaxAttempts = 2
			store, manager := fencedBackends()
			faults := &explicitFaultSession{failures: failures}
			c.store = &explicitFaultStore{FencedStore: store, session: faults}
			held, _, err := manager.Acquire(context.Background(), c.coordinationKey(), "a", "owner", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Release(context.Background()) }()
			delivered := make(chan Delivery, 3)
			p, err := newExplicitProcessor(context.Background(), c, "a", held, explicitHandlers{record: func(_ context.Context, d Delivery) error { delivered <- d; return nil }}, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { p.stop(); _ = p.wait() }()
			done := make(chan error, 1)
			go func() { done <- p.processPage(context.Background(), testPage(1, 1, 1).Records, nil) }()
			first := <-delivered
			if err := first.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
			if failures == 1 {
				second := <-delivered
				waitFor(t, "retried checkpoint", func() bool { got, _ := store.Get(context.Background(), c.coordinationKey(), "a"); return got == "100" })
				if faults.saves.Load() != 2 {
					t.Fatalf("saves=%d", faults.saves.Load())
				}
				if err := second.Ack(context.Background()); err != nil {
					t.Fatal(err)
				}
				third := <-delivered
				if err := third.Ack(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			} else {
				select {
				case <-p.done:
				case <-time.After(time.Second):
					t.Fatal("retry exhaustion did not stop runner")
				}
				if p.wait() == nil {
					t.Fatal("missing checkpoint failure")
				}
				if faults.saves.Load() != 2 {
					t.Fatalf("unbounded retry: %d", faults.saves.Load())
				}
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("blocked admission succeeded")
					}
				case <-time.After(time.Second):
					t.Fatal("admission stayed blocked")
				}
				if err := first.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExplicitAckDoesNotWaitForCheckpointIO(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{})
	store, manager := fencedBackends()
	faults := &explicitFaultSession{entered: make(chan struct{}, 1), unblock: make(chan struct{})}
	c.store = &explicitFaultStore{FencedStore: store, session: faults}
	held, _, err := manager.Acquire(context.Background(), c.coordinationKey(), "a", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Release(context.Background()) }()
	var d []Delivery
	p, err := newExplicitProcessor(context.Background(), c, "a", held, explicitHandlers{batch: func(_ context.Context, received []Delivery) error { d = received; return nil }}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { p.stop(); _ = p.wait() }()
	if err := p.processPage(context.Background(), testPage(1, 1).Records, nil); err != nil {
		t.Fatal(err)
	}
	if err := d[0].Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-faults.entered
	ack := make(chan error, 1)
	go func() { ack <- d[1].Ack(context.Background()) }()
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("checkpoint I/O blocked Ack")
	}
	close(faults.unblock)
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Get(context.Background(), c.coordinationKey(), "a")
	if got != "101" {
		t.Fatalf("checkpoint=%s", got)
	}
}

func TestExplicitRejectsUnfencedAndMismatchedDependencies(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{})
	store, manager := fencedBackends()
	held, _, err := manager.Acquire(context.Background(), c.coordinationKey(), "a", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Release(context.Background()) }()
	h := explicitHandlers{record: func(context.Context, Delivery) error { return nil }}
	if _, err := newExplicitProcessor(context.Background(), c, "a", held, h, 0); err == nil {
		t.Fatal("unfenced store accepted")
	}
	c.store = checkpoint.NewMemoryStore()
	if _, err := newExplicitProcessor(context.Background(), c, "a", held, h, 0); !errors.Is(err, lease.ErrLeaseMismatch) {
		t.Fatalf("mismatch=%v", err)
	}
	c.store = store
	c.admission = nil
	if _, err := newExplicitProcessor(context.Background(), c, "a", held, h, 0); err == nil {
		t.Fatal("unbounded explicit processing accepted")
	}
}

func TestExplicitDLQCannotCrossEarlierAsyncGap(t *testing.T) {
	var gap Delivery
	p, store, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{record: func(_ context.Context, d Delivery) error {
		if aws.ToString(d.Record.SequenceNumber) == "100" {
			gap = d
			return nil
		}
		return errors.New("poison")
	}}, 100, time.Hour)
	p.c.failurePolicy = FailurePolicySendToDLQ
	p.c.dlqPublisher = explicitDLQ(func(context.Context, PoisonRecord) error { return nil })
	if err := p.processPage(context.Background(), testPage(1, 1).Records, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a"); got != "" {
		t.Fatalf("crossed gap: %s", got)
	}
	if err := gap.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a"); got != "101" {
		t.Fatalf("checkpoint=%s", got)
	}
}

func TestExplicitProgressAcrossPages(t *testing.T) {
	var all []Delivery
	p, store, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{batch: func(_ context.Context, d []Delivery) error { all = append(all, d...); return nil }}, 100, time.Hour)
	for _, first := range []int{100, 200} {
		records := testPage(1, 1).Records
		for i := range records {
			records[i].SequenceNumber = aws.String(sequences(first+i, 1)[0])
		}
		if err := p.processPage(context.Background(), records, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, i := range []int{3, 2, 0} {
		if err := all[i].Ack(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a"); got != "100" {
		t.Fatalf("cross-page gap: %s", got)
	}
	if err := all[1].Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.flushCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(context.Background(), p.c.coordinationKey(), "a"); got != "201" {
		t.Fatalf("checkpoint=%s", got)
	}
}

func TestAckCapacityReleaseRacesRetryAndInvalidation(t *testing.T) {
	for range 100 {
		admission := testAdmission(t, InFlightLimits{MaxRecordsPerShard: 1})
		reservation, err := admission.acquire(context.Background(), "a", []int{3}, false)
		if err != nil {
			t.Fatal(err)
		}
		tracker := newAckTracker("a", func(context.Context) error { return nil })
		d := tracker.appendReserved(testPage(3).Records[0], func() { reservation.releaseRecord(0) })
		acked := make(chan error, 1)
		go func() { acked <- d.Ack(context.Background()) }()
		pending := tracker.retry([]Delivery{d})
		err = <-acked
		if err != nil && !errors.Is(err, ErrStaleDelivery) {
			t.Fatal(err)
		}
		if len(pending) == 1 {
			acked := make(chan error, 1)
			go func() { acked <- pending[0].Ack(context.Background()) }()
			tracker.invalidate()
			err = <-acked
			if err != nil && !errors.Is(err, ErrStaleDelivery) {
				t.Fatal(err)
			}
		} else {
			tracker.invalidate()
		}
		total, _, _ := admissionState(admission)
		if total != (admissionUsage{}) {
			t.Fatalf("release race leaked or double-released: %+v", total)
		}
	}
}

type lockingExplicitStore struct {
	checkpoint.FencedStore
	entered chan struct{}
}

func (s *lockingExplicitStore) Bind(ctx context.Context, key, shard string, held lease.FencedLease) (checkpoint.Session, error) {
	bound, err := s.FencedStore.Bind(ctx, key, shard, held)
	if err != nil {
		return nil, err
	}
	return &lockingExplicitSession{Session: bound, entered: s.entered}, nil
}

// Match Valkey's session locking: Save owns the lock throughout I/O, and
// Invalidate must acquire that same lock before it can return.
type lockingExplicitSession struct {
	checkpoint.Session
	mu      sync.Mutex
	entered chan struct{}
}

func (s *lockingExplicitSession) Save(ctx context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.entered)
	<-ctx.Done()
	return ctx.Err()
}
func (s *lockingExplicitSession) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Session.Invalidate()
}

func TestExplicitStopCancelsSaveBeforeSessionInvalidation(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerShard: 1})
	store, manager := fencedBackends()
	entered := make(chan struct{})
	c.store = &lockingExplicitStore{FencedStore: store, entered: entered}
	held, _, err := manager.Acquire(context.Background(), c.coordinationKey(), "a", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Release(context.Background()) }()
	delivered := make(chan Delivery, 3)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := newExplicitProcessor(parent, c, "a", held, explicitHandlers{record: func(_ context.Context, d Delivery) error { delivered <- d; return nil }}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); p.stop(); _ = p.wait() }()
	page := make(chan error, 1)
	go func() { page <- p.processPage(context.Background(), testPage(1, 1, 1).Records, nil) }()
	first := <-delivered
	if err := first.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	<-delivered
	stopped := make(chan struct{})
	go func() { p.stop(); close(stopped) }()
	select {
	case <-stopped:
		if err := p.wait(); err != nil {
			t.Fatalf("stop reported checkpoint failure: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop waited for session lock before canceling save")
	}
	select {
	case err := <-page:
		if err == nil {
			t.Fatal("admission not canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("admission stayed blocked")
	}
	if err := first.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
		t.Fatal(err)
	}
}
