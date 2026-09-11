package consumer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// recordCollector records the sequence numbers a consumer delivers. Tests wait
// on it rather than stopping the run from inside the handler: cancelling there
// would discard the very checkpoint most of these tests are about.
type recordCollector struct {
	mu        sync.Mutex
	sequences []string
	onRecord  func(Record) error
}

func (r *recordCollector) handle(_ context.Context, record Record) error {
	r.mu.Lock()
	r.sequences = append(r.sequences, aws.ToString(record.SequenceNumber))
	r.mu.Unlock()
	if r.onRecord != nil {
		return r.onRecord(record)
	}
	return nil
}

func (r *recordCollector) delivered() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.sequences)
}

func trimHorizonConfig() Config {
	return Config{StreamName: "stream", ConsumerGroup: "group", StartPosition: StartTrimHorizon}
}

// fencedBackends returns the built-in memory backends wired as a fenced pair.
func fencedBackends() (*checkpoint.MemoryStore, *lease.MemoryManager) {
	manager := lease.NewMemoryManager()
	return checkpoint.NewMemoryStoreWithLeaseManager(manager), manager
}

// startConsumer runs Start in the background and returns a stop function that
// cancels it and reports what Start returned.
func startConsumer(t *testing.T, cons *Consumer) func() error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- cons.Start(ctx) }()

	var once sync.Once
	var result error
	return func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-errCh:
			case <-time.After(5 * time.Second):
				t.Error("Start did not return after cancellation")
			}
		})
		return result
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// seedRecovery runs fn against a session bound to a throwaway lease, so a test
// can leave persisted recovery state behind exactly as a previous owner would.
func seedRecovery(t *testing.T, store *checkpoint.MemoryStore, manager *lease.MemoryManager, fn func(checkpoint.Session)) {
	t.Helper()

	held := acquireFencedLease(t, manager, testShardID)
	session, err := store.Bind(context.Background(), testCoordKey, testShardID, held)
	if err != nil {
		t.Fatalf("Bind() error = %v, want nil", err)
	}
	fn(session)
	if err := held.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v, want nil", err)
	}
}

// readRecovery inspects persisted recovery state after a run, waiting for the
// consumer to have released the shard lease.
func readRecovery(t *testing.T, store *checkpoint.MemoryStore, manager *lease.MemoryManager) checkpoint.RecoveryPosition {
	t.Helper()

	var position checkpoint.RecoveryPosition
	waitFor(t, "the consumer to release the shard lease", func() bool {
		held, acquired, err := manager.Acquire(context.Background(), testCoordKey, testShardID, "inspector", time.Minute)
		if err != nil {
			t.Fatalf("Acquire() error = %v, want nil", err)
		}
		if !acquired {
			return false
		}
		fenced, ok := held.(lease.FencedLease)
		if !ok {
			t.Fatalf("Acquire() returned %T, want a lease.FencedLease", held)
		}
		session, err := store.Bind(context.Background(), testCoordKey, testShardID, fenced)
		if err != nil {
			t.Fatalf("Bind() error = %v, want nil", err)
		}
		if position, err = session.Recovery(context.Background()); err != nil {
			t.Fatalf("Recovery() error = %v, want nil", err)
		}
		if err := held.Release(context.Background()); err != nil {
			t.Fatalf("Release() error = %v, want nil", err)
		}
		return true
	})
	return position
}

var errCollectorFailure = errors.New("handler failed")

func TestFencedConsumerPersistsInitialPositionBeforeHandlingRecords(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collector := &recordCollector{onRecord: func(Record) error { return errCollectorFailure }}
	cons := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, collector.handle)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cons.Start(ctx); !errors.Is(err, errCollectorFailure) {
		t.Fatalf("Start() error = %v, want wraps %v", err, errCollectorFailure)
	}

	position := readRecovery(t, store, manager)
	if position.Kind != checkpoint.RecoveryInitial || position.Sequence != "100" {
		t.Fatalf("recovery position = %+v, want the first fetched sequence recorded as an initial position", position)
	}
	if got := collector.delivered(); len(got) != 1 || got[0] != "100" {
		t.Fatalf("delivered = %v, want the first record handled once", got)
	}
}

func TestFencedConsumerReplaysInclusivelyFromInitialPosition(t *testing.T) {
	store, manager := fencedBackends()
	seedRecovery(t, store, manager, func(session checkpoint.Session) {
		if _, err := session.Initialize(context.Background(), "100"); err != nil {
			t.Fatalf("Initialize() error = %v, want nil", err)
		}
	})

	// The consumer starts from LATEST, which would anchor past every record.
	// The recorded initial position wins, and includes its own record.
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collector := &recordCollector{}
	cons := newTestConsumerWithHandler(t, stream, store, manager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "three delivered records", func() bool { return len(collector.delivered()) >= 3 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"100", "101", "102"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v: an initial position is replayed inclusively", got, want)
	}
}

func TestFencedConsumerResumesExclusivelyAfterCheckpoint(t *testing.T) {
	store, manager := fencedBackends()
	seedRecovery(t, store, manager, func(session checkpoint.Session) {
		if _, err := session.Initialize(context.Background(), "100"); err != nil {
			t.Fatalf("Initialize() error = %v, want nil", err)
		}
		if err := session.Save(context.Background(), "100"); err != nil {
			t.Fatalf("Save() error = %v, want nil", err)
		}
	})

	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collector := &recordCollector{}
	cons := newTestConsumerWithHandler(t, stream, store, manager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "two delivered records", func() bool { return len(collector.delivered()) >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"101", "102"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v: a checkpoint resumes strictly after itself", got, want)
	}
}

func TestFencedConsumerRecordsNothingBeforeTheFirstRecord(t *testing.T) {
	store, manager := fencedBackends()
	// An empty shard: the worker anchors at LATEST but never fetches a record,
	// which is the documented window before first-record protection begins.
	stream := newFakeStream(testShardID)
	cons := newTestConsumerWithHandler(t, stream, store, manager, (&recordCollector{}).handle)

	stop := startConsumer(t, cons)
	waitFor(t, "the shard to be polled", func() bool { return len(stream.iteratorRequests()) > 0 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if position := readRecovery(t, store, manager); position.Kind != checkpoint.RecoveryFresh {
		t.Fatalf("recovery position = %+v, want no recovery state before the first fetched record", position)
	}
}

func TestFencedConsumerHaltsOnTrimmedRecoveryAnchor(t *testing.T) {
	store, manager := fencedBackends()
	seedRecovery(t, store, manager, func(session checkpoint.Session) {
		if _, err := session.Initialize(context.Background(), "100"); err != nil {
			t.Fatalf("Initialize() error = %v, want nil", err)
		}
	})

	stream := newFakeStream(testShardID, sequences(100, 3)...)
	stream.trim(1)
	collector := &recordCollector{}
	cons := newTestConsumerWithHandler(t, stream, store, manager, collector.handle)
	cons.tuning.anchorVerifyBudget = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cons.Start(ctx); !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("Start() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if got := collector.delivered(); len(got) != 0 {
		t.Fatalf("delivered = %v, want nothing: a trimmed anchor must not be replaced by a later record", got)
	}
	if position := readRecovery(t, store, manager); position.Sequence != "100" {
		t.Fatalf("recovery position = %+v, want the recorded position left untouched", position)
	}
}

func TestFencedConsumerRefreshesExpiredIteratorFromLastFetchedSequence(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 4)...)
	// Invalidate every iterator once the first page has been served.
	stream.expireAfterRead = 1

	collector := &recordCollector{}
	cons := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, collector.handle,
		WithBatching(2, 1),
	)

	stop := startConsumer(t, cons)
	waitFor(t, "all four records", func() bool { return len(collector.delivered()) >= 4 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"100", "101", "102", "103"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v exactly once", got, want)
	}

	// The refresh reads the last fetched sequence inclusively — that read is
	// what proves it is still there — and drops it from the page, so 101 is
	// delivered once and 102 follows it.
	var refreshed bool
	for _, call := range stream.iteratorRequests() {
		if call.ShardIteratorType == types.ShardIteratorTypeAtSequenceNumber &&
			aws.ToString(call.StartingSequenceNumber) == "101" {
			refreshed = true
		}
	}
	if !refreshed {
		t.Fatalf("iterator requests = %+v, want a refresh anchored at the last fetched sequence 101", stream.iteratorRequests())
	}
}

func TestFencedConsumerKeepsTheProtectedRecordWhenRetentionRacesRecovery(t *testing.T) {
	store, manager := fencedBackends()
	seedRecovery(t, store, manager, func(session checkpoint.Session) {
		if _, err := session.Initialize(context.Background(), "100"); err != nil {
			t.Fatalf("Initialize() error = %v, want nil", err)
		}
	})

	stream := newFakeStream(testShardID, sequences(100, 3)...)
	// Retention drops the anchor as soon as the read that proved it readable
	// returns. Anything that derives another iterator from the anchor after
	// that point starts at 101, and the record the initial position exists to
	// protect is skipped and checkpointed past.
	var trimmed sync.Once
	stream.afterGetRecords = func(int) { trimmed.Do(func() { stream.trimLocked(1) }) }

	collector := &recordCollector{}
	cons := newTestConsumerWithHandler(t, stream, store, manager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "three delivered records", func() bool { return len(collector.delivered()) >= 3 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"100", "101", "102"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v: the verified page is the page that gets handled", got, want)
	}
}

func TestFencedConsumerReadsOnPastAnAnchorOnlyPage(t *testing.T) {
	store, manager := fencedBackends()
	seedRecovery(t, store, manager, func(session checkpoint.Session) {
		if _, err := session.Initialize(context.Background(), "100"); err != nil {
			t.Fatalf("Initialize() error = %v, want nil", err)
		}
		if err := session.Save(context.Background(), "100"); err != nil {
			t.Fatalf("Save() error = %v, want nil", err)
		}
	})

	stream := newFakeStream(testShardID, sequences(100, 3)...)
	collector := &recordCollector{}
	// One record per page makes the exclusive continuation's page empty: it
	// holds only the checkpointed anchor, which the continuation drops. A poll
	// interval far longer than the test's patience is what turns "read that
	// emptied page as the shard tip" into a failure rather than a slow run —
	// and in production, with a poll interval past the iterator's five-minute
	// life, into a shard that re-verifies the same anchor forever.
	cons := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, collector.handle,
		WithBatching(1, 1),
		WithPolling(30*time.Second, time.Second),
	)

	stop := startConsumer(t, cons)
	waitFor(t, "the records after the checkpoint", func() bool { return len(collector.delivered()) >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"101", "102"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v without waiting out a poll interval first", got, want)
	}
}

func TestFencedConsumerStopsCleanlyWhenOwnershipMovesAway(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 2)...)

	var cons *Consumer
	var stolen sync.Once
	collector := &recordCollector{onRecord: func(Record) error {
		stolen.Do(func() {
			if _, claimed, err := manager.Claim(
				context.Background(), testCoordKey, testShardID, cons.leaseOwner, "peer", time.Minute,
			); err != nil || !claimed {
				t.Errorf("Claim() = (%v, %v), want a successful takeover", claimed, err)
			}
		})
		return nil
	}}
	cons = newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "the page to be handled after the takeover", func() bool { return len(collector.delivered()) >= 2 })
	waitFor(t, "the shard worker to stop", func() bool {
		owners, err := manager.List(context.Background(), testCoordKey)
		return err == nil && owners[testShardID] == "peer"
	})
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil: losing a shard is a handoff, not a consumer failure", err)
	}

	if stored, err := store.Get(context.Background(), testCoordKey, testShardID); err != nil || stored != "" {
		t.Fatalf("checkpoint = (%q, %v), want no write from a worker whose lease moved on", stored, err)
	}
}

func TestUnfencedConsumerStillCheckpointsWhenBackendsDoNotPair(t *testing.T) {
	// A fenced-capable store paired with a different lease manager keeps the
	// original unfenced behavior instead of failing the run.
	storeManager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStoreWithLeaseManager(storeManager)
	consumerManager := lease.NewMemoryManager()
	stream := newFakeStream(testShardID, sequences(100, 2)...)

	collector := &recordCollector{}
	cons := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, consumerManager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "the unfenced checkpoint", func() bool {
		stored, err := store.Get(context.Background(), testCoordKey, testShardID)
		return err == nil && stored == "101"
	})
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"100", "101"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v", got, want)
	}
}

func TestFencedConsumerWritesProgressThroughSession(t *testing.T) {
	manager := lease.NewMemoryManager()
	plain := checkpoint.NewMemoryStore()
	stub := &stubSession{}
	store := &stubFencedStore{Store: plain, session: stub}
	stream := newFakeStream(testShardID, sequences(100, 2)...)

	collector := &recordCollector{}
	cons := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "the fenced checkpoint", func() bool { return slices.Contains(stub.savedValues(), "101") })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	if stored, err := plain.Get(context.Background(), testCoordKey, testShardID); err != nil || stored != "" {
		t.Fatalf("unfenced checkpoint = (%q, %v), want progress written only through the session", stored, err)
	}
	waitFor(t, "the session to be invalidated", func() bool {
		stub.mu.Lock()
		defer stub.mu.Unlock()
		return stub.invalidated
	})
}

func TestFencedConsumerRecoversFromPersistedStateWhenNothingFetchedYet(t *testing.T) {
	store, manager := fencedBackends()
	seedRecovery(t, store, manager, func(session checkpoint.Session) {
		if _, err := session.Initialize(context.Background(), "100"); err != nil {
			t.Fatalf("Initialize() error = %v, want nil", err)
		}
		if err := session.Save(context.Background(), "100"); err != nil {
			t.Fatalf("Save() error = %v, want nil", err)
		}
	})

	stream := newFakeStream(testShardID, sequences(100, 3)...)
	// The first read is anchor verification; the consuming read after it
	// expires before this worker has fetched anything, so it has no local
	// position and must fall back to persisted recovery state.
	stream.getRecordsErrs = []error{nil, &types.ExpiredIteratorException{Message: aws.String("expired")}}

	collector := &recordCollector{}
	cons := newTestConsumerWithHandler(t, stream, store, manager, collector.handle)

	stop := startConsumer(t, cons)
	waitFor(t, "two delivered records", func() bool { return len(collector.delivered()) >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	want := []string{"101", "102"}
	if got := collector.delivered(); !slices.Equal(got, want) {
		t.Fatalf("delivered = %v, want %v resumed from the persisted checkpoint", got, want)
	}
}
