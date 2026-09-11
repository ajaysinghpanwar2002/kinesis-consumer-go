package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const (
	testShardID    = "shard-1"
	testCoordKey   = "group:stream"
	testLeaseOwner = "owner-1"
)

// fencedFixture wires the two built-in memory backends together the way a
// fenced deployment does: one lease manager shared by the store and the
// consumer.
type fencedFixture struct {
	consumer *Consumer
	store    *checkpoint.MemoryStore
	manager  *lease.MemoryManager
	stream   *fakeStream
	held     lease.FencedLease
	session  *shardSession
	ctx      context.Context
}

func newFencedFixture(t *testing.T, stream *fakeStream, opts ...Option) *fencedFixture {
	t.Helper()

	manager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStoreWithLeaseManager(manager)
	f := &fencedFixture{store: store, manager: manager, stream: stream}
	f.consumer = newTestConsumer(t, stream, store, manager, opts...)
	f.held = acquireFencedLease(t, manager, testShardID)

	session, err := f.consumer.bindShardSession(context.Background(), testShardID, f.held)
	if err != nil {
		t.Fatalf("bindShardSession() error = %v, want nil", err)
	}
	if session == nil {
		t.Fatal("bindShardSession() = nil, want a fenced session")
	}
	f.session = session
	f.ctx = withShardSession(context.Background(), session)
	return f
}

func newTestConsumer(t *testing.T, client KinesisAPI, store checkpoint.Store, manager lease.Manager, opts ...Option) *Consumer {
	t.Helper()
	return newTestConsumerWithHandler(t, client, store, manager, func(context.Context, Record) error { return nil }, opts...)
}

func newTestConsumerWithHandler(
	t *testing.T,
	client KinesisAPI,
	store checkpoint.Store,
	manager lease.Manager,
	handler HandlerFunc,
	opts ...Option,
) *Consumer {
	t.Helper()
	return newTestConsumerWithConfig(t, Config{
		StreamName:    "stream",
		ConsumerGroup: "group",
		StartPosition: StartLatest,
	}, client, store, manager, handler, opts...)
}

func newTestConsumerWithConfig(
	t *testing.T,
	cfg Config,
	client KinesisAPI,
	store checkpoint.Store,
	manager lease.Manager,
	handler HandlerFunc,
	opts ...Option,
) *Consumer {
	t.Helper()

	base := []Option{
		WithLeaseManager(manager),
		WithRetry(1, time.Millisecond),
		WithHeartbeat(10*time.Millisecond, 2*time.Second),
		WithBatching(10, 1),
		WithPolling(time.Millisecond, time.Second),
		WithIdleTimeBetweenReads(0),
	}
	cons, err := New(cfg, client, store, handler, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	return cons
}

func acquireFencedLease(t *testing.T, manager *lease.MemoryManager, shardID string) lease.FencedLease {
	t.Helper()

	held, acquired, err := manager.Acquire(context.Background(), testCoordKey, shardID, testLeaseOwner, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("Acquire() = (%v, %v, %v), want a held lease", held, acquired, err)
	}
	fenced, ok := held.(lease.FencedLease)
	if !ok {
		t.Fatalf("Acquire() returned %T, want a lease.FencedLease", held)
	}
	return fenced
}

func TestBindShardSessionSkipsUnfencedStore(t *testing.T) {
	manager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStore()
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager)
	held := acquireFencedLease(t, manager, testShardID)

	session, err := cons.bindShardSession(context.Background(), testShardID, held)
	if err != nil {
		t.Fatalf("bindShardSession() error = %v, want nil", err)
	}
	if session != nil {
		t.Fatal("bindShardSession() bound a session for a store with no lease manager, want the unfenced path")
	}
}

func TestBindShardSessionSkipsUnfencedLease(t *testing.T) {
	manager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStoreWithLeaseManager(manager)
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager)

	session, err := cons.bindShardSession(context.Background(), testShardID, unfencedLease{})
	if err != nil {
		t.Fatalf("bindShardSession() error = %v, want nil", err)
	}
	if session != nil {
		t.Fatal("bindShardSession() bound a session for a lease that is not fenced, want the unfenced path")
	}
}

func TestBindShardSessionSkipsMismatchedBackends(t *testing.T) {
	storeManager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStoreWithLeaseManager(storeManager)
	otherManager := lease.NewMemoryManager()
	cons := newTestConsumer(t, newFakeStream(testShardID), store, otherManager)
	held := acquireFencedLease(t, otherManager, testShardID)

	session, err := cons.bindShardSession(context.Background(), testShardID, held)
	if err != nil {
		t.Fatalf("bindShardSession() error = %v, want nil", err)
	}
	if session != nil {
		t.Fatal("bindShardSession() bound a session across two lease managers, want the unfenced path")
	}
}

func TestBindShardSessionReportsInconsistentRecoveryState(t *testing.T) {
	manager := lease.NewMemoryManager()
	store := &stubFencedStore{
		Store:   checkpoint.NewMemoryStore(),
		bindErr: fmt.Errorf("%w: registry and value disagree", checkpoint.ErrRecoveryState),
	}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager)
	reporter := &recordingReporter{}
	cons.reporter = reporter
	held := acquireFencedLease(t, manager, testShardID)

	session, err := cons.bindShardSession(context.Background(), testShardID, held)
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("bindShardSession() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if session != nil {
		t.Fatal("bindShardSession() returned a session alongside a recovery error")
	}
	// Both built-in stores validate recovery inside Bind, so a startup failure
	// has no session to report it.
	if got := len(reporter.countersNamed(metricRecoveryFailures)); got != 1 {
		t.Fatalf("%s = %d, want 1", metricRecoveryFailures, got)
	}
}

func TestBindShardSessionFallsBackWhenBackendsDoNotPair(t *testing.T) {
	manager := lease.NewMemoryManager()
	store := &stubFencedStore{Store: checkpoint.NewMemoryStore(), bindErr: lease.ErrLeaseMismatch}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager)
	held := acquireFencedLease(t, manager, testShardID)

	session, err := cons.bindShardSession(context.Background(), testShardID, held)
	if err != nil {
		t.Fatalf("bindShardSession() error = %v, want nil", err)
	}
	if session != nil {
		t.Fatal("bindShardSession() bound a mismatched pair, want the unfenced path")
	}
}

func TestFencedShardIteratorResumesInclusivelyFromInitialPosition(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}

	iterator, page, err := f.consumer.getShardIterator(f.ctx, testShardID)
	if err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	assertAnchorPage(t, iterator, page)
	got := pageSequences(page.output.Records)
	want := []string{"100", "101", "102"}
	if !slices.Equal(got, want) {
		t.Fatalf("resumed page = %v, want %v (the initial position is inclusive)", got, want)
	}
	assertIteratorType(t, stream, types.ShardIteratorTypeAtSequenceNumber, "100")
}

func TestFencedShardIteratorDeliversTheVerifiedPage(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	// Retention drops the anchor the instant the read that proved it readable
	// returns. Deriving a second iterator now would silently start at 101,
	// with nothing left to notice that the protected record went missing.
	stream.afterGetRecords = func(int) { stream.trimLocked(1) }

	iterator, page, err := f.consumer.getShardIterator(f.ctx, testShardID)
	if err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	assertAnchorPage(t, iterator, page)
	if got := aws.ToString(page.output.Records[0].SequenceNumber); got != "100" {
		t.Fatalf("first resumed sequence = %q, want 100: the verified page is delivered, not read again", got)
	}
	if got := len(stream.iteratorRequests()); got != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1: resuming from an anchor derives one iterator, not two", got)
	}
}

func TestFencedShardIteratorResumesAfterCheckpoint(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	if err := f.session.save(context.Background(), "100"); err != nil {
		t.Fatalf("save() error = %v, want nil", err)
	}

	iterator, page, err := f.consumer.getShardIterator(f.ctx, testShardID)
	if err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	assertAnchorPage(t, iterator, page)
	got := pageSequences(page.output.Records)
	want := []string{"101", "102"}
	if !slices.Equal(got, want) {
		t.Fatalf("resumed page = %v, want %v (a checkpoint is exclusive)", got, want)
	}
	// The anchor is read inclusively — that is what proves it is still there —
	// and dropped from the page instead of skipped by a second derivation.
	assertIteratorType(t, stream, types.ShardIteratorTypeAtSequenceNumber, "100")
}

func TestFencedShardIteratorMarksAnAnchorOnlyPageEmptied(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	// One record per page, so the exclusive continuation's page carries the
	// anchor and nothing else: dropping it leaves an empty page that says
	// nothing about whether the shard has more records.
	f := newFencedFixture(t, stream, WithBatching(1, 1))
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	if err := f.session.save(context.Background(), "100"); err != nil {
		t.Fatalf("save() error = %v, want nil", err)
	}

	iterator, page, err := f.consumer.getShardIterator(f.ctx, testShardID)
	if err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	if page == nil || iterator != "" {
		t.Fatalf("getShardIterator() = (%q, %v), want the verified page and no iterator", iterator, page)
	}
	if got := pageSequences(page.output.Records); len(got) != 0 {
		t.Fatalf("resumed page = %v, want it emptied by dropping the anchor", got)
	}
	if !page.emptied {
		t.Fatal("page is not marked emptied, so the pass would read it as the shard tip and sleep a poll interval")
	}
	if page.output.NextShardIterator == nil {
		t.Fatal("emptied page has no NextShardIterator, so the shard has nothing to read on from")
	}
}

func TestFencedShardIteratorReportsCompletedShard(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	f := newFencedFixture(t, stream)
	if err := f.session.save(context.Background(), shardCompletionValue("100")); err != nil {
		t.Fatalf("save() error = %v, want nil", err)
	}

	_, _, err := f.consumer.getShardIterator(f.ctx, testShardID)
	if !errors.Is(err, errShardCompleted) {
		t.Fatalf("getShardIterator() error = %v, want %v", err, errShardCompleted)
	}
}

func TestFencedShardIteratorUsesStartPositionWhenFresh(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)

	if _, _, err := f.consumer.getShardIterator(f.ctx, testShardID); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}
	assertIteratorType(t, stream, types.ShardIteratorTypeLatest, "")
	if !f.session.needsInitialPosition() {
		t.Fatal("session does not require an initial position after a fresh recovery read")
	}
}

func TestFencedShardIteratorSurfacesRecoveryStateFailure(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	// Deleting the value behind the registry entry is the partial metadata
	// loss the contract requires a consumer to halt on.
	if err := f.store.Delete(context.Background(), testCoordKey, testShardID); err != nil {
		t.Fatalf("Delete() error = %v, want nil", err)
	}

	_, _, err := f.consumer.getShardIterator(f.ctx, testShardID)
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("getShardIterator() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
}

func TestInitializeShardRecoveryPersistsFirstFetchedSequence(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)
	if _, _, err := f.consumer.getShardIterator(f.ctx, testShardID); err != nil {
		t.Fatalf("getShardIterator() error = %v, want nil", err)
	}

	resume, err := f.consumer.initializeShardRecovery(f.ctx, testShardID, f.session, stream.records[0])
	if err != nil {
		t.Fatalf("initializeShardRecovery() error = %v, want nil", err)
	}
	if resume {
		t.Fatal("initializeShardRecovery() asked to resume elsewhere after writing the position itself")
	}
	position, err := f.session.recovery(context.Background())
	if err != nil {
		t.Fatalf("recovery() error = %v, want nil", err)
	}
	if position.Kind != checkpoint.RecoveryInitial || position.Sequence != "100" {
		t.Fatalf("recovery position = %+v, want an initial position at 100", position)
	}
	if f.session.needsInitialPosition() {
		t.Fatal("session still requires an initial position after writing one")
	}
}

func TestInitializeShardRecoveryYieldsToExistingPosition(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	f := newFencedFixture(t, stream)
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	f.session.setFresh(true) // a stale local view: the position already exists

	resume, err := f.consumer.initializeShardRecovery(f.ctx, testShardID, f.session, stream.records[2])
	if err != nil {
		t.Fatalf("initializeShardRecovery() error = %v, want nil", err)
	}
	if !resume {
		t.Fatal("initializeShardRecovery() overwrote an existing initial position, want it to resume from that position")
	}
	position, err := f.session.recovery(context.Background())
	if err != nil {
		t.Fatalf("recovery() error = %v, want nil", err)
	}
	if position.Sequence != "100" {
		t.Fatalf("initial position = %q, want the first one ever written (100)", position.Sequence)
	}
}

func TestInitializeShardRecoveryRejectsRecordWithoutSequence(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	f := newFencedFixture(t, stream)

	_, err := f.consumer.initializeShardRecovery(f.ctx, testShardID, f.session, Record{})
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("initializeShardRecovery() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
}

func TestFencedCheckpointSaveStopsOnLostOwnership(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	f := newFencedFixture(t, stream)
	if _, err := f.session.initialize(context.Background(), "100"); err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	// A peer takes the shard over.
	if _, claimed, err := f.manager.Claim(context.Background(), testCoordKey, testShardID, testLeaseOwner, "owner-2", time.Minute); err != nil || !claimed {
		t.Fatalf("Claim() = (%v, %v), want a successful transfer", claimed, err)
	}

	err := f.consumer.saveShardCheckpoint(f.ctx, testShardID, "100")
	if !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("saveShardCheckpoint() error = %v, want %v", err, lease.ErrNotOwned)
	}
	if stored, _ := f.store.Get(context.Background(), testCoordKey, testShardID); stored != "" {
		t.Fatalf("checkpoint = %q, want no write from a worker that lost the lease", stored)
	}
}

func TestFencedCheckpointSaveDoesNotRetryPermanentFailures(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	f := newFencedFixture(t, stream, WithRetry(3, time.Millisecond))
	f.session.invalidate()

	reporter := &recordingReporter{}
	f.consumer.reporter = reporter
	if err := f.consumer.saveShardCheckpoint(f.ctx, testShardID, "100"); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("saveShardCheckpoint() error = %v, want %v", err, lease.ErrNotOwned)
	}
	if got := len(reporter.countersNamed(metricCheckpointFailures)); got != 0 {
		t.Fatalf("%s = %d, want 0: a lost lease is a handoff, not a store failure", metricCheckpointFailures, got)
	}
}

func TestFencedRecoveryFailureIsCounted(t *testing.T) {
	manager := lease.NewMemoryManager()
	stub := &stubSession{recoveryErr: fmt.Errorf("%w: value is missing", checkpoint.ErrRecoveryState)}
	store := &stubFencedStore{Store: checkpoint.NewMemoryStore(), session: stub}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager)
	reporter := &recordingReporter{}
	cons.reporter = reporter
	session := &shardSession{c: cons, session: stub, shardID: testShardID}

	if _, err := session.recovery(context.Background()); !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("recovery() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if got := len(reporter.countersNamed(metricRecoveryFailures)); got != 1 {
		t.Fatalf("%s = %d, want 1", metricRecoveryFailures, got)
	}
}

func TestBindShardSessionRetriesTransientFailures(t *testing.T) {
	manager := lease.NewMemoryManager()
	store := &stubFencedStore{Store: checkpoint.NewMemoryStore(), transientBindFailures: 1}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager, WithRetry(3, time.Millisecond))
	held := acquireFencedLease(t, manager, testShardID)

	session, err := cons.bindShardSession(context.Background(), testShardID, held)
	if err != nil {
		t.Fatalf("bindShardSession() error = %v, want nil: a dropped connection is not a recovery failure", err)
	}
	if session == nil {
		t.Fatal("bindShardSession() = nil, want a session bound on the retry")
	}
	if got := store.calls(); got != 2 {
		t.Fatalf("Bind calls = %d, want 2", got)
	}
}

func TestBindShardSessionDoesNotRetryUnusableRecoveryState(t *testing.T) {
	manager := lease.NewMemoryManager()
	store := &stubFencedStore{
		Store:   checkpoint.NewMemoryStore(),
		bindErr: fmt.Errorf("%w: registry and value disagree", checkpoint.ErrRecoveryState),
	}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager, WithRetry(3, time.Millisecond))
	held := acquireFencedLease(t, manager, testShardID)

	if _, err := cons.bindShardSession(context.Background(), testShardID, held); !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("bindShardSession() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if got := store.calls(); got != 1 {
		t.Fatalf("Bind calls = %d, want 1: unusable recovery state is an answer, not a blip", got)
	}
}

func TestFencedRecoveryReadRetriesTransientFailures(t *testing.T) {
	manager := lease.NewMemoryManager()
	stub := &stubSession{
		position:                  checkpoint.RecoveryPosition{Kind: checkpoint.RecoveryCheckpoint, Sequence: "100"},
		transientRecoveryFailures: 1,
	}
	store := &stubFencedStore{Store: checkpoint.NewMemoryStore(), session: stub}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager, WithRetry(3, time.Millisecond))
	session := &shardSession{c: cons, session: stub, shardID: testShardID}

	position, err := session.recovery(context.Background())
	if err != nil {
		t.Fatalf("recovery() error = %v, want nil: the read this replaced retried too", err)
	}
	if position.Sequence != "100" {
		t.Fatalf("recovery position = %+v, want the checkpoint at 100", position)
	}
	if reads, _ := stub.calls(); reads != 2 {
		t.Fatalf("Recovery calls = %d, want 2", reads)
	}
}

func TestFencedRecoveryReadDoesNotRetryLostOwnership(t *testing.T) {
	manager := lease.NewMemoryManager()
	stub := &stubSession{recoveryErr: fmt.Errorf("read recovery: %w", lease.ErrNotOwned)}
	store := &stubFencedStore{Store: checkpoint.NewMemoryStore(), session: stub}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager, WithRetry(3, time.Millisecond))
	session := &shardSession{c: cons, session: stub, shardID: testShardID}

	if _, err := session.recovery(context.Background()); !errors.Is(err, lease.ErrNotOwned) {
		t.Fatalf("recovery() error = %v, want %v", err, lease.ErrNotOwned)
	}
	if reads, _ := stub.calls(); reads != 1 {
		t.Fatalf("Recovery calls = %d, want 1: a handoff cannot be retried into ownership", reads)
	}
}

func TestFencedInitializeRetriesTransientFailures(t *testing.T) {
	manager := lease.NewMemoryManager()
	stub := &stubSession{transientInitializeFailures: 1}
	store := &stubFencedStore{Store: checkpoint.NewMemoryStore(), session: stub}
	cons := newTestConsumer(t, newFakeStream(testShardID), store, manager, WithRetry(3, time.Millisecond))
	session := &shardSession{c: cons, session: stub, shardID: testShardID}

	position, err := session.initialize(context.Background(), "100")
	if err != nil {
		t.Fatalf("initialize() error = %v, want nil", err)
	}
	if position.Kind != checkpoint.RecoveryInitial || position.Sequence != "100" {
		t.Fatalf("initial position = %+v, want an initial position at 100", position)
	}
	// The write is once-only, so repeating it can only re-observe the position
	// that is already there.
	if _, writes := stub.calls(); writes != 2 {
		t.Fatalf("Initialize calls = %d, want 2", writes)
	}
}

// assertAnchorPage checks the shape of an anchored resumption: the page that
// proved the anchor readable, and no iterator, so the pass handles the page
// before it can read past it.
func assertAnchorPage(t *testing.T, iterator string, page *pendingShardPage) {
	t.Helper()

	if page == nil {
		t.Fatal("getShardIterator() returned no page, want the verified anchor page")
	}
	if iterator != "" {
		t.Fatalf("getShardIterator() = %q, want no iterator alongside a pending page", iterator)
	}
	if len(page.output.Records) == 0 {
		t.Fatal("verified anchor page carries no records")
	}
}

func assertIteratorType(t *testing.T, stream *fakeStream, want types.ShardIteratorType, wantSequence string) {
	t.Helper()

	calls := stream.iteratorRequests()
	if len(calls) == 0 {
		t.Fatal("no GetShardIterator calls recorded")
	}
	last := calls[len(calls)-1]
	if last.ShardIteratorType != want {
		t.Fatalf("iterator type = %s, want %s", last.ShardIteratorType, want)
	}
	if got := aws.ToString(last.StartingSequenceNumber); got != wantSequence {
		t.Fatalf("iterator sequence = %q, want %q", got, wantSequence)
	}
}

// unfencedLease is a lease.Lease with no fencing capability, standing in for a
// custom lease manager written against the original interface.
type unfencedLease struct{}

func (unfencedLease) Renew(context.Context, time.Duration) error { return nil }
func (unfencedLease) Release(context.Context) error              { return nil }

var _ lease.Lease = unfencedLease{}

// stubFencedStore is a checkpoint.FencedStore with scripted binding, so
// consumer-level handling of binding and recovery failures does not depend on
// how a particular backend stores its metadata.
type stubFencedStore struct {
	checkpoint.Store

	mu      sync.Mutex
	bindErr error
	// transientBindFailures fails that many Bind calls with a plain error
	// before the first success, standing in for a dropped connection.
	transientBindFailures int
	bindCalls             int
	session               *stubSession

	// bindGate, when set, holds every Bind open until it is closed, standing
	// in for a backend slow enough that the caller's own retry budget elapses.
	// It is fixed at construction, so no lock guards it.
	bindGate <-chan struct{}
}

func (s *stubFencedStore) Bind(ctx context.Context, _ string, _ string, _ lease.FencedLease) (checkpoint.Session, error) {
	if s.bindGate != nil {
		select {
		case <-s.bindGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindCalls++
	if s.transientBindFailures > 0 {
		s.transientBindFailures--
		return nil, errors.New("connection reset")
	}
	if s.bindErr != nil {
		return nil, s.bindErr
	}
	if s.session == nil {
		s.session = &stubSession{}
	}
	return s.session, nil
}

func (s *stubFencedStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bindCalls
}

type stubSession struct {
	mu            sync.Mutex
	position      checkpoint.RecoveryPosition
	recoveryErr   error
	initializeErr error
	saveErr       error
	saved         []string
	invalidated   bool
	// transient*Failures fail that many calls with a plain error before the
	// first success, standing in for a dropped connection.
	transientRecoveryFailures   int
	transientInitializeFailures int
	recoveryCalls               int
	initializeCalls             int
}

func (s *stubSession) Recovery(context.Context) (checkpoint.RecoveryPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryCalls++
	if s.transientRecoveryFailures > 0 {
		s.transientRecoveryFailures--
		return checkpoint.RecoveryPosition{}, errors.New("connection reset")
	}
	if s.recoveryErr != nil {
		return checkpoint.RecoveryPosition{}, s.recoveryErr
	}
	return s.position, nil
}

func (s *stubSession) Initialize(_ context.Context, sequence string) (checkpoint.RecoveryPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeCalls++
	if s.transientInitializeFailures > 0 {
		s.transientInitializeFailures--
		return checkpoint.RecoveryPosition{}, errors.New("connection reset")
	}
	if s.initializeErr != nil {
		return checkpoint.RecoveryPosition{}, s.initializeErr
	}
	if s.position.Kind == checkpoint.RecoveryFresh {
		s.position = checkpoint.RecoveryPosition{Kind: checkpoint.RecoveryInitial, Sequence: sequence}
	}
	return s.position, nil
}

func (s *stubSession) Save(_ context.Context, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = append(s.saved, value)
	s.position = checkpoint.RecoveryPosition{Kind: checkpoint.RecoveryCheckpoint, Sequence: value}
	return nil
}

func (s *stubSession) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidated = true
}

func (s *stubSession) calls() (recovery, initialize int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoveryCalls, s.initializeCalls
}

func (s *stubSession) savedValues() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.saved...)
}

var _ checkpoint.FencedStore = (*stubFencedStore)(nil)
