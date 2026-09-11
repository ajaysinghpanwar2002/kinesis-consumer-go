package consumer

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func boundedTestConsumer(t *testing.T, limits InFlightLimits) *Consumer {
	return &Consumer{admission: testAdmission(t, limits), logger: slog.New(slog.DiscardHandler), reporter: metrics.Nop{}, cfg: trimHorizonConfig(), store: &fakeCheckpointSaveStore{}, tuning: tuningConfig{checkpointEvery: 1, retryMaxAttempts: 1}}
}

func testPage(sizes ...int) *kinesis.GetRecordsOutput {
	out := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("next")}
	for i, size := range sizes {
		out.Records = append(out.Records, Record{SequenceNumber: aws.String(sequences(100+i, 1)[0]), Data: make([]byte, size)})
	}
	return out
}

func TestBoundedBatchPrefixesReleaseBeforeCheckpointAndPreserveApplicationRecords(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerShard: 3, MaxBytesPerInstance: 5})
	out := testPage(2, 3, 1, 0, 4)
	var batches [][]Record
	c.batchHandler = func(_ context.Context, records []Record) error {
		batches = append(batches, records)
		records[0].Data = append(records[0].Data, 42)
		return nil
	}
	store := c.store.(*fakeCheckpointSaveStore)
	store.onSave = func() {
		total, _, _ := admissionState(c.admission)
		if total != (admissionUsage{}) {
			t.Errorf("capacity held during checkpoint: %+v", total)
		}
	}
	slot, _ := c.admission.acquire(context.Background(), "a", nil, true)
	defer slot.release()
	seq, count, err := c.processBoundedPage(context.Background(), context.Background(), "a", out, 0, slot, "")
	if err != nil || seq != "104" || count != 0 {
		t.Fatalf("result=(%s,%d,%v)", seq, count, err)
	}
	var got [][]string
	for _, batch := range batches {
		var seqs []string
		for _, r := range batch {
			seqs = append(seqs, aws.ToString(r.SequenceNumber))
			if len(r.Data) == 0 && aws.ToString(r.SequenceNumber) != "103" {
				t.Fatal("application data cleared")
			}
		}
		got = append(got, seqs)
	}
	if !reflect.DeepEqual(got, [][]string{{"100", "101"}, {"102", "103", "104"}}) {
		t.Fatalf("batches=%v", got)
	}
	for _, r := range out.Records {
		if r.Data != nil || r.SequenceNumber != nil {
			t.Fatal("staging retains payload/sequence")
		}
	}
	_, slots, _ := admissionState(c.admission)
	if slots != 0 {
		t.Fatal("fully admitted page retained slot")
	}
}

func TestBoundedConcurrentRecordsReleaseCompletedPayloadBehindGap(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerInstance: 2})
	c.tuning.shardConcurrency = 2
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	gap := make(chan struct{})
	completed := make(chan struct{})
	c.handler = func(_ context.Context, r Record) error {
		if aws.ToString(r.SequenceNumber) == "100" {
			select {
			case <-gap:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			close(completed)
		}
		return nil
	}
	r, _ := c.admission.acquire(ctx, "a", []int{2, 2}, false)
	prefix := testPage(2, 2)
	done := make(chan error, 1)
	go func() {
		_, _, err := c.processRecordsPageWithCheckpoint(context.WithValue(ctx, admittedRecordsKey{}, r), "a", prefix, 0)
		done <- err
	}()
	<-completed
	waitFor(t, "completed record capacity released", func() bool { total, _, _ := admissionState(c.admission); return total.records == 1 })
	// The reservation mutex synchronizes the release with this read of the
	// cleared slot. The unfinished callback only touches slot zero.
	c.admission.mu.Lock()
	cleared := prefix.Records[1].Data == nil
	c.admission.mu.Unlock()
	if !cleared {
		t.Error("completed record retained behind earlier gap")
	}
	other, err := c.admission.acquire(ctx, "b", []int{2}, false)
	if err != nil {
		t.Fatal(err)
	}
	other.release()
	close(gap)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBoundedStagingHoldsSlotAndFlushesProgressWhilePaused(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerInstance: 1, MaxFetchSlots: 1})
	c.tuning.checkpointEvery = 100
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := make(chan struct{})
	finish := make(chan struct{})
	c.handler = func(_ context.Context, r Record) error {
		if aws.ToString(r.SequenceNumber) == "100" {
			close(first)
			<-finish
		}
		return nil
	}
	slot, _ := c.admission.acquire(ctx, "a", nil, true)
	defer slot.release()
	saved := make(chan struct{})
	c.store.(*fakeCheckpointSaveStore).onSave = func() { close(saved) }
	done := make(chan error, 1)
	out := testPage(1, 1)
	go func() { _, _, err := c.processBoundedPage(ctx, ctx, "a", out, 0, slot, ""); done <- err }()
	<-first
	// A competing shard is first in the capacity queue; it will take the
	// released record budget before the staged suffix can acquire it.
	otherCh := make(chan *admissionReservation, 1)
	go func() { r, _ := c.admission.acquire(ctx, "b", []int{1}, false); otherCh <- r }()
	waitFor(t, "competing capacity request", func() bool { _, _, n := admissionState(c.admission); return n == 1 })
	close(finish)
	other := <-otherCh
	defer other.release()
	select {
	case <-saved:
	case <-ctx.Done():
		t.Fatal("checkpoint stalled behind capacity wait")
	}
	_, slots, q := admissionState(c.admission)
	if slots != 1 || q != 1 {
		t.Fatalf("slots=%d queue=%d", slots, q)
	}
	other.release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBoundedDrainDiscardsClosedShardStagingAndPreservesAdmittedContext(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	stream.closed = true
	entered := make(chan context.Context, 1)
	finish := make(chan struct{})
	var mu sync.Mutex
	var delivered []string
	handler := func(ctx context.Context, r Record) error {
		mu.Lock()
		delivered = append(delivered, aws.ToString(r.SequenceNumber))
		mu.Unlock()
		entered <- ctx
		<-finish
		return nil
	}
	c := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, handler, WithInFlightLimits(InFlightLimits{MaxRecordsPerInstance: 1, MaxFetchSlots: 1}), WithGracefulDrain(time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	callbackCtx := <-entered
	cancel()
	waitFor(t, "drain started", c.isDraining)
	if callbackCtx.Err() != nil {
		t.Fatal("admitted context canceled during drain")
	}
	close(finish)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain stalled")
	}
	position := readRecovery(t, store, manager)
	if position.Kind != checkpoint.RecoveryCheckpoint || position.Sequence != "100" {
		t.Fatalf("recovery=%+v", position)
	}
	mu.Lock()
	got := slices.Clone(delivered)
	mu.Unlock()
	if !slices.Equal(got, []string{"100"}) {
		t.Fatalf("delivered=%v", got)
	}
	total, slots, q := admissionState(c.admission)
	if total != (admissionUsage{}) || slots != 0 || q != 0 {
		t.Fatalf("leaked %+v slots=%d q=%d", total, slots, q)
	}
}

func TestBoundedOversizedFirstRecordPersistsReplayPosition(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, "100")
	c := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, func(context.Context, Record) error { t.Error("oversized record delivered"); return nil }, WithInFlightLimits(InFlightLimits{MaxBytesPerShard: 1}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := c.Start(ctx)
	var oversized *OversizedRecordError
	if !errors.Is(err, ErrOversizedRecord) || !errors.As(err, &oversized) || oversized.SequenceNumber != "100" {
		t.Fatalf("error=%v", err)
	}
	position := readRecovery(t, store, manager)
	if position.Kind != checkpoint.RecoveryInitial || position.Sequence != "100" {
		t.Fatalf("recovery=%+v", position)
	}
	total, slots, q := admissionState(c.admission)
	if total != (admissionUsage{}) || slots != 0 || q != 0 {
		t.Fatalf("leaked %+v slots=%d q=%d", total, slots, q)
	}
}

func TestBoundedAnchorWaitUsesFetchSlotsOutsideVerificationBudget(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxFetchSlots: 1})
	c.client = newFakeStream(testShardID, "100", "101")
	c.tuning.anchorVerifyBudget = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	slot, _ := c.admission.acquire(ctx, "other", nil, true)
	result := make(chan *pendingShardPage, 1)
	errs := make(chan error, 1)
	go func() {
		page, err := c.verifyShardAnchorPage(ctx, testShardID, "100", nil)
		result <- page
		errs <- err
	}()
	waitFor(t, "anchor slot wait", func() bool { _, _, n := admissionState(c.admission); return n == 1 })
	time.Sleep(20 * time.Millisecond)
	slot.release()
	page := <-result
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	defer page.slot.release()
	page.dropLeadingRecord()
	if aws.ToString(page.output.Records[0].SequenceNumber) != "101" {
		t.Fatal("lost continuation")
	}
	_, slots, _ := admissionState(c.admission)
	if slots != 1 {
		t.Fatal("verification page lost fetch slot")
	}
}

func TestBoundedWorkerRenewsWhileAdmissionPausedAndCancellationReleasesSlot(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, "100")
	c := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, func(context.Context, Record) error { t.Error("record admitted without capacity"); return nil }, WithInFlightLimits(InFlightLimits{MaxRecordsPerInstance: 1}), WithHeartbeat(time.Millisecond, time.Second))
	held := &admissionRenewingLease{FencedLease: acquireFencedLease(t, manager, testShardID)}
	budget, _ := c.admission.acquire(context.Background(), "other", []int{1}, false)
	defer budget.release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runShardWorker(ctx, c, testShardID, held)
	waitFor(t, "admission paused", func() bool { _, _, n := admissionState(c.admission); return n == 1 })
	before := held.renewals.Load()
	waitFor(t, "renewal while capacity is blocked", func() bool { return held.renewals.Load() > before })
	stream.mu.Lock()
	reads := stream.getRecordsN
	stream.mu.Unlock()
	if reads != 1 {
		t.Fatalf("blocked shard fetched %d pages", reads)
	}
	cancel()
	waitShardWorkerDone(t, done, nil)
	_, slots, q := admissionState(c.admission)
	if slots != 0 || q != 0 {
		t.Fatalf("slots=%d q=%d", slots, q)
	}
}

func TestBoundedPageFailureAndOversizedSuffixReleaseBudgets(t *testing.T) {
	for _, failure := range []string{"handler", "oversized suffix", "checkpoint"} {
		t.Run(failure, func(t *testing.T) {
			c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerInstance: 1, MaxBytesPerShard: 2, MaxFetchSlots: 1})
			expected := errors.New("failed")
			c.handler = func(context.Context, Record) error {
				if failure == "handler" {
					return expected
				}
				return nil
			}
			out := testPage(1, 1)
			if failure == "oversized suffix" {
				out = testPage(1, 3)
				expected = ErrOversizedRecord
			}
			if failure == "checkpoint" {
				c.store.(*fakeCheckpointSaveStore).saveErr = expected
			}
			slot, _ := c.admission.acquire(context.Background(), "a", nil, true)
			_, _, err := c.processBoundedPage(context.Background(), context.Background(), "a", out, 0, slot, "")
			slot.release()
			if !errors.Is(err, expected) {
				t.Fatalf("error=%v", err)
			}
			total, slots, q := admissionState(c.admission)
			if total != (admissionUsage{}) || slots != 0 || q != 0 {
				t.Fatalf("leaked %+v slots=%d q=%d", total, slots, q)
			}
			for _, r := range out.Records {
				if r.Data != nil {
					t.Fatal("discarded staging retains data")
				}
			}
		})
	}
}

func TestBoundedDrainWakesFetchAndAdmissionWaits(t *testing.T) {
	for _, fetch := range []bool{true, false} {
		t.Run(map[bool]string{true: "fetch", false: "admission"}[fetch], func(t *testing.T) {
			store, manager := fencedBackends()
			stream := newFakeStream(testShardID, "100")
			c := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, func(context.Context, Record) error { t.Error("unexpected delivery"); return nil }, WithInFlightLimits(InFlightLimits{MaxRecordsPerInstance: 1, MaxFetchSlots: 1}), WithGracefulDrain(0))
			held, _ := c.admission.acquire(context.Background(), "other", []int{1}, fetch)
			defer held.release()
			stop := startConsumer(t, c)
			waitFor(t, "capacity wait", func() bool { _, _, n := admissionState(c.admission); return n == 1 })
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			_, _, q := admissionState(c.admission)
			if q != 0 {
				t.Fatalf("queued after drain: %d", q)
			}
		})
	}
}

func TestBoundedExpiredIteratorAfterSplitPageKeepsSequence(t *testing.T) {
	store, manager := fencedBackends()
	stream := newFakeStream(testShardID, sequences(100, 6)...)
	stream.expireAfterRead = 1
	collector := &recordCollector{}
	c := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, collector.handle, WithBatching(3, 100), WithInFlightLimits(InFlightLimits{MaxRecordsPerInstance: 1, MaxFetchSlots: 1}))
	stop := startConsumer(t, c)
	waitFor(t, "all records after iterator expiry", func() bool { return len(collector.delivered()) >= 6 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := collector.delivered(); !slices.Equal(got, sequences(100, 6)) {
		t.Fatalf("delivery=%v", got)
	}
	_, slots, q := admissionState(c.admission)
	if slots != 0 || q != 0 {
		t.Fatalf("slots=%d queue=%d", slots, q)
	}
}

type admissionRenewingLease struct {
	lease.FencedLease
	renewals atomic.Int64
}

func (l *admissionRenewingLease) Renew(ctx context.Context, ttl time.Duration) error {
	err := l.FencedLease.Renew(ctx, ttl)
	if err == nil {
		l.renewals.Add(1)
	}
	return err
}

func TestBoundedPassFlushesPreviousPageBeforeFirstAdmissionWait(t *testing.T) {
	c := boundedTestConsumer(t, InFlightLimits{MaxRecordsPerInstance: 1, MaxFetchSlots: 1})
	c.tuning.checkpointEvery = 100
	c.tuning.batchSize = 1
	c.client = newFakeStream(testShardID, "100", "101")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered, finish := make(chan struct{}), make(chan struct{})
	c.handler = func(_ context.Context, record Record) error {
		if aws.ToString(record.SequenceNumber) != "100" {
			t.Error("second page admitted without capacity")
			return nil
		}
		close(entered)
		<-finish
		return nil
	}
	saved := make(chan struct{})
	store := c.store.(*fakeCheckpointSaveStore)
	store.onSave = func() { close(saved) }
	done := make(chan error, 1)
	go func() { _, _, _, err := c.processShardRecordsPass(ctx, testShardID, 0, ""); done <- err }()
	<-entered
	otherCh := make(chan *admissionReservation, 1)
	go func() { r, _ := c.admission.acquire(ctx, "other", []int{1}, false); otherCh <- r }()
	waitFor(t, "other shard queued", func() bool { _, _, n := admissionState(c.admission); return n == 1 })
	close(finish)
	other := <-otherCh
	defer other.release()
	select {
	case <-saved:
	case <-ctx.Done():
		t.Fatal("prior-page checkpoint stalled behind first admission of the next page")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("pass error = %v", err)
	}
	if len(store.saveCalls) != 1 || store.saveCalls[0].sequenceNumber != "100" {
		t.Fatalf("checkpoint writes = %+v", store.saveCalls)
	}
	_, slots, q := admissionState(c.admission)
	if slots != 0 || q != 0 {
		t.Fatalf("slots=%d queue=%d", slots, q)
	}
}
