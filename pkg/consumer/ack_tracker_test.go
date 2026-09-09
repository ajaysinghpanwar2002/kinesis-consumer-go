package consumer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func testDelivery(t *ackTracker, sequence string) Delivery {
	return t.append(Record{SequenceNumber: &sequence, Data: []byte(sequence)})
}

func validAckTracker() *ackTracker {
	return newAckTracker("shard-1", func(context.Context) error { return nil })
}

func requireAck(t *testing.T, d Delivery) {
	t.Helper()
	if err := d.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func requireProgress(t *testing.T, tracker *ackTracker, sequence string, count uint64) {
	t.Helper()
	if got, n := tracker.progress(); got != sequence || n != count {
		t.Fatalf("progress = (%q, %d), want (%q, %d)", got, n, sequence, count)
	}
}

func TestDeliveryCopiesAndValidation(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	validationErr := backendErr
	calls := 0
	tracker := newAckTracker("shard-1", func(context.Context) error {
		calls++
		return validationErr
	})
	d := testDelivery(tracker, "100")
	copy := d
	copy.Record = Record{}
	copy.ShardID = "changed"
	if err := copy.Ack(context.Background()); !errors.Is(err, backendErr) {
		t.Fatalf("validation failure = %v", err)
	}
	requireProgress(t, tracker, "", 0)
	validationErr = nil
	requireAck(t, copy)
	requireAck(t, d)
	requireProgress(t, tracker, "100", 1)
	if calls != 3 {
		t.Fatalf("ownership validation calls = %d, want 3", calls)
	}
	validationErr = backendErr
	if err := d.Ack(context.Background()); !errors.Is(err, backendErr) {
		t.Fatalf("duplicate must validate ownership: %v", err)
	}
	tracker.invalidate()
	validationErr = nil
	for _, stale := range []Delivery{d, copy, {}, testDelivery(tracker, "200")} {
		if err := stale.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
			t.Fatalf("invalid session/zero handle = %v", err)
		}
	}
	requireProgress(t, tracker, "100", 1)
}

func TestAckTrackerDeliveryOrderAcrossPages(t *testing.T) {
	tracker := validAckTracker()
	// Deliberately nonnumeric and unordered strings: ordering belongs to delivery.
	firstPage := []Delivery{testDelivery(tracker, "z"), testDelivery(tracker, "a")}
	secondPage := []Delivery{testDelivery(tracker, "99999999999999999999999999999"), testDelivery(tracker, "b")}
	requireAck(t, secondPage[1])
	requireAck(t, firstPage[1])
	requireProgress(t, tracker, "", 0)
	requireAck(t, firstPage[0])
	requireProgress(t, tracker, "a", 2)
	requireAck(t, secondPage[0])
	requireProgress(t, tracker, "b", 4)
	if tracker.ranges.Len() != 0 {
		t.Fatal("completed prefix still tracked")
	}
}

func TestAckTrackerCompactsBehindGap(t *testing.T) {
	tracker := validAckTracker()
	gap := testDelivery(tracker, "first")
	for i := range 10000 {
		d := testDelivery(tracker, fmt.Sprint(i))
		requireAck(t, d)
		if d.state.element != nil {
			t.Fatal("accepted handle retains a tracking element")
		}
		if tracker.ranges.Len() != 2 {
			t.Fatalf("metadata grew behind one gap: %d ranges", tracker.ranges.Len())
		}
	}
	requireProgress(t, tracker, "", 0)
	requireAck(t, gap)
	requireProgress(t, tracker, "9999", 10001)
}

func TestAckTrackerRandomCompletionOrder(t *testing.T) {
	tracker := validAckTracker()
	const size = 200
	deliveries := make([]Delivery, size)
	done := make([]bool, size)
	for i := range deliveries {
		deliveries[i] = testDelivery(tracker, fmt.Sprint(i))
	}
	prefix := 0
	rng := rand.New(rand.NewPCG(1, 2))
	for _, index := range rng.Perm(size) {
		requireAck(t, deliveries[index])
		done[index] = true
		for prefix < size && done[prefix] {
			prefix++
		}
		sequence := ""
		if prefix != 0 {
			sequence = fmt.Sprint(prefix - 1)
		}
		requireProgress(t, tracker, sequence, uint64(prefix))
		// No two completed ranges may remain adjacent, regardless of ack order.
		previousDone := false
		for e := tracker.ranges.Front(); e != nil; e = e.Next() {
			r := e.Value.(*ackRange)
			if r.done && previousDone {
				t.Fatal("adjacent completed ranges were not compacted")
			}
			previousDone = r.done
		}
	}
}

func TestAckTrackerPartialRetry(t *testing.T) {
	tracker := validAckTracker()
	batch := []Delivery{testDelivery(tracker, "1"), testDelivery(tracker, "2"), testDelivery(tracker, "3")}
	requireAck(t, batch[1])
	pending := tracker.retry(batch)
	if len(pending) != 2 || *pending[0].Record.SequenceNumber != "1" || *pending[1].Record.SequenceNumber != "3" {
		t.Fatalf("retry did not preserve unfinished order: %+v", pending)
	}
	for _, index := range []int{0, 2} {
		if err := batch[index].Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
			t.Fatalf("failed attempt still valid: %v", err)
		}
		if batch[index].state.element != nil {
			t.Fatal("failed handle retains tracking entry")
		}
	}
	requireAck(t, batch[1])
	if again := tracker.retry(batch); len(again) != 0 {
		t.Fatal("same failure created competing retry handles")
	}
	requireAck(t, pending[1])
	requireProgress(t, tracker, "", 0)
	requireAck(t, pending[0])
	requireProgress(t, tracker, "3", 3)
}

func TestAckTrackerConcurrentDuplicateCalls(t *testing.T) {
	tracker := validAckTracker()
	var wg sync.WaitGroup
	for i := range 100 {
		d := testDelivery(tracker, fmt.Sprint(i))
		for range 4 {
			wg.Go(func() {
				if err := d.Ack(context.Background()); err != nil {
					t.Error(err)
				}
			})
		}
	}
	wg.Wait()
	requireProgress(t, tracker, "99", 100)
}

func TestAckTrackerAcceptanceRacesRetry(t *testing.T) {
	for range 200 {
		tracker := validAckTracker()
		d := testDelivery(tracker, "1")
		start := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			<-start
			result <- d.Ack(context.Background())
		}()
		close(start)
		pending := tracker.retry([]Delivery{d})
		err := <-result
		switch {
		case err == nil:
			if len(pending) != 0 {
				t.Fatal("accepted delivery was retried")
			}
		case errors.Is(err, ErrStaleDelivery):
			if len(pending) != 1 {
				t.Fatal("unaccepted delivery was lost")
			}
			requireAck(t, pending[0])
		default:
			t.Fatal(err)
		}
		requireProgress(t, tracker, "1", 1)
	}
}

func TestAckTrackerCancellationDuringValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tracker := newAckTracker("shard-1", func(context.Context) error {
		cancel()
		return nil
	})
	d := testDelivery(tracker, "1")
	if err := d.Ack(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	requireProgress(t, tracker, "", 0)
	requireAck(t, d)
}

func TestAckTrackerRetryDuringValidation(t *testing.T) {
	validating := make(chan struct{})
	finishValidation := make(chan struct{})
	var once sync.Once
	tracker := newAckTracker("shard-1", func(context.Context) error {
		once.Do(func() { close(validating) })
		<-finishValidation
		return nil
	})
	d := testDelivery(tracker, "1")
	ackResult := make(chan error, 1)
	go func() { ackResult <- d.Ack(context.Background()) }()
	<-validating
	pending := tracker.retry([]Delivery{d})
	requireProgress(t, tracker, "", 0)
	close(finishValidation)
	if err := <-ackResult; !errors.Is(err, ErrStaleDelivery) {
		t.Fatalf("invalidated attempt accepted after validation: %v", err)
	}
	if len(pending) != 1 {
		t.Fatal("retry lost unfinished delivery")
	}
	requireAck(t, pending[0])
	requireProgress(t, tracker, "1", 1)
}

func TestAckTrackerInvalidationSeversOutstandingLinks(t *testing.T) {
	tracker := validAckTracker()
	first := testDelivery(tracker, "1")
	second := testDelivery(tracker, "2")
	tracker.invalidate()
	for _, d := range []Delivery{first, second} {
		if e := d.state.element; e.Next() != nil || e.Prev() != nil {
			t.Fatal("stopped handle retains other tracking entries")
		}
		if err := d.Ack(context.Background()); !errors.Is(err, ErrStaleDelivery) {
			t.Fatalf("stopped handle = %v", err)
		}
	}
	if pending := tracker.retry([]Delivery{first, second}); len(pending) != 0 {
		t.Fatal("stopped tracker generated retries")
	}
}

func TestDeliveryDoesNotMutateApplicationPayload(t *testing.T) {
	tracker := validAckTracker()
	d := testDelivery(tracker, "payload")
	data := d.Record.Data
	requireAck(t, d)
	if string(data) != "payload" || string(d.Record.Data) != "payload" {
		t.Fatal("acknowledgment mutated application data")
	}
}
