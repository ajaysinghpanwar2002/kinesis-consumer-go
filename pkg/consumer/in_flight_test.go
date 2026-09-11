package consumer

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func testAdmission(t *testing.T, limits InFlightLimits) *admissionController {
	t.Helper()
	opts, err := applyOptions([]Option{WithInFlightLimits(limits)})
	if err != nil {
		t.Fatal(err)
	}
	a := newAdmissionController(opts.inFlight)
	t.Cleanup(a.stop)
	return a
}

func admissionState(a *admissionController) (admissionUsage, int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total, a.slots, len(a.queue)
}

func TestInFlightLimitDefaultsAndValidation(t *testing.T) {
	defaults := InFlightLimits{1000, 10 << 20, 10000, 64 << 20, 4}
	a := testAdmission(t, InFlightLimits{})
	if a.limits != defaults {
		t.Fatalf("defaults = %+v", a.limits)
	}
	fields := reflect.ValueOf(&defaults).Elem()
	for i := 0; i < fields.NumField(); i++ {
		invalid := defaults
		reflect.ValueOf(&invalid).Elem().Field(i).SetInt(-1)
		if _, err := applyOptions([]Option{WithInFlightLimits(invalid)}); err == nil {
			t.Fatalf("field %d accepted negative", i)
		}
	}
	opts, err := applyOptions(nil)
	if err != nil || newAdmissionController(opts.inFlight) != nil {
		t.Fatal("automatic defaults must not enable limits")
	}
}

func TestAdmissionAtomicLimitsAndEligibleOrder(t *testing.T) {
	a := testAdmission(t, InFlightLimits{MaxRecordsPerShard: 2, MaxBytesPerShard: 5, MaxRecordsPerInstance: 3, MaxBytesPerInstance: 7})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := a.acquire(ctx, "a", []int{3, 2, 0}, false)
	if err != nil || len(first.sizes) != 2 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	blocked := make(chan *admissionReservation, 1)
	go func() { r, _ := a.acquire(ctx, "a", []int{1}, false); blocked <- r }()
	waitFor(t, "shard a queued", func() bool { _, _, n := admissionState(a); return n == 1 })
	second, err := a.acquire(ctx, "b", []int{2, 0}, false)
	if err != nil || len(second.sizes) != 1 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	total, _, _ := admissionState(a)
	if total != (admissionUsage{3, 7}) {
		t.Fatalf("usage=%+v", total)
	}
	first.releaseRecord(0)
	var third *admissionReservation
	select {
	case third = <-blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if third == nil {
		t.Fatal("eligible waiter did not acquire")
	}
	first.release()
	first.release()
	second.release()
	third.release()
	total, _, q := admissionState(a)
	if total != (admissionUsage{}) || q != 0 || len(a.shards) != 0 {
		t.Fatalf("leaked accounting: %+v queue=%d", total, q)
	}
}

func TestAdmissionZeroBytesAndBothByteScopes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits InFlightLimits
		want   int
	}{
		{"shard", InFlightLimits{MaxBytesPerShard: 3, MaxBytesPerInstance: 10}, 2},
		{"instance", InFlightLimits{MaxBytesPerShard: 10, MaxBytesPerInstance: 3}, 2},
		{"zero bytes count", InFlightLimits{MaxRecordsPerInstance: 2}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAdmission(t, tc.limits)
			sizes := []int{1, 2, 1}
			if tc.name == "zero bytes count" {
				sizes = []int{0, 0, 0}
			}
			r, err := a.acquire(context.Background(), "a", sizes, false)
			if err != nil || len(r.sizes) != tc.want {
				t.Fatalf("reservation=%+v err=%v", r, err)
			}
			r.release()
		})
	}
}

func TestAdmissionCancellationAndFIFOFetchSlots(t *testing.T) {
	a := testAdmission(t, InFlightLimits{MaxFetchSlots: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	held, _ := a.acquire(ctx, "held", nil, true)
	canceled, cancelWait := context.WithCancel(ctx)
	failed := make(chan error, 1)
	go func() { _, err := a.acquire(canceled, "canceled", nil, true); failed <- err }()
	waitFor(t, "canceled waiter queued", func() bool { _, _, n := admissionState(a); return n == 1 })
	cancelWait()
	if err := <-failed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	results := make(chan string, 2)
	releases := make(chan *admissionReservation, 2)
	for i, shard := range []string{"first", "second"} {
		go func() {
			r, err := a.acquire(ctx, shard, nil, true)
			if err == nil {
				results <- shard
				releases <- r
			}
		}()
		waitFor(t, "slot waiter queued", func() bool { _, _, n := admissionState(a); return n == i+1 })
	}
	held.release()
	select {
	case got := <-results:
		if got != "first" {
			t.Fatalf("first grant=%s", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	(<-releases).release()
	select {
	case got := <-results:
		if got != "second" {
			t.Fatalf("second grant=%s", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	(<-releases).release()
	_, slots, q := admissionState(a)
	if slots != 0 || q != 0 {
		t.Fatalf("slots=%d queue=%d", slots, q)
	}
}

func TestAdmissionGrantCancellationRace(t *testing.T) {
	a := testAdmission(t, InFlightLimits{MaxRecordsPerInstance: 1})
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		held, _ := a.acquire(ctx, "held", []int{1}, false)
		done := make(chan struct{})
		go func() { defer close(done); r, _ := a.acquire(ctx, "waiter", []int{1}, false); r.release() }()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); held.release() }()
		go func() { defer wg.Done(); cancel() }()
		wg.Wait()
		<-done
		total, _, q := admissionState(a)
		if total != (admissionUsage{}) || q != 0 {
			t.Fatalf("leaked %+v q=%d", total, q)
		}
	}
}
