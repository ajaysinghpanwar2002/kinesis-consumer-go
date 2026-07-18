package backend

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSlotTrackerUnlimited(t *testing.T) {
	for _, max := range []int{0, -1} {
		t.Run(fmt.Sprintf("max=%d", max), func(t *testing.T) {
			tracker := NewSlotTracker(max)
			// Many reservations always succeed under the unlimited setting.
			for i := 0; i < 100; i++ {
				release, ok := tracker.Reserve(fmt.Sprintf("k%d", i))
				if !ok {
					t.Fatalf("Reserve #%d ok = false, want true (unlimited)", i)
				}
				if release == nil {
					t.Fatalf("Reserve #%d release = nil, want no-op func", i)
				}
				release() // no-op, must not panic
			}
		})
	}
}

func TestSlotTrackerNilReceiver(t *testing.T) {
	var tracker *SlotTracker
	if got := tracker.Max(); got != 0 {
		t.Fatalf("nil Max() = %d, want 0", got)
	}
	release, ok := tracker.Reserve("k")
	if !ok {
		t.Fatal("nil Reserve ok = false, want true (unlimited)")
	}
	if release == nil {
		t.Fatal("nil Reserve release = nil, want no-op func")
	}
	release() // must not panic
}

func TestSlotTrackerBoundedFillRejectReuse(t *testing.T) {
	tracker := NewSlotTracker(2)
	if got := tracker.Max(); got != 2 {
		t.Fatalf("Max() = %d, want 2", got)
	}

	releaseA, ok := tracker.Reserve("a")
	if !ok {
		t.Fatal("Reserve(a) ok = false, want true")
	}
	_, ok = tracker.Reserve("b")
	if !ok {
		t.Fatal("Reserve(b) ok = false, want true")
	}

	// Limit reached: next reservation is rejected with a nil release.
	release, ok := tracker.Reserve("c")
	if ok {
		t.Fatal("Reserve(c) ok = true, want false (limit reached)")
	}
	if release != nil {
		t.Fatal("Reserve(c) release != nil, want nil on rejection")
	}

	// Releasing a slot lets a new key reserve again.
	releaseA()
	if _, ok := tracker.Reserve("c"); !ok {
		t.Fatal("Reserve(c) after release ok = false, want true")
	}
}

func TestSlotTrackerReleaseFreesKey(t *testing.T) {
	tracker := NewSlotTracker(1)

	release, ok := tracker.Reserve("a")
	if !ok {
		t.Fatal("Reserve(a) ok = false, want true")
	}
	if _, ok := tracker.Reserve("b"); ok {
		t.Fatal("Reserve(b) ok = true, want false (limit reached)")
	}
	release()
	if _, ok := tracker.Reserve("b"); !ok {
		t.Fatal("Reserve(b) after release ok = false, want true")
	}
}

func TestSlotTrackerRedundantReserveIsRefused(t *testing.T) {
	tracker := NewSlotTracker(2)

	releaseA, ok := tracker.Reserve("a")
	if !ok {
		t.Fatal("Reserve(a) ok = false, want true")
	}

	// Reserving an already-held key must fail even with capacity to spare, and
	// must not disturb the original reservation.
	release, ok := tracker.Reserve("a")
	if ok {
		t.Fatal("redundant Reserve(a) ok = true, want false")
	}
	if release != nil {
		t.Fatal("redundant Reserve(a) release != nil, want nil on rejection")
	}

	// The original reservation still counts toward the limit exactly once.
	if _, ok := tracker.Reserve("b"); !ok {
		t.Fatal("Reserve(b) ok = false, want true (one slot still free)")
	}
	if _, ok := tracker.Reserve("c"); ok {
		t.Fatal("Reserve(c) ok = true, want false (limit reached)")
	}

	// The original release still works: release-then-reserve of the same key
	// succeeds.
	releaseA()
	if _, ok := tracker.Reserve("a"); !ok {
		t.Fatal("Reserve(a) after release ok = false, want true")
	}
}

func TestSlotTrackerUnlimitedAllowsDuplicateKeys(t *testing.T) {
	tracker := NewSlotTracker(0)

	// The unlimited path never tracks keys, so duplicate reservations succeed.
	releaseFirst, ok := tracker.Reserve("a")
	if !ok {
		t.Fatal("Reserve(a) ok = false, want true (unlimited)")
	}
	releaseSecond, ok := tracker.Reserve("a")
	if !ok {
		t.Fatal("duplicate Reserve(a) ok = false, want true (unlimited)")
	}
	releaseFirst()
	releaseSecond()
}

func TestSlotTrackerDoubleReleaseIsSafe(t *testing.T) {
	// The release func is intentionally not once-guarded; releasing twice must
	// be a harmless no-op (deleting an already-absent key) and must not free an
	// unrelated slot.
	tracker := NewSlotTracker(1)

	release, ok := tracker.Reserve("a")
	if !ok {
		t.Fatal("Reserve(a) ok = false, want true")
	}
	release()
	release() // second release must not panic or corrupt state

	releaseB, ok := tracker.Reserve("b")
	if !ok {
		t.Fatal("Reserve(b) ok = false, want true")
	}
	// The double release of "a" must not have granted extra capacity.
	if _, ok := tracker.Reserve("c"); ok {
		t.Fatal("Reserve(c) ok = true, want false (double release must not free extra capacity)")
	}
	releaseB()
}

func TestSlotTrackerConcurrentRespectsMax(t *testing.T) {
	const max = 4
	tracker := NewSlotTracker(max)

	var outstanding int64
	var maxObserved int64
	var wg sync.WaitGroup

	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("g%d-i%d", g, i)
				release, ok := tracker.Reserve(key)
				if !ok {
					continue
				}
				cur := atomic.AddInt64(&outstanding, 1)
				for {
					prev := atomic.LoadInt64(&maxObserved)
					if cur <= prev || atomic.CompareAndSwapInt64(&maxObserved, prev, cur) {
						break
					}
				}
				atomic.AddInt64(&outstanding, -1)
				release()
			}
		}(g)
	}
	wg.Wait()

	if maxObserved > max {
		t.Fatalf("observed %d concurrent reservations, want <= %d", maxObserved, max)
	}
}
