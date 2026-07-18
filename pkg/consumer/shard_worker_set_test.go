package consumer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestShardWorkerSetAddHasDone(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelled int32

	gen := workers.add("shard-1", func() { atomic.AddInt32(&cancelled, 1) })
	if gen == 0 {
		t.Fatal("add(shard-1) generation = 0, want non-zero")
	}
	if !workers.has("shard-1") {
		t.Fatal("has(shard-1) = false, want true")
	}

	workers.done("shard-1", gen)
	if workers.has("shard-1") {
		t.Fatal("has(shard-1) = true after done, want false")
	}
	if got := atomic.LoadInt32(&cancelled); got != 0 {
		t.Fatalf("cancel calls = %d, want 0", got)
	}
}

func TestShardWorkerSetAddIgnoresEmptyShardIDAndNilCancel(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelled int32

	if gen := workers.add("", func() { atomic.AddInt32(&cancelled, 1) }); gen != 0 {
		t.Fatalf("add(empty) generation = %d, want 0", gen)
	}
	if gen := workers.add("shard-1", nil); gen != 0 {
		t.Fatalf("add(shard-1, nil) generation = %d, want 0", gen)
	}

	if workers.has("") {
		t.Fatal("has(empty) = true, want false")
	}
	if workers.has("shard-1") {
		t.Fatal("has(shard-1) = true after nil cancel add, want false")
	}
	if got := atomic.LoadInt32(&cancelled); got != 0 {
		t.Fatalf("cancel calls = %d, want 0", got)
	}
}

func TestShardWorkerSetDoneWithZeroGenerationRemovesNothing(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	workers.add("shard-1", func() {})

	workers.done("shard-1", 0)
	if !workers.has("shard-1") {
		t.Fatal("has(shard-1) = false after done with generation 0, want true")
	}
}

func TestShardWorkerSetStaleDoneDoesNotDeregisterReplacement(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var oldCancelled int32
	var newCancelled int32

	// Old worker is stopped (shed), the shard is re-acquired, and only then
	// does the old worker goroutine's deferred done run — after its lease
	// release. That stale done must not delete the replacement registration.
	oldGen := workers.add("shard-1", func() { atomic.AddInt32(&oldCancelled, 1) })
	if !workers.stop("shard-1") {
		t.Fatal("stop(shard-1) = false, want true")
	}
	newGen := workers.add("shard-1", func() { atomic.AddInt32(&newCancelled, 1) })
	if newGen == oldGen {
		t.Fatalf("replacement generation = %d, want different from old generation %d", newGen, oldGen)
	}

	workers.done("shard-1", oldGen)

	if !workers.has("shard-1") {
		t.Fatal("has(shard-1) = false after stale done, want true (replacement must survive)")
	}
	if !workers.stop("shard-1") {
		t.Fatal("stop(shard-1) = false after stale done, want true (replacement must stay stoppable)")
	}
	if got := atomic.LoadInt32(&oldCancelled); got != 1 {
		t.Fatalf("old worker cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&newCancelled); got != 1 {
		t.Fatalf("replacement cancel calls = %d, want 1", got)
	}
}

func TestShardWorkerSetStopCancelsOneWorkerAndKeepsItsFence(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelledA int32
	var cancelledB int32

	genA := workers.add("shard-a", func() { atomic.AddInt32(&cancelledA, 1) })
	workers.add("shard-b", func() { atomic.AddInt32(&cancelledB, 1) })

	if !workers.stop("shard-a") {
		t.Fatal("stop(shard-a) = false, want true")
	}
	// The registration must survive stop as the local stale-worker fence:
	// the shard stays ineligible for re-acquire/claim until the worker's
	// deferred done runs (after its lease release).
	if !workers.has("shard-a") {
		t.Fatal("has(shard-a) = false after stop, want true until the worker finishes")
	}
	if workers.running("shard-a") {
		t.Fatal("running(shard-a) = true after stop, want false")
	}
	if !workers.has("shard-b") {
		t.Fatal("has(shard-b) = false after stopping shard-a, want true")
	}
	if !workers.running("shard-b") {
		t.Fatal("running(shard-b) = false after stopping shard-a, want true")
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&cancelledB); got != 0 {
		t.Fatalf("shard-b cancel calls = %d, want 0", got)
	}

	// A second stop of the stopping worker is a no-op: no double cancel, no
	// phantom move for callers counting stop results.
	if workers.stop("shard-a") {
		t.Fatal("stop(shard-a) = true while already stopping, want false")
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls after redundant stop = %d, want 1", got)
	}

	// The worker's deferred done lifts the fence.
	workers.done("shard-a", genA)
	if workers.has("shard-a") {
		t.Fatal("has(shard-a) = true after done, want false")
	}
}

func TestShardWorkerSetStopMissingReturnsFalse(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	if workers.stop("missing") {
		t.Fatal("stop(missing) = true, want false")
	}
}

func TestShardWorkerSetStopCancelsOutsideLock(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	done := make(chan struct{})

	var gen uint64
	gen = workers.add("shard-1", func() {
		workers.done("shard-1", gen)
		close(done)
	})

	if !workers.stop("shard-1") {
		t.Fatal("stop(shard-1) = false, want true")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop cancel did not complete; cancel may have been called while holding the worker set lock")
	}
}

func TestShardWorkerSetStopAllCancelsEveryWorkerOnceAndClearsSet(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelledA int32
	var cancelledB int32

	workers.add("shard-a", func() { atomic.AddInt32(&cancelledA, 1) })
	workers.add("shard-b", func() { atomic.AddInt32(&cancelledB, 1) })

	workers.stopAll()

	if workers.has("shard-a") {
		t.Fatal("has(shard-a) = true after stopAll, want false")
	}
	if workers.has("shard-b") {
		t.Fatal("has(shard-b) = true after stopAll, want false")
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&cancelledB); got != 1 {
		t.Fatalf("shard-b cancel calls = %d, want 1", got)
	}

	workers.stopAll()

	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls after second stopAll = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&cancelledB); got != 1 {
		t.Fatalf("shard-b cancel calls after second stopAll = %d, want 1", got)
	}
}

func TestShardWorkerSetStopAllCancelsOutsideLock(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	done := make(chan struct{})

	var gen uint64
	gen = workers.add("shard-1", func() {
		workers.done("shard-1", gen)
		close(done)
	})

	workers.stopAll()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopAll cancel did not complete; cancel may have been called while holding the worker set lock")
	}
}
