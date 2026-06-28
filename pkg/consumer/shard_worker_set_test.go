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

	workers.add("shard-1", func() { atomic.AddInt32(&cancelled, 1) })
	if !workers.has("shard-1") {
		t.Fatal("has(shard-1) = false, want true")
	}

	workers.done("shard-1")
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

	workers.add("", func() { atomic.AddInt32(&cancelled, 1) })
	workers.add("shard-1", nil)

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

func TestShardWorkerSetStopCancelsAndRemovesOneWorker(t *testing.T) {
	t.Parallel()

	workers := newShardWorkerSet()
	var cancelledA int32
	var cancelledB int32

	workers.add("shard-a", func() { atomic.AddInt32(&cancelledA, 1) })
	workers.add("shard-b", func() { atomic.AddInt32(&cancelledB, 1) })

	if !workers.stop("shard-a") {
		t.Fatal("stop(shard-a) = false, want true")
	}
	if workers.has("shard-a") {
		t.Fatal("has(shard-a) = true after stop, want false")
	}
	if !workers.has("shard-b") {
		t.Fatal("has(shard-b) = false after stopping shard-a, want true")
	}
	if got := atomic.LoadInt32(&cancelledA); got != 1 {
		t.Fatalf("shard-a cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&cancelledB); got != 0 {
		t.Fatalf("shard-b cancel calls = %d, want 0", got)
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

	workers.add("shard-1", func() {
		workers.done("shard-1")
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

	workers.add("shard-1", func() {
		workers.done("shard-1")
		close(done)
	})

	workers.stopAll()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopAll cancel did not complete; cancel may have been called while holding the worker set lock")
	}
}
