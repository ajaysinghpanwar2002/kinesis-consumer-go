//go:build integration

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// errFailFastBoom is the permanent handler error used to drive fail-fast. It is a
// package-level sentinel so the test can assert errors.Is against Start's return.
var errFailFastBoom = errors.New("integration: permanent handler failure")

// TestFailFastReturnsHandlerError proves the fail-fast failure policy (#14): when a
// handler permanently errors and the consumer is configured with
// WithFailurePolicy(FailurePolicyFailFast), Start returns the wrapped handler error
// (rather than skipping the record or blocking forever), and nothing is
// checkpointed for the shard.
//
// The discriminating assertion is errors.Is(startErr, errFailFastBoom) &&
// !isCanceled(startErr): that pair separates fail-fast from the skip/dlq policies.
// Under skip, Start would never return on its own (it would keep polling), so the
// bounded run context is a safety net that turns a non-failing policy into a
// DeadlineExceeded — which fails the errors.Is check with a clear message rather
// than hanging the suite. The companion mutation (flip the policy to
// FailurePolicySkip) is documented in TASK_PLAN and confirms the check fails.
//
// The store-state assertion is a second angle: fail-fast must not have committed
// the poison record. Checkpointing is page-boundary-after-success
// (saveShardCheckpointIfDue runs only once handleRecordsPage returns nil), the page
// never completes when a record errors, and no drain/catch-up runs on an
// internal-cancel fail-fast, so the checkpoint stays empty regardless of which
// record failed.
func TestFailFastReturnsHandlerError(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-failfast")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const (
		shardCount = 1
		total      = 10
	)

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

	// Pin every record to the single shard so the failing handler is reached in a
	// deterministic order.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != shardCount {
		t.Fatalf("expected %d shard hash key, got %d", shardCount, len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}
	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != shardCount {
		t.Fatalf("expected %d shard, got %d", shardCount, len(shardIDs))
	}
	shardID := shardIDs[0]

	payloads := makePayloads("failfast", total)
	putRecordsToShard(ctx, t, client, stream, hashKey, payloads)

	// Handler permanently fails on every record. Count invocations to prove it
	// actually ran (rather than Start failing for an unrelated reason).
	var calls atomic.Int64
	handler := func(_ context.Context, _ consumer.Record) error {
		calls.Add(1)
		return errFailFastBoom
	}

	cfg := consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithFailurePolicy(consumer.FailurePolicyFailFast),
		consumer.WithRetry(1, time.Millisecond), // WithRetry(1, 1ms): backoff must be > 0 by validation but is unused at 1 attempt
		consumer.WithBatching(10, 1),
		consumer.WithShardConcurrency(1), // sequential ordered handling, so the exact call-count assertion below is deterministic
		consumer.WithPolling(200*time.Millisecond, time.Second),
	)
	if err != nil {
		t.Fatalf("create fail-fast consumer: %v", err)
	}

	// Run Start with a bounded context so a non-failing policy cannot hang the test.
	// The time.After guard is slightly longer than the context bound, so even a
	// Start that ignores its context deadline (a real bug) fails rather than hangs.
	const runBudget = 20 * time.Second
	runCtx, cancel := context.WithTimeout(context.Background(), runBudget)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- cons.Start(runCtx) }()

	var startErr error
	select {
	case startErr = <-errCh:
	case <-time.After(runBudget + 10*time.Second):
		t.Fatal("Start did not return within the run budget; fail-fast did not stop the consumer")
	}

	t.Logf("Start returned err=%v after %d handler calls", startErr, calls.Load())

	// (1) Load-bearing: Start surfaced the handler error, and it is not a mere
	// cancellation/deadline. Under skip/dlq this would be nil-or-hang instead.
	if startErr == nil {
		t.Fatal("Start returned nil; fail-fast did not surface the handler error")
	}
	if isCanceled(startErr) {
		t.Fatalf("Start returned a cancellation/deadline error %v; fail-fast did not surface the handler error (a non-failing policy would let the run budget expire)", startErr)
	}
	if !errors.Is(startErr, errFailFastBoom) {
		t.Fatalf("Start error does not wrap the handler error; got %v", startErr)
	}

	// (2) The handler ran exactly once: with shard concurrency 1, records are
	// handled in order, so the first record's failure fails the page and fail-fast
	// aborts before any later record is attempted. More than one call would mean
	// fail-fast let processing continue past the first failure.
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler was invoked %d times; fail-fast must abort on the first failing record (want exactly 1)", got)
	}

	// (3) Second angle: fail-fast committed nothing for the shard.
	seq, err := store.Get(ctx, stream, shardID)
	if err != nil {
		t.Fatalf("read checkpoint for shard %s: %v", shardID, err)
	}
	if seq != "" {
		t.Fatalf("fail-fast checkpointed %q for shard %s; the poison page must not be committed", seq, shardID)
	}
}
