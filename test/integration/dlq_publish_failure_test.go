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

// errDLQFailHandlerBoom is the permanent handler error that drives a record to the
// DLQ path, and errDLQPublishBoom is the error the DLQ publisher itself returns.
// Both are package-level sentinels so the test can assert errors.Is against Start's
// return: the publish error is the load-bearing discriminator (fail-fast would wrap
// only the handler error, never the publish error).
var (
	errDLQFailHandlerBoom = errors.New("integration: permanent handler failure for dlq publish")
	errDLQPublishBoom     = errors.New("integration: dlq publish failed")
)

// failingDLQ is a DLQPublisher whose Publish always fails. It counts invocations so
// the test can prove the DLQ path was actually reached before Start returned.
type failingDLQ struct {
	calls atomic.Int64
}

func (d *failingDLQ) Publish(_ context.Context, _ consumer.PoisonRecord) error {
	d.calls.Add(1)
	return errDLQPublishBoom
}

// TestDLQPublishFailureFailsConsumer proves the DLQ publish-failure behavior (#17):
// when the failure policy is FailurePolicySendToDLQ but the DLQPublisher itself
// fails, the consumer must NOT silently checkpoint past the poison record as it does
// on a successful publish (IT-10a). Instead applyFailurePolicy returns the wrapped
// publish error, which propagates out of Start (like fail-fast, IT-8), and nothing
// is checkpointed for the shard.
//
// The discriminating assertion is errors.Is(startErr, errDLQPublishBoom): that is
// exactly what separates this from every other policy. Fail-fast wraps only the
// handler error; skip and a SUCCESSFUL send-to-dlq return nil and never surface an
// error at all. The bounded run context is a safety net — a non-failing policy (the
// documented mutation: make Publish return nil, turning send-to-dlq into skip) never
// returns on its own, so the run budget expires as a DeadlineExceeded, which fails
// the !isCanceled / errors.Is checks with a clear message rather than hanging.
//
// The store-state assertion is the second, behavior-defining angle: unlike a
// successful publish (which checkpoints past the record), a failed publish must
// commit nothing. Checkpointing is page-boundary-after-success
// (saveShardCheckpointIfDue runs only once handleRecordsPage returns nil); the page
// never completes when the publish error propagates, and no drain/catch-up runs on
// this internal-cancel path, so the checkpoint stays empty.
func TestDLQPublishFailureFailsConsumer(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-dlqfail")
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

	payloads := makePayloads("dlqfail", total)
	putRecordsToShard(ctx, t, client, stream, hashKey, payloads)

	// Handler permanently fails on every record, routing it to the DLQ. Count
	// invocations to prove the handler ran (rather than Start failing for an
	// unrelated reason).
	var calls atomic.Int64
	handler := func(_ context.Context, _ consumer.Record) error {
		calls.Add(1)
		return errDLQFailHandlerBoom
	}

	dlq := &failingDLQ{}
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithFailurePolicy(consumer.FailurePolicySendToDLQ),
		consumer.WithDLQPublisher(dlq),
		consumer.WithRetry(1, time.Millisecond), // 1 attempt: backoff must be > 0 by validation but is unused
		consumer.WithBatching(10, 1),
		consumer.WithShardConcurrency(1), // sequential ordered handling makes the exact call-count assertions deterministic
		consumer.WithPolling(200*time.Millisecond, time.Second),
	)
	if err != nil {
		t.Fatalf("create dlq-publish-failure consumer: %v", err)
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
		t.Fatal("Start did not return within the run budget; the DLQ publish failure did not stop the consumer")
	}

	t.Logf("Start returned err=%v after %d handler calls and %d dlq publish calls", startErr, calls.Load(), dlq.calls.Load())

	// (1) Load-bearing: Start surfaced the DLQ PUBLISH error, and it is not a mere
	// cancellation/deadline. This is the discriminator: fail-fast wraps only the
	// handler error, and a successful send-to-dlq (or skip) returns nil and would
	// let the run budget expire instead.
	if startErr == nil {
		t.Fatal("Start returned nil; a failed DLQ publish must not be silently swallowed")
	}
	if isCanceled(startErr) {
		t.Fatalf("Start returned a cancellation/deadline error %v; the DLQ publish failure was not surfaced (a successful publish would let the run budget expire)", startErr)
	}
	if !errors.Is(startErr, errDLQPublishBoom) {
		t.Fatalf("Start error does not wrap the DLQ publish error; got %v", startErr)
	}

	// (2) The chain also carries the underlying handler error (applyFailurePolicy
	// wraps both), confirming the failure originated from the poison record.
	if !errors.Is(startErr, errDLQFailHandlerBoom) {
		t.Fatalf("Start error does not wrap the underlying handler error; got %v", startErr)
	}

	// (3) The DLQ publish path was actually exercised exactly once: with shard
	// concurrency 1 the first record fails, retries exhaust, and the single publish
	// attempt fails and aborts the page before any later record is attempted.
	if got := dlq.calls.Load(); got != 1 {
		t.Fatalf("DLQ Publish was invoked %d times; want exactly 1 (the first poison record aborts the page)", got)
	}

	// (4) The handler ran exactly once for the same reason.
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler was invoked %d times; the publish failure must abort on the first failing record (want exactly 1)", got)
	}

	// (5) Second angle / behavior-defining: unlike a successful publish, a failed
	// publish committed nothing for the shard.
	seq, err := store.Get(ctx, integrationCoordinationIdentity(stream), shardID)
	if err != nil {
		t.Fatalf("read checkpoint for shard %s: %v", shardID, err)
	}
	if seq != "" {
		t.Fatalf("a failed DLQ publish checkpointed %q for shard %s; the poison page must not be committed", seq, shardID)
	}
}
