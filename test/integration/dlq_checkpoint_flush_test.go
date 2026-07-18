//go:build integration

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// errDLQFlushBoom is the permanent handler error for the poison record whose
// page must be checkpointed immediately after its DLQ publish.
var errDLQFlushBoom = errors.New("integration: permanent handler failure for dlq flush")

// recordsGatingClient wraps the real Kinesis client and, once a GetRecords
// response has contained the poison payload, blocks every subsequent
// GetRecords call until the caller's context is done. With the poison page
// pinned as the last page the consumer ever reads, none of the pre-existing
// checkpoint paths can run afterwards: the due-save path is disabled by a
// checkpointEvery larger than the stream, the caught-up tip flush needs an
// empty page the gate never returns, and there is no graceful drain. Any
// checkpoint that appears while the gate is closed can therefore only come
// from the immediate post-DLQ flush under test. Everything except GetRecords
// passes through untouched.
type recordsGatingClient struct {
	*kinesis.Client
	poison    string
	sawPoison atomic.Bool
}

func (g *recordsGatingClient) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	if g.sawPoison.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	out, err := g.Client.GetRecords(ctx, params, optFns...)
	if err == nil && out != nil {
		for _, rec := range out.Records {
			if string(rec.Data) == g.poison {
				g.sawPoison.Store(true)
				break
			}
		}
	}
	return out, err
}

// newDLQFlushConsumer builds a send-to-DLQ consumer with NO graceful drain
// (cancelling it is a crash stop that flushes nothing) and a checkpointEvery
// far larger than the whole stream (the periodic due-save path never fires).
func newDLQFlushConsumer(
	t *testing.T,
	stream string,
	client consumer.KinesisAPI,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	dlq consumer.DLQPublisher,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1000),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithRetry(2, 10*time.Millisecond),
		consumer.WithFailurePolicy(consumer.FailurePolicySendToDLQ),
		consumer.WithDLQPublisher(dlq),
		consumer.WithHeartbeat(200*time.Millisecond, 3*time.Second),
	)
	if err != nil {
		t.Fatalf("create dlq flush consumer for %s: %v", stream, err)
	}
	return cons
}

// TestDLQPageCheckpointFlushSurvivesCrashStop proves the Slice 9 fix (IT-27):
// after a successful page DLQ publish the checkpoint advances IMMEDIATELY,
// not at the next due checkpoint, so a crash right after the publish no
// longer replays the page and delivers its records both to the DLQ and fully
// processed downstream.
//
// The scenario is deterministic, not timing-based. A recordsGatingClient
// blocks GetRecords after the poison page, and the consumer runs with
// checkpointEvery larger than the stream and no graceful drain, closing every
// pre-existing checkpoint path (due-save, caught-up tip flush, drain flush).
// Before the fix the checkpoint provably stays empty forever and the wait
// below times out; with the fix the post-DLQ flush writes it while the gate
// is closed. The crash-stop + fresh-consumer half then shows the flushed
// checkpoint doing its real job: zero replay, zero double delivery.
func TestDLQPageCheckpointFlushSurvivesCrashStop(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("dlqflush")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}
	shardID := shardIDs[0]

	hashKeys := shardHashKeys(ctx, t, client, stream)
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	const (
		total1        = 20 // two 10-record pages; the poison ends page 2
		total2        = 10
		deliverBudget = 60 * time.Second
	)
	batch1 := makePayloads("dlqflush-b1", total1)
	poison := batch1[total1-1] // last record, so the poison page is the final page
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	store := newStore(t, uniqueName("dlqflush-kp"))
	t.Cleanup(func() { _ = store.Close() })

	gate := &recordsGatingClient{Client: client, poison: poison}
	coll1 := newCollector()
	dlq := &capturingDLQ{}
	handler := func(hctx context.Context, rec consumer.Record) error {
		if string(rec.Data) == poison {
			return errDLQFlushBoom
		}
		return coll1.handler()(hctx, rec)
	}
	consC1 := newDLQFlushConsumer(t, stream, gate, store, handler, dlq)
	cancelC1, stopC1 := runConsumer(t, consC1)
	defer stopC1()

	// Everything except the poison is delivered; the poison is DLQ'd once.
	nonPoison := batch1[:total1-1]
	if missing := coll1.waitFor(nonPoison, deliverBudget); len(missing) != 0 {
		t.Fatalf("C1 did not deliver the non-poison records: missing %d: %v", len(missing), missing)
	}
	waitForDLQCount(t, dlq, 1, deliverBudget)
	published := dlq.snapshot()
	if len(published) != 1 || string(published[0].Payload) != poison {
		t.Fatalf("DLQ captured %+v, want exactly the poison record", published)
	}

	// LOAD-BEARING: while the gate keeps the poison page as the last page ever
	// read, the ONLY code path that can save a checkpoint is the immediate
	// post-DLQ flush. Without the fix this wait times out with an empty value.
	flushed := waitForNonEmptyCheckpoint(ctx, t, store, stream, shardID, 30*time.Second)
	if published[0].SequenceNumber == "" || flushed != published[0].SequenceNumber {
		t.Fatalf("flushed checkpoint = %q, want the poison page's last sequence %q", flushed, published[0].SequenceNumber)
	}

	// Crash-stop C1 (no drain: nothing else is flushed on the way out).
	cancelC1()
	stopC1()

	// A fresh consumer with a never-failing handler resumes from the flushed
	// checkpoint: it must deliver only batch2 — zero batch1 replay and no
	// second trip of the poison through either path.
	batch2 := makePayloads("dlqflush-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	coll2 := newCollector()
	consC2 := newConsumer(t, stream, client, store, coll2.handler())
	_, stopC2 := runConsumer(t, consC2)
	defer stopC2()

	if missing := coll2.waitFor(batch2, deliverBudget); len(missing) != 0 {
		t.Fatalf("resumed C2 did not deliver batch2: missing %d/%d: %v", len(missing), total2, missing)
	}
	for _, p := range batch1 {
		if n := coll2.count(p); n != 0 {
			t.Fatalf("C2 replayed batch1 record %q %d times; the DLQ'd page must have been checkpointed past before the crash", p, n)
		}
	}
	if got := dlq.snapshot(); len(got) != 1 {
		t.Fatalf("DLQ captured %d records after resume, want still exactly 1 (no republish)", len(got))
	}

	t.Logf("dlq flush ok: checkpoint %q flushed behind the gate, crash-stop resume replayed 0/%d", flushed, total1)
}

// waitForDLQCount blocks until the DLQ has captured at least n records.
func waitForDLQCount(t *testing.T, dlq *capturingDLQ, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if len(dlq.snapshot()) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("DLQ did not capture %d records within %s; got %d", n, timeout, len(dlq.snapshot()))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForNonEmptyCheckpoint blocks until the shard's checkpoint has any
// stored value, returning it, and fails the test on timeout.
func waitForNonEmptyCheckpoint(ctx context.Context, t *testing.T, store interface {
	Get(context.Context, string, string) (string, error)
}, stream, shardID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		value, err := store.Get(ctx, integrationCoordinationIdentity(stream), shardID)
		if err != nil {
			lastErr = err
		} else if value != "" {
			return value
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint for shard %s stayed empty for %s (no post-DLQ flush); lastErr=%v", shardID, timeout, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
