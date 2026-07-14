//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// errDLQBoom is the permanent handler error for the DLQ-bound poison record.
var errDLQBoom = errors.New("integration: permanent handler failure for dlq")

// capturingDLQ is a DLQPublisher that records every published PoisonRecord. It is
// mutex-guarded so the test can read the captured records after the consumer stops.
type capturingDLQ struct {
	mu      sync.Mutex
	records []consumer.PoisonRecord
}

func (d *capturingDLQ) Publish(_ context.Context, rec consumer.PoisonRecord) error {
	d.mu.Lock()
	d.records = append(d.records, rec)
	d.mu.Unlock()
	return nil
}

func (d *capturingDLQ) snapshot() []consumer.PoisonRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]consumer.PoisonRecord, len(d.records))
	copy(out, d.records)
	return out
}

// TestDLQCapturesPoisonAndContinues proves the DLQ failure policy (#16): a record
// whose handler permanently fails, under WithFailurePolicy(FailurePolicySendToDLQ)
// with a DLQPublisher, is published to the DLQ exactly once with full and correct
// PoisonRecord metadata, and the consumer then CONTINUES past it and checkpoints
// past it (send-to-dlq behaves like skip once the publish succeeds).
//
// Structure mirrors IT-9a: C1 processes batch1 (with a mid-stream poison), the
// poison is DLQ'd, C1 is stopped, batch2 is produced, and a fresh C2 resumes.
// C2 replaying zero of batch1 proves the DLQ'd record's page was checkpointed.
//
// The metadata half needs no mutation: capture-count == 1 with correct fields is
// itself the discriminator (under Skip the publisher captures 0; a re-poll would
// capture 2). The companion mutation for the checkpoint half (fresh empty store
// for C2 -> replay > 0) is documented in TASK_PLAN and verified out of band.
func TestDLQCapturesPoisonAndContinues(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-dlq")
	keyPrefix := uniqueName("kcg-it-ckpt")
	const (
		shardCount  = 1
		total1      = 30
		poisonIdx   = 15
		maxAttempts = 2 // the poison exhausts these, then is sent to the DLQ
		total2      = 20
	)

	createStream(ctx, t, client, stream, shardCount)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

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

	batch1 := makePayloads("dlq-b1", total1)
	poison := batch1[poisonIdx]
	wantPartitionKey := fmt.Sprintf("pk-%d", poisonIdx) // putRecordsToShard sets pk-<index>
	nonPoison1 := make([]string, 0, total1-1)
	for i, p := range batch1 {
		if i != poisonIdx {
			nonPoison1 = append(nonPoison1, p)
		}
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, batch1)

	// Phase 1: C1 processes batch1; the poison permanently fails and is sent to the DLQ.
	collC1 := newCollector()
	record := collC1.handler()
	dlqHandler := func(hctx context.Context, rec consumer.Record) error {
		if string(rec.Data) == poison {
			return errDLQBoom
		}
		return record(hctx, rec)
	}
	dlq := &capturingDLQ{}
	cfg := consumer.Config{StreamName: stream, StartPosition: consumer.StartTrimHorizon}
	consumerC1, err := consumer.New(cfg, client, store, dlqHandler,
		consumer.WithFailurePolicy(consumer.FailurePolicySendToDLQ),
		consumer.WithDLQPublisher(dlq),
		consumer.WithRetry(maxAttempts, 10*time.Millisecond),
		consumer.WithBatching(10, 1),
		consumer.WithShardConcurrency(1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create dlq consumer C1: %v", err)
	}
	_, stopC1 := runConsumer(t, consumerC1)

	// Continuation: every non-poison record, including those after the poison, is
	// delivered. Records 16..19 share the poison's page, so their delivery implies
	// the poison was already handled (published) by the time this returns.
	if missing := collC1.waitFor(nonPoison1, 90*time.Second); missing != nil {
		stopC1()
		t.Fatalf("C1 did not deliver all non-poison batch1 records; missing %d/%d: %v", len(missing), len(nonPoison1), missing)
	}
	stopC1()

	// (1) The poison was published to the DLQ exactly once.
	published := dlq.snapshot()
	if len(published) != 1 {
		t.Fatalf("DLQ captured %d records; want exactly 1 (the single poison record)", len(published))
	}
	got := published[0]

	// (2) Full metadata on the captured poison record.
	if got.StreamName != stream {
		t.Errorf("poison StreamName = %q; want %q", got.StreamName, stream)
	}
	if got.StreamARN != "" {
		t.Errorf("poison StreamARN = %q; want empty (only StreamName was configured)", got.StreamARN)
	}
	if got.ShardID != shardID {
		t.Errorf("poison ShardID = %q; want %q", got.ShardID, shardID)
	}
	if got.PartitionKey != wantPartitionKey {
		t.Errorf("poison PartitionKey = %q; want %q", got.PartitionKey, wantPartitionKey)
	}
	if !bytes.Equal(got.Payload, []byte(poison)) {
		t.Errorf("poison Payload = %q; want %q", got.Payload, poison)
	}
	if got.Error != errDLQBoom.Error() {
		t.Errorf("poison Error = %q; want the raw handler error %q", got.Error, errDLQBoom.Error())
	}
	if got.Handler != "record" {
		t.Errorf("poison Handler = %q; want %q", got.Handler, "record")
	}
	if got.Attempts != maxAttempts {
		t.Errorf("poison Attempts = %d; want %d (all retries exhausted)", got.Attempts, maxAttempts)
	}
	if got.BatchSize != 1 {
		t.Errorf("poison BatchSize = %d; want 1 (record handler dlq's a single record)", got.BatchSize)
	}
	if got.RecordIndexInBatch != 0 || got.RecordSequenceInBatchOrder != 0 {
		t.Errorf("poison batch index/order = %d/%d; want 0/0", got.RecordIndexInBatch, got.RecordSequenceInBatchOrder)
	}
	if got.SequenceNumber == "" {
		t.Errorf("poison SequenceNumber is empty; want the Kinesis-assigned sequence")
	}

	// (3) The poison was never delivered to the handler-collector; non-poison exactly once.
	if n := collC1.count(poison); n != 0 {
		t.Fatalf("C1 delivered the poison record %d times; a DLQ'd record must not be delivered", n)
	}
	for _, p := range nonPoison1 {
		if n := collC1.count(p); n != 1 {
			t.Fatalf("C1 delivered non-poison record %q %d times; want exactly 1", p, n)
		}
	}

	// Phase 2: produce later records, then a fresh C2 resumes from the checkpoint.
	batch2 := makePayloads("dlq-b2", total2)
	putRecordsToShard(ctx, t, client, stream, hashKey, batch2)

	collC2 := newCollector()
	consumerC2 := newConsumer(t, stream, client, store, collC2.handler())
	_, stopC2 := runConsumer(t, consumerC2)
	defer stopC2()

	if missing := collC2.waitFor(batch2, 90*time.Second); missing != nil {
		t.Fatalf("resumed C2 did not deliver batch2; missing %d/%d: %v", len(missing), total2, missing)
	}

	// (4) LOAD-BEARING: C2 replayed none of batch1 — the DLQ'd record's page was
	// checkpointed, so the resume starts past it.
	replay := 0
	for _, p := range batch1 {
		if collC2.count(p) > 0 {
			replay++
		}
	}
	t.Logf("C1 delivered %d/%d non-poison (dlq captured %d), C2 delivered batch2 with replay=%d of batch1",
		len(nonPoison1), total1, len(published), replay)
	if replay != 0 {
		t.Fatalf("C2 replayed %d batch1 records; the DLQ'd record's page must have been checkpointed", replay)
	}

	// (5)+(6) Completeness: every non-poison batch1 record and every batch2 record
	// delivered at least once; the poison delivered by nobody (it went to the DLQ).
	all := append(append([]string(nil), nonPoison1...), batch2...)
	for _, p := range all {
		if collC1.count(p)+collC2.count(p) == 0 {
			t.Fatalf("record %q was never delivered by either consumer", p)
		}
	}
	if collC2.count(poison) != 0 {
		t.Fatalf("C2 delivered the poison record; it should have been checkpointed past and never re-read")
	}
}
