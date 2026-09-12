//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// TestExplicitAcknowledgmentDrainFlushesAndResumes proves the explicit
// acknowledgment shutdown contract against real Kinesis and Valkey: a consumer
// stopped mid-stream waits for the acknowledgments its application still owes,
// flushes the contiguous acknowledged prefix while it still owns the shard, and
// a fresh consumer resumes strictly after that prefix.
//
// The setup isolates the drain flush as the only possible checkpoint writer:
//
//   - The checkpoint interval is an hour and checkpointEvery is larger than the
//     whole stream, so neither the timed nor the counted trigger can fire.
//   - C1 is stopped mid-stream with a backlog remaining, so the catch-up flush
//     at the shard tip never fires either.
//
// The acknowledgments themselves come from a completion worker that flushes on
// its own timer and outlives the shutdown signal, which is the shutdown order
// the documentation requires of an explicit-mode application.
func TestExplicitAcknowledgmentDrainFlushesAndResumes(t *testing.T) {
	ctx := context.Background()
	client := newKinesisClient()

	stream := uniqueName("kcg-it-explicit")
	keyPrefix := uniqueName("kcg-it-explicit-ckpt")
	const (
		total     = 120
		batchSize = 10
		// Larger than the stream, so the counted trigger stays disabled.
		checkpointEvery = 1_000_000
		stopAtLeast     = 30
		// Bounds how far ahead of the completion worker delivery can run, so a
		// backlog is still on the stream when C1 is stopped.
		admittedPerShard = 20
	)

	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 90*time.Second)

	store := newStore(t, keyPrefix)
	t.Cleanup(func() { _ = store.Close() })

	var hashKey string
	for _, hk := range shardHashKeys(ctx, t, client, stream) {
		hashKey = hk
	}
	payloads := makePayloads("explicit", total)
	ordinalOf := make(map[string]int, total)
	for i, p := range payloads {
		ordinalOf[p] = i
	}
	putRecordsToShard(ctx, t, client, stream, hashKey, payloads)

	// The completion worker owns its own lifetime. Stopping it with the
	// shutdown signal would strand the drain waiting for acknowledgments that
	// could no longer happen.
	pending := make(chan consumer.Delivery, total)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	workerDone := make(chan struct{})
	var ackErr error
	var ackMu sync.Mutex
	go func() {
		defer close(workerDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var batch []consumer.Delivery
		flush := func() {
			for _, delivery := range batch {
				if err := delivery.Ack(context.Background()); err != nil {
					ackMu.Lock()
					if ackErr == nil {
						ackErr = err
					}
					ackMu.Unlock()
				}
			}
			batch = nil
		}
		for {
			select {
			case delivery := <-pending:
				batch = append(batch, delivery)
			case <-ticker.C:
				flush()
			case <-workerCtx.Done():
				flush()
				return
			}
		}
	}()

	collC1 := newCollector()
	record := collC1.handler()
	cons, err := consumer.New(
		consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon},
		client, store, nil,
		consumer.WithExplicitHandler(func(hctx context.Context, delivery consumer.Delivery) error {
			if err := record(hctx, delivery.Record); err != nil {
				return err
			}
			select {
			case pending <- delivery:
				return nil
			case <-hctx.Done():
				return hctx.Err()
			}
		}),
		consumer.WithCheckpointInterval(time.Hour),
		consumer.WithInFlightLimits(consumer.InFlightLimits{MaxRecordsPerShard: admittedPerShard}),
		consumer.WithBatching(batchSize, checkpointEvery),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(30*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, stopC1 := runConsumer(t, cons)

	if missing := collC1.waitFor(payloads[:stopAtLeast], 90*time.Second); missing != nil {
		stopC1()
		t.Fatalf("C1 did not reach its stop threshold; missing %d/%d", len(missing), stopAtLeast)
	}
	health := cons.Health()
	if len(health.Shards) != 1 || !health.Checkpoint.LastSuccess.IsZero() {
		t.Fatalf("before drain: expected one owned shard and no checkpoint writes: %+v", health)
	}
	for _, shard := range health.Shards {
		if shard.PersistedRecords != 0 || shard.PersistedSequence != "" || shard.Pressure.UnacknowledgedRecords > admittedPerShard {
			t.Fatalf("pre-drain progress/pressure: %+v", shard)
		}
	}
	// Stop the consumer first, then the application's own worker. Start returns
	// only once every admitted delivery has been acknowledged and flushed.
	stopC1()
	health = cons.Health()
	if len(health.Shards) != 0 || health.Pressure.UnacknowledgedRecords != 0 || health.Pressure.FetchSlots != 0 || health.Checkpoint.LastSuccess.IsZero() || health.Checkpoint.LastProgress.IsZero() {
		t.Fatalf("drain did not persist progress and clear worker pressure: %+v", health)
	}
	stopWorker()
	<-workerDone
	ackMu.Lock()
	firstAckErr := ackErr
	ackMu.Unlock()
	if firstAckErr != nil {
		t.Fatalf("acknowledging a delivery admitted before shutdown failed: %v", firstAckErr)
	}

	c1Ordinals := deliveredOrdinals(collC1, payloads, ordinalOf)
	frontier := len(c1Ordinals)
	if frontier == 0 {
		t.Fatal("C1 delivered nothing")
	}
	if !equalOrdinals(c1Ordinals, rangeOrdinals(0, frontier)) {
		t.Fatalf("C1 delivered a non-contiguous prefix %v; want exactly 0..%d", summarizeOrdinals(c1Ordinals), frontier-1)
	}
	if frontier >= total {
		t.Fatalf("C1 frontier=%d reached the tip (%d): the stop was not mid-stream, so the drain flush is not isolated from the catch-up flush", frontier, total)
	}

	// A fresh consumer resumes from the only checkpoint that can exist: the one
	// the drain flushed. Automatic mode is enough to read it back.
	collC2 := newCollector()
	successor := newConsumer(t, stream, client, store, collC2.handler())
	_, stopC2 := runConsumer(t, successor)
	defer stopC2()

	if missing := collC2.waitFor(payloads[frontier:], 90*time.Second); missing != nil {
		t.Fatalf("resumed C2 did not deliver the remainder; missing %d/%d: %v", len(missing), total-frontier, missing)
	}
	c2Ordinals := deliveredOrdinals(collC2, payloads, ordinalOf)
	replay := 0
	for _, o := range c2Ordinals {
		if o < frontier {
			replay++
		}
	}
	t.Logf("C1 frontier=%d, C2 delivered %d ordinals, replay=%d", frontier, len(c2Ordinals), replay)
	if replay != 0 {
		t.Fatalf("C2 replayed %d records at/below the acknowledged frontier %d; the drain did not flush the acknowledged prefix", replay, frontier)
	}
	if !equalOrdinals(c2Ordinals, rangeOrdinals(frontier, total)) {
		t.Fatalf("C2 delivered %v; want exactly %d..%d", summarizeOrdinals(c2Ordinals), frontier, total-1)
	}
}
