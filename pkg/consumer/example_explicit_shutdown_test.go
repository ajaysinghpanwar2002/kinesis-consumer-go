package consumer_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// completionWorker is the application-side half of explicit acknowledgment: it
// accumulates deliveries, writes them downstream in batches, and acknowledges
// each batch only once that write is durable.
//
// It flushes on a size threshold and on a timer. Both triggers are required. A
// batch that waits only for a size the shard's admission budget cannot reach
// never fills: nothing is acknowledged, no capacity is released, and the shard
// stalls holding records it can neither complete nor replace.
type completionWorker struct {
	deliveries chan consumer.Delivery
	maxBatch   int
	maxWait    time.Duration
}

// run owns the worker's lifetime. Its context is deliberately NOT the process
// shutdown signal: deliveries admitted before shutdown are still owed an
// acknowledgment, and the consumer's drain is waiting for exactly those.
func (w *completionWorker) run(ctx context.Context) {
	ticker := time.NewTicker(w.maxWait)
	defer ticker.Stop()

	batch := make([]consumer.Delivery, 0, w.maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.write(batch); err != nil {
			// Leave the records unacknowledged: they stay replayable, and the
			// shard's checkpoint cannot advance past them.
			log.Printf("downstream write failed: %v", err)
			batch = batch[:0]
			return
		}
		for _, delivery := range batch {
			// Acknowledge only after the write is durable. Success means the
			// record is accepted, not that a checkpoint has been persisted.
			if err := delivery.Ack(ctx); err != nil {
				log.Printf("acknowledge %s: %v", *delivery.Record.SequenceNumber, err)
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case delivery := <-w.deliveries:
			batch = append(batch, delivery)
			if len(batch) >= w.maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func (w *completionWorker) write([]consumer.Delivery) error { return nil }

// Example_explicitShutdown shows the shutdown order an application must follow
// when it acknowledges deliveries from its own workers.
//
// The ordering constraint is that the consumer's drain and the application's
// completion workers depend on each other: the drain waits for acknowledgments
// the workers still owe it, so the workers have to outlive the shutdown signal
// that starts the drain. Deriving their lifetime from that signal deadlocks the
// drain until its timeout, which then invalidates the outstanding handles and
// replays every record they covered.
func Example_explicitShutdown() {
	// Ctrl-C cancels ctx, which is what asks the consumer to drain.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The completion worker runs on its own context so the shutdown signal
	// does not stop it.
	worker := &completionWorker{
		deliveries: make(chan consumer.Delivery, 1024),
		maxBatch:   500,
		maxWait:    time.Second,
	}
	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.run(workerCtx)
	}()

	// The memory backends keep this example self-contained. They are a fenced
	// pair, which explicit mode requires, but they are for tests and local
	// development: use a durable backend for process-crash recovery.
	//
	// The memory store does not hand the consumer its lease manager, so the
	// pairing is completed by WithLeaseManager below. A store that implements
	// lease.Provider — the Valkey store does — needs no such option.
	manager := lease.NewMemoryManager()
	store := checkpoint.NewMemoryStoreWithLeaseManager(manager)

	cons, err := consumer.New(
		consumer.Config{StreamName: "events", ConsumerGroup: "billing"},
		kinesis.New(kinesis.Options{Region: "ap-south-1"}),
		store,
		nil, // explicit mode replaces the constructor handler
		consumer.WithExplicitHandler(func(ctx context.Context, delivery consumer.Delivery) error {
			// Handing the delivery on and returning does not acknowledge it,
			// and does not cancel the context the worker acknowledges with.
			select {
			case worker.deliveries <- delivery:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		consumer.WithLeaseManager(manager),
		consumer.WithCheckpointInterval(time.Second),
		consumer.WithGracefulDrain(30*time.Second),
	)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	defer cons.Close()

	// Start blocks past the shutdown signal. During the drain it stops fetching
	// but keeps heartbeats, acknowledgment handling, and checkpoints running,
	// waits for every admitted delivery to be acknowledged, and flushes the
	// contiguous acknowledged prefix before releasing its leases.
	if err := cons.Start(ctx); err != nil {
		log.Fatalf("consumer stopped: %v", err)
	}

	// Start has returned, so nothing is owed an acknowledgment any more. Only
	// now stop the application's own workers and close their dependencies.
	stopWorker()
	<-workerDone
}
