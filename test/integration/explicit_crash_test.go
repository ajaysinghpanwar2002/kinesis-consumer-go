//go:build integration

package integration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// The child acknowledges a suffix behind a gap, then stays alive until SIGKILL.
// No deferred shutdown, checkpoint flush, or lease release can run on that path.
func TestExplicitProcessCrashReplaysAcceptedSuffix(t *testing.T) {
	if stream := os.Getenv("KCG_CRASH_STREAM"); stream != "" {
		store := newStore(t, os.Getenv("KCG_CRASH_PREFIX"))
		defer store.Close()
		seen := 0
		c, err := consumer.New(consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartTrimHorizon}, newKinesisClient(), store, nil,
			consumer.WithExplicitHandler(func(ctx context.Context, d consumer.Delivery) error {
				seen++
				if seen > 1 {
					if err := d.Ack(ctx); err != nil {
						return err
					}
				}
				if seen == 10 {
					return os.WriteFile(os.Getenv("KCG_CRASH_READY"), []byte("accepted suffix behind gap"), 0600)
				}
				return nil
			}),
			consumer.WithBatching(10, 1), consumer.WithCheckpointInterval(10*time.Millisecond),
			consumer.WithHeartbeat(200*time.Millisecond, 3*time.Second),
			consumer.WithPolling(100*time.Millisecond, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Fatal("child exited before termination")
	}
	ctx := context.Background()
	client := newKinesisClient()
	stream, prefix := uniqueName("explicit-kill"), uniqueName("explicit-kill-state")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)
	payloads := makePayloads("kill", 10)
	putRecords(ctx, t, client, stream, payloads)
	shard := listShardIDs(ctx, t, client, stream)[0]
	store := newStore(t, prefix)
	defer store.Close()
	ready := filepath.Join(t.TempDir(), "ready")
	log, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestExplicitProcessCrashReplaysAcceptedSuffix$", "-test.timeout=90s")
	child.Env = append(os.Environ(), "KCG_CRASH_STREAM="+stream, "KCG_CRASH_PREFIX="+prefix, "KCG_CRASH_READY="+ready)
	child.Stdout, child.Stderr = log, log
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	waited := false
	defer func() {
		_ = child.Process.Kill()
		if !waited {
			<-exited
		}
	}()
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	waiting := true
	for waiting {
		select {
		case err := <-exited:
			waited = true
			data, _ := os.ReadFile(log.Name())
			t.Fatalf("child exited before readiness: %v\n%s", err, data)
		case <-deadline.C:
			t.Fatal("child did not acknowledge suffix before deadline")
		case <-tick.C:
			if _, err := os.Stat(ready); err == nil {
				waiting = false
			}
		}
	}
	if got, err := store.Get(ctx, integrationCoordinationIdentity(stream), shard); err != nil || got != "" {
		t.Fatalf("gap must prevent checkpoint: got %q, %v", got, err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	killErr := <-exited
	waited = true
	if killErr == nil {
		t.Fatal("child exited successfully; expected forced termination")
	}
	coll := newCollector()
	successor, err := consumer.New(consumer.Config{StreamName: stream, ConsumerGroup: integrationConsumerGroup, StartPosition: consumer.StartLatest}, client, store, coll.handler(),
		consumer.WithBatching(10, 1), consumer.WithPolling(100*time.Millisecond, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	_, stop := runConsumer(t, successor)
	defer stop()
	if missing := coll.waitFor(payloads, 60*time.Second); len(missing) != 0 {
		t.Fatalf("successor lost gap or accepted suffix: %v", missing)
	}
}
