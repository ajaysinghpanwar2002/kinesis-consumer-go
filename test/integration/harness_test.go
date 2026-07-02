//go:build integration

// Shared helpers for the integration suite: infrastructure clients, Kinesis
// stream lifecycle, a record producer, and a dedup record collector.
package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	valkeycheckpoint "github.com/pratilipi/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
)

// uniqueSeq disambiguates names within a single test process run.
var uniqueSeq atomic.Int64

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// isCanceled reports whether err is (or wraps) a context cancellation/deadline,
// which is the expected result of stopping a consumer.
func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func kinesisEndpoint() string { return envOr("KINESIS_ENDPOINT", "http://localhost:4566") }
func valkeyAddr() string      { return envOr("VALKEY_ADDR", "localhost:6379") }
func awsRegion() string       { return envOr("AWS_REGION", "us-east-1") }

// uniqueName builds a per-run identifier so reruns and parallel consumers do not
// collide on stream names or Valkey key prefixes.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), uniqueSeq.Add(1))
}

// newKinesisClient builds a Kinesis client pointed at LocalStack.
func newKinesisClient() *kinesis.Client {
	endpoint := kinesisEndpoint()
	return kinesis.New(kinesis.Options{
		Region:       awsRegion(),
		BaseEndpoint: aws.String(endpoint),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test", Source: "integration"}, nil
		}),
	})
}

// newStore builds a Valkey-backed checkpoint store (which also auto-provides the
// lease manager) under a per-run key prefix.
func newStore(t *testing.T, keyPrefix string) *valkeycheckpoint.Store {
	t.Helper()
	store, err := valkeycheckpoint.New(valkeyAddr(), valkeycheckpoint.WithKeyPrefix(keyPrefix))
	if err != nil {
		t.Fatalf("connect valkey at %s: %v", valkeyAddr(), err)
	}
	return store
}

// createStream creates a Kinesis stream and registers deletion cleanup. It
// retries briefly because the Kinesis service may still be warming up right after
// the container reports healthy.
func createStream(ctx context.Context, t *testing.T, client *kinesis.Client, name string, shards int32) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(name),
			ShardCount: aws.Int32(shards),
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("create stream %s: %v", name, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{
			StreamName:              aws.String(name),
			EnforceConsumerDeletion: aws.Bool(true),
		})
	})
}

// waitStreamActive blocks until the stream reports ACTIVE (LocalStack creation is
// asynchronous), failing the test on timeout.
func waitStreamActive(ctx context.Context, t *testing.T, client *kinesis.Client, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(name),
		})
		if err == nil && out.StreamDescriptionSummary != nil &&
			out.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s not ACTIVE within %s (last err: %v)", name, timeout, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// listShardIDs returns the current shard IDs for a stream.
func listShardIDs(ctx context.Context, t *testing.T, client *kinesis.Client, name string) []string {
	t.Helper()
	out, err := client.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String(name)})
	if err != nil {
		t.Fatalf("list shards for %s: %v", name, err)
	}
	ids := make([]string, 0, len(out.Shards))
	for _, s := range out.Shards {
		ids = append(ids, aws.ToString(s.ShardId))
	}
	return ids
}

// putRecords writes payloads to the stream, spreading them across shards via
// distinct partition keys, and fails if any record is rejected.
func putRecords(ctx context.Context, t *testing.T, client *kinesis.Client, name string, payloads []string) {
	t.Helper()
	entries := make([]types.PutRecordsRequestEntry, len(payloads))
	for i, p := range payloads {
		entries[i] = types.PutRecordsRequestEntry{
			Data:         []byte(p),
			PartitionKey: aws.String(fmt.Sprintf("pk-%d", i)),
		}
	}
	out, err := client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(name),
		Records:    entries,
	})
	if err != nil {
		t.Fatalf("put records to %s: %v", name, err)
	}
	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		t.Fatalf("put records to %s: %d of %d failed", name, *out.FailedRecordCount, len(payloads))
	}
}

// makePayloads builds n distinct payloads with the given tag.
func makePayloads(tag string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", tag, i)
	}
	return out
}

// collector records every payload a handler observes, keyed by payload string,
// counting occurrences so callers can assert at-least-once delivery.
type collector struct {
	mu   sync.Mutex
	seen map[string]int
}

func newCollector() *collector { return &collector{seen: make(map[string]int)} }

// handler returns a consumer.HandlerFunc that records each record's data.
func (c *collector) handler() consumer.HandlerFunc {
	return func(_ context.Context, rec consumer.Record) error {
		c.mu.Lock()
		c.seen[string(rec.Data)]++
		c.mu.Unlock()
		return nil
	}
}

// count returns how many times a payload was observed.
func (c *collector) count(payload string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[payload]
}

// missing returns the payloads not yet observed at least once.
func (c *collector) missing(payloads []string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, p := range payloads {
		if c.seen[p] == 0 {
			out = append(out, p)
		}
	}
	return out
}

// waitFor blocks until every payload has been observed at least once or the
// timeout elapses, returning the still-missing payloads on timeout.
func (c *collector) waitFor(payloads []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		missing := c.missing(payloads)
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return missing
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// newConsumer builds a consumer for the stream using the Valkey store (leasing is
// auto-provided). Batching/polling are tuned short for fast, deterministic tests,
// and graceful drain lets a canceled consumer checkpoint in-flight progress.
func newConsumer(t *testing.T, stream string, client *kinesis.Client, store *valkeycheckpoint.Store, handler consumer.HandlerFunc) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create consumer for %s: %v", stream, err)
	}
	return cons
}

// runConsumer starts the consumer in a goroutine and returns a cancel func plus a
// stop func that cancels and waits for Start to return (failing on a non-nil,
// non-cancellation error). Always call stop (e.g. via defer/t.Cleanup).
func runConsumer(t *testing.T, cons *consumer.Consumer) (cancel context.CancelFunc, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- cons.Start(ctx) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errCh:
				if err != nil && !isCanceled(err) {
					t.Errorf("consumer Start returned error: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Errorf("consumer Start did not return within 30s of cancel")
			}
		})
	}
	return cancel, stop
}
