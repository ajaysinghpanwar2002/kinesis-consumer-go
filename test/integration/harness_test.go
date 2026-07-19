//go:build integration

// Shared helpers for the integration suite: infrastructure clients, Kinesis
// stream lifecycle, a record producer, and a dedup record collector.
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
)

// uniqueSeq disambiguates names within a single test process run.
var uniqueSeq atomic.Int64

const integrationConsumerGroup = "integration-tests"

func integrationCoordinationIdentity(stream string) string {
	return coordinationIdentity(integrationConsumerGroup, stream)
}

func coordinationIdentity(group, stream string) string {
	return group + ":" + stream
}

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

// shardHashKeys returns, for each shard, a hash key that lands in that shard's
// hash-key range (the range's starting key), so a producer can pin a record to a
// specific shard via ExplicitHashKey. This lets a test know payload->shard even
// though the handler is not told which shard a record came from.
func shardHashKeys(ctx context.Context, t *testing.T, client *kinesis.Client, name string) map[string]string {
	t.Helper()
	out, err := client.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String(name)})
	if err != nil {
		t.Fatalf("list shards for %s: %v", name, err)
	}
	keys := make(map[string]string, len(out.Shards))
	for _, s := range out.Shards {
		if s.HashKeyRange == nil {
			continue
		}
		keys[aws.ToString(s.ShardId)] = aws.ToString(s.HashKeyRange.StartingHashKey)
	}
	return keys
}

// putRecordsToShard writes payloads pinned to one shard via ExplicitHashKey.
func putRecordsToShard(ctx context.Context, t *testing.T, client *kinesis.Client, name, hashKey string, payloads []string) {
	t.Helper()
	entries := make([]types.PutRecordsRequestEntry, len(payloads))
	for i, p := range payloads {
		entries[i] = types.PutRecordsRequestEntry{
			Data:            []byte(p),
			PartitionKey:    aws.String(fmt.Sprintf("pk-%d", i)),
			ExplicitHashKey: aws.String(hashKey),
		}
	}
	out, err := client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(name),
		Records:    entries,
	})
	if err != nil {
		t.Fatalf("put records to shard on %s: %v", name, err)
	}
	if out.FailedRecordCount != nil && *out.FailedRecordCount > 0 {
		t.Fatalf("put records to shard on %s: %d of %d failed", name, *out.FailedRecordCount, len(payloads))
	}
}

// putRecordsAcrossShards distributes payloads across all shards deterministically
// (round-robin by shard ID) using ExplicitHashKey, so every shard is guaranteed to
// receive records regardless of partition-key hashing.
func putRecordsAcrossShards(ctx context.Context, t *testing.T, client *kinesis.Client, name string, hashKeys map[string]string, payloads []string) {
	t.Helper()
	shardIDs := make([]string, 0, len(hashKeys))
	for id := range hashKeys {
		shardIDs = append(shardIDs, id)
	}
	sort.Strings(shardIDs)
	if len(shardIDs) == 0 {
		t.Fatalf("no shards to distribute records across on %s", name)
	}

	perShard := make(map[string][]string, len(shardIDs))
	for i, p := range payloads {
		id := shardIDs[i%len(shardIDs)]
		perShard[id] = append(perShard[id], p)
	}
	for _, id := range shardIDs {
		if ps := perShard[id]; len(ps) > 0 {
			putRecordsToShard(ctx, t, client, name, hashKeys[id], ps)
		}
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

// attributingCollector records which consumer processed each payload, so tests
// can prove that a specific consumer did real work rather than only that "some
// consumer" delivered each payload. The handler signature exposes no shard ID
// (Record = types.Record) and there is no shard ID in context, so attribution is
// per-consumer only; per-shard attribution is a separate concern.
type attributingCollector struct {
	mu   sync.Mutex
	seen map[string]map[string]int // consumerID -> payload -> observation count
}

func newAttributingCollector() *attributingCollector {
	return &attributingCollector{seen: make(map[string]map[string]int)}
}

// handlerFor returns a HandlerFunc that attributes every processed record to the
// given consumerID.
func (c *attributingCollector) handlerFor(consumerID string) consumer.HandlerFunc {
	return func(_ context.Context, rec consumer.Record) error {
		c.mu.Lock()
		byPayload := c.seen[consumerID]
		if byPayload == nil {
			byPayload = make(map[string]int)
			c.seen[consumerID] = byPayload
		}
		byPayload[string(rec.Data)]++
		c.mu.Unlock()
		return nil
	}
}

// processedBy returns how many distinct payloads from the set consumerID has
// processed at least once.
func (c *attributingCollector) processedBy(consumerID string, payloads []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	byPayload := c.seen[consumerID]
	n := 0
	for _, p := range payloads {
		if byPayload[p] > 0 {
			n++
		}
	}
	return n
}

// waitForProcessedBy blocks until consumerID has processed at least minCount
// distinct payloads from the set, returning the count actually reached (which is
// < minCount only on timeout).
func (c *attributingCollector) waitForProcessedBy(consumerID string, payloads []string, minCount int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		n := c.processedBy(consumerID, payloads)
		if n >= minCount {
			return n
		}
		if time.Now().After(deadline) {
			return n
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// consumersFor returns the distinct consumer IDs that processed payload at least
// once, sorted for deterministic diagnostics.
func (c *attributingCollector) consumersFor(payload string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for id, byPayload := range c.seen {
		if byPayload[payload] > 0 {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// missingAcrossAll returns payloads not yet observed by any consumer.
func (c *attributingCollector) missingAcrossAll(payloads []string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, p := range payloads {
		total := 0
		for _, byPayload := range c.seen {
			total += byPayload[p]
		}
		if total == 0 {
			out = append(out, p)
		}
	}
	return out
}

// waitForAll blocks until every payload has been observed by some consumer at
// least once or the timeout elapses, returning the still-missing payloads.
func (c *attributingCollector) waitForAll(payloads []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		missing := c.missingAcrossAll(payloads)
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
		ConsumerGroup: integrationConsumerGroup,
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

// newNoDrainConsumer builds a consumer with a configurable batch size and
// checkpoint frequency and NO graceful drain, so that cancelling it is an
// ungraceful ("crash") stop: the residual processed-but-not-yet-checkpointed
// records are not flushed. The batch size caps the GetRecords page size (so a
// checkpointEvery larger than it spans multiple pages), and a short heartbeat TTL
// is a backstop in case the lease release on the abrupt stop is delayed.
func newNoDrainConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	batchSize int32,
	checkpointEvery int,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(batchSize, checkpointEvery),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithHeartbeat(200*time.Millisecond, 3*time.Second),
	)
	if err != nil {
		t.Fatalf("create no-drain consumer for %s: %v", stream, err)
	}
	return cons
}

// newDrainConsumer mirrors newNoDrainConsumer but ENABLES graceful drain, so
// cancelling it is a graceful shutdown: in-flight progress is checkpointed before
// the worker exits. Batch size and checkpoint frequency are configurable so a test
// can, for example, set a checkpointEvery larger than the whole stream to disable
// the periodic due-save path and isolate the drain flush.
func newDrainConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	batchSize int32,
	checkpointEvery int,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(batchSize, checkpointEvery),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithHeartbeat(200*time.Millisecond, 3*time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create drain consumer for %s: %v", stream, err)
	}
	return cons
}

// newRebalancingConsumer builds a consumer tuned for the multi-consumer
// integration test. The rebalance interval is configurable so tests can keep
// ownership movement bounded without changing production defaults.
func newRebalancingConsumer(
	t *testing.T,
	stream string,
	client *kinesis.Client,
	store *valkeycheckpoint.Store,
	handler consumer.HandlerFunc,
	rebalanceInterval time.Duration,
) *consumer.Consumer {
	t.Helper()
	cfg := consumer.Config{
		StreamName:    stream,
		ConsumerGroup: integrationConsumerGroup,
		StartPosition: consumer.StartTrimHorizon,
	}
	cons, err := consumer.New(cfg, client, store, handler,
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithRetry(3, 100*time.Millisecond),
		consumer.WithRebalance(rebalanceInterval, 0, 500*time.Millisecond, 2),
		consumer.WithHeartbeat(100*time.Millisecond, 2*time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create rebalancing consumer for %s: %v", stream, err)
	}
	return cons
}

func newLeaseManager(t *testing.T, store *valkeycheckpoint.Store) lease.Manager {
	t.Helper()
	manager, err := store.LeaseManager()
	if err != nil {
		t.Fatalf("create lease manager: %v", err)
	}
	if closer, ok := manager.(io.Closer); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}
	return manager
}

func waitForSingleLeaseOwner(t *testing.T, manager lease.Manager, stream string, shardCount int, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]string
	var lastErr error
	for {
		owners, err := manager.List(context.Background(), integrationCoordinationIdentity(stream))
		if err == nil {
			last = owners
			counts := leaseOwnerCounts(owners)
			if len(owners) == shardCount && len(counts) == 1 {
				for owner := range counts {
					return owner
				}
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not converge to one owner for %d shards within %s; owners=%v lastErr=%v", stream, shardCount, timeout, last, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForDistributedLeaseOwners(t *testing.T, manager lease.Manager, stream string, shardCount, minOwners int, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]string
	var lastWorkers []string
	var lastErr error
	for {
		owners, err := manager.List(context.Background(), integrationCoordinationIdentity(stream))
		if err == nil {
			last = owners
			counts := leaseOwnerCounts(owners)
			if len(owners) == shardCount && len(counts) >= minOwners {
				return owners
			}
		} else {
			lastErr = err
		}
		if workers, err := manager.Workers(context.Background(), integrationCoordinationIdentity(stream)); err == nil {
			lastWorkers = workers
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not distribute %d shards across at least %d owners within %s; owners=%v workers=%v lastErr=%v", stream, shardCount, minOwners, timeout, last, lastWorkers, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForLeaseWorkers(t *testing.T, manager lease.Manager, stream string, minWorkers int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	var lastErr error
	for {
		workers, err := manager.Workers(context.Background(), integrationCoordinationIdentity(stream))
		if err == nil {
			last = workers
			if len(workers) >= minWorkers {
				return workers
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not report at least %d active workers within %s; workers=%v lastErr=%v", stream, minWorkers, timeout, last, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForWorkerCount waits until the live-worker set has exactly want members,
// returning that set. Unlike waitForLeaseWorkers (which waits for AT LEAST a
// count), this asserts an exact size, so it can prove a departed worker left the
// set within a window far shorter than its heartbeat TTL — the observable effect
// of clean-shutdown deregistration.
func waitForWorkerCount(t *testing.T, manager lease.Manager, stream string, want int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	var lastErr error
	for {
		workers, err := manager.Workers(context.Background(), integrationCoordinationIdentity(stream))
		if err == nil {
			last = workers
			if len(workers) == want {
				return workers
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s did not reach exactly %d active workers within %s; workers=%v lastErr=%v", stream, want, timeout, last, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForStableLeaseOwners waits until the shard->owner map covers all shards
// across at least minOwners owners AND has stayed identical for stableFor,
// returning that stable map. Used before measuring steady-state behavior so a
// concurrent rebalance move does not race the measurement.
func waitForStableLeaseOwners(t *testing.T, manager lease.Manager, stream string, shardCount, minOwners int, stableFor, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var prev map[string]string
	var stableSince time.Time
	for {
		owners, err := manager.List(context.Background(), integrationCoordinationIdentity(stream))
		valid := err == nil && len(owners) == shardCount && len(leaseOwnerCounts(owners)) >= minOwners
		if valid {
			if prev != nil && sameOwnerMap(prev, owners) {
				if time.Since(stableSince) >= stableFor {
					return owners
				}
			} else {
				prev = owners
				stableSince = time.Now()
			}
		} else {
			prev = nil
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s ownership did not stabilize across >=%d owners for %s within %s; last=%v", stream, minOwners, stableFor, timeout, prev)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sameOwnerMap reports whether two shard->owner maps are identical.
func sameOwnerMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func leaseOwnerCounts(owners map[string]string) map[string]int {
	counts := make(map[string]int)
	for _, owner := range owners {
		if owner == "" {
			continue
		}
		counts[owner]++
	}
	return counts
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
