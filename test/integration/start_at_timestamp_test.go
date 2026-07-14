//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
)

// TestStartAtTimestampSkipsBeforeTimestamp proves scenario #9 (IT-13b): a
// FRESH consumer (no checkpoint) configured with StartPosition: StartAtTimestamp
// skips records before the configured timestamp and delivers records after it.
//
// StartPosition only applies on a first run with no checkpoint — getShardIterator
// reads the checkpoint first and uses AFTER_SEQUENCE_NUMBER when one exists — so
// this uses a fresh store. The timestamp boundary is based on Kinesis-observed
// ApproximateArrivalTimestamp values from the "before" records, not a local-clock
// guess: a raw LocalStack probe showed that a timestamp captured immediately after
// PutRecords can still include those earlier records.
//
// The load-bearing assertion is the NEGATIVE one: the before-boundary records are
// delivered 0 times. The positive (all "after" delivered) is self-checking — if
// AT_TIMESTAMP is ignored or mapped to LATEST, the test times out. Mutation: flip
// StartPosition to StartTrimHorizon with the same fresh store → the before records
// get delivered → the before-count==0 assertion fires.
//
// Deliberately out of scope: exact equality-edge semantics for a record whose
// arrival timestamp equals the boundary, StartAtTimestamp with an existing
// checkpoint, multi-shard timestamp ordering, StreamARN, rebalance, and resharding.
func TestStartAtTimestampSkipsBeforeTimestamp(t *testing.T) {
	ctx := context.Background()

	client := newKinesisClient()
	stream := uniqueName("startattime")
	createStream(ctx, t, client, stream, 1)
	waitStreamActive(ctx, t, client, stream, 60*time.Second)

	shardIDs := listShardIDs(ctx, t, client, stream)
	if len(shardIDs) != 1 {
		t.Fatalf("want exactly 1 shard, got %d: %v", len(shardIDs), shardIDs)
	}
	shardID := shardIDs[0]

	const (
		beforeN       = 30
		afterN        = 20
		deliverBudget = 60 * time.Second
	)

	// Pin every record to the single shard so there is exactly one iterator to
	// reason about and delivery order == production order.
	hashKeys := shardHashKeys(ctx, t, client, stream)
	if len(hashKeys) != 1 {
		t.Fatalf("want exactly 1 shard hash key, got %d", len(hashKeys))
	}
	var hashKey string
	for _, hk := range hashKeys {
		hashKey = hk
	}

	// PRECONDITION for the negative assertion: these records must be before the
	// configured timestamp. We first write them, then read their Kinesis arrival
	// timestamps and choose a boundary safely after the latest one.
	before := makePayloads("sat-before", beforeN)
	putRecordsToShard(ctx, t, client, stream, hashKey, before)
	beforeArrivals := arrivalTimestampsForPayloads(ctx, t, client, stream, shardID, before)
	startAt := timestampAfterLatestArrival(t, before, beforeArrivals)

	// Make sure records produced next have arrival timestamps after startAt even
	// when the boundary is still slightly in the future relative to this process.
	if wait := time.Until(startAt.Add(500 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	after := makePayloads("sat-after", afterN)
	putRecordsToShard(ctx, t, client, stream, hashKey, after)

	// Fresh store: with a checkpoint present, getShardIterator would use
	// AFTER_SEQUENCE_NUMBER and StartPosition would be ignored entirely.
	store := newStore(t, uniqueName("startattime-kp"))

	coll := newCollector()
	cons, err := consumer.New(consumer.Config{
		StreamName:     stream,
		StartPosition:  consumer.StartAtTimestamp,
		StartTimestamp: &startAt,
	}, client, store, coll.handler(),
		consumer.WithBatching(10, 1),
		consumer.WithPolling(200*time.Millisecond, time.Second),
		consumer.WithGracefulDrain(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create StartAtTimestamp consumer: %v", err)
	}
	_, stop := runConsumer(t, cons)
	defer stop()

	// (1) The after-boundary batch must be delivered. This also proves the consumer
	// has streamed past the timestamp boundary before we check for skipped records.
	if missing := coll.waitFor(after, deliverBudget); len(missing) != 0 {
		t.Fatalf("StartAtTimestamp consumer did not deliver the after-boundary batch: missing %d/%d: %v", len(missing), afterN, missing)
	}

	// (2) LOAD-BEARING: none of the before-boundary records was delivered. By now
	// all "after" records arrived, so any before record was skipped, not merely
	// late. Mutation (StartTrimHorizon) would deliver the whole before set here.
	var leaked []string
	for _, p := range before {
		if coll.count(p) != 0 {
			leaked = append(leaked, p)
		}
	}
	if len(leaked) != 0 {
		t.Fatalf("StartAtTimestamp delivered %d/%d before-boundary records (want 0): %v", len(leaked), beforeN, leaked)
	}

	// (3) Completeness of the after-boundary batch: every "after" record delivered
	// at least once.
	for _, p := range after {
		if coll.count(p) == 0 {
			t.Fatalf("after-boundary record %q was never delivered", p)
		}
	}
	t.Logf("StartAtTimestamp ok: startAt=%s skipped all %d before-boundary records and delivered all %d after-boundary records", startAt.Format(time.RFC3339Nano), beforeN, afterN)
}

func arrivalTimestampsForPayloads(ctx context.Context, t *testing.T, client *kinesis.Client, stream, shardID string, payloads []string) map[string]time.Time {
	t.Helper()

	it, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(stream),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("get TRIM_HORIZON iterator for arrival timestamps: %v", err)
	}
	iterator := aws.ToString(it.ShardIterator)
	if iterator == "" {
		t.Fatalf("TRIM_HORIZON iterator for arrival timestamps was empty")
	}

	want := make(map[string]struct{}, len(payloads))
	for _, p := range payloads {
		want[p] = struct{}{}
	}
	seen := make(map[string]time.Time, len(payloads))
	deadline := time.Now().Add(15 * time.Second)
	for iterator != "" && time.Now().Before(deadline) {
		out, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: aws.String(iterator),
			Limit:         aws.Int32(10),
		})
		if err != nil {
			t.Fatalf("get records for arrival timestamps: %v", err)
		}
		for _, rec := range out.Records {
			payload := string(rec.Data)
			if _, ok := want[payload]; !ok {
				continue
			}
			if rec.ApproximateArrivalTimestamp == nil {
				t.Fatalf("record %q has nil ApproximateArrivalTimestamp", payload)
			}
			seen[payload] = *rec.ApproximateArrivalTimestamp
		}
		if len(seen) == len(want) {
			return seen
		}
		iterator = aws.ToString(out.NextShardIterator)
		time.Sleep(200 * time.Millisecond)
	}

	var missing []string
	for _, p := range payloads {
		if _, ok := seen[p]; !ok {
			missing = append(missing, p)
		}
	}
	t.Fatalf("arrival timestamp lookup missed %d/%d records: %v", len(missing), len(payloads), missing)
	return nil
}

func timestampAfterLatestArrival(t *testing.T, payloads []string, arrivals map[string]time.Time) time.Time {
	t.Helper()

	var latest time.Time
	for _, p := range payloads {
		arrival, ok := arrivals[p]
		if !ok {
			t.Fatalf("missing arrival timestamp for %q", p)
		}
		if arrival.After(latest) {
			latest = arrival
		}
	}
	if latest.IsZero() {
		t.Fatalf("no arrival timestamps found")
	}
	return latest.Add(time.Second)
}
