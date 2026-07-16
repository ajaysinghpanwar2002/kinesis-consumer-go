// Command valkey-example runs a Kinesis consumer that uses the Valkey backend
// for both checkpoints and shard leasing.
//
// The key thing this example demonstrates is that leasing is provided
// automatically: the Valkey checkpoint store implements lease.Provider, so the
// consumer detects it and coordinates shard ownership across workers without any
// explicit WithLeaseManager call. Run more than one copy of this program against
// the same stream and Valkey to watch shards spread across the workers.
//
// It also wires the two observability options the library leads with: a
// log/slog logger via WithLogger (always) and, when -statsd-addr is set, a
// statsd metrics reporter via WithMetrics.
//
// This program needs a real (or LocalStack) Kinesis endpoint and a reachable
// Valkey/Redis server, so it is not part of the automated test suite; it is
// verified only by compilation against the current library API.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	metricstatsd "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics/statsd"
)

func main() {
	var (
		streamName    = flag.String("stream-name", "", "Kinesis stream name")
		streamARN     = flag.String("stream-arn", "", "Kinesis stream ARN (alternative to -stream-name)")
		consumerGroup = flag.String("consumer-group", "", "Logical application group sharing checkpoints and shard ownership")
		region        = flag.String("region", "ap-south-1", "AWS region for the stream")
		endpoint      = flag.String("endpoint", os.Getenv("AWS_ENDPOINT_URL"), "Override Kinesis endpoint (e.g. http://localhost:4566 for LocalStack)")

		valkeyAddr    = flag.String("valkey-addr", "localhost:6379", "Valkey/Redis address (used for checkpoints and leasing)")
		valkeyPrefix  = flag.String("valkey-prefix", "kinesis-checkpoint", "Key prefix for checkpoints (leases use <prefix>-lease by default)")
		valkeyDB      = flag.Int("valkey-db", 0, "Valkey/Redis database (standalone only)")
		valkeyTLS     = flag.Bool("valkey-tls", false, "Enable TLS when connecting to Valkey")
		valkeyCluster = flag.Bool("valkey-cluster", false, "Treat the Valkey endpoint as a cluster")
		valkeyTimeout = flag.Duration("valkey-timeout", 5*time.Second, "Valkey ping/connect timeout")

		batchSize        = flag.Int("batch-size", 200, "GetRecords batch size")
		checkpointEvery  = flag.Int("checkpoint-every", 50, "Checkpoint every N records")
		pollInterval     = flag.Duration("poll-interval", time.Second, "Sleep when no records are returned")
		shardSync        = flag.Duration("shard-sync-interval", 30*time.Second, "How often to refresh the shard list")
		shardConcurrency = flag.Int("shard-concurrency", 1, "Goroutines per shard (values > 1 break per-shard ordering)")
		retryAttempts    = flag.Int("retry-attempts", 3, "Handler retry attempts before applying the failure policy")
		retryBackoff     = flag.Duration("retry-backoff", time.Second, "Backoff between handler retries")
		drainTimeout     = flag.Duration("drain-timeout", 30*time.Second, "Graceful drain timeout on shutdown (0 waits indefinitely)")

		statsdAddr = flag.String("statsd-addr", "", "statsd/Telegraf UDP address for metrics, e.g. localhost:8125 (empty disables metrics)")
	)
	flag.Parse()

	if (*streamName == "") == (*streamARN == "") {
		log.Fatal("exactly one of -stream-name or -stream-arn is required")
	}
	if *consumerGroup == "" {
		log.Fatal("-consumer-group is required")
	}

	// Stop the consumer on Ctrl-C / SIGINT so graceful drain can run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Build the Kinesis client directly from options to keep this example's
	// dependencies minimal. A production program would typically use
	// config.LoadDefaultConfig + kinesis.NewFromConfig to pick up the standard
	// AWS credential chain (env vars, shared config, IAM roles).
	kopts := kinesis.Options{
		Region: *region,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     envOrDefault("AWS_ACCESS_KEY_ID", "test"),
				SecretAccessKey: envOrDefault("AWS_SECRET_ACCESS_KEY", "test"),
				SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
				Source:          "valkey-example-static",
			}, nil
		}),
	}
	if *endpoint != "" {
		kopts.BaseEndpoint = endpoint
	}
	kinesisClient := kinesis.New(kopts)

	// Build the Valkey-backed checkpoint store. Because *Store also implements
	// lease.Provider, passing it as the consumer's store below is all that is
	// needed to turn on shard leasing.
	storeOpts := []valkeycheckpoint.Option{
		valkeycheckpoint.WithKeyPrefix(*valkeyPrefix),
		valkeycheckpoint.WithPingTimeout(*valkeyTimeout),
	}
	if *valkeyDB != 0 {
		storeOpts = append(storeOpts, valkeycheckpoint.WithDB(*valkeyDB))
	}
	if *valkeyTLS {
		storeOpts = append(storeOpts, valkeycheckpoint.WithTLS())
	}
	if *valkeyCluster {
		storeOpts = append(storeOpts, valkeycheckpoint.WithCluster())
	}

	store, err := valkeycheckpoint.New(*valkeyAddr, storeOpts...)
	if err != nil {
		log.Fatalf("connect to valkey for checkpoints/leasing: %v", err)
	}
	defer store.Close()

	cfg := consumer.Config{
		StreamName:    *streamName,
		StreamARN:     *streamARN,
		ConsumerGroup: *consumerGroup,
		StartPosition: consumer.StartLatest,
	}

	handler := func(_ context.Context, rec consumer.Record) error {
		log.Printf("record seq=%s pk=%s bytes=%d",
			aws.ToString(rec.SequenceNumber), aws.ToString(rec.PartitionKey), len(rec.Data))
		return nil
	}

	// Structured library logs go to stderr at Info; without WithLogger the
	// consumer is silent. See docs/logging.md for the event catalog.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Note: no WithLeaseManager here. The Valkey store satisfies lease.Provider,
	// so the consumer auto-detects it and leases shards through the same Valkey
	// connection settings (lease keys use the "<prefix>-lease" namespace).
	opts := []consumer.Option{
		consumer.WithBatching(int32(*batchSize), *checkpointEvery),
		consumer.WithPolling(*pollInterval, *shardSync),
		consumer.WithShardConcurrency(*shardConcurrency),
		consumer.WithRetry(*retryAttempts, *retryBackoff),
		consumer.WithGracefulDrain(*drainTimeout),
		consumer.WithLogger(logger),
	}

	// Metrics are opt-in: only wire a statsd reporter when an address is given,
	// so the example still runs without a statsd/Telegraf endpoint.
	// WithMetrics(nil) is rejected by New. See docs/metrics.md for the catalog.
	if *statsdAddr != "" {
		reporter, err := metricstatsd.New(*statsdAddr)
		if err != nil {
			log.Fatalf("connect to statsd at %s: %v", *statsdAddr, err)
		}
		defer reporter.Close()
		opts = append(opts, consumer.WithMetrics(reporter))
	}

	cons, err := consumer.New(cfg, kinesisClient, store, handler, opts...)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	log.Printf("starting consumer on %s (region %s) with valkey at %s",
		streamKey(*streamName, *streamARN), *region, *valkeyAddr)
	if err := cons.Start(ctx); err != nil {
		log.Fatalf("consumer stopped: %v", err)
	}
	log.Print("consumer stopped cleanly")
}

func streamKey(name, arn string) string {
	if name != "" {
		return name
	}
	return arn
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
