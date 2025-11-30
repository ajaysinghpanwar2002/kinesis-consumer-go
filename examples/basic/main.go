package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/redis/go-redis/v9"

	"github.com/pratilipi/kinesis-consumer-go/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/consumer"
	"github.com/pratilipi/kinesis-consumer-go/lease"
)

const (
	defaultStreamName = "bench-stream"
	defaultRegion     = "us-east-1"
	pprofAddr         = ":6060"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	streamName := env("STREAM_NAME", defaultStreamName)
	region := env("AWS_REGION", defaultRegion)
	endpoint := os.Getenv("AWS_ENDPOINT")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	shardConcurrency := envInt("SHARD_CONCURRENCY", 4)
	checkpointEvery := envInt("CHECKPOINT_EVERY", 1000)
	batchSize := envInt("BATCH_SIZE", 1000)
	startPosition := parseStartPosition(env("START_POSITION", string(consumer.StartLatest)))
	startTimestamp := parseStartTimestamp(os.Getenv("START_TIMESTAMP_RFC3339"))

	if shardConcurrency < 1 {
		slog.Warn("shard concurrency must be >=1; using 1 instead", slog.Int("provided", shardConcurrency))
		shardConcurrency = 1
	}
	if checkpointEvery < 1 {
		slog.Warn("checkpoint every must be >=1; using 1 instead", slog.Int("provided", checkpointEvery))
		checkpointEvery = 1
	}
	if batchSize < 1 {
		slog.Warn("batch size must be >=1; using 1 instead", slog.Int("provided", batchSize))
		batchSize = 1
	}
	if startPosition == consumer.StartAtTimestamp && startTimestamp == nil {
		slog.Warn("START_POSITION=AT_TIMESTAMP requires START_TIMESTAMP_RFC3339; falling back to LATEST")
		startPosition = consumer.StartLatest
	}

	slog.Info("benchmark consumer config",
		slog.String("stream", streamName),
		slog.String("region", region),
		slog.String("endpoint", endpoint),
		slog.String("redis", redisAddr),
		slog.Int("shard_concurrency", shardConcurrency),
		slog.Int("checkpoint_every", checkpointEvery),
		slog.Int("batch_size", batchSize),
		slog.String("start_position", string(startPosition)))

	servePprof()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithEndpointResolverWithOptions(resolveEndpoint(endpoint)))
	if err != nil {
		slog.Error("load aws config", slog.Any("err", err))
		return
	}
	kinesisClient := kinesis.NewFromConfig(awsCfg)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	store := checkpoint.NewRedisStore(redisClient, "kinesis:checkpoint", 30*24*time.Hour)
	leaseMgr := lease.NewRedisManager(redisClient, "kinesis:lease")

	var recordCount uint64
	var byteCount uint64

	handler := func(ctx context.Context, record types.Record) error {
		atomic.AddUint64(&recordCount, 1)
		atomic.AddUint64(&byteCount, uint64(len(record.Data)))
		return nil
	}

	go logConsumerMetrics(ctx, &recordCount, &byteCount)

	cfg := consumer.Config{
		StreamName:       streamName,
		Region:           region,
		StartPosition:    startPosition,
		StartTimestamp:   startTimestamp,
		ShardConcurrency: shardConcurrency,
		BatchSize:        int32(batchSize),
		CheckpointEvery:  checkpointEvery,
	}

	cons, err := consumer.New(cfg, kinesisClient, store, handler,
		consumer.WithLeaseManager(leaseMgr, "", 0, 0, 0))
	if err != nil {
		slog.Error("create consumer", slog.Any("err", err))
		return
	}

	if err := cons.Start(ctx); err != nil {
		slog.Error("consumer stopped", slog.Any("err", err))
	}
}

func logConsumerMetrics(ctx context.Context, records *uint64, bytes *uint64) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastRecords uint64
	var lastBytes uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curRecords := atomic.LoadUint64(records)
			curBytes := atomic.LoadUint64(bytes)

			deltaRecords := curRecords - lastRecords
			deltaBytes := curBytes - lastBytes

			lastRecords = curRecords
			lastBytes = curBytes

			rps := float64(deltaRecords) / 5.0
			mbps := float64(deltaBytes) / (1024.0 * 1024.0) / 5.0

			slog.Info("consumer throughput",
				slog.Float64("records_per_sec", rps),
				slog.Float64("mb_per_sec", mbps),
				slog.Uint64("total_records", curRecords))
		}
	}
}

func servePprof() {
	go func() {
		slog.Info("pprof server listening", slog.String("addr", pprofAddr))
		if err := http.ListenAndServe(pprofAddr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("pprof server error", slog.Any("err", err))
		}
	}()
}

func parseStartPosition(raw string) consumer.StartPosition {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case string(consumer.StartTrimHorizon):
		return consumer.StartTrimHorizon
	case string(consumer.StartAtTimestamp):
		return consumer.StartAtTimestamp
	default:
		return consumer.StartLatest
	}
}

func parseStartTimestamp(raw string) *time.Time {
	if raw == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		slog.Warn("invalid START_TIMESTAMP_RFC3339; ignoring", slog.String("value", raw), slog.Any("err", err))
		return nil
	}
	return &t
}

func resolveEndpoint(endpoint string) aws.EndpointResolverWithOptionsFunc {
	if endpoint == "" {
		return aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
	}

	return aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               endpoint,
			HostnameImmutable: true,
			PartitionID:       "aws",
		}, nil
	})
}

func env(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func envInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		slog.Warn("invalid int env; using default", slog.String("key", key), slog.String("value", val), slog.Int("default", def))
		return def
	}
	return parsed
}
