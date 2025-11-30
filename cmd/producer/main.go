package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const (
	defaultStreamName = "bench-stream"
	defaultRegion     = "us-east-1"
	maxPutBatchSize   = 500
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	streamName := env("STREAM_NAME", defaultStreamName)
	region := env("AWS_REGION", defaultRegion)
	endpoint := os.Getenv("AWS_ENDPOINT")
	shardCount := envInt("SHARD_COUNT", 4)
	batchSize := envInt("BATCH_SIZE", maxPutBatchSize)
	if batchSize > maxPutBatchSize {
		slog.Warn("batch size capped at Kinesis max", slog.Int("requested", batchSize), slog.Int("effective", maxPutBatchSize))
		batchSize = maxPutBatchSize
	}
	workerCount := envInt("WORKERS", 8)
	payloadBytes := envInt("PAYLOAD_BYTES", 1024)

	slog.Info("producer config",
		slog.String("stream", streamName),
		slog.String("region", region),
		slog.String("endpoint", endpoint),
		slog.Int("shards", shardCount),
		slog.Int("batch_size", batchSize),
		slog.Int("workers", workerCount),
		slog.Int("payload_bytes", payloadBytes))

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithEndpointResolverWithOptions(resolveEndpoint(endpoint)))
	if err != nil {
		slog.Error("load aws config", slog.Any("err", err))
		return
	}
	kinesisClient := kinesis.NewFromConfig(awsCfg)

	if err := ensureStream(ctx, kinesisClient, streamName, int32(shardCount)); err != nil {
		slog.Error("ensure stream", slog.Any("err", err))
		return
	}

	payload := make([]byte, payloadBytes)
	var pkCounter uint64
	nextPK := func() string {
		val := atomic.AddUint64(&pkCounter, 1)
		return strconv.FormatUint(val, 10)
	}

	var sentRecords uint64
	var sentBytes uint64

	go logProducerMetrics(ctx, &sentRecords, &sentBytes)

	entryTemplate := make([]types.PutRecordsRequestEntry, batchSize)
	for i := range entryTemplate {
		entryTemplate[i] = types.PutRecordsRequestEntry{
			Data: payload,
		}
	}

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			for ctx.Err() == nil {
				entries := make([]types.PutRecordsRequestEntry, batchSize)
				copy(entries, entryTemplate)
				for j := range entries {
					entries[j].PartitionKey = aws.String(nextPK())
				}

				resp, err := kinesisClient.PutRecords(ctx, &kinesis.PutRecordsInput{
					StreamName: aws.String(streamName),
					Records:    entries,
				})
				if err != nil {
					slog.Warn("put records failed", slog.Int("worker", workerID), slog.Any("err", err))
					time.Sleep(200 * time.Millisecond)
					continue
				}

				var succeeded int
				for _, rec := range resp.Records {
					if rec.ErrorCode == nil {
						succeeded++
					}
				}

				if failed := len(entries) - succeeded; failed > 0 {
					slog.Debug("put records partial success",
						slog.Int("worker", workerID),
						slog.Int("succeeded", succeeded),
						slog.Int("failed", failed))
				}

				atomic.AddUint64(&sentRecords, uint64(succeeded))
				atomic.AddUint64(&sentBytes, uint64(succeeded*payloadBytes))
			}
		}(i)
	}

	<-ctx.Done()
	slog.Info("producer stopping", slog.Any("reason", ctx.Err()))
}

func ensureStream(ctx context.Context, cli *kinesis.Client, name string, shardCount int32) error {
	_, err := cli.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(shardCount),
	})
	if err != nil {
		var alreadyExists *types.ResourceInUseException
		if !errors.As(err, &alreadyExists) {
			return err
		}
	}

	waiter := kinesis.NewStreamExistsWaiter(cli)
	if err := waiter.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(name)}, 5*time.Minute); err != nil {
		return err
	}

	return nil
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

func logProducerMetrics(ctx context.Context, records *uint64, bytes *uint64) {
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

			slog.Info("producer throughput",
				slog.Float64("records_per_sec", rps),
				slog.Float64("mb_per_sec", mbps),
				slog.Uint64("total_records", curRecords))
		}
	}
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
