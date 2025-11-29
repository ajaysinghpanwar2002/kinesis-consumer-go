package main

import (
	"context"
	"log"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/redis/go-redis/v9"

	"github.com/pratilipi/kinesis-consumer-go/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/consumer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	kinesisClient := kinesis.NewFromConfig(awsCfg)

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	store := checkpoint.NewRedisStore(redisClient, "kinesis:checkpoint", 30*24*time.Hour)

	handler := func(ctx context.Context, record types.Record) error {
		slog.InfoContext(ctx, "received record",
			slog.String("partition_key", aws.ToString(record.PartitionKey)),
			slog.String("sequence_number", aws.ToString(record.SequenceNumber)))
		return nil
	}

	consumerCfg := consumer.Config{
		StreamName:       "your-stream-name",
		StartPosition:    consumer.StartLatest,
		ShardConcurrency: 2,
		CheckpointEvery:  100,
	}

	// Add consumer.WithBatchHandler(yourBatchHandler) to process full GetRecords batches instead of individual records.
	c, err := consumer.New(consumerCfg, kinesisClient, store, handler)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}

	if err := c.Start(ctx); err != nil {
		log.Printf("consumer stopped: %v", err)
	}
}
