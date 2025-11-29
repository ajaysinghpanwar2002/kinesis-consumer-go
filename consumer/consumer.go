package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/pratilipi/kinesis-consumer-go/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/lease"
)

type HandlerFunc func(ctx context.Context, record types.Record) error
type BatchHandlerFunc func(ctx context.Context, records []types.Record) error

type Consumer struct {
	cfg          Config
	client       *kinesis.Client
	store        checkpoint.Store
	handler      HandlerFunc
	batchHandler BatchHandlerFunc
	leaseManager lease.Manager
	leaseOwner   string
	leaseTTL     time.Duration
	leaseRenew   time.Duration
	leaseRetry   time.Duration
	logger       *slog.Logger
	wg           sync.WaitGroup
}

func New(cfg Config, client *kinesis.Client, store checkpoint.Store, handler HandlerFunc, opts ...Option) (*Consumer, error) {
	if client == nil {
		return nil, errors.New("kinesis client is required")
	}
	if store == nil {
		return nil, errors.New("checkpoint store is required")
	}

	opt, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}
	if handler == nil && opt.batchHandler == nil {
		return nil, errors.New("handler is required (provide WithBatchHandler for batch processing)")
	}
	if opt.leaseManager != nil && opt.leaseOwner == "" {
		opt.leaseOwner = defaultOwner()
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Consumer{
		cfg:          cfg,
		client:       client,
		store:        store,
		handler:      handler,
		batchHandler: opt.batchHandler,
		leaseManager: opt.leaseManager,
		leaseOwner:   opt.leaseOwner,
		leaseTTL:     opt.leaseTTL,
		leaseRenew:   opt.leaseRenewInterval,
		leaseRetry:   opt.leaseRetryInterval,
		logger:       logger,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards found for stream %s", c.streamKey())
	}

	errCh := make(chan error, len(shards))

	for _, shardID := range shards {
		shardID := shardID
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			var err error
			if c.leaseManager != nil {
				err = c.consumeShardWithLease(ctx, shardID)
			} else {
				err = c.consumeShard(ctx, shardID)
			}
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		<-done
		return err
	case <-ctx.Done():
		<-done
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return ctx.Err()
	}
}

func (c *Consumer) listShards(ctx context.Context) ([]string, error) {
	var shardIDs []string
	var nextToken *string

	for {
		input := &kinesis.ListShardsInput{}
		if c.cfg.StreamARN != "" {
			input.StreamARN = aws.String(c.cfg.StreamARN)
		} else {
			input.StreamName = aws.String(c.cfg.StreamName)
		}
		if nextToken != nil {
			input.NextToken = nextToken
		}

		out, err := c.client.ListShards(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list shards: %w", err)
		}
		for _, shard := range out.Shards {
			if shard.ShardId != nil {
				shardIDs = append(shardIDs, *shard.ShardId)
			}
		}
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	return shardIDs, nil
}

func (c *Consumer) consumeShardWithLease(ctx context.Context, shardID string) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		l, acquired, err := c.leaseManager.Acquire(ctx, c.streamKey(), shardID, c.leaseOwner, c.leaseTTL)
		if err != nil {
			return fmt.Errorf("shard %s lease acquire: %w", shardID, err)
		}
		if !acquired {
			if err := sleepWithContext(ctx, c.leaseRetry); err != nil {
				return nil
			}
			continue
		}

		c.logger.Info("acquired shard lease", slog.String("shard", shardID), slog.String("owner", c.leaseOwner))
		err = c.consumeWithLeaseRenewal(ctx, shardID, l)
		if err != nil {
			if errors.Is(err, lease.ErrNotOwned) {
				c.logger.Warn("lost shard lease", slog.String("shard", shardID), slog.String("owner", c.leaseOwner), slog.String("stream", c.streamKey()))
				continue
			}
			return err
		}
		return nil
	}
}

func (c *Consumer) consumeWithLeaseRenewal(ctx context.Context, shardID string, l lease.Lease) error {
	shardCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	renewErrCh := make(chan error, 1)
	go c.renewLeaseLoop(shardCtx, shardID, l, renewErrCh, cancel)

	err := c.consumeShard(shardCtx, shardID)

	var renewErr error
	select {
	case renewErr = <-renewErrCh:
	default:
	}

	if releaseErr := l.Release(context.Background()); releaseErr != nil && !errors.Is(releaseErr, lease.ErrNotOwned) {
		if err == nil {
			err = fmt.Errorf("shard %s release lease: %w", shardID, releaseErr)
		} else {
			err = fmt.Errorf("%w; shard %s release lease: %v", err, shardID, releaseErr)
		}
	}

	if err == nil && renewErr != nil {
		err = fmt.Errorf("shard %s lease renewal: %w", shardID, renewErr)
	} else if renewErr != nil {
		err = fmt.Errorf("%w; shard %s lease renewal: %v", err, shardID, renewErr)
	}
	return err
}

func (c *Consumer) renewLeaseLoop(ctx context.Context, shardID string, l lease.Lease, errCh chan<- error, cancel context.CancelFunc) {
	ticker := time.NewTicker(c.leaseRenew)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.Renew(ctx, c.leaseTTL); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
}

func (c *Consumer) consumeShard(ctx context.Context, shardID string) error {
	iterator, err := c.initialIterator(ctx, shardID)
	if err != nil {
		return err
	}
	if iterator == nil {
		return fmt.Errorf("shard iterator is nil for shard %s", shardID)
	}

	c.logger.Info("starting shard", slog.String("shard", shardID))

	var lastSeq string
	processedSinceCheckpoint := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		out, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
			Limit:         aws.Int32(c.cfg.BatchSize),
		})
		if err != nil {
			return fmt.Errorf("shard %s get records: %w", shardID, err)
		}

		if len(out.Records) == 0 {
			iterator = out.NextShardIterator
			if iterator == nil {
				return c.flushCheckpoint(ctx, shardID, lastSeq)
			}
			if err := sleepWithContext(ctx, c.cfg.PollInterval); err != nil {
				return nil
			}
			continue
		}

		if c.batchHandler != nil {
			if err := c.handleBatchWithRetry(ctx, out.Records); err != nil {
				return fmt.Errorf("shard %s handler: %w", shardID, err)
			}
		} else {
			if err := c.handleRecords(ctx, out.Records); err != nil {
				return fmt.Errorf("shard %s handler: %w", shardID, err)
			}
		}

		lastSeq = aws.ToString(out.Records[len(out.Records)-1].SequenceNumber)
		processedSinceCheckpoint += len(out.Records)

		if processedSinceCheckpoint >= c.cfg.CheckpointEvery {
			if err := c.store.Save(ctx, c.streamKey(), shardID, lastSeq); err != nil {
				return fmt.Errorf("shard %s checkpoint: %w", shardID, err)
			}
			processedSinceCheckpoint = processedSinceCheckpoint % c.cfg.CheckpointEvery
		}

		iterator = out.NextShardIterator
		if iterator == nil {
			return c.flushCheckpoint(ctx, shardID, lastSeq)
		}
	}
}

func (c *Consumer) initialIterator(ctx context.Context, shardID string) (*string, error) {
	seq, err := c.store.Get(ctx, c.streamKey(), shardID)
	if err != nil {
		return nil, fmt.Errorf("shard %s get checkpoint: %w", shardID, err)
	}

	input := &kinesis.GetShardIteratorInput{
		ShardId: aws.String(shardID),
	}
	if c.cfg.StreamARN != "" {
		input.StreamARN = aws.String(c.cfg.StreamARN)
	} else {
		input.StreamName = aws.String(c.cfg.StreamName)
	}

	if seq != "" {
		c.logger.Info("resuming from checkpoint", slog.String("shard", shardID), slog.String("sequence_number", seq))
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = aws.String(seq)
	} else {
		switch c.cfg.StartPosition {
		case StartTrimHorizon:
			input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
		case StartAtTimestamp:
			input.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
			input.Timestamp = c.cfg.StartTimestamp
		default:
			input.ShardIteratorType = types.ShardIteratorTypeLatest
		}
	}

	out, err := c.client.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get shard iterator: %w", err)
	}

	return out.ShardIterator, nil
}

func (c *Consumer) handleRecords(ctx context.Context, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}
	if c.cfg.ShardConcurrency <= 1 {
		for _, record := range records {
			if err := c.handleWithRetry(ctx, record); err != nil {
				return err
			}
		}
		return nil
	}

	return c.handleRecordsConcurrently(ctx, records)
}

func (c *Consumer) handleRecordsConcurrently(ctx context.Context, records []types.Record) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerLimit := c.cfg.ShardConcurrency
	if workerLimit > len(records) {
		workerLimit = len(records)
	}

	sem := make(chan struct{}, workerLimit)
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once

	for _, record := range records {
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(rec types.Record) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := c.handleWithRetry(ctx, rec); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}(record)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (c *Consumer) handleBatchWithRetry(ctx context.Context, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	var lastErr error
	for attempt := 1; attempt <= c.cfg.Retry.MaxAttempts; attempt++ {
		if err := c.batchHandler(ctx, records); err != nil {
			lastErr = err
			if attempt == c.cfg.Retry.MaxAttempts {
				break
			}
			backoff := time.Duration(attempt) * c.cfg.Retry.Backoff
			if err := sleepWithContext(ctx, backoff); err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("batch handler failed after %d attempts: %w", c.cfg.Retry.MaxAttempts, lastErr)
}

func (c *Consumer) handleWithRetry(ctx context.Context, record types.Record) error {
	var lastErr error
	for attempt := 1; attempt <= c.cfg.Retry.MaxAttempts; attempt++ {
		if err := c.handler(ctx, record); err != nil {
			lastErr = err
			if attempt == c.cfg.Retry.MaxAttempts {
				break
			}
			backoff := time.Duration(attempt) * c.cfg.Retry.Backoff
			if err := sleepWithContext(ctx, backoff); err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("handler failed after %d attempts: %w", c.cfg.Retry.MaxAttempts, lastErr)
}

func (c *Consumer) flushCheckpoint(ctx context.Context, shardID, seq string) error {
	if seq == "" {
		return nil
	}
	if err := c.store.Save(ctx, c.streamKey(), shardID, seq); err != nil {
		return fmt.Errorf("shard %s final checkpoint: %w", shardID, err)
	}
	return nil
}

func (c *Consumer) streamKey() string {
	if c.cfg.StreamName != "" {
		return c.cfg.StreamName
	}
	return c.cfg.StreamARN
}

func defaultOwner() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
