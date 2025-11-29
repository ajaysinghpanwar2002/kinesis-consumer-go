package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
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

const shardCompletedPrefix = "SHARD_END"

var errShardCompleted = errors.New("shard already completed")

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

	tracker := newShardTracker()
	knownShards := make(map[string]types.Shard)
	errCh := make(chan error, 1)

	startShard := func(shardID string) {
		if !tracker.markActive(shardID) {
			return
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			var err error
			if c.leaseManager != nil {
				err = c.consumeShardWithLease(ctx, shardID, tracker)
			} else {
				err = c.consumeShard(ctx, shardID, tracker)
			}
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
			tracker.markInactive(shardID)
		}()
	}

	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards found for stream %s", c.streamKey())
	}

	c.addShards(knownShards, shards)
	if err := c.startReadyShards(ctx, knownShards, tracker, startShard); err != nil {
		return err
	}

	ticker := time.NewTicker(c.cfg.ShardSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-errCh:
			cancel()
			c.wg.Wait()
			return err
		default:
		}

		select {
		case <-ctx.Done():
			cancel()
			c.wg.Wait()
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case err := <-errCh:
			cancel()
			c.wg.Wait()
			return err
		case <-ticker.C:
			if ctx.Err() != nil {
				continue
			}

			newShards, err := c.listShards(ctx)
			if err != nil {
				c.logger.Error("list shards", slog.String("stream", c.streamKey()), slog.String("error", err.Error()))
				continue
			}
			c.addShards(knownShards, newShards)
			if err := c.startReadyShards(ctx, knownShards, tracker, startShard); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}
	}
}

func (c *Consumer) listShards(ctx context.Context) ([]types.Shard, error) {
	var shards []types.Shard
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
				shards = append(shards, shard)
			}
		}
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	return shards, nil
}

func (c *Consumer) consumeShardWithLease(ctx context.Context, shardID string, tracker *shardTracker) error {
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
		err = c.consumeWithLeaseRenewal(ctx, shardID, l, tracker)
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

func (c *Consumer) consumeWithLeaseRenewal(ctx context.Context, shardID string, l lease.Lease, tracker *shardTracker) error {
	shardCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	renewErrCh := make(chan error, 1)
	go c.renewLeaseLoop(shardCtx, shardID, l, renewErrCh, cancel)

	err := c.consumeShard(shardCtx, shardID, tracker)

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

func (c *Consumer) consumeShard(ctx context.Context, shardID string, tracker *shardTracker) error {
	iterator, err := c.initialIterator(ctx, shardID, tracker)
	if err != nil {
		if errors.Is(err, errShardCompleted) {
			return nil
		}
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
				return c.completeShard(ctx, shardID, lastSeq, tracker)
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
			return c.completeShard(ctx, shardID, lastSeq, tracker)
		}
	}
}

func (c *Consumer) initialIterator(ctx context.Context, shardID string, tracker *shardTracker) (*string, error) {
	seq, err := c.store.Get(ctx, c.streamKey(), shardID)
	if err != nil {
		return nil, fmt.Errorf("shard %s get checkpoint: %w", shardID, err)
	}
	if isShardCompletedCheckpoint(seq) {
		tracker.markCompleted(shardID)
		return nil, errShardCompleted
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

func (c *Consumer) completeShard(ctx context.Context, shardID, seq string, tracker *shardTracker) error {
	checkpoint := shardCompletionValue(seq)
	if err := c.store.Save(ctx, c.streamKey(), shardID, checkpoint); err != nil {
		return fmt.Errorf("shard %s final checkpoint: %w", shardID, err)
	}
	tracker.markCompleted(shardID)
	return nil
}

func (c *Consumer) shardCompleted(ctx context.Context, shardID string, tracker *shardTracker) (bool, error) {
	if tracker.isCompleted(shardID) {
		return true, nil
	}

	seq, err := c.store.Get(ctx, c.streamKey(), shardID)
	if err != nil {
		return false, fmt.Errorf("shard %s get checkpoint: %w", shardID, err)
	}
	if isShardCompletedCheckpoint(seq) {
		tracker.markCompleted(shardID)
		return true, nil
	}
	return false, nil
}

func (c *Consumer) startReadyShards(ctx context.Context, shardMap map[string]types.Shard, tracker *shardTracker, startShard func(string)) error {
	if ctx.Err() != nil {
		return nil
	}

	for shardID, shard := range shardMap {
		if shardID == "" || tracker.isActive(shardID) || tracker.isCompleted(shardID) {
			continue
		}

		completed, err := c.shardCompleted(ctx, shardID, tracker)
		if err != nil {
			return err
		}
		if completed {
			continue
		}

		ready, err := c.parentsReady(ctx, shard, shardMap, tracker)
		if err != nil {
			return err
		}
		if ready {
			startShard(shardID)
		}
	}
	return nil
}

func (c *Consumer) parentsReady(ctx context.Context, shard types.Shard, shardMap map[string]types.Shard, tracker *shardTracker) (bool, error) {
	parents := shardParents(shard)
	if len(parents) == 0 {
		return true, nil
	}

	for _, parentID := range parents {
		if parentID == "" {
			continue
		}
		if tracker.isCompleted(parentID) {
			continue
		}

		done, err := c.shardCompleted(ctx, parentID, tracker)
		if err != nil {
			return false, err
		}
		if !done {
			if _, exists := shardMap[parentID]; !exists {
				// Parent shard is not returned by ListShards (trimmed/expired),
				// so we cannot wait on its completion marker.
				continue
			}
			return false, nil
		}
	}
	return true, nil
}

func (c *Consumer) addShards(dst map[string]types.Shard, shards []types.Shard) {
	for _, shard := range shards {
		if shard.ShardId == nil {
			continue
		}
		dst[aws.ToString(shard.ShardId)] = shard
	}
}

func shardCompletionValue(seq string) string {
	if seq == "" {
		return shardCompletedPrefix
	}
	return shardCompletedPrefix + ":" + seq
}

func isShardCompletedCheckpoint(seq string) bool {
	return strings.HasPrefix(seq, shardCompletedPrefix)
}

func shardParents(shard types.Shard) []string {
	parents := make([]string, 0, 2)
	if shard.ParentShardId != nil && *shard.ParentShardId != "" {
		parents = append(parents, *shard.ParentShardId)
	}
	if shard.AdjacentParentShardId != nil && *shard.AdjacentParentShardId != "" {
		parents = append(parents, *shard.AdjacentParentShardId)
	}
	return parents
}

func (c *Consumer) streamKey() string {
	if c.cfg.StreamName != "" {
		return c.cfg.StreamName
	}
	return c.cfg.StreamARN
}

type shardTracker struct {
	mu        sync.Mutex
	active    map[string]struct{}
	completed map[string]struct{}
}

func newShardTracker() *shardTracker {
	return &shardTracker{
		active:    make(map[string]struct{}),
		completed: make(map[string]struct{}),
	}
}

func (t *shardTracker) markActive(shardID string) bool {
	if shardID == "" {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, done := t.completed[shardID]; done {
		return false
	}
	if _, running := t.active[shardID]; running {
		return false
	}
	t.active[shardID] = struct{}{}
	return true
}

func (t *shardTracker) markInactive(shardID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.active, shardID)
}

func (t *shardTracker) markCompleted(shardID string) {
	if shardID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.active, shardID)
	t.completed[shardID] = struct{}{}
}

func (t *shardTracker) isActive(shardID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.active[shardID]
	return ok
}

func (t *shardTracker) isCompleted(shardID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.completed[shardID]
	return ok
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
