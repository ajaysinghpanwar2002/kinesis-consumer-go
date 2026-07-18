package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (c *Consumer) handleRecordsPage(ctx context.Context, shardID string, out *kinesis.GetRecordsOutput) error {
	if out == nil || len(out.Records) == 0 {
		return nil
	}

	if c.batchHandler != nil {
		if err := c.handleBatchWithRetry(ctx, shardID, out.Records); err != nil {
			return fmt.Errorf("handle records page %s: batch handler: %w", shardID, err)
		}
		return nil
	}

	if c.handler == nil {
		return fmt.Errorf("handle records page %s: record handler is nil", shardID)
	}
	if c.shardConcurrency() > 1 {
		if err := c.handleRecordsConcurrently(ctx, shardID, out.Records); err != nil {
			return fmt.Errorf("handle records page %s: record handler: %w", shardID, err)
		}
		return nil
	}
	for _, record := range out.Records {
		if err := c.handleRecordWithRetry(ctx, shardID, record); err != nil {
			return fmt.Errorf("handle records page %s: record handler: %w", shardID, err)
		}
	}
	return nil
}

func (c *Consumer) shardConcurrency() int {
	if c == nil || c.tuning.shardConcurrency < 1 {
		return 1
	}
	return c.tuning.shardConcurrency
}

func (c *Consumer) handleRecordsConcurrently(ctx context.Context, shardID string, records []Record) error {
	workerLimit := c.shardConcurrency()
	if workerLimit > len(records) {
		workerLimit = len(records)
	}
	if workerLimit <= 1 {
		for _, record := range records {
			if err := c.handleRecordWithRetry(ctx, shardID, record); err != nil {
				return err
			}
		}
		return nil
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		next     int
		once     sync.Once
		firstErr error
		wg       sync.WaitGroup
	)

	nextRecord := func() (Record, bool) {
		if workerCtx.Err() != nil {
			return Record{}, false
		}
		mu.Lock()
		defer mu.Unlock()
		if workerCtx.Err() != nil || next >= len(records) {
			return Record{}, false
		}
		record := records[next]
		next++
		return record, true
	}

	for range workerLimit {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Handler panics are already recovered inside the attempt
			// (callHandlerAttempt), so a panic reaching here comes from the
			// library's own attempt path. These are bare goroutines — without
			// this guard such a panic would kill the whole process instead of
			// failing the page like any other first error.
			defer func() {
				if r := recover(); r != nil {
					once.Do(func() {
						firstErr = c.recoveredPanicError(shardID, handlerKindRecord, r)
						cancel()
					})
				}
			}()
			for {
				record, ok := nextRecord()
				if !ok {
					return
				}
				if err := c.handleRecordWithRetry(workerCtx, shardID, record); err != nil {
					once.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := workerCtx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
