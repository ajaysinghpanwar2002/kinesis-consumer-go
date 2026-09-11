package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
)

// defaultShardAnchorVerifyBudget bounds how long one verification may spend proving
// that a required recovery anchor is still readable. Kinesis may need several
// GetRecords calls to reach records, so a single empty response proves nothing
// and the read is repeated; the budget is what turns "still cannot tell" into a
// recovery error instead of an unbounded stall.
const defaultShardAnchorVerifyBudget = 30 * time.Second

// shardAnchorMinReadDelay floors the pause between verification reads. Read
// pacing can be disabled for ordinary consumption, which returns after an empty
// page and sleeps the poll interval; verification instead re-reads empty pages
// immediately, so without a floor it would spin against the shard's read limit
// for the whole budget.
const shardAnchorMinReadDelay = 100 * time.Millisecond

func (c *Consumer) anchorReadDelay() time.Duration {
	if c.tuning.idleTimeBetweenReads > shardAnchorMinReadDelay {
		return c.tuning.idleTimeBetweenReads
	}
	return shardAnchorMinReadDelay
}

func (c *Consumer) shardAnchorVerifyBudget() time.Duration {
	if c.tuning.anchorVerifyBudget > 0 {
		return c.tuning.anchorVerifyBudget
	}
	return defaultShardAnchorVerifyBudget
}

// pendingShardPage is a page that has already been read and still has to be
// handled before the shard reads again. Anchor verification produces one: the
// page that proves the anchor is still readable is the page the shard resumes
// from, so retention cannot drop the anchor between proving the position and
// consuming it.
type pendingShardPage struct {
	output *kinesis.GetRecordsOutput
	readAt time.Time
	took   time.Duration
	// emptied records that the page carried nothing but the anchor, which an
	// exclusive continuation then dropped. Such a page is empty because of what
	// was removed from it, not because the shard has no more records, so the
	// handler must read on from it rather than read it as "caught up".
	emptied bool
}

// dropLeadingRecord turns an inclusive anchor page into the exclusive
// continuation after it. The verified page always starts at the anchor, so the
// remainder is exactly what AFTER_SEQUENCE_NUMBER would have returned — down to
// and including an empty remainder, which is why an emptied page is marked.
func (p *pendingShardPage) dropLeadingRecord() {
	if len(p.output.Records) == 0 {
		return
	}
	remainder := *p.output
	remainder.Records = p.output.Records[1:]
	p.output = &remainder
	p.emptied = len(remainder.Records) == 0
}

// verifyShardAnchorPage proves that the sequence recovery requires is still the
// record the shard yields at that position, and returns the page that proves
// it. It reads inclusively — AT the anchor — so a trimmed anchor shows up as a
// later first record rather than silently becoming the resume point:
// substituting whatever is available now would skip every record between the
// anchor and it.
//
// The proving page is handed back rather than discarded. Deriving a second
// iterator after the check would reopen the window the check closes: retention
// can trim the anchor between the two calls, and the fresh iterator would then
// start at a later record that nothing re-checks.
//
// Transient failures are retried inside the budget. A trimmed or unreadable
// anchor, a missing stream or shard, and an exhausted budget all produce
// checkpoint.ErrRecoveryState, which halts the shard. Cancellation of the
// caller's context is returned as itself: a shutdown is not a recovery failure.
func (c *Consumer) verifyShardAnchorPage(ctx context.Context, shardID, sequence string) (*pendingShardPage, error) {
	page, err := c.readShardAnchorPage(ctx, shardID, sequence)
	if err != nil {
		// Verification errors never pass through a session, so this is the
		// boundary that counts them.
		return nil, c.observeRecoveryFailure(shardID, err)
	}
	return page, nil
}

func (c *Consumer) readShardAnchorPage(ctx context.Context, shardID, sequence string) (*pendingShardPage, error) {
	if sequence == "" {
		return nil, anchorRecoveryError(shardID, sequence, "recovery position has no sequence", nil)
	}

	budgetCtx, cancel := context.WithTimeout(ctx, c.shardAnchorVerifyBudget())
	defer cancel()

	var (
		iterator   string
		failures   int
		backoffRng *rand.Rand
		lastReadAt time.Time
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if budgetCtx.Err() != nil {
			return nil, c.anchorBudgetError(shardID, sequence)
		}

		if iterator == "" {
			derived, err := c.requestShardIterator(budgetCtx, shardID, &kinesis.GetShardIteratorInput{
				ShardIteratorType:      types.ShardIteratorTypeAtSequenceNumber,
				StartingSequenceNumber: aws.String(sequence),
			})
			if err != nil {
				if retryErr := c.retryAnchorVerify(ctx, budgetCtx, shardID, sequence, err, &failures, &backoffRng); retryErr != nil {
					return nil, retryErr
				}
				continue
			}
			if derived == "" {
				return nil, anchorRecoveryError(shardID, sequence, "kinesis returned an empty iterator for the anchor", nil)
			}
			iterator = derived
		}

		// Verification shares the shard's read limit with ordinary
		// consumption, so pace repeat reads.
		if wait := c.anchorReadDelay() - time.Since(lastReadAt); !lastReadAt.IsZero() && wait > 0 {
			if err := c.sleep(budgetCtx, wait); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				return nil, c.anchorBudgetError(shardID, sequence)
			}
		}

		readStart := time.Now()
		out, err := c.getRecords(budgetCtx, iterator)
		lastReadAt = time.Now()
		if err != nil {
			var expiredIterator *types.ExpiredIteratorException
			if errors.As(err, &expiredIterator) && ctx.Err() == nil {
				// Verification holds no position worth keeping, so an expired
				// iterator only costs a fresh derivation.
				iterator = ""
				continue
			}
			if retryErr := c.retryAnchorVerify(ctx, budgetCtx, shardID, sequence, err, &failures, &backoffRng); retryErr != nil {
				return nil, retryErr
			}
			// The iterator survives a failed read, exactly as in the main
			// pass, so the retry resumes from the same position.
			continue
		}
		failures = 0

		if len(out.Records) > 0 {
			first := aws.ToString(out.Records[0].SequenceNumber)
			if first == sequence {
				return &pendingShardPage{output: out, readAt: lastReadAt, took: lastReadAt.Sub(readStart)}, nil
			}
			return nil, anchorRecoveryError(shardID, sequence,
				fmt.Sprintf("anchor is no longer available; the shard now yields %s at that position", first), nil)
		}
		if pageEndsShard(out) {
			// The shard is closed and its records ran out before the anchor
			// appeared, so the anchor is gone for good.
			return nil, anchorRecoveryError(shardID, sequence, "closed shard ended before the anchor was reached", nil)
		}
		// An empty page alone does not prove the anchor is missing: Kinesis can
		// need several reads to reach records. Keep reading until the budget is
		// spent.
		iterator = aws.ToString(out.NextShardIterator)
	}
}

// retryAnchorVerify classifies a failed verification call. It returns nil when
// the caller should retry (after waiting out the backoff) and the error to fail
// with otherwise.
func (c *Consumer) retryAnchorVerify(
	ctx, budgetCtx context.Context,
	shardID, sequence string,
	err error,
	failures *int,
	backoffRng **rand.Rand,
) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return anchorRecoveryError(shardID, sequence, "stream or shard no longer exists", err)
	}
	var invalidArgument *types.InvalidArgumentException
	if errors.As(err, &invalidArgument) {
		return anchorRecoveryError(shardID, sequence, "anchor is not a readable position in this shard", err)
	}
	if !retryableGetRecordsError(err) {
		return anchorRecoveryError(shardID, sequence, "anchor could not be verified", err)
	}
	if budgetCtx.Err() != nil {
		return c.anchorBudgetError(shardID, sequence)
	}

	*failures++
	if *backoffRng == nil {
		*backoffRng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	backoff := getRecordsRetryDelay(*failures, *backoffRng)
	c.logger.Warn("recovery anchor verification failed; backing off",
		slog.String("shard", shardID),
		slog.String("sequence", sequence),
		slog.Int("consecutive_failures", *failures),
		slog.Duration("backoff", backoff),
		slog.Any("error", err),
	)
	if sleepErr := c.sleep(budgetCtx, backoff); sleepErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return c.anchorBudgetError(shardID, sequence)
	}
	return nil
}

func (c *Consumer) anchorBudgetError(shardID, sequence string) error {
	return anchorRecoveryError(shardID, sequence,
		fmt.Sprintf("anchor could not be verified within %v", c.shardAnchorVerifyBudget()), nil)
}

// anchorRecoveryError builds the halt-worthy failure, keeping any causal AWS
// error matchable alongside the recovery-state sentinel.
func anchorRecoveryError(shardID, sequence, reason string, cause error) error {
	if cause != nil {
		return fmt.Errorf("verify shard %s recovery anchor %s: %w: %s: %w",
			shardID, sequence, checkpoint.ErrRecoveryState, reason, cause)
	}
	return fmt.Errorf("verify shard %s recovery anchor %s: %w: %s",
		shardID, sequence, checkpoint.ErrRecoveryState, reason)
}
