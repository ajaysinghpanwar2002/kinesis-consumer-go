package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
)

/*
Input:
type GetShardIteratorInput struct {
    ShardId                *string
    ShardIteratorType      types.ShardIteratorType
    StartingSequenceNumber *string
    StreamARN              *string
    StreamCreationTimestamp *time.Time
    StreamName             *string
    Timestamp              *time.Time
}

GetShardIteratorInput
   ↓
validate required fields
   ↓
serialize to AWS JSON 1.1
   ↓
set operation target:
   X-Amz-Target: Kinesis_20131202.GetShardIterator
   ↓
resolve Kinesis endpoint
   ↓
sign request with AWS SigV4
   ↓
send HTTP POST request
   ↓
deserialize JSON response
   ↓
return GetShardIteratorOutput

Output:
type GetShardIteratorOutput struct {
    ShardIterator *string
    ResultMetadata middleware.Metadata
}
*/

// getShardIterator resolves where a shard worker resumes. A fenced worker
// resolves it from its session's recovery state; an unfenced one keeps reading
// the plain checkpoint value, as it always has.
//
// It returns exactly one of two things: an iterator to read from, or a page
// already read on the shard's behalf that must be handled before the next read
// (see pendingShardPage). Resuming from a recovery anchor takes the second
// form, because the read that proves the anchor is still there is the read the
// shard continues from.
func (c *Consumer) getShardIterator(ctx context.Context, shardID string) (string, *pendingShardPage, error) {
	if session := shardSessionFrom(ctx); session != nil {
		return c.getFencedShardIterator(ctx, shardID, session)
	}
	iterator, err := c.getCheckpointShardIterator(ctx, shardID)
	return iterator, nil, err
}

// getFencedShardIterator resolves the iterator from persisted recovery state.
// Unlike the checkpoint path it can distinguish "no progress yet" from "an
// inclusive replay position exists": a shard whose first observed record was
// recorded but never checkpointed resumes *at* that record rather than after
// it, and a shard whose metadata is missing or inconsistent halts instead of
// silently re-anchoring at the configured start position.
func (c *Consumer) getFencedShardIterator(ctx context.Context, shardID string, session *shardSession) (string, *pendingShardPage, error) {
	position, err := session.recovery(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("get shard iterator recovery %s: %w", shardID, err)
	}

	switch position.Kind {
	case checkpoint.RecoveryCompleted:
		return "", nil, fmt.Errorf("get shard iterator %s: %w", shardID, errShardCompleted)
	case checkpoint.RecoveryInitial:
		// The recorded record was observed but not necessarily processed, so
		// replay includes it.
		return c.anchoredShardRead(ctx, shardID, position.Sequence, true, nil)
	case checkpoint.RecoveryCheckpoint:
		// Everything up to and including the checkpoint is persisted progress.
		return c.anchoredShardRead(ctx, shardID, position.Sequence, false, nil)
	}

	// RecoveryFresh: nothing is persisted for this shard yet. Derive the same
	// start position an unfenced worker would; the pass then persists the first
	// fetched sequence before admitting any of it.
	iterator, err := c.startPositionShardIterator(ctx, shardID)
	return iterator, nil, err
}

// getCheckpointShardIterator is the unfenced derivation: resume after the
// stored checkpoint, or from the configured start position when there is none.
func (c *Consumer) getCheckpointShardIterator(ctx context.Context, shardID string) (string, error) {
	seq, err := c.readShardCheckpoint(ctx, shardID)
	if err != nil {
		return "", fmt.Errorf("get shard iterator checkpoint %s: %w", shardID, err)
	}
	if isShardCompletedCheckpoint(seq) {
		return "", fmt.Errorf("get shard iterator %s: %w", shardID, errShardCompleted)
	}
	if seq == "" {
		return c.startPositionShardIterator(ctx, shardID)
	}
	return c.requestShardIterator(ctx, shardID, &kinesis.GetShardIteratorInput{
		ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
		StartingSequenceNumber: aws.String(seq),
	})
}

// anchoredShardRead resumes a shard from a recovery anchor. The inclusive read
// that proves the anchor is still readable IS the read the shard continues
// from: an initial replay keeps the anchor, an exclusive checkpoint
// continuation drops it, and the records after it in the same page are
// delivered either way.
//
// Deriving a fresh iterator here instead would put a second call between the
// proof and the delivery. Retention can trim the anchor inside that window, and
// the new iterator would then start at a later record — the very substitution
// verification exists to prevent — with nothing left to detect it.
//
// The page is returned unhandled and with no iterator, so the pass advances
// past it only once its records are processed; an interrupted page is derived
// again from unchanged recovery state.
func (c *Consumer) anchoredShardRead(ctx context.Context, shardID, sequence string, inclusive bool, beforeWait func() error) (string, *pendingShardPage, error) {
	page, err := c.verifyShardAnchorPage(ctx, shardID, sequence, beforeWait)
	if err != nil {
		return "", nil, err
	}
	if !inclusive {
		page.dropLeadingRecord()
	}
	return "", page, nil
}

// startPositionShardIterator derives the iterator for a shard with no persisted
// progress at all.
func (c *Consumer) startPositionShardIterator(ctx context.Context, shardID string) (string, error) {
	input := &kinesis.GetShardIteratorInput{}
	switch {
	case c.parentage.hasKnownParents(shardID):
		// A checkpoint-less shard with a known parent is a reshard child being
		// picked up for the first time. Parent gating (readyShardIDs) already
		// guarantees every parent reached SHARD_END before this worker starts,
		// so TRIM_HORIZON continues exactly where the parents left off.
		// cfg.StartPosition (default StartLatest) would instead anchor at the
		// child's tip at worker-start time and silently drop every record
		// written between the reshard and pickup. StartPosition applies only
		// to parentless shards with no checkpoint.
		input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	case c.cfg.StartPosition == StartTrimHorizon:
		input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	case c.cfg.StartPosition == StartAtTimestamp:
		input.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		input.Timestamp = c.cfg.StartTimestamp
	default:
		input.ShardIteratorType = types.ShardIteratorTypeLatest
	}
	return c.requestShardIterator(ctx, shardID, input)
}

// requestShardIterator fills in the shard and stream identity shared by every
// derivation and performs the call.
func (c *Consumer) requestShardIterator(ctx context.Context, shardID string, input *kinesis.GetShardIteratorInput) (string, error) {
	input.ShardId = aws.String(shardID)
	if c.cfg.StreamARN != "" {
		input.StreamARN = aws.String(c.cfg.StreamARN)
	} else {
		input.StreamName = aws.String(c.cfg.StreamName)
	}

	out, err := c.client.GetShardIterator(ctx, input)
	if err != nil {
		return "", fmt.Errorf("get shard iterator %s: %w", shardID, err)
	}
	if out == nil {
		return "", fmt.Errorf("get shard iterator %s: %w", shardID, errNilKinesisOutput)
	}
	return aws.ToString(out.ShardIterator), nil
}
