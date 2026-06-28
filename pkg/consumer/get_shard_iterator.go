package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
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

func (c *Consumer) getShardIterator(ctx context.Context, shardID string) (string, error) {
	seq, err := c.readShardCheckpoint(ctx, shardID)
	if err != nil {
		return "", fmt.Errorf("get shard iterator checkpoint %s: %w", shardID, err)
	}
	if isShardCompletedCheckpoint(seq) {
		return "", fmt.Errorf("get shard iterator %s: %w", shardID, errShardCompleted)
	}

	input := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeLatest,
	}
	if seq != "" {
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
		return "", nil
	}
	return aws.ToString(out.ShardIterator), nil
}
