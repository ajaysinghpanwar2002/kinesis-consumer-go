package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

/*
ListShards is a AWS SDK operation wrapper around the Kinesis Data Streams ListShards API
It does not compute shard layout locally; it serializes the request, sends it to Kinesis, receives JSON, deserializes it into Go structs, and returns it.

Input: kinesis.ListShardsInput

	type ListShardsInput struct {
	    ExclusiveStartShardId   *string
	    MaxResults              *int32
	    NextToken               *string
	    ShardFilter             *types.ShardFilter
	    StreamARN               *string
	    StreamCreationTimestamp *time.Time
	    StreamId                *string // reserved / not implemented
	    StreamName              *string
	}

We normally provide either StreamARN or StreamName, or both. AWS recommends StreamARN.

Gotchas
NextToken is for pagination. Once we use NextToken, we should not also specify StreamName or StreamCreationTimestamp, because the token already identifies the stream.
Tokens expire after 300 seconds, after which Kinesis returns ExpiredNextTokenException

Output: kinesis.ListShardsOutput

	type ListShardsOutput struct {
		  NextToken      *string
		  Shards         []types.Shard
		  ResultMetadata middleware.Metadata
	}

	Each shard represents
	types.Shard{
	    ShardId:               *string,
	    ParentShardId:         *string,
	    AdjacentParentShardId: *string,
	    HashKeyRange:          *types.HashKeyRange,
	    SequenceNumberRange:   *types.SequenceNumberRange,
	}
*/
func (c *Consumer) listShards(ctx context.Context) ([]types.Shard, error) {
	var shards []types.Shard
	var nextToken *string

	for {
		// A NextToken already identifies the stream; the API rejects requests
		// that combine it with StreamName, so token pages send only the token.
		input := &kinesis.ListShardsInput{}
		if nextToken != nil {
			input.NextToken = nextToken
		} else if c.cfg.StreamARN != "" {
			input.StreamARN = aws.String(c.cfg.StreamARN)
		} else {
			input.StreamName = aws.String(c.cfg.StreamName)
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
