package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

/*
ListShards
   ↓
GetShardIterator
   ↓
GetRecords
   ↓
GetRecords
   ↓
GetRecords
   ...

Input:
type GetRecordsInput struct {
    ShardIterator *string
    Limit         *int32
    StreamARN     *string
}

GetRecordsInput
   ↓
validate required fields
   ↓
serialize to AWS JSON 1.1
   ↓
set operation target:
   X-Amz-Target: Kinesis_20131202.GetRecords
   ↓
resolve endpoint
   ↓
sign request with SigV4
   ↓
send HTTP POST
   ↓
receive JSON response
   ↓
deserialize into GetRecordsOutput

Limit: This is the maximum number of records to return in one call.
Important: this is a maximum, not a guarantee. You can ask for 100 records and receive 0, 10, or 100 depending on what is available at that iterator position.

Output:
type GetRecordsOutput struct {
    Records            []types.Record
    NextShardIterator  *string
    MillisBehindLatest *int64
    ChildShards        []types.ChildShard
    ResultMetadata     middleware.Metadata
}

Each Kinesis record contains:
type Record struct {
    Data                        []byte
    PartitionKey                *string
    SequenceNumber              *string
    ApproximateArrivalTimestamp *time.Time
    EncryptionType              types.EncryptionType
}

*/

func (c *Consumer) getRecords(ctx context.Context, shardIterator string) (*kinesis.GetRecordsOutput, error) {
	input := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(shardIterator),
	}
	// Cap the page size to the configured batch size so WithBatching actually
	// bounds how many records a single GetRecords call returns. Guard against a
	// zero value (Kinesis rejects Limit=0); real consumers always validate
	// batchSize >= 1, but a zero-value tuning must send no Limit at all.
	if c.tuning.batchSize > 0 {
		input.Limit = aws.Int32(c.tuning.batchSize)
	}

	out, err := c.client.GetRecords(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get records %s: %w", shardIterator, err)
	}
	if out == nil {
		return &kinesis.GetRecordsOutput{}, nil
	}
	return out, nil
}
