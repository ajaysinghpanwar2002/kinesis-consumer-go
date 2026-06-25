package consumer

import (
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// Client is the AWS SDK Kinesis client used by the consumer.
type Client = kinesis.Client

// ClientOptions is the AWS SDK Kinesis client options type.
type ClientOptions = kinesis.Options

// Record is the AWS SDK Kinesis record type delivered to handlers.
type Record = types.Record
