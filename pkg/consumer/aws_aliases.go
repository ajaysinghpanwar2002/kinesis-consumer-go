package consumer

import (
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// The aliases below deliberately expose AWS SDK for Go v2 types as this
// library's public API. Each is a compatibility contract, not an accident:
// there is no wrapper layer to convert types or fall behind the SDK, and the
// trade-off — stated on each alias below — is that this library's public
// surface tracks the SDK's, so an incompatible change to an aliased SDK type
// is an incompatible change here too.

// Client is the AWS SDK Kinesis client used by the consumer, exposed as a
// deliberate public compatibility contract: it satisfies the KinesisAPI
// interface that New accepts, and callers build it with the SDK directly
// (kinesis.New / kinesis.NewFromConfig) rather than through a wrapper. The
// library's API therefore tracks *kinesis.Client.
type Client = kinesis.Client

// ClientOptions is the AWS SDK Kinesis client options type, exposed as a
// deliberate public compatibility contract so callers configure the client
// with the SDK's own options type. The library's API therefore tracks
// kinesis.Options.
type ClientOptions = kinesis.Options

// Record is the AWS SDK Kinesis record type delivered to handlers, exposed as
// a deliberate public compatibility contract: handlers receive the SDK's own
// record type with no conversion. The library's API therefore tracks
// types.Record.
type Record = types.Record
