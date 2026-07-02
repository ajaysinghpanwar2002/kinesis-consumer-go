package consumer

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// pageEndsShard reports whether a GetRecords page marks the end of a shard: the
// shard is closed once Kinesis returns a nil/empty NextShardIterator. A nil page
// is not treated as a shard end (it is handled as a caught-up/no-op page).
func pageEndsShard(out *kinesis.GetRecordsOutput) bool {
	if out == nil {
		return false
	}
	return aws.ToString(out.NextShardIterator) == ""
}
