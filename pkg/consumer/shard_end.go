package consumer

import "github.com/aws/aws-sdk-go-v2/service/kinesis"

func shardRecordsPagesEnded(pages []*kinesis.GetRecordsOutput) bool {
	if len(pages) == 0 {
		return false
	}

	last := pages[len(pages)-1]
	if last == nil {
		return false
	}
	return last.NextShardIterator == nil || *last.NextShardIterator == ""
}
