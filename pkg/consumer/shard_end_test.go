package consumer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func TestShardRecordsPagesEnded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		pages []*kinesis.GetRecordsOutput
		want  bool
	}{
		{
			name: "empty pages",
			want: false,
		},
		{
			name:  "nil final page",
			pages: []*kinesis.GetRecordsOutput{nil},
			want:  false,
		},
		{
			name: "final page with next iterator",
			pages: []*kinesis.GetRecordsOutput{
				{NextShardIterator: aws.String("iterator-2")},
			},
			want: false,
		},
		{
			name: "final page with nil next iterator",
			pages: []*kinesis.GetRecordsOutput{
				{NextShardIterator: nil},
			},
			want: true,
		},
		{
			name: "final page with empty next iterator",
			pages: []*kinesis.GetRecordsOutput{
				{NextShardIterator: aws.String("")},
			},
			want: true,
		},
		{
			name: "uses final page",
			pages: []*kinesis.GetRecordsOutput{
				{NextShardIterator: aws.String("iterator-2")},
				{NextShardIterator: nil},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := shardRecordsPagesEnded(tt.pages)
			if got != tt.want {
				t.Fatalf("shardRecordsPagesEnded() = %v, want %v", got, tt.want)
			}
		})
	}
}
