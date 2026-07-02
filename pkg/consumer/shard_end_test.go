package consumer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func TestPageEndsShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		page *kinesis.GetRecordsOutput
		want bool
	}{
		{
			name: "nil page",
			page: nil,
			want: false,
		},
		{
			name: "page with next iterator",
			page: &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iterator-2")},
			want: false,
		},
		{
			name: "page with nil next iterator",
			page: &kinesis.GetRecordsOutput{NextShardIterator: nil},
			want: true,
		},
		{
			name: "page with empty next iterator",
			page: &kinesis.GetRecordsOutput{NextShardIterator: aws.String("")},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := pageEndsShard(tt.page); got != tt.want {
				t.Fatalf("pageEndsShard() = %v, want %v", got, tt.want)
			}
		})
	}
}
