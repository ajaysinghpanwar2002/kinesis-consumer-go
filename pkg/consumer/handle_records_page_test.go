package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestHandleRecordsPageInvokesRecordHandlerInOrder(t *testing.T) {
	t.Parallel()

	var got []string
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			got = append(got, aws.ToString(record.SequenceNumber))
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
			{SequenceNumber: aws.String("sequence-3")},
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if len(got) != 3 {
		t.Fatalf("handled records = %d, want 3", len(got))
	}
	for i, want := range []string{"sequence-1", "sequence-2", "sequence-3"} {
		if got[i] != want {
			t.Fatalf("handled record %d = %q, want %q", i, got[i], want)
		}
	}
}

func TestHandleRecordsPageInvokesBatchHandlerOnce(t *testing.T) {
	t.Parallel()

	var got [][]string
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			t.Fatal("record handler called with batch handler configured")
			return nil
		},
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			var sequences []string
			for _, record := range records {
				sequences = append(sequences, aws.ToString(record.SequenceNumber))
			}
			got = append(got, sequences)
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", out); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("batch handler calls = %d, want 1", len(got))
	}
	if len(got[0]) != 2 {
		t.Fatalf("batch record count = %d, want 2", len(got[0]))
	}
	if got[0][0] != "sequence-1" || got[0][1] != "sequence-2" {
		t.Fatalf("batch records = %v, want [sequence-1 sequence-2]", got[0])
	}
}

func TestHandleRecordsPageTreatsEmptyPageAsNoop(t *testing.T) {
	t.Parallel()

	calls := 0
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			calls++
			return nil
		},
		batchHandler: func(ctx context.Context, records []Record) error {
			calls++
			return nil
		},
	}

	if err := c.handleRecordsPage(context.Background(), "shard-1", &kinesis.GetRecordsOutput{}); err != nil {
		t.Fatalf("handleRecordsPage() error = %v, want nil", err)
	}
	if err := c.handleRecordsPage(context.Background(), "shard-1", nil); err != nil {
		t.Fatalf("handleRecordsPage(nil) error = %v, want nil", err)
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

func TestHandleRecordsPageWrapsRecordHandlerError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	calls := 0
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			calls++
			if aws.ToString(record.SequenceNumber) == "sequence-2" {
				return errBoom
			}
			return nil
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{
			{SequenceNumber: aws.String("sequence-1")},
			{SequenceNumber: aws.String("sequence-2")},
			{SequenceNumber: aws.String("sequence-3")},
		},
	}

	err := c.handleRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "handle records page shard-1: record handler: boom" {
		t.Fatalf("handleRecordsPage() error = %v, want %q", err, "handle records page shard-1: record handler: boom")
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestHandleRecordsPageWrapsBatchHandlerError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := &Consumer{
		batchHandler: func(ctx context.Context, records []Record) error {
			_ = ctx
			_ = records
			return errBoom
		},
	}
	out := &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}

	err := c.handleRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("handleRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "handle records page shard-1: batch handler: boom" {
		t.Fatalf("handleRecordsPage() error = %v, want %q", err, "handle records page shard-1: batch handler: boom")
	}
}
