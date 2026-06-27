package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestProcessRecordsPageReturnsLastSequenceAndCount(t *testing.T) {
	t.Parallel()

	var handled []string
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
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

	lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
	if err != nil {
		t.Fatalf("processRecordsPage() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	if len(handled) != 3 {
		t.Fatalf("handled records = %d, want 3", len(handled))
	}
}

func TestProcessRecordsPageTreatsNilAndEmptyPagesAsZeroProgress(t *testing.T) {
	t.Parallel()

	calls := 0
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			calls++
			return nil
		},
	}

	for _, out := range []*kinesis.GetRecordsOutput{nil, {}} {
		lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
		if err != nil {
			t.Fatalf("processRecordsPage() error = %v, want nil", err)
		}
		if lastSeq != "" {
			t.Fatalf("lastSeq = %q, want empty", lastSeq)
		}
		if count != 0 {
			t.Fatalf("count = %d, want 0", count)
		}
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

func TestProcessRecordsPageReturnsNoProgressWhenHandlerFails(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	c := &Consumer{
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
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
		},
	}

	lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("processRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process records page shard-1: handle records page shard-1: record handler: boom" {
		t.Fatalf("processRecordsPage() error = %v, want %q", err, "process records page shard-1: handle records page shard-1: record handler: boom")
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestProcessRecordsPageWrapsBatchHandlerError(t *testing.T) {
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

	lastSeq, count, err := c.processRecordsPage(context.Background(), "shard-1", out)
	if !errors.Is(err, errBoom) {
		t.Fatalf("processRecordsPage() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process records page shard-1: handle records page shard-1: batch handler: boom" {
		t.Fatalf("processRecordsPage() error = %v, want %q", err, "process records page shard-1: handle records page shard-1: batch handler: boom")
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}
