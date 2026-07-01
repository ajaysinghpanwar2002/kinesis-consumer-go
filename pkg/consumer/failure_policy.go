package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// FailurePolicy controls behavior when a handler keeps failing after retries.
type FailurePolicy string

const (
	FailurePolicyFailFast  FailurePolicy = "fail-fast"
	FailurePolicySkip      FailurePolicy = "skip"
	FailurePolicySendToDLQ FailurePolicy = "send-to-dlq"
)

// PoisonRecord is sent to a DLQ when a record is considered poison.
type PoisonRecord struct {
	StreamName                 string     `json:"stream_name,omitempty"`
	StreamARN                  string     `json:"stream_arn,omitempty"`
	ShardID                    string     `json:"shard_id"`
	SequenceNumber             string     `json:"sequence_number,omitempty"`
	PartitionKey               string     `json:"partition_key,omitempty"`
	ApproximateArrival         *time.Time `json:"approximate_arrival_timestamp,omitempty"`
	Payload                    []byte     `json:"payload,omitempty"`
	Error                      string     `json:"error"`
	Handler                    string     `json:"handler"`
	Attempts                   int        `json:"attempts"`
	FailedAt                   time.Time  `json:"failed_at"`
	BatchSize                  int        `json:"batch_size"`
	RecordIndexInBatch         int        `json:"record_index_in_batch"`
	RecordSequenceInBatchOrder int        `json:"record_sequence_in_batch_order"`
}

// DLQPublisher publishes poison records to a dead-letter destination.
type DLQPublisher interface {
	Publish(ctx context.Context, record PoisonRecord) error
}

func (p FailurePolicy) validate() error {
	switch p {
	case FailurePolicyFailFast, FailurePolicySkip, FailurePolicySendToDLQ:
		return nil
	case "":
		return errors.New("failure policy cannot be empty")
	default:
		return fmt.Errorf("invalid failure policy %q", p)
	}
}
