package consumer

import "context"

// HandlerFunc processes one Kinesis record.
type HandlerFunc func(ctx context.Context, record Record) error

// BatchHandlerFunc processes records from one GetRecords response together.
type BatchHandlerFunc func(ctx context.Context, records []Record) error
