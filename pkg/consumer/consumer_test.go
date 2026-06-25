package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/checkpoint"
)

type fakeCheckpointStore struct{}

func (fakeCheckpointStore) Get(context.Context, string, string) (string, error) {
	return "", nil
}

func (fakeCheckpointStore) Save(context.Context, string, string, string) error {
	return nil
}

func (fakeCheckpointStore) Delete(context.Context, string, string) error {
	return nil
}

func TestNewValidation(t *testing.T) {
	t.Parallel()

	client := &Client{}
	store := fakeCheckpointStore{}
	handler := func(context.Context, Record) error { return nil }

	tests := []struct {
		name    string
		cfg     Config
		client  *Client
		store   checkpoint.Store
		handler HandlerFunc
		opts    []Option
		want    string
	}{
		{
			name:    "nil client",
			cfg:     Config{StreamName: "stream"},
			client:  nil,
			store:   store,
			handler: handler,
			want:    "kinesis client is required",
		},
		{
			name:    "nil checkpoint store",
			cfg:     Config{StreamName: "stream"},
			client:  client,
			handler: handler,
			want:    "checkpoint store is required",
		},
		{
			name:   "missing handler",
			cfg:    Config{StreamName: "stream"},
			client: client,
			store:  store,
			want:   "handler is required (provide WithBatchHandler for batch processing)",
		},
		{
			name:    "invalid option",
			cfg:     Config{StreamName: "stream"},
			client:  client,
			store:   store,
			handler: handler,
			opts:    []Option{WithRetry(0, time.Second)},
			want:    "maxAttempts must be >= 1",
		},
		{
			name:    "invalid config",
			cfg:     Config{},
			client:  client,
			store:   store,
			handler: handler,
			want:    "stream name or ARN is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			consumer, err := New(tt.cfg, tt.client, tt.store, tt.handler, tt.opts...)
			if err == nil {
				t.Fatalf("New() error = nil, want %q", tt.want)
			}
			if consumer != nil {
				t.Fatalf("New() consumer = %v, want nil", consumer)
			}
			if err.Error() != tt.want {
				t.Fatalf("New() error = %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestNewAppliesDefaultsAndOptions(t *testing.T) {
	t.Parallel()

	client := &Client{}
	store := fakeCheckpointStore{}
	handler := func(context.Context, Record) error { return nil }

	c, err := New(
		Config{StreamName: "stream"},
		client,
		store,
		handler,
		WithRetry(2, 3*time.Second),
		WithBatching(4, 5),
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	if c == nil {
		t.Fatal("New() consumer = nil, want consumer")
	}
	if c.cfg.StartPosition != StartLatest {
		t.Fatalf("StartPosition = %q, want %q", c.cfg.StartPosition, StartLatest)
	}
	if c.client != client {
		t.Fatalf("client was not retained")
	}
	if c.store == nil {
		t.Fatalf("store = nil, want store")
	}
	if c.handler == nil {
		t.Fatalf("handler = nil, want handler")
	}
	if c.batchHandler != nil {
		t.Fatalf("batchHandler = %v, want nil", c.batchHandler)
	}
	if c.tuning.retryMaxAttempts != 2 {
		t.Fatalf("retryMaxAttempts = %d, want 2", c.tuning.retryMaxAttempts)
	}
	if c.tuning.retryBackoff != 3*time.Second {
		t.Fatalf("retryBackoff = %v, want %v", c.tuning.retryBackoff, 3*time.Second)
	}
	if c.tuning.batchSize != 4 {
		t.Fatalf("batchSize = %d, want 4", c.tuning.batchSize)
	}
	if c.tuning.checkpointEvery != 5 {
		t.Fatalf("checkpointEvery = %d, want 5", c.tuning.checkpointEvery)
	}
}

func TestNewAllowsBatchHandlerWithoutRecordHandler(t *testing.T) {
	t.Parallel()

	client := &Client{}
	store := fakeCheckpointStore{}
	batchHandler := func(context.Context, []Record) error { return nil }

	c, err := New(
		Config{StreamName: "stream"},
		client,
		store,
		nil,
		WithBatchHandler(batchHandler),
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	if c.handler != nil {
		t.Fatalf("handler = %v, want nil", c.handler)
	}
	if c.batchHandler == nil {
		t.Fatalf("batchHandler = nil, want handler")
	}
}
