package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/checkpoint"
	"github.com/pratilipi/kinesis-consumer-go/pkg/lease"
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

type fakeProviderStore struct {
	fakeCheckpointStore
	manager lease.Manager
	err     error
}

func (s fakeProviderStore) LeaseManager() (lease.Manager, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.manager, nil
}

type fakeLeaseManager struct{}

func (fakeLeaseManager) Acquire(context.Context, string, string, string, time.Duration) (lease.Lease, bool, error) {
	return nil, false, nil
}

func (fakeLeaseManager) List(context.Context, string) (map[string]string, error) {
	return nil, nil
}

func (fakeLeaseManager) Claim(context.Context, string, string, string, string, time.Duration) (lease.Lease, bool, error) {
	return nil, false, nil
}

func (fakeLeaseManager) Heartbeat(context.Context, string, string, time.Duration) error {
	return nil
}

func (fakeLeaseManager) Workers(context.Context, string) ([]string, error) {
	return nil, nil
}

func TestNewValidation(t *testing.T) {
	t.Parallel()

	client := &Client{}
	store := fakeCheckpointStore{}
	handler := func(context.Context, Record) error { return nil }
	leaseMgr := fakeLeaseManager{}

	tests := []struct {
		name    string
		cfg     Config
		client  KinesisAPI
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
			opts:    []Option{WithLeaseManager(leaseMgr), WithRetry(0, time.Second)},
			want:    "maxAttempts must be >= 1",
		},
		{
			name:    "invalid config",
			cfg:     Config{},
			client:  client,
			store:   store,
			handler: handler,
			opts:    []Option{WithLeaseManager(leaseMgr)},
			want:    "stream name or ARN is required",
		},
		{
			name:    "missing lease manager",
			cfg:     Config{StreamName: "stream"},
			client:  client,
			store:   store,
			handler: handler,
			want:    "lease manager is required; use a store that provides leasing or WithLeaseManager",
		},
		{
			name:    "send to dlq without publisher",
			cfg:     Config{StreamName: "stream"},
			client:  client,
			store:   store,
			handler: handler,
			opts:    []Option{WithLeaseManager(leaseMgr), WithFailurePolicy(FailurePolicySendToDLQ)},
			want:    "dlq publisher is required when failure policy is send-to-dlq",
		},
		{
			name:    "record handler and batch handler together",
			cfg:     Config{StreamName: "stream"},
			client:  client,
			store:   store,
			handler: handler,
			opts:    []Option{WithLeaseManager(leaseMgr), WithBatchHandler(func(context.Context, []Record) error { return nil })},
			want:    "provide either a record handler or WithBatchHandler, not both",
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
	leaseMgr := fakeLeaseManager{}

	c, err := New(
		Config{StreamName: "stream"},
		client,
		store,
		handler,
		WithLeaseManager(leaseMgr),
		WithRetry(2, 3*time.Second),
		WithBatching(4, 5),
		WithFailurePolicy(FailurePolicySendToDLQ),
		WithDLQPublisher(noopDLQPublisher{}),
		WithGracefulDrain(6*time.Second),
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
	if c.failurePolicy != FailurePolicySendToDLQ {
		t.Fatalf("failurePolicy = %q, want %q", c.failurePolicy, FailurePolicySendToDLQ)
	}
	if c.dlqPublisher == nil {
		t.Fatal("dlqPublisher = nil, want publisher")
	}
	if c.leaseManager != leaseMgr {
		t.Fatalf("leaseManager was not retained")
	}
	if c.leaseOwner == "" {
		t.Fatal("leaseOwner = empty, want generated owner")
	}
	if !c.gracefulDrain {
		t.Fatal("gracefulDrain = false, want true")
	}
	if c.drainTimeout != 6*time.Second {
		t.Fatalf("drainTimeout = %v, want %v", c.drainTimeout, 6*time.Second)
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
	leaseMgr := fakeLeaseManager{}

	c, err := New(
		Config{StreamName: "stream"},
		client,
		store,
		nil,
		WithLeaseManager(leaseMgr),
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

func TestNewAcceptsCustomKinesisAPI(t *testing.T) {
	t.Parallel()

	// A user-supplied implementation of KinesisAPI that is NOT a
	// *kinesis.Client (here a test double) must be accepted and retained by
	// New — the API-1 contract that unblocks doubles and instrumented
	// wrappers.
	var client KinesisAPI = &fakeKinesisClient{}
	store := fakeCheckpointStore{}
	handler := func(context.Context, Record) error { return nil }
	leaseMgr := fakeLeaseManager{}

	c, err := New(Config{StreamName: "stream"}, client, store, handler, WithLeaseManager(leaseMgr))
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	if c.client != client {
		t.Fatal("custom KinesisAPI client was not retained")
	}
}

func TestNewUsesLeaseProviderStore(t *testing.T) {
	t.Parallel()

	client := &Client{}
	leaseMgr := fakeLeaseManager{}
	store := fakeProviderStore{manager: leaseMgr}
	handler := func(context.Context, Record) error { return nil }

	c, err := New(Config{StreamName: "stream"}, client, store, handler)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	if c.leaseManager != leaseMgr {
		t.Fatalf("leaseManager was not resolved from store provider")
	}
	if c.leaseOwner == "" {
		t.Fatal("leaseOwner = empty, want generated owner")
	}
}

func TestNewWrapsLeaseProviderError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &Client{}
	store := fakeProviderStore{err: errBoom}
	handler := func(context.Context, Record) error { return nil }

	_, err := New(Config{StreamName: "stream"}, client, store, handler)
	if !errors.Is(err, errBoom) {
		t.Fatalf("New() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "create lease manager from store: boom" {
		t.Fatalf("New() error = %v, want %q", err, "create lease manager from store: boom")
	}
}
