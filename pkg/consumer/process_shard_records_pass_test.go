package consumer

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

func TestProcessShardRecordsPassDiscardsPageCompletedAfterPollingCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	getRecordsCalls := 0
	var handled []string
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("iterator-2"),
			},
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-2")},
					{SequenceNumber: aws.String("sequence-3")},
				},
				NextShardIterator: aws.String("iterator-3"),
			},
		},
		afterGetRecords: func() {
			getRecordsCalls++
			if getRecordsCalls == 2 {
				cancel()
			}
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(ctx, "shard-1", 1, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, context.Canceled)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	wantHandled := []string{"sequence-1", "sequence-2", "sequence-3"}
	if len(handled) != len(wantHandled) {
		t.Fatalf("handled records = %v, want %v", handled, wantHandled)
	}
	for i := range wantHandled {
		if handled[i] != wantHandled[i] {
			t.Fatalf("handled records = %v, want %v", handled, wantHandled)
		}
	}
	if len(client.getRecordsCalls) != 2 {
		t.Fatalf("GetRecords calls = %d, want 2", len(client.getRecordsCalls))
	}
	if aws.ToString(client.getRecordsCalls[0].ShardIterator) != "iterator-1" {
		t.Fatalf("first ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[0].ShardIterator), "iterator-1")
	}
	if aws.ToString(client.getRecordsCalls[1].ShardIterator) != "iterator-2" {
		t.Fatalf("second ShardIterator = %q, want %q", aws.ToString(client.getRecordsCalls[1].ShardIterator), "iterator-2")
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessShardRecordsPassCarriesCheckpointCount(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, err := c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}},
	}, 1)
	if err != nil {
		t.Fatalf("first processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" || count != 2 {
		t.Fatalf("first page = (%q, %d), want (%q, 2)", lastSeq, count, "sequence-1")
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls after first page = %d, want 0", len(store.saveCalls))
	}

	lastSeq, count, err = c.processRecordsPageWithCheckpoint(context.Background(), "shard-1", &kinesis.GetRecordsOutput{
		Records: []types.Record{{SequenceNumber: aws.String("sequence-2")}},
	}, count)
	if err != nil {
		t.Fatalf("second processRecordsPageWithCheckpoint() error = %v, want nil", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-2" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-2")
	}
}

func TestProcessShardRecordsPassFlushesCheckpointWhenCaughtUp(t *testing.T) {
	t.Parallel()

	// Open shard: a page with records followed by an empty page whose
	// NextShardIterator is non-nil (caught up). Even though checkpointEvery (100)
	// is not reached, the pass must flush the last processed sequence so the next
	// pass resumes past it instead of replaying. It must NOT be treated as a
	// completed shard.
	var handled []string
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-1")},
					{SequenceNumber: aws.String("sequence-2")},
				},
				NextShardIterator: aws.String("iterator-2"),
			},
			{NextShardIterator: aws.String("iterator-3")}, // empty: caught up to tip
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 100},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil (open shard, not completed)", err)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (flushed so the next pass resumes cleanly)", count)
	}
	if len(handled) != 2 {
		t.Fatalf("handled records = %v, want [sequence-1 sequence-2]", handled)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1 (flush of the resume point)", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-2" {
		t.Fatalf("flushed sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-2")
	}
	// Both pages must have been polled (records page, then the caught-up empty page).
	if len(client.getRecordsCalls) != 2 {
		t.Fatalf("GetRecords calls = %d, want 2", len(client.getRecordsCalls))
	}
}

func TestProcessShardRecordsPassProcessesEachPageBeforeFetchingNext(t *testing.T) {
	t.Parallel()

	// Streaming: a page must be handled before the next page is fetched, so memory
	// stays bounded to one page. The buffered (old) design would fetch all pages
	// first and handle them afterward, producing [fetch fetch handle:...].
	var order []string
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("iterator-2"),
			},
			{NextShardIterator: aws.String("iterator-3")}, // empty: caught up
		},
		afterGetRecords: func() { order = append(order, "fetch") },
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			order = append(order, "handle:"+aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	if _, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, ""); err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}

	want := []string{"fetch", "handle:sequence-1", "fetch"}
	if len(order) != len(want) {
		t.Fatalf("event order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("event order = %v, want %v (page 1 must be handled before page 2 is fetched)", order, want)
		}
	}
}

func TestProcessShardRecordsPassSavesShardEndWithLastSequence(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "")
	if !errors.Is(err, errShardCompleted) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errShardCompleted)
	}
	if err == nil || err.Error() != "process shard records pass shard-1: shard already completed" {
		t.Fatalf("processShardRecordsPass() error = %v, want %q", err, "process shard records pass shard-1: shard already completed")
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "SHARD_END:sequence-1" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "SHARD_END:sequence-1")
	}
}

func TestProcessShardRecordsPassCheckpointsThenSavesShardEndOnSamePage(t *testing.T) {
	t.Parallel()

	// A single page that both reaches the checkpointEvery threshold AND closes the
	// shard (nil NextShardIterator) must save the ordinary due checkpoint during
	// processing and then the SHARD_END completion checkpoint, and return
	// errShardCompleted.
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-1")},
					{SequenceNumber: aws.String("sequence-2")},
				},
			}, // nil NextShardIterator: closes the shard
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 2}, // reached exactly by this page
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "")
	if !errors.Is(err, errShardCompleted) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errShardCompleted)
	}
	if lastSeq != "sequence-2" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-2")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (reset by the due checkpoint)", count)
	}
	// Two saves: the due checkpoint (sequence-2) then the completion marker.
	if len(store.saveCalls) != 2 {
		t.Fatalf("Save calls = %d, want 2", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "sequence-2" {
		t.Fatalf("first save = %q, want %q", store.saveCalls[0].sequenceNumber, "sequence-2")
	}
	if store.saveCalls[1].sequenceNumber != "SHARD_END:sequence-2" {
		t.Fatalf("second save = %q, want %q", store.saveCalls[1].sequenceNumber, "SHARD_END:sequence-2")
	}
}

func TestProcessShardRecordsPassSavesShardEndWithoutLastSequence(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{},
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "")
	if !errors.Is(err, errShardCompleted) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errShardCompleted)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "SHARD_END" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "SHARD_END")
	}
}

func TestProcessShardRecordsPassDoesNotSaveShardEndWhenDraining(t *testing.T) {
	t.Parallel()

	var c *Consumer
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
		afterGetRecords: func() {
			c.draining.Store(true)
		},
	}
	store := &fakeCheckpointSaveStore{}
	c = &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessShardRecordsPassWrapsShardEndCheckpointError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
	}
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process shard records pass completion checkpoint shard-1: save shard completion checkpoint shard-1 SHARD_END:sequence-1: boom" {
		t.Fatalf("processShardRecordsPass() error = %v, want %q", err, "process shard records pass completion checkpoint shard-1: save shard completion checkpoint shard-1 SHARD_END:sequence-1: boom")
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1", len(store.saveCalls))
	}
	if store.saveCalls[0].sequenceNumber != "SHARD_END:sequence-1" {
		t.Fatalf("sequenceNumber = %q, want %q", store.saveCalls[0].sequenceNumber, "SHARD_END:sequence-1")
	}
}

func TestProcessShardRecordsPassWrapsIteratorError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{getShardIteratorErr: errBoom}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    &fakeCheckpointSaveStore{},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 2, "")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errBoom)
	}
	if err == nil || err.Error() != "process shard records pass shard-1: get shard iterator shard-1: boom" {
		t.Fatalf("processShardRecordsPass() error = %v, want %q", err, "process shard records pass shard-1: get shard iterator shard-1: boom")
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}

func TestProcessShardRecordsPassWrapsProcessingError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:        slog.New(slog.DiscardHandler),
		reporter:      metrics.Nop{},
		cfg:           Config{StreamName: "stream"},
		client:        client,
		store:         store,
		failurePolicy: FailurePolicyFailFast,
		tuning:        tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return errBoom
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 2, "")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errBoom)
	}
	want := "process shard records pass shard-1: process records page with checkpoint shard-1: process records page shard-1: handle records page shard-1: record handler: record handler failed after 1 attempts: boom"
	if err == nil || err.Error() != want {
		t.Fatalf("processShardRecordsPass() error = %v, want %q", err, want)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessShardRecordsPassRejectsPageCompletedAfterCanceledPolling(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var handled []string
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("iterator-1"),
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("iterator-2"),
			},
			{
				Records: []types.Record{{SequenceNumber: aws.String("sequence-2")}},
			},
		},
		afterGetRecords: cancel,
	}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(ctx, "shard-1", 1, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, context.Canceled)
	}
	if lastSeq != "" {
		t.Fatalf("lastSeq = %q, want empty", lastSeq)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(handled) != 1 || handled[0] != "sequence-1" {
		t.Fatalf("handled records = %v, want [sequence-1]", handled)
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1", len(client.getRecordsCalls))
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("Save calls = %d, want 0", len(store.saveCalls))
	}
}

func TestProcessShardRecordsPassReDerivesAfterExpiredIterator(t *testing.T) {
	t.Parallel()

	// A held iterator can outlive its ~5-minute TTL (a large pollInterval, or a slow
	// handler stretching the gap between reads). On ExpiredIteratorException the pass
	// must DROP the stale iterator, re-derive from the checkpoint, and continue — not
	// fail the shard. The pass is entered with a non-empty (held, now-stale) iterator,
	// so the single GetShardIterator call proves the re-derive happened on recovery.
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("fresh-iterator"),
		},
		getRecordsErrs: []error{&types.ExpiredIteratorException{}}, // first read: stale iterator expired
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			// After re-derive, a page that also closes the shard so the pass ends
			// deterministically without relying on cancellation timing.
			{Records: []types.Record{{SequenceNumber: aws.String("sequence-1")}}},
		},
	}
	store := &fakeCheckpointSaveStore{}
	var handled []string
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, nextIterator, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "stale-iterator")
	if !errors.Is(err, errShardCompleted) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v (recovered from expiry, then hit shard end)", err, errShardCompleted)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if nextIterator != "" {
		t.Fatalf("nextIterator = %q, want empty (shard completed)", nextIterator)
	}
	// The stale iterator was dropped and re-derived exactly once on recovery.
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1 (re-derive after expiry)", len(client.getShardIteratorCalls))
	}
	// Two reads: the expired one, then the successful one after re-derive.
	if len(client.getRecordsCalls) != 2 {
		t.Fatalf("GetRecords calls = %d, want 2 (expired, then retried)", len(client.getRecordsCalls))
	}
	if got := aws.ToString(client.getRecordsCalls[0].ShardIterator); got != "stale-iterator" {
		t.Fatalf("first GetRecords used %q, want the stale held iterator %q", got, "stale-iterator")
	}
	if got := aws.ToString(client.getRecordsCalls[1].ShardIterator); got != "fresh-iterator" {
		t.Fatalf("second GetRecords used %q, want the re-derived %q", got, "fresh-iterator")
	}
	if len(handled) != 1 || handled[0] != "sequence-1" {
		t.Fatalf("handled records = %v, want [sequence-1] (record read after recovery)", handled)
	}
	// No progress was pending when the iterator expired, so the only save is
	// the shard-completion checkpoint — no expiry flush.
	if len(store.saveCalls) != 1 {
		t.Fatalf("Save calls = %d, want 1 (completion only)", len(store.saveCalls))
	}
	if got := store.saveCalls[0].sequenceNumber; got != shardCompletionValue("sequence-1") {
		t.Fatalf("saved checkpoint = %q, want %q", got, shardCompletionValue("sequence-1"))
	}
}

// readThroughCheckpointSaveStore makes Save visible to subsequent Get calls,
// so re-derivation observes the flushed checkpoint like a real store.
type readThroughCheckpointSaveStore struct {
	fakeCheckpointSaveStore
}

func (s *readThroughCheckpointSaveStore) Save(ctx context.Context, streamName, shardID, sequenceNumber string) error {
	if err := s.fakeCheckpointSaveStore.Save(ctx, streamName, shardID, sequenceNumber); err != nil {
		return err
	}
	s.checkpoint = sequenceNumber
	return nil
}

func TestProcessShardRecordsPassExpiredIteratorFlushesUnsavedProgress(t *testing.T) {
	t.Parallel()

	// The iterator expires mid-catch-up with records processed but not yet
	// checkpointed. Re-derivation reads only the *stored* checkpoint, so the pass
	// must flush lastSeq first — with StartLatest (the default here) and no
	// checkpoint yet, a re-derived LATEST iterator would re-anchor to the shard
	// tip and silently skip everything since the last processed page. The flush
	// must instead drive an AFTER_SEQUENCE_NUMBER(lastSeq) re-derivation and the
	// next record must be processed — no gap.
	client := &fakeKinesisClient{
		getShardIteratorOut: &kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("fresh-iterator"),
		},
		getRecordsErrs: []error{
			nil,                               // page 1: succeeds
			&types.ExpiredIteratorException{}, // page 2: held iterator expired
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records: []types.Record{
					{SequenceNumber: aws.String("sequence-1")},
					{SequenceNumber: aws.String("sequence-2")},
				},
				NextShardIterator: aws.String("next-iterator"),
			},
			// After the flush and re-derive: the record right after the
			// flushed checkpoint.
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-3")}},
				NextShardIterator: aws.String("post-iterator"),
			},
			// Caught up. The caught-up flush saves sequence-3 (pending since
			// the expiry flush reset the count to 0).
			{NextShardIterator: aws.String("tip-iterator")},
		},
	}
	store := &readThroughCheckpointSaveStore{}
	var handled []string
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, nextIterator, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (flushed)", count)
	}
	if nextIterator != "tip-iterator" {
		t.Fatalf("nextIterator = %q, want %q", nextIterator, "tip-iterator")
	}
	// Two saves: the expiry flush with the in-memory progress, then the
	// caught-up flush for the post-recovery record.
	if len(store.saveCalls) != 2 {
		t.Fatalf("Save calls = %d, want 2 (expiry flush, caught-up flush)", len(store.saveCalls))
	}
	if got := store.saveCalls[0].sequenceNumber; got != "sequence-2" {
		t.Fatalf("expiry-flushed checkpoint = %q, want %q", got, "sequence-2")
	}
	if got := store.saveCalls[1].sequenceNumber; got != "sequence-3" {
		t.Fatalf("caught-up checkpoint = %q, want %q", got, "sequence-3")
	}
	// Re-derived exactly once, from the flushed checkpoint — not LATEST.
	if len(client.getShardIteratorCalls) != 1 {
		t.Fatalf("GetShardIterator calls = %d, want 1 (re-derive after expiry)", len(client.getShardIteratorCalls))
	}
	if got := client.getShardIteratorCalls[0].ShardIteratorType; got != types.ShardIteratorTypeAfterSequenceNumber {
		t.Fatalf("re-derive iterator type = %q, want %q", got, types.ShardIteratorTypeAfterSequenceNumber)
	}
	if got := aws.ToString(client.getShardIteratorCalls[0].StartingSequenceNumber); got != "sequence-2" {
		t.Fatalf("re-derive StartingSequenceNumber = %q, want %q", got, "sequence-2")
	}
	// Reads: page 1 on the held iterator, the expired read on its successor,
	// the post-recovery read on the re-derived iterator, then the caught-up read.
	if len(client.getRecordsCalls) != 4 {
		t.Fatalf("GetRecords calls = %d, want 4", len(client.getRecordsCalls))
	}
	if got := aws.ToString(client.getRecordsCalls[1].ShardIterator); got != "next-iterator" {
		t.Fatalf("expired read used %q, want %q", got, "next-iterator")
	}
	if got := aws.ToString(client.getRecordsCalls[2].ShardIterator); got != "fresh-iterator" {
		t.Fatalf("post-recovery read used %q, want %q", got, "fresh-iterator")
	}
	// The post-flush record was processed — recovery continued with no gap.
	if len(handled) != 3 || handled[2] != "sequence-3" {
		t.Fatalf("handled records = %v, want [sequence-1 sequence-2 sequence-3]", handled)
	}
}

func TestProcessShardRecordsPassExpiredIteratorFlushSaveFailureReturnsError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	client := &fakeKinesisClient{
		getRecordsErrs: []error{
			nil,
			&types.ExpiredIteratorException{},
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("next-iterator"),
			},
		},
	}
	store := &fakeCheckpointSaveStore{saveErr: errBoom}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, nextIterator, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator")
	if !errors.Is(err, errBoom) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errBoom)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1 (flush failed, progress still pending)", count)
	}
	if nextIterator != "" {
		t.Fatalf("nextIterator = %q, want empty on error", nextIterator)
	}
	if len(client.getShardIteratorCalls) != 0 {
		t.Fatalf("GetShardIterator calls = %d, want 0 (failed before re-derive)", len(client.getShardIteratorCalls))
	}
}

func TestProcessShardRecordsPassNilGetRecordsOutputDoesNotCheckpointShardEnd(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{getRecordsOuts: []*kinesis.GetRecordsOutput{nil}}
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		client:   client,
		store:    store,
	}

	lastSeq, count, iterator, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "iterator-1")
	if !errors.Is(err, errNilKinesisOutput) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps %v", err, errNilKinesisOutput)
	}
	if err == nil || err.Error() != "process shard records pass shard-1: get records iterator-1: kinesis protocol error: nil output without error" {
		t.Fatalf("processShardRecordsPass() error = %v, want nil-output protocol error", err)
	}
	if lastSeq != "" || count != 0 || iterator != "" {
		t.Fatalf("processShardRecordsPass() = (%q, %d, %q), want empty progress on error", lastSeq, count, iterator)
	}
	if len(store.saveCalls) != 0 {
		t.Fatalf("checkpoint Save calls = %d, want 0 (especially no SHARD_END)", len(store.saveCalls))
	}
}

func TestGetRecordsBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: 0},
		{failures: 1, want: 500 * time.Millisecond},
		{failures: 2, want: time.Second},
		{failures: 3, want: 2 * time.Second},
		{failures: 5, want: 8 * time.Second},
		{failures: 6, want: 10 * time.Second},
		{failures: 100, want: 10 * time.Second},
	}
	for _, tt := range tests {
		if got := getRecordsBackoff(tt.failures); got != tt.want {
			t.Fatalf("getRecordsBackoff(%d) = %v, want %v", tt.failures, got, tt.want)
		}
	}
}

func TestProcessShardRecordsPassRetriesRetryableGetRecordsErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "provisioned throughput exceeded", err: &types.ProvisionedThroughputExceededException{}},
		{name: "limit exceeded", err: &types.LimitExceededException{}},
		{name: "kms throttling", err: &types.KMSThrottlingException{}},
		{name: "server fault", err: &smithy.GenericAPIError{Code: "InternalFailure", Fault: smithy.FaultServer}},
		{name: "network error", err: &net.DNSError{Err: "no such host", IsTimeout: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeKinesisClient{
				getRecordsErrs: []error{tt.err}, // first read fails, second succeeds
				getRecordsOuts: []*kinesis.GetRecordsOutput{
					{
						Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
						NextShardIterator: aws.String("next-iterator"),
					},
					{NextShardIterator: aws.String("tip-iterator")}, // caught up
				},
			}
			store := &fakeCheckpointSaveStore{}
			logHandler := newCapturingHandler()
			var sleeps []time.Duration
			var handled []string
			c := &Consumer{
				logger:   slog.New(logHandler),
				reporter: metrics.Nop{},
				cfg:      Config{StreamName: "stream"},
				client:   client,
				store:    store,
				tuning:   tuningConfig{checkpointEvery: 10},
				sleepFn: func(ctx context.Context, d time.Duration) error {
					_ = ctx
					sleeps = append(sleeps, d)
					return nil
				},
				handler: func(ctx context.Context, record Record) error {
					_ = ctx
					handled = append(handled, aws.ToString(record.SequenceNumber))
					return nil
				},
			}

			lastSeq, count, nextIterator, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator")
			if err != nil {
				t.Fatalf("processShardRecordsPass() error = %v, want nil (survived retryable error)", err)
			}
			if lastSeq != "sequence-1" {
				t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
			}
			if count != 0 {
				t.Fatalf("count = %d, want 0 (caught-up flush)", count)
			}
			if nextIterator != "tip-iterator" {
				t.Fatalf("nextIterator = %q, want %q", nextIterator, "tip-iterator")
			}
			if len(handled) != 1 || handled[0] != "sequence-1" {
				t.Fatalf("handled records = %v, want [sequence-1]", handled)
			}
			// One backoff sleep at the base duration, then the retry succeeded.
			if len(sleeps) != 1 || sleeps[0] != 500*time.Millisecond {
				t.Fatalf("backoff sleeps = %v, want [500ms]", sleeps)
			}
			// The retry reused the same iterator — no re-derivation.
			if len(client.getRecordsCalls) != 3 {
				t.Fatalf("GetRecords calls = %d, want 3 (fail, retry, caught-up)", len(client.getRecordsCalls))
			}
			if got := aws.ToString(client.getRecordsCalls[1].ShardIterator); got != "held-iterator" {
				t.Fatalf("retry used iterator %q, want %q", got, "held-iterator")
			}
			if len(client.getShardIteratorCalls) != 0 {
				t.Fatalf("GetShardIterator calls = %d, want 0 (no re-derive on retryable errors)", len(client.getShardIteratorCalls))
			}
			var warns []capturedRecord
			for _, rec := range logHandler.snapshot() {
				if rec.message == "get records failed; backing off" {
					warns = append(warns, rec)
				}
			}
			if len(warns) != 1 {
				t.Fatalf("backoff warn logs = %d, want 1", len(warns))
			}
			if warns[0].level != slog.LevelWarn {
				t.Fatalf("backoff log level = %v, want %v", warns[0].level, slog.LevelWarn)
			}
			if warns[0].attrs["shard"] != "shard-1" {
				t.Fatalf("backoff log shard = %q, want shard-1", warns[0].attrs["shard"])
			}
			if warns[0].attrs["consecutive_failures"] != "1" {
				t.Fatalf("backoff log consecutive_failures = %q, want 1", warns[0].attrs["consecutive_failures"])
			}
		})
	}
}

func TestProcessShardRecordsPassBackoffGrowsAndResetsOnSuccess(t *testing.T) {
	t.Parallel()

	throttle := func() error { return &types.ProvisionedThroughputExceededException{} }
	client := &fakeKinesisClient{
		getRecordsErrs: []error{
			throttle(), // read 1: fail (backoff 500ms)
			throttle(), // read 2: fail (backoff 1s)
			nil,        // read 3: success — resets the backoff
			throttle(), // read 4: fail (backoff back at 500ms)
			nil,        // read 5: success (caught up)
		},
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("next-iterator"),
			},
			{NextShardIterator: aws.String("tip-iterator")},
		},
	}
	store := &fakeCheckpointSaveStore{}
	var sleeps []time.Duration
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			sleeps = append(sleeps, d)
			return nil
		},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	_, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 500 * time.Millisecond}
	if len(sleeps) != len(want) {
		t.Fatalf("backoff sleeps = %v, want %v", sleeps, want)
	}
	for i := range want {
		if sleeps[i] != want[i] {
			t.Fatalf("backoff sleeps = %v, want %v", sleeps, want)
		}
	}
}

func TestProcessShardRecordsPassNonRetryableGetRecordsErrorFails(t *testing.T) {
	t.Parallel()

	clientFault := &smithy.GenericAPIError{Code: "InvalidArgumentException", Fault: smithy.FaultClient}
	client := &fakeKinesisClient{getRecordsErr: clientFault}
	var sleeps []time.Duration
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		tuning:   tuningConfig{checkpointEvery: 10},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			sleeps = append(sleeps, d)
			return nil
		},
	}

	_, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator")
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("processShardRecordsPass() error = %v, want wraps the client fault", err)
	}
	if len(sleeps) != 0 {
		t.Fatalf("sleeps = %v, want none (client faults are fatal)", sleeps)
	}
	if len(client.getRecordsCalls) != 1 {
		t.Fatalf("GetRecords calls = %d, want 1 (no retry)", len(client.getRecordsCalls))
	}
}

func TestProcessShardRecordsPassBackoffCanceledDuringSleepReturnsIterator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeKinesisClient{
		getRecordsErrs: []error{&types.ProvisionedThroughputExceededException{}},
	}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		tuning:   tuningConfig{checkpointEvery: 10},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = d
			cancel()
			return context.Canceled
		},
	}

	lastSeq, count, nextIterator, err := c.processShardRecordsPass(ctx, "shard-1", 0, "held-iterator")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil (shutdown during backoff)", err)
	}
	if lastSeq != "" || count != 0 {
		t.Fatalf("lastSeq, count = %q, %d, want empty, 0", lastSeq, count)
	}
	if nextIterator != "held-iterator" {
		t.Fatalf("nextIterator = %q, want the held iterator preserved", nextIterator)
	}
}

func TestProcessShardRecordsPassPacesReads(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("iterator-2"),
			},
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-2")}},
				NextShardIterator: aws.String("iterator-3"),
			},
			{NextShardIterator: aws.String("tip-iterator")},
		},
	}
	store := &fakeCheckpointSaveStore{}
	var sleeps []time.Duration
	idle := 100 * time.Millisecond
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10, idleTimeBetweenReads: idle},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			sleeps = append(sleeps, d)
			return nil
		},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	_, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "iterator-1")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	// Three reads, and a pacing sleep before every read but the first.
	if len(client.getRecordsCalls) != 3 {
		t.Fatalf("GetRecords calls = %d, want 3", len(client.getRecordsCalls))
	}
	if len(sleeps) != 2 {
		t.Fatalf("pacing sleeps = %v, want 2 (before reads 2 and 3, none before the first)", sleeps)
	}
	for i, d := range sleeps {
		if d <= 0 || d > idle {
			t.Fatalf("pacing sleep %d = %v, want in (0, %v] (remainder of the idle floor)", i, d, idle)
		}
	}
}

func TestProcessShardRecordsPassZeroIdleTimeDisablesPacing(t *testing.T) {
	t.Parallel()

	client := &fakeKinesisClient{
		getRecordsOuts: []*kinesis.GetRecordsOutput{
			{
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-1")}},
				NextShardIterator: aws.String("iterator-2"),
			},
			{NextShardIterator: aws.String("tip-iterator")},
		},
	}
	store := &fakeCheckpointSaveStore{}
	var sleeps []time.Duration
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: metrics.Nop{},
		cfg:      Config{StreamName: "stream"},
		client:   client,
		store:    store,
		tuning:   tuningConfig{checkpointEvery: 10},
		sleepFn: func(ctx context.Context, d time.Duration) error {
			_ = ctx
			sleeps = append(sleeps, d)
			return nil
		},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	if _, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "iterator-1"); err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	if len(sleeps) != 0 {
		t.Fatalf("sleeps = %v, want none (pacing disabled at 0)", sleeps)
	}
}

func TestProcessShardRecordsPassCountsGetRecordsFailuresByKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantKind string
		fatal    bool
	}{
		{name: "throttle", err: &types.ProvisionedThroughputExceededException{}, wantKind: "throttle"},
		{name: "limit exceeded is throttle", err: &types.LimitExceededException{}, wantKind: "throttle"},
		{name: "kms throttling is throttle", err: &types.KMSThrottlingException{}, wantKind: "throttle"},
		{name: "generic throttling code is throttle", err: &smithy.GenericAPIError{Code: "ThrottlingException", Fault: smithy.FaultClient}, wantKind: "throttle"},
		{name: "expired iterator", err: &types.ExpiredIteratorException{}, wantKind: "expired"},
		{name: "server fault is other", err: &smithy.GenericAPIError{Code: "InternalFailure", Fault: smithy.FaultServer}, wantKind: "other"},
		{name: "fatal client fault is other", err: &smithy.GenericAPIError{Code: "InvalidArgumentException", Fault: smithy.FaultClient}, wantKind: "other", fatal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeKinesisClient{
				getShardIteratorOut: &kinesis.GetShardIteratorOutput{
					ShardIterator: aws.String("fresh-iterator"),
				},
				getRecordsErrs: []error{tt.err}, // first read fails
				getRecordsOuts: []*kinesis.GetRecordsOutput{
					{NextShardIterator: aws.String("tip-iterator")}, // recovery read: caught up
				},
			}
			reporter := &recordingReporter{}
			c := &Consumer{
				logger:   slog.New(slog.DiscardHandler),
				reporter: reporter,
				cfg:      Config{StreamName: "stream"},
				client:   client,
				store:    &fakeCheckpointSaveStore{},
				tuning:   tuningConfig{checkpointEvery: 10},
				sleepFn: func(ctx context.Context, d time.Duration) error {
					_ = ctx
					_ = d
					return nil
				},
				handler: func(ctx context.Context, record Record) error {
					_ = ctx
					_ = record
					return nil
				},
			}

			_, _, _, err := c.processShardRecordsPass(context.Background(), "shard-1", 0, "held-iterator")
			if tt.fatal && err == nil {
				t.Fatal("processShardRecordsPass() error = nil, want fatal error")
			}
			if !tt.fatal && err != nil {
				t.Fatalf("processShardRecordsPass() error = %v, want nil (survived)", err)
			}

			failures := reporter.countersNamed(metricGetRecordsFailures)
			if len(failures) != 1 {
				t.Fatalf("get_records_failures calls = %d, want 1", len(failures))
			}
			assertCounterTags(t, failures[0], map[string]string{
				"stream": "stream",
				"shard":  "shard-1",
				"kind":   tt.wantKind,
			})
		})
	}
}

// cancelingGetRecordsClient cancels the consumer context from inside the
// GetRecords call and returns an error, modeling shutdown arriving while a
// read is in flight.
type cancelingGetRecordsClient struct {
	*fakeKinesisClient
	cancel context.CancelFunc
}

func (c *cancelingGetRecordsClient) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	_ = ctx
	_ = params
	_ = optFns
	c.cancel()
	return nil, context.Canceled
}

func TestProcessShardRecordsPassNoGetRecordsFailureOnShutdownCancellation(t *testing.T) {
	t.Parallel()

	// The pass is entered with a live context; shutdown lands mid-read and the
	// call fails. That is cancellation, not a read failure: no counter, clean
	// return, held iterator preserved.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter := &recordingReporter{}
	client := &cancelingGetRecordsClient{fakeKinesisClient: &fakeKinesisClient{}, cancel: cancel}
	c := &Consumer{
		logger:   slog.New(slog.DiscardHandler),
		reporter: reporter,
		cfg:      Config{StreamName: "stream"},
		client:   client,
		tuning:   tuningConfig{checkpointEvery: 10},
	}

	lastSeq, count, nextIterator, err := c.processShardRecordsPass(ctx, "shard-1", 0, "held-iterator")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	if lastSeq != "" || count != 0 {
		t.Fatalf("lastSeq, count = %q, %d, want empty, 0", lastSeq, count)
	}
	if nextIterator != "held-iterator" {
		t.Fatalf("nextIterator = %q, want the held iterator preserved", nextIterator)
	}
	if failures := reporter.countersNamed(metricGetRecordsFailures); len(failures) != 0 {
		t.Fatalf("get_records_failures calls = %d, want 0 (shutdown cancellation)", len(failures))
	}
}
