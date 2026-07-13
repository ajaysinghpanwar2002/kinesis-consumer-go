package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func TestProcessShardRecordsPassPollsAndProcessesPagesInOrder(t *testing.T) {
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(ctx, "shard-1", 1, "")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	if lastSeq != "sequence-3" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-3")
	}
	if count != 4 {
		t.Fatalf("count = %d, want 4", count)
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

	ctx, cancel := context.WithCancel(context.Background())
	getRecordsCalls := 0
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
				Records:           []types.Record{{SequenceNumber: aws.String("sequence-2")}},
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			_ = ctx
			_ = record
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(ctx, "shard-1", 1, "")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 100},
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
	var client *fakeKinesisClient
	client = &fakeKinesisClient{
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
	_ = client
	store := &fakeCheckpointSaveStore{}
	c := &Consumer{
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 2}, // reached exactly by this page
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  &fakeCheckpointSaveStore{},
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

func TestProcessShardRecordsPassProcessesPartialPagesAfterCanceledPolling(t *testing.T) {
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 3},
		handler: func(ctx context.Context, record Record) error {
			handled = append(handled, aws.ToString(record.SequenceNumber))
			return nil
		},
	}

	lastSeq, count, _, err := c.processShardRecordsPass(ctx, "shard-1", 1, "")
	if err != nil {
		t.Fatalf("processShardRecordsPass() error = %v, want nil", err)
	}
	if lastSeq != "sequence-1" {
		t.Fatalf("lastSeq = %q, want %q", lastSeq, "sequence-1")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
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
		logger: slog.New(slog.DiscardHandler),
		cfg:    Config{StreamName: "stream"},
		client: client,
		store:  store,
		tuning: tuningConfig{checkpointEvery: 10},
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
}
