package consumer

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// newAnchorConsumer builds a consumer whose sleeps are instant, so retry
// backoffs inside anchor verification do not slow the tests down.
func newAnchorConsumer(t *testing.T, stream *fakeStream, budget time.Duration) *Consumer {
	t.Helper()

	manager := lease.NewMemoryManager()
	cons := newTestConsumer(t, stream, checkpoint.NewMemoryStoreWithLeaseManager(manager), manager)
	cons.tuning.anchorVerifyBudget = budget
	cons.sleepFn = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return cons
}

func TestVerifyShardAnchorPageAcceptsReadableAnchor(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	cons := newAnchorConsumer(t, stream, time.Second)

	if _, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "101"); err != nil {
		t.Fatalf("verifyShardAnchorPage() error = %v, want nil", err)
	}
}

func TestVerifyShardAnchorPageReturnsThePageThatProvesTheAnchor(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	cons := newAnchorConsumer(t, stream, time.Second)

	page, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100")
	if err != nil {
		t.Fatalf("verifyShardAnchorPage() error = %v, want nil", err)
	}
	got := pageSequences(page.output.Records)
	want := []string{"100", "101", "102"}
	if !slices.Equal(got, want) {
		t.Fatalf("verified page = %v, want %v: the proving read is the read the shard resumes from", got, want)
	}
	if page.output.NextShardIterator == nil {
		t.Fatal("verified page has no NextShardIterator, so the shard could not continue past it")
	}
}

func TestVerifyShardAnchorPageKeepsReadingThroughEmptyPages(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 2)...)
	stream.emptyPagesBeforeRecords = 3
	cons := newAnchorConsumer(t, stream, time.Second)

	if _, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100"); err != nil {
		t.Fatalf("verifyShardAnchorPage() error = %v, want nil: an empty page alone does not prove the anchor is gone", err)
	}
	if stream.getRecordsN < 4 {
		t.Fatalf("GetRecords calls = %d, want more than the empty pages served", stream.getRecordsN)
	}
}

func TestVerifyShardAnchorPageRejectsTrimmedAnchor(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 3)...)
	stream.trim(1)
	cons := newAnchorConsumer(t, stream, time.Second)
	reporter := &recordingReporter{}
	cons.reporter = reporter

	_, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100")
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if !strings.Contains(err.Error(), "101") {
		t.Fatalf("error = %v, want it to name the later record the shard now yields", err)
	}
	// Verification failures never pass through a session, so they are counted
	// here or nowhere.
	if got := len(reporter.countersNamed(metricRecoveryFailures)); got != 1 {
		t.Fatalf("%s = %d, want 1", metricRecoveryFailures, got)
	}
}

func TestVerifyShardAnchorPageDoesNotCountSuccessOrCancellation(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	cons := newAnchorConsumer(t, stream, time.Second)
	reporter := &recordingReporter{}
	cons.reporter = reporter

	if _, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100"); err != nil {
		t.Fatalf("verifyShardAnchorPage() error = %v, want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cons.verifyShardAnchorPage(ctx, testShardID, "100"); !errors.Is(err, context.Canceled) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, context.Canceled)
	}
	if got := len(reporter.countersNamed(metricRecoveryFailures)); got != 0 {
		t.Fatalf("%s = %d, want 0", metricRecoveryFailures, got)
	}
}

func TestVerifyShardAnchorPageRejectsAnchorPastClosedShardEnd(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 2)...)
	stream.trim(2)
	stream.closed = true
	cons := newAnchorConsumer(t, stream, time.Second)

	_, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100")
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
}

func TestVerifyShardAnchorPageRejectsSequenceOutsideShard(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 2)...)
	stream.invalidUnknownAnchor = true
	cons := newAnchorConsumer(t, stream, time.Second)

	_, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "9999")
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	var invalid *types.InvalidArgumentException
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want the causal InvalidArgumentException preserved", err)
	}
}

func TestVerifyShardAnchorPageRejectsMissingStream(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	stream.getRecordsErrs = []error{&types.ResourceNotFoundException{Message: aws.String("gone")}}
	cons := newAnchorConsumer(t, stream, time.Second)

	_, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100")
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Fatalf("error = %v, want the causal ResourceNotFoundException preserved", err)
	}
}

func TestVerifyShardAnchorPageRetriesTransientFailures(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	stream.getRecordsErrs = []error{
		&types.ProvisionedThroughputExceededException{Message: aws.String("slow down")},
		nil,
	}
	cons := newAnchorConsumer(t, stream, time.Second)

	if _, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100"); err != nil {
		t.Fatalf("verifyShardAnchorPage() error = %v, want nil after a throttled read", err)
	}
	if stream.getRecordsN != 2 {
		t.Fatalf("GetRecords calls = %d, want 2 (one throttled, one successful)", stream.getRecordsN)
	}
}

func TestVerifyShardAnchorPageFailsWhenBudgetIsSpent(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	// The shard never gets past an empty page, so verification can never
	// conclude either way.
	stream.emptyPagesBeforeRecords = 1 << 30
	cons := newAnchorConsumer(t, stream, 20*time.Millisecond)

	_, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100")
	if !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if !strings.Contains(err.Error(), "could not be verified within") {
		t.Fatalf("error = %v, want it to report the exhausted budget", err)
	}
}

func TestVerifyShardAnchorPageReturnsCancellationNotRecoveryFailure(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	stream.emptyPagesBeforeRecords = 1 << 30
	cons := newAnchorConsumer(t, stream, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	stream.onGetRecords = func(int) { cancel() }

	_, err := cons.verifyShardAnchorPage(ctx, testShardID, "100")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, context.Canceled)
	}
	if errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("error = %v, want a shutdown, not a recovery failure", err)
	}
}

func TestVerifyShardAnchorPageRejectsEmptyRecoverySequence(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	cons := newAnchorConsumer(t, stream, time.Second)

	if _, err := cons.verifyShardAnchorPage(context.Background(), testShardID, ""); !errors.Is(err, checkpoint.ErrRecoveryState) {
		t.Fatalf("verifyShardAnchorPage() error = %v, want %v", err, checkpoint.ErrRecoveryState)
	}
	if stream.getRecordsN != 0 {
		t.Fatalf("GetRecords calls = %d, want 0: an empty anchor is rejected without a read", stream.getRecordsN)
	}
}

func TestVerifyShardAnchorPageRederivesAfterExpiredIterator(t *testing.T) {
	stream := newFakeStream(testShardID, sequences(100, 1)...)
	stream.getRecordsErrs = []error{&types.ExpiredIteratorException{Message: aws.String("expired")}, nil}
	cons := newAnchorConsumer(t, stream, time.Second)

	if _, err := cons.verifyShardAnchorPage(context.Background(), testShardID, "100"); err != nil {
		t.Fatalf("verifyShardAnchorPage() error = %v, want nil after re-deriving an expired iterator", err)
	}
	if got := len(stream.iteratorRequests()); got != 2 {
		t.Fatalf("GetShardIterator calls = %d, want 2 (the original and its replacement)", got)
	}
}

// pageSequences names the records a page carries, which is what the anchor and
// resumption assertions are really about.
func pageSequences(records []types.Record) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, aws.ToString(record.SequenceNumber))
	}
	return out
}
