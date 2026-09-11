package consumer

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// fakeStream is a single-shard Kinesis simulator for the fencing and recovery
// tests. Unlike fakeKinesisClient it resolves iterators against actual record
// positions, which is what makes inclusive/exclusive resumption, trimmed
// anchors, and expired iterators observable.
type fakeStream struct {
	mu sync.Mutex

	shardID string
	records []types.Record
	// trimmed counts leading records the retention window has dropped. They
	// stay in records so a stale anchor can still be recognized as trimmed.
	trimmed int
	closed  bool

	iterators map[string]int
	nextToken int

	// emptyPagesBeforeRecords serves this many empty pages before any page
	// carrying records, modelling Kinesis needing several reads to reach data.
	emptyPagesBeforeRecords int
	servedEmpty             int

	// invalidUnknownAnchor rejects a sequence no live record carries, as
	// Kinesis does for a sequence outside the shard's range.
	invalidUnknownAnchor bool

	// expireAfterRead invalidates every outstanding iterator once this many
	// GetRecords calls have been served, including the one just handed back,
	// so the next read fails the way an iterator past its TTL does.
	expireAfterRead int

	listShardsErr  error
	getRecordsErrs []error

	iteratorCalls []kinesis.GetShardIteratorInput
	getRecordsN   int
	onGetRecords  func(n int)
	// afterGetRecords runs once a page has been built but before it is handed
	// back, which is where a test puts anything that happens between one read
	// and whatever the consumer does next.
	afterGetRecords func(n int)
}

func newFakeStream(shardID string, sequences ...string) *fakeStream {
	s := &fakeStream{shardID: shardID, iterators: map[string]int{}}
	for _, sequence := range sequences {
		s.records = append(s.records, types.Record{
			SequenceNumber: aws.String(sequence),
			PartitionKey:   aws.String("partition-" + sequence),
			Data:           []byte("payload-" + sequence),
		})
	}
	return s
}

// sequences returns decimal sequence numbers shaped like the ones the
// checkpoint backends accept: unsigned, no leading zeros.
func sequences(first, count int) []string {
	out := make([]string, 0, count)
	for i := range count {
		out = append(out, strconv.Itoa(first+i))
	}
	return out
}

func (s *fakeStream) ListShards(_ context.Context, _ *kinesis.ListShardsInput, _ ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listShardsErr != nil {
		return nil, s.listShardsErr
	}
	return &kinesis.ListShardsOutput{Shards: []types.Shard{{ShardId: aws.String(s.shardID)}}}, nil
}

func (s *fakeStream) GetShardIterator(_ context.Context, params *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.iteratorCalls = append(s.iteratorCalls, *params)

	index := 0
	switch params.ShardIteratorType {
	case types.ShardIteratorTypeTrimHorizon:
		index = s.trimmed
	case types.ShardIteratorTypeLatest:
		index = len(s.records)
	case types.ShardIteratorTypeAtTimestamp:
		index = s.trimmed
	case types.ShardIteratorTypeAtSequenceNumber, types.ShardIteratorTypeAfterSequenceNumber:
		sequence := aws.ToString(params.StartingSequenceNumber)
		at, ok := s.indexOfLocked(sequence)
		if !ok {
			if s.invalidUnknownAnchor {
				return nil, &types.InvalidArgumentException{
					Message: aws.String("sequence " + sequence + " is not in shard " + s.shardID),
				}
			}
			// The record is gone; the iterator lands on the oldest one left,
			// which is how a trimmed anchor becomes observable as a later
			// first record rather than as an error.
			index = s.trimmed
			break
		}
		index = at
		if params.ShardIteratorType == types.ShardIteratorTypeAfterSequenceNumber {
			index = at + 1
		}
		if index < s.trimmed {
			index = s.trimmed
		}
	default:
		return nil, fmt.Errorf("unsupported iterator type %s", params.ShardIteratorType)
	}

	return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(s.newIteratorLocked(index))}, nil
}

func (s *fakeStream) GetRecords(_ context.Context, params *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.getRecordsN++
	if s.onGetRecords != nil {
		s.onGetRecords(s.getRecordsN)
	}
	if len(s.getRecordsErrs) > 0 {
		err := s.getRecordsErrs[0]
		s.getRecordsErrs = s.getRecordsErrs[1:]
		if err != nil {
			return nil, err
		}
	}

	index, ok := s.iterators[aws.ToString(params.ShardIterator)]
	if !ok {
		return nil, &types.ExpiredIteratorException{Message: aws.String("iterator expired")}
	}
	if index < s.trimmed {
		index = s.trimmed
	}

	if index < len(s.records) && s.servedEmpty < s.emptyPagesBeforeRecords {
		s.servedEmpty++
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String(s.newIteratorLocked(index))}, nil
	}

	limit := len(s.records) - index
	if limit < 0 {
		limit = 0
	}
	if params.Limit != nil && int(*params.Limit) < limit {
		limit = int(*params.Limit)
	}
	page := make([]types.Record, limit)
	copy(page, s.records[index:index+limit])

	next := index + limit
	out := &kinesis.GetRecordsOutput{Records: page}
	if !s.closed || next < len(s.records) {
		out.NextShardIterator = aws.String(s.newIteratorLocked(next))
	}
	if s.expireAfterRead > 0 && s.getRecordsN >= s.expireAfterRead {
		s.expireAfterRead = 0
		s.iterators = map[string]int{}
	}
	if s.afterGetRecords != nil {
		s.afterGetRecords(s.getRecordsN)
	}
	return out, nil
}

func (s *fakeStream) indexOfLocked(sequence string) (int, bool) {
	for i, record := range s.records {
		if aws.ToString(record.SequenceNumber) == sequence {
			if i < s.trimmed {
				return 0, false
			}
			return i, true
		}
	}
	return 0, false
}

func (s *fakeStream) newIteratorLocked(index int) string {
	s.nextToken++
	token := fmt.Sprintf("iterator-%d", s.nextToken)
	s.iterators[token] = index
	return token
}

// trim drops the oldest count records from the retention window.
func (s *fakeStream) trim(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trimLocked(count)
}

// trimLocked is trim for a caller that already holds the stream lock, which is
// what a read hook needs to trim in the middle of a read.
func (s *fakeStream) trimLocked(count int) {
	s.trimmed += count
}

func (s *fakeStream) iteratorRequests() []kinesis.GetShardIteratorInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]kinesis.GetShardIteratorInput(nil), s.iteratorCalls...)
}

var _ KinesisAPI = (*fakeStream)(nil)
