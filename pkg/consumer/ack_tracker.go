package consumer

import (
	"container/list"
	"context"
	"sync"
)

// ackTracker contains metadata only: payload ownership stays with callbacks and
// their retry/staging buffers. validate must check the session's backend owner,
// generation and expiry, honor cancellation, and be safe for concurrent calls.
// The session must permanently invalidate this tracker when ownership is lost.
type ackTracker struct {
	mu        sync.Mutex
	shardID   string
	validate  func(context.Context) error
	invalid   bool
	ranges    list.List
	sequence  string
	completed uint64
	changed   chan struct{}
}

// Each unfinished record occupies one range. Adjacent completed records collapse
// into a single range, retaining only its final sequence and record count.
type ackRange struct {
	sequence string
	count    uint64
	done     bool
	release  func()
}

type deliveryState struct {
	tracker  *ackTracker
	element  *list.Element
	accepted bool
	stale    bool
}

func newAckTracker(shardID string, validate func(context.Context) error) *ackTracker {
	if validate == nil {
		panic("ack tracker requires ownership validation")
	}
	return &ackTracker{shardID: shardID, validate: validate, changed: make(chan struct{}, 1)}
}

// append registers records in delivery order, including across fetched pages.
// The caller must validate sequence anchors before registering records.
func (t *ackTracker) append(record Record) Delivery {
	return t.appendReserved(record, nil)
}

// release owns metadata only and must not call back into the tracker.
func (t *ackTracker) appendReserved(record Record, release func()) Delivery {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := &deliveryState{tracker: t, stale: t.invalid}
	if !t.invalid {
		sequence := ""
		if record.SequenceNumber != nil {
			sequence = *record.SequenceNumber
		}
		s.element = t.ranges.PushBack(&ackRange{sequence: sequence, count: 1, release: release})
	} else if release != nil {
		release()
	}
	return Delivery{Record: record, ShardID: t.shardID, state: s}
}

func (t *ackTracker) ack(ctx context.Context, s *deliveryState) error {
	t.mu.Lock()
	stale := t.invalid || s.stale
	t.mu.Unlock()
	if stale {
		return ErrStaleDelivery
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Backend I/O must not block invalidation or progress reads. Recheck validity
	// under the acceptance lock afterward: a failure may invalidate this attempt
	// while ownership validation is in flight.
	if err := t.validate(ctx); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.invalid || s.stale {
		return ErrStaleDelivery
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.accepted {
		return nil
	}
	s.accepted = true
	e := s.element
	s.element = nil
	r := e.Value.(*ackRange)
	r.done = true
	if r.release != nil {
		r.release()
		r.release = nil
	}
	if prev := e.Prev(); prev != nil && prev.Value.(*ackRange).done {
		r.count += prev.Value.(*ackRange).count
		t.ranges.Remove(prev)
	}
	if next := e.Next(); next != nil && next.Value.(*ackRange).done {
		n := next.Value.(*ackRange)
		r.count += n.count
		r.sequence = n.sequence
		t.ranges.Remove(next)
	}
	if e == t.ranges.Front() {
		t.sequence = r.sequence
		t.completed += r.count
		t.ranges.Remove(e)
	}
	select {
	case t.changed <- struct{}{}:
	default:
	}
	return nil
}

// retry invalidates unfinished handles from a failed callback atomically with
// Ack and returns fresh handles in the supplied delivery order. The caller owns
// the returned payload references and must clear consumed retry/page slots.
// Accepted handles remain valid. Foreign and previously invalidated handles are
// ignored, so handling the same failure twice cannot create competing attempts.
func (t *ackTracker) retry(deliveries []Delivery) []Delivery {
	t.mu.Lock()
	defer t.mu.Unlock()
	var pending []Delivery
	for _, d := range deliveries {
		s := d.state
		if s == nil || s.tracker != t || s.stale || s.accepted {
			continue
		}
		s.stale = true
		if !t.invalid {
			d.state = &deliveryState{tracker: t, element: s.element}
			pending = append(pending, d)
		}
		s.element = nil
	}
	return pending
}

func (t *ackTracker) invalidate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.invalid = true
	// Remove individually to sever links held by outstanding application handles.
	for e := t.ranges.Front(); e != nil; e = t.ranges.Front() {
		r := e.Value.(*ackRange)
		if r.release != nil {
			r.release()
			r.release = nil
		}
		t.ranges.Remove(e)
	}
}

// progress reports the contiguous candidate and cumulative covered record count.
// The caller must separately persist and track successful checkpoint writes.
func (t *ackTracker) progress() (string, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sequence, t.completed
}
