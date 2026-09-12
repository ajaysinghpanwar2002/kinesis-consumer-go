package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// InFlightLimits bounds admitted records and their payload bytes. Zero fields
// select defaults. Payload size is len(Record.Data) before application access.
// Staged pages, SDK decoding, active callbacks, and application-owned buffers
// are additional memory; MaxFetchSlots bounds fetched/staged pages, not RSS.
type InFlightLimits struct {
	MaxRecordsPerShard    int
	MaxBytesPerShard      int
	MaxRecordsPerInstance int
	MaxBytesPerInstance   int
	MaxFetchSlots         int
}

// ErrOversizedRecord identifies a record that cannot fit an empty byte budget.
var ErrOversizedRecord = errors.New("record exceeds in-flight byte limit")

// OversizedRecordError describes the first record that cannot be admitted.
type OversizedRecordError struct {
	ShardID        string
	SequenceNumber string
	Bytes          int
	Limit          int
}

func (e *OversizedRecordError) Error() string {
	return fmt.Sprintf("shard %s record %s: %v: %d bytes exceeds %d", e.ShardID, e.SequenceNumber, ErrOversizedRecord, e.Bytes, e.Limit)
}
func (e *OversizedRecordError) Unwrap() error { return ErrOversizedRecord }

// WithInFlightLimits enables admission limits, including for automatic handlers.
// Defaults are 1,000 records/10 MiB per shard, 10,000 records/64 MiB per instance,
// and four fetch slots. Negative fields are invalid. Batches are split into
// ordered prefixes that fit available capacity. Automatic completion releases
// capacity before checkpoint persistence; failed work remains replayable.
func WithInFlightLimits(limits InFlightLimits) Option {
	return func(o *options) error {
		resolved := limits
		defaults := []int{1000, 10 << 20, 10000, 64 << 20, 4}
		fields := []*int{&resolved.MaxRecordsPerShard, &resolved.MaxBytesPerShard, &resolved.MaxRecordsPerInstance, &resolved.MaxBytesPerInstance, &resolved.MaxFetchSlots}
		for i, field := range fields {
			if *field < 0 {
				return errors.New("in-flight limits cannot be negative")
			}
			if *field == 0 {
				*field = defaults[i]
			}
		}
		o.inFlight = &resolved
		return nil
	}
}

type admissionUsage struct{ records, bytes int }

type admissionRequest struct {
	ctx         context.Context
	shard       string
	sizes       []int
	slot        bool
	ready       chan struct{}
	reservation *admissionReservation
}

// admissionController reserves every applicable budget under one lock. Waiters
// are visited in arrival order; an ineligible shard does not block eligible
// shards. Grants reserve capacity before waking the waiter, so new arrivals
// cannot steal it. No callback, backend call, or wait holds this lock.
type admissionController struct {
	mu           sync.Mutex
	limits       InFlightLimits
	total        admissionUsage
	shards       map[string]admissionUsage
	slots        int
	queue        []*admissionRequest
	reservations map[*admissionReservation]struct{}
	stages       map[string]*admissionReservation
	pauses       map[string]*pauseState
	stopCtx      context.Context
	stop         context.CancelFunc
}

func newAdmissionController(limits *InFlightLimits) *admissionController {
	if limits == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &admissionController{limits: *limits, reservations: make(map[*admissionReservation]struct{}), stages: make(map[string]*admissionReservation), pauses: make(map[string]*pauseState), shards: make(map[string]admissionUsage), stopCtx: ctx, stop: cancel}
}

// A reservation retains sizes only, never payloads. Each record may release
// independently (and repeatedly) without changing another record's accounting.
type admissionReservation struct {
	controller *admissionController
	shard      string
	sizes      []int
	released   []bool
	slot       bool
	admitted   time.Time
	staged     int
	remaining  int
}

func (a *admissionController) acquire(ctx context.Context, shard string, sizes []int, slot bool) (*admissionReservation, error) {
	return a.acquireBeforeWait(ctx, shard, sizes, slot, nil)
}

func (a *admissionController) acquireBeforeWait(ctx context.Context, shard string, sizes []int, slot bool, beforeWait func() error) (*admissionReservation, error) {
	if a == nil {
		return nil, nil
	}
	r := &admissionRequest{ctx: ctx, shard: shard, sizes: sizes, slot: slot, ready: make(chan struct{})}
	a.mu.Lock()
	a.queue = append(a.queue, r)
	a.schedule()
	a.mu.Unlock()
	var waitErr error
	if beforeWait != nil {
		select {
		case <-r.ready:
		default:
			waitErr = beforeWait()
		}
	}
	if waitErr == nil {
		select {
		case <-r.ready:
		case <-ctx.Done():
		case <-a.stopCtx.Done():
		}
	}
	a.mu.Lock()
	err := waitErr
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = a.stopCtx.Err()
	}
	if err != nil {
		for i, queued := range a.queue {
			if queued == r {
				a.queue = append(a.queue[:i], a.queue[i+1:]...)
				break
			}
		}
		// Do not leave pointers to canceled requests in the backing array.
		a.compactQueue()
		if r.reservation != nil {
			r.reservation.releaseLocked()
		}
		a.schedule()
	}
	a.mu.Unlock()
	return r.reservation, err
}

func (a *admissionController) compactQueue() {
	clear(a.queue[len(a.queue):cap(a.queue)])
}

func (a *admissionController) schedule() {
	defer a.updatePauses(time.Now())
	if a.stopCtx.Err() != nil {
		return
	}
	remaining := a.queue[:0]
	for _, r := range a.queue {
		if r.ctx.Err() != nil {
			continue
		}
		n, bytes := 0, 0
		if r.slot {
			if a.slots < a.limits.MaxFetchSlots {
				n = 1
			}
		} else {
			shard := a.shards[r.shard]
			for _, size := range r.sizes {
				if n >= a.limits.MaxRecordsPerShard-shard.records || n >= a.limits.MaxRecordsPerInstance-a.total.records ||
					size > a.limits.MaxBytesPerShard-shard.bytes-bytes || size > a.limits.MaxBytesPerInstance-a.total.bytes-bytes {
					break
				}
				n++
				bytes += size
			}
		}
		if n == 0 {
			remaining = append(remaining, r)
			continue
		}
		reservation := &admissionReservation{controller: a, shard: r.shard, slot: r.slot, admitted: time.Now()}
		a.reservations[reservation] = struct{}{}
		if r.slot {
			a.slots++
			a.stages[r.shard] = reservation
		} else {
			reservation.sizes = append([]int(nil), r.sizes[:n]...)
			reservation.released = make([]bool, n)
			reservation.remaining = n
			usage := a.shards[r.shard]
			usage.records += n
			usage.bytes += bytes
			a.shards[r.shard] = usage
			a.total.records += n
			a.total.bytes += bytes
			if held := a.stages[r.shard]; held != nil {
				held.staged = max(0, held.staged-bytes)
			}
		}
		r.reservation = reservation
		close(r.ready)
	}
	a.queue = remaining
	a.compactQueue()
}

func (r *admissionReservation) releaseRecord(i int) {
	if r == nil {
		return
	}
	a := r.controller
	a.mu.Lock()
	r.releaseRecordLocked(i)
	a.schedule()
	a.mu.Unlock()
}

func (r *admissionReservation) releaseRecordLocked(i int) {
	if r.released[i] {
		return
	}
	r.released[i] = true
	size := r.sizes[i]
	usage := r.controller.shards[r.shard]
	usage.records--
	usage.bytes -= size
	if usage.records == 0 {
		delete(r.controller.shards, r.shard)
	} else {
		r.controller.shards[r.shard] = usage
	}
	r.controller.total.records--
	r.controller.total.bytes -= size
	r.remaining--
	if r.remaining == 0 {
		delete(r.controller.reservations, r)
	}
}

func (r *admissionReservation) releaseLocked() {
	if r.slot {
		r.controller.slots--
		r.slot = false
		r.staged = 0
		delete(r.controller.stages, r.shard)
		delete(r.controller.reservations, r)
	}
	for i := range r.sizes {
		r.releaseRecordLocked(i)
	}
}

func (r *admissionReservation) release() {
	if r == nil {
		return
	}
	a := r.controller
	a.mu.Lock()
	r.releaseLocked()
	a.schedule()
	a.mu.Unlock()
}
