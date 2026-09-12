package consumer

import "time"

// PressureHealth describes library admission and staging, excluding SDK decoding
// and application-owned references. In automatic mode, outstanding means admitted
// callbacks that have not completed. Durations use the monotonic clock.
type PressureHealth struct {
	UnacknowledgedRecords   int
	UnacknowledgedBytes     int
	OldestUnacknowledgedAge time.Duration
	StagedBytes             int
	FetchSlots              int
	PausedShards            int
	PauseDuration           time.Duration
	PausedDuration          time.Duration
	// Pauses uses only count, bytes, and fetch_slots keys. Consumer durations
	// measure the union of paused shard intervals, not their sum.
	Pauses map[string]PauseHealth
}

// PauseHealth measures a current pause and cumulative time for one reason.
type PauseHealth struct {
	Current time.Duration
	Total   time.Duration
}

type pauseClock struct {
	since time.Time
	total time.Duration
}

func (p *pauseClock) update(active bool, now time.Time) {
	if active && p.since.IsZero() {
		p.since = now
	}
	if !active && !p.since.IsZero() {
		p.total += now.Sub(p.since)
		p.since = time.Time{}
	}
}
func (p pauseClock) snapshot(now time.Time) PauseHealth {
	var current time.Duration
	if !p.since.IsZero() {
		current = max(0, now.Sub(p.since))
	}
	return PauseHealth{Current: current, Total: p.total + current}
}

type pauseState struct {
	any     pauseClock
	reasons [3]pauseClock
}

var pauseReasons = [...]string{"count", "bytes", "fetch_slots"}

// updatePauses runs under the admission lock after every scheduling change.
// A request may be constrained by both count and bytes; expose both reasons.
func (a *admissionController) updatePauses(now time.Time) {
	active := make(map[string][3]bool)
	for _, r := range a.queue {
		if r.ctx.Err() != nil || a.stopCtx.Err() != nil {
			continue
		}
		reasons := active[r.shard]
		if r.slot {
			reasons[2] = true
		} else if len(r.sizes) > 0 {
			usage := a.shards[r.shard]
			reasons[0] = reasons[0] || usage.records >= a.limits.MaxRecordsPerShard || a.total.records >= a.limits.MaxRecordsPerInstance
			reasons[1] = reasons[1] || r.sizes[0] > a.limits.MaxBytesPerShard-usage.bytes || r.sizes[0] > a.limits.MaxBytesPerInstance-a.total.bytes
		}
		active[r.shard] = reasons
	}
	var union [3]bool
	for shard, reasons := range active {
		if a.pauses[shard] == nil {
			a.pauses[shard] = &pauseState{}
		}
		for i := range union {
			union[i] = union[i] || reasons[i]
		}
	}
	active[""] = union
	if a.pauses[""] == nil {
		a.pauses[""] = &pauseState{}
	}
	for shard, state := range a.pauses {
		reasons := active[shard]
		state.any.update(reasons[0] || reasons[1] || reasons[2], now)
		for i := range reasons {
			state.reasons[i].update(reasons[i], now)
		}
	}
}

func (r *admissionReservation) stage(records []Record) {
	if r == nil {
		return
	}
	a := r.controller
	a.mu.Lock()
	defer a.mu.Unlock()
	if !r.slot {
		return
	}
	r.staged = 0
	for _, record := range records {
		r.staged += len(record.Data)
	}
}

func (a *admissionController) pressure(now time.Time) (map[string]PressureHealth, PressureHealth) {
	shards := make(map[string]PressureHealth)
	var total PressureHealth
	if a == nil {
		return shards, total
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for shard, usage := range a.shards {
		shards[shard] = PressureHealth{UnacknowledgedRecords: usage.records, UnacknowledgedBytes: usage.bytes}
	}
	total.UnacknowledgedRecords = a.total.records
	total.UnacknowledgedBytes = a.total.bytes
	total.FetchSlots = a.slots
	for r := range a.reservations {
		s := shards[r.shard]
		if r.slot {
			s.FetchSlots++
			s.StagedBytes += r.staged
			total.StagedBytes += r.staged
		} else {
			age := now.Sub(r.admitted)
			s.OldestUnacknowledgedAge = max(s.OldestUnacknowledgedAge, age)
			total.OldestUnacknowledgedAge = max(total.OldestUnacknowledgedAge, age)
		}
		shards[r.shard] = s
	}
	for shard, state := range a.pauses {
		s := shards[shard]
		if shard == "" {
			s = total
		}
		pause := state.any.snapshot(now)
		s.PauseDuration = pause.Current
		s.PausedDuration = pause.Total
		s.Pauses = make(map[string]PauseHealth, 3)
		for i, reason := range pauseReasons {
			s.Pauses[reason] = state.reasons[i].snapshot(now)
		}
		if !state.any.since.IsZero() && shard != "" {
			s.PausedShards = 1
			total.PausedShards++
		}
		if shard == "" {
			// Map iteration order must not overwrite the paused-shard count.
			s.PausedShards = total.PausedShards
			total = s
		} else {
			shards[shard] = s
		}
	}
	return shards, total
}
