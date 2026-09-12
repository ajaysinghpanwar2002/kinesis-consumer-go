# Admission limits and staging

`WithInFlightLimits` opts automatic record and batch handlers into bounded
admission. Consumers without this option keep their existing behavior.
[Explicit acknowledgment](explicit-processing.md) is always bounded: without
this option it uses the same values as its defaults.

```go
consumer.WithInFlightLimits(consumer.InFlightLimits{
    MaxRecordsPerShard:    1_000,
    MaxBytesPerShard:      10 << 20,
    MaxRecordsPerInstance: 10_000,
    MaxBytesPerInstance:   64 << 20,
    MaxFetchSlots:         4,
})
```

These are also the defaults for zero-valued fields. Negative fields fail
construction. Limits apply to one `Consumer`, across its shard workers, rather
than across consumers sharing a group. Record and byte budgets at both scopes
are reserved together before delivery. Bytes mean `len(Record.Data)`, captured
before the application receives the record; changing a record in a callback
does not change its accounting. Zero-byte records still consume record budget.

A batch receives the largest ordered prefix that fits the currently available
budgets. A later prefix may have a different size. With automatic record
handlers, `WithShardConcurrency` still controls concurrent callbacks within each
admitted prefix. Admission preserves source order; concurrent callbacks may
finish out of order. An automatic record's capacity is released when its handler
and failure policy finish. Batch capacity is released when the batch finishes.
In explicit mode each record's capacity is released when its acknowledgment is
accepted, independently of the other records in its prefix and of any checkpoint
write. Retries keep the reservation. A failure discards unfinished work for
replay. Capacity release precedes checkpoint persistence. Completed prefixes can be
checkpointed before a later prefix finishes; a failed prefix cannot advance
the checkpoint past itself.

When capacity is exhausted, requests wait in arrival order among eligible
shards. A shard at its own limit does not block other shards that can fit.
Releasing capacity wakes waiters without waiting for a checkpoint. Existing
lease-renewal and worker-heartbeat goroutines keep running. A worker flushes
completed progress before a capacity wait, in addition to its usual count,
tip, drain, and completion checkpoints. Checkpoint writes and retries hold no
admission-controller lock.

A fetch slot is held from before `GetRecords` until every record in that page
has been admitted or the page is discarded. Recovery-anchor verification reads
use the same slots; waiting for a slot does not consume the anchor-verification
time budget. Each shard holds at most one staged page and does not fetch another
while that page is blocked. Ordinary empty pages and failed reads release their
slots. Anchor verification holds its slot through its bounded verification
retries, then transfers it with the proving page or releases it on failure.

Graceful drain stops fetches and admissions, wakes capacity waits, and allows
admitted callbacks to finish with live processing contexts. Unadmitted staging
is discarded without checkpointing it or marking a closed shard complete.
Cancellation, shedding, and lease loss wake blocked workers through their worker
contexts. A callback that ignores cancellation can still outlive shutdown, as
with unlimited automatic handling.

A record larger than either empty byte budget returns `ErrOversizedRecord` from
`Start`; use `errors.Is` to identify it or `errors.As` for `*OversizedRecordError`
with the shard, sequence, payload size, and applicable limit. A fenced backend
persists the initial inclusive replay position before this check. Increasing
the limit and restarting therefore replays that first record. Custom unfenced
backends retain their existing recovery guarantees; see
[fenced recovery](fenced-recovery.md).

Admitted-payload budgets exclude staging, SDK decoding, active callback
references, tracking metadata, and buffers retained by the application. Four
fetch slots bound simultaneous fetched/staged pages, not total process memory.
The library clears its consumed staging and record-callback slots without
clearing application-owned data or batch slices. Heap profiling and measured
memory/performance results remain part of the later validation slice.

Application batches must flush on a timer as well as a size threshold. Waiting
only for a batch size larger than the available capacity can stall processing.
