# Explicit processing status

Explicit handler processing is implemented as an internal component. Public
activation remains unavailable until worker shutdown and resharding integration
is complete. `New` returns `ErrExplicitModeUnavailable` when configured with
`WithExplicitHandler` or `WithExplicitBatchHandler`. These options require a nil
constructor handler and exactly one handler mode. Automatic handlers continue
to use the existing processing path.

`WithCheckpointInterval` reserves the explicit-mode interval setting, requires a
positive duration, and is rejected for automatic handlers. The internal explicit
processor defaults to one second. This release does not enable an application
usage example; the shutdown example will accompany lifecycle activation.

## Internal integration contract

An `explicitProcessor` belongs to one shard acquisition. Its constructor requires
admission limits, a fenced lease, and a matching fenced checkpoint store. Binding
never falls back to an unfenced session. The caller supplies the shared instance
admission controller; public activation must resolve the documented explicit
limit defaults before constructing it.

One caller passes owned staged pages to `processPage`, retaining a fetch slot
until the page is admitted or discarded. Recovery initialization precedes the
first admission decision, including oversized-record rejection. If initialization
finds an existing different recovery position, the page is rejected for recovery;
it cannot be delivered from the later observation. The worker must first verify
persisted recovery anchors using the existing recovery read path.

Successful callback return does not acknowledge records or cancel their
completion context. `Ack` checks ownership, accepts completion atomically with
failed-attempt invalidation, and releases record/byte capacity. The tracker owns
only metadata. Active callbacks and application-retained deliveries may continue
to own payloads; the library clears consumed staging and retry slots without
clearing application-owned slices or data.

Failed callbacks and panics retry only unfinished deliveries, in their original
order, with fresh handles. Old unfinished handles become stale before retry
backoff or failure-policy work. Accepted handles stay accepted while the session
is live. Skip completes the remaining failed deliveries; DLQ completion occurs
only after successful publication. Failure or cancellation leaves unfinished
records replayable. Neither skip nor DLQ can advance a checkpoint across a gap.

A separate checkpoint runner consumes coalesced acknowledgment notifications and
an interval timer. It writes only the contiguous completed prefix, serializes
count/interval/flush requests, and uses the existing bounded checkpoint retry
policy and checkpoint metrics. Valkey ownership validation runs independently of the checkpoint mutation
lock and rechecks permanent local invalidation after its backend read. Acceptance
can release admission while a checkpoint write is blocked, provided the backend
can still answer ownership reads. Successful `Ack` does not promise persistence. Exhausted
writes invalidate the session, cancel processing, wake admission, and are
returned by `wait`; the worker must propagate that failure through `Start`.

`flushCheckpoint` flushes eligible progress while ownership remains live. `stop`
invalidates handles and cancels processing before waiting for the backend
session invalidation lock; `wait` joins the checkpoint runner. These are internal operations, not a completed graceful shutdown API.
Slice 7 must stop fetching and admission separately, preserve completion work
and heartbeats during drain, wait for all admitted work, flush before release,
and gate shard completion and children on persisted completion. It must also
connect lease loss, shedding, timeout, and immediate shutdown to invalidation.
New pressure metrics and health snapshots remain Slice 8; benchmarks and heap
profiles remain Slice 9.
