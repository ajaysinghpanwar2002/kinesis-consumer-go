package consumer

import "errors"

// ErrDrainTimeout is returned (wrapped) from Start when a graceful drain does
// not finish within the WithGracefulDrain timeout. The returned error also
// names the elapsed timeout in its message; match it with errors.Is.
var ErrDrainTimeout = errors.New("graceful drain timed out")

// ErrNoShards is returned (wrapped) from Start when the configured stream has
// no shards to consume. The returned error also names the stream in its
// message; match it with errors.Is.
var ErrNoShards = errors.New("no shards found")

// ErrConsumerClosed is returned from Start when the Consumer has been closed:
// either Close was called before Start, or Close stopped a running Start.
// Match it with errors.Is.
var ErrConsumerClosed = errors.New("consumer is closed")

// ErrConsumerAlreadyStarted is returned from every concurrent or sequential
// Start call after the first Start has claimed the Consumer. A Consumer is
// single-use even when its first run has already returned; construct a fresh
// Consumer with New for another run. Match it with errors.Is.
var ErrConsumerAlreadyStarted = errors.New("consumer has already been started")

// ErrHeartbeatStale is returned (wrapped) from Start when the worker-liveness
// heartbeat has kept failing until one heartbeat interval before the last
// successful heartbeat's TTL lapses — the point after which peers may treat
// this worker as dead and claim its shards away while it is still processing.
// The run stops inside that safety margin instead of dual-processing. The
// returned error also preserves the causal heartbeat failure; match the
// sentinel with errors.Is and inspect the cause with errors.Is/errors.As.
var ErrHeartbeatStale = errors.New("worker heartbeat stale")

// ErrShardSyncStale is returned (wrapped) from Start when periodic shard
// discovery has kept failing for longer than the configured maximum staleness
// (WithShardSyncMaxStaleness) and the consumer can no longer trust its shard
// map. The returned error also preserves the causal sync failure; match the
// sentinel with errors.Is and inspect the cause with errors.Is/errors.As.
var ErrShardSyncStale = errors.New("shard discovery stale")
