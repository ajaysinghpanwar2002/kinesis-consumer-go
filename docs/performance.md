# Processing performance and retained memory

The Slice 9 harness measures automatic handling and explicit synchronous,
asynchronous, and batch acknowledgment through the public consumer API. A timed
synthetic Kinesis source offers 1,157, 2,314, or 5,785 records/second, independently
of consumption. Unfetched records remain a numerical backlog, so a slow consumer
cannot silently reduce offered load or inflate the harness's payload heap.

The matrix uses one and four shards, 1 KiB and 16 KiB payloads, and memory and
real Valkey coordination. Additional cases inject a 1 ms delay before each
ownership validation and session operation. Those cases use four shards and
1 KiB payloads. All cases keep ownership checks enabled.

## Method

Each measured window lasts five seconds. Startup acquisition precedes the first
source read; that first read starts offered load. Measurements end before drain,
so completing outstanding work during shutdown cannot hide backlog. Midpoint and
final backlog distinguish a small polling delay from accumulating work. These are
short headroom experiments, not long-duration capacity guarantees.

The source returns at most 100 records per fetch. Read pacing is 1 ms, checkpoint
count is 100, and explicit checkpoint interval is the default one second.
Synchronous and batch callbacks acknowledge in order. Asynchronous callbacks
submit to one application completion worker that drains its queue every 2 ms;
that worker calls Ack sequentially across all shards. Its serialization is part
of that workload and can limit throughput independently of shard count.

Reported acknowledgment percentiles measure the duration of successful `Ack`
calls. They exclude queueing before Ack, application processing, and checkpoint
persistence. Automatic mode has no Ack call; its percentile fields are zero.
`BenchmarkOwnershipValidation` measures the ownership check alone, excluding
fetching, handlers, admission and tracking.

`CoordinationOpsPerRecord` counts calls to lease `Validate` and session
`Recovery`, `Initialize`, and `Save`, including startup calls during the run. It
excludes acquisition, renewal, discovery, internal backend commands and commands
inside Lua scripts; it is an API-operation count, not total Valkey command count.
CPU seconds are process user plus system CPU, excluding the Valkey server.
Allocation counters cover the consumer and synthetic source. Heap bytes are the
process heap at the end of the offered window, before drain, without forcing GC;
use the separate retained-heap experiment for reachability conclusions.

The source excludes AWS network, Kinesis throttling, SDK decoding and downstream
application costs. Real LocalStack/Kinesis recovery and resharding are exercised
by the [correctness tests](validation-matrix.md). In particular, synthetic
single-shard results at 5,785 records/second are not Kinesis service capacity
claims.

## Reproduce

The load and ownership harnesses support Linux and macOS. Use an isolated Valkey instance. The disposable Compose service can be configured
with the documented conservative persistence profile:

```sh
docker compose -f test/integration/docker-compose.yml up -d --wait
docker exec integration-valkey-1 valkey-cli CONFIG SET maxmemory-policy noeviction appendonly yes appendfsync always
KCG_LOAD=1 KCG_LOAD_DURATION=5s KCG_LOAD_RESULTS=/tmp/load.jsonl \
  go test -tags integration ./test/integration -run '^TestProcessingLoad$' \
  -count=1 -timeout 1200s -v
KCG_LOAD=1 go test -tags integration ./test/integration -run '^$' \
  -bench '^BenchmarkOwnershipValidation$' -benchmem -benchtime=1s
KCG_HEAP_PROFILE_DIR=/tmp/kcg-heap go test ./pkg/consumer \
  -run '^TestExplicitRetainedHeap' -count=1 -memprofilerate=1 -v
GOTOOLCHAIN=go1.26.6 go tool pprof -top -inuse_space /tmp/kcg-heap/gap-retry-true-512.pprof
docker compose -f test/integration/docker-compose.yml down -v
```

The JSONL results file is appended to. Use a new path for each run. The full
matrix has 120 cases and takes about ten minutes plus drain time. Run it without
other test suites or benchmarks competing for CPU. `KCG_LOAD_DURATION` accepts
durations of at least one second. `VALKEY_ADDR` selects another test server.
Normal unit/integration gates skip the load matrix unless `KCG_LOAD=1` is set.

## Retained payloads

The gap experiment keeps both the processor and an application-owned early
handle reachable. It allocates 256 KiB payloads through completed callbacks,
first 64 records and then 512, leaving record 1 unacknowledged. A second variant
retries a partial batch after acknowledging its suffix. Two full collections at
each boundary measure reachable memory. The test permits 8 MiB of runtime noise,
well below the shorter 16 MiB allocated suffix, and verifies that the final Ack
advances the contiguous prefix through all 512 records.

The staging experiment holds a 32-record page with 1 MiB payloads behind a
one-record admission budget. It checks that the staged payloads are actually
reachable before stop, then cancels the processor, joins its work, collects,
and verifies release while keeping the stopped processor reachable. It does not
retain application deliveries. Application-owned handles and callbacks that
still use payloads legitimately keep those payloads alive; the library does not
clear their data.

A public-consumer variant also allocates large buffers only in returned SDK
pages, keeps the source's stored records small, and measures a 512-record suffix
through the complete shard worker. It profiles both the live gap and the stopped
consumer while retaining the application's early handle.

## Results

[Measured processing results](performance-results.md) include machine details,
throughput and backlog tables, baseline Ack/cost comparisons, and all 120 cases
as CSV. Without injected latency, every tested combination kept up with at least
99% of offered load through 5×. Adding 1 ms per backend operation exposed growing
backlog in serialized acknowledgment workloads.

The isolated ownership benchmark measured 41.75 ns/op with zero allocations for
memory and 145.68 µs/op with 422 bytes and four allocations for Valkey. See the
[benchmark output](performance-data/ownership.txt). This excludes the surrounding
consumer and application work measured in the load matrix.

With allocation sampling set to one byte, the retained-heap run measured:

| Scenario | Payload allocated | Retained delta after collection |
| --- | ---: | ---: |
| 64 records with early gap | 16 MiB | 267 KiB |
| 512 records with early gap | 128 MiB | 275 KiB |
| 64 records with partial retry | 16 MiB | 259 KiB |
| 512 records with partial retry | 128 MiB | 260 KiB |
| Staged page, before stop | 32 MiB | 31.0 MiB |
| Same processor, after stop | 32 MiB | 1.5 KiB |
| Public consumer, 512-record gap | 128 MiB | 297 KiB |
| Same consumer, after stop | 128 MiB | 278 KiB |

The gap profiles retain one 256 KiB payload owned by the application's early
handle, not the completed suffix. The stopped staging profile retains none of
its large buffers. The public consumer still has that early application handle
reachable after stop, so its 256 KiB buffer is expected to remain. Heap deltas
also include runtime and test metadata.

[Raw heap measurements](performance-data/retained-heap.txt) and
[pprof summaries](performance-data/heap-profile-summary.txt) retain the evidence.
Profiles were generated by Go 1.26.3 and inspected with Go 1.26.6. The commands
above regenerate the binary profiles for further inspection.
