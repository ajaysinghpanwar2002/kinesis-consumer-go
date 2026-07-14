# Metrics

`kinesis-consumer-go` exposes opt-in consumer metrics through the small
`metrics.Reporter` interface. The default is `metrics.Nop{}`, so constructing a
consumer without `WithMetrics` emits nothing and requires no metrics
dependency.

The repository includes a dependency-free UDP statsd reporter and a complete
pipeline for Telegraf, InfluxDB, and Grafana. Applications can instead provide
their own reporter without coupling the consumer to a particular monitoring
system.

## Enable metrics

Create the reporter, keep it alive for the consumer's lifetime, and pass it to
`consumer.New`:

```go
import (
	"github.com/pratilipi/kinesis-consumer-go/pkg/consumer"
	metricstatsd "github.com/pratilipi/kinesis-consumer-go/pkg/metrics/statsd"
)

reporter, err := metricstatsd.New("localhost:8125")
if err != nil {
	return err
}
defer reporter.Close()

cons, err := consumer.New(cfg, kinesisClient, store, handler,
	consumer.WithMetrics(reporter),
)
```

`WithMetrics(nil)` is rejected with `metrics reporter cannot be nil`. Omitting
the option retains the silent no-op default.

The statsd reporter is safe for concurrent use and writes one UDP datagram per
emission. Runtime send failures are deliberately dropped: observability must
not block or fail record processing. UDP delivery is therefore best-effort.
`New` can still fail while validating/dialing the address, and `Close` returns
the socket close error.

### Custom reporters

Implement `metrics.Reporter` to send the same catalog elsewhere:

```go
type Reporter interface {
	Counter(name string, value int64, tags []metrics.Tag)
	Gauge(name string, value float64, tags []metrics.Tag)
	Timing(name string, value time.Duration, tags []metrics.Tag)
}
```

Calls can arrive concurrently from multiple shard workers and run inline with
consumer work. A custom implementation should therefore be concurrency-safe,
return quickly, and handle its own buffering or error reporting.

## Naming and wire format

All consumer names are dot-delimited under the stable
`kinesis_consumer.` prefix. The in-repo reporter emits standard statsd types
with Datadog-style tags, for example:

```text
kinesis_consumer.records_processed:1|c|#stream:orders,shard:shardId-000000000001,handler:record
kinesis_consumer.millis_behind_latest:1500|g|#stream:orders,shard:shardId-000000000001
kinesis_consumer.handler_duration:12.5|ms|#stream:orders,shard:shardId-000000000001,handler:record
```

The shipped [Telegraf sample](../dashboards/telegraf/telegraf.sample.conf) sets
`datadog_extensions = true` and `metric_separator = "_"`. Telegraf consequently
writes the first example to InfluxDB as the measurement
`kinesis_consumer_records_processed`, with `stream`, `shard`, and `handler` as
InfluxDB tags. The [Grafana dashboard](../dashboards/README.md) depends on those
underscore-normalized measurement names; changing `metric_separator` requires
changing its queries too.

With the sample configuration:

- counters and gauges use the InfluxDB field `value`;
- statsd timings are sent in milliseconds and Telegraf writes aggregate fields
  including `count`, `lower`, `upper`, `mean`, `median`, `stddev`, and
  `percentile_50`, `percentile_90`, `percentile_95`, and `percentile_99`;
- counters and timing aggregates reset on each Telegraf flush, so counter
  points are interval deltas rather than process-lifetime totals.

Metric names are not sanitized. Statsd tag keys and values replace commas,
pipes, hashes, and newlines with `_` so they cannot corrupt datagram framing;
colons are retained so stream ARNs remain readable.

## Tags

Every emitted metric has a `stream` tag. Shard-scoped metrics also have
`shard`; the remaining tags are fixed enums used only where shown in the
catalog.

| Tag | Values and meaning |
| --- | --- |
| `stream` | `Config.StreamName`, or `Config.StreamARN` when the consumer is configured by ARN |
| `shard` | Kinesis shard ID for the operation or worker |
| `handler` | `record` or `batch` |
| `policy` | `skip` (currently used only by `records_skipped`) |
| `kind` | `acquire`, `claim`, or `shed` for rebalance outcomes; `throttle`, `expired`, or `other` for GetRecords failures |
| `outcome` | `clean` or `error` for worker stops |

Owner and donor identifiers are deliberately not tags. Default owner IDs
contain a process ID and nanosecond timestamp, so retaining them would create
an unbounded number of InfluxDB series.

## Counter catalog

Counter values are deltas. Most emissions add one; record-oriented counters
can add the number of records in a successfully handled, skipped, or published
batch.

| Statsd name | Value/unit | Tags | Emitted when |
| --- | --- | --- | --- |
| `kinesis_consumer.records_processed` | records | `stream`, `shard`, `handler` | A record handler succeeds, or a batch handler succeeds (adds the batch size). Records handled by skip or DLQ policy are excluded. |
| `kinesis_consumer.pages_fetched` | pages | `stream`, `shard` | A `GetRecords` call succeeds, including an empty page. |
| `kinesis_consumer.get_records_failures` | failures | `stream`, `shard`, `kind` | A `GetRecords` call fails, counted per attempt: `throttle` (throughput/limit/KMS throttling), `expired` (expired iterator, recovered in place), or `other` (server faults, network errors, and fatal client errors). Shutdown cancellation is not counted. Pair with `pages_fetched` for a failure ratio. |
| `kinesis_consumer.records_skipped` | records | `stream`, `shard`, `policy=skip` | Handler retries are exhausted and skip policy accepts the record or page (adds the page size in batch mode). |
| `kinesis_consumer.dlq_records_published` | records | `stream`, `shard` | One poison record is successfully published. A failed publish is not counted. |
| `kinesis_consumer.handler_retries` | retry attempts | `stream`, `shard`, `handler` | A handler attempt after the first is about to run. |
| `kinesis_consumer.checkpoints_saved` | checkpoints | `stream`, `shard` | A regular, catch-up, drain, or shard-completion checkpoint save succeeds. |
| `kinesis_consumer.checkpoint_failures` | failures | `stream`, `shard` | Any regular or shard-completion checkpoint save fails. |
| `kinesis_consumer.lease_acquired` | leases | `stream`, `shard` | The ready-shard acquisition path used at startup and shard refresh obtains a lease. Rebalance acquisitions and claims are represented by `rebalance_moves`. |
| `kinesis_consumer.lease_released` | leases | `stream`, `shard` | A worker's bounded shutdown release succeeds. |
| `kinesis_consumer.lease_release_failures` | failures | `stream`, `shard` | A worker's bounded shutdown release fails or times out. |
| `kinesis_consumer.lease_renewals` | renewals | `stream`, `shard` | A scheduled lease renewal succeeds. |
| `kinesis_consumer.lease_renewal_failures` | failures | `stream`, `shard` | A scheduled renewal fails for a reason other than shutdown cancellation. Shutdown cancellation counts neither success nor failure. |
| `kinesis_consumer.heartbeat_failures` | failures | `stream` | A worker-liveness heartbeat send fails (live context). Sustained failures mean peers will treat this worker as dead and claim its shards away. |
| `kinesis_consumer.rebalance_pass_failures` | failures | `stream` | A rebalance pass returns an error and is skipped until the next tick (live context). Shard-sync failures are not counted here — they stop the consumer. |
| `kinesis_consumer.rebalance_moves` | moves | `stream`, `shard`, `kind` | An unowned shard is acquired, a donor shard is claimed, or a local worker is selected and stopped for shedding. |
| `kinesis_consumer.rebalance_skips` | skips | `stream`, `shard`, `kind` | A planned `acquire` or `claim` does not obtain a lease, typically because ownership changed concurrently. There is no `shed` skip emission. |
| `kinesis_consumer.shards_completed` | shards | `stream`, `shard` | A closed shard's `SHARD_END` completion checkpoint is saved successfully. |
| `kinesis_consumer.worker_starts` | workers | `stream`, `shard` | A registered shard-worker goroutine starts. |
| `kinesis_consumer.worker_stops` | workers | `stream`, `shard`, `outcome` | A shard worker returns; `clean` means nil and `error` means its processing, renewal, or release path returned an error. |

## Gauge catalog

Gauges are point-in-time observations. The four ownership gauges are emitted
from a successfully built rebalance-plan snapshot, before that pass executes
its moves.

| Statsd name | Value/unit | Tags | Emitted when |
| --- | --- | --- | --- |
| `kinesis_consumer.owned_shards` | shard count | `stream` | The rebalance snapshot records how many ready shards the local owner initially holds. |
| `kinesis_consumer.active_workers` | worker count | `stream` | The rebalance snapshot records the active consumer workers used for fair-share planning. |
| `kinesis_consumer.fair_share_low` | shard count | `stream` | The rebalance snapshot computes the minimum fair shard share per active worker. |
| `kinesis_consumer.fair_share_high` | shard count | `stream` | The rebalance snapshot computes the maximum fair shard share per active worker. |
| `kinesis_consumer.millis_behind_latest` | milliseconds | `stream`, `shard` | A successful `GetRecords` response includes AWS's `MillisBehindLatest` value. |

## Timing catalog

The consumer passes `time.Duration` values to `Reporter.Timing`. The in-repo
statsd reporter converts them to fractional milliseconds for the statsd `ms`
type.

| Statsd name | Tags | Outcomes included |
| --- | --- | --- |
| `kinesis_consumer.handler_duration` | `stream`, `shard`, `handler` | Every handler attempt, whether it succeeds, fails, or returns a context error. Retries are separate samples. |
| `kinesis_consumer.get_records_duration` | `stream`, `shard` | Successful `GetRecords` calls only, including empty pages. Failed and expired-iterator calls are excluded. |
| `kinesis_consumer.checkpoint_save_duration` | `stream`, `shard` | Successful regular and shard-completion saves only. |
| `kinesis_consumer.lease_acquire_duration` | `stream`, `shard` | Successful lease-manager `Acquire` calls, whether or not they obtain the lease. Failed calls and `Claim` calls are excluded. |
| `kinesis_consumer.rebalance_pass_duration` | `stream` | Every rebalance pass, including early returns and errors. |
| `kinesis_consumer.drain_duration` | `stream` | Every enabled graceful-drain wait, including clean, worker-error, and timeout outcomes. |

## Telegraf, InfluxDB, and Grafana

The packaged path is:

```text
consumer -> UDP statsd -> Telegraf -> InfluxDB -> Grafana
```

1. Wire `pkg/metrics/statsd` with `WithMetrics` as shown above.
2. Configure and run Telegraf from
   [`dashboards/telegraf/telegraf.sample.conf`](../dashboards/telegraf/telegraf.sample.conf).
   Its active output targets InfluxDB v2; the same file contains an alternative
   InfluxDB v1 output block.
3. Follow [`dashboards/README.md`](../dashboards/README.md) to import or
   provision the InfluxQL dashboard. InfluxDB v1 supports InfluxQL natively;
   InfluxDB v2 needs the documented one-time DBRP mapping.

The Telegraf and datasource URLs, credentials, organization, bucket/database,
DBRP mapping, and deployment mounts are operator-owned placeholders rather
than library defaults.

## Cardinality guidance

Per-shard tags are intentional: they make lag, checkpoint, lease, worker, and
rebalance problems diagnosable. They also create at least one series per shard
for every shard-scoped metric and additional series for bounded enum tags.

For streams with very large shard counts, monitor InfluxDB series cardinality
and retention, and narrow the dashboard's shard selector during investigation.
Do not add sequence numbers, record keys, error text, process owner IDs, donor
IDs, or other unbounded values as tags in custom reporters.
