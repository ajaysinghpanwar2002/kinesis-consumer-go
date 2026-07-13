# Grafana dashboards

This directory packages the observability pipeline for `kinesis-consumer-go`:

```text
consumer -> UDP statsd -> Telegraf -> InfluxDB -> Grafana
```

The [sample Telegraf config](telegraf/telegraf.sample.conf) accepts the
Datadog-style tags emitted by `pkg/metrics/statsd` and changes dots in metric
names to underscores. The dashboard therefore queries measurements such as
`kinesis_consumer_records_processed`, not the dotted statsd wire name.

## Prerequisites

1. Wire the consumer with the statsd reporter and run Telegraf using the sample
   config in this directory.
2. Confirm InfluxDB is receiving `kinesis_consumer_*` measurements.
3. Configure a Grafana InfluxDB datasource whose query language is **InfluxQL**.

The overview dashboard has an `InfluxDB datasource` selector, so it does not
embed an instance URL, database name, or credentials. It works with any
InfluxQL-configured Grafana datasource that can see the measurements.

## Import manually

1. In Grafana, open **Dashboards > New > Import**.
2. Upload [`grafana/kinesis-consumer-overview.json`](grafana/kinesis-consumer-overview.json).
3. Open the imported dashboard and select the InfluxDB datasource, stream, and
   shard at the top.

The `stream` and `shard` selectors support multiple values and **All**. A stream
does not appear until `kinesis_consumer_records_processed` has been written; a
shard does not appear until `kinesis_consumer_millis_behind_latest` has been
written. If a quiet/new consumer has only other measurements, query it directly
in Grafana Explore until these selector measurements arrive.

## Provision from files

Copy or mount the files into the paths used by Grafana:

```text
/etc/grafana/provisioning/dashboards/kinesis-consumer.yaml
/var/lib/grafana/dashboards/kinesis-consumer/kinesis-consumer-overview.json
```

Use
[`grafana/provisioning/dashboards/kinesis-consumer.yaml`](grafana/provisioning/dashboards/kinesis-consumer.yaml)
as the first file. The provider polls every 30 seconds, creates the dashboard in
the **Kinesis Consumers** folder, and treats the JSON file as authoritative.
Restart Grafana after installing the provider file. Changes saved in the UI are
disabled for this provisioned dashboard; edit the JSON source instead.

The provider intentionally does not create an InfluxDB datasource because its
URL and authentication are deployment-specific. Provision or create that
datasource separately, then select it with the dashboard's datasource selector.

## InfluxDB v1 datasource

Create a Grafana **InfluxDB** datasource with:

- Query language: `InfluxQL`
- URL: the InfluxDB v1 HTTP URL, for example `http://influxdb:8086`
- Database: the database written by Telegraf (the v1 sample uses
  `kinesis_consumer`)
- User/password: your InfluxDB v1 credentials, if authentication is enabled

Click **Save & test**, then select the datasource in the dashboard.

## InfluxDB v2 datasource and DBRP mapping

InfluxQL reaches an InfluxDB v2 bucket through a database/retention-policy
(DBRP) mapping. This is a one-time operation. The examples assume the sample
Telegraf bucket `kinesis-consumer`, expose it as the InfluxQL database
`kinesis_consumer`, and name the retention policy `autogen`.

Find the bucket ID:

```bash
influx bucket list --name kinesis-consumer
```

Create the mapping, replacing the placeholder with that ID:

```bash
influx v1 dbrp create \
  --bucket-id YOUR_BUCKET_ID \
  --db kinesis_consumer \
  --rp autogen \
  --default
```

The active `influx` CLI configuration (or `INFLUX_HOST`, `INFLUX_ORG`, and
`INFLUX_TOKEN`) must authorize this operation. Check the result with:

```bash
influx v1 dbrp list
```

Then create a Grafana **InfluxDB** datasource with:

- Query language: `InfluxQL`
- URL: the InfluxDB v2 HTTP URL, for example `http://influxdb:8086`
- Database: `kinesis_consumer` (the DB name in the mapping, not the bucket name)
- User: any non-empty string when authenticating with a v2 API token
- Password: a v2 API token with read access to the bucket
- HTTP method: `POST`

Click **Save & test**, then select the datasource in the dashboard. A v1
username/password authorization also works if your deployment uses the v1
compatibility authorization API.

## Verify and troubleshoot

From Grafana Explore or an InfluxQL client, verify the naming and fields:

```sql
SHOW MEASUREMENTS WITH MEASUREMENT =~ /^kinesis_consumer_/
SHOW TAG VALUES FROM "kinesis_consumer_records_processed" WITH KEY = "stream"
SELECT LAST("value") FROM "kinesis_consumer_owned_shards" GROUP BY "stream"
SELECT MEAN("mean"), MEAN("percentile_95")
  FROM "kinesis_consumer_handler_duration"
  WHERE time > now() - 15m
  GROUP BY time(1m), "stream", "handler" fill(null)
```

- No measurements: verify the reporter address, UDP routing, Telegraf input,
  and Telegraf output logs.
- Dotted measurement names: keep `metric_separator = "_"` in the sample
  Telegraf statsd input or update every dashboard query.
- InfluxDB v2 `database not found`: verify the DBRP database name, default
  mapping, bucket ID, and Grafana Database setting match.
- Missing timing lines: timings are emitted only when those operations occur;
  Telegraf writes the aggregate fields on its flush interval.

Counters are reset after each Telegraf flush by the sample config. Dashboard
counter panels therefore sum event deltas into each Grafana interval rather
than showing process-lifetime cumulative totals.

`millis_behind_latest` and several lifecycle metrics carry a `shard` tag. This
is useful for diagnosis but creates one series per shard. Monitor InfluxDB
series cardinality for streams with very large shard counts and narrow the
dashboard shard selection when investigating them.

