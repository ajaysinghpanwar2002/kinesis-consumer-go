package consumer

import "github.com/pratilipi/kinesis-consumer-go/pkg/metrics"

// Metric names are dot-delimited statsd-style names under a single prefix.
// Downstream pipelines map dots to underscores, so these land in InfluxDB as
// measurements like kinesis_consumer_records_processed; dashboards depend on
// these exact names.
const (
	metricRecordsProcessed    = "kinesis_consumer.records_processed"
	metricPagesFetched        = "kinesis_consumer.pages_fetched"
	metricRecordsSkipped      = "kinesis_consumer.records_skipped"
	metricDLQRecordsPublished = "kinesis_consumer.dlq_records_published"
	metricHandlerRetries      = "kinesis_consumer.handler_retries"
	metricCheckpointsSaved    = "kinesis_consumer.checkpoints_saved"
	metricCheckpointFailures  = "kinesis_consumer.checkpoint_failures"
)

// Tag keys are stable, low-cardinality dimensions: stream and shard are
// bounded identifiers; handler and policy are enums. Never tag with sequence
// numbers, owners of unbounded sets, or error strings.
const (
	metricTagStream  = "stream"
	metricTagShard   = "shard"
	metricTagHandler = "handler"
	metricTagPolicy  = "policy"
)

// shardTags builds the standard stream+shard tag set, appending any extra
// enum-valued tags, so every emission site shares one definition.
func (c *Consumer) shardTags(shardID string, extra ...metrics.Tag) []metrics.Tag {
	tags := make([]metrics.Tag, 0, 2+len(extra))
	tags = append(tags, metrics.Tag{Key: metricTagStream, Value: c.streamKey()})
	tags = append(tags, metrics.Tag{Key: metricTagShard, Value: shardID})
	return append(tags, extra...)
}
