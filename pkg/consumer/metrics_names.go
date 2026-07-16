package consumer

import "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"

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
	metricGetRecordsFailures  = "kinesis_consumer.get_records_failures"

	metricLeaseAcquired         = "kinesis_consumer.lease_acquired"
	metricLeaseReleased         = "kinesis_consumer.lease_released"
	metricLeaseReleaseFailures  = "kinesis_consumer.lease_release_failures"
	metricLeaseRenewals         = "kinesis_consumer.lease_renewals"
	metricLeaseRenewalFailures  = "kinesis_consumer.lease_renewal_failures"
	metricLeaseLost             = "kinesis_consumer.lease_lost"
	metricHeartbeatFailures     = "kinesis_consumer.heartbeat_failures"
	metricRebalanceMoves        = "kinesis_consumer.rebalance_moves"
	metricRebalanceSkips        = "kinesis_consumer.rebalance_skips"
	metricRebalancePassFailures = "kinesis_consumer.rebalance_pass_failures"
	metricShardsCompleted       = "kinesis_consumer.shards_completed"
	metricWorkerStarts          = "kinesis_consumer.worker_starts"
	metricWorkerStops           = "kinesis_consumer.worker_stops"

	metricOwnedShards        = "kinesis_consumer.owned_shards"
	metricActiveWorkers      = "kinesis_consumer.active_workers"
	metricFairShareLow       = "kinesis_consumer.fair_share_low"
	metricFairShareHigh      = "kinesis_consumer.fair_share_high"
	metricMillisBehindLatest = "kinesis_consumer.millis_behind_latest"

	metricHandlerDuration        = "kinesis_consumer.handler_duration"
	metricGetRecordsDuration     = "kinesis_consumer.get_records_duration"
	metricCheckpointSaveDuration = "kinesis_consumer.checkpoint_save_duration"
	metricLeaseAcquireDuration   = "kinesis_consumer.lease_acquire_duration"
	metricRebalancePassDuration  = "kinesis_consumer.rebalance_pass_duration"
	metricDrainDuration          = "kinesis_consumer.drain_duration"
)

// Tag keys are stable, low-cardinality dimensions: stream and shard are
// bounded identifiers; handler and policy are enums. Never tag with sequence
// numbers, owners of unbounded sets, or error strings.
const (
	metricTagStream        = "stream"
	metricTagConsumerGroup = "consumer_group"
	metricTagShard         = "shard"
	metricTagHandler       = "handler"
	metricTagPolicy        = "policy"
	metricTagKind          = "kind"
	metricTagOutcome       = "outcome"
)

// rebalance_moves / rebalance_skips kind tag values.
const (
	metricKindAcquire = "acquire"
	metricKindClaim   = "claim"
	metricKindShed    = "shed"
)

// get_records_failures kind tag values.
const (
	metricKindThrottle = "throttle"
	metricKindExpired  = "expired"
	metricKindOther    = "other"
)

// worker_stops outcome tag values.
const (
	metricOutcomeClean = "clean"
	metricOutcomeError = "error"
)

// streamTags builds the stream-only tag set for consumer-level metrics.
func (c *Consumer) streamTags() []metrics.Tag {
	tags := []metrics.Tag{{Key: metricTagStream, Value: c.canonicalStreamName()}}
	if c.cfg.ConsumerGroup != "" {
		tags = append(tags, metrics.Tag{Key: metricTagConsumerGroup, Value: c.cfg.ConsumerGroup})
	}
	return tags
}

// shardTags builds the standard stream+shard tag set, appending any extra
// enum-valued tags, so every emission site shares one definition.
func (c *Consumer) shardTags(shardID string, extra ...metrics.Tag) []metrics.Tag {
	tags := c.streamTags()
	tags = append(tags, metrics.Tag{Key: metricTagShard, Value: shardID})
	return append(tags, extra...)
}
