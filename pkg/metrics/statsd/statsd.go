// Package statsd provides a dependency-free UDP statsd implementation of
// metrics.Reporter.
//
// Datagrams use the Datadog tag extension (`name:value|type|#key:v,key2:v2`),
// which Telegraf's statsd input parses when `datadog_extensions = true`. With
// Telegraf's default `metric_separator = "_"`, the library's dot-delimited
// names (e.g. `kinesis_consumer.records_processed`) land in InfluxDB as
// measurements like `kinesis_consumer_records_processed`.
//
// Emission is fire-and-forget, statsd-style: runtime send failures are
// silently dropped and never block or fail consumer processing.
package statsd

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/metrics"
)

// Reporter emits metrics as statsd datagrams over UDP.
//
// It is safe for concurrent use: every emission writes one datagram through
// the shared connection and keeps no mutable state.
type Reporter struct {
	conn net.Conn
}

var _ metrics.Reporter = (*Reporter)(nil)

// New dials the statsd UDP endpoint (e.g. "localhost:8125") and returns a
// ready Reporter. The only errors are an empty address or a failed dial;
// after that, sends never report errors.
func New(addr string) (*Reporter, error) {
	if addr == "" {
		return nil, errors.New("statsd address cannot be empty")
	}
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial statsd %s: %w", addr, err)
	}
	return &Reporter{conn: conn}, nil
}

// Close releases the underlying socket. Emissions after Close are dropped.
func (r *Reporter) Close() error {
	return r.conn.Close()
}

func (r *Reporter) Counter(name string, value int64, tags []metrics.Tag) {
	r.send(name, strconv.FormatInt(value, 10), "c", tags)
}

func (r *Reporter) Gauge(name string, value float64, tags []metrics.Tag) {
	r.send(name, formatFloat(value), "g", tags)
}

// Timing emits the duration in milliseconds, the statsd `ms` convention.
func (r *Reporter) Timing(name string, value time.Duration, tags []metrics.Tag) {
	r.send(name, formatFloat(float64(value)/float64(time.Millisecond)), "ms", tags)
}

func (r *Reporter) send(name, value, metricType string, tags []metrics.Tag) {
	var datagram strings.Builder
	datagram.WriteString(name)
	datagram.WriteByte(':')
	datagram.WriteString(value)
	datagram.WriteByte('|')
	datagram.WriteString(metricType)
	writeTags(&datagram, tags)

	// Fire-and-forget: a full buffer, closed socket, or unreachable collector
	// must never affect consumer processing.
	_, _ = r.conn.Write([]byte(datagram.String()))
}

func writeTags(datagram *strings.Builder, tags []metrics.Tag) {
	if len(tags) == 0 {
		return
	}
	datagram.WriteString("|#")
	for i, tag := range tags {
		if i > 0 {
			datagram.WriteByte(',')
		}
		datagram.WriteString(sanitizeTag(tag.Key))
		datagram.WriteByte(':')
		datagram.WriteString(sanitizeTag(tag.Value))
	}
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// sanitizeTag replaces characters that would corrupt datagram framing.
// Colons are allowed: tag values such as stream ARNs contain them, and the
// Datadog format splits key from value on the first colon only.
var tagSanitizer = strings.NewReplacer(
	",", "_",
	"|", "_",
	"#", "_",
	"\n", "_",
	"\r", "_",
)

func sanitizeTag(s string) string {
	return tagSanitizer.Replace(s)
}
