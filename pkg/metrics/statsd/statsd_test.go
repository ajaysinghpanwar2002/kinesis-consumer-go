package statsd

import (
	"net"
	"testing"
	"time"

	"github.com/pratilipi/kinesis-consumer-go/pkg/metrics"
)

// newTestListener returns a loopback UDP listener and a function that blocks
// until the next datagram arrives.
func newTestListener(t *testing.T) (string, func() string) {
	t.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	receive := func() string {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline() error = %v, want nil", err)
		}
		buf := make([]byte, 4096)
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			t.Fatalf("ReadFrom() error = %v, want datagram", err)
		}
		return string(buf[:n])
	}
	return conn.LocalAddr().String(), receive
}

func newTestReporter(t *testing.T) (*Reporter, func() string) {
	t.Helper()

	addr, receive := newTestListener(t)
	reporter, err := New(addr)
	if err != nil {
		t.Fatalf("New(%q) error = %v, want nil", addr, err)
	}
	t.Cleanup(func() { _ = reporter.Close() })
	return reporter, receive
}

func TestNewRejectsEmptyAddress(t *testing.T) {
	t.Parallel()

	reporter, err := New("")
	if err == nil || err.Error() != "statsd address cannot be empty" {
		t.Fatalf("New(\"\") error = %v, want %q", err, "statsd address cannot be empty")
	}
	if reporter != nil {
		t.Fatalf("New(\"\") reporter = %v, want nil", reporter)
	}
}

func TestNewRejectsUnresolvableAddress(t *testing.T) {
	t.Parallel()

	reporter, err := New("not a host:port")
	if err == nil {
		t.Fatal("New() error = nil, want dial error")
	}
	if reporter != nil {
		t.Fatalf("New() reporter = %v, want nil", reporter)
	}
}

func TestCounterDatagram(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)
	reporter.Counter("kinesis_consumer.records_processed", 3, []metrics.Tag{
		{Key: "stream", Value: "stream"},
		{Key: "shard", Value: "shard-1"},
		{Key: "handler", Value: "batch"},
	})

	want := "kinesis_consumer.records_processed:3|c|#stream:stream,shard:shard-1,handler:batch"
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestCounterDatagramWithoutTags(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)
	reporter.Counter("kinesis_consumer.records_processed", 1, nil)

	want := "kinesis_consumer.records_processed:1|c"
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestGaugeDatagram(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)
	reporter.Gauge("kinesis_consumer.millis_behind_latest", 1500.5, []metrics.Tag{
		{Key: "stream", Value: "stream"},
	})

	want := "kinesis_consumer.millis_behind_latest:1500.5|g|#stream:stream"
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestTimingDatagramEmitsMilliseconds(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)
	reporter.Timing("kinesis_consumer.handler_duration", 1500*time.Microsecond, []metrics.Tag{
		{Key: "stream", Value: "stream"},
	})

	want := "kinesis_consumer.handler_duration:1.5|ms|#stream:stream"
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestHistogramDatagram(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)
	reporter.Histogram("kinesis_consumer.example", 0.25, nil)

	want := "kinesis_consumer.example:0.25|h"
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestTagValuesKeepColons(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:kinesis:us-east-1:111111111111:stream/test"
	reporter, receive := newTestReporter(t)
	reporter.Counter("kinesis_consumer.records_processed", 1, []metrics.Tag{
		{Key: "stream", Value: streamARN},
	})

	want := "kinesis_consumer.records_processed:1|c|#stream:" + streamARN
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestTagValuesSanitizeFramingCharacters(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)
	reporter.Counter("kinesis_consumer.records_processed", 1, []metrics.Tag{
		{Key: "bad,key|here", Value: "bad,value|with#framing\nchars"},
	})

	want := "kinesis_consumer.records_processed:1|c|#bad_key_here:bad_value_with_framing_chars"
	if got := receive(); got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

func TestEmitAfterCloseDoesNotPanic(t *testing.T) {
	t.Parallel()

	addr, _ := newTestListener(t)
	reporter, err := New(addr)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	if err := reporter.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	// Fire-and-forget: writes on a closed socket are silently dropped.
	reporter.Counter("kinesis_consumer.records_processed", 1, nil)
	reporter.Gauge("kinesis_consumer.owned_shards", 1, nil)
	reporter.Timing("kinesis_consumer.handler_duration", time.Millisecond, nil)
	reporter.Histogram("kinesis_consumer.example", 1, nil)
}

func TestConcurrentEmissionsDeliverWholeDatagrams(t *testing.T) {
	t.Parallel()

	reporter, receive := newTestReporter(t)

	const emitters = 8
	done := make(chan struct{})
	for i := 0; i < emitters; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			reporter.Counter("kinesis_consumer.records_processed", 1, []metrics.Tag{
				{Key: "stream", Value: "stream"},
			})
		}()
	}
	for i := 0; i < emitters; i++ {
		<-done
	}

	want := "kinesis_consumer.records_processed:1|c|#stream:stream"
	for i := 0; i < emitters; i++ {
		if got := receive(); got != want {
			t.Fatalf("datagram %d = %q, want %q", i, got, want)
		}
	}
}
