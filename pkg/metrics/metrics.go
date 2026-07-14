package metrics

import "time"

// Tag is a stable, low-cardinality metric dimension.
type Tag struct {
	Key   string
	Value string
}

// Reporter emits consumer metrics.
type Reporter interface {
	Counter(name string, value int64, tags []Tag)
	Gauge(name string, value float64, tags []Tag)
	Timing(name string, value time.Duration, tags []Tag)
}

// Nop is a no-op metrics reporter.
type Nop struct{}

func (Nop) Counter(string, int64, []Tag)        {}
func (Nop) Gauge(string, float64, []Tag)        {}
func (Nop) Timing(string, time.Duration, []Tag) {}

var _ Reporter = Nop{}
