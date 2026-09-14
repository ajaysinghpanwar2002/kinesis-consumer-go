//go:build integration && (linux || darwin)

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	valkeycheckpoint "github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/backend/valkey/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/consumer"
	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/lease"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// loadSource offers records according to elapsed wall time, independently of
// consumer progress. Iterator offsets represent backlog without retaining data.
// This isolates processing/coordination cost from AWS network and SDK decoding.
type loadSource struct {
	mu                 sync.Mutex
	start              time.Time
	ready              chan struct{}
	rate, shards, size int
	duration           time.Duration
}

func (s *loadSource) ListShards(context.Context, *kinesis.ListShardsInput, ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	out := &kinesis.ListShardsOutput{}
	for i := 0; i < s.shards; i++ {
		out.Shards = append(out.Shards, types.Shard{ShardId: aws.String(strconv.Itoa(i))})
	}
	return out, nil
}
func (s *loadSource) GetShardIterator(_ context.Context, in *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	offset := 0
	if in.StartingSequenceNumber != nil {
		n, err := strconv.Atoi(*in.StartingSequenceNumber)
		if err != nil {
			return nil, err
		}
		offset = n - 1
		if in.ShardIteratorType == types.ShardIteratorTypeAfterSequenceNumber {
			offset++
		}
	}
	return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(*in.ShardId + ":" + strconv.Itoa(offset))}, nil
}
func (s *loadSource) GetRecords(ctx context.Context, in *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parts := strings.Split(*in.ShardIterator, ":")
	shard, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	offset, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.start.IsZero() {
		s.start = time.Now()
		close(s.ready)
	}
	elapsed := min(time.Since(s.start), s.duration)
	due := int(elapsed.Seconds() * float64(s.rate))
	s.mu.Unlock()
	available := max(0, (due+s.shards-1-shard)/s.shards)
	n := min(max(0, available-offset), int(aws.ToInt32(in.Limit)))
	out := &kinesis.GetRecordsOutput{NextShardIterator: aws.String(parts[0] + ":" + strconv.Itoa(offset+n)), MillisBehindLatest: aws.Int64(int64(max(0, available-offset-n)))}
	for i := 0; i < n; i++ {
		out.Records = append(out.Records, types.Record{SequenceNumber: aws.String(strconv.Itoa(offset + i + 1)), Data: make([]byte, s.size)})
	}
	return out, nil
}

type measuredLease struct {
	lease.FencedLease
	ops   *atomic.Int64
	delay time.Duration
}

func (l *measuredLease) Validate(ctx context.Context) error {
	l.ops.Add(1)
	if err := loadDelay(ctx, l.delay); err != nil {
		return err
	}
	return l.FencedLease.Validate(ctx)
}

type measuredManager struct {
	lease.Manager
	ops   *atomic.Int64
	delay time.Duration
}

func (m *measuredManager) wrap(l lease.Lease, ok bool, err error) (lease.Lease, bool, error) {
	if err != nil || !ok {
		return l, ok, err
	}
	return &measuredLease{l.(lease.FencedLease), m.ops, m.delay}, ok, nil
}
func (m *measuredManager) Acquire(ctx context.Context, a, b, c string, d time.Duration) (lease.Lease, bool, error) {
	return m.wrap(m.Manager.Acquire(ctx, a, b, c, d))
}
func (m *measuredManager) Claim(ctx context.Context, a, b, c, d string, e time.Duration) (lease.Lease, bool, error) {
	return m.wrap(m.Manager.Claim(ctx, a, b, c, d, e))
}

type measuredStore struct {
	checkpoint.FencedStore
	ops   *atomic.Int64
	delay time.Duration
}

func (s *measuredStore) Bind(ctx context.Context, a, b string, l lease.FencedLease) (checkpoint.Session, error) {
	raw := l.(*measuredLease).FencedLease
	session, err := s.FencedStore.Bind(ctx, a, b, raw)
	if err != nil {
		return nil, err
	}
	return &measuredSession{session, s.ops, s.delay}, nil
}

type measuredSession struct {
	checkpoint.Session
	ops   *atomic.Int64
	delay time.Duration
}

func (s *measuredSession) Recovery(ctx context.Context) (checkpoint.RecoveryPosition, error) {
	s.ops.Add(1)
	if err := loadDelay(ctx, s.delay); err != nil {
		return checkpoint.RecoveryPosition{}, err
	}
	return s.Session.Recovery(ctx)
}
func (s *measuredSession) Initialize(ctx context.Context, seq string) (checkpoint.RecoveryPosition, error) {
	s.ops.Add(1)
	if err := loadDelay(ctx, s.delay); err != nil {
		return checkpoint.RecoveryPosition{}, err
	}
	return s.Session.Initialize(ctx, seq)
}
func (s *measuredSession) Save(ctx context.Context, seq string) error {
	s.ops.Add(1)
	if err := loadDelay(ctx, s.delay); err != nil {
		return err
	}
	return s.Session.Save(ctx, seq)
}
func loadDelay(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func loadBackends(t testing.TB, backend string) (checkpoint.FencedStore, lease.Manager) {
	t.Helper()
	if backend == "memory" {
		m := lease.NewMemoryManager()
		return checkpoint.NewMemoryStoreWithLeaseManager(m), m
	}
	s, err := valkeycheckpoint.New(valkeyAddr(), valkeycheckpoint.WithKeyPrefix(uniqueName("load")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	m, err := s.LeaseManager()
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := m.(interface{ Close() error }); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}
	return s, m
}
func cpuSeconds() float64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func percentile(samples []int64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	return float64(samples[min(len(samples)-1, int(float64(len(samples)-1)*p))]) / 1e3
}

type loadResult struct {
	Backend, Mode                                                                                                                                    string
	Shards, Payload, OfferedRate, Completed, Backlog, MidpointBacklog                                                                                int
	Seconds, InjectedLatencyMS, Throughput, AckP50US, AckP95US, AckP99US, CoordinationOpsPerRecord, CPUSeconds, AllocBytesPerRecord, AllocsPerRecord float64
	HeapBytes                                                                                                                                        uint64
}

// Opt-in because the full matrix deliberately measures saturation and takes
// minutes. Each case runs alone; saturation is a result, not a test failure.
func TestProcessingLoad(t *testing.T) {
	if os.Getenv("KCG_LOAD") != "1" {
		t.Skip("set KCG_LOAD=1 to run processing load matrix")
	}
	duration := 3 * time.Second
	if v := os.Getenv("KCG_LOAD_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			t.Fatal("KCG_LOAD_DURATION must be >=1s")
		}
		duration = d
	}
	for _, backend := range []string{"memory", "valkey"} {
		for _, mode := range []string{"automatic", "sync", "async", "batch"} {
			for _, shards := range []int{1, 4} {
				for _, size := range []int{1024, 16384} {
					for _, rate := range []int{1157, 2314, 5785} {
						name := fmt.Sprintf("%s/%s/shards=%d/bytes=%d/rate=%d", backend, mode, shards, size, rate)
						t.Run(name, func(t *testing.T) { runProcessingLoad(t, backend, mode, shards, size, rate, 0, duration) })
					}
				}
			}
		}
	}
	for _, backend := range []string{"memory", "valkey"} {
		for _, mode := range []string{"automatic", "sync", "async", "batch"} {
			for _, rate := range []int{1157, 2314, 5785} {
				t.Run(fmt.Sprintf("latency/%s/%s/rate=%d", backend, mode, rate), func(t *testing.T) { runProcessingLoad(t, backend, mode, 4, 1024, rate, time.Millisecond, duration) })
			}
		}
	}
}
func runProcessingLoad(t *testing.T, backend, mode string, shards, size, rate int, delay, duration time.Duration) {
	store, manager := loadBackends(t, backend)
	var ops, completed atomic.Int64
	measured := &measuredStore{store, &ops, delay}
	lm := &measuredManager{manager, &ops, delay}
	source := &loadSource{ready: make(chan struct{}), rate: rate, shards: shards, size: size, duration: duration}
	var mu sync.Mutex
	samples := make([]int64, 0, int(duration.Seconds()*float64(rate)))
	ackErrors := make(chan error, 1)
	ack := func(ctx context.Context, d consumer.Delivery) error {
		start := time.Now()
		err := d.Ack(ctx)
		elapsed := time.Since(start).Nanoseconds()
		if err == nil {
			mu.Lock()
			samples = append(samples, elapsed)
			mu.Unlock()
			completed.Add(1)
		}
		return err
	}
	pending := make(chan consumer.Delivery, 10000)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	if mode == "async" {
		go func() {
			defer close(workerDone)
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					for n := len(pending); n > 0; n-- {
						d := <-pending
						if err := ack(workerCtx, d); err != nil {
							select {
							case ackErrors <- err:
							default:
							}
							return
						}
					}
				}
			}
		}()
	} else {
		close(workerDone)
	}
	defer func() { stopWorker(); <-workerDone }()
	opts := []consumer.Option{consumer.WithLeaseManager(lm), consumer.WithLogger(slog.New(slog.DiscardHandler)), consumer.WithBatching(100, 100), consumer.WithPolling(time.Millisecond, time.Second), consumer.WithIdleTimeBetweenReads(time.Millisecond), consumer.WithHeartbeat(time.Second, 30*time.Second), consumer.WithGracefulDrain(30 * time.Second)}
	var handler consumer.HandlerFunc
	switch mode {
	case "automatic":
		handler = func(context.Context, consumer.Record) error { completed.Add(1); return nil }
	case "sync":
		opts = append(opts, consumer.WithExplicitHandler(ack))
	case "async":
		opts = append(opts, consumer.WithExplicitHandler(func(ctx context.Context, d consumer.Delivery) error {
			select {
			case pending <- d:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}))
	case "batch":
		opts = append(opts, consumer.WithExplicitBatchHandler(func(ctx context.Context, ds []consumer.Delivery) error {
			for _, d := range ds {
				if err := ack(ctx, d); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	c, err := consumer.New(consumer.Config{StreamName: "load", ConsumerGroup: uniqueName("load"), StartPosition: consumer.StartTrimHorizon}, source, measured, handler, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu := cpuSeconds()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	select {
	case <-source.ready:
	case err := <-done:
		t.Fatalf("start: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("source did not start")
	}
	midpoint := time.NewTimer(duration / 2)
	select {
	case <-midpoint.C:
	case err := <-done:
		midpoint.Stop()
		t.Fatalf("consumer failed: %v", err)
	}
	midBacklog := max(0, int(duration.Seconds()*float64(rate)/2)-int(completed.Load()))
	timer := time.NewTimer(duration / 2)
	select {
	case <-timer.C:
	case err := <-done:
		timer.Stop()
		t.Fatalf("consumer failed: %v", err)
	case err := <-ackErrors:
		timer.Stop()
		t.Fatalf("ack failed: %v", err)
	}
	// Freeze counters before drain so drain throughput cannot hide a backlog.
	count := completed.Load()
	opCount := ops.Load()
	cpu = cpuSeconds() - cpu
	runtime.ReadMemStats(&after)
	mu.Lock()
	latencies := append([]int64(nil), samples...)
	mu.Unlock()
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("drain timed out")
	}
	if count == 0 {
		t.Fatal("no completed records")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result := loadResult{MidpointBacklog: midBacklog, Backend: backend, Mode: mode, Shards: shards, Payload: size, OfferedRate: rate, Completed: int(count), Backlog: max(0, int(duration.Seconds()*float64(rate))-int(count)), Seconds: duration.Seconds(), InjectedLatencyMS: float64(delay) / float64(time.Millisecond), Throughput: float64(count) / duration.Seconds(), AckP50US: percentile(latencies, .5), AckP95US: percentile(latencies, .95), AckP99US: percentile(latencies, .99), CoordinationOpsPerRecord: float64(opCount) / float64(count), CPUSeconds: cpu, AllocBytesPerRecord: float64(after.TotalAlloc-before.TotalAlloc) / float64(count), AllocsPerRecord: float64(after.Mallocs-before.Mallocs) / float64(count), HeapBytes: after.HeapAlloc}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	if path := os.Getenv("KCG_LOAD_RESULTS"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(append(data, '\n'))
		closeErr := f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}

// BenchmarkOwnershipValidation isolates backend validation from fetching,
// callbacks, admission, acknowledgment tracking, and checkpoint work.
func BenchmarkOwnershipValidation(b *testing.B) {
	if os.Getenv("KCG_LOAD") != "1" {
		b.Skip("set KCG_LOAD=1 with real Valkey available")
	}
	for _, backend := range []string{"memory", "valkey"} {
		b.Run(backend, func(b *testing.B) {
			_, manager := loadBackends(b, backend)
			ctx := context.Background()
			held, ok, err := manager.Acquire(ctx, uniqueName("validation"), "shard", "owner", 10*time.Minute)
			if err != nil || !ok {
				b.Fatalf("acquire: %v, %v", ok, err)
			}
			b.Cleanup(func() { _ = held.Release(ctx) })
			fenced := held.(lease.FencedLease)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := fenced.Validate(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
