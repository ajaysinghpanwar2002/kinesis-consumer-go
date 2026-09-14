package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func retainedHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func retentionProfile(t *testing.T, name string) {
	t.Helper()
	if dir := os.Getenv("KCG_HEAP_PROFILE_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(dir, name+".pprof"))
		if err != nil {
			t.Fatal(err)
		}
		err = pprof.WriteHeapProfile(f)
		closeErr := f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}

// Keep the live processor and the early application handle reachable throughout
// collection. Increasing the completed suffix eightfold must not retain its
// payloads, including through retry buffers and compacted tracking ranges.
func TestExplicitRetainedHeapAcrossGap(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%v", retry), func(t *testing.T) {
			var gap Delivery
			attempt := 0
			p, _, _ := explicitTestProcessor(t, InFlightLimits{}, explicitHandlers{batch: func(ctx context.Context, ds []Delivery) error {
				attempt++
				for _, d := range ds {
					if aws.ToString(d.Record.SequenceNumber) == "1" {
						gap = d
						continue
					}
					if err := d.Ack(ctx); err != nil {
						return err
					}
				}
				if retry && attempt%2 == 1 {
					return errors.New("retry only the unfinished gap")
				}
				return nil
			}}, 1_000_000, time.Hour)
			p.c.tuning.retryMaxAttempts = 2
			p.c.tuning.retryBackoff = time.Nanosecond
			baseline := retainedHeap()
			const size = 256 << 10
			seq := 1
			for _, end := range []int{64, 512} {
				for seq <= end {
					records := make([]Record, 4)
					for i := range records {
						records[i] = Record{SequenceNumber: aws.String(fmt.Sprint(seq)), Data: make([]byte, size)}
						seq++
					}
					if err := p.processPage(context.Background(), records, nil); err != nil {
						t.Fatal(err)
					}
				}
				heap := retainedHeap()
				t.Logf("suffix=%d allocated_payload=%d retained_delta=%d", end-1, end*size, int64(heap)-int64(baseline))
				// Allow runtime noise, but substantially less than even the short suffix.
				if int64(heap)-int64(baseline) > 8<<20 {
					t.Fatal("completed suffix payloads remain reachable")
				}
				retentionProfile(t, fmt.Sprintf("gap-retry-%v-%d", retry, end))
				runtime.KeepAlive(p)
				runtime.KeepAlive(gap)
			}
			if seq, _ := p.tracker.progress(); seq != "" {
				t.Fatalf("checkpoint crossed gap: %s", seq)
			}
			if err := gap.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
			if seq, count := p.tracker.progress(); seq != "512" || count != 512 {
				t.Fatalf("progress=%s/%d", seq, count)
			}
		})
	}
}

func TestExplicitRetainedHeapStagingAndStop(t *testing.T) {
	const size = 1 << 20
	entered := make(chan struct{})
	p, _, _ := explicitTestProcessor(t, InFlightLimits{MaxRecordsPerShard: 1}, explicitHandlers{record: func(context.Context, Delivery) error { close(entered); return nil }}, 100, time.Hour)
	baseline := retainedHeap()
	done := make(chan error, 1)
	go func() {
		records := make([]Record, 32)
		for i := range records {
			records[i] = Record{SequenceNumber: aws.String(fmt.Sprint(i + 1)), Data: make([]byte, size)}
		}
		slot, err := p.c.admission.acquire(p.ctx, "a", nil, true)
		if err == nil {
			err = p.processPage(p.ctx, records, slot)
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not run")
	}
	waitFor(t, "staging paused", func() bool { return p.c.Health().Pressure.PausedShards > 0 })
	staged := retainedHeap()
	if int64(staged)-int64(baseline) < 24<<20 {
		t.Fatal("test did not retain the staged page")
	}
	retentionProfile(t, "staged")
	p.stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stopped page succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("page did not stop")
	}
	if err := p.wait(); err != nil {
		t.Fatal(err)
	}
	stopped := retainedHeap()
	t.Logf("staged_delta=%d stopped_delta=%d", int64(staged)-int64(baseline), int64(stopped)-int64(baseline))
	if int64(stopped)-int64(baseline) > 8<<20 {
		t.Fatal("stopped worker retains staged payloads")
	}
	retentionProfile(t, "stopped")
	runtime.KeepAlive(p)
}

// The stream retains only tiny source records; large buffers belong exclusively
// to returned SDK pages and the consumer/application paths being measured.
type heapStream struct {
	*fakeStream
	idle chan struct{}
	once sync.Once
}

func (s *heapStream) GetRecords(ctx context.Context, in *kinesis.GetRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	out, err := s.fakeStream.GetRecords(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	if len(out.Records) > 0 {
		out.MillisBehindLatest = aws.Int64(1)
	}
	for i := range out.Records {
		out.Records[i].Data = make([]byte, 256<<10)
	}
	if len(out.Records) == 0 {
		s.once.Do(func() { close(s.idle) })
	}
	return out, nil
}

func TestExplicitRetainedHeapWorker(t *testing.T) {
	stream := &heapStream{fakeStream: newFakeStream(testShardID, sequences(1, 512)...), idle: make(chan struct{})}
	store, manager := fencedBackends()
	gaps := make(chan Delivery, 1)
	c := newTestConsumerWithConfig(t, trimHorizonConfig(), stream, store, manager, nil,
		WithBatching(4, 1000), WithPolling(50*time.Millisecond, time.Second),
		WithExplicitHandler(func(ctx context.Context, d Delivery) error {
			if aws.ToString(d.Record.SequenceNumber) == "1" {
				gaps <- d
				return nil
			}
			return d.Ack(ctx)
		}))
	defer c.Close()
	baseline := retainedHeap()
	stop := startConsumer(t, c)
	defer stop()
	select {
	case <-stream.idle:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not finish suffix")
	}
	gap := <-gaps
	live := retainedHeap()
	if int64(live)-int64(baseline) > 8<<20 {
		t.Fatal("worker retains completed SDK page payloads")
	}
	retentionProfile(t, "worker-gap")
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	stopped := retainedHeap()
	t.Logf("worker allocated_payload=%d live_delta=%d stopped_delta=%d", 512*(256<<10), int64(live)-int64(baseline), int64(stopped)-int64(baseline))
	if int64(stopped)-int64(baseline) > 8<<20 {
		t.Fatal("stopped worker retains SDK page payloads")
	}
	retentionProfile(t, "worker-stopped")
	runtime.KeepAlive(c)
	runtime.KeepAlive(gap)
}
