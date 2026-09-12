package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ajaysinghpanwar2002/kinesis-consumer-go/pkg/checkpoint"
)

type admissionCheckpointSession struct {
	checkpoint.Session
	save func(context.Context, string) error
}

func (s *admissionCheckpointSession) Save(ctx context.Context, sequence string) error {
	return s.save(ctx, sequence)
}

func TestBoundedRecoverySlotWaitFlushesCompletedProgress(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		t.Run(map[bool]string{false: "persist before wait", true: "propagate checkpoint failure"}[failSave], func(t *testing.T) {
			stream := newFakeStream(testShardID, "100", "101")
			stream.expireAfterRead = 1
			f := newFencedFixture(t, stream, WithBatching(1, 100), WithInFlightLimits(InFlightLimits{MaxFetchSlots: 1}))
			defer f.held.Release(context.Background())
			f.consumer.cfg.StartPosition = StartTrimHorizon
			ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
			defer cancel()

			saved := make(chan string, 1)
			saveErr := errors.New("checkpoint unavailable")
			backend := f.session.session
			f.session.session = &admissionCheckpointSession{Session: backend, save: func(saveCtx context.Context, sequence string) error {
				if saveCtx != ctx {
					t.Error("checkpoint did not use the live worker context")
				}
				if failSave {
					return saveErr
				}
				if err := backend.Save(saveCtx, sequence); err != nil {
					return err
				}
				saved <- sequence
				return nil
			}}

			expiredRead, releaseRead := make(chan struct{}), make(chan struct{})
			stream.onGetRecords = func(n int) {
				if n == 2 {
					close(expiredRead)
					select {
					case <-releaseRead:
					case <-ctx.Done():
					}
				}
			}
			type passResult struct {
				sequence string
				count    int
				err      error
			}
			done := make(chan passResult, 1)
			go func() {
				seq, count, _, err := f.consumer.processShardRecordsPass(ctx, testShardID, 0, "")
				done <- passResult{seq, count, err}
			}()
			select {
			case <-expiredRead:
			case <-ctx.Done():
				t.Fatal("expired read was never attempted")
			}

			// Queue the competing shard while the expiring read owns the only slot.
			// Releasing that read grants the competitor the slot before recovery asks
			// for it, forcing recovery to wait with an uncheckpointed completed record.
			competitor := make(chan *admissionReservation, 1)
			go func() { r, _ := f.consumer.admission.acquire(ctx, "other", nil, true); competitor <- r }()
			waitFor(t, "competing fetch queued", func() bool { _, _, n := admissionState(f.consumer.admission); return n == 1 })
			close(releaseRead)
			other := <-competitor
			if other == nil {
				t.Fatal("competitor did not acquire the released slot")
			}
			defer other.release()

			if !failSave {
				select {
				case sequence := <-saved:
					if sequence != "100" {
						t.Fatalf("checkpoint = %s", sequence)
					}
				case <-ctx.Done():
					t.Fatal("checkpoint stalled behind recovery fetch-slot wait")
				}
				position, err := backend.Recovery(ctx)
				if err != nil || position.Kind != checkpoint.RecoveryCheckpoint || position.Sequence != "100" {
					t.Fatalf("recovery = %+v, %v", position, err)
				}
				cancel()
			}
			var result passResult
			select {
			case result = <-done:
			case <-ctx.Done():
				if failSave {
					t.Fatal("checkpoint failure did not stop the recovery wait")
				}
				select {
				case result = <-done:
				case <-time.After(time.Second):
					t.Fatal("cancellation did not stop the recovery wait")
				}
			}
			wantErr, wantCount := error(context.Canceled), 0
			if failSave {
				wantErr, wantCount = saveErr, 1
				health := f.consumer.Health()
				if !errors.Is(health.Checkpoint.LastFailure, saveErr) || health.Recovery.Failures != 0 || health.Recovery.LastError != nil {
					t.Fatalf("checkpoint-only outage was classified as recovery: %+v", health)
				}
			}
			if !errors.Is(result.err, wantErr) || result.sequence != "100" || result.count != wantCount {
				t.Fatalf("pass = %+v", result)
			}
			stream.mu.Lock()
			reads := stream.getRecordsN
			stream.mu.Unlock()
			if reads != 2 {
				t.Fatalf("recovery fetched without a slot: %d reads", reads)
			}
			_, slots, queued := admissionState(f.consumer.admission)
			if slots != 1 || queued != 0 {
				t.Fatalf("slots = %d, queued = %d", slots, queued)
			}
		})
	}
}
