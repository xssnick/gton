package collator

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type paceCapacityCase struct {
	name    string
	release func(*committeePace, time.Time)
}

func TestCommitteePaceCapacityWaitWakesOnCertificateOrFailedEmit(t *testing.T) {
	tests := []paceCapacityCase{
		{name: "certificate", release: func(p *committeePace, at time.Time) {
			p.noteCertified(paceCandidate(0), at)
		}},
		{name: "failed emit", release: func(p *committeePace, _ time.Time) {
			p.discardEmission(paceCandidate(0))
		}},
		{name: "later foreign certificate retires skipped slots", release: func(p *committeePace, at time.Time) {
			p.noteCertified(paceCandidate(10), at)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pace := newCommitteePace()
				rate := 400 * time.Millisecond
				for slot := range uint32(committeePaceOutstandingLimit) {
					emission := paceTestEmission(pace, slot, time.Now(), rate)
					// Old-window pending work is real work, not a fresh empty queue.
					emission.window.StartSlot = slot * 16
					pace.noteEmitted(paceCandidate(slot), emission)
				}
				ready := make(chan error, 1)
				go func() { ready <- pace.waitForCapacity(t.Context(), rate) }()
				synctest.Wait()
				select {
				case err := <-ready:
					t.Fatalf("full committee pipeline did not block: %v", err)
				default:
				}
				time.Sleep(rate)
				before := pace.snapshot()
				test.release(pace, time.Now())
				if err := <-ready; err != nil {
					t.Fatal(err)
				}
				if pace.snapshot() != before {
					t.Fatal("release without adjacent certificates trained capacity")
				}
			})
		})
	}
}

func TestCommitteePaceCapacityWaitCancellationAndExpiry(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pace := newCommitteePace()
			rate := 400 * time.Millisecond
			for slot := range uint32(committeePaceOutstandingLimit) {
				pace.noteEmitted(paceCandidate(slot), paceTestEmission(pace, slot, time.Now(), rate))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ready := make(chan error, 1)
			go func() { ready <- pace.waitForCapacity(ctx, rate) }()
			synctest.Wait()
			cancel()
			if err := <-ready; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled wait = %v", err)
			}
			if len(pace.emitted) != committeePaceOutstandingLimit {
				t.Fatal("canceling a new build discarded already emitted candidates")
			}
		})
	})
	t.Run("expiry without any ACK", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			pace := newCommitteePace()
			rate := 400 * time.Millisecond
			for slot := range uint32(committeePaceOutstandingLimit) {
				pace.noteEmitted(paceCandidate(slot), paceTestEmission(pace, slot, time.Now(), rate))
			}
			before := pace.budget(rate)
			ready := make(chan error, 1)
			go func() { ready <- pace.waitForCapacity(t.Context(), rate) }()
			synctest.Wait()
			time.Sleep(committeePaceEmissionRetention)
			if err := <-ready; err != nil {
				t.Fatal(err)
			}
			if got := pace.budget(rate); got != before || len(pace.emitted) != 0 {
				t.Fatalf("expiry shrank capacity or retained dead candidates: %+v, %d", got, len(pace.emitted))
			}
		})
	})
}

func TestCommitteePaceCapacityExcludesNoWorkAndArtificialCaps(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	for slot := range uint32(40) {
		emission := paceTestEmission(pace, slot, time.Now(), rate)
		emission.artificial = slot%2 == 0
		emission.demand = slot%2 == 0
		pace.noteEmitted(paceCandidate(slot), emission)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := pace.waitForCapacity(ctx, rate); err != nil {
		t.Fatalf("non-demand candidates blocked admission: %v", err)
	}
}

func BenchmarkCommitteePaceUnblockedCapacity(b *testing.B) {
	pace := newCommitteePace()
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		if err := pace.waitForCapacity(ctx, 400*time.Millisecond); err != nil {
			b.Fatal(err)
		}
	}
}
