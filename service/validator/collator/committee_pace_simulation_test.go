package collator

import (
	"testing"
	"testing/synctest"
	"time"
)

type paceSimCertificate struct {
	slot uint32
	at   time.Time
}

// The model has a real sequential certificate queue: feedback is delivered
// only when that queue finishes, not synchronously after issuing each block.
// A cost switch changes both the builder's work and the committee's work per
// transaction; a validator-only slowdown is a separate congestion episode.
func TestCommitteePaceQueuedFeedbackAndTenfoldCostSwitch(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { runPaceCostSimulation(t, false) })
	})
	t.Run("already-started speculative successor", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { runPaceCostSimulation(t, true) })
	})
}

func runPaceCostSimulation(t *testing.T, speculative bool) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Now()
	finish := start
	pending := make([]paceSimCertificate, 0, 8)
	var lightTransactions, heavyTransactions uint32
	var initialRampSlot uint32
	var reduced time.Duration
	var lagBeforeReduction, reducedLag time.Duration
	var peakLag time.Duration
	maxOutstanding := committeePaceOutstandingLimit
	if speculative {
		maxOutstanding++
	}
	for slot := range uint32(192) {
		now := time.Now()
		for len(pending) > 0 && !pending[0].at.After(now) {
			certificate := pending[0]
			pending = pending[1:]
			pace.noteCertified(paceCandidate(certificate.slot), certificate.at)
		}
		// Runtime futures snapshot their budget before the admission gate. An ACK
		// while waiting may lower the next revision, but this one build still
		// uses the original immutable request; its later ACK must remain stale.
		budget := pace.budget(rate)
		// A successor may have passed its gate just before its predecessor was
		// emitted. Allow that one in-flight build to overshoot, as runtime does.
		alreadyStarted := speculative && len(pending) == committeePaceOutstandingLimit && slot%2 == 1
		if !alreadyStarted {
			ready := make(chan error, 1)
			go func() { ready <- pace.waitForCapacity(t.Context(), rate) }()
		wait:
			for {
				synctest.Wait()
				select {
				case err := <-ready:
					if err != nil {
						t.Fatal(err)
					}
					break wait
				default:
				}
				time.Sleep(time.Until(pending[0].at))
				now = time.Now()
				for len(pending) > 0 && !pending[0].at.After(now) {
					certificate := pending[0]
					pending = pending[1:]
					pace.noteCertified(paceCandidate(certificate.slot), certificate.at)
				}
			}
		}
		if initialRampSlot == 0 && budget.duration == 380*time.Millisecond {
			initialRampSlot = slot
		}
		unit := time.Millisecond
		if slot >= 16 {
			unit *= 10
		}
		tail := 20 * time.Millisecond
		transactions := max(uint32(1), uint32((budget.duration-budget.finishReserve)/unit))
		elapsed := time.Duration(transactions)*unit + tail
		emission := paceTestEmission(pace, slot, now, rate)
		emission.budget = budget
		emission.transactions = transactions
		emission.elapsed, emission.tail = elapsed, tail
		pace.noteBuilt(budget, elapsed, tail, true, false)
		pace.noteEmitted(paceCandidate(slot), emission)
		validation := elapsed + 10*time.Millisecond
		if slot >= 32 && slot < 96 {
			validation = elapsed * 3
		}
		finish = maxTime(finish, now).Add(validation)
		peakLag = max(peakLag, finish.Sub(now))
		pending = append(pending, paceSimCertificate{slot: slot, at: finish})
		if len(pending) > maxOutstanding {
			t.Fatalf("admission exceeded outstanding bound: %d", len(pending))
		}
		if slot == 15 {
			lightTransactions = transactions
		}
		if slot == 31 {
			heavyTransactions = transactions
			if budget.duration != 380*time.Millisecond {
				t.Fatalf("tenfold transaction cost changed the time envelope: %+v", budget)
			}
		}
		if slot == 79 {
			lagBeforeReduction = finish.Sub(now)
		}
		if slot == 95 {
			reduced = budget.duration
			reducedLag = finish.Sub(now)
			if reduced >= 200*time.Millisecond || reducedLag > 5*rate {
				t.Fatalf("sustained slowdown still increased lag after feedback: budget %s queue %s -> %s",
					reduced, lagBeforeReduction, reducedLag)
			}
		}
		time.Sleep(rate)
	}
	if initialRampSlot > 8 || initialRampSlot == 0 {
		t.Fatalf("initial ramp took %d slots, want at most eight", initialRampSlot)
	}
	if lightTransactions < 9*heavyTransactions || lightTransactions > 11*heavyTransactions {
		t.Fatalf("time budget did not absorb tenfold cost: light %d heavy %d", lightTransactions, heavyTransactions)
	}
	if final := pace.budget(rate).duration; final <= reduced*2 {
		t.Fatalf("queue drain did not permit recovery: %s -> %s", reduced, final)
	}
	if peakLag > time.Duration(maxOutstanding)*3*rate {
		t.Fatalf("gate did not bound the sudden slowdown backlog: %s", peakLag)
	}
	t.Logf("ceiling after %d slots; cost switch tx/block %d -> %d; congestion budget %s, lag %s -> %s; peak lag %s; recovered %s",
		initialRampSlot, lightTransactions, heavyTransactions, reduced, lagBeforeReduction, reducedLag,
		peakLag, pace.budget(rate).duration)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func TestCommitteePaceProtocolLimitMayDecreaseButNotIncrease(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(3) {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		emission.limited = false
		certifyPaceTestEmission(pace, slot, emission,
			start.Add(500*time.Millisecond+time.Duration(slot)*700*time.Millisecond))
	}
	reduced := pace.budget(rate)
	if reduced.duration >= rate/2 {
		t.Fatal("protocol-full candidates could not reduce overloaded committee budget")
	}
	for slot := uint32(3); slot < 12; slot++ {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		emission.limited = false
		certifyPaceTestEmission(pace, slot, emission, emission.at.Add(2*time.Second))
	}
	if got := pace.budget(rate); got != reduced {
		t.Fatalf("protocol limit incorrectly justified a larger time budget: %+v -> %+v", reduced, got)
	}
}

func BenchmarkCommitteePaceBudget(b *testing.B) {
	pace := newCommitteePace()
	b.ReportAllocs()
	for b.Loop() {
		pace.budget(400 * time.Millisecond)
	}
}

func BenchmarkCommitteePaceFeedback(b *testing.B) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	var slot uint32
	b.ReportAllocs()
	for b.Loop() {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		pace.noteBuilt(emission.budget, emission.elapsed, 20*time.Millisecond, true, false)
		certifyPaceTestEmission(pace, slot, emission, emission.at.Add(300*time.Millisecond))
		slot++
	}
}

func BenchmarkCommitteePaceFullPendingTable(b *testing.B) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	var slot uint32
	for range committeePaceEmissionLimit {
		pace.noteEmitted(paceCandidate(slot), paceTestEmission(pace, slot, start, rate))
		slot++
	}
	b.ReportAllocs()
	for b.Loop() {
		pace.noteEmitted(paceCandidate(slot), paceTestEmission(pace, slot, start, rate))
		slot++
	}
}
