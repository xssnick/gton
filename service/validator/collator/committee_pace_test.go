package collator

import (
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

func paceCandidate(slot uint32) simplex.CandidateID {
	return simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(slot + 1)}}
}

func paceTestEmission(pace *committeePace, slot uint32, at time.Time, rate time.Duration) paceEmission {
	budget := pace.budget(rate)
	emission := paceEmission{
		at: at, targetRate: rate, budget: budget,
		window:       WindowID{StartSlot: slot / 16 * 16},
		transactions: 100, elapsed: budget.duration, limited: true, demand: true,
	}
	if slot > 0 {
		emission.parent = simplex.Parent(paceCandidate(slot - 1))
	}
	return emission
}

func certifyPaceTestEmission(pace *committeePace, slot uint32, emission paceEmission, at time.Time) bool {
	pace.noteEmitted(paceCandidate(slot), emission)
	_, sampled := pace.noteCertified(paceCandidate(slot), at)
	return sampled
}

func TestCommitteePaceTimeBudgetBoundsAndFastStart(t *testing.T) {
	t.Parallel()
	rate := 400 * time.Millisecond
	pace := newCommitteePace()
	start := time.Unix(1_700_000_000, 0)
	if got := pace.budget(rate); got.duration != 200*time.Millisecond || got.finishReserve != 25*time.Millisecond {
		t.Fatalf("initial budget = %+v", got)
	}
	for slot := range uint32(6) {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		certifyPaceTestEmission(pace, slot, emission, emission.at.Add(300*time.Millisecond))
		if slot == 2 && pace.budget(rate).duration != 300*time.Millisecond {
			t.Fatalf("two timely intervals did not grow budget by 50%%: %+v", pace.budget(rate))
		}
	}
	if got := pace.budget(rate).duration; got != 380*time.Millisecond {
		t.Fatalf("fast-start budget = %s, want ceiling 380ms", got)
	}
	if got := newCommitteePace().budget(20 * time.Millisecond); got.duration != 19*time.Millisecond {
		t.Fatalf("small target budget = %+v, floor must not exceed ceiling", got)
	}
	if got := newCommitteePace().budget(0); got.duration != 0 {
		t.Fatalf("disabled target budget = %+v", got)
	}
}

func TestCommitteePaceRecoversWithOldLagAndMicrosecondJitter(t *testing.T) {
	t.Parallel()
	rate := 400 * time.Millisecond
	pace := newCommitteePace()
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(12) {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		certifyPaceTestEmission(pace, slot, emission,
			emission.at.Add(2*time.Second+time.Duration(slot)*time.Microsecond))
	}
	if got := pace.budget(rate).duration; got != 380*time.Millisecond {
		t.Fatalf("constant old lag or 400.001ms cadence suppressed recovery: %s", got)
	}
}

func TestCommitteePaceRequiresTwoSlowIntervalsAndIgnoresStaleAcks(t *testing.T) {
	t.Parallel()
	rate := 400 * time.Millisecond
	pace := newCommitteePace()
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(15) {
		pace.noteEmitted(paceCandidate(slot),
			paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate))
	}
	initial := pace.budget(rate)
	pace.noteCertified(paceCandidate(0), start.Add(500*time.Millisecond))
	pace.noteCertified(paceCandidate(1), start.Add(1200*time.Millisecond))
	if got := pace.budget(rate); got != initial {
		t.Fatalf("one slow interval changed budget: %+v", got)
	}
	pace.noteCertified(paceCandidate(2), start.Add(1900*time.Millisecond))
	reduced := pace.budget(rate)
	if reduced.duration >= initial.duration || reduced.duration < initial.duration/2 {
		t.Fatalf("two slow intervals produced unbounded reduction: %+v -> %+v", initial, reduced)
	}
	for slot := uint32(3); slot < 15; slot++ {
		pace.noteCertified(paceCandidate(slot), start.Add(500*time.Millisecond+time.Duration(slot)*700*time.Millisecond))
	}
	if got := pace.budget(rate); got != reduced {
		t.Fatalf("old budget acknowledgments compounded slowdown: %+v -> %+v", reduced, got)
	}
	if pace.snapshot().samples != 2 {
		t.Fatalf("stale acknowledgments counted as fresh evidence: %+v", pace.snapshot())
	}
	// New-window current-revision evidence can recover despite the old queue.
	for slot := uint32(16); slot <= 20; slot++ {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		certifyPaceTestEmission(pace, slot, emission, emission.at.Add(10*time.Second))
	}
	if got := pace.budget(rate).duration; got <= reduced.duration || got > reduced.duration*109/100 {
		t.Fatalf("steady recovery budget = %s, want one bounded 8%% probe above %s", got, reduced.duration)
	}
}

func TestCommitteePaceOneStallDoesNotPoisonRecovery(t *testing.T) {
	t.Parallel()
	rate := 400 * time.Millisecond
	pace := newCommitteePace()
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(7) {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		lag := 500 * time.Millisecond
		if slot > 0 {
			lag = 1500 * time.Millisecond
		}
		certifyPaceTestEmission(pace, slot, emission, emission.at.Add(lag))
	}
	if got := pace.budget(rate).duration; got != 380*time.Millisecond {
		t.Fatalf("single stall poisoned subsequent steady cadence: %s", got)
	}
}

type paceNoEvidenceCase struct {
	name       string
	limited    bool
	artificial bool
	txs        uint32
}

func TestCommitteePaceNoWorkAndArtificialCapsCannotTrain(t *testing.T) {
	tests := []paceNoEvidenceCase{
		{name: "no time pressure", txs: 200},
		{name: "no transactions", limited: true},
		{name: "first slot", limited: true, artificial: true, txs: 100},
		{name: "reused first-slot future", limited: true, artificial: true, txs: 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pace := newCommitteePace()
			rate := 400 * time.Millisecond
			start := time.Unix(1_700_000_000, 0)
			before := pace.budget(rate)
			for slot := range uint32(12) {
				emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
				emission.limited, emission.artificial, emission.transactions = test.limited, test.artificial, test.txs
				emission.demand = test.limited
				if certifyPaceTestEmission(pace, slot, emission, emission.at.Add(300*time.Millisecond)) {
					t.Fatal("uninformative candidate trained capacity")
				}
			}
			if got := pace.budget(rate); got != before || !pace.snapshot().sampledAt.IsZero() {
				t.Fatalf("uninformative candidates changed or refreshed capacity: %+v, %+v", got, pace.snapshot())
			}
		})
	}
}

func TestCommitteePaceIgnoresUnknownDuplicateFailedAndExpiredEmissions(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	if _, sampled := pace.noteCertified(paceCandidate(0), start); sampled {
		t.Fatal("unknown certificate trained capacity")
	}
	emission := paceTestEmission(pace, 0, start, rate)
	pace.noteEmitted(paceCandidate(0), emission)
	pace.discardEmission(paceCandidate(0))
	if _, sampled := pace.noteCertified(paceCandidate(0), start.Add(rate)); sampled {
		t.Fatal("failed emission trained capacity")
	}
	pace.noteEmitted(paceCandidate(1), paceTestEmission(pace, 1, start, rate))
	pace.noteCertified(paceCandidate(1), start.Add(rate))
	before := pace.snapshot()
	pace.noteCertified(paceCandidate(1), start.Add(2*rate))
	if got := pace.snapshot(); got != before {
		t.Fatalf("duplicate certificate changed estimate: %+v", got)
	}
	pace.noteEmitted(paceCandidate(2), paceTestEmission(pace, 2, start, rate))
	if _, sampled := pace.noteCertified(paceCandidate(2), start.Add(committeePaceEmissionRetention+time.Second)); sampled {
		t.Fatal("expired certificate trained capacity")
	}
}

func TestCommitteePaceMixedTransactionCostsUseSameTimeEvidence(t *testing.T) {
	t.Parallel()
	light, heavy := newCommitteePace(), newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(12) {
		at := start.Add(time.Duration(slot) * rate)
		a, b := paceTestEmission(light, slot, at, rate), paceTestEmission(heavy, slot, at, rate)
		a.transactions, b.transactions = 1000, 1
		certifyPaceTestEmission(light, slot, a, at.Add(300*time.Millisecond))
		certifyPaceTestEmission(heavy, slot, b, at.Add(300*time.Millisecond))
		if light.budget(rate) != heavy.budget(rate) {
			t.Fatal("equal work-time evidence depended on transaction count")
		}
	}
}

type paceAdjacencyCase struct {
	name   string
	change func(*paceEmission)
}

func TestCommitteePaceDoesNotMixWindowsGapsOrForeignParents(t *testing.T) {
	tests := []paceAdjacencyCase{
		{name: "another window", change: func(e *paceEmission) { e.window.StartSlot = 16 }},
		{name: "foreign parent", change: func(e *paceEmission) { e.parent = simplex.Parent(paceCandidate(90)) }},
		{name: "another session", change: func(e *paceEmission) { e.window.SessionID[0]++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pace := newCommitteePace()
			rate := 400 * time.Millisecond
			start := time.Unix(1_700_000_000, 0)
			certifyPaceTestEmission(pace, 0, paceTestEmission(pace, 0, start, rate), start.Add(rate))
			next := paceTestEmission(pace, 1, start.Add(rate), rate)
			test.change(&next)
			if certifyPaceTestEmission(pace, 1, next, start.Add(2*rate)) {
				t.Fatal("unrelated certificate interval trained capacity")
			}
			if !pace.snapshot().sampledAt.IsZero() {
				t.Fatal("unrelated certificate interval refreshed history")
			}
		})
	}
	t.Run("skipped and reordered slots", func(t *testing.T) {
		pace := newCommitteePace()
		rate := 400 * time.Millisecond
		start := time.Unix(1_700_000_000, 0)
		for _, slot := range []uint32{0, 2, 1} {
			emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
			if certifyPaceTestEmission(pace, slot, emission, emission.at.Add(rate)) {
				t.Fatal("gap or reordered certificate trained capacity")
			}
		}
	})
}

func TestCommitteePaceFinishReserveAdaptsWithoutInvalidatingAcks(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	before := pace.budget(rate)
	pace.noteBuilt(before, 250*time.Millisecond, 80*time.Millisecond, true, false)
	grown := pace.budget(rate)
	if grown.finishReserve != 100*time.Millisecond || grown.revision != before.revision {
		t.Fatalf("finish reserve did not grow immediately without changing capacity revision: %+v", grown)
	}
	pace.noteBuilt(grown, 150*time.Millisecond, 8*time.Millisecond, true, false)
	after := pace.budget(rate)
	if after.finishReserve >= grown.finishReserve || after.finishReserve < 90*time.Millisecond {
		t.Fatalf("finish reserve did not release slowly: %+v", after)
	}
	if !pace.snapshot().sampledAt.IsZero() {
		t.Fatal("build reserve refreshed committee history without a certificate")
	}
	pace.noteBuilt(after, 2*time.Second, time.Second, false, false)
	pace.noteBuilt(after, 2*time.Second, time.Second, true, true)
	if got := pace.budget(rate); got != after {
		t.Fatalf("idle/artificial finish changed reserve: %+v", got)
	}
}

func TestCommitteePaceSlowBuildCannotProbeUpward(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(12) {
		emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
		emission.elapsed = 2 * rate
		certifyPaceTestEmission(pace, slot, emission, emission.at.Add(300*time.Millisecond))
	}
	if got := pace.budget(rate).duration; got != rate/2 {
		t.Fatalf("slow local builds raised budget despite timely certificates: %s", got)
	}
}

func TestCommitteePacePendingBoundAndNoAutonomousShrink(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)
	for slot := range uint32(committeePaceEmissionLimit + 30) {
		pace.noteEmitted(paceCandidate(slot),
			paceTestEmission(pace, slot, start.Add(time.Duration(slot)*time.Millisecond), rate))
	}
	if len(pace.emitted) != committeePaceEmissionLimit {
		t.Fatalf("pending records = %d", len(pace.emitted))
	}
	before := pace.budget(rate)
	for range 1000 {
		if got := pace.budget(rate); got != before {
			t.Fatal("missing acknowledgments autonomously shrank budget")
		}
	}
	pace.noteEmitted(paceCandidate(1000),
		paceTestEmission(pace, 1000, start.Add(time.Minute), rate))
	if len(pace.emitted) != 1 {
		t.Fatalf("expired pending records retained: %d", len(pace.emitted))
	}
}

func TestCommitteePaceConcurrentFeedback(t *testing.T) {
	t.Parallel()
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Now()
	var work sync.WaitGroup
	work.Go(func() {
		for slot := range uint32(200) {
			emission := paceTestEmission(pace, slot, start.Add(time.Duration(slot)*rate), rate)
			certifyPaceTestEmission(pace, slot, emission, emission.at.Add(300*time.Millisecond))
		}
	})
	work.Go(func() {
		for range 1000 {
			budget := pace.budget(rate)
			pace.noteBuilt(budget, budget.duration, 10*time.Millisecond, true, false)
		}
	})
	work.Wait()
	if got := pace.budget(rate); got.duration > rate || got.finishReserve > got.duration/2 {
		t.Fatalf("concurrent feedback produced invalid budget: %+v", got)
	}
}
