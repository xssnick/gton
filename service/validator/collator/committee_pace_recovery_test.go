package collator

import (
	"testing"
	"time"
)

type paceRecoveryCase struct {
	name       string
	heavyAfter uint32
}

type paceRecoveryCertificate struct {
	slot     uint32
	at       time.Time
	revision uint64
}

// Delayed certificates are delivered at their own event times, including while
// a successor is building. Thus a build can finish with a stale budget revision;
// feeding each certificate back synchronously would make recovery unrealistically
// fast. The fixed confirmation latency models a healthy committee with old lag.
func TestCommitteePaceRecoversWithinTwoLeaderWindowsWithLongTail(t *testing.T) {
	tests := []paceRecoveryCase{
		{name: "one millisecond work units", heavyAfter: 32},
		{name: "tenfold cost after eight own blocks", heavyAfter: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runPaceRecoveryWithLongTail(t, test)
		})
	}
}

func runPaceRecoveryWithLongTail(t *testing.T, test paceRecoveryCase) {
	t.Helper()

	const (
		rate          = 400 * time.Millisecond
		tail          = 125 * time.Millisecond
		initialBudget = 93440 * time.Microsecond
		confirmation  = 2 * rate
		windowSlots   = uint32(16)
		// Three other leaders' windows separate our two windows.
		windowStride = uint32(64)
	)
	start := time.Unix(1_700_000_000, 0)
	pace := newCommitteePace()
	pace.seed(paceEstimate{
		fraction:      float64(initialBudget) / float64(rate),
		finishReserve: initialBudget / 2,
		congested:     true,
		samples:       1,
		sampledAt:     start,
	}, 0, rate)
	if got := pace.budget(rate).duration; got != initialBudget {
		t.Fatalf("initial budget = %s, want %s", got, initialBudget)
	}

	pending := make([]paceRecoveryCertificate, 0, committeePaceOutstandingLimit)
	var recoveredAt time.Time
	var staleCertificates, ownBlocks, heavyBlocks uint32
	var maxOutstanding int
	advance := func(until time.Time) {
		for len(pending) > 0 && !pending[0].at.After(until) {
			certificate := pending[0]
			pending = pending[1:]
			before := pace.budget(rate)
			_, sampled := pace.noteCertified(paceCandidate(certificate.slot), certificate.at)
			after := pace.budget(rate)
			if certificate.revision != before.revision {
				staleCertificates++
				if sampled || after != before {
					t.Fatalf("stale certificate trained recovery: %+v -> %+v", before, after)
				}
			}
			if after.duration < initialBudget {
				t.Fatalf("healthy fixed-lag committee reduced budget: %s", after.duration)
			}
			if recoveredAt.IsZero() && after.duration >= 300*time.Millisecond {
				recoveredAt = certificate.at
			}
		}
	}

	for window := range uint32(2) {
		windowStart := window * windowStride
		advance(start.Add(time.Duration(windowStart) * rate))
		if len(pending) != 0 {
			t.Fatal("previous leader window still has outstanding certificates")
		}

		for offset := range windowSlots {
			slot := windowStart + offset
			buildAt := start.Add(time.Duration(slot) * rate)
			advance(buildAt)
			budget := pace.budget(rate)
			unit := time.Millisecond
			if ownBlocks >= test.heavyAfter {
				unit *= 10
				heavyBlocks++
			}
			transactions := max(uint32(1), uint32((budget.duration-budget.finishReserve)/unit))
			artificial := offset == 0
			if artificial {
				transactions = min(transactions, uint32(100))
			}
			elapsed := time.Duration(transactions)*unit + tail
			if elapsed >= rate {
				t.Fatalf("model cannot sustain its emission cadence: body %s, target %s", elapsed, rate)
			}
			emittedAt := buildAt.Add(elapsed)
			advance(emittedAt)
			pace.noteBuilt(
				budget,
				elapsed,
				tail,
				!artificial,
				artificial,
			)
			emission := paceTestEmission(
				pace,
				slot,
				emittedAt,
				rate,
			)
			emission.budget = budget
			emission.transactions = transactions
			emission.elapsed, emission.tail = elapsed, tail
			emission.artificial = artificial
			emission.limited = !artificial
			pace.noteEmitted(paceCandidate(slot), emission)
			pending = append(pending, paceRecoveryCertificate{
				slot: slot, at: emittedAt.Add(confirmation), revision: budget.revision,
			})
			ownBlocks++
			maxOutstanding = max(maxOutstanding, len(pending))
			if len(pending) > committeePaceOutstandingLimit {
				t.Fatalf("healthy model exceeded the outstanding gate: %d", len(pending))
			}
		}
		advance(start.Add(time.Duration(windowStart+windowSlots) * rate))
	}

	if recoveredAt.IsZero() {
		t.Fatalf("two leader windows did not recover 93.44ms to 300ms: final %s, stale ACKs %d",
			pace.budget(rate).duration, staleCertificates)
	}
	if staleCertificates == 0 {
		t.Fatal("model did not exercise delayed old-revision certificates")
	}
	if test.heavyAfter < ownBlocks && heavyBlocks == 0 {
		t.Fatal("model did not exercise the transaction cost switch")
	}
	t.Logf("recovered in %s including %s without leadership; final %s; stale ACKs %d; max outstanding %d",
		recoveredAt.Sub(start), time.Duration(windowStride-windowSlots)*rate,
		pace.budget(rate).duration, staleCertificates, maxOutstanding)
}
