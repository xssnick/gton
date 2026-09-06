package collator

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func paceCandidate(slot uint32) simplex.CandidateID {
	return simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(slot + 1)}}
}

// The stand's measurement, replayed: blocks of 400 transactions certified at a
// 466 ms cadence in a full pipeline. With the paced budget of one slot plus the
// window slack less the jitter reserve — 432 ms at a 400 ms target rate, 332 of
// them for transactions — the committee can hold about 332 / 0.915 ≈ 363 of
// them, and that is what the cap must come to — well under the ceiling, well
// over the floor.
func TestCommitteePaceCapsToTheMeasuredCadence(t *testing.T) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	if got := pace.transactionCap(rate); got != adaptiveTransactionStart {
		t.Fatalf("cap before any sample = %d, want the start value %d", got, adaptiveTransactionStart)
	}

	start := time.Unix(1_700_000_000, 0)
	// Candidates leave every 250 ms and certificates come every 466 ms: every
	// block after the first was queued behind the previous certificate and the
	// committee fell 216 ms further behind on each, so each interval is the
	// committee's time on that block. The first certificate measures an idle
	// committee from the emission and is not a sample.
	for slot := uint32(0); slot < 12; slot++ {
		pace.noteEmitted(paceCandidate(slot), start.Add(time.Duration(slot)*250*time.Millisecond), 400)
	}
	cert := start.Add(500 * time.Millisecond)
	for slot := uint32(0); slot < 12; slot++ {
		spent, sampled := pace.noteCertified(paceCandidate(slot), cert)
		if sampled != (slot > 0) {
			t.Fatalf("certificate %d sampled = %v (spent %s), want %v", slot, sampled, spent, slot > 0)
		}
		cert = cert.Add(466 * time.Millisecond)
	}
	perTransaction, samples := pace.estimate()
	if samples != 11 {
		t.Fatalf("samples = %d, want 11", samples)
	}
	// (466 - 100) / 400 = 0.915 ms per transaction.
	if perTransaction < 0.9 || perTransaction > 0.93 {
		t.Fatalf("ms per transaction = %.3f, want 0.915", perTransaction)
	}
	// Derived from the budget rather than written out, so a change to the
	// committee's per-slot allowance moves the expectation with it instead of
	// failing this test.
	cap := pace.transactionCap(rate)
	want := uint32(pacedBudget(rate) / float64(time.Millisecond) / perTransaction)
	if cap < want-25 || cap > want+25 {
		t.Fatalf("cap = %d, want about %d for a 466 ms cadence on 400-transaction blocks", cap, want)
	}
}

// A certificate the committee produced while idle measures delivery and
// whatever it was busy with before — the previous leader's tail, on the
// stand — and never the block's cost: it is not a sample. A certificate that
// keeps our own cadence proves only that the committee is at least that fast,
// so it relaxes the estimate and the cap probes upward, from the start value
// when nothing has been measured yet.
func TestCommitteePaceRelaxesWhileTheCommitteeKeepsPace(t *testing.T) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)

	// Idle: emitted, certified 383 ms later with nothing before it. On the
	// stand this read as 2.8 ms per transaction and cut the cap to 76. It is
	// not a sample; the committee had time to spare, so the cap grows.
	pace.noteEmitted(paceCandidate(1), start, 100)
	if _, sampled := pace.noteCertified(paceCandidate(1), start.Add(383*time.Millisecond)); sampled {
		t.Fatal("an idle committee's certificate became a cost sample")
	}
	if got := pace.transactionCap(rate); got <= adaptiveTransactionStart {
		t.Fatalf("cap after an idle certificate = %d, want above the start value: the committee had time to spare", got)
	}

	// Keeping pace: blocks every 400 ms, certificates every 400 ms, each block
	// queued behind the previous certificate by a constant 1.1 s of pipeline
	// latency. The cap climbs; the estimate never reads that latency as cost.
	emit := start.Add(time.Second)
	cert := emit.Add(1100 * time.Millisecond)
	previous := pace.transactionCap(rate)
	for slot := uint32(2); slot < 14; slot++ {
		pace.noteEmitted(paceCandidate(slot), emit, 300)
		if _, sampled := pace.noteCertified(paceCandidate(slot), cert); sampled {
			t.Fatalf("certificate %d on our own cadence became a cost sample", slot)
		}
		cap := pace.transactionCap(rate)
		if slot > 2 && cap <= previous && previous < adaptiveTransactionCeiling {
			t.Fatalf("cap after certificate %d = %d, want above %d: a committee keeping pace lets the cap grow", slot, cap, previous)
		}
		previous = cap
		emit = emit.Add(rate)
		cert = cert.Add(rate)
	}
	if _, samples := pace.estimate(); samples != 0 {
		t.Fatalf("samples = %d, want none: nothing measured the committee falling behind", samples)
	}
	if previous <= adaptiveTransactionStart {
		t.Fatalf("cap = %d after twelve certificates on cadence, want above the start value %d", previous, adaptiveTransactionStart)
	}
}

// Certificates for candidates this node did not emit, for empty candidates, and
// for blocks too small to measure the per-transaction cost leave the estimate
// alone — though a small block's certificate still marks the committee busy.
func TestCommitteePaceIgnoresWhatItCannotMeasure(t *testing.T) {
	pace := newCommitteePace()
	start := time.Unix(1_700_000_000, 0)
	if _, sampled := pace.noteCertified(paceCandidate(9), start); sampled {
		t.Fatal("a certificate for a candidate never emitted became a sample")
	}
	pace.noteEmitted(paceCandidate(1), start, 0)
	if _, sampled := pace.noteCertified(paceCandidate(1), start.Add(50*time.Millisecond)); sampled {
		t.Fatal("an empty candidate became a sample")
	}
	pace.noteEmitted(paceCandidate(2), start, 5)
	if _, sampled := pace.noteCertified(paceCandidate(2), start.Add(120*time.Millisecond)); sampled {
		t.Fatal("a five-transaction block became a sample")
	}
	if _, samples := pace.estimate(); samples != 0 {
		t.Fatalf("samples = %d, want none", samples)
	}
	if got := pace.transactionCap(400 * time.Millisecond); got != adaptiveTransactionStart {
		t.Fatalf("cap = %d, want the start value with nothing measured", got)
	}
	// The small block's certificate marked the committee busy until +120 ms:
	// the next block, emitted at +0 too, was queued behind it, and its
	// certificate at +400 came 280 ms later — far behind the zero interval
	// between the two emissions — so it is a sample measured from the previous
	// certificate, not from its emission.
	pace.noteEmitted(paceCandidate(3), start, 200)
	spent, sampled := pace.noteCertified(paceCandidate(3), start.Add(400*time.Millisecond))
	if !sampled || spent != 280*time.Millisecond {
		t.Fatalf("spent = %s sampled = %v, want 280 ms measured from the previous certificate", spent, sampled)
	}
}

// One absurd certificate cannot pin the cap at either end: the per-transaction
// cost is clamped before it enters the estimate, and the cap is clamped to its
// floor and ceiling after.
func TestCommitteePaceIsBoundedAtBothEnds(t *testing.T) {
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)

	// Blocks emitted together, each certified a minute after the previous
	// one: every certificate is a sample, and since one sample may at most
	// double the estimate it takes a run of them to reach the clamp.
	slow := newCommitteePace()
	pinEstimateAtTheClamp(slow, start)
	// At the clamped ms per transaction the budget buys very little, and the
	// moving estimate approaches the clamp from below; the floor is what stops
	// a still slower reading from going lower. The bound comes from the budget
	// so it follows a change to the committee's per-slot allowance.
	clamped := uint32(pacedBudget(rate)/float64(time.Millisecond)/committeeMaxMillisPerTransaction) + 20
	if got := slow.transactionCap(rate); got < adaptiveTransactionFloor || got > clamped {
		t.Fatalf("cap after a run of minute-long certificates = %d, want the floor or just above it", got)
	}
	perTransaction, _ := slow.estimate()
	if perTransaction < 0.9*committeeMaxMillisPerTransaction || perTransaction > committeeMaxMillisPerTransaction {
		t.Fatalf("ms per transaction = %.3f, want at the clamp %.1f", perTransaction, committeeMaxMillisPerTransaction)
	}

	// The relaxation stops at the ceiling: a committee that keeps pace for
	// long enough gets the cap the limits would let a block reach anyway, and
	// the estimate never goes below its clamp.
	fast := newCommitteePace()
	emit := start
	cert := start.Add(time.Second)
	fast.noteEmitted(paceCandidate(1), emit, 300)
	fast.noteCertified(paceCandidate(1), cert)
	for slot := uint32(2); slot < 80; slot++ {
		emit = emit.Add(rate)
		cert = cert.Add(rate)
		fast.noteEmitted(paceCandidate(slot), emit, 300)
		fast.noteCertified(paceCandidate(slot), cert)
	}
	if got := fast.transactionCap(rate); got != adaptiveTransactionCeiling {
		t.Fatalf("cap after eighty certificates on cadence = %d, want the ceiling %d", got, adaptiveTransactionCeiling)
	}
	if perTransaction, _ = fast.estimate(); perTransaction < committeeMinMillisPerTransaction {
		t.Fatalf("ms per transaction relaxed to %.3f, below the clamp %.1f", perTransaction, committeeMinMillisPerTransaction)
	}
}

// The stand at a session start, replayed: one certificate 1.36 s after the
// previous on a 125-transaction block, a stall that is not what the block
// cost. It may at most double the estimate; and the next certificate, back in
// 90 ms on a 63-transaction block, bounds the cost at 90/63 ms and pulls the
// estimate straight down to it — the cap recovers in one certificate instead
// of thirty.
func TestCommitteePaceStallIsSteppedAndUndoneByAFastCertificate(t *testing.T) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)

	// A measured committee: 300-transaction blocks certified 400 ms apart
	// while queued 1.1 s deep, one sample at 466 ms to set the estimate.
	pace.noteEmitted(paceCandidate(1), start, 300)
	pace.noteCertified(paceCandidate(1), start.Add(1100*time.Millisecond))
	pace.noteEmitted(paceCandidate(2), start.Add(400*time.Millisecond), 400)
	if _, sampled := pace.noteCertified(paceCandidate(2), start.Add(1566*time.Millisecond)); !sampled {
		t.Fatal("a certificate 466 ms behind the previous one on a queued block is a sample")
	}
	before, _ := pace.estimate()
	if before < 0.9 || before > 0.93 {
		t.Fatalf("ms per transaction = %.3f, want 0.915", before)
	}

	// The stall: queued, certified 1362 ms after the previous certificate.
	pace.noteEmitted(paceCandidate(3), start.Add(800*time.Millisecond), 125)
	if _, sampled := pace.noteCertified(paceCandidate(3), start.Add(2928*time.Millisecond)); !sampled {
		t.Fatal("the stalled certificate is a sample, only a bounded one")
	}
	stalled, _ := pace.estimate()
	// Unbounded, (1362-100)/125 = 10 ms clamped to 5 would have moved the
	// estimate to 0.915 + 0.3*(5-0.915) = 2.14; the step limit holds the
	// sample at 1.83 and the estimate at 1.19.
	if stalled > before*(1+committeePaceSmoothing*(committeePaceMaxSampleStep-1))+0.001 {
		t.Fatalf("ms per transaction after a stall = %.3f, want at most %.3f: one sample may at most double the estimate", stalled, before*(1+committeePaceSmoothing*(committeePaceMaxSampleStep-1)))
	}
	if stalled <= before {
		t.Fatalf("ms per transaction after a stall = %.3f, want above %.3f: the stall still counts", stalled, before)
	}

	// Recovery: the committee is idle and answers a 63-transaction block in
	// 90 ms. The cost cannot exceed 90/63 = 1.43 ms; the estimate drops to it.
	pace.noteEmitted(paceCandidate(4), start.Add(4*time.Second), 63)
	if _, sampled := pace.noteCertified(paceCandidate(4), start.Add(4090*time.Millisecond)); sampled {
		t.Fatal("an idle certificate is a bound, not a sample")
	}
	bounded, _ := pace.estimate()
	if bounded > 90.0/63+0.001 {
		t.Fatalf("ms per transaction after a 90 ms certificate on 63 transactions = %.3f, want at most %.3f", bounded, 90.0/63)
	}
	if cap := pace.transactionCap(rate); cap < 150 {
		t.Fatalf("cap after the fast certificate = %d, want at least 150", cap)
	}

	// A queued certificate bounds from the previous certificate, not from
	// the emission: 154 transactions certified 150 ms after the previous
	// certificate bound the cost at 150/154 ms.
	pace.noteEmitted(paceCandidate(5), start.Add(4050*time.Millisecond), 154)
	pace.noteCertified(paceCandidate(5), start.Add(4240*time.Millisecond))
	queuedBound, _ := pace.estimate()
	if queuedBound > 150.0/154+0.001 {
		t.Fatalf("ms per transaction after a 150 ms queued certificate on 154 transactions = %.3f, want at most %.3f", queuedBound, 150.0/154)
	}
}

// The service hands every slot the shard's paced cap and the first slot the
// smaller of that and firstSlotTransactions; shards do not share an estimate.
func TestServiceTransactionCapPerShardAndFirstSlot(t *testing.T) {
	service := &Service{}
	rate := 400 * time.Millisecond
	loaded := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	quiet := groups.ShardID{Workchain: 0, Shard: 1 << 62}

	if got := service.transactionCap(loaded, rate, false); got != adaptiveTransactionStart {
		t.Fatalf("cap with nothing measured = %d, want the start value", got)
	}
	if got := service.transactionCap(loaded, rate, true); got != firstSlotTransactions {
		t.Fatalf("first-slot cap with nothing measured = %d, want %d", got, firstSlotTransactions)
	}

	// A committee slower than the start value assumes: 400-transaction blocks
	// certified 700 ms apart, 1.5 ms per transaction, a cap of about 221.
	start := time.Unix(1_700_000_000, 0)
	pace := service.pace(loaded)
	for slot := uint32(0); slot < 4; slot++ {
		pace.noteEmitted(paceCandidate(slot), start, 400)
	}
	cert := start.Add(500 * time.Millisecond)
	for slot := uint32(0); slot < 4; slot++ {
		pace.noteCertified(paceCandidate(slot), cert)
		cert = cert.Add(700 * time.Millisecond)
	}
	paced := service.transactionCap(loaded, rate, false)
	if paced >= adaptiveTransactionStart || paced <= firstSlotTransactions {
		t.Fatalf("paced cap = %d, want between the first-slot cap and the start value", paced)
	}
	if got := service.transactionCap(loaded, rate, true); got != firstSlotTransactions {
		t.Fatalf("first-slot cap = %d, want %d while the paced cap is larger", got, firstSlotTransactions)
	}
	if got := service.transactionCap(quiet, rate, false); got != adaptiveTransactionStart {
		t.Fatalf("another shard's cap = %d, want the start value: shards do not share an estimate", got)
	}

	// Through the public hook, the same sample path: a block emitted right
	// after the others, queued behind the previous certificate, certified a
	// second after it — far behind the emission cadence.
	service.pace(loaded).noteEmitted(paceCandidate(40), start.Add(100*time.Millisecond), 400)
	service.ObserveConsensusNotarized(loaded, paceCandidate(40), cert.Add(time.Second))
	if got := service.transactionCap(loaded, rate, false); got >= paced {
		t.Fatalf("cap after a slower certificate = %d, want below %d", got, paced)
	}
	if got := firstSlotTransactionCap(30); got != 30 {
		t.Fatalf("first-slot cap under a paced cap of 30 = %d, want 30", got)
	}
}

// A committee faster than our own cadence certifies each block before the
// next one is emitted, so none of its certificates is ever "queued". On the
// stand that froze the cap at 44 transactions for four windows in a row while
// every block was certified on time. An idle committee's certificate relaxes
// the estimate harder than a pace-keeping one: from the floor the cap is back
// above the start value within a dozen certificates, one window.
func TestCommitteePaceRecoversFromTheFloorWhenTheCommitteeIsIdle(t *testing.T) {
	pace := newCommitteePace()
	rate := 400 * time.Millisecond
	start := time.Unix(1_700_000_000, 0)

	// A run of stalls pins the estimate at the clamp: cap at the floor's
	// neighbourhood.
	pinEstimateAtTheClamp(pace, start)
	floored := pace.transactionCap(rate)
	if floored > uint32(pacedBudget(rate)/float64(time.Millisecond)/committeeMaxMillisPerTransaction)+20 {
		t.Fatalf("cap after the stall = %d, want the floor's neighbourhood", floored)
	}

	// Small blocks every 400 ms, each certified 250 ms after its emission —
	// before the next one leaves. Never queued, never a sample.
	emit := start.Add(time.Hour)
	previous := floored
	certificates := 0
	for slot := uint32(3); slot < 40; slot++ {
		pace.noteEmitted(paceCandidate(slot), emit, uint32(previous))
		if _, sampled := pace.noteCertified(paceCandidate(slot), emit.Add(250*time.Millisecond)); sampled {
			t.Fatalf("an idle committee's certificate %d became a sample", slot)
		}
		certificates++
		cap := pace.transactionCap(rate)
		if cap <= previous && cap < adaptiveTransactionCeiling {
			t.Fatalf("cap after idle certificate %d = %d, want above %d", slot, cap, previous)
		}
		previous = cap
		if cap >= adaptiveTransactionStart {
			break
		}
		emit = emit.Add(rate)
	}
	if previous < adaptiveTransactionStart {
		t.Fatalf("cap = %d after %d idle certificates, want back above the start value %d", previous, certificates, adaptiveTransactionStart)
	}
	if certificates > 12 {
		t.Fatalf("recovery took %d idle certificates, want at most twelve — one window", certificates)
	}
}

// pinEstimateAtTheClamp feeds a pace eight blocks emitted together and
// certified a minute apart: every certificate after the first is a sample of
// a minute-long block, and the estimate doubles per sample until it sits at
// the clamp.
func pinEstimateAtTheClamp(pace *committeePace, start time.Time) {
	// One sample may at most double the estimate and the moving average only
	// approaches the clamp, so the run has to be long enough for the estimate
	// implied by adaptiveTransactionStart to get there. Emit every candidate
	// before certifying any: noteEmitted is what prunes the emission table,
	// and these certificates are minutes apart.
	const certificates = 16
	for slot := uint32(1); slot <= certificates; slot++ {
		pace.noteEmitted(paceCandidate(slot), start, 100)
	}
	cert := start.Add(300 * time.Millisecond)
	for slot := uint32(1); slot <= certificates; slot++ {
		pace.noteCertified(paceCandidate(slot), cert)
		cert = cert.Add(time.Minute)
	}
}
