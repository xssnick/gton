package collator

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
)

type paceIdentityTestCase struct {
	name   string
	change func(*Session)
}

func historySession(generation byte) Session {
	return Session{
		ID:                   [32]byte{generation},
		Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
		CatchainSeqno:        uint32(generation),
		ValidatorSetHash:     uint32(generation),
		ConsensusVersion:     2,
		ProtocolVersion:      1,
		SlotsPerLeaderWindow: 16,
		Validators: []SessionValidator{
			{PublicKey: [32]byte{1}, ADNLID: [32]byte{11}, Weight: 10},
			{PublicKey: [32]byte{2}, ADNLID: [32]byte{12}, Weight: 20},
		},
	}
}

func TestCommitteePaceHistoryIdentity(t *testing.T) {
	t.Parallel()
	rate := 400 * time.Millisecond
	base := historySession(1)
	key := committeePaceKey(base, rate)
	if got := committeePaceKey(historySession(2), rate); got != key {
		t.Fatal("equivalent rotation changed the history identity")
	}
	if got := committeePaceKey(base, rate+time.Millisecond); got == key {
		t.Fatal("changed target rate reused the history identity")
	}
	rotated := historySession(2)
	slices.Reverse(rotated.Validators)
	before := slices.Clone(rotated.Validators)
	if got := committeePaceKey(rotated, rate); got != key {
		t.Fatal("same committee in a different leader order changed the history identity")
	}
	if !slices.Equal(rotated.Validators, before) {
		t.Fatal("history identity computation reordered the live consensus roster")
	}

	tests := []paceIdentityTestCase{
		{name: "workchain", change: func(s *Session) { s.Shard.Workchain++ }},
		{name: "shard", change: func(s *Session) { s.Shard.Shard >>= 1 }},
		{name: "consensus version", change: func(s *Session) { s.ConsensusVersion++ }},
		{name: "consensus flags", change: func(s *Session) { s.ConsensusFlags++ }},
		{name: "protocol version", change: func(s *Session) { s.ProtocolVersion++ }},
		{name: "transport", change: func(s *Session) { s.UseQUIC = !s.UseQUIC }},
		{name: "window size", change: func(s *Session) { s.SlotsPerLeaderWindow++ }},
		{name: "public key", change: func(s *Session) { s.Validators[0].PublicKey[0] += 10 }},
		{name: "ADNL identity", change: func(s *Session) { s.Validators[0].ADNLID[0]++ }},
		{name: "weight", change: func(s *Session) { s.Validators[0].Weight++ }},
		{name: "roster size", change: func(s *Session) { s.Validators = s.Validators[:1] }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := base
			changed.Validators = slices.Clone(base.Validators)
			test.change(&changed)
			if committeePaceKey(changed, rate) == key {
				t.Fatal("changed committee or schedule reused the history identity")
			}
		})
	}
}

// Recording a numeric observation directly isolates history from the feedback
// model's convergence tests. No test-only hooks are needed in production.
func recordHistorySample(service *Service, session Session, rate time.Duration, at time.Time, transactions uint32) *committeePace {
	pace := service.pace(session, rate)
	pace.mu.Lock()
	pace.millisPerTransaction = impliedMillisPerTransaction(rate, transactions)
	pace.samples = 3
	pace.lastSample = at
	pace.mu.Unlock()
	service.rememberPace(session.ID, pace, at)
	return pace
}

func TestPaceHistorySeedsFutureSessionOnUse(t *testing.T) {
	t.Parallel()
	service := &Service{}
	rate := 400 * time.Millisecond
	oldSession, futureSession := historySession(1), historySession(2)
	slices.Reverse(futureSession.Validators)
	future := service.pace(futureSession, rate)
	if got := future.transactionCap(rate); got != adaptiveTransactionStart {
		t.Fatalf("unmeasured cap = %d, want %d", got, adaptiveTransactionStart)
	}

	now := time.Now()
	old := recordHistorySample(service, oldSession, rate, now, 800)
	old.noteEmitted(paceCandidate(1), paceEmission{at: now, transactions: 100, targetRate: rate})
	old.mu.Lock()
	old.lastCertificate = now
	old.lastCertifiedEmission = now.Add(-time.Millisecond)
	old.mu.Unlock()

	if got := service.transactionCap(futureSession, rate, false); got != 800 {
		t.Fatalf("future session cap = %d, want predecessor's latest 800", got)
	}
	if got := service.transactionCap(futureSession, rate, true); got != firstSlotTransactions {
		t.Fatalf("first-slot cap = %d, want %d", got, firstSlotTransactions)
	}
	if future == old {
		t.Fatal("rotation shared the committee object")
	}
	future.mu.Lock()
	defer future.mu.Unlock()
	if len(future.emitted) != 0 || !future.lastCertificate.IsZero() || !future.lastCertifiedEmission.IsZero() {
		t.Fatal("rotation inherited candidate or certificate bookkeeping")
	}
	if !future.lastSample.IsZero() {
		t.Fatal("inherited history was counted as a local observation")
	}
}

func TestPaceHistoryLatePreviousGenerationCannotOverwrite(t *testing.T) {
	t.Parallel()
	service := &Service{}
	rate := 400 * time.Millisecond
	now := time.Now()
	oldSession, nextSession := historySession(1), historySession(2)
	recordHistorySample(service, oldSession, rate, now, 800)
	recordHistorySample(service, nextSession, rate, now.Add(time.Millisecond), 600)
	recordHistorySample(service, oldSession, rate, now.Add(2*time.Millisecond), 900)
	if got := service.transactionCap(nextSession, rate, false); got != 600 {
		t.Fatalf("active session cap overwritten by predecessor: %d", got)
	}
	if got := service.transactionCap(historySession(3), rate, false); got != 600 {
		t.Fatalf("retained cap overwritten by predecessor: %d", got)
	}
}

func TestPaceHistoryTransfersCertificateEstimateUntilLocalEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := &Service{}
		rate := 400 * time.Millisecond
		oldSession, nextSession := historySession(1), historySession(2)
		old := service.pace(oldSession, rate)
		emitPaceCandidate(old, paceCandidate(1), time.Now(), rate, 400, 400, false)
		time.Sleep(200 * time.Millisecond)
		service.ObserveConsensusNotarized(oldSession.ID, paceCandidate(1), time.Now())
		wantCost, wantSamples := old.estimate()
		if wantCost <= 0 || wantSamples != 0 {
			t.Fatalf("timely upward probe = %g ms/tx, %d samples, want positive cost with no slowdown sample", wantCost, wantSamples)
		}

		next := service.pace(nextSession, rate)
		if cost, samples := next.estimate(); cost != wantCost || samples != wantSamples {
			t.Fatalf("inherited estimate = %g ms/tx, %d samples, want %g and %d", cost, samples, wantCost, wantSamples)
		}
		// An unknown certificate is not local evidence and must not prevent a
		// newer estimate from the predecessor from seeding this session.
		service.ObserveConsensusNotarized(nextSession.ID, paceCandidate(99), time.Now())
		if !next.snapshot().sampledAt.IsZero() {
			t.Fatal("unknown certificate claimed ownership of the inherited estimate")
		}

		// Even an underfilled local block establishes this session's own
		// certificate timeline without changing the inherited cost or count.
		emitPaceCandidate(next, paceCandidate(2), time.Now(), rate, 1, next.transactionCap(rate), false)
		time.Sleep(200 * time.Millisecond)
		certifiedAt := time.Now()
		service.ObserveConsensusNotarized(nextSession.ID, paceCandidate(2), certifiedAt)
		if next.snapshot().sampledAt != certifiedAt {
			t.Fatal("local certificate did not claim ownership of the estimate")
		}

		time.Sleep(time.Millisecond)
		recordHistorySample(service, oldSession, rate, time.Now(), 900)
		service.pace(nextSession, rate)
		if cost, samples := next.estimate(); cost != wantCost || samples != wantSamples {
			t.Fatalf("local model overwritten after its certificate: %g ms/tx, %d samples", cost, samples)
		}
		if next.lastCertificate != certifiedAt || len(next.emitted) != 0 {
			t.Fatal("local certificate bookkeeping changed during history lookup")
		}
	})
}

func TestPaceHistoryDoesNotRetainUnmeasuredCost(t *testing.T) {
	t.Parallel()
	service := &Service{}
	session := historySession(1)
	rate := 400 * time.Millisecond
	pace := service.pace(session, rate)
	now := time.Now()
	emitPaceCandidate(pace, paceCandidate(1), now.Add(-time.Millisecond), rate, 1, 400, false)
	service.ObserveConsensusNotarized(session.ID, paceCandidate(1), now)
	if len(service.paceHistory) != 0 {
		t.Fatal("unmeasured zero cost was retained as committee capacity")
	}
}

func TestPaceHistoryRetirementKeepsOnlyNumericEstimate(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(61, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	pace := fixture.service.pace(session, update.TargetRate)
	pace.mu.Lock()
	pace.millisPerTransaction = impliedMillisPerTransaction(update.TargetRate, 650)
	pace.lastSample = time.Now()
	pace.mu.Unlock()
	pace.noteEmitted(paceCandidate(1), paceEmission{
		at: time.Now(), targetRate: update.TargetRate,
		transactions: 650, transactionCap: 650,
	})
	if err := fixture.service.RetireSession(t.Context(), session.ID); err != nil {
		t.Fatal(err)
	}
	if fixture.service.existingPace(session.ID) != nil {
		t.Fatal("retirement retained the old session bookkeeping")
	}
	next := session
	next.ID[0]++
	next.CatchainSeqno++
	if got := fixture.service.transactionCap(next, update.TargetRate, false); got != 650 {
		t.Fatalf("cap after predecessor retirement = %d, want 650", got)
	}
}

func TestPaceHistoryAgesWithoutIdleRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := &Service{}
		rate := 400 * time.Millisecond
		recordHistorySample(service, historySession(1), rate, time.Now(), 800)
		time.Sleep(paceHistoryFreshness + time.Second)
		aged := service.pace(historySession(2), rate)
		if got := aged.transactionCap(rate); got != adaptiveTransactionStart {
			t.Fatalf("aged optimistic cap = %d, want cold-start %d", got, adaptiveTransactionStart)
		}
		estimate := aged.snapshot()
		if estimate.millisPerTransaction != impliedMillisPerTransaction(rate, adaptiveTransactionStart) || !estimate.sampledAt.IsZero() {
			t.Fatalf("aged estimate = %+v, want cold-start cost without renewed observation", estimate)
		}
		time.Sleep(paceHistoryRetention)
		if got := service.transactionCap(historySession(3), rate, false); got != adaptiveTransactionStart {
			t.Fatalf("expired history cap = %d, want cold-start %d", got, adaptiveTransactionStart)
		}
		if len(service.paceHistory) != 0 {
			t.Fatal("idle seeded session renewed expired history")
		}
	})
}

func TestPaceHistoryKeepsConservativeAgedEstimate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := &Service{}
		rate := 400 * time.Millisecond
		recordHistorySample(service, historySession(1), rate, time.Now(), 200)
		time.Sleep(paceHistoryFreshness + time.Second)
		if got := service.transactionCap(historySession(2), rate, false); got != 200 {
			t.Fatalf("aging raised restrictive cap to %d", got)
		}
	})
}

func TestPaceHistoryTargetRateChangeIsolatesOldCallbacks(t *testing.T) {
	t.Parallel()
	service := &Service{}
	session := historySession(1)
	rate := 400 * time.Millisecond
	old := recordHistorySample(service, session, rate, time.Now(), 800)
	next := service.pace(session, 2*rate)
	if next == old {
		t.Fatal("changed target rate retained the active feedback model")
	}
	service.rememberPace(session.ID, old, time.Now())
	if got := next.transactionCap(2 * rate); got != adaptiveTransactionStart {
		t.Fatalf("late old-rate callback seeded new-rate model: %d", got)
	}
}

func TestPaceHistoryBoundedByCommitteeIdentities(t *testing.T) {
	t.Parallel()
	service := &Service{}
	rate := 400 * time.Millisecond
	now := time.Now()
	for i := range paceHistoryLimit + 10 {
		session := historySession(byte(i + 1))
		session.Validators[0].Weight = uint64(i + 1)
		recordHistorySample(service, session, rate, now.Add(time.Duration(i)*time.Millisecond), 500)
		service.pacesMu.Lock()
		delete(service.paces, session.ID)
		service.pacesMu.Unlock()
	}
	if got := len(service.paceHistory); got != paceHistoryLimit {
		t.Fatalf("retained committee identities = %d, want %d", got, paceHistoryLimit)
	}
	oldest := historySession(1)
	oldest.Validators[0].Weight = 1
	if got := service.transactionCap(oldest, rate, false); got != adaptiveTransactionStart {
		t.Fatalf("oldest evicted committee still retained cap %d", got)
	}
}

func TestPaceHistoryConcurrentRotationAndCertificates(t *testing.T) {
	t.Parallel()
	service := &Service{}
	rate := 400 * time.Millisecond
	oldSession, nextSession := historySession(1), historySession(2)
	recordHistorySample(service, oldSession, rate, time.Now(), 800)
	var work sync.WaitGroup
	work.Go(func() {
		for range 100 {
			service.transactionCap(nextSession, rate, false)
		}
	})
	work.Go(func() {
		for range 100 {
			service.ObserveConsensusNotarized(oldSession.ID, paceCandidate(1), time.Now())
		}
	})
	work.Go(func() {
		for range 100 {
			service.pacesMu.Lock()
			service.rememberPaceLocked(oldSession.ID, time.Now())
			delete(service.paces, oldSession.ID)
			service.pacesMu.Unlock()
		}
	})
	work.Wait()
	if got := service.transactionCap(nextSession, rate, false); got != 800 {
		t.Fatalf("concurrent rotation lost retained cap: %d", got)
	}
}

func BenchmarkPaceHistoryActiveBudget(b *testing.B) {
	service := &Service{}
	rate := 400 * time.Millisecond
	session := historySession(1)
	recordHistorySample(service, session, rate, time.Now(), 800)
	b.ReportAllocs()
	for b.Loop() {
		service.transactionCap(session, rate, false)
	}
}

func BenchmarkPaceHistorySeededBudget(b *testing.B) {
	service := &Service{}
	rate := 400 * time.Millisecond
	recordHistorySample(service, historySession(1), rate, time.Now(), 800)
	session := historySession(2)
	service.pace(session, rate)
	b.ReportAllocs()
	for b.Loop() {
		service.transactionCap(session, rate, false)
	}
}

func BenchmarkCommitteePaceHistoryIdentity(b *testing.B) {
	session := historySession(1)
	session.Validators = make([]SessionValidator, 100)
	for i := range session.Validators {
		session.Validators[i].PublicKey[0] = byte(i * 37 % len(session.Validators))
		session.Validators[i].ADNLID[0] = byte(i)
		session.Validators[i].Weight = uint64(i + 1)
	}
	b.ReportAllocs()
	for b.Loop() {
		committeePaceKey(session, 400*time.Millisecond)
	}
}
