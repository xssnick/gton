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

// Recording a numeric observation directly isolates history from feedback.
func recordHistorySample(
	service *Service,
	session Session,
	rate time.Duration,
	at time.Time,
	fraction float64,
) *committeePace {
	pace := service.pace(session, rate)
	pace.mu.Lock()
	pace.fraction = fraction
	pace.finishReserve = 30 * time.Millisecond
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
	oldSession, nextSession := historySession(1), historySession(2)
	slices.Reverse(nextSession.Validators)
	next := service.pace(nextSession, rate)
	if got := next.budget(rate).duration; got != rate/2 {
		t.Fatalf("cold budget = %s", got)
	}
	now := time.Now()
	old := recordHistorySample(service, oldSession, rate, now, 0.8)
	emission := paceTestEmission(old, 1, now, rate)
	old.noteEmitted(paceCandidate(1), emission)
	old.last = paceCertificate{id: paceCandidate(0), at: now}
	if got := service.pace(nextSession, rate).budget(rate).duration; got != 320*time.Millisecond {
		t.Fatalf("future budget = %s, want predecessor's latest estimate", got)
	}
	if next == old || len(next.emitted) != 0 || !next.last.at.IsZero() || !next.lastSample.IsZero() {
		t.Fatal("rotation inherited candidate bookkeeping or renewed evidence")
	}
}

func TestPaceHistoryLatePreviousGenerationCannotOverwrite(t *testing.T) {
	t.Parallel()
	service := &Service{}
	rate := 400 * time.Millisecond
	now := time.Now()
	oldSession, nextSession := historySession(1), historySession(2)
	recordHistorySample(service, oldSession, rate, now, 0.8)
	recordHistorySample(service, nextSession, rate, now.Add(time.Millisecond), 0.6)
	recordHistorySample(service, oldSession, rate, now.Add(2*time.Millisecond), 0.9)
	if got := service.pace(historySession(3), rate).budget(rate).duration; got != 240*time.Millisecond {
		t.Fatalf("retained budget overwritten by predecessor: %s", got)
	}
}

func TestPaceHistoryWaitsForInformativeLocalEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := &Service{}
		rate := 400 * time.Millisecond
		oldSession, nextSession := historySession(1), historySession(2)
		recordHistorySample(service, oldSession, rate, time.Now(), 0.6)
		next := service.pace(nextSession, rate)
		for slot := range uint32(3) {
			emission := paceTestEmission(next, slot, time.Now(), rate)
			emission.limited = false
			emission.demand = false
			next.noteEmitted(paceCandidate(slot), emission)
			time.Sleep(rate)
			service.ObserveConsensusNotarized(nextSession.ID, paceCandidate(slot), time.Now())
		}
		if !next.snapshot().sampledAt.IsZero() {
			t.Fatal("underfilled certificates claimed fresh capacity")
		}
		recordHistorySample(service, oldSession, rate, time.Now(), 0.8)
		if got := service.pace(nextSession, rate).budget(rate).duration; got != 320*time.Millisecond {
			t.Fatalf("underfill blocked newer predecessor evidence: %s", got)
		}
		for slot := uint32(3); slot < 6; slot++ {
			emission := paceTestEmission(next, slot, time.Now(), rate)
			next.noteEmitted(paceCandidate(slot), emission)
			time.Sleep(rate)
			service.ObserveConsensusNotarized(nextSession.ID, paceCandidate(slot), time.Now())
		}
		before := next.budget(rate)
		if next.snapshot().sampledAt.IsZero() {
			t.Fatal("informative local certificates did not own the model")
		}
		recordHistorySample(service, oldSession, rate, time.Now(), 0.5)
		if got := service.pace(nextSession, rate).budget(rate); got != before {
			t.Fatalf("local model overwritten by predecessor: %+v", got)
		}
	})
}

func TestPaceHistoryDoesNotRetainNoWorkOrFirstSlot(t *testing.T) {
	t.Parallel()
	service := &Service{}
	session := historySession(1)
	rate := 400 * time.Millisecond
	pace := service.pace(session, rate)
	now := time.Now()
	for slot := range uint32(6) {
		emission := paceTestEmission(pace, slot, now.Add(time.Duration(slot)*rate), rate)
		emission.artificial = slot%2 == 0
		emission.limited = slot%2 == 0
		emission.demand = emission.limited
		pace.noteEmitted(paceCandidate(slot), emission)
		service.ObserveConsensusNotarized(session.ID, paceCandidate(slot), emission.at.Add(rate))
	}
	if len(service.paceHistory) != 0 {
		t.Fatal("uninformative certificates retained or refreshed history")
	}
}

func TestPaceHistoryRetirementKeepsOnlyNumericEstimate(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(61, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	pace := recordHistorySample(fixture.service, session, update.TargetRate, time.Now(), 0.75)
	pace.noteEmitted(paceCandidate(1), paceTestEmission(pace, 1, time.Now(), update.TargetRate))
	if err := fixture.service.RetireSession(t.Context(), session.ID); err != nil {
		t.Fatal(err)
	}
	if fixture.service.existingPace(session.ID) != nil {
		t.Fatal("retirement retained old session")
	}
	next := session
	next.ID[0]++
	next.CatchainSeqno++
	if got := fixture.service.pace(next, update.TargetRate).budget(update.TargetRate).duration; got != time.Duration(float64(update.TargetRate)*0.75) {
		t.Fatalf("retirement lost numeric estimate: %s", got)
	}
}

func TestPaceHistoryAgesWithoutIdleRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := &Service{}
		rate := 400 * time.Millisecond
		recordHistorySample(service, historySession(1), rate, time.Now(), 0.8)
		time.Sleep(paceHistoryFreshness + time.Second)
		aged := service.pace(historySession(2), rate)
		if got := aged.budget(rate).duration; got != rate/2 || !aged.snapshot().sampledAt.IsZero() {
			t.Fatalf("aged optimistic budget = %s, evidence = %+v", got, aged.snapshot())
		}
		time.Sleep(paceHistoryRetention)
		if got := service.pace(historySession(3), rate).budget(rate).duration; got != rate/2 {
			t.Fatalf("expired history budget = %s", got)
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
		recordHistorySample(service, historySession(1), rate, time.Now(), 0.25)
		time.Sleep(paceHistoryFreshness + time.Second)
		if got := service.pace(historySession(2), rate).budget(rate).duration; got != rate/4 {
			t.Fatalf("aging raised restrictive budget: %s", got)
		}
	})
}

func TestPaceHistoryTargetRateChangeIsolatesOldCallbacks(t *testing.T) {
	t.Parallel()
	service := &Service{}
	session := historySession(1)
	rate := 400 * time.Millisecond
	old := recordHistorySample(service, session, rate, time.Now(), 0.8)
	next := service.pace(session, 2*rate)
	service.rememberPace(session.ID, old, time.Now())
	if next == old || next.budget(2*rate).duration != rate {
		t.Fatal("changed target rate retained or trained old model")
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
		recordHistorySample(service, session, rate, now.Add(time.Duration(i)*time.Millisecond), 0.75)
		service.pacesMu.Lock()
		delete(service.paces, session.ID)
		service.pacesMu.Unlock()
	}
	if got := len(service.paceHistory); got != paceHistoryLimit {
		t.Fatalf("retained identities = %d, want %d", got, paceHistoryLimit)
	}
	oldest := historySession(1)
	oldest.Validators[0].Weight = 1
	if got := service.pace(oldest, rate).budget(rate).duration; got != rate/2 {
		t.Fatalf("oldest evicted identity retained budget: %s", got)
	}
}

func TestPaceHistoryConcurrentRotationAndCertificates(t *testing.T) {
	t.Parallel()
	service := &Service{}
	rate := 400 * time.Millisecond
	oldSession, nextSession := historySession(1), historySession(2)
	recordHistorySample(service, oldSession, rate, time.Now(), 0.8)
	var work sync.WaitGroup
	work.Go(func() {
		for range 100 {
			service.pace(nextSession, rate).budget(rate)
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
	if got := service.pace(nextSession, rate).budget(rate).duration; got != 320*time.Millisecond {
		t.Fatalf("concurrent rotation lost budget: %s", got)
	}
}

func BenchmarkPaceHistoryActiveBudget(b *testing.B) {
	service := &Service{}
	rate := 400 * time.Millisecond
	session := historySession(1)
	recordHistorySample(service, session, rate, time.Now(), 0.8)
	b.ReportAllocs()
	for b.Loop() {
		service.pace(session, rate).budget(rate)
	}
}

func BenchmarkPaceHistorySeededBudget(b *testing.B) {
	service := &Service{}
	rate := 400 * time.Millisecond
	recordHistorySample(service, historySession(1), rate, time.Now(), 0.8)
	session := historySession(2)
	service.pace(session, rate)
	b.ReportAllocs()
	for b.Loop() {
		service.pace(session, rate).budget(rate)
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
