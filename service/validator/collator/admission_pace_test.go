package collator

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type admissionCostCase struct {
	name string
	cost time.Duration
	want uint64
}

func TestAdmissionPaceChargesActualWork(t *testing.T) {
	tests := []admissionCostCase{
		{name: "cheap", cost: time.Millisecond, want: 8},
		{name: "ten-times-more-expensive", cost: 10 * time.Millisecond, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := &collation{
					limits:    &blockLimitStatus{},
					admission: newAdmissionPace(10*time.Millisecond, 2*time.Millisecond),
				}
				c.admission.startWork()
				for !c.paceExpired() {
					time.Sleep(test.cost)
					c.limits.transactions++
				}
				if c.limits.transactions != test.want {
					t.Fatalf("admitted %d transactions, want %d", c.limits.transactions, test.want)
				}
				if !c.admission.limited {
					t.Fatal("actual budget stop was not recorded")
				}
			})
		})
	}
}

func TestAdmissionPacePreservesProgressAfterLargePreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &collation{
			limits:    &blockLimitStatus{},
			admission: newAdmissionPace(40*time.Millisecond, time.Second),
		}
		time.Sleep(time.Second)
		c.admission.startWork()
		if c.admission.allowance != 10*time.Millisecond {
			t.Fatalf("useful allowance = %s, want 10ms", c.admission.allowance)
		}
		time.Sleep(20 * time.Millisecond)
		if c.paceExpired() {
			t.Fatal("first canonical transaction was starved")
		}
		c.limits.transactions++
		if !c.paceExpired() {
			t.Fatal("expired budget did not close after useful progress")
		}
		c.admission.observe(&c.stats)
		if c.stats.PaceElapsed != 1020*time.Millisecond {
			t.Fatalf("reported body hides preparation: %s", c.stats.PaceElapsed)
		}
	})
}

func TestAdmissionPaceRejectedWorkCanCloseBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &collation{
			limits:    &blockLimitStatus{},
			admission: newAdmissionPace(time.Millisecond, 0),
		}
		c.admission.startWork()
		time.Sleep(time.Second)
		c.stats.ExternalAttempts = 64
		if c.paceExpired() {
			t.Fatal("speculative reservations counted as canonical progress")
		}
		c.stats.ExternalNotAccepted = 1
		if !c.paceExpired() {
			t.Fatal("a rejected external flood disabled the active-time budget")
		}
	})
}

func TestAdmissionPaceSeparatesWaitingAndFinalTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newAdmissionPace(100*time.Millisecond, 20*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		p.startWork()
		time.Sleep(time.Second)
		p.wait += time.Second
		time.Sleep(30 * time.Millisecond)
		p.close()
		time.Sleep(5 * time.Millisecond)
		var stats Stats
		p.observe(&stats)
		if stats.PaceElapsed != 55*time.Millisecond || stats.PaceTail != 5*time.Millisecond {
			t.Fatalf("body/tail = %s/%s, want 55ms/5ms", stats.PaceElapsed, stats.PaceTail)
		}
		if stats.PaceLimited {
			t.Fatal("natural completion was reported as a pace limit")
		}
	})
}

func TestAdmissionPaceKeepsWorkersButBoundsLookahead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newAdmissionPace(100*time.Millisecond, 0)
		p.startWork()
		if got := p.waveLimit(16); got != 64 {
			t.Fatalf("early wave = %d, want 64", got)
		}
		time.Sleep(80 * time.Millisecond)
		if got := p.waveLimit(16); got != 16 {
			t.Fatalf("near-cutoff wave = %d, want all 16 workers without queued lookahead", got)
		}
		if got := p.waveLimit(4); got != 4 {
			t.Fatalf("generated near-cutoff wave = %d, want 4", got)
		}
	})
}

// pacedExternalWorkers uses the production plan/retirement machinery with a
// synthetic service time on test-owned workers. No VM hook or clock injection
// enters production: every transaction still runs through the real emulator.
func pacedExternalWorkers(c *collation, workers int, cost func(*externalPlan) time.Duration) {
	c.externalWaves.start(c, 1)
	w := &c.externalWaves
	w.queue = make(chan *externalPlan, internalWaveLength)
	for range workers {
		w.workers.Add(1)
		go func() {
			defer w.workers.Done()
			for plan := range w.queue {
				if w.abandoned.Load() {
					plan.wg.Done()
					continue
				}
				time.Sleep(cost(plan))
				c.speculateExternal(plan)
			}
		}()
	}
}

type admissionWaveCase struct {
	name     string
	accounts int
	workers  int
	cost     time.Duration
}

func TestAdmissionPaceExternalWavesHandleMixedCosts(t *testing.T) {
	tests := []admissionWaveCase{
		{name: "parallel-cheap", accounts: 32, workers: 16, cost: time.Millisecond},
		{name: "parallel-heavy", accounts: 32, workers: 16, cost: 10 * time.Millisecond},
		{name: "two-serial-account-chains-cheap", accounts: 2, workers: 2, cost: time.Millisecond},
		{name: "two-serial-account-chains-heavy", accounts: 2, workers: 2, cost: 10 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				req := newExternalWaveFixture(t, test.accounts, 0, 32).request
				req.PaceBudget = 10 * time.Millisecond
				req.PaceFinishReserve = 2 * time.Millisecond
				c, err := testBuilder().prepareShardPhases(t.Context(), req, collationAttempt{})
				if err != nil {
					t.Fatal(err)
				}
				defer c.stopWaves()
				pacedExternalWorkers(c, test.workers, func(*externalPlan) time.Duration { return test.cost })
				result, err := c.processExternalBatchInWaves(req.Externals, time.Time{}, test.workers)
				if err != nil {
					t.Fatal(err)
				}
				if result.stop != ExternalStopDeadline || !c.admission.limited {
					t.Fatalf("stop = %v, paced = %v", result.stop, c.admission.limited)
				}
				if c.limits.transactions == 0 || c.stats.ExternalInvalid != 0 {
					t.Fatalf("paced cutoff lost valid progress: %+v", c.stats)
				}
				if test.cost == 10*time.Millisecond && c.limits.transactions != 1 {
					t.Fatalf("heavy transactions admitted = %d, want one atomic transaction", c.limits.transactions)
				}
				if test.accounts == 2 && test.cost == time.Millisecond && c.limits.transactions != 15 {
					t.Fatalf("two serial chains admitted %d transactions, want 15", c.limits.transactions)
				}
				if test.accounts > 2 && test.cost == time.Millisecond && c.limits.transactions <= 15 {
					t.Fatalf("parallel work was charged as serial: %d transactions", c.limits.transactions)
				}
				if err = c.processNewMessages(false); err != nil {
					t.Fatal(err)
				}
				candidate, err := c.finishShard()
				if err != nil {
					t.Fatal(err)
				}

				// Canonical prefix parity catches any mutated state or leaked
				// proof reads from the speculative results past the cutoff.
				req.PaceBudget = 0
				req.PaceFinishReserve = 0
				req.MaxTransactions = candidate.Stats.Transactions
				req.internalWaveWorkers = -1
				reference, err := testBuilder().BuildShard(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(candidate.BlockBOC, reference.BlockBOC) ||
					!bytes.Equal(candidate.CollatedData, reference.CollatedData) {
					t.Fatal("paced wave produced a different block from its sequential prefix")
				}
			})
		})
	}
}

func TestAdmissionPaceTailIncludesRunningSpeculation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		req := newExternalWaveFixture(t, 32, 0, 1).request
		req.PaceBudget = 2 * time.Millisecond
		c, err := testBuilder().prepareShardPhases(t.Context(), req, collationAttempt{})
		if err != nil {
			t.Fatal(err)
		}
		defer c.stopWaves()
		first, second := req.Externals[0].Ref, req.Externals[1].Ref
		pacedExternalWorkers(c, 16, func(plan *externalPlan) time.Duration {
			switch plan.input.Ref {
			case first:
				return time.Millisecond
			case second:
				return 3 * time.Millisecond
			default:
				return 10 * time.Millisecond
			}
		})
		if _, err = c.processExternalBatchInWaves(req.Externals, time.Time{}, 16); err != nil {
			t.Fatal(err)
		}
		c.admission.observe(&c.stats)
		if !c.stats.PaceLimited || c.stats.PaceTail < 7*time.Millisecond {
			t.Fatalf("running VM tail was not charged after cutoff: %+v", c.stats)
		}
	})
}

func TestAdmissionPaceZeroLeavesDeterministicBuildUnchanged(t *testing.T) {
	req := newExternalWaveFixture(t, 2, 0, 2).request
	reference, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.PaceFinishReserve = time.Hour
	got, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.BlockBOC, reference.BlockBOC) || got.Stats != reference.Stats {
		t.Fatal("a reserve without a budget changed the deterministic build")
	}
}

type paceEmptyExternalSource struct {
	takeDelay time.Duration
	nextCalls int
}

func (s *paceEmptyExternalSource) TakeReady(int) []msgpool.ExternalSnapshot {
	time.Sleep(s.takeDelay)
	return nil
}

func (s *paceEmptyExternalSource) Next(ctx context.Context, _ int) ([]msgpool.ExternalSnapshot, error) {
	s.nextCalls++
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAdmissionPaceExhaustedUnderloadDoesNotClaimDemand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		req := newExternalWaveFixture(t, 2, 0, 1).request
		req.PaceBudget = 10 * time.Millisecond
		source := &paceEmptyExternalSource{takeDelay: req.PaceBudget}
		candidate, _, err := testBuilder().buildShardWithReadyExternals(
			t.Context(), req, source, time.Time{}, time.Now().Add(time.Second), 64, time.Time{}, time.Time{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if candidate.Stats.PaceLimited || candidate.Stats.ExternalIncluded != 2 {
			t.Fatalf("empty source falsely reported capacity-limited demand: %+v", candidate.Stats)
		}
		if source.nextCalls != 0 {
			t.Fatal("exhausted underloaded build waited for new demand")
		}
	})
}

func TestAdmissionPaceReadySourceWaitIsExcluded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		req := newExternalWaveFixture(t, 2, 0, 1).request
		req.PaceBudget = 10 * time.Millisecond
		req.Externals = nil
		source := &paceEmptyExternalSource{}
		candidate, _, err := testBuilder().buildShardWithReadyExternals(
			t.Context(), req, source, time.Time{}, time.Now().Add(time.Second), 64, time.Time{}, time.Time{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if candidate.Stats.ExternalWait != time.Second {
			t.Fatalf("external wait = %s, want 1s", candidate.Stats.ExternalWait)
		}
		if candidate.Stats.PaceElapsed != 0 || candidate.Stats.PaceLimited {
			t.Fatalf("external wait polluted active body: %+v", candidate.Stats)
		}
	})
}

func BenchmarkAdmissionPace(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			c := &collation{limits: &blockLimitStatus{transactions: 1}}
			if enabled {
				c.admission = newAdmissionPace(time.Hour, time.Second)
				c.admission.startWork()
			}
			b.ReportAllocs()
			for b.Loop() {
				_ = c.paceExpired()
				_ = c.admission.waveLimit(16)
			}
		})
	}
}

func BenchmarkAdmissionPaceBuild(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			req := fullCollatedMainnetRequest(b)
			if enabled {
				req.PaceBudget = time.Hour
				req.PaceFinishReserve = time.Second
			}
			builder := testBuilder()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := builder.BuildShard(context.Background(), req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
