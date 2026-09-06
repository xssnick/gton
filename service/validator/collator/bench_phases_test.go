package collator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Phase timings come from driving the unexported pipeline directly instead of
// from a tracing hook inside BuildShard: nothing in this file exists at
// runtime, so a production collation carries no timer, no branch and no
// interface call. The cost of that choice is that the phase order lives in two
// places, which the guards in bench_phases_guard_test.go keep honest.

// collationPhases is the order BuildShard runs. The two initial queue proofs
// are folded into "prepare"; they are setup for the pipeline, not a phase of
// it.
var collationPhases = []string{
	"prepare",
	"cleanup-queue",
	"dispatch-queue",
	"internals",
	"externals",
	"internals-top-up",
	"new-messages",
	"build-collated-proofs",
	"processed-info",
	"claimed-local-cleanup",
	"finish-accounts",
	"finish",
}

// verificationPhases is the order VerifyShardCandidate runs, with the semantic
// replay broken out into the passes SemanticVerifier performs. "structural"
// holds VerifyShardCandidate's own checks with the semantic time removed.
var verificationPhases = []string{
	"decode",
	"structural",
	"semantic-prepare",
	"semantic-precheck",
	"semantic-accounts",
	"semantic-queue-verify",
	"semantic-master",
}

// phaseTimer accumulates wall time per phase across benchmark iterations. The
// phases run in situ, in one real pipeline pass per iteration, so each one sees
// the caches the previous phase left warm — the same conditions production has.
type phaseTimer struct {
	total map[string]time.Duration
	// nested records phases measured around an inner pipeline they do not own.
	// The subtraction happens once, at report time: applying it per iteration
	// would remove the accumulated child totals over and over.
	nested map[string][]string
}

func newPhaseTimer() *phaseTimer {
	return &phaseTimer{total: map[string]time.Duration{}, nested: map[string][]string{}}
}

func (p *phaseTimer) run(tb testing.TB, phase string, fn func() error) {
	started := time.Now()
	err := fn()
	p.total[phase] += time.Since(started)
	if err != nil {
		tb.Fatalf("phase %q: %v", phase, err)
	}
}

func (p *phaseTimer) nest(parent string, children ...string) {
	p.nested[parent] = children
}

// report emits one metric per phase for benchstat, and logs the same numbers as
// a table in pipeline order. The phase sum excludes timing scaffolding and
// worker shutdown; the benchmark's ns/op includes both.
func (p *phaseTimer) report(b *testing.B, phases []string) {
	for parent, children := range p.nested {
		for _, child := range children {
			p.total[parent] -= p.total[child]
		}
	}

	var total time.Duration
	for _, phase := range phases {
		total += p.total[phase]
	}
	width := 0
	for _, phase := range phases {
		width = max(width, len(phase))
	}

	var table strings.Builder
	fmt.Fprintf(&table, "phase breakdown over %d iterations:", b.N)
	for _, phase := range phases {
		each := p.total[phase] / time.Duration(b.N)
		share := 0.0
		if total > 0 {
			share = 100 * float64(p.total[phase]) / float64(total)
		}
		fmt.Fprintf(&table, "\n  %-*s %9.3f ms  %5.1f%%", width, phase, float64(each.Microseconds())/1000, share)
		b.ReportMetric(float64(p.total[phase].Nanoseconds())/float64(b.N), phase+"-ns/op")
	}
	fmt.Fprintf(&table, "\n  %-*s %9.3f ms", width, "phase sum", float64((total/time.Duration(b.N)).Microseconds())/1000)
	b.Log(table.String())
}

// collateThroughAccounts runs the pipeline up to and including finishAccounts
// and hands back the collation itself. finish() seals the read set and empties
// it, so a caller that needs to see what the collation read has to stop here.
func collateThroughAccounts(tb testing.TB, builder *Builder, req ShardRequest, timer *phaseTimer) *collation {
	tb.Helper()

	c := collateBeforeAccounts(tb, builder, req, timer)
	timer.run(tb, "finish-accounts", c.finishAccounts)
	return c
}

// collateBeforeAccounts stops one phase earlier, so a caller can bracket
// finishAccounts itself — what it costs, or what it asks storage for — instead
// of only seeing the collation after it ran.
func collateBeforeAccounts(tb testing.TB, builder *Builder, req ShardRequest, timer *phaseTimer) *collation {
	tb.Helper()

	ctx := context.Background()
	var c *collation
	// All wave work is finished before this helper returns. Join here so
	// callers inspecting accounts cannot retain idle pools between iterations,
	// including when a phase aborts the test.
	defer func() {
		if c != nil {
			c.stopWaves()
		}
	}()
	timer.run(tb, "prepare", func() error {
		var err error
		if c, err = builder.prepare(ctx, req); err != nil {
			return err
		}
		if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
			return err
		}
		return c.limits.addProof(c.dispatchQueue.RootCell())
	})
	timer.run(tb, "cleanup-queue", c.cleanupOutQueue)
	timer.run(tb, "dispatch-queue", c.processDispatchQueue)
	timer.run(tb, "internals", c.processInternals)
	timer.run(tb, "externals", c.processExternals)
	timer.run(tb, "internals-top-up", c.topUpInternals)
	timer.run(tb, "new-messages", func() error {
		return c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete())
	})
	timer.run(tb, "build-collated-proofs", c.buildDeferredCollatedProofs)
	timer.run(tb, "processed-info", c.updateProcessedInfo)
	timer.run(tb, "claimed-local-cleanup", c.cleanupClaimedLocalDequeues)
	return c
}

// collateWithPhases mirrors Builder.BuildShard so each step can be timed.
// TestCollationPhaseOrderMatchesBuildShard fails if the two drift apart.
func collateWithPhases(tb testing.TB, builder *Builder, req ShardRequest, timer *phaseTimer) *Candidate {
	tb.Helper()

	c := collateThroughAccounts(tb, builder, req, timer)

	var candidate *Candidate
	timer.run(tb, "finish", func() error {
		var err error
		candidate, err = c.finish()
		return err
	})
	return candidate
}

// phaseSemantics is the production semantic verifier with its four passes timed
// individually. It mirrors SemanticVerifier.VerifyCandidateTransition; see
// TestSemanticPhaseOrderMatchesVerifier.
type phaseSemantics struct {
	verifier *SemanticVerifier
	timer    *phaseTimer
	tb       testing.TB
}

func (p *phaseSemantics) VerifyCandidateTransition(ctx context.Context, transition CandidateTransition) error {
	var replay *semanticReplay
	p.timer.run(p.tb, "semantic-prepare", func() error {
		var err error
		replay, err = newSemanticReplay(ctx, p.verifier, transition)
		return err
	})
	var queues *semanticQueueValidation
	p.timer.run(p.tb, "semantic-precheck", func() error {
		var precheck sync.WaitGroup
		var precheckErr error
		precheck.Add(1)
		go func() {
			defer precheck.Done()
			precheckErr = replay.precheckAccountUpdates()
		}()
		var queuesErr error
		queues, queuesErr = replay.prepareQueueValidation()
		precheck.Wait()
		if precheckErr != nil {
			return precheckErr
		}
		return queuesErr
	})
	p.timer.run(p.tb, "semantic-accounts", replay.verifyAccounts)
	p.timer.run(p.tb, "semantic-queue-verify", queues.verifyAfterReplay)
	p.timer.run(p.tb, "semantic-master", replay.verifyMasterSemantics)
	transition.prepared.candidate.shape = replay.shape

	return nil
}

// verifyWithPhases mirrors VerifyShardCandidate. The structural pass is measured
// around the semantic verifier it calls, so its own time is what is left after
// the semantic phases are removed.
func verifyWithPhases(tb testing.TB, req ShardVerificationRequest, timer *phaseTimer) {
	tb.Helper()

	ctx := context.Background()
	semantics, ok := req.Semantics.(*SemanticVerifier)
	if !ok {
		tb.Fatalf("workload semantics is %T, want the production verifier", req.Semantics)
	}
	req.Semantics = &phaseSemantics{verifier: semantics, timer: timer, tb: tb}

	previous := []PreviousBlock{req.Previous}
	if req.Previous2 != nil {
		previous = append(previous, *req.Previous2)
	}
	var prepared *preparedValidationCandidate
	timer.run(tb, "decode", func() error {
		var err error
		prepared, err = prepareVerificationCandidate(
			ctx,
			req.Masterchain.Config,
			req.Candidate,
			previous,
		)
		return err
	})
	timer.run(tb, "structural", func() error {
		return verifyPreparedShardCandidate(ctx, req, prepared)
	})
	timer.nest("structural", verificationPhases[2:]...)
}

func BenchmarkCollateShard(b *testing.B) {
	for _, profile := range benchProfiles {
		b.Run(profile.name, func(b *testing.B) {
			workload := benchWorkloadFor(b, profile)
			builder := testBuilder()
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := builder.BuildShard(ctx, workload.request); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportWorkloadShape(b, workload)
		})
	}
}

func BenchmarkCollateShardPhases(b *testing.B) {
	for _, profile := range benchProfiles {
		b.Run(profile.name, func(b *testing.B) {
			workload := benchWorkloadFor(b, profile)
			builder := testBuilder()
			timer := newPhaseTimer()

			b.ResetTimer()
			for b.Loop() {
				collateWithPhases(b, builder, workload.request, timer)
			}
			b.StopTimer()
			timer.report(b, collationPhases)
			reportWorkloadShape(b, workload)
		})
	}
}

func BenchmarkVerifyShard(b *testing.B) {
	for _, profile := range benchProfiles {
		b.Run(profile.name, func(b *testing.B) {
			workload := benchWorkloadFor(b, profile)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := VerifyShardCandidate(ctx, workload.verification); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportWorkloadShape(b, workload)
		})
	}
}

func BenchmarkVerifyShardPhases(b *testing.B) {
	for _, profile := range benchProfiles {
		b.Run(profile.name, func(b *testing.B) {
			workload := benchWorkloadFor(b, profile)
			timer := newPhaseTimer()

			b.ResetTimer()
			for b.Loop() {
				verifyWithPhases(b, workload.verification, timer)
			}
			b.StopTimer()
			timer.report(b, verificationPhases)
			reportWorkloadShape(b, workload)
		})
	}
}

// reportWorkloadShape prints what the block actually contained, so a ns/op is
// readable as a cost per transaction rather than a cost per profile name.
func reportWorkloadShape(b *testing.B, workload *benchWorkload) {
	stats := workload.candidate.Stats
	b.ReportMetric(float64(stats.Transactions), "tx/block")
	b.ReportMetric(float64(stats.GasUsed), "gas/block")
	b.ReportMetric(float64(len(workload.candidate.BlockBOC)), "blockB")
	b.ReportMetric(float64(len(workload.candidate.CollatedData)), "collatedB")
}
