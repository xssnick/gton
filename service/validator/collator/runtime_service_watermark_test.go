package collator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

// prepareBarrierSession activates a session and arms it for production the way
// runtimeFixture.prepare does, but takes the genesis anchor from the caller so
// a masterchain session can be built as well. It returns the managed session
// because everything these tests observe — the production barrier and the
// empty-block policy — lives there and has no public accessor.
func prepareBarrierSession(
	t *testing.T,
	fixture *runtimeFixture,
	session Session,
	update SessionUpdate,
	genesis ton.BlockIDExt,
) *managedCollatorSession {
	t.Helper()
	if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.ActivateSession(context.Background(), SessionActivation{
		SessionID:      session.ID,
		Genesis:        []ton.BlockIDExt{genesis},
		MinMasterchain: cloneBlockID(update.MasterchainBlock),
	}); err != nil {
		t.Fatal(err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = true
	managed.progressApplied = true
	managed.mu.Unlock()

	return managed
}

func finalizedWatermark(managed *managedCollatorSession) uint32 {
	managed.policyMu.Lock()
	defer managed.policyMu.Unlock()

	return managed.emptyPolicy.LastMCFinalizedSeqno
}

// TestDeferredSessionRefreshAdvancesTheEmptyPolicyWatermark pins the direct
// cause of the seven-of-sixteen hard stop measured on the stand: a leader
// window holds productionMu for all of its slots, so every routine masterchain
// refresh arriving during it is refused by the production barrier. The
// empty-block watermark used to be fed only after that barrier was taken, which
// meant LastMCFinalizedSeqno froze for the whole window while the shard chain
// kept advancing — and the shard rule LastMCFinalizedSeqno+8 < nextSeqno then
// degraded every remaining slot to an empty candidate. If this fails, a
// sixteen-slot window produces real blocks for its first eight slots and empty
// ones for the rest, which is how 3.0% of the shardchain came from a validator
// entitled to 6.7%.
//
// The watermark is a masterchain observation, not a producer decision, so it
// must move while the refresh itself is still only staged. What must NOT move
// is the published record: a staged refresh is not effective state until the
// pipeline accepts it. Masterchain sessions are excluded on purpose — their
// branch of the policy reads LastConsensusFinalizedSeqno, which
// ObserveConsensusFinalized already advances mid-window.
func TestDeferredSessionRefreshAdvancesTheEmptyPolicyWatermark(t *testing.T) {
	for _, test := range []struct {
		name        string
		seed        byte
		masterchain bool
	}{
		{name: "shard advances behind the barrier", seed: 90},
		{name: "masterchain holds", seed: 91, masterchain: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
			defer fixture.close(t)

			session, update := fixture.session(test.seed, 16, 0, time.Now())
			genesis := runtimeTestBlockID(
				session.Shard.Workchain,
				session.Shard.Shard,
				session.CatchainSeqno+1,
			)
			if test.masterchain {
				session.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
				genesis = cloneBlockID(update.MasterchainBlock)
			}
			managed := prepareBarrierSession(t, fixture, session, update, genesis)

			baseline := finalizedWatermark(managed)
			// The exact block the stand hard-stopped on: the shard is nine
			// blocks past what the masterchain has registered, which is one
			// past the +8 the reference validator allows.
			hardStop := CandidateState{NextSeqno: baseline + 9}

			// Held exactly as produceWindow holds it for the whole leader
			// window. Every assertion below is taken while it is held, because
			// the deferred worker applies the staged refresh the moment it is
			// released.
			managed.productionMu.Lock()

			before, err := fixture.service.Session(context.Background(), session.ID)
			if err != nil {
				managed.productionMu.Unlock()
				t.Fatal(err)
			}
			emptyBefore := managed.shouldGenerateEmpty(false, hardStop)

			refresh := before.Update
			refresh.MasterchainBlock = runtimeTestBlockID(
				before.Update.MasterchainBlock.Workchain,
				before.Update.MasterchainBlock.Shard,
				before.Update.MasterchainBlock.SeqNo+1,
			)
			refresh.HasFinalizedBlock = true
			refresh.FinalizedBlock = runtimeTestBlockID(
				session.Shard.Workchain,
				session.Shard.Shard,
				baseline+5,
			)
			err = fixture.service.UpdateSession(context.Background(), refresh)
			after, readErr := fixture.service.Session(context.Background(), session.ID)
			finalized := finalizedWatermark(managed)
			emptyAfter := managed.shouldGenerateEmpty(false, hardStop)
			managed.productionMu.Unlock()

			if !errors.Is(err, ErrSessionUpdateDeferred) {
				t.Fatalf("refresh under the production barrier = %v, want session update deferred", err)
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			// Staging is not acceptance. The published record is what every
			// other caller mirrors, and a refresh the pipeline has not taken
			// may not appear in it.
			if !after.Update.Equal(before.Update) {
				t.Fatalf("staged refresh leaked into the published record: %+v", after.Update)
			}
			if !emptyBefore {
				t.Fatalf(
					"baseline watermark %d already admits next seqno %d; the hard stop is not reproduced",
					baseline,
					hardStop.NextSeqno,
				)
			}

			if test.masterchain {
				if finalized != baseline {
					t.Fatalf(
						"masterchain watermark = %d, want the untouched baseline %d",
						finalized,
						baseline,
					)
				}
				if !emptyAfter {
					t.Fatal("masterchain refresh moved the shard watermark it must not feed")
				}

				return
			}

			if finalized != refresh.FinalizedBlock.SeqNo {
				t.Fatalf(
					"shard watermark = %d, want the staged observation %d (baseline %d)",
					finalized,
					refresh.FinalizedBlock.SeqNo,
					baseline,
				)
			}
			if emptyAfter {
				t.Fatalf(
					"slot at next seqno %d still degrades to empty after observing %d: the window hard-stops",
					hardStop.NextSeqno,
					refresh.FinalizedBlock.SeqNo,
				)
			}
		})
	}
}

// TestRuntimeCollationMustBeEmptyDegradesTheSlot pins the escape hatch the
// per-slot masterchain view pick needs: when no admissible view lets a slot
// carry a real block, the build says so with errCollationMustBeEmpty and the
// slot degrades to an empty candidate, which is a consensus-level skip and
// therefore always protocol-valid.
//
// It must never be retryable. produceWindowWithRetry has no attempt cap and its
// delay saturates at 20ms, so a refusal treated as retryable would spin for the
// rest of the leader window, re-restoring and re-emitting every slot already on
// the wire and producing nothing new. That is strictly worse than the empty
// candidate this degradation emits.
//
// The second case is the one slot that cannot degrade: with no consensus parent
// there is no predecessor for an empty candidate to carry, so the window has to
// end with the error rather than spin on it.
func TestRuntimeCollationMustBeEmptyDegradesTheSlot(t *testing.T) {
	t.Run("degrades one slot and finishes the window", func(t *testing.T) {
		const (
			slots     = uint32(3)
			emptySlot = uint32(1)
		)

		var (
			mu       sync.Mutex
			emits    = make(map[uint32]int, slots)
			refusals int
		)
		pipeline := &runtimeTestPipeline{}
		pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
			if request.Slot == emptySlot {
				mu.Lock()
				refusals++
				mu.Unlock()

				return nil, errCollationMustBeEmpty
			}

			return runtimeBuiltCandidate(request), nil
		}

		emitted := make(chan CandidateArtifact, 2*slots)
		fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
			mu.Lock()
			emits[artifact.Candidate.ID.Slot]++
			mu.Unlock()
			emitted <- artifact

			return nil
		})
		defer fixture.close(t)

		session, update := fixture.session(96, slots, 0, time.Now())
		fixture.prepare(t, session, update)
		if err := fixture.service.CommitDelegation(
			context.Background(),
			fixture.request(t, session, 0),
		); err != nil {
			t.Fatal(err)
		}

		artifacts := make([]CandidateArtifact, 0, slots)
		for range slots {
			artifacts = append(artifacts, runtimeAwaitArtifact(t, emitted))
		}
		for i, artifact := range artifacts {
			if artifact.Candidate.ID.Slot != uint32(i) {
				t.Fatalf("candidate %d is for slot %d, want %d", i, artifact.Candidate.ID.Slot, i)
			}
		}
		if artifacts[0].Candidate.Empty {
			t.Fatalf("slot 0 = %+v, want an ordinary collated block", artifacts[0].Candidate)
		}
		degraded := artifacts[emptySlot].Candidate
		if !degraded.Empty {
			t.Fatalf("refused slot %d = %+v, want an empty candidate", emptySlot, degraded)
		}
		// Without this the empty candidate could just as well have come from
		// the empty-block policy, and the sentinel path would be untested.
		mu.Lock()
		refused := refusals
		mu.Unlock()
		if refused == 0 {
			t.Fatalf("slot %d degraded without the build ever refusing it", emptySlot)
		}
		// An empty candidate is a skip: it carries the predecessor's block, and
		// it chains to the candidate it followed so the consensus lineage stays
		// unbroken across the degradation.
		if !sameBlockID(degraded.Block, artifacts[0].Candidate.Block) {
			t.Fatalf(
				"empty candidate block = %+v, want the predecessor %+v",
				degraded.Block,
				artifacts[0].Candidate.Block,
			)
		}
		if degraded.Parent != simplex.Parent(artifacts[0].Candidate.ID) {
			t.Fatalf(
				"empty candidate parent = %+v, want %+v",
				degraded.Parent,
				simplex.Parent(artifacts[0].Candidate.ID),
			)
		}
		// The slot after the refused one collates normally: the degradation is
		// one slot's verdict, not the window's.
		next := artifacts[emptySlot+1].Candidate
		if next.Empty {
			t.Fatalf("slot %d = %+v, want an ordinary collated block", emptySlot+1, next)
		}
		if next.Parent != simplex.Parent(degraded.ID) {
			t.Fatalf("slot %d parent = %+v, want %+v", emptySlot+1, next.Parent, simplex.Parent(degraded.ID))
		}

		runtimeAwait(t, func() bool {
			status, statusErr := fixture.service.Status(context.Background())

			return statusErr == nil && status.CompletedWindows == 1 && status.ActiveWindows == 0
		})
		status, err := fixture.service.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if status.FailedWindows != 0 {
			t.Fatalf("failed windows = %d, want the degraded slot to keep the window healthy", status.FailedWindows)
		}
		// The regression signal for "not retried": a window sent back through
		// produceWindowWithRetry restores every slot it already emitted from its
		// marker and emits it again, so a second delivery of any slot is the
		// spin this sentinel exists to prevent.
		mu.Lock()
		deliveries := make(map[uint32]int, len(emits))
		for slot, count := range emits {
			deliveries[slot] = count
		}
		mu.Unlock()
		if len(deliveries) != int(slots) {
			t.Fatalf("emitted slots = %v, want exactly %d", deliveries, slots)
		}
		for slot, count := range deliveries {
			if count != 1 {
				t.Fatalf("slot %d emitted %d times, want once: the window was retried", slot, count)
			}
		}
	})

	t.Run("window ends when no parent can carry the skip", func(t *testing.T) {
		pipeline := &runtimeTestPipeline{}
		pipeline.build = func(context.Context, BuildRequest) (*Candidate, error) {
			return nil, errCollationMustBeEmpty
		}
		emitted := make(chan CandidateArtifact, 4)
		fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		})
		defer fixture.close(t)

		// No selected consensus base, so the producer opens its first slot on
		// the genesis parent and signEmptyArtifact has nothing to point at.
		session, update := fixture.session(97, 2, 0, time.Now())
		if update.CurrentBase.Exists {
			t.Fatal("fixture window already carries a consensus base")
		}
		fixture.prepare(t, session, update)
		if err := fixture.service.CommitDelegation(
			context.Background(),
			fixture.request(t, session, 0),
		); err != nil {
			t.Fatal(err)
		}

		runtimeAwait(t, func() bool {
			status, statusErr := fixture.service.Status(context.Background())

			return statusErr == nil && status.FailedWindows == 1 && status.ActiveWindows == 0
		})
		status, err := fixture.service.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(status.LastError, errCollationMustBeEmpty.Error()) {
			t.Fatalf("window ended with %q, want the must-be-empty refusal", status.LastError)
		}
		// FailedWindows is incremented after produceWindowWithRetry returns, so
		// the retry loop is provably done and the build count is final. One
		// attempt is the whole point: the refusal is not retryable.
		if builds := pipeline.buildCount(); builds != 1 {
			t.Fatalf("build attempts = %d, want 1: the refused window spun", builds)
		}
		if len(emitted) != 0 {
			t.Fatalf("emitted %d candidates, want none from a window that never had a parent", len(emitted))
		}

		managed, err := fixture.service.runningSession(session.ID)
		if err != nil {
			t.Fatal(err)
		}
		managed.mu.Lock()
		window := managed.authorizations[WindowID{SessionID: session.ID}]
		jobs := len(managed.productions)
		managed.mu.Unlock()
		if window.State != delegatedAuthorizationCancelled || jobs != 0 {
			t.Fatalf("terminal refusal outcome: state=%d jobs=%d, want cancelled/zero", window.State, jobs)
		}
	})
}
