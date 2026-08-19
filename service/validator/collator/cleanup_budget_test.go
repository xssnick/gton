package collator

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// budgetProfile is a queue-heavy shape whose entries are all covered by the
// authenticated masterchain frontier, so cleanup has real work to stop in the
// middle of and the resulting block can be handed to our own validator.
var budgetProfile = benchProfile{name: "budget", accounts: 2_000, queued: 1_200}

// buildShardBudgetedCleanup mirrors buildShardAttemptPaced and arms the cleanup
// budget immediately before the cleanup phase, so the measured window covers
// cleanup alone rather than cleanup plus everything prepare does first.
func buildShardBudgetedCleanup(
	b *Builder,
	ctx context.Context,
	req ShardRequest,
	budget time.Duration,
) (*Candidate, error) {
	c, err := b.prepare(ctx, req)
	if err != nil {
		return nil, err
	}
	if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
		return nil, err
	}
	if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		return nil, err
	}
	c.req.queueCleanupUntil = time.Now().Add(budget)
	if err = c.cleanupOutQueue(); err != nil {
		return nil, err
	}
	if err = c.processDispatchQueue(); err != nil {
		return nil, err
	}
	if err = c.processInternals(); err != nil {
		return nil, err
	}
	if err = c.processExternals(); err != nil {
		return nil, err
	}
	if err = c.processNewMessages(
		c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete(),
	); err != nil {
		return nil, err
	}
	return c.finishShard()
}

// verifyBudgetedCandidate runs the real rule over the candidate:
// NewSemanticVerifier on the live TVM, never testCandidateTransitionVerifier or
// acceptingCandidateTransitionVerifier — the accepting stub answers yes to
// everything and would make every assertion below vacuous.
//
// The predecessor state is handed over RESIDENT. That is deliberate and it is
// what makes the own-shard rule the only thing under test: with a full tree
// there is no pruned branch anywhere in the input, so a rejection cannot be a
// proof-coverage artefact. Registered shard neighbours are given the same
// resident queue for the same reason; the masterchain neighbour is exempt
// because its queue travels in the already-authenticated Masterchain view.
func verifyBudgetedCandidate(t *testing.T, req ShardRequest, candidate *Candidate) {
	t.Helper()

	resident := loadPreviousShardState(t, req)
	neighbors := append([]Neighbor(nil), req.Neighbors...)
	for i := range neighbors {
		if neighbors[i].Block.Workchain == address.MasterchainID {
			continue
		}
		neighbors[i].OutMsgQueueInfo = resident.OutMsgQueueInfo
	}
	if err := VerifyShardCandidate(context.Background(), ShardVerificationRequest{
		Previous:           req.Previous,
		Masterchain:        req.Masterchain,
		Neighbors:          neighbors,
		NeighborShardEndLT: req.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}); err != nil {
		t.Fatalf("a partially cleaned candidate was rejected by our own validator: %v", err)
	}
}

// TestQueueCleanupUntilDerivation pins the port of Collator::queue_cleanup_timeout_
// (collator.cpp:85-97) including the case that matters most here: a zero soft
// deadline leaves the budget inert, which is what keeps every deterministic
// entry point reproducing its pre-budget candidate.
func TestQueueCleanupUntilDerivation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	soft := now.Add(time.Second)

	for _, tc := range []struct {
		name              string
		soft              time.Time
		externalWaitUntil time.Time
		want              time.Time
	}{
		{"inert without a soft deadline", time.Time{}, now.Add(time.Hour), time.Time{}},
		// Shardchain: the external wait is the slot boundary and the soft
		// deadline one target rate later, so the budget is the soft deadline.
		{"external wait before soft", soft, now.Add(100 * time.Millisecond), soft},
		{"external wait after soft", soft, soft.Add(time.Second), soft.Add(time.Second)},
		// Masterchain: no external wait, so a quarter of what is left.
		{"quarter of the remaining budget", soft, time.Time{}, now.Add(250 * time.Millisecond)},
		{"already past the soft deadline", now.Add(-time.Second), time.Time{}, now},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := queueCleanupUntil(now, tc.soft, tc.externalWaitUntil)
			if !got.Equal(tc.want) {
				t.Fatalf("queueCleanupUntil = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQueueCleanupBudgetIsInertByDefault guards the whole golden suite against
// accidental arming: BuildShard leaves QueueCleanupUntil zero, so its candidate
// must be byte-identical to one built with a budget that cannot fire.
func TestQueueCleanupBudgetIsInertByDefault(t *testing.T) {
	req := benchRequest(t, budgetProfile)
	if !req.QueueCleanupUntil.IsZero() {
		t.Fatal("the fixture armed the budget; the test would prove nothing")
	}
	first, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Stats.QueueCleaned != uint32(budgetProfile.queued) {
		t.Fatalf("cleaned %d entries, want the whole queue of %d",
			first.Stats.QueueCleaned, budgetProfile.queued)
	}
	if first.Stats.QueueCleanupStop != CleanupStopExhausted {
		t.Fatalf("stop reason = %s, want exhausted", first.Stats.QueueCleanupStop)
	}

	armed := benchRequest(t, budgetProfile)
	armed.QueueCleanupUntil = time.Now().Add(time.Hour)
	second, err := testBuilder().BuildShard(context.Background(), armed)
	if err != nil {
		t.Fatal(err)
	}
	if second.Stats.QueueCleanupStop != CleanupStopExhausted {
		t.Fatalf("armed-far-ahead stop reason = %s, want exhausted", second.Stats.QueueCleanupStop)
	}
	if !bytes.Equal(first.BlockBOC, second.BlockBOC) {
		t.Fatal("a budget that never fires changed the block")
	}
}

// TestQueueCleanupStopsAtBudget arms the budget in the past. Cleanup must
// dequeue nothing, report the budget as the stop reason, and still produce a
// block our own validator accepts.
//
// That holds because every one of budgetProfile's 1200 entries is bound for a
// NEIGHBOR — the masterchain — and no ordinary block owes a neighbor
// exhaustiveness: dequeues are checked one descriptor at a time
// (semanticQueueValidation.dequeue), and only verifyMergedQueueCleanup — gated
// on BlockInfo.AfterMerge — demands that everything deliverable is gone. C++
// agrees: check_delivered_dequeued runs only under after_merge_.
//
// It does NOT generalize to the own-shard half of the queue, which every
// validator does check entry by entry. See
// TestQueueCleanupBudgetNeverTruncatesTheOwnShardHalf.
func TestQueueCleanupStopsAtBudget(t *testing.T) {
	req := benchRequest(t, budgetProfile)
	req.QueueCleanupUntil = time.Now().Add(-time.Hour)

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("an expired cleanup budget aborted the build: %v", err)
	}
	if candidate.Stats.QueueCleaned != 0 {
		t.Fatalf("dequeued %d entries past an expired budget", candidate.Stats.QueueCleaned)
	}
	if candidate.Stats.QueueCleanupStop != CleanupStopBudget {
		t.Fatalf("stop reason = %s, want budget", candidate.Stats.QueueCleanupStop)
	}
	if candidate.Stats.OutQueueSize != uint64(budgetProfile.queued) {
		t.Fatalf("out queue size %d, want the untouched %d",
			candidate.Stats.OutQueueSize, budgetProfile.queued)
	}
	verifyBudgetedCandidate(t, req, candidate)
}

// TestQueueCleanupStopsPartwayThroughBudget cuts cleanup in the middle rather
// than before it starts, and requires the partial result to validate. Together
// with the expired case above this pins "early stop is legal" as a tested rule
// rather than a comment.
func TestQueueCleanupStopsPartwayThroughBudget(t *testing.T) {
	req := benchRequest(t, budgetProfile)
	started := time.Now()
	full, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	whole := time.Since(started)
	if full.Stats.QueueCleaned == 0 {
		t.Fatal("fixture dequeued nothing")
	}

	// The cut point is timing-dependent by construction: what is asserted is
	// that a cut happened at all and that what came out is still a valid block.
	var partial *Candidate
	for _, share := range []int{4, 16, 64, 256} {
		budgeted := benchRequest(t, budgetProfile)
		partial, err = buildShardBudgetedCleanup(
			testBuilder(), context.Background(), budgeted, whole/time.Duration(share),
		)
		if err != nil {
			t.Fatalf("budgeted build: %v", err)
		}
		if partial.Stats.QueueCleanupStop == CleanupStopBudget &&
			partial.Stats.QueueCleaned > 0 &&
			partial.Stats.QueueCleaned < full.Stats.QueueCleaned {
			t.Logf("cleanup stopped at %d of %d entries", partial.Stats.QueueCleaned, full.Stats.QueueCleaned)
			verifyBudgetedCandidate(t, budgeted, partial)
			return
		}
	}
	t.Skipf("cleanup never stopped partway on this machine (cleaned %d of %d, stop %s)",
		partial.Stats.QueueCleaned, full.Stats.QueueCleaned, partial.Stats.QueueCleanupStop)
}

// The budget must be derived from the instant the BUILD started, not from the
// instant acquisition finished, and that is what C++ does: queue_cleanup_timeout_
// is computed in the Collator constructor (collator.cpp:85-97), before start_up
// issues a single query for a predecessor state, a masterchain state or a shard
// block.
//
// The masterchain branch is the one that differs at all — with an external wait
// the budget is max(wait, soft) and no clock reading enters it. There, with no
// external wait, the budget is a quarter of what REMAINS of the soft deadline,
// so a reading taken after acquisition measures the quarter over a remainder
// acquisition has already eaten: the window shrinks in proportion to how slow
// the node's state loads were and reaches exactly zero when acquisition reaches
// the soft deadline. Derived at build start it is a fixed quarter of the slot,
// whatever acquisition cost — which is the property this pins, and the one C++
// has.
//
// What it deliberately does NOT claim: that this hands cleanup more wall clock.
// f(x) = x + (soft-x)/4 is increasing in x, so the post-acquisition deadline is
// always the later INSTANT of the two, and an acquisition past a quarter of the
// slot leaves a build-start budget that has already expired — in C++ too, where
// the queries run under the same constructor-set timeout. The defect fixed here
// is that our budget depended on acquisition latency at all; the reference's
// does not.
func TestQueueCleanupBudgetDoesNotDependOnAcquisitionLatency(t *testing.T) {
	const slot = 2 * time.Second
	started := time.Unix(1_700_000_000, 0)
	soft := started.Add(slot)
	want := slot / 4

	for _, share := range []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1} {
		acquired := started.Add(time.Duration(float64(slot) * share))
		atStart := queueCleanupUntil(started, soft, time.Time{})
		atAcquisition := queueCleanupUntil(acquired, soft, time.Time{})

		if got := atStart.Sub(started); got != want {
			t.Fatalf("acquisition at %.0f%% of the slot moved the build-start window to %v, want %v",
				share*100, got, want)
		}
		if share == 1 && !atAcquisition.Equal(acquired) {
			t.Fatalf("the control is wrong: an acquisition that ran to the soft deadline left %v",
				atAcquisition.Sub(acquired))
		}
		t.Logf("acquisition %5.0f%% of a %v slot: window %v from build start, %v from acquisition",
			share*100, slot, atStart.Sub(started), atAcquisition.Sub(acquired))
	}

	// The shard branch takes neither reading: an external wait pins the budget to
	// an absolute instant, so acquisition latency cannot enter it at all.
	wait := soft.Add(500 * time.Millisecond)
	if a, b := queueCleanupUntil(started, soft, wait), queueCleanupUntil(soft, soft, wait); !a.Equal(b) {
		t.Fatalf("the external-wait branch moved with the clock: %v against %v", a, b)
	}
}

// And the call site itself, because the derivation above is correct for any
// instant and the defect was which instant it was handed. BuildCandidate takes
// one clock reading before acquisition — assembly, the span the collation-stage
// metric is measured against — and both branches must derive the budget from
// that identifier and not from a fresh reading taken after AcquireMaster or
// AcquireShard returned.
func TestQueueCleanupBudgetIsDerivedFromTheBuildStartInstant(t *testing.T) {
	var arguments []string
	forEachCall(t, "local_acquisition_build.go", "BuildCandidate", func(call *ast.CallExpr) {
		callee, ok := call.Fun.(*ast.Ident)
		if !ok || callee.Name != "queueCleanupUntil" || len(call.Args) == 0 {
			return
		}
		if ident, isIdent := call.Args[0].(*ast.Ident); isIdent {
			arguments = append(arguments, ident.Name)
			return
		}
		arguments = append(arguments, "<not an identifier>")
	})
	if len(arguments) != 2 {
		t.Fatalf("BuildCandidate derives the cleanup budget %d times, want once per chain", len(arguments))
	}
	for i, argument := range arguments {
		if argument != "assembly" {
			t.Fatalf("branch %d derives the cleanup budget from %q, want the build-start instant %q",
				i, argument, "assembly")
		}
	}
}

// staleOwnQueueRequest builds the shape the six budget tests above never build,
// which is why none of them caught the defect this test pins. budgetProfile's
// 1200 queue entries are all masterchain-bound and its predecessor carries no
// processed records at all, so the own-shard part of cleanup has zero work in
// every one of them and can be truncated without consequence.
//
// Here the predecessor's OWN out-queue holds entries destined back into this
// shard which the predecessor's own ProcessedInfo already covers: this shard
// processed them and owes a dequeue descriptor for each. Their lts are
// distinct, so nothing below depends on the enqueued_lt tie mechanism.
//
// One fresh message is offered on top. Without it the candidate would leave
// ProcessedInfo unchanged, verifyProcessedInfo would return before its source
// loop, and no validator would ever walk the queue.
//
// capFullCollatedData is ON here, and that is not decoration: it is the
// configuration the live three-node incident ran in, and it is the only one in
// which the collated proof exists at all. Without it collated data degenerates
// to a 33-byte marker, the proof-size estimator is never installed, and
// traceProcessedQueueValidationClosure records its cells into nothing — so
// every test built on this fixture would be exercising a different program from
// the one that failed. Every consumer of this fixture is a
// cleanup/proof-closure test and every one of them needs it.
func staleOwnQueueRequest(t *testing.T, stale int) ShardRequest {
	t.Helper()

	req := emptyCandidateRequest(t)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	startLT := requestStartLT(t, req)
	fee := tlb.FromNanoTONU(100_000)

	receivers := make([]*address.Address, stale+1)
	contracts := make([]activeContract, 0, len(receivers))
	for i := range receivers {
		receivers[i] = address.NewAddress(0, 0, bytes.Repeat([]byte{byte(0x40 + i)}, 32))
		contracts = append(contracts, activeContract{
			address: receivers[i],
			code:    externalAcceptCode(t),
			balance: 10_000_000_000,
		})
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		activeContracts(t, req.Header.GenUtime, contracts...))

	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x21}, 32))
	for i := range stale {
		msg, enqueued := queuedInternal(t, source, receivers[i], startLT-100+uint64(i),
			req.Header.GenUtime-1, fee, fee, 96,
			msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
		req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	}

	// The predecessor's own frontier covers every one of them.
	bound := cell.BeginCell().
		MustStoreUInt(startLT-100+uint64(stale), 64).
		MustStoreSlice(bytes.Repeat([]byte{0xff}, 32), 256)
	covering := processedDictionary(t, msgpool.ShardAll, req.Masterchain.ID.SeqNo-1, bound)
	rewritePreviousShardState(t, &req, func(state *tlb.ShardStateUnsplit) {
		var queue tlb.OutMsgQueueInfo
		if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
			t.Fatal(err)
		}
		queue.ProcInfo = covering
		rewritten, err := queue.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		state.OutMsgQueueInfo = rewritten
	})

	fresh, freshEnqueued := queuedInternal(t, source, receivers[stale], startLT-1,
		req.Header.GenUtime-1, fee, fee, 96,
		msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, fresh.Key, freshEnqueued)
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{fresh}}

	queueSize := uint64(stale + 1)
	req.Previous.OutQueueSize = &queueSize
	// The registered set is the predecessor's own shard plus the masterchain,
	// which is what capFullCollatedData demands: collated data must carry a proof
	// for every registered neighbour, and a bare masterchainNeighbor leaves the
	// shard's own registration unaccounted for. It also matches what the incident
	// network was running. The predecessor entry is withheld and replaced by the
	// local predecessor inside normalizeTrivialNeighbors, so the effective set —
	// and therefore everything cleanup does — is the same one either way.
	attachFullCollatedTestNeighbors(t, &req)
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return startLT }
	bindZerostatePredecessor(t, &req)

	return req
}

// bindZerostatePredecessor makes the fixture's synthetic predecessor acceptable
// to capFullCollatedData without a predecessor block.
//
// The capability binds Previous.ID.RootHash to Previous.State through the
// predecessor block's state update (buildPreviousBlockStateProof), and this
// fixture has no such block: the state it needs — the shard's own frontier
// covering entries that are still queued — is one no VALID predecessor can
// hand over, which is exactly why the defect took a live network to find. A
// block that newly covers a queued own entry it did not import is rejected by
// verifySourceProcessed for "newly processed queue message ... has no exact
// InMsg", so collating the parent cannot produce this shape; only the residual
// documented on cleanupOutQueue — a post-merge frontier whose coverage is not a
// prefix of the stream's rank — lets a real chain reach it.
//
// prepare takes the other branch of the same binding for a seqno-0 predecessor
// (build.go:475-479): the id's root hash is compared against the state root
// directly, because there is no preceding block cell. Pointing the fixture at
// that branch costs one field and one hash and leaves everything under test —
// the queue, the frontier, the drain, the proof closure — exactly as it was.
func bindZerostatePredecessor(t *testing.T, req *ShardRequest) {
	t.Helper()

	rewritePreviousShardState(t, req, func(state *tlb.ShardStateUnsplit) { state.Seqno = 0 })
	req.Previous.ID.SeqNo = 0
	req.Previous.ID.RootHash = req.Previous.State.WithoutTrace().Hash()
	req.Previous.Block = nil
	for i := range req.Neighbors {
		if req.Neighbors[i].Block.Workchain == req.Previous.ID.Workchain &&
			req.Neighbors[i].Block.Shard == req.Previous.ID.Shard {
			req.Neighbors[i].Block = *req.Previous.ID.Copy()
		}
	}
	for i := range req.Masterchain.Groups.Active {
		session := &req.Masterchain.Groups.Active[i]
		if session.Shard != req.Shard || len(session.Registered) != 1 ||
			session.Registered[0].Shard != req.Shard {
			continue
		}
		session.Registered[0].Block = *req.Previous.ID.Copy()
	}
}

// TestQueueCleanupBudgetNeverTruncatesTheOwnShardHalf is the one bound that is
// not a bound: an expired budget may leave a neighbor's entries queued, but
// never one of our own that we have already processed.
//
// Before the own-shard half was lifted out of the budgeted round-robin the
// expired arm dequeued nothing, and the block it produced was rejected by a
// RESIDENT-state validator — no proofs, no pruned cells anywhere in the input —
// with "processed message ... remains in the local predecessor queue without a
// dequeue descriptor". C++ rejects the same block in the same words
// (validate-query.cpp:5283-5289). The budget is the only variable between the
// two arms below.
func TestQueueCleanupBudgetNeverTruncatesTheOwnShardHalf(t *testing.T) {
	const stale = 6

	for _, arm := range []struct {
		name   string
		budget time.Duration
		stop   CleanupStopReason
	}{
		{"budget inert", time.Hour, CleanupStopExhausted},
		{"budget already expired", -time.Hour, CleanupStopBudget},
	} {
		t.Run(arm.name, func(t *testing.T) {
			req := staleOwnQueueRequest(t, stale)
			candidate, err := buildShardBudgetedCleanup(
				testBuilder(), context.Background(), req, arm.budget)
			if err != nil {
				t.Fatal(err)
			}
			// The validator runs first so a regression reports the symptom
			// that took a live network down rather than a counter.
			verifyBudgetedCandidate(t, req, candidate)
			if candidate.Stats.QueueCleaned != stale {
				t.Fatalf("cleaned %d own-shard entries, want all %d: the budget truncated work a validator requires",
					candidate.Stats.QueueCleaned, stale)
			}
			if candidate.Stats.QueueCleanupStop != arm.stop {
				t.Fatalf("cleanup stop = %v, want %v", candidate.Stats.QueueCleanupStop, arm.stop)
			}
			// Cleanup takes the stale entries and the import takes the fresh
			// one, which is delivered in this same block.
			if candidate.Stats.OutQueueSize != 0 {
				t.Fatalf("out queue size = %d, want an empty queue", candidate.Stats.OutQueueSize)
			}
			// capFullCollatedData is ON through staleOwnQueueRequest, and the
			// arm above is only the configuration the incident ran in while it
			// stays on: without it collated data is a 33-byte marker and the
			// proof half of this fixture is not exercised at all.
			if len(candidate.CollatedData) <= 33 {
				t.Fatalf("collated data is %d bytes, i.e. the bare marker: "+
					"capFullCollatedData is not in effect", len(candidate.CollatedData))
			}
		})
	}
}

// TestQueueCleanupDrainsOwnShardWithoutRegisteredNeighbours covers the case the
// mandatory drain used to skip in full: an EMPTY registered neighbour set.
//
// Cleanup returned before anything ran whenever the masterchain had registered
// nobody, on the reasoning that with no neighbours there is nobody to clean up
// for. The predecessor is a neighbour of itself, so that reasoning is wrong in
// exactly the half that cannot be deferred: normalizeTrivialNeighbors
// contributes the local predecessor whether or not the registered list is
// empty, and its already-processed own entries still owe a dequeue descriptor
// in this block.
//
// capFullCollatedData is deliberately OFF here, and this is the only test in
// the file where it is: staleOwnQueueRequest turns it ON and every other
// consumer keeps it. The two configurations are mutually exclusive here rather
// than by preference — the capability requires a collated proof for every
// neighbour the masterchain registered (collated.go:300-316), and this fixture
// removes the neighbours while the masterchain groups still register two, so
// leaving it on fails the build outright with "full collated data is missing 2
// registered neighbors" before cleanup runs at all. That was measured, not
// assumed.
//
// Nothing under test is lost with it off. What the capability adds is proof
// coverage, and the rejection this test exists for is a RESIDENT-state one:
// verifyBudgetedCandidate hands the validator a full predecessor tree with no
// pruned cell anywhere in it. That is the whole point of defect 2 — the block
// is invalid, not the proof.
//
// Verification runs NewSemanticVerifier on the live TVM through
// verifyBudgetedCandidate, never an accepting stub.
func TestQueueCleanupDrainsOwnShardWithoutRegisteredNeighbours(t *testing.T) {
	const stale = 5

	req := staleOwnQueueRequest(t, stale)
	req.Masterchain.Config.capabilities &^= capFullCollatedData
	req.Neighbors = nil

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// The validator first, so a regression reports the symptom that took the
	// live network down rather than a counter.
	verifyBudgetedCandidate(t, req, candidate)
	if candidate.Stats.QueueCleaned != stale {
		t.Fatalf("cleaned %d own-shard entries with no registered neighbours, want all %d",
			candidate.Stats.QueueCleaned, stale)
	}
	if candidate.Stats.OutQueueSize != 0 {
		t.Fatalf("out queue size = %d, want an empty queue", candidate.Stats.OutQueueSize)
	}
}

// TestMandatoryDequeueOverflowFailsTheCollation pins the resolution of the one
// collision the mandatory drain cannot argue its way out of: the drain may not
// be truncated, and it may not push the block past its hard limits. Both
// demands are absolute, so the collation fails and the slot is lost — which is
// strictly better than the two alternatives, a block every peer rejects for a
// missing dequeue descriptor, or a block that violates ParamLimits.
//
// The fixture runs under capFullCollatedData (staleOwnQueueRequest) because the
// collated-data axis is one of the four the drain can push over, and because it
// is the configuration the incident ran in. The control arm asserts the same
// fixture under its configured limits still produces a block our own
// NewSemanticVerifier accepts, so the failing arm is attributable to the
// ceiling and to nothing else.
func TestMandatoryDequeueOverflowFailsTheCollation(t *testing.T) {
	const stale = 6

	req := staleOwnQueueRequest(t, stale)
	control, err := buildShardBudgetedCleanup(testBuilder(), context.Background(), req, time.Hour)
	if err != nil {
		t.Fatalf("control build: %v", err)
	}
	verifyBudgetedCandidate(t, req, control)
	if control.Stats.QueueCleaned != stale {
		t.Fatalf("control cleaned %d entries, want %d", control.Stats.QueueCleaned, stale)
	}
	// Guard against a fixture that proves nothing: without real collated data
	// the capability is not in effect and the collated axis cannot move.
	if len(control.CollatedData) <= 33 {
		t.Fatalf("collated data is %d bytes, i.e. the bare marker: capFullCollatedData is not in effect",
			len(control.CollatedData))
	}

	c, err := testBuilder().prepare(context.Background(), staleOwnQueueRequest(t, stale))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
		t.Fatal(err)
	}
	if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		t.Fatal(err)
	}
	// A hard byte ceiling one byte above where the drain starts. Cleanup is the
	// first phase of a shard collation, so nothing but the drain can be blamed
	// for crossing it.
	entry := c.limits.estimatedBytes()
	c.limits.limits.bytes = limitThresholds{entry, entry, entry, entry + 1}

	err = c.cleanupOutQueue()
	if !errors.Is(err, ErrMandatoryDequeueOverflow) {
		t.Fatalf("cleanup error = %v, want ErrMandatoryDequeueOverflow", err)
	}
	// The message is the operator's only view of a slot that produced nothing,
	// so it has to carry the axis and the progress, not just the verdict.
	for _, want := range []string{"estimated block bytes", "dequeued 1 already-processed own-shard entries"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not report %q", err, want)
		}
	}
	if errors.Is(err, ErrSizeLimit) {
		t.Fatal("the overflow must not be a size-limit error: retryUnderSizeLimit would narrow the byte " +
			"budget and burn two more attempts reaching the identical verdict")
	}
	t.Logf("collation refused to ship: %v", err)
}

// mandatoryDrainFixture prepares a collation over the stale fixture and returns
// it with the predecessor's own cleanup part, the one cleanupOutQueue drains
// outside every discretionary bound. Driving the drain directly is what lets
// the tests below put the byte ceiling exactly where they want it relative to
// the work, which is not expressible through a whole build.
func mandatoryDrainFixture(t *testing.T, stale int) (*collation, *cleanupPart) {
	t.Helper()

	c, err := testBuilder().prepare(context.Background(), staleOwnQueueRequest(t, stale))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
		t.Fatal(err)
	}
	if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		t.Fatal(err)
	}
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		t.Fatal(err)
	}
	for i := range neighbors {
		if neighbors[i].Shard != c.shard {
			continue
		}
		stream, streamErr := c.neighborQueueStream(neighbors[i].Shard)
		if streamErr != nil {
			t.Fatal(streamErr)
		}
		return c, &cleanupPart{neighbor: neighbors[i], stream: stream}
	}
	t.Fatal("the effective neighbour set carries no predecessor part, so there is no mandatory drain")

	return nil, nil
}

// TestMandatoryDequeueOverflowBoundary pins the two things
// TestMandatoryDequeueOverflowFailsTheCollation leaves open: WHERE the ceiling
// is, and where the check sits relative to the dequeue.
//
// The boundary is not decorative. hardOverflow reports an axis standing AT or
// past its hard threshold (limits.go, `>=`), the same comparison
// block-limit-status uses, so a drain that lands exactly on the limit is over
// it. Both arms below are one byte apart and disagree, which is the only way to
// pin a comparison rather than its neighbourhood.
//
// The position matters because the two mutations are silent in opposite
// directions. Checking BEFORE the step turns a block that was already at its
// ceiling for other reasons into a failed slot even when the drain has nothing
// to do — and the drain having nothing to do is the ordinary case, since
// importInternal dequeues an offered own entry in the same block. Checking
// after it, as the code does, means the drain always makes progress before it
// refuses, and an empty drain never refuses at all.
func TestMandatoryDequeueOverflowBoundary(t *testing.T) {
	const stale = 6

	// What a completed drain actually costs, measured rather than guessed.
	measured, local := mandatoryDrainFixture(t, stale)
	entry := measured.limits.estimatedBytes()
	if err := measured.cleanupMandatoryDrain(local); err != nil {
		t.Fatalf("drain under the fixture's own limits: %v", err)
	}
	if measured.stats.QueueCleaned != stale {
		t.Fatalf("the drain took %d entries, want all %d", measured.stats.QueueCleaned, stale)
	}
	peak := measured.limits.estimatedBytes()
	if peak <= entry {
		t.Fatalf("the drain moved the estimate from %d to %d, so the ceiling below is not a ceiling on it",
			entry, peak)
	}
	t.Logf("a complete drain of %d entries takes the estimate from %d to %d bytes", stale, entry, peak)

	for _, arm := range []struct {
		name string
		// hard is the byte ceiling, relative to what a complete drain reaches.
		hard uint64
		fail bool
	}{
		{name: "one byte above the completed drain", hard: peak + 1},
		{name: "exactly at the completed drain", hard: peak, fail: true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			c, part := mandatoryDrainFixture(t, stale)
			c.limits.limits.bytes = limitThresholds{arm.hard, arm.hard, arm.hard, arm.hard}

			err := c.cleanupMandatoryDrain(part)
			if arm.fail {
				if !errors.Is(err, ErrMandatoryDequeueOverflow) {
					t.Fatalf("drain error = %v, want ErrMandatoryDequeueOverflow at the boundary", err)
				}
				t.Logf("refused at the boundary: %v", err)
				return
			}
			if err != nil {
				t.Fatalf("a ceiling one byte above a complete drain refused it: %v", err)
			}
			if c.stats.QueueCleaned != stale {
				t.Fatalf("the drain took %d entries under a ceiling it fits, want %d",
					c.stats.QueueCleaned, stale)
			}
			if what, value, limit, over := c.limits.hardOverflow(); over {
				t.Fatalf("the completed drain left %s at %d against %d, so the arms are not one byte apart",
					what, value, limit)
			}
		})
	}

	// The check's position: a block already past its ceiling before the drain
	// starts. The drain must still take an entry — it is mandatory work — and
	// refuse after it, naming the one dequeue it made. A check moved ahead of
	// the step reports zero.
	t.Run("already over before the drain", func(t *testing.T) {
		c, part := mandatoryDrainFixture(t, stale)
		below := c.limits.estimatedBytes() - 1
		c.limits.limits.bytes = limitThresholds{below, below, below, below}

		err := c.cleanupMandatoryDrain(part)
		if !errors.Is(err, ErrMandatoryDequeueOverflow) {
			t.Fatalf("drain error = %v, want ErrMandatoryDequeueOverflow", err)
		}
		if !strings.Contains(err.Error(), "dequeued 1 already-processed own-shard entries") {
			t.Fatalf("error %q does not report the dequeue it made before refusing", err)
		}
		if c.stats.QueueCleaned != 1 {
			t.Fatalf("the drain dequeued %d entries before refusing, want exactly 1", c.stats.QueueCleaned)
		}
	})

	// And the same ceiling with nothing to drain. The predecessor's own
	// frontier covers none of the queue, so the mandatory half is empty and the
	// overflow it never caused must not be reported.
	t.Run("already over with an empty drain", func(t *testing.T) {
		c, part := mandatoryDrainFixture(t, 0)
		below := c.limits.estimatedBytes() - 1
		c.limits.limits.bytes = limitThresholds{below, below, below, below}

		if err := c.cleanupMandatoryDrain(part); err != nil {
			t.Fatalf("an empty mandatory drain refused a block it did not grow: %v", err)
		}
		if c.stats.QueueCleaned != 0 {
			t.Fatalf("the empty drain dequeued %d entries", c.stats.QueueCleaned)
		}
	})
}
