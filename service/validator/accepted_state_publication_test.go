package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// nodeLiveView models the node's live view the way the running node wires it: a
// read answers only for what has been published, and acceptance is what
// publishes. It is deliberately NOT a store — nothing here ever becomes readable
// by being committed, because the case under test is the one where no commit is
// coming: a shard block only this node has finalized so far, whose applied state
// the sync pipeline cannot produce until a masterchain block carrying this shard
// top is applied.
type nodeLiveView struct {
	mu        sync.Mutex
	tips      map[string]ChainTip
	loads     int
	published int
	// requireWait models the backend the resolver actually talks to: with
	// ChainStateRequest.Wait false, LocalSessionBackend.loadChainTip reads once and
	// hands back the not-ready error instead of waiting for the publication edge.
	requireWait bool
	// beforeLoad runs at the start of a load, outside this stub's lock. It is how a
	// concurrent resolve winning the applied-state race is modelled deterministically.
	beforeLoad func()
}

func newNodeLiveView() *nodeLiveView {
	return &nodeLiveView{tips: map[string]ChainTip{}}
}

func (v *nodeLiveView) load(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
	v.mu.Lock()
	hook := v.beforeLoad
	v.mu.Unlock()
	if hook != nil {
		hook()
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.loads++
	if v.requireWait && !request.Wait {
		return ChainStateData{}, fmt.Errorf(
			"%w: this backend answers a waiting request only, as loadChainTip does",
			ErrBlockNotReady,
		)
	}
	data := ChainStateData{Tips: make([]ChainTip, len(request.Blocks))}
	for i, block := range request.Blocks {
		tip, ok := v.tips[storage.FormatBlockRef(block)]
		if !ok {
			return ChainStateData{}, fmt.Errorf(
				"%w: exact state for block %d is unavailable",
				ErrBlockNotReady,
				block.SeqNo,
			)
		}
		data.Tips[i] = tip
	}

	return data, nil
}

// publish is exactly what BlockAccepter.publishAcceptedState does with the state
// acceptance is handed: it takes the tip snapshot and makes it readable, with the
// state cell passed through rather than copied.
func (v *nodeLiveView) publish(acceptance BlockAcceptance) error {
	if acceptance.state == nil {
		return nil
	}

	block := acceptance.Candidate.Candidate.Block
	state, err := acceptance.state.acceptedTipState(block)
	if err != nil {
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.published++
	v.tips[storage.FormatBlockRef(block)] = ChainTip{
		ID:       *block.Copy(),
		BlockBOC: acceptance.Candidate.BlockBOC,
		Block:    testTipBlockFor(block),
		State:    state.Cell,
	}

	return nil
}

func (v *nodeLiveView) forget(block ton.BlockIDExt) {
	v.mu.Lock()
	delete(v.tips, storage.FormatBlockRef(block))
	v.mu.Unlock()
}

func (v *nodeLiveView) counters() (int, int) {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.loads, v.published
}

// TestFinalizedShardParentResolvesWithoutWaitingForTheApplyPipeline is THE
// regression test for the measured incident: a run that lost basechain finality
// for 28.1% of its length because every candidate needed the applied state of a
// block this node had itself finalized, and the only producer of an applied shard
// state is the shard client — which cannot get there until a masterchain block
// carrying that shard top is applied.
//
// The shape is exact:
//
//  1. the shard block is finalized locally, so acceptance runs;
//  2. NOTHING commits it: the modelled node answers only for what was published,
//     so the sync pipeline has provably not applied this block;
//  3. the session's own in-memory margin is then released, exactly as
//     notifyFinalized releases it five slots later on a 200 ms network;
//  4. a later candidate resolves that block as its parent.
//
// Step 4 used to block. It polled a read that failed in microseconds, once a
// second, until the shard client eventually got there — 146 s in the field. Now
// it resolves from what acceptance published, and the assertions are that it
// resolves at all inside a deadline far shorter than any poll interval, that the
// state is the very cell the producer computed (pointer identity, which
// chain_state.go's carry-back depends on), and — the control that makes the rest
// evidence — that with the publication removed the same resolve does NOT complete.
func TestFinalizedShardParentResolvesWithoutWaitingForTheApplyPipeline(t *testing.T) {
	// Far below the one-second interval the old poll paced at, and far below any
	// plausible apply. A resolve that needs either cannot pass.
	const withoutWaiting = 250 * time.Millisecond

	setup := func(t *testing.T) (*stateResolver, *nodeLiveView, *CandidateArtifact, *cell.Cell) {
		t.Helper()

		store := newRuntimeTestStorage()
		config, privateKey := runtimeTestConfig(0x94, &runtimeTestJournal{})
		candidates := newResolverForTest(
			store,
			&retryCandidateProvider{called: make(chan struct{}, 1)},
			1,
			simplex.DefaultParams(),
		)
		t.Cleanup(candidates.close)

		artifact := runtimeBlockArtifact(t, config, privateKey, 1, simplex.Genesis(), 1, 0x95)
		if err := candidates.stage(artifact, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
		)

		live := newNodeLiveView()
		backend := newRuntimeTestBackend()
		backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
			return live.publish(acceptance)
		}
		resolver := newStateResolver(
			config.Shard,
			config.StorageID,
			store,
			backend,
			candidates,
			StoredSessionState{},
			nil,
			simplex.DefaultParams(),
			4,
		)
		t.Cleanup(resolver.close)
		if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
			t.Fatal(err)
		}
		// The genesis load is the only one the modelled node is allowed to serve,
		// and it happens during start. From here on it answers publications only.
		backend.load = live.load

		// The successor state this node computed while validating the block: a
		// distinctive full tree, so the assertion below is about identity and not
		// about content.
		computed := cell.BeginCell().
			MustStoreUInt(0x5eed, 32).
			MustStoreRef(cell.BeginCell().MustStoreUInt(0x0fed, 32).EndCell()).
			EndCell()
		validated := &ChainState{
			shard: config.Shard,
			tips: []ChainTip{{
				ID:       *artifact.Candidate.Block.Copy(),
				BlockBOC: artifact.BlockBOC,
				Block:    testTipBlockFor(artifact.Candidate.Block),
				State:    computed,
			}},
			root: computed,
		}
		resolver.rememberValidatedState(artifact.Candidate.ID, ResolvedState{State: validated})

		return resolver, live, artifact, computed
	}

	// finalizeAndRelease finalizes the block and then releases everything this
	// session holds in memory for it, which is what the retention margins do a
	// second later on the measured network. What is left is only what acceptance
	// published.
	finalizeAndRelease := func(t *testing.T, resolver *stateResolver, artifact *CandidateArtifact) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := resolver.finalizeWith(ctx, artifact.Candidate.ID, simplex.VerifiedCertificate{}, nil); err != nil {
			t.Fatalf("finalize the locally produced shard block: %v", err)
		}

		resolver.notifyFinalized(1_000_000, retentionFloorNone)
		resolver.mu.Lock()
		delete(resolver.states, simplex.Parent(artifact.Candidate.ID))
		resolver.finalized[artifact.Candidate.ID].appliedState = nil
		delete(resolver.applied, artifact.Candidate.ID)
		resolver.mu.Unlock()
	}

	t.Run("resolves from what acceptance published", func(t *testing.T) {
		resolver, live, artifact, computed := setup(t)
		finalizeAndRelease(t, resolver, artifact)
		if _, published := live.counters(); published != 1 {
			t.Fatalf("acceptance published %d states, want the one it just accepted", published)
		}
		loadsBefore, _ := live.counters()

		ctx, cancel := context.WithTimeout(context.Background(), withoutWaiting)
		defer cancel()
		resolved, err := resolver.resolve(ctx, simplex.Parent(artifact.Candidate.ID))
		if err != nil {
			t.Fatalf("resolve a locally finalized parent the pipeline has not applied: %v", err)
		}
		if resolved.State == nil {
			t.Fatal("the resolved parent carries no state")
		}
		// Identity, not equality. The live view returns a resident state as
		// itself, and chain_state.go compares tip states BY POINTER for the
		// live-successor carry-back, so a copy here would silently cost a full
		// re-apply per candidate with no error and no metric.
		if resolved.State.root != computed {
			t.Error("the resolved parent is a different materialization of the state this node computed")
		}
		if tips := resolved.State.tipStates(); len(tips) != 1 || tips[0] != computed {
			t.Error("the resolved tip state is not the computed one")
		}
		loadsAfter, _ := live.counters()
		if loadsAfter-loadsBefore != 1 {
			t.Errorf("the resolve took %d reads, want exactly one that succeeded", loadsAfter-loadsBefore)
		}
	})

	// THE CONTROL. Without the publication the modelled node has nothing to
	// answer with and never will, which is precisely the field condition. If this
	// subtest passed, the one above would prove nothing about the publication.
	t.Run("without the publication it never resolves", func(t *testing.T) {
		resolver, live, artifact, _ := setup(t)
		finalizeAndRelease(t, resolver, artifact)
		live.forget(artifact.Candidate.Block)

		ctx, cancel := context.WithTimeout(context.Background(), withoutWaiting)
		defer cancel()
		if _, err := resolver.resolve(ctx, simplex.Parent(artifact.Candidate.ID)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("resolve without a published state = %v, want the wait this change removes", err)
		}
	})
}

// The state acceptance publishes has to be the one the producer computed, and it
// has to be a FULL tree. acceptedTipState is the only producer of a published
// state, so these are the properties every publication has by construction.
func TestAcceptedTipStateOnlyReleasesThisStatesOwnFullRoot(t *testing.T) {
	fixture := newLiveSuccessorFixture(t, 0x30)
	state, artifact := fixture.state(t, 0x30)
	next, err := state.validatedCandidateState(artifact, fixture.successorOf(artifact), nil)
	if err != nil {
		t.Fatalf("advance the parent with a validated candidate: %v", err)
	}

	published, err := next.acceptedTipState(artifact.Candidate.Block)
	if err != nil {
		t.Fatalf("snapshot the accepted tip: %v", err)
	}
	if published.Cell != next.root {
		t.Fatal("the published state is not this chain state's own root")
	}
	rootHash := next.root.HashKeyAt(0)
	if string(published.StateRootHash) != string(rootHash[:]) {
		t.Fatal("the published state root hash does not name the published cell")
	}
	if !sameBlockID(published.Block, artifact.Candidate.Block) {
		t.Fatal("the published state names another block")
	}
	// FULL, and a hash check cannot tell: the narrow root a proof-backed
	// verification produces has the same hash. Reading a leaf of a subtree the
	// transition never touched is the only test that separates them.
	bits, err := untouchedLeafBits(published.Cell)
	if err != nil {
		t.Fatalf("walk the untouched subtree of the published state: %v", err)
	}
	if bits != fixture.untouchedBit {
		t.Fatalf("published untouched leaf = %#x, want %#x", bits, fixture.untouchedBit)
	}

	// The block has to be this state's single ordinary tip. Another block's id is
	// a mis-wiring and is refused rather than guessed at.
	other := liveSuccessorArtifact(0x39)
	if _, err = next.acceptedTipState(other.Candidate.Block); err == nil {
		t.Fatal("a state released a snapshot for a block that is not its tip")
	}
	// So is a merge state: it is the state of neither of its two tips.
	merged := &ChainState{shard: next.shard, tips: []ChainTip{next.tips[0], next.tips[0]}, root: next.root}
	if _, err = merged.acceptedTipState(artifact.Candidate.Block); err == nil {
		t.Fatal("a merge state released a snapshot for one of its tips")
	}
}

// chainTipWaitObserver counts the backstop alarm and nothing else. Every other
// method is inherited because ValidationObserver is a bounded-enum interface and a
// partial implementation would not compile.
type chainTipWaitObserver struct {
	selfRejectionObserver
	mu        sync.Mutex
	backstops int
}

func (o *chainTipWaitObserver) AddChainTipWaitBackstop(collator.MetricChain) {
	o.mu.Lock()
	o.backstops++
	o.mu.Unlock()
}

func (o *chainTipWaitObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.backstops
}

// The predecessor read waits on the publication EDGE, not on a clock. What used
// to happen here was a 1 Hz poll against a 200 ms slot rate, every attempt failing
// in microseconds — and silently, which is why a 149 s standstill was measured in
// the field with no log line naming its cause.
//
// The assertions are that the read is genuinely blocked before the publication,
// that the publication alone releases it — no interval elapses in this test, which
// is what "edge" means — and that it comes back with the state that was published.
func TestChainTipReadWaitsForThePublicationEdgeRatherThanPolling(t *testing.T) {
	state := cell.BeginCell().MustStoreUInt(0x9a, 8).EndCell()
	blockRoot := acceptanceTestBlockRoot(t, groups.ShardID{Workchain: 0, Shard: -1 << 63}, 7, 0x33445566)
	blockBOC := blockRoot.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	block := localBackendTestBlockID(0, -1<<63, 2, blockRoot.Hash(), blockBOC)
	node := &localBackendTestNode{
		states:     map[string]*storage.BlockState{},
		stateCells: map[string]*cell.Cell{},
		blocks:     map[string][]byte{},
	}
	backend := &LocalSessionBackend{node: node}

	loaded := make(chan ChainStateData, 1)
	failed := make(chan error, 1)
	go func() {
		data, err := backend.LoadChainState(context.Background(), ChainStateRequest{
			Blocks: []ton.BlockIDExt{block},
			Wait:   true,
		})
		if err != nil {
			failed <- err

			return
		}
		loaded <- data
	}()

	// Blocked, and demonstrably so: nothing has been published, and the read is not
	// going to succeed on its own.
	select {
	case data := <-loaded:
		t.Fatalf("the read returned %+v before anything was published", data)
	case err := <-failed:
		t.Fatalf("the read failed instead of waiting: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := node.PublishAcceptedBlockState(storage.LiveBlockArtifacts{
		Block:     block,
		BlockData: blockBOC,
		State: &storage.BlockState{
			Block:         block,
			StateRootHash: state.Hash(),
			Cell:          state,
		},
	}); err != nil {
		t.Fatalf("publish the accepted state: %v", err)
	}

	select {
	case data := <-loaded:
		if len(data.Tips) != 1 || data.Tips[0].State != state {
			t.Fatalf("the released read returned %+v, want the published state itself", data.Tips)
		}
	case err := <-failed:
		t.Fatalf("the read failed after the publication: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the publication did not release the waiting read")
	}
}

// A waiting read that is never released must not become a silent hang. The
// backstop is the alarm for the one thing an edge-triggered wait cannot see — a
// path that makes a state readable without raising a signal — so firing it logs
// and counts, and hands the not-ready error back for the caller's own retry to
// deal with.
func TestChainTipWaitBackstopAlarmsAndReturnsNotReady(t *testing.T) {
	block := localBackendTestBlockID(0, -1<<63, 9, bytes.Repeat([]byte{0x9c}, 32), nil)
	node := &localBackendTestNode{
		states:     map[string]*storage.BlockState{},
		stateCells: map[string]*cell.Cell{},
		blocks:     map[string][]byte{},
	}
	observer := &chainTipWaitObserver{}
	backend := &LocalSessionBackend{node: node, metrics: observer}

	// The caller's own context is what bounds this wait in the test; the backstop
	// is 30 s and is exercised on the path below through the same select.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := backend.LoadChainState(ctx, ChainStateRequest{Blocks: []ton.BlockIDExt{block}, Wait: true})
	if !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("abandoned wait error = %v, want the read's own not-ready", err)
	}
	// A wait the CALLER abandoned is not an alarm: nothing is wrong with the node.
	if count := observer.count(); count != 0 {
		t.Fatalf("a caller-cancelled wait raised %d alarms, want none", count)
	}

	// And the backstop itself, driven directly so the test does not wait 30 s for
	// it. What it must do is count and complain.
	backend.observeChainTipWaitBackstop(block, chainTipWaitBackstop, ErrBlockNotReady)
	if count := observer.count(); count != 1 {
		t.Fatalf("the backstop raised %d alarms, want exactly one", count)
	}
}

// TestChainTipWaitBackstopIsNotRestartedByUnrelatedPublications is the
// regression for the backstop timer's lifetime.
//
// Built inside the select, the timer was rebuilt on every loop iteration, and
// the loop iterates on every publication in the shard — not only on a
// publication of the block being waited for. The alarm therefore required
// chainTipWaitBackstop of total publication SILENCE rather than that long spent
// waiting for the block in hand, which on a busy node means it never fires: the
// 1 Hz poll had been replaced by an unbounded wait with no working backstop.
//
// The test drives exactly that shape: a wait for a block that never arrives,
// while unrelated publications keep waking the loop faster than the backstop. It
// must still fire, once, and hand back the read's own not-ready error.
func TestChainTipWaitBackstopIsNotRestartedByUnrelatedPublications(t *testing.T) {
	waited := localBackendTestBlockID(0, -1<<63, 11, bytes.Repeat([]byte{0x7a}, 32), nil)
	other := localBackendTestBlockID(0, -1<<63, 12, bytes.Repeat([]byte{0x7b}, 32), nil)

	node := &localBackendTestNode{
		states:     map[string]*storage.BlockState{},
		stateCells: map[string]*cell.Cell{},
		blocks:     map[string][]byte{},
	}
	observer := &chainTipWaitObserver{}
	backend := &LocalSessionBackend{
		node:    node,
		metrics: observer,
		// Short enough to run as a test, long enough that the wake-ups below are
		// comfortably faster than it. The property under test is a ratio, not a
		// duration: wake interval well under the backstop.
		waitBackstop: 300 * time.Millisecond,
	}

	type outcome struct {
		err error
	}
	done := make(chan outcome, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		_, err := backend.LoadChainState(ctx, ChainStateRequest{
			Blocks: []ton.BlockIDExt{waited},
			Wait:   true,
		})
		done <- outcome{err: err}
	}()

	// Keep the loop awake. Each publication is of a DIFFERENT block, so every
	// iteration re-reads, fails again, and re-enters the select — which is
	// precisely where a per-iteration timer would be reborn.
	stop := make(chan struct{})
	publishing := make(chan struct{})
	go func() {
		defer close(publishing)
		state := cell.BeginCell().MustStoreUInt(0xab, 8).EndCell()
		for {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
			}
			_ = node.PublishAcceptedBlockState(storage.LiveBlockArtifacts{
				Block:     other,
				BlockData: []byte{0x01},
				State: &storage.BlockState{
					Block:         other,
					StateRootHash: state.Hash(),
					Cell:          state,
				},
			})
		}
	}()

	var built outcome
	select {
	case built = <-done:
	case <-time.After(15 * time.Second):
		close(stop)
		<-publishing
		t.Fatal("the wait never ended: the backstop is being restarted by unrelated publications, so " +
			"this wait is unbounded")
	}
	close(stop)
	<-publishing

	if !errors.Is(built.err, ErrBlockNotReady) {
		t.Fatalf("backstop error = %v, want the read's own not-ready", built.err)
	}
	if count := observer.count(); count != 1 {
		t.Fatalf("the backstop raised %d alarms, want exactly one", count)
	}
}
