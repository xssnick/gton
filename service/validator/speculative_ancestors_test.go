package validator

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type speculativeAncestorFixture struct {
	runtime  *sessionRuntime
	provider *retryCandidateProvider
	b0, b1   *CandidateArtifact
	parent   ResolvedState
	tip      ResolvedState
}

func newSpeculativeAncestorFixture(t *testing.T) speculativeAncestorFixture {
	t.Helper()
	config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	provider := &retryCandidateProvider{called: make(chan struct{}, 4)}
	runtime := newWarmupRuntime(t, config, provider)
	runtime.codec = runtime.candidates.codec
	runtime.state.Params = simplex.DefaultParams()
	request := successorShardRequest(t)
	builder := collator.NewBuilder(tvm.NewTVM(), tlb.GlobalVersion{Version: 1})
	anchor, err := builder.BuildShard(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	anchorRoot, err := cell.FromBOC(anchor.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := newChainState(ChainStateRequest{
		Shard: config.Shard, Blocks: []ton.BlockIDExt{anchor.ID}, MinMasterchain: request.Masterchain.ID,
	}, ChainStateData{Tips: []ChainTip{{ID: anchor.ID, BlockBOC: anchor.BlockBOC, Block: anchorRoot, State: anchor.State}}})
	if err != nil {
		t.Fatal(err)
	}
	runtime.states.genesis = genesis
	runtime.states.backend.(*runtimeTestBackend).load = func(context.Context, ChainStateRequest) (ChainStateData, error) {
		t.Error("resident speculative ancestry caused a state-store read")

		return ChainStateData{}, errors.New("unexpected state-store read")
	}
	previous := anchor
	parentID := simplex.Genesis()
	fixture := speculativeAncestorFixture{runtime: runtime, provider: provider}
	for _, slot := range []uint32{4, 7} {
		root, err := cell.FromBOC(previous.BlockBOC)
		if err != nil {
			t.Fatal(err)
		}
		queueSize := previous.Stats.OutQueueSize
		request.Previous = collator.PreviousBlock{ID: previous.ID, Block: root, State: previous.State, OutQueueSize: &queueSize}
		request.Header.GenUtime++
		request.Header.GenUtimeMS += 1000
		built, err := builder.BuildShard(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		candidate := simplex.Candidate{Parent: parentID, Block: built.ID}
		candidate.ID = candidate.ComputeID(slot)
		artifact := &CandidateArtifact{Candidate: candidate, BlockBOC: built.BlockBOC,
			generationTimeKnown: true, generationTimeMS: request.Header.GenUtimeMS}
		if err = runtime.candidates.stage(artifact, []byte{byte(slot)}); err != nil {
			t.Fatal(err)
		}
		if slot == 4 {
			fixture.b0 = artifact
			runtime.candidates.observeNotarization(candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(candidate.ID)))
			fixture.parent, err = runtime.states.resolve(t.Context(), simplex.Parent(candidate.ID))
			if err != nil {
				t.Fatal(err)
			}
		} else {
			fixture.b1 = artifact
			prepared := make(chan ResolvedState, 1)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			go func() {
				_, _ = runtime.states.resolveWithPreparation(ctx, simplex.Parent(candidate.ID), func(state ResolvedState) {
					prepared <- state
				})
			}()
			select {
			case fixture.tip = <-prepared:
			case <-time.After(time.Second):
				t.Fatal("B1 was not prepared from resident certified B0")
			}
		}
		previous, parentID = built, simplex.Parent(candidate.ID)
	}

	return fixture
}

func TestSpeculativeAncestorsReachObserverAndEmbeddedHandoffs(t *testing.T) {
	f := newSpeculativeAncestorFixture(t)
	offered := make(chan sessionSpeculativeWindow, 1)
	f.runtime.speculate = func(_ context.Context, window sessionSpeculativeWindow) error {
		offered <- window

		return nil
	}
	f.runtime.warmCandidateState(f.b1.Candidate.ID)
	var window sessionSpeculativeWindow
	select {
	case window = <-offered:
	case <-time.After(time.Second):
		t.Fatal("observer did not offer its prepared successor")
	}
	if len(window.Ancestors) != 2 || window.Ancestors[0] != f.parent.State.tips[0].Block ||
		window.Ancestors[1] != f.runtime.states.genesis.tips[0].Block {
		t.Fatal("observer did not carry the exact resident B0 and genesis block roots")
	}
	observer, err := collatorSpeculativeWindow(f.runtime.config.SessionID, window)
	if err != nil {
		t.Fatal(err)
	}
	backend := speculativeTestBackend(0, len(f.runtime.config.Validators))
	backend.config = f.runtime.config
	self := backend.self.(*localBackendTestCollator)
	self.speculateWake = make(chan struct{}, 1)
	backend.speculateNextWindow(t.Context(), speculativeTestView(4, time.Second), f.b1, f.tip.State, f.tip.GenUtime, f.runtime.states)
	select {
	case <-self.speculateWake:
	case <-time.After(time.Second):
		t.Fatal("embedded producer did not offer its prepared successor")
	}
	self.mu.Lock()
	embedded := self.speculateCall[0]
	self.mu.Unlock()
	if !reflect.DeepEqual(embedded.Base, observer.Base) {
		t.Fatal("embedded and observer handed off different bound ancestor capabilities")
	}
	if len(f.provider.called) != 0 {
		t.Fatal("resident ancestor handoff queried a peer")
	}
	f.runtime.states.mu.Lock()
	finished := f.runtime.states.states[simplex.Parent(f.b1.Candidate.ID)].finished
	f.runtime.states.mu.Unlock()
	if finished {
		t.Fatal("ancestor handoff opened B1's certificate gate")
	}
}

func TestSpeculativeAncestorSnapshotStopsAtUnavailableOrUnverifiedParents(t *testing.T) {
	for _, mode := range []string{"missing", "released", "uncertified", "mismatched block", "different shard", "different seqno"} {
		t.Run(mode, func(t *testing.T) {
			f := newSpeculativeAncestorFixture(t)
			id := f.b0.Candidate.ID
			switch mode {
			case "released":
				f.runtime.states.mu.Lock()
				delete(f.runtime.states.states, simplex.Parent(id))
				f.runtime.states.mu.Unlock()
			default:
				f.runtime.candidates.mu.Lock()
				entry := f.runtime.candidates.entries[id]
				switch mode {
				case "missing":
					delete(f.runtime.candidates.entries, id)
				case "uncertified":
					entry.notarization = simplex.VerifiedCertificate{}
				case "mismatched block":
					lineage := *entry.lineage
					lineage.rootHash[0] ^= 1
					entry.lineage = &lineage
				case "different shard":
					lineage := *entry.lineage
					lineage.shard ^= 1 << 62
					entry.lineage = &lineage
				case "different seqno":
					lineage := *entry.lineage
					lineage.seqno--
					entry.lineage = &lineage
				}
				f.runtime.candidates.mu.Unlock()
			}
			if roots := f.runtime.states.speculativeAncestorBlocks(f.b1.Candidate.ID, f.tip.State); len(roots) != 0 {
				t.Fatalf("%s parent produced %d roots", mode, len(roots))
			}
			if len(f.provider.called) != 0 {
				t.Fatal("best-effort ancestor snapshot queried a peer")
			}
		})
	}
}

func TestSpeculativeAncestorSnapshotSkipsOnlyCertifiedEmptyAliases(t *testing.T) {
	f := newSpeculativeAncestorFixture(t)
	empty := simplex.Candidate{Parent: simplex.Parent(f.b0.Candidate.ID), Block: f.b0.Candidate.Block, Empty: true}
	empty.ID = empty.ComputeID(5)
	if err := f.runtime.candidates.stage(&CandidateArtifact{Candidate: empty}, []byte{5}); err != nil {
		t.Fatal(err)
	}
	tip := *f.b1
	tip.Candidate.Parent = simplex.Parent(empty.ID)
	tip.Candidate.ID = tip.Candidate.ComputeID(7)
	if err := f.runtime.candidates.stage(&tip, []byte{0x99}); err != nil {
		t.Fatal(err)
	}
	if roots := f.runtime.states.speculativeAncestorBlocks(tip.Candidate.ID, f.tip.State); len(roots) != 0 {
		t.Fatal("uncertified empty alias exposed its parent's root")
	}
	f.runtime.candidates.observeNotarization(empty.ID, resolverTestSeal(t, simplex.NotarizeVote(empty.ID)))
	if _, err := f.runtime.states.resolve(t.Context(), simplex.Parent(empty.ID)); err != nil {
		t.Fatal(err)
	}
	roots := f.runtime.states.speculativeAncestorBlocks(tip.Candidate.ID, f.tip.State)
	if len(roots) != 2 || roots[0] != f.parent.State.tips[0].Block || roots[1] != f.runtime.states.genesis.tips[0].Block {
		t.Fatal("certified empty alias duplicated or lost an ordinary ancestor")
	}
}
