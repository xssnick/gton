package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// acceptanceSplitShard is a basechain shard, so a final certificate makes the
// accepter build a shard-top description — the one acceptance step that waits on
// the node's apply pipeline.
var acceptanceSplitShard = groups.ShardID{Workchain: 0, Shard: -1 << 63}

// Finalization is a chain: a candidate finalizes its parent first, and nothing of
// the child moves until the parent's acceptance returns. The shard-top
// description used to run inside that acceptance and retry at one hertz until
// the node had indexed the masterchain block the shard block refers to — so a
// node whose apply pipeline was a few hundred milliseconds behind turned every
// finalization into a one-second wait, stopped feeding the sync pipeline the
// blocks it had just finalized, and the pipeline fell back to downloading them.
//
// The description now runs detached. What this pins: the handoff of a parent
// whose description is NEVER ready returns at once, the child's handoff follows
// it at once, the description is still attempted and retried, and closing the
// session ends the detached retry.
func TestFinalizationHandoffDoesNotWaitOnTheShardTopDescription(t *testing.T) {
	storage := newRuntimeTestStorage()
	backend := newRuntimeTestBackend()

	var mu sync.Mutex
	var handoffs []BlockAcceptance
	descriptions := make(map[simplex.CandidateID]int)
	preparations := 0
	backend.prepareAcceptance = func(_ context.Context, acceptance BlockAcceptance) (PreparedBlockAcceptance, error) {
		mu.Lock()
		preparations++
		mu.Unlock()
		return &runtimeTestBlockAcceptance{backend: backend, acceptance: acceptance}, nil
	}
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		mu.Lock()
		defer mu.Unlock()
		handoffs = append(handoffs, acceptance)
		return nil
	}
	backend.description = func(_ context.Context, acceptance BlockAcceptance) error {
		mu.Lock()
		defer mu.Unlock()
		descriptions[acceptance.Candidate.Candidate.ID]++
		return ErrBlockNotReady
	}

	candidates := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, simplex.DefaultParams())
	t.Cleanup(candidates.close)
	parentID := simplex.CandidateID{Slot: 0, Hash: [32]byte{0xa1}}
	parent := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     parentID,
		Parent: simplex.Genesis(),
		Block:  ton.BlockIDExt{Workchain: 0, Shard: -1 << 63, SeqNo: 1},
	}}
	childID := simplex.CandidateID{Slot: 1, Hash: [32]byte{0xb2}}
	child := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     childID,
		Parent: simplex.Parent(parentID),
		Block:  ton.BlockIDExt{Workchain: 0, Shard: -1 << 63, SeqNo: 2},
	}}
	for i, artifact := range []*CandidateArtifact{parent, child} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(artifact.Candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)))
	}

	resolver := newStateResolver(
		acceptanceSplitShard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		nil,
		simplex.DefaultParams(),
		4,
	)
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	// Far below the one-second retry the description is stuck in.
	const promptly = 500 * time.Millisecond
	for _, artifact := range []*CandidateArtifact{parent, child} {
		ctx, cancel := context.WithTimeout(context.Background(), promptly)
		err := resolver.finalize(ctx, artifact.Candidate.ID, resolverTestSeal(t, simplex.FinalizeVote(artifact.Candidate.ID)))
		cancel()
		if err != nil {
			t.Fatalf("finalize %v while its description is not ready: %v", artifact.Candidate.ID, err)
		}
	}

	mu.Lock()
	got := append([]BlockAcceptance(nil), handoffs...)
	mu.Unlock()
	if len(got) != 2 || got[0].Candidate != parent || got[1].Candidate != child {
		t.Fatalf("handoffs = %d, want the parent then the child", len(got))
	}

	// The description was attempted, and it is being retried, detached.
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		parentAttempts := descriptions[parentID]
		childAttempts := descriptions[childID]
		prepared := preparations
		mu.Unlock()
		if prepared != 2 {
			t.Fatalf("preparation count = %d, want one for each accepted block", prepared)
		}
		if parentAttempts >= 2 && childAttempts >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent/child description attempts = %d/%d, want each detached retry to keep going", parentAttempts, childAttempts)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Closing the session ends the detached retry: close joins every resolver
	// goroutine, so a retry that ignored the session context would hang here.
	closed := make(chan struct{})
	go func() {
		resolver.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("closing the resolver did not end the detached description retry")
	}
}

// acceptanceSplitBlock is a real shard block whose state update advances parent
// to a successor derived from it, serialized the way a candidate carries it, so
// ChainState.apply can advance a resident parent state by it.
func acceptanceSplitBlock(t *testing.T, parent *cell.Cell, seqno uint32, marker uint64) (*CandidateArtifact, *cell.Cell) {
	t.Helper()
	update, successor := testStateUpdate(parent, marker)

	prefixBits, err := acceptanceSplitShard.PrefixBits()
	if err != nil {
		t.Fatalf("derive shard prefix length: %v", err)
	}
	header := tlb.BlockHeader{}
	header.NotMaster = true
	header.Shard = tlb.ShardIdent{
		PrefixBits:  int8(prefixBits),
		WorkchainID: acceptanceSplitShard.Workchain,
		ShardPrefix: uint64(acceptanceSplitShard.Shard) & (uint64(acceptanceSplitShard.Shard) - 1),
	}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.MinRefMcSeqno = 1
	header.PrevRef = tlb.BlkPrevInfo{Prev1: acceptanceTestExtBlockRef(seqno-1, 0x31)}
	masterRef := acceptanceTestExtBlockRef(1, 0x41)
	header.MasterRef = &masterRef
	info, err := header.ToCell()
	if err != nil {
		t.Fatalf("build block header: %v", err)
	}
	valueFlow, err := (tlb.ValueFlow{}).ToCell()
	if err != nil {
		t.Fatalf("build block value flow: %v", err)
	}
	root := cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreInt(-239, 32).
		MustStoreRef(info).
		MustStoreRef(valueFlow).
		MustStoreRef(update).
		MustStoreRef(acceptanceTestBlockExtra(false)).
		EndCell()
	boc := root.ToBOC()
	fileHash := sha256.Sum256(boc)

	artifact := &CandidateArtifact{
		Candidate: simplex.Candidate{
			Parent: simplex.Genesis(),
			Block: ton.BlockIDExt{
				Workchain: acceptanceSplitShard.Workchain,
				Shard:     acceptanceSplitShard.Shard,
				SeqNo:     seqno,
				RootHash:  root.Hash(),
				FileHash:  fileHash[:],
			},
		},
		BlockBOC:            boc,
		generationTimeMS:    uint64(time.Now().UnixMilli()),
		generationTimeKnown: true,
	}
	artifact.Candidate.ID = simplex.CandidateID{Slot: seqno - 1, Hash: [32]byte(bytes.Repeat([]byte{byte(marker)}, 32))}
	return artifact, successor
}

// A finalized parent whose state this node never computed — it did not validate
// the block — used to be resolved from the node's store, which answers only after
// the apply pipeline has reached the block. Now it is rebuilt from the nearest
// resident ancestor state by applying the block's own update, and the store is
// not asked. The control is the same block over a parent the update does not
// describe: the rebuild fails and the resolve waits on the store as before, so
// the store read that the first case skips is provably the one this replaces.
func TestFinalizedParentStateIsRebuiltFromAResidentAncestor(t *testing.T) {
	setup := func(t *testing.T, applicable bool) (*stateResolver, *runtimeTestBackend, *CandidateArtifact, *cell.Cell, *int) {
		t.Helper()
		storage := newRuntimeTestStorage()
		backend := newRuntimeTestBackend()
		candidates := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, simplex.DefaultParams())
		t.Cleanup(candidates.close)

		// The genesis root the backend serves is backend.stateRoot; a block whose
		// update advances that root applies, one built over another root does not.
		parent := backend.stateRoot
		if !applicable {
			parent = cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
		}
		artifact, successor := acceptanceSplitBlock(t, parent, 1, 0x5a)
		if err := candidates.stage(artifact, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(artifact.Candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)))

		resolver := newStateResolver(
			acceptanceSplitShard,
			SessionStorageID{},
			storage,
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
		// From here the store never answers: the modelled node has not applied
		// the block and never will inside this test.
		loads := 0
		backend.load = func(context.Context, ChainStateRequest) (ChainStateData, error) {
			loads++
			return ChainStateData{}, ErrBlockNotReady
		}
		return resolver, backend, artifact, successor, &loads
	}

	t.Run("rebuilt from the genesis without a store read", func(t *testing.T) {
		resolver, _, artifact, successor, loads := setup(t, true)
		// Finalized, and nothing resident for it: no validated state, no applied
		// state — the block this node did not validate.
		resolver.mu.Lock()
		resolver.finalized[artifact.Candidate.ID] = &finalizedState{isDone: true, reconciled: true}
		resolver.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		resolved, err := resolver.resolve(ctx, simplex.Parent(artifact.Candidate.ID))
		if err != nil {
			t.Fatalf("resolve a finalized parent the pipeline has not applied: %v", err)
		}
		if resolved.State == nil || !bytes.Equal(resolved.State.root.Hash(), successor.Hash()) {
			t.Fatal("the rebuilt state is not the block's successor state")
		}
		if *loads != 0 {
			t.Fatalf("the resolve read the store %d times, want none", *loads)
		}
		// Filed under the finalization marker, so the next resolve and the
		// acceptance publication share the one materialization.
		if state := resolver.residentAppliedState(artifact.Candidate.ID, artifact.Candidate.Block); state != resolved.State {
			t.Fatal("the rebuilt state was not remembered under its finalization marker")
		}
	})

	t.Run("a block that does not apply still waits on the store", func(t *testing.T) {
		resolver, _, artifact, _, loads := setup(t, false)
		resolver.mu.Lock()
		resolver.finalized[artifact.Candidate.ID] = &finalizedState{isDone: true, reconciled: true}
		resolver.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if _, err := resolver.resolve(ctx, simplex.Parent(artifact.Candidate.ID)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("resolve = %v, want the store wait", err)
		}
		if *loads == 0 {
			t.Fatal("the control never reached the store, so the first case proves nothing")
		}
	})

	// Acceptance publishes the state it holds into the live view so later readers
	// do not wait for the store. For a block this node never validated there was
	// nothing to publish; now the finalization rebuilds it first.
	t.Run("finalization publishes a rebuilt state", func(t *testing.T) {
		resolver, backend, artifact, successor, _ := setup(t, true)
		var published *ChainState
		backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
			published = acceptance.state
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := resolver.finalize(ctx, artifact.Candidate.ID, simplex.VerifiedCertificate{}); err != nil {
			t.Fatalf("finalize a block this node did not validate: %v", err)
		}
		if published == nil {
			t.Fatal("acceptance published no state for a block whose state could be rebuilt")
		}
		if !bytes.Equal(published.root.Hash(), successor.Hash()) {
			t.Fatal("acceptance published a state that is not the block's successor")
		}
	})
}
