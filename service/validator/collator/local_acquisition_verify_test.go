package collator

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestLocalAcquisitionValidatesWithoutPreparedCollatorSession(t *testing.T) {
	fixture, shardState := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder: testBuilder(),
		Store: &localValidationStore{states: []localValidationState{
			{block: fixture.request.Previous.ID, root: fixture.request.Previous.State},
			{block: fixture.oldShard, root: shardState},
		}},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(acquisition.sessions) != 0 {
		t.Fatal("validation fixture unexpectedly has a prepared collator session")
	}

	result, err := acquisition.ValidateCandidate(context.Background(), ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{fixture.request.Previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	})
	if err != nil {
		t.Fatalf("validate self-contained candidate: %v", err)
	}
	want := time.UnixMilli(int64(fixture.request.Header.GenUtimeMS))
	if !result.ValidAfter.Equal(want) {
		t.Fatalf("valid-after = %v, want %v", result.ValidAfter, want)
	}
	if result.Successor.BlockRoot == nil || result.Successor.StateUpdate == nil {
		t.Fatal("the validated transition is unavailable")
	}
}

func TestLocalAcquisitionPrefersFullCollatedNeighborProof(t *testing.T) {
	request := advanceCandidateRequest(t, emptyCandidateRequest(t))
	request.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &request)
	candidate, err := testBuilder().BuildShard(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCandidate(context.Background(), request.Masterchain.Config, candidate)
	if err != nil {
		t.Fatal(err)
	}

	id := request.Previous.ID
	neighbors, err := (&LocalAcquisition{}).loadExpectedNeighbors(
		context.Background(),
		new(localMasterView),
		map[neighborShardKey]ton.BlockIDExt{{workchain: id.Workchain, shard: id.Shard}: id},
		[]PreviousBlock{{ID: id}},
		nil,
		&verified.collated,
		false,
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatalf("load full-collated neighbor before partial predecessor: %v", err)
	}
	if len(neighbors) != 1 || !neighbors[0].Block.Equals(&id) || neighbors[0].OutMsgQueueInfo == nil {
		t.Fatalf("loaded neighbors = %+v, want the proven predecessor", neighbors)
	}
	master := &localMasterView{
		state:    tlb.ShardStateUnsplit{GenLT: request.Masterchain.EndLT},
		registry: &ShardRegistry{leaves: make(map[shardRegistryKey]shardRegistryLeaf)},
		context:  request.Masterchain,
	}
	if _, err = (&LocalAcquisition{}).historicalShardEndLT(
		context.Background(),
		master,
		[]PreviousBlock{{ID: id}},
		nil,
		&verified.collated,
		nil,
		acquisitionReadImmediate,
	); err != nil {
		t.Fatalf("load historical frontier from full-collated predecessor: %v", err)
	}
}

// A shard is its own neighbor, so the session's own predecessor is always in
// the expected set — and the state supplied with the session is the same full
// tree the node store holds. Reading it back out of storage costs a block read,
// a state read and a second full decode of a tree already in memory, so the
// supplied state is preferred and the store is never asked.
//
// Two neighbors is the shape that arms the warm-up at all, and the store is
// asked from two independent places: synchronously by the resolution loop, and
// from goroutines the warm-up never joins. The counting store answers nothing
// and only records that it was consulted, which settles the loop half — but a
// counter cannot settle the warm-up half, because loadExpectedNeighbors returns
// long before those goroutines run. That half is pinned by asserting the
// selection pendingWarmSources makes, which is the whole of the decision and is
// synchronous.
func TestNeighborsPreferSuppliedPredecessorOverStore(t *testing.T) {
	fixture := newMergePredecessorFixture(t)
	if fixture.req.Previous2 == nil {
		t.Fatal("merge fixture has no second predecessor")
	}
	first, second := fixture.req.Previous, *fixture.req.Previous2

	store := new(countingNeighborStore)
	acquisition := &LocalAcquisition{store: store}
	views := make(map[msgpool.ShardIdent]*localNeighborView)
	expected := map[neighborShardKey]ton.BlockIDExt{
		{workchain: first.ID.Workchain, shard: first.ID.Shard}:   first.ID,
		{workchain: second.ID.Workchain, shard: second.ID.Shard}: second.ID,
	}
	keys := make([]neighborShardKey, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	previous := []PreviousBlock{first, second}
	if warm := acquisition.pendingWarmSources(keys, expected, previous, views, nil); len(warm) != 0 {
		t.Fatalf("the warm-up starts %d store reads for blocks the session supplied: %+v", len(warm), warm)
	}

	neighbors, err := acquisition.loadExpectedNeighbors(
		context.Background(),
		new(localMasterView),
		expected,
		previous,
		views,
		nil,
		true,
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatalf("load neighbors from the supplied predecessors: %v", err)
	}
	if reads := store.reads.Load(); reads != 0 {
		t.Fatalf("the node store was read %d times for blocks the session supplied", reads)
	}
	if len(neighbors) != 2 {
		t.Fatalf("loaded neighbors = %d, want both predecessors", len(neighbors))
	}
	for _, id := range []ton.BlockIDExt{first.ID, second.ID} {
		view := views[blockShardIdent(id)]
		if view == nil || view.state.OutMsgQueueInfo == nil {
			t.Fatalf("neighbor view for %d carries no outbound queue", id.SeqNo)
		}
		// Never traced, although this collation emits proofs: the block is a
		// collation predecessor, and BuildFullCollatedProofs skips exactly those.
		// TestFullCollatedProofsSkipPredecessorNeighbors is what holds that
		// closed; the cost of getting it wrong is a walk of the whole predecessor
		// outbound queue to fill a proof nobody reads.
		if view.proof != nil {
			t.Fatalf("neighbor view for %d is traced for a proof that is discarded", id.SeqNo)
		}
	}
}

// The reason the predecessor neighbour view is left untraced: its proof is never
// emitted. This pins the discard at the emitter, so the two facts cannot drift
// apart — restoring the trace would then be caught as pure waste, and dropping
// this skip would be caught as a missing proof.
func TestFullCollatedProofsSkipPredecessorNeighbors(t *testing.T) {
	fixture := newMergePredecessorFixture(t)
	if fixture.req.Previous2 == nil {
		t.Fatal("merge fixture has no second predecessor")
	}
	first, second := fixture.req.Previous, *fixture.req.Previous2

	views := make(map[msgpool.ShardIdent]*localNeighborView)
	neighbors := make([]Neighbor, 0, 2)
	for _, previous := range []PreviousBlock{first, second} {
		// Traced on purpose: the emitter must skip these because of which block
		// they are, not because they happen to carry no builder.
		view, err := localViewFromPrevious(previous, true, true)
		if err != nil {
			t.Fatalf("decode predecessor %d: %v", previous.ID.SeqNo, err)
		}
		source := blockShardIdent(previous.ID)
		views[source] = view
		neighbors = append(neighbors, localNeighbor(view, source))
	}

	provider := &localFullProofProvider{views: views}
	proofs, err := provider.BuildFullCollatedProofs(context.Background(), FullCollatedProofRequest{
		Previous:  first,
		Previous2: &second,
		Neighbors: neighbors,
	})
	if err != nil {
		t.Fatalf("build proofs for a neighbor set of predecessors only: %v", err)
	}
	if len(proofs) != 0 {
		t.Fatalf("emitted %d proofs for neighbors that are all collation predecessors", len(proofs))
	}
}

// The other half of the preference, and the reason it is safe to have one at
// all: a predecessor re-pointed at the candidate's own collated proof carries a
// state that answers what that candidate reads and stops at a pruned boundary
// anywhere else. Taking it as a neighbour source would decode an outbound queue
// off that boundary and reject a valid candidate, so Proven sends it to the
// node store instead — and this test pins that the flag, not the caller, is
// what decides.
func TestNeighborsIgnoreProvenPredecessorAsNeighborSource(t *testing.T) {
	candidate, err := testBuilder().BuildShard(context.Background(), emptyCandidateRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	canonical := candidatePrevious(t, candidate)
	partial := cell.BeginCell().EndCell()
	if _, err = localViewFromPrevious(PreviousBlock{ID: canonical.ID, State: partial}, false, false); err == nil {
		t.Fatal("test predecessor unexpectedly contains the outbound queue")
	}

	acquisition := &LocalAcquisition{store: &localValidationStore{states: []localValidationState{{
		block: canonical.ID,
		root:  canonical.State,
		data:  canonical.Block,
	}}}}
	id := canonical.ID
	neighbors, err := acquisition.loadExpectedNeighbors(
		context.Background(),
		new(localMasterView),
		map[neighborShardKey]ton.BlockIDExt{{workchain: id.Workchain, shard: id.Shard}: id},
		[]PreviousBlock{{ID: id, State: partial, Proven: true}},
		nil,
		nil,
		false,
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(neighbors) != 1 || neighbors[0].OutMsgQueueInfo == nil {
		t.Fatalf("loaded neighbors = %+v, want canonical full state", neighbors)
	}
}

// countingNeighborStore answers no read and counts every one it is asked for.
type countingNeighborStore struct {
	reads atomic.Int64
}

func (s *countingNeighborStore) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	s.reads.Add(1)

	return nil, storage.ErrNotFound
}

func (s *countingNeighborStore) LoadStateCellTree(
	context.Context,
	ton.BlockIDExt,
	[]byte,
) (*cell.Cell, error) {
	s.reads.Add(1)

	return nil, storage.ErrNotFound
}

func (s *countingNeighborStore) BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	s.reads.Add(1)

	return nil, storage.ErrNotFound
}

func (*countingNeighborStore) WaitBlockArtifacts(context.Context, ton.BlockIDExt) error {
	return storage.ErrNotFound
}

func TestLocalAcquisitionPublishesResidentMasterchainViewBeforeStorage(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = acquisition.PublishMasterchainView(
		context.Background(),
		fixture.request.Groups,
		fixture.request.Previous.Block,
		fixture.request.Previous.State,
	); err != nil {
		t.Fatalf("publish resident view: %v", err)
	}
	// The store is empty, so a projection that reaches storage cannot succeed:
	// returning at all proves the resident view was consulted first.
	projected, err := acquisition.projectedMasterView(
		context.Background(),
		fixture.request.Previous.ID,
		time.Now(),
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatalf("project resident view without stored artifacts: %v", err)
	}
	if projected.previous.Block != fixture.request.Previous.Block ||
		projected.previous.State != fixture.request.Previous.State ||
		projected.context.Groups != fixture.request.Groups {
		t.Fatal("resident masterchain view did not preserve the coherent hook artifacts")
	}
	if err = acquisition.PublishMasterchainView(
		context.Background(),
		fixture.request.Groups,
		fixture.request.Previous.Block,
		fixture.request.Previous.State,
	); err != nil {
		t.Fatalf("idempotent resident publish: %v", err)
	}
}

// The applied-block hook and controller reconciliation both publish, each
// internally serialized but not against the other, and the view is now built
// outside the lock. A publish that lost that race must not reinstall the older
// view: the caller has no way to tell, and the install also retires block-cache
// entries the newer view still references.
func TestLocalAcquisitionPublishRefusesToRegressTheResidentMasterchainView(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = acquisition.PublishMasterchainView(
		context.Background(),
		fixture.request.Groups,
		fixture.request.Previous.Block,
		fixture.request.Previous.State,
	); err != nil {
		t.Fatalf("publish resident view: %v", err)
	}

	// Stand in for the newer view the other publisher installed while this one
	// was still decoding its own.
	newer := *acquisition.master.view
	newer.context.ID = *newer.context.ID.Copy()
	newer.context.ID.SeqNo++
	acquisition.master.view = &newer
	acquisition.blocks.mu.Lock()
	generation := acquisition.blocks.generation
	acquisition.blocks.mu.Unlock()

	if err = acquisition.PublishMasterchainView(
		context.Background(),
		fixture.request.Groups,
		fixture.request.Previous.Block,
		fixture.request.Previous.State,
	); err != nil {
		t.Fatalf("stale resident publish: %v", err)
	}
	if acquisition.master.view != &newer {
		t.Fatal("a stale publish replaced a newer resident masterchain view")
	}
	acquisition.blocks.mu.Lock()
	advanced := acquisition.blocks.generation
	acquisition.blocks.mu.Unlock()
	if advanced != generation {
		t.Fatal("a stale publish retired block cache entries the newer view still references")
	}
}

func TestValidationResolvesCandidateMasterchainReferenceAheadOfCollatorView(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	previous := candidatePrevious(t, candidate)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	asOf := time.Unix(int64(firstViewGenUtime(t, candidate)), 0)
	projected, err := tracker.Project(fixture.request.Groups, groups.ApplyInput{
		Block: previous.ID,
		Root:  previous.State,
		AsOf:  asOf,
	})
	if err != nil {
		t.Fatal(err)
	}
	acquisition := &LocalAcquisition{
		store: &localValidationStore{states: []localValidationState{
			{block: fixture.request.Previous.ID, root: fixture.request.Previous.State},
			{block: previous.ID, root: previous.State, data: previous.Block},
		}},
		groups: &localAcquisitionTestGroups{
			snapshot:  fixture.request.Groups,
			projected: projected,
		},
		configs: localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)},
	}
	base, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	reference := &tlb.ExtBlkRef{
		SeqNo:    previous.ID.SeqNo,
		RootHash: bytes.Clone(previous.ID.RootHash),
		FileHash: bytes.Clone(previous.ID.FileHash),
	}
	view, err := acquisition.validationMasterReference(
		context.Background(),
		base,
		reference,
		asOf,
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatalf("resolve ahead candidate masterchain reference: %v", err)
	}
	if !view.context.ID.Equals(&previous.ID) {
		t.Fatalf("resolved masterchain view = %v, want %v", view.context.ID, previous.ID)
	}
}

func TestLocalAcquisitionPreparesSessionFromRetainedMasterchainView(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	session := localMasterTestSession(t, fixture.request.Groups)
	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)

	current := *fixture.request.Groups
	current.MasterchainBlock = cloneBlockID(current.MasterchainBlock)
	current.MasterchainBlock.SeqNo++
	current.MasterchainBlock.RootHash[0]++

	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{
		targetShardIdent(session.Shard),
	}); err != nil {
		t.Fatal(err)
	}
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder: testBuilder(),
		Store: &localValidationStore{states: []localValidationState{{
			block: fixture.request.Previous.ID,
			root:  fixture.request.Previous.State,
		}}},
		Groups: &localAcquisitionTestGroups{
			snapshot:  &current,
			projected: fixture.request.Groups,
		},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = acquisition.PrepareSession(t.Context(), session.Session, update); err != nil {
		t.Fatalf("prepare session from retained masterchain view: %v", err)
	}
}

func TestMasterValidationTopsRequiresCandidateDescriptor(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCandidate(context.Background(), fixture.request.Config, candidate)
	if err != nil {
		t.Fatal(err)
	}
	verified.collated.roots = nil

	acquisition := &LocalAcquisition{configs: localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)}}
	master, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = acquisition.masterValidationTops(context.Background(), master, &verified); err == nil {
		t.Fatal("changed shard registry without a candidate TopBlockDescrSet was accepted")
	}
}

func TestMasterValidationTopsRejectsUnsignedCandidateDescriptor(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCandidate(context.Background(), fixture.request.Config, candidate)
	if err != nil {
		t.Fatal(err)
	}

	acquisition := &LocalAcquisition{configs: localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)}}
	master, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = acquisition.masterValidationTops(context.Background(), master, &verified)
	if err == nil || !strings.Contains(err.Error(), "no validator signatures") {
		t.Fatalf("unsigned TopBlockDescr error = %v", err)
	}
}

// TestPrepareValidationTopValidatorSetRejectsNextSet pins the strictness C++
// expresses by withholding allow_next_vset from check_one_shard's prevalidate
// (validate-query.cpp:2044-2045): a masterchain block may only claim a shard top
// whose descriptor the shard's current set signed. The next set stays legal on
// the broadcast-admission side alone.
func TestPrepareValidationTopValidatorSetRejectsNextSet(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	catchainSeqno := uint32(123)
	raw := &tlb.BlockchainConfig{Root: fixture.request.Config.execution.Root()}
	nextValidators, err := blockproof.NextValidatorsForBlock(raw, &fixture.newShard, catchainSeqno)
	if err != nil {
		t.Fatal(err)
	}
	next, err := blockproof.PrepareValidatorSet(catchainSeqno, nextValidators)
	if err != nil {
		t.Fatal(err)
	}
	currentValidators, err := blockproof.CurrentValidatorsForBlock(raw, &fixture.newShard, catchainSeqno)
	if err != nil {
		t.Fatal(err)
	}
	current, err := blockproof.PrepareValidatorSet(catchainSeqno, currentValidators)
	if err != nil {
		t.Fatal(err)
	}
	if current.Hash() == next.Hash() {
		t.Skip("fixture current and next shard validator sets are identical")
	}

	selected, err := prepareValidationTopValidatorSet(
		fixture.request.Config,
		fixture.newShard,
		catchainSeqno,
		next.Hash(),
	)
	if err == nil {
		t.Fatalf("next-set TopBlockDescr was accepted with validator hash %08x", selected.Hash())
	}
	if !strings.Contains(err.Error(), "is not the shard's current set") {
		t.Fatalf("next-set TopBlockDescr error = %v", err)
	}

	selected, err = prepareValidationTopValidatorSet(
		fixture.request.Config,
		fixture.newShard,
		catchainSeqno,
		current.Hash(),
	)
	if err != nil {
		t.Fatalf("select current validator set: %v", err)
	}
	if selected.Hash() != current.Hash() {
		t.Fatalf("selected validator hash = %08x, want current %08x", selected.Hash(), current.Hash())
	}
}

type localValidationState struct {
	block ton.BlockIDExt
	root  *cell.Cell
	data  *cell.Cell
}

type localValidationStore struct {
	states []localValidationState
}

func (s *localValidationStore) BlockState(
	ctx context.Context,
	block ton.BlockIDExt,
) (*storage.BlockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i := range s.states {
		if !s.states[i].block.Equals(&block) {
			continue
		}
		hash := s.states[i].root.Hash()
		return &storage.BlockState{
			Block:         cloneBlockID(block),
			StateRootHash: bytes.Clone(hash),
			Cell:          s.states[i].root,
		}, nil
	}

	return nil, storage.ErrNotFound
}

func (s *localValidationStore) LoadStateCellTree(
	ctx context.Context,
	block ton.BlockIDExt,
	_ []byte,
) (*cell.Cell, error) {
	state, err := s.BlockState(ctx, block)
	if err != nil {
		return nil, err
	}

	return state.Cell, nil
}

func (s *localValidationStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i := range s.states {
		if s.states[i].block.Equals(&block) && s.states[i].data != nil {
			return s.states[i].data, nil
		}
	}

	return nil, storage.ErrNotFound
}

func (s *localValidationStore) WaitBlockArtifacts(ctx context.Context, block ton.BlockIDExt) error {
	_, err := s.BlockState(ctx, block)
	if err == nil && block.SeqNo != 0 {
		_, err = s.BlockRoot(ctx, block)
	}

	return err
}

func localValidationMasterFixture(t *testing.T) (masterBuildFixture, *cell.Cell) {
	t.Helper()
	fixture := newMasterBuildFixture(t, false)

	base := emptyCandidateRequest(t)
	var shard tlb.ShardStateUnsplit
	if err := parseExact(&shard, base.Previous.State); err != nil {
		t.Fatal(err)
	}
	shard.Seqno = 0
	shard.GenUTime = fixture.oldState.GenUTime - 1
	shard.GenLT = fixture.oldState.GenLT - 1_000
	shard.McStateExtra = nil
	shardRoot, err := tlb.ToCell(&shard)
	if err != nil {
		t.Fatal(err)
	}
	shardHash := shardRoot.HashKey()
	fixture.oldShard = ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(shard.ShardIdent.GetShardID()),
		RootHash:  bytes.Clone(shardHash[:]),
		FileHash:  bytes.Repeat([]byte{0x31}, 32),
	}
	fixture.oldExtra.ShardHashes = masterBuildShardHashes(t, masterBuildShardDescriptor(
		t,
		fixture.oldShard,
		0,
		shard.GenUTime,
	))
	extraRoot, err := tlb.ToCell(&fixture.oldExtra)
	if err != nil {
		t.Fatal(err)
	}
	fixture.oldState.McStateExtra = extraRoot
	masterRoot, err := tlb.ToCell(&fixture.oldState)
	if err != nil {
		t.Fatal(err)
	}
	masterHash := masterRoot.HashKey()
	fixture.request.Previous.ID.RootHash = bytes.Clone(masterHash[:])
	fixture.request.Previous.State = masterRoot

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := tracker.Apply(groups.ApplyInput{
		Block: fixture.request.Previous.ID,
		Root:  masterRoot,
		AsOf:  time.Unix(int64(fixture.oldState.GenUTime), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Groups = tracked.Snapshot
	fixture.request.ShardTops = nil

	return fixture, shardRoot
}

func localValidationSessionUpdate(
	t *testing.T,
	session Session,
	snapshot *groups.Snapshot,
	masterchain ton.BlockIDExt,
) SessionUpdate {
	t.Helper()
	var active *groups.Session
	for i := range snapshot.Active {
		if snapshot.Active[i].ID == session.ID {
			active = &snapshot.Active[i]
			break
		}
	}
	if active == nil {
		t.Fatal("session is absent from validation snapshot")
	}
	update := SessionUpdate{
		SessionID:        session.ID,
		TargetRate:       time.Second,
		MasterchainBlock: cloneBlockID(masterchain),
		Registered:       append([]groups.ShardDescription(nil), active.Registered...),
	}
	if active.FinalizedBlock != nil {
		update.HasFinalizedBlock = true
		update.FinalizedBlock = cloneBlockID(*active.FinalizedBlock)
	}

	return update
}

// benchNeighborQueueMessages is how many enqueued messages the benchmarked
// neighbor carries. The mainnet fixture cannot serve here: its shard covers the
// whole basechain, so every message it produces is delivered in the same block
// and its outbound queue is empty — and an empty queue measures nothing, since
// the cost the trace flag controls is one usage-tree node plus one pinned
// materialized cell per queue cell the replay scan walks.
const benchNeighborQueueMessages = 2000

func benchNeighborQueuePrevious(tb testing.TB) (PreviousBlock, msgpool.ShardIdent) {
	tb.Helper()

	request := emptyCandidateRequest(tb)
	source := blockShardIdent(request.Previous.ID)
	src := address.NewAddress(0, 0, bytes.Repeat([]byte{0x91}, 32))
	createdLT := requestStartLT(tb, request) - 10
	previous := request.Previous
	for i := range benchNeighborQueueMessages {
		destination := make([]byte, 32)
		binary.BigEndian.PutUint32(destination, uint32(i))
		message, value := queuedInternalWithReferencedBody(
			tb,
			src,
			address.NewAddress(0, 0, destination),
			createdLT,
			request.Header.GenUtime-1,
			tlb.FromNanoTONU(100_000),
			tlb.FromNanoTONU(100_000),
			96,
			source,
		)
		previous.State = stateWithQueueMessage(tb, previous.State, message.Key, value)
	}

	return previous, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
}

func benchmarkNeighborQueueScan(b *testing.B, trace bool) {
	b.Helper()

	previous, target := benchNeighborQueuePrevious(b)
	bound := semanticMessageBound{lt: ^uint64(0)}
	for i := range bound.hash {
		bound.hash[i] = 0xff
	}
	scan := func() {
		view, err := localViewFromPrevious(previous, true, trace)
		if err != nil {
			b.Fatal(err)
		}
		visited := 0
		if err = walkSemanticQueuePrefix(view.queue.OutQueue, target, bound, func(semanticQueueEntry) error {
			visited++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if visited != benchNeighborQueueMessages {
			b.Fatalf("queue scan visited %d messages, want %d", visited, benchNeighborQueueMessages)
		}
	}
	scan()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		scan()
	}
}

// BenchmarkNeighborQueueScanTraced is the collation shape on a network that
// runs capFullCollatedData; the untraced row is candidate validation and a
// collation without the capability, which discard the proof either way.
func BenchmarkNeighborQueueScanTraced(b *testing.B) {
	benchmarkNeighborQueueScan(b, true)
}

func BenchmarkNeighborQueueScanUntraced(b *testing.B) {
	benchmarkNeighborQueueScan(b, false)
}

// TestLoadExpectedNeighborsTracesOnlyWhenProofsAreNeeded pins the split the
// needProofs flag introduces: the neighbor view is otherwise identical, so a
// future caller that forgets the flag loses the proof rather than the queue.
func TestLoadExpectedNeighborsTracesOnlyWhenProofsAreNeeded(t *testing.T) {
	candidate, err := testBuilder().BuildShard(context.Background(), emptyCandidateRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	canonical := candidatePrevious(t, candidate)
	acquisition := &LocalAcquisition{store: &localValidationStore{states: []localValidationState{{
		block: canonical.ID,
		root:  canonical.State,
		data:  canonical.Block,
	}}}}
	id := canonical.ID

	load := func(needProofs bool) *localNeighborView {
		t.Helper()
		views := make(map[msgpool.ShardIdent]*localNeighborView)
		if _, err := acquisition.loadExpectedNeighbors(
			context.Background(),
			new(localMasterView),
			map[neighborShardKey]ton.BlockIDExt{{workchain: id.Workchain, shard: id.Shard}: id},
			nil,
			views,
			nil,
			needProofs,
			acquisitionReadImmediate,
		); err != nil {
			t.Fatal(err)
		}
		view := views[blockShardIdent(id)]
		if view == nil {
			t.Fatal("neighbor view was not registered")
		}
		return view
	}

	traced := load(true)
	if traced.proof == nil {
		t.Fatal("a collation that attaches proofs got an untraced neighbor view")
	}
	plain := load(false)
	if plain.proof != nil {
		t.Fatal("a neighbor view was traced although no proof is emitted")
	}
	if !equalCell(plain.state.OutMsgQueueInfo, traced.state.OutMsgQueueInfo) {
		t.Fatal("untraced neighbor view decoded a different outbound queue")
	}
	if !equalProcessedRecords(plain.processed, traced.processed) {
		t.Fatal("untraced neighbor view decoded a different processed frontier")
	}
}

// The prepared-config cache is keyed by config root hash, so without a cap it
// retains one parsed config per distinct config the process ever sees.
func TestLocalConfigCacheEvictsOldestBeyondCap(t *testing.T) {
	cache := localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)}
	hashes := make([]cell.Hash, 0, maxLocalPreparedConfigs+2)
	for i := range maxLocalPreparedConfigs + 2 {
		var hash cell.Hash
		hash[0] = byte(i + 1)
		hashes = append(hashes, hash)
		cache.store(hash, localPreparedConfig{config: &Config{}})
	}

	if len(cache.entries) != maxLocalPreparedConfigs || len(cache.order) != maxLocalPreparedConfigs {
		t.Fatalf("cache holds %d entries / %d ordered keys, want %d of each",
			len(cache.entries), len(cache.order), maxLocalPreparedConfigs)
	}
	for _, evicted := range hashes[:2] {
		if _, exists := cache.entries[evicted]; exists {
			t.Fatalf("oldest config %x survived eviction", evicted[:1])
		}
	}
	for _, retained := range hashes[2:] {
		if _, exists := cache.entries[retained]; !exists {
			t.Fatalf("config %x was evicted before older ones", retained[:1])
		}
	}

	// An incumbent wins, so every caller of one config root shares one instance.
	first := cache.store(hashes[2], localPreparedConfig{config: &Config{}})
	again := cache.store(hashes[2], localPreparedConfig{config: &Config{}})
	if first.config != again.config {
		t.Fatal("cache replaced an incumbent prepared config")
	}
	if len(cache.entries) != maxLocalPreparedConfigs {
		t.Fatalf("repeated store changed the cache size to %d", len(cache.entries))
	}
}

// masterViewCacheTestView builds the minimum a cache-level test needs: the id
// is the whole key, and views are immutable so nothing else is read.
func masterViewCacheTestView(seqno uint32, fill byte) *localMasterView {
	root := make([]byte, 32)
	file := make([]byte, 32)
	for i := range root {
		root[i] = fill
		file[i] = fill ^ 0xff
	}
	return &localMasterView{
		context: MasterchainContext{
			ID: ton.BlockIDExt{
				Workchain: masterchainWorkchainID,
				Shard:     math.MinInt64,
				SeqNo:     seqno,
				RootHash:  root,
				FileHash:  file,
			},
		},
	}
}

// A session update and a shard candidate's masterchain reference both routinely
// lag the newest applied block, so the view the resident slot replaces is
// exactly the one validation asks for next.
func TestLocalMasterViewCacheServesDemotedResident(t *testing.T) {
	var cache localMasterViewCache
	first := masterViewCacheTestView(10, 0x11)
	second := masterViewCacheTestView(11, 0x22)

	cache.view = first
	if view, resident := cache.lookup(first.context.ID); view != first || !resident {
		t.Fatalf("resident lookup = (%p, %v), want (%p, true)", view, resident, first)
	}

	cache.demoteLocked(cache.view)
	cache.view = second
	cache.sweepLocked(second.context.ID.SeqNo)

	view, resident := cache.lookup(first.context.ID)
	if view != first {
		t.Fatalf("demoted lookup = %p, want %p", view, first)
	}
	if resident {
		t.Fatal("a demoted view reported itself as resident; its group binding would go unchecked")
	}
	if view, resident = cache.lookup(second.context.ID); view != second || !resident {
		t.Fatalf("new resident lookup = (%p, %v), want (%p, true)", view, resident, second)
	}
}

// A block root hash can never name two states, but a lookup must still refuse an
// id that only agrees on that hash.
func TestLocalMasterViewCacheRejectsMismatchedID(t *testing.T) {
	var cache localMasterViewCache
	view := masterViewCacheTestView(10, 0x33)
	cache.demoteLocked(view)

	other := view.context.ID
	other.SeqNo++
	if got, _ := cache.lookup(other); got != nil {
		t.Fatal("lookup returned a view for a different block id")
	}
}

func TestLocalMasterViewCacheEvictsAndSweeps(t *testing.T) {
	var cache localMasterViewCache
	views := make([]*localMasterView, 0, maxLocalMasterViews+2)
	for i := range maxLocalMasterViews + 2 {
		view := masterViewCacheTestView(uint32(100+i), byte(i+1))
		views = append(views, view)
		cache.demoteLocked(view)
	}
	if len(cache.entries) != maxLocalMasterViews || len(cache.order) != maxLocalMasterViews {
		t.Fatalf("cache holds %d entries / %d ordered keys, want %d of each",
			len(cache.entries), len(cache.order), maxLocalMasterViews)
	}
	for _, evicted := range views[:2] {
		if got, _ := cache.lookup(evicted.context.ID); got != nil {
			t.Fatalf("view at seqno %d survived eviction", evicted.context.ID.SeqNo)
		}
	}

	// A view that fell further behind than the block cache keeps its blocks is
	// dropped even though insertion order would have kept it.
	resident := views[len(views)-1].context.ID.SeqNo + maxLocalMasterViewLag
	cache.sweepLocked(resident)
	for _, view := range views {
		got, _ := cache.lookup(view.context.ID)
		wantKept := view.context.ID.SeqNo+maxLocalMasterViewLag > resident
		if (got != nil) != wantKept {
			t.Fatalf("view at seqno %d kept = %v, want %v", view.context.ID.SeqNo, got != nil, wantKept)
		}
	}
}

// store is the cache-only path: unlike a resident install it must not retire
// block-cache entries, because those generations bound one masterchain block.
func TestLocalMasterViewCacheStoreLeavesResidentAlone(t *testing.T) {
	var cache localMasterViewCache
	resident := masterViewCacheTestView(20, 0x44)
	cache.view = resident

	cache.store(masterViewCacheTestView(19, 0x55))
	if cache.view != resident {
		t.Fatal("store replaced the resident view")
	}
	// Storing the resident id again must not duplicate it into the entry map.
	cache.store(masterViewCacheTestView(20, 0x44))
	if len(cache.entries) != 1 {
		t.Fatalf("entries = %d, want only the non-resident view", len(cache.entries))
	}
}
