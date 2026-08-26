package collator

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

func openLocalTestBranch(t testing.TB, pool *msgpool.Pool, destination msgpool.ShardIdent) *msgpool.Branch {
	t.Helper()
	branch, err := pool.Internals().OpenBranch(destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(branch.Close)

	return branch
}

func TestLocalShardTopQueueIsRequestLocal(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	destination := blockShardIdent(fixture.request.Previous.ID)
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	view, err := localViewFromPrevious(fixture.request.Previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	acquisition := &LocalAcquisition{messages: pool}
	if _, err = acquisition.localSeedCut(destination, destination, view); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Internals().SourceTop(destination, destination); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatalf("request-local shard top changed finalized internals: %v", err)
	}
}

func TestValidateAcquisitionGroupRetainsFinalizedBlockWithoutExactDescriptor(t *testing.T) {
	id := [32]byte{0x71}
	shardID := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	genesis := ton.BlockIDExt{
		Workchain: shardID.Workchain,
		Shard:     shardID.Shard,
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	finalized := ton.BlockIDExt{
		Workchain: shardID.Workchain,
		Shard:     shardID.Shard,
		SeqNo:     11,
		RootHash:  bytes.Repeat([]byte{0x31}, 32),
		FileHash:  bytes.Repeat([]byte{0x32}, 32),
	}
	validator := groups.Validator{PublicKey: [32]byte{0x41}, ADNL: [32]byte{0x42}, Weight: 7}
	session := ActivatedSession{
		Session: Session{
			ID:                   id,
			Shard:                shardID,
			CatchainSeqno:        5,
			SlotsPerLeaderWindow: 4,
			Validators: []SessionValidator{{
				PublicKey: validator.PublicKey,
				ADNLID:    validator.ADNL,
				Weight:    validator.Weight,
			}},
		},
		Genesis:        []ton.BlockIDExt{genesis},
		MinMasterchain: master,
	}
	update := SessionUpdate{
		SessionID:         id,
		MasterchainBlock:  master,
		HasFinalizedBlock: true,
		FinalizedBlock:    finalized,
	}
	group := groups.Session{
		ID:             id,
		Shard:          shardID,
		CatchainSeqno:  session.CatchainSeqno,
		Validators:     []groups.Validator{validator},
		Genesis:        []ton.BlockIDExt{genesis},
		MinMasterchain: master,
		FinalizedBlock: nil,
	}
	snapshot := &groups.Snapshot{Active: []groups.Session{group}}
	if err := validateAcquisitionGroup(session, update, snapshot, true); err != nil {
		t.Fatalf("retained finalized observation without exact descriptor: %v", err)
	}

	exact := finalized
	exact.SeqNo++
	exact.RootHash = bytes.Repeat([]byte{0x51}, 32)
	exact.FileHash = bytes.Repeat([]byte{0x52}, 32)
	snapshot.Active[0].FinalizedBlock = &exact
	if err := validateAcquisitionGroup(session, update, snapshot, true); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("conflicting exact finalized observation error = %v, want ErrInvalidInput", err)
	}
	update.FinalizedBlock = exact
	if err := validateAcquisitionGroup(session, update, snapshot, true); err != nil {
		t.Fatalf("matching exact finalized observation: %v", err)
	}
}

func TestLocalViewFromPreviousReadsForkedAccountsAsProof(t *testing.T) {
	request := emptyCandidateRequest(t)
	request.Previous.State = stateWithAccounts(t, request.Previous.State, activeContracts(
		t,
		request.Header.GenUtime,
		activeContract{
			address: address.NewAddress(0, 0, bytes.Repeat([]byte{0x00}, 32)),
			code:    externalAcceptCode(t),
			balance: 1_000_000_000,
		},
		activeContract{
			address: address.NewAddress(0, 0, bytes.Repeat([]byte{0xff}, 32)),
			code:    externalAcceptCode(t),
			balance: 1_000_000_000,
		},
	))

	view, err := localViewFromPrevious(request.Previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProofVirtualized(proof, request.Previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	proven := request.Previous
	proven.State = virtualState
	if _, err = localViewFromPrevious(proven, false, false); err != nil {
		t.Fatal(err)
	}
}

func TestLocalInternalSourceRejectsUntrackedOlderPredecessor(t *testing.T) {
	request := emptyCandidateRequest(t)
	destination := blockShardIdent(request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}

	ref, err := localSourceRef(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	newer := ref
	newer.Seqno++
	newer.RootHash[0]++
	if err = pool.Internals().Seed(destination, destination, newer, nil, 0); err != nil {
		t.Fatal(err)
	}

	acquisition := &LocalAcquisition{messages: pool}
	if err = acquisition.ensureInternalSource(
		destination,
		destination,
		ref,
		request.Previous.State,
	); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("older predecessor error = %v, want ErrAcquisitionNotReady", err)
	}
	top, err := pool.Internals().SourceTop(destination, destination)
	if err != nil {
		t.Fatal(err)
	}
	if top != newer {
		t.Fatalf("internal source top regressed to %+v, want %+v", top, newer)
	}
}

func TestLocalInternalCutUsesRequestLocalOlderPredecessor(t *testing.T) {
	request := emptyCandidateRequest(t)
	destination := blockShardIdent(request.Previous.ID)
	queued, enqueued := queuedInternal(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x31}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x32}, 32)),
		1,
		request.Header.GenUtime,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		0,
		destination,
	)
	request.Previous.State = stateWithQueueMessage(t, request.Previous.State, queued.Key, enqueued)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}

	ref, err := localSourceRef(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	newer := ref
	newer.Seqno++
	newer.RootHash[0]++
	if err = pool.Internals().Seed(destination, destination, newer, nil, 0); err != nil {
		t.Fatal(err)
	}
	view, err := localViewFromPrevious(request.Previous, true, true)
	if err != nil {
		t.Fatal(err)
	}

	acquisition := &LocalAcquisition{messages: pool}
	branch := openLocalTestBranch(t, pool, destination)
	cut, err := acquisition.cutCommittedViews(
		branch,
		destination,
		map[msgpool.ShardIdent]*localNeighborView{destination: view},
		nil,
		nil,
		true,
		&prewarmHints{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].EnvHash != queued.EnvHash {
		t.Fatalf("request-local queue returned %+v, want envelope %x", cut.Messages, queued.EnvHash)
	}
	top, err := pool.Internals().SourceTop(destination, destination)
	if err != nil {
		t.Fatal(err)
	}
	if top != newer {
		t.Fatalf("request-local cut regressed shared source to %+v, want %+v", top, newer)
	}
}

func TestLocalInternalSourceUsesRetainedCommittedHistory(t *testing.T) {
	request := emptyCandidateRequest(t)
	destination := blockShardIdent(request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}

	ref, err := localSourceRef(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, ref, nil, 0); err != nil {
		t.Fatal(err)
	}
	newer := ref
	newer.Seqno++
	newer.RootHash[0]++
	if err = pool.Internals().ApplyBlock(
		destination,
		destination,
		newer,
		&msgpool.InternalsDelta{},
	); err != nil {
		t.Fatal(err)
	}

	acquisition := &LocalAcquisition{messages: pool}
	if err = acquisition.ensureInternalSource(destination, destination, ref, request.Previous.State); err != nil {
		t.Fatal(err)
	}
	top, err := pool.Internals().SourceTop(destination, destination)
	if err != nil {
		t.Fatal(err)
	}
	if top != newer {
		t.Fatalf("internal source top regressed to %+v, want %+v", top, newer)
	}
}

func TestLocalResolveArtifactKeepsPrivateBranchAfterCommittedPromotion(t *testing.T) {
	request := emptyCandidateRequest(t)
	destination := blockShardIdent(request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	ref, err := localSourceRef(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, ref, nil, 0); err != nil {
		t.Fatal(err)
	}

	candidateID := simplex.CandidateID{Slot: 7, Hash: [32]byte{0x71}}
	tip := ref.RootHash
	state := localCandidateState{
		block:     request.Previous,
		queueTip:  &tip,
		queueBase: []PreviousBlock{request.Previous},
	}
	key, err := blockRootKey(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed := &localAcquisitionSession{
		session: Session{Shard: groups.ShardID{
			Workchain: request.Previous.ID.Workchain,
			Shard:     request.Previous.ID.Shard,
		}},
		candidates: map[simplex.CandidateID]localCandidateState{candidateID: state},
		blocks:     map[[32]byte]localCandidateState{key: state},
	}
	acquisition := &LocalAcquisition{messages: pool}
	resolved, err := acquisition.resolveArtifactState(context.Background(), managed, CandidateArtifact{
		Candidate: simplex.Candidate{ID: candidateID, Block: request.Previous.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.queueTip == nil || *resolved.queueTip != tip || len(resolved.queueBase) != 1 {
		t.Fatalf("applied candidate lost its private queue lineage: %+v", resolved)
	}
	if managed.candidates[candidateID].queueTip == nil || managed.blocks[key].queueTip == nil {
		t.Fatal("committed promotion mutated the private session lineage")
	}
}

func TestLocalCandidateCutKeepsSpeculativeQueueChain(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()

	destination := blockShardIdent(fixture.request.Previous.ID)
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	baseRef, err := localSourceRef(fixture.request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, baseRef, nil, 0); err != nil {
		t.Fatal(err)
	}

	tip := [32]byte{0x8a}
	branch := openLocalTestBranch(t, pool, destination)
	if err = branch.AddCandidate(msgpool.CandidateRequest{
		ID:    tip,
		Seqno: baseRef.Seqno + 1,
		Delta: &msgpool.InternalsDelta{},
		Base:  candidateSources([]PreviousBlock{fixture.request.Previous}),
	}); err != nil {
		t.Fatal(err)
	}

	speculative := fixture.request.Previous
	speculative.ID = testBlockID(
		fixture.request.Previous.ID.Workchain,
		fixture.request.Previous.ID.Shard,
		fixture.request.Previous.ID.SeqNo+1,
		0x8b,
	)
	acquisition := &LocalAcquisition{messages: pool}
	if _, err = acquisition.cutCommittedViews(branch, destination, map[msgpool.ShardIdent]*localNeighborView{
		destination: {previous: speculative},
	}, []PreviousBlock{fixture.request.Previous}, &tip,
		true,
		&prewarmHints{}); err != nil {
		t.Fatalf("cut chained speculative candidate: %v", err)
	}

	top, err := pool.Internals().SourceTop(destination, destination)
	if err != nil {
		t.Fatal(err)
	}
	if top != baseRef {
		t.Fatalf("committed queue top = %+v, want base %+v", top, baseRef)
	}
}

func TestLocalCandidateCutTracksPromotedPoolBase(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()

	destination := blockShardIdent(fixture.request.Previous.ID)
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	base, err := localSourceRef(fixture.request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, base, nil, 0); err != nil {
		t.Fatal(err)
	}
	parent := msgpool.SourceRef{Seqno: base.Seqno + 1, RootHash: [32]byte{0x91}}
	child := msgpool.SourceRef{Seqno: base.Seqno + 2, RootHash: [32]byte{0x92}}
	branch := openLocalTestBranch(t, pool, destination)
	if err = branch.AddCandidate(msgpool.CandidateRequest{
		ID: parent.RootHash, Seqno: parent.Seqno,
		Base:  candidateSources([]PreviousBlock{fixture.request.Previous}),
		Delta: &msgpool.InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	if err = branch.AddCandidate(msgpool.CandidateRequest{
		ID: child.RootHash, Parent: &parent.RootHash, Seqno: child.Seqno,
		Delta: &msgpool.InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().ApplyBlock(destination, destination, parent, &msgpool.InternalsDelta{}); err != nil {
		t.Fatal(err)
	}

	applied := fixture.request.Previous
	applied.ID.SeqNo = parent.Seqno
	applied.ID.RootHash = bytes.Clone(parent.RootHash[:])
	acquisition := &LocalAcquisition{messages: pool}
	if _, err = acquisition.cutCommittedViews(
		branch,
		destination,
		map[msgpool.ShardIdent]*localNeighborView{destination: {previous: applied}},
		[]PreviousBlock{fixture.request.Previous},
		&child.RootHash,
		true,
		&prewarmHints{},
	); err != nil {
		t.Fatalf("cut after parent promotion: %v", err)
	}
}

// The merge interleaves every run in canonical order and must not mutate its
// inputs: the committed cut and the request-local runs are borrowed.
func TestMergeLocalCutsInterleavesRunsInCanonicalOrder(t *testing.T) {
	message := func(lt uint64, hash byte) *msgpool.InternalMessage {
		var key msgpool.QueueKey
		key[len(key)-1] = hash

		return &msgpool.InternalMessage{EnqueuedLT: lt, Key: key}
	}
	committed := &msgpool.Cut{Messages: []*msgpool.InternalMessage{
		message(10, 1),
		message(40, 4),
	}}
	local := [][]*msgpool.InternalMessage{
		{message(20, 2), message(50, 5)},
		{message(30, 3), message(60, 6)},
	}

	cut := mergeLocalCuts(committed, local)
	if len(cut.Messages) != 6 {
		t.Fatalf("merged cut = %+v", cut)
	}
	for index, want := range []uint64{10, 20, 30, 40, 50, 60} {
		if cut.Messages[index].EnqueuedLT != want {
			t.Fatalf("message %d lt = %d, want %d", index, cut.Messages[index].EnqueuedLT, want)
		}
	}
	if len(committed.Messages) != 2 || len(local[0]) != 2 || len(local[1]) != 2 {
		t.Fatal("merge mutated an input run")
	}

	// More is carried from the committed cut alone; there is no local cap that
	// could truncate a run.
	committed.More = true
	if !mergeLocalCuts(committed, local).More {
		t.Fatal("merged cut dropped the committed More flag")
	}
}

func TestEffectiveShardMessageViewsUsesContinuedMergePredecessor(t *testing.T) {
	rootID := int64(shard.Root)
	left, err := shard.Child(rootID, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(rootID, false)
	if err != nil {
		t.Fatal(err)
	}
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(rootID)}
	leftSource := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}
	rightSource := msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}
	foreignSource := msgpool.ShardIdent{Workchain: 1, Shard: uint64(rootID)}
	leftView := &localNeighborView{previous: PreviousBlock{ID: testBlockID(0, left, 9, 0x41)}}
	rightView := &localNeighborView{previous: PreviousBlock{ID: testBlockID(0, right, 9, 0x51)}}
	foreignView := &localNeighborView{previous: PreviousBlock{ID: testBlockID(1, rootID, 9, 0x61)}}
	registered := map[msgpool.ShardIdent]*localNeighborView{
		leftSource:    leftView,
		rightSource:   rightView,
		foreignSource: foreignView,
	}
	predecessor := PreviousBlock{ID: testBlockID(0, rootID, 10, 0x31)}

	effective := effectiveShardMessageViews(target, []PreviousBlock{predecessor}, registered)
	if len(effective) != 2 || effective[foreignSource] != foreignView {
		t.Fatalf("effective views = %+v, want foreign and merged predecessor", effective)
	}
	if effective[leftSource] != nil || effective[rightSource] != nil {
		t.Fatal("continued-merge message views retained a registered child")
	}
	merged := effective[target]
	if merged == nil || !merged.previous.ID.Equals(&predecessor.ID) {
		t.Fatalf("merged message view = %+v, want predecessor %v", merged, predecessor.ID)
	}
	if registered[leftSource] != leftView || registered[rightSource] != rightView {
		t.Fatal("message-view normalization mutated proof views")
	}

	immediate := effectiveShardMessageViews(target, []PreviousBlock{
		{ID: leftView.previous.ID},
		{ID: rightView.previous.ID},
	}, registered)
	if len(immediate) != len(registered) || immediate[leftSource] != leftView || immediate[rightSource] != rightView {
		t.Fatal("immediate merge unexpectedly replaced its two predecessor views")
	}
}

func TestLocalFullProofProviderDoesNotTraceReplacedRegisteredSelfQueue(t *testing.T) {
	request := emptyCandidateRequest(t)
	target := blockShardIdent(request.Previous.ID)
	message, enqueued := queuedInternalWithReferencedBody(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x41}, 32)),
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x42}, 32)),
		requestStartLT(t, request)-10,
		request.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		96,
		target,
	)
	message.SourceSeqno = 0

	registeredPrevious := request.Previous
	registeredPrevious.ID = testBlockID(target.Workchain, int64(target.Shard), 0, 0x51)
	registeredPrevious.State = stateWithQueueMessage(t, registeredPrevious.State, message.Key, enqueued)
	registeredView, err := localViewFromPrevious(registeredPrevious, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, read := registeredView.proof.ReadSet().Contains(message.Root.HashKey()); read {
		t.Fatal("registered self queue message was read before the queue scan")
	}
	beforeProof, err := registeredView.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}

	registered := map[msgpool.ShardIdent]*localNeighborView{target: registeredView}
	effective := effectiveShardMessageViews(target, []PreviousBlock{request.Previous}, registered)
	if effective[target] == nil || effective[target].proof != nil ||
		!effective[target].previous.ID.Equals(&request.Previous.ID) {
		t.Fatal("self message source was not replaced with the exact predecessor")
	}

	provider := &localFullProofProvider{
		proofViews:   registered,
		messageViews: effective,
	}
	proofs, err := provider.BuildFullCollatedProofs(context.Background(), FullCollatedProofRequest{
		Previous:  request.Previous,
		Neighbors: []Neighbor{localNeighbor(registeredView, target)},
		QueueScan: &FullCollatedQueueScan{
			Target: target,
			LT:     message.EnqueuedLT,
			Hash:   processedInfinityHash,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(proofs) != 1 {
		t.Fatalf("registered zerostate proofs = %d, want 1", len(proofs))
	}
	// Emitting the registered state proof is still required, but the effective
	// predecessor owns the inbound scan. Therefore queue scanning must not add a
	// single cell to the registered proof.
	if !bytes.Equal(proofs[0].ToBOC(), beforeProof.ToBOC()) {
		t.Fatal("proof provider widened the stale registered self-state proof with queue scan reads")
	}
}

func TestContinuedMergeCutReadsMergedPredecessorQueue(t *testing.T) {
	request := emptyCandidateRequest(t)
	target := blockShardIdent(request.Previous.ID)
	left, err := shard.Child(request.Previous.ID.Shard, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(request.Previous.ID.Shard, false)
	if err != nil {
		t.Fatal(err)
	}
	queued, enqueued := queuedInternal(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32)),
		1,
		request.Header.GenUtime,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		0,
		target,
	)
	request.Previous.State = stateWithQueueMessage(t, request.Previous.State, queued.Key, enqueued)
	registered := map[msgpool.ShardIdent]*localNeighborView{
		{Workchain: target.Workchain, Shard: uint64(left)}: {
			previous: PreviousBlock{ID: testBlockID(target.Workchain, left, 9, 0x81)},
		},
		{Workchain: target.Workchain, Shard: uint64(right)}: {
			previous: PreviousBlock{ID: testBlockID(target.Workchain, right, 9, 0x91)},
		},
	}

	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{target}); err != nil {
		t.Fatal(err)
	}
	acquisition := &LocalAcquisition{messages: pool}
	branch := openLocalTestBranch(t, pool, target)
	cut, err := acquisition.cutCommittedViews(
		branch,
		target,
		effectiveShardMessageViews(target, []PreviousBlock{request.Previous}, registered),
		nil,
		nil,
		true,
		&prewarmHints{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].EnvHash != queued.EnvHash {
		t.Fatalf("continued-merge cut = %+v, want merged predecessor envelope %x", cut.Messages, queued.EnvHash)
	}
}

func TestLocalMasterMessagesIgnoreUnselectedReadyViews(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	destination := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll}
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	acquisition := &LocalAcquisition{
		messages: pool,
		configs: localConfigCache{
			entries: make(map[cell.Hash]localPreparedConfig),
		},
	}
	master, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	basechain := emptyCandidateRequest(t).Previous
	selectedPrevious := basechain
	selectedPrevious.ID = cloneBlockID(fixture.newShard)
	selectedView, err := localViewFromPrevious(selectedPrevious, true, true)
	if err != nil {
		t.Fatal(err)
	}
	deferredPrevious := basechain
	deferredPrevious.ID = testBlockID(1, basechain.ID.Shard, basechain.ID.SeqNo, 0x43)
	deferredView, err := localViewFromPrevious(deferredPrevious, true, true)
	if err != nil {
		t.Fatal(err)
	}
	selectedKey, err := blockRootKey(selectedPrevious.ID)
	if err != nil {
		t.Fatal(err)
	}
	deferredKey, err := blockRootKey(deferredPrevious.ID)
	if err != nil {
		t.Fatal(err)
	}
	provider := &localShardTopProvider{
		acquisition: acquisition,
		master:      master,
		views: map[[32]byte]*localNeighborView{
			selectedKey: selectedView,
			deferredKey: deferredView,
		},
	}
	if _, err = acquisition.acquireMasterMessages(
		context.Background(),
		openLocalTestBranch(t, pool, destination),
		master,
		[]PreviousBlock{fixture.request.Previous},
		nil,
		nil,
		fixture.request.ShardTops,
		provider,
		&prewarmHints{},
	); err != nil {
		t.Fatal(err)
	}
	deferredSource := blockShardIdent(deferredPrevious.ID)
	if _, err = pool.Internals().SourceTop(destination, deferredSource); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatalf("unselected readiness view entered the masterchain cut: %v", err)
	}
}

func TestLocalNeighborStateProofPrunesUnrelatedState(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	view, err := localViewFromPrevious(fixture.request.Previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	options := cell.BOCSerializeOptions{WithCRC32C: true}
	proofBOC, err := proof.ToBOCWithOptionsErr(options)
	if err != nil {
		t.Fatal(err)
	}
	fullBOC, err := fixture.request.Previous.State.ToBOCWithOptionsErr(options)
	if err != nil {
		t.Fatal(err)
	}
	if len(proofBOC) >= len(fullBOC) {
		t.Fatalf("usage proof is not compact: proof=%d full_state=%d", len(proofBOC), len(fullBOC))
	}
}

func TestLocalNeighborStateProofKeepsAuxiliaryStateForCXX(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	var sourceState tlb.ShardStateUnsplit
	if err := parseExact(&sourceState, fixture.request.Previous.State); err != nil {
		t.Fatal(err)
	}
	var sourceStats tlb.ShardStateStats
	if err := parseExact(&sourceStats, sourceState.Stats); err != nil {
		t.Fatal(err)
	}
	// A unique auxiliary hash prevents an equal cell loaded elsewhere in the
	// state DAG from making this path appear covered by accident.
	sourceStats.OverloadHistory ^= 0x3141592653589793
	statsCell, err := sourceStats.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	sourceState.Stats = statsCell
	stateRoot, err := tlb.ToCell(&sourceState)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Previous.State = stateRoot

	view, err := localViewFromPrevious(fixture.request.Previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, read := view.proof.ReadSet().Contains(stateRoot.MustRefHashAt(2)); !read {
		t.Fatal("auxiliary shard state was not traced into the neighbor proof")
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProof(proof, stateRoot.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	var provenStats tlb.ShardStateStats
	if err = parseExact(&provenStats, provenState.Stats); err != nil {
		t.Fatalf("proof cannot initialize C++ ShardStateQ: %v", err)
	}
}

func TestLocalNeighborStateProofLoadsLazyAuxiliaryState(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	lazyState, err := cell.FromBOCWithOptions(
		fixture.request.Previous.State.ToBOC(),
		cell.BOCParseOptions{TrustedHashes: true, Lazy: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	previous := fixture.request.Previous
	previous.State = lazyState
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, read := view.proof.ReadSet().Contains(lazyState.MustRefHashAt(2)); !read {
		t.Fatal("lazy auxiliary shard state was not traced into the neighbor proof")
	}
}

func TestLocalNeighborProofTracesImportedMessageContents(t *testing.T) {
	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	message, enqueued := queuedInternalWithReferencedBody(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32)),
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32)),
		requestStartLT(t, request)-10,
		request.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		96,
		source,
	)
	parsedEnvelope, err := parseSemanticEnvelope(message.EnvelopeCell)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := message.Key.MsgHash(), parsedEnvelope.message.HashKey(); got != want {
		t.Fatalf("test queue key message hash %x differs from parsed message hash %x", got, want)
	}
	previous := request.Previous
	previous.State = stateWithQueueMessage(t, previous.State, message.Key, enqueued)
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	target := msgpool.ShardIdent{Workchain: message.Key.NextHop().Workchain, Shard: msgpool.ShardAll}
	if err = traceInternalCut(
		FullCollatedQueueScan{Target: target, LT: message.EnqueuedLT, Hash: message.Root.HashKey()},
		[]*msgpool.InternalMessage{message},
		map[msgpool.ShardIdent]*localNeighborView{source: view},
	); err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProofVirtualized(proof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	queue, err := parseQueueInfo(provenState.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = loadSemanticQueueEntry(queue.OutQueue, message.Key, nil); err != nil {
		t.Fatalf("proven queue entry cannot be validated by the candidate consumer: %v", err)
	}
}

func TestLocalNeighborProofTracesProcessedQueuePrefix(t *testing.T) {
	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	target := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll}
	src := address.NewAddress(0, 0, bytes.Repeat([]byte{0x73}, 32))
	startLT := requestStartLT(t, request)

	messages := make([]*msgpool.InternalMessage, 3)
	values := make([]*cell.Cell, 3)
	for i := range messages {
		dst := address.NewAddress(0, 0xff, bytes.Repeat([]byte{byte(0x74 + i)}, 32))
		messages[i], values[i] = queuedInternalWithReferencedBody(
			t,
			src,
			dst,
			startLT-uint64(30-i*10),
			request.Header.GenUtime-1,
			tlb.FromNanoTONU(100_000),
			tlb.FromNanoTONU(100_000),
			96,
			source,
		)
	}
	previous := request.Previous
	for i := range messages {
		previous.State = stateWithQueueMessage(t, previous.State, messages[i].Key, values[i])
	}

	// An exact lookup of the imported message does not authenticate the older
	// already-processed leaf or the root of the first subtree beyond the new
	// ProcessedInfo bound. This is the incomplete proof the former builder
	// emitted under load.
	incomplete, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = loadSemanticQueueEntry(incomplete.queue.OutQueue, messages[1].Key, nil); err != nil {
		t.Fatal(err)
	}
	incompleteProof, err := incomplete.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	incompleteRoot, err := cell.UnwrapProofVirtualized(incompleteProof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var incompleteState tlb.ShardStateUnsplit
	if err = parseProofExact(&incompleteState, incompleteRoot); err != nil {
		t.Fatal(err)
	}
	incompleteQueue, err := parseNeighborQueueInfo(incompleteState.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	bound := semanticMessageBound{lt: messages[1].EnqueuedLT, hash: messages[1].Root.HashKey()}
	if err = walkSemanticQueuePrefix(incompleteQueue.OutQueue, target, bound, nil); err == nil {
		t.Fatal("exact message lookup unexpectedly produced a complete queue-prefix proof")
	}

	// The production trace follows OutputQueueMerger through the same bound.
	// It includes message 0 even though the message pool may omit it as already
	// processed, and only the augmentation boundary for message 2.
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	scan := FullCollatedQueueScan{Target: target, LT: bound.lt, Hash: bound.hash}
	if err = traceInternalCut(
		scan,
		[]*msgpool.InternalMessage{messages[1]},
		map[msgpool.ShardIdent]*localNeighborView{source: view},
	); err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProofVirtualized(proof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	queue, err := parseNeighborQueueInfo(provenState.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	if err = walkSemanticQueuePrefix(queue.OutQueue, target, bound, func(semanticQueueEntry) error {
		visited++
		return nil
	}); err != nil {
		t.Fatalf("queue-prefix proof cannot be consumed: %v", err)
	}
	if visited != 2 {
		t.Fatalf("visited queue entries = %d, want 2", visited)
	}
}

func TestWalkSemanticQueuePrefixAcceptsCXXTargetPathProof(t *testing.T) {
	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(0x4000000000000000)}
	src := address.NewAddress(0, 0, bytes.Repeat([]byte{0x81}, 32))
	startLT := requestStartLT(t, request)
	inside, insideValue := queuedInternalWithReferencedBody(
		t,
		src,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x21}, 32)),
		startLT-10,
		request.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		96,
		source,
	)
	outside, outsideValue := queuedInternalWithReferencedBody(
		t,
		src,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xe1}, 32)),
		startLT-20,
		request.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		96,
		source,
	)
	if !target.ContainsPrefix(inside.Key.NextHop()) || target.ContainsPrefix(outside.Key.NextHop()) {
		t.Fatal("test messages do not straddle the target shard prefix")
	}

	previous := request.Previous
	previous.State = stateWithQueueMessage(t, previous.State, inside.Key, insideValue)
	previous.State = stateWithQueueMessage(t, previous.State, outside.Key, outsideValue)
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	// C++ replace_by_prefix follows only the target child. An exact lookup has
	// the same proof shape: the sibling on the other side stays pruned.
	if _, err = loadSemanticQueueEntry(view.queue.OutQueue, inside.Key, nil); err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProofVirtualized(proof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	queue, err := parseNeighborQueueInfo(provenState.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	bound := semanticMessageBound{lt: inside.EnqueuedLT, hash: inside.Root.HashKey()}
	visited := 0
	if err = walkSemanticQueuePrefix(queue.OutQueue, target, bound, func(semanticQueueEntry) error {
		visited++
		return nil
	}); err != nil {
		t.Fatalf("C++ target-path proof cannot be consumed: %v", err)
	}
	if visited != 1 {
		t.Fatalf("visited queue entries = %d, want 1", visited)
	}
}

func TestWalkSemanticQueuePrefixSkipsLaterEqualLTPayload(t *testing.T) {
	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	target := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	src := address.NewAddress(0, 0, bytes.Repeat([]byte{0x91}, 32))
	createdLT := requestStartLT(t, request) - 10
	messages := make([]*msgpool.InternalMessage, 2)
	values := make([]*cell.Cell, 2)
	for i := range messages {
		messages[i], values[i] = queuedInternalWithReferencedBody(
			t,
			src,
			address.NewAddress(0, 0, bytes.Repeat([]byte{byte(0x31 + i)}, 32)),
			createdLT,
			request.Header.GenUtime-1,
			tlb.FromNanoTONU(100_000),
			tlb.FromNanoTONU(100_000),
			96,
			source,
		)
	}
	if bytes.Compare(messages[0].Root.Hash(), messages[1].Root.Hash()) > 0 {
		messages[0], messages[1] = messages[1], messages[0]
		values[0], values[1] = values[1], values[0]
	}

	previous := request.Previous
	for i := range messages {
		previous.State = stateWithQueueMessage(t, previous.State, messages[i].Key, values[i])
	}
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	bound := semanticMessageBound{lt: createdLT, hash: messages[0].Root.HashKey()}
	_, _, err = view.queue.OutQueue.TraverseExtra(func(key *cell.Cell, extra, value *cell.Slice) (int, error) {
		if value == nil {
			return 6, nil
		}
		minimum := extra.Copy()
		lt, loadErr := minimum.LoadUInt(64)
		if loadErr != nil {
			return 0, loadErr
		}
		leafBound, loadErr := semanticQueueLeafBound(key, lt)
		if loadErr != nil || !leafBound.lessEqual(bound) {
			return 0, loadErr
		}
		_, loadErr = parseSemanticQueueEntry(key, value, extra)
		return 0, loadErr
	})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProofVirtualized(proof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	queue, err := parseNeighborQueueInfo(provenState.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	laterKey := cell.BeginCell().MustStoreSlice(messages[1].Key[:], 352).EndCell()
	laterValue, err := queue.OutQueue.LoadValue(laterKey)
	if err != nil {
		t.Fatal(err)
	}
	var later tlb.EnqueuedMsg
	if err = loadExactSlice(&later, laterValue); err != nil {
		t.Fatal(err)
	}
	if later.Msg.GetType() != cell.PrunedCellType {
		t.Fatal("C++-shape proof unexpectedly materialized the later equal-LT payload")
	}

	visited := 0
	if err = walkSemanticQueuePrefix(queue.OutQueue, target, bound, func(semanticQueueEntry) error {
		visited++
		return nil
	}); err != nil {
		t.Fatalf("C++ equal-LT frontier proof cannot be consumed: %v", err)
	}
	if visited != 1 {
		t.Fatalf("visited queue entries = %d, want 1", visited)
	}
}

func TestWalkSemanticQueuePrefixLeavesNeighborStateInitOpaque(t *testing.T) {
	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	target := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	message, _ := queuedInternalWithReferencedBody(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xa1}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xa2}, 32)),
		requestStartLT(t, request)-10,
		request.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		96,
		source,
	)
	message, enqueued := queuedInternalWithStateInitRef(t, message)
	previous := request.Previous
	previous.State = stateWithQueueMessage(t, previous.State, message.Key, enqueued)
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(message.Key[:], 352).EndCell()
	value, extra, err := view.queue.OutQueue.LoadValueExtra(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = parseSemanticNeighborQueueEntry(key, value, extra); err != nil {
		t.Fatal(err)
	}
	proof, err := view.proof.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProofVirtualized(proof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	queue, err := parseNeighborQueueInfo(provenState.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	value, extra, err = queue.OutQueue.LoadValueExtra(key)
	if err != nil {
		t.Fatal(err)
	}
	var provenEnqueued tlb.EnqueuedMsg
	if err = loadExactSlice(&provenEnqueued, value); err != nil {
		t.Fatal(err)
	}
	var provenEnvelope tlb.MsgEnvelope
	if err = parseExact(&provenEnvelope, provenEnqueued.Msg); err != nil {
		t.Fatal(err)
	}
	messageLoader := provenEnvelope.Msg.MustBeginParse()
	var info semanticInternalMessageInfo
	if err = tlb.LoadFromCell(&info, messageLoader); err != nil {
		t.Fatal(err)
	}
	hasStateInit, err := messageLoader.LoadBoolBit()
	if err != nil || !hasStateInit {
		t.Fatalf("load StateInit flag: present=%v err=%v", hasStateInit, err)
	}
	stateInitInRef, err := messageLoader.LoadBoolBit()
	if err != nil || !stateInitInRef {
		t.Fatalf("load StateInit layout: in_ref=%v err=%v", stateInitInRef, err)
	}
	stateInit, err := messageLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if stateInit.GetType() != cell.PrunedCellType {
		t.Fatal("C++-shape neighbor proof unexpectedly materialized the opaque StateInit")
	}

	bound := semanticMessageBound{lt: message.EnqueuedLT, hash: message.Root.HashKey()}
	visited := 0
	if err = walkSemanticQueuePrefix(queue.OutQueue, target, bound, func(semanticQueueEntry) error {
		visited++
		return nil
	}); err != nil {
		t.Fatalf("C++ opaque-StateInit proof cannot be consumed: %v", err)
	}
	if visited != 1 {
		t.Fatalf("visited queue entries = %d, want 1", visited)
	}
}

func queuedInternalWithStateInitRef(
	t *testing.T,
	message *msgpool.InternalMessage,
) (*msgpool.InternalMessage, *cell.Cell) {
	t.Helper()

	var internal tlb.InternalMessage
	if err := parseExact(&internal, message.Root); err != nil {
		t.Fatal(err)
	}
	internal.StateInit = &tlb.StateInit{
		Code: cell.BeginCell().MustStoreRef(
			cell.BeginCell().MustStoreUInt(0x53, 8).EndCell(),
		).EndCell(),
	}
	builder := cell.BeginCell()
	if err := tlb.StoreMessageWithLayout(builder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     &internal,
	}, tlb.MessageLayout{StateInitInRef: true, BodyInRef: true}); err != nil {
		t.Fatal(err)
	}
	root := builder.EndCell()
	envelope := message.Envelope
	envelope.Msg = root
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{
		EnqueuedLT: message.EnqueuedLT,
		Msg:        envelopeCell,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	result := *message
	result.Key = msgpool.MakeQueueKey(message.Key.NextHop(), root.HashKey())
	result.EnvHash = envelopeCell.HashKey()
	result.Envelope = envelope
	result.EnvelopeCell = envelopeCell
	result.Root = root

	return &result, enqueued
}

func queuedInternalWithReferencedBody(
	tb testing.TB,
	src, dst *address.Address,
	createdLT uint64,
	createdAt uint32,
	fwdFee, remainingFee tlb.Coins,
	curBits uint8,
	source msgpool.ShardIdent,
) (*msgpool.InternalMessage, *cell.Cell) {
	tb.Helper()

	internal := &tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     src,
		DstAddr:     dst,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		FwdFee:      fwdFee,
		CreatedLT:   createdLT,
		CreatedAt:   createdAt,
		Body:        cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell(),
	}
	messageBuilder := cell.BeginCell()
	if err := tlb.StoreMessageWithLayout(messageBuilder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     internal,
	}, tlb.MessageLayout{BodyInRef: true}); err != nil {
		tb.Fatal(err)
	}
	message := messageBuilder.EndCell()
	envelopeCell, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: curBits},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: remainingFee,
		Msg:             message,
	}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, envelopeCell); err != nil {
		tb.Fatal(err)
	}
	srcPrefix, err := accountPrefixFromAddress(src)
	if err != nil {
		tb.Fatal(err)
	}
	dstPrefix, err := accountPrefixFromAddress(dst)
	if err != nil {
		tb.Fatal(err)
	}
	nextHop := msgpool.InterpolatePrefix(srcPrefix, dstPrefix, 96)
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: createdLT, Msg: envelopeCell}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}

	return &msgpool.InternalMessage{
		Key:          msgpool.MakeQueueKey(nextHop, message.HashKey()),
		EnqueuedLT:   createdLT,
		QueueLT:      createdLT,
		EnvHash:      envelopeCell.HashKey(),
		Envelope:     envelope,
		EnvelopeCell: envelopeCell,
		Root:         message,
		Source:       source,
		SourceSeqno:  1,
	}, enqueued
}

func TestLocalNeighborViewAcceptsCollatedProofWithoutAuxiliaryState(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	builder := cell.NewMerkleProofBuilder(fixture.request.Previous.State)
	root := builder.Root()

	var state tlb.ShardStateUnsplit
	if err := parseProofExact(&state, root); err != nil {
		t.Fatal(err)
	}
	queue, err := parseQueueInfo(state.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tlb.LoadProcessedUptoRecords(queue.ProcInfo, uint64(fixture.request.Previous.ID.Shard)); err != nil {
		t.Fatal(err)
	}
	proof, err := builder.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProof(proof, fixture.request.Previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}

	if _, err = localViewFromPrevious(PreviousBlock{
		ID:    fixture.request.Previous.ID,
		State: virtualState,
	}, false, false); err != nil {
		t.Fatalf("valid C++-style collated proof was rejected: %v", err)
	}
}

func TestLocalNeighborViewAcceptsCollatedProofWithoutDispatchQueue(t *testing.T) {
	request := emptyCandidateRequest(t)
	previous := request.Previous
	previous.State = previousStateWithDispatchQueue(
		t,
		previous.State,
		nonEmptyDispatchQueue(t, request.Header.GenUtime-1, requestStartLT(t, request)-10),
	)
	builder := cell.NewMerkleProofBuilder(previous.State)
	root := builder.Root()

	var state tlb.ShardStateUnsplit
	if err := parseProofExact(&state, root); err != nil {
		t.Fatal(err)
	}
	queue, err := parseNeighborQueueInfo(state.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tlb.LoadProcessedUptoRecords(queue.ProcInfo, uint64(previous.ID.Shard)); err != nil {
		t.Fatal(err)
	}
	proof, err := builder.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	virtualState, err := cell.UnwrapProof(proof, previous.State.Hash())
	if err != nil {
		t.Fatal(err)
	}

	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, virtualState); err != nil {
		t.Fatal(err)
	}
	if _, err = parseQueueInfo(provenState.OutMsgQueueInfo); err == nil {
		t.Fatal("fixture unexpectedly materialized the irrelevant dispatch queue")
	}
	if _, err = localViewFromPrevious(PreviousBlock{ID: previous.ID, State: virtualState}, false, false); err != nil {
		t.Fatalf("valid C++-style neighbor proof was rejected: %v", err)
	}
}

func TestHistoricalShardEndLTReusesMatchingNeighborFrontier(t *testing.T) {
	id := testBlockID(-1, -1<<63, 17, 0x41)
	current := &localMasterView{
		state:    tlb.ShardStateUnsplit{GenLT: 1234},
		registry: &ShardRegistry{leaves: make(map[shardRegistryKey]shardRegistryLeaf)},
		context:  MasterchainContext{ID: id},
	}
	acquisition := &LocalAcquisition{}

	resolve, err := acquisition.historicalShardEndLT(
		context.Background(),
		current,
		[]PreviousBlock{{ID: id}},
		[]Neighbor{{Block: id}},
		nil,
		nil,
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := resolve(id.SeqNo, masterchainWorkchainID, 0); got != current.state.GenLT {
		t.Fatalf("masterchain end lt = %d, want %d", got, current.state.GenLT)
	}
}

func TestHistoricalShardEndLTUsesCandidateRegistryForNewMasterFrontier(t *testing.T) {
	masterID := testBlockID(-1, -1<<63, 17, 0x42)
	shardID := testBlockID(0, -1<<63, 9, 0x43)
	registry := func(endLT uint64) *ShardRegistry {
		return &ShardRegistry{leaves: map[shardRegistryKey]shardRegistryLeaf{
			{}: {
				top:    ShardTop{Block: shardID},
				fields: shardDescriptorFields{endLT: endLT},
			},
		}}
	}
	current := &localMasterView{
		state:    tlb.ShardStateUnsplit{GenLT: 1_000},
		registry: registry(100),
		context:  MasterchainContext{ID: masterID},
	}
	next := registry(200)
	resolve, err := (&LocalAcquisition{}).historicalShardEndLT(
		context.Background(),
		current,
		nil,
		nil,
		nil,
		next,
		acquisitionReadImmediate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := resolve(masterID.SeqNo, 0, 0); got != 100 {
		t.Fatalf("predecessor shard end lt = %d, want 100", got)
	}
	if got := resolve(masterID.SeqNo+1, 0, 0); got != 200 {
		t.Fatalf("candidate shard end lt = %d, want 200", got)
	}
	processed, err := newShardEndLTResolver(resolve).alreadyProcessed(
		[]tlb.ProcessedUptoRecord{{
			ShardPrefix: uint64(1) << 63,
			MCSeqno:     masterID.SeqNo + 1,
			LastMsgLT:   150,
			LastMsgHash: [32]byte{0xff},
		}},
		masterchainWorkchainID,
		uint64(1)<<63,
		&tlb.ProcessedMsgDescr{
			CurWorkchain:  0,
			CurPrefix:     0,
			NextWorkchain: masterchainWorkchainID,
			NextPrefix:    0,
			LT:            150,
			EnqueuedLT:    150,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("candidate ProcessedInfo did not cover a message registered by the selected shard top")
	}
}

func TestLocalFullCollatedSlotTwoAcceptsCandidateAddedInternals(t *testing.T) {
	request := emptyCandidateRequest(t)
	view, err := localViewFromPrevious(request.Previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	source := blockShardIdent(request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{source}); err != nil {
		t.Fatal(err)
	}
	base, err := localSourceRef(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(source, source, base, nil, 0); err != nil {
		t.Fatal(err)
	}
	branch := openLocalTestBranch(t, pool, source)
	message, _ := queuedInternal(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x41}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x42}, 32)),
		requestStartLT(t, request),
		request.Header.GenUtime,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		0,
		source,
	)
	message.SourceSeqno = request.Previous.ID.SeqNo + 1
	candidateID := [32]byte{2}
	if err = branch.AddCandidate(msgpool.CandidateRequest{
		ID:    candidateID,
		Seqno: message.SourceSeqno,
		Base:  []msgpool.CandidateSource{{Source: source, Visible: base}},
		Delta: &msgpool.InternalsDelta{
			Added:      []*msgpool.InternalMessage{message},
			AddedTotal: 1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	cut, err := branch.Cut(msgpool.CutRequest{
		Sources:      map[msgpool.ShardIdent]msgpool.CutSource{source: {Visible: base}},
		CandidateTip: &candidateID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0] != message {
		t.Fatal("slot-two cut does not contain the candidate-added internal message")
	}
	// Candidate-added entries are authenticated by their candidate predecessor,
	// not by this older neighbor view.
	if err = traceInternalCut(
		FullCollatedQueueScan{
			Target: msgpool.ShardIdent{Workchain: message.Key.NextHop().Workchain, Shard: msgpool.ShardAll},
			LT:     message.EnqueuedLT,
			Hash:   message.Root.HashKey(),
		},
		cut.Messages,
		map[msgpool.ShardIdent]*localNeighborView{source: view},
	); err != nil {
		t.Fatalf("candidate entry was incorrectly traced in the older neighbor state: %v", err)
	}
}

func TestLocalSessionCleanupAfterDestinationRotation(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*LocalAcquisition, Session, SessionUpdate) error
	}{
		{
			name: "update",
			run: func(acquisition *LocalAcquisition, session Session, update SessionUpdate) error {
				update.CurrentWindowStart += session.SlotsPerLeaderWindow
				update.CurrentWindowObservedSlot = update.CurrentWindowStart
				update.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(
					time.Duration(session.SlotsPerLeaderWindow) * update.TargetRate,
				)

				return acquisition.UpdateSession(context.Background(), session, update)
			},
		},
		{
			name: "retire",
			run: func(acquisition *LocalAcquisition, session Session, _ SessionUpdate) error {
				return acquisition.RetireSession(context.Background(), session.ID)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			acquisition, pool, session, update := localRotationFixture(t)
			defer pool.Close()
			managed, err := acquisition.session(session.ID)
			if err != nil {
				t.Fatal(err)
			}
			candidateID := simplex.CandidateID{Slot: 2, Hash: [32]byte{0x21}}
			queueRoot := [32]byte{0x31}
			managed.mu.Lock()
			retainedSeedSlot := update.CurrentWindowStart + session.SlotsPerLeaderWindow
			managed.seeds[1] = [32]byte{0x11}
			managed.seeds[retainedSeedSlot] = [32]byte{0x12}
			managed.candidates[candidateID] = localCandidateState{
				queueTip: &queueRoot,
			}
			managed.blocks[queueRoot] = managed.candidates[candidateID]
			managed.mu.Unlock()

			// The topology is published before the session supervisor observes
			// the matching rotation, which removes the whole destination state.
			if err = pool.Internals().ReconcileDestinations(nil); err != nil {
				t.Fatal(err)
			}
			if err = test.run(acquisition, session.Session, update); err != nil {
				t.Fatalf("cleanup after destination removal: %v", err)
			}
			if _, err = acquisition.session(session.ID); test.name == "retire" && !errors.Is(err, ErrNotFound) {
				t.Fatalf("retired session remains published: %v", err)
			}
			if test.name == "retire" && managed.seeds != nil {
				t.Fatal("retired session retained request seeds")
			}
			if test.name == "update" {
				managed, err = acquisition.session(session.ID)
				if err != nil {
					t.Fatal(err)
				}
				managed.mu.Lock()
				defer managed.mu.Unlock()
				if len(managed.candidates) != 0 || managed.update.CurrentWindowStart != retainedSeedSlot {
					t.Fatal("session update was not committed after destination removal")
				}
				if _, exists := managed.seeds[1]; exists {
					t.Fatal("session update retained a seed from an expired window")
				}
				if managed.seeds[retainedSeedSlot] != ([32]byte{0x12}) {
					t.Fatal("session update removed a seed from the current window")
				}
			}
		})
	}
}

func TestLocalPrepareSessionRetrySurvivesDestinationRemoval(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	if err := pool.Internals().ReconcileDestinations(nil); err != nil {
		t.Fatal(err)
	}
	if err := acquisition.PrepareSession(context.Background(), session.Session, update); err != nil {
		t.Fatalf("retry exact prepared session after destination removal: %v", err)
	}
}

func TestLocalPrepareSessionWaitsForDestinationTopology(t *testing.T) {
	acquisition, pool, template, update := localRotationFixture(t)
	defer pool.Close()

	session := template.Session
	session.ID[0] ^= 0xff
	update.SessionID = session.ID
	if err := pool.Internals().ReconcileDestinations(nil); err != nil {
		t.Fatal(err)
	}
	if err := acquisition.PrepareSession(
		context.Background(),
		session,
		update,
	); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("prepare without destination error = %v, want ErrAcquisitionNotReady", err)
	}
	if _, err := acquisition.session(session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("not-ready prepare published a partial session: %v", err)
	}

	destination := targetShardIdent(session.Shard)
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	if err := acquisition.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatalf("prepare after destination publication: %v", err)
	}
	if _, err := acquisition.session(session.ID); err != nil {
		t.Fatalf("prepared session is absent: %v", err)
	}
}

func TestLocalFirstCandidateResolvesFromSessionGenesis(t *testing.T) {
	genesis := PreviousBlock{ID: runtimeTestBlockID(0, -1<<63, 20)}
	masterchainFinalized := PreviousBlock{ID: runtimeTestBlockID(0, -1<<63, 21)}
	genesisKey, err := blockRootKey(genesis.ID)
	if err != nil {
		t.Fatal(err)
	}
	finalizedKey, err := blockRootKey(masterchainFinalized.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed := &localAcquisitionSession{
		blocks: map[[32]byte]localCandidateState{
			genesisKey:   {block: genesis},
			finalizedKey: {block: masterchainFinalized},
		},
	}
	request := BuildRequest{
		Session: ActivatedSession{Genesis: []ton.BlockIDExt{genesis.ID}},
		Update: SessionUpdate{
			HasFinalizedBlock: true,
			FinalizedBlock:    masterchainFinalized.ID,
		},
	}

	chain, err := (&LocalAcquisition{}).resolveChain(context.Background(), managed, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.previous) != 1 || !chain.previous[0].ID.Equals(&genesis.ID) {
		t.Fatalf("first candidate predecessor = %+v, want session genesis %v", chain.previous, genesis.ID)
	}
}

func TestLocalSessionUpdateRetainsSpeculationWithinLeaderWindow(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	managed, err := acquisition.session(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	candidateID := simplex.CandidateID{Slot: update.CurrentWindowStart, Hash: [32]byte{0x71}}
	queueTip := [32]byte{0x72}
	state := localCandidateState{queueTip: &queueTip}
	managed.mu.Lock()
	managed.candidates[candidateID] = state
	managed.blocks[queueTip] = state
	managed.mu.Unlock()

	next := update
	next.CurrentWindowObservedSlot++
	if err = acquisition.UpdateSession(context.Background(), session.Session, next); err != nil {
		t.Fatal(err)
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if _, exists := managed.candidates[candidateID]; !exists {
		t.Fatal("same-window update dropped the previous local candidate")
	}
	if _, exists := managed.blocks[queueTip]; !exists {
		t.Fatal("same-window update dropped the previous local candidate queue")
	}
}

func TestLocalSessionUpdateRejectsUnmaterializedConsensusBase(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	next := cloneSessionUpdate(update)
	next.CurrentWindowStart += session.SlotsPerLeaderWindow
	next.CurrentWindowObservedSlot = next.CurrentWindowStart
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(update.TargetRate)
	next.CurrentBase = simplex.Parent(simplex.CandidateID{
		Slot: update.CurrentWindowStart,
		Hash: [32]byte{0x73},
	})
	if err := acquisition.UpdateSession(context.Background(), session.Session, next); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("unmaterialized selected-base error = %v, want ErrCandidateConflict", err)
	}

	managed, err := acquisition.session(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	current := cloneSessionUpdate(managed.update)
	managed.mu.Unlock()
	if !current.Equal(update) {
		t.Fatalf("rejected selected base mutated session update: %+v", current)
	}
}

func TestLocalSessionWindowAdvanceCheckpointsCommittedBranch(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	managed, err := acquisition.session(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	base := session.Genesis[0]
	baseRef, err := localSourceRef(base)
	if err != nil {
		t.Fatal(err)
	}
	destination := targetShardIdent(session.Shard)
	if err = pool.Internals().Seed(destination, blockShardIdent(base), baseRef, nil, 0); err != nil {
		t.Fatal(err)
	}
	tip := [32]byte{0x72}
	candidateBlock := cloneBlockID(base)
	candidateBlock.SeqNo++
	candidateBlock.RootHash = bytes.Clone(tip[:])
	message, _ := queuedInternal(
		t,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x73}, 32)),
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x74}, 32)),
		1,
		1,
		tlb.FromNanoTONU(1),
		tlb.FromNanoTONU(1),
		0,
		destination,
	)
	delta := &msgpool.InternalsDelta{
		Added:      []*msgpool.InternalMessage{message},
		AddedTotal: 1,
	}
	candidateID := simplex.CandidateID{Slot: update.CurrentWindowStart, Hash: [32]byte{0x71}}
	if err = managed.branch.AddCandidate(msgpool.CandidateRequest{
		ID:    tip,
		Seqno: candidateBlock.SeqNo,
		Base:  []msgpool.CandidateSource{{Source: blockShardIdent(base), Visible: baseRef}},
		Delta: delta,
	}); err != nil {
		t.Fatal(err)
	}
	state := localCandidateState{
		block:     PreviousBlock{ID: candidateBlock},
		queueBase: []PreviousBlock{{ID: base}},
		queueTip:  &tip,
	}
	managed.mu.Lock()
	managed.candidates[candidateID] = state
	managed.blocks[tip] = state
	managed.mu.Unlock()

	ref := msgpool.SourceRef{Seqno: candidateBlock.SeqNo, RootHash: tip}
	if err = pool.Internals().ApplyBlock(
		destination,
		blockShardIdent(candidateBlock),
		ref,
		delta,
	); err != nil {
		t.Fatal(err)
	}

	next := update
	next.CurrentBase = simplex.Parent(candidateID)
	next.CurrentWindowStart += session.SlotsPerLeaderWindow
	next.CurrentWindowObservedSlot = next.CurrentWindowStart
	next.CurrentWindowStartAt = next.CurrentWindowStartAt.Add(
		time.Duration(session.SlotsPerLeaderWindow) * next.TargetRate,
	)
	if err = acquisition.UpdateSession(context.Background(), session.Session, next); err != nil {
		t.Fatal(err)
	}

	managed.mu.Lock()
	checkpoint := managed.candidates[candidateID]
	managed.mu.Unlock()
	if checkpoint.queueTip != nil || len(checkpoint.queueBase) != 0 {
		t.Fatalf("committed window base retained speculative lineage: %+v", checkpoint)
	}
	if _, cutErr := managed.branch.Cut(msgpool.CutRequest{CandidateTip: &tip}); !errors.Is(cutErr, msgpool.ErrCutStale) {
		t.Fatalf("checkpoint retained preceding branch tip: %v", cutErr)
	}

	// The masterchain registry intentionally still names the pre-window base.
	// Once Retain(nil) drops the speculative lineage, the committed cut must
	// nevertheless bind our source to the newer consensus predecessor.
	registered := map[msgpool.ShardIdent]*localNeighborView{
		destination: {previous: PreviousBlock{ID: base}},
	}
	cut, err := acquisition.cutCommittedViews(
		managed.branch,
		destination,
		effectiveShardMessageViews(destination, []PreviousBlock{checkpoint.block}, registered),
		nil,
		nil,
		true,
		&prewarmHints{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].EnvHash != message.EnvHash {
		t.Fatalf("post-checkpoint cut = %+v, want committed predecessor envelope %x", cut.Messages, message.EnvHash)
	}
}

func TestLocalSessionUpdateKeepsInstalledExactMasterchainView(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	groupsSource, ok := acquisition.groups.(*localAcquisitionTestGroups)
	if !ok {
		t.Fatal("unexpected local group source")
	}
	ahead := *groupsSource.snapshot
	ahead.MasterchainBlock = cloneBlockID(ahead.MasterchainBlock)
	ahead.MasterchainBlock.SeqNo++
	groupsSource.snapshot = &ahead

	next := update
	next.CurrentWindowObservedSlot++
	if err := acquisition.UpdateSession(context.Background(), session.Session, next); err != nil {
		t.Fatalf("update bound to installed masterchain view: %v", err)
	}
}

func TestLocalFullProofProviderUsesStateProofForZerostateNeighbor(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x5a, 8).EndCell()
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     -1 << 63,
		SeqNo:     0,
		RootHash:  bytes.Clone(root.Hash()),
	}
	provider := &localFullProofProvider{proofViews: map[msgpool.ShardIdent]*localNeighborView{
		blockShardIdent(id): {
			previous: PreviousBlock{ID: id, State: root},
			proof:    cell.NewMerkleProofBuilder(root),
		},
	}}
	proofs, err := provider.BuildFullCollatedProofs(context.Background(), FullCollatedProofRequest{
		Previous:  PreviousBlock{ID: testBlockID(0, -1<<63, 1, 0x31)},
		Neighbors: []Neighbor{{Block: id, Shard: blockShardIdent(id)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(proofs) != 1 {
		t.Fatalf("zerostate neighbor proofs = %d, want state proof only", len(proofs))
	}
	state, err := cell.UnwrapProof(proofs[0], root.Hash())
	if err != nil {
		t.Fatalf("unwrap zerostate neighbor proof: %v", err)
	}
	if state.HashKey() != root.HashKey() {
		t.Fatal("zerostate neighbor proof has a different virtual root")
	}
}

func TestLocalResolveEmptyArtifactUsesExactChain(t *testing.T) {
	sessionID := [32]byte{0x51}
	previousID := simplex.CandidateID{Slot: 1, Hash: [32]byte{0x52}}
	previousBlock := PreviousBlock{ID: testBlockID(0, -1<<63, 11, 0x53)}
	queueTip := [32]byte{0x54}
	storageKey := cell.BeginCell().MustStoreUInt(1, 1).EndCell().HashKey()
	storageStats := AccountStorageStats{storageKey: cell.BeginCell().EndCell()}
	master := &localMasterView{}
	state := localCandidateState{
		block:        previousBlock,
		storageStats: storageStats,
		queueTip:     &queueTip,
		master:       master,
	}
	managed := &localAcquisitionSession{
		session: Session{Shard: groups.ShardID{
			Workchain: previousBlock.ID.Workchain,
			Shard:     previousBlock.ID.Shard,
		}},
		candidates: map[simplex.CandidateID]localCandidateState{previousID: state},
		blocks:     make(map[[32]byte]localCandidateState),
	}
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	acquisition := &LocalAcquisition{messages: pool}
	request := BuildRequest{
		Session: ActivatedSession{Session: Session{ID: sessionID}},
		Slot:    2,
		Parent:  simplex.Parent(previousID),
		Previous: &CandidateArtifact{
			SessionID: sessionID,
			Candidate: simplex.Candidate{ID: previousID, Block: previousBlock.ID},
		},
	}
	resolved, err := acquisition.resolveArtifactBlock(
		context.Background(), managed, request, previousBlock.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.block.ID.Equals(&previousBlock.ID) || resolved.queueTip == nil ||
		*resolved.queueTip != queueTip || resolved.master != master || resolved.storageStats[storageKey] == nil {
		t.Fatal("exact empty artifact lost predecessor acquisition state")
	}

	crossBranch := testBlockID(0, previousBlock.ID.Shard, 11, 0x55)
	crossKey, err := blockRootKey(crossBranch)
	if err != nil {
		t.Fatal(err)
	}
	managed.blocks[crossKey] = localCandidateState{block: PreviousBlock{ID: crossBranch}}
	if _, err = acquisition.resolveArtifactBlock(
		context.Background(), managed, request, crossBranch,
	); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("cross-branch empty artifact error = %v, want ErrCandidateConflict", err)
	}

	unknownBase := simplex.CandidateID{Slot: 1, Hash: [32]byte{0x56}}
	wrongBaseRequest := BuildRequest{
		Session: ActivatedSession{Session: Session{ID: sessionID}},
		Update: SessionUpdate{
			CurrentBase: simplex.Parent(unknownBase),
		},
		Slot:   2,
		Parent: simplex.Parent(unknownBase),
	}
	if _, err = acquisition.resolveArtifactBlock(
		context.Background(), managed, wrongBaseRequest, crossBranch,
	); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("wrong-base empty artifact error = %v, want ErrAcquisitionNotReady", err)
	}
}

func TestLocalRestoreAcceptsEmptyFinalizedAnchor(t *testing.T) {
	sessionID := [32]byte{0x71}
	finalized := runtimeTestBlockID(0, -1<<63, 9)
	session := Session{
		ID:                   sessionID,
		Shard:                groups.ShardID{Workchain: finalized.Workchain, Shard: finalized.Shard},
		SlotsPerLeaderWindow: 1,
		Validators:           []SessionValidator{{Weight: 1}},
	}
	activation := SessionActivation{SessionID: sessionID}
	update := SessionUpdate{
		SessionID:         sessionID,
		HasFinalizedBlock: true,
		FinalizedBlock:    finalized,
	}
	blockKey, err := blockRootKey(finalized)
	if err != nil {
		t.Fatal(err)
	}
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	destination := targetShardIdent(session.Shard)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	managed := &localAcquisitionSession{
		session:    session,
		branch:     openLocalTestBranch(t, pool, destination),
		activation: &activation,
		update:     update,
		candidates: make(map[simplex.CandidateID]localCandidateState),
		blocks: map[[32]byte]localCandidateState{
			blockKey: {block: PreviousBlock{ID: finalized}},
		},
	}
	acquisition := &LocalAcquisition{
		messages: pool,
		sessions: map[[32]byte]*localAcquisitionSession{sessionID: managed},
	}
	parent := simplex.Parent(simplex.CandidateID{Slot: 1, Hash: [32]byte{0x72}})
	candidate := simplex.Candidate{
		Parent: parent,
		Leader: 0,
		Empty:  true,
		Block:  finalized,
	}
	candidate.ID = candidate.ComputeID(2)
	artifact := CandidateArtifact{SessionID: sessionID, Candidate: candidate}
	err = acquisition.RestoreCandidate(context.Background(), BuildRequest{
		Session:         activatedSession(session, activation),
		Update:          update,
		Slot:            2,
		Leader:          0,
		Parent:          parent,
		FinalizedAnchor: &finalized,
	}, artifact)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	restored, exists := managed.candidates[candidate.ID]
	managed.mu.Unlock()
	if !exists || !restored.block.ID.Equals(&finalized) {
		t.Fatal("empty finalized anchor was not materialized")
	}
}

func TestLocalRestoreAcceptsFinalizedAnchorAfterParentPruned(t *testing.T) {
	sessionID := [32]byte{0x73}
	finalized := runtimeTestBlockID(0, -1<<63, 10)
	parent := simplex.Parent(simplex.CandidateID{Slot: 4, Hash: [32]byte{0x74}})
	session := Session{
		ID:                   sessionID,
		Shard:                groups.ShardID{Workchain: finalized.Workchain, Shard: finalized.Shard},
		SlotsPerLeaderWindow: 1,
		Validators:           []SessionValidator{{Weight: 1}},
	}
	activation := SessionActivation{SessionID: sessionID}
	update := SessionUpdate{
		SessionID:         sessionID,
		HasFinalizedBlock: true,
		FinalizedBlock:    runtimeTestBlockID(0, -1<<63, 11),
		CurrentBase:       parent,
	}
	blockKey, err := blockRootKey(finalized)
	if err != nil {
		t.Fatal(err)
	}
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	destination := targetShardIdent(session.Shard)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	managed := &localAcquisitionSession{
		session:    session,
		branch:     openLocalTestBranch(t, pool, destination),
		activation: &activation,
		update:     update,
		candidates: make(map[simplex.CandidateID]localCandidateState),
		blocks: map[[32]byte]localCandidateState{
			blockKey: {block: PreviousBlock{ID: finalized}},
		},
	}
	acquisition := &LocalAcquisition{
		messages: pool,
		sessions: map[[32]byte]*localAcquisitionSession{sessionID: managed},
	}
	candidate := simplex.Candidate{
		Parent: parent,
		Leader: 0,
		Block:  finalized,
	}
	candidate.ID = candidate.ComputeID(5)
	artifact := CandidateArtifact{SessionID: sessionID, Candidate: candidate}
	err = acquisition.RestoreCandidate(context.Background(), BuildRequest{
		Session:         activatedSession(session, activation),
		Update:          update,
		Slot:            5,
		Leader:          0,
		Parent:          parent,
		FinalizedAnchor: &finalized,
	}, artifact)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	restored, exists := managed.candidates[candidate.ID]
	managed.mu.Unlock()
	if !exists || !restored.block.ID.Equals(&finalized) {
		t.Fatal("finalized anchor was not materialized without its pruned parent")
	}
}

func TestLocalAdvanceConsensusBaseUsesSelectedStateWithoutStoreRead(t *testing.T) {
	buildFixture := newMasterBuildFixture(t, false)
	built, err := testBuilder().BuildMaster(context.Background(), buildFixture.request)
	if err != nil {
		t.Fatal(err)
	}
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()
	store := &finalizedAnchorNoReadStore{}
	acquisition.store = store

	candidateID := simplex.CandidateID{
		Slot: update.CurrentWindowStart,
		Hash: [32]byte{0x7a},
	}
	base, err := NewSelectedBaseState(
		session.ID,
		candidateID,
		built.ID,
		built.BlockBOC,
		candidateBlock(t, built),
		built.State,
	)
	if err != nil {
		t.Fatal(err)
	}
	next := update
	next.CurrentWindowObservedSlot++
	next.CurrentBase = simplex.Parent(candidateID)
	if err = acquisition.AdvanceConsensusBase(context.Background(), ConsensusBaseUpdate{
		Session: session,
		Update:  next,
		Base:    base,
	}); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 {
		t.Fatalf("selected base caused %d local state reads", store.calls)
	}

	managed, err := acquisition.session(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	adopted, exists := managed.candidates[candidateID]
	installedUpdate := cloneSessionUpdate(managed.update)
	candidateCount := len(managed.candidates)
	blockCount := len(managed.blocks)
	managed.mu.Unlock()
	if !exists || candidateCount != 1 || blockCount != 1 || adopted.block.Block == nil ||
		adopted.block.State == nil || !adopted.block.ID.Equals(&built.ID) ||
		adopted.block.State.HashKeyAt(0) != built.State.HashKeyAt(0) {
		t.Fatal("selected base was not retained as the exact acquisition root")
	}
	if !installedUpdate.Equal(next) {
		t.Fatalf("installed update = %+v, want %+v", installedUpdate, next)
	}
}

func TestLocalRestoreFinalizedAnchorDropsPrecedingCandidateQueues(t *testing.T) {
	base := emptyCandidateRequest(t).Previous
	destination := blockShardIdent(base.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	baseRef, err := localSourceRef(base.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, baseRef, nil, 0); err != nil {
		t.Fatal(err)
	}
	branch := openLocalTestBranch(t, pool, destination)

	sessionID := [32]byte{0x75}
	managed := &localAcquisitionSession{
		session: Session{
			ID:    sessionID,
			Shard: groups.ShardID{Workchain: base.ID.Workchain, Shard: base.ID.Shard},
		},
		branch:     branch,
		candidates: make(map[simplex.CandidateID]localCandidateState),
		blocks:     make(map[[32]byte]localCandidateState),
	}
	baseKey, err := blockRootKey(base.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.blocks[baseKey] = localCandidateState{block: base}
	for index := 0; index < 64; index++ {
		tip := [32]byte{byte(index + 1), 0x76}
		if err = branch.AddCandidate(msgpool.CandidateRequest{
			ID:    tip,
			Seqno: baseRef.Seqno + 1,
			Base:  candidateSources([]PreviousBlock{base}),
			Delta: &msgpool.InternalsDelta{},
		}); err != nil {
			t.Fatal(err)
		}
		managed.candidates[simplex.CandidateID{
			Slot: uint32(index + 1),
			Hash: tip,
		}] = localCandidateState{queueTip: &tip}
	}

	anchorID := simplex.CandidateID{Slot: 65, Hash: [32]byte{0x77}}
	artifact := CandidateArtifact{
		SessionID: sessionID,
		Candidate: simplex.Candidate{ID: anchorID, Block: base.ID},
	}
	request := BuildRequest{
		Session: ActivatedSession{Session: managed.session},
		Slot:    anchorID.Slot,
	}
	acquisition := &LocalAcquisition{messages: pool}
	if err = acquisition.restoreFinalizedCandidateAnchor(
		context.Background(),
		managed,
		request,
		artifact,
	); err != nil {
		t.Fatal(err)
	}

	if len(managed.candidates) != 1 || len(managed.blocks) != 1 {
		t.Fatalf("restored indexes have %d candidates and %d blocks, want one anchor", len(managed.candidates), len(managed.blocks))
	}
	if restored, exists := managed.candidates[anchorID]; !exists || !restored.block.ID.Equals(&base.ID) {
		t.Fatal("finalized anchor did not replace preceding speculative branches")
	}

	next := [32]byte{0x78}
	if err = branch.AddCandidate(msgpool.CandidateRequest{
		ID:    next,
		Seqno: baseRef.Seqno + 1,
		Base:  candidateSources([]PreviousBlock{base}),
		Delta: &msgpool.InternalsDelta{},
	}); err != nil {
		t.Fatalf("candidate capacity was not released after finalized anchor: %v", err)
	}
}

func TestLocalCommitRollsBackCandidateWhenExternalCompletionFails(t *testing.T) {
	request := emptyCandidateRequest(t)
	request.Internals = &msgpool.Cut{}
	built, err := testBuilder().BuildShard(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	destination := blockShardIdent(request.Previous.ID)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	completeErr := errors.New("complete externals failed")
	messages := &localAcquisitionFailingMessages{pool: pool, completeErr: completeErr}
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{messages: messages, accountPrewarmer: warmer}
	sessionID := [32]byte{0x61}
	session := ActivatedSession{
		Session: Session{
			ID:    sessionID,
			Shard: request.Shard,
		},
		Genesis: []ton.BlockIDExt{request.Previous.ID},
	}
	update := SessionUpdate{SessionID: sessionID}
	activation := SessionActivation{SessionID: sessionID, Genesis: session.Genesis}
	managed := &localAcquisitionSession{
		session:    session.Session,
		branch:     openLocalTestBranch(t, pool, destination),
		activation: &activation,
		update:     update,
		candidates: make(map[simplex.CandidateID]localCandidateState),
		blocks:     make(map[[32]byte]localCandidateState),
	}
	genesisKey, err := blockRootKey(request.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.blocks[genesisKey] = localCandidateState{block: request.Previous}
	build := BuildRequest{Session: session, Update: update, Slot: 2}
	candidateID := simplex.CandidateID{Slot: build.Slot, Hash: [32]byte{0x62}}
	artifact := CandidateArtifact{
		SessionID: sessionID,
		Candidate: simplex.Candidate{
			ID:    candidateID,
			Block: built.ID,
		},
		BlockBOC: built.BlockBOC,
	}
	err = acquisition.commitCandidateLocked(context.Background(), managed, build, built, artifact, &prewarmHints{})
	if !errors.Is(err, completeErr) {
		t.Fatalf("commit error = %v, want external completion failure", err)
	}
	if messages.completeCalls != 1 || len(managed.candidates) != 0 || len(managed.blocks) != 1 ||
		managed.blocks[genesisKey].block.State != request.Previous.State {
		t.Fatal("failed candidate commit mutated the managed acquisition state")
	}
	queueID, keyErr := blockRootKey(built.ID)
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	if _, cutErr := managed.branch.Cut(msgpool.CutRequest{CandidateTip: &queueID}); !errors.Is(cutErr, msgpool.ErrCutStale) {
		t.Fatalf("rolled-back candidate cut error = %v, want ErrCutStale", cutErr)
	}
	if len(warmer.accounts) != 0 || len(warmer.roots) != 0 {
		t.Fatalf("failed candidate scheduled account/envelope prewarms: accounts=%v roots=%x",
			warmer.accounts, warmer.roots)
	}
	acquisition.dispatchPrewarm.mu.Lock()
	running := acquisition.dispatchPrewarm.running
	pending := len(acquisition.dispatchPrewarm.pending)
	acquisition.dispatchPrewarm.mu.Unlock()
	if running || pending != 0 {
		t.Fatalf("failed candidate scheduled dispatch prewarm: running=%t pending=%d", running, pending)
	}
}

func TestLocalRestoreRejectsLeaderOutsideRoster(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	err := acquisition.RestoreCandidate(context.Background(), BuildRequest{
		Session: session,
		Update:  update,
		Leader:  uint32(len(session.Validators)),
	}, CandidateArtifact{})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("restore leader error = %v, want ErrInvalidInput", err)
	}
}

func TestLocalSoftTimeoutWaitsWithoutConsensusParent(t *testing.T) {
	acquisition, pool, session, update := localRotationFixture(t)
	defer pool.Close()

	decision, err := acquisition.SoftTimeout(context.Background(), SoftTimeoutRequest{
		Current: BuildRequest{Session: session, Update: update},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != SoftTimeoutWait {
		t.Fatalf("soft timeout action = %d, want wait", decision.Action)
	}
}

func TestLocalMasterProjectionChainsAcrossSpeculativeSlots(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	builder := testBuilder()
	first, err := builder.BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	acquisition := &LocalAcquisition{
		groups: tracker,
		configs: localConfigCache{
			entries: make(map[cell.Hash]localPreparedConfig),
		},
	}
	base, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	firstPrevious := candidatePrevious(t, first)
	firstView, err := acquisition.masterViewForPredecessor(
		context.Background(),
		base,
		firstPrevious,
		time.Unix(int64(firstViewGenUtime(t, first)), 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !firstView.context.ID.Equals(&first.ID) ||
		!firstView.context.Groups.MasterchainBlock.Equals(&first.ID) {
		t.Fatal("first speculative view is not bound to the first candidate")
	}

	secondRequest := fixture.request
	secondRequest.Previous = firstPrevious
	secondRequest.Config = firstView.context.Config
	secondRequest.Groups = firstView.context.Groups
	secondRequest.Header.GenUtime++
	secondRequest.Header.GenUtimeMS += 1000
	secondRequest.ShardTops = nil
	second, err := builder.BuildMaster(context.Background(), secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondPrevious := candidatePrevious(t, second)
	secondView, err := acquisition.masterViewForPredecessor(
		context.Background(),
		firstView,
		secondPrevious,
		time.Unix(int64(firstViewGenUtime(t, second)), 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !secondView.context.ID.Equals(&second.ID) || secondView.context.ID.SeqNo != firstView.context.ID.SeqNo+1 {
		t.Fatal("second speculative projection did not chain from the first projected snapshot")
	}
}

func TestLocalMasterProjectionAllowsSpeculativeSessionRotation(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	first, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	previous := candidatePrevious(t, first)
	asOf := time.Unix(int64(firstViewGenUtime(t, first)), 0)
	projected, err := tracker.Project(fixture.request.Groups, groups.ApplyInput{
		Block: previous.ID,
		Root:  previous.State,
		AsOf:  asOf,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := localMasterTestSession(t, fixture.request.Groups)
	rotated := *projected
	rotated.Active = append([]groups.Session(nil), projected.Active...)
	for i := range rotated.Active {
		if rotated.Active[i].ID == session.ID {
			rotated.Active = append(rotated.Active[:i], rotated.Active[i+1:]...)
			break
		}
	}

	// C++ keeps the ValidatorSet injected into ManagerFacade immutable for the
	// consensus runtime. A speculative block may cross the rotation boundary;
	// only applying that block retires the old runtime.
	acquisition := &LocalAcquisition{
		groups: &localAcquisitionTestGroups{projected: &rotated},
		configs: localConfigCache{
			entries: make(map[cell.Hash]localPreparedConfig),
		},
	}
	base, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	view, err := acquisition.masterViewForPredecessor(context.Background(), base, previous, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if view.context.Groups != &rotated {
		t.Fatal("speculative rotated group view was not retained")
	}
}

func localMasterTestSession(t *testing.T, snapshot *groups.Snapshot) ActivatedSession {
	t.Helper()
	var active *groups.Session
	for i := range snapshot.Active {
		if snapshot.Active[i].Shard.IsMasterchain() {
			active = &snapshot.Active[i]
			break
		}
	}
	if active == nil {
		t.Fatal("fixture has no active masterchain group")
	}
	validators := make([]SessionValidator, len(active.Validators))
	for i := range active.Validators {
		validators[i] = SessionValidator{
			PublicKey: active.Validators[i].PublicKey,
			ADNLID:    active.Validators[i].ADNL,
			Weight:    active.Validators[i].Weight,
		}
	}

	return ActivatedSession{
		Session: Session{
			ID:                   active.ID,
			Shard:                active.Shard,
			CatchainSeqno:        active.CatchainSeqno,
			ValidatorSetHash:     active.ValidatorSetHash,
			ConsensusVersion:     2,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 4,
			Validators:           validators,
		},
		Genesis:        active.Genesis,
		MinMasterchain: active.MinMasterchain,
	}
}

func candidatePrevious(t *testing.T, candidate *Candidate) PreviousBlock {
	t.Helper()
	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}

	return PreviousBlock{
		ID:           candidate.ID,
		Block:        root,
		State:        candidate.State,
		OutQueueSize: uint64Pointer(candidate.Stats.OutQueueSize),
	}
}

func firstViewGenUtime(t *testing.T, candidate *Candidate) uint32 {
	t.Helper()
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}

	return state.GenUTime
}

type localAcquisitionFailingMessages struct {
	pool          *msgpool.Pool
	completeErr   error
	completeCalls int
}

func (*localAcquisitionFailingMessages) SelectForBlock(msgpool.ShardIdent, int) []msgpool.ExternalSnapshot {
	return nil
}

func (m *localAcquisitionFailingMessages) OpenExternalStream(
	shard msgpool.ShardIdent,
	capacity int,
) (*msgpool.ExternalStream, error) {
	return m.pool.OpenExternalStream(shard, capacity)
}

func (m *localAcquisitionFailingMessages) OpenExternalStreamExcluding(
	shard msgpool.ShardIdent,
	capacity int,
	exclude [][32]byte,
) (*msgpool.ExternalStream, error) {
	return m.pool.OpenExternalStreamExcluding(shard, capacity, exclude)
}

func (m *localAcquisitionFailingMessages) OpenExternalSnapshot(
	shard msgpool.ShardIdent,
	capacity int,
) (*msgpool.ExternalStream, error) {
	return m.pool.OpenExternalSnapshot(shard, capacity)
}

func (m *localAcquisitionFailingMessages) Complete([]msgpool.ExternalFeedback) error {
	m.completeCalls++

	return m.completeErr
}

func (m *localAcquisitionFailingMessages) Internals() *msgpool.Internals {
	return m.pool.Internals()
}

type finalizedAnchorNoReadStore struct {
	calls int
}

func (s *finalizedAnchorNoReadStore) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	s.calls++

	return nil, errors.New("unexpected finalized-anchor store read")
}

func (s *finalizedAnchorNoReadStore) LoadStateCellTree(
	context.Context,
	ton.BlockIDExt,
	[]byte,
) (*cell.Cell, error) {
	s.calls++

	return nil, errors.New("unexpected finalized-anchor state tree read")
}

func (s *finalizedAnchorNoReadStore) BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	s.calls++

	return nil, errors.New("unexpected finalized-anchor block read")
}

func (s *finalizedAnchorNoReadStore) WaitBlockArtifacts(context.Context, ton.BlockIDExt) error {
	s.calls++

	return errors.New("unexpected finalized-anchor readiness wait")
}

type localAcquisitionTestStore struct {
	block ton.BlockIDExt
	state *cell.Cell
}

func (s *localAcquisitionTestStore) BlockState(
	ctx context.Context,
	block ton.BlockIDExt,
) (*storage.BlockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !block.Equals(&s.block) {
		return nil, storage.ErrNotFound
	}
	hash := s.state.Hash()

	return &storage.BlockState{
		Block:         cloneBlockID(s.block),
		StateRootHash: bytes.Clone(hash),
		Cell:          s.state,
	}, nil
}

func (s *localAcquisitionTestStore) LoadStateCellTree(
	ctx context.Context,
	block ton.BlockIDExt,
	_ []byte,
) (*cell.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !block.Equals(&s.block) {
		return nil, storage.ErrNotFound
	}

	return s.state, nil
}

func (*localAcquisitionTestStore) BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (s *localAcquisitionTestStore) WaitBlockArtifacts(ctx context.Context, block ton.BlockIDExt) error {
	_, err := s.BlockState(ctx, block)
	if err == nil && block.SeqNo != 0 {
		_, err = s.BlockRoot(ctx, block)
	}

	return err
}

type localAcquisitionTestGroups struct {
	snapshot  *groups.Snapshot
	projected *groups.Snapshot
}

func (g *localAcquisitionTestGroups) Snapshot() (*groups.Snapshot, error) {
	return g.snapshot, nil
}

func (g *localAcquisitionTestGroups) Project(
	_ *groups.Snapshot,
	input groups.ApplyInput,
) (*groups.Snapshot, error) {
	if g.projected != nil && input.Block.Equals(&g.projected.MasterchainBlock) {
		return g.projected, nil
	}

	return g.snapshot, nil
}

func (g *localAcquisitionTestGroups) WaitProject(
	ctx context.Context,
	previous *groups.Snapshot,
	input groups.ApplyInput,
) (*groups.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return g.Project(previous, input)
}

func localRotationFixture(
	t *testing.T,
) (*LocalAcquisition, *msgpool.Pool, ActivatedSession, SessionUpdate) {
	t.Helper()
	fixture := newMasterBuildFixture(t, false)
	session := localMasterTestSession(t, fixture.request.Groups)
	var active *groups.Session
	for i := range fixture.request.Groups.Active {
		candidate := &fixture.request.Groups.Active[i]
		if candidate.ID == session.ID {
			active = candidate
			break
		}
	}
	if active == nil {
		t.Fatal("masterchain session is absent from its source snapshot")
	}
	update := SessionUpdate{
		SessionID:                 session.ID,
		TargetRate:                time.Second,
		MasterchainBlock:          cloneBlockID(fixture.request.Previous.ID),
		Registered:                append([]groups.ShardDescription(nil), active.Registered...),
		HasCurrentWindow:          true,
		CurrentWindowStart:        session.SlotsPerLeaderWindow,
		CurrentWindowObservedSlot: session.SlotsPerLeaderWindow,
		CurrentWindowStartAt:      time.Unix(int64(fixture.oldState.GenUTime+1), 0),
	}
	if active.FinalizedBlock != nil {
		update.HasFinalizedBlock = true
		update.FinalizedBlock = cloneBlockID(*active.FinalizedBlock)
	}
	pool := msgpool.New(msgpool.Config{})
	destination := targetShardIdent(session.Shard)
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder: testBuilder(),
		Store: &localAcquisitionTestStore{
			block: fixture.request.Previous.ID,
			state: fixture.request.Previous.State,
		},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err = acquisition.PrepareSession(context.Background(), session.Session, update); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err = acquisition.ActivateSession(context.Background(), SessionActivation{
		SessionID:      session.ID,
		Genesis:        session.Genesis,
		MinMasterchain: session.MinMasterchain,
	}, update); err != nil {
		pool.Close()
		t.Fatal(err)
	}

	return acquisition, pool, session, update
}

// A build that runs beside a block its producer is still finishing may not fall
// back to walking a whole source queue out of its state. That walk is thousands
// of entries deep and it runs with the acquisition session's mutex held — the
// same mutex AdvanceConsensusBase takes to open a window, and the one
// CommitCandidate takes between signing a block and putting it on the wire. A
// successor speculating on a block must not stand in front of that block, so it
// gives the slot back and the producer collates it the ordinary way.
//
// The refusal is invisible to a behavioural test that only checks the resulting
// cut, because seeding produces the right answer either way — just far too late
// and holding the wrong lock. What it produces instead is ErrAcquisitionNotReady,
// which the producer's adoption check turns into "start this slot myself".
func TestUnpinnedSourceIsSeededOnlyWhenSeedingIsAllowed(t *testing.T) {
	request := emptyCandidateRequest(t)
	destination := blockShardIdent(request.Previous.ID)
	queued, enqueued := queuedInternal(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x41}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x42}, 32)),
		1,
		request.Header.GenUtime,
		tlb.FromNanoTONU(100_000),
		tlb.FromNanoTONU(100_000),
		0,
		destination,
	)
	request.Previous.State = stateWithQueueMessage(t, request.Previous.State, queued.Key, enqueued)

	// The source is never seeded into the pool, so PinSource misses and the
	// walk is the only way to a cut. That is exactly the state a successor
	// finds when its predecessor has not been committed yet.
	newCut := func(t *testing.T, seedAllowed bool) (*msgpool.Cut, error) {
		t.Helper()
		pool := msgpool.New(msgpool.Config{})
		t.Cleanup(pool.Close)
		if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
			t.Fatal(err)
		}
		view, err := localViewFromPrevious(request.Previous, true, true)
		if err != nil {
			t.Fatal(err)
		}
		acquisition := &LocalAcquisition{messages: pool}

		return acquisition.cutCommittedViews(
			openLocalTestBranch(t, pool, destination),
			destination,
			map[msgpool.ShardIdent]*localNeighborView{destination: view},
			nil,
			nil,
			seedAllowed,
			&prewarmHints{},
		)
	}

	// Allowed: the walk runs and the queued message is in the cut.
	cut, err := newCut(t, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].EnvHash != queued.EnvHash {
		t.Fatalf("seeded cut returned %+v, want envelope %x", cut.Messages, queued.EnvHash)
	}

	// Refused: the slot goes back to the producer, and it must be recognisable
	// as that rather than as a collation failure.
	if _, err = newCut(t, false); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("refused cut error = %v, want ErrAcquisitionNotReady", err)
	}
}
