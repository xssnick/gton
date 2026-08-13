package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	beforeSplitTriggerMargin = uint64(13)
	beforeSplitBlockMargin   = uint64(11)
)

func (a *LocalAcquisition) AcquireShard(ctx context.Context, request BuildRequest) (ShardRequest, error) {
	managed, err := a.lockRequestSession(request)
	if err != nil {
		return ShardRequest{}, err
	}
	defer managed.mu.Unlock()

	if request.Session.Shard.IsMasterchain() {
		return ShardRequest{}, fmt.Errorf("%w: shard acquisition received a masterchain session", ErrInvalidInput)
	}
	chain, err := a.resolveChain(ctx, managed, request)
	if err != nil {
		return ShardRequest{}, err
	}
	if err = requirePredecessorMasterchain(managed.master, chain.previous); err != nil {
		return ShardRequest{}, err
	}
	header, err := a.requestHeader(request)
	if err != nil {
		return ShardRequest{}, err
	}
	beforeSplit, err := deriveBeforeSplit(
		managed.master.context,
		header,
		request.Session.Shard,
		managed.update.Registered,
	)
	if err != nil {
		return ShardRequest{}, err
	}
	seed, err := a.requestSeed(managed, request.Slot)
	if err != nil {
		return ShardRequest{}, err
	}
	messages, err := a.acquireShardMessages(
		ctx,
		managed.branch,
		managed.master,
		request.Session.Shard,
		chain.previous,
		chain.queueBase,
		chain.candidateTip,
	)
	if err != nil {
		return ShardRequest{}, err
	}

	result := ShardRequest{
		Shard:               request.Session.Shard,
		Previous:            chain.previous[0],
		Masterchain:         managed.master.context,
		Header:              header,
		BeforeSplit:         beforeSplit,
		RandSeed:            seed,
		CreatedBy:           request.Session.Validators[request.Leader].PublicKey,
		MaxExternalAttempts: a.maxExternalAttempts,
		StorageStats:        chain.storage,
		Dispatch:            a.dispatch,
		Internals:           messages.cut,
		Neighbors:           messages.neighbors,
		NeighborShardEndLT:  messages.shardEndLT,
	}
	if len(chain.previous) == 2 {
		second := chain.previous[1]
		result.Previous2 = &second
	}
	if managed.master.context.Config.capabilities&capFullCollatedData != 0 {
		result.FullCollatedProofs = messages.proofs
	}

	return result, nil
}

func deriveBeforeSplit(
	master MasterchainContext,
	header HeaderParams,
	target groups.ShardID,
	registered []groups.ShardDescription,
) (bool, error) {
	if len(registered) != 1 {
		return false, nil
	}
	description := &registered[0]
	if description.Shard != target || description.BeforeSplit || description.BeforeMerge ||
		description.FSM.Kind != groups.ShardFSMSplit {
		return false, nil
	}

	depth, err := target.PrefixBits()
	if err != nil {
		return false, fmt.Errorf("%w: derive split shard depth: %v", ErrInvalidInput, err)
	}
	if depth >= masterShardMaxSplitDepth {
		return false, nil
	}

	start := uint64(description.FSM.UTime)
	end := start + uint64(description.FSM.Interval)
	// C++ consults wall time before init_utime and later serializes the
	// consensus slot time. The slot time is the deterministic protocol input
	// with the same accept-set; using it here keeps local and remote collation
	// reproducible while retaining both C++ safety margins.
	tmpNow := max(uint64(master.GenUtime), uint64(header.GenUtime))
	return splitWindowAllowsBeforeSplit(tmpNow, uint64(header.GenUtime), start, end), nil
}

func splitWindowAllowsBeforeSplit(tmpNow, blockNow, start, end uint64) bool {
	return tmpNow >= start && tmpNow+beforeSplitTriggerMargin < end &&
		blockNow+beforeSplitBlockMargin <= end
}

func (a *LocalAcquisition) AcquireMaster(ctx context.Context, request BuildRequest) (MasterRequest, error) {
	managed, err := a.lockRequestSession(request)
	if err != nil {
		return MasterRequest{}, err
	}
	defer managed.mu.Unlock()

	if !request.Session.Shard.IsMasterchain() {
		return MasterRequest{}, fmt.Errorf("%w: master acquisition received a shardchain session", ErrInvalidInput)
	}
	chain, err := a.resolveChain(ctx, managed, request)
	if err != nil {
		return MasterRequest{}, err
	}
	if len(chain.previous) != 1 {
		return MasterRequest{}, fmt.Errorf("%w: masterchain requires exactly one predecessor", ErrInvalidInput)
	}
	header, err := a.requestHeader(request)
	if err != nil {
		return MasterRequest{}, err
	}
	master := chain.master
	if master == nil {
		master, err = a.masterViewForPredecessor(
			ctx,
			managed.master,
			chain.previous[0],
			time.UnixMilli(int64(header.GenUtimeMS)),
		)
		if err != nil {
			return MasterRequest{}, err
		}
	}
	seed, err := a.requestSeed(managed, request.Slot)
	if err != nil {
		return MasterRequest{}, err
	}
	if a.shardTops == nil {
		return MasterRequest{}, fmt.Errorf("%w: master shard-top inbox is unavailable", ErrAcquisitionNotReady)
	}

	ltLimit := master.state.GenLT
	if math.MaxUint64-ltLimit < masterMaxLTGrowth {
		ltLimit = math.MaxUint64
	} else {
		ltLimit += masterMaxLTGrowth
	}
	provider := newLocalShardTopProvider(a, master)
	tops, err := a.shardTops.Select(ctx, ShardTopSelection{
		Masterchain:   cloneBlockID(master.context.ID),
		VertSeqno:     master.context.VertSeqno,
		GenUtime:      header.GenUtime,
		GlobalVersion: master.context.Config.globalVersion,
		LTLimit:       ltLimit,
		Registry:      master.registry,
		Provider:      provider,
	})
	if err != nil {
		return MasterRequest{}, fmt.Errorf("select masterchain shard tops: %w", err)
	}

	messages, err := a.acquireMasterMessages(
		ctx,
		managed.branch,
		master,
		chain.previous,
		chain.queueBase,
		chain.candidateTip,
		tops,
		provider,
	)
	if err != nil {
		return MasterRequest{}, err
	}
	result := MasterRequest{
		Previous:            chain.previous[0],
		Config:              master.context.Config,
		Groups:              master.context.Groups,
		Header:              header,
		RandSeed:            seed,
		CreatedBy:           request.Session.Validators[request.Leader].PublicKey,
		MaxExternalAttempts: a.maxExternalAttempts,
		StorageStats:        chain.storage,
		Dispatch:            a.dispatch,
		Internals:           messages.cut,
		Neighbors:           messages.neighbors,
		NeighborShardEndLT:  messages.shardEndLT,
		ShardTops:           tops,
	}
	if master.context.Config.capabilities&capFullCollatedData != 0 {
		result.FullCollatedProofs = messages.proofs
	}

	return result, nil
}

func (a *LocalAcquisition) SoftTimeout(
	ctx context.Context,
	request SoftTimeoutRequest,
) (SoftTimeoutDecision, error) {
	managed, err := a.session(request.Current.Session.ID)
	if err != nil {
		return SoftTimeoutDecision{}, err
	}
	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.retired {
		return SoftTimeoutDecision{}, ErrSessionRetired
	}
	// consensus.empty carries a mandatory parent candidate id. The first
	// candidate after a finalized base has no such parent yet, so a slow build
	// must continue to its hard deadline instead of attempting an invalid
	// empty artifact.
	if !request.Current.Parent.Exists {
		return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
	}
	chain, err := a.resolveChain(ctx, managed, request.Current)
	if err != nil {
		return SoftTimeoutDecision{}, err
	}
	if len(chain.previous) != 1 {
		return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
	}

	return SoftTimeoutDecision{Action: SoftTimeoutEmitEmpty, Block: cloneBlockID(chain.previous[0].ID)}, nil
}

func (a *LocalAcquisition) lockRequestSession(request BuildRequest) (*localAcquisitionSession, error) {
	return a.lockPipelineSession(request, false)
}

func (a *LocalAcquisition) lockRestoreSession(request BuildRequest) (*localAcquisitionSession, error) {
	return a.lockPipelineSession(request, true)
}

func (a *LocalAcquisition) lockPipelineSession(
	request BuildRequest,
	restore bool,
) (*localAcquisitionSession, error) {
	managed, err := a.session(request.Session.ID)
	if err != nil {
		return nil, err
	}
	managed.mu.Lock()
	if managed.retired {
		managed.mu.Unlock()
		return nil, ErrSessionRetired
	}
	if managed.activation == nil || !sameActivatedSession(
		activatedSession(managed.session, *managed.activation),
		request.Session,
	) || (!restore && !managed.update.Equal(request.Update)) ||
		(restore && !sameSessionChainUpdate(managed.update, request.Update)) {
		managed.mu.Unlock()
		return nil, ErrSessionConflict
	}
	if uint64(request.Leader) >= uint64(len(request.Session.Validators)) {
		managed.mu.Unlock()
		return nil, fmt.Errorf("%w: delegated leader index is outside the roster", ErrInvalidInput)
	}

	return managed, nil
}

func (a *LocalAcquisition) resolveChain(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
) (localResolvedChain, error) {
	if request.Previous != nil {
		if request.Previous.SessionID != request.Session.ID ||
			request.Previous.Candidate.ID.Slot >= request.Slot ||
			request.Parent != simplex.Parent(request.Previous.Candidate.ID) {
			return localResolvedChain{}, fmt.Errorf("%w: previous artifact does not bind the build request", ErrCandidateConflict)
		}
		state, err := a.resolveArtifactState(ctx, managed, *request.Previous)
		if err != nil {
			return localResolvedChain{}, err
		}

		return localResolvedChain{
			previous:     []PreviousBlock{state.block},
			storage:      state.storageStats,
			queueBase:    clonePreviousBlocks(state.queueBase),
			candidateTip: cloneHashPointer(state.queueTip),
			master:       state.master,
		}, nil
	}

	if request.Parent != request.Update.CurrentBase {
		return localResolvedChain{}, fmt.Errorf("%w: first build parent differs from the current base", ErrCandidateConflict)
	}
	if request.Update.CurrentBase.Exists {
		state, exists := managed.candidates[request.Update.CurrentBase.ID]
		if !exists {
			return localResolvedChain{}, fmt.Errorf("%w: current consensus base candidate state is unavailable",
				ErrAcquisitionNotReady)
		}

		return localResolvedChain{
			previous:     []PreviousBlock{state.block},
			storage:      state.storageStats,
			queueBase:    clonePreviousBlocks(state.queueBase),
			candidateTip: cloneHashPointer(state.queueTip),
			master:       state.master,
		}, nil
	}
	previous := make([]PreviousBlock, len(request.Session.Genesis))
	for i := range request.Session.Genesis {
		block, _, err := a.resolveBlock(ctx, managed, request.Session.Genesis[i])
		if err != nil {
			return localResolvedChain{}, err
		}
		previous[i] = block
	}

	return localResolvedChain{previous: previous}, nil
}

func (a *LocalAcquisition) resolveArtifactState(
	ctx context.Context,
	managed *localAcquisitionSession,
	artifact CandidateArtifact,
) (localCandidateState, error) {
	if state, exists := managed.candidates[artifact.Candidate.ID]; exists {
		if !state.block.ID.Equals(&artifact.Candidate.Block) {
			return localCandidateState{}, ErrCandidateConflict
		}
		// The private branch remains authoritative for the rest of this
		// producer window even after the same block reaches the committed feed.
		// Re-rooting here would rebuild queue-sized indexes for every applied
		// block and would diverge from C++ ChainStateRef ownership.
		return state, nil
	}
	previous, _, err := a.resolveBlock(ctx, managed, artifact.Candidate.Block)
	if err != nil {
		return localCandidateState{}, err
	}

	return localCandidateState{block: previous}, nil
}

func (a *LocalAcquisition) resolveBlock(
	ctx context.Context,
	managed *localAcquisitionSession,
	id ton.BlockIDExt,
) (PreviousBlock, AccountStorageStats, error) {
	key, err := blockRootKey(id)
	if err != nil {
		return PreviousBlock{}, nil, err
	}
	if candidate, exists := managed.blocks[key]; exists {
		if !candidate.block.ID.Equals(&id) {
			return PreviousBlock{}, nil, ErrCandidateConflict
		}
		return candidate.block, candidate.storageStats, nil
	}
	previous, _, err := a.loadPrevious(ctx, id)
	if err != nil {
		return PreviousBlock{}, nil, err
	}

	return previous, nil, nil
}

func requirePredecessorMasterchain(master *localMasterView, previous []PreviousBlock) error {
	for i := range previous {
		var state tlb.ShardStateUnsplit
		if err := parseExact(&state, previous[i].State); err != nil {
			return fmt.Errorf("%w: decode predecessor %d: %v", ErrInvalidInput, i, err)
		}
		var stats tlb.ShardStateStats
		if err := parseExact(&stats, state.Stats); err != nil {
			return fmt.Errorf("%w: decode predecessor %d statistics: %v", ErrInvalidInput, i, err)
		}
		if err := master.requirePredecessorMasterReference(previous[i].ID, stats.MasterRef); err != nil {
			return err
		}
	}

	return nil
}

func blockRootKey(id ton.BlockIDExt) ([32]byte, error) {
	if len(id.RootHash) != 32 {
		return [32]byte{}, fmt.Errorf("%w: block root hash is not 256 bits", ErrInvalidInput)
	}
	var key [32]byte
	copy(key[:], id.RootHash)

	return key, nil
}

func cloneHashPointer(hash *[32]byte) *[32]byte {
	if hash == nil {
		return nil
	}
	cloned := *hash

	return &cloned
}

func (a *LocalAcquisition) CommitCandidate(ctx context.Context, commit CandidateCommit) error {
	managed, err := a.lockRequestSession(commit.Request)
	if err != nil {
		return err
	}
	defer managed.mu.Unlock()

	return a.commitCandidateLocked(ctx, managed, commit.Request, commit.Built, commit.Artifact)
}

func (a *LocalAcquisition) RestoreCandidate(
	ctx context.Context,
	request BuildRequest,
	artifact CandidateArtifact,
) error {
	managed, err := a.lockRestoreSession(request)
	if err != nil {
		return err
	}
	defer managed.mu.Unlock()

	if _, exists := managed.candidates[artifact.Candidate.ID]; exists {
		return nil
	}
	if request.FinalizedAnchor != nil {
		if request.Previous != nil ||
			!sameBlockID(artifact.Candidate.Block, *request.FinalizedAnchor) {
			return ErrCandidateConflict
		}
		// C++ StateResolver loads an accepted candidate's exact block state
		// through ChainState::from_manager. Only speculative descendants apply
		// their Merkle updates to the preceding candidate state.
		return a.restoreFinalizedCandidateAnchor(ctx, managed, request, artifact)
	}
	if artifact.Candidate.Empty {
		return a.commitCandidateLocked(ctx, managed, request, nil, artifact)
	}

	chain, err := a.resolveChain(ctx, managed, request)
	if err != nil {
		return err
	}
	root, block, state, err := restoreCandidateState(artifact, chain.previous)
	if err != nil {
		return err
	}
	queueSize, err := exactOutQueueSize(state.OutMsgQueueInfo)
	if err != nil {
		return err
	}
	built := &Candidate{
		ID:          cloneBlockID(artifact.Candidate.Block),
		CreatedBy:   request.Session.Validators[request.Leader].PublicKey,
		BlockBOC:    artifact.BlockBOC,
		State:       root,
		StateUpdate: block.StateUpdate,
		Stats:       Stats{OutQueueSize: queueSize},
	}
	return a.commitCandidateLocked(ctx, managed, request, built, artifact)
}

func (a *LocalAcquisition) restoreFinalizedCandidateAnchor(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
	artifact CandidateArtifact,
) error {
	if artifact.SessionID != request.Session.ID || artifact.Candidate.ID.Slot != request.Slot ||
		artifact.Candidate.Parent != request.Parent {
		return ErrCandidateConflict
	}
	var (
		previous     PreviousBlock
		storageStats AccountStorageStats
		err          error
	)
	if request.FinalizedAnchorState != nil {
		previous, err = finalizedAnchorPrevious(request.FinalizedAnchorState, artifact.Candidate.Block)
		if err != nil {
			return err
		}
	} else {
		// Remote collators cannot receive node-resident cells. The in-process
		// resolver always supplies FinalizedAnchorState, so this read remains a
		// compatibility path for that separate storage-owning deployment.
		previous, storageStats, err = a.resolveBlock(ctx, managed, artifact.Candidate.Block)
		if err != nil {
			return err
		}
	}
	if err = managed.branch.Retain(nil); err != nil {
		return fmt.Errorf("clear candidates preceding finalized anchor: %w", err)
	}
	key, err := blockRootKey(previous.ID)
	if err != nil {
		return err
	}
	clear(managed.candidates)
	clear(managed.blocks)
	state := localCandidateState{block: previous, storageStats: storageStats}
	managed.candidates[artifact.Candidate.ID] = state
	managed.blocks[key] = state

	return nil
}

func finalizedAnchorPrevious(anchor *FinalizedAnchorState, id ton.BlockIDExt) (PreviousBlock, error) {
	if anchor == nil || !sameBlockID(anchor.Block, id) || anchor.State == nil {
		return PreviousBlock{}, ErrCandidateConflict
	}

	previous := PreviousBlock{
		ID:    cloneBlockID(anchor.Block),
		State: anchor.State,
	}
	if id.SeqNo == 0 {
		if len(anchor.BlockBOC) != 0 {
			return PreviousBlock{}, ErrCandidateConflict
		}
	} else {
		root, err := cell.FromBOC(anchor.BlockBOC)
		if err != nil {
			return PreviousBlock{}, fmt.Errorf("%w: decode finalized anchor block: %v", ErrCandidateConflict, err)
		}
		if !bytes.Equal(root.Hash(), id.RootHash) {
			return PreviousBlock{}, fmt.Errorf("%w: finalized anchor block root differs from its id", ErrCandidateConflict)
		}
		previous.Block = root
	}

	state, err := verifyPredecessor("finalized anchor", &previous)
	if err != nil {
		return PreviousBlock{}, err
	}
	queueSize, err := exactOutQueueSize(state.OutMsgQueueInfo)
	if err != nil {
		return PreviousBlock{}, err
	}
	previous.OutQueueSize = uint64Pointer(queueSize)

	return previous, nil
}

func (a *LocalAcquisition) commitCandidateLocked(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
	built *Candidate,
	artifact CandidateArtifact,
) error {
	if artifact.SessionID != request.Session.ID || artifact.Candidate.ID.Slot != request.Slot ||
		artifact.Candidate.Parent != request.Parent {
		return ErrCandidateConflict
	}
	if existing, exists := managed.candidates[artifact.Candidate.ID]; exists {
		if existing.block.ID.Equals(&artifact.Candidate.Block) {
			return nil
		}
		return ErrCandidateConflict
	}
	if artifact.Candidate.Empty {
		state, err := a.resolveArtifactBlock(ctx, managed, request, artifact.Candidate.Block)
		if err != nil {
			return err
		}
		managed.candidates[artifact.Candidate.ID] = state

		return nil
	}
	if built == nil || built.State == nil || !built.ID.Equals(&artifact.Candidate.Block) ||
		!bytes.Equal(built.BlockBOC, artifact.BlockBOC) {
		return ErrCandidateConflict
	}
	root, block, err := parseCandidateBlock(built.ID, built.BlockBOC)
	if err != nil {
		return err
	}
	if block.StateUpdate.HashKeyAt(0) != built.StateUpdate.HashKeyAt(0) {
		return ErrCandidateConflict
	}
	chain, err := a.resolveChain(ctx, managed, request)
	if err != nil {
		return err
	}
	stateRoot, candidateState, err := applyCandidateStateUpdate(chain.previous, block.StateUpdate)
	if err != nil {
		return err
	}
	if stateRoot.HashKeyAt(0) != built.State.HashKeyAt(0) {
		return ErrCandidateConflict
	}
	previous := PreviousBlock{
		ID:           cloneBlockID(built.ID),
		Block:        root,
		State:        stateRoot,
		OutQueueSize: uint64Pointer(built.Stats.OutQueueSize),
	}
	if _, err = verifyPredecessor("candidate", &previous); err != nil {
		return err
	}
	state := localCandidateState{
		block:        previous,
		storageStats: built.StorageStats,
	}
	if request.Session.Shard.IsMasterchain() {
		base := chain.master
		if base == nil {
			base = managed.master
		}
		state.master, err = a.masterViewForPredecessor(
			ctx,
			base,
			previous,
			time.Unix(int64(candidateState.GenUTime), 0),
		)
		if err != nil {
			return err
		}
	}
	queueID, err := blockRootKey(built.ID)
	if err != nil {
		return err
	}
	state.queueTip = &queueID
	if chain.candidateTip == nil {
		state.queueBase = clonePreviousBlocks(chain.previous)
	} else {
		parent, exists := managed.blocks[*chain.candidateTip]
		if !exists || parent.queueTip == nil {
			return fmt.Errorf("%w: candidate queue parent is unavailable", ErrAcquisitionNotReady)
		}
		state.queueBase = clonePreviousBlocks(parent.queueBase)
	}
	if chain.candidateTip == nil {
		if err = ensureCandidateBase(managed.branch, chain.previous); err != nil {
			return err
		}
	}
	source := targetShardIdent(groups.ShardID{Workchain: built.ID.Workchain, Shard: built.ID.Shard})
	delta, err := managed.branch.DeltaFromBlockRoot(
		source,
		msgpool.SourceRef{Seqno: built.ID.SeqNo, RootHash: queueID},
		root,
		block.BlockInfo.StartLt,
	)
	if err != nil {
		return fmt.Errorf("derive candidate message delta: %w", err)
	}
	queueRequest := msgpool.CandidateRequest{
		ID:    queueID,
		Seqno: built.ID.SeqNo,
		Delta: delta,
	}
	if chain.candidateTip != nil {
		queueRequest.Parent = cloneHashPointer(chain.candidateTip)
	} else {
		queueRequest.Base = candidateSources(chain.previous)
	}
	if err = managed.branch.AddCandidate(queueRequest); err != nil {
		return fmt.Errorf("commit candidate message delta: %w", err)
	}
	if err = a.messages.Complete(built.Externals); err != nil {
		managed.branch.DropCandidate(queueID)

		return fmt.Errorf("complete candidate external messages: %w", err)
	}
	managed.candidates[artifact.Candidate.ID] = state
	managed.blocks[queueID] = state

	return nil
}

func (a *LocalAcquisition) resolveArtifactBlock(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
	id ton.BlockIDExt,
) (localCandidateState, error) {
	chain, err := a.resolveChain(ctx, managed, request)
	if err != nil {
		return localCandidateState{}, err
	}
	if len(chain.previous) != 1 || !chain.previous[0].ID.Equals(&id) {
		return localCandidateState{}, fmt.Errorf(
			"%w: empty candidate block differs from the exact predecessor",
			ErrCandidateConflict,
		)
	}

	return localCandidateState{
		block:        chain.previous[0],
		storageStats: chain.storage,
		queueBase:    clonePreviousBlocks(chain.queueBase),
		queueTip:     cloneHashPointer(chain.candidateTip),
		master:       chain.master,
	}, nil
}

func clonePreviousBlocks(previous []PreviousBlock) []PreviousBlock {
	if len(previous) == 0 {
		return nil
	}
	cloned := make([]PreviousBlock, len(previous))
	copy(cloned, previous)
	for i := range cloned {
		cloned[i].ID = cloneBlockID(cloned[i].ID)
		if cloned[i].OutQueueSize != nil {
			value := *cloned[i].OutQueueSize
			cloned[i].OutQueueSize = &value
		}
	}

	return cloned
}

func restoreCandidateState(
	artifact CandidateArtifact,
	previous []PreviousBlock,
) (*cell.Cell, tlb.Block, tlb.ShardStateUnsplit, error) {
	// Only the header and the state update are needed here; the block root is
	// re-derived by the callers that actually hold on to it.
	_, block, err := parseCandidateBlock(artifact.Candidate.Block, artifact.BlockBOC)
	if err != nil {
		return nil, tlb.Block{}, tlb.ShardStateUnsplit{}, err
	}
	stateRoot, state, err := applyCandidateStateUpdate(previous, block.StateUpdate)
	if err != nil {
		return nil, tlb.Block{}, tlb.ShardStateUnsplit{}, err
	}
	if state.Seqno != artifact.Candidate.Block.SeqNo ||
		state.ShardIdent.WorkchainID != artifact.Candidate.Block.Workchain ||
		int64(state.ShardIdent.GetShardID()) != artifact.Candidate.Block.Shard {
		return nil, tlb.Block{}, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: recovered state differs from candidate ID", ErrInvalidInput)
	}
	return stateRoot, block, state, nil
}

// applyCandidateStateUpdate derives a live successor tree from the exact
// predecessor and the block's Merkle update. Builder's state is a trace-pruned
// view for block serialization; retaining it would lose untouched lazy cells
// (notably the masterchain config dictionary). Applying the update here keeps
// those immutable branches attached to their storage-backed loader, just as
// ordinary block application does.
func applyCandidateStateUpdate(
	previous []PreviousBlock,
	update *cell.Cell,
) (*cell.Cell, tlb.ShardStateUnsplit, error) {
	var oldRoot *cell.Cell
	var err error
	switch len(previous) {
	case 1:
		oldRoot = previous[0].State
	case 2:
		oldRoot, err = mechanicalSplitState(previous[0].State, previous[1].State)
		if err != nil {
			return nil, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: combine predecessor states: %v", ErrInvalidInput, err)
		}
	default:
		return nil, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: invalid predecessor count", ErrInvalidInput)
	}
	stateRoot, err := cell.ApplyMerkleUpdate(oldRoot.WithoutTrace(), update)
	if err != nil {
		return nil, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: apply state update: %v", ErrInvalidInput, err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, stateRoot); err != nil {
		return nil, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: decode updated state: %v", ErrInvalidInput, err)
	}

	return stateRoot, state, nil
}

func parseCandidateBlock(id ton.BlockIDExt, boc []byte) (*cell.Cell, tlb.Block, error) {
	root, err := cell.FromBOC(boc)
	if err != nil {
		return nil, tlb.Block{}, fmt.Errorf("%w: decode candidate BOC: %v", ErrInvalidInput, err)
	}
	if !bytes.Equal(root.Hash(), id.RootHash) {
		return nil, tlb.Block{}, fmt.Errorf("%w: candidate root differs from its block ID", ErrInvalidInput)
	}
	var block tlb.Block
	if err = parseExact(&block, root); err != nil {
		return nil, tlb.Block{}, fmt.Errorf("%w: decode candidate block: %v", ErrInvalidInput, err)
	}
	if block.BlockInfo.SeqNo != id.SeqNo || block.BlockInfo.Shard.WorkchainID != id.Workchain ||
		int64(block.BlockInfo.Shard.GetShardID()) != id.Shard {
		return nil, tlb.Block{}, fmt.Errorf("%w: candidate header differs from its block ID", ErrInvalidInput)
	}

	return root, block, nil
}

func candidateSources(previous []PreviousBlock) []msgpool.CandidateSource {
	sources := make([]msgpool.CandidateSource, len(previous))
	for i := range previous {
		var hash [32]byte
		copy(hash[:], previous[i].ID.RootHash)
		sources[i] = msgpool.CandidateSource{
			Source: targetShardIdent(groups.ShardID{
				Workchain: previous[i].ID.Workchain,
				Shard:     previous[i].ID.Shard,
			}),
			Visible: msgpool.SourceRef{Seqno: previous[i].ID.SeqNo, RootHash: hash},
		}
	}

	return sources
}

// ensureCandidateBase pins the first candidate's exact predecessor queues.
// The local leader normally reuses snapshots already captured by Branch.Cut.
// A voter restoring a candidate after shared history was compacted derives the
// same private snapshot from the authenticated predecessor state, without
// regressing or materializing the process-wide committed feed.
func ensureCandidateBase(
	branch *msgpool.Branch,
	previous []PreviousBlock,
) error {
	for index := range previous {
		source := targetShardIdent(groups.ShardID{
			Workchain: previous[index].ID.Workchain,
			Shard:     previous[index].ID.Shard,
		})
		ref, err := localSourceRef(previous[index].ID)
		if err != nil {
			return err
		}
		if err = branch.PinSource(source, ref); err == nil {
			continue
		}
		if !errors.Is(err, msgpool.ErrCutNotReady) && !errors.Is(err, msgpool.ErrCutStale) &&
			!errors.Is(err, msgpool.ErrNotFound) {
			return fmt.Errorf("pin candidate predecessor queue: %w", err)
		}
		if _, err = branch.SeedSourceFromStateRoot(source, ref, previous[index].State); err != nil {
			return fmt.Errorf("seed candidate predecessor queue: %w", err)
		}
	}

	return nil
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

// ResolveCandidateState returns the exact normal predecessor state used by
// consensus::EmptyBlockPolicy. The first candidate of a session is always
// collated and therefore does not call this method; every later candidate must
// have a single normal predecessor which an empty candidate can reference.
func (a *LocalAcquisition) ResolveCandidateState(
	ctx context.Context,
	request BuildRequest,
) (CandidateState, error) {
	managed, err := a.lockRequestSession(request)
	if err != nil {
		return CandidateState{}, err
	}
	defer managed.mu.Unlock()

	chain, err := a.resolveChain(ctx, managed, request)
	if err != nil {
		return CandidateState{}, err
	}
	if len(chain.previous) != 1 {
		return CandidateState{}, fmt.Errorf("%w: candidate state is not a normal predecessor", ErrAcquisitionNotReady)
	}
	previous := chain.previous[0]
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, previous.State); err != nil {
		return CandidateState{}, fmt.Errorf("%w: decode candidate predecessor state: %v", ErrInvalidInput, err)
	}
	if state.Seqno != previous.ID.SeqNo {
		return CandidateState{}, fmt.Errorf("%w: candidate predecessor state seqno differs from block", ErrInvalidInput)
	}
	if state.Seqno == math.MaxUint32 {
		return CandidateState{}, fmt.Errorf("%w: candidate predecessor seqno overflows", ErrInvalidInput)
	}

	return CandidateState{
		Block:       cloneBlockID(previous.ID),
		NextSeqno:   state.Seqno + 1,
		BeforeSplit: state.BeforeSplit,
	}, nil
}

// BuildCandidate acquires the deterministic inputs for the slot and runs the
// stateless Builder over them. Acquisition stays behind AcquireShard and
// AcquireMaster rather than being inlined: those release the session lock on
// return, and SoftTimeout contends for it concurrently with the in-flight build.
func (a *LocalAcquisition) BuildCandidate(ctx context.Context, request BuildRequest) (*Candidate, error) {
	// Taken before acquisition, not before collation: the reference collator
	// opens this span when its actor is constructed, so waiting for predecessor
	// and masterchain states counts against the slot the same way collating
	// does. The split decision reads the two spans apart.
	assembly := time.Now()
	chain := metricChain(request.Session.Shard.IsMasterchain())
	if request.Session.Shard.IsMasterchain() {
		stream, err := a.messages.OpenExternalSnapshot(
			targetShardIdent(request.Session.Shard),
			a.externalLimit,
		)
		if err != nil {
			return nil, fmt.Errorf("open ready masterchain external snapshot: %w", err)
		}
		defer stream.Close()

		input, err := a.AcquireMaster(ctx, request)
		if err != nil {
			a.observeCollationStage(chain, CollationStageAcquireInputs, assembly)

			return nil, err
		}
		a.observeCollationStage(chain, CollationStageAcquireInputs, assembly)

		started := time.Now()
		candidate, buildErr := a.builder.buildMasterWithReadyExternals(
			ctx,
			input,
			stream,
			request.ExternalProcessUntil,
			a.externalLimit,
		)
		a.observeCollationStage(chain, CollationStageCore, started)
		a.logCoreCollation(request, started, candidate, buildErr)
		return candidate, buildErr
	}

	stream, err := a.messages.OpenExternalStream(targetShardIdent(request.Session.Shard), a.externalLimit)
	if err != nil {
		return nil, fmt.Errorf("open ready external stream: %w", err)
	}
	defer stream.Close()

	input, err := a.AcquireShard(ctx, request)
	if err != nil {
		a.observeCollationStage(chain, CollationStageAcquireInputs, assembly)

		return nil, err
	}
	a.observeCollationStage(chain, CollationStageAcquireInputs, assembly)

	started := time.Now()
	candidate, buildErr := a.builder.buildShardWithReadyExternals(
		ctx,
		input,
		stream,
		request.ExternalProcessUntil,
		request.ExternalWaitUntil,
		a.externalLimit,
		assembly,
	)
	a.observeCollationStage(chain, CollationStageCore, started)
	a.logCoreCollation(request, started, candidate, buildErr)
	return candidate, buildErr
}

func (a *LocalAcquisition) observeCollationStage(
	chain MetricChain,
	stage CollationStage,
	started time.Time,
) {
	if a.collationObserver == nil {
		return
	}

	a.collationObserver.ObserveCollationStage(CollationStageObservation{
		Chain:    chain,
		Stage:    stage,
		Duration: time.Since(started),
	})
}

func (a *LocalAcquisition) logCoreCollation(
	request BuildRequest,
	started time.Time,
	candidate *Candidate,
	buildErr error,
) {
	event := a.log.Debug()
	if event == nil {
		return
	}

	event.
		Hex("session_id", request.Session.ID[:]).
		Int32("workchain", request.Session.Shard.Workchain).
		Int64("shard", request.Session.Shard.Shard).
		Uint32("slot", request.Slot).
		Uint32("leader", request.Leader).
		Float64("collation_core_ms", float64(time.Since(started))/float64(time.Millisecond)).
		Float64("external_wait_ms", candidateExternalWaitMS(candidate)).
		Uint32("external_batches", candidateExternalBatches(candidate)).
		Str("external_stop", candidateExternalStop(candidate)).
		Int("block_bytes", candidateBytes(candidate)).
		Int("collated_bytes", candidateCollatedBytes(candidate)).
		Str("overload_reason", candidateOverloadReason(candidate)).
		Err(buildErr).
		Msg("collation_core_measure_done")
}

// candidateOverloadReason separates a shard that split because a block-limit
// axis filled up from one that split because collation stopped fitting in its
// slot. The two call for opposite investigations, and the bit in the state
// cannot tell them apart.
func candidateOverloadReason(candidate *Candidate) string {
	if candidate == nil {
		return OverloadNone.String()
	}
	return candidate.Stats.OverloadReason.String()
}

func candidateExternalWaitMS(candidate *Candidate) float64 {
	if candidate == nil {
		return 0
	}
	return float64(candidate.Stats.ExternalWait) / float64(time.Millisecond)
}

func candidateExternalBatches(candidate *Candidate) uint32 {
	if candidate == nil {
		return 0
	}
	return candidate.Stats.ExternalBatches
}

func candidateExternalStop(candidate *Candidate) string {
	if candidate == nil {
		return ExternalStopUnknown.String()
	}
	return candidate.Stats.ExternalStop.String()
}
