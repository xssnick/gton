package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/rs/zerolog"
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
	previous, _, err := a.loadPrevious(ctx, id, acquisitionReadImmediate)
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
	blockRoot, stateRoot, block, state, err := restoreCandidateState(artifact, chain.previous)
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
		State:       stateRoot,
		StateUpdate: block.StateUpdate,
		Stats:       Stats{OutQueueSize: queueSize},
	}
	// The restore has already parsed this block and applied its update over this
	// exact chain; the commit used to do both a second time. verifyPredecessor
	// still runs, because a restored candidate is a from-disk tree and that check
	// is the only thing binding it.
	derived := PreviousBlock{
		ID:           cloneBlockID(built.ID),
		Block:        blockRoot,
		State:        stateRoot,
		OutQueueSize: uint64Pointer(queueSize),
	}
	if _, err = verifyPredecessor("candidate", &derived); err != nil {
		return err
	}
	ready := &readyCommit{
		chain: chain,
		derivation: commitDerivation{
			root:     blockRoot,
			state:    stateRoot,
			startLT:  block.BlockInfo.StartLt,
			genUTime: state.GenUTime,
		},
	}

	return a.commitDerivedCandidateLocked(ctx, managed, request, built, artifact, ready)
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

// commitCandidateLocked records one produced or restored candidate as the
// state a later build extends. It is the only writer of managed.candidates and
// managed.blocks for a non-empty candidate.
//
// The block's derivation — a parsed root, the successor state, the header's
// start logical time and the state's generation time — reaches it one of two
// ways. Our own builder already holds all four when it returns, and hands them
// over in the candidate's capsule; anything else replays the bytes. Which one
// happens is decided by the type: only finish() can create a capsule. See
// candidate_built_commit.go and deriveCommitLocked.
func (a *LocalAcquisition) commitCandidateLocked(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
	built *Candidate,
	artifact CandidateArtifact,
) error {
	return a.commitDerivedCandidateLocked(ctx, managed, request, built, artifact, nil)
}

// commitDerivedCandidateLocked is commitCandidateLocked with the derivation
// optionally already in hand. RestoreCandidate needs that: it parses the block
// and applies the update to check the restored artifact before it gets here, and
// passing the result through is what keeps the restore path from doing both
// twice.
func (a *LocalAcquisition) commitDerivedCandidateLocked(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
	built *Candidate,
	artifact CandidateArtifact,
	ready *readyCommit,
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

	var (
		chain      localResolvedChain
		derivation commitDerivation
		err        error
	)
	if ready != nil {
		chain, derivation = ready.chain, ready.derivation
	} else {
		if chain, err = a.resolveChain(ctx, managed, request); err != nil {
			return err
		}
		if derivation, err = a.deriveCommitLocked(built, chain.previous); err != nil {
			return err
		}
	}

	return a.recordCandidateLocked(ctx, managed, request, built, artifact, chain, derivation)
}

// readyCommit carries a derivation its caller already produced, together with
// the chain it was produced against. The two travel as one because a derivation
// is only meaningful for the predecessors it was applied over.
type readyCommit struct {
	chain      localResolvedChain
	derivation commitDerivation
}

// deriveCommitLocked prefers the derivation our own builder already performed
// and falls back to replaying the serialized bytes.
//
// Removing the replay from the self path removes, per produced block: a full
// eager parse of the block BOC, a second application of the Merkle update over
// the predecessor, and verifyPredecessor's re-parse of both the block and the
// state. The reference does strictly less than what remains — ChainState::apply
// (chain-state.cpp:116) deserializes the candidate lazily, unpacks it, applies
// the update, and checks nothing at all about the predecessor it applied over,
// on its own candidate as well as on foreign ones.
//
// What the removed checks covered is either impossible by construction here or
// caught where the value is next used: the ID was built from the same header the
// block was serialized from, the accounts were prefix-checked as they were
// written, and the next build re-parses the state it receives. The single clause
// whose failure would have been silent — a published state that is not the
// target its own update names — is asserted in finish(), in O(1), where the two
// hashes are already computed.
func (a *LocalAcquisition) deriveCommitLocked(
	built *Candidate,
	previous []PreviousBlock,
) (commitDerivation, error) {
	if derivation, bound := built.built.bind(built.ID, built.State, previous); bound {
		return derivation, nil
	}
	if built.built != nil {
		// A capsule that does not bind means the chain being committed is not the
		// chain the block was built over. Replay still produces the right answer —
		// or the right rejection — but this is never expected on the self path and
		// a silent fallback would cost the whole saving with nothing to show.
		a.builtBindMisses.Add(1)
		a.log.Warn().
			Uint32("seqno", built.ID.SeqNo).
			Int("predecessors", len(previous)).
			Msg("built candidate capsule does not bind its commit chain; replaying the block")
	}

	return deriveCommitByReplay(built, previous)
}

// deriveCommitByReplay is the derivation for a block this node did not build:
// one restored from the durable candidate store, one delivered by a foreign
// Pipeline, or one whose capsule did not bind. Here the parse is genuine work
// and verifyPredecessor is the only binding there is, so both stay.
func deriveCommitByReplay(built *Candidate, previous []PreviousBlock) (commitDerivation, error) {
	root, block, err := parseCandidateBlock(built.ID, built.BlockBOC)
	if err != nil {
		return commitDerivation{}, err
	}
	if block.StateUpdate.HashKeyAt(0) != built.StateUpdate.HashKeyAt(0) {
		return commitDerivation{}, ErrCandidateConflict
	}
	stateRoot, candidateState, _, err := applyCandidateStateUpdate(previous, block.StateUpdate)
	if err != nil {
		return commitDerivation{}, err
	}
	if stateRoot.HashKeyAt(0) != built.State.HashKeyAt(0) {
		return commitDerivation{}, ErrCandidateConflict
	}
	derived := PreviousBlock{
		ID:           cloneBlockID(built.ID),
		Block:        root,
		State:        stateRoot,
		OutQueueSize: uint64Pointer(built.Stats.OutQueueSize),
	}
	if _, err = verifyPredecessor("candidate", &derived); err != nil {
		return commitDerivation{}, err
	}

	return commitDerivation{
		root:     root,
		state:    stateRoot,
		startLT:  block.BlockInfo.StartLt,
		genUTime: candidateState.GenUTime,
	}, nil
}

// recordCandidateLocked installs one derived candidate: the predecessor view a
// later build extends, the masterchain view it is anchored to, the queue tip and
// base, and the message-pool delta. Both derivations end here, so the fast and
// the replayed path cannot drift apart in what they record.
func (a *LocalAcquisition) recordCandidateLocked(
	ctx context.Context,
	managed *localAcquisitionSession,
	request BuildRequest,
	built *Candidate,
	artifact CandidateArtifact,
	chain localResolvedChain,
	derivation commitDerivation,
) error {
	previous := PreviousBlock{
		ID:           cloneBlockID(built.ID),
		Block:        derivation.root,
		State:        derivation.state,
		OutQueueSize: uint64Pointer(built.Stats.OutQueueSize),
	}
	state := localCandidateState{
		block:        previous,
		storageStats: built.StorageStats,
	}
	var err error
	if request.Session.Shard.IsMasterchain() {
		base := chain.master
		if base == nil {
			base = managed.master
		}
		state.master, err = a.masterViewForPredecessor(
			ctx,
			base,
			previous,
			time.Unix(int64(derivation.genUTime), 0),
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
		derivation.root,
		derivation.startLT,
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

// restoreCandidateState replays one stored candidate: it parses the block and
// applies the block's update over the resolved predecessors. Both roots are
// returned, because the commit that follows needs the block root as well and
// re-deriving it would be a second parse of bytes this one just walked.
func restoreCandidateState(
	artifact CandidateArtifact,
	previous []PreviousBlock,
) (*cell.Cell, *cell.Cell, tlb.Block, tlb.ShardStateUnsplit, error) {
	blockRoot, block, err := parseCandidateBlock(artifact.Candidate.Block, artifact.BlockBOC)
	if err != nil {
		return nil, nil, tlb.Block{}, tlb.ShardStateUnsplit{}, err
	}
	stateRoot, state, _, err := applyCandidateStateUpdate(previous, block.StateUpdate)
	if err != nil {
		return nil, nil, tlb.Block{}, tlb.ShardStateUnsplit{}, err
	}
	if state.Seqno != artifact.Candidate.Block.SeqNo ||
		state.ShardIdent.WorkchainID != artifact.Candidate.Block.Workchain ||
		int64(state.ShardIdent.GetShardID()) != artifact.Candidate.Block.Shard {
		return nil, nil, tlb.Block{}, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: recovered state differs from candidate ID", ErrInvalidInput)
	}
	return blockRoot, stateRoot, block, state, nil
}

// applyCandidateStateUpdate derives a live successor tree from the exact
// predecessor and the block's Merkle update. Builder's state is a trace-pruned
// view for block serialization; retaining it would lose untouched lazy cells
// (notably the masterchain config dictionary). Applying the update here keeps
// those immutable branches attached to their storage-backed loader, just as
// ordinary block application does.
// Its third result is the single root the update was applied to — the sole
// predecessor state, or the merged root built over two — so a caller that needs
// to say which tree this successor belongs to can name it without rebuilding
// the merge.
func applyCandidateStateUpdate(
	previous []PreviousBlock,
	update *cell.Cell,
) (*cell.Cell, tlb.ShardStateUnsplit, cell.Hash, error) {
	var none cell.Hash
	var oldRoot *cell.Cell
	var err error
	switch len(previous) {
	case 1:
		oldRoot = previous[0].State
	case 2:
		if oldRoot, err = mechanicalSplitState(previous[0].State, previous[1].State); err != nil {
			return nil, tlb.ShardStateUnsplit{}, none, fmt.Errorf("%w: combine predecessor states: %v", ErrInvalidInput, err)
		}
	default:
		return nil, tlb.ShardStateUnsplit{}, none, fmt.Errorf("%w: invalid predecessor count", ErrInvalidInput)
	}
	stateRoot, err := cell.ApplyMerkleUpdate(oldRoot.WithoutTrace(), update)
	if err != nil {
		return nil, tlb.ShardStateUnsplit{}, none, fmt.Errorf("%w: apply state update: %v", ErrInvalidInput, err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, stateRoot); err != nil {
		return nil, tlb.ShardStateUnsplit{}, none, fmt.Errorf("%w: decode updated state: %v", ErrInvalidInput, err)
	}

	return stateRoot, state, oldRoot.HashKeyAt(0), nil
}

func parseCandidateBlock(id ton.BlockIDExt, boc []byte) (*cell.Cell, tlb.Block, error) {
	candidateBlockParses.Add(1)
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
			a.observeCollationStage(chain, CollationStageAcquireInputs, time.Since(assembly))

			return nil, err
		}
		a.observeCollationStage(chain, CollationStageAcquireInputs, time.Since(assembly))
		// From assembly, the instant this build started, and not from a reading
		// taken now. C++ sets queue_cleanup_timeout_ in the Collator constructor
		// (collator.cpp:85-97), before start_up issues one query; this branch has
		// no external wait, so the budget is a quarter of the soft deadline, and
		// measured from here that quarter would be taken over a remainder the
		// state loads above have already eaten — a window that shrinks with this
		// node's disk latency and reaches zero when acquisition reaches the soft
		// deadline. See TestQueueCleanupBudgetDoesNotDependOnAcquisitionLatency.
		input.QueueCleanupUntil = queueCleanupUntil(
			assembly,
			request.BuildSoftDeadline,
			request.ExternalWaitUntil,
		)
		input.InternalMsgUntil = internalMsgUntil(
			assembly,
			request.BuildSoftDeadline,
			request.ExternalWaitUntil,
		)

		started := time.Now()
		candidate, external, buildErr := a.builder.buildMasterWithReadyExternals(
			ctx,
			input,
			stream,
			request.ExternalProcessUntil,
			a.externalLimit,
		)
		elapsed := time.Since(started)
		a.observeCandidateAssembly(chain, elapsed, external.wait)
		a.logCandidateAssembly(request, elapsed, external, candidate, buildErr)
		a.reportCollationAlarms(chain, request, candidate, buildErr)
		return candidate, buildErr
	}

	stream, err := a.messages.OpenExternalStream(targetShardIdent(request.Session.Shard), a.externalLimit)
	if err != nil {
		return nil, fmt.Errorf("open ready external stream: %w", err)
	}
	defer stream.Close()

	input, err := a.AcquireShard(ctx, request)
	if err != nil {
		a.observeCollationStage(chain, CollationStageAcquireInputs, time.Since(assembly))

		return nil, err
	}
	a.observeCollationStage(chain, CollationStageAcquireInputs, time.Since(assembly))
	// The budget is derived here rather than inside the collation so that the
	// deterministic entry points — BuildShard, restore, every test — keep a zero
	// instant and an inert budget, and from assembly rather than from a fresh
	// reading for the reason the masterchain branch above spells out. On this
	// branch the external wait pins the budget to an absolute instant anyway, so
	// the two readings agree; taking the same one keeps that an accident of the
	// parameters instead of a second rule.
	input.QueueCleanupUntil = queueCleanupUntil(
		assembly,
		request.BuildSoftDeadline,
		request.ExternalWaitUntil,
	)
	input.InternalMsgUntil = internalMsgUntil(
		assembly,
		request.BuildSoftDeadline,
		request.ExternalWaitUntil,
	)

	started := time.Now()
	candidate, external, buildErr := a.builder.buildShardWithReadyExternals(
		ctx,
		input,
		stream,
		request.ExternalProcessUntil,
		request.ExternalWaitUntil,
		a.externalLimit,
		assembly,
	)
	elapsed := time.Since(started)
	a.observeCandidateAssembly(chain, elapsed, external.wait)
	a.logCandidateAssembly(request, elapsed, external, candidate, buildErr)
	a.reportCollationAlarms(chain, request, candidate, buildErr)
	return candidate, buildErr
}

func (a *LocalAcquisition) observeCollationStage(
	chain MetricChain,
	stage CollationStage,
	duration time.Duration,
) {
	if a.collationObserver == nil {
		return
	}

	a.collationObserver.ObserveCollationStage(CollationStageObservation{
		Chain:    chain,
		Stage:    stage,
		Duration: duration,
	})
}

func (a *LocalAcquisition) observeCandidateAssembly(
	chain MetricChain,
	duration time.Duration,
	externalWait time.Duration,
) {
	a.observeCollationStage(chain, CollationStageAssembleCandidate, duration-externalWait)
	a.observeCollationStage(chain, CollationStageWaitExternalMessages, externalWait)
}

func (a *LocalAcquisition) logCandidateAssembly(
	request BuildRequest,
	duration time.Duration,
	external readyExternalStats,
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
		Float64("build_ms", float64(duration)/float64(time.Millisecond)).
		Float64("assemble_candidate_ms", float64(duration-external.wait)/float64(time.Millisecond)).
		Float64("wait_external_messages_ms", float64(external.wait)/float64(time.Millisecond)).
		Uint32("external_batches", external.batches).
		Uint32("build_attempts", external.attempts).
		Str("external_stop", external.stop.String()).
		Str("external_phase", externalFailurePhase(external, buildErr)).
		Int("block_bytes", candidateBytes(candidate)).
		Int("collated_bytes", candidateCollatedBytes(candidate)).
		Str("overload_reason", candidateOverloadReason(candidate)).
		Uint32("queue_cleaned", candidateQueueCleaned(candidate)).
		Str("queue_cleanup_stop", candidateQueueCleanupStop(candidate)).
		Str("processed_scan_trace_error", candidateProcessedScanTraceError(candidate)).
		Err(buildErr)
	logExternalShape(event, external)
	event.Msg("candidate assembly finished")
}

// externalFailurePhase is the field the previous investigation needed and did
// not have. external_stop is only meaningful on a SUCCESSFUL attempt — the
// reason it read "unknown" on all 71 order-violation events is that it was
// assigned after the wave loop — so on a failure this reports which phase the
// attempt died in instead, and on a success it reports nothing, leaving
// external_stop as the answer.
func externalFailurePhase(external readyExternalStats, buildErr error) string {
	if buildErr == nil {
		return ""
	}
	if external.failedAt == "" {
		// Reached only if the error came from outside the wave loop, e.g. the
		// size-limit retry gave up. Naming that is still better than "unknown".
		return "outside_wave_loop"
	}
	return external.failedAt
}

// logExternalShape adds the counters that used to die with the nil candidate.
// block_bytes and collated_bytes are zero on every failure because they are read
// off the candidate, which is why no field record said how many externals had
// been pre-admitted or how far the internal import had got — the two facts that
// would have distinguished the field's external_batches==1 events from an
// unreachable path on sight.
func logExternalShape(event *zerolog.Event, external readyExternalStats) {
	if !external.shape.valid {
		return
	}
	shape := external.shape
	event.
		Int("externals_pre_admitted", shape.preAdmitted).
		Uint32("internals_imported", shape.internalsImported).
		Uint32("immediate_delivered", shape.immediateDelivered).
		Uint32("enqueued_messages", shape.enqueuedMessages).
		Uint64("start_lt", shape.startLT).
		Uint64("last_processed_lt", shape.lastProcLT)
}

// candidateQueueCleaned and candidateQueueCleanupStop mirror the two
// observability points the reference collator emits around cleanup
// (stats_.msg_queue_cleaned and its two "completed only partially" warnings).
// A wall-clock budget that fires silently is a budget nobody will ever tune.
func candidateQueueCleaned(candidate *Candidate) uint32 {
	if candidate == nil {
		return 0
	}
	return candidate.Stats.QueueCleaned
}

func candidateQueueCleanupStop(candidate *Candidate) string {
	if candidate == nil {
		return CleanupStopExhausted.String()
	}
	return candidate.Stats.QueueCleanupStop.String()
}

// candidateProcessedScanTraceError surfaces the one failure this collator
// deliberately swallows. Without it the operator sees a block that every peer
// rejects for a pruned-branch error and nothing that says why the proof was
// short in the first place.
func candidateProcessedScanTraceError(candidate *Candidate) string {
	if candidate == nil {
		return ""
	}
	return candidate.Stats.ProcessedScanTraceError
}

// reportCollationAlarms raises the two producer faults that a debug log line
// cannot carry, because in both cases the node needs an operator and neither
// shows up in any other series.
//
// A short collated proof rides on a build that SUCCEEDED: the block is signed,
// broadcast and counted as a candidate like any other, and the only trace of
// the defect is a field on a Debug event this node most likely does not emit.
// What the network sees is every peer rejecting the block with "augmented dict
// has special cells in tree structure", which names a cell and not a cause.
//
// A mandatory-dequeue overflow is the opposite shape: no block at all, and one
// failed build among all the other reasons builds fail, indistinguishable in
// builds_total{result="error"} from a cancelled slot or a bad input.
//
// Both are logged at Error and both move a counter, so a node in either state
// can be found from :8085 without reading anyone else's logs. See
// gton_collator_alarms_total in METRICS.md.
func (a *LocalAcquisition) reportCollationAlarms(
	chain MetricChain,
	request BuildRequest,
	candidate *Candidate,
	buildErr error,
) {
	if reason := candidateProcessedScanTraceError(candidate); reason != "" {
		a.observeCollationAlarm(chain, CollationAlarmShortCollatedProof)
		if event := a.log.Error(); event != nil {
			event.
				Str("reason", reason).
				Hex("session_id", request.Session.ID[:]).
				Int32("workchain", request.Session.Shard.Workchain).
				Int64("shard", request.Session.Shard.Shard).
				Uint32("slot", request.Slot).
				Msg("collated proof does not cover the predecessor queue scan a validator will make; " +
					"the block was produced and will very likely be rejected as a pruned branch")
		}
	}
	if !errors.Is(buildErr, ErrMandatoryDequeueOverflow) {
		return
	}
	a.observeCollationAlarm(chain, CollationAlarmMandatoryDequeueOverflow)
	if event := a.log.Error(); event != nil {
		event.
			Err(buildErr).
			Hex("session_id", request.Session.ID[:]).
			Int32("workchain", request.Session.Shard.Workchain).
			Int64("shard", request.Session.Shard.Shard).
			Uint32("slot", request.Slot).
			Msg("collation lost the slot: the predecessor's already-processed own-shard queue entries " +
				"do not fit one block, and the dequeue they require cannot be truncated")
	}
}

func (a *LocalAcquisition) observeCollationAlarm(chain MetricChain, alarm CollationAlarm) {
	if a.collationObserver == nil {
		return
	}
	a.collationObserver.ObserveCollationAlarm(chain, alarm)
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
