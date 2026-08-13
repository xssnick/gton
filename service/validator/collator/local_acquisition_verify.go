package collator

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// ValidationRequest binds an ordinary consensus candidate to the exact,
// ordered predecessor states resolved by Simplex. Inputs are borrowed and
// immutable for the duration of ValidateCandidate.
type ValidationRequest struct {
	Session      ActivatedSession
	Update       SessionUpdate
	Previous     []PreviousBlock
	Candidate    simplex.Candidate
	BlockBOC     []byte
	CollatedData []byte
}

// ValidationResult is the earliest wall-clock instant at which an accepted
// candidate may be voted for. It is the exact ConsensusExtraData millisecond
// timestamp, matching ValidateCandidateResult::ok_from_utime.
type ValidationResult struct {
	ValidAfter time.Time
}

// ValidateCandidate acquires every authenticated auxiliary view required by
// deterministic validation. It never reads the shard-top inbox: a masterchain
// candidate carries the exact TopBlockDescrSet that its shard transition uses.
func (a *LocalAcquisition) ValidateCandidate(
	ctx context.Context,
	request ValidationRequest,
) (ValidationResult, error) {
	chain := metricChain(request.Session.Shard.IsMasterchain())
	if err := ctx.Err(); err != nil {
		return ValidationResult{}, err
	}
	if request.Candidate.Empty {
		return ValidationResult{}, fmt.Errorf("%w: local validation received an empty candidate", ErrInvalidInput)
	}
	if len(request.Previous) == 0 || len(request.Previous) > 2 {
		return ValidationResult{}, fmt.Errorf("%w: candidate has %d predecessors", ErrInvalidInput, len(request.Previous))
	}

	if err := validateSessionRecord(SessionRecord{
		Session: request.Session.Session,
		Activation: &SessionActivation{
			SessionID:      request.Session.ID,
			Genesis:        request.Session.Genesis,
			MinMasterchain: request.Session.MinMasterchain,
		},
		Update: request.Update,
	}); err != nil {
		return ValidationResult{}, err
	}
	if uint64(request.Candidate.Leader) >= uint64(len(request.Session.Validators)) {
		return ValidationResult{}, fmt.Errorf("%w: candidate leader index is outside the roster", ErrInvalidInput)
	}
	artifact := CandidateArtifact{
		SessionID:    request.Session.ID,
		Candidate:    request.Candidate,
		BlockBOC:     request.BlockBOC,
		CollatedData: request.CollatedData,
	}
	started := a.validationStageStarted()
	previous, collated, block, stateRoot, err := a.restoreValidationState(request, artifact)
	a.observeValidationStage(chain, ValidationCoreStageRestoreState, started)
	if err != nil {
		return ValidationResult{}, err
	}
	started = a.validationStageStarted()
	base, err := a.validationMasterView(
		ctx,
		request.Session,
		request.Update,
		time.Unix(int64(block.BlockInfo.GenUtime), 0),
	)
	a.observeValidationStage(chain, ValidationCoreStageMasterView, started)
	if err != nil {
		return ValidationResult{}, err
	}
	built := &Candidate{
		ID:               cloneBlockID(request.Candidate.Block),
		CreatedBy:        request.Session.Validators[request.Candidate.Leader].PublicKey,
		BlockBOC:         request.BlockBOC,
		CollatedData:     request.CollatedData,
		CollatedFileHash: request.Candidate.CollatedFileHash,
		State:            stateRoot,
		StateUpdate:      block.StateUpdate,
	}

	var verified verifiedCandidate
	if request.Session.Shard.IsMasterchain() {
		if len(previous) != 1 {
			return ValidationResult{}, fmt.Errorf("%w: masterchain candidate requires one predecessor", ErrInvalidInput)
		}
		started = a.validationStageStarted()
		master, viewErr := a.validationMasterPredecessor(
			ctx,
			base,
			previous[0],
			time.Unix(int64(block.BlockInfo.GenUtime), 0),
		)
		a.observeValidationStage(chain, ValidationCoreStageChainInputs, started)
		if viewErr != nil {
			return ValidationResult{}, viewErr
		}
		started = a.validationStageStarted()
		verified, err = verifyCandidateWith(ctx, master.context.Config, built, &collated)
		a.observeValidationStage(chain, ValidationCoreStageDecode, started)
		if err != nil {
			return ValidationResult{}, err
		}
		started = a.validationStageStarted()
		if err = a.validateMasterCandidate(ctx, master, previous[0], built, &verified); err != nil {
			a.observeValidationStage(chain, ValidationCoreStageTransition, started)

			return ValidationResult{}, err
		}
		a.observeValidationStage(chain, ValidationCoreStageTransition, started)
	} else {
		started = a.validationStageStarted()
		master, viewErr := a.validationMasterReference(
			ctx,
			base,
			block.BlockInfo.MasterRef,
			time.Unix(int64(block.BlockInfo.GenUtime), 0),
		)
		a.observeValidationStage(chain, ValidationCoreStageChainInputs, started)
		if viewErr != nil {
			return ValidationResult{}, viewErr
		}
		started = a.validationStageStarted()
		verified, err = verifyCandidateWith(ctx, master.context.Config, built, &collated)
		a.observeValidationStage(chain, ValidationCoreStageDecode, started)
		if err != nil {
			return ValidationResult{}, err
		}
		started = a.validationStageStarted()
		if err = a.validateShardCandidate(ctx, request.Session.Session, master, previous, built, &verified); err != nil {
			a.observeValidationStage(chain, ValidationCoreStageTransition, started)

			return ValidationResult{}, err
		}
		a.observeValidationStage(chain, ValidationCoreStageTransition, started)
	}

	a.observeValidatedCandidate(chain, request, &verified)

	return ValidationResult{ValidAfter: time.UnixMilli(int64(verified.collated.genUtimeMS))}, nil
}

// observeValidatedCandidate reports the shape of a block this node checked but
// did not build, onto the same metrics collation reports its own blocks to. The
// counts come from the replay, which walked the descriptors and transactions to
// verify them; nothing here re-reads the block. Only accepted candidates are
// reported: a rejected one has no meaningful shape, and the reasons it was
// rejected are their own metric.
func (a *LocalAcquisition) observeValidatedCandidate(
	chain MetricChain,
	request ValidationRequest,
	verified *verifiedCandidate,
) {
	if a.collationObserver == nil {
		return
	}

	a.collationObserver.ObserveCollationCandidate(CandidateObservation{
		Chain:         chain,
		Origin:        CandidateOriginValidation,
		Kind:          CandidateKindBlock,
		BlockBytes:    len(request.BlockBOC),
		CollatedBytes: len(request.CollatedData),
		Shape:         verified.shape,
	})
}

func (a *LocalAcquisition) validationStageStarted() time.Time {
	if a.validationObserver == nil {
		return time.Time{}
	}

	return time.Now()
}

func (a *LocalAcquisition) observeValidationStage(
	chain MetricChain,
	stage ValidationCoreStage,
	started time.Time,
) {
	if a.validationObserver == nil {
		return
	}

	a.validationObserver.ObserveValidationCoreStage(ValidationCoreStageObservation{
		Chain:    chain,
		Stage:    stage,
		Duration: time.Since(started),
	})
}

// restoreValidationState decodes the candidate once and decides which tree its
// successor state is built on.
//
// With full collated data a shard candidate is self-contained, so the
// predecessors are re-pointed at the states it proves and the resident ones are
// never touched — the node can validate a shard it does not hold, and cannot
// accept a candidate whose proof is narrower than the reference validator
// requires. The masterchain keeps its resident predecessor: collated data
// carries no masterchain state proof, because every validator already holds
// that state.
func (a *LocalAcquisition) restoreValidationState(
	request ValidationRequest,
	artifact CandidateArtifact,
) ([]PreviousBlock, verifiedCollatedData, tlb.Block, *cell.Cell, error) {
	_, block, err := parseCandidateBlock(artifact.Candidate.Block, artifact.BlockBOC)
	if err != nil {
		return nil, verifiedCollatedData{}, tlb.Block{}, nil, err
	}
	collated, err := verifyCollatedData(&Candidate{
		CollatedData:     artifact.CollatedData,
		CollatedFileHash: artifact.Candidate.CollatedFileHash,
	}, block.BlockInfo.GenUtime)
	if err != nil {
		return nil, verifiedCollatedData{}, tlb.Block{}, nil, err
	}

	previous := request.Previous
	if collated.full && !request.Session.Shard.IsMasterchain() {
		if previous, err = provenPredecessorStates(&collated, previous); err != nil {
			return nil, verifiedCollatedData{}, tlb.Block{}, nil, err
		}
	}
	stateRoot, state, err := applyCandidateStateUpdate(previous, block.StateUpdate)
	if err != nil {
		return nil, verifiedCollatedData{}, tlb.Block{}, nil, err
	}
	if state.Seqno != artifact.Candidate.Block.SeqNo ||
		state.ShardIdent.WorkchainID != artifact.Candidate.Block.Workchain ||
		int64(state.ShardIdent.GetShardID()) != artifact.Candidate.Block.Shard {
		return nil, verifiedCollatedData{}, tlb.Block{}, nil,
			fmt.Errorf("%w: recovered state differs from candidate ID", ErrInvalidInput)
	}

	return previous, collated, block, stateRoot, nil
}

func (a *LocalAcquisition) validationMasterView(
	ctx context.Context,
	session ActivatedSession,
	update SessionUpdate,
	asOf time.Time,
) (*localMasterView, error) {
	master, err := a.projectedMasterView(ctx, update.MasterchainBlock, asOf)
	if err != nil {
		return nil, err
	}
	if err = master.requireAncestor(session.MinMasterchain); err != nil {
		return nil, err
	}
	if err = validateAcquisitionGroup(session, update, master.context.Groups, true); err != nil {
		return nil, err
	}

	return master, nil
}

func (a *LocalAcquisition) projectedMasterView(
	ctx context.Context,
	id ton.BlockIDExt,
	asOf time.Time,
) (*localMasterView, error) {
	// The applied-block hook publishes the newest coherent block/state pair
	// before the node makes it visible through LocalStateStore. Session updates
	// run from that same hook, so an exact resident hit must precede storage.
	// localMasterView and everything reachable from it are immutable.
	a.master.mu.RLock()
	resident := a.master.view
	if resident != nil && id.Equals(&resident.context.ID) {
		a.master.mu.RUnlock()

		return resident, nil
	}
	a.master.mu.RUnlock()

	previous, state, err := a.loadPrevious(ctx, id)
	if err != nil {
		return nil, err
	}
	snapshot, err := a.groups.Project(nil, groups.ApplyInput{
		Block: id,
		Root:  previous.State,
		AsOf:  asOf,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: project validator groups: %v", ErrAcquisitionNotReady, err)
	}
	if snapshot == nil || !snapshot.MasterchainBlock.Equals(&id) {
		return nil, fmt.Errorf("%w: projected validator groups belong to another masterchain block", ErrInvalidInput)
	}
	if !snapshot.Ready {
		return nil, fmt.Errorf("%w: projected validator group snapshot is not ready", ErrAcquisitionNotReady)
	}
	master, err := a.masterView(previous, state, snapshot)
	if err != nil {
		return nil, err
	}
	if master.context.Config.execution.Root().HashKey() != snapshot.ConfigRootHash {
		return nil, fmt.Errorf("%w: projected validator group config differs from masterchain state", ErrInvalidInput)
	}

	return master, nil
}

func (a *LocalAcquisition) validationMasterReference(
	ctx context.Context,
	base *localMasterView,
	reference *tlb.ExtBlkRef,
	asOf time.Time,
) (*localMasterView, error) {
	if reference == nil {
		return nil, fmt.Errorf("%w: shard candidate has no masterchain reference", ErrInvalidInput)
	}
	if len(reference.RootHash) != 32 || len(reference.FileHash) != 32 {
		return nil, fmt.Errorf("%w: candidate masterchain reference has invalid hashes", ErrInvalidInput)
	}
	if reference.SeqNo > base.context.ID.SeqNo {
		// Validation is independent from the local collator actor in C++. Its
		// manager resolves the exact masterchain reference carried by the shard
		// candidate, which can legitimately be newer than this runtime's last
		// published collation view. Load that authenticated state directly and
		// require the cached view to be its ancestor.
		id := ton.BlockIDExt{
			Workchain: masterchainWorkchainID,
			Shard:     math.MinInt64,
			SeqNo:     reference.SeqNo,
			RootHash:  bytes.Clone(reference.RootHash),
			FileHash:  bytes.Clone(reference.FileHash),
		}
		view, err := a.projectedMasterView(ctx, id, asOf)
		if err != nil {
			return nil, err
		}
		if err = view.requireAncestor(base.context.ID); err != nil {
			return nil, fmt.Errorf("%w: candidate masterchain reference does not extend the selected history", ErrInvalidInput)
		}

		return view, nil
	}
	id, err := base.blockAt(reference.SeqNo)
	if err != nil {
		return nil, err
	}
	if !equalBlockHash(id, reference.RootHash, reference.FileHash) {
		return nil, fmt.Errorf("%w: candidate masterchain reference is not in the selected history", ErrInvalidInput)
	}
	if id.Equals(&base.context.ID) {
		return base, nil
	}

	previous, state, err := a.loadPrevious(ctx, id)
	if err != nil {
		return nil, err
	}
	projected, err := a.groups.Project(nil, groups.ApplyInput{Block: id, Root: previous.State, AsOf: asOf})
	if err != nil {
		return nil, fmt.Errorf("%w: project referenced validator groups: %v", ErrAcquisitionNotReady, err)
	}
	if projected == nil || !projected.MasterchainBlock.Equals(&id) {
		return nil, fmt.Errorf("%w: projected validator groups belong to another masterchain block", ErrInvalidInput)
	}

	view, err := a.masterView(previous, state, projected)
	if err != nil {
		return nil, err
	}
	if err = validateProjectedMasterView(view, projected); err != nil {
		return nil, err
	}

	return view, nil
}

func (a *LocalAcquisition) validationMasterPredecessor(
	ctx context.Context,
	base *localMasterView,
	previous PreviousBlock,
	asOf time.Time,
) (*localMasterView, error) {
	if previous.ID.Equals(&base.context.ID) {
		return base, nil
	}
	if previous.ID.SeqNo > base.context.ID.SeqNo {
		view, err := a.masterViewForPredecessor(ctx, base, previous, asOf)
		if err != nil {
			return nil, err
		}
		if err = validateProjectedMasterView(view, view.context.Groups); err != nil {
			return nil, err
		}

		return view, nil
	}
	if err := base.requireAncestor(previous.ID); err != nil {
		return nil, err
	}
	state, err := verifyPredecessor("master", &previous)
	if err != nil {
		return nil, err
	}
	projected, err := a.groups.Project(nil, groups.ApplyInput{
		Block: previous.ID,
		Root:  previous.State,
		AsOf:  asOf,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: project predecessor validator groups: %v", ErrAcquisitionNotReady, err)
	}
	if projected == nil || !projected.MasterchainBlock.Equals(&previous.ID) {
		return nil, fmt.Errorf("%w: projected validator groups belong to another masterchain block", ErrInvalidInput)
	}

	view, err := a.masterView(previous, state, projected)
	if err != nil {
		return nil, err
	}
	if err = validateProjectedMasterView(view, projected); err != nil {
		return nil, err
	}

	return view, nil
}

func validateProjectedMasterView(view *localMasterView, snapshot *groups.Snapshot) error {
	if snapshot == nil || !snapshot.Ready || !snapshot.MasterchainBlock.Equals(&view.context.ID) {
		return fmt.Errorf("%w: projected validator group snapshot is not ready for the masterchain view", ErrAcquisitionNotReady)
	}
	if view.context.Config.execution.Root().HashKey() != snapshot.ConfigRootHash {
		return fmt.Errorf("%w: projected validator group config differs from masterchain state", ErrInvalidInput)
	}

	return nil
}

func (a *LocalAcquisition) validateShardCandidate(
	ctx context.Context,
	session Session,
	master *localMasterView,
	previous []PreviousBlock,
	candidate *Candidate,
	verified *verifiedCandidate,
) error {
	expected, err := expectedShardNeighbors(master.context, session.Shard)
	if err != nil {
		return err
	}
	// Candidate validation never emits a collated proof: no
	// FullCollatedProofProvider is constructed on this path, so tracing a
	// resident neighbor state would only pin a materialized cell per queue cell
	// the replay walks.
	neighbors, err := a.loadExpectedNeighbors(ctx, master, expected, previous, nil, &verified.collated, false)
	if err != nil {
		return err
	}
	endLT, err := a.historicalShardEndLT(ctx, master, previous, neighbors, &verified.collated, nil)
	if err != nil {
		return err
	}
	request := ShardVerificationRequest{
		Previous:           previous[0],
		Masterchain:        master.context,
		Neighbors:          neighbors,
		NeighborShardEndLT: endLT,
		Semantics:          a.semantics,
		Candidate:          candidate,
		stateProven:        verified.collated.full,
	}
	if len(previous) == 2 {
		request.Previous2 = &previous[1]
	}

	return verifyPreparedShardCandidate(ctx, request, verified)
}

func (a *LocalAcquisition) validateMasterCandidate(
	ctx context.Context,
	master *localMasterView,
	previous PreviousBlock,
	candidate *Candidate,
	verified *verifiedCandidate,
) error {
	registry, tops, err := a.masterValidationTops(ctx, master, verified)
	if err != nil {
		return err
	}
	expected := expectedMasterNeighbors(registry, previous.ID)
	neighbors, err := a.loadExpectedNeighbors(
		ctx,
		master,
		expected,
		[]PreviousBlock{previous},
		nil,
		&verified.collated,
		false,
	)
	if err != nil {
		return err
	}
	endLT, err := a.historicalShardEndLT(
		ctx,
		master,
		[]PreviousBlock{previous},
		neighbors,
		&verified.collated,
		registry,
	)
	if err != nil {
		return err
	}

	return verifyPreparedMasterCandidate(ctx, MasterVerificationRequest{
		Previous:           previous,
		Config:             master.context.Config,
		Groups:             master.context.Groups,
		ShardTops:          tops,
		Neighbors:          neighbors,
		NeighborShardEndLT: endLT,
		Semantics:          a.semantics,
		Candidate:          candidate,
	}, verified)
}

func equalBlockHash(id ton.BlockIDExt, rootHash, fileHash []byte) bool {
	return bytes.Equal(id.RootHash, rootHash) && bytes.Equal(id.FileHash, fileHash)
}
