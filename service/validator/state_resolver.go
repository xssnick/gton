package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	errFinalizedLineageAhead        = errors.New("validator runtime: finalized lineage anchor is not an ancestor")
	errFinalizedCandidateNotApplied = errors.New("validator runtime: finalized candidate state is not applied")
)

const consensusExtraDataTag = uint64(0x638eb292)

// ResolvedState is one cached candidate-parent state. GenUtime is zero for
// consensus genesis and exact to the millisecond for ordinary candidates.
type ResolvedState struct {
	State    *ChainState
	GenUtime time.Time
}

type resolvedLineage struct {
	Candidates         []*CandidateArtifact
	AppliedAnchor      *ton.BlockIDExt
	AppliedAnchorState *ChainState
}

type stateResolver struct {
	shard      groups.ShardID
	storageID  SessionStorageID
	storage    ValidatorStorage
	backend    SessionBackend
	candidates *candidateResolver
	recovery   []*simplex.Certificate

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	genesis   *ChainState
	startAt   *SessionStart
	anchor    *resolvedAnchorState
	states    map[simplex.ParentID]*stateFlight
	finalized map[simplex.CandidateID]*finalizedState
	persisted map[simplex.CandidateID]struct{}
	isClosed  bool
}

type stateFlight struct {
	done   chan struct{}
	result ResolvedState
	err    error
}

type finalizedState struct {
	isDone     bool
	reconciled bool
	applied    bool
	// appliedState is the exact immutable state loaded from the ordinary node
	// store. Keeping it with the finalization marker lets the in-process
	// collator restore the same anchor without a second storage read.
	appliedState *ChainState
	inFlight     *resolverFlight
}

// resolvedAnchorState is the latest ordinary finalized anchor supplied by an
// applied session state. It is deliberately separate from finalized: a
// masterchain update can prove an anchor applied before this session's
// certificate replay has marked the candidate done.
type resolvedAnchorState struct {
	id    simplex.CandidateID
	block ton.BlockIDExt
	state *ChainState
}

func newStateResolver(
	shard groups.ShardID,
	storageID SessionStorageID,
	storage ValidatorStorage,
	backend SessionBackend,
	candidates *candidateResolver,
	stored StoredSessionState,
	recovery []*simplex.Certificate,
) *stateResolver {
	ctx, cancel := context.WithCancel(context.Background())
	r := &stateResolver{
		shard:      shard,
		storageID:  storageID,
		storage:    storage,
		backend:    backend,
		candidates: candidates,
		recovery:   recovery,
		ctx:        ctx,
		cancel:     cancel,
		states:     make(map[simplex.ParentID]*stateFlight),
		finalized:  make(map[simplex.CandidateID]*finalizedState, len(stored.Finalized)),
		persisted:  make(map[simplex.CandidateID]struct{}, len(stored.Finalized)),
	}
	for _, id := range stored.Finalized {
		r.finalized[id] = &finalizedState{isDone: true}
		r.persisted[id] = struct{}{}
	}
	// A final certificate is itself durable consensus evidence. Include it in
	// the replay set even if the process crashed before MarkFinalized completed.
	for _, certificate := range recovery {
		id := certificate.Vote.ID
		if r.finalized[id] == nil {
			r.finalized[id] = &finalizedState{isDone: true}
		}
	}

	return r
}

func (r *stateResolver) start(ctx context.Context, start SessionStart) error {
	if len(start.Genesis) == 0 {
		return errors.New("validator runtime: session genesis is unavailable")
	}

	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()

		return ErrResolverClosed
	}
	if r.startAt != nil {
		matches := sessionStartEqual(*r.startAt, start)
		r.mu.Unlock()
		if !matches {
			return errors.New("validator runtime: state resolver start differs from recovery")
		}

		return nil
	}
	r.mu.Unlock()

	request := ChainStateRequest{
		Shard:          r.shard,
		Blocks:         start.Genesis,
		MinMasterchain: start.MinMasterchain,
	}
	data, err := r.backend.LoadChainState(ctx, request)
	if err != nil {
		return fmt.Errorf("validator runtime: load genesis chain state: %w", err)
	}
	genesis, err := newChainState(request, data)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()

		return ErrResolverClosed
	}
	if r.startAt != nil {
		matches := sessionStartEqual(*r.startAt, start)
		r.mu.Unlock()
		if !matches {
			return errors.New("validator runtime: state resolver start differs from recovery")
		}

		return nil
	}
	r.genesis = genesis
	storedStart := cloneSessionStart(start)
	r.startAt = &storedStart
	r.mu.Unlock()

	if err = r.reconcileAppliedRecovery(ctx); err != nil {
		return fmt.Errorf("validator runtime: reconcile applied finalizations: %w", err)
	}

	// The C++ validator awaits FinalizeBlock before its state resolver records
	// a candidate as done. Our node ingress is deliberately asynchronous, so a
	// crash can leave the consensus journal ahead of the node database. Replay
	// masterchain finalizations in slot order before Simplex starts: its normal
	// unordered certificate bootstrap may prune older slots after seeing the
	// newest certificate and therefore cannot repair the missing prefix itself.
	for _, certificate := range r.recovery {
		if err = r.finalizeWith(ctx, certificate.Vote.ID, certificate, nil); err != nil {
			return fmt.Errorf("validator runtime: replay masterchain finalization: %w", err)
		}
	}

	return nil
}

// reconcileAppliedRecovery finds the newest finalization which is durable in
// both the validator journal and the node's block store. A readable
// masterchain state implies that its whole predecessor chain was applied, so
// replay only needs to cover the crash-gap after that point. This keeps restart
// proportional to the missing tail instead of the lifetime of the session.
func (r *stateResolver) reconcileAppliedRecovery(ctx context.Context) error {
	if len(r.recovery) == 0 || len(r.persisted) == 0 {
		return nil
	}

	appliedThrough := -1
	var appliedState *ChainState
	var appliedStateID simplex.CandidateID
	var lastChecked *ton.BlockIDExt
	for i := len(r.recovery) - 1; i >= 0; i-- {
		id := r.recovery[i].Vote.ID
		if _, persisted := r.persisted[id]; !persisted {
			continue
		}

		resolution, err := r.candidates.resolve(ctx, id)
		if err != nil {
			return err
		}
		block := resolution.Candidate.Candidate.Block
		if lastChecked != nil && sameBlockID(block, *lastChecked) {
			continue
		}
		checked := *block.Copy()
		lastChecked = &checked

		request := ChainStateRequest{
			Shard:          r.shard,
			Blocks:         []ton.BlockIDExt{block},
			MinMasterchain: r.genesis.minMasterchain,
		}
		data, loadErr := r.backend.LoadChainState(ctx, request)
		if loadErr == nil {
			loaded, stateErr := newChainState(request, data)
			if stateErr != nil {
				return stateErr
			}
			if !resolution.Candidate.Candidate.Empty {
				appliedState = loaded
				appliedStateID = id
			}
			appliedThrough = i
			break
		}
		if !errors.Is(loadErr, ErrBlockNotReady) && !errors.Is(loadErr, context.DeadlineExceeded) {
			return loadErr
		}
	}
	if appliedThrough < 0 {
		return nil
	}

	r.mu.Lock()
	for i := 0; i <= appliedThrough; i++ {
		id := r.recovery[i].Vote.ID
		if _, persisted := r.persisted[id]; !persisted {
			continue
		}
		state := r.finalized[id]
		if state != nil {
			state.isDone = true
			state.reconciled = true
			state.applied = true
			if appliedState != nil && id == appliedStateID {
				state.appliedState = appliedState
			}
		}
	}
	r.mu.Unlock()

	return nil
}

func cloneSessionStart(start SessionStart) SessionStart {
	result := SessionStart{
		Genesis:        make([]ton.BlockIDExt, len(start.Genesis)),
		MinMasterchain: *start.MinMasterchain.Copy(),
	}
	for i := range start.Genesis {
		result.Genesis[i] = *start.Genesis[i].Copy()
	}

	return result
}

func (r *stateResolver) resolve(ctx context.Context, id simplex.ParentID) (ResolvedState, error) {
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()

		return ResolvedState{}, ErrResolverClosed
	}
	flight := r.states[id]
	if flight == nil {
		flight = &stateFlight{done: make(chan struct{})}
		r.states[id] = flight
		r.wg.Add(1)
		go r.resolveLoop(id, flight)
	}
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ResolvedState{}, ctx.Err()
	case <-flight.done:
		return flight.result, flight.err
	}
}

// stateCacheRetainedSlots is the margin of already-finalized parents kept
// resolved. Simplex prunes its own slot map at slot+1, so nothing below the
// finalized slot can become the parent of a new candidate; the margin only
// absorbs a leader window that opened just before the finalization it now
// trails, because re-resolving a released parent reloads a whole state from
// the node.
const stateCacheRetainedSlots = 4

// notifyFinalized releases resolved parent states consensus can no longer
// build on. Nothing else drops them before the session object itself is
// released, so a long catchain otherwise keeps every intermediate state
// version of the session reachable. An applied successor shares the unchanged
// subtrees of its parent, so an ordinary flight uniquely pins the superseded
// part of that state plus its block BOC; a finalized parent pins a whole
// separately loaded state tree, which is where the bulk of the bytes are.
//
// A flight that is still resolving is never removed. resolveLoop reconciles
// r.states[id] against its own flight only on the error path, so dropping an
// in-flight entry would let the next resolve start a second, concurrent
// ApplyMerkleUpdate over a full state.
func (r *stateResolver) notifyFinalized(slot uint32) {
	if slot < stateCacheRetainedSlots {
		return
	}
	watermark := slot - stateCacheRetainedSlots

	r.mu.Lock()
	for id, flight := range r.states {
		if !id.Exists || id.ID.Slot >= watermark {
			continue
		}
		select {
		case <-flight.done:
			delete(r.states, id)
		default:
		}
	}
	r.mu.Unlock()
}

// stateCacheStats is a debug projection of the session-scoped state cache.
// BlockBOCBytes covers only the block payloads of the retained tips: the
// applied state roots dominate the real footprint, but they are shared,
// immutable cell trees whose size cannot be taken without walking them, so
// Resolved — one distinct retained root each — is the growth figure that
// matters.
type stateCacheStats struct {
	States        int
	Resolved      int
	Finalized     int
	BlockBOCBytes int64
}

func (r *stateResolver) cacheStats() stateCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := stateCacheStats{States: len(r.states), Finalized: len(r.finalized)}
	for _, flight := range r.states {
		select {
		case <-flight.done:
		default:
			continue
		}
		stats.Resolved++
		if flight.result.State == nil {
			continue
		}
		for i := range flight.result.State.tips {
			stats.BlockBOCBytes += int64(len(flight.result.State.tips[i].BlockBOC))
		}
	}

	return stats
}

func (r *stateResolver) lineage(
	ctx context.Context,
	base simplex.ParentID,
	finalizedBlock *ton.BlockIDExt,
) (resolvedLineage, error) {
	lineage := make([]*CandidateArtifact, 0)
	matchedAnchor := finalizedBlock == nil
	appliedIndex := -1
	var appliedState *ChainState
	for base.Exists {
		resolution, err := r.candidates.resolve(ctx, base.ID)
		if err != nil {
			return resolvedLineage{}, err
		}
		artifact := resolution.Candidate
		if artifact == nil || artifact.Candidate.ID != base.ID {
			return resolvedLineage{}, errors.New("validator runtime: resolved lineage candidate differs from its id")
		}
		lineage = append(lineage, artifact)
		attemptedAppliedState := false
		var appliedStateErr error
		if appliedIndex < 0 && !artifact.Candidate.Empty &&
			(finalizedBlock == nil || artifact.Candidate.Block.SeqNo >= finalizedBlock.SeqNo) {
			attemptedAppliedState = true
			state, stateErr := r.appliedCandidateState(ctx, base.ID, artifact.Candidate.Block)
			appliedStateErr = stateErr
			if stateErr == nil {
				appliedIndex = len(lineage) - 1
				appliedState = state
			} else if !errors.Is(stateErr, errFinalizedCandidateNotApplied) &&
				!errors.Is(stateErr, ErrBlockNotReady) && !errors.Is(stateErr, context.DeadlineExceeded) {
				return resolvedLineage{}, stateErr
			}
		}
		if finalizedBlock != nil && sameBlockID(artifact.Candidate.Block, *finalizedBlock) {
			matchedAnchor = true
			if appliedIndex < 0 {
				if !attemptedAppliedState || errors.Is(appliedStateErr, errFinalizedCandidateNotApplied) {
					state, stateErr := r.finalizedAnchorState(ctx, base.ID, artifact.Candidate.Block)
					appliedState = state
					appliedStateErr = stateErr
				}
				if appliedStateErr != nil {
					return resolvedLineage{}, appliedStateErr
				}
				appliedIndex = len(lineage) - 1
			}
			break
		}
		base = artifact.Candidate.Parent
	}
	if !matchedAnchor {
		r.mu.Lock()
		genesis := r.genesis
		r.mu.Unlock()
		if genesis == nil {
			return resolvedLineage{}, errors.New("validator runtime: state resolver is not started")
		}
		for i := range genesis.tips {
			if sameBlockID(genesis.tips[i].ID, *finalizedBlock) {
				matchedAnchor = true
				break
			}
		}
		if !matchedAnchor {
			return resolvedLineage{}, errFinalizedLineageAhead
		}
	}
	var appliedAnchor *ton.BlockIDExt
	if appliedIndex >= 0 {
		anchor := *lineage[appliedIndex].Candidate.Block.Copy()
		appliedAnchor = &anchor
		lineage = lineage[:appliedIndex+1]
	}
	for left, right := 0, len(lineage)-1; left < right; left, right = left+1, right-1 {
		lineage[left], lineage[right] = lineage[right], lineage[left]
	}

	return resolvedLineage{
		Candidates:         lineage,
		AppliedAnchor:      appliedAnchor,
		AppliedAnchorState: appliedState,
	}, nil
}

func (r *stateResolver) appliedCandidateState(
	ctx context.Context,
	id simplex.CandidateID,
	block ton.BlockIDExt,
) (*ChainState, error) {
	r.mu.Lock()
	state := r.finalized[id]
	if state == nil || !state.isDone {
		r.mu.Unlock()

		return nil, errFinalizedCandidateNotApplied
	}
	if state.appliedState != nil {
		resolved := state.appliedState
		r.mu.Unlock()

		return resolved, nil
	}
	r.mu.Unlock()

	return r.loadAppliedCandidateState(ctx, id, block)
}

func (r *stateResolver) loadAppliedCandidateState(
	ctx context.Context,
	id simplex.CandidateID,
	block ton.BlockIDExt,
) (*ChainState, error) {
	r.mu.Lock()
	state := r.finalized[id]
	if state == nil || !state.isDone {
		r.mu.Unlock()

		return nil, errFinalizedCandidateNotApplied
	}
	if state.appliedState != nil {
		resolved := state.appliedState
		r.mu.Unlock()

		return resolved, nil
	}
	genesis := r.genesis
	r.mu.Unlock()
	if genesis == nil {
		return nil, errors.New("validator runtime: state resolver is not started")
	}

	request := ChainStateRequest{
		Shard:          r.shard,
		Blocks:         []ton.BlockIDExt{block},
		MinMasterchain: genesis.minMasterchain,
	}
	data, err := r.backend.LoadChainState(ctx, request)
	if err != nil {
		return nil, err
	}
	resolved, err := newChainState(request, data)
	if err != nil {
		return nil, err
	}
	if len(resolved.tips) != 1 || !sameBlockID(resolved.tips[0].ID, block) {
		return nil, errors.New("validator runtime: applied candidate state is not a normal anchor")
	}

	r.mu.Lock()
	state = r.finalized[id]
	if state != nil && state.isDone {
		state.applied = true
		if state.appliedState == nil {
			state.appliedState = resolved
		}
		resolved = state.appliedState
	}
	r.mu.Unlock()

	return resolved, nil
}

func (r *stateResolver) finalizedAnchorState(
	ctx context.Context,
	id simplex.CandidateID,
	block ton.BlockIDExt,
) (*ChainState, error) {
	r.mu.Lock()
	if r.anchor != nil && r.anchor.id == id && sameBlockID(r.anchor.block, block) {
		resolved := r.anchor.state
		r.mu.Unlock()

		return resolved, nil
	}
	genesis := r.genesis
	r.mu.Unlock()
	if genesis == nil {
		return nil, errors.New("validator runtime: state resolver is not started")
	}

	request := ChainStateRequest{
		Shard:          r.shard,
		Blocks:         []ton.BlockIDExt{block},
		MinMasterchain: genesis.minMasterchain,
	}
	data, err := r.backend.LoadChainState(ctx, request)
	if err != nil {
		return nil, err
	}
	resolved, err := newChainState(request, data)
	if err != nil {
		return nil, err
	}
	if len(resolved.tips) != 1 || !sameBlockID(resolved.tips[0].ID, block) {
		return nil, errors.New("validator runtime: finalized anchor state is not normal")
	}

	r.mu.Lock()
	if r.anchor != nil && r.anchor.id == id && sameBlockID(r.anchor.block, block) {
		resolved = r.anchor.state
	} else {
		r.anchor = &resolvedAnchorState{id: id, block: *block.Copy(), state: resolved}
	}
	if state := r.finalized[id]; state != nil && state.isDone && state.appliedState == nil {
		state.applied = true
		state.appliedState = resolved
	}
	r.mu.Unlock()

	return resolved, nil
}

func (r *stateResolver) resolveLoop(id simplex.ParentID, flight *stateFlight) {
	defer r.wg.Done()

	result, err := r.resolveInner(id)
	r.mu.Lock()
	flight.result = result
	flight.err = err
	if err != nil && r.states[id] == flight {
		delete(r.states, id)
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *stateResolver) resolveInner(id simplex.ParentID) (ResolvedState, error) {
	if !id.Exists {
		r.mu.Lock()
		genesis := r.genesis
		r.mu.Unlock()
		if genesis == nil {
			return ResolvedState{}, errors.New("validator runtime: state resolver is not started")
		}

		return ResolvedState{State: genesis}, nil
	}

	resolution, err := r.candidates.resolve(r.ctx, id.ID)
	if err != nil {
		return ResolvedState{}, err
	}
	artifact := resolution.Candidate
	if artifact.Candidate.Empty {
		return r.resolve(r.ctx, artifact.Candidate.Parent)
	}
	genUtime, err := candidateGenUtime(artifact.CollatedData)
	if err != nil {
		return ResolvedState{}, err
	}

	r.mu.Lock()
	isFinalized := r.finalized[id.ID] != nil && r.finalized[id.ID].isDone
	genesis := r.genesis
	r.mu.Unlock()
	if isFinalized {
		if genesis == nil {
			return ResolvedState{}, errors.New("validator runtime: state resolver is not started")
		}
		request := ChainStateRequest{
			Shard:          r.shard,
			Blocks:         []ton.BlockIDExt{artifact.Candidate.Block},
			MinMasterchain: genesis.minMasterchain,
		}
		data, loadErr := r.loadFinalizedChainState(request)
		if loadErr != nil {
			return ResolvedState{}, loadErr
		}
		state, loadErr := newChainState(request, data)
		if loadErr != nil {
			return ResolvedState{}, loadErr
		}
		r.rememberAppliedCandidateState(id.ID, state)

		return ResolvedState{State: state, GenUtime: genUtime}, nil
	}

	previous, err := r.resolve(r.ctx, artifact.Candidate.Parent)
	if err != nil {
		return ResolvedState{}, err
	}
	state, err := previous.State.apply(artifact)
	if err != nil {
		return ResolvedState{}, err
	}

	return ResolvedState{State: state, GenUtime: genUtime}, nil
}

func (r *stateResolver) rememberAppliedCandidateState(id simplex.CandidateID, resolved *ChainState) {
	r.mu.Lock()
	state := r.finalized[id]
	if state != nil && state.isDone {
		state.applied = true
		if state.appliedState == nil {
			state.appliedState = resolved
		}
	}
	r.mu.Unlock()
}

// loadFinalizedChainState waits for the node's normal apply pipeline to make
// an already-finalized block readable. SubmitBlockLocally deliberately only
// queues acceptance, so a final certificate can arrive before its state is in
// the live store. Treating that short gap as a fatal session error stops every
// validator in an otherwise healthy network.
func (r *stateResolver) loadFinalizedChainState(
	request ChainStateRequest,
) (ChainStateData, error) {
	for {
		data, err := r.backend.LoadChainState(r.ctx, request)
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, ErrBlockNotReady) && !errors.Is(err, context.DeadlineExceeded) {
			return ChainStateData{}, fmt.Errorf("validator runtime: load finalized chain state: %w", err)
		}
		if err = waitDuration(r.ctx, time.Second); err != nil {
			return ChainStateData{}, ErrResolverClosed
		}
	}
}

func candidateGenUtime(collatedData []byte) (time.Time, error) {
	roots, err := cell.FromBOCMultiRoot(collatedData)
	if err != nil {
		return time.Time{}, fmt.Errorf("validator runtime: decode collated data time: %w", err)
	}
	for _, root := range roots {
		loader, loadErr := root.BeginParse()
		if loadErr != nil || loader.BitsLeft() < 128 {
			continue
		}
		tag, tagErr := loader.LoadUInt(32)
		if tagErr != nil || tag != consensusExtraDataTag {
			continue
		}
		if _, loadErr = loader.LoadUInt(32); loadErr != nil {
			continue
		}
		milliseconds, timeErr := loader.LoadUInt(64)
		if timeErr != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
			continue
		}
		if milliseconds > uint64(^uint64(0)>>1) {
			return time.Time{}, errors.New("validator runtime: candidate generation time overflows int64")
		}

		return time.UnixMilli(int64(milliseconds)), nil
	}

	return time.Time{}, errors.New("validator runtime: candidate has no consensus extra data")
}

func (r *stateResolver) finalize(
	ctx context.Context,
	id simplex.CandidateID,
	certificate *simplex.Certificate,
) error {
	return r.finalizeWith(ctx, id, certificate, nil)
}

func (r *stateResolver) finalizeWith(
	ctx context.Context,
	id simplex.CandidateID,
	certificate *simplex.Certificate,
	certifiedCandidate *CandidateArtifact,
) error {
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()

		return ErrResolverClosed
	}
	state := r.finalized[id]
	if state == nil {
		state = &finalizedState{}
		r.finalized[id] = state
	}
	if state.isDone && state.reconciled {
		r.mu.Unlock()

		return nil
	}
	flight := state.inFlight
	if flight == nil {
		replay := state.isDone
		flight = &resolverFlight{done: make(chan struct{})}
		state.inFlight = flight
		r.wg.Add(1)
		go r.finalizeLoop(id, certificate, certifiedCandidate, replay, flight)
	}
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flight.done:
		return flight.err
	}
}

func (r *stateResolver) finalizeLoop(
	id simplex.CandidateID,
	certificate *simplex.Certificate,
	certifiedCandidate *CandidateArtifact,
	replay bool,
	flight *resolverFlight,
) {
	defer r.wg.Done()

	err := r.finalizeInner(id, certificate, certifiedCandidate, replay)
	r.mu.Lock()
	flight.err = err
	state := r.finalized[id]
	if state != nil && state.inFlight == flight {
		if err == nil {
			state.isDone = true
			state.reconciled = true
			state.inFlight = nil
		} else if replay {
			// The durable finalization remains authoritative. Keep it so state
			// resolution never falls back to applying a Merkle update over an
			// already-final block; a repeated notification can retry ingress.
			state.inFlight = nil
		} else {
			delete(r.finalized, id)
		}
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *stateResolver) finalizeInner(
	id simplex.CandidateID,
	finalCertificate *simplex.Certificate,
	certifiedCandidate *CandidateArtifact,
	replay bool,
) error {
	if finalCertificate == nil && r.shard.IsMasterchain() {
		return nil
	}

	resolution, err := r.candidates.resolve(r.ctx, id)
	if err != nil {
		return err
	}
	artifact := resolution.Candidate
	if finalCertificate != nil && certifiedCandidate == nil {
		if finalCertificate.Vote != simplex.FinalizeVote(id) {
			return errors.New("validator runtime: finalization vote mismatch")
		}
		certifiedCandidate = artifact
	}

	if artifact.Candidate.Empty {
		if artifact.Candidate.Parent.Exists {
			if err = r.finalizeWith(
				r.ctx,
				artifact.Candidate.Parent.ID,
				finalCertificate,
				certifiedCandidate,
			); err != nil {
				return err
			}
		}
	} else {
		if artifact.Candidate.Parent.Exists {
			if err = r.finalizeWith(r.ctx, artifact.Candidate.Parent.ID, nil, nil); err != nil {
				return err
			}
		}

		certificate := resolution.Notarization
		certified := artifact
		if finalCertificate != nil {
			certificate = finalCertificate
			certified = certifiedCandidate
		}
		if err = r.acceptBlock(BlockAcceptance{
			Candidate:          artifact,
			Certificate:        certificate,
			CertifiedCandidate: certified,
			Replay:             replay,
		}); err != nil {
			return err
		}
		if replay && r.shard.IsMasterchain() {
			if err = r.waitReplayApplied(artifact.Candidate.Block); err != nil {
				return err
			}
		}
	}

	// The wait follows r.ctx so close() stays bounded by its own cancellation
	// instead of by storage always firing this callback. A cancelled wait
	// leaves the marker possibly committed, which is exactly what recovery
	// expects: finalization is replayed from the durable record.
	if err = awaitStorageWrite(r.ctx, func(done func(error)) {
		r.storage.MarkFinalized(r.storageID, id, done)
	}); err != nil {
		return fmt.Errorf("validator runtime: mark candidate finalized: %w", err)
	}

	return nil
}

// waitReplayApplied preserves the ordering guaranteed by the C++
// StateResolver, where FinalizeBlock completes before the candidate is marked
// done. Normal Go acceptance remains asynchronous; only crash recovery waits,
// because submitting a later masterchain block before its predecessor reaches
// the live store can leave an unrecoverable hole when every validator restarts
// from the same consensus journal.
func (r *stateResolver) waitReplayApplied(block ton.BlockIDExt) error {
	r.mu.Lock()
	genesis := r.genesis
	r.mu.Unlock()
	if genesis == nil {
		return errors.New("validator runtime: state resolver is not started")
	}

	request := ChainStateRequest{
		Shard:          r.shard,
		Blocks:         []ton.BlockIDExt{block},
		MinMasterchain: genesis.minMasterchain,
	}
	data, err := r.loadFinalizedChainState(request)
	if err != nil {
		return err
	}
	if _, err = newChainState(request, data); err != nil {
		return fmt.Errorf("validator runtime: load replayed chain state: %w", err)
	}

	return nil
}

func (r *stateResolver) acceptBlock(acceptance BlockAcceptance) error {
	for {
		err := r.backend.AcceptBlock(r.ctx, acceptance)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrBlockNotReady) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("validator runtime: accept finalized block: %w", err)
		}
		acceptance.Retry = true
		if err = waitDuration(r.ctx, time.Second); err != nil {
			return ErrResolverClosed
		}
	}
}

func (r *stateResolver) close() {
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()
		r.wg.Wait()

		return
	}
	r.isClosed = true
	r.cancel()
	r.mu.Unlock()

	r.wg.Wait()
}
