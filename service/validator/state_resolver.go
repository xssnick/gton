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
	// Walk is what the walk cost, which is not derivable from the result: the
	// returned lineage is trimmed at the applied anchor, while the walk visited
	// everything down to it and paid per step.
	Walk lineageWalkStats
}

// lineageWalkStats is one walk's depth and the residency of each step.
type lineageWalkStats struct {
	Visited int
	Steps   [lineageStepSourceCount]int
}

type stateResolver struct {
	shard      groups.ShardID
	storageID  SessionStorageID
	storage    ValidatorStorage
	backend    SessionBackend
	candidates *candidateResolver
	recovery   []simplex.VerifiedCertificate
	// slotsPerLeaderWindow is a critical session parameter and never changes for
	// the life of this resolver. targetRate is noncritical and does, through
	// updateParams; both retention margins here are derived from them rather
	// than written as slot literals.
	slotsPerLeaderWindow uint32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu sync.Mutex
	// targetRate is the noncritical slot rate the wall-clock retention margins
	// are converted against. It is read under mu because updateParams writes it.
	targetRate            time.Duration
	resolveTimeoutCap     time.Duration
	maxLeaderWindowDesync uint32
	genesis               *ChainState
	startAt               *SessionStart
	anchor                *resolvedAnchorState
	states                map[simplex.ParentID]*stateFlight
	finalized             map[simplex.CandidateID]*finalizedState
	// applied holds exactly the finalization markers still carrying a loaded
	// state. finalized itself is never pruned — its markers are what keep an
	// already-final block from being re-applied through a Merkle update — so the
	// release sweep is driven from here instead, and costs what it retains.
	applied   map[simplex.CandidateID]*finalizedState
	persisted map[simplex.CandidateID]struct{}
	// lineageFloor is the oldest slot a leader window's lineage walk has had to
	// reach, and lineageFloorKnown says whether any walk has happened at all. It
	// is the producer stating its own retention requirement: the walk descends to
	// the masterchain-visible finalized anchor, and both sweeps must leave
	// everything from there up alone. Its monotone maximum is what frees the
	// slots the anchor has moved past.
	lineageFloor      uint32
	lineageFloorKnown bool
	isClosed          bool
}

type stateFlight struct {
	done      chan struct{}
	cancel    context.CancelFunc
	result    ResolvedState
	err       error
	waiters   int
	finished  bool
	cancelErr error
	expires   time.Time
	timer     *time.Timer
}

type finalizedState struct {
	isDone     bool
	reconciled bool
	// appliedState is the exact immutable state loaded from the ordinary node
	// store. Keeping it with the finalization marker lets the in-process
	// collator restore the same anchor without a second storage read.
	//
	// It is a whole separately loaded state tree, so it is kept only while a
	// lineage walk can still reach this slot: past that, notifyFinalized drops
	// it and loadAppliedCandidateState reloads on demand. The marker beside it
	// never goes away.
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
	recovery []simplex.VerifiedCertificate,
	params simplex.Params,
	slotsPerLeaderWindow uint32,
) *stateResolver {
	ctx, cancel := context.WithCancel(context.Background())
	r := &stateResolver{
		shard:                 shard,
		storageID:             storageID,
		storage:               storage,
		backend:               backend,
		candidates:            candidates,
		recovery:              recovery,
		slotsPerLeaderWindow:  slotsPerLeaderWindow,
		targetRate:            params.TargetRate,
		resolveTimeoutCap:     params.CandidateResolveTimeoutCap,
		maxLeaderWindowDesync: params.MaxLeaderWindowDesync,
		ctx:                   ctx,
		cancel:                cancel,
		states:                make(map[simplex.ParentID]*stateFlight),
		finalized:             make(map[simplex.CandidateID]*finalizedState, len(stored.Finalized)),
		applied:               make(map[simplex.CandidateID]*finalizedState),
		persisted:             make(map[simplex.CandidateID]struct{}, len(stored.Finalized)),
	}
	for _, id := range stored.Finalized {
		r.finalized[id] = &finalizedState{isDone: true}
		r.persisted[id] = struct{}{}
	}
	// A final certificate is authenticated consensus evidence. Include it in
	// the replay set even if the process crashed before MarkFinalized completed.
	for _, certificate := range recovery {
		id := certificate.Vote().ID
		if r.finalized[id] == nil {
			r.finalized[id] = &finalizedState{isDone: true}
		}
	}

	return r
}

// updateParams installs a new noncritical parameter set. Only the slot rate
// matters here, and only because the retention margins are wall-clock budgets
// converted against it.
func (r *stateResolver) updateParams(params simplex.Params) {
	r.mu.Lock()
	r.targetRate = params.TargetRate
	r.resolveTimeoutCap = params.CandidateResolveTimeoutCap
	r.maxLeaderWindowDesync = params.MaxLeaderWindowDesync
	r.mu.Unlock()
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
		if err = r.finalizeWith(ctx, certificate.Vote().ID, certificate, nil); err != nil {
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
		id := r.recovery[i].Vote().ID
		if _, persisted := r.persisted[id]; !persisted {
			continue
		}

		resolution, err := r.candidates.resolveFinalization(ctx, id)
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
		id := r.recovery[i].Vote().ID
		if _, persisted := r.persisted[id]; !persisted {
			continue
		}
		state := r.finalized[id]
		if state != nil {
			state.isDone = true
			state.reconciled = true
			if appliedState != nil && id == appliedStateID {
				r.rememberAppliedStateLocked(id, state, appliedState)
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
	for {
		r.mu.Lock()
		if r.isClosed {
			r.mu.Unlock()

			return ResolvedState{}, ErrResolverClosed
		}
		flight := r.states[id]
		if flight != nil {
			select {
			case <-flight.done:
				result, err := flight.result, flight.err
				flight.finished = true
				if flight.timer != nil {
					flight.timer.Stop()
					flight.timer = nil
				}
				r.mu.Unlock()

				return result, err
			default:
			}
			if flight.cancelErr != nil {
				done := flight.done
				r.mu.Unlock()
				select {
				case <-ctx.Done():
					return ResolvedState{}, ctx.Err()
				case <-done:
					continue
				}
			}
		}
		if flight == nil {
			flightCtx, cancel := context.WithCancel(r.ctx)
			flight = &stateFlight{done: make(chan struct{}), cancel: cancel}
			r.states[id] = flight
			r.armStateExpiryLocked(id, flight)
			r.wg.Add(1)
			go r.resolveLoop(flightCtx, id, flight)
		}
		flight.waiters++
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			r.releaseStateWaiter(id, flight)

			return ResolvedState{}, ctx.Err()
		case <-flight.done:
			r.releaseStateWaiter(id, flight)

			return flight.result, flight.err
		}
	}
}

func (r *stateResolver) armStateExpiryLocked(id simplex.ParentID, flight *stateFlight) {
	if flight.expires.IsZero() {
		params := simplex.Params{
			TargetRate:                 r.targetRate,
			CandidateResolveTimeoutCap: r.resolveTimeoutCap,
			MaxLeaderWindowDesync:      r.maxLeaderWindowDesync,
		}
		flight.expires = time.Now().Add(resolverFlightTTL(params, r.slotsPerLeaderWindow))
	}
	delay := time.Until(flight.expires)
	if delay <= 0 {
		delay = time.Nanosecond
	}
	flight.timer = time.AfterFunc(delay, func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		flight.timer = nil
		if r.states[id] != flight || flight.finished {
			return
		}
		flight.cancelErr = context.DeadlineExceeded
		flight.cancel()
	})
}

func (r *stateResolver) releaseStateWaiter(id simplex.ParentID, flight *stateFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if flight.waiters > 0 {
		flight.waiters--
	}
	if flight.waiters != 0 || r.states[id] != flight || flight.finished {
		return
	}
	if flight.timer != nil {
		flight.timer.Stop()
		flight.timer = nil
	}
	flight.cancelErr = context.Canceled
	flight.cancel()
}

func (r *stateResolver) finishStateFlightLocked(
	id simplex.ParentID,
	flight *stateFlight,
	result ResolvedState,
	err error,
	cache bool,
) {
	if flight.finished {
		return
	}

	flight.finished = true
	flight.result = result
	flight.err = err
	if flight.timer != nil {
		flight.timer.Stop()
		flight.timer = nil
	}
	if !cache && r.states[id] == flight {
		delete(r.states, id)
	}
	close(flight.done)
	if flight.cancel != nil {
		flight.cancel()
	}
}

// rememberValidatedState makes an exact post-validation state available to
// descendants before notarization, finalization, or the ordinary node apply
// pipeline. If a finalized-state store lookup won the race and is already
// polling, this result completes the same flight and cancels the redundant
// lookup without replacing the identity seen by its waiters.
func (r *stateResolver) rememberValidatedState(id simplex.CandidateID, resolved ResolvedState) {
	parent := simplex.Parent(id)

	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()

		return
	}
	flight := r.states[parent]
	if flight == nil {
		done := make(chan struct{})
		close(done)
		r.states[parent] = &stateFlight{done: done, result: resolved, finished: true}
		r.mu.Unlock()

		return
	}
	select {
	case <-flight.done:
		r.mu.Unlock()

		return
	default:
	}
	r.finishStateFlightLocked(parent, flight, resolved, nil, true)
	r.mu.Unlock()
}

// stateRetainedSlots is the margin of already-finalized parents kept resolved.
// Simplex prunes its own slot map at slot+1, so nothing below the finalized
// slot can become the parent of a new candidate; the margin only absorbs a
// leader window that opened just before the finalization it now trails, because
// re-resolving a released parent reloads a whole state from the node.
//
// A leader window is SlotsPerLeaderWindow slots, which is configured per
// session, so the margin is derived from that value rather than from the four
// slots this network happens to use.
func (r *stateResolver) stateRetainedSlots() uint32 {
	return stateRetainedSlots(r.slotsPerLeaderWindow)
}

// finalizedStateRetainedSlots is the margin of applied finalized states kept in
// memory. It matches the candidate payload margin deliberately, because the
// same lineage walk consumes both: a walk that still finds its candidates
// resident still finds their states, and one that has gone far enough back to
// reload the bytes reloads the state with them. The anchor of the current
// leader window is not in this set — resolvedAnchorState is separate for
// exactly that reason.
func (r *stateResolver) finalizedStateRetainedSlots() uint32 {
	return candidateRetainedSlots(r.targetRate)
}

// retentionFloorNone is the floor of a session no consumer constrains: nothing
// has walked a lineage here yet, so the fixed margins are the whole policy.
const retentionFloorNone = ^uint32(0)

// retentionFloor is the oldest slot the finalization sweeps must not release
// below, together with the producer's own request and whether the retention
// budget is what overrode it.
//
// Capped is not a statement about how far behind this node is. It says the
// retained payloads between the producer's anchor and the tip no longer fit the
// session's memory budget, which is the only reason to stop honouring a request
// that costs memory.
type retentionFloor struct {
	Slot   uint32
	Capped bool
	// Anchor is where the last completed lineage walk stopped, and AnchorKnown
	// whether any walk has completed at all. It is reported so the distance
	// between it and the finalized slot stays visible without being what the
	// sweeps are bounded by.
	Anchor      uint32
	AnchorKnown bool
}

// notifyFinalized releases resolved parent states consensus can no longer
// build on, and the applied states of finalizations it has left behind.
// Nothing else drops either before the session object itself is released, so a
// long catchain otherwise keeps every intermediate state version of the session
// reachable. An applied successor shares the unchanged subtrees of its parent,
// so an ordinary flight uniquely pins the superseded part of that state plus
// its block BOC; a finalized parent pins a whole separately loaded state tree,
// which is where the bulk of the bytes are.
//
// A flight with an active waiter is never removed by this sweep. The last
// waiter cancels its flight; the loop removes it only after the current
// ApplyMerkleUpdate returns, and a later caller waits for that cleanup before
// restarting. That keeps the non-context-aware cell apply single-flight.
//
// It returns the floor both sweeps ran under, because the candidate payloads
// have the same consumer and must be released against the same bound.
//
// budgetFloor is the oldest slot the session's retention budget still allows,
// computed from the retained payloads themselves by candidateResolver.
// retentionCapFloor. It is passed in rather than read here so neither resolver
// takes the other's lock.
func (r *stateResolver) notifyFinalized(slot uint32, budgetFloor uint32) retentionFloor {
	r.mu.Lock()
	defer r.mu.Unlock()

	floor := r.retentionFloorLocked(slot, budgetFloor)
	if margin := r.stateRetainedSlots(); slot >= margin {
		watermark := slot - margin
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
	}
	stateMargin := r.finalizedStateRetainedSlots()
	if slot < stateMargin {
		return floor
	}

	// The finalization marker stays; only the state tree behind it goes. A
	// finalization still being applied keeps its own, and is reconsidered at the
	// next finalization. The producer's floor bounds this exactly as it bounds
	// the candidate payloads: the state the next leader window anchors on is the
	// one at the bottom of the same walk, and reloading it is a whole ChainState
	// out of the node backend.
	watermark := min(slot-stateMargin, floor.Slot)
	for id, state := range r.applied {
		if id.Slot >= watermark || state.inFlight != nil {
			continue
		}
		state.appliedState = nil
		delete(r.applied, id)
	}

	return floor
}

// retentionFloorLocked reports what the local producer still needs, bounded by
// the session's retention budget.
//
// The requirement is stated by the consumer rather than guessed at: a lineage
// walk reports where it stopped, which is the masterchain-visible finalized
// anchor and therefore the oldest slot the next one can be asked to reach. A
// session that has never walked one constrains nothing, which is what keeps an
// observer — or any session with no local producer — on the fixed margins.
//
// What bounds that requirement is budgetFloor, and it is a measurement of the
// retained payloads rather than a slot distance. The distinction is the whole
// point: this bound used to be slot - 64, so a session that skipped 64 slots
// gave up on its producer's lineage while holding nothing at all — skips are
// slots that never had a block and never will, and charging them as production
// lag turned a stalled session into one that also had to read its lineage back
// from storage. Slots the session never produced in are now free, because they
// cost nothing.
func (r *stateResolver) retentionFloorLocked(slot uint32, budgetFloor uint32) retentionFloor {
	if !r.lineageFloorKnown {
		return retentionFloor{Slot: retentionFloorNone}
	}

	floor := retentionFloor{Slot: r.lineageFloor, Anchor: r.lineageFloor, AnchorKnown: true}
	if budgetFloor > r.lineageFloor && budgetFloor <= slot {
		floor.Slot = budgetFloor
		floor.Capped = true
	}

	return floor
}

// noteLineageFloorLocked records how far back a completed lineage walk reached.
// It only ever moves forward: the anchor advances as the node applies, and the
// slots it has moved past are the ones the sweeps are free to release again.
func (r *stateResolver) noteLineageFloorLocked(slot uint32) {
	if !r.lineageFloorKnown || slot > r.lineageFloor {
		r.lineageFloor = slot
		r.lineageFloorKnown = true
	}
}

// lineageAnchor is where the last completed leader-window walk stopped, as of
// right now. Lineage walks run per leader window and keep running through a
// standstill — they are what a stalled session spends its time on — so this
// advances between finalizations, and reading it from the last finalization
// instead would freeze it for exactly as long as the session is stuck.
func (r *stateResolver) lineageAnchor() (uint32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lineageFloor, r.lineageFloorKnown
}

// stateCacheStats is a debug projection of the session-scoped state cache.
//
// BlockBOCBytes is the one figure here that is bytes, and it is a floor rather
// than a measurement: it counts the block payloads of the retained tips and
// nothing else. Two much larger things sit beside every one of those payloads
// and cannot be sized without walking them — the applied state root, and the
// parsed block DAG each tip pins through ChainTip.Block, which is typically
// several times the payload it was decoded from. Both are shared immutable cell
// trees, so Resolved — one distinct retained tip set each — is the growth
// figure that matters, and the byte count is only useful for spotting a retained
// count that stops matching the payload sizes.
//
// AppliedStates counts the same kind of root held beside a finalization
// marker. It is the figure that turns from a margin into a leak if a path that
// stores one ever escapes the release sweep, which is why it is reported
// separately from the finalization markers themselves.
type stateCacheStats struct {
	States        int
	Resolved      int
	Finalized     int
	AppliedStates int
	BlockBOCBytes int64
}

func (r *stateResolver) cacheStats() stateCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := stateCacheStats{
		States:        len(r.states),
		Finalized:     len(r.finalized),
		AppliedStates: len(r.applied),
	}
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

// lineage walks back from base to the applied anchor.
//
// Every return carries Walk, including the error ones. The walk stats are the
// only account of what a failed walk cost — how deep it got, and how many of
// those steps had to leave memory — and the caller's failure observation exists
// for exactly that case: an abstain that came from a lineage that could not be
// assembled. Returning a zero resolvedLineage alongside the error dropped the
// depth and the step sources on the floor and left the instrumentation dead on
// the only path it was added for. Nothing else on an error return is meaningful
// and no caller reads it; Walk is, and callers must be able to.
func (r *stateResolver) lineage(
	ctx context.Context,
	base simplex.ParentID,
	finalizedBlock *ton.BlockIDExt,
) (resolvedLineage, error) {
	lineage := make([]*CandidateArtifact, 0)
	matchedAnchor := finalizedBlock == nil
	appliedIndex := -1
	var walk lineageWalkStats
	var appliedState *ChainState
	for base.Exists {
		walk.Visited++
		walk.Steps[r.candidates.payloadResidency(base.ID)]++
		resolution, err := r.candidates.resolve(ctx, base.ID)
		if err != nil {
			return resolvedLineage{Walk: walk}, err
		}
		artifact := resolution.Candidate
		if artifact == nil || artifact.Candidate.ID != base.ID {
			return resolvedLineage{Walk: walk}, errors.New("validator runtime: resolved lineage candidate differs from its id")
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
				return resolvedLineage{Walk: walk}, stateErr
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
					return resolvedLineage{Walk: walk}, appliedStateErr
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
			return resolvedLineage{Walk: walk}, errors.New("validator runtime: state resolver is not started")
		}
		for i := range genesis.tips {
			if sameBlockID(genesis.tips[i].ID, *finalizedBlock) {
				matchedAnchor = true
				break
			}
		}
		if !matchedAnchor {
			return resolvedLineage{Walk: walk}, errFinalizedLineageAhead
		}
	}
	// The walk is the consumer both retention sweeps exist for, so it states what
	// it needed as soon as it is known to have succeeded: everything from the
	// slot it stopped at up to the tip, which is the whole visited range and not
	// only the part trimmed below. Nothing here depends on the trimming, because
	// a walk that reads a candidate needed that candidate whether or not it ends
	// up handing it to the producer.
	if len(lineage) > 0 {
		r.mu.Lock()
		r.noteLineageFloorLocked(lineage[len(lineage)-1].Candidate.ID.Slot)
		r.mu.Unlock()
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
		Walk:               walk,
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
		r.rememberAppliedStateLocked(id, state, resolved)
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
		r.rememberAppliedStateLocked(id, state, resolved)
	}
	r.mu.Unlock()

	return resolved, nil
}

func (r *stateResolver) resolveLoop(
	ctx context.Context,
	id simplex.ParentID,
	flight *stateFlight,
) {
	defer r.wg.Done()

	result, err := r.resolveInner(ctx, id)
	r.mu.Lock()
	if flight.finished {
		r.mu.Unlock()

		return
	}
	if flight.cancelErr != nil {
		err = flight.cancelErr
	}
	r.finishStateFlightLocked(id, flight, result, err, err == nil)
	r.mu.Unlock()
}

func (r *stateResolver) resolveInner(ctx context.Context, id simplex.ParentID) (ResolvedState, error) {
	if !id.Exists {
		r.mu.Lock()
		genesis := r.genesis
		r.mu.Unlock()
		if genesis == nil {
			return ResolvedState{}, errors.New("validator runtime: state resolver is not started")
		}

		return ResolvedState{State: genesis}, nil
	}

	resolution, err := r.candidates.resolve(ctx, id.ID)
	if err != nil {
		return ResolvedState{}, err
	}
	artifact := resolution.Candidate
	if artifact.Candidate.Empty {
		return r.resolve(ctx, artifact.Candidate.Parent)
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
		// The state this session may already hold for this exact block, before any
		// store read. Two things were wrong with going straight to the load:
		//
		//   - it re-read a state that was already in memory, and when the store
		//     could not answer it waited for one it was holding;
		//   - it returned the freshly built state rather than the one already
		//     cached under this marker, so one parent could end up with two
		//     materializations. chain_state.go compares tip states BY POINTER for
		//     the live-successor carry-back, so the second one silently costs a
		//     full re-apply per candidate — with no error and no metric.
		//
		// Both are fixed by asking first and by returning the canonical object.
		if state := r.residentAppliedState(id.ID, artifact.Candidate.Block); state != nil {
			return ResolvedState{State: state, GenUtime: genUtime}, nil
		}
		request := ChainStateRequest{
			Shard:          r.shard,
			Blocks:         []ton.BlockIDExt{artifact.Candidate.Block},
			MinMasterchain: genesis.minMasterchain,
		}
		data, loadErr := r.loadFinalizedChainState(ctx, request)
		if loadErr != nil {
			return ResolvedState{}, loadErr
		}
		state, loadErr := newChainState(request, data)
		if loadErr != nil {
			return ResolvedState{}, loadErr
		}

		return ResolvedState{State: r.rememberAppliedCandidateState(id.ID, state), GenUtime: genUtime}, nil
	}

	previous, err := r.resolve(ctx, artifact.Candidate.Parent)
	if err != nil {
		return ResolvedState{}, err
	}
	state, err := previous.State.apply(artifact)
	if err != nil {
		return ResolvedState{}, err
	}

	return ResolvedState{State: state, GenUtime: genUtime}, nil
}

// residentAppliedState returns the applied state this session already holds for
// one finalized candidate, or nil. It never reads anything: it is the question
// "do we already have this" asked before the read that would answer it again.
//
// The tip is checked against the block the caller is resolving. The marker is
// keyed by candidate id and only ever carries that candidate's own state, so the
// check cannot fail today; it is here because a mismatch would hand out a state
// for another block, which is not a failure worth discovering later.
func (r *stateResolver) residentAppliedState(
	id simplex.CandidateID,
	block ton.BlockIDExt,
) *ChainState {
	r.mu.Lock()
	var resolved *ChainState
	if state := r.finalized[id]; state != nil && state.isDone && state.appliedState != nil {
		resolved = state.appliedState
		r.applied[id] = state
	}
	r.mu.Unlock()
	if resolved == nil {
		return nil
	}
	if tip, err := resolved.NormalBlock(); err != nil || !sameBlockID(tip, block) {
		return nil
	}

	return resolved
}

// rememberAppliedCandidateState caches one loaded state under its finalization
// marker and returns the CANONICAL state for that marker — the one already
// cached when a concurrent resolve won the race, and the argument otherwise.
// Callers must use the returned value: handing out a state the marker did not
// adopt is how one block ends up with two materializations, which
// chain_state.go's pointer comparison turns into a silent full re-apply.
func (r *stateResolver) rememberAppliedCandidateState(
	id simplex.CandidateID,
	resolved *ChainState,
) *ChainState {
	r.mu.Lock()
	if state := r.finalized[id]; state != nil && state.isDone {
		r.rememberAppliedStateLocked(id, state, resolved)
		resolved = state.appliedState
	}
	r.mu.Unlock()

	return resolved
}

// rememberAppliedStateLocked keeps one loaded state with its finalization
// marker and queues it for release. Every assignment to appliedState goes
// through here: the queue is what keeps the release sweep proportional to what
// is retained rather than to every candidate this session has finalized.
func (r *stateResolver) rememberAppliedStateLocked(
	id simplex.CandidateID,
	state *finalizedState,
	resolved *ChainState,
) {
	if state.appliedState == nil {
		state.appliedState = resolved
	}
	r.applied[id] = state
}

// loadFinalizedChainState reads an already-finalized block's state, tolerating
// the gap between the final certificate and the block becoming readable.
// SubmitBlockLocally deliberately only queues acceptance, so that gap exists by
// design and treating it as a fatal session error stops every validator in an
// otherwise healthy network.
//
// THE WAIT IS NOT HERE ANY MORE, and this loop is no longer a poll. A local
// backend blocks inside its own read until a publication edge tells it to look
// again — see LocalSessionBackend.loadChainTip — and it returns not-ready only
// after its backstop has fired and complained. The interval below is therefore
// what paces a backend that does not wait at all, which is the only case left
// where returning instantly in a tight loop would spin. On the real path it is
// reached at most once per backstop, so the 1 Hz that used to be the whole
// mechanism is now the fallback of a fallback.
//
// Two callers, two different bounds, and both are deliberate: resolveInner
// runs under the shared resolve flight, bounded by its waiters and the session
// horizon TTL, while waitReplayApplied runs under r.ctx during crash replay,
// which is exact parity with the reference's masterchain finalize ordering.
func (r *stateResolver) loadFinalizedChainState(
	ctx context.Context,
	request ChainStateRequest,
) (ChainStateData, error) {
	// This is the caller that exists in order to wait, so it asks the backend to
	// wait rather than asking it again every second.
	request.Wait = true
	for {
		data, err := r.backend.LoadChainState(ctx, request)
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, ErrBlockNotReady) && !errors.Is(err, context.DeadlineExceeded) {
			return ChainStateData{}, fmt.Errorf("validator runtime: load finalized chain state: %w", err)
		}
		if err = waitDuration(ctx, time.Second); err != nil {
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
	certificate simplex.VerifiedCertificate,
) error {
	return r.finalizeWith(ctx, id, certificate, nil)
}

func (r *stateResolver) finalizeWith(
	ctx context.Context,
	id simplex.CandidateID,
	certificate simplex.VerifiedCertificate,
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
	certificate simplex.VerifiedCertificate,
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
			delete(r.applied, id)
		}
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *stateResolver) finalizeInner(
	id simplex.CandidateID,
	finalCertificate simplex.VerifiedCertificate,
	certifiedCandidate *CandidateArtifact,
	replay bool,
) error {
	if finalCertificate.IsZero() && r.shard.IsMasterchain() {
		return nil
	}

	resolution, err := r.candidates.resolveFinalization(r.ctx, id)
	if err != nil {
		return err
	}
	artifact := resolution.Candidate
	if !finalCertificate.IsZero() && certifiedCandidate == nil {
		if finalCertificate.Vote() != simplex.FinalizeVote(id) {
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
			if err = r.finalizeWith(
				r.ctx,
				artifact.Candidate.Parent.ID,
				simplex.VerifiedCertificate{},
				nil,
			); err != nil {
				return err
			}
		}

		certificate := resolution.Notarization
		certified := artifact
		if !finalCertificate.IsZero() {
			certificate = finalCertificate
			certified = certifiedCandidate
		}
		if err = r.acceptBlock(BlockAcceptance{
			Candidate:          artifact,
			Certificate:        certificate,
			CertifiedCandidate: certified,
			Replay:             replay,
			state:              r.acceptedCandidateState(id, artifact.Candidate.Block),
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
	data, err := r.loadFinalizedChainState(r.ctx, request)
	if err != nil {
		return err
	}
	if _, err = newChainState(request, data); err != nil {
		return fmt.Errorf("validator runtime: load replayed chain state: %w", err)
	}

	return nil
}

// acceptedCandidateState returns the state this session already holds for the
// block being accepted, so acceptance can publish it into the live view.
//
// It reads what is already there and computes nothing. The state of candidate id
// is the resolved state of the parent relation "my parent is id", which is where
// rememberValidatedState installs it after a successful validation, and where the
// applied-state cache keeps it after a load. Both are the same immutable object
// every other reader of this slot got, so publishing it cannot introduce a second
// materialization of one block.
//
// Nil is the ordinary answer in three cases, none of them an error: a replayed
// finalization after a restart (nothing was validated in this process), a
// finalization whose flight is still resolving, and a state the retention margins
// have already released. In all three the reader falls back to the wait, which is
// what happens today for every block.
func (r *stateResolver) acceptedCandidateState(
	id simplex.CandidateID,
	block ton.BlockIDExt,
) *ChainState {
	r.mu.Lock()
	var resolved *ChainState
	if state := r.finalized[id]; state != nil && state.appliedState != nil {
		resolved = state.appliedState
	} else if flight := r.states[simplex.Parent(id)]; flight != nil {
		select {
		case <-flight.done:
			if flight.err == nil {
				resolved = flight.result.State
			}
		default:
		}
	}
	r.mu.Unlock()
	if resolved == nil {
		return nil
	}
	// The tip has to be this exact block. An empty-candidate chain resolves the
	// same parent relation for several slots, so the state under this key is not
	// necessarily the successor of THIS block.
	if tip, err := resolved.NormalBlock(); err != nil || !sameBlockID(tip, block) {
		return nil
	}

	return resolved
}

// acceptBlock hands one finalized block to the node, retrying at 1 Hz for as long
// as the session lives.
//
// THIS POLL IS DELIBERATE, and it is the one that stays. It is parity with
// cppnode/ton/validator/consensus/bridge.cpp:69, where accept_block retries after
// coro_sleep(Timestamp::in(1.0)) on timeout or notready, forever, with the
// broadcast modes cleared on the second attempt — the same shape as
// acceptance.Retry here. The 1 Hz polls the change around it removed were reads of
// state this node itself had published, where the thing being waited for raises an
// edge; this one waits on the node's whole apply pipeline accepting a block, which
// publishes no such edge and whose failure modes are not ours to interpret. There
// is nothing to subscribe to, and the reference paces it exactly this way.
//
// Anyone tidying this into an edge-triggered wait would have to invent the edge,
// and would be diverging from the reference to do it. The retry above it in
// loadFinalizedChainState is the converted one; this is not that.
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
