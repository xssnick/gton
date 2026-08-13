package groups

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const futureShardHorizon = 60 * time.Second

var (
	ErrNoSnapshot                  = errors.New("validator groups snapshot is not initialized")
	ErrStaleMasterchainState       = errors.New("stale masterchain state")
	ErrConflictingMasterchainState = errors.New("conflicting masterchain state")
	ErrTrackerAlreadyBootstrapped  = errors.New("validator group tracker is already bootstrapped")
)

// UnsafeRotationRule describes one local emergency catchain rotation. It is
// explicit because this value is not stored on-chain.
type UnsafeRotationRule struct {
	CatchainSeqno        uint32
	FromMasterchainSeqno uint32
	ID                   uint32
}

// TrackerOptions contains local policy and the previous-rotation bootstrap
// projection needed before the first masterchain state is applied.
type TrackerOptions struct {
	MaximalVerticalSeqno uint32
	// StartGroupsFromSeqno suppresses lifecycle transitions for earlier
	// masterchain states. Snapshots still advance during catch-up; the first
	// state at or above the threshold starts its sessions from a clean lifecycle
	// view. Zero starts lifecycle management immediately.
	StartGroupsFromSeqno uint32
	UnsafeRotations      []UnsafeRotationRule
	// InitialCollators is the registry pinned at the previous all-shards
	// rotation. Startup code must derive it from that masterchain state. It is
	// needed because a non-key rotation uses the old registry while reconciling
	// groups and pins the registry carried by the new state only afterwards.
	// ResolveCollatorsByValidator derives this value from the previous rotation.
	InitialCollators []CollatorRegistryEntry
}

// ApplyInput identifies one applied masterchain state and the wall-clock point
// at which the 60-second future split/merge horizon is evaluated.
type ApplyInput struct {
	Block ton.BlockIDExt
	Root  *cell.Cell
	AsOf  time.Time
}

// Session is a fully resolved current or tentative validator group. Published
// sessions are immutable; callers must not mutate their slices or block IDs.
type Session struct {
	ID               [32]byte
	Shard            ShardID
	CatchainSeqno    uint32
	ValidatorSetHash uint32
	Validators       []Validator
	Genesis          []ton.BlockIDExt
	// Registered contains the current masterchain ShardDescr projections
	// governing an active shard. It is refreshed with every snapshot even when
	// the consensus session ID and Genesis stay unchanged; tentative sessions
	// leave it empty until promotion.
	Registered     []ShardDescription
	MinMasterchain ton.BlockIDExt
	FinalizedBlock *ton.BlockIDExt
}

// Snapshot is the immutable group view derived from one masterchain state.
// Active contains current topology leaves; Future contains tentative sessions.
type Snapshot struct {
	MasterchainBlock  ton.BlockIDExt
	ConfigRootHash    [32]byte
	GenUTime          uint32
	LastKeyBlockSeqno uint32
	Ready             bool
	// LifecycleEnabled reports whether this state reached StartGroupsFromSeqno
	// and may drive session preparation, activation, and shutdown. Active and
	// Future are still populated before the threshold so catch-up can preserve
	// group origins without starting historical sessions.
	LifecycleEnabled     bool
	Config               *Config
	Active               []Session
	Future               []Session
	CollatorsByValidator []CollatorRegistryEntry
	PersistentOverlay    []PersistentOverlayMember

	// pinnedCollators is the registry effective after this state has been
	// processed. It differs from CollatorsByValidator only at a non-key
	// all-shards rotation and keeps Project independent from mutable Tracker
	// state.
	pinnedCollators  []CollatorRegistryEntry
	rotatedAllShards bool
	// rotationID is the unsafe rotation namespace this snapshot's session ids
	// were derived under. It is retained because it is not recoverable from the
	// snapshot: unsafeRotationID also depends on the masterchain seqno, so it
	// can change for an unchanged catchain seqno. Active session reuse is gated
	// on it.
	rotationID uint32
}

// TransitionKind describes one lifecycle edge between consecutive snapshots.
type TransitionKind uint8

const (
	TransitionStopped TransitionKind = iota + 1
	TransitionDiscarded
	TransitionPromoted
	TransitionStarted
	TransitionPrepared
)

func (k TransitionKind) String() string {
	switch k {
	case TransitionStopped:
		return "stopped"
	case TransitionDiscarded:
		return "discarded"
	case TransitionPromoted:
		return "promoted"
	case TransitionStarted:
		return "started"
	case TransitionPrepared:
		return "prepared"
	default:
		return "unknown"
	}
}

// Transition contains the session after a start-like edge or before a
// stop-like edge.
type Transition struct {
	Kind    TransitionKind
	Session Session
}

// ApplyResult contains the committed immutable snapshot and its lifecycle
// diff. An idempotent replay has an empty Transitions slice.
type ApplyResult struct {
	Snapshot    *Snapshot
	Transitions []Transition
	// CollatorRegistryIssue is advisory and never fails an apply: it reports
	// that this state's all-shards rotation could not read the delegated
	// collator registry, so delegation is off until a later rotation reads it.
	// It is set only on the rotation that resolved the registry.
	CollatorRegistryIssue error
}

// Tracker serializes masterchain updates and atomically publishes snapshots.
type Tracker struct {
	mu                   sync.RWMutex
	maximalVerticalSeqno uint32
	startGroupsFromSeqno uint32
	unsafeRotations      map[uint32]UnsafeRotationRule
	initialCollators     []CollatorRegistryEntry
	snapshot             atomic.Pointer[Snapshot]
	history              map[replayBlockKey]*Snapshot
	historyOrder         []replayBlockKey
}

// NewTracker constructs an empty tracker and validates all local rotation
// rules up front.
func NewTracker(options TrackerOptions) (*Tracker, error) {
	unsafeRotations := make(map[uint32]UnsafeRotationRule, len(options.UnsafeRotations))
	for i, rule := range options.UnsafeRotations {
		if rule.ID == 0 {
			return nil, fmt.Errorf("unsafe rotation %d has zero id", i)
		}
		if _, exists := unsafeRotations[rule.CatchainSeqno]; exists {
			return nil, fmt.Errorf("duplicate unsafe rotation for catchain seqno %d", rule.CatchainSeqno)
		}

		unsafeRotations[rule.CatchainSeqno] = rule
	}
	initialCollators, err := normalizeCollatorRegistry(options.InitialCollators)
	if err != nil {
		return nil, fmt.Errorf("initial collator registry: %w", err)
	}

	return &Tracker{
		maximalVerticalSeqno: options.MaximalVerticalSeqno,
		startGroupsFromSeqno: options.StartGroupsFromSeqno,
		unsafeRotations:      unsafeRotations,
		initialCollators:     initialCollators,
	}, nil
}

// Snapshot returns the latest immutable snapshot.
func (t *Tracker) Snapshot() (*Snapshot, error) {
	snapshot := t.snapshot.Load()
	if snapshot == nil {
		return nil, ErrNoSnapshot
	}

	return snapshot, nil
}

// Apply parses and commits one masterchain state. The same full block ID is
// idempotent; older states and same-height forks are rejected explicitly.
func (t *Tracker) Apply(input ApplyInput) (ApplyResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	previous := t.snapshot.Load()
	result, err := t.project(previous, input)
	if err != nil {
		return ApplyResult{}, err
	}
	if result.Snapshot != previous {
		t.recordSnapshotLocked(result.Snapshot)
		t.snapshot.Store(result.Snapshot)
	}

	return result, nil
}

// Project derives the snapshot Apply would publish after previous, without
// mutating Tracker or emitting lifecycle effects. It is used for speculative
// masterchain candidates whose state is not applied to the node yet. Snapshots
// are immutable and may be passed directly from Snapshot.
//
// The result equals Apply's only when a base is available: either previous is
// given, or the block is still retained in the rotation window kept by
// recordSnapshotLocked. Projected from no base at all the derivation has
// nothing to carry forward — Ready collapses to this state's own
// rotated_all_shards, so an ordinary masterchain block comes back with
// Ready=false and empty Active/Future and no error. A key-block rotation still
// matches Apply exactly — projectCollatorRegistry ignores previous on a key
// state — but a non-key all-shards rotation does not, because there the
// registry falls back to the tracker's initial entries instead of the
// predecessor's pinned ones. Callers must therefore check Ready before
// validating anything against the returned group view.
func (t *Tracker) Project(previous *Snapshot, input ApplyInput) (*Snapshot, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if previous == nil {
		if snapshot := t.history[replayKey(input.Block)]; snapshot != nil {
			return snapshot, nil
		}
	}

	result, err := t.project(previous, input)
	if err != nil {
		return nil, err
	}

	return result.Snapshot, nil
}

func (t *Tracker) project(previous *Snapshot, input ApplyInput) (ApplyResult, error) {
	if input.AsOf.IsZero() {
		return ApplyResult{}, fmt.Errorf("validator groups observation time is required")
	}
	if previous != nil {
		switch {
		case input.Block.SeqNo < previous.MasterchainBlock.SeqNo:
			return ApplyResult{}, fmt.Errorf("%w: got %d after %d", ErrStaleMasterchainState, input.Block.SeqNo, previous.MasterchainBlock.SeqNo)
		case input.Block.SeqNo == previous.MasterchainBlock.SeqNo:
			if input.Block.Equals(&previous.MasterchainBlock) {
				return ApplyResult{Snapshot: previous}, nil
			}

			return ApplyResult{}, fmt.Errorf("%w at seqno %d", ErrConflictingMasterchainState, input.Block.SeqNo)
		}
	}

	state, err := ParseState(StateInput{Block: input.Block, Root: input.Root})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("parse masterchain state: %w", err)
	}
	if state.IsKeyState && !state.RotatedAllShards {
		return ApplyResult{}, errors.New("key masterchain state did not rotate all shards")
	}
	var configRootHash [32]byte
	copy(configRootHash[:], state.ConfigRoot.Hash())
	var config *Config
	if previous != nil && previous.ConfigRootHash == configRootHash {
		config = previous.Config
	} else {
		config, err = ParseConfig(state.ConfigRoot)
		if err != nil {
			return ApplyResult{}, fmt.Errorf("parse validator config: %w", err)
		}
	}

	ready := state.RotatedAllShards
	if previous != nil {
		ready = ready || previous.Ready
	}

	eligible := state.Block.SeqNo >= t.startGroupsFromSeqno
	firstEligible := eligible && (previous == nil || previous.MasterchainBlock.SeqNo < t.startGroupsFromSeqno)

	rotationID := t.unsafeRotationID(state.Block.SeqNo, state.CatchainSeqno)

	var active []Session
	var future []Session
	if ready {
		active, err = t.buildActiveSessions(previous, state, config, rotationID)
		if err != nil {
			return ApplyResult{}, err
		}
		// No tentative generation is derived on a rotation state: previous
		// tentatives are promoted into the freshly rotated active sessions and
		// the next generation is prepared only after observing the following
		// state. This is a local choice, not a protocol rule —
		// ValidatorManagerImpl::update_shards runs its future_shards loop over
		// get_next_validator_set on every masterchain block, rotation states
		// included, and gates only the session GC lists on rotated_all_shards.
		// It costs one masterchain block of tentative pre-warm. Lifting it is
		// not a local edit: Snapshot.Future also feeds the message pool
		// destination set and the shard-top next-validator-set lookup, both of
		// which would then change at every rotation block.
		if !state.RotatedAllShards {
			future, err = t.buildFutureSessions(state, config, input.AsOf, rotationID)
			if err != nil {
				return ApplyResult{}, err
			}
			futurePrevious := previous
			if firstEligible {
				futurePrevious = nil
			}
			future, err = reconcileFutureSessions(futurePrevious, active, future)
			if err != nil {
				return ApplyResult{}, err
			}
		}
		if firstEligible && previous != nil {
			previousActive, err := indexSessions(previous.Active)
			if err != nil {
				return ApplyResult{}, fmt.Errorf("index catch-up active sessions: %w", err)
			}
			for i := range active {
				if old, exists := previousActive[active[i].ID]; exists {
					preserveActiveSession(&active[i], old)
				}
			}
		}
	}

	collators, pinnedCollators, registryIssue := t.projectCollatorRegistry(previous, state, config)
	overlayMembers := persistentOverlayWithCollators(config.PersistentOverlayMembers(), collators)
	snapshot := &Snapshot{
		MasterchainBlock:     state.Block,
		ConfigRootHash:       configRootHash,
		GenUTime:             state.GenUTime,
		LastKeyBlockSeqno:    state.LastKeyBlockSeqno,
		Ready:                ready,
		LifecycleEnabled:     eligible,
		Config:               config,
		Active:               active,
		Future:               future,
		CollatorsByValidator: collators,
		PersistentOverlay:    overlayMembers,
		pinnedCollators:      pinnedCollators,
		rotatedAllShards:     state.RotatedAllShards,
		rotationID:           rotationID,
	}

	lifecyclePrevious := previous
	if firstEligible {
		lifecyclePrevious = nil
	}
	transitions, err := reconcileSnapshot(lifecyclePrevious, snapshot)
	if err != nil {
		return ApplyResult{}, err
	}
	if !eligible {
		transitions = nil
	}

	return ApplyResult{
		Snapshot:              snapshot,
		Transitions:           transitions,
		CollatorRegistryIssue: registryIssue,
	}, nil
}

// recordSnapshotLocked retains canonical derived views from the previous
// all-shards rotation onward. This is the same history boundary used by the
// C++ validator manager: an active consensus session cannot predate that
// rotation, while keeping one complete previous generation lets an old
// session drain during the next rotation.
func (t *Tracker) recordSnapshotLocked(snapshot *Snapshot) {
	if t.history == nil {
		t.history = make(map[replayBlockKey]*Snapshot)
	}
	key := replayKey(snapshot.MasterchainBlock)
	if t.history[key] != nil {
		return
	}

	if snapshot.rotatedAllShards {
		previousRotation := -1
		for i := len(t.historyOrder) - 1; i >= 0; i-- {
			if t.history[t.historyOrder[i]].rotatedAllShards {
				previousRotation = i
				break
			}
		}
		if previousRotation > 0 {
			for _, stale := range t.historyOrder[:previousRotation] {
				delete(t.history, stale)
			}
			copy(t.historyOrder, t.historyOrder[previousRotation:])
			t.historyOrder = t.historyOrder[:len(t.historyOrder)-previousRotation]
		}
	}

	t.history[key] = snapshot
	t.historyOrder = append(t.historyOrder, key)
}

func (t *Tracker) unsafeRotationID(masterchainSeqno, catchainSeqno uint32) uint32 {
	rule, exists := t.unsafeRotations[catchainSeqno]
	if !exists || masterchainSeqno < rule.FromMasterchainSeqno {
		return 0
	}

	return rule.ID
}

func (t *Tracker) buildActiveSessions(previous *Snapshot, state *State, config *Config, rotationID uint32) ([]Session, error) {
	targets, err := state.CurrentTargets()
	if err != nil {
		return nil, fmt.Errorf("resolve active group topology: %w", err)
	}

	reusable := reusableActiveSessions(previous, config, state.LastKeyBlockSeqno, rotationID)
	sessions := make([]Session, 0, len(targets))
	for i := range targets {
		target := &targets[i]
		catchainSeqno, err := state.CatchainSeqnoFor(target.Shard)
		if err != nil {
			return nil, fmt.Errorf("resolve catchain seqno for active shard %+v: %w", target.Shard, err)
		}

		var session Session
		if reused, exists := reusable[activeSessionKey{Shard: target.Shard, CatchainSeqno: catchainSeqno}]; exists {
			session = Session{
				ID:               reused.ID,
				Shard:            target.Shard,
				CatchainSeqno:    catchainSeqno,
				ValidatorSetHash: reused.ValidatorSetHash,
				Validators:       reused.Validators,
			}
		} else if session, err = t.buildSession(
			config, config.ActiveValidators, target.Shard, catchainSeqno, state.LastKeyBlockSeqno, rotationID,
		); err != nil {
			return nil, fmt.Errorf("build active session for shard %+v: %w", target.Shard, err)
		}
		session.Genesis = target.Genesis
		session.Registered = target.Registered
		session.MinMasterchain = state.Block
		session.FinalizedBlock = exactFinalizedBlock(state, target.Shard)
		sessions = append(sessions, session)
	}

	sortSessions(sessions)
	return sessions, nil
}

// activeSessionKey addresses the immutable half of an active session in the
// previous snapshot.
type activeSessionKey struct {
	Shard         ShardID
	CatchainSeqno uint32
}

// reusableActiveSessions indexes the previous active sessions whose roster, set
// hash and session id are provably the ones this state would recompute. An
// active roster is selectRoster(config.ActiveValidators, shard, catchainSeqno)
// with config.Catchain, and its session id adds only the tracker's own vertical
// seqno, the last key block seqno and the unsafe rotation namespace — so
// pinning the config by pointer identity, the two seqnos by equality and the
// shard plus catchain seqno by the lookup key pins every input. Recomputing
// them is measurably the most expensive part of every applied masterchain
// state, and reconcileSnapshot discards the fresh rosters again anyway.
//
// Deliberately not used for tentative sessions: SelectTentativeValidatorSet
// swaps the current set for the next one on gen_utime crossing a lifetime
// boundary while the configuration is byte-identical, so these gates would
// serve a stale session id there. See
// TestTrackerFutureSessionsFollowTentativeSetBoundary.
func reusableActiveSessions(
	previous *Snapshot,
	config *Config,
	lastKeyBlockSeqno, rotationID uint32,
) map[activeSessionKey]*Session {
	if previous == nil || previous.Config != config ||
		previous.LastKeyBlockSeqno != lastKeyBlockSeqno || previous.rotationID != rotationID {
		return nil
	}

	index := make(map[activeSessionKey]*Session, len(previous.Active))
	for i := range previous.Active {
		session := &previous.Active[i]
		index[activeSessionKey{Shard: session.Shard, CatchainSeqno: session.CatchainSeqno}] = session
	}

	return index
}

func (t *Tracker) buildFutureSessions(state *State, config *Config, asOf time.Time, rotationID uint32) ([]Session, error) {
	shards, err := state.FutureShards(asOf, futureShardHorizon)
	if err != nil {
		return nil, fmt.Errorf("resolve future group topology: %w", err)
	}

	masterchainSet, err := SelectTentativeValidatorSet(TentativeValidatorSetInput{
		Current:          config.ActiveValidators,
		Next:             config.NextValidators,
		GenUTime:         state.GenUTime,
		CatchainLifetime: config.Catchain.MasterchainLifetime,
	})
	if err != nil {
		return nil, fmt.Errorf("select future masterchain validator set: %w", err)
	}
	shardSet, err := SelectTentativeValidatorSet(TentativeValidatorSetInput{
		Current:          config.ActiveValidators,
		Next:             config.NextValidators,
		GenUTime:         state.GenUTime,
		CatchainLifetime: config.Catchain.ShardLifetime,
	})
	if err != nil {
		return nil, fmt.Errorf("select future shard validator set: %w", err)
	}

	sessions := make([]Session, 0, len(shards))
	for _, shard := range shards {
		catchainSeqno, err := state.CatchainSeqnoFor(shard)
		if err != nil {
			return nil, fmt.Errorf("resolve catchain seqno for future shard %+v: %w", shard, err)
		}
		if catchainSeqno == math.MaxUint32 {
			return nil, fmt.Errorf("future catchain seqno overflows for shard %+v", shard)
		}

		validatorSet := shardSet
		if shard.IsMasterchain() {
			validatorSet = masterchainSet
		}

		session, err := t.buildSession(config, validatorSet, shard, catchainSeqno+1, state.LastKeyBlockSeqno, rotationID)
		if err != nil {
			return nil, fmt.Errorf("build future session for shard %+v: %w", shard, err)
		}
		session.FinalizedBlock = exactFinalizedBlock(state, shard)
		sessions = append(sessions, session)
	}

	sortSessions(sessions)
	return sessions, nil
}

func (t *Tracker) buildSession(config *Config, validatorSet ValidatorSet, shard ShardID, catchainSeqno, lastKeyBlockSeqno, rotationID uint32) (Session, error) {
	validators := selectRoster(RosterInput{
		Set:           validatorSet,
		Workchain:     shard.Workchain,
		Shard:         uint64(shard.Shard),
		CatchainSeqno: catchainSeqno,
		Catchain:      config.Catchain,
	})

	setHash, err := ValidatorSetHash(ValidatorSetHashInput{
		CatchainSeqno: catchainSeqno,
		Validators:    validators,
	})
	if err != nil {
		return Session{}, err
	}

	members := make([]Member, len(validators))
	for i := range validators {
		members[i] = Member{
			PublicKeyHash: validators[i].PublicKeyHash,
			ADNL:          validators[i].ADNL,
			Weight:        validators[i].Weight,
		}
	}
	sessionID, err := SessionID(SessionIDInput{
		Workchain:            shard.Workchain,
		Shard:                shard.Shard,
		MaximalVerticalSeqno: t.maximalVerticalSeqno,
		LastKeyBlockSeqno:    lastKeyBlockSeqno,
		CatchainSeqno:        catchainSeqno,
		UnsafeRotation:       UnsafeRotation{ID: rotationID},
		Members:              members,
	})
	if err != nil {
		return Session{}, err
	}

	return Session{
		ID:               sessionID,
		Shard:            shard,
		CatchainSeqno:    catchainSeqno,
		ValidatorSetHash: setHash,
		Validators:       validators,
	}, nil
}

func exactFinalizedBlock(state *State, shard ShardID) *ton.BlockIDExt {
	if shard.IsMasterchain() {
		block := state.Block
		return &block
	}
	index, exists := state.shardIndex[shard]
	if !exists {
		return nil
	}

	block := state.Shards[index].Block
	return &block
}

// A tentative group survives a temporary change in the desired future
// topology until a related active group advances far enough to supersede it.
// This preserves session state across transient FSM/config observations.
func reconcileFutureSessions(previous *Snapshot, active, desired []Session) ([]Session, error) {
	if previous == nil {
		return desired, nil
	}

	activeByID, err := indexSessions(active)
	if err != nil {
		return nil, fmt.Errorf("index active sessions for future reconciliation: %w", err)
	}
	desiredByID, err := indexSessions(desired)
	if err != nil {
		return nil, fmt.Errorf("index desired future sessions: %w", err)
	}

	future := make([]Session, 0, len(desired)+len(previous.Future))
	for i := range desired {
		session := desired[i]
		if !futureSessionSuperseded(active, session) {
			future = append(future, session)
		}
	}
	for i := range previous.Future {
		session := previous.Future[i]
		if _, promoted := activeByID[session.ID]; promoted {
			continue
		}
		if _, refreshed := desiredByID[session.ID]; refreshed {
			continue
		}
		if futureSessionSuperseded(active, session) {
			continue
		}

		future = append(future, session)
	}

	sortSessions(future)
	return future, nil
}

func futureSessionSuperseded(active []Session, future Session) bool {
	for i := range active {
		current := &active[i]
		if current.Shard.Workchain != future.Shard.Workchain {
			continue
		}

		equal := current.Shard.Shard == future.Shard.Shard
		related := shardIsAncestor(current.Shard, future.Shard) || shardIsAncestor(future.Shard, current.Shard)
		if (equal && current.CatchainSeqno >= future.CatchainSeqno) ||
			(related && current.CatchainSeqno > future.CatchainSeqno) {
			return true
		}
	}

	return false
}

func reconcileSnapshot(previous, next *Snapshot) ([]Transition, error) {
	if previous == nil {
		transitions := make([]Transition, 0, len(next.Active)+len(next.Future))
		for i := range next.Active {
			transitions = append(transitions, Transition{Kind: TransitionStarted, Session: next.Active[i]})
		}
		for i := range next.Future {
			transitions = append(transitions, Transition{Kind: TransitionPrepared, Session: next.Future[i]})
		}
		return transitions, nil
	}

	oldActive, err := indexSessions(previous.Active)
	if err != nil {
		return nil, fmt.Errorf("index previous active sessions: %w", err)
	}
	oldFuture, err := indexSessions(previous.Future)
	if err != nil {
		return nil, fmt.Errorf("index previous future sessions: %w", err)
	}
	newActive, err := indexSessions(next.Active)
	if err != nil {
		return nil, fmt.Errorf("index next active sessions: %w", err)
	}
	newFuture, err := indexSessions(next.Future)
	if err != nil {
		return nil, fmt.Errorf("index next future sessions: %w", err)
	}

	transitions := make([]Transition, 0, len(previous.Active)+len(previous.Future)+len(next.Active)+len(next.Future))
	for i := range previous.Active {
		session := previous.Active[i]
		if _, exists := newActive[session.ID]; !exists {
			transitions = append(transitions, Transition{Kind: TransitionStopped, Session: session})
		}
	}
	for i := range previous.Future {
		session := previous.Future[i]
		if _, promoted := newActive[session.ID]; promoted {
			continue
		}
		if _, exists := newFuture[session.ID]; !exists {
			transitions = append(transitions, Transition{Kind: TransitionDiscarded, Session: session})
		}
	}

	for i := range next.Active {
		session := &next.Active[i]
		if old, exists := oldActive[session.ID]; exists {
			preserveActiveSession(session, old)
			continue
		}
		if old, promoted := oldFuture[session.ID]; promoted {
			preserveFutureSession(session, old)
			transitions = append(transitions, Transition{Kind: TransitionPromoted, Session: *session})
			continue
		}

		transitions = append(transitions, Transition{Kind: TransitionStarted, Session: *session})
	}
	for i := range next.Future {
		session := &next.Future[i]
		if old, exists := oldFuture[session.ID]; exists {
			preserveFutureSession(session, old)
			continue
		}
		if _, active := newActive[session.ID]; active {
			return nil, fmt.Errorf("session %x is both active and future", session.ID)
		}

		transitions = append(transitions, Transition{Kind: TransitionPrepared, Session: *session})
	}

	return transitions, nil
}

func indexSessions(sessions []Session) (map[[32]byte]*Session, error) {
	index := make(map[[32]byte]*Session, len(sessions))
	for i := range sessions {
		session := &sessions[i]
		if previous, exists := index[session.ID]; exists {
			return nil, fmt.Errorf("duplicate session id %x for shards %+v and %+v", session.ID, previous.Shard, session.Shard)
		}

		index[session.ID] = session
	}
	return index, nil
}

func preserveActiveSession(next, previous *Session) {
	// Registered and FinalizedBlock describe the newest masterchain snapshot,
	// not the lifetime origin of the consensus session.
	next.Validators = previous.Validators
	next.Genesis = previous.Genesis
	next.MinMasterchain = previous.MinMasterchain
}

func preserveFutureSession(next, previous *Session) {
	next.Validators = previous.Validators
}

func sortSessions(sessions []Session) {
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].Shard.Workchain != sessions[j].Shard.Workchain {
			return sessions[i].Shard.Workchain < sessions[j].Shard.Workchain
		}
		if sessions[i].Shard.Shard != sessions[j].Shard.Shard {
			return uint64(sessions[i].Shard.Shard) < uint64(sessions[j].Shard.Shard)
		}
		if sessions[i].CatchainSeqno != sessions[j].CatchainSeqno {
			return sessions[i].CatchainSeqno < sessions[j].CatchainSeqno
		}
		return bytes.Compare(sessions[i].ID[:], sessions[j].ID[:]) < 0
	})
}
