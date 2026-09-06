package collator

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

var ErrAcquisitionNotReady = errors.New("collator: local acquisition not ready")

const defaultLocalExternalLimit = 500

// LocalMessageSource is the cohesive external/internal message boundary used
// by local acquisition. msgpool.Pool implements it directly.
type LocalMessageSource interface {
	OpenExternalStream(msgpool.ShardIdent, int) (*msgpool.ExternalStream, error)
	// OpenExternalStreamExcluding is OpenExternalStream with a set of messages
	// the stream must never offer. A build started before its predecessor
	// reported what it consumed has to be told directly, or it executes those
	// messages a second time and its feedback for them is dropped as stale.
	OpenExternalStreamExcluding(msgpool.ShardIdent, int, [][32]byte) (*msgpool.ExternalStream, error)
	OpenExternalSnapshot(msgpool.ShardIdent, int) (*msgpool.ExternalStream, error)
	Complete([]msgpool.ExternalFeedback) error
	Internals() *msgpool.Internals
}

type LocalShardTopSource interface {
	Select(context.Context, ShardTopSelection) ([]ShardTop, error)
}

// AccountPrewarmer accepts best-effort account-state and exact cell-tree cache
// hints. Enqueue methods must not wait for I/O; candidate acquisition and
// collation do not depend on warming for correctness.
type AccountPrewarmer interface {
	EnqueueRoot(cell.Hash) bool
	EnqueueAccount(workchain int32, account [32]byte) bool
	PrewarmAccountNow(workchain int32, account [32]byte) bool
}

type LocalAcquisitionOptions struct {
	Builder                *Builder
	Logger                 zerolog.Logger
	CollationObserver      CollationObserver
	ValidationObserver     ValidationCoreObserver
	Store                  LocalStateStore
	Groups                 LocalGroupSource
	Messages               LocalMessageSource
	ShardTops              LocalShardTopSource
	Semantics              CandidateTransitionVerifier
	AccountPrewarmer       AccountPrewarmer
	AccountPrewarmCapacity int
	Random                 io.Reader
	ExternalLimit          int
	MaxExternalAttempts    int
	Dispatch               *DispatchPolicy
}

// LocalAcquisition materializes exact Builder requests from the node's local
// authenticated state. Independent sessions do not share a build lock.
type LocalAcquisition struct {
	builder                *Builder
	log                    zerolog.Logger
	collationObserver      CollationObserver
	validationObserver     ValidationCoreObserver
	store                  LocalStateStore
	groups                 LocalGroupSource
	messages               LocalMessageSource
	shardTops              LocalShardTopSource
	semantics              CandidateTransitionVerifier
	accountPrewarmer       AccountPrewarmer
	accountPrewarmCapacity int
	random                 io.Reader
	randomMu               sync.Mutex

	externalLimit       int
	maxExternalAttempts int
	dispatch            DispatchPolicy

	configs                   localConfigCache
	master                    localMasterViewCache
	blocks                    localBlockCache
	dispatchPrewarm           dispatchPrewarmScheduler
	dispatchPrewarmGeneration atomic.Uint64

	// builtBindMisses counts commits of a candidate this node built whose
	// derivation capsule did not bind the chain being committed, and which
	// therefore fell back to replaying the block. It is expected to stay at zero;
	// a non-zero value means every produced block is paying for a full parse and a
	// second Merkle apply again.
	builtBindMisses atomic.Int64

	// selectedViewFallbacks counts slots whose newest resident masterchain view
	// was refused by the per-slot pick and which therefore built against an
	// older one. Sustained non-zero means the node is silently back to building
	// on the view its session update pinned, which is the behaviour the
	// per-slot pick exists to replace.
	selectedViewFallbacks atomic.Int64

	mu       sync.RWMutex
	sessions map[[32]byte]*localAcquisitionSession
}

// localMasterViewCache holds the resident masterchain view plus a small set of
// recently demoted ones.
//
// The resident slot alone is not enough. A session update pins the masterchain
// block it was issued for, and a shard candidate pins the one its MasterRef
// names; both routinely lag the newest applied block by the time validation
// runs, so every candidate would otherwise rebuild the whole view — state extra,
// shard registry, masterchain history tuple and libraries — from scratch.
//
// The two are kept in one struct so a lookup takes one lock and reads the
// resident slot first, and so the entry map can only ever be written by the same
// critical section that installs a resident view.
type localMasterViewCache struct {
	mu   sync.RWMutex
	view *localMasterView
	// entries is keyed by block root hash. Views are immutable, so an entry is
	// only ever evicted, never invalidated.
	entries map[[32]byte]*localMasterView
	// order is insertion order, evicted from the front, exactly as
	// localConfigCache does at the same cap.
	order [][32]byte
}

type localAcquisitionSession struct {
	mu sync.Mutex

	session                   Session
	branch                    *msgpool.Branch
	activation                *SessionActivation
	update                    SessionUpdate
	master                    *localMasterView
	retired                   bool
	dispatchPrewarmGeneration uint64

	seeds      map[uint32][32]byte
	candidates map[simplex.CandidateID]localCandidateState
	blocks     map[[32]byte]localCandidateState
}

type localCandidateState struct {
	block        PreviousBlock
	storageStats AccountStorageStats
	// queueBase is the committed predecessor set of the first speculative
	// queue delta in this chain. A later candidate's block is a valid state
	// predecessor, but must not replace this committed queue view: the delta
	// chain itself supplies that transition to msgpool.Cut.
	queueBase []PreviousBlock
	queueTip  *[32]byte
	master    *localMasterView
}

type localResolvedChain struct {
	previous     []PreviousBlock
	storage      AccountStorageStats
	queueBase    []PreviousBlock
	candidateTip *[32]byte
	master       *localMasterView
}

func NewLocalAcquisition(options LocalAcquisitionOptions) (*LocalAcquisition, error) {
	if options.Builder == nil {
		return nil, errors.New("collator: local acquisition builder is required")
	}
	if options.Store == nil {
		return nil, errors.New("collator: local acquisition store is required")
	}
	if options.Groups == nil {
		return nil, errors.New("collator: local acquisition group source is required")
	}
	if options.Messages == nil {
		return nil, errors.New("collator: local acquisition message source is required")
	}
	if options.Semantics == nil {
		return nil, errors.New("collator: local acquisition semantic verifier is required")
	}
	if options.ExternalLimit < 0 || options.MaxExternalAttempts < 0 {
		return nil, errors.New("collator: local acquisition limits must not be negative")
	}
	if options.AccountPrewarmer != nil && options.AccountPrewarmCapacity <= 0 {
		return nil, errors.New("collator: account prewarm capacity must be positive")
	}
	if options.ExternalLimit == 0 {
		options.ExternalLimit = defaultLocalExternalLimit
	}
	if options.MaxExternalAttempts == 0 {
		options.MaxExternalAttempts = math.MaxInt
	}
	if options.Random == nil {
		options.Random = cryptorand.Reader
	}
	dispatch := ReferenceDispatchPolicy()
	if options.Dispatch != nil {
		dispatch = *options.Dispatch
	}

	return &LocalAcquisition{
		builder:                options.Builder,
		log:                    options.Logger,
		collationObserver:      options.CollationObserver,
		validationObserver:     options.ValidationObserver,
		store:                  options.Store,
		groups:                 options.Groups,
		messages:               options.Messages,
		shardTops:              options.ShardTops,
		semantics:              options.Semantics,
		accountPrewarmer:       options.AccountPrewarmer,
		accountPrewarmCapacity: options.AccountPrewarmCapacity,
		random:                 options.Random,
		externalLimit:          options.ExternalLimit,
		maxExternalAttempts:    options.MaxExternalAttempts,
		dispatch:               dispatch,
		configs:                localConfigCache{log: options.Logger, entries: make(map[cell.Hash]localPreparedConfig)},
		blocks:                 localBlockCache{entries: make(map[[32]byte]*localBlockSource)},
		sessions:               make(map[[32]byte]*localAcquisitionSession),
	}, nil
}

func (a *LocalAcquisition) PrepareSession(ctx context.Context, session Session, update SessionUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	staged, err := a.stageSession(session, update)
	if err != nil {
		return err
	}

	a.mu.Lock()
	if current := a.sessions[session.ID]; current != nil {
		a.mu.Unlock()

		current.mu.Lock()
		defer current.mu.Unlock()
		if current.retired {
			return ErrSessionRetired
		}
		if current.session.Equal(session) && current.update.Equal(update) {
			return nil
		}

		return ErrSessionConflict
	}
	staged.branch, err = a.messages.Internals().OpenBranch(targetShardIdent(session.Shard))
	if err != nil {
		a.mu.Unlock()
		if errors.Is(err, msgpool.ErrNotFound) {
			return fmt.Errorf(
				"%w: open session internal-message branch: %v",
				ErrAcquisitionNotReady,
				err,
			)
		}

		return fmt.Errorf("open session internal-message branch: %w", err)
	}
	a.sessions[session.ID] = staged
	a.enqueueRegisteredDispatchPrewarm(ctx, staged.dispatchPrewarmOwner(), session.Shard, update.Registered)
	a.mu.Unlock()

	return nil
}

// ActivateSession binds exact predecessor anchors to an already prepared
// tentative session. The expensive authenticated master view is staged before
// the in-memory record changes, so a failed activation leaves the tentative
// session query-ready and retryable.
func (a *LocalAcquisition) ActivateSession(
	ctx context.Context,
	activation SessionActivation,
	update SessionUpdate,
) error {
	managed, err := a.session(activation.SessionID)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.retired {
		return ErrSessionRetired
	}
	if managed.activation != nil {
		if managed.activation.Equal(activation) && managed.update.Equal(update) {
			return nil
		}

		return ErrSessionConflict
	}
	if !managed.update.Equal(update) {
		return ErrSessionConflict
	}
	if err = validateSessionActivation(managed.session, activation); err != nil {
		return err
	}
	activated := activatedSession(managed.session, activation)
	_, master, err := a.stageMaster(ctx, activated, update, true, nil)
	if err != nil {
		return err
	}

	clonedActivation := cloneSessionActivation(activation)
	managed.activation = &clonedActivation
	managed.master = master
	if activated.Shard.IsMasterchain() {
		a.enqueueDispatchStatePrewarm(ctx, managed.dispatchPrewarmOwner(), master.previous.ID, master.previous.State)
	}

	return nil
}

func (a *LocalAcquisition) UpdateSession(ctx context.Context, session Session, update SessionUpdate) error {
	managed, err := a.session(session.ID)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.retired {
		return ErrSessionRetired
	}
	if !managed.session.Equal(session) {
		return ErrSessionConflict
	}
	if managed.update.Equal(update) {
		return nil
	}
	if err = ValidateSessionUpdateAdvance(managed.update, update); err != nil {
		return err
	}
	if managed.update.CurrentBase != update.CurrentBase && update.CurrentBase.Exists {
		if _, exists := managed.candidates[update.CurrentBase.ID]; !exists {
			return fmt.Errorf("%w: selected consensus base is not materialized", ErrCandidateConflict)
		}
	}
	var master *localMasterView
	if managed.activation != nil {
		_, master, err = a.stageMaster(
			ctx,
			activatedSession(managed.session, *managed.activation),
			update,
			true,
			managed.master,
		)
		if err != nil {
			return err
		}
	}
	advanceWindow := !managed.update.HasCurrentWindow ||
		update.CurrentWindowStart > managed.update.CurrentWindowStart
	if advanceWindow {
		var tip *[32]byte
		if update.CurrentBase.Exists {
			if kept, exists := managed.candidates[update.CurrentBase.ID]; exists {
				tip = kept.queueTip
				if tip != nil {
					destination := targetShardIdent(managed.session.Shard)
					source := blockShardIdent(kept.block.ID)
					ref, refErr := localSourceRef(kept.block.ID)
					if refErr != nil {
						return refErr
					}
					refErr = a.messages.Internals().ValidateSourceRef(destination, source, ref)
					if refErr == nil {
						// The consensus window is the ownership boundary: once its
						// selected base is committed, the next window can pin that
						// exact run instead of carrying the preceding speculative DAG.
						key, keyErr := blockRootKey(kept.block.ID)
						if keyErr != nil {
							return keyErr
						}
						kept.queueBase = nil
						kept.queueTip = nil
						managed.candidates[update.CurrentBase.ID] = kept
						managed.blocks[key] = kept
						tip = nil
					} else if !errors.Is(refErr, msgpool.ErrCutNotReady) &&
						!errors.Is(refErr, msgpool.ErrCutStale) && !errors.Is(refErr, msgpool.ErrNotFound) {
						return fmt.Errorf("read consensus base queue position: %w", refErr)
					}
				}
			}
		}
		if err = managed.branch.Retain(tip); err != nil {
			return fmt.Errorf("retain consensus internal-message branch: %w", err)
		}
	}
	managed.update = cloneSessionUpdate(update)
	managed.master = master
	if advanceWindow {
		// The current leader can build several chained candidates before
		// Simplex advances the consensus base. Intermediate observations within
		// that same window must retain their speculative internal-message deltas:
		// the next local candidate cuts on the previous one. Rotation to a new
		// leader window is the ownership boundary that safely prunes them.
		managed.pruneCandidates(update.CurrentBase)
	}
	for slot := range managed.seeds {
		if slot < update.CurrentWindowStart {
			delete(managed.seeds, slot)
		}
	}
	if update.CurrentBase.Exists {
		if base, exists := managed.candidates[update.CurrentBase.ID]; exists && base.block.State != nil {
			a.enqueueDispatchStatePrewarm(ctx, managed.dispatchPrewarmOwner(), base.block.ID, base.block.State)
		}
	} else {
		a.enqueueRegisteredDispatchPrewarm(
			ctx,
			managed.dispatchPrewarmOwner(),
			managed.session.Shard,
			update.Registered,
		)
	}

	return nil
}

// AdvanceConsensusBase atomically re-roots speculative acquisition at the
// full state selected by Simplex and publishes the matching session update.
// The old private queue branch remains intact if staging or Retain fails.
func (a *LocalAcquisition) AdvanceConsensusBase(
	ctx context.Context,
	request ConsensusBaseUpdate,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	managed, err := a.session(request.Session.ID)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.retired {
		return ErrSessionRetired
	}
	if managed.activation == nil || !sameActivatedSession(
		activatedSession(managed.session, *managed.activation),
		request.Session,
	) {
		return ErrSessionConflict
	}
	if err = validateSessionUpdate(managed.session, request.Update); err != nil {
		return err
	}
	if err = ValidateSessionUpdateAdvance(managed.update, request.Update); err != nil {
		return err
	}
	if request.Update.CurrentBase.Exists != (request.Base != nil) ||
		(request.Base != nil && !request.Base.matches(request.Session.ID, request.Update.CurrentBase.ID)) {
		return ErrCandidateConflict
	}
	if managed.update.Equal(request.Update) && selectedBaseInstalled(managed, request.Base) {
		return nil
	}

	var (
		baseState localCandidateState
		baseKey   [32]byte
	)
	if request.Base != nil {
		if request.Base.block.ID.Workchain != managed.session.Shard.Workchain ||
			request.Base.block.ID.Shard != managed.session.Shard.Shard {
			return ErrCandidateConflict
		}
		baseKey, err = blockRootKey(request.Base.block.ID)
		if err != nil {
			return err
		}
		baseState = localCandidateState{block: clonePreviousBlock(request.Base.block)}
		if existing, exists := managed.candidates[request.Base.candidate]; exists {
			if !sameSelectedBase(existing, request.Base) {
				return ErrCandidateConflict
			}
			baseState = existing
		} else if existing, exists = managed.blocks[baseKey]; exists {
			if !sameSelectedBase(existing, request.Base) {
				return ErrCandidateConflict
			}
			baseState = existing
		}
	}

	var master *localMasterView
	_, master, err = a.stageMaster(
		ctx,
		request.Session,
		request.Update,
		true,
		managed.master,
	)
	if err != nil {
		return err
	}

	if !selectedBaseInstalled(managed, request.Base) {
		retainTip := baseState.queueTip
		if retainTip == nil && request.Base != nil && managed.branch.HasCandidate(baseKey) {
			// A speculative first-slot build installs its foreign base only in
			// the private branch. The resolver-owned selected state deliberately
			// carries no queue lineage, but its block root still names that exact
			// branch node. Retain it so opening the window cannot invalidate the
			// parked successor which is already descending from the selected base.
			retainTip = &baseKey
		}
		if err = managed.branch.Retain(retainTip); err != nil {
			return fmt.Errorf("retain selected consensus base: %w", err)
		}
		if request.Base != nil {
			managed.candidates[request.Base.candidate] = baseState
			managed.blocks[baseKey] = baseState
		}
		managed.pruneCandidates(request.Update.CurrentBase)
	}

	managed.update = cloneSessionUpdate(request.Update)
	managed.master = master
	for slot := range managed.seeds {
		if slot < request.Update.CurrentWindowStart {
			delete(managed.seeds, slot)
		}
	}
	if request.Base != nil {
		a.enqueueDispatchStatePrewarm(ctx, managed.dispatchPrewarmOwner(), baseState.block.ID, baseState.block.State)
	} else {
		a.enqueueRegisteredDispatchPrewarm(
			ctx,
			managed.dispatchPrewarmOwner(),
			request.Session.Shard,
			request.Update.Registered,
		)
	}

	return nil
}

func selectedBaseInstalled(managed *localAcquisitionSession, base *SelectedBaseState) bool {
	if base == nil {
		return len(managed.candidates) == 0 && len(managed.blocks) == 0
	}
	state, exists := managed.candidates[base.candidate]
	if !exists || len(managed.candidates) != 1 || len(managed.blocks) != 1 ||
		!sameSelectedBase(state, base) {
		return false
	}
	key, err := blockRootKey(state.block.ID)
	if err != nil {
		return false
	}
	blockState, exists := managed.blocks[key]

	return exists && sameSelectedBase(blockState, base)
}

func sameSelectedBase(state localCandidateState, base *SelectedBaseState) bool {
	return sameBlockID(state.block.ID, base.block.ID) && state.block.Block != nil && state.block.State != nil &&
		state.block.OutQueueSize != nil && base.block.OutQueueSize != nil &&
		*state.block.OutQueueSize == *base.block.OutQueueSize &&
		state.block.Block.HashKeyAt(0) == base.block.Block.HashKeyAt(0) &&
		state.block.State.HashKeyAt(0) == base.block.State.HashKeyAt(0)
}

func (a *LocalAcquisition) RetireSession(ctx context.Context, sessionID [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.RLock()
	managed := a.sessions[sessionID]
	a.mu.RUnlock()
	if managed == nil {
		return nil
	}
	managed.mu.Lock()
	if managed.retired {
		managed.mu.Unlock()
		return nil
	}
	managed.branch.Close()
	managed.retired = true
	managed.seeds = nil
	managed.candidates = nil
	managed.blocks = nil
	managed.mu.Unlock()
	a.cancelSessionDispatchPrewarms(managed.dispatchPrewarmOwner())

	a.mu.Lock()
	if a.sessions[sessionID] == managed {
		delete(a.sessions, sessionID)
	}
	a.mu.Unlock()

	return nil
}

func (a *LocalAcquisition) stageSession(session Session, update SessionUpdate) (*localAcquisitionSession, error) {
	if err := validateSessionRecord(SessionRecord{Session: session, Update: update}); err != nil {
		return nil, err
	}

	return &localAcquisitionSession{
		session:                   cloneSession(session),
		update:                    cloneSessionUpdate(update),
		dispatchPrewarmGeneration: a.dispatchPrewarmGeneration.Add(1),
		seeds:                     make(map[uint32][32]byte),
		candidates:                make(map[simplex.CandidateID]localCandidateState),
		blocks:                    make(map[[32]byte]localCandidateState),
	}, nil
}

func (a *LocalAcquisition) stageMaster(
	ctx context.Context,
	session ActivatedSession,
	update SessionUpdate,
	requireActive bool,
	installed *localMasterView,
) (*groups.Snapshot, *localMasterView, error) {
	// A session update can race an immediately newer applied masterchain
	// block. Its own MasterchainBlock still pins the view it must use, and the
	// installed view is the immutable exact snapshot for that block.
	if installed != nil && update.MasterchainBlock.Equals(&installed.context.ID) {
		if err := installed.requireAncestor(session.MinMasterchain); err != nil {
			return nil, nil, err
		}
		if err := validateAcquisitionGroup(session, update, installed.context.Groups, requireActive); err != nil {
			return nil, nil, err
		}

		return installed.context.Groups, installed, nil
	}
	master, err := a.projectedMasterView(
		ctx,
		update.MasterchainBlock,
		time.Now(),
		acquisitionReadImmediate,
	)
	if err != nil {
		return nil, nil, err
	}
	snapshot := master.context.Groups
	if err = master.requireAncestor(session.MinMasterchain); err != nil {
		return nil, nil, err
	}
	if err = validateAcquisitionGroup(session, update, snapshot, requireActive); err != nil {
		return nil, nil, err
	}

	return snapshot, master, nil
}

func (a *LocalAcquisition) session(id [32]byte) (*localAcquisitionSession, error) {
	a.mu.RLock()
	managed := a.sessions[id]
	a.mu.RUnlock()
	if managed == nil {
		return nil, ErrNotFound
	}

	return managed, nil
}

func validateAcquisitionGroup(
	session ActivatedSession,
	update SessionUpdate,
	snapshot *groups.Snapshot,
	requireActive bool,
) error {
	var matched *groups.Session
	for i := range snapshot.Active {
		if snapshot.Active[i].ID == session.ID {
			matched = &snapshot.Active[i]
			break
		}
	}
	active := matched != nil
	if matched == nil && !requireActive {
		for i := range snapshot.Future {
			if snapshot.Future[i].ID == session.ID {
				matched = &snapshot.Future[i]
				break
			}
		}
	}
	if matched == nil {
		return fmt.Errorf("%w: session is absent from the exact validator group snapshot", ErrAcquisitionNotReady)
	}
	if err := requireSameSessionIdentity(session, matched); err != nil {
		return err
	}
	if active {
		if !equalShardDescriptions(update.Registered, matched.Registered) {
			return fmt.Errorf("%w: registered shard view differs from the group snapshot", ErrInvalidInput)
		}
		// A split or merge target may have no exact descriptor in this
		// masterchain state. C++ skips notify_mc_finalized in that case, so nil
		// leaves the session's last authenticated observation unchanged.
		if matched.FinalizedBlock != nil && (!update.HasFinalizedBlock ||
			!matched.FinalizedBlock.Equals(&update.FinalizedBlock)) {
			return fmt.Errorf("%w: finalized block differs from the group snapshot", ErrInvalidInput)
		}
	}

	return nil
}

// requireSameSessionIdentity compares the fields a running session may never
// see change under it: shard, catchain seqno, genesis, minimum masterchain
// block and every roster entry. They are exactly the fields the candidate
// header is stamped from out of the snapshot the build binds to, so a snapshot
// that fails this may not be adopted by a producer that has already emitted
// blocks bound to another one.
//
// Split out of validateAcquisitionGroup so the per-slot masterchain view pick
// can ask the identity question about a snapshot NEWER than the one the session
// update pinned — where the rest of that function, which compares the frozen
// update's registered and finalized view against the snapshot, is by
// construction the wrong question.
func requireSameSessionIdentity(session ActivatedSession, matched *groups.Session) error {
	if matched.Shard != session.Shard || matched.CatchainSeqno != session.CatchainSeqno ||
		!equalBlockIDs(matched.Genesis, session.Genesis) ||
		!matched.MinMasterchain.Equals(&session.MinMasterchain) ||
		len(matched.Validators) != len(session.Validators) {
		return fmt.Errorf("%w: session differs from the validator group snapshot", ErrInvalidInput)
	}
	for i := range session.Validators {
		left, right := session.Validators[i], matched.Validators[i]
		if left.PublicKey != right.PublicKey || left.ADNLID != groups.ValidatorADNL(right) || left.Weight != right.Weight {
			return fmt.Errorf("%w: session validator %d differs from the group roster", ErrInvalidInput, i)
		}
	}

	return nil
}

func equalShardDescriptions(left, right []groups.ShardDescription) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		a, b := left[i], right[i]
		if a.Shard != b.Shard || !a.Block.Equals(&b.Block) ||
			a.NextCatchainSeqno != b.NextCatchainSeqno || a.BeforeSplit != b.BeforeSplit ||
			a.BeforeMerge != b.BeforeMerge || a.FSM != b.FSM {
			return false
		}
	}

	return true
}

func (a *LocalAcquisition) requestHeader(request BuildRequest) (HeaderParams, error) {
	// A speculative build is for the window after the one the session is in, so
	// the slot offset below would measure from the wrong start. The caller
	// supplies the instant instead — the same one the observed window computes
	// from the parent's generation time — and the block is stamped with it.
	// clampLocalHeaderTime still raises it to the predecessor and masterchain
	// bounds afterwards, which is what keeps an estimate that turns out early
	// from producing a block the reference rejects as non-monotonic.
	if spec := request.speculative; spec != nil {
		return headerParamsAt(spec.at)
	}
	// The session-start bet is built before any window exists: its instant is
	// the activation, and clampLocalHeaderTime raises it past the genesis and
	// the masterchain just as it does for a speculative build. On the stand
	// every such bet failed here for a night — the header wanted a window the
	// session had not observed yet — and window zero kept being collated only
	// once the engine had started.
	if !request.sessionStartAt.IsZero() {
		return headerParamsAt(request.sessionStartAt)
	}
	if !request.Update.HasCurrentWindow || request.Slot < request.Update.CurrentWindowStart {
		return HeaderParams{}, fmt.Errorf("%w: build slot is outside the current window", ErrInvalidInput)
	}
	offset := uint64(request.Slot - request.Update.CurrentWindowStart)
	rate := uint64(request.Update.TargetRate)
	if offset != 0 && rate > math.MaxInt64/offset {
		return HeaderParams{}, fmt.Errorf("%w: slot time overflows", ErrInvalidInput)
	}
	return headerParamsAt(request.Update.CurrentWindowStartAt.Add(time.Duration(offset * rate)))
}

// headerParamsAt is the slot instant expressed as the two header fields the
// protocol carries. Both come from one instant so gen_utime_ms/1000 == gen_utime
// by construction, which the reference validator checks (validate-query.cpp:2464).
func headerParamsAt(at time.Time) (HeaderParams, error) {
	seconds := at.Unix()
	milliseconds := at.UnixMilli()
	if seconds < 0 || seconds > math.MaxUint32 || milliseconds < 0 {
		return HeaderParams{}, fmt.Errorf("%w: candidate time is outside the protocol range", ErrInvalidInput)
	}

	return HeaderParams{GenUtime: uint32(seconds), GenUtimeMS: uint64(milliseconds)}, nil
}

// clampLocalHeaderTime mirrors Collator::init_utime: consensus supplies the
// preferred slot time, while the authenticated predecessor and masterchain
// states supply its lower bound. This belongs to local acquisition rather than
// Builder so externally supplied requests still have to satisfy the protocol
// checks without being silently rewritten.
func clampLocalHeaderTime(
	header HeaderParams,
	globalVersion uint32,
	minGenUtime uint32,
) (HeaderParams, error) {
	bound := uint64(minGenUtime)
	if globalVersion < 13 {
		bound++
	}
	if bound > math.MaxUint32 {
		return HeaderParams{}, fmt.Errorf("%w: candidate time exceeds the protocol range", ErrInvalidInput)
	}
	boundMS := bound * 1000
	if header.GenUtimeMS >= boundMS {
		return header, nil
	}
	header.GenUtime = uint32(bound)
	header.GenUtimeMS = boundMS

	return header, nil
}

func (a *LocalAcquisition) requestSeed(managed *localAcquisitionSession, slot uint32) ([32]byte, error) {
	if seed, exists := managed.seeds[slot]; exists {
		return seed, nil
	}
	var seed [32]byte
	a.randomMu.Lock()
	if _, err := io.ReadFull(a.random, seed[:]); err != nil {
		a.randomMu.Unlock()
		return [32]byte{}, fmt.Errorf("generate candidate random seed: %w", err)
	}
	a.randomMu.Unlock()
	managed.seeds[slot] = seed

	return seed, nil
}

func (s *localAcquisitionSession) pruneCandidates(base simplex.ParentID) {
	if !base.Exists {
		clear(s.candidates)
		clear(s.blocks)
		return
	}
	state, exists := s.candidates[base.ID]
	if !exists {
		clear(s.candidates)
		clear(s.blocks)
		return
	}

	key, err := blockRootKey(state.block.ID)
	if err != nil {
		clear(s.candidates)
		clear(s.blocks)

		return
	}
	for id, candidate := range s.candidates {
		if id == base.ID {
			continue
		}
		if sameBlockID(candidate.block.ID, state.block.ID) || candidate.queueTip == nil ||
			!s.branch.HasCandidate(*candidate.queueTip) {
			delete(s.candidates, id)
		}
	}
	for root, candidate := range s.blocks {
		if root == key {
			continue
		}
		if candidate.queueTip == nil || !s.branch.HasCandidate(*candidate.queueTip) {
			delete(s.blocks, root)
		}
	}
	s.candidates[base.ID] = state
	s.blocks[key] = state
}

func targetShardIdent(shard groups.ShardID) msgpool.ShardIdent {
	return msgpool.ShardIdentFrom(shard.Workchain, shard.Shard)
}
