package collator

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
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
	OpenExternalSnapshot(msgpool.ShardIdent, int) (*msgpool.ExternalStream, error)
	Complete([]msgpool.ExternalFeedback) error
	Internals() *msgpool.Internals
}

type LocalShardTopSource interface {
	Select(context.Context, ShardTopSelection) ([]ShardTop, error)
}

type LocalAcquisitionOptions struct {
	Builder             *Builder
	Logger              zerolog.Logger
	CollationObserver   CollationObserver
	ValidationObserver  ValidationCoreObserver
	Store               LocalStateStore
	Groups              LocalGroupSource
	Messages            LocalMessageSource
	ShardTops           LocalShardTopSource
	Semantics           CandidateTransitionVerifier
	Random              io.Reader
	ExternalLimit       int
	MaxExternalAttempts int
	Dispatch            *DispatchPolicy
}

// LocalAcquisition materializes exact Builder requests from the node's local
// authenticated state. Independent sessions do not share a build lock.
type LocalAcquisition struct {
	builder            *Builder
	log                zerolog.Logger
	collationObserver  CollationObserver
	validationObserver ValidationCoreObserver
	store              LocalStateStore
	groups             LocalGroupSource
	messages           LocalMessageSource
	shardTops          LocalShardTopSource
	semantics          CandidateTransitionVerifier
	random             io.Reader
	randomMu           sync.Mutex

	externalLimit       int
	maxExternalAttempts int
	dispatch            DispatchPolicy

	configs localConfigCache
	master  localMasterViewCache
	blocks  localBlockCache

	mu       sync.RWMutex
	sessions map[[32]byte]*localAcquisitionSession
}

type localMasterViewCache struct {
	mu   sync.RWMutex
	view *localMasterView
}

type localAcquisitionSession struct {
	mu sync.Mutex

	session    Session
	branch     *msgpool.Branch
	activation *SessionActivation
	update     SessionUpdate
	master     *localMasterView
	retired    bool

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
		builder:             options.Builder,
		log:                 options.Logger,
		collationObserver:   options.CollationObserver,
		validationObserver:  options.ValidationObserver,
		store:               options.Store,
		groups:              options.Groups,
		messages:            options.Messages,
		shardTops:           options.ShardTops,
		semantics:           options.Semantics,
		random:              options.Random,
		externalLimit:       options.ExternalLimit,
		maxExternalAttempts: options.MaxExternalAttempts,
		dispatch:            dispatch,
		configs:             localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)},
		blocks:              localBlockCache{entries: make(map[[32]byte]*localBlockSource)},
		sessions:            make(map[[32]byte]*localAcquisitionSession),
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

	return nil
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
	managed.branch.Close()
	managed.retired = true
	managed.seeds = nil
	managed.candidates = nil
	managed.blocks = nil
	managed.mu.Unlock()

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
		session:    cloneSession(session),
		update:     cloneSessionUpdate(update),
		seeds:      make(map[uint32][32]byte),
		candidates: make(map[simplex.CandidateID]localCandidateState),
		blocks:     make(map[[32]byte]localCandidateState),
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
	master, err := a.projectedMasterView(ctx, update.MasterchainBlock, time.Now())
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
	if !request.Update.HasCurrentWindow || request.Slot < request.Update.CurrentWindowStart {
		return HeaderParams{}, fmt.Errorf("%w: build slot is outside the current window", ErrInvalidInput)
	}
	offset := uint64(request.Slot - request.Update.CurrentWindowStart)
	rate := uint64(request.Update.TargetRate)
	if offset != 0 && rate > math.MaxInt64/offset {
		return HeaderParams{}, fmt.Errorf("%w: slot time overflows", ErrInvalidInput)
	}
	at := request.Update.CurrentWindowStartAt.Add(time.Duration(offset * rate))
	seconds := at.Unix()
	milliseconds := at.UnixMilli()
	if seconds < 0 || seconds > math.MaxUint32 || milliseconds < 0 {
		return HeaderParams{}, fmt.Errorf("%w: candidate time is outside the protocol range", ErrInvalidInput)
	}

	return HeaderParams{GenUtime: uint32(seconds), GenUtimeMS: uint64(milliseconds)}, nil
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

	clear(s.candidates)
	clear(s.blocks)
	s.candidates[base.ID] = state
	if key, err := blockRootKey(state.block.ID); err == nil {
		s.blocks[key] = state
	}
}

func targetShardIdent(shard groups.ShardID) msgpool.ShardIdent {
	return msgpool.ShardIdentFrom(shard.Workchain, shard.Shard)
}
