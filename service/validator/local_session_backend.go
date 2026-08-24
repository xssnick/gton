package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	defaultLocalSessionCloseTimeout = 10 * time.Second
	delegationDeliveryTimeout       = 500 * time.Millisecond
)

var ErrLocalSessionBackendClosed = errors.New("validator local backend: closed")

// LocalSessionNode is the cohesive local-node boundary required by a complete
// validator session backend. Reads resolve exact authenticated chain inputs;
// local submission enters the ordinary block verification and apply pipeline.
type LocalSessionNode interface {
	BlockData(context.Context, ton.BlockIDExt) ([]byte, error)
	BlockProof(context.Context, storage.ServedProofKind, ton.BlockIDExt) ([]byte, error)
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
	LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error)
	SubmitBlockLocally(p2p.DownloadedBlock)
	// PublishAcceptedBlockState extends the live view with a block this session
	// has finalized and the state it computed for it, before either reaches the
	// node database. It is the other half of every read above: those reads go
	// through the live view, and this is what puts into the live view the one
	// thing it could not otherwise answer for — the state of a shard block only
	// this node has finalized so far.
	PublishAcceptedBlockState(storage.LiveBlockArtifacts) error
	// BlockArtifactsSignal is the publication edge a read waits on. It replaces
	// polling the store: the caller takes the channel before its read and blocks
	// on it only if that read found nothing.
	BlockArtifactsSignal() <-chan struct{}
}

// LocalSessionBackendOptions contains the shared node services and immutable
// inputs for one prepared consensus runtime. Acquisition and Collator are
// required only for a voting identity. ProductionMode is explicit for voting
// sessions: self production requires CandidateRouter, delegated production
// forbids it.
type LocalSessionBackendOptions struct {
	Config  SessionConfig
	Initial SessionState
	Node    LocalSessionNode
	// Groups supplies the newest applied masterchain shard registry for
	// TopBlockDescr construction. It is required when a finalized shard block
	// is accepted; the session update may lag this applied view.
	Groups          collator.LocalGroupSource
	Storage         ValidatorStorage
	Delegations     DelegationAuthorizationStorage
	Acquisition     *collator.LocalAcquisition
	Collator        collator.Collator
	ProductionMode  collator.ProductionMode
	CandidateRouter *LocalCandidateRouter
	// ConsensusFinalized receives a final-certificate block observation. It is
	// used by a standalone observer; an in-process collator is wired directly.
	ConsensusFinalized func(context.Context, ton.BlockIDExt) error
	Publisher          BlockPublisher
	ShardTops          *collator.ShardTopInbox
	// Metrics is the process-wide validation observer. It is optional and used
	// here only for the predecessor-wait backstop alarm, which has no other place
	// to report from: the wait happens inside a backend read, below every session
	// runtime that carries an observer of its own.
	Metrics ValidationObserver
	Logger  zerolog.Logger
	// CloseTimeout bounds permanent retirement because SessionBackend.Retire
	// has no caller context. Zero uses the fixed ten-second lifecycle bound.
	CloseTimeout time.Duration
}

// localConsensusProgressCollator is intentionally narrower than Collator: a
// remote collator owns this state itself, while the in-process Service must
// receive the authenticated Simplex progression before it can produce.
type localConsensusProgressCollator interface {
	ApplyConsensusProgress(context.Context, collator.ConsensusProgress) error
	ObserveConsensusFinalized(context.Context, [32]byte, ton.BlockIDExt) error
}

type localSelfWindowCollator interface {
	ActivateSelfWindow(context.Context, collator.SelfWindowRequest) error
	SpeculateSelfWindow(context.Context, collator.SpeculativeWindowRequest) error
}

type localWindowRoute struct {
	id     collator.WindowID
	end    uint32
	submit CandidateSubmitter
}

type localValidationView struct {
	session collator.ActivatedSession
	update  collator.SessionUpdate
}

type localDeferredWindow struct {
	window  simplex.Window
	startAt time.Time
}

// LocalSessionBackend binds one Simplex runtime to local authenticated state,
// validation, acceptance, and either an in-process or remote Collator.
type LocalSessionBackend struct {
	config         SessionConfig
	session        collator.Session
	activation     *collator.SessionActivation
	node           LocalSessionNode
	groups         collator.LocalGroupSource
	delegations    DelegationAuthorizationStorage
	acquisition    *collator.LocalAcquisition
	collator       collator.Collator
	productionMode collator.ProductionMode
	progress       localConsensusProgressCollator
	self           localSelfWindowCollator
	finalized      func(context.Context, ton.BlockIDExt) error
	accepter       *BlockAccepter
	metrics        ValidationObserver
	log            zerolog.Logger
	closeAfter     time.Duration
	// waitBackstop is chainTipWaitBackstop, overridden only by this package's
	// own tests. A wall-clock alarm sized for the field is not something a test
	// can wait out, and the property that needs a test — that the alarm is NOT
	// restarted by an unrelated publication — is invisible without driving the
	// loop across several wake-ups.
	waitBackstop     time.Duration
	validator        *ValidatorIdentity
	validation       atomic.Pointer[localValidationView]
	collatorDeferred atomic.Bool

	controlMu         sync.Mutex
	validationChanged chan struct{}
	// collatorReady is closed once deferred producer recovery resolves. Waiters
	// then recheck whether it resolved as ready, terminally unavailable, or
	// closed. The channel is initialized lazily for test-built backends; after
	// construction its field and Once are owned under controlMu.
	collatorReady     chan struct{}
	collatorReadyOnce sync.Once
	state             SessionState
	update            collator.SessionUpdate
	// pendingWindow remembers the authenticated window observed while an
	// ahead recovered producer waits for the node's masterchain view. Its exact
	// selected-base capability is deliberately not retained across that state
	// change: successful catch-up asks the runtime to recheck ancestry and send
	// a newly bound capability instead.
	// Guarded by controlMu.
	pendingWindow *localDeferredWindow
	// appliedWindow is the latest selected-base proof accepted by the in-process
	// producer. It becomes stale when the finalized anchor advances, so
	// UpdateSession can turn it into recheckWindow atomically with the new chain
	// view. Delegated/observer progress is owned by its remote consumer and does
	// not participate in this local handshake.
	appliedWindow *localDeferredWindow
	// handledWindow aliases the appliedWindow generation whose matching
	// HandleLeaderWindow successfully crossed the producer boundary. A later
	// finalized block must not invalidate that running multi-slot snapshot.
	handledWindow *localDeferredWindow
	// recheckWindow records the consensus window whose selected-base proof must
	// be checked again after producer catch-up, or after the finalized anchor
	// advances in the gap before HandleLeaderWindow. Every matching attempt is
	// rejected until ObserveConsensusProgress installs a freshly bound
	// capability for this or a newer window.
	recheckWindow *localDeferredWindow
	// collatorUnavailable permanently disables production for this runtime
	// attachment after the collator reports a terminal lifecycle failure.
	// Consensus validation keeps using the authenticated session updates; only
	// retirement may reset the collator, matching C++ producer-task isolation.
	collatorUnavailable bool
	closed              bool
	collatorRetired     bool
	releaseRoute        func()

	routeMu sync.RWMutex
	window  *localWindowRoute
}

var _ SessionBackend = (*LocalSessionBackend)(nil)

// LocalSessionBackendPreparation is the atomic local-backend construction
// result. RuntimeState keeps the node's applied masterchain view while carrying
// forward the exact session-local finalized anchor recovered by the producer.
// The runtime must start from this state so its ancestry guard cannot be weaker
// than the durable producer view.
type LocalSessionBackendPreparation struct {
	Backend      *LocalSessionBackend
	RuntimeState SessionState
}

func PrepareLocalSessionBackend(
	ctx context.Context,
	options LocalSessionBackendOptions,
) (LocalSessionBackendPreparation, error) {
	if err := ctx.Err(); err != nil {
		return LocalSessionBackendPreparation{}, err
	}
	if options.Node == nil {
		return LocalSessionBackendPreparation{}, errors.New("validator local backend: node is required")
	}
	if !options.Config.Shard.IsMasterchain() && options.Groups == nil {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: validator-group source is required for a shard session",
		)
	}
	if options.CloseTimeout < 0 {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: close timeout must not be negative",
		)
	}
	if options.CloseTimeout == 0 {
		options.CloseTimeout = defaultLocalSessionCloseTimeout
	}

	initial := cloneSessionState(options.Initial)
	if initial.Params == (simplex.Params{}) {
		initial.Params = simplex.DefaultParams()
	}
	session := localCollatorSession(options.Config)
	update := localCollatorUpdate(options.Config.SessionID, initial)
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    options.Config,
		Node:      options.Node,
		Publisher: options.Publisher,
		ShardTops: options.ShardTops,
		Logger:    options.Logger,
	})
	if err != nil {
		return LocalSessionBackendPreparation{}, err
	}

	backend := &LocalSessionBackend{
		config:            options.Config,
		session:           session,
		node:              options.Node,
		groups:            options.Groups,
		delegations:       options.Delegations,
		acquisition:       options.Acquisition,
		collator:          options.Collator,
		productionMode:    options.ProductionMode,
		accepter:          accepter,
		metrics:           options.Metrics,
		finalized:         options.ConsensusFinalized,
		log:               options.Logger,
		closeAfter:        options.CloseTimeout,
		validator:         options.Config.Identity.Validator,
		validationChanged: make(chan struct{}),
		collatorReady:     make(chan struct{}),
		state:             initial,
		update:            update,
	}
	if backend.validator == nil {
		backend.signalCollatorReady()

		return LocalSessionBackendPreparation{
			Backend:      backend,
			RuntimeState: cloneSessionState(backend.state),
		}, nil
	}
	if backend.validator.Signer == nil {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: validator signer is required",
		)
	}
	if int(backend.validator.Index) >= len(options.Config.Validators) {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: validator index is outside the roster",
		)
	}
	if options.Acquisition == nil {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: local acquisition is required for a validator",
		)
	}
	if options.Storage == nil {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: validator storage is required for a validator",
		)
	}
	if options.Collator == nil {
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: collator is required for a validator",
		)
	}
	switch options.ProductionMode {
	case collator.ProductionModeSelf:
		if options.CandidateRouter == nil {
			return LocalSessionBackendPreparation{}, errors.New(
				"validator local backend: self production requires a candidate router",
			)
		}
		progress, ok := options.Collator.(localConsensusProgressCollator)
		if !ok {
			return LocalSessionBackendPreparation{}, errors.New(
				"validator local backend: in-process collator does not accept consensus progress",
			)
		}
		self, ok := options.Collator.(localSelfWindowCollator)
		if !ok {
			return LocalSessionBackendPreparation{}, errors.New(
				"validator local backend: in-process collator does not accept self windows",
			)
		}
		backend.progress = progress
		backend.self = self
		backend.finalized = func(ctx context.Context, block ton.BlockIDExt) error {
			return progress.ObserveConsensusFinalized(ctx, options.Config.SessionID, block)
		}
		backend.releaseRoute, err = options.CandidateRouter.Register(
			options.Config.SessionID,
			backend.routeCandidate,
		)
		if err != nil {
			return LocalSessionBackendPreparation{}, fmt.Errorf(
				"validator local backend: register candidate route: %w",
				err,
			)
		}
	case collator.ProductionModeDelegated:
		if options.CandidateRouter != nil {
			return LocalSessionBackendPreparation{}, errors.New(
				"validator local backend: delegated production forbids a candidate router",
			)
		}
		if options.Delegations == nil {
			return LocalSessionBackendPreparation{}, errors.New(
				"validator local backend: delegated production requires authorization storage",
			)
		}
	default:
		return LocalSessionBackendPreparation{}, errors.New(
			"validator local backend: production mode is invalid",
		)
	}
	preparation, err := prepareLocalCollatorSession(ctx, options.Collator, session, update)
	if err != nil {
		if backend.releaseRoute != nil {
			backend.releaseRoute()
		}
		return LocalSessionBackendPreparation{}, err
	}
	backend.update = preparation.update
	if preparation.update.HasFinalizedBlock {
		finalized := *preparation.update.FinalizedBlock.Copy()
		backend.state.FinalizedBlock = &finalized
	}
	backend.collatorDeferred.Store(!preparation.ready)
	if preparation.ready {
		backend.signalCollatorReady()
	}

	return LocalSessionBackendPreparation{
		Backend:      backend,
		RuntimeState: cloneSessionState(backend.state),
	}, nil
}

// ObserveConsensusProgress publishes the exact authenticated Simplex view to
// an in-process collator before the leader-window delegation reaches it. A
// remote collator receives that progression in its own observer and therefore
// leaves progress nil here.
func (b *LocalSessionBackend) ObserveConsensusProgress(
	ctx context.Context,
	progress sessionConsensusProgress,
) error {
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return ErrLocalSessionBackendClosed
	}
	if b.progress == nil {
		applyConsensusWindow(&b.update, progress.Window, progress.StartAt)
		b.publishValidationView()

		return nil
	}
	if b.collatorDeferred.Load() {
		b.pendingWindow = &localDeferredWindow{
			window:  progress.Window,
			startAt: progress.StartAt,
		}

		return nil
	}

	converted, err := collatorConsensusProgress(
		b.config.SessionID,
		progress,
	)
	if err != nil {
		return fmt.Errorf("validator local backend: convert consensus progress: %w", err)
	}
	if b.collatorUnavailable {
		applyConsensusWindow(&b.update, progress.Window, progress.StartAt)
		b.publishValidationView()

		return nil
	}
	if err := b.progress.ApplyConsensusProgress(ctx, converted); err != nil {
		if errors.Is(err, collator.ErrSessionUnavailable) {
			b.quarantineCollator(&b.update)
			applyConsensusWindow(&b.update, progress.Window, progress.StartAt)
			b.publishValidationView()

			return nil
		}

		return fmt.Errorf("validator local backend: apply consensus progress: %w", err)
	}

	b.applyObservedConsensusWindow(progress)

	return nil
}

// ActivateSession binds the exact consensus start to the already prepared
// tentative collator session. Observers have no local production backend, but
// still validate the start through the consensus runtime state resolver.
func (b *LocalSessionBackend) ActivateSession(ctx context.Context, start SessionStart) error {
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return ErrLocalSessionBackendClosed
	}

	activation := localCollatorActivation(b.config.SessionID, start)
	if b.activation != nil {
		if b.activation.Equal(activation) {
			return nil
		}

		return collator.ErrSessionConflict
	}
	if b.validator != nil && !b.collatorDeferred.Load() && !b.collatorUnavailable {
		if err := b.collator.ActivateSession(ctx, activation); err != nil {
			if !errors.Is(err, collator.ErrSessionUnavailable) {
				return fmt.Errorf("validator local backend: activate collator session: %w", err)
			}

			b.quarantineCollator(&b.update)
		}
	}

	b.activation = &activation
	b.publishValidationView()
	return nil
}

func (b *LocalSessionBackend) UpdateSession(ctx context.Context, state SessionState) error {
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return ErrLocalSessionBackendClosed
	}

	state = cloneSessionState(state)
	if state.Params == (simplex.Params{}) {
		state.Params = simplex.DefaultParams()
	}
	next := localCollatorUpdate(b.config.SessionID, state)
	next.PreserveProgress(b.update)
	if err := mergeLocalSessionFinalized(b.config.Shard, &next, b.update); err != nil {
		return fmt.Errorf("validator local backend: merge finalized progress: %w", err)
	}
	if next.HasFinalizedBlock {
		finalized := *next.FinalizedBlock.Copy()
		state.FinalizedBlock = &finalized
	} else {
		state.FinalizedBlock = nil
	}
	finalizedAdvanced := next.HasFinalizedBlock &&
		(!b.update.HasFinalizedBlock || next.FinalizedBlock.SeqNo > b.update.FinalizedBlock.SeqNo)
	if b.validator != nil {
		if b.collatorUnavailable {
			b.state = state
			b.update = next
			b.publishValidationView()

			return nil
		}
		if b.collatorDeferred.Load() && b.update.MasterchainBlock.SeqNo > next.MasterchainBlock.SeqNo {
			if finalizedAdvanced {
				b.markAppliedWindowRecheck()
			}
			b.state = state

			return nil
		}
		if err := b.collator.UpdateSession(ctx, next); err != nil {
			if !errors.Is(err, collator.ErrSessionUnavailable) {
				return fmt.Errorf("validator local backend: update collator session: %w", err)
			}

			b.quarantineCollator(&next)
		} else if b.collatorDeferred.Load() && b.activation != nil {
			if err := b.collator.ActivateSession(ctx, *b.activation); err != nil {
				if !errors.Is(err, collator.ErrSessionUnavailable) {
					return fmt.Errorf("validator local backend: activate recovered collator session: %w", err)
				}

				b.quarantineCollator(&next)
			}
		}
		if !b.collatorUnavailable && finalizedAdvanced {
			b.markAppliedWindowRecheck()
		}
		if b.collatorDeferred.Load() && b.pendingWindow != nil {
			b.markWindowRecheck(b.pendingWindow)
			b.pendingWindow = nil
		}
		b.endCollatorDefer()
	}

	b.state = state
	b.update = next
	b.publishValidationView()
	return nil
}

// quarantineCollator isolates a terminal producer failure without discarding
// the authenticated consensus view needed by local validation. Callers hold
// controlMu and commit update after this returns.
func (b *LocalSessionBackend) quarantineCollator(update *collator.SessionUpdate) {
	b.collatorUnavailable = true
	b.clearWindowRoute(0)
	deferred := b.pendingWindow
	if b.recheckWindow != nil &&
		(deferred == nil || windowProgressAdvances(deferred.window, b.recheckWindow.window)) {
		deferred = b.recheckWindow
	}
	if deferred != nil {
		applyConsensusWindow(update, deferred.window, deferred.startAt)
	}
	b.pendingWindow = nil
	b.appliedWindow = nil
	b.handledWindow = nil
	b.recheckWindow = nil
	b.endCollatorDefer()
}

// applyObservedConsensusWindow commits one freshly checked selected-base view.
// Callers hold controlMu. A newer successful view discharges any older proof
// debt; failed and deferred observations never reach this helper.
func (b *LocalSessionBackend) applyObservedConsensusWindow(progress sessionConsensusProgress) {
	applyConsensusWindow(&b.update, progress.Window, progress.StartAt)
	if b.appliedWindow == nil || windowProgressAdvances(b.appliedWindow.window, progress.Window) {
		previous := b.appliedWindow
		wasHandled := previous != nil && b.handledWindow == previous && previous.window == progress.Window
		b.appliedWindow = &localDeferredWindow{
			window:  progress.Window,
			startAt: progress.StartAt,
		}
		if wasHandled {
			b.handledWindow = b.appliedWindow
		}
	}
	if b.recheckWindow != nil && windowProgressAdvances(b.recheckWindow.window, progress.Window) {
		b.recheckWindow = nil
	}
	b.publishValidationView()
}

// markWindowRecheck preserves the newest selected-base proof debt. The
// metadata is immutable under controlMu, so sharing the pointer does not copy
// the Window or create an ownership edge.
func (b *LocalSessionBackend) markWindowRecheck(window *localDeferredWindow) {
	if window == nil {
		return
	}
	if b.recheckWindow == nil || windowProgressAdvances(b.recheckWindow.window, window.window) {
		b.recheckWindow = window
	}
}

func windowProgressAdvances(current, next simplex.Window) bool {
	if next.StartSlot != current.StartSlot {
		return next.StartSlot > current.StartSlot
	}
	if next.ObservedSlot != current.ObservedSlot {
		return next.ObservedSlot > current.ObservedSlot
	}

	return next == current
}

func (b *LocalSessionBackend) markAppliedWindowRecheck() {
	if b.appliedWindow == nil || b.handledWindow == b.appliedWindow {
		return
	}

	b.markWindowRecheck(b.appliedWindow)
}

func (b *LocalSessionBackend) markWindowHandled(window simplex.Window) {
	if b.appliedWindow != nil && b.appliedWindow.window == window {
		b.handledWindow = b.appliedWindow
	}
}

// collatorReadinessLocked returns the one lifecycle signal awaited by windows
// that arrive during recovered producer catch-up. Callers hold controlMu.
func (b *LocalSessionBackend) collatorReadinessLocked() <-chan struct{} {
	if b.collatorReady == nil {
		b.collatorReady = make(chan struct{})
	}

	return b.collatorReady
}

// signalCollatorReady wakes every deferred window exactly once. It is called
// under controlMu after construction; constructor calls happen before publish.
func (b *LocalSessionBackend) signalCollatorReady() {
	b.collatorReadyOnce.Do(func() {
		if b.collatorReady == nil {
			b.collatorReady = make(chan struct{})
		}
		close(b.collatorReady)
	})
}

// endCollatorDefer publishes the state transition before waking window waiters.
// The caller keeps controlMu through the matching state/update commit.
func (b *LocalSessionBackend) endCollatorDefer() {
	b.collatorDeferred.Store(false)
	b.signalCollatorReady()
}

func (b *LocalSessionBackend) LoadChainState(
	ctx context.Context,
	request ChainStateRequest,
) (ChainStateData, error) {
	if err := ctx.Err(); err != nil {
		return ChainStateData{}, err
	}

	data := ChainStateData{Tips: make([]ChainTip, len(request.Blocks))}
	for i := range request.Blocks {
		tip, err := b.loadChainTip(ctx, request.Blocks[i], request.Wait)
		if err != nil {
			return ChainStateData{}, err
		}
		data.Tips[i] = tip
	}

	return data, nil
}

func (b *LocalSessionBackend) ValidateCandidate(
	ctx context.Context,
	request CandidateValidationRequest,
) (CandidateValidation, error) {
	state, artifact := request.Parent, request.Artifact
	if state == nil || artifact == nil {
		return CandidateValidation{}, errors.New("validator local backend: candidate validation request is incomplete")
	}
	if b.validator == nil {
		return CandidateValidation{}, errors.New("validator local backend: observer cannot validate candidates")
	}
	view, err := b.waitValidationView(ctx)
	if err != nil {
		return CandidateValidation{}, err
	}

	// No predecessor BOC is decoded here any more: every producer of a tip —
	// the store loader, the applied path and the validated path — hands over the
	// root it already parsed. A missing one is a local plumbing fault and is
	// reported as such, because the alternative is silently skipping the block
	// half of the collator's predecessor verification.
	previous := make([]collator.PreviousBlock, len(state.tips))
	for i := range state.tips {
		tip := &state.tips[i]
		if tip.ID.SeqNo != 0 && tip.Block == nil {
			return CandidateValidation{}, fmt.Errorf(
				"validator local backend: predecessor %d carries no parsed block",
				tip.ID.SeqNo,
			)
		}
		previous[i] = collator.PreviousBlock{ID: *tip.ID.Copy(), State: tip.State, Block: tip.Block}
	}

	// The announcement is offered on every call and taken only by the one path
	// that needs it: a shard candidate with full collated data, whose
	// verification runs on the candidate's own proof and so never produces a
	// successor of the parent this node holds. There the collator calls back
	// once it has passed every stage a lagging node retries on, and the apply
	// over the full live parent — the one the collator must never see — overlaps
	// the semantic replay instead of following it. That matters in exactly the
	// case that costs disk reads: a parent reloaded from the node store after
	// the finalized-watermark sweep released it.
	//
	// On every other path the collator applied the update to these very cells
	// and hands the result back, so nothing is announced, no goroutine starts
	// and the same tree is not walked twice.
	pending := request.successor
	var blockRoot *cell.Cell
	var collatedRoots []*cell.Cell
	if artifact.validationRoots != nil {
		blockRoot = artifact.validationRoots.block
		collatedRoots = artifact.validationRoots.collated
	}
	result, err := b.acquisition.ValidateCandidate(ctx, collator.ValidationRequest{
		Session:            view.session,
		Update:             view.update,
		Previous:           previous,
		Candidate:          artifact.Candidate,
		BlockBOC:           artifact.BlockBOC,
		CollatedData:       artifact.CollatedData,
		Digested:           artifact.digested,
		BlockRoot:          blockRoot,
		CollatedRoots:      collatedRoots,
		AnnounceTransition: pending.announce,
	})
	if err != nil {
		return CandidateValidation{}, classifyLocalCandidateError(err)
	}

	next, err := state.validatedCandidateState(artifact, CandidateSuccessor{
		BlockRoot:   result.Successor.BlockRoot,
		StateUpdate: result.Successor.StateUpdate,
		Prepared:    result.Successor.Prepared,
		StateHash:   result.Successor.StateHash,
		Live:        result.Successor.Live,
	}, pending)
	if err != nil {
		// Raw, and never through classifyLocalCandidateError. The collator has
		// already accepted this candidate; a failure to build our own successor
		// from it is local bookkeeping, and turning it into ErrCandidateRejected
		// would make this node vote against a block it just found valid.
		return CandidateValidation{}, err
	}

	b.speculateNextWindow(ctx, view, artifact, next, result.ValidAfter)

	return CandidateValidation{ValidAfter: result.ValidAfter, State: next}, nil
}

// speculateNextWindow bets that the window about to open will open on the
// candidate just validated, and starts its first block now.
//
// This is the only place in the node that knows both halves at the same time:
// the successor state of a candidate the network has not notarized yet, and
// which validator leads the window that its notarization will open. The
// certificate that opens that window arrives roughly one collation later, so a
// block started here is in every other validator's hands when they finish
// applying this base, instead of a collation after they finish.
//
// It runs off the validation goroutine on purpose. This function is reached
// between the semantic verdict and the notarize vote it authorizes, and nothing
// it does — least of all decoding a state to bind the base capability — belongs
// in front of that vote.
func (b *LocalSessionBackend) speculateNextWindow(
	ctx context.Context,
	view *localValidationView,
	artifact *CandidateArtifact,
	successor *ChainState,
	validAfter time.Time,
) {
	if successor == nil || len(successor.tips) != 1 {
		return
	}
	bet, ok := b.nextWindowBet(view, artifact.Candidate.ID.Slot, validAfter, time.Now())
	if !ok {
		return
	}
	tip := successor.tips[0]
	candidate := artifact.Candidate.ID
	session := b.config.SessionID
	// Detached from the validation context: that context belongs to a vote which
	// is about to be cast, and this work outlives it by design.
	specCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), bet.deadline)
	go func() {
		defer cancel()
		base, err := collator.NewSelectedBaseState(
			session,
			candidate,
			tip.ID,
			tip.BlockBOC,
			tip.Block,
			tip.State,
		)
		if err != nil {
			b.log.Debug().
				Err(err).
				Uint32("window_start", bet.startSlot).
				Msg("skipping speculative window: base capability is not bindable")

			return
		}
		if err = b.self.SpeculateSelfWindow(specCtx, collator.SpeculativeWindowRequest{
			SessionID: session,
			StartSlot: bet.startSlot,
			Leader:    b.validator.Index,
			Base:      base,
			StartAt:   bet.startAt,
			Deadline:  bet.deadline,
		}); err != nil {
			b.log.Debug().
				Err(err).
				Uint32("window_start", bet.startSlot).
				Msg("speculative window was not started")
		}
	}()
}

// speculativeWindowBet is the decision half of speculateNextWindow: which
// window a just-validated candidate would open, and on what schedule. It is
// separated from the binding half so the rule can be read — and gated — without
// a block to bind.
type speculativeWindowBet struct {
	startSlot uint32
	startAt   time.Time
	deadline  time.Time
}

// nextWindowBet reports whether the candidate validated at slot is the one whose
// notarization opens a window this node leads, and on what schedule its first
// block should be stamped.
//
// Only the last slot of the current window qualifies. It is the candidate whose
// certificate opens the next window and therefore the base that window will
// carry unless the network skips it — the 21% of the time it does, the producer
// falls back to collating on the base consensus really selected.
func (b *LocalSessionBackend) nextWindowBet(
	view *localValidationView,
	slot uint32,
	validAfter time.Time,
	now time.Time,
) (speculativeWindowBet, bool) {
	if b.self == nil || b.validator == nil || b.productionMode != collator.ProductionModeSelf {
		return speculativeWindowBet{}, false
	}
	if b.config.Shard.IsMasterchain() || view == nil {
		return speculativeWindowBet{}, false
	}
	windowSize := b.config.Protocol.SlotsPerLeaderWindow
	validators := uint32(len(b.config.Validators))
	if windowSize == 0 || validators == 0 || slot == math.MaxUint32 {
		return speculativeWindowBet{}, false
	}
	nextStart := slot + 1
	if nextStart%windowSize != 0 {
		return speculativeWindowBet{}, false
	}
	if nextStart/windowSize%validators != b.validator.Index {
		return speculativeWindowBet{}, false
	}
	if !view.update.HasCurrentWindow || view.update.CurrentWindowStart+windowSize != nextStart {
		return speculativeWindowBet{}, false
	}
	targetRate := view.update.TargetRate
	if targetRate <= 0 {
		return speculativeWindowBet{}, false
	}
	// The instant the observed window would compute, by the rule it uses
	// (sessionRuntime.handleWindow): the parent's generation time plus one target
	// rate, never earlier than now and never more than one rate ahead. It stamps
	// the block's header and nothing else; the producer's schedule still comes
	// from the window when it is really observed.
	startAt := now
	if !validAfter.IsZero() {
		if earliest := validAfter.Add(targetRate); earliest.After(startAt) {
			startAt = earliest
		}
		if latest := now.Add(targetRate); startAt.After(latest) {
			startAt = latest
		}
	}

	// Three target rates bounds a bet nobody comes to collect: longer than any
	// observed gap between this estimate and the window it predicts, and short
	// enough that a bet lost to a stalled session dies within a window.
	return speculativeWindowBet{
		startSlot: nextStart,
		startAt:   startAt,
		deadline:  startAt.Add(3 * targetRate),
	}, true
}

func (b *LocalSessionBackend) AcceptBlock(ctx context.Context, acceptance BlockAcceptance) error {
	b.controlMu.Lock()
	if b.closed {
		b.controlMu.Unlock()
		return ErrLocalSessionBackendClosed
	}
	observeFinalized := b.finalized != nil && !b.collatorDeferred.Load() && !b.collatorUnavailable
	b.controlMu.Unlock()

	// The registry view is resolved inside acceptance, after the block reached
	// local ingress and the network. Resolving it here would abort the whole
	// acceptance while the group tracker has no snapshot yet, and every retry of
	// that acceptance carries Retry, so the block would never be published.
	// Acceptance calls this only for a finalized shard block.
	resolveView := func() (BlockAcceptanceView, error) {
		return currentBlockAcceptanceView(b.groups, b.config.Shard)
	}
	if err := b.accepter.Accept(ctx, acceptance, resolveView); err != nil {
		return err
	}
	// A deferred collator was recovered ahead of the node database and already
	// persisted this consensus progress. Replaying it before UpdateSession
	// reaches that durable view would address an intentionally inactive
	// collator and make crash recovery retry forever.
	if observeFinalized && !acceptance.Certificate.IsZero() &&
		acceptance.Certificate.Vote().Kind == simplex.VoteFinalize {
		if err := b.finalized(ctx, acceptance.Candidate.Candidate.Block); err != nil {
			return fmt.Errorf("validator local backend: observe consensus finalization: %w", err)
		}
	}

	return nil
}

func (b *LocalSessionBackend) HandleLeaderWindow(ctx context.Context, window LeaderWindow) error {
	if b.validator == nil {
		return nil
	}
	if err := validateLocalLeaderWindow(b.config, window); err != nil {
		return err
	}

	b.controlMu.Lock()
	if err := ctx.Err(); err != nil {
		b.controlMu.Unlock()
		return err
	}
	if b.closed {
		b.controlMu.Unlock()
		return ErrLocalSessionBackendClosed
	}
	if b.collatorDeferred.Load() {
		ready := b.collatorReadinessLocked()
		b.controlMu.Unlock()
		b.log.Debug().
			Uint32("window_start", window.Window.StartSlot).
			Msg("waiting for recovered collator view before handling leader window")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready:
		}

		b.controlMu.Lock()
		if err := ctx.Err(); err != nil {
			b.controlMu.Unlock()
			return err
		}
		if b.closed {
			b.controlMu.Unlock()
			return ErrLocalSessionBackendClosed
		}
	}
	defer b.controlMu.Unlock()
	if b.collatorUnavailable {
		return nil
	}
	if b.recheckWindow != nil && b.recheckWindow.window == window.Window {
		return errLeaderWindowNeedsRecheck
	}
	if window.Window.ObservedSlot != window.Window.StartSlot {
		if !b.update.HasCurrentWindow {
			return nil
		}
		err := b.authorizeUpcomingWindow(ctx, window.Window)
		if b.ignoreAdvisoryCollatorError("authorize upcoming recovered window", window.Window.EndSlot, err) {
			b.markWindowHandled(window.Window)

			return nil
		}
		if err == nil {
			b.markWindowHandled(window.Window)
		}

		return err
	}

	if b.productionMode == collator.ProductionModeSelf {
		b.setWindowRoute(window)
	}
	next := localCollatorUpdate(b.config.SessionID, b.state)
	next.PreserveProgress(b.update)
	next.HasCurrentWindow = true
	next.CurrentWindowStart = window.Window.StartSlot
	next.CurrentWindowObservedSlot = window.Window.ObservedSlot
	next.CurrentWindowStartAt = window.StartAt
	next.CurrentBase = window.Window.Base
	if err := b.collator.UpdateSession(ctx, next); err != nil {
		if errors.Is(err, collator.ErrSessionUnavailable) {
			b.quarantineCollator(&next)
			b.update = next
			b.publishValidationView()

			return nil
		}

		b.clearWindowRoute(window.Window.StartSlot)
		return fmt.Errorf("validator local backend: update collator leader window: %w", err)
	}
	b.update = next
	b.publishValidationView()
	if err := b.authorizeUpcomingWindow(ctx, window.Window); err != nil {
		if !b.ignoreAdvisoryCollatorError("authorize upcoming window", window.Window.EndSlot, err) {
			b.clearWindowRoute(window.Window.StartSlot)
			return err
		}
	}
	if !window.Window.LocalLeader {
		b.markWindowHandled(window.Window)

		return nil
	}

	if b.productionMode == collator.ProductionModeDelegated {
		// Delegated authority is committed durably during W-1. There is no
		// current-window persistence fallback: a missing preauthorization skips W.
		b.markWindowHandled(window.Window)

		return nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		b.clearWindowRoute(window.Window.StartSlot)
		return errors.New("validator local backend: self window has no deadline")
	}
	if err := b.self.ActivateSelfWindow(ctx, collator.SelfWindowRequest{
		SessionID: b.config.SessionID,
		StartSlot: window.Window.StartSlot,
		Deadline:  deadline,
		Signer:    b.validator.Signer,
	}); err != nil {
		if errors.Is(err, collator.ErrSessionUnavailable) {
			b.quarantineCollator(&b.update)

			return nil
		}

		b.clearWindowRoute(window.Window.StartSlot)
		return fmt.Errorf("validator local backend: activate self window: %w", err)
	}
	b.markWindowHandled(window.Window)

	return nil
}

func (b *LocalSessionBackend) authorizeUpcomingWindow(ctx context.Context, window simplex.Window) error {
	if b.productionMode != collator.ProductionModeDelegated {
		return nil
	}
	nextStart := window.EndSlot

	return b.authorizeDelegatedWindow(ctx, nextStart)
}

func (b *LocalSessionBackend) authorizeDelegatedWindow(ctx context.Context, nextStart uint32) error {
	if b.productionMode != collator.ProductionModeDelegated {
		return nil
	}
	windowSize := b.config.Protocol.SlotsPerLeaderWindow
	if nextStart > math.MaxUint32-windowSize || nextStart > math.MaxInt32 {
		return errors.New("validator local backend: upcoming leader window overflows slot space")
	}
	nextLeader := nextStart / windowSize % uint32(len(b.config.Validators))
	if nextLeader != b.validator.Index {
		return nil
	}

	authorization, err := b.delegationAuthorization(ctx, nextStart)
	if err != nil {
		return err
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, delegationDeliveryTimeout)
	err = b.collator.CommitDelegation(deliveryCtx, collator.WindowRequest{
		SessionID:  b.config.SessionID,
		SourceADNL: b.config.Identity.ADNLID,
		PleaseCollate: simplex.ConsensusPleaseCollate{
			WindowStartSlot: int32(nextStart),
			Signature:       authorization.Signature,
		},
	})
	cancel()
	if err != nil && !errors.Is(err, collator.ErrAlreadyDelegated) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		b.log.Debug().
			Err(err).
			Hex("session", b.config.SessionID[:]).
			Uint32("window", nextStart).
			Msg("durable delegation delivery was inconclusive")
	}

	return nil
}

func (b *LocalSessionBackend) delegationAuthorization(
	ctx context.Context,
	start uint32,
) (DelegationAuthorization, error) {
	authorization, err := b.delegations.DelegationAuthorization(ctx, b.config.StorageID, start)
	if err == nil {
		if err = b.validateDelegationAuthorization(start, authorization); err != nil {
			return DelegationAuthorization{}, err
		}

		return authorization, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return DelegationAuthorization{}, fmt.Errorf(
			"validator local backend: load upcoming leader authorization %d: %w",
			start,
			err,
		)
	}

	err = b.collator.Probe(ctx, collator.WindowPreparation{
		SessionID:  b.config.SessionID,
		SourceADNL: b.config.Identity.ADNLID,
		StartSlot:  start,
	})
	if err != nil && !errors.Is(err, collator.ErrAlreadyDelegated) {
		return DelegationAuthorization{}, fmt.Errorf(
			"validator local backend: probe upcoming leader window %d: %w",
			start,
			err,
		)
	}

	signature, err := simplex.SignDelegation(
		b.validator.Signer,
		b.config.SessionID,
		start,
		b.collator.CollatorID(),
	)
	if err != nil {
		return DelegationAuthorization{}, fmt.Errorf(
			"validator local backend: sign upcoming leader delegation %d: %w",
			start,
			err,
		)
	}
	if !simplex.VerifyDelegationSignature(
		b.config.Validators[b.validator.Index].PublicKey[:],
		b.config.SessionID,
		start,
		b.collator.CollatorID(),
		signature,
	) {
		return DelegationAuthorization{}, ErrDelegationConflict
	}
	authorization = DelegationAuthorization{
		StartSlot: start,
		Collator:  b.collator.CollatorID(),
		Signature: signature,
	}
	if err = awaitStorageWrite(ctx, func(done func(error)) {
		b.delegations.SaveDelegationAuthorization(ctx, b.config.StorageID, authorization, done)
	}); err != nil {
		return DelegationAuthorization{}, fmt.Errorf(
			"validator local backend: save upcoming leader authorization %d: %w",
			start,
			err,
		)
	}

	return authorization, nil
}

func (b *LocalSessionBackend) validateDelegationAuthorization(
	start uint32,
	authorization DelegationAuthorization,
) error {
	if authorization.StartSlot != start || authorization.Collator != b.collator.CollatorID() ||
		!simplex.VerifyDelegationSignature(
			b.config.Validators[b.validator.Index].PublicKey[:],
			b.config.SessionID,
			start,
			authorization.Collator,
			authorization.Signature,
		) {
		return ErrDelegationConflict
	}

	return nil
}

func (b *LocalSessionBackend) ignoreAdvisoryCollatorError(operation string, start uint32, err error) bool {
	if err == nil || !isAdvisoryCollatorError(err) {
		return false
	}
	b.log.Warn().
		Err(err).
		Hex("session", b.config.SessionID[:]).
		Str("operation", operation).
		Uint32("window", start).
		Msg("collator unavailable; skipping delegated production")

	return true
}

func (b *LocalSessionBackend) logDelegationFailure(operation string, start uint32, err error) {
	b.log.Warn().
		Err(err).
		Hex("session", b.config.SessionID[:]).
		Str("operation", operation).
		Uint32("window", start).
		Msg("delegated production was not prepared")
}

func isAdvisoryCollatorError(err error) bool {
	return errors.Is(err, collator.ErrUnavailable) ||
		errors.Is(err, collator.ErrWindowTooFar) ||
		errors.Is(err, collator.ErrStaleWindow) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (b *LocalSessionBackend) HandleMisbehavior(
	ctx context.Context,
	validator uint32,
	misbehavior simplex.Misbehavior,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.log.Warn().
		Hex("session", b.config.SessionID[:]).
		Uint32("validator", validator).
		Uint8("kind", uint8(misbehavior.Kind)).
		Int("proof_1_bytes", len(misbehavior.Proof1)).
		Int("proof_2_bytes", len(misbehavior.Proof2)).
		Msg("observed simplex validator misbehavior")

	return nil
}

func (b *LocalSessionBackend) Close() error {
	b.controlMu.Lock()
	if b.closed {
		b.controlMu.Unlock()

		return nil
	}
	// Teardown is synchronous here, so there is no intermediate draining state:
	// every observer either sees an open backend or a fully closed one. Unlike
	// collator.RemoteCollator, this backend has nothing in flight to wait for.
	b.validation.Store(nil)
	b.signalValidationChanged()
	b.clearWindowRoute(0)
	if b.releaseRoute != nil {
		b.releaseRoute()
		b.releaseRoute = nil
	}
	b.closed = true
	b.pendingWindow = nil
	b.appliedWindow = nil
	b.handledWindow = nil
	b.recheckWindow = nil
	b.signalCollatorReady()
	b.controlMu.Unlock()

	return nil
}

func (b *LocalSessionBackend) Retire() error {
	if err := b.Close(); err != nil {
		return err
	}
	if b.validator == nil {
		return nil
	}

	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if b.collatorRetired {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.closeAfter)
	err := b.collator.RetireSession(ctx, b.config.SessionID)
	cancel()
	if err != nil {
		return fmt.Errorf("validator local backend: retire collator session: %w", err)
	}
	b.collatorRetired = true

	return nil
}

// publishValidationView makes the current authenticated session view available
// without coupling candidate validation to local collation or its WAL. Callers
// hold controlMu. Session/activation and the arrays referenced by update are
// immutable after publication.
func (b *LocalSessionBackend) publishValidationView() {
	if b.activation == nil || b.collatorDeferred.Load() || b.closed {
		b.validation.Store(nil)
		b.signalValidationChanged()
		return
	}
	b.validation.Store(&localValidationView{
		session: collator.ActivatedSession{
			Session:        b.session,
			Genesis:        b.activation.Genesis,
			MinMasterchain: b.activation.MinMasterchain,
		},
		update: b.update,
	})
	b.signalValidationChanged()
}

func (b *LocalSessionBackend) signalValidationChanged() {
	if b.validationChanged != nil {
		close(b.validationChanged)
	}
	b.validationChanged = make(chan struct{})
}

func (b *LocalSessionBackend) waitValidationView(ctx context.Context) (*localValidationView, error) {
	if view := b.validation.Load(); view != nil {
		return view, nil
	}

	for {
		b.controlMu.Lock()
		if b.closed {
			b.controlMu.Unlock()

			return nil, ErrLocalSessionBackendClosed
		}
		view := b.validation.Load()
		changed := b.validationChanged
		b.controlMu.Unlock()

		if view != nil {
			return view, nil
		}

		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// loadChainTip reads one predecessor, waiting for it on an EDGE rather than
// polling for it.
//
// What it replaces: the caller used to loop at 1 Hz around a read that failed in
// microseconds, against a 200 ms slot rate. Nothing about that wait was paced by
// the thing being waited for, and it was silent — no log, no metric — which is
// how a 149 s basechain standstill was measured in the field with zero lines
// naming its cause.
//
// Why the loop is HERE and not around LoadChainState: the predicate under test
// must be the caller's own read. The store's own WaitBlockArtifacts checks the
// state, the state cells and the block root — but a chain tip also needs the
// block BOC, so waiting on that helper and then reading would report ready for a
// block this read still cannot serve. Taking the signal, reading, and blocking
// only if the read failed makes the predicate exact by construction, whatever the
// signal happens to mean.
//
// Why it cannot lose a wake-up: the signal is taken BEFORE the read, and every
// publication closes it under the same lock that installs the artifacts. A
// publication that lands during the read wakes the wait that follows it. A signal
// that is not about this block costs one extra read and nothing else, because
// success is decided by the read and never by the wake-up.
//
// The backstop is not a poll interval. It is the alarm for the one thing this
// structure cannot see: a publication path that makes a state readable without
// raising a signal would otherwise turn a slow-but-correct poll into a silent
// hang. Firing it logs at Warn and counts, then hands the not-ready error back so
// the caller's own retry decides what to do — which is also what keeps a genuine
// catch-up (crash replay, or a block this node did not finalize) behaving exactly
// as it did before.
func (b *LocalSessionBackend) loadChainTip(
	ctx context.Context,
	id ton.BlockIDExt,
	wait bool,
) (ChainTip, error) {
	if err := storage.ValidateBlockIDHashes(id); err != nil {
		return ChainTip{}, fmt.Errorf("validator local backend: invalid chain tip id: %w", err)
	}
	if !wait {
		return b.readChainTip(ctx, id)
	}

	started := time.Now()
	// ONE timer for the whole wait, armed before the loop. Built inside the
	// select it would be rebuilt on every iteration, and the loop iterates on
	// every publication in the shard — not only on a publication of the block in
	// hand. The alarm would then require chainTipWaitBackstop of total
	// publication SILENCE rather than that long spent waiting for this block, so
	// on a busy node it would never fire at all and this wait would be unbounded
	// with no backstop: exactly the silent hang the 1 Hz poll was replaced to
	// avoid. See TestChainTipWaitBackstopIsNotRestartedByUnrelatedPublications.
	backstop := time.NewTimer(b.chainTipWaitBackstop())
	defer backstop.Stop()

	for {
		// Strictly before the read.
		notify := b.node.BlockArtifactsSignal()

		tip, err := b.readChainTip(ctx, id)
		if err == nil || !errors.Is(err, ErrBlockNotReady) {
			return tip, err
		}

		select {
		case <-notify:
		case <-backstop.C:
			b.observeChainTipWaitBackstop(id, time.Since(started), err)

			return ChainTip{}, err
		case <-ctx.Done():
			return ChainTip{}, err
		}
	}
}

// chainTipWaitBackstop bounds one blind wait for a predecessor. It is the same
// order of magnitude as the reference node's own promise timeout for a block
// state, and it is deliberately far above any interval a healthy node would wait:
// reaching it is the alarm, not the mechanism.
const chainTipWaitBackstop = 30 * time.Second

func (b *LocalSessionBackend) chainTipWaitBackstop() time.Duration {
	if b.waitBackstop > 0 {
		return b.waitBackstop
	}
	return chainTipWaitBackstop
}

func (b *LocalSessionBackend) observeChainTipWaitBackstop(
	id ton.BlockIDExt,
	waited time.Duration,
	cause error,
) {
	if b.metrics != nil {
		b.metrics.AddChainTipWaitBackstop(localSessionMetricChain(b.config.Shard))
	}
	b.log.Warn().
		Err(cause).
		Str("block", storage.FormatBlockRef(id)).
		Dur("waited", waited).
		Msg("predecessor state did not become readable within the wait backstop")
}

func localSessionMetricChain(shard groups.ShardID) collator.MetricChain {
	if shard.IsMasterchain() {
		return collator.MetricChainMasterchain
	}

	return collator.MetricChainShardchain
}

func (b *LocalSessionBackend) readChainTip(ctx context.Context, id ton.BlockIDExt) (ChainTip, error) {
	stored, err := b.node.BlockState(ctx, id)
	if err != nil {
		return ChainTip{}, localSessionReadError("state metadata", id, err)
	}
	if !sameBlockID(stored.Block, id) {
		return ChainTip{}, errors.New("validator local backend: stored state belongs to another block")
	}
	if len(stored.StateRootHash) != sha256.Size {
		return ChainTip{}, errors.New("validator local backend: stored state root hash is invalid")
	}

	root := stored.Cell
	if root == nil {
		root, err = b.node.LoadStateCellTree(ctx, id, stored.StateRootHash)
		if err != nil {
			return ChainTip{}, localSessionReadError("state cells", id, err)
		}
	}
	if !bytes.Equal(root.Hash(), stored.StateRootHash) {
		return ChainTip{}, errors.New("validator local backend: state root differs from stored metadata")
	}

	tip := ChainTip{ID: *id.Copy(), State: root}
	if id.SeqNo == 0 {
		if !bytes.Equal(root.Hash(), id.RootHash) {
			return ChainTip{}, errors.New("validator local backend: zerostate root differs from block id")
		}
		if len(stored.StateFileHash) != 0 && len(stored.StateFileHash) != sha256.Size {
			return ChainTip{}, errors.New("validator local backend: zerostate file hash metadata is invalid")
		}
		if len(stored.StateFileHash) == sha256.Size && !bytes.Equal(stored.StateFileHash, id.FileHash) {
			return ChainTip{}, errors.New("validator local backend: zerostate file hash differs from block id")
		}
		return tip, nil
	}

	blockBOC, err := b.node.BlockData(ctx, id)
	if err != nil {
		return ChainTip{}, localSessionReadError("block data", id, err)
	}
	// The file hash of these bytes is checked once, at the boundary that has to
	// check it for every backend rather than only for this one: newChainState
	// binds both halves of a tip to its id — the cell by root hash, the bytes by
	// file hash — before anything reads either. Hashing the payload here as well
	// paid a second full sha256 of a multi-megabyte predecessor per loaded tip
	// to reach the same rejection one step earlier. The reference loads its
	// predecessors through BlockQ::init, which checks the root hash and nothing
	// else (impl/block.cpp:64).
	blockRoot, err := cell.FromBOC(blockBOC)
	if err != nil {
		return ChainTip{}, fmt.Errorf("validator local backend: decode block data: %w", err)
	}
	if !bytes.Equal(blockRoot.Hash(), id.RootHash) {
		return ChainTip{}, errors.New("validator local backend: block root hash mismatch")
	}
	// One reflection parse: identity first, then the trailing-data check on the
	// same loader. Going through ParseVerifiedBlockCell and re-parsing for the
	// trailing check decoded the whole block twice per finalized parent.
	loader, err := blockRoot.BeginParse()
	if err != nil {
		return ChainTip{}, fmt.Errorf("validator local backend: parse chain tip block: %w", err)
	}
	var parsed tlb.Block
	if err = tlb.LoadFromCell(&parsed, loader); err != nil {
		return ChainTip{}, fmt.Errorf("validator local backend: parse chain tip block: %w", err)
	}
	if err = storage.VerifyBlockIdentity(id, &parsed); err != nil {
		return ChainTip{}, fmt.Errorf("validator local backend: verify chain tip block identity: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return ChainTip{}, errors.New("validator local backend: chain tip block has trailing data")
	}
	tip.BlockBOC = blockBOC
	// The root was decoded and hash-checked above; keeping it is what spares
	// every candidate validation a second decode of this same predecessor.
	tip.Block = blockRoot

	return tip, nil
}

func (b *LocalSessionBackend) routeCandidate(
	ctx context.Context,
	artifact collator.CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.routeMu.RLock()
	window := b.window
	if window == nil || window.id != artifact.WindowID ||
		artifact.Candidate.ID.Slot < window.id.StartSlot || artifact.Candidate.ID.Slot >= window.end {
		b.routeMu.RUnlock()
		return fmt.Errorf(
			"%w for session %x window %d",
			ErrCandidateRouteNotFound,
			artifact.SessionID,
			artifact.WindowID.StartSlot,
		)
	}
	submit := window.submit
	b.routeMu.RUnlock()

	candidate := artifact.Candidate
	if candidate.Delegation != nil {
		return errors.New("validator local backend: self candidate carries delegated authority")
	}
	// The index, not the signature. The collator verified this signature the
	// instant it was produced, against Session.Validators[window.Leader], and
	// localCollatorSession builds that roster by copying b.config.Validators —
	// so the only thing a second ed25519 verification here could establish over
	// that one is that the leader the collator signed for is the validator this
	// backend is, which is this comparison. Everything else it would re-derive
	// is a value that has not left the process; C++ verifies its own candidate
	// signature nowhere at all (window-producer.cpp:118-131).
	//
	// The candidate is still verified once more against the codec's roster and
	// leader schedule in encodeForBroadcast, which is the last gate before the
	// bytes reach the network and the one that covers the delegated route too.
	if candidate.Leader != b.validator.Index {
		return errors.New("validator local backend: local candidate names another leader")
	}

	// The collator held the parsed roots of both BOCs when it emitted this, so
	// the validation of our own candidate borrows them exactly as the validation
	// of a received one borrows the decoder's. Everything that retains an
	// artifact strips them again; see attachPayloadLocked.
	var roots *candidateValidationRoots
	if blockRoot, collatedRoots := artifact.ValidationRoots(); blockRoot != nil && len(collatedRoots) != 0 {
		roots = &candidateValidationRoots{block: blockRoot, collated: collatedRoots}
	}
	generationTimeMS, generationTimeKnown := artifact.GenerationTimeMS()

	return submit(ctx, &CandidateArtifact{
		Candidate:           candidate,
		BlockBOC:            artifact.BlockBOC,
		CollatedData:        artifact.CollatedData,
		prepared:            artifact.Prepared(),
		digested:            artifact.Digested(),
		validationRoots:     roots,
		generationTimeMS:    generationTimeMS,
		generationTimeKnown: generationTimeKnown,
	})
}

func (b *LocalSessionBackend) setWindowRoute(window LeaderWindow) {
	b.routeMu.Lock()
	if window.Window.LocalLeader {
		b.window = &localWindowRoute{
			id: collator.WindowID{
				SessionID: b.config.SessionID,
				StartSlot: window.Window.StartSlot,
			},
			end:    window.Window.EndSlot,
			submit: window.Submit,
		}
	} else {
		b.window = nil
	}
	b.routeMu.Unlock()
}

func (b *LocalSessionBackend) clearWindowRoute(start uint32) {
	b.routeMu.Lock()
	if b.window != nil && (start == 0 || b.window.id.StartSlot == start) {
		b.window = nil
	}
	b.routeMu.Unlock()
}

func localSessionReadError(kind string, id ton.BlockIDExt, err error) error {
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: exact %s for block %d is unavailable", ErrBlockNotReady, kind, id.SeqNo)
	}

	return fmt.Errorf("validator local backend: read %s for block %d: %w", kind, id.SeqNo, err)
}

func classifyLocalCandidateError(err error) error {
	switch {
	case errors.Is(err, collator.ErrAcquisitionNotReady):
		return fmt.Errorf("%w: %w", ErrBlockNotReady, err)
	case errors.Is(err, collator.ErrInvalidInput),
		errors.Is(err, collator.ErrUnsupported),
		errors.Is(err, collator.ErrSizeLimit),
		errors.Is(err, collator.ErrCollatedRootNotFound):
		return fmt.Errorf("%w: %w", ErrCandidateRejected, err)
	default:
		return err
	}
}

func validateLocalLeaderWindow(config SessionConfig, window LeaderWindow) error {
	start := window.Window.StartSlot
	windowSize := config.Protocol.SlotsPerLeaderWindow
	if windowSize == 0 || start%windowSize != 0 {
		return errors.New("validator local backend: leader window is not aligned")
	}
	if start > math.MaxUint32-windowSize || window.Window.EndSlot != start+windowSize {
		return errors.New("validator local backend: leader window end is invalid")
	}
	if len(config.Validators) == 0 {
		return errors.New("validator local backend: validator roster is empty")
	}
	expected := start / windowSize % uint32(len(config.Validators))
	if window.Window.Leader != expected {
		return errors.New("validator local backend: leader window has an unexpected leader")
	}
	local := config.Identity.Validator != nil && config.Identity.Validator.Index == expected
	if window.Window.LocalLeader != local {
		return errors.New("validator local backend: local leader flag differs from the session identity")
	}
	if window.StartAt.IsZero() {
		return errors.New("validator local backend: leader window start time is zero")
	}
	if local && window.Submit == nil {
		return errors.New("validator local backend: local leader submitter is required")
	}

	return nil
}

func localCollatorSession(config SessionConfig) collator.Session {
	validators := make([]collator.SessionValidator, len(config.Validators))
	for i := range config.Validators {
		validator := &config.Validators[i]
		validators[i] = collator.SessionValidator{
			PublicKey: validator.PublicKey,
			ADNLID:    groups.ValidatorADNL(*validator),
			Weight:    validator.Weight,
		}
	}
	return collator.Session{
		ID:                   config.SessionID,
		Shard:                config.Shard,
		CatchainSeqno:        config.CatchainSeqno,
		ValidatorSetHash:     config.ValidatorSetHash,
		ConsensusVersion:     config.Protocol.Version,
		ConsensusFlags:       config.Protocol.Flags,
		ProtocolVersion:      config.Protocol.ProtocolVersion,
		UseQUIC:              config.Protocol.UseQUIC,
		SlotsPerLeaderWindow: config.Protocol.SlotsPerLeaderWindow,
		Validators:           validators,
	}
}

func localCollatorActivation(sessionID [32]byte, start SessionStart) collator.SessionActivation {
	activation := collator.SessionActivation{
		SessionID:      sessionID,
		Genesis:        make([]ton.BlockIDExt, len(start.Genesis)),
		MinMasterchain: *start.MinMasterchain.Copy(),
	}
	for i := range start.Genesis {
		activation.Genesis[i] = *start.Genesis[i].Copy()
	}

	return activation
}

func localCollatorUpdate(sessionID [32]byte, state SessionState) collator.SessionUpdate {
	update := collator.SessionUpdate{
		SessionID:                 sessionID,
		TargetRate:                state.Params.TargetRate,
		NoEmptyBlocksOnErrTimeout: state.Params.NoEmptyBlocksOnErrTimeout,
		MasterchainBlock:          *state.MasterchainBlock.Copy(),
		Registered:                append([]groups.ShardDescription(nil), state.Registered...),
	}
	for i := range update.Registered {
		update.Registered[i].Block = *update.Registered[i].Block.Copy()
	}
	if state.FinalizedBlock != nil {
		update.HasFinalizedBlock = true
		update.FinalizedBlock = *state.FinalizedBlock.Copy()
	}

	return update
}

type localCollatorPreparation struct {
	update collator.SessionUpdate
	ready  bool
}

func prepareLocalCollatorSession(
	ctx context.Context,
	producer collator.Collator,
	session collator.Session,
	latest collator.SessionUpdate,
) (localCollatorPreparation, error) {
	recovered, err := producer.Session(ctx, session.ID)
	if errors.Is(err, collator.ErrNotFound) || errors.Is(err, collator.ErrSessionRetired) {
		if err = producer.PrepareSession(ctx, session, latest); err != nil {
			return localCollatorPreparation{}, fmt.Errorf(
				"validator local backend: prepare collator session: %w",
				err,
			)
		}

		return localCollatorPreparation{update: latest, ready: true}, nil
	}
	if err != nil {
		return localCollatorPreparation{}, fmt.Errorf("validator local backend: load collator session: %w", err)
	}
	if !recovered.Session.Equal(session) {
		return localCollatorPreparation{}, fmt.Errorf(
			"validator local backend: recovered collator session differs from runtime: %w",
			collator.ErrSessionConflict,
		)
	}
	if recovered.Update.MasterchainBlock.SeqNo > latest.MasterchainBlock.SeqNo {
		// Consensus recovery must be able to replay the final certificates which
		// advance the node to this durable collator view. C++ starts the consensus
		// bus independently and lets CollatorProducer wait for state availability;
		// retaining the recovered update here provides the same dependency order.
		target := recovered.Update
		target.PreserveProgress(latest)
		if err = mergeLocalSessionFinalized(session.Shard, &target, latest); err != nil {
			return localCollatorPreparation{}, fmt.Errorf(
				"validator local backend: merge recovered collator progress: %w",
				err,
			)
		}

		return localCollatorPreparation{update: target}, nil
	}

	latest.PreserveProgress(recovered.Update)
	if err = mergeLocalSessionFinalized(session.Shard, &latest, recovered.Update); err != nil {
		return localCollatorPreparation{}, fmt.Errorf(
			"validator local backend: merge collator progress: %w",
			err,
		)
	}
	if err = producer.UpdateSession(ctx, latest); err != nil {
		return localCollatorPreparation{}, fmt.Errorf(
			"validator local backend: reconcile recovered collator session: %w",
			err,
		)
	}

	return localCollatorPreparation{update: latest, ready: true}, nil
}

// mergeLocalSessionFinalized carries the strongest session-local finalized
// anchor into the selected masterchain update. Masterchain snapshots can move
// ahead independently of this consensus observation, but neither side may
// rewrite an exact finalized height or name another shard.
func mergeLocalSessionFinalized(
	shard groups.ShardID,
	target *collator.SessionUpdate,
	other collator.SessionUpdate,
) error {
	if target.HasFinalizedBlock &&
		(target.FinalizedBlock.Workchain != shard.Workchain || target.FinalizedBlock.Shard != shard.Shard) {
		return collator.ErrSessionConflict
	}
	if !other.HasFinalizedBlock {
		return nil
	}
	if other.FinalizedBlock.Workchain != shard.Workchain || other.FinalizedBlock.Shard != shard.Shard {
		return collator.ErrSessionConflict
	}
	if !target.HasFinalizedBlock || other.FinalizedBlock.SeqNo > target.FinalizedBlock.SeqNo {
		target.HasFinalizedBlock = true
		target.FinalizedBlock = *other.FinalizedBlock.Copy()

		return nil
	}
	if other.FinalizedBlock.SeqNo == target.FinalizedBlock.SeqNo &&
		!sameBlockID(other.FinalizedBlock, target.FinalizedBlock) {
		return collator.ErrSessionConflict
	}

	return nil
}

func applyConsensusWindow(update *collator.SessionUpdate, window simplex.Window, observedStart time.Time) {
	startAt := time.Time{}
	if update.HasCurrentWindow && update.CurrentWindowStart == window.StartSlot {
		startAt = update.CurrentWindowStartAt
	} else if window.ObservedSlot == window.StartSlot {
		startAt = observedStart
	}

	update.HasCurrentWindow = true
	update.CurrentWindowStart = window.StartSlot
	update.CurrentWindowObservedSlot = window.ObservedSlot
	update.CurrentWindowStartAt = startAt
	update.CurrentBase = window.Base
}
