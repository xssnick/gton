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
	Logger             zerolog.Logger
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

// LocalSessionBackend binds one Simplex runtime to local authenticated state,
// validation, acceptance, and either an in-process or remote Collator.
type LocalSessionBackend struct {
	config           SessionConfig
	session          collator.Session
	activation       *collator.SessionActivation
	node             LocalSessionNode
	groups           collator.LocalGroupSource
	delegations      DelegationAuthorizationStorage
	acquisition      *collator.LocalAcquisition
	collator         collator.Collator
	productionMode   collator.ProductionMode
	progress         localConsensusProgressCollator
	self             localSelfWindowCollator
	finalized        func(context.Context, ton.BlockIDExt) error
	accepter         *BlockAccepter
	log              zerolog.Logger
	closeAfter       time.Duration
	validator        *ValidatorIdentity
	validation       atomic.Pointer[localValidationView]
	validationClosed atomic.Bool
	collatorDeferred atomic.Bool

	controlMu sync.Mutex
	state     SessionState
	update    collator.SessionUpdate
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

func NewLocalSessionBackend(
	ctx context.Context,
	options LocalSessionBackendOptions,
) (*LocalSessionBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Node == nil {
		return nil, errors.New("validator local backend: node is required")
	}
	if !options.Config.Shard.IsMasterchain() && options.Groups == nil {
		return nil, errors.New("validator local backend: validator-group source is required for a shard session")
	}
	if options.CloseTimeout < 0 {
		return nil, errors.New("validator local backend: close timeout must not be negative")
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
		return nil, err
	}

	backend := &LocalSessionBackend{
		config:         options.Config,
		session:        session,
		node:           options.Node,
		groups:         options.Groups,
		delegations:    options.Delegations,
		acquisition:    options.Acquisition,
		collator:       options.Collator,
		productionMode: options.ProductionMode,
		accepter:       accepter,
		finalized:      options.ConsensusFinalized,
		log:            options.Logger,
		closeAfter:     options.CloseTimeout,
		validator:      options.Config.Identity.Validator,
		state:          initial,
		update:         update,
	}
	if backend.validator == nil {
		return backend, nil
	}
	if backend.validator.Signer == nil {
		return nil, errors.New("validator local backend: validator signer is required")
	}
	if int(backend.validator.Index) >= len(options.Config.Validators) {
		return nil, errors.New("validator local backend: validator index is outside the roster")
	}
	if options.Acquisition == nil {
		return nil, errors.New("validator local backend: local acquisition is required for a validator")
	}
	if options.Storage == nil {
		return nil, errors.New("validator local backend: validator storage is required for a validator")
	}
	if options.Collator == nil {
		return nil, errors.New("validator local backend: collator is required for a validator")
	}
	switch options.ProductionMode {
	case collator.ProductionModeSelf:
		if options.CandidateRouter == nil {
			return nil, errors.New("validator local backend: self production requires a candidate router")
		}
		progress, ok := options.Collator.(localConsensusProgressCollator)
		if !ok {
			return nil, errors.New("validator local backend: in-process collator does not accept consensus progress")
		}
		self, ok := options.Collator.(localSelfWindowCollator)
		if !ok {
			return nil, errors.New("validator local backend: in-process collator does not accept self windows")
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
			return nil, fmt.Errorf("validator local backend: register candidate route: %w", err)
		}
	case collator.ProductionModeDelegated:
		if options.CandidateRouter != nil {
			return nil, errors.New("validator local backend: delegated production forbids a candidate router")
		}
		if options.Delegations == nil {
			return nil, errors.New("validator local backend: delegated production requires authorization storage")
		}
	default:
		return nil, errors.New("validator local backend: production mode is invalid")
	}
	preparation, err := prepareLocalCollatorSession(ctx, options.Collator, session, update)
	if err != nil {
		if backend.releaseRoute != nil {
			backend.releaseRoute()
		}
		return nil, err
	}
	backend.update = preparation.update
	backend.collatorDeferred.Store(!preparation.ready)

	return backend, nil
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
		applyConsensusWindow(&b.update, progress)
		b.publishValidationView()

		return nil
	}

	converted, err := collatorConsensusProgress(
		b.config.SessionID,
		b.config.Protocol.SlotsPerLeaderWindow,
		progress,
	)
	if err != nil {
		return fmt.Errorf("validator local backend: convert consensus progress: %w", err)
	}
	if b.collatorDeferred.Load() {
		applyConsensusWindow(&b.update, progress)

		return nil
	}
	if b.collatorUnavailable {
		return fmt.Errorf(
			"validator local backend: apply consensus progress: %w",
			collator.ErrSessionUnavailable,
		)
	}
	if err := b.progress.ApplyConsensusProgress(ctx, converted); err != nil {
		if errors.Is(err, collator.ErrSessionUnavailable) {
			b.collatorUnavailable = true
			b.clearWindowRoute(0)
		}

		return fmt.Errorf("validator local backend: apply consensus progress: %w", err)
	}

	applyConsensusWindow(&b.update, progress)
	b.publishValidationView()

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
	if b.validator != nil && !b.collatorDeferred.Load() {
		if err := b.collator.ActivateSession(ctx, activation); err != nil {
			return fmt.Errorf("validator local backend: activate collator session: %w", err)
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
	if b.validator != nil {
		if b.collatorUnavailable {
			b.state = state
			b.update = next
			b.publishValidationView()

			return nil
		}
		if b.collatorDeferred.Load() && b.update.MasterchainBlock.SeqNo > next.MasterchainBlock.SeqNo {
			b.state = state

			return nil
		}
		if err := b.collator.UpdateSession(ctx, next); err != nil {
			return fmt.Errorf("validator local backend: update collator session: %w", err)
		}
		if b.collatorDeferred.Load() && b.activation != nil {
			if err := b.collator.ActivateSession(ctx, *b.activation); err != nil {
				return fmt.Errorf("validator local backend: activate recovered collator session: %w", err)
			}
		}
		b.collatorDeferred.Store(false)
	}

	b.state = state
	b.update = next
	b.publishValidationView()
	return nil
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
		tip, err := b.loadChainTip(ctx, request.Blocks[i])
		if err != nil {
			return ChainStateData{}, err
		}
		data.Tips[i] = tip
	}

	return data, nil
}

func (b *LocalSessionBackend) ValidateCandidate(
	ctx context.Context,
	state *ChainState,
	artifact *CandidateArtifact,
) (CandidateValidation, error) {
	if b.validator == nil {
		return CandidateValidation{}, errors.New("validator local backend: observer cannot validate candidates")
	}
	if b.validationClosed.Load() {
		return CandidateValidation{}, ErrLocalSessionBackendClosed
	}
	view := b.validation.Load()
	if view == nil {
		return CandidateValidation{}, fmt.Errorf("%w: collator session view is not ready", ErrBlockNotReady)
	}

	previous := make([]collator.PreviousBlock, len(state.tips))
	for i := range state.tips {
		tip := &state.tips[i]
		previous[i] = collator.PreviousBlock{ID: *tip.ID.Copy(), State: tip.State}
		if tip.ID.SeqNo == 0 {
			continue
		}

		root, err := cell.FromBOC(tip.BlockBOC)
		if err != nil {
			return CandidateValidation{}, fmt.Errorf(
				"validator local backend: decode predecessor %d block: %w",
				tip.ID.SeqNo,
				err,
			)
		}
		if !bytes.Equal(root.Hash(), tip.ID.RootHash) {
			return CandidateValidation{}, errors.New("validator local backend: predecessor block root hash mismatch")
		}
		previous[i].Block = root
	}

	result, err := b.acquisition.ValidateCandidate(ctx, collator.ValidationRequest{
		Session:      view.session,
		Update:       view.update,
		Previous:     previous,
		Candidate:    artifact.Candidate,
		BlockBOC:     artifact.BlockBOC,
		CollatedData: artifact.CollatedData,
	})
	if err != nil {
		return CandidateValidation{}, classifyLocalCandidateError(err)
	}

	return CandidateValidation{ValidAfter: result.ValidAfter}, nil
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
	if observeFinalized && acceptance.Certificate != nil &&
		acceptance.Certificate.Vote.Kind == simplex.VoteFinalize {
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
	if b.collatorDeferred.Load() {
		b.log.Debug().
			Uint32("window_start", window.Window.StartSlot).
			Msg("skipping leader window until recovered collator view catches up")

		return nil
	}
	if err := validateLocalLeaderWindow(b.config, window); err != nil {
		return err
	}

	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return ErrLocalSessionBackendClosed
	}
	if b.collatorUnavailable {
		return nil
	}
	if window.Window.ObservedSlot != window.Window.StartSlot {
		if !b.update.HasCurrentWindow {
			return nil
		}
		err := b.authorizeUpcomingWindow(ctx, window.Window)
		if b.ignoreAdvisoryCollatorError("authorize upcoming recovered window", window.Window.EndSlot, err) {
			return nil
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
		return nil
	}

	if b.productionMode == collator.ProductionModeDelegated {
		// Delegated authority is committed durably during W-1. There is no
		// current-window persistence fallback: a missing preauthorization skips W.
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
		b.clearWindowRoute(window.Window.StartSlot)
		return fmt.Errorf("validator local backend: activate self window: %w", err)
	}

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
	b.validationClosed.Store(true)
	b.validation.Store(nil)
	b.clearWindowRoute(0)
	if b.releaseRoute != nil {
		b.releaseRoute()
		b.releaseRoute = nil
	}
	b.closed = true
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
}

func (b *LocalSessionBackend) loadChainTip(ctx context.Context, id ton.BlockIDExt) (ChainTip, error) {
	if err := storage.ValidateBlockIDHashes(id); err != nil {
		return ChainTip{}, fmt.Errorf("validator local backend: invalid chain tip id: %w", err)
	}
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
	fileHash := sha256.Sum256(blockBOC)
	if !bytes.Equal(fileHash[:], id.FileHash) {
		return ChainTip{}, errors.New("validator local backend: block file hash mismatch")
	}
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
	validatorKey := b.config.Validators[b.validator.Index].PublicKey
	if !simplex.VerifyCandidateSignature(
		validatorKey[:],
		b.config.SessionID,
		candidate.ID,
		candidate.Signature,
	) {
		return errors.New("validator local backend: local candidate signature is invalid")
	}

	return submit(ctx, &CandidateArtifact{
		Candidate:    candidate,
		BlockBOC:     artifact.BlockBOC,
		CollatedData: artifact.CollatedData,
		prepared:     artifact.Prepared(),
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
		return localCollatorPreparation{update: recovered.Update}, nil
	}

	latest.PreserveProgress(recovered.Update)
	if err = producer.UpdateSession(ctx, latest); err != nil {
		return localCollatorPreparation{}, fmt.Errorf(
			"validator local backend: reconcile recovered collator session: %w",
			err,
		)
	}

	return localCollatorPreparation{update: latest, ready: true}, nil
}

func applyConsensusWindow(update *collator.SessionUpdate, progress sessionConsensusProgress) {
	startAt := time.Time{}
	if update.HasCurrentWindow && update.CurrentWindowStart == progress.Window.StartSlot {
		startAt = update.CurrentWindowStartAt
	} else if progress.Window.ObservedSlot == progress.Window.StartSlot {
		startAt = progress.StartAt
	}

	update.HasCurrentWindow = true
	update.CurrentWindowStart = progress.Window.StartSlot
	update.CurrentWindowObservedSlot = progress.Window.ObservedSlot
	update.CurrentWindowStartAt = startAt
	update.CurrentBase = progress.Window.Base
}
