package collator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	productionRetryInitialDelay = 5 * time.Millisecond
	productionRetryMaxDelay     = 20 * time.Millisecond
	// productionBarrierMinWait and productionBarrierMaxWait clamp the unwind
	// window productionBarrierWait derives from the slot rate, so neither an
	// unset nor an absurd TargetRate can turn the barrier into a TryLock or into
	// a stall of the session control lock.
	productionBarrierMinWait = 50 * time.Millisecond
	productionBarrierMaxWait = time.Second
	// Session writes cross their caller-visible commit boundary when the opaque
	// pipeline accepts the update. Persistence then retries under the service
	// lifetime with a bounded cadence.
	sessionWriteRetryInitialDelay = 100 * time.Millisecond
	sessionWriteRetryMaxDelay     = 2 * time.Second
	// prepareRollbackTimeout bounds compensation after the caller which began a
	// tentative session has already gone away. The cleanup is derived from the
	// service lifetime, so an RPC deadline cannot poison the deterministic
	// session ID while shutdown still cancels it promptly.
	prepareRollbackTimeout = 10 * time.Second
	// The protocol's fixed maximum future window. This is a delegated-collation
	// admission bound, not a configurable validator desync limit.
	delegationWindowHorizon = uint32(20)
	// retiredTombstoneLimit bounds the retirement fence. Session IDs are
	// deterministic per validator-group generation, so a retired ID is
	// essentially never admitted again and the fence would otherwise grow one
	// permanent entry per rotation for the process lifetime. A tombstone only
	// has to outlive the operations that resolved a handle before its
	// retirement, which the newest entries always cover.
	retiredTombstoneLimit = 1024
)

// ServiceOptions assembles one collator runtime with a fixed authority mode.
type ServiceOptions struct {
	ProductionMode ProductionMode
	Storage        CollatorStorage
	Pipeline       Pipeline
	Keys           SigningKeys
	CollatorKeyID  [32]byte
	Observer       CollationObserver
	// Logger receives one completion event per block collation. A zero logger
	// disables the optional instrumentation without changing production flow.
	Logger zerolog.Logger
	// AllowedValidators is operator policy keyed by the authenticated ADNL ID
	// of a validator. An empty set permits nobody unless AllowAllValidators is
	// explicitly enabled.
	AllowedValidators map[[32]byte]struct{}
	// AllowAllValidators admits every authenticated validator in the prepared
	// session roster. It is used by the in-process validator collator and by a
	// standalone operator who explicitly disables the extra allowlist.
	AllowAllValidators bool
	Emit               EmitCandidate
}

type serviceState uint8

const (
	serviceNew serviceState = iota
	serviceStarting
	serviceRunning
	serviceClosing
	serviceClosed
)

// Service is a crash-safe local collator backend. It does not own
// Storage, Pipeline, SigningKeys, or the candidate emitter.
type Service struct {
	opts      ServiceOptions
	log       zerolog.Logger
	publicKey ed25519.PublicKey
	allowed   map[[32]byte]struct{}
	allowAll  bool

	startMu  ctxMutex
	closeMu  ctxMutex
	mu       sync.Mutex
	state    serviceState
	started  bool
	runCtx   context.Context
	cancel   context.CancelFunc
	sessions map[[32]byte]*managedCollatorSession
	retired  map[[32]byte]struct{}
	// retiredOrder mirrors the keys of retired in insertion order so the fence
	// can be evicted oldest-first. Both are mutated only under mu.
	retiredOrder    [][32]byte
	productions     sync.WaitGroup
	productionWait  sync.Once
	productionsDone chan struct{}
	productionDone  sync.Once

	active        atomic.Int64
	retrying      atomic.Int64
	completed     atomic.Uint64
	failed        atomic.Uint64
	statusMu      sync.Mutex
	lastError     string
	lastCompleted time.Time
}

var _ ControllerBackend = (*Service)(nil)

type managedCollatorSession struct {
	controlMu      ctxMutex
	mu             sync.Mutex
	policyMu       sync.Mutex
	productionMu   ctxMutex
	sessionWriteMu ctxMutex
	record         SessionRecord
	emptyPolicy    simplex.EmptyBlockPolicy
	policyStarted  bool
	ready          bool
	// pipelineReady distinguishes a durable recovered descriptor from the
	// opaque acquisition session which materializes it. Startup may recover the
	// descriptor before the authenticated message topology contains its shard;
	// the descriptor stays visible for exact reconciliation or retirement while
	// every production gate remains closed until PrepareSession succeeds.
	pipelineReady   bool
	activationReady bool
	progressReady   bool
	// sessionWritePending records that the pipeline accepted managed.record,
	// but storage has not confirmed that exact revision yet. The in-memory
	// record is authoritative from that point: callers advance their mirror,
	// while production stays closed until the service-owned writer catches up.
	sessionWritePending     bool
	progressReadyAfterWrite bool
	sessionWriteRevision    uint64
	// sessionWriteReserved counts opaque-pipeline mutations which crossed the
	// point where they may commit but have not published their accepted record
	// yet. The reservation starts the writer under Service.mu before that point,
	// so Close cannot begin waiting between the pipeline commit and WAL handoff.
	sessionWriteReserved uint32
	sessionWriteRunning  bool
	sessionWriteWake     chan struct{}
	authorizations       map[WindowID]delegatedAuthorization
	selfWindows          map[WindowID]SelfWindowRequest
	productions          map[WindowID]*productionJob
	updating             bool
	retiring             bool
	unavailable          bool
	// prepareCleanupPending distinguishes a failed tentative-prepare rollback
	// from a terminal active-session failure. An exact Prepare may retry only
	// this cleanup; a conflicting descriptor must never retire its generation.
	prepareCleanupPending bool
	// emitted keeps the artifacts this process signed for the window it is
	// currently producing, so a retried production re-emits the same bytes
	// instead of collating a second candidate for a slot it already broadcast.
	// The durable record is only the signature marker, so this memory is the
	// entire difference between resuming a window and ending it. One window is
	// held at a time: remembering a slot of a new window drops the previous one.
	emittedMu     sync.Mutex
	emittedWindow WindowID
	emitted       map[uint32]CandidateArtifact
}

func (m *managedCollatorSession) rememberEmitted(id WindowID, artifact CandidateArtifact) {
	m.emittedMu.Lock()
	defer m.emittedMu.Unlock()

	if m.emitted == nil || m.emittedWindow != id {
		m.emittedWindow = id
		m.emitted = make(map[uint32]CandidateArtifact)
	}
	m.emitted[artifact.Candidate.ID.Slot] = artifact
}

func (m *managedCollatorSession) recallEmitted(id WindowID, slot uint32) (CandidateArtifact, bool) {
	m.emittedMu.Lock()
	defer m.emittedMu.Unlock()

	if m.emittedWindow != id {
		return CandidateArtifact{}, false
	}
	artifact, found := m.emitted[slot]

	return artifact, found
}

// forgetEmitted releases a completed window's payloads. A failed production
// keeps them: it stays eligible for relaunch, and relaunching without them
// would end the window at its first already signed slot.
func (m *managedCollatorSession) forgetEmitted(id WindowID) {
	m.emittedMu.Lock()
	defer m.emittedMu.Unlock()

	if m.emittedWindow == id {
		m.emittedWindow = WindowID{}
		m.emitted = nil
	}
}

type productionJob struct {
	session *managedCollatorSession
	window  productionWindow
	ctx     context.Context
	cancel  context.CancelFunc
}

type productionWindow struct {
	ID                  WindowID
	Leader              uint32
	Authority           CandidateAuthority
	DelegationSignature []byte
	SelfSigner          simplex.Signer
	Deadline            time.Time
}

type delegatedAuthorizationState uint8

const (
	delegatedAuthorizationPending delegatedAuthorizationState = iota + 1
	delegatedAuthorizationCompleted
	delegatedAuthorizationCancelled
)

type delegatedAuthorization struct {
	ID                  WindowID
	Leader              uint32
	SourceADNL          [32]byte
	CollatorKeyID       [32]byte
	DelegationSignature []byte
	State               delegatedAuthorizationState
}

func delegatedProductionWindow(window delegatedAuthorization) productionWindow {
	return productionWindow{
		ID:                  window.ID,
		Leader:              window.Leader,
		Authority:           CandidateAuthorityDelegated,
		DelegationSignature: append([]byte(nil), window.DelegationSignature...),
	}
}

func sameDelegationAuthorization(left, right delegatedAuthorization) bool {
	return left.ID == right.ID && left.Leader == right.Leader &&
		left.SourceADNL == right.SourceADNL && left.CollatorKeyID == right.CollatorKeyID &&
		bytes.Equal(left.DelegationSignature, right.DelegationSignature)
}

func selfProductionWindow(session Session, request SelfWindowRequest) productionWindow {
	leader := request.StartSlot / session.SlotsPerLeaderWindow % uint32(len(session.Validators))

	return productionWindow{
		ID:         WindowID{SessionID: request.SessionID, StartSlot: request.StartSlot},
		Leader:     leader,
		Authority:  CandidateAuthoritySelf,
		SelfSigner: request.Signer,
		Deadline:   request.Deadline,
	}
}

type managedSessionRef struct {
	id      [32]byte
	managed *managedCollatorSession
}

type candidateBuildResult struct {
	candidate *Candidate
	elapsed   time.Duration
	err       error
}

type candidateBuildFuture struct {
	request      BuildRequest
	hardDeadline time.Time
	started      time.Time
	result       chan candidateBuildResult
	done         chan struct{}
	cancel       context.CancelFunc
}

func candidateBytes(candidate *Candidate) int {
	if candidate == nil {
		return 0
	}
	return len(candidate.BlockBOC)
}

func candidateCollatedBytes(candidate *Candidate) int {
	if candidate == nil {
		return 0
	}
	return len(candidate.CollatedData)
}

var errHardBuildDeadline = errors.New("collator runtime: hard build deadline exceeded")

// errWindowNotResumable ends a window whose already signed slot cannot be
// re-emitted, because the artifact behind its durable marker did not survive
// into this process. Retrying cannot help: the marker is durable and the
// payload is not coming back, so this error is deliberately terminal.
var errWindowNotResumable = errors.New("collator runtime: signed window cannot be resumed")

func (f *candidateBuildFuture) stop() {
	f.cancel()
	<-f.done
}

// NewService validates dependencies and resolves the collator public key.
func NewService(opts ServiceOptions) (*Service, error) {
	if opts.ProductionMode != ProductionModeSelf && opts.ProductionMode != ProductionModeDelegated {
		return nil, errors.New("collator runtime: production mode is invalid")
	}
	if opts.Storage == nil {
		return nil, errors.New("collator runtime: storage is required")
	}
	if opts.Pipeline == nil {
		return nil, errors.New("collator runtime: pipeline is required")
	}
	if opts.Keys == nil {
		return nil, errors.New("collator runtime: signing keys are required")
	}
	if opts.Emit == nil {
		return nil, errors.New("collator runtime: candidate emitter is required")
	}
	if opts.CollatorKeyID == ([32]byte{}) {
		return nil, errors.New("collator runtime: collator key id is zero")
	}
	if !slices.Contains(opts.Keys.KeyIDs(), opts.CollatorKeyID) {
		return nil, fmt.Errorf("collator runtime: collator key %x is unavailable", opts.CollatorKeyID)
	}

	publicKey, err := opts.Keys.PublicKeyFor(opts.CollatorKeyID)
	if err != nil {
		return nil, fmt.Errorf("collator runtime: resolve collator public key: %w", err)
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("collator runtime: collator public key has length %d", len(publicKey))
	}
	if simplex.KeyNodeIDShort(publicKey) != opts.CollatorKeyID {
		return nil, errors.New("collator runtime: collator key id does not match public key")
	}

	allowed := make(map[[32]byte]struct{}, len(opts.AllowedValidators))
	for id := range opts.AllowedValidators {
		allowed[id] = struct{}{}
	}

	return &Service{
		opts:            opts,
		log:             opts.Logger,
		startMu:         newCtxMutex(),
		closeMu:         newCtxMutex(),
		publicKey:       append(ed25519.PublicKey(nil), publicKey...),
		allowed:         allowed,
		allowAll:        opts.AllowAllValidators,
		state:           serviceNew,
		sessions:        make(map[[32]byte]*managedCollatorSession),
		retired:         make(map[[32]byte]struct{}),
		productionsDone: make(chan struct{}),
	}, nil
}

// CollatorID returns the node ID of the signing key used for delegations and
// candidate signatures.
func (s *Service) CollatorID() [32]byte {
	return s.opts.CollatorKeyID
}

// Session returns the effective view of a prepared session. It includes a
// pipeline-accepted update whose durable callback is still unresolved, because
// callers must reconcile from the state the opaque pipeline actually holds.
// After restart the durable store again defines this view.
func (s *Service) Session(ctx context.Context, sessionID [32]byte) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	managed, err := s.runningSession(sessionID)
	if err != nil {
		return SessionRecord{}, err
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.retiring {
		return SessionRecord{}, ErrSessionRetired
	}
	if managed.prepareCleanupPending {
		return SessionRecord{}, ErrSessionRetired
	}
	if managed.unavailable {
		return SessionRecord{}, ErrSessionUnavailable
	}
	if !managed.ready {
		return SessionRecord{}, ErrNotFound
	}

	return cloneSessionRecord(managed.record), nil
}

// Start restores durable sessions. Delegation authority exists only in receiver
// memory and is deliberately not recovered after a process restart.
// Repeated calls after success are idempotent.
func (s *Service) Start(ctx context.Context) error {
	s.startMu.Lock()
	startLocked := true
	defer func() {
		if startLocked {
			s.startMu.Unlock()
		}
	}()

	startCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	switch s.state {
	case serviceRunning:
		s.mu.Unlock()
		cancel()
		return nil
	case serviceClosing, serviceClosed:
		s.mu.Unlock()
		cancel()
		return ErrClosed
	case serviceStarting:
		s.mu.Unlock()
		cancel()
		return errors.New("collator runtime: start is already in progress")
	}
	s.state = serviceStarting
	s.runCtx = startCtx
	s.cancel = cancel
	s.mu.Unlock()

	recovered := make(map[[32]byte]*managedCollatorSession)
	records, err := s.opts.Storage.Sessions(startCtx)
	if err != nil {
		return s.failStart(startCtx, recovered, fmt.Errorf("collator runtime: load sessions: %w", err))
	}

	recovered = make(map[[32]byte]*managedCollatorSession, len(records))
	for i := range records {
		record := cloneSessionRecord(records[i])
		if err = validateSessionRecord(record); err != nil {
			return s.failStart(startCtx, recovered, fmt.Errorf("collator runtime: recover session: %w", err))
		}
		if _, duplicate := recovered[record.Session.ID]; duplicate {
			return s.failStart(startCtx, recovered,
				fmt.Errorf("%w: duplicate recovered session %x", ErrSessionConflict, record.Session.ID))
		}
		pipelineReady := true
		if err = s.opts.Pipeline.PrepareSession(startCtx, record.Session, record.Update); err != nil {
			if !errors.Is(err, ErrAcquisitionNotReady) {
				return s.failStart(startCtx, recovered,
					fmt.Errorf("collator runtime: recover pipeline session %x: %w", record.Session.ID, err))
			}
			pipelineReady = false
			s.log.Debug().
				Err(err).
				Hex("session_id", record.Session.ID[:]).
				Msg("recovered collator session is waiting for acquisition topology")
		}

		managed := newManagedCollatorSession(record, true)
		managed.pipelineReady = pipelineReady
		recovered[record.Session.ID] = managed
		if record.Activation != nil && pipelineReady {
			if err = s.opts.Pipeline.ActivateSession(startCtx, *record.Activation, record.Update); err != nil {
				// A durable session can legitimately lag the latest node view: the
				// validator supervisor has not yet replayed its current update while
				// the collator starts. Keep the tentative pipeline session alive so
				// that replay can advance it and retry this exact activation. Failing
				// startup here would prevent the supervisor from ever doing so.
				if !errors.Is(err, ErrAcquisitionNotReady) {
					return s.failStart(startCtx, recovered,
						fmt.Errorf("collator runtime: recover active pipeline session %x: %w", record.Session.ID, err))
				}
			} else {
				managed.activationReady = true
			}
		}
	}

	s.mu.Lock()
	if s.state != serviceStarting {
		s.mu.Unlock()
		return s.failStart(startCtx, recovered, ErrClosed)
	}
	s.sessions = recovered
	s.state = serviceRunning
	s.started = true
	s.mu.Unlock()

	go func() {
		<-startCtx.Done()
		s.beginClose()
	}()

	s.startMu.Unlock()
	startLocked = false
	return nil
}

func (s *Service) failStart(
	ctx context.Context,
	recovered map[[32]byte]*managedCollatorSession,
	cause error,
) error {
	cleanupErr := s.retireRecovered(ctx, recovered)

	s.mu.Lock()
	stop := s.cancel
	for id, managed := range recovered {
		managed.mu.Lock()
		managed.unavailable = true
		managed.mu.Unlock()
		s.sessions[id] = managed
	}
	closing := s.state == serviceClosing || len(recovered) != 0
	if closing {
		s.state = serviceClosing
	} else if s.state == serviceStarting {
		s.state = serviceNew
		s.runCtx = nil
		s.cancel = nil
	}
	s.mu.Unlock()

	if stop != nil {
		stop()
	}
	if closing {
		s.startProductionWait()
	}
	return errors.Join(cause, cleanupErr)
}

func (s *Service) retireRecovered(
	ctx context.Context,
	recovered map[[32]byte]*managedCollatorSession,
) error {
	var cleanupErrs []error
	for id := range recovered {
		if err := s.opts.Pipeline.RetireSession(ctx, id); err != nil && !isRetiredSessionError(err) {
			cleanupErrs = append(cleanupErrs,
				fmt.Errorf("%w: retire recovered session %x: %w", ErrSessionUnavailable, id, err))
			continue
		}
		delete(recovered, id)
	}
	return errors.Join(cleanupErrs...)
}

// Close cancels accepted production and waits for window goroutines and pipeline session
// retirement up to ctx's deadline. Receiver delegations are process-local and must be resent.
func (s *Service) Close(ctx context.Context) error {
	s.beginClose()
	if err := s.startMu.LockCtx(ctx); err != nil {
		return err
	}
	s.startMu.Unlock()

	s.mu.Lock()
	state := s.state
	s.mu.Unlock()
	if state == serviceClosed {
		return nil
	}

	select {
	case <-s.productionsDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := s.closeMu.LockCtx(ctx); err != nil {
		return err
	}
	defer s.closeMu.Unlock()

	s.mu.Lock()
	if s.state == serviceClosed {
		s.mu.Unlock()
		return nil
	}
	sessions := make([]managedSessionRef, 0, len(s.sessions))
	for id, managed := range s.sessions {
		sessions = append(sessions, managedSessionRef{id: id, managed: managed})
	}
	s.mu.Unlock()

	var closeErrs []error
	for _, session := range sessions {
		if err := session.managed.controlMu.LockCtx(ctx); err != nil {
			closeErrs = append(closeErrs, err)
			break
		}
		s.mu.Lock()
		current := s.sessions[session.id] == session.managed
		s.mu.Unlock()
		if current {
			session.managed.mu.Lock()
			prepareCleanupPending := session.managed.prepareCleanupPending
			session.managed.mu.Unlock()
			if prepareCleanupPending {
				if err := s.rollbackPreparedSession(ctx, session.managed, session.id); err != nil {
					closeErrs = append(closeErrs, err)
				}
			} else if err := s.opts.Pipeline.RetireSession(ctx, session.id); err != nil &&
				!isRetiredSessionError(err) {
				closeErrs = append(closeErrs, fmt.Errorf("retire session %x: %w", session.id, err))
			}
		}
		session.managed.controlMu.Unlock()
	}

	if len(closeErrs) != 0 {
		return errors.Join(closeErrs...)
	}
	s.mu.Lock()
	s.state = serviceClosed
	s.sessions = nil
	// runningStateError rejects every caller with ErrClosed before it can read a
	// tombstone, so freeing the fence here is observationally inert.
	s.retired = nil
	s.retiredOrder = nil
	s.mu.Unlock()

	return nil
}

// ctxMutex is the package's cancellable mutex. A buffered channel hands the
// lock over the moment it is released, where the polling loop it replaces made
// every contended waiter — including produceWindow, which takes productionMu
// immediately before a leader window starts building — observe the release up
// to a tick late and burn a runtime timer for the whole wait. Waiters are FIFO
// rather than barging, so a slow holder no longer starves anyone.
//
// Its zero value is unusable on purpose: an owner that skipped its constructor
// would otherwise block forever on a nil channel. Unlock panics on an
// unbalanced release, keeping the safety net sync.Mutex gives the hand-balanced
// non-defer unlock sites in this file.
type ctxMutex struct {
	ch chan struct{}
}

func newCtxMutex() ctxMutex {
	return ctxMutex{ch: make(chan struct{}, 1)}
}

func (m *ctxMutex) TryLock() bool {
	select {
	case m.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// tryProductionBarrier claims the barrier without waiting. It serves the
// callers whose rejection is cheap because something retries them: UpdateSession
// is replayed by the validator supervisor, which classifies exactly this error
// and reschedules the session update.
func tryProductionBarrier(ctx context.Context, barrier *ctxMutex) error {
	if barrier.TryLock() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	return errProductionRunning()
}

// waitProductionBarrier claims the barrier, giving a production that the caller
// has just cancelled a bounded window to unwind. Cancellation is asynchronous:
// the producer still has to return from its in-flight build or storage wait,
// retire its bookkeeping and only then release the barrier, so a TryLock issued
// microseconds after the cancel loses that race by construction. Losing it is
// not a slot but a whole leader window — a rejected consensus progress leaves
// production disarmed until the next one arrives, and the runtime never even
// opens the window it rejected.
//
// The wait stays a fraction of a slot because the caller holds the session
// control lock and the window pump for its whole duration. Waiting out a
// producer that is genuinely wedged is not the goal; giving a cancelled one
// time to notice is.
func waitProductionBarrier(ctx context.Context, barrier *ctxMutex, wait time.Duration) error {
	if barrier.TryLock() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if wait <= 0 {
		return errProductionRunning()
	}

	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	if err := barrier.LockCtx(waitCtx); err == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	return errProductionRunning()
}

// productionBarrierWait derives the unwind window from the session slot rate. A
// quarter slot is orders of magnitude longer than a cancelled producer needs to
// return from a cancellable wait, and still leaves the rest of the slot to
// materialize the progress this call is carrying.
func productionBarrierWait(update SessionUpdate) time.Duration {
	wait := update.TargetRate / 4
	if wait < productionBarrierMinWait {
		return productionBarrierMinWait
	}
	if wait > productionBarrierMaxWait {
		return productionBarrierMaxWait
	}

	return wait
}

func errProductionRunning() error {
	return fmt.Errorf("%w: local production is still running", ErrAcquisitionNotReady)
}

// Lock blocks until the lock is free. It serves the barriers that are not
// scoped to a caller's context, such as the start/close handshake.
func (m *ctxMutex) Lock() {
	m.ch <- struct{}{}
}

// LockCtx acquires a free lock before consulting ctx, so an already expired
// caller still wins an uncontended lock exactly as the TryLock-first poll did.
func (m *ctxMutex) LockCtx(ctx context.Context) error {
	if m.TryLock() {
		return nil
	}
	select {
	case m.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *ctxMutex) Unlock() {
	select {
	case <-m.ch:
	default:
		panic("collator runtime: unlock of unlocked ctxMutex")
	}
}

func (s *Service) beginClose() {
	s.mu.Lock()
	wait := false
	switch s.state {
	case serviceNew:
		s.state = serviceClosed
		s.productionDone.Do(func() { close(s.productionsDone) })
	case serviceStarting, serviceRunning:
		s.state = serviceClosing
		if s.cancel != nil {
			s.cancel()
		}
		wait = true
	case serviceClosing:
		wait = true
	}
	s.mu.Unlock()
	if wait {
		s.startProductionWait()
	}
}

func (s *Service) startProductionWait() {
	s.productionWait.Do(func() {
		go func() {
			s.productions.Wait()
			s.productionDone.Do(func() { close(s.productionsDone) })
		}()
	})
}

// PrepareSession makes one shard session durable and ready for delegation.
func (s *Service) PrepareSession(ctx context.Context, session Session, update SessionUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record := cloneSessionRecord(SessionRecord{Session: session, Update: update})
	if err := validateSessionRecord(record); err != nil {
		return err
	}

	managed, created, err := s.admitPreparedSession(record)
	if err != nil {
		return err
	}

	if !created {
		if err = managed.controlMu.LockCtx(ctx); err != nil {
			return err
		}
	}
	defer managed.controlMu.Unlock()
	created, err = s.recheckPreparedSession(record.Session.ID, managed, created)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	prepareCleanupPending := managed.prepareCleanupPending
	if prepareCleanupPending && !managed.record.Session.Equal(record.Session) {
		managed.mu.Unlock()

		return ErrSessionConflict
	}
	if prepareCleanupPending {
		managed.mu.Unlock()
		if err = managed.productionMu.LockCtx(ctx); err != nil {
			return err
		}
		cleanupErr := s.rollbackPreparedSession(s.runCtx, managed, record.Session.ID)
		managed.productionMu.Unlock()
		if cleanupErr != nil {
			return cleanupErr
		}

		// This call resolved the quarantined generation. Its handle is now gone;
		// the next supervisor retry must pass through normal admission and create
		// a fresh generation rather than continue using the retired pointer.
		return ErrSessionRetired
	}

	if managed.unavailable {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	if managed.ready {
		// Preparation describes only the tentative session. Once activation has
		// durably attached its anchors, replaying the same descriptor remains an
		// idempotent prepare rather than conflicting with that extra state.
		exact := managed.record.Session.Equal(record.Session) &&
			managed.record.Update.Equal(record.Update)
		pipelineReady := managed.pipelineReady
		managed.mu.Unlock()
		if !exact {
			return ErrSessionConflict
		}
		if pipelineReady {
			return nil
		}

		if err = managed.productionMu.LockCtx(ctx); err != nil {
			return err
		}
		err = s.ensurePipelineSession(ctx, managed, record)
		managed.productionMu.Unlock()

		return err
	}
	if managed.record.Session.ID != ([32]byte{}) && !managed.record.Session.Equal(record.Session) {
		managed.mu.Unlock()
		if created {
			s.removePreparedSession(record.Session.ID, managed)
		}
		return ErrSessionConflict
	}
	managed.mu.Unlock()

	if err = managed.productionMu.LockCtx(ctx); err != nil {
		if created {
			s.removePreparedSession(record.Session.ID, managed)
		}
		return err
	}
	defer managed.productionMu.Unlock()

	if err := s.opts.Pipeline.PrepareSession(ctx, record.Session, record.Update); err != nil {
		if created {
			s.removePreparedSession(record.Session.ID, managed)
		}
		return fmt.Errorf("collator runtime: prepare pipeline session: %w", err)
	}
	if err := awaitSessionWrite(ctx, func(done func(error)) {
		s.opts.Storage.SaveSession(ctx, record, done)
	}); err != nil {
		saveErr := fmt.Errorf("collator runtime: save session: %w", err)
		if cleanupErr := s.rollbackPreparedSession(s.runCtx, managed, record.Session.ID); cleanupErr != nil {
			return errors.Join(saveErr, cleanupErr)
		}

		return saveErr
	}

	managed.mu.Lock()
	managed.record = record
	managed.ready = true
	managed.pipelineReady = true
	managed.prepareCleanupPending = false
	managed.mu.Unlock()
	return s.reconcileWindows(ctx, managed)
}

// ensurePipelineSession materializes one durable descriptor which startup had
// to defer because its authenticated message topology was not available yet.
// The caller owns controlMu and productionMu, so no activation, update,
// retirement, or producer can observe a half-prepared opaque session.
func (s *Service) ensurePipelineSession(
	ctx context.Context,
	managed *managedCollatorSession,
	record SessionRecord,
) error {
	managed.mu.Lock()
	ready := managed.pipelineReady
	managed.mu.Unlock()
	if ready {
		return nil
	}

	if err := s.opts.Pipeline.PrepareSession(ctx, record.Session, record.Update); err != nil {
		return fmt.Errorf("collator runtime: recover deferred pipeline session: %w", err)
	}

	managed.mu.Lock()
	managed.pipelineReady = true
	managed.mu.Unlock()

	return nil
}

// rollbackPreparedSession compensates a pipeline prepare whose durable session
// save failed or has an unknown outcome. SaveSession admission completed while
// controlMu was held, so DeleteSession is FIFO-after any late commit. Both
// opaque and durable state must be gone before the handle can be admitted
// again. A partial cleanup stays quarantined so exact Prepare and permanent
// retirement can both retry it without admitting production.
func (s *Service) rollbackPreparedSession(
	baseCtx context.Context,
	managed *managedCollatorSession,
	sessionID [32]byte,
) error {
	cleanupCtx, cancel := context.WithTimeout(baseCtx, prepareRollbackTimeout)
	defer cancel()

	retireErr := s.opts.Pipeline.RetireSession(cleanupCtx, sessionID)
	if isRetiredSessionError(retireErr) {
		retireErr = nil
	}
	deleteErr := s.opts.Storage.DeleteSession(cleanupCtx, sessionID)
	if isRetiredSessionError(deleteErr) {
		deleteErr = nil
	}
	if retireErr == nil && deleteErr == nil {
		managed.mu.Lock()
		managed.unavailable = false
		managed.prepareCleanupPending = false
		managed.mu.Unlock()
		s.removePreparedSession(sessionID, managed)

		return nil
	}

	managed.mu.Lock()
	managed.unavailable = true
	managed.prepareCleanupPending = true
	managed.mu.Unlock()

	var cleanupErrs []error
	if retireErr != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("retire pipeline session: %w", retireErr))
	}
	if deleteErr != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("delete durable session: %w", deleteErr))
	}

	return fmt.Errorf(
		"%w: roll back prepared session %x: %w",
		ErrSessionUnavailable,
		sessionID,
		errors.Join(cleanupErrs...),
	)
}

// ActivateSession durably binds exact predecessor anchors to a tentative
// session, then activates the already prepared pipeline in place. Persistence
// intentionally precedes the idempotent pipeline call: a crash or pipeline
// failure leaves an activated durable record which an exact retry or restart
// can finish without guessing whether opaque pipeline state changed.
func (s *Service) ActivateSession(ctx context.Context, activation SessionActivation) (resultErr error) {
	activation = cloneSessionActivation(activation)
	managed, err := s.beginSessionOp(ctx, activation.SessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()

	managed.mu.Lock()
	current := cloneSessionRecord(managed.record)
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	if !managed.ready {
		managed.mu.Unlock()
		return ErrNotFound
	}
	if err = validateSessionActivation(current.Session, activation); err != nil {
		managed.mu.Unlock()
		return err
	}
	if current.Activation != nil {
		if !current.Activation.Equal(activation) {
			managed.mu.Unlock()
			return ErrSessionConflict
		}
		if managed.activationReady && !managed.unavailable {
			managed.mu.Unlock()
			return nil
		}
	} else if managed.unavailable {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	pipelineReady := managed.pipelineReady
	managed.updating = true
	managed.mu.Unlock()
	defer func() {
		managed.mu.Lock()
		managed.updating = false
		managed.mu.Unlock()
	}()

	if err = managed.productionMu.LockCtx(ctx); err != nil {
		return err
	}
	defer managed.productionMu.Unlock()
	if !pipelineReady {
		if err = s.ensurePipelineSession(ctx, managed, current); err != nil {
			return err
		}
	}

	if current.Activation == nil {
		current.Activation = &activation
		if err = managed.sessionWriteMu.LockCtx(ctx); err != nil {
			return err
		}
		if err = awaitSessionWrite(ctx, func(done func(error)) {
			s.opts.Storage.SaveSession(ctx, current, done)
		}); err != nil {
			managed.sessionWriteMu.Unlock()
			return fmt.Errorf("collator runtime: save session activation: %w", err)
		}
		managed.mu.Lock()
		managed.record = cloneSessionRecord(current)
		managed.mu.Unlock()
		// Publish while sessionWriteMu still excludes the asynchronous writer.
		// Otherwise it can snapshot the pre-activation record and enqueue that
		// older revision after the durable activation write.
		managed.sessionWriteMu.Unlock()
	}

	if err = s.opts.Pipeline.ActivateSession(ctx, activation, current.Update); err != nil {
		managed.mu.Lock()
		managed.activationReady = false
		if errors.Is(err, ErrAcquisitionNotReady) {
			managed.unavailable = false
			managed.mu.Unlock()

			return fmt.Errorf("collator runtime: activate pipeline session: %w", err)
		}
		managed.unavailable = true
		managed.mu.Unlock()

		return fmt.Errorf("%w: activate pipeline session: %v", ErrSessionUnavailable, err)
	}
	managed.observeSessionStart(activation, current.Update)

	managed.mu.Lock()
	managed.activationReady = true
	managed.unavailable = false
	managed.updating = false
	managed.mu.Unlock()

	return s.reconcileWindows(s.runCtx, managed)
}

// UpdateSession atomically publishes a newer observed-window view to the
// pipeline and production lifecycle. Advancing the window cancels prior production.
func (s *Service) UpdateSession(ctx context.Context, update SessionUpdate) (resultErr error) {
	managed, err := s.beginSessionOp(ctx, update.SessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()

	managed.mu.Lock()
	current := cloneSessionRecord(managed.record)
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	if managed.unavailable {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	if err = validateSessionUpdate(current.Session, update); err != nil {
		managed.mu.Unlock()
		return err
	}
	pipelineChanged := !current.Update.Equal(update)
	pipelineReady := managed.pipelineReady
	writePending := managed.sessionWritePending
	progressReadyAfterWrite := managed.progressReady
	if writePending {
		progressReadyAfterWrite = managed.progressReadyAfterWrite
	}
	if !pipelineChanged && pipelineReady {
		managed.mu.Unlock()
		if !writePending {
			// Pipeline and session WAL state are already current. Reconciliation is
			// service-owned work after that commit boundary: a cleanup failure cannot
			// make the caller retain an older view than the one accepted here.
			s.reconcileFinishedSession(managed)
		}

		return nil
	}
	if pipelineChanged && current.Update.HasCurrentWindow && update.HasCurrentWindow &&
		update.CurrentWindowStart < current.Update.CurrentWindowStart {
		managed.mu.Unlock()
		return fmt.Errorf("%w: current window regressed", ErrStaleWindow)
	}
	if pipelineChanged {
		err = ValidateSessionUpdateAdvance(current.Update, update)
	}
	if err != nil {
		managed.mu.Unlock()
		return err
	}
	managed.updating = true
	if pipelineChanged && update.HasCurrentWindow && (!current.Update.HasCurrentWindow ||
		update.CurrentWindowStart > current.Update.CurrentWindowStart) {
		for id, job := range managed.productions {
			if id.StartSlot < update.CurrentWindowStart {
				job.cancel()
			}
		}
	}
	managed.mu.Unlock()
	defer func() {
		managed.mu.Lock()
		retrying := resultErr != nil && errors.Is(resultErr, ErrAcquisitionNotReady) &&
			managed.updating && !managed.unavailable && !managed.retiring
		resume := resultErr != nil && !retrying && managed.updating &&
			!managed.unavailable && !managed.retiring
		if !retrying {
			managed.updating = false
		}
		managed.mu.Unlock()
		if resume {
			_ = s.reconcileWindows(s.runCtx, managed)
		}
	}()

	if err = tryProductionBarrier(ctx, &managed.productionMu); err != nil {
		return err
	}
	if !pipelineReady {
		if err = s.ensurePipelineSession(ctx, managed, current); err != nil {
			managed.productionMu.Unlock()

			return err
		}
		if !pipelineChanged {
			managed.productionMu.Unlock()
			managed.mu.Lock()
			managed.updating = false
			managed.mu.Unlock()
			if !writePending {
				s.reconcileFinishedSession(managed)
			}

			return nil
		}
	}

	if pipelineChanged {
		if err = s.reserveSessionWrite(managed); err != nil {
			managed.productionMu.Unlock()

			return err
		}
		if err = s.opts.Pipeline.UpdateSession(ctx, current.Session, update); err != nil {
			s.releaseSessionWriteReservation(managed)
			managed.productionMu.Unlock()
			return fmt.Errorf("collator runtime: update pipeline session: %w", err)
		}
		current.Update = cloneSessionUpdate(update)
	}
	s.publishSessionWrite(managed, current, progressReadyAfterWrite)
	managed.observeMasterchainFinalized(update)
	managed.productionMu.Unlock()
	// Pipeline acceptance is the public commit boundary. The service-owned
	// writer now makes this exact revision durable; production and stale-window
	// reconciliation remain closed until its callback succeeds.

	return nil
}

// ApplyConsensusProgress materializes the authenticated candidate lineage
// before publishing its observed window. The operation is serialized with
// all other session mutations. Candidate restoration is idempotent and may
// retain an exact prefix on error; a later window retries that prefix before
// advancing the published base. Pipeline updates and exact storage writes are
// idempotent, so every failed producer-side step remains retryable without
// making the consensus session unavailable.
func (s *Service) ApplyConsensusProgress(
	ctx context.Context,
	progress ConsensusProgress,
) (resultErr error) {
	managed, err := s.beginSessionOp(ctx, progress.SessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()

	managed.mu.Lock()
	current := cloneSessionRecord(managed.record)
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	if managed.unavailable || !managed.ready || !managed.pipelineReady ||
		!managed.activationReady || current.Activation == nil {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	if err = validateConsensusProgress(current.Session, progress); err != nil {
		managed.mu.Unlock()
		return err
	}
	if len(progress.Candidates) > 0 {
		first := progress.Candidates[0].Candidate
		finalizedAnchor := progress.FinalizedAnchor != nil &&
			sameBlockID(first.Block, *progress.FinalizedAnchor)
		if first.Parent.Exists && first.Parent != current.Update.CurrentBase && !finalizedAnchor {
			managed.mu.Unlock()
			return fmt.Errorf(
				"%w: first lineage parent %s differs from current base %s",
				ErrCandidateConflict,
				first.Parent,
				current.Update.CurrentBase,
			)
		}
	}

	next := cloneSessionUpdate(current.Update)
	next.HasCurrentWindow = true
	next.CurrentWindowStart = progress.Window.StartSlot
	next.CurrentWindowObservedSlot = progress.Window.ObservedSlot
	if current.Update.HasCurrentWindow && current.Update.CurrentWindowStart == progress.Window.StartSlot {
		next.CurrentWindowStartAt = current.Update.CurrentWindowStartAt
	} else if progress.Window.ObservedSlot == progress.Window.StartSlot {
		next.CurrentWindowStartAt = progress.StartAt
	} else {
		next.CurrentWindowStartAt = time.Time{}
	}
	next.CurrentBase = progress.Window.Base
	if err = validateSessionUpdate(current.Session, next); err != nil {
		managed.mu.Unlock()
		return err
	}
	if err = ValidateSessionUpdateAdvance(current.Update, next); err != nil {
		managed.mu.Unlock()
		return err
	}
	writePending := managed.sessionWritePending

	managed.updating = true
	// This progress supersedes production from the previously observed view.
	// If local restoration fails, C++ drops that producer task; it does not
	// restart an authorization against the older base. A later successfully
	// materialized progress view arms production again.
	managed.progressReady = false
	// A newer progress attempt disarms every older pending WAL revision before
	// restoration starts. If restoration fails, an older callback must not
	// re-arm production against the superseded base.
	managed.progressReadyAfterWrite = false
	if !current.Update.HasCurrentWindow || next.CurrentWindowStart > current.Update.CurrentWindowStart {
		for id, job := range managed.productions {
			if id.StartSlot < next.CurrentWindowStart {
				job.cancel()
			}
		}
	}
	managed.mu.Unlock()
	defer func() {
		managed.mu.Lock()
		resume := resultErr != nil && managed.updating && !managed.unavailable && !managed.retiring
		managed.updating = false
		managed.mu.Unlock()
		if resume {
			_ = s.reconcileWindows(s.runCtx, managed)
		}
	}()

	// The cancels above are asynchronous, so this call must outlive the unwind
	// of the productions it just superseded. Rejecting here is the expensive
	// outcome: it disarms production and the runtime skips the window outright.
	if err = waitProductionBarrier(ctx, &managed.productionMu, productionBarrierWait(next)); err != nil {
		return err
	}
	defer managed.productionMu.Unlock()

	active := activatedSession(current.Session, *current.Activation)
	restoreUpdate := cloneSessionUpdate(current.Update)
	if len(progress.Candidates) > 0 {
		restoreUpdate.CurrentBase = progress.Candidates[0].Candidate.Parent
	}
	var finalizedAnchor *ton.BlockIDExt
	if progress.FinalizedAnchor != nil {
		anchor := cloneBlockID(*progress.FinalizedAnchor)
		finalizedAnchor = &anchor
	}
	finalizedAnchorState := progress.FinalizedAnchorState
	var previous *CandidateArtifact
	for i := range progress.Candidates {
		artifact := progress.Candidates[i]
		request := BuildRequest{
			Session:              active,
			Update:               restoreUpdate,
			Slot:                 artifact.Candidate.ID.Slot,
			Leader:               artifact.Candidate.Leader,
			Parent:               artifact.Candidate.Parent,
			Previous:             previous,
			FinalizedAnchor:      finalizedAnchor,
			FinalizedAnchorState: finalizedAnchorState,
		}
		if err = s.opts.Pipeline.RestoreCandidate(ctx, request, artifact); err != nil {
			return fmt.Errorf("collator runtime: restore consensus candidate: %w", err)
		}
		previous = &progress.Candidates[i]
		finalizedAnchor = nil
		finalizedAnchorState = nil
	}

	if err = s.reserveSessionWrite(managed); err != nil {
		return err
	}
	if err = s.opts.Pipeline.UpdateSession(ctx, current.Session, next); err != nil {
		s.releaseSessionWriteReservation(managed)
		return fmt.Errorf("collator runtime: publish consensus progress: %w", err)
	}

	changed := writePending || !current.Update.Equal(next)
	current.Update = cloneSessionUpdate(next)
	if changed {
		s.publishSessionWrite(managed, current, true)
	} else {
		s.releaseSessionWriteReservation(managed)
		managed.mu.Lock()
		managed.record = current
		managed.progressReady = true
		managed.updating = false
		managed.mu.Unlock()
	}
	if !changed {
		// The opaque pipeline and session WAL already expose this exact progress.
		// Cleanup and production scheduling belong to the service lifetime.
		s.reconcileFinishedSession(managed)
	}

	return nil
}

// ObserveConsensusFinalized advances the producer watermark as soon as a
// final certificate is observed. This deliberately does not wait for the
// asynchronously accepted block to appear in the node database: the C++
// BlockProducer and CollatorProducer consume the same FinalizeBlock event.
func (s *Service) ObserveConsensusFinalized(
	ctx context.Context,
	sessionID [32]byte,
	block ton.BlockIDExt,
) error {
	managed, err := s.beginSessionOp(ctx, sessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()

	managed.mu.Lock()
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	// Finalization only advances the producer policy watermark. Recovery may
	// replay its durable prefix before ActivateSession binds the exact genesis;
	// retain that watermark so activation can seed the policy monotonically.
	// A recovered durable activation may also still be waiting for the node view
	// which makes its pipeline ready, but C++ delivers FinalizeBlock meanwhile.
	if managed.unavailable || !managed.ready {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	shard := managed.record.Session.Shard
	managed.mu.Unlock()

	if err = validateBlockID(block); err != nil || block.Workchain != shard.Workchain || block.Shard != shard.Shard {
		return fmt.Errorf("%w: finalized block does not belong to the session shard", ErrInvalidInput)
	}
	managed.observeConsensusFinalized(block.SeqNo)

	return nil
}

// RetireSession cancels production, retires the pipeline, and removes durable
// session state. Pipeline and storage operations are idempotent by contract.
func (s *Service) RetireSession(ctx context.Context, sessionID [32]byte) error {
	managed, err := s.beginSessionOp(ctx, sessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()

	managed.mu.Lock()
	wasRetiring := managed.retiring
	wasUnavailable := managed.unavailable
	durable := managed.ready
	managed.retiring = true
	for _, job := range managed.productions {
		job.cancel()
	}
	managed.mu.Unlock()
	managed.wakeSessionWrite()

	if err = managed.productionMu.LockCtx(ctx); err != nil {
		if !wasRetiring {
			managed.mu.Lock()
			managed.retiring = false
			managed.mu.Unlock()
			managed.wakeSessionWrite()
			if !wasUnavailable {
				_ = s.reconcileWindows(s.runCtx, managed)
			}
		}
		return err
	}
	if err = managed.sessionWriteMu.LockCtx(ctx); err != nil {
		resume := false
		if !wasRetiring {
			managed.mu.Lock()
			managed.retiring = false
			managed.mu.Unlock()
			resume = !wasUnavailable
		}
		managed.productionMu.Unlock()
		managed.wakeSessionWrite()
		if resume {
			_ = s.reconcileWindows(s.runCtx, managed)
		}

		return err
	}
	if err = s.opts.Pipeline.RetireSession(ctx, sessionID); err != nil && !isRetiredSessionError(err) {
		resume := false
		if !wasRetiring {
			managed.mu.Lock()
			managed.retiring = false
			managed.mu.Unlock()
			resume = !wasUnavailable
		}
		managed.sessionWriteMu.Unlock()
		managed.productionMu.Unlock()
		managed.wakeSessionWrite()
		if resume {
			_ = s.reconcileWindows(s.runCtx, managed)
		}
		return fmt.Errorf("collator runtime: retire pipeline session: %w", err)
	}
	managed.mu.Lock()
	managed.unavailable = true
	managed.mu.Unlock()
	if err = s.opts.Storage.DeleteSession(ctx, sessionID); err != nil &&
		(durable || (!errors.Is(err, ErrNotFound) && !errors.Is(err, ErrSessionRetired))) {
		// RetireSession already changed the opaque pipeline. Keep the session
		// closed and retry the idempotent retirement instead of resuming work
		// against the stale durable descriptor.
		managed.sessionWriteMu.Unlock()
		managed.productionMu.Unlock()
		managed.wakeSessionWrite()
		return fmt.Errorf("collator runtime: delete session: %w", err)
	}
	managed.sessionWriteMu.Unlock()
	managed.productionMu.Unlock()
	managed.wakeSessionWrite()

	s.mu.Lock()
	delete(s.sessions, sessionID)
	s.fenceRetiredLocked(sessionID)
	s.mu.Unlock()
	return nil
}

// fenceRetiredLocked records a retirement tombstone and evicts the oldest ones
// beyond retiredTombstoneLimit. The caller holds mu.
func (s *Service) fenceRetiredLocked(id [32]byte) {
	if _, fenced := s.retired[id]; fenced {
		return
	}
	s.retired[id] = struct{}{}
	s.retiredOrder = append(s.retiredOrder, id)
	for len(s.retiredOrder) > retiredTombstoneLimit {
		delete(s.retired, s.retiredOrder[0])
		s.retiredOrder = s.retiredOrder[1:]
	}
}

// releaseRetiredLocked drops a tombstone together with its ordering entry, so a
// later eviction cannot reclaim a fence that a re-admission already cleared.
// The caller holds mu.
func (s *Service) releaseRetiredLocked(id [32]byte) {
	if _, fenced := s.retired[id]; !fenced {
		return
	}
	delete(s.retired, id)
	s.retiredOrder = slices.DeleteFunc(s.retiredOrder, func(fenced [32]byte) bool { return fenced == id })
}

// Probe validates an upcoming delegation without reserving memory or durable
// state, matching consensus.pleaseCollatePrepare.
func (s *Service) Probe(ctx context.Context, preparation WindowPreparation) error {
	if s.opts.ProductionMode != ProductionModeDelegated {
		return ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	managed, err := s.beginSessionOp(ctx, preparation.SessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()
	id := WindowID{SessionID: preparation.SessionID, StartSlot: preparation.StartSlot}
	managed.mu.Lock()
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	if managed.unavailable {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	_, err = s.validateWindowSource(managed.record, preparation.SourceADNL, preparation.StartSlot)
	if err != nil {
		managed.mu.Unlock()
		return err
	}
	if _, exists := managed.authorizations[id]; exists {
		managed.mu.Unlock()
		return ErrAlreadyDelegated
	}
	if _, exists := managed.productions[id]; exists {
		managed.mu.Unlock()
		return ErrAlreadyDelegated
	}
	managed.mu.Unlock()

	return nil
}

// CommitDelegation accepts one final leader authorization in receiver memory
// and starts current-window production. Candidate markers, not delegations,
// are the durable anti-equivocation boundary.
func (s *Service) CommitDelegation(ctx context.Context, request WindowRequest) error {
	if s.opts.ProductionMode != ProductionModeDelegated {
		return ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	managed, err := s.beginSessionOp(ctx, request.SessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()
	id := request.ID()
	managed.mu.Lock()
	if managed.retiring {
		managed.mu.Unlock()
		return ErrSessionRetired
	}
	if managed.unavailable {
		managed.mu.Unlock()
		return ErrSessionUnavailable
	}
	leader, err := s.validateWindowSource(
		managed.record,
		request.SourceADNL,
		id.StartSlot,
	)
	if err != nil {
		managed.mu.Unlock()
		return err
	}
	window := delegatedAuthorization{
		ID:                  id,
		Leader:              leader,
		SourceADNL:          request.SourceADNL,
		CollatorKeyID:       s.opts.CollatorKeyID,
		DelegationSignature: append([]byte(nil), request.PleaseCollate.Signature...),
		State:               delegatedAuthorizationPending,
	}
	if existing, exists := managed.authorizations[id]; exists {
		managed.mu.Unlock()
		if sameDelegationAuthorization(existing, window) {
			return nil
		}

		return ErrWindowConflict
	}
	if _, exists := managed.productions[id]; exists {
		managed.mu.Unlock()

		return ErrWindowConflict
	}
	if !simplex.VerifyDelegationSignature(
		ed25519.PublicKey(managed.record.Session.Validators[leader].PublicKey[:]),
		request.SessionID,
		id.StartSlot,
		s.opts.CollatorKeyID,
		request.PleaseCollate.Signature,
	) {
		managed.mu.Unlock()

		return errors.New("collator runtime: delegation signature is invalid")
	}

	managed.authorizations[id] = window
	if managed.productionEligibleLocked(id.StartSlot) {
		_ = s.launchProductionLocked(managed, delegatedProductionWindow(window))
	}
	managed.mu.Unlock()

	return nil
}

// ActivateSelfWindow opens one local-validator production window in memory.
// Its durable authority is the session/progress WAL plus each candidate's
// anti-equivocation marker; there is deliberately no delegation fsync at
// activation. A late WAL completion cannot start work after Deadline.
func (s *Service) ActivateSelfWindow(ctx context.Context, request SelfWindowRequest) error {
	if s.opts.ProductionMode != ProductionModeSelf {
		return ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.Signer == nil {
		return fmt.Errorf("%w: self-window signer is absent", ErrInvalidInput)
	}
	if request.Deadline.IsZero() {
		return fmt.Errorf("%w: self-window deadline is absent", ErrInvalidInput)
	}
	if !time.Now().Before(request.Deadline) {
		return ErrStaleWindow
	}

	managed, err := s.beginSessionOp(ctx, request.SessionID)
	if err != nil {
		return err
	}
	defer managed.controlMu.Unlock()

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.retiring {
		return ErrSessionRetired
	}
	if managed.unavailable || !managed.ready {
		return ErrSessionUnavailable
	}
	current := managed.record
	if request.StartSlot%current.Session.SlotsPerLeaderWindow != 0 {
		return fmt.Errorf("%w: self window is not aligned", ErrInvalidInput)
	}
	if !productionWindowReady(current.Update) || current.Update.CurrentWindowStart != request.StartSlot {
		return ErrStaleWindow
	}
	id := WindowID{SessionID: request.SessionID, StartSlot: request.StartSlot}
	if _, exists := managed.authorizations[id]; exists {
		return ErrWindowConflict
	}
	if existing, exists := managed.selfWindows[id]; exists {
		if !existing.Deadline.Equal(request.Deadline) {
			return ErrWindowConflict
		}

		return nil
	}
	if managed.productions[id] != nil {
		return ErrWindowConflict
	}

	managed.selfWindows[id] = request
	if managed.productionEligibleLocked(request.StartSlot) {
		return s.launchProductionLocked(managed, selfProductionWindow(current.Session, request))
	}

	return nil
}

// Status reports active production and shared-database pressure.
func (s *Service) Status(ctx context.Context) (Status, error) {
	storage, err := s.opts.Storage.Status(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("collator runtime: storage status: %w", err)
	}
	s.mu.Lock()
	state := s.state
	started := s.started
	s.mu.Unlock()
	s.statusMu.Lock()
	lastError := s.lastError
	lastCompleted := s.lastCompleted
	s.statusMu.Unlock()

	return Status{
		Started:          started,
		Closing:          state == serviceClosing,
		Closed:           state == serviceClosed,
		ActiveWindows:    int(s.active.Load()),
		RetryingWindows:  int(s.retrying.Load()),
		CompletedWindows: s.completed.Load(),
		FailedWindows:    s.failed.Load(),
		LastError:        lastError,
		LastCompleted:    lastCompleted,
		Storage:          storage,
	}, nil
}

func runningStateError(state serviceState) error {
	switch state {
	case serviceRunning:
		return nil
	case serviceNew, serviceStarting:
		return ErrNotStarted
	default:
		return ErrClosed
	}
}

func (s *Service) admitPreparedSession(record SessionRecord) (*managedCollatorSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := runningStateError(s.state); err != nil {
		return nil, false, err
	}
	id := record.Session.ID
	managed := s.sessions[id]
	if managed != nil {
		return managed, false, nil
	}
	// A session ID can legitimately disappear from a tentative topology and be
	// admitted again when that same validator group becomes active. Retirement
	// is a generation boundary, not a permanent ban on the consensus ID. Clear
	// the tombstone only while publishing the new generation under Service.mu;
	// preparations already queued on the retired handle still fail the recheck
	// below or conflict with this new pointer.
	s.releaseRetiredLocked(id)

	managed = newManagedCollatorSession(record, false)
	// Publish an unprepared handle only after its creator owns the lifecycle
	// lock. Every competing operation can resolve the pointer under Service.mu,
	// but cannot observe or mutate it before PrepareSession finishes or removes
	// it on an atomic failure.
	managed.controlMu.Lock()
	s.sessions[id] = managed
	return managed, true, nil
}

func (s *Service) recheckPreparedSession(
	id [32]byte,
	managed *managedCollatorSession,
	created bool,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := runningStateError(s.state); err != nil {
		return false, err
	}
	if _, retired := s.retired[id]; retired {
		return false, ErrSessionRetired
	}
	current := s.sessions[id]
	if current == nil {
		s.sessions[id] = managed
		return true, nil
	}
	if current != managed {
		return false, ErrSessionConflict
	}
	return created, nil
}

func (s *Service) runningSession(id [32]byte) (*managedCollatorSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := runningStateError(s.state); err != nil {
		return nil, err
	}
	if _, retired := s.retired[id]; retired {
		return nil, ErrSessionRetired
	}
	managed := s.sessions[id]
	if managed == nil {
		return nil, ErrNotFound
	}
	return managed, nil
}

// beginSessionOp resolves a running session and hands the caller its lifecycle
// lock. On success the caller owns controlMu and must release it; on every
// error path the lock is already released here, because the caller cannot have
// armed its defer yet.
func (s *Service) beginSessionOp(ctx context.Context, id [32]byte) (*managedCollatorSession, error) {
	managed, err := s.runningSession(id)
	if err != nil {
		return nil, err
	}
	if err = managed.controlMu.LockCtx(ctx); err != nil {
		return nil, err
	}
	if err = s.recheckRunningSession(id, managed); err != nil {
		managed.controlMu.Unlock()

		return nil, err
	}

	return managed, nil
}

// recheckRunningSession closes the gap between admission and controlMu. The
// caller holds controlMu, while Service.mu is held only for this bounded state
// check, so Close can never retire the handle concurrently or be blocked on a
// global lock held across pipeline/storage I/O.
func (s *Service) recheckRunningSession(id [32]byte, managed *managedCollatorSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := runningStateError(s.state); err != nil {
		return err
	}
	if _, retired := s.retired[id]; retired {
		return ErrSessionRetired
	}
	current := s.sessions[id]
	if current == nil {
		return ErrNotFound
	}
	if current != managed {
		return ErrSessionConflict
	}
	return nil
}

func (s *Service) removePreparedSession(id [32]byte, managed *managedCollatorSession) {
	s.mu.Lock()
	if s.sessions[id] == managed {
		delete(s.sessions, id)
	}
	s.mu.Unlock()
}

func newManagedCollatorSession(record SessionRecord, ready bool) *managedCollatorSession {
	managed := &managedCollatorSession{
		controlMu:        newCtxMutex(),
		productionMu:     newCtxMutex(),
		sessionWriteMu:   newCtxMutex(),
		record:           cloneSessionRecord(record),
		ready:            ready,
		pipelineReady:    ready,
		authorizations:   make(map[WindowID]delegatedAuthorization),
		selfWindows:      make(map[WindowID]SelfWindowRequest),
		productions:      make(map[WindowID]*productionJob),
		sessionWriteWake: make(chan struct{}, 1),
	}
	if record.Activation != nil {
		managed.observeSessionStart(*record.Activation, record.Update)
	}

	return managed
}

func (m *managedCollatorSession) observeSessionStart(
	activation SessionActivation,
	update SessionUpdate,
) {
	seqno := uint32(0)
	for i := range activation.Genesis {
		seqno = max(seqno, activation.Genesis[i].SeqNo)
	}

	m.policyMu.Lock()
	m.emptyPolicy.ObserveSessionStart(seqno)
	if update.HasFinalizedBlock {
		m.emptyPolicy.ObserveMCFinalized(update.FinalizedBlock.SeqNo)
	}
	m.policyStarted = true
	m.policyMu.Unlock()
}

func (m *managedCollatorSession) observeConsensusFinalized(seqno uint32) {
	m.policyMu.Lock()
	m.emptyPolicy.ObserveConsensusFinalized(seqno)
	m.policyMu.Unlock()
}

func (m *managedCollatorSession) observeMasterchainFinalized(update SessionUpdate) {
	if !update.HasFinalizedBlock {
		return
	}

	m.policyMu.Lock()
	if m.policyStarted {
		m.emptyPolicy.ObserveMCFinalized(update.FinalizedBlock.SeqNo)
	}
	m.policyMu.Unlock()
}

func (m *managedCollatorSession) shouldGenerateEmpty(
	isMasterchain bool,
	state CandidateState,
) bool {
	m.policyMu.Lock()
	defer m.policyMu.Unlock()

	return m.policyStarted && m.emptyPolicy.ShouldGenerateEmptyBlock(
		isMasterchain,
		state.BeforeSplit,
		state.NextSeqno,
	)
}

func (m *managedCollatorSession) allowEmptyOnGenerationFailure(timeout time.Duration) bool {
	m.policyMu.Lock()
	defer m.policyMu.Unlock()

	return m.policyStarted && m.emptyPolicy.AllowEmptyOnGenerationFailure(timeout)
}

func (s *Service) validateWindowSource(record SessionRecord, source [32]byte, start uint32) (uint32, error) {
	session := record.Session
	update := record.Update
	if start%session.SlotsPerLeaderWindow != 0 {
		return 0, errors.New("collator runtime: window start is not aligned")
	}
	baseline := uint32(0)
	if update.HasCurrentWindow {
		baseline = update.CurrentWindowStart
		if start < baseline {
			return 0, ErrStaleWindow
		}
	}
	distance := (start - baseline) / session.SlotsPerLeaderWindow
	if distance >= delegationWindowHorizon {
		return 0, ErrWindowTooFar
	}
	leader := start / session.SlotsPerLeaderWindow % uint32(len(session.Validators))
	if session.Validators[leader].ADNLID != source {
		return 0, ErrUnauthorized
	}
	if _, allowed := s.allowed[source]; !s.allowAll && !allowed {
		return 0, ErrUnauthorized
	}
	return leader, nil
}

// reconcileWindows forgets receiver-memory authority which progress has moved
// past and starts production for authority matching the published window.
func (s *Service) reconcileWindows(ctx context.Context, managed *managedCollatorSession) error {
	managed.mu.Lock()
	defer managed.mu.Unlock()

	if managed.retiring || managed.unavailable || managed.sessionWritePending {
		return nil
	}
	current := managed.record.Update.CurrentWindowStart
	now := time.Now()
	for id, request := range managed.selfWindows {
		if id.StartSlot < current || !now.Before(request.Deadline) {
			delete(managed.selfWindows, id)
			managed.forgetEmitted(id)
		}
	}
	for id := range managed.authorizations {
		if id.StartSlot < current {
			delete(managed.authorizations, id)
			managed.forgetEmitted(id)
		}
	}
	if !managed.productionEligibleLocked(current) {
		return nil
	}

	return s.launchCurrentProductionsLocked(ctx, managed, current)
}

// launchCurrentProductionsLocked starts every authorization that opens the
// published window and is not already running. The caller holds managed.mu and
// has validated productionEligibleLocked(current) under that same acquisition.
func (s *Service) launchCurrentProductionsLocked(
	ctx context.Context,
	managed *managedCollatorSession,
	current uint32,
) error {
	for id, window := range managed.authorizations {
		if id.StartSlot != current || window.State != delegatedAuthorizationPending || managed.productions[id] != nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.launchProductionLocked(managed, delegatedProductionWindow(window)); err != nil {
			return err
		}
	}
	for id, request := range managed.selfWindows {
		if id.StartSlot != current || managed.productions[id] != nil {
			continue
		}
		if !time.Now().Before(request.Deadline) {
			delete(managed.selfWindows, id)

			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.launchProductionLocked(
			managed,
			selfProductionWindow(managed.record.Session, request),
		); err != nil {
			return err
		}
	}

	return nil
}

func productionWindowReady(update SessionUpdate) bool {
	return update.HasCurrentWindow && update.CurrentWindowObservedSlot == update.CurrentWindowStart
}

// productionEligibleLocked is the single definition of "this session may start
// producing the window opening at start right now". Every launch decision must
// go through it so a new gate cannot be added to one caller and forgotten in
// the other. The caller holds m.mu.
func (m *managedCollatorSession) productionEligibleLocked(start uint32) bool {
	return !m.updating && !m.retiring && !m.unavailable && m.pipelineReady && m.activationReady && m.progressReady &&
		m.record.Activation != nil && productionWindowReady(m.record.Update) &&
		m.record.Update.CurrentWindowStart == start
}

func (s *Service) launchProductionLocked(managed *managedCollatorSession, window productionWindow) error {
	s.mu.Lock()
	if err := runningStateError(s.state); err != nil {
		s.mu.Unlock()

		return err
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if window.Authority == CandidateAuthoritySelf {
		if !time.Now().Before(window.Deadline) {
			s.mu.Unlock()

			return ErrStaleWindow
		}
		ctx, cancel = context.WithDeadline(s.runCtx, window.Deadline)
	} else {
		ctx, cancel = context.WithCancel(s.runCtx)
	}
	window.DelegationSignature = append([]byte(nil), window.DelegationSignature...)
	job := &productionJob{session: managed, window: window, ctx: ctx, cancel: cancel}
	managed.productions[window.ID] = job
	s.productions.Add(1)
	s.mu.Unlock()

	go s.produceWindow(job)

	return nil
}

func (s *Service) produceWindow(job *productionJob) {
	defer s.productions.Done()
	defer job.cancel()
	s.active.Add(1)
	defer s.active.Add(-1)

	var err error
	if s.opts.Observer != nil {
		job.session.mu.Lock()
		chain := metricChain(job.session.record.Session.Shard.IsMasterchain())
		job.session.mu.Unlock()
		started := time.Now()
		s.opts.Observer.AddCollationWindowInflight(chain, 1)
		defer func() {
			s.opts.Observer.AddCollationWindowInflight(chain, -1)
			s.opts.Observer.ObserveCollationWindow(WindowObservation{
				Chain:    chain,
				Result:   collationResult(err),
				Duration: time.Since(started),
			})
		}()
	}

	err = job.session.productionMu.LockCtx(job.ctx)
	if err == nil {
		err = s.produceWindowWithRetry(job)
		// Bookkeeping lands before the production barrier is released, so a
		// duplicate receiver call and advancing progress see one coherent state.
		job.session.releaseProduction(job.window.ID, err)
		if err == nil {
			job.session.forgetEmitted(job.window.ID)
		}
		job.session.productionMu.Unlock()
	} else {
		job.session.releaseProduction(job.window.ID, err)
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, ErrStaleWindow) {
		// Lifecycle calls cancel accepted work before joining production. If that
		// call later aborts, reconciling after removal closes both completion orders.
		s.reconcileFinishedSession(job.session)
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		s.failed.Add(1)
		s.recordProductionError(err)
	} else if err == nil {
		s.completed.Add(1)
		s.statusMu.Lock()
		s.lastCompleted = time.Now()
		s.statusMu.Unlock()
	}
}

// releaseProduction drops the running job. A terminal result stays in receiver
// memory as an idempotency record until progress moves past its window. A
// lifecycle cancellation remains pending so an aborted update can resume it.
func (m *managedCollatorSession) releaseProduction(id WindowID, resultErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.productions, id)
	terminal := resultErr == nil || !errors.Is(resultErr, context.Canceled)
	if terminal {
		if window, exists := m.authorizations[id]; exists {
			if resultErr == nil {
				window.State = delegatedAuthorizationCompleted
			} else {
				window.State = delegatedAuthorizationCancelled
			}
			m.authorizations[id] = window
		}
		delete(m.selfWindows, id)
	}
}

func (s *Service) produceWindowWithRetry(job *productionJob) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	retries := 0
	wait := productionInputWait{start: time.Now()}
	for {
		err := s.runProduction(job)
		if errors.Is(err, ErrAcquisitionNotReady) {
			wait.observe(err)
		}
		if !retryableProductionError(err) {
			s.logProductionInputWait(job, wait, err)

			return err
		}
		if s.opts.Observer != nil {
			job.session.mu.Lock()
			chain := metricChain(job.session.record.Session.Shard.IsMasterchain())
			job.session.mu.Unlock()
			s.opts.Observer.ObserveCollationRetry(chain, productionRetryReason(err))
		}

		s.recordProductionError(err)
		delay := productionRetryDelay(retries)
		retries++
		if delay == 0 {
			if err = job.ctx.Err(); err != nil {
				return err
			}

			continue
		}

		s.retrying.Add(1)
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		select {
		case <-job.ctx.Done():
			s.retrying.Add(-1)

			return job.ctx.Err()
		case <-timer.C:
			s.retrying.Add(-1)
		}
	}
}

// productionInputWait accumulates one leader window's wait for local inputs.
// The per-attempt failure is not an event worth a line — the same window
// produces a burst of identical ones — so the window reports what it waited for
// once, when it stops waiting.
type productionInputWait struct {
	start    time.Time
	attempts int
	first    error
}

func (w *productionInputWait) observe(err error) {
	w.attempts++
	if w.first == nil {
		w.first = err
	}
}

// logProductionInputWait emits that one line. Its message is deliberately
// distinct from "block collation failed" so a search for genuine build failures
// does not have to filter this out — which is exactly what the previous shared
// message forced, and what made a 0.18% real failure rate read as 39%.
func (s *Service) logProductionInputWait(job *productionJob, wait productionInputWait, err error) {
	if wait.attempts == 0 {
		return
	}
	event := s.log.Debug()
	if event == nil {
		return
	}

	event.
		Hex("session_id", job.window.ID.SessionID[:]).
		Uint32("window_start", job.window.ID.StartSlot).
		Uint32("leader", job.window.Leader).
		Int("attempts", wait.attempts).
		Dur("waited", time.Since(wait.start)).
		AnErr("first_error", wait.first).
		AnErr("outcome", err).
		Msg("collation window waited for inputs")
}

func productionRetryDelay(retries int) time.Duration {
	if retries == 0 {
		return 0
	}
	delay := productionRetryInitialDelay << min(retries-1, 2)
	if delay > productionRetryMaxDelay {
		return productionRetryMaxDelay
	}

	return delay
}

func (s *Service) recordProductionError(err error) {
	s.statusMu.Lock()
	s.lastError = err.Error()
	s.statusMu.Unlock()
}

// reserveSessionWrite joins the service lifetime before an opaque pipeline
// mutation may commit. beginClose changes the service state under the same
// Service.mu before waiting for productions, so it cannot start its wait in
// the gap between Pipeline.UpdateSession and publication of the accepted view.
func (s *Service) reserveSessionWrite(managed *managedCollatorSession) error {
	start := false
	managed.mu.Lock()
	if managed.retiring {
		managed.mu.Unlock()

		return ErrSessionRetired
	}
	if managed.unavailable {
		managed.mu.Unlock()

		return ErrSessionUnavailable
	}
	s.mu.Lock()
	if err := runningStateError(s.state); err != nil {
		s.mu.Unlock()
		managed.mu.Unlock()

		return err
	}
	managed.sessionWriteReserved++
	if !managed.sessionWriteRunning {
		managed.sessionWriteRunning = true
		s.productions.Add(1)
		start = true
	}
	s.mu.Unlock()
	managed.mu.Unlock()

	if start {
		go s.runSessionWrites(managed)
	}

	return nil
}

// releaseSessionWriteReservation closes a pipeline attempt which did not
// commit or whose accepted view was already durable. The joined worker remains
// responsible for deciding when no reservation or pending revision remains.
func (s *Service) releaseSessionWriteReservation(managed *managedCollatorSession) {
	managed.mu.Lock()
	if managed.sessionWriteReserved == 0 {
		managed.mu.Unlock()
		panic("collator runtime: release of unreserved session write")
	}
	managed.sessionWriteReserved--
	managed.mu.Unlock()
	managed.wakeSessionWrite()
}

// publishSessionWrite records the view the opaque pipeline has accepted and
// transfers its pre-commit reservation to one pending durable revision.
// Callers still hold the production barrier, so production cannot observe the
// revision between the pipeline commit and this publication.
func (s *Service) publishSessionWrite(
	managed *managedCollatorSession,
	record SessionRecord,
	progressReady bool,
) {
	managed.mu.Lock()
	if managed.sessionWriteReserved == 0 {
		managed.mu.Unlock()
		panic("collator runtime: publish of unreserved session write")
	}
	managed.record = cloneSessionRecord(record)
	managed.sessionWriteRevision++
	managed.sessionWritePending = true
	managed.progressReadyAfterWrite = progressReady
	managed.progressReady = false
	managed.updating = false
	managed.sessionWriteReserved--
	managed.mu.Unlock()
	managed.wakeSessionWrite()
}

func (m *managedCollatorSession) wakeSessionWrite() {
	select {
	case m.sessionWriteWake <- struct{}{}:
	default:
	}
}

// runSessionWrites coalesces accepted revisions and persists only the newest
// one after any in-flight write completes. sessionWriteMu linearizes each
// synchronous store admission with RetireSession's DeleteSession admission;
// the callback may complete later, but FIFO ordering then guarantees delete
// remains the last durable operation for that generation.
func (s *Service) runSessionWrites(managed *managedCollatorSession) {
	defer s.productions.Done()
	retryDelay := sessionWriteRetryInitialDelay

	for {
		select {
		case <-managed.sessionWriteWake:
		default:
		}

		managed.sessionWriteMu.Lock()

		managed.mu.Lock()
		if managed.unavailable {
			managed.sessionWriteRunning = false
			managed.mu.Unlock()
			managed.sessionWriteMu.Unlock()

			return
		}
		if managed.retiring {
			managed.mu.Unlock()
			managed.sessionWriteMu.Unlock()
			<-managed.sessionWriteWake

			continue
		}
		if !managed.sessionWritePending {
			if managed.sessionWriteReserved == 0 {
				managed.sessionWriteRunning = false
				managed.mu.Unlock()
				managed.sessionWriteMu.Unlock()

				return
			}
			managed.mu.Unlock()
			managed.sessionWriteMu.Unlock()
			<-managed.sessionWriteWake

			continue
		}
		revision := managed.sessionWriteRevision
		record := cloneSessionRecord(managed.record)
		managed.mu.Unlock()

		result := make(chan error, 1)
		// Once the pipeline has accepted a revision, service shutdown drains its
		// WAL independently of the caller and run contexts. Close joins this
		// worker and may time out, but it cannot silently discard the revision.
		s.opts.Storage.SaveSession(context.WithoutCancel(s.runCtx), record, func(err error) { result <- err })
		managed.sessionWriteMu.Unlock()
		err := <-result

		managed.mu.Lock()
		latest := revision == managed.sessionWriteRevision
		if err == nil && latest && !managed.retiring && !managed.unavailable {
			managed.sessionWritePending = false
			managed.progressReady = managed.progressReadyAfterWrite
			managed.progressReadyAfterWrite = false
			reserved := managed.sessionWriteReserved != 0
			if !reserved {
				managed.sessionWriteRunning = false
			}
			managed.mu.Unlock()
			s.reconcileFinishedSession(managed)
			if reserved {
				continue
			}

			return
		}
		if managed.unavailable {
			managed.sessionWriteRunning = false
			managed.mu.Unlock()

			return
		}
		managed.mu.Unlock()

		if err == nil || !latest {
			retryDelay = sessionWriteRetryInitialDelay

			continue
		}
		s.recordSessionWriteError(managed, err)

		timer := time.NewTimer(retryDelay)
		select {
		case <-managed.sessionWriteWake:
			timer.Stop()
			retryDelay = sessionWriteRetryInitialDelay
		case <-timer.C:
			retryDelay = min(retryDelay*2, sessionWriteRetryMaxDelay)
		}
	}
}

func (s *Service) recordSessionWriteError(managed *managedCollatorSession, err error) {
	managed.mu.Lock()
	id := managed.record.Session.ID
	managed.mu.Unlock()

	s.statusMu.Lock()
	s.lastError = fmt.Sprintf("collator runtime: persist accepted session %x: %v", id, err)
	s.statusMu.Unlock()
	s.log.Warn().
		Err(err).
		Hex("session_id", id[:]).
		Msg("collator session persistence retry failed")
}

func (s *Service) reconcileFinishedSession(managed *managedCollatorSession) {
	s.recordReconcileError(s.reconcileWindows(s.runCtx, managed))
}

func (s *Service) recordReconcileError(err error) bool {
	if err == nil || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
		return false
	}

	s.statusMu.Lock()
	s.lastError = err.Error()
	s.statusMu.Unlock()

	return true
}

func retryableProductionError(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errHardBuildDeadline) &&
		!errors.Is(err, ErrStaleWindow) && !errors.Is(err, errWindowNotResumable) &&
		!errors.Is(err, ErrCandidateConflict) && !errors.Is(err, ErrWindowConflict) &&
		!errors.Is(err, ErrSessionConflict) && !errors.Is(err, ErrInvalidInput) &&
		!errors.Is(err, ErrUnsupported) && !errors.Is(err, ErrSizeLimit) &&
		!errors.Is(err, ErrCollatedRootNotFound)
}

func (s *Service) runProduction(job *productionJob) error {
	managed := job.session
	if err := job.ctx.Err(); err != nil {
		return err
	}

	managed.mu.Lock()
	record := cloneSessionRecord(managed.record)
	managed.mu.Unlock()
	chain := metricChain(record.Session.Shard.IsMasterchain())
	if record.Activation == nil {
		return ErrSessionUnavailable
	}
	activeSession := activatedSession(record.Session, *record.Activation)
	if !productionWindowReady(record.Update) || record.Update.CurrentWindowStart != job.window.ID.StartSlot {
		return ErrStaleWindow
	}

	parent := record.Update.CurrentBase
	var previous *CandidateArtifact
	var future *candidateBuildFuture
	defer func() {
		if future != nil {
			future.stop()
		}
	}()
	// advance carries the consensus lineage into the next slot. Every branch
	// that emits a candidate must go through it: a branch that chains the next
	// candidate to a stale parent is consensus-visible and nothing else catches it.
	advance := func(artifact CandidateArtifact) {
		parent = simplex.Parent(artifact.Candidate.ID)
		previous = &artifact
	}
	end := job.window.ID.StartSlot + record.Session.SlotsPerLeaderWindow
	if end < job.window.ID.StartSlot {
		return errors.New("collator runtime: leader window overflows slot space")
	}
	for slot := job.window.ID.StartSlot; slot < end; slot++ {
		stored, err := s.opts.Storage.Candidate(job.ctx, job.window.ID, slot)
		if err == nil {
			productionStarted := s.metricStageStarted()
			remembered, found := managed.recallEmitted(job.window.ID, slot)
			artifact, artifactErr := s.artifactFromRecord(record.Session, job.window, stored, remembered, found)
			if artifactErr != nil {
				s.observeCandidateProduction(chain, CandidateKindRecovered, productionStarted, artifactErr)

				return artifactErr
			}
			if artifact.Candidate.Parent != parent {
				s.observeCandidateProduction(
					chain,
					CandidateKindRecovered,
					productionStarted,
					ErrCandidateConflict,
				)

				return ErrCandidateConflict
			}
			var finalizedAnchor *ton.BlockIDExt
			if previous == nil && record.Update.HasFinalizedBlock &&
				sameBlockID(artifact.Candidate.Block, record.Update.FinalizedBlock) {
				anchor := cloneBlockID(record.Update.FinalizedBlock)
				finalizedAnchor = &anchor
			}
			restore := BuildRequest{
				Session:         activeSession,
				Update:          record.Update,
				Slot:            slot,
				Leader:          job.window.Leader,
				Parent:          parent,
				Previous:        previous,
				FinalizedAnchor: finalizedAnchor,
			}
			stageStarted := s.metricStageStarted()
			if err = s.opts.Pipeline.RestoreCandidate(job.ctx, restore, artifact); err != nil {
				s.observeMetricStage(chain, CollationStageRestoreCandidate, stageStarted)
				s.observeCandidateProduction(chain, CandidateKindRecovered, productionStarted, err)

				return fmt.Errorf("collator runtime: restore candidate state: %w", err)
			}
			s.observeMetricStage(chain, CollationStageRestoreCandidate, stageStarted)
			stageStarted = s.metricStageStarted()
			if err = waitUntil(job.ctx, broadcastTime(record, slot)); err != nil {
				s.observeMetricStage(chain, CollationStageWaitBroadcastSlot, stageStarted)
				s.observeCandidateProduction(chain, CandidateKindRecovered, productionStarted, err)

				return err
			}
			s.observeMetricStage(chain, CollationStageWaitBroadcastSlot, stageStarted)
			if s.opts.Observer != nil {
				s.opts.Observer.ObserveScheduleLateness(
					chain,
					ScheduleEventBroadcast,
					time.Since(broadcastTime(record, slot)),
				)
			}
			stageStarted = s.metricStageStarted()
			if err = s.opts.Emit(job.ctx, artifact); err != nil {
				s.observeMetricStage(chain, CollationStageDeliverCandidate, stageStarted)
				s.observeCandidateProduction(chain, CandidateKindRecovered, productionStarted, err)

				return fmt.Errorf("collator runtime: emit recovered candidate: %w", err)
			}
			s.observeMetricStage(chain, CollationStageDeliverCandidate, stageStarted)
			s.observeCandidateProduction(chain, CandidateKindRecovered, productionStarted, nil)
			advance(artifact)
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("collator runtime: load candidate: %w", err)
		}

		request := BuildRequest{
			Session:              activeSession,
			Update:               record.Update,
			Slot:                 slot,
			Leader:               job.window.Leader,
			Parent:               parent,
			Previous:             previous,
			ExternalWaitUntil:    externalWaitUntil(record, slot),
			ExternalProcessUntil: externalProcessUntil(record, slot),
			BuildSoftDeadline:    softBuildDeadline(record, slot),
		}
		// emitEmpty commits this slot as an empty candidate. It deliberately does
		// not touch future: the soft-timeout path keeps its live build for the
		// next slot, and the commit always carries the current slot's request
		// rather than the one the surviving future was started from.
		emitEmpty := func(block ton.BlockIDExt) error {
			started := time.Now()
			stageStarted := s.metricStageStarted()
			artifact, emptyErr := s.signEmptyArtifact(record.Session, job.window, slot, parent, block)
			s.observeMetricStage(chain, CollationStageSignCandidate, stageStarted)
			if emptyErr != nil {
				s.observeCandidateProduction(chain, CandidateKindEmpty, started, emptyErr)

				return emptyErr
			}
			s.logCollatedCandidate(request, &artifact.Candidate, 0, 0, nil, time.Since(started))
			if emptyErr = s.persistAndEmit(job.ctx, managed, record, artifact, &CandidateCommit{
				Request:  request,
				Artifact: artifact,
			}, started, CandidateKindEmpty); emptyErr != nil {
				return emptyErr
			}
			advance(artifact)

			return nil
		}
		if future == nil {
			if err = waitUntil(job.ctx, buildStartTime(record, slot)); err != nil {
				return err
			}
			if s.opts.Observer != nil {
				s.opts.Observer.ObserveScheduleLateness(
					chain,
					ScheduleEventBuildStart,
					time.Since(buildStartTime(record, slot)),
				)
			}
			if parent.Exists {
				stageStarted := s.metricStageStarted()
				state, stateErr := s.opts.Pipeline.ResolveCandidateState(job.ctx, request)
				s.observeMetricStage(chain, CollationStageResolveCandidateState, stageStarted)
				if stateErr != nil {
					return fmt.Errorf("collator runtime: resolve candidate state for slot %d: %w", slot, stateErr)
				}
				if managed.shouldGenerateEmpty(record.Session.Shard.IsMasterchain(), state) {
					if err = emitEmpty(state.Block); err != nil {
						return err
					}
					continue
				}
			}
			future = s.startBuildFuture(job.ctx, request, hardBuildDeadline(record, slot))
		}

		result, softTimeout, waitErr := awaitBuildUntil(job.ctx, future, softBuildDeadline(record, slot))
		if waitErr != nil {
			return waitErr
		}
		if softTimeout {
			if !managed.allowEmptyOnGenerationFailure(record.Update.NoEmptyBlocksOnErrTimeout) {
				if s.opts.Observer != nil {
					// Distinct from an ordinary wait: this producer is not slow, it
					// is forbidden to emit the empty block that would end the wait
					// because the session has not finalized for
					// NoEmptyBlocksOnErrTimeout. Reported as the same action, the
					// two are indistinguishable — and in the field this one was the
					// reason both Go nodes stayed silent in their own windows.
					s.opts.Observer.ObserveCollationDeadline(
						chain,
						CollationDeadlineSoft,
						DeadlineActionWaitNoEmpty,
					)
				}
				result, waitErr = awaitBuild(job.ctx, future)
				if waitErr != nil {
					return waitErr
				}
				softTimeout = false
			}
		}
		if softTimeout {
			decision, decisionErr := s.opts.Pipeline.SoftTimeout(job.ctx, SoftTimeoutRequest{
				Active:  future.request,
				Current: request,
			})
			if decisionErr != nil {
				if s.opts.Observer != nil {
					s.opts.Observer.ObserveCollationDeadline(
						chain,
						CollationDeadlineSoft,
						DeadlineActionAbort,
					)
				}

				return fmt.Errorf("collator runtime: decide soft timeout for slot %d: %w", slot, decisionErr)
			}
			switch decision.Action {
			case SoftTimeoutWait:
				if s.opts.Observer != nil {
					s.opts.Observer.ObserveCollationDeadline(
						chain,
						CollationDeadlineSoft,
						DeadlineActionWait,
					)
				}
				result, waitErr = awaitBuild(job.ctx, future)
				if waitErr != nil {
					return waitErr
				}
			case SoftTimeoutEmitEmpty:
				if s.opts.Observer != nil {
					s.opts.Observer.ObserveCollationDeadline(
						chain,
						CollationDeadlineSoft,
						DeadlineActionEmitEmpty,
					)
				}
				if err = emitEmpty(decision.Block); err != nil {
					return err
				}
				continue
			default:
				if s.opts.Observer != nil {
					s.opts.Observer.ObserveCollationDeadline(
						chain,
						CollationDeadlineSoft,
						DeadlineActionAbort,
					)
				}

				return fmt.Errorf("%w: pipeline returned an invalid soft-timeout action", ErrCandidateConflict)
			}
		}
		finishedFuture := future
		future.stop()
		future = nil
		if result.err != nil {
			if errors.Is(result.err, context.DeadlineExceeded) && !time.Now().Before(finishedFuture.hardDeadline) {
				if s.opts.Observer != nil {
					s.opts.Observer.ObserveCollationDeadline(
						chain,
						CollationDeadlineHard,
						DeadlineActionAbort,
					)
				}
				return fmt.Errorf(
					"%w for slot %d: %v",
					errHardBuildDeadline,
					finishedFuture.request.Slot,
					result.err,
				)
			}
			return fmt.Errorf("collator runtime: build slot %d: %w", slot, result.err)
		}
		stageStarted := s.metricStageStarted()
		artifact, err := s.signArtifact(record.Session, job.window, slot, parent, result.candidate)
		s.observeMetricStage(chain, CollationStageSignCandidate, stageStarted)
		if err != nil {
			s.observeCandidateProduction(chain, CandidateKindBlock, finishedFuture.started, err)

			return err
		}
		s.logCollatedCandidate(
			request,
			&artifact.Candidate,
			len(result.candidate.BlockBOC),
			len(result.candidate.CollatedData),
			&result.candidate.Stats,
			result.elapsed,
		)
		if err = s.persistAndEmit(job.ctx, managed, record, artifact, &CandidateCommit{
			// A future can survive an emitted empty slot. Its built state still
			// extends the same block, but the signed candidate belongs to the
			// current slot and parent.
			Request:  request,
			Built:    result.candidate,
			Artifact: artifact,
		}, finishedFuture.started, CandidateKindBlock); err != nil {
			return err
		}
		advance(artifact)
	}

	return nil
}

func (s *Service) startBuildFuture(
	ctx context.Context,
	request BuildRequest,
	hardDeadline time.Time,
) *candidateBuildFuture {
	buildCtx, cancel := context.WithDeadline(ctx, hardDeadline)
	future := &candidateBuildFuture{
		request:      request,
		hardDeadline: hardDeadline,
		result:       make(chan candidateBuildResult, 1),
		done:         make(chan struct{}),
		cancel:       cancel,
	}
	// C++ starts Collator::perf_timer_ when the actor is constructed, before
	// its first scheduler turn. Start here for the same wall-clock boundary:
	// acquisition, collation and candidate serialization are included, while
	// empty-block policy, signing, persistence and broadcast stay outside.
	started := time.Now()
	future.started = started
	chain := metricChain(request.Session.Shard.IsMasterchain())
	if s.opts.Observer != nil {
		s.opts.Observer.AddCollationBuildInflight(chain, 1)
	}
	go func() {
		defer close(future.done)
		candidate, err := s.opts.Pipeline.BuildCandidate(buildCtx, request)
		elapsed := time.Since(started)
		if s.opts.Observer != nil {
			s.opts.Observer.AddCollationBuildInflight(chain, -1)
			s.opts.Observer.ObserveCollationBuild(CollationBuildObservation{
				Chain:    chain,
				Result:   collationResult(err),
				Duration: elapsed,
			})
		}
		// A build that stopped because a local input has not arrived is not a
		// failure and is deliberately silent here. The window producer retries it
		// on a 0/5/10/20/20 ms schedule, so one waiting window used to emit
		// around twenty-one of these lines in well under a second — measured on
		// the test network as 27,027 of 27,103 "block collation failed" lines in
		// five hours, which made the collator read as failing 39% of its builds.
		// The wait is reported once per window by logProductionInputWait, and
		// counted honestly by gton_collator_retries_total{reason="not_ready"}.
		if err != nil && !errors.Is(err, ErrAcquisitionNotReady) {
			if event := s.log.Warn(); event != nil {
				event.
					Hex("session_id", request.Session.ID[:]).
					Int32("workchain", request.Session.Shard.Workchain).
					Int64("shard", request.Session.Shard.Shard).
					Uint32("slot", request.Slot).
					Uint32("leader", request.Leader).
					Uint32("window_start", request.Update.CurrentWindowStart).
					Uint32("window_end", request.Update.CurrentWindowStart+request.Session.SlotsPerLeaderWindow).
					Dur("elapsed", elapsed).
					Err(err).
					Msg("block collation failed")
			}
		}
		future.result <- candidateBuildResult{candidate: candidate, elapsed: elapsed, err: err}
	}()
	return future
}

func (s *Service) logCollatedCandidate(
	request BuildRequest,
	candidate *simplex.Candidate,
	blockBytes int,
	collatedBytes int,
	stats *Stats,
	elapsed time.Duration,
) {
	event := s.log.Info()
	if event == nil && s.opts.Observer == nil {
		return
	}

	var values Stats
	if stats != nil {
		values = *stats
	}
	if s.opts.Observer != nil {
		kind := CandidateKindBlock
		if candidate.Empty {
			kind = CandidateKindEmpty
		}
		s.opts.Observer.ObserveCollationCandidate(CandidateObservation{
			Chain:         metricChain(request.Session.Shard.IsMasterchain()),
			Origin:        CandidateOriginCollation,
			Kind:          kind,
			BlockBytes:    blockBytes,
			CollatedBytes: collatedBytes,
			Shape:         values.CollationShape(),
			Stats:         values,
		})
	}

	if event == nil {
		return
	}

	event.
		Hex("session_id", request.Session.ID[:]).
		Int32("workchain", request.Session.Shard.Workchain).
		Int64("shard", request.Session.Shard.Shard).
		Uint32("slot", request.Slot).
		Uint32("leader", request.Leader).
		Uint32("window_start", request.Update.CurrentWindowStart).
		Uint32("window_end", request.Update.CurrentWindowStart+request.Session.SlotsPerLeaderWindow).
		Bool("is_empty", candidate.Empty).
		Uint32("block_seqno", candidate.Block.SeqNo).
		Hex("candidate_hash", candidate.ID.Hash[:]).
		Hex("block_root_hash", candidate.Block.RootHash).
		Hex("block_file_hash", candidate.Block.FileHash).
		Int("block_bytes", blockBytes).
		Int("collated_bytes", collatedBytes).
		Uint32("transactions", values.Transactions).
		Uint32("external_messages", values.ExternalIncluded).
		Uint32("internal_messages", values.InternalsImported).
		Uint64("gas_used", values.GasUsed).
		Uint64("out_queue_size", values.OutQueueSize).
		Uint8("load_class", uint8(values.Load)).
		Dur("elapsed", elapsed).
		Msg("block collated")
}

func awaitBuildUntil(
	ctx context.Context,
	future *candidateBuildFuture,
	softDeadline time.Time,
) (candidateBuildResult, bool, error) {
	select {
	case result := <-future.result:
		return result, false, nil
	default:
	}
	delay := time.Until(softDeadline)
	if delay <= 0 {
		return candidateBuildResult{}, true, nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return candidateBuildResult{}, false, ctx.Err()
	case result := <-future.result:
		return result, false, nil
	case <-timer.C:
		return candidateBuildResult{}, true, nil
	}
}

func awaitBuild(ctx context.Context, future *candidateBuildFuture) (candidateBuildResult, error) {
	select {
	case <-ctx.Done():
		return candidateBuildResult{}, ctx.Err()
	case result := <-future.result:
		return result, nil
	}
}

func (s *Service) persistAndEmit(
	ctx context.Context,
	managed *managedCollatorSession,
	record SessionRecord,
	artifact CandidateArtifact,
	commit *CandidateCommit,
	productionStarted time.Time,
	kind CandidateKind,
) (resultErr error) {
	chain := metricChain(record.Session.Shard.IsMasterchain())
	if s.opts.Observer != nil {
		defer func() {
			s.observeCandidateProduction(chain, kind, productionStarted, resultErr)
		}()
	}

	candidateRecord := recordFromArtifact(artifact)
	// Remember before the marker is durable, never after. A marker that lands
	// while its payload was never remembered is unresumable, and the write is
	// abandonable, so its outcome can be unknown to this goroutine.
	managed.rememberEmitted(artifact.WindowID, artifact)
	// Submitted here, awaited below. The marker's whole cost is one synced
	// commit — measured at 4.88 ms per candidate against 4.87 ms for a bare
	// synced Pebble commit on the same device, so the encoding, the window scan
	// and the signature checks together are under 1% of it — and waiting for it
	// here served nothing: what the marker has to be earlier than is the
	// broadcast, not the state commit that follows building the block. The
	// reference does not wait for its equivalent at all; the collator producer
	// detaches its store (simplex/collator-producer.cpp:81) and consensus.cpp
	// starts one at :228 and collects it at :252, after the parent wait, the
	// state resolve, the block interval and a whole validation.
	//
	// The property is unchanged, and it is the one that matters: no candidate is
	// emitted for a slot whose marker is not durable.
	markerWrite := startStorageWrite(func(done func(error)) {
		s.opts.Storage.SaveCandidate(candidateRecord, done)
	})
	var stageStarted time.Time
	if commit != nil {
		stageStarted = s.metricStageStarted()
		if err := s.opts.Pipeline.CommitCandidate(ctx, *commit); err != nil {
			s.observeMetricStage(chain, CollationStageCommitCandidateState, stageStarted)

			return fmt.Errorf("collator runtime: commit candidate state: %w", err)
		}
		s.observeMetricStage(chain, CollationStageCommitCandidateState, stageStarted)
	}
	stageStarted = s.metricStageStarted()
	if err := waitUntil(ctx, broadcastTime(record, artifact.Candidate.ID.Slot)); err != nil {
		s.observeMetricStage(chain, CollationStageWaitBroadcastSlot, stageStarted)

		return err
	}
	s.observeMetricStage(chain, CollationStageWaitBroadcastSlot, stageStarted)
	// The marker's commit has to have landed before this candidate reaches
	// anyone, and this is the last instant where that is still true. The stage
	// keeps its name and now reports what the producer actually spends on the
	// marker rather than the whole synced commit: everything above overlapped
	// it, so on a healthy device this is the residue.
	stageStarted = s.metricStageStarted()
	if err := markerWrite.wait(ctx); err != nil {
		s.observeMetricStage(chain, CollationStagePersistCandidate, stageStarted)

		return fmt.Errorf("collator runtime: save candidate: %w", err)
	}
	s.observeMetricStage(chain, CollationStagePersistCandidate, stageStarted)
	if s.opts.Observer != nil {
		s.opts.Observer.ObserveScheduleLateness(
			chain,
			ScheduleEventBroadcast,
			time.Since(broadcastTime(record, artifact.Candidate.ID.Slot)),
		)
	}
	stageStarted = s.metricStageStarted()
	if err := s.opts.Emit(ctx, artifact); err != nil {
		s.observeMetricStage(chain, CollationStageDeliverCandidate, stageStarted)

		return fmt.Errorf("collator runtime: emit candidate: %w", err)
	}
	s.observeMetricStage(chain, CollationStageDeliverCandidate, stageStarted)
	return nil
}

func (s *Service) candidateDelegation(window productionWindow) *simplex.Delegation {
	if window.Authority != CandidateAuthorityDelegated {
		return nil
	}

	return &simplex.Delegation{
		CollatorKey: append(ed25519.PublicKey(nil), s.publicKey...),
		Signature:   append([]byte(nil), window.DelegationSignature...),
	}
}

func (s *Service) signWindowCandidate(
	session Session,
	window productionWindow,
	id simplex.CandidateID,
) ([]byte, error) {
	var signer simplex.Signer
	var publicKey ed25519.PublicKey
	switch window.Authority {
	case CandidateAuthoritySelf:
		signer = window.SelfSigner
		publicKey = session.Validators[window.Leader].PublicKey[:]
	case CandidateAuthorityDelegated:
		signer = boundCollatorSigner{keys: s.opts.Keys, keyID: s.opts.CollatorKeyID}
		publicKey = s.publicKey
	default:
		return nil, fmt.Errorf("%w: candidate authority is invalid", ErrCandidateConflict)
	}
	if signer == nil {
		return nil, fmt.Errorf("%w: candidate signer is absent", ErrCandidateConflict)
	}

	signature, err := simplex.SignCandidate(signer, session.ID, id)
	if err != nil {
		return nil, fmt.Errorf("collator runtime: sign candidate: %w", err)
	}
	if !simplex.VerifyCandidateSignature(publicKey, session.ID, id, signature) {
		return nil, fmt.Errorf("%w: signing key returned an invalid candidate signature", ErrCandidateConflict)
	}

	return signature, nil
}

func (s *Service) signArtifact(
	session Session,
	window productionWindow,
	slot uint32,
	parent simplex.ParentID,
	built *Candidate,
) (CandidateArtifact, error) {
	if built == nil {
		return CandidateArtifact{}, fmt.Errorf("%w: pipeline returned no candidate", ErrCandidateConflict)
	}
	if built.ID.Workchain != session.Shard.Workchain || built.ID.Shard != session.Shard.Shard {
		return CandidateArtifact{}, fmt.Errorf("%w: candidate belongs to another shard", ErrCandidateConflict)
	}
	if built.CreatedBy != session.Validators[window.Leader].PublicKey {
		return CandidateArtifact{}, fmt.Errorf("%w: candidate creator is not the delegated leader", ErrCandidateConflict)
	}
	if len(built.ID.RootHash) != sha256.Size || len(built.ID.FileHash) != sha256.Size {
		return CandidateArtifact{}, fmt.Errorf("%w: candidate block hashes have invalid length", ErrCandidateConflict)
	}
	// Only for a candidate whose digests this process did not compute. Our own
	// builder returns ID.FileHash and CollatedFileHash as the sha256 it took of
	// these exact buffers inside finish(), so the two hashes below would be a
	// megabyte of sha256 spent comparing a value with itself — the reference
	// producer takes the collator's hashes as given and re-derives neither
	// (window-producer.cpp:99-131). A Pipeline supplied from outside this
	// package is a different matter: there the hashes are a claim, and a claim
	// that is wrong is a signature over an ID no other validator can reproduce,
	// which burns the whole leader window.
	if !built.digested {
		fileHash := sha256.Sum256(built.BlockBOC)
		if !bytes.Equal(fileHash[:], built.ID.FileHash) {
			return CandidateArtifact{}, fmt.Errorf("%w: candidate file hash does not match block BOC", ErrCandidateConflict)
		}
		if sha256.Sum256(built.CollatedData) != built.CollatedFileHash {
			return CandidateArtifact{}, fmt.Errorf("%w: collated file hash does not match collated data", ErrCandidateConflict)
		}
	}

	candidate := simplex.Candidate{
		Parent:           parent,
		Leader:           window.Leader,
		Block:            cloneBlockID(built.ID),
		CollatedFileHash: built.CollatedFileHash,
		Delegation:       s.candidateDelegation(window),
	}
	candidate.ID = candidate.ComputeID(slot)
	signature, err := s.signWindowCandidate(session, window, candidate.ID)
	if err != nil {
		return CandidateArtifact{}, err
	}
	candidate.Signature = signature

	return CandidateArtifact{
		SessionID:    session.ID,
		WindowID:     window.ID,
		Candidate:    candidate,
		BlockBOC:     built.BlockBOC,
		CollatedData: built.CollatedData,
		prepared:     built.prepared,
		// Either the builder took both digests of these very buffers, or the
		// block above just re-derived them and compared. Both are the same
		// statement, and it is the one the leader's own validation of this
		// candidate would otherwise re-establish by hashing the collated data
		// again.
		digested: true,
	}, nil
}

func (s *Service) signEmptyArtifact(
	session Session,
	window productionWindow,
	slot uint32,
	parent simplex.ParentID,
	block ton.BlockIDExt,
) (CandidateArtifact, error) {
	if !parent.Exists {
		return CandidateArtifact{}, fmt.Errorf("%w: empty candidate has no consensus parent", ErrCandidateConflict)
	}
	if err := validateBlockID(block); err != nil {
		return CandidateArtifact{}, fmt.Errorf("%w: empty candidate block: %v", ErrCandidateConflict, err)
	}
	if block.Workchain != session.Shard.Workchain || block.Shard != session.Shard.Shard {
		return CandidateArtifact{}, fmt.Errorf("%w: empty candidate block belongs to another shard", ErrCandidateConflict)
	}
	candidate := simplex.Candidate{
		Parent:     parent,
		Leader:     window.Leader,
		Empty:      true,
		Block:      cloneBlockID(block),
		Delegation: s.candidateDelegation(window),
	}
	candidate.ID = candidate.ComputeID(slot)
	signature, err := s.signWindowCandidate(session, window, candidate.ID)
	if err != nil {
		return CandidateArtifact{}, err
	}
	candidate.Signature = signature
	return CandidateArtifact{SessionID: session.ID, WindowID: window.ID, Candidate: candidate}, nil
}

// artifactFromRecord rebinds a durable signature marker to the artifact this
// process still holds for that slot. The marker alone cannot produce an
// artifact — it carries no payload — so a producer that no longer remembers the
// slot it signed reports errWindowNotResumable and ends the window rather than
// collating a second candidate for it.
func (s *Service) artifactFromRecord(
	session Session,
	window productionWindow,
	record CandidateRecord,
	remembered CandidateArtifact,
	found bool,
) (CandidateArtifact, error) {
	if record.WindowID != window.ID || record.Authority != window.Authority ||
		record.ID.Slot < window.ID.StartSlot ||
		record.ID.Slot >= window.ID.StartSlot+session.SlotsPerLeaderWindow ||
		record.Leader != window.Leader {
		return CandidateArtifact{}, ErrCandidateConflict
	}
	var delegation *simplex.Delegation
	var signerKey ed25519.PublicKey
	switch window.Authority {
	case CandidateAuthoritySelf:
		if record.DelegationKey != ([ed25519.PublicKeySize]byte{}) || len(record.DelegationSignature) != 0 {
			return CandidateArtifact{}, ErrCandidateConflict
		}
		signerKey = session.Validators[window.Leader].PublicKey[:]
	case CandidateAuthorityDelegated:
		if record.DelegationKey != [ed25519.PublicKeySize]byte(s.publicKey) ||
			!bytes.Equal(record.DelegationSignature, window.DelegationSignature) {
			return CandidateArtifact{}, ErrCandidateConflict
		}
		delegation = &simplex.Delegation{
			CollatorKey: append(ed25519.PublicKey(nil), record.DelegationKey[:]...),
			Signature:   record.DelegationSignature,
		}
		signerKey = s.publicKey
	default:
		return CandidateArtifact{}, ErrCandidateConflict
	}
	if record.Block.Workchain != session.Shard.Workchain || record.Block.Shard != session.Shard.Shard ||
		validateBlockID(record.Block) != nil {
		return CandidateArtifact{}, ErrCandidateConflict
	}
	candidate := simplex.Candidate{
		ID:               record.ID,
		Parent:           record.Parent,
		Leader:           record.Leader,
		Empty:            record.Empty,
		Block:            record.Block,
		CollatedFileHash: record.CollatedFileHash,
		Signature:        record.Signature,
		Delegation:       delegation,
	}
	if candidate.ComputeID(candidate.ID.Slot) != candidate.ID ||
		!simplex.VerifyCandidateSignature(signerKey, session.ID, candidate.ID, candidate.Signature) {
		return CandidateArtifact{}, ErrCandidateConflict
	}
	if candidate.Empty && (!candidate.Parent.Exists || record.CollatedFileHash != ([32]byte{})) {
		return CandidateArtifact{}, ErrCandidateConflict
	}
	if !found {
		return CandidateArtifact{}, fmt.Errorf(
			"%w: slot %d was signed by an earlier producer whose candidate is gone",
			errWindowNotResumable,
			record.ID.Slot,
		)
	}
	// The marker is the authority. An artifact that does not answer to the
	// signed candidate ID is a different block, and emitting it under that
	// signature is exactly the equivocation the marker exists to prevent.
	if remembered.Candidate.ID != record.ID || remembered.SessionID != session.ID ||
		remembered.WindowID != window.ID {
		return CandidateArtifact{}, ErrCandidateConflict
	}
	if candidate.Empty {
		if len(remembered.BlockBOC) != 0 || len(remembered.CollatedData) != 0 {
			return CandidateArtifact{}, ErrCandidateConflict
		}
	} else {
		if sha256.Sum256(remembered.CollatedData) != record.CollatedFileHash {
			return CandidateArtifact{}, ErrCandidateConflict
		}
		fileHash := sha256.Sum256(remembered.BlockBOC)
		if !bytes.Equal(fileHash[:], record.Block.FileHash) {
			return CandidateArtifact{}, ErrCandidateConflict
		}
	}

	// The capsule rides along for the same reason the BOCs do: this artifact is
	// the one this process built, and the two hash checks just above are exactly
	// the binding a serializer would otherwise re-derive by parsing both BOCs
	// again. Keeping it is also what makes rememberEmitted's retention of it pay
	// for itself instead of holding a compressed payload nobody reads.
	return CandidateArtifact{
		SessionID:    session.ID,
		WindowID:     window.ID,
		Candidate:    candidate,
		BlockBOC:     remembered.BlockBOC,
		CollatedData: remembered.CollatedData,
		prepared:     remembered.prepared,
		// Established by the two hash checks above, which had to run here: they
		// bind the payload this process still holds in memory to the digests the
		// durable marker recorded for the candidate this window already signed.
		// An empty candidate takes neither of them and carries no payload to
		// have digested.
		digested: !candidate.Empty,
	}, nil
}

type boundCollatorSigner struct {
	keys  SigningKeys
	keyID [32]byte
}

func (s boundCollatorSigner) Sign(payload []byte) ([]byte, error) {
	return s.keys.Sign(s.keyID, payload)
}

// awaitSessionWrite synchronously admits a session record while the caller
// still owns its lifecycle lock, then waits abandonably for the durable
// callback. Once submit returns, a later retirement is necessarily behind this
// write in the store FIFO even if ctx expires before the callback arrives.
func awaitSessionWrite(ctx context.Context, submit func(func(error))) error {
	result := make(chan error, 1)
	submit(func(err error) { result <- err })

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
	}
	// Prefer a callback which completed at the cancellation boundary. Treating
	// that known result as unknown would create needless session recovery work.
	select {
	case err := <-result:
		return err
	default:
		return ctx.Err()
	}
}

// awaitStorageWrite submits one durable write and waits for its callback until
// ctx is done. The submission runs on its own goroutine on purpose: the shared
// store enqueues under a global lock behind a bounded queue, so a saturated
// writer blocks the submitting goroutine before any callback exists at all.
// Waiting abandonably on the callback while the enqueue can still pin the
// caller indefinitely would leave the whole point of the cancellable wait
// unmet — that caller is the producer, and it holds the production barrier.
//
// Abandoning a wait never cancels the write. The store commits an exact
// duplicate idempotently and rejects a conflicting one, so an abandoned wait
// leaves an unknown outcome: retry the whole step, never assume either result.
// In particular an abandoned candidate write must not be treated as persisted.
// Persist-before-emit is what stops a restarted producer from signing a second
// candidate for a slot it already broadcast.
//
// Ordering across two writes from one caller survives, because a completed wait
// means the callback has already fired. Only an abandoned wait can leave a
// straggler that lands later, and every write this package abandons is either
// idempotent on retry or belongs to a window that is being torn down.
func awaitStorageWrite(ctx context.Context, submit func(func(error))) error {
	return startStorageWrite(submit).wait(ctx)
}

// storageWriteFlight is one submitted durable write whose outcome has not been
// collected yet. It exists so a caller can put the write in the store's queue
// at the point in its sequence where it belongs and collect the result at the
// point where the write actually has to have landed — which for the candidate
// marker is immediately before the candidate is emitted, not before the state
// commit that follows building it.
type storageWriteFlight struct {
	result chan error
}

func startStorageWrite(submit func(func(error))) *storageWriteFlight {
	flight := &storageWriteFlight{result: make(chan error, 1)}
	go submit(func(err error) { flight.result <- err })

	return flight
}

func (f *storageWriteFlight) wait(ctx context.Context) error {
	select {
	case err := <-f.result:
		return err
	case <-ctx.Done():
	}
	// A write that landed while ctx was expiring is a known outcome, and the
	// select above picks between two ready cases at random. Reporting a
	// committed write as abandoned would send the caller down the retry path it
	// keeps for genuinely unknown results.
	select {
	case err := <-f.result:
		return err
	default:
		return ctx.Err()
	}
}

func waitUntil(ctx context.Context, at time.Time) error {
	delay := time.Until(at)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func buildStartTime(record SessionRecord, slot uint32) time.Time {
	start := slotStartTime(record, slot)
	if record.Session.Shard.IsMasterchain() {
		return start
	}
	return start.Add(-record.Update.TargetRate)
}

func broadcastTime(record SessionRecord, slot uint32) time.Time {
	return slotStartTime(record, slot)
}

func externalWaitUntil(record SessionRecord, slot uint32) time.Time {
	if record.Session.Shard.IsMasterchain() {
		return time.Time{}
	}
	return slotStartTime(record, slot)
}

func externalProcessUntil(record SessionRecord, slot uint32) time.Time {
	start := slotStartTime(record, slot)
	if record.Session.Shard.IsMasterchain() {
		return start.Add(record.Update.TargetRate * 3 / 4)
	}
	return start
}

func slotStartTime(record SessionRecord, slot uint32) time.Time {
	offset := time.Duration(slot-record.Update.CurrentWindowStart) * record.Update.TargetRate
	return record.Update.CurrentWindowStartAt.Add(offset)
}

func softBuildDeadline(record SessionRecord, slot uint32) time.Time {
	slotStart := record.Update.CurrentWindowStartAt.Add(
		time.Duration(slot-record.Update.CurrentWindowStart) * record.Update.TargetRate,
	)
	return slotStart.Add(record.Update.TargetRate)
}

func hardBuildDeadline(record SessionRecord, slot uint32) time.Time {
	slotStart := record.Update.CurrentWindowStartAt.Add(
		time.Duration(slot-record.Update.CurrentWindowStart) * record.Update.TargetRate,
	)
	return slotStart.Add(max(3*record.Update.TargetRate, 60*time.Second))
}
