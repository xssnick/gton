package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"

	corestorage "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// ConsensusObserverNetwork owns the process-wide delegated-collation endpoint
// and every session transport used by a standalone collator or observer.
// PrepareSession creates an inactive overlay exactly once and returns its
// session-scoped transport. Run is started later by the session runtime.
// Implementations must make UpdateSession, RetireSession, and Close
// idempotent and must join their receiver goroutines before returning from
// Close or from the SessionNetwork.Run call cancelled by retirement.
type ConsensusObserverNetwork interface {
	LocalADNLID() [32]byte
	Start(context.Context, collator.RemoteHandlers) error
	PrepareSession(context.Context, collator.OverlaySession) (SessionNetwork, error)
	UpdateSession(context.Context, collator.OverlaySession) error
	RetireSession(context.Context, [32]byte) error
	Close(context.Context) error
}

// ConsensusObserverOptions binds a standalone collator observer to the same
// node/state and validator database used by ordinary consensus runtimes.
type ConsensusObserverOptions struct {
	Network   ConsensusObserverNetwork
	Storage   ValidatorStorage
	Node      LocalSessionNode
	Groups    collator.LocalGroupSource
	Publisher BlockPublisher
	ShardTops *collator.ShardTopInbox
	Tracer    simplex.Tracer
	Logger    zerolog.Logger

	CloseTimeout time.Duration
	// CaptureDir is the directory under the node data dir where a candidate this
	// observer's session refuses with a TVM/semantic replay error is dumped for
	// offline replay. Empty disables capture.
	CaptureDir string
}

type consensusObserverState uint8

const (
	consensusObserverNew consensusObserverState = iota
	consensusObserverRunning
	consensusObserverClosing
	consensusObserverClosed
)

type observerSessionPhase uint8

const (
	observerSessionRecovered observerSessionPhase = iota
	observerSessionPreparing
	observerSessionPrepared
	observerSessionStarting
	observerSessionActive
	observerSessionFailed
	observerSessionRetiring
	observerSessionRetired
)

// ConsensusObserver owns the projected non-validator session lifecycles. A
// protocol-1 plain observer is block-sync-only; other projected roles run a
// non-voting Simplex Pool whose progress is derived from authenticated votes
// and certificates.
type ConsensusObserver struct {
	network      ConsensusObserverNetwork
	storage      ValidatorStorage
	node         LocalSessionNode
	groups       collator.LocalGroupSource
	publisher    BlockPublisher
	shardTops    *collator.ShardTopInbox
	tracer       simplex.Tracer
	log          zerolog.Logger
	closeTimeout time.Duration
	captureDir   string
	localADNLID  [32]byte

	mu           sync.RWMutex
	state        consensusObserverState
	events       collator.ConsensusObserverEvents
	lifetime     context.Context
	cancel       context.CancelFunc
	sessions     map[[32]byte]*observerSession
	recovered    [][32]byte
	recoveryDone bool
	workersWG    sync.WaitGroup
}

type observerSession struct {
	lifecycleMu sync.Mutex
	mu          sync.Mutex

	descriptor       collator.ConsensusObserverSession
	config           SessionConfig
	state            SessionState
	limits           CandidateLimits
	network          SessionNetwork
	runtime          observerSessionRuntime
	activation       *collator.SessionActivation
	phase            observerSessionPhase
	terminal         error
	runCancel        context.CancelFunc
	watchDone        chan struct{}
	pending          *collator.ConsensusProgress
	pendingFinalized []collator.ConsensusFinalization

	overlayRetired bool
	storageDeleted bool
	progressWG     sync.WaitGroup
}

type observerSessionRuntime interface {
	SessionRuntime
	runWithStartup(context.Context, SessionStart, chan<- error) error
	publishCandidate(context.Context, *CandidateArtifact) error
	fail(error)
}

type observerRuntimeInput struct {
	config SessionConfig
	state  SessionState
	limits CandidateLimits
}

var _ collator.ConsensusObserver = (*ConsensusObserver)(nil)

func NewConsensusObserver(options ConsensusObserverOptions) (*ConsensusObserver, error) {
	if options.Network == nil {
		return nil, errors.New("validator consensus observer: network is required")
	}
	if options.Storage == nil {
		return nil, errors.New("validator consensus observer: storage is required")
	}
	if options.Node == nil {
		return nil, errors.New("validator consensus observer: node is required")
	}
	if options.Groups == nil {
		return nil, errors.New("validator consensus observer: validator-group source is required")
	}
	if options.CloseTimeout < 0 {
		return nil, errors.New("validator consensus observer: close timeout must not be negative")
	}
	localADNLID := options.Network.LocalADNLID()
	if localADNLID == ([32]byte{}) {
		return nil, errors.New("validator consensus observer: local adnl id is zero")
	}

	return &ConsensusObserver{
		network:      options.Network,
		storage:      options.Storage,
		node:         options.Node,
		groups:       options.Groups,
		publisher:    options.Publisher,
		shardTops:    options.ShardTops,
		tracer:       options.Tracer,
		log:          options.Logger,
		closeTimeout: options.CloseTimeout,
		captureDir:   options.CaptureDir,
		localADNLID:  localADNLID,
		sessions:     make(map[[32]byte]*observerSession),
	}, nil
}

func (o *ConsensusObserver) LocalADNLID() [32]byte {
	return o.localADNLID
}

func (o *ConsensusObserver) Start(
	ctx context.Context,
	handlers collator.RemoteHandlers,
	events collator.ConsensusObserverEvents,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if handlers.Probe == nil || handlers.Commit == nil {
		return errors.New("validator consensus observer: remote handlers are incomplete")
	}
	if events.Progressed == nil || events.Finalized == nil {
		return errors.New("validator consensus observer: event handlers are incomplete")
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	switch o.state {
	case consensusObserverRunning:
		return nil
	case consensusObserverClosing, consensusObserverClosed:
		return collator.ErrClosed
	}

	lifetime, cancel := context.WithCancel(ctx)
	o.events = events
	o.lifetime = lifetime
	o.cancel = cancel
	if err := o.network.Start(lifetime, handlers); err != nil {
		cancel()
		closeErr := o.network.Close(ctx)
		if closeErr == nil {
			o.state = consensusObserverClosed
			o.lifetime = nil
			o.cancel = nil
		} else {
			// The lifecycle owner retries Close with a fresh context after a
			// failed Start. Preserve the cancelled lifetime and network handle
			// until that cleanup succeeds.
			o.state = consensusObserverClosing
		}

		return errors.Join(fmt.Errorf("validator consensus observer: start network: %w", err), closeErr)
	}
	o.state = consensusObserverRunning

	return nil
}

// RecoverSessions installs lightweight placeholders for durable observer
// namespaces owned by this local ADNL identity. No overlay or runtime is
// opened here; Controller decides whether each namespace is desired or stale.
func (o *ConsensusObserver) RecoverSessions(ctx context.Context) ([][32]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != consensusObserverRunning {
		return nil, collator.ErrClosed
	}
	if o.recoveryDone {
		return slices.Clone(o.recovered), nil
	}
	if len(o.sessions) != 0 {
		return nil, errors.New("validator consensus observer: recovery must precede session preparation")
	}

	// Recovery is the first controller startup step after network readiness.
	// Keep the observer lock across this single descriptor-only read so Close
	// and PrepareSession cannot observe a partially installed inventory.
	stored, err := o.storage.Sessions(ctx)
	if err != nil {
		return nil, fmt.Errorf("validator consensus observer: enumerate storage sessions: %w", err)
	}

	recovered := make(map[[32]byte]SessionStorageID)
	for _, id := range stored {
		if err = id.Validate(); err != nil {
			return nil, fmt.Errorf("validator consensus observer: invalid stored session: %w", err)
		}
		if id.IsValidator || id.LocalADNLID != o.localADNLID {
			continue
		}
		if _, duplicate := recovered[id.SessionID]; duplicate {
			return nil, fmt.Errorf(
				"validator consensus observer: duplicate stored observer session %x",
				id.SessionID,
			)
		}
		recovered[id.SessionID] = id
	}

	ids := make([][32]byte, 0, len(recovered))
	for id, storageID := range recovered {
		o.sessions[id] = &observerSession{
			config: SessionConfig{
				SessionID: id,
				StorageID: storageID,
			},
			phase: observerSessionRecovered,
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	o.recovered = slices.Clone(ids)
	o.recoveryDone = true

	return ids, nil
}

func (o *ConsensusObserver) PrepareSession(
	ctx context.Context,
	descriptor collator.ConsensusObserverSession,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	input, err := o.runtimeInput(descriptor)
	if err != nil {
		return err
	}

	session, created, err := o.lockSession(descriptor.Overlay.Session.ID)
	if err != nil {
		return err
	}
	defer session.mu.Unlock()
	recovered := !created && session.phase == observerSessionRecovered
	if recovered && session.config.StorageID != input.config.StorageID {
		return collator.ErrSessionConflict
	}
	if !created && !recovered {
		switch session.phase {
		case observerSessionRetiring, observerSessionRetired:
			return collator.ErrSessionRetired
		}
		if session.descriptor.Equal(descriptor) {
			return nil
		}

		return collator.ErrSessionConflict
	}

	session.descriptor = descriptor
	session.config = input.config
	session.state = cloneSessionState(input.state)
	session.limits = input.limits
	session.phase = observerSessionPreparing
	sessionNetwork, err := o.network.PrepareSession(ctx, descriptor.Overlay)
	if err != nil {
		o.resetFailedPreparation(descriptor.Overlay.Session.ID, session, recovered)

		return fmt.Errorf("validator consensus observer: prepare network session: %w", err)
	}
	if sessionNetwork == nil {
		cleanupErr := o.network.RetireSession(context.WithoutCancel(ctx), descriptor.Overlay.Session.ID)
		o.resetFailedPreparation(descriptor.Overlay.Session.ID, session, recovered)

		return errors.Join(
			errors.New("validator consensus observer: network returned no session transport"),
			cleanupErr,
		)
	}
	session.network = sessionNetwork
	runtime, err := o.prepareRuntime(ctx, session)
	if err != nil {
		cleanupErr := o.network.RetireSession(context.WithoutCancel(ctx), descriptor.Overlay.Session.ID)
		o.resetFailedPreparation(descriptor.Overlay.Session.ID, session, recovered)

		return errors.Join(err, cleanupErr)
	}
	session.runtime = runtime
	session.phase = observerSessionPrepared

	return nil
}

// resetFailedPreparation preserves a cold-start namespace so an exact
// reconciliation retry can reopen it. Brand-new failed sessions have no
// lifecycle owner and are removed from the live inventory.
func (o *ConsensusObserver) resetFailedPreparation(
	id [32]byte,
	session *observerSession,
	recovered bool,
) {
	if !recovered {
		o.removeSession(id, session)
		session.phase = observerSessionRetired
		return
	}

	storageID := session.config.StorageID
	session.descriptor = collator.ConsensusObserverSession{}
	session.config = SessionConfig{SessionID: id, StorageID: storageID}
	session.state = SessionState{}
	session.limits = CandidateLimits{}
	session.network = nil
	session.runtime = nil
	session.phase = observerSessionRecovered
}

func (o *ConsensusObserver) ActivateSession(
	ctx context.Context,
	activation collator.SessionActivation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session, err := o.session(activation.SessionID)
	if err != nil {
		return err
	}

	session.lifecycleMu.Lock()
	defer session.lifecycleMu.Unlock()
	o.mu.RLock()
	state := o.state
	o.mu.RUnlock()
	if state != consensusObserverRunning {
		return collator.ErrClosed
	}

	session.mu.Lock()
	if session.activation != nil && !session.activation.Equal(activation) {
		session.mu.Unlock()
		return collator.ErrSessionConflict
	}
	switch session.phase {
	case observerSessionActive:
		session.mu.Unlock()
		return nil
	case observerSessionFailed:
		terminal := session.terminal
		session.mu.Unlock()
		return terminal
	case observerSessionRetiring, observerSessionRetired:
		session.mu.Unlock()
		return collator.ErrSessionRetired
	case observerSessionPreparing:
		session.mu.Unlock()
		return collator.ErrSessionUnavailable
	case observerSessionRecovered:
		session.mu.Unlock()
		return collator.ErrSessionUnavailable
	}
	if session.activation == nil {
		value := cloneObserverActivation(activation)
		session.activation = &value
	}
	session.phase = observerSessionStarting
	runtime := session.runtime
	session.mu.Unlock()

	if runtime == nil {
		var prepareErr error
		runtime, prepareErr = o.prepareRuntime(ctx, session)
		if prepareErr != nil {
			session.mu.Lock()
			session.phase = observerSessionPrepared
			session.mu.Unlock()

			return prepareErr
		}
		session.mu.Lock()
		session.runtime = runtime
		session.mu.Unlock()
	}

	lifetime, err := o.runningContext()
	if err != nil {
		session.mu.Lock()
		session.phase = observerSessionPrepared
		session.mu.Unlock()

		return err
	}
	runCtx, cancel := context.WithCancel(lifetime)
	startup := make(chan error, 1)
	returned := make(chan error, 1)
	session.mu.Lock()
	session.runCancel = cancel
	start := observerSessionStart(*session.activation)
	session.mu.Unlock()
	go func() {
		returned <- runtime.runWithStartup(runCtx, start, startup)
	}()

	var startupErr error
	wasCancelled := false
	select {
	case startupErr = <-startup:
	case <-ctx.Done():
		wasCancelled = true
		cancel()
		startupErr = <-startup
	}
	if startupErr == nil && runCtx.Err() != nil {
		startupErr = runCtx.Err()
	}
	if startupErr != nil || wasCancelled {
		cancel()
		runErr := <-returned
		closeErr := runtime.Close()
		session.mu.Lock()
		session.runCancel = nil
		session.pending = nil
		session.pendingFinalized = nil
		if startupErr != nil && closeErr == nil {
			session.runtime = nil
			session.phase = observerSessionPrepared
		} else {
			session.phase = observerSessionFailed
			session.terminal = errors.Join(startupErr, runErr, closeErr)
			if wasCancelled {
				session.terminal = errors.Join(ctx.Err(), session.terminal)
			}
		}
		session.mu.Unlock()
		if wasCancelled {
			return errors.Join(ctx.Err(), startupErr, runErr, closeErr)
		}

		return errors.Join(startupErr, runErr, closeErr)
	}

	session.mu.Lock()
	session.phase = observerSessionActive
	session.watchDone = make(chan struct{})
	o.workersWG.Add(1)
	go o.watchRuntime(session, runtime, cancel, returned, session.watchDone)
	if session.pending != nil {
		pending := *session.pending
		session.pending = nil
		session.progressWG.Add(1)
		o.workersWG.Add(1)
		go o.flushProgress(runCtx, session, runtime, pending)
	}
	if len(session.pendingFinalized) > 0 {
		pending := session.pendingFinalized
		session.pendingFinalized = nil
		session.progressWG.Add(1)
		o.workersWG.Add(1)
		go o.flushFinalizations(runCtx, session, runtime, pending)
	}
	session.mu.Unlock()

	return nil
}

func (o *ConsensusObserver) UpdateSession(
	ctx context.Context,
	descriptor collator.ConsensusObserverSession,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	input, err := o.runtimeInput(descriptor)
	if err != nil {
		return err
	}
	session, err := o.session(descriptor.Overlay.Session.ID)
	if err != nil {
		return err
	}

	session.lifecycleMu.Lock()
	defer session.lifecycleMu.Unlock()

	session.mu.Lock()
	switch session.phase {
	case observerSessionFailed:
		terminal := session.terminal
		session.mu.Unlock()

		return terminal
	case observerSessionRetiring, observerSessionRetired:
		session.mu.Unlock()

		return collator.ErrSessionRetired
	case observerSessionPreparing:
		session.mu.Unlock()

		return collator.ErrSessionUnavailable
	case observerSessionRecovered:
		session.mu.Unlock()

		return collator.ErrSessionUnavailable
	}
	if !session.descriptor.Overlay.Session.Equal(descriptor.Overlay.Session) ||
		session.config.Protocol != input.config.Protocol || session.limits != input.limits {
		session.mu.Unlock()

		return collator.ErrSessionConflict
	}
	if session.descriptor.Equal(descriptor) {
		session.mu.Unlock()

		return nil
	}
	updateNetwork := !session.descriptor.Overlay.Equal(descriptor.Overlay)
	runtime := session.runtime
	updateRuntime := runtime != nil &&
		(!sessionStateEqual(session.state, input.state) || session.descriptor.Params != descriptor.Params)
	if !updateNetwork && !updateRuntime {
		session.descriptor = descriptor
		session.state = cloneSessionState(input.state)
		session.config = input.config
		session.mu.Unlock()

		return nil
	}

	// lifecycleMu excludes competing session lifecycle mutations while the
	// external calls run. session.mu must remain free: Runtime.Update serializes
	// its commit with progress observation, whose callback re-enters this session.
	// Holding it here would invert session.mu and runtime.lifecycleMu; holding it
	// over network I/O would stall the same progress for no additional invariant.
	session.mu.Unlock()
	if updateNetwork {
		if err = o.network.UpdateSession(ctx, descriptor.Overlay); err != nil {
			return fmt.Errorf("validator consensus observer: update network session: %w", err)
		}
	}
	if updateRuntime {
		if err = runtime.Update(ctx, input.state); err != nil {
			return fmt.Errorf("validator consensus observer: update runtime: %w", err)
		}
	}

	session.mu.Lock()
	if session.phase == observerSessionFailed {
		terminal := session.terminal
		session.mu.Unlock()

		return terminal
	}
	if session.runtime != runtime {
		session.mu.Unlock()

		return collator.ErrSessionUnavailable
	}
	session.descriptor = descriptor
	session.state = cloneSessionState(input.state)
	session.config = input.config
	session.mu.Unlock()

	return nil
}

func (o *ConsensusObserver) BroadcastCandidate(
	ctx context.Context,
	artifact collator.CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if artifact.SessionID != artifact.WindowID.SessionID {
		return collator.ErrCandidateConflict
	}
	session, err := o.session(artifact.SessionID)
	if err != nil {
		return err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if session.phase == observerSessionFailed {
		return session.terminal
	}
	if session.phase != observerSessionActive || session.runtime == nil {
		return collator.ErrSessionUnavailable
	}
	windowSize := session.config.Protocol.SlotsPerLeaderWindow
	windowEnd := uint64(artifact.WindowID.StartSlot) + uint64(windowSize)
	if windowSize == 0 || artifact.WindowID.StartSlot%windowSize != 0 ||
		uint64(artifact.Candidate.ID.Slot) < uint64(artifact.WindowID.StartSlot) ||
		uint64(artifact.Candidate.ID.Slot) >= windowEnd {
		return collator.ErrCandidateConflict
	}

	// A delegated candidate is built by this very process — the collator↔validator
	// link carries only pleaseCollatePrepare/pleaseCollate, never the candidate —
	// so its payload was already compressed from the roots it was built from,
	// exactly as on the self route in LocalSessionBackend.routeCandidate. Dropping
	// the capsule here made the standalone collator compress every payload twice
	// per slot and throw the first one away, then reach the second by parsing both
	// BOCs back into cells and re-serializing them to prove they were canonical.
	generationTimeMS, generationTimeKnown := artifact.GenerationTimeMS()

	return session.runtime.publishCandidate(ctx, &CandidateArtifact{
		Candidate:           artifact.Candidate,
		BlockBOC:            artifact.BlockBOC,
		CollatedData:        artifact.CollatedData,
		prepared:            artifact.Prepared(),
		digested:            artifact.Digested(),
		generationTimeMS:    generationTimeMS,
		generationTimeKnown: generationTimeKnown,
	})
}

func (o *ConsensusObserver) RetireSession(ctx context.Context, id [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	session, err := o.session(id)
	if err != nil {
		return err
	}

	session.lifecycleMu.Lock()
	defer session.lifecycleMu.Unlock()
	o.mu.RLock()
	state := o.state
	o.mu.RUnlock()
	if state != consensusObserverRunning {
		return collator.ErrClosed
	}

	session.mu.Lock()
	if session.phase == observerSessionRetired {
		watchDone := session.watchDone
		session.mu.Unlock()
		if err = waitObserverSession(ctx, watchDone, &session.progressWG); err != nil {
			return err
		}
		o.removeSession(id, session)

		return nil
	}
	session.phase = observerSessionRetiring
	session.pending = nil
	session.pendingFinalized = nil
	cancel := session.runCancel
	session.runCancel = nil
	runtime := session.runtime
	storageID := session.config.StorageID
	session.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var retireErr error
	if runtime != nil {
		if closeErr := runtime.Close(); closeErr != nil {
			retireErr = errors.Join(retireErr, fmt.Errorf("close runtime: %w", closeErr))
		} else {
			session.mu.Lock()
			if session.runtime == runtime {
				session.runtime = nil
			}
			session.mu.Unlock()
		}
	}

	session.mu.Lock()
	runtimeClosed := session.runtime == nil
	overlayRetired := session.overlayRetired
	session.mu.Unlock()
	if runtimeClosed && !overlayRetired {
		if networkErr := o.network.RetireSession(ctx, id); networkErr != nil {
			retireErr = errors.Join(retireErr, fmt.Errorf("retire network session: %w", networkErr))
		} else {
			session.mu.Lock()
			session.overlayRetired = true
			session.mu.Unlock()
		}
	}

	session.mu.Lock()
	deleteStorage := !session.storageDeleted && session.runtime == nil && session.overlayRetired
	session.mu.Unlock()
	if deleteStorage {
		deleteErr := o.storage.DeleteSession(ctx, storageID)
		if deleteErr != nil && !errors.Is(deleteErr, corestorage.ErrNotFound) {
			retireErr = errors.Join(retireErr, fmt.Errorf("delete observer storage: %w", deleteErr))
		} else {
			session.mu.Lock()
			session.storageDeleted = true
			session.mu.Unlock()
		}
	}

	session.mu.Lock()
	watchDone := session.watchDone
	if retireErr == nil && session.runtime == nil && session.overlayRetired && session.storageDeleted {
		session.phase = observerSessionRetired
	}
	retired := session.phase == observerSessionRetired
	session.mu.Unlock()

	if waitErr := waitObserverSession(ctx, watchDone, &session.progressWG); waitErr != nil {
		return errors.Join(retireErr, waitErr)
	}
	if retired {
		o.removeSession(id, session)
	}
	if retireErr != nil {
		return fmt.Errorf("validator consensus observer: retire session %x: %w", id, retireErr)
	}

	return nil
}

func (o *ConsensusObserver) Close(ctx context.Context) error {
	o.mu.Lock()
	switch o.state {
	case consensusObserverClosed:
		o.mu.Unlock()
		return nil
	case consensusObserverNew:
		o.state = consensusObserverClosed
		o.mu.Unlock()
		return nil
	}
	o.state = consensusObserverClosing
	if o.cancel != nil {
		o.cancel()
	}
	sessions := make([]*observerSession, 0, len(o.sessions))
	for _, session := range o.sessions {
		sessions = append(sessions, session)
	}
	o.mu.Unlock()

	var closeErr error
	for _, session := range sessions {
		session.lifecycleMu.Lock()
		session.mu.Lock()
		if session.phase != observerSessionRetired {
			session.phase = observerSessionRetiring
		}
		session.pending = nil
		session.pendingFinalized = nil
		cancel := session.runCancel
		session.runCancel = nil
		runtime := session.runtime
		session.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if runtime != nil {
			if runtimeErr := runtime.Close(); runtimeErr != nil {
				closeErr = errors.Join(closeErr, runtimeErr)
			} else {
				session.mu.Lock()
				if session.runtime == runtime {
					session.runtime = nil
				}
				session.mu.Unlock()
			}
		}
		session.lifecycleMu.Unlock()
	}
	// The closing state prevents new session lookups. Taking every lifecycleMu
	// above also drains activations that already held a session pointer, so no
	// worker can be added once this wait begins.
	if workersErr := waitObserverWorkers(ctx, &o.workersWG); workersErr != nil {
		return fmt.Errorf(
			"validator consensus observer: close: %w",
			errors.Join(closeErr, workersErr),
		)
	}
	if networkErr := o.network.Close(ctx); networkErr != nil {
		closeErr = errors.Join(closeErr, networkErr)
	}
	if closeErr != nil {
		return fmt.Errorf("validator consensus observer: close: %w", closeErr)
	}

	o.mu.Lock()
	o.state = consensusObserverClosed
	o.lifetime = nil
	o.cancel = nil
	clear(o.sessions)
	o.mu.Unlock()

	return nil
}

func waitObserverSession(
	ctx context.Context,
	watchDone <-chan struct{},
	progress *sync.WaitGroup,
) error {
	if watchDone != nil {
		select {
		case <-watchDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan struct{})
	go func() {
		progress.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitObserverWorkers(ctx context.Context, workers *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *ConsensusObserver) lockSession(id [32]byte) (*observerSession, bool, error) {
	o.mu.Lock()
	if o.state != consensusObserverRunning {
		state := o.state
		o.mu.Unlock()
		if state == consensusObserverNew {
			return nil, false, collator.ErrNotStarted
		}

		return nil, false, collator.ErrClosed
	}
	if session := o.sessions[id]; session != nil {
		o.mu.Unlock()
		session.mu.Lock()
		return session, false, nil
	}

	session := &observerSession{phase: observerSessionPreparing}
	session.mu.Lock()
	o.sessions[id] = session
	o.mu.Unlock()

	return session, true, nil
}

func (o *ConsensusObserver) session(id [32]byte) (*observerSession, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.state != consensusObserverRunning {
		if o.state == consensusObserverNew {
			return nil, collator.ErrNotStarted
		}

		return nil, collator.ErrClosed
	}
	session := o.sessions[id]
	if session == nil {
		return nil, collator.ErrNotFound
	}

	return session, nil
}

func (o *ConsensusObserver) runningContext() (context.Context, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.state != consensusObserverRunning {
		if o.state == consensusObserverNew {
			return nil, collator.ErrNotStarted
		}

		return nil, collator.ErrClosed
	}

	return o.lifetime, nil
}

func (o *ConsensusObserver) removeSession(id [32]byte, session *observerSession) {
	o.mu.Lock()
	if o.sessions[id] == session {
		delete(o.sessions, id)
	}
	o.mu.Unlock()
}

func (o *ConsensusObserver) prepareRuntime(
	ctx context.Context,
	session *observerSession,
) (observerSessionRuntime, error) {
	config := session.config
	if config.Protocol.ProtocolVersion == 1 &&
		session.descriptor.Overlay.Role == collator.OverlayRoleObserver {
		runtime, err := PrepareBlockSyncObserverRuntime(ctx, config, session.network, session.limits)
		if err != nil {
			return nil, fmt.Errorf("validator consensus observer: prepare block-sync runtime: %w", err)
		}

		return runtime, nil
	}

	config.Journal = o.storage.Journal(config.StorageID, len(config.Validators))
	backendPreparation, err := PrepareLocalSessionBackend(ctx, LocalSessionBackendOptions{
		Config:  config,
		Initial: session.state,
		Node:    o.node,
		Groups:  o.groups,
		Storage: o.storage,
		ConsensusFinalized: func(ctx context.Context, block ton.BlockIDExt) error {
			return o.handleFinalization(ctx, session, block)
		},
		Publisher:    o.publisher,
		ShardTops:    o.shardTops,
		Logger:       o.log,
		CloseTimeout: o.closeTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("validator consensus observer: prepare local backend: %w", err)
	}
	backend := backendPreparation.Backend
	runtime, err := prepareSessionRuntime(ctx, config, backendPreparation.RuntimeState, RuntimeOptions{
		Storage:    o.storage,
		Network:    session.network,
		Backend:    backend,
		Limits:     session.limits,
		Tracer:     o.tracer,
		Logger:     &o.log,
		CaptureDir: o.captureDir,
		observeConsensusProgress: func(ctx context.Context, progress sessionConsensusProgress) error {
			return o.handleProgress(ctx, session, progress)
		},
	})
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("validator consensus observer: prepare runtime: %w", err),
			backend.Close(),
		)
	}

	return runtime, nil
}

func (o *ConsensusObserver) runtimeInput(
	descriptor collator.ConsensusObserverSession,
) (observerRuntimeInput, error) {
	overlay := descriptor.Overlay
	session := overlay.Session
	if session.ID == ([32]byte{}) || descriptor.Update.SessionID != session.ID {
		return observerRuntimeInput{}, errors.New("validator consensus observer: session id mismatch")
	}
	if session.ConsensusVersion != 2 || session.ProtocolVersion > simplex.MaxProtocolVersion {
		return observerRuntimeInput{}, fmt.Errorf(
			"validator consensus observer: unsupported simplex version %d protocol %d",
			session.ConsensusVersion,
			session.ProtocolVersion,
		)
	}
	if overlay.Role != collator.OverlayRoleObserver && overlay.Role != collator.OverlayRoleCollator {
		return observerRuntimeInput{}, errors.New("validator consensus observer: invalid overlay role")
	}
	if !session.Shard.IsMasterchain() {
		registered := slices.Contains(overlay.AllCollators, o.localADNLID)
		if registered != (overlay.Role == collator.OverlayRoleCollator) {
			return observerRuntimeInput{}, errors.New(
				"validator consensus observer: non-masterchain role differs from global collator registration",
			)
		}
	}
	if session.ProtocolVersion == 0 {
		return observerRuntimeInput{}, errors.New(
			"validator consensus observer: protocol version 0 has no non-validator identity",
		)
	}
	expectedBroadcastMode := collator.CandidateBroadcastPrivateOverlay
	if session.ProtocolVersion == 1 {
		expectedBroadcastMode = collator.CandidateBroadcastBlockSyncOverlay
	}
	if overlay.BroadcastMode != expectedBroadcastMode {
		return observerRuntimeInput{}, errors.New(
			"validator consensus observer: candidate broadcast mode differs from protocol",
		)
	}
	if overlay.ObserversInPrivateOverlay != (session.ProtocolVersion >= 2) {
		return observerRuntimeInput{}, errors.New(
			"validator consensus observer: private-overlay observer policy differs from protocol",
		)
	}
	if len(session.Validators) == 0 || len(overlay.AllOverlayNodes) == 0 {
		return observerRuntimeInput{}, errors.New("validator consensus observer: empty validator or overlay roster")
	}
	if err := descriptor.Params.Validate(); err != nil {
		return observerRuntimeInput{}, err
	}
	if descriptor.Update.TargetRate != descriptor.Params.TargetRate {
		return observerRuntimeInput{}, errors.New("validator consensus observer: update target rate differs from simplex params")
	}
	limits := CandidateLimits{
		MaxBlockBytes:        overlay.MaxBlockSize,
		MaxCollatedDataBytes: overlay.MaxCollatedDataSize,
	}
	if err := limits.validate(); err != nil {
		return observerRuntimeInput{}, err
	}
	shardPrefixLen, err := session.Shard.PrefixBits()
	if err != nil {
		return observerRuntimeInput{}, err
	}
	validators := make([]groups.Validator, len(session.Validators))
	for i := range session.Validators {
		validator := session.Validators[i]
		keyID, hashErr := groups.PublicKeyHash(validator.PublicKey)
		if hashErr != nil {
			return observerRuntimeInput{}, fmt.Errorf("validator consensus observer: hash validator %d: %w", i, hashErr)
		}
		validators[i] = groups.Validator{
			PublicKey:     validator.PublicKey,
			PublicKeyHash: keyID,
			ADNL:          validator.ADNLID,
			Weight:        validator.Weight,
		}
	}
	protocol := SessionProtocol{
		Version:              session.ConsensusVersion,
		Flags:                session.ConsensusFlags,
		ProtocolVersion:      session.ProtocolVersion,
		UseQUIC:              session.UseQUIC,
		SlotsPerLeaderWindow: session.SlotsPerLeaderWindow,
	}
	config := SessionConfig{
		SessionID:            session.ID,
		Shard:                session.Shard,
		CatchainSeqno:        session.CatchainSeqno,
		ValidatorSetHash:     session.ValidatorSetHash,
		Validators:           validators,
		OverlayMembers:       overlay.AllOverlayNodes,
		AllCurrentValidators: overlay.AllCurrentValidators,
		CollatorsByValidator: overlay.CollatorsByValidator,
		AllCollators:         overlay.AllCollators,
		ShardPrefixLen:       shardPrefixLen,
		Protocol:             protocol,
		CandidateLimits:      limits,
		Identity:             SessionIdentity{ADNLID: o.localADNLID},
		StorageID: SessionStorageID{
			SessionID:      session.ID,
			Shard:          session.Shard,
			CatchainSeqno:  session.CatchainSeqno,
			IsValidator:    false,
			LocalADNLID:    o.localADNLID,
			ValidatorIndex: simplex.ObserverIndex,
			Protocol:       protocol,
		},
	}
	state := SessionState{
		MasterchainBlock: descriptor.Update.MasterchainBlock,
		Params:           descriptor.Params,
		Registered:       descriptor.Update.Registered,
	}
	if descriptor.Update.HasFinalizedBlock {
		value := *descriptor.Update.FinalizedBlock.Copy()
		state.FinalizedBlock = &value
	}

	return observerRuntimeInput{config: config, state: state, limits: limits}, nil
}

func (o *ConsensusObserver) handleProgress(
	ctx context.Context,
	session *observerSession,
	progress sessionConsensusProgress,
) error {
	// UpdateSession assigns the whole config under session.mu from the collator
	// reconcile goroutine, so this window-event goroutine may only read it from
	// inside the same critical section.
	session.mu.Lock()
	sessionID := session.config.SessionID
	converted, err := collatorConsensusProgress(sessionID, progress)
	if err != nil {
		session.mu.Unlock()

		return fmt.Errorf("validator observer: convert consensus progress: %w", err)
	}
	switch session.phase {
	case observerSessionStarting:
		session.pending = &converted
		session.mu.Unlock()
		return nil
	case observerSessionActive:
		session.mu.Unlock()
		return o.deliverProgress(ctx, converted)
	case observerSessionRetiring, observerSessionRetired, observerSessionFailed:
		session.mu.Unlock()
		return context.Canceled
	default:
		session.mu.Unlock()
		return collator.ErrSessionUnavailable
	}
}

func (o *ConsensusObserver) handleFinalization(
	ctx context.Context,
	session *observerSession,
	block ton.BlockIDExt,
) error {
	// Same as handleProgress: the config is a whole-struct assignment guarded by
	// session.mu, never a stable snapshot for the backend goroutine.
	session.mu.Lock()
	finalization := collator.ConsensusFinalization{
		SessionID: session.config.SessionID,
		Block:     *block.Copy(),
	}
	switch session.phase {
	case observerSessionStarting:
		session.pendingFinalized = append(session.pendingFinalized, finalization)
		session.mu.Unlock()
		return nil
	case observerSessionActive:
		session.mu.Unlock()
		return o.deliverFinalization(ctx, finalization)
	case observerSessionRetiring, observerSessionRetired, observerSessionFailed:
		session.mu.Unlock()
		return context.Canceled
	default:
		session.mu.Unlock()
		return collator.ErrSessionUnavailable
	}
}

func (o *ConsensusObserver) deliverProgress(
	ctx context.Context,
	progress collator.ConsensusProgress,
) error {
	o.mu.RLock()
	events := o.events
	state := o.state
	o.mu.RUnlock()
	if state != consensusObserverRunning {
		return collator.ErrClosed
	}

	return events.Progressed(ctx, progress)
}

func (o *ConsensusObserver) deliverFinalization(
	ctx context.Context,
	finalization collator.ConsensusFinalization,
) error {
	o.mu.RLock()
	events := o.events
	state := o.state
	o.mu.RUnlock()
	if state != consensusObserverRunning {
		return collator.ErrClosed
	}

	return events.Finalized(ctx, finalization)
}

func (o *ConsensusObserver) flushProgress(
	ctx context.Context,
	session *observerSession,
	runtime observerSessionRuntime,
	progress collator.ConsensusProgress,
) {
	defer o.workersWG.Done()
	defer session.progressWG.Done()

	if err := o.deliverProgress(ctx, progress); err != nil {
		runtime.fail(fmt.Errorf("validator consensus observer: flush startup progress: %w", err))
	}
}

func (o *ConsensusObserver) flushFinalizations(
	ctx context.Context,
	session *observerSession,
	runtime observerSessionRuntime,
	finalizations []collator.ConsensusFinalization,
) {
	defer o.workersWG.Done()
	defer session.progressWG.Done()

	for i := range finalizations {
		if err := o.deliverFinalization(ctx, finalizations[i]); err != nil {
			runtime.fail(fmt.Errorf("validator consensus observer: flush startup finalization: %w", err))

			return
		}
	}
}

// watchRuntime owns the cancellation of the run it watches: runCancel is the
// only handle to the context the startup flush workers were launched with, so
// a runtime that terminates on its own must release it here. Retirement waits
// on those workers, and they may be parked inside a collator callback that the
// retiring goroutine itself is blocking; leaving the run context live turns
// that into a deadlock bounded only by the caller's deadline.
func (o *ConsensusObserver) watchRuntime(
	session *observerSession,
	runtime observerSessionRuntime,
	runCancel context.CancelFunc,
	returned <-chan error,
	done chan<- struct{},
) {
	defer o.workersWG.Done()
	defer close(done)

	runErr := <-returned
	// Released before Close blocks on the runtime goroutines: the flush workers
	// must observe the cancellation as early as possible.
	runCancel()
	closeErr := runtime.Close()
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.runtime != runtime {
		// A newer activation already installed its own runtime together with its
		// own runCancel; neither field belongs to this run anymore.
		return
	}
	if session.phase == observerSessionRetiring || session.phase == observerSessionRetired {
		return
	}
	if runErr == nil {
		runErr = errors.New("validator consensus observer: runtime stopped unexpectedly")
	}
	session.phase = observerSessionFailed
	session.terminal = errors.Join(runErr, closeErr)
	session.runCancel = nil
	if closeErr == nil {
		session.runtime = nil
	}
}

func observerSessionStart(activation collator.SessionActivation) SessionStart {
	start := SessionStart{
		Genesis:        make([]ton.BlockIDExt, len(activation.Genesis)),
		MinMasterchain: *activation.MinMasterchain.Copy(),
	}
	for i := range activation.Genesis {
		start.Genesis[i] = *activation.Genesis[i].Copy()
	}

	return start
}

func cloneObserverActivation(activation collator.SessionActivation) collator.SessionActivation {
	genesis := make([]ton.BlockIDExt, len(activation.Genesis))
	for i := range activation.Genesis {
		genesis[i] = *activation.Genesis[i].Copy()
	}
	activation.Genesis = genesis
	activation.MinMasterchain = *activation.MinMasterchain.Copy()

	return activation
}
