package validator

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	ErrSessionRuntimeStarted    = errors.New("validator runtime: session already started")
	ErrSessionRuntimeClosed     = errors.New("validator runtime: session closed")
	ErrLeaderWindowClosed       = errors.New("validator runtime: leader window closed")
	errWindowFinalizedChanged   = errors.New("validator runtime: finalized block changed while opening window")
	errLeaderWindowNeedsRecheck = errors.New(
		"validator runtime: leader window needs recheck after producer catch-up",
	)
)

// SessionReceiver is the inbound half of one private-overlay session. Network
// implementations call it only while Run is active and must join their own
// receiver goroutines before Run returns.
type SessionReceiver interface {
	ReceiveConsensusMessage(source simplex.PeerID, sourceValidator int, data []byte)
	PrecheckCandidateBroadcast(slot uint32, broadcastID [32]byte, signatureChecked bool) error
	// ReceiveCandidate accepts the bare candidate payload carried by the
	// private overlay and the parsed delegation from its extra. It verifies both
	// again and installs the candidate without first copying the payload into a
	// resolver-only wrapper. A successful return transfers an immutable decoded
	// view to the transport so it can relay the non-empty block into
	// public/custom overlays without a second candidate decompression pass.
	ReceiveCandidate(
		ctx context.Context,
		expectedSlot uint32,
		payload []byte,
		delegation *simplex.Delegation,
	) (CandidateArtifact, error)
	ServeCandidate(
		ctx context.Context,
		source simplex.PeerID,
		request CandidateRequest,
	) (CandidateResponse, error)
}

// SessionNetwork is deliberately only a protocol boundary for now. A concrete
// ADNL/private-overlay implementation is session-scoped and belongs at the
// composition root, not inside the consensus core. It authenticates the source
// and expected slot before committing the precheck or delivering a candidate;
// it also passes the authenticated peer to ServeCandidate. The runtime
// independently verifies schedule, delegation, signatures, and request limits.
// Start synchronously binds the receiver and completes all initialization. It
// must not deliver an inbound callback before returning. Run then owns the
// blocking session lifetime and returns when ctx is cancelled or the transport
// fails.
type SessionNetwork interface {
	simplex.Transport
	CandidateProvider
	// BroadcastCandidate validates and hands off CandidateGenerated without
	// waiting for private-overlay delivery. Public relay and private delivery are
	// independent best-effort paths. The buffers remain runtime-owned and
	// immutable; the network may retain their backing storage until its bounded
	// workers finish or the session is stopped.
	BroadcastCandidate(
		ctx context.Context,
		broadcast simplex.CandidateBroadcast,
		artifact CandidateArtifact,
	) error
	Start(context.Context, SessionReceiver) error
	Run(context.Context) error
}

type sessionConsensusProgress struct {
	Window    simplex.Window
	StartAt   time.Time
	BaseState *ChainState
}

type consensusProgressObserver func(context.Context, sessionConsensusProgress) error

// sessionSpeculativeWindow is one session's bet that the window about to open
// will open on a candidate it already holds, offered to whatever produces
// blocks for this session.
//
// It exists on the observer path only. A validator collating for itself makes
// the same bet from its own validation, where the successor state is already in
// hand; an observer has to resolve that state first, which is what the goroutine
// that fills this in does.
type sessionSpeculativeWindow struct {
	StartSlot uint32
	Leader    uint32
	Base      simplex.CandidateID
	BaseState *ChainState
	StartAt   time.Time
	Deadline  time.Time
}

type speculativeWindowObserver func(context.Context, sessionSpeculativeWindow) error

type consensusProgressObserverBackend interface {
	ObserveConsensusProgress(context.Context, sessionConsensusProgress) error
}

// consensusNotarizationObserverBackend is the optional half a backend
// implements to learn when the committee certified a candidate. The in-process
// collator sizes its blocks by the certificates on its own; see
// collator/committee_pace.go.
type consensusNotarizationObserverBackend interface {
	ObserveConsensusNotarized(simplex.CandidateID, time.Time)
}

// RuntimeOptions are already-bound dependencies for exactly one session.
// A SessionPreparer closure at the composition root creates distinct Network
// and Backend values for every concurrent validator group.
type RuntimeOptions struct {
	Storage  ValidatorStorage
	Network  SessionNetwork
	Backend  SessionBackend
	Limits   CandidateLimits
	Tracer   simplex.Tracer
	Observer ValidationObserver
	Logger   *zerolog.Logger
	// CaptureDir is the directory under the node data dir where a candidate this
	// session refuses with a TVM/semantic replay error is dumped for offline
	// replay. Empty disables capture, which is the case for every test runtime
	// that does not set it.
	CaptureDir string

	observeConsensusProgress consensusProgressObserver
	observeSpeculation       speculativeWindowObserver
	observeNotarization      func(simplex.CandidateID, time.Time)
}

// newSessionFailedCandidateCapturer builds the refused-candidate capturer for a
// session, stamped with its consensus identity. It returns nil when no capture
// directory is configured, and every capture call is a no-op on a nil capturer.
func newSessionFailedCandidateCapturer(config SessionConfig, options RuntimeOptions) *failedCandidateCapturer {
	capturer := newFailedCandidateCapturer(options.CaptureDir, options.Observer, options.Logger)
	if capturer == nil {
		return nil
	}
	sessionID := hex.EncodeToString(config.SessionID[:])
	var namespace string
	if ns, err := config.StorageID.Namespace(); err == nil {
		namespace = hex.EncodeToString(ns[:])
	}

	return capturer.withIdentity(sessionID, namespace)
}

type sessionRuntimePhase uint8

const (
	sessionRuntimePrepared sessionRuntimePhase = iota
	sessionRuntimeRunning
	sessionRuntimeStopped
	sessionRuntimeClosed
)

type sessionRuntime struct {
	config    SessionConfig
	storage   ValidatorStorage
	network   SessionNetwork
	backend   SessionBackend
	codec     *candidateCodec
	observe   consensusProgressObserver
	speculate speculativeWindowObserver
	notarized func(simplex.CandidateID, time.Time)
	metrics   ValidationObserver
	log       *zerolog.Logger

	// ownSlots holds, for every slot this node published a candidate for, when
	// it went out — until consensus says what became of it. It is the only
	// place the two facts meet: the producer knows when it published and
	// nothing else does, and the consensus hooks know the outcome and not the
	// instant. Without it the panel that matters most to a leader — did my
	// block make it, and how long did the committee take — cannot be drawn.
	ownSlotsMu sync.Mutex
	ownSlots   map[uint32]ownSlotState

	candidates *candidateResolver
	states     *stateResolver
	engine     *simplex.Engine
	runner     *simplex.Runner
	capturer   *failedCandidateCapturer

	lifecycleMu sync.Mutex
	phase       sessionRuntimePhase
	// runnerLaunched reports that the Simplex loop goroutine exists, so posting
	// onto its queue can complete. The phase turns Running much earlier, while
	// run still activates the backend and opens the private overlay; routing an
	// update through the runner in that window would block forever on a loop
	// that may never start. Guarded by lifecycleMu rather than an atomic: the
	// mutex both closes the window and publishes the direct engine writes that
	// happened before the loop goroutine was created.
	runnerLaunched bool
	terminalErr    error
	runCancel      context.CancelFunc
	runDone        chan struct{}
	runCtx         context.Context

	stateMu sync.RWMutex
	state   SessionState

	workMu     sync.Mutex
	workActive bool
	workCtx    context.Context
	workWG     sync.WaitGroup

	// warming holds the candidates whose successor state a background resolve
	// is already computing. It exists only on an observer session: see
	// warmCandidateState.
	warmingMu sync.Mutex
	warming   map[simplex.CandidateID]struct{}

	fatal chan error

	windowEventsMu   sync.Mutex
	windowEvents     []simplex.Window
	windowEventsHead int
	windowEventsWake chan struct{}

	// Leader-window persistence is status telemetry, so one owned worker keeps
	// only the newest record waiting behind its current durable write.
	leaderWindowRecordMu      sync.Mutex
	leaderWindowRecordPending LeaderWindowRecord
	leaderWindowRecordSet     bool
	leaderWindowRecordWake    chan struct{}

	// candidateCache is the last cache projection this session published. The
	// gauges behind it are process-wide while several sessions of one chain are
	// live at once, so a session only ever reports its own change.
	candidateCacheMu sync.Mutex
	candidateCache   candidateCacheStats
	// retentionCapped is whether the last sweep pruned past what the local
	// producer asked to keep. It exists so entering that state is logged once
	// instead of on every finalization, and it is the one part of the retention
	// gauge set that is a statement about a sweep rather than about now — the
	// rest is read live off the resolvers, see ConsensusStats.
	retentionCapped bool

	windowMu sync.Mutex
	window   *windowSubmitter

	// progressMu owns the short-lived context used while this runtime turns a
	// durably observed Simplex window into local collator progress. A later
	// window can arrive on the engine goroutine while the ordered dispatcher is
	// in a typed local-readiness retry, so it cancels only that strictly older
	// attempt. Ordinary observer calls remain serial and are never interrupted.
	progressMu          sync.Mutex
	progressLatestStart uint32
	progressLatestSet   bool
	progressStart       uint32
	progressGeneration  uint64
	progressRetrying    bool
	progressCancel      context.CancelFunc

	resourceCloseOnce sync.Once
	backendCloseMu    sync.Mutex
	backendClosed     bool
	backendRetired    bool
}

// PrepareSessionRuntime builds one inactive runtime. It intentionally accepts
// session-scoped dependencies instead of hiding per-session construction in a
// factory interface.
func PrepareSessionRuntime(
	ctx context.Context,
	config SessionConfig,
	initial SessionState,
	options RuntimeOptions,
) (SessionRuntime, error) {
	runtime, err := prepareSessionRuntime(ctx, config, initial, options)
	if err != nil {
		return nil, err
	}

	return runtime, nil
}

func prepareSessionRuntime(
	ctx context.Context,
	config SessionConfig,
	initial SessionState,
	options RuntimeOptions,
) (*sessionRuntime, error) {
	if err := validateRuntimeConfig(config, options); err != nil {
		return nil, err
	}
	if err := options.Limits.validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	params := initial.Params
	if params == (simplex.Params{}) {
		// Engine construction explicitly defines the zero value as the protocol
		// defaults. Resolve it once so the candidate resolver and backend see the
		// same values as the engine.
		params = simplex.DefaultParams()
		initial.Params = params
	}
	if err := validateRuntimeState(config, initial); err != nil {
		return nil, err
	}

	loaded, err := config.Journal.Bootstrap()
	if err != nil {
		return nil, fmt.Errorf("validator runtime: bootstrap simplex journal: %w", err)
	}
	if loaded == nil {
		return nil, errors.New("validator runtime: simplex journal returned no bootstrap state")
	}
	validators := runtimeValidators(config.Validators)
	// The journal is verified once here and handed to all three of its
	// consumers — the resolver, the masterchain finalization replay and the
	// engine — instead of each of them re-verifying the certificates it reads.
	bootstrap, err := simplex.ValidateBootstrap(config.SessionID, validators, loaded)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: %w", err)
	}
	stored, err := options.Storage.LoadSession(ctx, config.StorageID)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: load session storage: %w", err)
	}
	if stored.ID != config.StorageID {
		return nil, errors.New("validator runtime: loaded session descriptor mismatch")
	}

	codec, err := newCandidateCodec(config, options.Limits)
	if err != nil {
		return nil, err
	}
	recoveryFinalizations, err := masterchainRecoveryFinalizations(config.Shard, bootstrap)
	if err != nil {
		return nil, err
	}
	resolver, err := newCandidateResolver(candidateResolverOptions{
		Session:            config.StorageID,
		SessionID:          config.SessionID,
		Storage:            options.Storage,
		Provider:           options.Network,
		Codec:              codec,
		Validators:         validators,
		PeerCount:          len(config.OverlayMembers),
		ValidateCandidates: config.Identity.Validator != nil,
		Limits:             options.Limits,
		Params:             params,
		Stored:             stored,
		Bootstrap:          bootstrap,
	})
	if err != nil {
		return nil, err
	}

	runtime := &sessionRuntime{
		config:                 config,
		storage:                options.Storage,
		network:                options.Network,
		backend:                options.Backend,
		codec:                  codec,
		observe:                options.observeConsensusProgress,
		speculate:              options.observeSpeculation,
		notarized:              options.observeNotarization,
		metrics:                options.Observer,
		log:                    options.Logger,
		candidates:             resolver,
		capturer:               newSessionFailedCandidateCapturer(config, options),
		state:                  cloneSessionState(initial),
		fatal:                  make(chan error, 1),
		phase:                  sessionRuntimePrepared,
		windowEventsWake:       make(chan struct{}, 1),
		leaderWindowRecordWake: make(chan struct{}, 1),
		warming:                make(map[simplex.CandidateID]struct{}),
	}
	if runtime.observe == nil {
		if observer, ok := options.Backend.(consensusProgressObserverBackend); ok {
			runtime.observe = observer.ObserveConsensusProgress
		}
	}
	if runtime.notarized == nil {
		// Voting runtimes report to their producer-owning backend. Standalone
		// observers supply the controller's collator callback explicitly above.
		if observer, ok := options.Backend.(consensusNotarizationObserverBackend); ok {
			runtime.notarized = observer.ObserveConsensusNotarized
		}
	}
	runtime.states = newStateResolver(
		config.Shard,
		config.StorageID,
		options.Storage,
		options.Backend,
		resolver,
		stored,
		recoveryFinalizations,
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	runtime.states.describeFailed = func(id simplex.CandidateID, err error) {
		// The same shape OnFinalized gives an acceptance error: the block is
		// accepted and consensus continues, only the shard-top description that
		// helps masterchain collators include it sooner is missing.
		if runtime.log != nil {
			runtime.log.Warn().
				Err(err).
				Hex("session_id", config.SessionID[:]).
				Uint32("slot", id.Slot).
				Msg("shard-top description of an accepted block failed; consensus continues")
		}
	}

	localIndex := simplex.ObserverIndex
	var signer simplex.Signer
	if config.Identity.Validator != nil {
		localIndex = int(config.Identity.Validator.Index)
		signer = config.Identity.Validator.Signer
	}
	engine, err := simplex.NewEngine(simplex.Config{
		SessionID:            config.SessionID,
		ProtocolVersion:      config.Protocol.ProtocolVersion,
		Validators:           validators,
		LocalIndex:           localIndex,
		SlotsPerLeaderWindow: config.Protocol.SlotsPerLeaderWindow,
		ShardPrefixLen:       config.ShardPrefixLen,
		Workchain:            config.Shard.Workchain,
		Shard:                uint64(config.Shard.Shard),
		CCSeqno:              config.CatchainSeqno,
		Params:               &params,
		Journal:              config.Journal,
		Bootstrap:            bootstrap,
		Transport:            options.Network,
		Hooks:                runtime,
		Tracer:               options.Tracer,
		Signer:               signer,
		Logger:               options.Logger,
	})
	if err != nil {
		closeErr := runtime.closeResources()

		return nil, errors.Join(err, closeErr)
	}
	runtime.engine = engine
	runtime.runner = simplex.NewRunner(engine)
	if options.Observer != nil {
		// From here on this session has a consensus position worth reporting,
		// and closeResources is the only way out of every path below.
		options.Observer.RegisterConsensusSession(config.StorageID, runtime)
	}
	if err = options.Backend.UpdateSession(ctx, runtime.state); err != nil {
		updateErr := fmt.Errorf("validator runtime: initialize session backend: %w", err)
		closeErr := runtime.closeResources()

		return nil, errors.Join(updateErr, closeErr)
	}
	return runtime, nil
}

func validateRuntimeConfig(config SessionConfig, options RuntimeOptions) error {
	if options.Storage == nil || options.Network == nil || options.Backend == nil {
		return errors.New("validator runtime: storage, network, and backend are required")
	}
	if config.Journal == nil {
		return errors.New("validator runtime: simplex journal is required")
	}
	if err := config.StorageID.Validate(); err != nil {
		return err
	}
	if config.SessionID != config.StorageID.SessionID || config.Shard != config.StorageID.Shard ||
		config.CatchainSeqno != config.StorageID.CatchainSeqno ||
		config.Protocol != config.StorageID.Protocol ||
		config.Identity.ADNLID != config.StorageID.LocalADNLID {
		return errors.New("validator runtime: storage descriptor differs from session config")
	}
	if len(config.Validators) == 0 {
		return errors.New("validator runtime: empty validator set")
	}
	if len(config.OverlayMembers) == 0 {
		return errors.New("validator runtime: empty overlay roster")
	}
	if !config.Shard.IsValid() {
		return errors.New("validator runtime: invalid session shard")
	}
	prefix, err := config.Shard.PrefixBits()
	if err != nil {
		return err
	}
	if prefix != config.ShardPrefixLen {
		return fmt.Errorf("validator runtime: shard prefix length %d, want %d", config.ShardPrefixLen, prefix)
	}

	identity := config.Identity.Validator
	if identity == nil {
		if config.StorageID.IsValidator || config.StorageID.ValidatorIndex != simplex.ObserverIndex {
			return errors.New("validator runtime: observer storage descriptor has validator identity")
		}

		return nil
	}
	if identity.Signer == nil {
		return errors.New("validator runtime: validator signer is required")
	}
	if identity.Index >= uint32(len(config.Validators)) {
		return fmt.Errorf("validator runtime: local validator index %d is out of range", identity.Index)
	}
	local := config.Validators[identity.Index]
	if local.PublicKeyHash != identity.KeyID || config.StorageID.ValidatorKeyID != identity.KeyID ||
		!config.StorageID.IsValidator || config.StorageID.ValidatorIndex != int(identity.Index) {
		return errors.New("validator runtime: local validator identity differs from roster or storage")
	}
	if groups.ValidatorADNL(local) != config.Identity.ADNLID {
		return errors.New("validator runtime: local validator ADNL identity differs from roster")
	}

	return nil
}

func validateRuntimeState(config SessionConfig, state SessionState) error {
	if state.FinalizedBlock != nil &&
		(state.FinalizedBlock.Workchain != config.Shard.Workchain || state.FinalizedBlock.Shard != config.Shard.Shard) {
		return errors.New("validator runtime: finalized block belongs to another shard")
	}

	return nil
}

func runtimeValidators(validators []groups.Validator) []simplex.Validator {
	result := make([]simplex.Validator, len(validators))
	for i := range validators {
		result[i] = simplex.Validator{
			PublicKey: ed25519.PublicKey(validators[i].PublicKey[:]),
			Weight:    validators[i].Weight,
		}
	}

	return result
}

// masterchainRecoveryFinalizations selects the durable finalizations to replay.
// Their signatures were verified when the bootstrap state was validated, which
// is what the argument type carries.
func masterchainRecoveryFinalizations(
	shard groups.ShardID,
	bootstrap simplex.ValidatedBootstrap,
) ([]simplex.VerifiedCertificate, error) {
	if !shard.IsMasterchain() {
		return nil, nil
	}
	state := bootstrap.State()
	if state == nil {
		return nil, errors.New("validator runtime: bootstrap state was not validated")
	}

	certificates := bootstrap.Certificates()
	finalizations := make([]simplex.VerifiedCertificate, 0, len(certificates)/2)
	bySlot := make(map[uint32]simplex.CandidateID, len(certificates)/2)
	for _, certificate := range certificates {
		if certificate.Vote().Kind != simplex.VoteFinalize {
			continue
		}

		id := certificate.Vote().ID
		if previous, exists := bySlot[id.Slot]; exists {
			if previous != id {
				return nil, fmt.Errorf(
					"validator runtime: conflicting bootstrap final certificates for slot %d",
					id.Slot,
				)
			}

			continue
		}
		bySlot[id.Slot] = id
		finalizations = append(finalizations, certificate)
	}

	sort.Slice(finalizations, func(i, j int) bool {
		return finalizations[i].Vote().ID.Slot < finalizations[j].Vote().ID.Slot
	})

	return finalizations, nil
}

func (r *sessionRuntime) Run(ctx context.Context, start SessionStart) error {
	return r.run(ctx, start, nil)
}

// Recover replays durable finalizations while the runtime is still inactive.
// Future groups need this phase too: the node database may lag their consensus
// journal after asynchronous local acceptance, but starting their Simplex
// engine before the masterchain activates them would be a protocol violation.
func (r *sessionRuntime) Recover(ctx context.Context, start SessionStart) error {
	r.lifecycleMu.Lock()
	phase := r.phase
	r.lifecycleMu.Unlock()
	if phase == sessionRuntimeClosed {
		return ErrSessionRuntimeClosed
	}
	if phase != sessionRuntimePrepared {
		return ErrSessionRuntimeStarted
	}

	return r.states.start(ctx, start)
}

func (r *sessionRuntime) runWithStartup(
	ctx context.Context,
	start SessionStart,
	startup chan<- error,
) error {
	return r.run(ctx, start, startup)
}

func (r *sessionRuntime) run(
	ctx context.Context,
	start SessionStart,
	startup chan<- error,
) (resultErr error) {
	startupSent := false
	cleanCancellation := false
	signalStartup := func(err error) {
		if startup == nil || startupSent {
			return
		}
		startupSent = true
		startup <- err
	}
	defer func() {
		startupResult := resultErr
		if cleanCancellation {
			resultErr = nil
		}
		signalStartup(startupResult)
	}()

	r.lifecycleMu.Lock()
	if r.phase == sessionRuntimeClosed {
		r.lifecycleMu.Unlock()

		return ErrSessionRuntimeClosed
	}
	if r.phase != sessionRuntimePrepared {
		r.lifecycleMu.Unlock()

		return ErrSessionRuntimeStarted
	}
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	r.phase = sessionRuntimeRunning
	r.runCtx = runCtx
	r.runCancel = cancel
	r.runDone = runDone
	r.lifecycleMu.Unlock()

	defer func() {
		cancel()
		r.closeWindow(context.Canceled)
		r.stopWork()

		r.lifecycleMu.Lock()
		if resultErr != nil && !cleanCancellation {
			r.terminalErr = resultErr
		}
		if r.phase != sessionRuntimeClosed {
			r.phase = sessionRuntimeStopped
		}
		close(runDone)
		r.lifecycleMu.Unlock()
	}()

	// Timed per phase: the session's first window is due against the
	// committee's first-block timeout, and every phase here is on that path.
	runStarted := time.Now()
	if err := r.backend.ActivateSession(runCtx, start); err != nil {
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}
		return fmt.Errorf("validator runtime: activate session backend: %w", err)
	}
	activated := time.Since(runStarted)
	statesAt := time.Now()
	if err := r.states.start(runCtx, start); err != nil {
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}
		return err
	}
	statesReady := time.Since(statesAt)
	r.startWork(runCtx)
	if !r.spawn(r.runLeaderWindowRecords) {
		return errors.New("validator runtime: start leader window recorder")
	}
	if !r.spawn(r.runWindowEvents) {
		return errors.New("validator runtime: start consensus window dispatcher")
	}
	networkAt := time.Now()
	if err := r.network.Start(runCtx, r); err != nil {
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}
		return fmt.Errorf("validator runtime: start network: %w", err)
	}
	networkStarted := time.Since(networkAt)
	simplexAt := time.Now()
	results := make(chan runtimeComponentResult, 2)
	runnerStartup := make(chan error, 1)
	r.lifecycleMu.Lock()
	r.runnerLaunched = true
	r.lifecycleMu.Unlock()
	go func() {
		results <- runtimeComponentResult{
			name: "simplex",
			err:  r.runner.RunWithStartup(runCtx, runnerStartup),
		}
	}()
	go func() { results <- runtimeComponentResult{name: "network", err: r.network.Run(runCtx)} }()

	remaining := 2
	started := false
	for !started && resultErr == nil {
		select {
		case err := <-runnerStartup:
			if err != nil {
				resultErr = fmt.Errorf("validator runtime: start simplex: %w", err)
				break
			}
			started = true
			// The logger is optional on this runtime, as every other use of it
			// in this file assumes; the in-process test runtimes run without one.
			if r.log != nil {
				r.log.Info().
					Hex("session_id", r.config.SessionID[:]).
					Int32("workchain", r.config.Shard.Workchain).
					Int64("shard", r.config.Shard.Shard).
					Dur("activate", activated).
					Dur("states", statesReady).
					Dur("network", networkStarted).
					Dur("simplex", time.Since(simplexAt)).
					Dur("total", time.Since(runStarted)).
					Msg("validator session started")
			}
		case err := <-r.fatal:
			resultErr = err
		case result := <-results:
			remaining--
			resultErr = unexpectedComponentExit(runCtx, result)
		case <-runCtx.Done():
			cleanCancellation = true
			resultErr = runCtx.Err()
		}
	}
	if resultErr == nil {
		// Prefer a component failure already queued at the readiness boundary
		// over reporting a live session. Failures after this check are ordinary
		// asynchronous runtime failures and are observed by the owner watcher.
		select {
		case err := <-r.fatal:
			resultErr = err
		case result := <-results:
			remaining--
			resultErr = unexpectedComponentExit(runCtx, result)
		default:
			signalStartup(nil)
		}
	}

	if resultErr == nil {
		select {
		case err := <-r.fatal:
			resultErr = err
		case result := <-results:
			remaining--
			resultErr = unexpectedComponentExit(runCtx, result)
		case <-runCtx.Done():
		}
	}

	cancel()
	for remaining > 0 {
		component := <-results
		remaining--
		if resultErr == nil && component.err != nil && !errors.Is(component.err, context.Canceled) {
			resultErr = fmt.Errorf("validator runtime: %s failed: %w", component.name, component.err)
		}
	}
	if resultErr != nil {
		return resultErr
	}

	return nil
}

type runtimeComponentResult struct {
	name string
	err  error
}

func unexpectedComponentExit(ctx context.Context, result runtimeComponentResult) error {
	if ctx.Err() != nil {
		return nil
	}
	if result.err == nil {
		return fmt.Errorf("validator runtime: %s stopped unexpectedly", result.name)
	}

	return fmt.Errorf("validator runtime: %s failed: %w", result.name, result.err)
}

func (r *sessionRuntime) Update(ctx context.Context, state SessionState) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRuntimeState(r.config, state); err != nil {
		return err
	}
	state = cloneSessionState(state)
	r.stateMu.RLock()
	currentFinalized := r.state.FinalizedBlock
	switch {
	case currentFinalized == nil:
	case state.FinalizedBlock == nil || state.FinalizedBlock.SeqNo < currentFinalized.SeqNo:
		// Missing and older shard descriptors cannot revoke the session-local
		// anchor already accepted by consensus. LocalSessionBackend preserves the
		// same progress in its collator update, so the runtime keeps both views
		// aligned before the backend sees this masterchain snapshot.
		finalized := *currentFinalized.Copy()
		state.FinalizedBlock = &finalized
	case state.FinalizedBlock.SeqNo == currentFinalized.SeqNo &&
		!sameBlockID(*state.FinalizedBlock, *currentFinalized):
		r.stateMu.RUnlock()

		return errors.New("validator runtime: finalized block conflicts with current state")
	}
	r.stateMu.RUnlock()

	switch r.phase {
	case sessionRuntimePrepared, sessionRuntimeRunning:
	default:
		if r.terminalErr != nil {
			return r.terminalErr
		}
		return ErrSessionRuntimeClosed
	}
	if err := state.Params.Validate(); err != nil {
		return err
	}
	if err := r.backend.UpdateSession(ctx, state); err != nil {
		return fmt.Errorf("validator runtime: update session backend: %w", err)
	}

	var paramsErr error
	if !r.runnerLaunched {
		// Nothing serves the runner queue yet, so the engine is still exclusively
		// ours to touch directly under lifecycleMu.
		paramsErr = r.engine.UpdateParams(state.Params)
	} else {
		paramsErr = r.runner.UpdateParams(state.Params)
	}
	if paramsErr != nil {
		terminalErr := fmt.Errorf("validator runtime: update simplex params after backend commit: %w", paramsErr)
		r.fail(terminalErr)

		return terminalErr
	}

	r.candidates.updateParams(state.Params)
	r.states.updateParams(state.Params)

	r.stateMu.Lock()
	r.state = state
	r.stateMu.Unlock()

	return nil
}

func (r *sessionRuntime) Close() error {
	r.lifecycleMu.Lock()
	if r.phase == sessionRuntimeClosed {
		done := r.runDone
		r.lifecycleMu.Unlock()
		if done != nil {
			<-done
		}
		return r.closeResources()
	}
	r.phase = sessionRuntimeClosed
	cancel := r.runCancel
	done := r.runDone
	r.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return r.closeResources()
}

func (r *sessionRuntime) Retire() error {
	closeErr := r.Close()

	r.backendCloseMu.Lock()
	defer r.backendCloseMu.Unlock()
	if r.backendRetired {
		return closeErr
	}
	retireErr := r.backend.Retire()
	if retireErr == nil {
		r.backendRetired = true
		r.backendClosed = true
	}

	return errors.Join(closeErr, retireErr)
}

func (r *sessionRuntime) closeResources() error {
	r.resourceCloseOnce.Do(func() {
		r.states.close()
		r.candidates.close()
		// A retired session must not leave its share of the process-wide gauges
		// behind. Its counters do stay: the collector takes a final reading and
		// keeps it, because work this session did is work the process did.
		r.publishCandidateCache(candidateCacheStats{})
		if r.metrics != nil {
			r.metrics.UnregisterConsensusSession(r.config.StorageID)
		}
	})

	r.backendCloseMu.Lock()
	defer r.backendCloseMu.Unlock()
	if r.backendClosed {
		return nil
	}

	err := r.backend.Close()
	if err == nil {
		r.backendClosed = true
	}

	return err
}

func (r *sessionRuntime) startWork(ctx context.Context) {
	r.workMu.Lock()
	r.workCtx = ctx
	r.workActive = true
	r.workMu.Unlock()
}

func (r *sessionRuntime) stopWork() {
	r.workMu.Lock()
	r.workActive = false
	r.workCtx = nil
	r.workMu.Unlock()
	r.workWG.Wait()
}

func (r *sessionRuntime) spawn(work func(context.Context)) bool {
	r.workMu.Lock()
	if !r.workActive {
		r.workMu.Unlock()

		return false
	}
	ctx := r.workCtx
	r.workWG.Add(1)
	r.workMu.Unlock()

	go func() {
		defer r.workWG.Done()
		work(ctx)
	}()

	return true
}

func (r *sessionRuntime) execute(ctx context.Context, work func(context.Context) error) error {
	r.workMu.Lock()
	if !r.workActive {
		r.workMu.Unlock()

		return context.Canceled
	}
	runtimeCtx := r.workCtx
	r.workWG.Add(1)
	r.workMu.Unlock()
	defer r.workWG.Done()

	// A child of a never-cancelled parent is pure overhead: it would take the
	// shared runtime context's mutex twice and mutate its children map twice
	// per call, which is measurable on the per-message consensus ingress where
	// every caller passes context.Background().
	if ctx.Done() == nil {
		return work(runtimeCtx)
	}

	commandCtx, cancel := context.WithCancel(runtimeCtx)
	stop := context.AfterFunc(ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	return work(commandCtx)
}

func (r *sessionRuntime) fail(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrResolverClosed) {
		r.workMu.Lock()
		ctx := r.workCtx
		active := r.workActive
		r.workMu.Unlock()
		if !active || ctx.Err() != nil {
			return
		}
	}
	select {
	case r.fatal <- err:
	default:
	}
}

func (r *sessionRuntime) ReceiveConsensusMessage(
	source simplex.PeerID,
	sourceValidator int,
	data []byte,
) {
	_ = r.execute(context.Background(), func(context.Context) error {
		r.runner.HandleMessage(source, sourceValidator, data)

		return nil
	})
}

func (r *sessionRuntime) PrecheckCandidateBroadcast(
	slot uint32,
	broadcastID [32]byte,
	signatureChecked bool,
) error {
	return r.execute(context.Background(), func(context.Context) error {
		return r.runner.PrecheckCandidateBroadcast(slot, broadcastID, signatureChecked)
	})
}

func (r *sessionRuntime) ReceiveCandidate(
	ctx context.Context,
	expectedSlot uint32,
	payload []byte,
	delegation *simplex.Delegation,
) (received CandidateArtifact, err error) {
	err = r.execute(ctx, func(context.Context) error {
		// The canonical wire is not built here. Nothing on this path reads it:
		// the candidate is stored when the engine says so, served when a peer
		// asks, and the admission check stands on the signed id. The lazy wire
		// builds the bytes for the first of those that comes, and a candidate
		// none of them comes for — most of them, on a node that is not the
		// leader — never pays the serialization and the LZ4 pass at all.
		artifact, lazy, err := r.codec.decodeBroadcastDeferred(payload, delegation, expectedSlot)
		if err != nil {
			return err
		}
		if err = r.candidates.stageDeferred(artifact, lazy); err != nil {
			return err
		}

		if err = r.runner.SubmitCandidate(&artifact.Candidate); err != nil {
			return err
		}
		// The earliest instant a candidate's successor state can be computed,
		// and the one a validator gets for free by validating it. On an observer
		// this is what keeps the window that opens on this candidate from paying
		// the apply itself — and, for the last slot of a window, what makes the
		// bet on the next one possible at all.
		r.warmCandidateState(artifact.Candidate.ID)
		received = *artifact

		return nil
	})

	return received, err
}

func (r *sessionRuntime) ServeCandidate(
	ctx context.Context,
	source simplex.PeerID,
	request CandidateRequest,
) (response CandidateResponse, err error) {
	err = r.execute(ctx, func(commandCtx context.Context) error {
		response, err = r.candidates.response(commandCtx, source, request)

		return err
	})

	return response, err
}

func (r *sessionRuntime) ValidateCandidate(candidate *simplex.Candidate, done func(error)) {
	if !r.spawn(func(ctx context.Context) {
		// C++ runs try_notarize as a detached task: both CandidateReject and a
		// validation-task error abort only this candidate, never the consensus
		// actor. Preserve that distinction in the error chain for diagnostics, but
		// deliver either outcome solely through the candidate continuation.
		// The deadline is wall clock and flat, because the reference's is and
		// because most of it is a wait on this node's apply pipeline rather than
		// validation work; see retention.go. It expires into an abstention, never
		// into a rejection, and session retirement cancels it earlier.
		validationCtx, cancel := context.WithTimeout(ctx, candidateValidationTimeout)
		defer cancel()

		done(r.validateCandidate(validationCtx, candidate))
	}) {
		done(context.Canceled)
	}
}

func (r *sessionRuntime) validateCandidate(ctx context.Context, candidate *simplex.Candidate) error {
	if r.metrics == nil {
		_, err := r.validateCandidateCore(ctx, candidate, time.Time{})

		return err
	}

	metricContext := r.validationMetricContext(candidate)
	started := time.Now()
	r.metrics.AddValidationInflight(metricContext, 1)
	decisionDuration, err := r.validateCandidateCore(ctx, candidate, started)
	readyDuration := time.Since(started)
	if decisionDuration == 0 {
		decisionDuration = readyDuration
	}
	r.metrics.AddValidationInflight(metricContext, -1)
	r.metrics.ObserveValidation(ValidationObservation{
		ValidationContext: metricContext,
		Result:            validationResult(err),
		DecisionDuration:  decisionDuration,
		ReadyDuration:     readyDuration,
	})

	return err
}

func (r *sessionRuntime) validateCandidateCore(
	ctx context.Context,
	candidate *simplex.Candidate,
	validationStarted time.Time,
) (time.Duration, error) {
	chain := r.validationChain()
	stageStarted := r.validationStageStarted()
	artifact, err := r.candidates.candidate(ctx, candidate.ID)
	r.observeValidationStage(chain, ValidationStageLoadCandidate, stageStarted)
	if err != nil {
		return 0, fmt.Errorf("validator runtime: load candidate for validation: %w", err)
	}
	if r.metrics != nil {
		r.metrics.ObserveValidationCandidateSize(chain, len(artifact.BlockBOC), len(artifact.CollatedData))
	}
	stageStarted = r.validationStageStarted()
	parent, err := r.states.resolve(ctx, candidate.Parent)
	r.observeValidationStage(chain, ValidationStageResolveParent, stageStarted)
	if err != nil {
		return 0, fmt.Errorf("validator runtime: resolve candidate parent: %w", err)
	}
	if candidate.Empty {
		started := time.Now()
		stageStarted = r.validationStageStarted()
		normal, normalErr := parent.State.NormalBlock()
		if normalErr != nil || !sameBlockID(normal, candidate.Block) {
			r.observeValidationStage(chain, ValidationStageSemanticValidation, stageStarted)

			return 0, fmt.Errorf("%w: empty candidate references the wrong block", ErrCandidateRejected)
		}
		r.observeValidationStage(chain, ValidationStageSemanticValidation, stageStarted)
		r.logBlockValidated(candidate, artifact, time.Since(started))

		return validationDecisionDuration(validationStarted), nil
	}
	r.stateMu.RLock()
	minBlockInterval := r.state.Params.MinBlockInterval
	r.stateMu.RUnlock()
	if !parent.GenUtime.IsZero() {
		stageStarted = r.validationStageStarted()
		if err = waitUntil(ctx, parent.GenUtime.Add(minBlockInterval)); err != nil {
			r.observeValidationStage(chain, ValidationStageWaitMinBlockInterval, stageStarted)

			return 0, err
		}
		r.observeValidationStage(chain, ValidationStageWaitMinBlockInterval, stageStarted)
	}

	request := CandidateValidationRequest{
		Parent:    parent.State,
		Artifact:  artifact,
		successor: parent.State.pendingSuccessor(ctx),
	}
	started := time.Now()
	stageStarted = r.validationStageStarted()
	validation, validateErr := r.backend.ValidateCandidate(ctx, request)
	semanticDuration := time.Since(started)
	r.observeValidationStage(chain, ValidationStageSemanticValidation, stageStarted)
	if r.metrics != nil {
		r.metrics.ObserveValidationTask(ValidationTaskObservation{
			Chain:    chain,
			Result:   validationTaskResult(validateErr),
			Duration: semanticDuration,
		})
	}
	if validateErr == nil {
		r.states.rememberValidatedState(candidate.ID, ResolvedState{
			State:    validation.State,
			GenUtime: validation.ValidAfter,
		})
		r.logBlockValidated(candidate, artifact, semanticDuration)
		decisionDuration := validationDecisionDuration(validationStarted)

		stageStarted = r.validationStageStarted()
		err = waitUntil(ctx, validation.ValidAfter)
		r.observeValidationStage(chain, ValidationStageWaitValidAfter, stageStarted)

		return decisionDuration, err
	}
	// A semantic/TVM replay disagreement is dumped for offline replay before the
	// error is classified below. This is the only place the candidate, its
	// collated proofs and the resolved parent are all in hand; a not-ready or
	// cancelled abstain is benign and is filtered out inside the capturer.
	if r.capturer != nil && isCapturableValidationFailure(validateErr) {
		r.capturer.capture(candidate, artifact, parent.State, chain, validateErr)
	}
	if errors.Is(validateErr, ErrCandidateRejected) {
		r.reportSelfRejection(candidate, validateErr)
		if r.log != nil {
			r.log.Debug().
				Err(validateErr).
				Hex("session_id", r.config.SessionID[:]).
				Uint32("slot", candidate.ID.Slot).
				Int32("workchain", candidate.Block.Workchain).
				Int64("shard", candidate.Block.Shard).
				Uint32("block_seqno", candidate.Block.SeqNo).
				Msg("candidate validation rejected")
		}

		return 0, validateErr
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}

	return 0, fmt.Errorf("validator runtime: validate candidate: %w", validateErr)
}

// reportSelfRejection raises the alarm for a candidate this node produced and
// then refused. It mirrors the reference exactly, including the wording:
// BlockValidator's try_notarize logs
//
//	LOG(ERROR) << "BUG! Candidate " << event->candidate->id
//	           << " is self-rejected: " << ...get<CandidateReject>().reason;
//
// when a rejected candidate's leader is the local index
// (cppnode/ton/validator/consensus/block-validator.cpp:112-117), on the same
// branch, before returning the same rejection.
//
// We do re-validate our own candidates and that is a deliberate decision, not
// an oversight: publishCandidate broadcasts at session_runtime.go:1821 and only
// submits for validation at :1842, so the candidate is on the wire before this
// runs and this work overlaps the remote validators' identical work rather than
// delaying it. C++ makes the same choice — try_notarize (consensus.cpp:225-260)
// awaits the validation result before voting with no local-index short circuit.
// What the two get for it is this: a producer and a validator that disagree
// about the same bytes is a collator/validator asymmetry, which is a defect in
// this node and nowhere else, and it is otherwise silent — the candidate simply
// never gets our vote and every peer's rejection log names their node.
//
// The predicate is the reference's, and it is not a heuristic. candidate.Leader
// is not attacker-controlled: verifyCandidate pins it to
// schedule.ExpectedLeader(slot) and then verifies the candidate signature
// against that leader's key (candidate_artifact.go:445-460), so a remote peer
// cannot present a candidate carrying our index without our key. An observer
// runtime has no Identity.Validator at all and therefore no local index to
// match, which is the second reason a remote candidate can never reach this.
//
// A delegated window is included on purpose. The block was built by a collator
// we authorized with our own delegation signature and it is published under our
// leadership, so it is our production path failing our validation path — the
// thing this alarm exists to surface.
func (r *sessionRuntime) reportSelfRejection(candidate *simplex.Candidate, reason error) {
	if r.config.Identity.Validator == nil || candidate.Leader != r.config.Identity.Validator.Index {
		return
	}
	if r.metrics != nil {
		r.metrics.AddSelfRejectedCandidate(r.validationChain())
	}
	if r.log == nil {
		return
	}
	event := r.log.Error()
	if event == nil {
		return
	}
	event.
		Err(reason).
		Hex("session_id", r.config.SessionID[:]).
		Uint32("slot", candidate.ID.Slot).
		Uint32("leader", candidate.Leader).
		Hex("candidate_hash", candidate.ID.Hash[:]).
		Int32("workchain", candidate.Block.Workchain).
		Int64("shard", candidate.Block.Shard).
		Uint32("block_seqno", candidate.Block.SeqNo).
		Bool("is_empty", candidate.Empty).
		Msg("BUG! candidate is self-rejected: this node produced a block its own validator refuses")
}

func (r *sessionRuntime) logBlockValidated(
	candidate *simplex.Candidate,
	artifact *CandidateArtifact,
	elapsed time.Duration,
) {
	if r.log == nil {
		return
	}
	event := r.log.Info()
	if event == nil {
		return
	}

	windowSize := r.config.Protocol.SlotsPerLeaderWindow
	windowStart := candidate.ID.Slot / windowSize * windowSize
	event.
		Hex("session_id", r.config.SessionID[:]).
		Int32("workchain", candidate.Block.Workchain).
		Int64("shard", candidate.Block.Shard).
		Uint32("slot", candidate.ID.Slot).
		Uint32("leader", candidate.Leader).
		Uint32("window_start", windowStart).
		Uint32("window_end", windowStart+windowSize).
		Bool("is_empty", candidate.Empty).
		Uint32("block_seqno", candidate.Block.SeqNo).
		Hex("candidate_hash", candidate.ID.Hash[:]).
		Hex("block_root_hash", candidate.Block.RootHash).
		Hex("block_file_hash", candidate.Block.FileHash).
		Int("block_bytes", len(artifact.BlockBOC)).
		Int("collated_bytes", len(artifact.CollatedData)).
		Dur("elapsed", elapsed).
		Msg("block validated")
}

func (r *sessionRuntime) StoreCandidate(candidate *simplex.Candidate, done func(error)) {
	if !r.spawn(func(ctx context.Context) { done(r.candidates.store(ctx, candidate.ID)) }) {
		done(context.Canceled)
	}
}

func (r *sessionRuntime) HandleWindow(window simplex.Window) {
	r.workMu.Lock()
	active := r.workActive
	r.workMu.Unlock()
	if !active {
		r.fail(errors.New("validator runtime: leader window received while stopped"))

		return
	}
	r.noteObservedWindow(window.StartSlot)
	if window.LocalLeader {
		r.recordLeaderWindow(window)
	}

	// Simplex calls hooks from its single engine goroutine. Keep that callback
	// non-blocking, but preserve its order exactly: the C++ implementation gets
	// the same property from the consensus actor mailbox. Running one goroutine
	// per window would let a later session update prune candidate state still
	// needed by an earlier update.
	r.windowEventsMu.Lock()
	r.windowEvents = append(r.windowEvents, window)
	r.windowEventsMu.Unlock()
	select {
	case r.windowEventsWake <- struct{}{}:
	default:
	}
}

func (r *sessionRuntime) runWindowEvents(ctx context.Context) {
	defer r.clearWindowEvents()

	for {
		if window, ok := r.nextWindowEvent(); ok {
			r.handleWindow(ctx, window)
			if ctx.Err() != nil {
				return
			}

			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-r.windowEventsWake:
		}
	}
}

func (r *sessionRuntime) nextWindowEvent() (simplex.Window, bool) {
	r.windowEventsMu.Lock()
	defer r.windowEventsMu.Unlock()

	if r.windowEventsHead == len(r.windowEvents) {
		return simplex.Window{}, false
	}

	window := r.windowEvents[r.windowEventsHead]
	r.windowEvents[r.windowEventsHead] = simplex.Window{}
	r.windowEventsHead++
	if r.windowEventsHead == len(r.windowEvents) {
		r.windowEvents = r.windowEvents[:0]
		r.windowEventsHead = 0
	}

	return window, true
}

func (r *sessionRuntime) clearWindowEvents() {
	r.windowEventsMu.Lock()
	clear(r.windowEvents)
	r.windowEvents = nil
	r.windowEventsHead = 0
	r.windowEventsMu.Unlock()
}

func (r *sessionRuntime) recordLeaderWindow(window simplex.Window) {
	record := LeaderWindowRecord{
		Base:       window.Base,
		StartSlot:  window.StartSlot,
		EndSlot:    window.EndSlot,
		ObservedAt: window.ObservedAt,
	}

	r.leaderWindowRecordMu.Lock()
	r.leaderWindowRecordPending = record
	r.leaderWindowRecordSet = true
	r.leaderWindowRecordMu.Unlock()
	select {
	case r.leaderWindowRecordWake <- struct{}{}:
	default:
	}
}

func (r *sessionRuntime) runLeaderWindowRecords(ctx context.Context) {
	defer r.clearLeaderWindowRecord()

	for {
		if record, ok := r.nextLeaderWindowRecord(); ok {
			err := awaitStorageWrite(ctx, func(done func(error)) {
				r.storage.RecordLeaderWindow(r.config.StorageID, record, done)
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if r.log != nil {
					r.log.Warn().
						Err(err).
						Hex("session_id", r.config.SessionID[:]).
						Uint32("window_start", record.StartSlot).
						Msg("failed to record leader window telemetry")
				}
			}

			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-r.leaderWindowRecordWake:
		}
	}
}

func (r *sessionRuntime) nextLeaderWindowRecord() (LeaderWindowRecord, bool) {
	r.leaderWindowRecordMu.Lock()
	defer r.leaderWindowRecordMu.Unlock()

	if !r.leaderWindowRecordSet {
		return LeaderWindowRecord{}, false
	}
	record := r.leaderWindowRecordPending
	r.leaderWindowRecordPending = LeaderWindowRecord{}
	r.leaderWindowRecordSet = false

	return record, true
}

func (r *sessionRuntime) clearLeaderWindowRecord() {
	r.leaderWindowRecordMu.Lock()
	r.leaderWindowRecordPending = LeaderWindowRecord{}
	r.leaderWindowRecordSet = false
	r.leaderWindowRecordMu.Unlock()
}

func (r *sessionRuntime) handleWindow(ctx context.Context, window simplex.Window) {
	r.stateMu.RLock()
	targetRate := r.state.Params.TargetRate
	r.stateMu.RUnlock()
	windowCtx, finishWindow := r.beginWindowProgress(ctx, window, targetRate)
	defer finishWindow()
	if err := windowCtx.Err(); err != nil {
		return
	}

	base, err := r.resolveWindowBase(windowCtx, window)
	if err != nil {
		if windowCtx.Err() == nil {
			r.fail(fmt.Errorf("validator runtime: resolve leader window base: %w", err))
		}

		return
	}
	now := time.Now()
	startAt := now
	if !base.GenUtime.IsZero() {
		earliest := base.GenUtime.Add(targetRate)
		if earliest.After(startAt) {
			startAt = earliest
		}
		if latest := now.Add(targetRate); startAt.After(latest) {
			startAt = latest
		}
	}
	for {
		if r.observe != nil {
			progressStartAt := time.Time{}
			if window.ObservedSlot == window.StartSlot {
				progressStartAt = startAt
			}
			var baseState *ChainState
			if window.Base.Exists {
				baseState = base.State
			}
			progress := sessionConsensusProgress{
				Window:    window,
				StartAt:   progressStartAt,
				BaseState: baseState,
			}
			for {
				finalizedBlock := r.currentFinalizedBlock()
				lineageErr := r.verifyWindowAncestry(windowCtx, window, finalizedBlock)
				if lineageErr != nil {
					if windowCtx.Err() != nil {
						return
					}
					if errors.Is(lineageErr, errFinalizedLineageAhead) {
						// A restarted validator can have a locally applied block ahead of
						// the Simplex state restored from its session WAL. C++ keeps the
						// consensus actor alive and lets it learn the missing certificates;
						// block production for that stale view is merely not started.
						if r.log != nil {
							r.log.Debug().
								Hex("session_id", r.config.SessionID[:]).
								Uint32("window_start", window.StartSlot).
								Msg("skipping consensus window behind locally finalized state")
						}

						return
					}
					r.fail(fmt.Errorf("validator runtime: resolve consensus lineage: %w", lineageErr))

					return
				}

				progressErr := r.observeWindowProgress(windowCtx, progress, finalizedBlock)
				if progressErr == errWindowFinalizedChanged {
					continue
				}
				if progressErr != nil {
					if windowCtx.Err() == nil && r.log != nil {
						r.log.Warn().
							Err(progressErr).
							Hex("session_id", r.config.SessionID[:]).
							Uint32("window_start", window.StartSlot).
							Msg("local producer rejected consensus progress; skipping window")
					}

					return
				}

				break
			}
		}
		submitter := r.openWindow(window)
		err = r.backend.HandleLeaderWindow(windowCtx, LeaderWindow{
			Window:  window,
			StartAt: startAt,
			Submit:  submitter.submit,
		})
		if errors.Is(err, errLeaderWindowNeedsRecheck) {
			r.detachWindow(submitter, err)

			continue
		}
		if err != nil {
			submitter.close(err)
			if windowCtx.Err() == nil && r.log != nil {
				r.log.Warn().
					Err(err).
					Hex("session_id", r.config.SessionID[:]).
					Uint32("window_start", window.StartSlot).
					Msg("local producer rejected leader window; skipping window")
			}
		}

		return
	}
}

// noteObservedWindow advances the cancellation fence before the dispatcher
// starts work. It is called from the Simplex hook goroutine, while the actual
// progress work is deliberately serialized by runWindowEvents.
func (r *sessionRuntime) noteObservedWindow(startSlot uint32) {
	r.progressMu.Lock()
	defer r.progressMu.Unlock()

	if r.progressLatestSet && startSlot <= r.progressLatestStart {
		return
	}
	r.progressLatestSet = true
	r.progressLatestStart = startSlot
	if r.progressCancel != nil && r.progressRetrying && r.progressStart < startSlot {
		r.progressCancel()
	}
}

func (r *sessionRuntime) beginWindowProgress(
	ctx context.Context,
	window simplex.Window,
	targetRate time.Duration,
) (context.Context, func()) {
	deadline := windowProgressDeadline(window, targetRate)
	var cancel context.CancelFunc
	if deadline.IsZero() {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}

	r.progressMu.Lock()
	r.progressGeneration++
	generation := r.progressGeneration
	superseded := r.progressLatestSet && r.progressLatestStart > window.StartSlot
	if !superseded {
		r.progressStart = window.StartSlot
		r.progressRetrying = false
		r.progressCancel = cancel
	}
	r.progressMu.Unlock()
	if superseded {
		cancel()
	}

	return ctx, func() {
		r.progressMu.Lock()
		if r.progressGeneration == generation {
			r.progressCancel = nil
			r.progressRetrying = false
		}
		r.progressMu.Unlock()
		cancel()
	}
}

// windowProgressDeadline includes the grace the voter selected for this
// observation, which can exceed the configured baseline after skipped windows.
// Reading the current first-block timeout here would lose that association
// when window dispatch lags behind consensus.
func windowProgressDeadline(window simplex.Window, targetRate time.Duration) time.Time {
	if window.ObservedAt.IsZero() || targetRate <= 0 || window.EndSlot <= window.ObservedSlot {
		return time.Time{}
	}

	remainingSlots := uint64(window.EndSlot - window.ObservedSlot)
	if remainingSlots > uint64(time.Duration(1<<63-1)/targetRate) {
		return time.Time{}
	}
	return window.ObservedAt.Add(time.Duration(remainingSlots) * targetRate).Add(window.FirstBlockTimeout)
}

func (r *sessionRuntime) observeWindowProgress(
	ctx context.Context,
	progress sessionConsensusProgress,
	finalizedBlock *ton.BlockIDExt,
) error {
	for {
		r.lifecycleMu.Lock()
		if err := ctx.Err(); err != nil {
			r.lifecycleMu.Unlock()

			return err
		}
		if !r.finalizedBlockMatches(finalizedBlock) {
			r.lifecycleMu.Unlock()

			return errWindowFinalizedChanged
		}
		err := r.observe(ctx, progress)
		r.lifecycleMu.Unlock()
		if err == nil || !isTransientProgressError(err) {
			return err
		}
		r.markWindowProgressRetrying(progress.Window.StartSlot)
		if err = waitConsensusProgress(ctx, err); err != nil {
			return err
		}
	}
}

func (r *sessionRuntime) currentFinalizedBlock() *ton.BlockIDExt {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()

	if r.state.FinalizedBlock == nil {
		return nil
	}
	finalized := *r.state.FinalizedBlock.Copy()

	return &finalized
}

func (r *sessionRuntime) finalizedBlockMatches(finalizedBlock *ton.BlockIDExt) bool {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()

	current := r.state.FinalizedBlock
	if current == nil || finalizedBlock == nil {
		return current == nil && finalizedBlock == nil
	}

	return sameBlockID(*current, *finalizedBlock)
}

func (r *sessionRuntime) resolveWindowBase(
	ctx context.Context,
	window simplex.Window,
) (ResolvedState, error) {
	for {
		base, err := r.states.resolve(ctx, window.Base)
		if err == nil || !errors.Is(err, ErrBlockNotReady) {
			return base, err
		}
		r.markWindowProgressRetrying(window.StartSlot)
		if err = waitDuration(ctx, consensusProgressRetryInterval); err != nil {
			return ResolvedState{}, err
		}
	}
}

func (r *sessionRuntime) markWindowProgressRetrying(startSlot uint32) {
	r.progressMu.Lock()
	if r.progressCancel != nil && r.progressStart == startSlot {
		r.progressRetrying = true
		if r.progressLatestSet && r.progressLatestStart > startSlot {
			r.progressCancel()
		}
	}
	r.progressMu.Unlock()
}

func isTransientProgressError(err error) bool {
	return errors.Is(err, ErrBlockNotReady) || errors.Is(err, collator.ErrAcquisitionNotReady) ||
		errors.Is(err, collator.ErrSessionUpdateDeferred)
}

// consensusProgressReadiness is supplied by a transient refusal whose owner
// can signal when another attempt is useful. Waiting must happen outside the
// runtime lifecycle lock: controller reconciliation may need that lock itself.
type consensusProgressReadiness interface {
	WaitReady(context.Context) error
}

func waitConsensusProgress(ctx context.Context, cause error) error {
	var readiness consensusProgressReadiness
	if errors.As(cause, &readiness) {
		return readiness.WaitReady(ctx)
	}

	// Incomplete state/acquisition and deferred production updates have no
	// readiness signal. Their existing bounded retry remains necessary.
	return waitDuration(ctx, consensusProgressRetryInterval)
}

func (r *sessionRuntime) verifyWindowAncestry(
	ctx context.Context,
	window simplex.Window,
	finalizedBlock *ton.BlockIDExt,
) error {
	loggedNotReady := false
	started := time.Now()
	for {
		walk, err := r.states.ancestry(ctx, window.Base, finalizedBlock)
		if err == nil {
			r.observeLineageWalk(LineageWalkSuccess, walk, started)

			return nil
		}
		if !errors.Is(err, ErrBlockNotReady) {
			r.observeLineageWalk(LineageWalkFailure, walk, started)

			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.markWindowProgressRetrying(window.StartSlot)
		if !loggedNotReady && r.log != nil {
			r.log.Debug().
				Err(err).
				Hex("session_id", r.config.SessionID[:]).
				Uint32("window_start", window.StartSlot).
				Msg("consensus window is waiting for an ancestry candidate")
			loggedNotReady = true
		}

		// The parent link may arrive after the window notification. Keep this
		// ordered worker pending; session retirement cancels the wait.
		if err = waitDuration(ctx, consensusProgressRetryInterval); err != nil {
			return err
		}
	}
}

// observeLineageWalk reports one completed walk. The retry loop above waits for
// a state rather than walking again, so a walk is observed once per attempt that
// reached a verdict, with the wait it ended on included in its duration: the
// wait is what the walk costs.
//
// A walk with Visited == 0 is reported too, and that is the point of this
// function rather than an edge case it tolerates. This used to return early on
// it, which blinded the instrumentation to precisely the failure it was added
// for: a walk that finds the session finalized ahead of the base it was handed
// descends nothing, fails immediately, and therefore had Visited == 0 every
// time. Zero visits in a bounded duration under LineageWalkFailure is the
// signature of that shape; dropping the sample left it indistinguishable from a
// window that never walked at all.
func (r *sessionRuntime) observeLineageWalk(
	result LineageWalkResult,
	walk lineageWalkStats,
	started time.Time,
) {
	if r.metrics == nil {
		return
	}

	r.metrics.ObserveLineageWalk(LineageWalkObservation{
		Chain:      r.validationChain(),
		Result:     result,
		Candidates: walk.Visited,
		Duration:   time.Since(started),
		Steps:      walk.Steps,
	})
}

// ownSlotState is one published candidate of ours awaiting its verdict.
type ownSlotState struct {
	publishedAt time.Time
	// slotStartAt is the instant the committee's ladder gives this slot:
	// window observation plus one target rate per slot of offset. Every voter
	// arms its skip alarm off exactly this anchor (voterWindowObserved), so a
	// notarization measured from here is the share of the slot's real budget
	// this block consumed — the one number that says whether we fit. Zero when
	// the window was not readable at publication, and then only the
	// publish-relative measurement is reported.
	slotStartAt time.Time
	notarized   bool
}

// ownSlotsRetained bounds the map against a session that publishes far ahead of
// finalization. The sweep below normally keeps it at a window's worth; this is
// the backstop for the case where finalization stops entirely, and it drops the
// oldest entries because those are the ones consensus has already left behind.
const ownSlotsRetained = 256

// noteOwnCandidatePublished stamps the instant one of our candidates went out.
func (r *sessionRuntime) noteOwnCandidatePublished(slot uint32, at time.Time) {
	r.ownSlotsMu.Lock()
	defer r.ownSlotsMu.Unlock()

	if r.ownSlots == nil {
		r.ownSlots = make(map[uint32]ownSlotState)
	}
	if len(r.ownSlots) >= ownSlotsRetained {
		oldest := slot
		for candidate := range r.ownSlots {
			if candidate < oldest {
				oldest = candidate
			}
		}
		delete(r.ownSlots, oldest)
	}
	r.ownSlots[slot] = ownSlotState{publishedAt: at, slotStartAt: r.slotLadderStart(slot)}
}

// slotLadderStart is where the committee's clock puts this slot: the instant
// the leader window was observed, plus one target rate per slot into it. It is
// the same arithmetic the voter does, and it is deliberately anchored on the
// window observation rather than on the producer's own schedule, because the
// deadline this is measured against is the voters' and not ours.
func (r *sessionRuntime) slotLadderStart(slot uint32) time.Time {
	r.windowMu.Lock()
	submitter := r.window
	r.windowMu.Unlock()
	if submitter == nil {
		return time.Time{}
	}
	// Read without s.mu: this runs inside publishCandidate, which submit calls
	// with s.mu already held by this very goroutine, and Go mutexes are not
	// reentrant — taking it here deadlocks the session outright, and with it
	// the engine goroutine that later wants ownSlotsMu to report the
	// notarization. Observed on the test stand: every inbound candidate piled
	// up behind the stalled engine (929 goroutines in PrecheckCandidateBroadcast)
	// and the node stopped validating a minute after start.
	//
	// The field is safe to read: a submitter's window is assigned once, before
	// the submitter is installed under windowMu, and submit and close only ever
	// write nextSlot, parent, accepted, runtime and err.
	window := submitter.window
	if window.ObservedAt.IsZero() || slot < window.StartSlot {
		return time.Time{}
	}
	r.stateMu.RLock()
	targetRate := r.state.Params.TargetRate
	r.stateMu.RUnlock()
	if targetRate <= 0 {
		return time.Time{}
	}

	return window.ObservedAt.Add(time.Duration(slot-window.StartSlot) * targetRate)
}

// observeOwnNotarization reports the round trip for one of our own slots and
// marks it as having made it. A candidate that is not ours is a map miss.
func (r *sessionRuntime) observeOwnNotarization(slot uint32, at time.Time) {
	if r.metrics == nil {
		return
	}
	r.ownSlotsMu.Lock()
	state, ours := r.ownSlots[slot]
	if ours && !state.notarized {
		state.notarized = true
		r.ownSlots[slot] = state
	}
	r.ownSlotsMu.Unlock()
	if !ours || state.publishedAt.IsZero() {
		return
	}
	chain := r.metricChain()
	r.metrics.ObserveOwnSlot(chain, OwnSlotNotarized)
	r.metrics.ObserveOwnNotarization(chain, at.Sub(state.publishedAt))
	if !state.slotStartAt.IsZero() {
		r.metrics.ObserveOwnSlotNotarization(chain, at.Sub(state.slotStartAt))
	}
}

// observeOwnFinalization reports the finalization round trip for one of our
// slots and retires every earlier slot of ours the chain has now passed. A
// slot consensus moved past without notarizing is one this node built,
// delivered and lost.
func (r *sessionRuntime) observeOwnFinalization(slot uint32, at time.Time) {
	if r.metrics == nil {
		return
	}
	r.ownSlotsMu.Lock()
	state, ours := r.ownSlots[slot]
	delete(r.ownSlots, slot)
	lost := 0
	for candidate, passed := range r.ownSlots {
		if candidate >= slot {
			continue
		}
		delete(r.ownSlots, candidate)
		if !passed.notarized {
			lost++
		}
	}
	r.ownSlotsMu.Unlock()

	chain := r.metricChain()
	for range lost {
		r.metrics.ObserveOwnSlot(chain, OwnSlotLost)
	}
	if ours && !state.publishedAt.IsZero() {
		r.metrics.ObserveOwnFinalization(chain, at.Sub(state.publishedAt))
	}
}

func (r *sessionRuntime) metricChain() collator.MetricChain {
	if r.config.Shard.IsMasterchain() {
		return collator.MetricChainMasterchain
	}

	return collator.MetricChainShardchain
}

func (r *sessionRuntime) OnNotarized(id simplex.CandidateID, certificate simplex.VerifiedCertificate) {
	r.candidates.observeNotarization(id, certificate)
	r.observeOwnNotarization(id.Slot, time.Now())
	if r.notarized != nil {
		// A map update on the collator's side; nothing here waits on it.
		r.notarized(id, time.Now())
	}
	// The backstop, and deliberately not the main path. For the candidate a
	// window opens ON this buys nothing: the certificate that notarizes it
	// announces that window in the same engine turn, so this resolve and the
	// window's own join one single flight. What it does warm is every earlier
	// slot of the window — notarized while the window still runs — which is what
	// leaves one apply at the window boundary instead of the whole chain, and a
	// candidate whose payload never reached this node at all, which the resolver
	// then fetches from a peer instead of at the window start.
	r.warmCandidateState(id)
}

// warmCandidateState resolves, in the background, the state a candidate
// produces — the state a leader window opening on that candidate needs.
//
// It exists because an observer session never validates. A validator resolves
// and applies every candidate it votes on, so by the time a window opens on one
// its state is already resident and resolveWindowBase is a cache hit. An
// observer — which is what a standalone collator runs — reaches the window with
// nothing resolved and pays the whole chain of applies from the last finalized
// block on the critical path, at the exact instant its first block is due.
//
// It is deliberately not done on a validator. stateResolver.resolve is
// single-flight, and rememberValidatedState does not replace a flight that has
// already finished; a background resolve that won that race would leave the
// session holding a different materialization of the state validation just
// built, and chain_state.go compares tip states by pointer, so every later
// candidate would silently pay a full re-apply.
//
// The context is the session's own, never a caller's: the last waiter to leave
// an unfinished flight cancels it, so a warm-up bounded by a shorter lifetime
// could take down work a foreground resolve is about to join. The flight's own
// TTL and session shutdown bound it instead.
func (r *sessionRuntime) warmCandidateState(id simplex.CandidateID) {
	r.warmState(id, true)
}

// warmPublishedCandidateState warms the state of a block this node produced
// itself, without betting on it.
//
// A standalone collator's own candidate never reaches the receive path — it is
// broadcast from here — so without this the one window entry that matters most
// is the coldest: the collator that leads two windows in a row would resolve and
// apply its own last block at the instant the next window's first block is due,
// which is exactly the block the cross-window handoff already built and parked.
//
// It places no bet, and that is not an omission. The producer that built this
// block has already parked a successor for the next window under this very
// candidate (promoteNextWindowBet); a second bet would either be refused as a
// duplicate or race the first one out of the slot.
func (r *sessionRuntime) warmPublishedCandidateState(id simplex.CandidateID) {
	r.warmState(id, false)
}

func (r *sessionRuntime) warmState(id simplex.CandidateID, offerBet bool) {
	if r.config.Identity.Validator != nil {
		return
	}

	r.warmingMu.Lock()
	if _, warming := r.warming[id]; warming {
		r.warmingMu.Unlock()

		return
	}
	r.warming[id] = struct{}{}
	r.warmingMu.Unlock()

	if !r.spawn(func(ctx context.Context) {
		defer func() {
			r.warmingMu.Lock()
			delete(r.warming, id)
			r.warmingMu.Unlock()
		}()

		// What this call leaves behind is the resolver's own cached flight,
		// which is what the window start then finds; an error here is one the
		// window start reports for itself.
		resolved, err := r.states.resolve(ctx, simplex.Parent(id))
		if err == nil && offerBet {
			r.offerSpeculativeWindow(ctx, id, resolved)
		}
		// Last, and deliberately: everything above is what a window opening in
		// the next few hundred milliseconds waits for, and this is not.
		r.persistNotarizedCandidate(id)
	}) {
		r.warmingMu.Lock()
		delete(r.warming, id)
		r.warmingMu.Unlock()
	}
}

// persistNotarizedCandidate submits the durable write of a notarized candidate
// on an observer session.
//
// A validator has already written it: its voter stores every candidate before
// voting, so the write finalization asks for is long done and joins a finished
// flight. An observer never votes and never runs that hook, so nothing wrote
// it, and the whole serialize-compress-submit lands inside finalizeInner —
// between a finalization and the block acceptance that follows it. Submitting
// it here keeps the same durability, which a peerless restart depends on, with
// the cost on a background goroutine instead.
//
// It is submitted, not joined. A failed write has no voter to report to, so it
// is logged and the finalization's own store reports it for real.
func (r *sessionRuntime) persistNotarizedCandidate(id simplex.CandidateID) {
	if r.config.Identity.Validator != nil {
		return
	}
	// A candidate that never gathers a quorum never finalizes, so nothing will
	// ever ask for it durably. The warm-up reaches here on that path too, when
	// it resolved a candidate whose payload had to come from a peer and the
	// resolve failed.
	if notarized, _ := r.candidates.localHalves(id); !notarized {
		return
	}
	if _, err := r.candidates.storeAsync(id, func(storeErr error) {
		if storeErr == nil {
			return
		}
		r.log.Debug().Err(storeErr).Uint32("slot", id.Slot).
			Msg("notarized candidate was not persisted ahead of finalization")
	}); err != nil {
		r.log.Debug().Err(err).Uint32("slot", id.Slot).
			Msg("notarized candidate was not submitted for persistence")
	}
}

// offerSpeculativeWindow bets that the window about to open will open on the
// candidate whose state was just resolved, and offers that bet to whatever
// produces blocks for this session.
//
// Only the last slot of a window qualifies: it is the candidate whose
// certificate opens the next window and therefore the base that window carries
// unless the network skips it. Everything else this needs is derived here
// rather than asked for — the leader from the session's own round-robin
// schedule, the schedule of the window to come from the base's generation time
// — because an observer has no window of its own to read them from.
//
// Whether the bet may be taken at all is not decided here. The producer knows:
// a standalone collator holds a delegation for that window or it does not, and
// it refuses the offer when it does not.
func (r *sessionRuntime) offerSpeculativeWindow(
	ctx context.Context,
	id simplex.CandidateID,
	resolved ResolvedState,
) {
	if r.speculate == nil || r.config.Shard.IsMasterchain() {
		return
	}
	if resolved.State == nil || len(resolved.State.tips) != 1 {
		return
	}
	windowSize := r.config.Protocol.SlotsPerLeaderWindow
	validators := uint32(len(r.config.Validators))
	if windowSize == 0 || validators == 0 || id.Slot == math.MaxUint32 {
		return
	}
	nextStart := id.Slot + 1
	if nextStart%windowSize != 0 {
		return
	}
	r.stateMu.RLock()
	targetRate := r.state.Params.TargetRate
	r.stateMu.RUnlock()
	if targetRate <= 0 {
		return
	}

	// The instant the observed window would compute, by the rule handleWindow
	// uses: the parent's generation time plus one target rate, never earlier
	// than now and never more than one rate ahead. It stamps the block's header
	// and nothing else; the producer's schedule still comes from the window when
	// it is really observed.
	now := time.Now()
	startAt := now
	if !resolved.GenUtime.IsZero() {
		if earliest := resolved.GenUtime.Add(targetRate); earliest.After(startAt) {
			startAt = earliest
		}
		if latest := now.Add(targetRate); startAt.After(latest) {
			startAt = latest
		}
	}

	if err := r.speculate(ctx, sessionSpeculativeWindow{
		StartSlot: nextStart,
		Leader:    r.codec.schedule.ExpectedLeader(nextStart),
		Base:      id,
		BaseState: resolved.State,
		StartAt:   startAt,
		// Three target rates bounds a bet nobody comes to collect: longer than
		// any observed gap between this estimate and the window it predicts, and
		// short enough that a bet lost to a stalled session dies within a window.
		Deadline: startAt.Add(3 * targetRate),
	}); err != nil && r.log != nil {
		r.log.Debug().
			Err(err).
			Hex("session_id", r.config.SessionID[:]).
			Uint32("window_start", nextStart).
			Msg("speculative window was not started")
	}
}

func (r *sessionRuntime) OnFinalized(id simplex.CandidateID, certificate simplex.VerifiedCertificate) {
	// Simplex maintains this watermark for its own slot map; the resolver
	// caches are the only session-scoped structures that never saw it. The
	// candidate sweep runs before finalization is spawned below, which then walks
	// back over parents it may have just released; that order is safe only
	// because a released payload is either durable or one consensus never
	// accepted.
	//
	// Candidate payloads run under the ancestry floor. The state resolver reports
	// how far the latest guarded walk reached, while resolved and applied state
	// trees use their fixed parent-resolution margins.
	//
	// What bounds that request is measured first, off the candidate resolver's
	// own retained payloads, and handed to the state resolver: the two never
	// nest their locks, and the bound is a memory budget rather than a slot
	// distance.
	r.observeOwnFinalization(id.Slot, time.Now())
	budgetFloor := r.candidates.retentionCapFloor(id.Slot)
	floor := r.states.notifyFinalized(id.Slot, budgetFloor)
	candidates := r.candidates.notifyFinalized(id.Slot, floor.Slot)
	r.publishCandidateCache(candidates)
	r.noteRetentionFloorCapped(id.Slot, floor, candidates)
	r.logResolverCaches(id.Slot, candidates)

	if !r.spawn(func(ctx context.Context) {
		if err := r.states.finalize(ctx, id, certificate); err != nil {
			// C++ runs StateResolver::finalize_blocks as a detached task. Local
			// persistence or shard-top construction may fail while the node is
			// catching up, but that must not stop voting in an otherwise healthy
			// consensus session; the ordinary block sync path can still ingest it.
			if r.log != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrResolverClosed) {
				r.log.Warn().
					Err(err).
					Hex("session_id", r.config.SessionID[:]).
					Uint32("slot", id.Slot).
					Msg("finalized block acceptance failed; consensus continues")
			}
		}
	}) {
		r.fail(errors.New("validator runtime: finalization received while stopped"))
	}
}

const (
	// consensusProgressRetryInterval keeps a locally incomplete state or
	// acquisition on the same leader window without consuming most of a
	// short block interval. It is only reached after a typed local-readiness
	// error and its wait is cancelled at the next observed window boundary.
	consensusProgressRetryInterval = 20 * time.Millisecond
)

// ConsensusStats reports this session's consensus position for the
// process-wide collector. It is called from a Prometheus scrape, so it reads
// the Simplex counters through the runner's published snapshot and never posts
// onto the consensus loop: a scrape must not be able to block behind the
// goroutine whose stall it is being scraped to explain.
// The retained payloads and the lineage anchor are read from the resolvers
// themselves rather than from what the last finalization published. A session
// that has stopped finalizing keeps accepting candidates and keeps walking a
// lineage per leader window, so both of those numbers move; a projection
// refreshed only by OnFinalized would hold its pre-standstill values for the
// whole standstill, which is the one interval these gauges exist for. Capped is
// the exception and stays as the last sweep left it, because it is a statement
// about a sweep and no sweep has run since.
func (r *sessionRuntime) ConsensusStats() ConsensusSessionStats {
	r.candidateCacheMu.Lock()
	capped := r.retentionCapped
	r.candidateCacheMu.Unlock()

	cache := r.candidates.cacheProjection()
	anchor, anchorKnown := r.states.lineageAnchor()

	return ConsensusSessionStats{
		Chain: r.validationChain(),
		Stats: r.runner.StatsSnapshot(),
		Retention: ConsensusRetentionStats{
			AnchorSlot:       anchor,
			AnchorKnown:      anchorKnown,
			Capped:           capped,
			BudgetBytes:      r.candidates.retentionBudgetBytes(),
			RetainedPayloads: cache.Candidates,
			RetainedBytes:    cache.Bytes,
		},
	}
}

// publishCandidateCache reports this session's own change to the process-wide
// candidate cache gauges.
func (r *sessionRuntime) publishCandidateCache(stats candidateCacheStats) {
	if r.metrics == nil {
		return
	}

	r.candidateCacheMu.Lock()
	previous := r.candidateCache
	r.candidateCache = stats
	r.candidateCacheMu.Unlock()

	r.metrics.AddCandidateCache(r.validationChain(), CandidateCacheDelta{
		Retained: int64(stats.Candidates - previous.Candidates),
		Released: int64((stats.Entries - stats.Candidates) - (previous.Entries - previous.Candidates)),
		Bytes:    stats.Bytes - previous.Bytes,
	})
}

// noteRetentionFloorCapped reports the sweeps pruning past what the local
// producer asked them to keep. That happens when the payloads between the
// producer's anchor and the finalized slot no longer fit this session's
// retention budget — so it is not an error, but it is the point where the
// lineage walk starts paying storage reads again and it must not be invisible.
//
// The wording is deliberately about memory and not about how far behind this
// node is. The previous line said "local block production is too far behind
// consensus", which was drawn from a slot distance that skipped slots inflated
// on their own; in the field it fired a median of 94 s after the fault it was
// read as announcing, and it sent an investigation to the wrong subsystem.
//
// The counter ticks for every finalization spent in that state, which is what
// makes the condition a rate rather than an event. The log line marks the
// transitions only.
func (r *sessionRuntime) noteRetentionFloorCapped(
	slot uint32,
	floor retentionFloor,
	candidates candidateCacheStats,
) {
	if r.metrics != nil && floor.Capped {
		r.metrics.AddCandidateRetentionCapped(r.validationChain())
	}

	r.candidateCacheMu.Lock()
	changed := r.retentionCapped != floor.Capped
	r.retentionCapped = floor.Capped
	r.candidateCacheMu.Unlock()
	if !changed || r.log == nil {
		return
	}
	if !floor.Capped {
		r.log.Info().
			Hex("session_id", r.config.SessionID[:]).
			Uint32("slot", slot).
			Msg("consensus candidate retention follows the local producer again")

		return
	}
	r.log.Warn().
		Hex("session_id", r.config.SessionID[:]).
		Uint32("slot", slot).
		Uint32("retention_floor", floor.Slot).
		Uint32("lineage_anchor", floor.Anchor).
		Int("retained_payloads", candidates.Candidates).
		Int64("retained_bytes", candidates.Bytes).
		Msg("consensus candidate retention reached its memory budget and resumed pruning below " +
			"the lineage the local producer asked to keep; lineage walks will read from storage")
}

// resolverCacheLogPeriod spaces the cache projection out over roughly a minute
// of this session's own slots. Both snapshots walk their whole map, so this
// stays off the per-slot path and out of the way unless debug logging is on.
func (r *sessionRuntime) resolverCacheLogPeriod() uint32 {
	r.stateMu.RLock()
	params := r.state.Params
	r.stateMu.RUnlock()

	return resolverCacheLogPeriod(params.TargetRate)
}

// logResolverCaches reports what the session-scoped resolver caches retain.
// The candidate projection is the one the finalization sweep just produced, so
// this costs no second walk of that map; it reports what survived the sweep.
func (r *sessionRuntime) logResolverCaches(slot uint32, candidates candidateCacheStats) {
	if r.log == nil || r.log.GetLevel() > zerolog.DebugLevel || slot%r.resolverCacheLogPeriod() != 0 {
		return
	}

	states := r.states.cacheStats()
	r.log.Debug().
		Hex("session_id", r.config.SessionID[:]).
		Uint32("slot", slot).
		Int("candidate_entries", candidates.Entries).
		Int("candidates_retained", candidates.Candidates).
		Int("candidates_stored", candidates.Stored).
		Int64("candidate_bytes", candidates.Bytes).
		Int("state_flights", states.States).
		Int("states_retained", states.Resolved).
		Int("states_finalized", states.Finalized).
		Int("states_applied", states.AppliedStates).
		// Named for what it counts. The retained block payloads are a floor on
		// the footprint of these tips, not a measure of it: the state roots and
		// the parsed block DAGs beside them are larger and cannot be sized
		// without a walk this log line has no business doing.
		Int64("state_block_boc_bytes", states.BlockBOCBytes).
		Msg("consensus session resolver caches")
}

func (r *sessionRuntime) OnMisbehavior(validator uint32, misbehavior simplex.Misbehavior) {
	if !r.spawn(func(ctx context.Context) {
		if err := r.backend.HandleMisbehavior(ctx, validator, misbehavior); err != nil {
			r.fail(fmt.Errorf("validator runtime: handle misbehavior: %w", err))
		}
	}) {
		r.fail(errors.New("validator runtime: misbehavior received while stopped"))
	}
}

func (r *sessionRuntime) OnFatal(err error) {
	r.fail(err)
}

func (r *sessionRuntime) openWindow(window simplex.Window) *windowSubmitter {
	submitter := &windowSubmitter{
		runtime:  r,
		window:   window,
		nextSlot: window.StartSlot,
		parent:   window.Base,
	}
	if window.ObservedSlot != window.StartSlot {
		submitter.close(ErrLeaderWindowClosed)

		return submitter
	}

	r.windowMu.Lock()
	previous := r.window
	if previous != nil && previous.window.StartSlot >= window.StartSlot {
		r.windowMu.Unlock()
		submitter.close(ErrLeaderWindowClosed)

		return submitter
	}
	r.window = submitter
	r.windowMu.Unlock()
	if previous != nil {
		previous.close(ErrLeaderWindowClosed)
	}

	return submitter
}

func (r *sessionRuntime) closeWindow(err error) {
	r.windowMu.Lock()
	window := r.window
	r.window = nil
	r.windowMu.Unlock()
	if window != nil {
		window.close(err)
	}
}

// detachWindow clears one rechecked opening without disturbing a newer window
// which may already have replaced it. The same StartSlot can then install a
// fresh submitter after the runtime revalidates its finalized anchor.
func (r *sessionRuntime) detachWindow(submitter *windowSubmitter, err error) {
	r.windowMu.Lock()
	if r.window == submitter {
		r.window = nil
	}
	r.windowMu.Unlock()

	submitter.close(err)
}

type windowSubmitter struct {
	mu       sync.Mutex
	runtime  *sessionRuntime
	window   simplex.Window
	nextSlot uint32
	parent   simplex.ParentID
	accepted map[uint32]simplex.CandidateID
	err      error
}

func (s *windowSubmitter) submit(ctx context.Context, artifact *CandidateArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := &artifact.Candidate
	if s.runtime == nil {
		if s.err != nil {
			return s.err
		}

		return ErrLeaderWindowClosed
	}
	if accepted, exists := s.accepted[candidate.ID.Slot]; exists {
		if accepted == candidate.ID {
			// A local collator persists before delivery and retries its whole
			// window after a later transient build failure. The same candidate
			// must therefore be harmless when it is replayed into this already
			// advanced Simplex submitter.
			return nil
		}

		return errors.New("validator runtime: different candidate reuses an accepted leader slot")
	}
	if candidate.ID.Slot != s.nextSlot || candidate.ID.Slot >= s.window.EndSlot {
		return errors.New("validator runtime: candidate slot is outside leader window order")
	}
	if candidate.Leader != s.window.Leader {
		return errors.New("validator runtime: candidate belongs to another leader")
	}
	if candidate.Parent != s.parent {
		return errors.New("validator runtime: candidate does not extend leader window chain")
	}

	runtime := s.runtime
	err := runtime.publishCandidate(ctx, artifact)
	if err != nil {
		return err
	}

	s.parent = simplex.Parent(candidate.ID)
	if s.accepted == nil {
		s.accepted = make(map[uint32]simplex.CandidateID, s.window.EndSlot-s.window.StartSlot)
	}
	s.accepted[candidate.ID.Slot] = candidate.ID
	s.nextSlot++
	if s.nextSlot == s.window.EndSlot {
		s.runtime = nil
		s.err = ErrLeaderWindowClosed
	}

	return nil
}

// publishCandidate hands one locally produced candidate to the network, to the
// resolver and to Simplex. Everything here is synchronous except the wait on
// the durable write: the record is submitted to the store from this goroutine,
// at this point in the sequence, and only the fsync is left behind.
//
// The wait belongs to the voter, not to the producer. Simplex enqueues its own
// StoreCandidate hook for every accepted candidate including our own
// (simplex/voter.go:216), joins this same flight, and gates the notarize vote on
// it — so nothing irreversible happens before the wire is readable. That is
// exactly where the reference keeps the wait: the C++ producer detaches its
// store (collator-producer.cpp:81) and consensus co_awaits it on the voting path
// (consensus.cpp:253).
//
// The submission itself must stay here. An observer runtime — the standalone or
// delegated collator composition — has no voter at all (Engine.SubmitCandidate
// returns early when e.voter is nil), so this call is the only durable write of
// its own candidate wire that ever happens. Nothing downstream ever looks at its
// outcome, so reportCandidatePersistFailure is where a failure goes: a counter
// to alert on and, on that runtime, an Error naming what was lost.
func (r *sessionRuntime) publishCandidate(ctx context.Context, artifact *CandidateArtifact) error {
	return r.execute(ctx, func(commandCtx context.Context) error {
		var err error
		artifact, err = artifact.withGenerationTime()
		if err != nil {
			return err
		}
		wire, broadcast, err := r.codec.encodeForBroadcast(artifact)
		if err != nil {
			return err
		}
		// CandidateGenerated is published before CandidateReceived. The network
		// sees the bare candidate plus BroadcastExtra, while the resolver stores
		// the consensus.candidate representation used by DB and request paths.
		broadcastErr := r.network.BroadcastCandidate(commandCtx, broadcast, *artifact)
		if broadcastErr != nil && r.log != nil {
			r.log.Warn().
				Err(broadcastErr).
				Hex("session_id", r.config.SessionID[:]).
				Uint32("slot", artifact.Candidate.ID.Slot).
				Msg("candidate broadcast handoff failed; continuing local consensus")
		}
		if err = r.candidates.stage(artifact, wire); err != nil {
			return err
		}
		// Staged, so the resolve below reads it from this session rather than
		// asking a peer for a block this node just made.
		r.warmPublishedCandidateState(artifact.Candidate.ID)
		slot := artifact.Candidate.ID.Slot
		if _, err = r.candidates.storeAsync(artifact.Candidate.ID, func(storeErr error) {
			if storeErr == nil {
				return
			}
			r.reportCandidatePersistFailure(slot, storeErr)
		}); err != nil {
			return err
		}

		r.noteOwnCandidatePublished(artifact.Candidate.ID.Slot, time.Now())

		return r.runner.SubmitCandidate(&artifact.Candidate)
	})
}

// reportCandidatePersistFailure is where a failed durable write of our own
// candidate ends up. It runs on the store's callback goroutine.
//
// On a voting runtime the voter joins the same flight and gates its notarize
// vote on it, so this is a second report of a failure consensus already acts
// on. On an observer runtime — the delegated and standalone collator
// composition — Engine.SubmitCandidate returns before acceptCandidate and there
// is no voter, so this write is the only durable copy of a candidate that has
// already been broadcast, and this is the only place its failure is ever
// mentioned. It is reported at Error there for that reason, and the counter is
// what an operator can alert on.
//
// It does not retry. The only durable path is the store's own write FIFO, and
// re-entering it from inside its completion callback is how that FIFO
// deadlocks; its failures are a closed store, a conflicting record, or the
// device, none of which a retry from here resolves.
func (r *sessionRuntime) reportCandidatePersistFailure(slot uint32, err error) {
	if r.metrics != nil {
		r.metrics.AddCandidatePersistFailure(r.validationChain())
	}
	if r.log == nil {
		return
	}
	if r.config.Identity.Validator != nil {
		r.log.Warn().
			Err(err).
			Hex("session_id", r.config.SessionID[:]).
			Uint32("slot", slot).
			Msg("durable write of our own candidate failed; the notarize vote for it will fail too")

		return
	}
	r.log.Error().
		Err(err).
		Hex("session_id", r.config.SessionID[:]).
		Uint32("slot", slot).
		Msg("durable write of our own candidate failed on a runtime with no voter; the candidate was broadcast but is not recoverable from this node")
}

func (s *windowSubmitter) close(err error) {
	s.mu.Lock()
	if s.runtime != nil {
		s.runtime = nil
		s.err = err
	}
	s.mu.Unlock()
}

func cloneSessionState(state SessionState) SessionState {
	state.MasterchainBlock = *state.MasterchainBlock.Copy()
	state.Registered = append([]groups.ShardDescription(nil), state.Registered...)
	if state.FinalizedBlock != nil {
		finalized := *state.FinalizedBlock.Copy()
		state.FinalizedBlock = &finalized
	}

	return state
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	if deadline.IsZero() || !deadline.After(time.Now()) {
		return nil
	}

	return waitDuration(ctx, time.Until(deadline))
}

func waitDuration(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
