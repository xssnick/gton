package validator

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
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
	ErrSessionRuntimeStarted = errors.New("validator runtime: session already started")
	ErrSessionRuntimeClosed  = errors.New("validator runtime: session closed")
	ErrLeaderWindowClosed    = errors.New("validator runtime: leader window closed")
)

// SessionReceiver is the inbound half of one private-overlay session. Network
// implementations call it only while Run is active and must join their own
// receiver goroutines before Run returns.
type SessionReceiver interface {
	ReceiveConsensusMessage(source simplex.PeerID, sourceValidator int, data []byte)
	PrecheckCandidateBroadcast(slot uint32, broadcastID [32]byte, signatureChecked bool) error
	// ReceiveCandidate verifies and installs one private-overlay candidate. A
	// successful return transfers an immutable decoded view to the transport so
	// it can relay the non-empty block into public/custom overlays without a
	// second candidate decompression pass.
	ReceiveCandidate(ctx context.Context, expectedSlot uint32, wire []byte) (CandidateArtifact, error)
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
	Window             simplex.Window
	StartAt            time.Time
	FinalizedAnchor    *ton.BlockIDExt
	AppliedAnchorState *ChainState
	Candidates         []*CandidateArtifact
}

type consensusProgressObserver func(context.Context, sessionConsensusProgress) error

type consensusProgressObserverBackend interface {
	ObserveConsensusProgress(context.Context, sessionConsensusProgress) error
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

	observeConsensusProgress consensusProgressObserver
}

type sessionRuntimePhase uint8

const (
	sessionRuntimePrepared sessionRuntimePhase = iota
	sessionRuntimeRunning
	sessionRuntimeStopped
	sessionRuntimeClosed
)

type sessionRuntime struct {
	config  SessionConfig
	storage ValidatorStorage
	network SessionNetwork
	backend SessionBackend
	codec   *candidateCodec
	observe consensusProgressObserver
	metrics ValidationObserver
	log     *zerolog.Logger

	candidates *candidateResolver
	states     *stateResolver
	engine     *simplex.Engine
	runner     *simplex.Runner

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
		Session:    config.StorageID,
		SessionID:  config.SessionID,
		Storage:    options.Storage,
		Provider:   options.Network,
		Codec:      codec,
		Validators: validators,
		PeerCount:  len(config.OverlayMembers),
		Limits:     options.Limits,
		Params:     params,
		Stored:     stored,
		Bootstrap:  bootstrap,
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
		metrics:                options.Observer,
		log:                    options.Logger,
		candidates:             resolver,
		state:                  cloneSessionState(initial),
		fatal:                  make(chan error, 1),
		phase:                  sessionRuntimePrepared,
		windowEventsWake:       make(chan struct{}, 1),
		leaderWindowRecordWake: make(chan struct{}, 1),
	}
	if runtime.observe == nil {
		if observer, ok := options.Backend.(consensusProgressObserverBackend); ok {
			runtime.observe = observer.ObserveConsensusProgress
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

	if err := r.backend.ActivateSession(runCtx, start); err != nil {
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}
		return fmt.Errorf("validator runtime: activate session backend: %w", err)
	}
	if err := r.states.start(runCtx, start); err != nil {
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}
		return err
	}
	r.startWork(runCtx)
	if !r.spawn(r.runLeaderWindowRecords) {
		return errors.New("validator runtime: start leader window recorder")
	}
	if !r.spawn(r.runWindowEvents) {
		return errors.New("validator runtime: start consensus window dispatcher")
	}
	if err := r.network.Start(runCtx, r); err != nil {
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}
		return fmt.Errorf("validator runtime: start network: %w", err)
	}
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
	wire []byte,
) (received CandidateArtifact, err error) {
	err = r.execute(ctx, func(context.Context) error {
		// decodeCanonical derives the wire from the roots it just parsed, so the
		// signature is verified once and neither BOC is parsed a second time.
		artifact, canonical, err := r.codec.decodeCanonical(wire, nil)
		if err != nil {
			return err
		}
		if artifact.Candidate.ID.Slot != expectedSlot {
			return errors.New("validator runtime: candidate broadcast slot mismatch")
		}
		if err = r.candidates.stage(artifact, canonical); err != nil {
			return err
		}

		if err = r.runner.SubmitCandidate(&artifact.Candidate); err != nil {
			return err
		}
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
	var finalizedBlock *ton.BlockIDExt
	if r.observe != nil && r.state.FinalizedBlock != nil {
		value := *r.state.FinalizedBlock.Copy()
		finalizedBlock = &value
	}
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
	if r.observe != nil {
		lineage, lineageErr := r.resolveWindowLineage(windowCtx, window, finalizedBlock)
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
		progressStartAt := time.Time{}
		if window.ObservedSlot == window.StartSlot {
			progressStartAt = startAt
		}
		progress := sessionConsensusProgress{
			Window:             window,
			StartAt:            progressStartAt,
			FinalizedAnchor:    lineage.AppliedAnchor,
			AppliedAnchorState: lineage.AppliedAnchorState,
			Candidates:         lineage.Candidates,
		}
		if progressErr := r.observeWindowProgress(windowCtx, progress); progressErr != nil {
			if windowCtx.Err() == nil && r.log != nil {
				r.log.Warn().
					Err(progressErr).
					Hex("session_id", r.config.SessionID[:]).
					Uint32("window_start", window.StartSlot).
					Msg("local producer rejected consensus progress; skipping window")
			}

			return
		}
	}
	submitter := r.openWindow(window)
	if err = r.backend.HandleLeaderWindow(windowCtx, LeaderWindow{
		Window:  window,
		StartAt: startAt,
		Submit:  submitter.submit,
	}); err != nil {
		submitter.close(err)
		if windowCtx.Err() == nil && r.log != nil {
			r.log.Warn().
				Err(err).
				Hex("session_id", r.config.SessionID[:]).
				Uint32("window_start", window.StartSlot).
				Msg("local producer rejected leader window; skipping window")
		}
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

func windowProgressDeadline(window simplex.Window, targetRate time.Duration) time.Time {
	if window.ObservedAt.IsZero() || targetRate <= 0 || window.EndSlot <= window.ObservedSlot {
		return time.Time{}
	}

	remainingSlots := uint64(window.EndSlot - window.ObservedSlot)
	if remainingSlots > uint64(time.Duration(1<<63-1)/targetRate) {
		return time.Time{}
	}

	return window.ObservedAt.Add(time.Duration(remainingSlots) * targetRate)
}

func (r *sessionRuntime) observeWindowProgress(
	ctx context.Context,
	progress sessionConsensusProgress,
) error {
	for {
		err := r.observe(ctx, progress)
		if err == nil || !isTransientProgressError(err) {
			return err
		}
		r.markWindowProgressRetrying(progress.Window.StartSlot)
		if err = waitDuration(ctx, consensusProgressRetryInterval); err != nil {
			return err
		}
	}
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
	return errors.Is(err, ErrBlockNotReady) || errors.Is(err, collator.ErrAcquisitionNotReady)
}

func (r *sessionRuntime) resolveWindowLineage(
	ctx context.Context,
	window simplex.Window,
	finalizedBlock *ton.BlockIDExt,
) (resolvedLineage, error) {
	loggedNotReady := false
	started := time.Now()
	for {
		lineage, err := r.states.lineage(ctx, window.Base, finalizedBlock)
		if err == nil {
			r.observeLineageWalk(LineageWalkSuccess, lineage.Walk, started)

			return lineage, nil
		}
		if !errors.Is(err, ErrBlockNotReady) {
			r.observeLineageWalk(LineageWalkFailure, lineage.Walk, started)

			return resolvedLineage{}, err
		}
		if ctx.Err() != nil {
			return resolvedLineage{}, ctx.Err()
		}
		r.markWindowProgressRetrying(window.StartSlot)
		if !loggedNotReady && r.log != nil {
			r.log.Debug().
				Err(err).
				Hex("session_id", r.config.SessionID[:]).
				Uint32("window_start", window.StartSlot).
				Msg("consensus window is waiting for finalized state")
			loggedNotReady = true
		}

		// A masterchain shard descriptor can become visible before its exact
		// block state is available locally. C++ ChainState::from_manager waits
		// for that state without stopping the consensus actor. Keep this ordered
		// production worker pending as well; session retirement cancels the wait.
		if err = waitDuration(ctx, consensusProgressRetryInterval); err != nil {
			return resolvedLineage{}, err
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

func (r *sessionRuntime) OnNotarized(id simplex.CandidateID, certificate simplex.VerifiedCertificate) {
	r.candidates.observeNotarization(id, certificate)
}

func (r *sessionRuntime) OnFinalized(id simplex.CandidateID, certificate simplex.VerifiedCertificate) {
	// Simplex maintains this watermark for its own slot map; the resolver
	// caches are the only session-scoped structures that never saw it. The
	// candidate sweep runs before finalization is spawned below, which then walks
	// back over parents it may have just released; that order is safe only
	// because a released payload is either durable or one consensus never
	// accepted.
	//
	// Both sweeps run under one floor. The margins below the finalized slot are
	// fixed, and the leader window that consumes what they retain is not: it
	// walks back to the applied anchor, so the state resolver reports how far
	// that is and neither sweep releases above it.
	//
	// What bounds that request is measured first, off the candidate resolver's
	// own retained payloads, and handed to the state resolver: the two never
	// nest their locks, and the bound is a memory budget rather than a slot
	// distance.
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
		slot := artifact.Candidate.ID.Slot
		if _, err = r.candidates.storeAsync(artifact.Candidate.ID, func(storeErr error) {
			if storeErr == nil {
				return
			}
			r.reportCandidatePersistFailure(slot, storeErr)
		}); err != nil {
			return err
		}

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
