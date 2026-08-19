package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/validator/simplex"
)

var errBlockSyncObserverPublicationUnsupported = errors.New(
	"validator block-sync observer: candidate publication is unsupported",
)

// blockSyncObserverRuntime owns the protocol-1 block-sync transport for a
// pure observer. It deliberately has no consensus, storage, or backend state:
// the transport authenticates the broadcast source and this receiver verifies
// the candidate itself before handing the decoded artifact back to ingress.
type blockSyncObserverRuntime struct {
	config  SessionConfig
	network SessionNetwork
	codec   *candidateCodec

	lifecycleMu sync.Mutex
	phase       sessionRuntimePhase
	terminalErr error
	runCancel   context.CancelFunc
	runDone     chan struct{}
}

var _ SessionRuntime = (*blockSyncObserverRuntime)(nil)
var _ SessionReceiver = (*blockSyncObserverRuntime)(nil)

// PrepareBlockSyncObserverRuntime builds one inactive protocol-1 observer
// runtime around an already prepared session network.
func PrepareBlockSyncObserverRuntime(
	ctx context.Context,
	config SessionConfig,
	network SessionNetwork,
	limits CandidateLimits,
) (*blockSyncObserverRuntime, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if network == nil {
		return nil, errors.New("validator block-sync observer: network is required")
	}
	if config.Protocol.Version != 2 || config.Protocol.ProtocolVersion != 1 {
		return nil, fmt.Errorf(
			"validator block-sync observer: requires simplex config v2 protocol 1, got config v%d protocol %d",
			config.Protocol.Version,
			config.Protocol.ProtocolVersion,
		)
	}
	if config.Identity.Validator != nil {
		return nil, errors.New("validator block-sync observer: validator identity is not allowed")
	}
	if config.SessionID == ([32]byte{}) {
		return nil, errors.New("validator block-sync observer: session id is zero")
	}
	if config.Identity.ADNLID == ([32]byte{}) {
		return nil, errors.New("validator block-sync observer: local adnl id is zero")
	}
	if !config.Shard.IsValid() {
		return nil, errors.New("validator block-sync observer: invalid session shard")
	}
	prefix, err := config.Shard.PrefixBits()
	if err != nil {
		return nil, err
	}
	if prefix != config.ShardPrefixLen {
		return nil, fmt.Errorf(
			"validator block-sync observer: shard prefix length %d, want %d",
			config.ShardPrefixLen,
			prefix,
		)
	}

	codec, err := newCandidateCodec(config, limits)
	if err != nil {
		return nil, err
	}

	return &blockSyncObserverRuntime{
		config:  config,
		network: network,
		codec:   codec,
		phase:   sessionRuntimePrepared,
	}, nil
}

func (r *blockSyncObserverRuntime) Recover(ctx context.Context, _ SessionStart) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	switch r.phase {
	case sessionRuntimePrepared:
		return nil
	case sessionRuntimeClosed:
		return ErrSessionRuntimeClosed
	default:
		return ErrSessionRuntimeStarted
	}
}

func (r *blockSyncObserverRuntime) Run(ctx context.Context, start SessionStart) error {
	return r.run(ctx, start, nil)
}

func (r *blockSyncObserverRuntime) runWithStartup(
	ctx context.Context,
	start SessionStart,
	startup chan<- error,
) error {
	return r.run(ctx, start, startup)
}

func (r *blockSyncObserverRuntime) run(
	ctx context.Context,
	_ SessionStart,
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
	r.runCancel = cancel
	r.runDone = runDone
	r.lifecycleMu.Unlock()

	defer func() {
		cancel()

		r.lifecycleMu.Lock()
		if resultErr != nil && !cleanCancellation && r.terminalErr == nil {
			r.terminalErr = resultErr
		}
		if r.phase != sessionRuntimeClosed {
			r.phase = sessionRuntimeStopped
		}
		close(runDone)
		r.lifecycleMu.Unlock()
	}()

	if err := r.network.Start(runCtx, r); err != nil {
		r.lifecycleMu.Lock()
		terminalErr := r.terminalErr
		r.lifecycleMu.Unlock()
		if terminalErr != nil {
			return terminalErr
		}
		if runCtx.Err() != nil {
			cleanCancellation = true

			return runCtx.Err()
		}

		return fmt.Errorf("validator block-sync observer: start network: %w", err)
	}
	signalStartup(nil)

	runErr := r.network.Run(runCtx)
	r.lifecycleMu.Lock()
	terminalErr := r.terminalErr
	r.lifecycleMu.Unlock()
	if terminalErr != nil {
		return terminalErr
	}
	if runCtx.Err() != nil {
		cleanCancellation = true

		return runCtx.Err()
	}
	if runErr == nil {
		return errors.New("validator block-sync observer: network stopped unexpectedly")
	}

	return fmt.Errorf("validator block-sync observer: network failed: %w", runErr)
}

func (r *blockSyncObserverRuntime) Update(ctx context.Context, state SessionState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRuntimeState(r.config, state); err != nil {
		return err
	}

	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	switch r.phase {
	case sessionRuntimePrepared, sessionRuntimeRunning:
		return nil
	default:
		if r.terminalErr != nil {
			return r.terminalErr
		}

		return ErrSessionRuntimeClosed
	}
}

func (r *blockSyncObserverRuntime) Close() error {
	r.lifecycleMu.Lock()
	if r.phase == sessionRuntimeClosed {
		done := r.runDone
		r.lifecycleMu.Unlock()
		if done != nil {
			<-done
		}

		return nil
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

	return nil
}

func (r *blockSyncObserverRuntime) Retire() error {
	return r.Close()
}

func (*blockSyncObserverRuntime) ReceiveConsensusMessage(simplex.PeerID, int, []byte) {}

func (*blockSyncObserverRuntime) PrecheckCandidateBroadcast(uint32, [32]byte, bool) error {
	return nil
}

func (r *blockSyncObserverRuntime) ReceiveCandidate(
	ctx context.Context,
	expectedSlot uint32,
	wire []byte,
) (CandidateArtifact, error) {
	if err := ctx.Err(); err != nil {
		return CandidateArtifact{}, err
	}

	r.lifecycleMu.Lock()
	phase := r.phase
	terminalErr := r.terminalErr
	r.lifecycleMu.Unlock()
	if phase != sessionRuntimeRunning {
		if terminalErr != nil {
			return CandidateArtifact{}, terminalErr
		}

		return CandidateArtifact{}, context.Canceled
	}

	artifact, _, err := r.codec.decodeCanonical(wire, nil)
	if err != nil {
		return CandidateArtifact{}, err
	}
	if artifact.Candidate.ID.Slot != expectedSlot {
		return CandidateArtifact{}, errors.New("validator block-sync observer: candidate broadcast slot mismatch")
	}

	return *artifact, nil
}

func (*blockSyncObserverRuntime) ServeCandidate(
	ctx context.Context,
	_ simplex.PeerID,
	_ CandidateRequest,
) (CandidateResponse, error) {
	if err := ctx.Err(); err != nil {
		return CandidateResponse{}, err
	}

	return CandidateResponse{}, ErrCandidateUnavailable
}

func (*blockSyncObserverRuntime) publishCandidate(
	ctx context.Context,
	_ *CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return errBlockSyncObserverPublicationUnsupported
}

func (r *blockSyncObserverRuntime) fail(err error) {
	if err == nil {
		return
	}

	r.lifecycleMu.Lock()
	if r.phase != sessionRuntimeRunning || r.terminalErr != nil {
		r.lifecycleMu.Unlock()

		return
	}
	r.terminalErr = err
	cancel := r.runCancel
	r.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
	}
}
