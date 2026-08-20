package validator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

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

	params          simplex.Params
	admissions      map[uint32]*observerCandidateAdmission
	validatorWeight []uint64
	quorumWeight    uint64
	bootstrap       map[uint32]observerSlotObservation
	bootstrapWeight uint64
	clockSlot       uint32
	clockAt         time.Time
	clockSet        bool
}

type observerCandidateAdmission struct {
	broadcastID [32]byte
	phase       observerCandidateAdmissionPhase
	candidateID simplex.CandidateID
	leader      uint32
}

type observerSlotObservation struct {
	slot uint32
	at   time.Time
}

type observerWeightedSlot struct {
	slot   uint32
	weight uint64
}

type observerCandidateAdmissionPhase uint8

const (
	observerCandidateAuthenticated observerCandidateAdmissionPhase = iota + 1
	observerCandidateDelivered
)

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
	validatorWeight := make([]uint64, len(config.Validators))
	var totalWeight uint64
	for i := range config.Validators {
		validatorWeight[i] = config.Validators[i].Weight
		totalWeight += config.Validators[i].Weight
	}

	return &blockSyncObserverRuntime{
		config:          config,
		network:         network,
		codec:           codec,
		phase:           sessionRuntimePrepared,
		params:          simplex.DefaultParams(),
		admissions:      make(map[uint32]*observerCandidateAdmission),
		validatorWeight: validatorWeight,
		quorumWeight:    totalWeight*2/3 + 1,
		bootstrap:       make(map[uint32]observerSlotObservation, len(validatorWeight)),
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
		now := time.Now()
		if r.clockSet {
			r.clockSlot = r.currentSlotLocked(now)
			r.clockAt = now
		}
		r.params = state.Params
		r.pruneAdmissionsLocked(now)

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
	clear(r.admissions)
	clear(r.bootstrap)
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

func (r *blockSyncObserverRuntime) PrecheckCandidateBroadcast(
	slot uint32,
	broadcastID [32]byte,
	signatureChecked bool,
) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	if r.phase != sessionRuntimeRunning {
		if r.terminalErr != nil {
			return r.terminalErr
		}

		return ErrSessionRuntimeClosed
	}
	now := time.Now()
	r.pruneAdmissionsLocked(now)
	if r.clockSet {
		current := r.currentSlotLocked(now)
		horizon := r.admissionHorizonLocked()
		if uint64(slot) > uint64(current)+uint64(horizon) {
			return errors.New("validator block-sync observer: candidate slot is too far in the future")
		}
		if current > horizon && slot < current-horizon {
			return errors.New("validator block-sync observer: candidate slot is outside the retained horizon")
		}
	}

	if admission := r.admissions[slot]; admission != nil {
		if admission.broadcastID != broadcastID {
			return errors.New("validator block-sync observer: conflicting candidate broadcast")
		}
		if !signatureChecked {
			return nil
		}

		return errors.New("validator block-sync observer: duplicate candidate broadcast")
	}
	// The first precheck runs before the transport has authenticated the outer
	// broadcast signature. Reserving a slot here would let one bad signature
	// poison that slot forever because the transport has no rollback callback.
	// The authenticated callback below is the first state this runtime owns.
	if !signatureChecked {
		return nil
	}
	leader := r.expectedLeader(slot)
	if !r.clockSet {
		if previous, observed := r.bootstrap[leader]; observed && previous.slot != slot {
			delete(r.admissions, previous.slot)
		}
	}
	if uint64(len(r.admissions)) >= r.admissionCapacityLocked() {
		return errors.New("validator block-sync observer: candidate admission horizon is full")
	}

	r.admissions[slot] = &observerCandidateAdmission{
		broadcastID: broadcastID,
		phase:       observerCandidateAuthenticated,
		leader:      leader,
	}
	r.observeAuthenticatedSlotLocked(leader, slot, now)

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

	r.lifecycleMu.Lock()
	if r.phase != sessionRuntimeRunning {
		terminalErr = r.terminalErr
		r.lifecycleMu.Unlock()
		if terminalErr != nil {
			return CandidateArtifact{}, terminalErr
		}

		return CandidateArtifact{}, context.Canceled
	}
	admission := r.admissions[expectedSlot]
	if admission != nil && admission.phase == observerCandidateDelivered {
		conflict := admission.candidateID != artifact.Candidate.ID
		r.lifecycleMu.Unlock()
		if conflict {
			return CandidateArtifact{}, errors.New("validator block-sync observer: conflicting candidate for slot")
		}

		return CandidateArtifact{}, errors.New("validator block-sync observer: duplicate candidate for slot")
	}
	if admission == nil {
		// A bootstrap outlier can be evicted when the same leader later supplies a
		// fresher authenticated slot. The transport may still finish that already
		// allocated stream; its fully verified candidate remains useful to the block
		// cache, but it must not recreate admission state or move the quorum clock.
		r.lifecycleMu.Unlock()

		return *artifact, nil
	}
	if admission.leader != artifact.Candidate.Leader {
		r.lifecycleMu.Unlock()

		return CandidateArtifact{}, errors.New("validator block-sync observer: admitted candidate leader mismatch")
	}
	admission.phase = observerCandidateDelivered
	admission.candidateID = artifact.Candidate.ID
	r.pruneAdmissionsLocked(time.Now())
	r.lifecycleMu.Unlock()

	return *artifact, nil
}

func (r *blockSyncObserverRuntime) admissionHorizonLocked() uint32 {
	horizon := uint64(r.params.MaxLeaderWindowDesync) *
		uint64(r.config.Protocol.SlotsPerLeaderWindow)
	if horizon > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(horizon)
}

func (r *blockSyncObserverRuntime) admissionCapacityLocked() uint64 {
	if !r.clockSet {
		return uint64(len(r.validatorWeight))
	}
	horizon := uint64(r.admissionHorizonLocked())

	return min(uint64(math.MaxUint32)+1, horizon*2+1)
}

func (r *blockSyncObserverRuntime) expectedLeader(slot uint32) uint32 {
	window := slot / r.config.Protocol.SlotsPerLeaderWindow

	return window % uint32(len(r.validatorWeight))
}

// observeAuthenticatedSlotLocked bootstraps a restarted observer without
// trusting its first scheduled source as a clock oracle. The transport has
// already checked the outer signature (and delegation, when present), so one
// latest observation per leader is authenticated. A 2/3+1 weighted set is the
// first point at which a Byzantine minority cannot place the weighted median;
// until then admissions stay bounded by the roster and no provisional slot is
// allowed to reject another valid candidate.
func (r *blockSyncObserverRuntime) observeAuthenticatedSlotLocked(
	leader uint32,
	slot uint32,
	now time.Time,
) {
	if r.clockSet {
		return
	}
	if _, observed := r.bootstrap[leader]; !observed {
		r.bootstrapWeight += r.validatorWeight[leader]
	}
	r.bootstrap[leader] = observerSlotObservation{slot: slot, at: now}
	if r.bootstrapWeight < r.quorumWeight {
		return
	}

	observations := make([]observerWeightedSlot, 0, len(r.bootstrap))
	for index, observation := range r.bootstrap {
		observations = append(observations, observerWeightedSlot{
			slot:   projectObserverSlot(observation.slot, observation.at, now, r.params.TargetRate),
			weight: r.validatorWeight[index],
		})
	}
	sort.Slice(observations, func(i, j int) bool {
		return observations[i].slot < observations[j].slot
	})
	medianWeight := r.bootstrapWeight/2 + 1
	var accumulated uint64
	for i := range observations {
		accumulated += observations[i].weight
		if accumulated < medianWeight {
			continue
		}

		r.clockSlot = observations[i].slot
		break
	}
	r.clockAt = now
	r.clockSet = true
	clear(r.bootstrap)
	r.bootstrapWeight = 0
	r.pruneAdmissionsLocked(now)
}

func projectObserverSlot(slot uint32, from time.Time, now time.Time, targetRate time.Duration) uint32 {
	if targetRate <= 0 || !now.After(from) {
		return slot
	}

	elapsed := uint64(now.Sub(from) / targetRate)
	projected := uint64(slot) + elapsed
	if projected > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(projected)
}

func (r *blockSyncObserverRuntime) currentSlotLocked(now time.Time) uint32 {
	if !r.clockSet {
		return r.clockSlot
	}

	return projectObserverSlot(r.clockSlot, r.clockAt, now, r.params.TargetRate)
}

func (r *blockSyncObserverRuntime) pruneAdmissionsLocked(now time.Time) {
	if !r.clockSet {
		return
	}

	var floor uint32
	floorSet := false
	var ceiling uint64 = math.MaxUint32
	current := r.currentSlotLocked(now)
	horizon := r.admissionHorizonLocked()
	if current > horizon {
		floor = current - horizon - 1
		floorSet = true
	}
	ceiling = min(uint64(math.MaxUint32), uint64(current)+uint64(horizon))
	for slot := range r.admissions {
		if (floorSet && slot <= floor) || uint64(slot) > ceiling {
			delete(r.admissions, slot)
		}
	}
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
