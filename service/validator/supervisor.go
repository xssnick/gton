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

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	// The four retry classes share one cadence today, but they answer to
	// different failures: a preparation that may be permanently misconfigured,
	// a masterchain state that has not been applied yet, a cleanup that failed,
	// and a session that exited its run. They are named separately so one can
	// be tuned without moving the others.
	sessionPrepareRetryDelay         = time.Second
	sessionStateRetryDelay           = time.Second
	sessionCloseRetryDelay           = time.Second
	sessionRestartDelay              = time.Second
	maxConcurrentSessionPreparations = 4

	// escalatedSessionPrepareFailures is about half a minute of retries: long
	// enough that a transient failure has cleared, short enough that a
	// permanent misconfiguration (a validator ADNL that is not the node ADNL,
	// a journal reopened with a different signature capacity) is still
	// reported while an operator is watching.
	escalatedSessionPrepareFailures = 30
)

// sessionRetry is one session's pending retry deadline. prepareFailures counts
// consecutive preparation failures and drives log discipline only — never the
// deadline itself: a permanently misconfigured session must be reported, not
// repeated at Warn once per second forever. The counter clears with the entry,
// so every deadline reset also resets the reporting.
type sessionRetry struct {
	at              time.Time
	prepareFailures int
}

// SessionRuntime is one prepared consensus session. Recover reconciles durable
// finalizations without joining or voting when activation inputs are already
// known, including for future groups. Run activates it exactly once and blocks
// until ctx is cancelled or the session fails. Update applies one complete
// masterchain-derived state both before and during Run. Close is idempotent and
// waits for every resource owned by the runtime to stop. Retire additionally
// deletes durable producer state after a real topology removal.
type SessionRuntime interface {
	Recover(ctx context.Context, start SessionStart) error
	Run(ctx context.Context, start SessionStart) error
	Update(ctx context.Context, state SessionState) error
	Close() error
	Retire() error
}

// SessionPreparer builds a ready but inactive runtime. ctx bounds preparation
// only; a successful runtime owns its resources until Close and must not retain
// ctx as its lifetime context.
type SessionPreparer func(
	ctx context.Context,
	config SessionConfig,
	initial SessionState,
	start SessionStart,
) (SessionRuntime, error)

// SessionProtocol is the immutable part of config 29/30 that determines the
// session runtime and wire protocol. Noncritical values live in SessionState.
type SessionProtocol struct {
	Version              uint8
	Flags                uint8
	ProtocolVersion      uint8
	UseQUIC              bool
	SlotsPerLeaderWindow uint32
}

// ValidatorIdentity is present only when the local ADNL identity votes in the
// session. Keeping the index, key ID and signer together prevents partial
// voting identities; a nil SessionIdentity.Validator denotes an observer.
type ValidatorIdentity struct {
	Index  uint32
	KeyID  [32]byte
	Signer simplex.Signer
}

// SessionIdentity is the one local ADNL identity running this session.
type SessionIdentity struct {
	ADNLID    [32]byte
	Validator *ValidatorIdentity
}

// SessionConfig contains only inputs that are fixed for the lifetime of one
// prepared runtime.
type SessionConfig struct {
	SessionID        [32]byte
	Shard            groups.ShardID
	CatchainSeqno    uint32
	ValidatorSetHash uint32
	Validators       []groups.Validator
	OverlayMembers   [][32]byte
	// CollatorsByValidator is the config-46 authorization set filtered to this
	// shard roster. OverlayMembers is deliberately wider and includes observers
	// which may receive direct traffic but must not originate candidates.
	CollatorsByValidator []groups.CollatorRegistryEntry
	ShardPrefixLen       uint32
	Protocol             SessionProtocol
	CandidateLimits      CandidateLimits
	Identity             SessionIdentity
	// StorageID is the durable identity compared during reconciliation.
	// Journal is its bound handle and is deliberately not interface-compared.
	StorageID SessionStorageID
	Journal   simplex.Journal
}

// SessionState is the atomic, masterchain-derived part of a session. It may be
// updated while a future runtime is prepared or while an active runtime runs.
type SessionState struct {
	MasterchainBlock ton.BlockIDExt
	Params           simplex.Params
	Registered       []groups.ShardDescription
	FinalizedBlock   *ton.BlockIDExt
}

// SessionStart contains the activation inputs unavailable while a session is
// merely tentative.
type SessionStart struct {
	Genesis        []ton.BlockIDExt
	MinMasterchain ton.BlockIDExt
}

type desiredSession struct {
	config SessionConfig
	state  SessionState
	start  SessionStart
	active bool
}

type sessionSupervisor struct {
	log     zerolog.Logger
	keys    SigningKeys
	storage ValidatorStorage
	prepare SessionPreparer
	// observerADNLIDs is the transport inventory supplied by composition. The
	// signing keyring may be associated with more persistent identities than
	// this process can actually bind.
	observerADNLIDs map[[32]byte]struct{}

	mu             sync.Mutex
	latest         *groups.Snapshot
	highestSeqno   uint32
	hasSnapshot    bool
	isStarted      bool
	isClosed       bool
	cancel         context.CancelFunc
	done           chan struct{}
	doneOnce       sync.Once
	wake           chan struct{}
	sessionResults chan sessionRunResult
	prepareResults chan sessionPrepareResult

	statusMu sync.Mutex
	status   SessionSupervisorStatus
}

// SessionPhase describes the owner-visible state of one desired consensus
// runtime. A retry remains desired, but is waiting for its retry deadline.
type SessionPhase string

const (
	SessionPhaseDesired   SessionPhase = "desired"
	SessionPhasePreparing SessionPhase = "preparing"
	SessionPhasePrepared  SessionPhase = "prepared"
	SessionPhaseRunning   SessionPhase = "running"
	SessionPhaseRetry     SessionPhase = "retry"
)

// SessionRuntimeStatus is an immutable projection of one supervisor actor.
// It carries runtime state only; durable per-session telemetry is reported
// separately through StorageStatus, because the supervisor never calls storage
// while publishing status.
type SessionRuntimeStatus struct {
	StorageID     SessionStorageID
	SessionID     [32]byte
	ADNLID        [32]byte
	LocalIndex    int
	Shard         groups.ShardID
	CatchainSeqno uint32
	Active        bool
	Phase         SessionPhase
	RetryAt       time.Time
}

// SessionSupervisorStatus is a race-safe snapshot of reconciliation state.
// Desired is the complete target set; Preparing, Prepared, and Running report
// the current execution subsets.
type SessionSupervisorStatus struct {
	Started                bool
	Closed                 bool
	LatestMasterchainSeqno uint32
	Desired                int
	Preparing              int
	Prepared               int
	Running                int
	Sessions               []SessionRuntimeStatus
}

type managedConsensusSession struct {
	session SessionRuntime
	desired desiredSession

	isRunning     bool
	runCancel     context.CancelFunc
	done          chan struct{}
	closeRetryAt  time.Time
	retireOnClose bool
	stateRetryAt  time.Time
}

type sessionActorID struct {
	SessionID   [32]byte
	LocalIndex  int
	LocalADNLID [32]byte
}

type sessionRunResult struct {
	id      sessionActorID
	managed *managedConsensusSession
	err     error
}

type sessionPreparation struct {
	desired desiredSession
	cancel  context.CancelFunc
}

type sessionPrepareResult struct {
	id          sessionActorID
	preparation *sessionPreparation
	session     SessionRuntime
	err         error
}

type sessionSpecError struct {
	id  [32]byte
	err error
}

func newSessionSupervisor(
	log zerolog.Logger,
	keys SigningKeys,
	storage ValidatorStorage,
	prepare SessionPreparer,
	observerADNLIDs ...[32]byte,
) *sessionSupervisor {
	availableObservers := make(map[[32]byte]struct{}, len(observerADNLIDs))
	for _, id := range observerADNLIDs {
		availableObservers[id] = struct{}{}
	}

	return &sessionSupervisor{
		log:             log,
		keys:            keys,
		storage:         storage,
		prepare:         prepare,
		observerADNLIDs: availableObservers,
		done:            make(chan struct{}),
		wake:            make(chan struct{}, 1),
		sessionResults:  make(chan sessionRunResult, 1),
		prepareResults:  make(chan sessionPrepareResult),
	}
}

// Start launches reconciliation. A snapshot accepted before Start is handled
// immediately. Repeated calls do not create another loop.
func (s *sessionSupervisor) Start(ctx context.Context) {
	s.mu.Lock()
	if s.isStarted || s.isClosed {
		s.mu.Unlock()
		return
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.isStarted = true
	s.cancel = cancel
	hasSnapshot := s.hasSnapshot
	s.mu.Unlock()
	s.publishLifecycle(true, false, 0)

	go s.run(runCtx)
	if hasSnapshot {
		s.notify()
	}
}

// Reconcile publishes the newest complete group view without waiting for any
// session I/O. Snapshots at or below the highest accepted masterchain seqno
// are idempotent and cannot roll the supervisor back.
func (s *sessionSupervisor) Reconcile(snapshot *groups.Snapshot) {
	s.mu.Lock()
	if s.isClosed || (s.hasSnapshot && snapshot.MasterchainBlock.SeqNo <= s.highestSeqno) {
		s.mu.Unlock()
		return
	}

	s.latest = snapshot
	s.highestSeqno = snapshot.MasterchainBlock.SeqNo
	s.hasSnapshot = true
	isStarted := s.isStarted
	s.mu.Unlock()
	s.publishLifecycle(false, false, snapshot.MasterchainBlock.SeqNo)

	if isStarted {
		s.notify()
	}
}

// Close stops every active session, closes every prepared future session, and
// waits for the reconciliation loop. It is safe to call more than once and
// before Start.
func (s *sessionSupervisor) Close() {
	s.mu.Lock()
	if s.isClosed {
		done := s.done
		s.mu.Unlock()
		<-done
		return
	}

	s.isClosed = true
	cancel := s.cancel
	isStarted := s.isStarted
	s.mu.Unlock()
	s.publishLifecycle(false, true, 0)

	if cancel != nil {
		cancel()
	}
	if !isStarted {
		s.doneOnce.Do(func() { close(s.done) })
	}
	<-s.done
}

func (s *sessionSupervisor) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *sessionSupervisor) run(ctx context.Context) {
	desired := make(map[sessionActorID]desiredSession)
	managed := make(map[sessionActorID]*managedConsensusSession)
	preparing := make(map[sessionActorID]*sessionPreparation)
	retryAt := make(map[sessionActorID]sessionRetry)
	var prepareWG sync.WaitGroup
	defer func() {
		s.mu.Lock()
		s.isClosed = true
		s.mu.Unlock()
		s.publishLifecycle(false, true, 0)
		s.publishRuntimeStatus(desired, managed, preparing, retryAt)
		s.doneOnce.Do(func() { close(s.done) })
	}()

	// Timer channels are synchronous, so a Stop that loses the race leaves
	// nothing to drain and receiving here would block forever.
	retryTimer := time.NewTimer(time.Hour)
	retryTimer.Stop()
	defer retryTimer.Stop()
	var retryC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			cancelSessionPreparations(preparing)
			s.closeManaged(managed)
			prepareWG.Wait()
			clear(preparing)
			s.publishRuntimeStatus(desired, managed, preparing, retryAt)
			return
		case <-s.wake:
			snapshot := s.latestSnapshot()
			if snapshot != nil {
				var specErrors []sessionSpecError
				nextDesired, specErrors := s.desiredSessions(snapshot)
				s.pinCriticalSessionProtocols(desired, nextDesired)
				s.bindSessionJournals(desired, nextDesired)
				for i := range specErrors {
					item := &specErrors[i]
					s.log.Error().Err(item.err).Hex("session_id", item.id[:]).Msg("validator session specification rejected")
				}
				retainSessionRetryDeadlines(desired, nextDesired, retryAt)
				desired = nextDesired
				s.reconcileDesired(ctx, desired, managed, preparing, retryAt, &prepareWG)
			}
		case result := <-s.sessionResults:
			s.handleRunResult(result, desired, managed, retryAt)
		case result := <-s.prepareResults:
			s.handlePrepareResult(ctx, result, desired, managed, preparing, retryAt)
			s.reconcileDesired(ctx, desired, managed, preparing, retryAt, &prepareWG)
		case <-retryC:
			s.reconcileDesired(ctx, desired, managed, preparing, retryAt, &prepareWG)
		}

		retryC = resetSessionRetryTimer(retryTimer, desired, managed, preparing, retryAt)
		s.publishRuntimeStatus(desired, managed, preparing, retryAt)
	}
}

// pinCriticalSessionProtocols follows the group lifetime: critical config 30
// values are fixed when a session ID is first observed. Reusing the
// durable vote journal with different slot/window semantics could reinterpret
// recovered progress and must not restart the same signing identity. A real
// critical change therefore requires a new session ID (normally via a key
// block or an explicit unsafe rotation).
func (s *sessionSupervisor) pinCriticalSessionProtocols(
	previous map[sessionActorID]desiredSession,
	next map[sessionActorID]desiredSession,
) {
	for id, candidate := range next {
		current, exists := previous[id]
		if !exists || current.config.Protocol == candidate.config.Protocol {
			continue
		}

		s.log.Error().
			Hex("session_id", id.SessionID[:]).
			Int("local_index", id.LocalIndex).
			Interface("configured", candidate.config.Protocol).
			Interface("pinned", current.config.Protocol).
			Msg("validator critical session config changed without a new session id")
		candidate.config.Protocol = current.config.Protocol
		candidate.config.StorageID.Protocol = current.config.StorageID.Protocol
		next[id] = candidate
	}
}

func (s *sessionSupervisor) bindSessionJournals(
	previous map[sessionActorID]desiredSession,
	next map[sessionActorID]desiredSession,
) {
	for id, candidate := range next {
		current, exists := previous[id]
		if exists && current.config.StorageID == candidate.config.StorageID &&
			len(current.config.Validators) == len(candidate.config.Validators) {
			candidate.config.Journal = current.config.Journal
		} else {
			candidate.config.Journal = s.storage.Journal(candidate.config.StorageID, len(candidate.config.Validators))
		}
		next[id] = candidate
	}
}

// Status returns a detached immutable supervisor snapshot.
func (s *sessionSupervisor) Status() SessionSupervisorStatus {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	status := s.status
	status.Sessions = append([]SessionRuntimeStatus(nil), s.status.Sessions...)
	return status
}

func (s *sessionSupervisor) publishLifecycle(started, closed bool, masterchainSeqno uint32) {
	s.statusMu.Lock()
	s.status.Started = s.status.Started || started
	s.status.Closed = s.status.Closed || closed
	if masterchainSeqno > s.status.LatestMasterchainSeqno {
		s.status.LatestMasterchainSeqno = masterchainSeqno
	}
	s.statusMu.Unlock()
}

// publishRuntimeStatus is called only by the reconciliation goroutine. It
// projects the goroutine-owned maps before taking statusMu, so status readers
// can never hold a supervisor lock across runtime or storage work.
func (s *sessionSupervisor) publishRuntimeStatus(
	desired map[sessionActorID]desiredSession,
	managed map[sessionActorID]*managedConsensusSession,
	preparing map[sessionActorID]*sessionPreparation,
	retryAt map[sessionActorID]sessionRetry,
) {
	ids := make(map[sessionActorID]struct{}, len(desired)+len(managed)+len(preparing)+len(retryAt))
	for id := range desired {
		ids[id] = struct{}{}
	}
	for id := range managed {
		ids[id] = struct{}{}
	}
	for id := range preparing {
		ids[id] = struct{}{}
	}
	for id := range retryAt {
		ids[id] = struct{}{}
	}

	ordered := make([]sessionActorID, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return sessionActorLess(ordered[i], ordered[j])
	})

	sessions := make([]SessionRuntimeStatus, 0, len(ordered))
	running := 0
	prepared := 0
	for _, id := range ordered {
		spec, exists := desired[id]
		if !exists {
			if preparation := preparing[id]; preparation != nil {
				spec = preparation.desired
			} else if current := managed[id]; current != nil {
				spec = current.desired
			} else {
				continue
			}
		}

		phase := SessionPhaseDesired
		sessionRetryAt := retryAt[id].at
		if current := managed[id]; current != nil {
			if !current.closeRetryAt.IsZero() {
				phase = SessionPhaseRetry
				sessionRetryAt = current.closeRetryAt
			} else if current.isRunning {
				phase = SessionPhaseRunning
				running++
			} else {
				phase = SessionPhasePrepared
				prepared++
			}
		} else if preparing[id] != nil {
			phase = SessionPhasePreparing
		} else if _, retrying := retryAt[id]; retrying {
			phase = SessionPhaseRetry
		}

		sessions = append(sessions, SessionRuntimeStatus{
			StorageID:     spec.config.StorageID,
			SessionID:     id.SessionID,
			ADNLID:        id.LocalADNLID,
			LocalIndex:    id.LocalIndex,
			Shard:         spec.config.Shard,
			CatchainSeqno: spec.config.CatchainSeqno,
			Active:        spec.active,
			Phase:         phase,
			RetryAt:       sessionRetryAt,
		})
	}

	s.statusMu.Lock()
	s.status.Desired = len(desired)
	s.status.Preparing = len(preparing)
	s.status.Prepared = prepared
	s.status.Running = running
	s.status.Sessions = sessions
	s.statusMu.Unlock()
}

func (s *sessionSupervisor) latestSnapshot() *groups.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.latest
}

func (s *sessionSupervisor) desiredSessions(snapshot *groups.Snapshot) (map[sessionActorID]desiredSession, []sessionSpecError) {
	desired := make(map[sessionActorID]desiredSession)
	if !snapshot.Ready || !snapshot.LifecycleEnabled {
		return desired, nil
	}

	localKeys := make(map[[32]byte]struct{})
	for _, id := range s.keys.KeyIDs() {
		localKeys[id] = struct{}{}
	}
	overlayMembers, localObserverIDs := resolveOverlayMembers(
		snapshot.PersistentOverlay,
		localKeys,
		s.observerADNLIDs,
	)

	var failures []sessionSpecError
	add := func(group groups.Session, active bool) {
		localIndex, keyID, err := matchLocalValidator(group, localKeys)
		if err != nil {
			failures = append(failures, sessionSpecError{id: group.ID, err: err})
			return
		}
		var localValidatorADNL [32]byte
		if localIndex >= 0 {
			localValidatorADNL = groups.ValidatorADNL(group.Validators[localIndex])
			session, err := s.buildDesiredSession(
				snapshot,
				group,
				overlayMembers,
				localIndex,
				keyID,
				localValidatorADNL,
				active,
			)
			if err != nil {
				failures = append(failures, sessionSpecError{id: group.ID, err: err})
				return
			}
			desired[sessionActorID{
				SessionID:   group.ID,
				LocalIndex:  localIndex,
				LocalADNLID: localValidatorADNL,
			}] = session
		}

		for _, observerID := range localObserverIDs {
			if observerID == localValidatorADNL {
				continue
			}

			session, err := s.buildDesiredSession(
				snapshot,
				group,
				overlayMembers,
				simplex.ObserverIndex,
				[32]byte{},
				observerID,
				active,
			)
			if err != nil {
				failures = append(failures, sessionSpecError{id: group.ID, err: err})
				return
			}
			desired[sessionActorID{
				SessionID:   group.ID,
				LocalIndex:  simplex.ObserverIndex,
				LocalADNLID: observerID,
			}] = session
		}
	}

	for i := range snapshot.Future {
		add(snapshot.Future[i], false)
	}
	for i := range snapshot.Active {
		add(snapshot.Active[i], true)
	}

	return desired, failures
}

func (s *sessionSupervisor) buildDesiredSession(
	snapshot *groups.Snapshot,
	group groups.Session,
	overlayMembers [][32]byte,
	localIndex int,
	keyID [32]byte,
	localADNLID [32]byte,
	active bool,
) (desiredSession, error) {
	if snapshot.Config == nil {
		return desiredSession{}, fmt.Errorf("validator session %x has no blockchain config", group.ID)
	}

	consensus := snapshot.Config.NewConsensus.Shard
	if group.Shard.IsMasterchain() {
		consensus = snapshot.Config.NewConsensus.Masterchain
	}
	if consensus == nil {
		return desiredSession{}, fmt.Errorf("validator session %x has no simplex config", group.ID)
	}
	if !consensus.SupportedProtocol() {
		return desiredSession{}, fmt.Errorf(
			"validator session %x requires simplex config v2 protocol version 3 or newer, got config v%d protocol version %d",
			group.ID,
			consensus.Version,
			consensus.ProtocolVersion,
		)
	}
	shardPrefixLen, err := group.Shard.PrefixBits()
	if err != nil {
		return desiredSession{}, fmt.Errorf("validator session %x shard: %w", group.ID, err)
	}

	identity := SessionIdentity{ADNLID: localADNLID}
	// The conversion is the guard: SessionProtocol and groups.SimplexProtocol
	// must stay structurally identical, so a field added to one and not the
	// other stops compiling here instead of silently projecting a zero value.
	protocol := SessionProtocol(consensus.Protocol())
	storageID := SessionStorageID{
		SessionID:      group.ID,
		Shard:          group.Shard,
		CatchainSeqno:  group.CatchainSeqno,
		IsValidator:    localIndex != simplex.ObserverIndex,
		ValidatorKeyID: keyID,
		LocalADNLID:    localADNLID,
		ValidatorIndex: localIndex,
		Protocol:       protocol,
	}
	if localIndex != simplex.ObserverIndex {
		identity.Validator = &ValidatorIdentity{
			Index:  uint32(localIndex),
			KeyID:  keyID,
			Signer: boundSessionSigner{keys: s.keys, keyID: keyID},
		}
	}

	var collatorsByValidator []groups.CollatorRegistryEntry
	if !group.Shard.IsMasterchain() {
		collatorsByValidator = sessionCollatorRegistry(snapshot.CollatorsByValidator, group.Validators)
	}

	return desiredSession{
		config: SessionConfig{
			SessionID:            group.ID,
			Shard:                group.Shard,
			CatchainSeqno:        group.CatchainSeqno,
			ValidatorSetHash:     group.ValidatorSetHash,
			Validators:           group.Validators,
			OverlayMembers:       overlayMembers,
			CollatorsByValidator: collatorsByValidator,
			ShardPrefixLen:       shardPrefixLen,
			Protocol:             protocol,
			CandidateLimits: CandidateLimits{
				MaxBlockBytes:        snapshot.Config.MaxBlockSize,
				MaxCollatedDataBytes: snapshot.Config.MaxCollatedDataSize,
			},
			Identity:  identity,
			StorageID: storageID,
		},
		state: SessionState{
			MasterchainBlock: snapshot.MasterchainBlock,
			Params:           consensus.SimplexParams(),
			Registered:       group.Registered,
			FinalizedBlock:   group.FinalizedBlock,
		},
		start: SessionStart{
			Genesis:        group.Genesis,
			MinMasterchain: group.MinMasterchain,
		},
		active: active,
	}, nil
}

func resolveOverlayMembers(
	members []groups.PersistentOverlayMember,
	localKeys map[[32]byte]struct{},
	observerADNLIDs map[[32]byte]struct{},
) ([][32]byte, [][32]byte) {
	all := make([][32]byte, len(members))
	local := make([][32]byte, 0, len(localKeys))
	for i := range members {
		member := &members[i]
		all[i] = member.ADNL
		if _, available := observerADNLIDs[member.ADNL]; !available {
			continue
		}
		for _, keyID := range member.ValidatorKeyIDs {
			if _, exists := localKeys[keyID]; exists {
				local = append(local, member.ADNL)
				break
			}
		}
	}

	return all, local
}

func sessionCollatorRegistry(
	registry []groups.CollatorRegistryEntry,
	validators []groups.Validator,
) []groups.CollatorRegistryEntry {
	if len(registry) == 0 {
		return nil
	}

	keys := make(map[[32]byte]struct{}, len(validators))
	for i := range validators {
		keys[validators[i].PublicKeyHash] = struct{}{}
	}
	result := make([]groups.CollatorRegistryEntry, 0, len(validators))
	for i := range registry {
		entry := registry[i]
		if _, exists := keys[entry.ValidatorKeyID]; exists {
			result = append(result, entry)
		}
	}

	return result
}

func matchLocalValidator(group groups.Session, localKeys map[[32]byte]struct{}) (int, [32]byte, error) {
	localIndex := -1
	var keyID [32]byte
	for i := range group.Validators {
		candidateID := group.Validators[i].PublicKeyHash
		if _, exists := localKeys[candidateID]; !exists {
			continue
		}
		if localIndex >= 0 {
			return -1, [32]byte{}, fmt.Errorf(
				"validator session %x contains multiple local signing keys at indexes %d and %d",
				group.ID,
				localIndex,
				i,
			)
		}

		localIndex = i
		keyID = candidateID
	}

	return localIndex, keyID, nil
}

type boundSessionSigner struct {
	keys  SigningKeys
	keyID [32]byte
}

func (s boundSessionSigner) Sign(payload []byte) ([]byte, error) {
	return s.keys.Sign(s.keyID, payload)
}

func (s *sessionSupervisor) reconcileDesired(
	ctx context.Context,
	desired map[sessionActorID]desiredSession,
	managed map[sessionActorID]*managedConsensusSession,
	preparing map[sessionActorID]*sessionPreparation,
	retryAt map[sessionActorID]sessionRetry,
	prepareWG *sync.WaitGroup,
) {
	now := time.Now()
	for _, id := range sortedManagedSessionIDs(managed) {
		current := managed[id]
		if !current.closeRetryAt.IsZero() {
			if now.Before(current.closeRetryAt) {
				continue
			}

			if s.stopManaged(id, current, current.retireOnClose) == nil {
				delete(managed, id)
			}
			continue
		}

		next, exists := desired[id]
		if exists && !(current.isRunning && !next.active) {
			continue
		}

		delete(retryAt, id)
		if s.stopManaged(id, current, !exists) == nil {
			delete(managed, id)
		}
	}
	for id, preparation := range preparing {
		next, exists := desired[id]
		if !exists || sessionConfigChanged(preparation.desired.config, next.config) {
			preparation.cancel()
		}
	}

	for _, id := range sortedSessionSpecIDs(desired) {
		next := desired[id]
		current := managed[id]
		if current != nil && !current.closeRetryAt.IsZero() {
			continue
		}
		if current != nil && !current.stateRetryAt.IsZero() && now.Before(current.stateRetryAt) {
			continue
		}
		configChanged := current != nil && sessionConfigChanged(current.desired.config, next.config)
		startChanged := current != nil && current.isRunning && !sessionStartEqual(current.desired.start, next.start)
		if current != nil && (configChanged || startChanged) {
			delete(retryAt, id)
			if s.stopManaged(id, current, true) != nil {
				continue
			}

			delete(managed, id)
			current = nil
		}
		if current != nil {
			if !sessionStateEqual(current.desired.state, next.state) {
				if err := current.session.Update(ctx, next.state); err != nil {
					if errors.Is(err, collator.ErrAcquisitionNotReady) {
						// Applying a masterchain block is asynchronous: the group hook
						// may observe the new view before the block's complete state is
						// available to local acquisition. The old consensus session is
						// still valid; closing it would retire its collator namespace and
						// make the same session ID impossible to recover.
						current.stateRetryAt = now.Add(sessionStateRetryDelay)
						s.log.Debug().
							Err(err).
							Hex("session_id", id.SessionID[:]).
							Msg("validator session state is not available yet; retrying update")
						continue
					}
					s.log.Warn().Err(err).Hex("session_id", id.SessionID[:]).Msg("validator session state update failed")
					delete(retryAt, id)
					if s.stopManaged(id, current, false) == nil {
						delete(managed, id)
					}
					retryAt[id] = sessionRetry{at: now.Add(sessionStateRetryDelay)}
					continue
				}
			}

			current.stateRetryAt = time.Time{}
			current.desired = next
			if next.active && !current.isRunning {
				s.startManaged(ctx, id, current)
			}
			continue
		}

		if retry, exists := retryAt[id]; exists && now.Before(retry.at) {
			continue
		}
		if preparing[id] != nil || len(preparing) >= maxConcurrentSessionPreparations {
			continue
		}

		s.startPreparation(ctx, id, next, preparing, prepareWG)
	}
}

func (s *sessionSupervisor) startPreparation(
	ctx context.Context,
	id sessionActorID,
	desired desiredSession,
	preparing map[sessionActorID]*sessionPreparation,
	prepareWG *sync.WaitGroup,
) {
	prepareCtx, cancel := context.WithCancel(ctx)
	preparation := &sessionPreparation{desired: desired, cancel: cancel}
	preparing[id] = preparation
	prepareWG.Add(1)

	go func() {
		defer prepareWG.Done()

		session, err := s.prepare(prepareCtx, desired.config, desired.state, desired.start)
		if err == nil && len(desired.start.Genesis) != 0 {
			if recoverErr := session.Recover(prepareCtx, desired.start); recoverErr != nil {
				closeErr := session.Close()
				err = errors.Join(
					fmt.Errorf("validator session recovery failed: %w", recoverErr),
					closeErr,
				)
				session = nil
			}
		}
		cancel()
		result := sessionPrepareResult{
			id:          id,
			preparation: preparation,
			session:     session,
			err:         err,
		}
		select {
		case s.prepareResults <- result:
		case <-ctx.Done():
			if err == nil {
				if closeErr := session.Close(); closeErr != nil {
					s.log.Warn().Err(closeErr).Hex("session_id", id.SessionID[:]).Msg("validator session close failed")
				}
			}
		}
	}()
}

// logPreparationFailure reports a failed preparation once at Warn and then
// stays quiet: several preparation failures are permanent by construction (a
// configured validator ADNL the node does not own, a journal reopened with a
// different signature capacity), and those would otherwise emit one Warn per
// second for as long as the session is desired. One Error escalation makes a
// misconfiguration that never clears visible exactly once.
func (s *sessionSupervisor) logPreparationFailure(id sessionActorID, failures int, err error) {
	event := s.log.Debug()
	switch failures {
	case 1:
		event = s.log.Warn()
	case escalatedSessionPrepareFailures:
		event = s.log.Error()
	}
	event.Err(err).Hex("session_id", id.SessionID[:]).Int("attempts", failures).
		Msg("validator session preparation failed")
}

func (s *sessionSupervisor) handlePrepareResult(
	ctx context.Context,
	result sessionPrepareResult,
	desired map[sessionActorID]desiredSession,
	managed map[sessionActorID]*managedConsensusSession,
	preparing map[sessionActorID]*sessionPreparation,
	retryAt map[sessionActorID]sessionRetry,
) {
	if preparing[result.id] != result.preparation {
		// This preparation was superseded by another instance of the same
		// session, which is not a topology removal: retiring here would delete
		// the collator namespace and make the session id unrecoverable for the
		// preparation that replaced us. Close only, and leave the map entry
		// alone — it belongs to the newer preparation. Note the network-side
		// registration is keyed by sessionID and is torn down even by Close
		// under the composition wrapper, so a real supersede flow would need
		// more than this branch.
		if result.err == nil {
			s.closePrepared(result.id, result.session, false)
		}
		return
	}
	delete(preparing, result.id)

	next, exists := desired[result.id]
	stillDesired := exists && !sessionConfigChanged(result.preparation.desired.config, next.config)
	if result.err != nil {
		if ctx.Err() == nil && stillDesired {
			retry := retryAt[result.id]
			retry.prepareFailures++
			retry.at = time.Now().Add(sessionPrepareRetryDelay)
			retryAt[result.id] = retry
			s.logPreparationFailure(result.id, retry.prepareFailures, result.err)
		}
		return
	}
	if ctx.Err() != nil || managed[result.id] != nil {
		s.closePrepared(result.id, result.session, false)
		return
	}
	if !stillDesired {
		s.closePrepared(result.id, result.session, true)
		return
	}

	if !sessionStateEqual(result.preparation.desired.state, next.state) {
		if err := result.session.Update(ctx, next.state); err != nil {
			s.log.Warn().Err(err).Hex("session_id", result.id.SessionID[:]).Msg("validator session initial state update failed")
			s.closePrepared(result.id, result.session, false)
			retryAt[result.id] = sessionRetry{at: time.Now().Add(sessionStateRetryDelay)}
			return
		}
	}

	current := &managedConsensusSession{session: result.session, desired: next}
	managed[result.id] = current
	delete(retryAt, result.id)
	if next.active {
		s.startManaged(ctx, result.id, current)
	}
}

func cancelSessionPreparations(preparing map[sessionActorID]*sessionPreparation) {
	for _, preparation := range preparing {
		preparation.cancel()
	}
}

func (s *sessionSupervisor) closePrepared(id sessionActorID, session SessionRuntime, retire bool) {
	var err error
	if retire {
		err = session.Retire()
	} else {
		err = session.Close()
	}
	if err != nil {
		s.log.Warn().Err(err).Hex("session_id", id.SessionID[:]).Bool("retire", retire).
			Msg("validator session cleanup failed")
	}
}

func (s *sessionSupervisor) startManaged(ctx context.Context, id sessionActorID, managed *managedConsensusSession) {
	managed.isRunning = true
	managed.done = make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	managed.runCancel = cancel
	start := managed.desired.start

	go func() {
		err := managed.session.Run(runCtx, start)
		close(managed.done)
		select {
		case s.sessionResults <- sessionRunResult{id: id, managed: managed, err: err}:
		case <-ctx.Done():
		}
	}()
}

func (s *sessionSupervisor) handleRunResult(
	result sessionRunResult,
	desired map[sessionActorID]desiredSession,
	managed map[sessionActorID]*managedConsensusSession,
	retryAt map[sessionActorID]sessionRetry,
) {
	current := managed[result.id]
	if current != result.managed {
		return
	}
	if !current.closeRetryAt.IsZero() {
		return
	}

	delete(retryAt, result.id)
	if s.stopManaged(result.id, current, false) == nil {
		delete(managed, result.id)
	}
	if result.err != nil {
		s.log.Warn().Err(result.err).Hex("session_id", result.id.SessionID[:]).Msg("validator session stopped")
	} else {
		s.log.Warn().Hex("session_id", result.id.SessionID[:]).Msg("validator session stopped unexpectedly")
	}

	if session, exists := desired[result.id]; exists && session.active {
		retryAt[result.id] = sessionRetry{at: time.Now().Add(sessionRestartDelay)}
	}
}

func (s *sessionSupervisor) closeManaged(managed map[sessionActorID]*managedConsensusSession) {
	for _, id := range sortedManagedSessionIDs(managed) {
		s.stopManaged(id, managed[id], false)
		delete(managed, id)
	}
}

func (s *sessionSupervisor) stopManaged(
	id sessionActorID,
	managed *managedConsensusSession,
	retire bool,
) error {
	if managed.isRunning {
		managed.runCancel()
		<-managed.done
		managed.isRunning = false
		managed.runCancel = nil
		managed.done = nil
	}

	managed.retireOnClose = managed.retireOnClose || retire
	var err error
	if managed.retireOnClose {
		err = managed.session.Retire()
	} else {
		err = managed.session.Close()
	}
	if err != nil {
		managed.closeRetryAt = time.Now().Add(sessionCloseRetryDelay)
		s.log.Warn().Err(err).Hex("session_id", id.SessionID[:]).Bool("retire", managed.retireOnClose).
			Msg("validator session cleanup failed")
		return err
	}

	managed.closeRetryAt = time.Time{}
	managed.retireOnClose = false
	return nil
}

// sessionConfigChanged compares only what StorageID does not already pin.
// buildDesiredSession derives StorageID and the session identity from the same
// locals in one call, and validateRuntimeConfig rejects any divergence, so
// SessionID, Shard, CatchainSeqno, Protocol, ShardPrefixLen and Identity are
// implied by StorageID here.
//
// The invariant holds only for configs buildDesiredSession produced. The
// consensus observer deliberately assembles partial configs where it does not,
// and must not use this predicate.
func sessionConfigChanged(previous, next SessionConfig) bool {
	return previous.StorageID != next.StorageID ||
		previous.ValidatorSetHash != next.ValidatorSetHash ||
		previous.CandidateLimits != next.CandidateLimits ||
		!slices.Equal(previous.Validators, next.Validators) ||
		!slices.Equal(previous.OverlayMembers, next.OverlayMembers) ||
		!slices.EqualFunc(previous.CollatorsByValidator, next.CollatorsByValidator, collatorRegistryEntryEqual)
}

func collatorRegistryEntryEqual(previous, next groups.CollatorRegistryEntry) bool {
	return previous.ValidatorKeyID == next.ValidatorKeyID &&
		slices.Equal(previous.CollatorADNLIDs, next.CollatorADNLIDs)
}

func sessionStateEqual(previous, next SessionState) bool {
	return sameBlockID(previous.MasterchainBlock, next.MasterchainBlock) &&
		previous.Params == next.Params &&
		slices.EqualFunc(previous.Registered, next.Registered, shardDescriptionEqual) &&
		blockIDPtrEqual(previous.FinalizedBlock, next.FinalizedBlock)
}

func sessionStartEqual(previous, next SessionStart) bool {
	return slices.EqualFunc(previous.Genesis, next.Genesis, sameBlockID) &&
		sameBlockID(previous.MinMasterchain, next.MinMasterchain)
}

func shardDescriptionEqual(previous, next groups.ShardDescription) bool {
	return previous.Shard == next.Shard &&
		sameBlockID(previous.Block, next.Block) &&
		previous.NextCatchainSeqno == next.NextCatchainSeqno &&
		previous.BeforeSplit == next.BeforeSplit &&
		previous.BeforeMerge == next.BeforeMerge &&
		previous.FSM == next.FSM
}

func blockIDPtrEqual(previous, next *ton.BlockIDExt) bool {
	if previous == nil || next == nil {
		return previous == nil && next == nil
	}

	return previous.Equals(next)
}

func retainSessionRetryDeadlines(
	previous map[sessionActorID]desiredSession,
	next map[sessionActorID]desiredSession,
	retryAt map[sessionActorID]sessionRetry,
) {
	for id := range retryAt {
		previousSpec, wasDesired := previous[id]
		nextSpec, isDesired := next[id]
		if !wasDesired || !isDesired || previousSpec.active != nextSpec.active ||
			sessionConfigChanged(previousSpec.config, nextSpec.config) {
			delete(retryAt, id)
		}
	}
}

func resetSessionRetryTimer(
	timer *time.Timer,
	desired map[sessionActorID]desiredSession,
	managed map[sessionActorID]*managedConsensusSession,
	preparing map[sessionActorID]*sessionPreparation,
	retryAt map[sessionActorID]sessionRetry,
) <-chan time.Time {
	timer.Stop()

	var earliest time.Time
	for _, current := range managed {
		if current.closeRetryAt.IsZero() {
			if current.stateRetryAt.IsZero() {
				continue
			}
			if earliest.IsZero() || current.stateRetryAt.Before(earliest) {
				earliest = current.stateRetryAt
			}
			continue
		}
		if earliest.IsZero() || current.closeRetryAt.Before(earliest) {
			earliest = current.closeRetryAt
		}
	}
	for id, retry := range retryAt {
		if _, exists := desired[id]; !exists || managed[id] != nil || preparing[id] != nil {
			continue
		}
		if earliest.IsZero() || retry.at.Before(earliest) {
			earliest = retry.at
		}
	}
	if earliest.IsZero() {
		return nil
	}

	delay := time.Until(earliest)
	if delay < 0 {
		delay = 0
	}
	timer.Reset(delay)
	return timer.C
}

func sortedSessionSpecIDs(sessions map[sessionActorID]desiredSession) []sessionActorID {
	ids := make([]sessionActorID, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		// Preparation is bounded and may involve network I/O. Keep an active
		// group from waiting behind tentative future overlays when all slots
		// are occupied.
		leftActive := sessions[ids[i]].active
		rightActive := sessions[ids[j]].active
		if leftActive != rightActive {
			return leftActive
		}

		return sessionActorLess(ids[i], ids[j])
	})

	return ids
}

func sortedManagedSessionIDs(sessions map[sessionActorID]*managedConsensusSession) []sessionActorID {
	ids := make([]sessionActorID, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return sessionActorLess(ids[i], ids[j])
	})

	return ids
}

func sessionActorLess(left, right sessionActorID) bool {
	if cmp := bytes.Compare(left.SessionID[:], right.SessionID[:]); cmp != 0 {
		return cmp < 0
	}
	if left.LocalIndex != right.LocalIndex {
		return left.LocalIndex < right.LocalIndex
	}

	return bytes.Compare(left.LocalADNLID[:], right.LocalADNLID[:]) < 0
}
