package validator

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

type supervisorTestKeys struct {
	ids [][32]byte

	mu       sync.Mutex
	signedBy [][32]byte
}

func (k *supervisorTestKeys) KeyIDs() [][32]byte {
	return append([][32]byte(nil), k.ids...)
}

func (k *supervisorTestKeys) Sign(keyID [32]byte, _ []byte) ([]byte, error) {
	k.mu.Lock()
	k.signedBy = append(k.signedBy, keyID)
	k.mu.Unlock()

	return []byte{0x73}, nil
}

func (k *supervisorTestKeys) lastSigningKey() [32]byte {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(k.signedBy) == 0 {
		return [32]byte{}
	}

	return k.signedBy[len(k.signedBy)-1]
}

type supervisorTestPreparer struct {
	mu       sync.Mutex
	attempts map[[32]byte]int
	failures map[[32]byte]int
	sessions map[[32]byte][]*supervisorTestSession
}

func newSupervisorTestPreparer() *supervisorTestPreparer {
	return &supervisorTestPreparer{
		attempts: make(map[[32]byte]int),
		failures: make(map[[32]byte]int),
		sessions: make(map[[32]byte][]*supervisorTestSession),
	}
}

func (p *supervisorTestPreparer) prepare(
	ctx context.Context,
	config SessionConfig,
	initial SessionState,
	_ SessionStart,
) (SessionRuntime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	id := config.SessionID
	p.attempts[id]++
	if p.failures[id] > 0 {
		p.failures[id]--
		return nil, errors.New("prepare failed")
	}

	session := &supervisorTestSession{prepareCtx: ctx, config: config, initial: initial}
	p.sessions[id] = append(p.sessions[id], session)

	return session, nil
}

func (p *supervisorTestPreparer) failNext(id [32]byte, count int) {
	p.mu.Lock()
	p.failures[id] = count
	p.mu.Unlock()
}

func (p *supervisorTestPreparer) attemptCount(id [32]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.attempts[id]
}

func (p *supervisorTestPreparer) sessionCount(id [32]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.sessions[id])
}

func (p *supervisorTestPreparer) session(id [32]byte, index int) *supervisorTestSession {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.sessions[id][index]
}

func (p *supervisorTestPreparer) latestSession(id [32]byte) *supervisorTestSession {
	p.mu.Lock()
	defer p.mu.Unlock()

	sessions := p.sessions[id]
	if len(sessions) == 0 {
		return nil
	}

	return sessions[len(sessions)-1]
}

func (p *supervisorTestPreparer) sessionsFor(id [32]byte) []*supervisorTestSession {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]*supervisorTestSession(nil), p.sessions[id]...)
}

type supervisorTestSession struct {
	prepareCtx context.Context
	config     SessionConfig
	initial    SessionState

	mu       sync.Mutex
	runCtx   context.Context
	recovers []SessionStart
	runs     []SessionStart
	updates  []SessionState
	events   []string
	closed   bool
	retired  bool
	closeOne sync.Once
}

func (s *supervisorTestSession) Recover(_ context.Context, start SessionStart) error {
	s.mu.Lock()
	s.recovers = append(s.recovers, start)
	s.mu.Unlock()

	return nil
}

func (s *supervisorTestSession) Run(ctx context.Context, start SessionStart) error {
	s.mu.Lock()
	s.runCtx = ctx
	s.runs = append(s.runs, start)
	s.events = append(s.events, "run")
	s.mu.Unlock()

	<-ctx.Done()
	return nil
}

func (s *supervisorTestSession) Update(_ context.Context, state SessionState) error {
	s.mu.Lock()
	s.updates = append(s.updates, state)
	s.events = append(s.events, "update")
	s.mu.Unlock()

	return nil
}

func (s *supervisorTestSession) Close() error {
	s.closeOne.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	})

	return nil
}

func (s *supervisorTestSession) Retire() error {
	s.mu.Lock()
	s.retired = true
	s.mu.Unlock()

	return s.Close()
}

func (s *supervisorTestSession) runCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.runs)
}

func (s *supervisorTestSession) recoverCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.recovers)
}

func (s *supervisorTestSession) updateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.updates)
}

func (s *supervisorTestSession) latestState() (SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.updates) == 0 {
		return SessionState{}, false
	}

	return s.updates[len(s.updates)-1], true
}

func (s *supervisorTestSession) updatedBeforeRun() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.events) >= 2 && s.events[0] == "update" && s.events[1] == "run"
}

func (s *supervisorTestSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

func (s *supervisorTestSession) isRetired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.retired
}

func (s *supervisorTestSession) prepareContextCanceled() bool {
	select {
	case <-s.prepareCtx.Done():
		return true
	default:
		return false
	}
}

func (s *supervisorTestSession) runContextCanceled() bool {
	s.mu.Lock()
	runCtx := s.runCtx
	s.mu.Unlock()
	if runCtx == nil {
		return false
	}

	select {
	case <-runCtx.Done():
		return true
	default:
		return false
	}
}

type supervisorNotReadyUpdateSession struct {
	*supervisorTestSession

	updateAttempts atomic.Int32
}

func (s *supervisorNotReadyUpdateSession) Update(ctx context.Context, state SessionState) error {
	if err := s.supervisorTestSession.Update(ctx, state); err != nil {
		return err
	}
	if s.updateAttempts.Add(1) == 1 {
		return collator.ErrAcquisitionNotReady
	}

	return nil
}

type supervisorFailingSession struct {
	started   chan struct{}
	result    chan error
	startOnce sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
	retired   atomic.Bool
}

func (*supervisorFailingSession) Recover(context.Context, SessionStart) error { return nil }

func newSupervisorFailingSession() *supervisorFailingSession {
	return &supervisorFailingSession{
		started: make(chan struct{}),
		result:  make(chan error, 1),
	}
}

func (s *supervisorFailingSession) Run(ctx context.Context, _ SessionStart) error {
	s.startOnce.Do(func() { close(s.started) })

	select {
	case err := <-s.result:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (*supervisorFailingSession) Update(context.Context, SessionState) error { return nil }

func (s *supervisorFailingSession) Close() error {
	s.closeOnce.Do(func() { s.closed.Store(true) })

	return nil
}

func (s *supervisorFailingSession) Retire() error {
	s.retired.Store(true)

	return s.Close()
}

type supervisorRetryCloseSession struct {
	started    chan struct{}
	firstClose chan struct{}
	startOnce  sync.Once
	runs       atomic.Int32
	closeCalls atomic.Int32
	closed     atomic.Bool
}

func (*supervisorRetryCloseSession) Recover(context.Context, SessionStart) error { return nil }

func newSupervisorRetryCloseSession() *supervisorRetryCloseSession {
	return &supervisorRetryCloseSession{
		started:    make(chan struct{}),
		firstClose: make(chan struct{}),
	}
}

func (s *supervisorRetryCloseSession) Run(ctx context.Context, _ SessionStart) error {
	s.runs.Add(1)
	s.startOnce.Do(func() { close(s.started) })

	<-ctx.Done()
	return nil
}

func (*supervisorRetryCloseSession) Update(context.Context, SessionState) error { return nil }

func (s *supervisorRetryCloseSession) Close() error {
	if s.closeCalls.Add(1) == 1 {
		close(s.firstClose)
		return errors.New("close failed")
	}

	s.closed.Store(true)
	return nil
}

func (s *supervisorRetryCloseSession) Retire() error { return s.Close() }

func TestSessionSupervisorMatchesLocalKeyBeforeStart(t *testing.T) {
	localKey := supervisorTestID(0x11)
	foreignKey := supervisorTestID(0x22)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), preparer.prepare)

	localGroup := supervisorTestGroup(0x31, supervisorTestShard(), foreignKey, localKey)
	foreignGroup := supervisorTestGroup(0x32, supervisorTestShard(), foreignKey)
	snapshot := supervisorTestSnapshot(10, []groups.Session{localGroup, foreignGroup}, nil)
	snapshot.Config.NewConsensus.Shard.NoncriticalParams = []groups.NoncriticalParam{{ID: 0, Value: 77}}

	supervisor.Reconcile(snapshot)
	if got := preparer.attemptCount(localGroup.ID); got != 0 {
		t.Fatalf("session prepared before supervisor start: %d", got)
	}

	supervisor.Start(context.Background())
	waitFor(t, func() bool {
		return preparer.sessionCount(localGroup.ID) == 1 && preparer.session(localGroup.ID, 0).runCount() == 1
	}, "local active session did not start")

	if got := preparer.attemptCount(foreignGroup.ID); got != 0 {
		t.Fatalf("foreign group preparation attempts = %d, want 0", got)
	}
	session := preparer.session(localGroup.ID, 0)
	identity := session.config.Identity
	if identity.Validator == nil || identity.Validator.Index != 1 || identity.Validator.KeyID != localKey || identity.ADNLID != localKey {
		t.Fatalf(
			"local identity = index %d key %x ADNL %x",
			identity.Validator.Index,
			identity.Validator.KeyID,
			identity.ADNLID,
		)
	}
	if session.initial.Params.TargetRate != 77*time.Millisecond {
		t.Fatalf("initial session state = %+v", session.initial)
	}
	if _, err := identity.Validator.Signer.Sign([]byte("vote")); err != nil {
		t.Fatal(err)
	}
	if got := keys.lastSigningKey(); got != localKey {
		t.Fatalf("session signer used key %x, want %x", got, localKey)
	}

	supervisor.Close()
	if !session.isClosed() || !session.prepareContextCanceled() || !session.runContextCanceled() {
		t.Fatal("active session was not canceled and closed")
	}
}

func TestSessionSupervisorRunsPersistentObserverAcrossPromotion(t *testing.T) {
	localKey := supervisorTestID(0x24)
	observerADNL := supervisorTestID(0xa4)
	foreignADNL := supervisorTestID(0xa5)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		keys,
		newValidatorTestStorage(),
		preparer.prepare,
		observerADNL,
	)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	active := supervisorTestGroup(0x34, supervisorTestShard(), supervisorTestID(0x44))
	future := supervisorTestGroup(0x35, supervisorTestShard(), supervisorTestID(0x45))
	first := supervisorTestSnapshot(11, []groups.Session{active}, []groups.Session{future})
	first.PersistentOverlay = []groups.PersistentOverlayMember{
		{ADNL: observerADNL, ValidatorKeyIDs: [][32]byte{localKey}},
		{ADNL: foreignADNL, ValidatorKeyIDs: [][32]byte{supervisorTestID(0x46)}},
	}
	supervisor.Reconcile(first)
	waitFor(t, func() bool {
		return preparer.sessionCount(active.ID) == 1 && preparer.sessionCount(future.ID) == 1 &&
			preparer.session(active.ID, 0).runCount() == 1
	}, "persistent observer runtimes were not created")

	activeObserver := preparer.session(active.ID, 0)
	futureObserver := preparer.session(future.ID, 0)
	if futureObserver.runCount() != 0 {
		t.Fatal("future observer started before promotion")
	}
	if activeObserver.config.Identity.Validator != nil || activeObserver.config.Identity.ADNLID != observerADNL ||
		activeObserver.config.SessionID != active.ID {
		t.Fatalf("observer config = %+v", activeObserver.config)
	}
	if len(activeObserver.config.OverlayMembers) != 2 || activeObserver.config.OverlayMembers[0] != observerADNL ||
		activeObserver.config.OverlayMembers[1] != foreignADNL {
		t.Fatalf("observer overlay members = %x", activeObserver.config.OverlayMembers)
	}

	promoted := future
	promoted.Registered = []groups.ShardDescription{{Shard: future.Shard}}
	finalized := ton.BlockIDExt{Workchain: future.Shard.Workchain, Shard: future.Shard.Shard, SeqNo: 12}
	promoted.FinalizedBlock = &finalized
	second := supervisorTestSnapshot(12, []groups.Session{active, promoted}, nil)
	second.PersistentOverlay = first.PersistentOverlay
	second.Config.NewConsensus.Shard.NoncriticalParams = []groups.NoncriticalParam{{ID: 0, Value: 333}}
	supervisor.Reconcile(second)
	waitFor(t, func() bool {
		return futureObserver.runCount() == 1 && futureObserver.updateCount() == 1
	}, "future observer was not updated and promoted")
	if preparer.sessionCount(future.ID) != 1 {
		t.Fatal("observer promotion recreated the prepared runtime")
	}
	promotedState, ok := futureObserver.latestState()
	if !ok || promotedState.Params.TargetRate != 333*time.Millisecond || len(promotedState.Registered) != 1 ||
		promotedState.FinalizedBlock == nil || promotedState.FinalizedBlock.SeqNo != 12 {
		t.Fatalf("promoted observer state = %+v", promotedState)
	}

	third := supervisorTestSnapshot(13, []groups.Session{active, future}, nil)
	third.PersistentOverlay = []groups.PersistentOverlayMember{{
		ADNL:            foreignADNL,
		ValidatorKeyIDs: [][32]byte{supervisorTestID(0x46)},
	}}
	supervisor.Reconcile(third)
	waitFor(t, func() bool { return activeObserver.isClosed() && futureObserver.isClosed() }, "removed observer identities stayed running")
}

func TestSessionSupervisorKeepsObserverBesideValidatorIdentity(t *testing.T) {
	localKey := supervisorTestID(0x25)
	validatorADNL := supervisorTestID(0xb4)
	observerADNL := supervisorTestID(0xb5)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		keys,
		newValidatorTestStorage(),
		preparer.prepare,
		observerADNL,
	)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	group := supervisorTestGroup(0x36, supervisorTestShard(), localKey)
	group.Validators[0].ADNL = validatorADNL
	snapshot := supervisorTestSnapshot(14, []groups.Session{group}, nil)
	snapshot.PersistentOverlay = []groups.PersistentOverlayMember{
		{ADNL: validatorADNL, ValidatorKeyIDs: [][32]byte{localKey}},
		{ADNL: observerADNL, ValidatorKeyIDs: [][32]byte{localKey}},
	}
	supervisor.Reconcile(snapshot)
	waitFor(t, func() bool {
		if preparer.sessionCount(group.ID) != 2 {
			return false
		}
		for _, session := range preparer.sessionsFor(group.ID) {
			if session.runCount() != 1 {
				return false
			}
		}
		return true
	}, "validator and observer identities did not run independently")

	var validatorFound, observerFound bool
	for _, session := range preparer.sessionsFor(group.ID) {
		identity := session.config.Identity
		if identity.Validator != nil {
			validatorFound = identity.Validator.Index == 0 && identity.Validator.KeyID == localKey &&
				identity.ADNLID == validatorADNL && identity.Validator.Signer != nil
		} else {
			observerFound = identity.ADNLID == observerADNL
		}
	}
	if !validatorFound || !observerFound {
		t.Fatalf("identity specs = %+v", preparer.sessionsFor(group.ID))
	}
}

func TestSessionSupervisorDoesNotDuplicateCollatorAsProtocolOneObserver(t *testing.T) {
	localKey := supervisorTestID(0x28)
	validatorADNL := supervisorTestID(0xb8)
	collatorADNL := supervisorTestID(0xb9)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		keys,
		newValidatorTestStorage(),
		newSupervisorTestPreparer().prepare,
		collatorADNL,
	)
	defer supervisor.Close()

	group := supervisorTestGroup(0x38, supervisorTestShard(), localKey)
	group.Validators[0].ADNL = validatorADNL
	snapshot := supervisorTestSnapshot(16, []groups.Session{group}, nil)
	snapshot.Config.NewConsensus.Shard.ProtocolVersion = 1
	snapshot.PersistentOverlay = []groups.PersistentOverlayMember{
		{ADNL: validatorADNL, ValidatorKeyIDs: [][32]byte{localKey}},
		{ADNL: collatorADNL, ValidatorKeyIDs: [][32]byte{localKey}},
	}
	snapshot.CollatorsByValidator = []groups.CollatorRegistryEntry{{
		ValidatorKeyID:  localKey,
		CollatorADNLIDs: [][32]byte{collatorADNL},
	}}

	desired, failures := supervisor.desiredSessions(snapshot)
	if len(failures) != 0 {
		t.Fatalf("desired sessions failed: %+v", failures)
	}
	if len(desired) != 1 {
		t.Fatalf("collator identity created a redundant protocol-1 observer: %d sessions", len(desired))
	}
	for _, session := range desired {
		if session.config.Identity.Validator == nil || session.config.Identity.ADNLID != validatorADNL {
			t.Fatalf("remaining identity = %+v, want validator", session.config.Identity)
		}
	}
}

func TestSessionSupervisorOnlyRunsTransportAvailableObservers(t *testing.T) {
	localKey := supervisorTestID(0x26)
	availableADNL := supervisorTestID(0xb6)
	unavailableADNL := supervisorTestID(0xb7)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		keys,
		newValidatorTestStorage(),
		newSupervisorTestPreparer().prepare,
		availableADNL,
	)
	defer supervisor.Close()

	group := supervisorTestGroup(0x37, supervisorTestShard(), supervisorTestID(0x27))
	snapshot := supervisorTestSnapshot(15, []groups.Session{group}, nil)
	snapshot.PersistentOverlay = []groups.PersistentOverlayMember{
		{ADNL: availableADNL, ValidatorKeyIDs: [][32]byte{localKey}},
		{ADNL: unavailableADNL, ValidatorKeyIDs: [][32]byte{localKey}},
	}
	desired, failures := supervisor.desiredSessions(snapshot)
	if len(failures) != 0 || len(desired) != 1 {
		t.Fatalf("observer desired/failures = %d/%d", len(desired), len(failures))
	}
	for _, session := range desired {
		if session.config.Identity.ADNLID != availableADNL || session.config.Identity.Validator != nil {
			t.Fatalf("available observer identity = %+v", session.config.Identity)
		}
		if !slices.Equal(session.config.OverlayMembers, [][32]byte{availableADNL, unavailableADNL}) {
			t.Fatalf("complete overlay membership = %x", session.config.OverlayMembers)
		}
	}
}

func TestSessionSupervisorProtocolIdentityAndAllCollatorMatrix(t *testing.T) {
	localKey := supervisorTestID(0xc1)
	observerADNL := supervisorTestID(0xc2)
	rosterCollator := supervisorTestID(0xc3)
	foreignCollator := supervisorTestID(0xc4)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		keys,
		newValidatorTestStorage(),
		newSupervisorTestPreparer().prepare,
		observerADNL,
	)
	defer supervisor.Close()

	group := supervisorTestGroup(0xc5, supervisorTestShard(), localKey)
	for protocolVersion := uint8(0); protocolVersion <= simplex.MaxProtocolVersion; protocolVersion++ {
		snapshot := supervisorTestSnapshot(20+uint32(protocolVersion), []groups.Session{group}, nil)
		snapshot.Config.NewConsensus.Shard.ProtocolVersion = protocolVersion
		snapshot.PersistentOverlay = []groups.PersistentOverlayMember{
			{ADNL: localKey, ValidatorKeyIDs: [][32]byte{localKey}},
			{ADNL: observerADNL, ValidatorKeyIDs: [][32]byte{localKey}},
		}
		snapshot.CollatorsByValidator = []groups.CollatorRegistryEntry{
			{ValidatorKeyID: localKey, CollatorADNLIDs: [][32]byte{rosterCollator}},
			{ValidatorKeyID: supervisorTestID(0xcf), CollatorADNLIDs: [][32]byte{foreignCollator}},
		}

		desired, failures := supervisor.desiredSessions(snapshot)
		if len(failures) != 0 {
			t.Fatalf("protocol %d failures = %+v", protocolVersion, failures)
		}
		wantSessions := 2
		if protocolVersion == 0 {
			wantSessions = 1
		}
		if len(desired) != wantSessions {
			t.Fatalf("protocol %d desired sessions = %d, want %d", protocolVersion, len(desired), wantSessions)
		}
		wantAll := [][32]byte{rosterCollator, foreignCollator}
		for _, session := range desired {
			if session.config.Protocol.ProtocolVersion != protocolVersion {
				t.Fatalf("protocol %d projected as %d", protocolVersion, session.config.Protocol.ProtocolVersion)
			}
			if !slices.Equal(session.config.AllCollators, wantAll) {
				t.Fatalf("protocol %d all collators = %x, want %x",
					protocolVersion, session.config.AllCollators, wantAll)
			}
			if len(session.config.CollatorsByValidator) != 1 ||
				session.config.CollatorsByValidator[0].ValidatorKeyID != localKey {
				t.Fatalf("protocol %d delegation registry = %+v",
					protocolVersion, session.config.CollatorsByValidator)
			}
		}
	}
}

func TestSessionSupervisorPreparesPromotesAndRemovesGroups(t *testing.T) {
	localKey := supervisorTestID(0x41)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), preparer.prepare)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	future := supervisorTestGroup(0x51, supervisorTestShard(), localKey)
	active := supervisorTestGroup(0x52, groups.ShardID{Workchain: -1, Shard: math.MinInt64}, localKey)
	supervisor.Reconcile(supervisorTestSnapshot(20, []groups.Session{active}, []groups.Session{future}))
	waitFor(t, func() bool {
		return preparer.sessionCount(future.ID) == 1 && preparer.sessionCount(active.ID) == 1 &&
			preparer.session(future.ID, 0).recoverCount() == 1 &&
			preparer.session(active.ID, 0).recoverCount() == 1 &&
			preparer.session(active.ID, 0).runCount() == 1
	}, "active and future sessions were not created")

	futureSession := preparer.session(future.ID, 0)
	activeSession := preparer.session(active.ID, 0)
	if futureSession.runCount() != 0 {
		t.Fatal("future session started before promotion")
	}
	if activeSession.config.Protocol.SlotsPerLeaderWindow != 9 {
		t.Fatalf("masterchain config slots = %d, want 9", activeSession.config.Protocol.SlotsPerLeaderWindow)
	}

	supervisor.Reconcile(supervisorTestSnapshot(21, []groups.Session{future, active}, nil))
	waitFor(t, func() bool { return futureSession.runCount() == 1 }, "future session was not promoted")
	if got := preparer.sessionCount(future.ID); got != 1 {
		t.Fatalf("promotion recreated session %d times", got)
	}

	supervisor.Reconcile(supervisorTestSnapshot(22, []groups.Session{future}, nil))
	waitFor(t, activeSession.isClosed, "removed active session was not closed")
	waitFor(t, activeSession.isRetired, "removed active session was not retired")

	supervisor.Reconcile(supervisorTestSnapshot(23, nil, nil))
	waitFor(t, futureSession.isClosed, "remaining session was not closed")
	waitFor(t, futureSession.isRetired, "remaining session was not retired")
	if !futureSession.prepareContextCanceled() || !activeSession.prepareContextCanceled() ||
		!futureSession.runContextCanceled() || !activeSession.runContextCanceled() {
		t.Fatal("removed session context stayed active")
	}
}

func TestSessionSupervisorDoesNotRecoverTentativeGroupWithoutGenesis(t *testing.T) {
	localKey := supervisorTestID(0x53)
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		&supervisorTestKeys{ids: [][32]byte{localKey}},
		newValidatorTestStorage(),
		preparer.prepare,
	)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	future := supervisorTestGroup(0x54, supervisorTestShard(), localKey)
	future.Genesis = nil
	supervisor.Reconcile(supervisorTestSnapshot(20, nil, []groups.Session{future}))
	waitFor(t, func() bool { return preparer.sessionCount(future.ID) == 1 }, "future session was not prepared")

	session := preparer.session(future.ID, 0)
	if session.recoverCount() != 0 {
		t.Fatal("tentative session recovered before its genesis became available")
	}
	if session.runCount() != 0 {
		t.Fatal("tentative session ran before promotion")
	}
}

func TestSessionSupervisorUpdatesParamsAndPinsCriticalRuntime(t *testing.T) {
	localKey := supervisorTestID(0x61)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), preparer.prepare)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	group := supervisorTestGroup(0x62, supervisorTestShard(), localKey)
	first := supervisorTestSnapshot(30, []groups.Session{group}, nil)
	first.Config.NewConsensus.Shard.NoncriticalParams = []groups.NoncriticalParam{{ID: 0, Value: 500}}
	supervisor.Reconcile(first)
	waitFor(t, func() bool {
		return preparer.sessionCount(group.ID) == 1 && preparer.session(group.ID, 0).runCount() == 1
	}, "initial session did not start")
	initial := preparer.session(group.ID, 0)

	updatedGroup := group
	updatedGroup.Registered = []groups.ShardDescription{{Shard: group.Shard}}
	finalized := ton.BlockIDExt{Workchain: group.Shard.Workchain, Shard: group.Shard.Shard, SeqNo: 77}
	updatedGroup.FinalizedBlock = &finalized
	updated := supervisorTestSnapshot(31, []groups.Session{updatedGroup}, nil)
	updated.Config.NewConsensus.Shard.NoncriticalParams = []groups.NoncriticalParam{{ID: 0, Value: 900}}
	supervisor.Reconcile(updated)
	waitFor(t, func() bool {
		_, stateUpdated := initial.latestState()
		return initial.updateCount() == 1 && stateUpdated
	}, "noncritical params and group context were not updated")
	if got := preparer.sessionCount(group.ID); got != 1 {
		t.Fatalf("noncritical update recreated session %d times", got)
	}
	latestState, ok := initial.latestState()
	if !ok || latestState.MasterchainBlock.SeqNo != 31 ||
		latestState.Params.TargetRate != 900*time.Millisecond || len(latestState.Registered) != 1 ||
		latestState.FinalizedBlock == nil || latestState.FinalizedBlock.SeqNo != 77 {
		t.Fatalf("latest session state = %+v", latestState)
	}

	critical := supervisorTestSnapshot(32, []groups.Session{group}, nil)
	critical.Config.NewConsensus.Shard.SlotsPerLeaderWindow++
	supervisor.Reconcile(critical)
	waitFor(t, func() bool {
		return initial.updateCount() == 2
	}, "state accompanying a critical config change was not applied")
	if initial.isClosed() || preparer.sessionCount(group.ID) != 1 {
		t.Fatal("critical config change recreated the same consensus session")
	}
	if got := initial.config.Protocol.SlotsPerLeaderWindow; got != first.Config.NewConsensus.Shard.SlotsPerLeaderWindow {
		t.Fatalf("critical slots per leader window = %d, want pinned %d", got, first.Config.NewConsensus.Shard.SlotsPerLeaderWindow)
	}

	overlayChanged := supervisorTestSnapshot(33, []groups.Session{group}, nil)
	overlayChanged.Config.NewConsensus.Shard.SlotsPerLeaderWindow++
	overlayChanged.PersistentOverlay = []groups.PersistentOverlayMember{{ADNL: supervisorTestID(0x99)}}
	supervisor.Reconcile(overlayChanged)
	waitFor(t, func() bool {
		return initial.isClosed() && preparer.sessionCount(group.ID) == 2
	}, "overlay membership change did not recreate the runtime")
	second := preparer.latestSession(group.ID)
	if got := second.config.Protocol.SlotsPerLeaderWindow; got != first.Config.NewConsensus.Shard.SlotsPerLeaderWindow {
		t.Fatalf("recreated runtime critical slots = %d, want pinned %d", got, first.Config.NewConsensus.Shard.SlotsPerLeaderWindow)
	}
}

func TestSessionSupervisorRetriesUnavailableStateWithoutRetiringSession(t *testing.T) {
	// A full retry cycle of the production constants is what this asserts, so it
	// spends its second asleep in the supervisor's retry delay rather than
	// computing, and running it beside its siblings costs almost nothing.
	//
	// Its deadlines are upper bounds, so contention is not free here; they are
	// budgeted for it. One second waits for a preparation that only pushes to a
	// channel, and three seconds wait for a retry the production constant fires
	// after one. Nothing here asserts that something has not happened yet, which
	// is the assertion a late-scheduled goroutine turns into a flake.
	t.Parallel()
	localKey := supervisorTestID(0x69)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	sessions := make(chan *supervisorNotReadyUpdateSession, 1)
	preparations := atomic.Int32{}
	prepare := func(
		ctx context.Context,
		config SessionConfig,
		initial SessionState,
		_ SessionStart,
	) (SessionRuntime, error) {
		preparations.Add(1)
		session := &supervisorNotReadyUpdateSession{supervisorTestSession: &supervisorTestSession{
			prepareCtx: ctx,
			config:     config,
			initial:    initial,
		}}
		sessions <- session

		return session, nil
	}
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), prepare)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	group := supervisorTestGroup(0x6a, supervisorTestShard(), localKey)
	supervisor.Reconcile(supervisorTestSnapshot(100, []groups.Session{group}, nil))
	var session *supervisorNotReadyUpdateSession
	select {
	case session = <-sessions:
	case <-time.After(time.Second):
		t.Fatal("initial session was not prepared")
	}
	waitFor(t, func() bool { return session.runCount() == 1 }, "initial session did not start")

	supervisor.Reconcile(supervisorTestSnapshot(101, []groups.Session{group}, nil))
	waitForSupervisor(t, 3*time.Second, func() bool {
		state, updated := session.latestState()
		return session.updateAttempts.Load() >= 2 && updated && state.MasterchainBlock.SeqNo == 101
	}, "unavailable session state was not retried")

	if session.isClosed() || preparations.Load() != 1 {
		t.Fatalf("transient update closed/recreated session: closed=%t preparations=%d", session.isClosed(), preparations.Load())
	}
}

func TestSessionSupervisorRejectsAmbiguousAndUnsupportedSessions(t *testing.T) {
	firstKey := supervisorTestID(0x71)
	secondKey := supervisorTestID(0x72)
	keys := &supervisorTestKeys{ids: [][32]byte{firstKey, secondKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), preparer.prepare)
	defer supervisor.Close()

	ambiguous := supervisorTestGroup(0x73, supervisorTestShard(), firstKey, secondKey)
	desired, failures := supervisor.desiredSessions(supervisorTestSnapshot(40, []groups.Session{ambiguous}, nil))
	if len(desired) != 0 || len(failures) != 1 {
		t.Fatalf("ambiguous group desired/failures = %d/%d", len(desired), len(failures))
	}
	if got := failures[0].reason; got != SessionSpecRejectionInvalidSessionSpec {
		t.Fatalf("ambiguous session rejection reason = %d, want invalid session specification", got)
	}

	unsupported := supervisorTestSnapshot(41, []groups.Session{
		supervisorTestGroup(0x74, supervisorTestShard(), firstKey),
	}, nil)
	unsupported.Config.NewConsensus.Shard.ProtocolVersion = simplex.MaxProtocolVersion + 1
	desired, failures = supervisor.desiredSessions(unsupported)
	if len(desired) != 0 || len(failures) != 1 {
		t.Fatalf("unsupported group desired/failures = %d/%d", len(desired), len(failures))
	}
	if got := failures[0].reason; got != SessionSpecRejectionUnsupportedSimplexProtocol {
		t.Fatalf("unsupported session rejection reason = %d, want unsupported simplex protocol", got)
	}

	disabled := supervisorTestSnapshot(42, []groups.Session{ambiguous}, nil)
	disabled.LifecycleEnabled = false
	desired, failures = supervisor.desiredSessions(disabled)
	if len(desired) != 0 || len(failures) != 0 {
		t.Fatalf("disabled lifecycle desired/failures = %d/%d", len(desired), len(failures))
	}
}

func TestSessionSupervisorRetriesPreparationAndIgnoresOlderSnapshots(t *testing.T) {
	// A full retry cycle of the production constants is what this asserts, so it
	// spends its second asleep rather than computing.
	//
	// Deliberately not parallel. The assertion after the 100 ms sleep is that
	// the retry has not happened yet, and the deadline it is racing is the
	// production one second: a test goroutine descheduled for the remaining
	// 900 ms fails it by arriving late rather than by the supervisor being
	// wrong.
	localKey := supervisorTestID(0x81)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	group := supervisorTestGroup(0x82, supervisorTestShard(), localKey)
	preparer.failNext(group.ID, 1)

	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), preparer.prepare)
	supervisor.Start(context.Background())
	supervisor.Reconcile(supervisorTestSnapshot(50, []groups.Session{group}, nil))
	waitFor(t, func() bool { return preparer.attemptCount(group.ID) == 1 }, "first preparation was not attempted")

	for seqno := uint32(51); seqno <= 60; seqno++ {
		supervisor.Reconcile(supervisorTestSnapshot(seqno, []groups.Session{group}, nil))
	}
	time.Sleep(100 * time.Millisecond)
	if got := preparer.attemptCount(group.ID); got != 1 {
		t.Fatalf("unchanged snapshots bypassed retry deadline: %d attempts", got)
	}

	waitForSupervisor(t, 4*time.Second, func() bool {
		return preparer.attemptCount(group.ID) >= 2 && preparer.sessionCount(group.ID) == 1 &&
			preparer.session(group.ID, 0).runCount() == 1
	}, "failed preparation was not retried")

	other := supervisorTestGroup(0x83, supervisorTestShard(), localKey)
	supervisor.Reconcile(supervisorTestSnapshot(60, []groups.Session{other}, nil))
	supervisor.Reconcile(supervisorTestSnapshot(59, []groups.Session{other}, nil))
	supervisor.Close()

	if got := preparer.attemptCount(group.ID); got != 2 {
		t.Fatalf("accepted session preparation attempts = %d, want 2", got)
	}
	if got := preparer.attemptCount(other.ID); got != 0 {
		t.Fatalf("older/equal snapshot prepared replacement %d times", got)
	}
}

func TestSessionSupervisorBoundsConcurrentPreparationAndPrioritizesActiveSessions(t *testing.T) {
	const sessionCount = maxConcurrentSessionPreparations + 2

	localKey := supervisorTestID(0x84)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	prepared := newSupervisorTestPreparer()
	started := make(chan [32]byte, sessionCount)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	var inFlight atomic.Int32
	var peak atomic.Int32
	prepare := func(
		ctx context.Context,
		config SessionConfig,
		initial SessionState,
		start SessionStart,
	) (SessionRuntime, error) {
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
		}
		started <- config.SessionID

		select {
		case <-release:
			return prepared.prepare(ctx, config, initial, start)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), prepare)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	groupsToPrepare := make([]groups.Session, 0, sessionCount)
	for i := 0; i < sessionCount; i++ {
		groupsToPrepare = append(groupsToPrepare, supervisorTestGroup(byte(0x85+i), supervisorTestShard(), localKey))
	}
	active := groupsToPrepare[len(groupsToPrepare)-1]
	supervisor.Reconcile(supervisorTestSnapshot(55, []groups.Session{active}, groupsToPrepare[:len(groupsToPrepare)-1]))

	activeStarted := false
	for i := 0; i < maxConcurrentSessionPreparations; i++ {
		select {
		case id := <-started:
			activeStarted = activeStarted || id == active.ID
		case <-time.After(time.Second):
			t.Fatal("concurrent session preparation did not start")
		}
	}
	if !activeStarted {
		t.Fatal("active session was queued behind future sessions")
	}
	select {
	case id := <-started:
		t.Fatalf("session %x exceeded the preparation concurrency limit", id)
	case <-time.After(100 * time.Millisecond):
	}
	if got := peak.Load(); got != maxConcurrentSessionPreparations {
		t.Fatalf("peak concurrent preparations = %d, want %d", got, maxConcurrentSessionPreparations)
	}

	releaseAll()
	waitFor(t, func() bool {
		for i := range groupsToPrepare {
			if prepared.sessionCount(groupsToPrepare[i].ID) != 1 {
				return false
			}
		}
		return true
	}, "queued session preparations did not complete")
}

func TestSessionSupervisorUpdatesStateChangedDuringPreparation(t *testing.T) {
	localKey := supervisorTestID(0x94)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	group := supervisorTestGroup(0x95, supervisorTestShard(), localKey)
	marker := supervisorTestGroup(0x96, supervisorTestShard(), localKey)
	prepared := newSupervisorTestPreparer()
	started := make(chan struct{})
	updateObserved := make(chan struct{})
	release := make(chan struct{})
	prepare := func(
		ctx context.Context,
		config SessionConfig,
		initial SessionState,
		start SessionStart,
	) (SessionRuntime, error) {
		if config.SessionID == marker.ID {
			close(updateObserved)
			return prepared.prepare(ctx, config, initial, start)
		}

		close(started)
		select {
		case <-release:
			return prepared.prepare(ctx, config, initial, start)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), prepare)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	initial := supervisorTestSnapshot(56, []groups.Session{group}, nil)
	initial.Config.NewConsensus.Shard.NoncriticalParams = []groups.NoncriticalParam{{ID: 0, Value: 100}}
	supervisor.Reconcile(initial)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("session preparation did not start")
	}

	updated := supervisorTestSnapshot(57, []groups.Session{group}, []groups.Session{marker})
	updated.Config.NewConsensus.Shard.NoncriticalParams = []groups.NoncriticalParam{{ID: 0, Value: 200}}
	supervisor.Reconcile(updated)
	select {
	case <-updateObserved:
	case <-time.After(time.Second):
		t.Fatal("updated snapshot was not reconciled while another session prepared")
	}
	close(release)

	waitFor(t, func() bool {
		return prepared.sessionCount(group.ID) == 1 && prepared.session(group.ID, 0).runCount() == 1 &&
			prepared.session(group.ID, 0).updateCount() == 1
	}, "state changed during preparation was not applied before activation")
	session := prepared.session(group.ID, 0)
	state, ok := session.latestState()
	if session.initial.Params.TargetRate != 100*time.Millisecond || !ok || state.Params.TargetRate != 200*time.Millisecond ||
		!session.updatedBeforeRun() {
		t.Fatalf("initial/latest target rate = %s/%s", session.initial.Params.TargetRate, state.Params.TargetRate)
	}
}

func TestSessionSupervisorStatusPublishesDetailedDeterministicSnapshot(t *testing.T) {
	localKey := supervisorTestID(0xa1)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	storage := newValidatorTestStorage()
	prepared := newSupervisorTestPreparer()
	started := make(chan [32]byte, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }

	prepare := func(
		ctx context.Context,
		config SessionConfig,
		initial SessionState,
		start SessionStart,
	) (SessionRuntime, error) {
		started <- config.SessionID
		select {
		case <-release:
			return prepared.prepare(ctx, config, initial, start)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, storage, prepare)
	defer supervisor.Close()
	defer releaseAll()

	future := supervisorTestGroup(0xb1, supervisorTestShard(), localKey)
	active := supervisorTestGroup(0xb2, groups.ShardID{Workchain: -1, Shard: math.MinInt64}, localKey)
	supervisor.Reconcile(supervisorTestSnapshot(80, []groups.Session{active}, []groups.Session{future}))
	beforeStart := supervisor.Status()
	if beforeStart.Started || beforeStart.Closed || beforeStart.LatestMasterchainSeqno != 80 {
		t.Fatalf("status before start = %+v", beforeStart)
	}

	supervisor.Start(context.Background())
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("session preparation did not start")
		}
	}
	waitFor(t, func() bool {
		status := supervisor.Status()
		return status.Desired == 2 && status.Preparing == 2 && len(status.Sessions) == 2
	}, "preparing status was not published")

	preparing := supervisor.Status()
	if !preparing.Started || preparing.Closed || preparing.LatestMasterchainSeqno != 80 ||
		preparing.Prepared != 0 || preparing.Running != 0 {
		t.Fatalf("preparing supervisor status = %+v", preparing)
	}
	if preparing.Sessions[0].SessionID != future.ID || preparing.Sessions[1].SessionID != active.ID {
		t.Fatalf("session status order = %x, %x", preparing.Sessions[0].SessionID, preparing.Sessions[1].SessionID)
	}
	for i := range preparing.Sessions {
		session := preparing.Sessions[i]
		if session.Phase != SessionPhasePreparing || session.LocalIndex != 0 || session.ADNLID != localKey ||
			session.StorageID.SessionID != session.SessionID || !session.StorageID.IsValidator ||
			session.StorageID.ValidatorKeyID != localKey || session.StorageID.ValidatorIndex != 0 {
			t.Fatalf("preparing session %d = %+v", i, session)
		}
	}

	preparing.Sessions[0].Phase = SessionPhaseRetry
	if got := supervisor.Status().Sessions[0].Phase; got != SessionPhasePreparing {
		t.Fatalf("caller mutated published status phase to %q", got)
	}

	releaseAll()
	waitFor(t, func() bool {
		status := supervisor.Status()
		return status.Preparing == 0 && status.Prepared == 1 && status.Running == 1
	}, "prepared/running status was not published")

	ready := supervisor.Status()
	if ready.Sessions[0].Phase != SessionPhasePrepared || ready.Sessions[0].Active ||
		ready.Sessions[1].Phase != SessionPhaseRunning || !ready.Sessions[1].Active {
		t.Fatalf("ready session phases = %+v", ready.Sessions)
	}
	for _, session := range prepared.sessionsFor(future.ID) {
		if session.config.Journal == nil || session.config.StorageID != ready.Sessions[0].StorageID {
			t.Fatalf("future storage config = %+v", session.config.StorageID)
		}
	}

	supervisor.Close()
	closed := supervisor.Status()
	if !closed.Closed || closed.Preparing != 0 || closed.Prepared != 0 || closed.Running != 0 {
		t.Fatalf("closed supervisor status = %+v", closed)
	}
}

func TestSessionSupervisorRetriesFailedRuntimeCloseBeforeReplacement(t *testing.T) {
	// A full retry cycle of the production constants is what this asserts, so it
	// spends its second asleep rather than computing.
	//
	// Deliberately not parallel. The 500 ms budget below is load-bearing: it has
	// to expire before the one-second close retry, because the assertions that
	// follow it are that the close has been attempted exactly once and no
	// replacement prepared. That is an assertion about something not having
	// happened yet, and contention satisfies it by arriving late.
	localKey := supervisorTestID(0xd1)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	failedClose := newSupervisorRetryCloseSession()
	replacements := newSupervisorTestPreparer()
	var prepareAttempts atomic.Int32
	prepare := func(
		ctx context.Context,
		config SessionConfig,
		initial SessionState,
		start SessionStart,
	) (SessionRuntime, error) {
		if prepareAttempts.Add(1) == 1 {
			return failedClose, nil
		}

		return replacements.prepare(ctx, config, initial, start)
	}

	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), prepare)
	supervisor.Start(context.Background())
	t.Cleanup(supervisor.Close)

	group := supervisorTestGroup(0xd2, supervisorTestShard(), localKey)
	supervisor.Reconcile(supervisorTestSnapshot(82, []groups.Session{group}, nil))
	select {
	case <-failedClose.started:
	case <-time.After(time.Second):
		t.Fatal("initial runtime did not start")
	}

	rotated := supervisorTestSnapshot(83, []groups.Session{group}, nil)
	rotated.PersistentOverlay = []groups.PersistentOverlayMember{{ADNL: supervisorTestID(0xd3)}}
	supervisor.Reconcile(rotated)
	select {
	case <-failedClose.firstClose:
	case <-time.After(time.Second):
		t.Fatal("initial runtime was not closed during rotation")
	}

	waitForSupervisor(t, 500*time.Millisecond, func() bool {
		status := supervisor.Status()
		return status.Running == 0 && status.Prepared == 0 && len(status.Sessions) == 1 &&
			status.Sessions[0].Phase == SessionPhaseRetry && !status.Sessions[0].RetryAt.IsZero()
	}, "failed runtime close was not retained for retry")
	if calls := failedClose.closeCalls.Load(); calls != 1 {
		t.Fatalf("close calls before retry = %d, want 1", calls)
	}
	if attempts := prepareAttempts.Load(); attempts != 1 {
		t.Fatalf("preparation attempts before close retry = %d, want 1", attempts)
	}

	waitForSupervisor(t, 3*time.Second, func() bool {
		return failedClose.closed.Load() && failedClose.closeCalls.Load() == 2 &&
			prepareAttempts.Load() == 2 && replacements.sessionCount(group.ID) == 1 &&
			replacements.session(group.ID, 0).runCount() == 1
	}, "runtime close was not retried before preparing its replacement")
	if runs := failedClose.runs.Load(); runs != 1 {
		t.Fatalf("old runtime run count = %d, want 1", runs)
	}
}

func TestSessionSupervisorStatusReportsRetryDeadline(t *testing.T) {
	localKey := supervisorTestID(0xc1)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	runtime := newSupervisorFailingSession()
	prepare := func(context.Context, SessionConfig, SessionState, SessionStart) (SessionRuntime, error) {
		return runtime, nil
	}
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), prepare)
	supervisor.Start(context.Background())
	defer supervisor.Close()

	group := supervisorTestGroup(0xc2, supervisorTestShard(), localKey)
	supervisor.Reconcile(supervisorTestSnapshot(81, []groups.Session{group}, nil))
	select {
	case <-runtime.started:
	case <-time.After(time.Second):
		t.Fatal("failing runtime did not start")
	}
	waitFor(t, func() bool { return supervisor.Status().Running == 1 }, "running status was not published")

	failedAt := time.Now()
	runtime.result <- errors.New("session failed")
	waitFor(t, func() bool {
		status := supervisor.Status()
		return status.Running == 0 && len(status.Sessions) == 1 && status.Sessions[0].Phase == SessionPhaseRetry
	}, "retry status was not published")

	status := supervisor.Status()
	if status.Sessions[0].RetryAt.Before(failedAt) || status.Sessions[0].RetryAt.IsZero() || !runtime.closed.Load() {
		t.Fatalf("retry session status = %+v, closed=%t", status.Sessions[0], runtime.closed.Load())
	}
	if runtime.retired.Load() {
		t.Fatal("failed still-desired runtime was permanently retired")
	}
}

func TestSessionSupervisorCloseRacesWithReconcile(t *testing.T) {
	localKey := supervisorTestID(0x91)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	supervisor := newSessionSupervisor(zerolog.Nop(), keys, newValidatorTestStorage(), preparer.prepare)
	supervisor.Start(context.Background())

	group := supervisorTestGroup(0x92, supervisorTestShard(), localKey)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for seqno := uint32(1); seqno <= 100; seqno++ {
			supervisor.Reconcile(supervisorTestSnapshot(seqno, []groups.Session{group}, nil))
		}
	}()
	go func() {
		defer wg.Done()
		supervisor.Close()
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			status := supervisor.Status()
			if len(status.Sessions) > 0 {
				status.Sessions[0].Phase = SessionPhaseRetry
			}
		}
	}()
	wg.Wait()

	supervisor.Reconcile(supervisorTestSnapshot(101, []groups.Session{group}, nil))
	supervisor.Close()
}

func supervisorTestID(value byte) [32]byte {
	var id [32]byte
	id[0] = value
	return id
}

func supervisorTestShard() groups.ShardID {
	return groups.ShardID{Workchain: 0, Shard: math.MinInt64}
}

func supervisorTestGroup(id byte, shard groups.ShardID, validatorIDs ...[32]byte) groups.Session {
	validators := make([]groups.Validator, len(validatorIDs))
	for i := range validatorIDs {
		validators[i] = groups.Validator{PublicKeyHash: validatorIDs[i], Weight: uint64(i + 1)}
	}

	return groups.Session{
		ID:               supervisorTestID(id),
		Shard:            shard,
		CatchainSeqno:    7,
		ValidatorSetHash: 8,
		Validators:       validators,
		Genesis: []ton.BlockIDExt{{
			Workchain: shard.Workchain,
			Shard:     shard.Shard,
			SeqNo:     1,
		}},
		MinMasterchain: ton.BlockIDExt{
			Workchain: -1,
			Shard:     math.MinInt64,
			SeqNo:     1,
		},
	}
}

func supervisorTestSnapshot(seqno uint32, active, future []groups.Session) *groups.Snapshot {
	masterchain := groups.SimplexConfig{
		Version:              2,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: 9,
	}
	shard := groups.SimplexConfig{
		Version:              2,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: 4,
	}

	return &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{
			Workchain: -1,
			Shard:     math.MinInt64,
			SeqNo:     seqno,
		},
		Ready:            true,
		LifecycleEnabled: true,
		Config: &groups.Config{NewConsensus: groups.NewConsensusConfig{
			Masterchain: &masterchain,
			Shard:       &shard,
		}},
		Active: active,
		Future: future,
	}
}

func TestSessionCollatorRegistryExcludesObserverAuthorities(t *testing.T) {
	local := supervisorTestID(0x81)
	foreign := supervisorTestID(0x82)
	localCollator := supervisorTestID(0x91)
	foreignCollator := supervisorTestID(0x92)

	got := sessionCollatorRegistry([]groups.CollatorRegistryEntry{
		{ValidatorKeyID: local, CollatorADNLIDs: [][32]byte{localCollator}},
		{ValidatorKeyID: foreign, CollatorADNLIDs: [][32]byte{foreignCollator}},
	}, []groups.Validator{{PublicKeyHash: local}})
	if len(got) != 1 || got[0].ValidatorKeyID != local ||
		len(got[0].CollatorADNLIDs) != 1 || got[0].CollatorADNLIDs[0] != localCollator {
		t.Fatalf("filtered collator registry = %+v", got)
	}
}

func TestSessionSupervisorOmitsMasterchainCollatorRegistry(t *testing.T) {
	localKey := supervisorTestID(0x83)
	collatorADNL := supervisorTestID(0x93)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		keys,
		newValidatorTestStorage(),
		newSupervisorTestPreparer().prepare,
	)
	defer supervisor.Close()

	registry := []groups.CollatorRegistryEntry{{
		ValidatorKeyID:  localKey,
		CollatorADNLIDs: [][32]byte{collatorADNL},
	}}
	masterchain := supervisorTestGroup(
		0x84,
		groups.ShardID{Workchain: -1, Shard: math.MinInt64},
		localKey,
	)
	masterSnapshot := supervisorTestSnapshot(102, []groups.Session{masterchain}, nil)
	masterSnapshot.CollatorsByValidator = registry
	masterDesired, failures := supervisor.desiredSessions(masterSnapshot)
	if len(failures) != 0 || len(masterDesired) != 1 {
		t.Fatalf("masterchain desired/failures = %d/%d", len(masterDesired), len(failures))
	}
	for _, session := range masterDesired {
		if len(session.config.CollatorsByValidator) != 0 {
			t.Fatalf("masterchain collator registry = %+v", session.config.CollatorsByValidator)
		}
		if !slices.Equal(session.config.AllCollators, [][32]byte{collatorADNL}) {
			t.Fatalf("masterchain all-collator roster = %x", session.config.AllCollators)
		}
	}

	basechain := supervisorTestGroup(0x85, supervisorTestShard(), localKey)
	baseSnapshot := supervisorTestSnapshot(103, []groups.Session{basechain}, nil)
	baseSnapshot.CollatorsByValidator = registry
	baseDesired, failures := supervisor.desiredSessions(baseSnapshot)
	if len(failures) != 0 || len(baseDesired) != 1 {
		t.Fatalf("basechain desired/failures = %d/%d", len(baseDesired), len(failures))
	}
	for _, session := range baseDesired {
		if !slices.EqualFunc(
			session.config.CollatorsByValidator,
			registry,
			collatorRegistryEntryEqual,
		) {
			t.Fatalf("basechain collator registry = %+v", session.config.CollatorsByValidator)
		}
		if !slices.Equal(session.config.AllCollators, [][32]byte{collatorADNL}) {
			t.Fatalf("basechain all-collator roster = %x", session.config.AllCollators)
		}
	}
}

func TestSessionConfigChangedObservesTransportRosters(t *testing.T) {
	previous := SessionConfig{
		AllCollators:         [][32]byte{{1}},
		AllCurrentValidators: [][32]byte{{2}},
	}
	next := SessionConfig{
		AllCollators:         [][32]byte{{1}},
		AllCurrentValidators: [][32]byte{{2}},
	}
	if sessionConfigChanged(previous, next) {
		t.Fatal("equal transport rosters changed the session config")
	}
	next.AllCollators = [][32]byte{{2}}
	if !sessionConfigChanged(previous, next) {
		t.Fatal("all-collator roster change was ignored")
	}
	next = previous
	next.AllCurrentValidators = [][32]byte{{3}}
	if !sessionConfigChanged(previous, next) {
		t.Fatal("current-validator intermediate roster change was ignored")
	}
}

func waitForSupervisor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatal(message)
}

// TestSessionConfigRoundTripsThroughCollatorSession pins the two
// opposite-direction hand projections of one consensus-group descriptor against
// each other. buildDesiredSession turns a groups.Session into SessionConfig,
// localCollatorSession turns that into the collator.Session an in-process
// collator sees, and ConsensusObserver.runtimeInput turns a collator.Session
// back into SessionConfig for a delegating observer. Every hop enumerates the
// same identity and protocol fields by hand, so a field added to one and missed
// by another is a zero value at run time rather than a compile error.
func TestSessionConfigRoundTripsThroughCollatorSession(t *testing.T) {
	localADNL := supervisorTestID(0x71)
	validator := groups.Validator{
		PublicKey: supervisorTestID(0x41),
		ADNL:      supervisorTestID(0x42),
		Weight:    11,
	}
	keyID, err := groups.PublicKeyHash(validator.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	validator.PublicKeyHash = keyID

	// Every protocol field carries a distinct non-zero value so a dropped one
	// shows up as a mismatch instead of coinciding with the zero value.
	consensus := groups.SimplexConfig{
		Version:              2,
		Flags:                5,
		ProtocolVersion:      3,
		UseQUIC:              true,
		SlotsPerLeaderWindow: 6,
	}
	group := supervisorTestGroup(0x21, supervisorTestShard(), validator.PublicKeyHash)
	group.Validators = []groups.Validator{validator}
	snapshot := supervisorTestSnapshot(3, []groups.Session{group}, nil)
	snapshot.Config.NewConsensus.Shard = &consensus
	snapshot.Config.MaxBlockSize = 1 << 20
	snapshot.Config.MaxCollatedDataSize = 1 << 19
	snapshot.Config.AllCurrentValidators = [][32]byte{validator.ADNL, supervisorTestID(0x43)}
	snapshot.CollatorsByValidator = []groups.CollatorRegistryEntry{{
		ValidatorKeyID:  validator.PublicKeyHash,
		CollatorADNLIDs: [][32]byte{localADNL},
	}}

	supervisor := newSessionSupervisor(
		zerolog.Nop(),
		&supervisorTestKeys{},
		newValidatorTestStorage(),
		newSupervisorTestPreparer().prepare,
	)
	overlayMembers := [][32]byte{validator.ADNL, localADNL}
	desired, err := supervisor.buildDesiredSession(
		snapshot,
		group,
		overlayMembers,
		groups.AllCollatorADNLIDs(snapshot.CollatorsByValidator),
		simplex.ObserverIndex,
		[32]byte{},
		localADNL,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	config := desired.config

	observer := &ConsensusObserver{localADNLID: localADNL}
	input, err := observer.runtimeInput(collator.ConsensusObserverSession{
		Overlay: collator.OverlaySession{
			Session:                   localCollatorSession(config),
			Role:                      collator.OverlayRoleCollator,
			CollatorsByValidator:      config.CollatorsByValidator,
			AllCollators:              config.AllCollators,
			AllCurrentValidators:      config.AllCurrentValidators,
			AllOverlayNodes:           config.OverlayMembers,
			MaxBlockSize:              config.CandidateLimits.MaxBlockBytes,
			MaxCollatedDataSize:       config.CandidateLimits.MaxCollatedDataBytes,
			BroadcastMode:             collator.CandidateBroadcastPrivateOverlay,
			ObserversInPrivateOverlay: true,
		},
		Update: collator.SessionUpdate{
			SessionID:        config.SessionID,
			TargetRate:       desired.state.Params.TargetRate,
			MasterchainBlock: snapshot.MasterchainBlock,
		},
		Params: desired.state.Params,
	})
	if err != nil {
		t.Fatal(err)
	}

	got := input.config
	if got.Protocol != config.Protocol {
		t.Fatalf("round-tripped protocol = %+v, want %+v", got.Protocol, config.Protocol)
	}
	if got.SessionID != config.SessionID {
		t.Fatalf("round-tripped session id = %x, want %x", got.SessionID, config.SessionID)
	}
	if got.Shard != config.Shard {
		t.Fatalf("round-tripped shard = %+v, want %+v", got.Shard, config.Shard)
	}
	if got.CatchainSeqno != config.CatchainSeqno {
		t.Fatalf("round-tripped catchain seqno = %d, want %d", got.CatchainSeqno, config.CatchainSeqno)
	}
	if got.ValidatorSetHash != config.ValidatorSetHash {
		t.Fatalf("round-tripped validator set hash = %d, want %d", got.ValidatorSetHash, config.ValidatorSetHash)
	}
	if got.ShardPrefixLen != config.ShardPrefixLen {
		t.Fatalf("round-tripped shard prefix len = %d, want %d", got.ShardPrefixLen, config.ShardPrefixLen)
	}
	if got.CandidateLimits != config.CandidateLimits {
		t.Fatalf("round-tripped candidate limits = %+v, want %+v", got.CandidateLimits, config.CandidateLimits)
	}
	if !slices.Equal(got.Validators, config.Validators) {
		t.Fatalf("round-tripped validators = %+v, want %+v", got.Validators, config.Validators)
	}
	if !slices.Equal(got.AllCurrentValidators, config.AllCurrentValidators) {
		t.Fatalf(
			"round-tripped current-validator roster = %x, want %x",
			got.AllCurrentValidators,
			config.AllCurrentValidators,
		)
	}
}
