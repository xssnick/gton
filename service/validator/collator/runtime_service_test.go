package collator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/blockstats"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

type runtimeTestKeys struct {
	private ed25519.PrivateKey
	public  ed25519.PublicKey
	id      [32]byte
}

func newRuntimeTestKeys(t *testing.T) *runtimeTestKeys {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &runtimeTestKeys{private: private, public: public, id: simplex.KeyNodeIDShort(public)}
}

func (k *runtimeTestKeys) KeyIDs() [][32]byte { return [][32]byte{k.id} }

func (k *runtimeTestKeys) PublicKeyFor(id [32]byte) (ed25519.PublicKey, error) {
	if id != k.id {
		return nil, ErrNotFound
	}
	return append(ed25519.PublicKey(nil), k.public...), nil
}

func (k *runtimeTestKeys) Sign(id [32]byte, payload []byte) ([]byte, error) {
	if id != k.id {
		return nil, ErrNotFound
	}
	return ed25519.Sign(k.private, payload), nil
}

type runtimePrivateSigner ed25519.PrivateKey

func (s runtimePrivateSigner) Sign(payload []byte) ([]byte, error) {
	return ed25519.Sign(ed25519.PrivateKey(s), payload), nil
}

type runtimeLockObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *runtimeLockObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })

	return c.Context.Done()
}

type runtimeTestPipeline struct {
	mu       sync.Mutex
	prepare  func(context.Context, Session, SessionUpdate) error
	activate func(context.Context, SessionActivation, SessionUpdate) error
	update   func(context.Context, Session, SessionUpdate) error
	advance  func(context.Context, ConsensusBaseUpdate) error
	retire   func(context.Context, [32]byte) error
	state    func(context.Context, BuildRequest) (CandidateState, error)
	build    func(context.Context, BuildRequest) (*Candidate, error)
	soft     func(context.Context, SoftTimeoutRequest) (SoftTimeoutDecision, error)
	restore  func(context.Context, BuildRequest, CandidateArtifact) error
	// successorPolicy is the empty-block decision a produced block carries into
	// the offer it hands to the next slot. It is separate from state because the
	// two are asked at different moments about different slots: state answers for
	// a slot the producer is about to schedule, this one for a slot a block has
	// just made possible.
	successorPolicy func(BuildRequest, *Candidate) CandidateState
	// successorRoot overrides the predecessor root an offer names. A real
	// collation always names the block it just built; a size-limit retry then
	// rebuilds that block and the root the offer named becomes one no block will
	// ever carry, which is the case this exists to reproduce.
	successorRoot func(BuildRequest, *Candidate) []byte
	commit        func(context.Context, CandidateCommit) error
	prepared      int
	activated     int
	updated       int
	retired       int
	built         int
}

func (p *runtimeTestPipeline) counts() (prepared, activated, updated, retired, built int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.prepared, p.activated, p.updated, p.retired, p.built
}

func (p *runtimeTestPipeline) PrepareSession(ctx context.Context, session Session, update SessionUpdate) error {
	p.mu.Lock()
	p.prepared++
	prepare := p.prepare
	p.mu.Unlock()
	if prepare != nil {
		return prepare(ctx, session, update)
	}
	return nil
}

func (p *runtimeTestPipeline) ActivateSession(
	ctx context.Context,
	activation SessionActivation,
	update SessionUpdate,
) error {
	p.mu.Lock()
	p.activated++
	activate := p.activate
	p.mu.Unlock()
	if activate != nil {
		return activate(ctx, activation, update)
	}

	return nil
}

func (p *runtimeTestPipeline) UpdateSession(ctx context.Context, session Session, update SessionUpdate) error {
	p.mu.Lock()
	p.updated++
	updateFn := p.update
	p.mu.Unlock()
	if updateFn != nil {
		return updateFn(ctx, session, update)
	}
	return nil
}

func (p *runtimeTestPipeline) AdvanceConsensusBase(
	ctx context.Context,
	request ConsensusBaseUpdate,
) error {
	p.mu.Lock()
	p.updated++
	advance := p.advance
	update := p.update
	p.mu.Unlock()
	if advance != nil {
		return advance(ctx, request)
	}
	if update != nil {
		return update(ctx, request.Session.Session, request.Update)
	}

	return nil
}

func (p *runtimeTestPipeline) ResolveCandidateState(
	ctx context.Context,
	request BuildRequest,
) (CandidateState, error) {
	p.mu.Lock()
	state := p.state
	p.mu.Unlock()
	if state != nil {
		return state(ctx, request)
	}
	if request.Previous != nil {
		block := cloneBlockID(request.Previous.Candidate.Block)

		return CandidateState{Block: block, NextSeqno: block.SeqNo + 1}, nil
	}
	if request.Update.HasFinalizedBlock {
		block := cloneBlockID(request.Update.FinalizedBlock)

		return CandidateState{Block: block, NextSeqno: block.SeqNo + 1}, nil
	}
	block := runtimeTestBlockID(
		request.Session.Shard.Workchain,
		request.Session.Shard.Shard,
		request.Session.CatchainSeqno+1,
	)

	return CandidateState{Block: block, NextSeqno: block.SeqNo + 1}, nil
}

func (p *runtimeTestPipeline) BuildCandidate(ctx context.Context, request BuildRequest) (*Candidate, error) {
	p.mu.Lock()
	p.built++
	build := p.build
	p.mu.Unlock()
	var (
		candidate *Candidate
		err       error
	)
	if build != nil {
		candidate, err = build(ctx, request)
	} else {
		candidate = runtimeBuiltCandidate(request)
	}
	if err == nil && candidate != nil {
		p.mu.Lock()
		policy, rootOverride := p.successorPolicy, p.successorRoot
		p.mu.Unlock()
		runtimeHandOffSuccessor(request, candidate, policy, rootOverride)
	}

	return candidate, err
}

// runtimeHandOffSuccessor is the synchronous stand-in for what a real collation
// does on its block-BOC branch: the moment the block exists and the record has
// gone inert, it offers the block to whoever asked for the next slot early.
//
// It runs at the end of the build rather than partway through it, which is the
// one thing this stand-in cannot reproduce — the overlap itself. Everything the
// producer does with the offer, which is what these tests are about, is the
// same either way.
func runtimeHandOffSuccessor(
	request BuildRequest,
	candidate *Candidate,
	successorPolicy func(BuildRequest, *Candidate) CandidateState,
	successorRoot func(BuildRequest, *Candidate) []byte,
) {
	if request.onSuccessor == nil {
		return
	}
	id := cloneBlockID(candidate.ID)
	if successorRoot != nil {
		id.RootHash = successorRoot(request, candidate)
	}
	policy := CandidateState{
		Block:     cloneBlockID(candidate.ID),
		NextSeqno: candidate.ID.SeqNo + 1,
	}
	if successorPolicy != nil {
		policy = successorPolicy(request, candidate)
	}
	// The three cells a real offer carries. A fixture that left them nil would
	// let a successor that reads them past every test in this package.
	root := candidate.State
	if root == nil {
		root = cell.BeginCell().MustStoreUInt(uint64(candidate.ID.SeqNo), 32).EndCell()
	}
	update := candidate.StateUpdate
	if update == nil {
		update = root
	}
	request.onSuccessor(SuccessorOffer{
		successorPayload: successorPayload{
			ID:          id,
			Root:        root,
			State:       root,
			StateUpdate: update,
			StartLT:     uint64(request.Slot) * 1000,
		},
		Policy:          policy,
		predecessorSlot: request.Slot,
		handoffAt:       time.Now(),
	})
}

func (p *runtimeTestPipeline) SoftTimeout(ctx context.Context, request SoftTimeoutRequest) (SoftTimeoutDecision, error) {
	p.mu.Lock()
	soft := p.soft
	p.mu.Unlock()
	if soft != nil {
		return soft(ctx, request)
	}
	return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
}

func (p *runtimeTestPipeline) RestoreCandidate(
	ctx context.Context,
	request BuildRequest,
	artifact CandidateArtifact,
) error {
	p.mu.Lock()
	restore := p.restore
	p.mu.Unlock()
	if restore != nil {
		return restore(ctx, request, artifact)
	}
	return nil
}

func (p *runtimeTestPipeline) CommitCandidate(ctx context.Context, commit CandidateCommit) error {
	p.mu.Lock()
	commitFn := p.commit
	p.mu.Unlock()
	if commitFn != nil {
		return commitFn(ctx, commit)
	}
	return nil
}

func (p *runtimeTestPipeline) RetireSession(ctx context.Context, sessionID [32]byte) error {
	p.mu.Lock()
	p.retired++
	retire := p.retire
	p.mu.Unlock()
	if retire != nil {
		return retire(ctx, sessionID)
	}
	return nil
}

func (p *runtimeTestPipeline) buildCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.built
}

type runtimeMemoryStorage struct {
	mu               sync.Mutex
	sessions         map[[32]byte]SessionRecord
	candidates       map[runtimeCandidateKey]CandidateRecord
	saveSessionError func(SessionRecord) error
	deleteError      func([32]byte) error
}

type runtimeBlockingSessionsStorage struct {
	*runtimeMemoryStorage
	entered chan struct{}
	once    sync.Once
}

func (s *runtimeBlockingSessionsStorage) Sessions(ctx context.Context) ([]SessionRecord, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// runtimeCancelAfterSaveStorage expires the caller context once the numbered
// session write has already committed, leaving the update with a known durable
// outcome and an unusable caller context.
type runtimeCancelAfterSaveStorage struct {
	*runtimeMemoryStorage
	saves  atomic.Int32
	at     int32
	cancel func()
}

func (s *runtimeCancelAfterSaveStorage) SaveSession(
	ctx context.Context,
	record SessionRecord,
	done func(error),
) {
	s.runtimeMemoryStorage.SaveSession(ctx, record, done)
	if s.saves.Add(1) == s.at {
		s.cancel()
	}
}

// runtimeBlockedSessionAdmissionStorage models a store call which has not yet
// completed admission. Runtime session lifecycle must stay serialized across
// the call itself: returning only because ctx expired would let retirement
// overtake a request which can still enter the store afterwards.
type runtimeBlockedSessionAdmissionStorage struct {
	*runtimeMemoryStorage
	entered       chan struct{}
	release       chan struct{}
	deleteEntered chan struct{}
	enterOnce     sync.Once
	deleteOnce    sync.Once
}

func (s *runtimeBlockedSessionAdmissionStorage) SaveSession(
	ctx context.Context,
	record SessionRecord,
	done func(error),
) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.release
	if err := ctx.Err(); err != nil {
		done(err)

		return
	}
	s.runtimeMemoryStorage.SaveSession(ctx, record, done)
}

func (s *runtimeBlockedSessionAdmissionStorage) DeleteSession(
	ctx context.Context,
	id [32]byte,
) error {
	s.deleteOnce.Do(func() { close(s.deleteEntered) })

	return s.runtimeMemoryStorage.DeleteSession(ctx, id)
}

type runtimeFirstSessionAdmissionTimeoutStorage struct {
	*runtimeMemoryStorage
	entered       chan struct{}
	secondEntered chan struct{}
	releaseSecond chan struct{}
	calls         atomic.Int32
}

func (s *runtimeFirstSessionAdmissionTimeoutStorage) SaveSession(
	ctx context.Context,
	record SessionRecord,
	done func(error),
) {
	switch s.calls.Add(1) {
	case 1:
		close(s.entered)
		<-ctx.Done()
		done(ctx.Err())

		return
	case 2:
		close(s.secondEntered)
		<-s.releaseSecond
	}
	s.runtimeMemoryStorage.SaveSession(ctx, record, done)
}

type runtimeLateSessionCommitStorage struct {
	*runtimeMemoryStorage
	stored  chan struct{}
	release chan struct{}
	at      int32
	calls   atomic.Int32
}

func (s *runtimeLateSessionCommitStorage) SaveSession(
	ctx context.Context,
	record SessionRecord,
	done func(error),
) {
	at := s.at
	if at == 0 {
		at = 1
	}
	if s.calls.Add(1) != at {
		s.runtimeMemoryStorage.SaveSession(ctx, record, done)

		return
	}
	s.runtimeMemoryStorage.SaveSession(ctx, record, func(err error) {
		close(s.stored)
		go func() {
			<-s.release
			done(err)
		}()
	})
}

type runtimeCommittedSessionErrorStorage struct {
	*runtimeMemoryStorage
	err error
}

func (s *runtimeCommittedSessionErrorStorage) SaveSession(
	ctx context.Context,
	record SessionRecord,
	done func(error),
) {
	s.runtimeMemoryStorage.SaveSession(ctx, record, func(err error) {
		if err != nil {
			done(err)

			return
		}
		done(s.err)
	})
}

type runtimeLateCandidateSaveStorage struct {
	*runtimeMemoryStorage
	saved       chan CandidateRecord
	release     chan struct{}
	releaseOnce sync.Once
}

func (s *runtimeLateCandidateSaveStorage) SaveCandidate(record CandidateRecord, done func(error)) {
	s.runtimeMemoryStorage.SaveCandidate(record, func(err error) {
		if err == nil {
			s.saved <- cloneRuntimeCandidateRecord(record)
		}
		<-s.release
		done(err)
	})
}

func (s *runtimeLateCandidateSaveStorage) releaseCallback() {
	s.releaseOnce.Do(func() { close(s.release) })
}

type runtimeObservedCandidateSaveStorage struct {
	*runtimeMemoryStorage
	accepted chan struct{}
	once     sync.Once
}

func (s *runtimeObservedCandidateSaveStorage) SaveCandidate(record CandidateRecord, done func(error)) {
	s.runtimeMemoryStorage.SaveCandidate(record, func(err error) {
		done(err)
		s.once.Do(func() { close(s.accepted) })
	})
}

// runtimeStuckWriterStorage blocks inside the submission itself rather than
// before its callback, the way the shared store blocks on a full write queue
// while holding its global lock: no callback exists yet, so a wait that only
// listens for one cannot be abandoned.
type runtimeStuckWriterStorage struct {
	*runtimeMemoryStorage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *runtimeStuckWriterStorage) SaveCandidate(record CandidateRecord, done func(error)) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	s.runtimeMemoryStorage.SaveCandidate(record, done)
}

type runtimeCandidateKey struct {
	window WindowID
	slot   uint32
}

type runtimeCountingSigner struct {
	private ed25519.PrivateKey
	calls   atomic.Int32
}

func (s *runtimeCountingSigner) Sign(payload []byte) ([]byte, error) {
	s.calls.Add(1)

	return ed25519.Sign(s.private, payload), nil
}

func newRuntimeMemoryStorage() *runtimeMemoryStorage {
	return &runtimeMemoryStorage{
		sessions:   make(map[[32]byte]SessionRecord),
		candidates: make(map[runtimeCandidateKey]CandidateRecord),
	}
}

func (s *runtimeMemoryStorage) SaveSession(
	_ context.Context,
	record SessionRecord,
	done func(error),
) {
	s.mu.Lock()
	if s.saveSessionError != nil {
		if err := s.saveSessionError(record); err != nil {
			s.mu.Unlock()
			done(err)

			return
		}
	}
	existing, exists := s.sessions[record.Session.ID]
	if exists && !existing.Session.Equal(record.Session) {
		s.mu.Unlock()
		done(ErrSessionConflict)
		return
	}
	s.sessions[record.Session.ID] = cloneSessionRecord(record)
	s.mu.Unlock()
	done(nil)
}

func (s *runtimeMemoryStorage) Session(_ context.Context, id [32]byte) (SessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.sessions[id]
	if !exists {
		return SessionRecord{}, ErrNotFound
	}
	return cloneSessionRecord(record), nil
}

func (s *runtimeMemoryStorage) Sessions(context.Context) ([]SessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]SessionRecord, 0, len(s.sessions))
	for _, record := range s.sessions {
		records = append(records, cloneSessionRecord(record))
	}
	return records, nil
}

func (s *runtimeMemoryStorage) DeleteSession(_ context.Context, id [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteError != nil {
		if err := s.deleteError(id); err != nil {
			return err
		}
	}
	if _, exists := s.sessions[id]; !exists {
		return ErrSessionRetired
	}
	delete(s.sessions, id)
	for key := range s.candidates {
		if key.window.SessionID == id {
			delete(s.candidates, key)
		}
	}
	return nil
}

func (s *runtimeMemoryStorage) SaveCandidate(record CandidateRecord, done func(error)) {
	key := runtimeCandidateKey{window: record.WindowID, slot: record.ID.Slot}
	s.mu.Lock()
	existing, exists := s.candidates[key]
	if exists && !reflect.DeepEqual(existing, record) {
		s.mu.Unlock()
		done(ErrCandidateConflict)
		return
	}
	s.candidates[key] = cloneRuntimeCandidateRecord(record)
	s.mu.Unlock()
	done(nil)
}

func (s *runtimeMemoryStorage) Candidate(_ context.Context, id WindowID, slot uint32) (CandidateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.candidates[runtimeCandidateKey{window: id, slot: slot}]
	if !exists {
		return CandidateRecord{}, ErrNotFound
	}
	return cloneRuntimeCandidateRecord(record), nil
}

func (s *runtimeMemoryStorage) Status(context.Context) (StorageStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := StorageStatus{
		Sessions:   uint64(len(s.sessions)),
		Candidates: uint64(len(s.candidates)),
	}
	return status, nil
}

func cloneRuntimeCandidateRecord(record CandidateRecord) CandidateRecord {
	record.Block = cloneBlockID(record.Block)
	record.Signature = append([]byte(nil), record.Signature...)
	record.DelegationSignature = append([]byte(nil), record.DelegationSignature...)
	return record
}

type runtimeFixture struct {
	service    *Service
	storage    *runtimeMemoryStorage
	pipeline   *runtimeTestPipeline
	keys       *runtimeTestKeys
	leaderPriv ed25519.PrivateKey
	leaderPub  ed25519.PublicKey
	sourceADNL [32]byte
}

type runtimeScheduleObserver struct {
	mu     sync.Mutex
	events map[ScheduleEvent]int
}

func (o *runtimeScheduleObserver) ObserveScheduleLateness(_ MetricChain, event ScheduleEvent, _ time.Duration) {
	o.mu.Lock()
	o.events[event]++
	o.mu.Unlock()
}

func (o *runtimeScheduleObserver) count(event ScheduleEvent) int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.events[event]
}

func (*runtimeScheduleObserver) AddCollationBuildInflight(MetricChain, int)                {}
func (*runtimeScheduleObserver) ObserveCollationBuild(CollationBuildObservation)           {}
func (*runtimeScheduleObserver) ObserveCollationStage(CollationStageObservation)           {}
func (*runtimeScheduleObserver) ObserveCollationCandidate(CandidateObservation)            {}
func (*runtimeScheduleObserver) ObserveCandidateProduction(CandidateProductionObservation) {}
func (*runtimeScheduleObserver) ObserveCollationDeadline(MetricChain, CollationDeadline, DeadlineAction) {
}
func (*runtimeScheduleObserver) ObserveCollationRetry(MetricChain, ProductionRetryReason)   {}
func (*runtimeScheduleObserver) ObserveCollationAlarm(MetricChain, CollationAlarm)          {}
func (*runtimeScheduleObserver) ObservePipelineHandoff(MetricChain, PipelineHandoffOutcome) {}
func (*runtimeScheduleObserver) ObservePipelineHandoffPickup(MetricChain, time.Duration)    {}
func (*runtimeScheduleObserver) ObservePipelineOverlap(MetricChain, time.Duration)          {}
func (*runtimeScheduleObserver) AddCollationWindowInflight(MetricChain, int)                {}
func (*runtimeScheduleObserver) ObserveCollationWindow(WindowObservation)                   {}

func newRuntimeFixture(
	t *testing.T,
	_ int,
	_ int,
	pipeline *runtimeTestPipeline,
	storage *runtimeMemoryStorage,
	emit EmitCandidate,
) *runtimeFixture {
	t.Helper()
	if storage == nil {
		storage = newRuntimeMemoryStorage()
	}

	return newRuntimeFixtureWithMode(
		t,
		ProductionModeDelegated,
		pipeline,
		storage,
		storage,
		emit,
	)
}

func newRuntimeSelfFixture(
	t *testing.T,
	pipeline *runtimeTestPipeline,
	storage CollatorStorage,
	memory *runtimeMemoryStorage,
	emit EmitCandidate,
) *runtimeFixture {
	t.Helper()
	if memory == nil {
		memory = newRuntimeMemoryStorage()
	}
	if storage == nil {
		storage = memory
	}

	return newRuntimeFixtureWithMode(t, ProductionModeSelf, pipeline, storage, memory, emit)
}

func newRuntimeFixtureWithMode(
	t *testing.T,
	mode ProductionMode,
	pipeline *runtimeTestPipeline,
	storage CollatorStorage,
	memory *runtimeMemoryStorage,
	emit EmitCandidate,
) *runtimeFixture {
	t.Helper()
	if pipeline == nil {
		pipeline = &runtimeTestPipeline{}
	}
	if emit == nil {
		emit = func(context.Context, CandidateArtifact) error { return nil }
	}
	leaderPub, leaderPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := newRuntimeTestKeys(t)
	source := sha256.Sum256([]byte("validator-adnl"))
	service, err := NewService(ServiceOptions{
		ProductionMode:    mode,
		Storage:           storage,
		Pipeline:          pipeline,
		Keys:              keys,
		CollatorKeyID:     keys.id,
		AllowedValidators: map[[32]byte]struct{}{source: {}},
		Emit:              emit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &runtimeFixture{
		service:    service,
		storage:    memory,
		pipeline:   pipeline,
		keys:       keys,
		leaderPriv: leaderPriv,
		leaderPub:  leaderPub,
		sourceADNL: source,
	}
}

func newUnstartedRuntimeService(
	t *testing.T,
	storage CollatorStorage,
	pipeline *runtimeTestPipeline,
	keys *runtimeTestKeys,
	allowed map[[32]byte]struct{},
	emit EmitCandidate,
) *Service {
	t.Helper()
	if emit == nil {
		emit = func(context.Context, CandidateArtifact) error { return nil }
	}
	service, err := NewService(ServiceOptions{
		ProductionMode:    ProductionModeDelegated,
		Storage:           storage,
		Pipeline:          pipeline,
		Keys:              keys,
		CollatorKeyID:     keys.id,
		AllowedValidators: allowed,
		Emit:              emit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func (f *runtimeFixture) close(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *runtimeFixture) session(seed byte, slots uint32, current uint32, startAt time.Time) (Session, SessionUpdate) {
	var id [32]byte
	id[0] = seed
	var key [ed25519.PublicKeySize]byte
	copy(key[:], f.leaderPub)
	master := runtimeTestBlockID(-1, -1<<63, uint32(seed))
	session := Session{
		ID:                   id,
		Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
		CatchainSeqno:        uint32(seed),
		ConsensusVersion:     2,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: slots,
		Validators: []SessionValidator{{
			PublicKey: key,
			ADNLID:    f.sourceADNL,
			Weight:    1,
		}},
	}
	update := SessionUpdate{
		SessionID:                 id,
		TargetRate:                100 * time.Millisecond,
		NoEmptyBlocksOnErrTimeout: 15 * time.Second,
		MasterchainBlock:          master,
		HasCurrentWindow:          true,
		CurrentWindowStart:        current,
		CurrentWindowObservedSlot: current,
		CurrentWindowStartAt:      startAt,
	}
	return session, update
}

func runtimeConsensusProgress(session Session, update SessionUpdate) ConsensusProgress {
	return ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         update.CurrentBase,
			ObservedSlot: update.CurrentWindowObservedSlot,
			StartSlot:    update.CurrentWindowStart,
			EndSlot:      update.CurrentWindowStart + session.SlotsPerLeaderWindow,
			Leader:       update.CurrentWindowStart / session.SlotsPerLeaderWindow % uint32(len(session.Validators)),
			ObservedAt:   time.Now(),
		},
		StartAt: update.CurrentWindowStartAt,
	}
}

func runtimeSelectedBase(
	t testing.TB,
	session Session,
	candidate simplex.CandidateID,
) *SelectedBaseState {
	t.Helper()

	built, err := testBuilder().BuildShard(context.Background(), emptyCandidateRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewSelectedBaseState(
		session.ID,
		candidate,
		built.ID,
		built.BlockBOC,
		candidateBlock(t, built),
		built.State,
	)
	if err != nil {
		t.Fatal(err)
	}

	return base
}

func TestRuntimeBackendReportsCollatorID(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	var backend Collator = fixture.service
	if got := backend.CollatorID(); got != fixture.keys.id {
		t.Fatalf("collator id = %x, want %x", got, fixture.keys.id)
	}
}

func TestRuntimeBuildStartLatenessExcludesFirstShardWindowSlot(t *testing.T) {
	emitted := make(chan CandidateArtifact, 2)
	releaseFirst := make(chan struct{})
	fixture := newRuntimeSelfFixture(
		t,
		nil,
		nil,
		nil,
		func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact
			if artifact.Candidate.ID.Slot == 0 {
				<-releaseFirst
			}

			return nil
		},
	)
	defer fixture.close(t)

	observer := &runtimeScheduleObserver{events: make(map[ScheduleEvent]int)}
	fixture.service.opts.Observer = observer
	session, update := fixture.session(91, 2, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}

	first := runtimeAwaitArtifact(t, emitted)
	close(releaseFirst)
	second := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != 0 || second.Candidate.ID.Slot != 1 {
		t.Fatalf("emitted slots = %d, %d, want 0, 1", first.Candidate.ID.Slot, second.Candidate.ID.Slot)
	}
	// One sample for two slots, and it belongs to the second one. There used to
	// be a check here that the count was still zero while the first slot's
	// emission was held open; pipelining starts the second slot's build before
	// that emission returns, so the count is already one at that point and the
	// check said nothing about which slot it came from. The pair below says it
	// exactly: a pipelined build reports both series, so a build_lead sample
	// proves the second slot reported build_start, and a total of one then
	// proves the first slot did not.
	if got := observer.count(ScheduleEventBuildStart); got != 1 {
		t.Fatalf("build-start observations = %d over two slots, want 1", got)
	}
	if got := observer.count(ScheduleEventBuildLead); got != 1 {
		t.Fatalf("build-lead observations = %d, want 1 — the second slot was not pipelined, so the "+
			"assertion above no longer proves the first slot was excluded", got)
	}
	if got := observer.count(ScheduleEventBroadcast); got != 2 {
		t.Fatalf("broadcast observations = %d, want 2", got)
	}
}

func TestRuntimeRejectsInvalidProductionMode(t *testing.T) {
	if _, err := NewService(ServiceOptions{}); err == nil || !strings.Contains(err.Error(), "production mode") {
		t.Fatalf("zero production mode error = %v, want production-mode rejection", err)
	}
}

func TestRuntimeProductionModesRejectOppositeAuthority(t *testing.T) {
	self := newRuntimeSelfFixture(t, nil, nil, nil, nil)
	if err := self.service.Probe(context.Background(), WindowPreparation{}); !errors.Is(err, ErrUnsupported) {
		self.close(t)
		t.Fatalf("self-mode probe error = %v, want ErrUnsupported", err)
	}
	if err := self.service.CommitDelegation(context.Background(), WindowRequest{}); !errors.Is(err, ErrUnsupported) {
		self.close(t)
		t.Fatalf("self-mode delegation error = %v, want ErrUnsupported", err)
	}
	self.close(t)

	delegated := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer delegated.close(t)
	if err := delegated.service.ActivateSelfWindow(
		context.Background(),
		SelfWindowRequest{},
	); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("delegated-mode self activation error = %v, want ErrUnsupported", err)
	}
}

func TestRuntimeSelfActivationStartsBeforeConsensusProgressWALCallback(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeLateSessionCommitStorage{
		runtimeMemoryStorage: baseStorage,
		stored:               make(chan struct{}),
		release:              make(chan struct{}),
		at:                   3,
	}
	built := make(chan BuildRequest, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		built <- request

		return runtimeBuiltCandidate(request), nil
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeSelfFixture(
		t,
		pipeline,
		storage,
		baseStorage,
		func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		},
	)
	var releaseProgress sync.Once
	release := func() {
		releaseProgress.Do(func() { close(storage.release) })
	}
	defer func() {
		release()
		fixture.close(t)
	}()

	session, initial := fixture.session(62, 1, 0, time.Time{})
	initial.HasCurrentWindow = false
	fixture.prepare(t, session, initial)

	observed := initial
	observed.HasCurrentWindow = true
	observed.CurrentWindowStartAt = time.Now().Add(-time.Second)
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, observed),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.stored:
	case <-time.After(time.Second):
		t.Fatal("consensus progress did not reach durable storage")
	}
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := runtimeAwaitBuild(t, built)
	if request.Slot != 0 {
		t.Fatalf("self build slot = %d, want 0", request.Slot)
	}
	runtimeAwaitArtifact(t, emitted)
	managed.mu.Lock()
	pending := managed.sessionWritePending
	armed := managed.progressReady
	managed.mu.Unlock()
	if !pending || !armed {
		t.Fatalf("production before WAL callback = pending %t, armed %t, want true, true", pending, armed)
	}

	release()
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
}

func TestRuntimeSelfActivationDoesNotSaveDelegation(t *testing.T) {
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeSelfFixture(
		t,
		nil,
		nil,
		nil,
		func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		},
	)
	defer fixture.close(t)

	session, update := fixture.session(63, 1, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.Candidate.Delegation != nil {
		t.Fatalf("self candidate carries delegation %+v", artifact.Candidate.Delegation)
	}
	if !simplex.VerifyCandidateSignature(
		fixture.leaderPub,
		session.ID,
		artifact.Candidate.ID,
		artifact.Candidate.Signature,
	) {
		t.Fatal("self candidate is not signed by the validator session key")
	}
	marker, err := fixture.storage.Candidate(
		context.Background(),
		WindowID{SessionID: session.ID, StartSlot: 0},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if marker.Authority != CandidateAuthoritySelf ||
		marker.DelegationKey != ([ed25519.PublicKeySize]byte{}) ||
		len(marker.DelegationSignature) != 0 {
		t.Fatalf("self candidate marker has delegated authority: %+v", marker)
	}
}

func TestRuntimeSelfCandidateMarkerPrecedesEmission(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeLateCandidateSaveStorage{
		runtimeMemoryStorage: baseStorage,
		saved:                make(chan CandidateRecord, 1),
		release:              make(chan struct{}),
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeSelfFixture(
		t,
		nil,
		storage,
		baseStorage,
		func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		},
	)
	defer func() {
		storage.releaseCallback()
		fixture.close(t)
	}()

	session, update := fixture.session(64, 1, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	var marker CandidateRecord
	select {
	case marker = <-storage.saved:
	case <-time.After(time.Second):
		t.Fatal("self candidate marker was not submitted")
	}
	if marker.Authority != CandidateAuthoritySelf {
		t.Fatalf("candidate marker authority = %d, want self", marker.Authority)
	}
	select {
	case artifact := <-emitted:
		t.Fatalf("candidate emitted before marker callback: %+v", artifact)
	default:
	}

	storage.releaseCallback()
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.Candidate.ID != marker.ID {
		t.Fatalf("emitted candidate ID = %v, marker ID = %v", artifact.Candidate.ID, marker.ID)
	}
}

func TestRuntimeSelfDeadlineAndProgressCancelProduction(t *testing.T) {
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	var enteredOnce sync.Once
	var cancelledOnce sync.Once
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, _ BuildRequest) (*Candidate, error) {
		enteredOnce.Do(func() { close(entered) })
		<-ctx.Done()
		cancelledOnce.Do(func() { close(cancelled) })

		return nil, ctx.Err()
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeSelfFixture(
		t,
		pipeline,
		nil,
		nil,
		func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		},
	)
	defer fixture.close(t)

	session, update := fixture.session(65, 1, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(-time.Nanosecond)),
	); !errors.Is(err, ErrStaleWindow) {
		t.Fatalf("expired self activation error = %v, want ErrStaleWindow", err)
	}
	if builds := pipeline.buildCount(); builds != 0 {
		t.Fatalf("expired self activation built %d candidates", builds)
	}
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("self production did not enter candidate build")
	}

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, next),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("advanced consensus progress did not cancel self production")
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool { return runtimeProductionCount(managed) == 0 })
	select {
	case artifact := <-emitted:
		t.Fatalf("cancelled self production emitted a candidate: %+v", artifact)
	default:
	}
}

func TestRuntimeRestartDoesNotRebuildSelfCandidateMarker(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	emitEntered := make(chan CandidateArtifact, 1)
	first := newRuntimeSelfFixture(
		t,
		nil,
		storage,
		storage,
		func(ctx context.Context, artifact CandidateArtifact) error {
			emitEntered <- artifact
			<-ctx.Done()

			return ctx.Err()
		},
	)
	session, update := first.session(66, 1, 0, time.Now().Add(-time.Second))
	first.prepare(t, session, update)
	if err := first.service.ActivateSelfWindow(
		context.Background(),
		first.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	signed := runtimeAwaitArtifact(t, emitEntered)
	marker, err := storage.Candidate(context.Background(), signed.WindowID, signed.Candidate.ID.Slot)
	if err != nil {
		t.Fatal(err)
	}
	if marker.Authority != CandidateAuthoritySelf {
		t.Fatalf("persisted marker authority = %d, want self", marker.Authority)
	}
	first.close(t)

	secondPipeline := &runtimeTestPipeline{}
	secondEmitted := make(chan CandidateArtifact, 1)
	second, err := NewService(ServiceOptions{
		ProductionMode:    ProductionModeSelf,
		Storage:           storage,
		Pipeline:          secondPipeline,
		Keys:              first.keys,
		CollatorKeyID:     first.keys.id,
		AllowedValidators: map[[32]byte]struct{}{first.sourceADNL: {}},
		Emit: func(_ context.Context, artifact CandidateArtifact) error {
			secondEmitted <- artifact

			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := second.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if err = second.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, update),
	); err != nil {
		t.Fatal(err)
	}
	signer := &runtimeCountingSigner{private: first.leaderPriv}
	if err = second.ActivateSelfWindow(context.Background(), SelfWindowRequest{
		SessionID: session.ID,
		StartSlot: 0,
		Deadline:  time.Now().Add(5 * time.Second),
		Signer:    signer,
	}); err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool {
		status, statusErr := second.Status(context.Background())

		return statusErr == nil && status.FailedWindows == 1 && status.ActiveWindows == 0
	})
	status, err := second.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status.LastError, errWindowNotResumable.Error()) {
		t.Fatalf("restarted self window error = %q, want nonresumable marker", status.LastError)
	}
	if builds := secondPipeline.buildCount(); builds != 0 {
		t.Fatalf("restart rebuilt %d candidates behind a self marker", builds)
	}
	if calls := signer.calls.Load(); calls != 0 {
		t.Fatalf("restart signed %d candidates behind a self marker", calls)
	}
	select {
	case artifact := <-secondEmitted:
		t.Fatalf("restart emitted a replacement candidate: %+v", artifact)
	default:
	}
}

func TestRuntimeRejectsCandidateCreatedByNonLeader(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, _ := fixture.session(61, 1, 0, time.Now())
	built := runtimeBuiltCandidate(BuildRequest{Session: ActivatedSession{Session: session}, Slot: 0})
	built.CreatedBy[0] ^= 0xff
	_, err := fixture.service.signArtifact(
		session,
		productionWindow{Leader: 0, Authority: CandidateAuthorityDelegated},
		0,
		simplex.Genesis(),
		built,
	)
	if !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("candidate creator error = %v, want ErrCandidateConflict", err)
	}
}

func TestRuntimeCloseCancelsAndJoinsStart(t *testing.T) {
	storage := &runtimeBlockingSessionsStorage{
		runtimeMemoryStorage: newRuntimeMemoryStorage(),
		entered:              make(chan struct{}),
	}
	pipeline := &runtimeTestPipeline{}
	keys := newRuntimeTestKeys(t)
	service := newUnstartedRuntimeService(t, storage, pipeline, keys, nil, nil)

	startResult := make(chan error, 1)
	go func() { startResult <- service.Start(context.Background()) }()
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("start did not enter storage recovery")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("start error = %v, want context cancellation", err)
	}
	status, err := service.Status(context.Background())
	if err != nil || !status.Closed || status.Closing {
		t.Fatalf("closed status=%+v err=%v", status, err)
	}
}

func TestRuntimeStartDefersRecoveredSessionUntilMessageTopologyIsReady(t *testing.T) {
	acquisition, pool, template, update := localRotationFixture(t)
	defer pool.Close()

	session := template.Session
	session.ID[0] ^= 0xff
	update.SessionID = session.ID
	storage := newRuntimeMemoryStorage()
	storage.sessions[session.ID] = SessionRecord{Session: session, Update: update}
	if err := pool.Internals().ReconcileDestinations(nil); err != nil {
		t.Fatal(err)
	}

	keys := newRuntimeTestKeys(t)
	service, err := NewService(ServiceOptions{
		ProductionMode:     ProductionModeDelegated,
		Storage:            storage,
		Pipeline:           acquisition,
		Keys:               keys,
		CollatorKeyID:      keys.id,
		AllowAllValidators: true,
		Emit:               func(context.Context, CandidateArtifact) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("start with deferred acquisition topology: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	recovered, err := service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatalf("load deferred durable session: %v", err)
	}
	if !recovered.Session.Equal(session) || !recovered.Update.Equal(update) {
		t.Fatal("deferred durable session differs from storage")
	}
	managed, err := service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	pipelineReady := managed.pipelineReady
	managed.mu.Unlock()
	if pipelineReady {
		t.Fatal("pipeline became ready without the session destination")
	}

	destination := targetShardIdent(session.Shard)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	if err = service.UpdateSession(context.Background(), update); err != nil {
		t.Fatalf("heal deferred session with exact update: %v", err)
	}
	managed.mu.Lock()
	pipelineReady = managed.pipelineReady
	managed.mu.Unlock()
	if !pipelineReady {
		t.Fatal("exact update did not prepare the deferred pipeline")
	}
	if _, err = acquisition.session(session.ID); err != nil {
		t.Fatalf("healed acquisition session is absent: %v", err)
	}
}

func TestRuntimeDeferredRecoveredSessionCanRetireWithoutPipelineState(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	seed := newRuntimeFixture(t, 1, 1, nil, storage, nil)
	session, update := seed.session(42, 1, 0, time.Now())
	seed.prepare(t, session, update)
	seed.close(t)

	pipeline := &runtimeTestPipeline{
		prepare: func(context.Context, Session, SessionUpdate) error {
			return ErrAcquisitionNotReady
		},
		retire: func(context.Context, [32]byte) error {
			return ErrNotFound
		},
	}
	service := newUnstartedRuntimeService(
		t,
		storage,
		pipeline,
		seed.keys,
		map[[32]byte]struct{}{seed.sourceADNL: {}},
		nil,
	)
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.RetireSession(context.Background(), session.ID); err != nil {
		t.Fatalf("retire deferred recovered session: %v", err)
	}
	if _, err := storage.Session(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("durable deferred session after retirement = %v, want ErrNotFound", err)
	}
	if _, err := service.Session(context.Background(), session.ID); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("retired deferred session lookup = %v, want ErrSessionRetired", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDeferredRecoveredActivationNotReadyRemainsRetryable(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	seed := newRuntimeFixture(t, 1, 1, nil, storage, nil)
	session, update := seed.session(45, 1, 0, time.Now())
	seed.prepare(t, session, update)
	activation := *storage.sessions[session.ID].Activation
	seed.close(t)

	var topologyReady atomic.Bool
	var activationAttempts atomic.Int32
	pipeline := &runtimeTestPipeline{
		prepare: func(context.Context, Session, SessionUpdate) error {
			if !topologyReady.Load() {
				return ErrAcquisitionNotReady
			}

			return nil
		},
		activate: func(context.Context, SessionActivation, SessionUpdate) error {
			if activationAttempts.Add(1) == 1 {
				return ErrAcquisitionNotReady
			}

			return nil
		},
	}
	service := newUnstartedRuntimeService(
		t,
		storage,
		pipeline,
		seed.keys,
		map[[32]byte]struct{}{seed.sourceADNL: {}},
		nil,
	)
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	topologyReady.Store(true)
	if err := service.ActivateSession(
		context.Background(),
		activation,
	); !errors.Is(err, ErrAcquisitionNotReady) || errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("first activation error = %v, want retryable ErrAcquisitionNotReady", err)
	}
	if _, err := service.Session(context.Background(), session.ID); err != nil {
		t.Fatalf("transient activation poisoned the recovered session: %v", err)
	}
	if err := service.ActivateSession(context.Background(), activation); err != nil {
		t.Fatalf("retry recovered activation: %v", err)
	}
	prepared, activated, _, _, _ := pipeline.counts()
	if prepared != 2 || activated != 2 {
		t.Fatalf("pipeline prepare/activate calls = %d/%d, want 2/2", prepared, activated)
	}
}

func TestRuntimeStartRejectsNonTransientRecoveredPipelineFailure(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	seed := newRuntimeFixture(t, 1, 1, nil, storage, nil)
	session, update := seed.session(43, 1, 0, time.Now())
	seed.prepare(t, session, update)
	seed.close(t)

	want := errors.New("corrupt branch state")
	pipeline := &runtimeTestPipeline{
		prepare: func(context.Context, Session, SessionUpdate) error { return want },
	}
	service := newUnstartedRuntimeService(
		t,
		storage,
		pipeline,
		seed.keys,
		map[[32]byte]struct{}{seed.sourceADNL: {}},
		nil,
	)
	if err := service.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("start error = %v, want terminal pipeline failure", err)
	}
}

func TestRuntimePrepareAdmissionTimeoutRollsBackForExactRetry(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeFirstSessionAdmissionTimeoutStorage{
		runtimeMemoryStorage: baseStorage,
		entered:              make(chan struct{}),
		secondEntered:        make(chan struct{}),
		releaseSecond:        make(chan struct{}),
	}
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, baseStorage, nil)
	fixture.service.opts.Storage = storage
	defer fixture.close(t)
	releasedSecond := false
	defer func() {
		if !releasedSecond {
			close(storage.releaseSecond)
		}
	}()

	session, update := fixture.session(141, 1, 0, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- fixture.service.PrepareSession(ctx, session, update)
	}()
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("prepare did not enter session-write admission")
	}
	retryStarted := make(chan struct{})
	retryResult := make(chan error, 1)
	go func() {
		close(retryStarted)
		retryResult <- fixture.service.PrepareSession(context.Background(), session, update)
	}()
	<-retryStarted
	// The retry has resolved the admitted handle and is queued on its lifecycle
	// lock. Successful rollback removes that handle; recheck must safely publish
	// its clean generation again rather than lose or conflict the exact retry.
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled prepare error = %v, want context canceled", err)
	}
	select {
	case <-storage.secondEntered:
	case <-time.After(time.Second):
		t.Fatal("queued exact prepare did not re-admit the clean generation")
	}
	if _, err := baseStorage.Session(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("session after cancelled admission = %v, want ErrNotFound", err)
	}
	close(storage.releaseSecond)
	releasedSecond = true
	if err := <-retryResult; err != nil {
		t.Fatalf("queued exact prepare retry: %v", err)
	}
	record, err := baseStorage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Session.Equal(session) || !record.Update.Equal(update) {
		t.Fatal("exact prepare retry stored a different session")
	}
	pipeline.mu.Lock()
	prepared, retired := pipeline.prepared, pipeline.retired
	pipeline.mu.Unlock()
	if prepared != 2 || retired != 1 {
		t.Fatalf("pipeline prepare/retire calls = %d/%d, want 2/1", prepared, retired)
	}
}

func TestRuntimePrepareUnknownCommitIsDeletedBeforeExactRetry(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeLateSessionCommitStorage{
		runtimeMemoryStorage: baseStorage,
		stored:               make(chan struct{}),
		release:              make(chan struct{}),
	}
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, baseStorage, nil)
	fixture.service.opts.Storage = storage
	defer fixture.close(t)
	released := false
	defer func() {
		if !released {
			close(storage.release)
		}
	}()

	session, update := fixture.session(142, 1, 0, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- fixture.service.PrepareSession(ctx, session, update)
	}()
	select {
	case <-storage.stored:
	case <-time.After(time.Second):
		t.Fatal("prepare session write did not commit")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("prepare with unknown callback error = %v, want context canceled", err)
	}
	if _, err := baseStorage.Session(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("late committed session survived rollback: %v", err)
	}
	close(storage.release)
	released = true
	if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatalf("exact prepare after late commit cleanup: %v", err)
	}
	record, err := baseStorage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Session.Equal(session) || !record.Update.Equal(update) {
		t.Fatal("exact retry after late commit stored a different session")
	}
}

func TestRuntimePrepareRollbackFailureCanRetryThroughSessionProbe(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	saveErr := errors.New("session WAL failed")
	storage.saveSessionError = func(SessionRecord) error { return saveErr }
	retireErr := errors.New("pipeline rollback failed")
	var deletes atomic.Int32
	storage.deleteError = func([32]byte) error {
		deletes.Add(1)

		return nil
	}
	var failRetire atomic.Bool
	failRetire.Store(true)
	pipeline := &runtimeTestPipeline{}
	pipeline.retire = func(context.Context, [32]byte) error {
		if failRetire.Load() {
			return retireErr
		}
		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, storage, nil)
	session, update := fixture.session(42, 1, 0, time.Now())

	err := fixture.service.PrepareSession(context.Background(), session, update)
	if !errors.Is(err, saveErr) || !errors.Is(err, retireErr) || !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("prepare error = %v, want save and rollback failures", err)
	}
	if deletes.Load() != 1 {
		t.Fatalf("durable cleanup attempts = %d, want 1 despite pipeline failure", deletes.Load())
	}
	if _, err = fixture.service.Session(context.Background(), session.ID); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("cleanup-pending session probe = %v, want ErrSessionRetired", err)
	}

	conflict := session
	conflict.ValidatorSetHash++
	if err = fixture.service.PrepareSession(context.Background(), conflict, update); !errors.Is(
		err,
		ErrSessionConflict,
	) {
		t.Fatalf("conflicting cleanup retry = %v, want ErrSessionConflict", err)
	}
	if deletes.Load() != 1 {
		t.Fatalf("conflicting descriptor triggered cleanup: deletes=%d, want 1", deletes.Load())
	}

	failRetire.Store(false)
	if err = fixture.service.PrepareSession(context.Background(), session, update); !errors.Is(
		err,
		ErrSessionRetired,
	) {
		t.Fatalf("successful cleanup retry = %v, want ErrSessionRetired", err)
	}
	storage.saveSessionError = nil
	if err = fixture.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatalf("prepare next session generation: %v", err)
	}
	fixture.close(t)
	pipeline.mu.Lock()
	retired := pipeline.retired
	pipeline.mu.Unlock()
	if retired != 3 {
		t.Fatalf("pipeline retire attempts = %d, want 3 including close", retired)
	}
}

func TestRuntimeCloseRetriesPendingPrepareCleanup(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	saveErr := errors.New("session WAL callback failed after commit")
	deleteErr := errors.New("temporary session delete failure")
	var deletes atomic.Int32
	baseStorage.deleteError = func([32]byte) error {
		if deletes.Add(1) == 1 {
			return deleteErr
		}

		return nil
	}
	storage := &runtimeCommittedSessionErrorStorage{
		runtimeMemoryStorage: baseStorage,
		err:                  saveErr,
	}
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, baseStorage, nil)
	fixture.service.opts.Storage = storage

	session, update := fixture.session(143, 1, 0, time.Now())
	err := fixture.service.PrepareSession(context.Background(), session, update)
	if !errors.Is(err, saveErr) || !errors.Is(err, deleteErr) ||
		!errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("prepare error = %v, want save and cleanup failures", err)
	}
	if _, err = baseStorage.Session(context.Background(), session.ID); err != nil {
		t.Fatalf("committed session was not retained after failed delete: %v", err)
	}
	if _, err = fixture.service.Session(context.Background(), session.ID); !errors.Is(
		err,
		ErrSessionRetired,
	) {
		t.Fatalf("cleanup-pending session probe = %v, want ErrSessionRetired", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = fixture.service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = baseStorage.Session(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("durable session after Close cleanup = %v, want ErrNotFound", err)
	}
	if deletes.Load() != 2 {
		t.Fatalf("durable cleanup attempts = %d, want 2", deletes.Load())
	}
	pipeline.mu.Lock()
	retired := pipeline.retired
	pipeline.mu.Unlock()
	if retired != 2 {
		t.Fatalf("pipeline cleanup attempts = %d, want 2", retired)
	}
}

func TestRuntimeQueuedConflictingPrepareDoesNotReinsertCleanedPlaceholder(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(143, 1, 0, time.Now())
	record := SessionRecord{Session: session, Update: update}
	managed := newManagedCollatorSession(record, false)

	managed.controlMu.Lock()
	fixture.service.mu.Lock()
	fixture.service.sessions[session.ID] = managed
	fixture.service.mu.Unlock()

	conflict := session
	conflict.ValidatorSetHash++
	observed := make(chan struct{})
	prepareCtx := &runtimeLockObservedContext{Context: context.Background(), observed: observed}
	prepareResult := make(chan error, 1)
	go func() {
		prepareResult <- fixture.service.PrepareSession(prepareCtx, conflict, update)
	}()
	select {
	case <-observed:
	case <-time.After(time.Second):
		managed.controlMu.Unlock()
		t.Fatal("conflicting prepare did not queue on the old placeholder")
	}

	// Successful compensation removes the clean placeholder while an already
	// admitted conflicting Prepare still holds its pointer. That waiter may
	// inspect the old descriptor, but must not leave the pointer reinserted.
	fixture.service.removePreparedSession(session.ID, managed)
	managed.controlMu.Unlock()
	if err := <-prepareResult; !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("queued conflicting prepare = %v, want ErrSessionConflict", err)
	}

	if err := fixture.service.PrepareSession(context.Background(), conflict, update); err != nil {
		t.Fatalf("prepare fresh conflicting generation: %v", err)
	}
	stored, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Session.Equal(conflict) {
		t.Fatal("queued conflict left the cleaned placeholder installed")
	}
}

func TestRuntimeRetiredSessionCanBePreparedAgain(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(43, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	if err := fixture.service.RetireSession(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatalf("prepare next session generation: %v", err)
	}
	record, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Session.Equal(session) || !record.Update.Equal(update) {
		t.Fatal("reopened session differs from the prepared generation")
	}
}

func runtimeTestBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := sha256.Sum256([]byte(fmt.Sprintf("root-%d-%d-%d", workchain, shard, seqno)))
	file := sha256.Sum256(root[:])
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  append([]byte(nil), root[:]...),
		FileHash:  append([]byte(nil), file[:]...),
	}
}

func (f *runtimeFixture) prepare(t *testing.T, session Session, update SessionUpdate) {
	t.Helper()
	if err := f.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatal(err)
	}
	activation := SessionActivation{
		SessionID:      session.ID,
		Genesis:        []ton.BlockIDExt{runtimeTestBlockID(0, -1<<63, session.CatchainSeqno+1)},
		MinMasterchain: update.MasterchainBlock,
	}
	if err := f.service.ActivateSession(context.Background(), activation); err != nil {
		t.Fatal(err)
	}
	if update.HasCurrentWindow {
		managed, err := f.service.runningSession(session.ID)
		if err != nil {
			t.Fatal(err)
		}
		managed.mu.Lock()
		managed.progressReady = true
		managed.progressApplied = true
		managed.mu.Unlock()
	}
}

// forceProgressReady isolates tests of post-admission transitions from the
// delegated-ingress readiness contract. Production code reaches this state
// only through a successful ApplyConsensusProgress call.
func (f *runtimeFixture) forceProgressReady(t *testing.T, sessionID [32]byte) {
	t.Helper()
	managed, err := f.service.runningSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = true
	managed.progressApplied = true
	managed.mu.Unlock()
}

func (f *runtimeFixture) request(t *testing.T, session Session, start uint32) WindowRequest {
	t.Helper()
	signature, err := simplex.SignDelegation(
		runtimePrivateSigner(f.leaderPriv),
		session.ID,
		start,
		f.keys.id,
	)
	if err != nil {
		t.Fatal(err)
	}
	return WindowRequest{
		SessionID:  session.ID,
		SourceADNL: f.sourceADNL,
		PleaseCollate: simplex.ConsensusPleaseCollate{
			WindowStartSlot: int32(start),
			Signature:       signature,
		},
	}
}

func (f *runtimeFixture) selfRequest(session Session, start uint32, deadline time.Time) SelfWindowRequest {
	return SelfWindowRequest{
		SessionID: session.ID,
		StartSlot: start,
		Deadline:  deadline,
		Signer:    runtimePrivateSigner(f.leaderPriv),
	}
}

func runtimeBuiltCandidate(request BuildRequest) *Candidate {
	blockBOC := []byte(fmt.Sprintf("block-%x-%d", request.Session.ID[:2], request.Slot))
	fileHash := sha256.Sum256(blockBOC)
	rootHash := sha256.Sum256(fileHash[:])
	collated := []byte(fmt.Sprintf("collated-%x-%d", request.Session.ID[:2], request.Slot))
	leader := request.Slot / request.Session.SlotsPerLeaderWindow % uint32(len(request.Session.Validators))
	return &Candidate{
		ID: ton.BlockIDExt{
			Workchain: request.Session.Shard.Workchain,
			Shard:     request.Session.Shard.Shard,
			SeqNo:     request.Slot + 1,
			RootHash:  append([]byte(nil), rootHash[:]...),
			FileHash:  append([]byte(nil), fileHash[:]...),
		},
		CreatedBy:        request.Session.Validators[leader].PublicKey,
		BlockBOC:         blockBOC,
		CollatedData:     collated,
		CollatedFileHash: sha256.Sum256(collated),
	}
}

func TestRuntimeProbeAndCommitValidation(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 2, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(1, 2, 0, time.Now().Add(time.Second))
	fixture.prepare(t, session, update)

	probe := WindowPreparation{SessionID: session.ID, SourceADNL: fixture.sourceADNL, StartSlot: 2}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatal(err)
	}
	status, err := fixture.storage.Status(context.Background())
	if err != nil {
		t.Fatalf("probe persisted state: status=%+v err=%v", status, err)
	}
	probe.SourceADNL[0] ^= 0xff
	if err = fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized probe error = %v", err)
	}
	probe.SourceADNL = fixture.sourceADNL
	probe.StartSlot = 1
	if err = fixture.service.Probe(context.Background(), probe); err == nil {
		t.Fatal("unaligned probe was accepted")
	}
	probe.StartSlot = 42
	if err = fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrWindowTooFar) {
		t.Fatalf("far-future probe error = %v", err)
	}

	request := fixture.request(t, session, 2)
	request.PleaseCollate.Signature[0] ^= 0xff
	if err = fixture.service.CommitDelegation(context.Background(), request); err == nil {
		t.Fatal("tampered delegation was accepted")
	}
	request = fixture.request(t, session, 2)
	if err = fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err = fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("exact duplicate delegation: %v", err)
	}
	conflict := request
	conflict.PleaseCollate.Signature = append([]byte(nil), request.PleaseCollate.Signature...)
	conflict.PleaseCollate.Signature[0] ^= 0xff
	if err = fixture.service.CommitDelegation(context.Background(), conflict); !errors.Is(err, ErrWindowConflict) {
		t.Fatalf("conflicting duplicate delegation error = %v, want ErrWindowConflict", err)
	}
	probe.StartSlot = 2
	if err = fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrAlreadyDelegated) {
		t.Fatalf("probe after final delegation error = %v", err)
	}
}

func TestRuntimeCommitDelegationStaysInReceiverMemory(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(62, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	probe := WindowPreparation{SessionID: session.ID, SourceADNL: fixture.sourceADNL, StartSlot: 1}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatal(err)
	}
	request := fixture.request(t, session, 1)
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = false
	managed.mu.Unlock()
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("exact duplicate delegation after progress disarmed: %v", err)
	}

	managed.mu.Lock()
	window, authorized := managed.authorizations[request.ID()]
	managed.mu.Unlock()
	if !authorized || !sameDelegationAuthorization(window, delegatedAuthorization{
		ID:                  request.ID(),
		Leader:              0,
		SourceADNL:          request.SourceADNL,
		CollatorKeyID:       fixture.keys.id,
		DelegationSignature: request.PleaseCollate.Signature,
		State:               delegatedAuthorizationPending,
	}) {
		t.Fatal("accepted delegation was not installed in receiver memory")
	}
}

func TestRuntimeDelegationHorizonMatchesReference(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(21, 3, 0, time.Now())
	fixture.prepare(t, session, update)

	probe := WindowPreparation{
		SessionID:  session.ID,
		SourceADNL: fixture.sourceADNL,
		StartSlot:  19 * session.SlotsPerLeaderWindow,
	}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatalf("window at distance 19 rejected: %v", err)
	}
	probe.StartSlot = 20 * session.SlotsPerLeaderWindow
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrWindowTooFar) {
		t.Fatalf("window at distance 20 error = %v, want ErrWindowTooFar", err)
	}
}

func TestRuntimeDelegationRequiresFreshConsensusProgress(t *testing.T) {
	emitted := make(chan CandidateArtifact, 1)
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)
	session, update := fixture.session(22, 1, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)

	probe := WindowPreparation{SessionID: session.ID, SourceADNL: fixture.sourceADNL}
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("probe before fresh progress = %v, want ErrAcquisitionNotReady", err)
	}
	request := fixture.request(t, session, 0)
	if err := fixture.service.CommitDelegation(context.Background(), request); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("commit before fresh progress = %v, want ErrAcquisitionNotReady", err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	_, authorized := managed.authorizations[request.ID()]
	managed.mu.Unlock()
	if authorized {
		t.Fatal("rejected delegation was retained in receiver memory")
	}
	if pipeline.buildCount() != 0 {
		t.Fatalf("builds before fresh progress = %d, want 0", pipeline.buildCount())
	}

	observed := update
	observed.HasCurrentWindow = true
	observed.CurrentWindowStartAt = time.Now()
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, observed),
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatalf("probe after fresh progress: %v", err)
	}
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("commit after fresh progress: %v", err)
	}
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("idempotent commit after fresh progress: %v", err)
	}
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrAlreadyDelegated) {
		t.Fatalf("probe after final delegation error = %v, want ErrAlreadyDelegated", err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.WindowID != (WindowID{SessionID: session.ID}) || artifact.Candidate.ID.Slot != 0 {
		t.Fatalf("candidate after fresh progress = %+v", artifact)
	}
}

func TestRuntimeDefersStaleActivationUntilRecoveredSessionAdvances(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	first := newRuntimeFixture(t, 1, 1, nil, storage, nil)
	session, update := first.session(24, 1, 0, time.Now())
	if err := first.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatal(err)
	}
	activation := SessionActivation{
		SessionID:      session.ID,
		Genesis:        []ton.BlockIDExt{runtimeTestBlockID(0, -1<<63, session.CatchainSeqno+1)},
		MinMasterchain: update.MasterchainBlock,
	}
	if err := first.service.ActivateSession(context.Background(), activation); err != nil {
		t.Fatal(err)
	}
	first.close(t)

	activateCalls := 0
	pipeline := &runtimeTestPipeline{
		activate: func(context.Context, SessionActivation, SessionUpdate) error {
			activateCalls++
			if activateCalls == 1 {
				return ErrAcquisitionNotReady
			}
			return nil
		},
	}
	second, err := NewService(ServiceOptions{
		ProductionMode:    ProductionModeDelegated,
		Storage:           storage,
		Pipeline:          pipeline,
		Keys:              first.keys,
		CollatorKeyID:     first.keys.id,
		AllowedValidators: map[[32]byte]struct{}{first.sourceADNL: {}},
		Emit:              func(context.Context, CandidateArtifact) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = second.Start(context.Background()); err != nil {
		t.Fatalf("start with stale activation: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := second.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	}()

	finalized := runtimeTestBlockID(session.Shard.Workchain, session.Shard.Shard, session.CatchainSeqno+2)
	if err = second.ObserveConsensusFinalized(context.Background(), session.ID, finalized); err != nil {
		t.Fatalf("observe finalization while durable activation is pending: %v", err)
	}
	managed, err := second.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	activationReady := managed.activationReady
	managed.mu.Unlock()
	if activationReady {
		t.Fatal("recovered activation unexpectedly became ready")
	}
	managed.policyMu.Lock()
	finalizedSeqno := managed.emptyPolicy.LastConsensusFinalizedSeqno
	managed.policyMu.Unlock()
	if finalizedSeqno != finalized.SeqNo {
		t.Fatalf("finalized watermark = %d, want %d", finalizedSeqno, finalized.SeqNo)
	}

	next := update
	next.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, update.MasterchainBlock.SeqNo+1)
	if err = second.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("advance recovered session: %v", err)
	}
	if err = second.ActivateSession(context.Background(), activation); err != nil {
		t.Fatalf("activate advanced recovered session: %v", err)
	}
	if activateCalls != 2 {
		t.Fatalf("activation calls = %d, want 2", activateCalls)
	}
}

func TestRuntimeConsensusFinalizationHandlesInactiveSession(t *testing.T) {
	t.Run("tentative retained through activation", func(t *testing.T) {
		fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
		defer fixture.close(t)

		session, update := fixture.session(60, 1, 0, time.Now())
		if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
			t.Fatal(err)
		}
		block := runtimeTestBlockID(session.Shard.Workchain, session.Shard.Shard, session.CatchainSeqno+7)
		if err := fixture.service.ObserveConsensusFinalized(
			context.Background(),
			session.ID,
			block,
		); err != nil {
			t.Fatalf("tentative finalization error = %v", err)
		}
		activation := SessionActivation{
			SessionID:      session.ID,
			Genesis:        []ton.BlockIDExt{runtimeTestBlockID(0, -1<<63, session.CatchainSeqno+1)},
			MinMasterchain: update.MasterchainBlock,
		}
		if err := fixture.service.ActivateSession(context.Background(), activation); err != nil {
			t.Fatal(err)
		}
		managed, err := fixture.service.runningSession(session.ID)
		if err != nil {
			t.Fatal(err)
		}
		managed.policyMu.Lock()
		finalized := managed.emptyPolicy.LastConsensusFinalizedSeqno
		managed.policyMu.Unlock()
		if finalized != block.SeqNo {
			t.Fatalf("finalized watermark after activation = %d, want %d", finalized, block.SeqNo)
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		pipeline := &runtimeTestPipeline{
			activate: func(context.Context, SessionActivation, SessionUpdate) error {
				return errors.New("pipeline activation failed")
			},
		}
		fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
		defer fixture.close(t)

		session, update := fixture.session(61, 1, 0, time.Now())
		if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
			t.Fatal(err)
		}
		activation := SessionActivation{
			SessionID:      session.ID,
			Genesis:        []ton.BlockIDExt{runtimeTestBlockID(0, -1<<63, session.CatchainSeqno+1)},
			MinMasterchain: update.MasterchainBlock,
		}
		if err := fixture.service.ActivateSession(context.Background(), activation); !errors.Is(err, ErrSessionUnavailable) {
			t.Fatalf("activation error = %v, want ErrSessionUnavailable", err)
		}
		block := runtimeTestBlockID(session.Shard.Workchain, session.Shard.Shard, session.CatchainSeqno+1)
		if err := fixture.service.ObserveConsensusFinalized(
			context.Background(),
			session.ID,
			block,
		); !errors.Is(err, ErrSessionUnavailable) {
			t.Fatalf("unavailable finalization error = %v, want ErrSessionUnavailable", err)
		}
	})
}

func TestRuntimeCandidateDurabilityTimingAndSignatures(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	emitted := make(chan CandidateArtifact, 2)
	var emitterErr atomic.Value
	fixture := newRuntimeFixture(t, 1, 2, nil, storage, func(ctx context.Context, artifact CandidateArtifact) error {
		if _, err := storage.Candidate(ctx, artifact.WindowID, artifact.Candidate.ID.Slot); err != nil {
			emitterErr.Store(err)
			return err
		}
		emitted <- artifact
		return nil
	})
	defer fixture.close(t)
	startAt := time.Now().Add(150 * time.Millisecond)
	session, update := fixture.session(2, 2, 0, startAt)
	fixture.prepare(t, session, update)
	request := fixture.request(t, session, 0)
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	// A shard candidate that carries a block leaves the moment its marker is
	// durable; the slot gate is kept for empty candidates and the masterchain
	// only (see broadcastAt). The emitter above has already checked the marker,
	// so what is asserted here is that the gate did not hold the block back.
	first := runtimeAwaitArtifact(t, emitted)
	if !time.Now().Before(startAt) {
		t.Fatal("the first candidate waited for the slot gate")
	}
	second := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != 0 || second.Candidate.ID.Slot != 1 ||
		second.Candidate.Parent != simplex.Parent(first.Candidate.ID) {
		t.Fatalf("candidate chain is invalid: first=%v second=%v", first.Candidate.ID, second.Candidate.ID)
	}
	for _, artifact := range []CandidateArtifact{first, second} {
		if !simplex.VerifyCandidateSignature(
			fixture.keys.public,
			session.ID,
			artifact.Candidate.ID,
			artifact.Candidate.Signature,
		) {
			t.Fatalf("slot %d candidate signature is invalid", artifact.Candidate.ID.Slot)
		}
		if artifact.Candidate.Delegation == nil || !simplex.VerifyDelegationSignature(
			fixture.leaderPub,
			session.ID,
			0,
			fixture.keys.id,
			artifact.Candidate.Delegation.Signature,
		) {
			t.Fatalf("slot %d delegation is invalid", artifact.Candidate.ID.Slot)
		}
	}
	if err, _ := emitterErr.Load().(error); err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && status.ActiveWindows == 0
	})
}

func TestRuntimeLogsCompletedBlockCollation(t *testing.T) {
	emitted := make(chan CandidateArtifact, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		candidate := runtimeBuiltCandidate(request)
		candidate.Stats = Stats{
			Transactions:      11,
			ExternalIncluded:  7,
			InternalsImported: 4,
			GasUsed:           12345,
			OutQueueSize:      9,
			Load:              LoadMedium,
		}

		return candidate, nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(81, 1, 4, time.Now())
	fixture.prepare(t, session, update)
	// Installed after preparation: activating the session logs its own line,
	// and this test counts only the lines of the collation itself.
	var output strings.Builder
	fixture.service.log = zerolog.New(&output).Level(zerolog.InfoLevel)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 4)); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && status.ActiveWindows == 0
	})

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("runtime log lines = %d, want built and emitted; output=%q", len(lines), output.String())
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("decode collation log: %v; output=%q", err, lines[0])
	}
	for field, want := range map[string]any{
		"level":             "info",
		"message":           "block collated",
		"slot":              float64(4),
		"window_start":      float64(4),
		"window_end":        float64(5),
		"is_empty":          false,
		"block_seqno":       float64(artifact.Candidate.Block.SeqNo),
		"block_bytes":       float64(len(artifact.BlockBOC)),
		"collated_bytes":    float64(len(artifact.CollatedData)),
		"transactions":      float64(11),
		"external_messages": float64(7),
		"internal_messages": float64(4),
		"gas_used":          float64(12345),
		"out_queue_size":    float64(9),
		"load_class":        float64(LoadMedium),
	} {
		if got := event[field]; got != want {
			t.Fatalf("collation log field %q = %v, want %v; event=%v", field, got, want, event)
		}
	}
	if _, exists := event["elapsed"]; !exists {
		t.Fatalf("collation log has no elapsed field: %v", event)
	}
	if event["block_root_hash"] == "" || event["block_file_hash"] == "" {
		t.Fatalf("collation log has incomplete block identity: %v", event)
	}
	var emittedEvent map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &emittedEvent); err != nil {
		t.Fatalf("decode emitted log: %v; output=%q", err, lines[1])
	}
	for field, want := range map[string]any{
		"message":      "candidate emitted",
		"slot":         float64(4),
		"window_start": float64(4),
		"window_end":   float64(5),
		"is_empty":     false,
		"block_seqno":  float64(artifact.Candidate.Block.SeqNo),
		"replayed":     false,
	} {
		if got := emittedEvent[field]; got != want {
			t.Fatalf("emitted log field %q = %v, want %v; event=%v", field, got, want, emittedEvent)
		}
	}
	if emittedEvent["candidate_hash"] == "" || emittedEvent["block_root_hash"] == "" ||
		emittedEvent["block_file_hash"] == "" {
		t.Fatalf("emitted log has incomplete candidate identity: %v", emittedEvent)
	}
}

func TestRuntimeLogsCompletedEmptyCandidate(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(82, 1, 4, time.Now())
	update.CurrentBase = simplex.Parent(simplex.CandidateID{Slot: 3, Hash: [32]byte{0x52}})
	emptyBlock := runtimeTestBlockID(session.Shard.Workchain, session.Shard.Shard, 18)
	pipeline.state = func(context.Context, BuildRequest) (CandidateState, error) {
		return CandidateState{Block: emptyBlock, NextSeqno: emptyBlock.SeqNo + 1, BeforeSplit: true}, nil
	}
	fixture.prepare(t, session, update)
	// Installed after preparation: activating the session logs its own line,
	// and this test counts only the lines of the collation itself.
	var output strings.Builder
	fixture.service.log = zerolog.New(&output).Level(zerolog.InfoLevel)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 4)); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if !artifact.Candidate.Empty {
		t.Fatal("before-split policy built an ordinary candidate")
	}
	if builds := pipeline.buildCount(); builds != 0 {
		t.Fatalf("empty candidate ran %d block builds", builds)
	}
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && status.ActiveWindows == 0
	})

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("empty runtime log lines = %d, want selected, built and emitted; output=%q", len(lines), output.String())
	}
	var selected map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &selected); err != nil {
		t.Fatal(err)
	}
	if selected["message"] != "empty candidate selected" || selected["reason"] != "produce_policy" {
		t.Fatalf("empty selection event = %v", selected)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &event); err != nil {
		t.Fatalf("decode empty collation log: %v; output=%q", err, lines[0])
	}
	for field, want := range map[string]any{
		"message":        "block collated",
		"slot":           float64(4),
		"window_start":   float64(4),
		"window_end":     float64(5),
		"is_empty":       true,
		"block_seqno":    float64(emptyBlock.SeqNo),
		"block_bytes":    float64(0),
		"collated_bytes": float64(0),
	} {
		if got := event[field]; got != want {
			t.Fatalf("empty collation log field %q = %v, want %v; event=%v", field, got, want, event)
		}
	}
	var emittedEvent map[string]any
	if err := json.Unmarshal([]byte(lines[2]), &emittedEvent); err != nil {
		t.Fatalf("decode empty emitted log: %v; output=%q", err, lines[1])
	}
	if emittedEvent["message"] != "candidate emitted" || emittedEvent["is_empty"] != true ||
		emittedEvent["replayed"] != false {
		t.Fatalf("empty emitted event = %v", emittedEvent)
	}
}

func TestRuntimeSerializesSessionWindowsAndParallelizesSessions(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	entered := make(chan BuildRequest, 2)
	release := make(chan struct{}, 2)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		defer active.Add(-1)
		entered <- request
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return runtimeBuiltCandidate(request), nil
		}
	}
	emitted := make(chan CandidateArtifact, 2)
	fixture := newRuntimeFixture(t, 2, 2, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact
		return nil
	})
	defer fixture.close(t)
	startAt := time.Now()
	first, firstUpdate := fixture.session(3, 1, 0, startAt)
	second, secondUpdate := fixture.session(4, 1, 0, startAt)
	fixture.prepare(t, first, firstUpdate)
	fixture.prepare(t, second, secondUpdate)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, first, 0)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, second, 0)); err != nil {
		t.Fatal(err)
	}
	runtimeAwaitBuild(t, entered)
	runtimeAwaitBuild(t, entered)
	if maximum.Load() != 2 {
		t.Fatalf("independent sessions max concurrency = %d, want 2", maximum.Load())
	}
	release <- struct{}{}
	release <- struct{}{}
	runtimeAwaitArtifact(t, emitted)
	runtimeAwaitArtifact(t, emitted)

	oldEntered := make(chan struct{}, 1)
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		current := active.Add(1)
		defer active.Add(-1)
		if request.Slot == 1 {
			oldEntered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if current > 1 {
			return nil, errors.New("same session productions overlapped")
		}
		return runtimeBuiltCandidate(request), nil
	}
	third, thirdUpdate := fixture.session(5, 1, 1, time.Now())
	fixture.prepare(t, third, thirdUpdate)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, third, 2)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, third, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldEntered:
	case <-time.After(time.Second):
		t.Fatal("current production did not start")
	}
	next := thirdUpdate
	next.CurrentWindowStart = 2
	next.CurrentWindowObservedSlot = 2
	next.CurrentWindowStartAt = time.Now()
	if err := fixture.service.UpdateSession(context.Background(), next); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("busy advancing update error = %v, want acquisition not ready", err)
	}
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())
		return status.ActiveWindows == 0
	})
	if err := fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("retry advancing update: %v", err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.SessionID != third.ID || artifact.Candidate.ID.Slot != 2 {
		t.Fatalf("unexpected transitioned candidate: %+v", artifact)
	}
}

func TestRuntimeStartsEveryActiveWindowAndCloseCancels(t *testing.T) {
	entered := make(chan [32]byte, 2)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		entered <- request.Session.ID
		<-ctx.Done()

		return nil, ctx.Err()
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	first, firstUpdate := fixture.session(6, 1, 0, time.Now())
	second, secondUpdate := fixture.session(7, 1, 0, time.Now())
	fixture.prepare(t, first, firstUpdate)
	fixture.prepare(t, second, secondUpdate)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, first, 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not start")
	}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, second, 0)); err != nil {
		t.Fatalf("second commit error = %v", err)
	}
	managed, err := fixture.service.runningSession(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	_, authorized := managed.authorizations[WindowID{SessionID: second.ID}]
	managed.mu.Unlock()
	if !authorized {
		t.Fatal("second delegation was not installed in receiver memory")
	}
	select {
	case id := <-entered:
		if id != second.ID {
			t.Fatalf("started session %x, want %x", id, second.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("second active window did not start immediately")
	}
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && status.ActiveWindows == 2
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Probe(context.Background(), WindowPreparation{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close probe error = %v", err)
	}
}

// A persisted candidate marker may outlive the process that signed it, but the
// payload behind it does not. The successor must neither re-emit (it has no
// bytes) nor rebuild (collation is not byte-reproducible, so a rebuilt
// candidate would be a second signature on a slot already broadcast). It ends
// the window instead, and does not retry it.
func TestRuntimePendingCandidateEndsWindowInsteadOfRebuilding(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	emitFailed := make(chan struct{}, 1)
	firstPipeline := &runtimeTestPipeline{}
	first := newRuntimeFixture(t, 1, 1, firstPipeline, storage, func(context.Context, CandidateArtifact) error {
		select {
		case emitFailed <- struct{}{}:
		default:
		}
		return errors.New("broadcast unavailable")
	})
	session, update := first.session(8, 1, 0, time.Now())
	first.prepare(t, session, update)
	if err := first.service.CommitDelegation(context.Background(), first.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-emitFailed:
	case <-time.After(time.Second):
		t.Fatal("first emission did not fail")
	}
	first.close(t)

	recovered := make(chan CandidateArtifact, 1)
	secondPipeline := &runtimeTestPipeline{}
	secondService, err := NewService(ServiceOptions{
		ProductionMode:    ProductionModeDelegated,
		Storage:           storage,
		Pipeline:          secondPipeline,
		Keys:              first.keys,
		CollatorKeyID:     first.keys.id,
		AllowedValidators: map[[32]byte]struct{}{first.sourceADNL: {}},
		Emit: func(_ context.Context, artifact CandidateArtifact) error {
			recovered <- artifact
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = secondService.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := secondService.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	select {
	case artifact := <-recovered:
		t.Fatalf("recovered candidate emitted before fresh consensus progress: %+v", artifact)
	case <-time.After(20 * time.Millisecond):
	}
	if err = secondService.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, update),
	); err != nil {
		t.Fatal(err)
	}
	// Receiver authority is not durable. Replaying progress after restart must
	// stay idle until the validator resends the final delegation.
	select {
	case artifact := <-recovered:
		t.Fatalf("recovered candidate emitted without a delegation resend: %+v", artifact)
	case <-time.After(20 * time.Millisecond):
	}
	if secondPipeline.buildCount() != 0 {
		t.Fatalf("successor built %d candidates without a delegation resend", secondPipeline.buildCount())
	}
	if err = secondService.CommitDelegation(
		context.Background(),
		first.request(t, session, 0),
	); err != nil {
		t.Fatalf("resend delegation after restart: %v", err)
	}
	runtimeAwait(t, func() bool {
		status, statusErr := secondService.Status(context.Background())

		return statusErr == nil && status.FailedWindows == 1 && status.ActiveWindows == 0
	})
	status, err := secondService.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status.LastError, errWindowNotResumable.Error()) {
		t.Fatalf("window ended with %q, want an unresumable-window failure", status.LastError)
	}
	select {
	case artifact := <-recovered:
		t.Fatalf("successor emitted a candidate it no longer has bytes for: %+v", artifact)
	default:
	}
	if secondPipeline.buildCount() != 0 {
		t.Fatalf("successor rebuilt %d candidates for a slot it already signed", secondPipeline.buildCount())
	}
	if status.RetryingWindows != 0 {
		t.Fatalf("unresumable window is retrying %d times", status.RetryingWindows)
	}
}

func TestRuntimeRestartRequiresFreshProgressBeforeNextDelegation(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	first := newRuntimeFixture(t, 1, 1, nil, storage, nil)
	session, update := first.session(68, 4, 0, time.Time{})
	update.HasCurrentWindow = false
	first.prepare(t, session, update)
	previous := update
	previous.HasCurrentWindow = true
	previous.CurrentWindowObservedSlot = 2
	if err := first.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, previous),
	); err != nil {
		t.Fatal(err)
	}
	if err := first.service.CommitDelegation(
		context.Background(),
		first.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	first.close(t)

	emitted := make(chan CandidateArtifact, 1)
	secondPipeline := &runtimeTestPipeline{}
	second, err := NewService(ServiceOptions{
		ProductionMode:    ProductionModeDelegated,
		Storage:           storage,
		Pipeline:          secondPipeline,
		Keys:              first.keys,
		CollatorKeyID:     first.keys.id,
		AllowedValidators: map[[32]byte]struct{}{first.sourceADNL: {}},
		Emit: func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := second.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	}()

	managed, err := second.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	recoveredAuthorities := len(managed.authorizations)
	managed.mu.Unlock()
	if recoveredAuthorities != 0 {
		t.Fatalf("receiver recovered %d in-memory delegations, want zero", recoveredAuthorities)
	}

	probe := WindowPreparation{
		SessionID:  session.ID,
		SourceADNL: first.sourceADNL,
		StartSlot:  4,
	}
	if err = second.Probe(context.Background(), probe); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("recovered probe before fresh progress = %v, want ErrAcquisitionNotReady", err)
	}
	request := first.request(t, session, 4)
	if err = second.CommitDelegation(
		context.Background(),
		request,
	); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("recovered commit before fresh progress = %v, want ErrAcquisitionNotReady", err)
	}
	next := previous
	next.HasCurrentWindow = true
	next.CurrentWindowStart = 4
	next.CurrentWindowObservedSlot = 4
	next.CurrentWindowStartAt = time.Now()
	if err = second.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, next),
	); err != nil {
		t.Fatal(err)
	}
	if err = second.Probe(context.Background(), probe); err != nil {
		t.Fatalf("recovered probe after fresh progress: %v", err)
	}
	if err = second.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("fresh next-window delegation after receiver restart: %v", err)
	}
	if err = second.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("idempotent next-window delegation after receiver restart: %v", err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.WindowID != (WindowID{SessionID: session.ID, StartSlot: 4}) ||
		artifact.Candidate.ID.Slot != 4 {
		t.Fatalf("fresh next-window candidate after receiver restart = %+v", artifact)
	}
}

func TestRuntimeProgressAdvancesGenesisWithoutSelectedBase(t *testing.T) {
	advanced := make(chan ConsensusBaseUpdate, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.advance = func(_ context.Context, request ConsensusBaseUpdate) error {
		advanced <- request

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(55, 4, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)
	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: time.Now(),
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	request := <-advanced
	if request.Base != nil || request.Update.CurrentBase.Exists || request.Session.ID != session.ID {
		t.Fatalf("genesis base update = %+v", request)
	}
}

func TestRuntimeProgressRejectsMisboundSelectedBase(t *testing.T) {
	for _, test := range []struct {
		name          string
		changeBinding func(*Session, *simplex.CandidateID)
	}{
		{
			name: "different session",
			changeBinding: func(session *Session, _ *simplex.CandidateID) {
				session.ID[0] ^= 0xff
			},
		},
		{
			name: "different candidate",
			changeBinding: func(_ *Session, candidate *simplex.CandidateID) {
				candidate.Hash[0] ^= 0xff
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			pipeline := &runtimeTestPipeline{}
			fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
			defer fixture.close(t)

			session, update := fixture.session(58, 4, 0, time.Time{})
			update.HasCurrentWindow = false
			fixture.prepare(t, session, update)
			selected := simplex.CandidateID{Slot: 0, Hash: [32]byte{0x61}}
			boundSession := session
			boundCandidate := selected
			test.changeBinding(&boundSession, &boundCandidate)
			progress := ConsensusProgress{
				SessionID: session.ID,
				Window: simplex.Window{
					Base:         simplex.Parent(selected),
					ObservedSlot: 1,
					StartSlot:    0,
					EndSlot:      4,
					Leader:       0,
					ObservedAt:   time.Now(),
				},
				Base: runtimeSelectedBase(t, boundSession, boundCandidate),
			}
			if err := fixture.service.ApplyConsensusProgress(
				context.Background(),
				progress,
			); !errors.Is(err, ErrCandidateConflict) {
				t.Fatalf("misbound progress error = %v, want ErrCandidateConflict", err)
			}
			_, _, advanced, _, _ := pipeline.counts()
			if advanced != 0 {
				t.Fatalf("misbound base reached pipeline %d times", advanced)
			}
		})
	}
}

func TestRuntimeFirstMidWindowProgressDoesNotProduce(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(56, 4, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)

	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 2,
			StartSlot:    0,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := fixture.storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Update.CurrentWindowObservedSlot != 2 || !record.Update.CurrentWindowStartAt.IsZero() {
		t.Fatalf("stored mid-window progress = %+v", record.Update)
	}
	// launchProductionLocked publishes the job before spawning its goroutine, so
	// an empty map proves synchronously that nothing was started.
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if launched := runtimeProductionCount(managed); launched != 0 {
		t.Fatalf("mid-window recovery registered %d productions", launched)
	}
	if builds := fixture.pipeline.buildCount(); builds != 0 {
		t.Fatalf("mid-window recovery built %d candidates", builds)
	}
}

func TestRuntimeProgressAdvancesBaseAndPreservesWindowStart(t *testing.T) {
	advancedRequests := make(chan ConsensusBaseUpdate, 2)
	var restores atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.advance = func(_ context.Context, request ConsensusBaseUpdate) error {
		advancedRequests <- request

		return nil
	}
	pipeline.restore = func(context.Context, BuildRequest, CandidateArtifact) error {
		restores.Add(1)

		return errors.New("unexpected lineage restore")
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(57, 4, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)

	startedAt := time.Now().Add(-time.Second)
	initial := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: startedAt,
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	initialRequest := <-advancedRequests
	if initialRequest.Base != nil {
		t.Fatal("genesis progress carried a selected base")
	}

	candidateID := simplex.CandidateID{Slot: 0, Hash: [32]byte{0x71}}
	selected := runtimeSelectedBase(t, session, candidateID)
	advanced := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Parent(candidateID),
			ObservedSlot: 1,
			StartSlot:    0,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: time.Now(),
		Base:    selected,
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), advanced); err != nil {
		t.Fatal(err)
	}
	advancedRequest := <-advancedRequests
	if advancedRequest.Base != selected || advancedRequest.Update.CurrentBase != advanced.Window.Base {
		t.Fatalf("selected base update = %+v", advancedRequest)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := fixture.storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.CurrentWindowStartAt.Equal(startedAt) ||
		record.Update.CurrentBase != simplex.Parent(candidateID) {
		t.Fatalf("advanced progress = %+v", record.Update)
	}
	if restores.Load() != 0 {
		t.Fatalf("direct selected-base progress restored %d lineage candidates", restores.Load())
	}
}

func TestRuntimeProgressBaseAdvanceFailureCanRetry(t *testing.T) {
	advanceErr := fmt.Errorf("install selected base: %w", ErrAcquisitionNotReady)
	var calls atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.advance = func(context.Context, ConsensusBaseUpdate) error {
		if calls.Add(1) == 1 {
			return advanceErr
		}

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(60, 4, 0, time.Now())
	fixture.prepare(t, session, update)

	candidateID := simplex.CandidateID{Slot: 0, Hash: [32]byte{0x81}}
	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Parent(candidateID),
			ObservedSlot: 1,
			StartSlot:    0,
			EndSlot:      session.SlotsPerLeaderWindow,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: update.CurrentWindowStartAt,
		Base:    runtimeSelectedBase(t, session, candidateID),
	}
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		progress,
	); !errors.Is(err, advanceErr) {
		t.Fatalf("first progress error = %v, want %v", err, advanceErr)
	}
	record, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(update) {
		t.Fatalf("failed base advance changed session update: %+v", record.Update)
	}
	if err = fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatalf("retry progress: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("base advance calls = %d, want 2", calls.Load())
	}
	record, err = fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Update.CurrentBase != progress.Window.Base ||
		record.Update.CurrentWindowObservedSlot != progress.Window.ObservedSlot {
		t.Fatalf("retried progress was not committed: %+v", record.Update)
	}
}

func TestRuntimeProgressBaseAdvanceFailureDoesNotResumeOldProduction(t *testing.T) {
	advanceErr := fmt.Errorf("install selected base: %w", ErrAcquisitionNotReady)
	var advanceCalls atomic.Int32
	started := make(chan struct{}, 2)
	buildCancelled := make(chan struct{})
	releaseBuild := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, _ BuildRequest) (*Candidate, error) {
		started <- struct{}{}
		<-ctx.Done()
		close(buildCancelled)
		<-releaseBuild

		return nil, ctx.Err()
	}
	pipeline.advance = func(context.Context, ConsensusBaseUpdate) error {
		if advanceCalls.Add(1) == 1 {
			return advanceErr
		}

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(62, 2, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old production did not start")
	}

	candidateID := simplex.CandidateID{Slot: 2, Hash: [32]byte{0x91}}
	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Parent(candidateID),
			ObservedSlot: 3,
			StartSlot:    2,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		Base: runtimeSelectedBase(t, session, candidateID),
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("busy progress error = %v, want acquisition not ready", err)
	}
	select {
	case <-buildCancelled:
	case <-time.After(time.Second):
		t.Fatal("old production was not cancelled")
	}
	close(releaseBuild)
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())
		return status.ActiveWindows == 0
	})
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); !errors.Is(err, advanceErr) {
		t.Fatalf("base advance error = %v, want %v", err, advanceErr)
	}
	select {
	case <-started:
		t.Fatal("failed progress relaunched production against the old base")
	case <-time.After(20 * time.Millisecond):
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatalf("retry progress: %v", err)
	}
	if _, err := fixture.service.Session(context.Background(), session.ID); err != nil {
		t.Fatalf("session poisoned by base advance failure: %v", err)
	}
}

func TestRuntimeRestartProgressPreservesDurableWindowStart(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	first := newRuntimeFixture(t, 1, 1, nil, storage, nil)
	session, update := first.session(58, 4, 0, time.Time{})
	update.HasCurrentWindow = false
	first.prepare(t, session, update)
	startedAt := time.Now().Add(-time.Second)
	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: startedAt,
	}
	if err := first.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	first.close(t)

	second := newUnstartedRuntimeService(
		t,
		storage,
		&runtimeTestPipeline{},
		first.keys,
		map[[32]byte]struct{}{first.sourceADNL: {}},
		nil,
	)
	if err := second.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := second.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	progress.StartAt = time.Now()
	progress.Window.ObservedAt = time.Now()
	if err := second.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	record, err := storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.CurrentWindowStartAt.Equal(startedAt) {
		t.Fatalf("restart replaced durable window start %s with %s",
			startedAt, record.Update.CurrentWindowStartAt)
	}
}

func TestRuntimeProgressStorageFailureCanRetry(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	var saves atomic.Int32
	saveErr := errors.New("progress WAL failure")
	storage.saveSessionError = func(SessionRecord) error {
		if saves.Add(1) == 3 {
			return saveErr
		}

		return nil
	}
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, storage, nil)
	defer fixture.close(t)
	session, update := fixture.session(59, 4, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)
	fixture.forceProgressReady(t, session.ID)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      4,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: time.Now(),
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatalf("accepted progress returned storage error: %v", err)
	}
	if _, err := fixture.service.Session(context.Background(), session.ID); err != nil {
		t.Fatalf("session poisoned by progress storage failure: %v", err)
	}
	runtimeAwait(t, func() bool {
		status, statusErr := fixture.service.Status(context.Background())

		return statusErr == nil && strings.Contains(status.LastError, saveErr.Error())
	})
	runtimeAwait(t, func() bool { return pipeline.buildCount() != 0 })
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.HasCurrentWindow || record.Update.CurrentWindowStartAt != progress.StartAt {
		t.Fatalf("retried progress was not committed: %+v", record.Update)
	}
	if saves.Load() != 4 {
		t.Fatalf("session save attempts = %d, want 4", saves.Load())
	}
}

func TestRuntimeProgressStorageFailureDropsStaleArmState(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	var saves atomic.Int32
	saveErr := errors.New("session WAL failure")
	storage.saveSessionError = func(SessionRecord) error {
		// 1 prepare, 2 activate, 3 the failed update, 4 the failed progress.
		if n := saves.Add(1); n == 3 || n == 4 {
			return saveErr
		}

		return nil
	}
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, storage, nil)
	defer fixture.close(t)

	session, update := fixture.session(96, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Authorize the next window without arming it. Its production can only
	// start once the published session view advances onto that window.
	if err = fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 1),
	); err != nil {
		t.Fatal(err)
	}

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now().Add(time.Hour)
	if err = fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("accepted update returned storage error: %v", err)
	}
	progress := runtimeConsensusProgress(session, next)
	if err = fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatalf("accepted progress returned storage error: %v", err)
	}

	// The accepted progress supersedes the view whose arm state the failed update
	// captured. An exact update cannot re-arm production behind the pending WAL.
	if err = fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("exact update while progress WAL is pending: %v", err)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	runtimeAwait(t, func() bool { return runtimeProductionCount(managed) == 1 })
	if launched := runtimeProductionCount(managed); launched != 1 {
		t.Fatalf("productions after service-owned WAL retry = %d, want 1", launched)
	}
}

func runtimeProductionCount(managed *managedCollatorSession) int {
	managed.mu.Lock()
	defer managed.mu.Unlock()

	return len(managed.productions)
}

func TestRuntimeRetriesDurableCandidateAfterBroadcastFailure(t *testing.T) {
	var attempts atomic.Int32
	emitted := make(chan CandidateArtifact, 1)
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		if attempts.Add(1) == 1 {
			return context.DeadlineExceeded
		}
		emitted <- artifact
		return nil
	})
	defer fixture.close(t)
	session, update := fixture.session(9, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	request := fixture.request(t, session, 0)
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	runtimeAwaitArtifact(t, emitted)
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && status.ActiveWindows == 0
	})
	if attempts.Load() != 2 {
		t.Fatalf("broadcast attempts = %d, want 2", attempts.Load())
	}
	if pipeline.buildCount() != 1 {
		t.Fatalf("durable candidate was rebuilt %d times", pipeline.buildCount())
	}
}

func TestProductionRetryDelayIsImmediateThenCapped(t *testing.T) {
	t.Parallel()

	want := []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	for retry, delay := range want {
		if got := productionRetryDelay(retry); got != delay {
			t.Fatalf("retry %d delay = %s, want %s", retry, got, delay)
		}
	}
}

func TestRuntimeMasterchainWaitsWithEmptyCandidatesUntilConsensusFinalization(t *testing.T) {
	emitted := make(chan CandidateArtifact, 4)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		candidate := runtimeBuiltCandidate(request)
		candidate.ID.SeqNo = 165

		return candidate, nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(31, 4, 0, time.Now())
	session.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
	update.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 164)
	update.HasFinalizedBlock = true
	update.FinalizedBlock = cloneBlockID(update.MasterchainBlock)
	if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.ActivateSession(context.Background(), SessionActivation{
		SessionID:      session.ID,
		Genesis:        []ton.BlockIDExt{cloneBlockID(update.MasterchainBlock)},
		MinMasterchain: cloneBlockID(update.MasterchainBlock),
	}); err != nil {
		t.Fatal(err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = true
	managed.progressApplied = true
	managed.mu.Unlock()

	if err = fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}

	first := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.Empty || first.Candidate.Block.SeqNo != 165 {
		t.Fatalf("first candidate = %+v, want ordinary masterchain block 165", first.Candidate)
	}
	previous := first
	for slot := uint32(1); slot < session.SlotsPerLeaderWindow; slot++ {
		artifact := runtimeAwaitArtifact(t, emitted)
		if !artifact.Candidate.Empty || artifact.Candidate.ID.Slot != slot ||
			artifact.Candidate.Block.SeqNo != 165 {
			t.Fatalf("candidate at slot %d = %+v, want empty reference to block 165", slot, artifact.Candidate)
		}
		// The empty-policy loop advances the consensus lineage like every other
		// emitting branch: each candidate chains to the one it followed.
		if artifact.Candidate.Parent != simplex.Parent(previous.Candidate.ID) {
			t.Fatalf("candidate at slot %d parent = %+v, want %+v",
				slot, artifact.Candidate.Parent, simplex.Parent(previous.Candidate.ID))
		}
		previous = artifact
	}
	if got := pipeline.buildCount(); got != 1 {
		t.Fatalf("masterchain builds = %d, want 1", got)
	}
}

func TestRuntimeConsensusFinalizationAllowsNextMasterchainBlock(t *testing.T) {
	emitted := make(chan CandidateArtifact, 2)
	finalizationErr := make(chan error, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		candidate := runtimeBuiltCandidate(request)
		candidate.ID.SeqNo = 165
		if request.Previous != nil {
			candidate.ID.SeqNo = request.Previous.Candidate.Block.SeqNo + 1
		}

		return candidate, nil
	}
	var service *Service
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(ctx context.Context, artifact CandidateArtifact) error {
		if artifact.Candidate.ID.Slot == 0 {
			finalizationErr <- service.ObserveConsensusFinalized(ctx, artifact.SessionID, artifact.Candidate.Block)
		}
		emitted <- artifact

		return nil
	})
	service = fixture.service
	defer fixture.close(t)

	session, update := fixture.session(32, 2, 0, time.Now())
	session.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
	update.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 164)
	update.HasFinalizedBlock = true
	update.FinalizedBlock = cloneBlockID(update.MasterchainBlock)
	if err := service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatal(err)
	}
	if err := service.ActivateSession(context.Background(), SessionActivation{
		SessionID:      session.ID,
		Genesis:        []ton.BlockIDExt{cloneBlockID(update.MasterchainBlock)},
		MinMasterchain: cloneBlockID(update.MasterchainBlock),
	}); err != nil {
		t.Fatal(err)
	}
	managed, err := service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = true
	managed.progressApplied = true
	managed.mu.Unlock()
	if err = service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}

	first := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.Empty || first.Candidate.Block.SeqNo != 165 {
		t.Fatalf("first candidate = %+v, want ordinary block 165", first.Candidate)
	}
	if err = <-finalizationErr; err != nil {
		t.Fatal(err)
	}
	second := runtimeAwaitArtifact(t, emitted)
	if second.Candidate.Empty || second.Candidate.Block.SeqNo != 166 {
		t.Fatalf("second candidate = %+v, want ordinary block 166", second.Candidate)
	}
	if got := pipeline.buildCount(); got != 2 {
		t.Fatalf("masterchain builds = %d, want 2", got)
	}
}

func TestRuntimeBeforeSplitStateEmitsEmptyWithoutStartingBuild(t *testing.T) {
	emitted := make(chan CandidateArtifact, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.state = func(_ context.Context, request BuildRequest) (CandidateState, error) {
		return CandidateState{
			Block:       runtimeTestBlockID(request.Session.Shard.Workchain, request.Session.Shard.Shard, 41),
			NextSeqno:   42,
			BeforeSplit: true,
		}, nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(33, 1, 4, time.Now())
	update.CurrentBase = simplex.Parent(simplex.CandidateID{Slot: 3, Hash: sha256.Sum256([]byte("base"))})
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, update.CurrentWindowStart),
	); err != nil {
		t.Fatal(err)
	}

	artifact := runtimeAwaitArtifact(t, emitted)
	if !artifact.Candidate.Empty || artifact.Candidate.Block.SeqNo != 41 {
		t.Fatalf("before-split candidate = %+v, want empty reference", artifact.Candidate)
	}
	if got := pipeline.buildCount(); got != 0 {
		t.Fatalf("before-split builds = %d, want 0", got)
	}
}

func TestRuntimeSoftTimeoutKeepsHardBuildFuture(t *testing.T) {
	release := make(chan struct{})
	softReached := make(chan struct{}, 1)
	buildCanceled := make(chan struct{}, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		select {
		case <-ctx.Done():
			buildCanceled <- struct{}{}
			return nil, ctx.Err()
		case <-release:
			return runtimeBuiltCandidate(request), nil
		}
	}
	pipeline.soft = func(context.Context, SoftTimeoutRequest) (SoftTimeoutDecision, error) {
		softReached <- struct{}{}
		return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact
		return nil
	})
	defer fixture.close(t)
	session, update := fixture.session(10, 1, 0, time.Now())
	update.TargetRate = 20 * time.Millisecond
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-softReached:
	case <-time.After(time.Second):
		t.Fatal("soft timeout was not observed")
	}
	select {
	case <-buildCanceled:
		t.Fatal("soft timeout canceled the hard build future")
	default:
	}
	close(release)
	runtimeAwaitArtifact(t, emitted)
}

func TestRuntimeEmptyTimeoutReusesBuildFuture(t *testing.T) {
	release := make(chan struct{})
	softRequests := make(chan SoftTimeoutRequest, 2)
	commits := make(chan CandidateCommit, 2)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return runtimeBuiltCandidate(request), nil
		}
	}
	pipeline.soft = func(_ context.Context, request SoftTimeoutRequest) (SoftTimeoutDecision, error) {
		softRequests <- request
		if request.Current.Slot != 2 {
			return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
		}
		return SoftTimeoutDecision{
			Action: SoftTimeoutEmitEmpty,
			Block: runtimeTestBlockID(
				request.Current.Session.Shard.Workchain,
				request.Current.Session.Shard.Shard,
				77,
			),
		}, nil
	}
	pipeline.commit = func(_ context.Context, commit CandidateCommit) error {
		commits <- commit
		return nil
	}
	emitted := make(chan CandidateArtifact, 2)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact
		return nil
	})
	defer fixture.close(t)
	session, update := fixture.session(11, 2, 2, time.Now())
	update.TargetRate = 20 * time.Millisecond
	update.CurrentBase = simplex.Parent(simplex.CandidateID{Slot: 1, Hash: sha256.Sum256([]byte("base"))})
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 2)); err != nil {
		t.Fatal(err)
	}
	firstTimeout := <-softRequests
	if firstTimeout.Active.Slot != 2 || firstTimeout.Current.Slot != 2 {
		t.Fatalf("first timeout request = active %d current %d", firstTimeout.Active.Slot, firstTimeout.Current.Slot)
	}
	first := runtimeAwaitArtifact(t, emitted)
	if !first.Candidate.Empty || first.Candidate.ID.Slot != 2 {
		t.Fatalf("first candidate is not the expected empty: %+v", first.Candidate)
	}
	firstCommit := <-commits
	if firstCommit.Request.Slot != 2 || firstCommit.Built != nil ||
		firstCommit.Artifact.Candidate.ID != first.Candidate.ID {
		t.Fatalf("empty commit does not bind its current request: %+v", firstCommit)
	}
	select {
	case secondTimeout := <-softRequests:
		if secondTimeout.Active.Slot != 2 || secondTimeout.Current.Slot != 3 {
			t.Fatalf(
				"reused timeout request = active %d current %d",
				secondTimeout.Active.Slot,
				secondTimeout.Current.Slot,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("second soft timeout was not observed")
	}
	close(release)
	second := runtimeAwaitArtifact(t, emitted)
	if second.Candidate.Empty || second.Candidate.ID.Slot != 3 ||
		second.Candidate.Parent != simplex.Parent(first.Candidate.ID) {
		t.Fatalf("reused future candidate is invalid: %+v", second.Candidate)
	}
	secondCommit := <-commits
	if secondCommit.Request.Slot != 3 || secondCommit.Request.Parent != simplex.Parent(first.Candidate.ID) ||
		secondCommit.Built == nil || secondCommit.Artifact.Candidate.ID != second.Candidate.ID {
		t.Fatalf("reused future commit does not bind the current slot and parent: %+v", secondCommit)
	}
	if pipeline.buildCount() != 1 {
		t.Fatalf("build future started %d times, want 1", pipeline.buildCount())
	}
}

func TestRuntimeFailedUpdateResumesPreviousWindow(t *testing.T) {
	started := make(chan struct{}, 1)
	var builds atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		if builds.Add(1) == 1 {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return runtimeBuiltCandidate(request), nil
	}
	pipeline.update = func(context.Context, Session, SessionUpdate) error {
		return errors.New("authenticated acquisition update failed")
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact
		return nil
	})
	defer fixture.close(t)
	session, update := fixture.session(12, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old window build did not start")
	}
	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	if err := fixture.service.UpdateSession(context.Background(), next); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("busy update error = %v, want acquisition not ready", err)
	}
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())
		return status.ActiveWindows == 0
	})
	if err := fixture.service.UpdateSession(context.Background(), next); err == nil {
		t.Fatal("failing pipeline update was accepted")
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.Candidate.ID.Slot != 0 {
		t.Fatalf("resumed slot = %d, want 0", artifact.Candidate.ID.Slot)
	}
	if builds.Load() != 2 {
		t.Fatalf("build attempts = %d, want 2", builds.Load())
	}
}

func TestRuntimeUpdateStorageFailureCanRetry(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	var saves atomic.Int32
	saveErr := errors.New("session WAL failure")
	storage.saveSessionError = func(SessionRecord) error {
		if saves.Add(1) == 3 {
			return saveErr
		}

		return nil
	}

	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, storage, nil)
	defer fixture.close(t)

	session, update := fixture.session(18, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now().Add(time.Second)
	if err := fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("accepted update returned storage error: %v", err)
	}
	record, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatalf("session poisoned by update storage failure: %v", err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("effective update after storage failure = %+v, want %+v", record.Update, next)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	pipeline.mu.Lock()
	updates := pipeline.updated
	pipeline.mu.Unlock()
	if updates != 1 {
		t.Fatalf("pipeline update calls = %d, want 1", updates)
	}
	record, err = fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("retried update was not committed: %+v", record.Update)
	}
}

func TestRuntimeSessionAdmissionCompletesBeforeRetirement(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	fixture := newRuntimeFixture(t, 1, 1, nil, baseStorage, nil)
	defer fixture.close(t)

	session, update := fixture.session(181, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	storage := &runtimeBlockedSessionAdmissionStorage{
		runtimeMemoryStorage: baseStorage,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
		deleteEntered:        make(chan struct{}),
	}
	fixture.service.opts.Storage = storage
	released := false
	defer func() {
		if !released {
			close(storage.release)
		}
	}()

	next := update
	next.CurrentWindowStart++
	next.CurrentWindowObservedSlot++
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(time.Second)
	updateCtx, cancelUpdate := context.WithCancel(context.Background())
	defer cancelUpdate()
	updateResult := make(chan error, 1)
	go func() {
		updateResult <- fixture.service.UpdateSession(updateCtx, next)
	}()
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("session update did not enter storage admission")
	}

	retireStarted := make(chan struct{})
	retireResult := make(chan error, 1)
	go func() {
		close(retireStarted)
		retireResult <- fixture.service.RetireSession(context.Background(), session.ID)
	}()
	<-retireStarted
	cancelUpdate()
	select {
	case <-storage.deleteEntered:
		t.Fatal("retirement overtook an unfinished session-write admission")
	case <-time.After(50 * time.Millisecond):
	}

	close(storage.release)
	released = true
	if err := <-updateResult; err != nil {
		t.Fatalf("accepted update returned caller cancellation: %v", err)
	}
	select {
	case <-storage.deleteEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement did not start after session admission returned")
	}
	if err := <-retireResult; err != nil {
		t.Fatal(err)
	}
	if _, err := baseStorage.Session(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired session read = %v, want ErrNotFound", err)
	}
}

func TestRuntimeRetireFailureResumesAcceptedSessionWrite(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	writeErr := errors.New("accepted session write failed while retiring")
	var writeFailed atomic.Bool
	stored := make(chan struct{})
	releaseWrite := make(chan struct{})
	storage := &runtimeLateSessionCommitStorage{
		runtimeMemoryStorage: baseStorage,
		stored:               stored,
		release:              releaseWrite,
	}
	retireEntered := make(chan struct{})
	releaseRetire := make(chan struct{})
	retireErr := errors.New("temporary pipeline retirement failure")
	var retireCalls atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.retire = func(context.Context, [32]byte) error {
		if retireCalls.Add(1) == 1 {
			close(retireEntered)
			<-releaseRetire

			return retireErr
		}

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, baseStorage, nil)
	defer fixture.close(t)

	session, update := fixture.session(184, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	baseStorage.saveSessionError = func(SessionRecord) error {
		if writeFailed.CompareAndSwap(false, true) {
			return writeErr
		}

		return nil
	}
	fixture.service.opts.Storage = storage
	next := update
	next.CurrentWindowStart++
	next.CurrentWindowObservedSlot++
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(time.Second)
	if err := fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stored:
	case <-time.After(time.Second):
		t.Fatal("accepted update did not reach storage")
	}

	retireResult := make(chan error, 1)
	go func() {
		retireResult <- fixture.service.RetireSession(context.Background(), session.ID)
	}()
	select {
	case <-retireEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement did not enter the pipeline")
	}
	close(releaseWrite)
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && strings.Contains(status.LastError, writeErr.Error())
	})
	close(releaseRetire)
	if err := <-retireResult; !errors.Is(err, retireErr) {
		t.Fatalf("retire error = %v, want %v", err, retireErr)
	}

	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := baseStorage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("durable update after failed retirement = %+v, want %+v", record.Update, next)
	}
}

func TestRuntimeUpdateStorageFailureAdvancesFromAcceptedPipelineView(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	var saves atomic.Int32
	saveErr := errors.New("session WAL failure")
	storage.saveSessionError = func(SessionRecord) error {
		if saves.Add(1) == 3 {
			return saveErr
		}

		return nil
	}

	var pipelineUpdate SessionUpdate
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(_ context.Context, _ Session, next SessionUpdate) error {
		if err := ValidateSessionUpdateAdvance(pipelineUpdate, next); err != nil {
			return err
		}
		pipelineUpdate = cloneSessionUpdate(next)

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, storage, nil)
	defer fixture.close(t)

	session, update := fixture.session(180, 1, 0, time.Now())
	pipelineUpdate = cloneSessionUpdate(update)
	fixture.prepare(t, session, update)

	first := update
	first.CurrentWindowStart = 1
	first.CurrentWindowObservedSlot = 1
	first.CurrentWindowStartAt = time.Now().Add(time.Second)
	if err := fixture.service.UpdateSession(context.Background(), first); err != nil {
		t.Fatalf("accepted first update returned storage error: %v", err)
	}

	next := first
	next.MasterchainBlock.SeqNo++
	next.MasterchainBlock.RootHash = bytes.Repeat([]byte{0x81}, 32)
	next.MasterchainBlock.FileHash = bytes.Repeat([]byte{0x82}, 32)
	next.CurrentWindowStart = 2
	next.CurrentWindowObservedSlot = 2
	next.CurrentWindowStartAt = first.CurrentWindowStartAt.Add(time.Second)
	if err := fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("advance accepted pipeline view: %v", err)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)

	record, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("advanced effective update = %+v, want %+v", record.Update, next)
	}
	pipeline.mu.Lock()
	updates := pipeline.updated
	pipeline.mu.Unlock()
	if updates != 2 {
		t.Fatalf("pipeline update calls = %d, want 2", updates)
	}
}

func TestRuntimeAcceptedProgressKeepsCallerAndWALViewsMonotonic(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeLateSessionCommitStorage{
		runtimeMemoryStorage: baseStorage,
		stored:               make(chan struct{}),
		release:              make(chan struct{}),
		at:                   3,
	}
	fixture := newRuntimeFixture(t, 1, 1, nil, baseStorage, nil)
	fixture.service.opts.Storage = storage
	defer fixture.close(t)
	released := false
	defer func() {
		if !released {
			close(storage.release)
		}
	}()

	session, update := fixture.session(182, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	next := update
	next.CurrentWindowStart++
	next.CurrentWindowObservedSlot++
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(time.Second)

	progressCtx, cancelProgress := context.WithCancel(context.Background())
	progressResult := make(chan error, 1)
	go func() {
		progressResult <- fixture.service.ApplyConsensusProgress(
			progressCtx,
			runtimeConsensusProgress(session, next),
		)
	}()
	select {
	case <-storage.stored:
	case <-time.After(time.Second):
		t.Fatal("accepted progress did not reach session storage")
	}
	cancelProgress()
	if err := <-progressResult; err != nil {
		t.Fatalf("accepted progress returned its expired caller context: %v", err)
	}

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	pending := managed.sessionWritePending
	armed := managed.progressReady
	managed.mu.Unlock()
	if !pending || !armed {
		t.Fatalf("pending accepted progress state = pending %t, armed %t, want true, true", pending, armed)
	}

	newer := next
	newer.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, next.MasterchainBlock.SeqNo+1)
	if err = fixture.service.UpdateSession(context.Background(), newer); err != nil {
		t.Fatalf("newer MC update regressed accepted progress: %v", err)
	}

	close(storage.release)
	released = true
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	stored, err := baseStorage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Update.Equal(newer) {
		t.Fatalf("durable update = %+v, want newest accepted %+v", stored.Update, newer)
	}
}

func TestRuntimeFailedProgressCannotRearmOlderPendingRevision(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	advanceErr := errors.New("install selected consensus base")
	pipeline := &runtimeTestPipeline{}
	pipeline.advance = func(context.Context, ConsensusBaseUpdate) error {
		return advanceErr
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, baseStorage, nil)
	defer fixture.close(t)

	session, update := fixture.session(185, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 1),
	); err != nil {
		t.Fatal(err)
	}
	storage := &runtimeLateSessionCommitStorage{
		runtimeMemoryStorage: baseStorage,
		stored:               make(chan struct{}),
		release:              make(chan struct{}),
	}
	fixture.service.opts.Storage = storage

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(time.Second)
	if err := fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.stored:
	case <-time.After(time.Second):
		t.Fatal("accepted update did not reach storage")
	}

	candidateID := simplex.CandidateID{Slot: 1, Hash: [32]byte{0xa1}}
	progress := ConsensusProgress{
		SessionID: session.ID,
		Window: simplex.Window{
			Base:         simplex.Parent(candidateID),
			ObservedSlot: 2,
			StartSlot:    2,
			EndSlot:      3,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: next.CurrentWindowStartAt.Add(time.Second),
		Base:    runtimeSelectedBase(t, session, candidateID),
	}
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); !errors.Is(err, advanceErr) {
		t.Fatalf("failed progress error = %v, want %v", err, advanceErr)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	armedAfterWrite := managed.progressReadyAfterWrite
	managed.mu.Unlock()
	if armedAfterWrite {
		t.Fatal("failed progress retained the older pending revision's arm state")
	}
	close(storage.release)
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)

	managed.mu.Lock()
	armed := managed.progressReady
	managed.mu.Unlock()
	if armed {
		t.Fatal("older WAL callback re-armed production after newer progress failed")
	}
	if builds := pipeline.buildCount(); builds != 0 {
		t.Fatalf("builds after failed progress = %d, want 0", builds)
	}
}

func TestRuntimeUpdateAcceptsEquivalentWindowTime(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(20, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	equivalent := update
	equivalent.CurrentWindowStartAt = time.Unix(
		update.CurrentWindowStartAt.Unix(),
		int64(update.CurrentWindowStartAt.Nanosecond()),
	).In(time.FixedZone("recovered", 4*60*60))
	if update.CurrentWindowStartAt == equivalent.CurrentWindowStartAt ||
		!update.CurrentWindowStartAt.Equal(equivalent.CurrentWindowStartAt) {
		t.Fatal("test times do not differ structurally while representing the same instant")
	}
	if err := fixture.service.UpdateSession(context.Background(), equivalent); err != nil {
		t.Fatal(err)
	}
	pipeline.mu.Lock()
	updated := pipeline.updated
	pipeline.mu.Unlock()
	if updated != 0 {
		t.Fatalf("equivalent duplicate reached pipeline %d times", updated)
	}
}

func TestRuntimeOrdinaryUpdateCannotAdvanceSelectedConsensusBase(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(43, 1, 4, time.Now())
	update.CurrentBase = simplex.Parent(simplex.CandidateID{
		Slot: 3,
		Hash: sha256.Sum256([]byte("selected-base")),
	})
	fixture.prepare(t, session, update)

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = true
	managed.progressApplied = true
	managed.mu.Unlock()

	next := cloneSessionUpdate(update)
	next.CurrentWindowStart++
	next.CurrentWindowObservedSlot++
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(update.TargetRate)
	next.CurrentBase = simplex.Parent(simplex.CandidateID{
		Slot: 4,
		Hash: sha256.Sum256([]byte("unbound-base")),
	})
	if err = fixture.service.UpdateSession(context.Background(), next); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("ordinary selected-base advance error = %v, want ErrCandidateConflict", err)
	}

	pipeline.mu.Lock()
	updated := pipeline.updated
	pipeline.mu.Unlock()
	if updated != 0 {
		t.Fatalf("unbound selected base reached pipeline %d times", updated)
	}
	managed.mu.Lock()
	base := managed.record.Update.CurrentBase
	ready := managed.progressReady
	pending := managed.sessionWritePending
	managed.mu.Unlock()
	if base != update.CurrentBase || !ready || pending {
		t.Fatalf("rejected base mutated runtime base/ready/pending = %v/%v/%v", base, ready, pending)
	}
}

func TestRuntimeRejectsNonMonotonicUpdateBeforePipeline(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(44, 1, 4, time.Now())
	update.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 10)
	update.HasFinalizedBlock = true
	update.FinalizedBlock = runtimeTestBlockID(session.Shard.Workchain, session.Shard.Shard, 7)
	update.CurrentBase = simplex.Parent(simplex.CandidateID{
		Slot: 3,
		Hash: sha256.Sum256([]byte("current-base")),
	})
	fixture.prepare(t, session, update)

	tests := []struct {
		name   string
		mutate func(*SessionUpdate)
	}{
		{name: "masterchain regression", mutate: func(next *SessionUpdate) {
			next.MasterchainBlock.SeqNo--
		}},
		{name: "masterchain fork", mutate: func(next *SessionUpdate) {
			next.MasterchainBlock.RootHash[0] ^= 1
		}},
		{name: "finalized removal", mutate: func(next *SessionUpdate) {
			next.HasFinalizedBlock = false
			next.FinalizedBlock = ton.BlockIDExt{}
		}},
		{name: "finalized regression", mutate: func(next *SessionUpdate) {
			next.FinalizedBlock.SeqNo--
		}},
		{name: "current window removal", mutate: func(next *SessionUpdate) {
			next.HasCurrentWindow = false
			next.CurrentWindowStart = 0
			next.CurrentWindowObservedSlot = 0
			next.CurrentWindowStartAt = time.Time{}
			next.CurrentBase = simplex.Genesis()
		}},
		{name: "current base regression", mutate: func(next *SessionUpdate) {
			next.CurrentBase = simplex.Parent(simplex.CandidateID{
				Slot: 2,
				Hash: sha256.Sum256([]byte("older-base")),
			})
		}},
		{name: "current base fork", mutate: func(next *SessionUpdate) {
			next.CurrentBase.ID.Hash[0] ^= 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := cloneSessionUpdate(update)
			test.mutate(&next)
			if err := fixture.service.UpdateSession(context.Background(), next); !errors.Is(err, ErrSessionConflict) {
				t.Fatalf("update error = %v, want ErrSessionConflict", err)
			}
		})
	}

	pipeline.mu.Lock()
	updated := pipeline.updated
	pipeline.mu.Unlock()
	if updated != 0 {
		t.Fatalf("rejected updates reached pipeline %d times", updated)
	}
}

func TestRuntimeUpdateAcceptsMutableTargetRate(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(24, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	changed := update
	changed.TargetRate = 250 * time.Millisecond
	if err := fixture.service.UpdateSession(context.Background(), changed); err != nil {
		t.Fatal(err)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := fixture.storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Update.TargetRate != changed.TargetRate {
		t.Fatalf("stored target rate = %s, want %s", record.Update.TargetRate, changed.TargetRate)
	}

	invalid := changed
	invalid.TargetRate = 0
	if err = fixture.service.UpdateSession(context.Background(), invalid); err == nil {
		t.Fatal("zero target rate update was accepted")
	}
	record, err = fixture.storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Update.TargetRate != changed.TargetRate {
		t.Fatalf("target rate after rejected update = %s, want %s", record.Update.TargetRate, changed.TargetRate)
	}
}

func TestRuntimeCommittedUpdateSchedulesWithServiceContext(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	var cancelUpdate context.CancelFunc
	// The caller context expires once the update is durably committed. Cancelling
	// before the write completes is a different case: the outcome is then unknown
	// and the update is meant to abandon it rather than schedule on top of it.
	storage := &runtimeCancelAfterSaveStorage{
		runtimeMemoryStorage: baseStorage,
		at:                   3,
		cancel:               func() { cancelUpdate() },
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeFixture(t, 1, 1, nil, baseStorage, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact
		return nil
	})
	fixture.service.opts.Storage = storage
	defer fixture.close(t)
	session, update := fixture.session(48, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 1),
	); err != nil {
		t.Fatal(err)
	}

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	updateCtx, cancel := context.WithCancel(context.Background())
	cancelUpdate = cancel
	defer cancel()
	if err := fixture.service.UpdateSession(updateCtx, next); err != nil {
		t.Fatalf("committed update returned error: %v", err)
	}
	artifact := runtimeAwaitArtifact(t, emitted)
	if artifact.Candidate.ID.Slot != 1 {
		t.Fatalf("scheduled slot = %d, want 1", artifact.Candidate.ID.Slot)
	}
}

func TestRuntimeCommittedUpdateForgetsStaleAuthorizationWithServiceContext(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	var cancelUpdate context.CancelFunc
	storage := &runtimeCancelAfterSaveStorage{
		runtimeMemoryStorage: baseStorage,
		at:                   3,
		cancel:               func() { cancelUpdate() },
	}
	fixture := newRuntimeFixture(t, 1, 1, nil, baseStorage, nil)
	fixture.service.opts.Storage = storage
	defer fixture.close(t)

	session, update := fixture.session(55, 1, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)
	fixture.forceProgressReady(t, session.ID)
	windowID := WindowID{SessionID: session.ID}
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}

	next := update
	next.HasCurrentWindow = true
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	updateCtx, cancel := context.WithCancel(context.Background())
	cancelUpdate = cancel
	defer cancel()
	if err := fixture.service.UpdateSession(updateCtx, next); err != nil {
		t.Fatalf("committed update returned caller cancellation: %v", err)
	}
	runtimeAwait(t, func() bool { return updateCtx.Err() != nil })
	if updateCtx.Err() == nil {
		t.Fatal("test did not expire the caller after the durable commit")
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	_, retained := managed.authorizations[windowID]
	managed.mu.Unlock()
	if retained {
		t.Fatal("stale in-memory authorization survived the committed update")
	}

	newer := next
	newer.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, next.MasterchainBlock.SeqNo+1)
	if err := fixture.service.UpdateSession(context.Background(), newer); err != nil {
		t.Fatalf("following MC update regressed committed window: %v", err)
	}
}

func TestRuntimeStaleDelegationCleanupForgetsInMemoryAuthorization(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(53, 1, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)
	fixture.forceProgressReady(t, session.ID)
	windowID := WindowID{SessionID: session.ID}
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}

	next := update
	next.HasCurrentWindow = true
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	if err := fixture.service.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("advance past in-memory delegation: %v", err)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("service retained pre-commit view after cleanup: %+v", record.Update)
	}

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	_, retained := managed.authorizations[windowID]
	managed.mu.Unlock()
	if retained {
		t.Fatal("stale receiver authorization was retained")
	}
	pipeline.mu.Lock()
	updates := pipeline.updated
	pipeline.mu.Unlock()
	if updates != 1 {
		t.Fatalf("pipeline update calls = %d, want 1", updates)
	}
}

func TestRuntimeQueuedControlCallHonorsContext(t *testing.T) {
	updateEntered := make(chan struct{}, 1)
	releaseUpdate := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(context.Context, Session, SessionUpdate) error {
		updateEntered <- struct{}{}
		<-releaseUpdate
		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(49, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	next := update
	next.TargetRate += time.Millisecond
	firstResult := make(chan error, 1)
	go func() { firstResult <- fixture.service.UpdateSession(context.Background(), next) }()
	select {
	case <-updateEntered:
	case <-time.After(time.Second):
		t.Fatal("first update did not enter pipeline")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := fixture.service.Probe(ctx, WindowPreparation{
		SessionID:  session.ID,
		SourceADNL: fixture.sourceADNL,
		StartSlot:  0,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued probe error = %v, want deadline exceeded", err)
	}
	close(releaseUpdate)
	if err = <-firstResult; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeProductionBarrierStagesAndCoalescesRoutineUpdate(t *testing.T) {
	buildEntered := make(chan struct{}, 1)
	releaseBuild := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		buildEntered <- struct{}{}
		<-releaseBuild
		return runtimeBuiltCandidate(request), nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(50, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-buildEntered:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}

	first := update
	first.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, update.MasterchainBlock.SeqNo+1)
	firstResult := make(chan error, 1)
	go func() { firstResult <- fixture.service.UpdateSession(context.Background(), first) }()
	select {
	case err := <-firstResult:
		if !errors.Is(err, ErrSessionUpdateDeferred) {
			t.Fatalf("first update error = %v, want session update deferred", err)
		}
	case <-time.After(200 * time.Millisecond):
		close(releaseBuild)
		<-firstResult
		t.Fatal("first update blocked at the production barrier")
	}

	second := first
	second.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, first.MasterchainBlock.SeqNo+1)
	if err := fixture.service.UpdateSession(context.Background(), second); !errors.Is(err, ErrSessionUpdateDeferred) {
		close(releaseBuild)
		t.Fatalf("coalesced update error = %v, want session update deferred", err)
	}
	record, err := fixture.service.Session(context.Background(), session.ID)
	if err != nil {
		close(releaseBuild)
		t.Fatal(err)
	}
	if !record.Update.Equal(update) {
		close(releaseBuild)
		t.Fatalf("staged update leaked before pipeline acceptance: %+v", record.Update)
	}
	close(releaseBuild)
	runtimeAwait(t, func() bool {
		record, readErr := fixture.service.Session(context.Background(), session.ID)

		return readErr == nil && record.Update.Equal(second)
	})
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	pipeline.mu.Lock()
	updates := pipeline.updated
	pipeline.mu.Unlock()
	if updates != 1 {
		t.Fatalf("coalesced pipeline updates = %d, want 1", updates)
	}
}

func TestRuntimeConsensusProgressFoldsDeferredRefresh(t *testing.T) {
	advanceErr := errors.New("install selected consensus progress")
	for _, test := range []struct {
		name       string
		seed       byte
		advanceErr error
	}{
		{name: "accepted", seed: 52},
		{name: "failed keeps pending", seed: 54, advanceErr: advanceErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			buildEntered := make(chan struct{}, 1)
			buildCancelled := make(chan struct{}, 1)
			releaseBuild := make(chan struct{})
			advanced := make(chan ConsensusBaseUpdate, 1)
			pipeline := &runtimeTestPipeline{}
			pipeline.build = func(ctx context.Context, _ BuildRequest) (*Candidate, error) {
				buildEntered <- struct{}{}
				<-ctx.Done()
				buildCancelled <- struct{}{}
				<-releaseBuild

				return nil, ctx.Err()
			}
			pipeline.advance = func(_ context.Context, request ConsensusBaseUpdate) error {
				advanced <- request

				return test.advanceErr
			}
			if test.advanceErr != nil {
				// Keep the deferred worker from independently accepting the refresh
				// after the failed combined attempt, so the failure invariant remains
				// observable without stopping the worker by test-only hooks.
				pipeline.update = func(context.Context, Session, SessionUpdate) error {
					return ErrAcquisitionNotReady
				}
			}

			fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
			defer fixture.close(t)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseBuild) }) }
			defer release()

			session, update := fixture.session(test.seed, 1, 0, time.Now())
			update.TargetRate = 2 * time.Second
			fixture.prepare(t, session, update)
			if err := fixture.service.CommitDelegation(
				context.Background(),
				fixture.request(t, session, 0),
			); err != nil {
				t.Fatal(err)
			}
			select {
			case <-buildEntered:
			case <-time.After(time.Second):
				t.Fatal("build did not start")
			}

			managed, err := fixture.service.runningSession(session.ID)
			if err != nil {
				t.Fatal(err)
			}
			managed.policyMu.Lock()
			baselineFinalized := managed.emptyPolicy.LastMCFinalizedSeqno
			managed.policyMu.Unlock()

			refresh := update
			refresh.MasterchainBlock = runtimeTestBlockID(
				update.MasterchainBlock.Workchain,
				update.MasterchainBlock.Shard,
				update.MasterchainBlock.SeqNo+1,
			)
			refresh.HasFinalizedBlock = true
			refresh.FinalizedBlock = runtimeTestBlockID(
				session.Shard.Workchain,
				session.Shard.Shard,
				session.CatchainSeqno+20,
			)
			if err = fixture.service.UpdateSession(context.Background(), refresh); !errors.Is(
				err,
				ErrSessionUpdateDeferred,
			) {
				t.Fatalf("routine refresh error = %v, want session update deferred", err)
			}

			progressUpdate := update
			progressUpdate.CurrentWindowStart = 1
			progressUpdate.CurrentWindowObservedSlot = 1
			progressUpdate.CurrentWindowStartAt = time.Now()
			progressResult := make(chan error, 1)
			go func() {
				progressResult <- fixture.service.ApplyConsensusProgress(
					context.Background(),
					runtimeConsensusProgress(session, progressUpdate),
				)
			}()
			// The advancing progress owns controlMu and has cancelled the old
			// producer. Releasing the build now makes it deterministically win the
			// production barrier ahead of the deferred worker.
			select {
			case <-buildCancelled:
			case <-time.After(time.Second):
				t.Fatal("advancing progress did not cancel production")
			}
			release()
			err = <-progressResult
			if test.advanceErr == nil && err != nil {
				t.Fatalf("combined progress: %v", err)
			}
			if test.advanceErr != nil && !errors.Is(err, test.advanceErr) {
				t.Fatalf("combined progress error = %v, want %v", err, test.advanceErr)
			}

			combined := refresh
			combined.CurrentWindowStart = progressUpdate.CurrentWindowStart
			combined.CurrentWindowObservedSlot = progressUpdate.CurrentWindowObservedSlot
			combined.CurrentWindowStartAt = progressUpdate.CurrentWindowStartAt
			combined.CurrentBase = progressUpdate.CurrentBase
			request := <-advanced
			if !request.Update.Equal(combined) {
				t.Fatalf("combined pipeline update = %+v, want %+v", request.Update, combined)
			}

			if test.advanceErr != nil {
				record, readErr := fixture.service.Session(context.Background(), session.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				managed.mu.Lock()
				pending := managed.deferredUpdate
				unavailable := managed.unavailable
				managed.mu.Unlock()
				if !record.Update.Equal(update) {
					t.Fatalf("failed progress published update = %+v, want %+v", record.Update, update)
				}
				if pending == nil || !pending.Equal(refresh) {
					t.Fatalf("failed progress pending refresh = %+v, want %+v", pending, refresh)
				}
				if unavailable {
					t.Fatal("failed combined progress quarantined the session")
				}
				// The watermark is a masterchain observation, not a
				// consequence of pipeline acceptance: it advances when the
				// refresh is validated and staged, and a later progress
				// failure does not retract it. That decoupling is the point —
				// a refresh that only lands after the barrier is released
				// arrives too late for the window it had to unblock. It cannot
				// produce an invalid block either way, because the build
				// re-checks the same registry rule against the masterchain
				// view it actually stamps and degrades the slot when no view
				// admits the chain.
				managed.policyMu.Lock()
				finalized := managed.emptyPolicy.LastMCFinalizedSeqno
				managed.policyMu.Unlock()
				if finalized != refresh.FinalizedBlock.SeqNo {
					t.Fatalf(
						"failed progress finalized watermark = %d, want the staged observation %d (baseline %d)",
						finalized,
						refresh.FinalizedBlock.SeqNo,
						baselineFinalized,
					)
				}

				return
			}

			runtimeAwait(t, func() bool {
				managed.mu.Lock()
				defer managed.mu.Unlock()

				return managed.deferredUpdate == nil && !managed.deferredUpdateRunning
			})
			runtimeAwaitSessionWrite(t, fixture.service, session.ID)
			record, readErr := fixture.storage.Session(context.Background(), session.ID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !record.Update.Equal(combined) {
				t.Fatalf("stored combined update = %+v, want %+v", record.Update, combined)
			}
			managed.mu.Lock()
			unavailable := managed.unavailable
			managed.mu.Unlock()
			if unavailable {
				t.Fatal("accepted combined progress quarantined the session")
			}
			managed.policyMu.Lock()
			finalized := managed.emptyPolicy.LastMCFinalizedSeqno
			managed.policyMu.Unlock()
			if finalized != refresh.FinalizedBlock.SeqNo {
				t.Fatalf(
					"absorbed refresh finalized watermark = %d, want %d",
					finalized,
					refresh.FinalizedBlock.SeqNo,
				)
			}
		})
	}
}

func TestDeferredUpdateWorkerExitHandshakeKeepsConcurrentStage(t *testing.T) {
	managed := newManagedCollatorSession(SessionRecord{}, true)
	managed.deferredUpdateRunning = true
	pending := SessionUpdate{SessionID: [32]byte{0x51}}
	managed.deferredUpdate = sessionUpdatePointer(pending)

	if managed.stopDeferredUpdateWorkerIfIdle() {
		t.Fatal("worker exited while a staged update was present")
	}
	managed.mu.Lock()
	running := managed.deferredUpdateRunning
	managed.deferredUpdate = nil
	managed.mu.Unlock()
	if !running {
		t.Fatal("worker cleared running while retaining staged work")
	}
	if !managed.stopDeferredUpdateWorkerIfIdle() {
		t.Fatal("idle worker did not complete its exit handshake")
	}
	managed.mu.Lock()
	running = managed.deferredUpdateRunning
	managed.mu.Unlock()
	if running {
		t.Fatal("idle worker remained marked running")
	}
}

func TestRuntimeProductionBarrierDefersConsensusProgressWithoutBlocking(t *testing.T) {
	buildEntered := make(chan struct{}, 1)
	releaseBuild := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		buildEntered <- struct{}{}
		<-releaseBuild

		return runtimeBuiltCandidate(request), nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(51, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-buildEntered:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}

	result := make(chan error, 1)
	go func() {
		result <- fixture.service.ApplyConsensusProgress(
			context.Background(),
			runtimeConsensusProgress(session, update),
		)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrAcquisitionNotReady) {
			t.Fatalf("progress error = %v, want acquisition not ready", err)
		}
	case <-time.After(200 * time.Millisecond):
		close(releaseBuild)
		<-result
		t.Fatal("consensus progress blocked at the production barrier")
	}
	close(releaseBuild)
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())
		return status.ActiveWindows == 0
	})
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, update),
	); err != nil {
		t.Fatalf("retry consensus progress: %v", err)
	}
}

// A superseding progress cancels the production it replaces, but cancellation
// is asynchronous: the producer holds the barrier until its in-flight build
// returns. Claiming the barrier with an immediate TryLock loses that race by
// construction and costs the whole leader window, because a rejected progress
// leaves production disarmed and the runtime never opens the window.
func TestRuntimeConsensusProgressWaitsOutCancelledProduction(t *testing.T) {
	buildEntered := make(chan struct{}, 1)
	buildCancelled := make(chan struct{}, 1)
	releaseBuild := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, _ BuildRequest) (*Candidate, error) {
		buildEntered <- struct{}{}
		<-ctx.Done()
		buildCancelled <- struct{}{}
		<-releaseBuild

		return nil, ctx.Err()
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBuild) }) }
	defer release()

	session, update := fixture.session(53, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-buildEntered:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	// Hold the barrier past the cancel, well inside the bounded wait.
	go func() {
		<-buildCancelled
		time.Sleep(10 * time.Millisecond)
		release()
	}()

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, next),
	); err != nil {
		t.Fatalf("consensus progress: %v", err)
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	armed := managed.progressReady
	managed.mu.Unlock()
	if !armed {
		t.Fatal("consensus progress did not arm production for the published window")
	}
}

// The producer must not stay wedged in a durable write that blocks before its
// callback exists. It holds the production barrier for the whole wait, so an
// unabandonable submission turns a stalled writer into a skipped window.
func TestRuntimeCancelledProductionAbandonsStuckStorageWrite(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeStuckWriterStorage{
		runtimeMemoryStorage: baseStorage,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	fixture := newRuntimeFixture(t, 1, 1, nil, baseStorage, nil)
	fixture.service.opts.Storage = storage
	defer fixture.close(t)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(storage.release) })

	session, update := fixture.session(54, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("candidate write did not start")
	}

	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = time.Now()
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, next),
	); err != nil {
		t.Fatalf("consensus progress: %v", err)
	}
}

// Once the pipeline has committed a candidate and its anti-equivocation marker
// has landed, superseding the window may stop every later build but must not
// suppress delivery of that already signed candidate. In particular, the
// progress that opens the next window commonly arrives at the same instant the
// previous window's last candidate is waiting for its broadcast slot.
func TestRuntimeCommittedCandidateEmitsAcrossWindowCancellation(t *testing.T) {
	const firstBlockBudget = 700 * time.Millisecond

	committed := make(chan struct{})
	var committedOnce sync.Once
	pipeline := &runtimeTestPipeline{}
	pipeline.commit = func(context.Context, CandidateCommit) error {
		committedOnce.Do(func() { close(committed) })

		return nil
	}
	emitted := make(chan CandidateArtifact, 1)
	memory := newRuntimeMemoryStorage()
	storage := &runtimeObservedCandidateSaveStorage{
		runtimeMemoryStorage: memory,
		accepted:             make(chan struct{}),
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, memory, func(ctx context.Context, artifact CandidateArtifact) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		emitted <- artifact

		return nil
	})
	fixture.service.opts.Storage = storage
	defer fixture.close(t)

	// The old schedule is intentionally five seconds ahead. A superseding
	// progress must accelerate the already committed candidate rather than hold
	// the progress barrier until that stale broadcast instant.
	session, update := fixture.session(0x56, 1, 0, time.Now().Add(5*time.Second))
	update.TargetRate = 10 * time.Second
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("candidate was not committed")
	}
	select {
	case <-storage.accepted:
	case <-time.After(time.Second):
		t.Fatal("candidate marker was not accepted")
	}

	openedAt := time.Now()
	next := update
	next.CurrentWindowStart = 1
	next.CurrentWindowObservedSlot = 1
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(update.TargetRate)
	if err := fixture.service.ApplyConsensusProgress(
		context.Background(),
		runtimeConsensusProgress(session, next),
	); err != nil {
		t.Fatalf("consensus progress: %v", err)
	}
	if elapsed := time.Since(openedAt); elapsed > firstBlockBudget {
		t.Fatalf("superseding progress held the production barrier for %s, budget %s",
			elapsed, firstBlockBudget)
	}

	timer := time.NewTimer(time.Until(openedAt.Add(firstBlockBudget)))
	defer timer.Stop()
	select {
	case artifact := <-emitted:
		if artifact.Candidate.ID.Slot != 0 {
			t.Fatalf("emitted slot = %d, want committed slot 0", artifact.Candidate.ID.Slot)
		}
	case <-timer.C:
		t.Fatalf("committed candidate missed the %s delivery budget", firstBlockBudget)
	}
}

// The other half of the marker contract: an in-process retry re-emits the slot
// it already signed from memory. The payload never reaches disk, so this path
// is the only thing keeping a retryable mid-window failure from ending the
// window at its first already signed slot.
func TestRuntimeRetriedWindowReemitsSignedSlotFromMemory(t *testing.T) {
	var failures atomic.Int32
	pipeline := &runtimeTestPipeline{}
	// The failure is injected into the build rather than into the state
	// resolution, because a pipelined slot never resolves: its predecessor hands
	// it the verdict along with the block. What this test is about — a window
	// that fails after it has already signed a slot, and re-emits that slot from
	// memory when it retries — is the same either way.
	//
	// Slot 1 fails its first two builds. The first is the pipelined successor
	// slot 0 handed over; depending on goroutine scheduling, its result is either
	// declined as already failed or adopted just before it reports the failure.
	// The two legal orderings cause one or two retries, and both must replay the
	// signed slot from memory rather than build it again.
	var buildsMu sync.Mutex
	buildsPerSlot := map[uint32]int{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		buildsMu.Lock()
		buildsPerSlot[request.Slot]++
		buildsMu.Unlock()
		if request.Slot == 1 && failures.Add(1) <= 2 {
			return nil, errors.New("candidate build unavailable")
		}

		return runtimeBuiltCandidate(request), nil
	}
	pipeline.state = func(_ context.Context, request BuildRequest) (CandidateState, error) {
		block := cloneBlockID(request.Previous.Candidate.Block)

		return CandidateState{Block: block, NextSeqno: block.SeqNo + 1}, nil
	}
	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(55, 2, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}

	signed := runtimeAwaitArtifact(t, emitted)
	if signed.Candidate.ID.Slot != 0 {
		t.Fatalf("first emitted slot = %d, want 0", signed.Candidate.ID.Slot)
	}
	var (
		resumed []CandidateArtifact
		next    CandidateArtifact
	)

collectReplays:
	for attempts := 0; attempts < 3; attempts++ {
		artifact := runtimeAwaitArtifact(t, emitted)
		switch artifact.Candidate.ID.Slot {
		case 0:
			resumed = append(resumed, artifact)
		case 1:
			next = artifact
			break collectReplays
		default:
			t.Fatalf("emitted slot = %d, want replayed 0 or next slot 1", artifact.Candidate.ID.Slot)
		}
	}
	if len(resumed) == 0 || next.Candidate.ID.Slot != 1 {
		t.Fatalf("replays before slot 1 = %d, want at least one followed by slot 1", len(resumed))
	}
	for _, replay := range resumed {
		if replay.Candidate.ID != signed.Candidate.ID {
			t.Fatal("retry re-emitted a different candidate for a slot already signed")
		}
		if !slices.Equal(replay.BlockBOC, signed.BlockBOC) ||
			!slices.Equal(replay.CollatedData, signed.CollatedData) {
			t.Fatal("retry re-emitted different bytes for a slot already signed")
		}
	}
	// Per slot, not in total: the retried slot is legitimately built twice — the
	// attempt that failed and the one that succeeded — and what this asserts is
	// that the slot which was already signed is not among them.
	buildsMu.Lock()
	defer buildsMu.Unlock()
	if buildsPerSlot[0] != 1 {
		t.Fatalf("slot 0 was built %d times; the signed slot was rebuilt instead of re-emitted "+
			"from memory", buildsPerSlot[0])
	}

	stored, err := fixture.storage.Candidate(context.Background(), signed.WindowID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ID != signed.Candidate.ID {
		t.Fatalf("stored marker = %v, want the signed candidate %v", stored.ID, signed.Candidate.ID)
	}
}

func TestAwaitStorageWriteAbandonsBlockedSubmission(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- awaitStorageWrite(ctx, func(func(error)) {
			close(entered)
			<-release
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("submission did not start")
	}

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not abandon a submission blocked before its callback")
	}
}

func TestProductionBarrierWaitClampsToSlotRate(t *testing.T) {
	cases := []struct {
		name string
		rate time.Duration
		want time.Duration
	}{
		{name: "unset rate", rate: 0, want: productionBarrierMinWait},
		{name: "quarter slot", rate: 2400 * time.Millisecond, want: 600 * time.Millisecond},
		{name: "capped", rate: time.Minute, want: productionBarrierMaxWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := productionBarrierWait(SessionUpdate{TargetRate: tc.rate}); got != tc.want {
				t.Fatalf("wait = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCollationScheduleMatchesReferenceByChain(t *testing.T) {
	windowStart := time.Unix(1_900_000_000, 0)
	update := SessionUpdate{
		TargetRate:                2500 * time.Millisecond,
		CurrentWindowStart:        20,
		CurrentWindowStartAt:      windowStart,
		HasCurrentWindow:          true,
		CurrentWindowObservedSlot: 20,
	}
	for _, test := range []struct {
		name       string
		shard      groups.ShardID
		startLead  time.Duration
		processFor time.Duration
		wait       bool
	}{
		{
			name:      "shardchain",
			shard:     groups.ShardID{Workchain: 0, Shard: -1 << 63},
			startLead: update.TargetRate,
			wait:      true,
		},
		{
			name:       "masterchain",
			shard:      groups.ShardID{Workchain: -1, Shard: -1 << 63},
			processFor: update.TargetRate * 3 / 4,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := SessionRecord{Session: Session{Shard: test.shard}, Update: update}
			slot := uint32(23)
			slotStart := windowStart.Add(3 * update.TargetRate)
			if got := broadcastTime(record, slot); !got.Equal(slotStart) {
				t.Fatalf("broadcast time = %s, want %s", got, slotStart)
			}
			if got := buildStartTime(record, slot); !got.Equal(slotStart.Add(-test.startLead)) {
				t.Fatalf("build start = %s, want %s", got, slotStart.Add(-test.startLead))
			}
			waitUntil := externalWaitUntil(record, slot)
			if test.wait && !waitUntil.Equal(slotStart) {
				t.Fatalf("external wait boundary = %s, want %s", waitUntil, slotStart)
			}
			if !test.wait && !waitUntil.IsZero() {
				t.Fatalf("masterchain external wait boundary = %s, want zero", waitUntil)
			}
			if got := externalProcessUntil(record, slot); !got.Equal(slotStart.Add(test.processFor)) {
				t.Fatalf("external process deadline = %s, want %s", got, slotStart.Add(test.processFor))
			}
		})
	}
}

func TestRuntimeReconcileForgetsStaleAuthorization(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(57, 1, 1, time.Now())
	fixture.prepare(t, session, update)
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}

	stale := delegatedAuthorization{
		ID:            WindowID{SessionID: session.ID, StartSlot: 0},
		CollatorKeyID: fixture.keys.id,
		State:         delegatedAuthorizationPending,
	}
	managed.mu.Lock()
	managed.authorizations[stale.ID] = stale
	managed.mu.Unlock()

	if err = fixture.service.reconcileWindows(context.Background(), managed); err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	_, retained := managed.authorizations[stale.ID]
	managed.mu.Unlock()
	if retained {
		t.Fatal("stale authorization survived reconciliation")
	}
}

func TestRuntimeDelegatedCompletionStaysMemoryOnlyAndIdempotent(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(52, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	request := fixture.request(t, session, 0)
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool {
		managed.mu.Lock()
		window := managed.authorizations[request.ID()]
		managed.mu.Unlock()

		return window.State == delegatedAuthorizationCompleted
	})

	managed.mu.Lock()
	window := managed.authorizations[request.ID()]
	managed.mu.Unlock()
	if window.State != delegatedAuthorizationCompleted {
		t.Fatalf("completed window state = %d, want %d", window.State, delegatedAuthorizationCompleted)
	}
	if err = fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("exact completed duplicate: %v", err)
	}
	if builds := pipeline.buildCount(); builds != 1 {
		t.Fatalf("builds after exact completed duplicate = %d, want 1", builds)
	}
}

func TestRuntimeRetireStorageFailureKeepsSessionClosed(t *testing.T) {
	storage := newRuntimeMemoryStorage()
	var deletes atomic.Int32
	storage.deleteError = func([32]byte) error {
		if deletes.Add(1) == 1 {
			return errors.New("session delete WAL failure")
		}

		return nil
	}

	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, storage, nil)
	defer fixture.close(t)
	session, update := fixture.session(19, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	if err := fixture.service.RetireSession(context.Background(), session.ID); err == nil {
		t.Fatal("retirement unexpectedly ignored storage failure")
	}
	if err := fixture.service.Probe(context.Background(), WindowPreparation{
		SessionID:  session.ID,
		SourceADNL: fixture.sourceADNL,
		StartSlot:  0,
	}); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("probe error = %v, want ErrSessionRetired", err)
	}
	if err := fixture.service.UpdateSession(context.Background(), update); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("update error = %v, want ErrSessionRetired", err)
	}
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("delegation error = %v, want ErrSessionRetired", err)
	}

	if err := fixture.service.RetireSession(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	pipeline.mu.Lock()
	retirements := pipeline.retired
	pipeline.mu.Unlock()
	if retirements != 2 {
		t.Fatalf("pipeline retirements = %d, want 2", retirements)
	}
	if _, err := storage.Session(context.Background(), session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stored session after retry = %v, want ErrNotFound", err)
	}
}

func TestRuntimeSerializesConcurrentSessionUpdates(t *testing.T) {
	firstEntered := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(ctx context.Context, _ Session, _ SessionUpdate) error {
		if calls.Add(1) == 1 {
			firstEntered <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseFirst:
			}
		}
		return nil
	}
	fixture := newRuntimeFixture(t, 1, 2, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(13, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	first := update
	first.CurrentWindowStart = 1
	first.CurrentWindowObservedSlot = 1
	first.CurrentWindowStartAt = time.Now().Add(time.Second)
	second := update
	second.CurrentWindowStart = 2
	second.CurrentWindowObservedSlot = 2
	second.CurrentWindowStartAt = time.Now().Add(2 * time.Second)
	results := make(chan error, 2)
	go func() { results <- fixture.service.UpdateSession(context.Background(), first) }()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first update did not enter pipeline")
	}
	go func() { results <- fixture.service.UpdateSession(context.Background(), second) }()
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("concurrent pipeline updates = %d, want 1", calls.Load())
	}
	close(releaseFirst)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	runtimeAwaitSessionWrite(t, fixture.service, session.ID)
	record, err := fixture.storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Update.CurrentWindowStart != 2 {
		t.Fatalf("stored current window = %d, want 2", record.Update.CurrentWindowStart)
	}
}

func TestRuntimeRetireJoinsCancelledBuild(t *testing.T) {
	started := make(chan struct{}, 1)
	exited := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, _ BuildRequest) (*Candidate, error) {
		started <- struct{}{}
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}
	pipeline.retire = func(context.Context, [32]byte) error {
		select {
		case <-exited:
			return nil
		default:
			return errors.New("pipeline retired before build exited")
		}
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	session, update := fixture.session(14, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("build did not start")
	}
	if err := fixture.service.RetireSession(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Probe(context.Background(), WindowPreparation{SessionID: session.ID}); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("post-retire probe error = %v", err)
	}
	fixture.close(t)
}

func TestRuntimeQueuedCallsStopAtClosingBoundary(t *testing.T) {
	updateEntered := make(chan struct{}, 1)
	releaseUpdate := make(chan struct{})
	var updateCalls atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(ctx context.Context, _ Session, _ SessionUpdate) error {
		if updateCalls.Add(1) == 1 {
			updateEntered <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseUpdate:
			}
		}

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	session, update := fixture.session(24, 1, 0, time.Now().Add(time.Second))
	fixture.prepare(t, session, update)

	holderUpdate := update
	holderUpdate.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 2401)
	holderResult := make(chan error, 1)
	go func() { holderResult <- fixture.service.UpdateSession(context.Background(), holderUpdate) }()
	select {
	case <-updateEntered:
	case <-time.After(time.Second):
		t.Fatal("control-holder update did not enter pipeline")
	}

	queuedUpdate := holderUpdate
	queuedUpdate.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 2402)
	request := fixture.request(t, session, 0)
	results := make(map[string]chan error)
	started := make(chan struct{}, 5)
	queue := func(name string, call func() error) {
		result := make(chan error, 1)
		results[name] = result
		go func() {
			started <- struct{}{}
			result <- call()
		}()
	}
	queue("prepare", func() error {
		return fixture.service.PrepareSession(context.Background(), session, update)
	})
	queue("update", func() error {
		return fixture.service.UpdateSession(context.Background(), queuedUpdate)
	})
	queue("retire", func() error {
		return fixture.service.RetireSession(context.Background(), session.ID)
	})
	queue("probe", func() error {
		return fixture.service.Probe(context.Background(), WindowPreparation{
			SessionID:  session.ID,
			SourceADNL: fixture.sourceADNL,
			StartSlot:  0,
		})
	})
	queue("commit", func() error {
		return fixture.service.CommitDelegation(context.Background(), request)
	})
	for range 5 {
		<-started
	}
	// Let every call pass initial admission and block on the held controlMu.
	time.Sleep(20 * time.Millisecond)

	closeResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closeResult <- fixture.service.Close(ctx)
	}()
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())
		return err == nil && status.Closing
	})
	close(releaseUpdate)
	if err := <-holderResult; err != nil {
		t.Fatalf("already admitted update failed: %v", err)
	}
	for _, name := range []string{"prepare", "update", "retire", "probe", "commit"} {
		if err := <-results[name]; !errors.Is(err, ErrClosed) {
			t.Fatalf("queued %s error = %v, want ErrClosed", name, err)
		}
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}

	pipeline.mu.Lock()
	prepared := pipeline.prepared
	updated := pipeline.updated
	retired := pipeline.retired
	pipeline.mu.Unlock()
	if prepared != 1 || updated != 1 || retired != 1 {
		t.Fatalf("pipeline calls after close boundary: prepare=%d update=%d retire=%d", prepared, updated, retired)
	}
}

func TestRuntimeQueuedPrepareDoesNotReinsertRetiredSession(t *testing.T) {
	retireEntered := make(chan struct{}, 1)
	releaseRetire := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.retire = func(ctx context.Context, _ [32]byte) error {
		retireEntered <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releaseRetire:
			return nil
		}
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(25, 1, 0, time.Now())
	fixture.prepare(t, session, update)

	retireResult := make(chan error, 1)
	go func() { retireResult <- fixture.service.RetireSession(context.Background(), session.ID) }()
	select {
	case <-retireEntered:
	case <-time.After(time.Second):
		t.Fatal("retirement did not enter pipeline")
	}
	prepareStarted := make(chan struct{})
	prepareResult := make(chan error, 1)
	go func() {
		close(prepareStarted)
		prepareResult <- fixture.service.PrepareSession(context.Background(), session, update)
	}()
	<-prepareStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseRetire)
	if err := <-retireResult; err != nil {
		t.Fatal(err)
	}
	if err := <-prepareResult; !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("queued prepare error = %v, want ErrSessionRetired", err)
	}

	fixture.service.mu.Lock()
	managed := fixture.service.sessions[session.ID]
	_, retired := fixture.service.retired[session.ID]
	fixture.service.mu.Unlock()
	if managed != nil || !retired {
		t.Fatalf("retired session was reinserted: managed=%p tombstone=%v", managed, retired)
	}
}

func TestRuntimeRetirementFenceIsBounded(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	service := fixture.service
	service.mu.Lock()
	defer service.mu.Unlock()
	var oldest [32]byte
	for i := range retiredTombstoneLimit + 16 {
		var id [32]byte
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		id[2] = 0xff
		if i == 0 {
			oldest = id
		}
		service.fenceRetiredLocked(id)
		service.fenceRetiredLocked(id)
	}
	if len(service.retired) != retiredTombstoneLimit || len(service.retiredOrder) != retiredTombstoneLimit {
		t.Fatalf("fence size map=%d order=%d, want %d",
			len(service.retired), len(service.retiredOrder), retiredTombstoneLimit)
	}
	if _, fenced := service.retired[oldest]; fenced {
		t.Fatal("oldest tombstone survived eviction")
	}

	// A re-admission drops the ordering entry with the tombstone, so a later
	// eviction cannot reclaim a fence the next retirement re-established.
	newest := service.retiredOrder[len(service.retiredOrder)-1]
	service.releaseRetiredLocked(newest)
	service.fenceRetiredLocked(newest)
	if got := len(service.retiredOrder); got != retiredTombstoneLimit {
		t.Fatalf("fence order after re-admission = %d, want %d", got, retiredTombstoneLimit)
	}
	for i := range retiredTombstoneLimit - 1 {
		var id [32]byte
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		id[3] = 0xff
		service.fenceRetiredLocked(id)
	}
	if _, fenced := service.retired[newest]; !fenced {
		t.Fatal("re-established tombstone was evicted by its stale ordering entry")
	}
}

func TestRuntimeCloseJoinsInflightControlAndCanRetryCleanup(t *testing.T) {
	updateEntered := make(chan struct{}, 1)
	updateExited := make(chan struct{})
	releaseUpdate := make(chan struct{})
	retireCalled := make(chan struct{}, 2)
	var retireCalls atomic.Int32
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(context.Context, Session, SessionUpdate) error {
		updateEntered <- struct{}{}
		<-releaseUpdate
		close(updateExited)
		return nil
	}
	pipeline.retire = func(context.Context, [32]byte) error {
		select {
		case <-updateExited:
		default:
			return errors.New("retire raced with update")
		}
		retireCalled <- struct{}{}
		if retireCalls.Add(1) == 1 {
			return errors.New("temporary retire failure")
		}
		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	session, update := fixture.session(15, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	next := update
	next.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 999)
	updateResult := make(chan error, 1)
	go func() { updateResult <- fixture.service.UpdateSession(context.Background(), next) }()
	select {
	case <-updateEntered:
	case <-time.After(time.Second):
		t.Fatal("update did not enter pipeline")
	}
	closeResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		closeResult <- fixture.service.Close(ctx)
	}()
	select {
	case <-retireCalled:
		t.Fatal("close retired pipeline while update was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseUpdate)
	<-updateResult
	if err := <-closeResult; err == nil {
		t.Fatal("first close unexpectedly ignored retire failure")
	}
	status, err := fixture.service.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Closing || status.Closed {
		t.Fatalf("failed close status = %+v", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = fixture.service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = fixture.service.Status(context.Background())
	if err != nil || !status.Closed || status.Closing {
		t.Fatalf("successful retry close status=%+v err=%v", status, err)
	}
	record, err := fixture.storage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("accepted update lost across Close = %+v, want %+v", record.Update, next)
	}
}

func TestRuntimeCloseDrainsUpdateCommittedBeforePublication(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	committed := make(chan struct{})
	releasePipeline := make(chan struct{})
	retireCalled := make(chan struct{}, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(context.Context, Session, SessionUpdate) error {
		close(committed)
		<-releasePipeline

		return nil
	}
	pipeline.retire = func(context.Context, [32]byte) error {
		retireCalled <- struct{}{}

		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, baseStorage, nil)

	session, update := fixture.session(187, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	storage := &runtimeBlockedSessionAdmissionStorage{
		runtimeMemoryStorage: baseStorage,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
		deleteEntered:        make(chan struct{}),
	}
	fixture.service.opts.Storage = storage

	next := update
	next.CurrentWindowStart++
	next.CurrentWindowObservedSlot++
	next.CurrentWindowStartAt = update.CurrentWindowStartAt.Add(time.Second)
	updateResult := make(chan error, 1)
	go func() {
		updateResult <- fixture.service.UpdateSession(context.Background(), next)
	}()
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("update did not commit in the pipeline")
	}

	closeResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closeResult <- fixture.service.Close(ctx)
	}()
	runtimeAwait(t, func() bool {
		status, err := fixture.service.Status(context.Background())

		return err == nil && status.Closing
	})
	close(releasePipeline)
	if err := <-updateResult; err != nil {
		t.Fatalf("pipeline-committed update returned after Close began: %v", err)
	}
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("accepted update did not enter storage after Close began")
	}
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before accepted WAL admission completed: %v", err)
	case <-retireCalled:
		t.Fatal("Close retired the pipeline before accepted WAL admission completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(storage.release)
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	record, err := baseStorage.Session(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Update.Equal(next) {
		t.Fatalf("accepted update lost at Close boundary = %+v, want %+v", record.Update, next)
	}
}

func TestRuntimePrepareIsIdempotent(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(16, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	fixture.prepare(t, session, update)
	pipeline.mu.Lock()
	prepared := pipeline.prepared
	pipeline.mu.Unlock()
	if prepared != 1 {
		t.Fatalf("pipeline prepare calls = %d, want 1", prepared)
	}
	changed := update
	changed.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 321)
	if err := fixture.service.PrepareSession(context.Background(), session, changed); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("changed duplicate prepare error = %v", err)
	}
}

func TestRuntimeConcurrentPrepareTimeoutKeepsCreatorHandle(t *testing.T) {
	prepareEntered := make(chan struct{}, 1)
	releasePrepare := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.prepare = func(context.Context, Session, SessionUpdate) error {
		prepareEntered <- struct{}{}
		<-releasePrepare
		return nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	session, update := fixture.session(51, 1, 0, time.Now())

	creatorResult := make(chan error, 1)
	go func() {
		creatorResult <- fixture.service.PrepareSession(context.Background(), session, update)
	}()
	select {
	case <-prepareEntered:
	case <-time.After(time.Second):
		t.Fatal("creator did not enter pipeline prepare")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := fixture.service.PrepareSession(ctx, session, update); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued prepare error = %v, want deadline exceeded", err)
	}
	close(releasePrepare)
	if err := <-creatorResult; err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.PrepareSession(context.Background(), session, update); err != nil {
		t.Fatal(err)
	}

	fixture.close(t)
	pipeline.mu.Lock()
	prepared, retired := pipeline.prepared, pipeline.retired
	pipeline.mu.Unlock()
	if prepared != 1 || retired != 1 {
		t.Fatalf("pipeline lifecycle prepare=%d retire=%d, want 1/1", prepared, retired)
	}
}

func TestRuntimeDuplicatePrepareDoesNotWaitForProduction(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return runtimeBuiltCandidate(request), nil
		}
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(47, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("production did not start")
	}

	result := make(chan error, 1)
	go func() {
		result <- fixture.service.PrepareSession(context.Background(), session, update)
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		close(release)
		<-result
		t.Fatal("idempotent prepare waited for active production")
	}
	close(release)
}

func TestRuntimeCandidateConflictDoesNotHotRetry(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		candidate := runtimeBuiltCandidate(request)
		candidate.ID.Workchain = 1
		return candidate, nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(17, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())
		return status.FailedWindows == 1
	})
	// FailedWindows is incremented after produceWindowWithRetry returns, so the
	// retry loop is provably done and the build count is final.
	if pipeline.buildCount() != 1 {
		t.Fatalf("deterministic conflict build attempts = %d, want 1", pipeline.buildCount())
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	window := managed.authorizations[WindowID{SessionID: session.ID}]
	managed.mu.Unlock()
	if window.State != delegatedAuthorizationCancelled {
		t.Fatalf("terminal conflict state = %d, want cancelled", window.State)
	}

	// Exact reconciliation is allowed to reschedule pending authority. A
	// terminal failure must therefore leave a non-pending idempotency record.
	if err = fixture.service.UpdateSession(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	jobs := len(managed.productions)
	managed.mu.Unlock()
	if jobs != 0 || pipeline.buildCount() != 1 {
		t.Fatalf("terminal conflict relaunched: jobs=%d builds=%d", jobs, pipeline.buildCount())
	}
}

func TestRuntimeStaleEmitErrorDoesNotRelaunch(t *testing.T) {
	var emits atomic.Int32
	pipeline := &runtimeTestPipeline{}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(context.Context, CandidateArtifact) error {
		emits.Add(1)

		return ErrStaleWindow
	})
	defer fixture.close(t)

	session, update := fixture.session(65, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	request := fixture.request(t, session, 0)
	if err := fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())

		return status.FailedWindows == 1
	})

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	window := managed.authorizations[WindowID{SessionID: session.ID}]
	jobs := len(managed.productions)
	managed.mu.Unlock()
	if window.State != delegatedAuthorizationCancelled || jobs != 0 {
		t.Fatalf("stale emit outcome: state=%d jobs=%d, want cancelled/zero", window.State, jobs)
	}
	if emits.Load() != 1 || pipeline.buildCount() != 1 {
		t.Fatalf("stale emit attempts=%d builds=%d, want 1/1", emits.Load(), pipeline.buildCount())
	}
	if err = fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("exact terminal duplicate: %v", err)
	}

	if err = fixture.service.UpdateSession(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	jobs = len(managed.productions)
	managed.mu.Unlock()
	if jobs != 0 || emits.Load() != 1 || pipeline.buildCount() != 1 {
		t.Fatalf(
			"stale emit relaunched after reconcile: jobs=%d attempts=%d builds=%d",
			jobs,
			emits.Load(),
			pipeline.buildCount(),
		)
	}
}

func TestRuntimePermanentBuildErrorsDoNotHotRetry(t *testing.T) {
	for _, buildErr := range []error{
		ErrInvalidInput,
		ErrUnsupported,
		ErrSizeLimit,
		ErrCollatedRootNotFound,
	} {
		t.Run(buildErr.Error(), func(t *testing.T) {
			pipeline := &runtimeTestPipeline{}
			pipeline.build = func(context.Context, BuildRequest) (*Candidate, error) {
				return nil, fmt.Errorf("deterministic build failure: %w", buildErr)
			}
			fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
			defer fixture.close(t)
			session, update := fixture.session(45, 1, 0, time.Now())
			fixture.prepare(t, session, update)
			if err := fixture.service.CommitDelegation(
				context.Background(),
				fixture.request(t, session, 0),
			); err != nil {
				t.Fatal(err)
			}
			runtimeAwait(t, func() bool {
				status, _ := fixture.service.Status(context.Background())
				return status.FailedWindows == 1
			})
			if pipeline.buildCount() != 1 {
				t.Fatalf("build attempts = %d, want 1", pipeline.buildCount())
			}
		})
	}
}

func TestRuntimeExpiredHardDeadlineDoesNotHotRetry(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, _ BuildRequest) (*Candidate, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(46, 1, 0, time.Now().Add(-time.Minute-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, 0),
	); err != nil {
		t.Fatal(err)
	}
	runtimeAwait(t, func() bool {
		status, _ := fixture.service.Status(context.Background())
		return status.FailedWindows == 1
	})
	if pipeline.buildCount() != 1 {
		t.Fatalf("expired build attempts = %d, want 1", pipeline.buildCount())
	}
}

func runtimeAwaitArtifact(t *testing.T, ch <-chan CandidateArtifact) CandidateArtifact {
	t.Helper()
	select {
	case artifact := <-ch:
		return artifact
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for candidate")
		return CandidateArtifact{}
	}
}

func runtimeAwaitBuild(t *testing.T, ch <-chan BuildRequest) BuildRequest {
	t.Helper()
	select {
	case request := <-ch:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for build")
		return BuildRequest{}
	}
}

func runtimeAwait(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for runtime state")
}

func runtimeAwaitSessionWrite(t *testing.T, service *Service, id [32]byte) {
	t.Helper()
	runtimeAwait(t, func() bool {
		managed, err := service.runningSession(id)
		if err != nil {
			return false
		}
		managed.mu.Lock()
		ready := !managed.sessionWritePending && !managed.sessionWriteRunning
		managed.mu.Unlock()

		return ready
	})
}

func TestCtxMutexContract(t *testing.T) {
	mutex := newCtxMutex()
	if !mutex.TryLock() {
		t.Fatal("fresh mutex refused TryLock")
	}
	if mutex.TryLock() {
		t.Fatal("held mutex granted TryLock")
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	blocked := make(chan error, 1)
	go func() { blocked <- mutex.LockCtx(expired) }()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("contended LockCtx with expired context = %v, want context.Canceled", err)
	}
	mutex.Unlock()
	// A free lock still wins over an expired context, exactly as the TryLock-first
	// poll loop it replaced did.
	if err := mutex.LockCtx(expired); err != nil {
		t.Fatalf("uncontended LockCtx with expired context = %v, want nil", err)
	}

	handed := make(chan struct{})
	go func() {
		if err := mutex.LockCtx(context.Background()); err != nil {
			t.Error(err)
		}
		close(handed)
	}()
	mutex.Unlock()
	select {
	case <-handed:
	case <-time.After(time.Second):
		t.Fatal("release did not hand the lock to the waiter")
	}
	mutex.Unlock()

	// The unbalanced-unlock panic replaces the one sync.Mutex gave the
	// hand-balanced non-defer unlock sites in the runtime.
	defer func() {
		if recover() == nil {
			t.Fatal("unlock of an unlocked mutex did not panic")
		}
	}()
	mutex.Unlock()
}

// syncLogBuffer is a zerolog sink the test goroutine can read while the
// producer goroutine is still writing: a producer keeps running after it emits
// its candidate, and the line these tests are about is written on the way out.
type syncLogBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)

	return len(p), nil
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.data)
}

// awaitLogLine waits for one message to appear in the buffer and returns the
// whole log.
func awaitLogLine(t *testing.T, buffer *syncLogBuffer, message string) string {
	t.Helper()

	deadline := time.After(3 * time.Second)
	for {
		if output := buffer.String(); strings.Contains(output, message) {
			return output
		}
		select {
		case <-deadline:
			t.Fatalf("log line %q never appeared; log=%q", message, buffer.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A local input that has not arrived yet is not a collation failure. The window
// producer retries on a 0/5/10/20/20 ms schedule, so one waiting window used to
// emit a burst of identical "block collation failed" lines — 27,027 of 27,103
// such lines in five hours on the test network, against 48 genuine ones — and
// the same error mapped onto the error result of the build counter, which is
// what made a 0.18% failure rate read as 39%.
func TestRuntimeDoesNotLogNotReadyRetriesAsCollationFailures(t *testing.T) {
	emitted := make(chan CandidateArtifact, 1)
	pipeline := &runtimeTestPipeline{}
	var attempts atomic.Int32
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		if attempts.Add(1) <= 3 {
			return nil, fmt.Errorf("%w: exact neighbor state for block 371 is unavailable", ErrAcquisitionNotReady)
		}

		return runtimeBuiltCandidate(request), nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	output := &syncLogBuffer{}
	fixture.service.log = zerolog.New(output).Level(zerolog.DebugLevel)
	session, update := fixture.session(83, 1, 4, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 4)); err != nil {
		t.Fatal(err)
	}
	runtimeAwaitArtifact(t, emitted)
	// The window summary is written as the producer leaves the window, after
	// the candidate it finally built has already been emitted.
	log := awaitLogLine(t, output, "collation window waited for inputs")

	messages := map[string]int{}
	var waits []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode producer log: %v; line=%q", err, line)
		}
		message, _ := event["message"].(string)
		messages[message]++
		if message == "collation window waited for inputs" {
			waits = append(waits, event)
		}
	}

	if got := messages["block collation failed"]; got != 0 {
		t.Fatalf("not-ready retries emitted %d collation failure lines; log=%s", got, log)
	}
	if len(waits) != 1 {
		t.Fatalf("input waits logged = %d, want exactly one summary per window; log=%s", len(waits), log)
	}
	if got := waits[0]["attempts"]; got != float64(3) {
		t.Fatalf("input wait attempts = %v, want the 3 retries; event=%v", got, waits[0])
	}
	if _, exists := waits[0]["waited"]; !exists {
		t.Fatalf("input wait has no waited duration: %v", waits[0])
	}
}

// A genuine build failure is a defect and stays visible, at a level that is
// affordable now that the retries are gone: 48 lines in five hours rather than
// 27,103.
func TestRuntimeLogsGenuineCollationFailureAtWarn(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	buildErr := errors.New("outbound queue is absent")
	pipeline.build = func(context.Context, BuildRequest) (*Candidate, error) {
		return nil, buildErr
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	output := &syncLogBuffer{}
	fixture.service.log = zerolog.New(output).Level(zerolog.WarnLevel)
	session, update := fixture.session(84, 1, 4, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 4)); err != nil {
		t.Fatal(err)
	}

	log := awaitLogLine(t, output, "block collation failed")
	line := strings.SplitN(strings.TrimSpace(log), "\n", 2)[0]
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("decode failure log: %v; line=%q", err, line)
	}
	if event["level"] != "warn" {
		t.Fatalf("collation failure level = %v, want warn; event=%v", event["level"], event)
	}
}

// The build counter must not call a retried wait an error either: that is the
// half of the defect a log change alone would leave in place.
func TestCollationResultSeparatesNotReadyFromError(t *testing.T) {
	wrapped := fmt.Errorf("%w: exact neighbor state for block 371 is unavailable", ErrAcquisitionNotReady)
	if got := collationResult(wrapped); got != CollationResultNotReady {
		t.Fatalf("not-ready build result = %d, want CollationResultNotReady (%d)", got, CollationResultNotReady)
	}
	if got := collationResult(errors.New("outbound queue is absent")); got != CollationResultError {
		t.Fatalf("genuine build result = %d, want CollationResultError", got)
	}

	// validator-engine's two-valued counter must not see it at all: the attempt
	// that eventually runs is the one it should count.
	stats := blockstats.New()
	observer := &blockStatsObserver{stats: stats}
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain:  MetricChainShardchain,
		Result: CollationResultNotReady,
	})
	if collated := stats.BlockStats().Collated.Shard; collated != (blockstats.Counter{}) {
		t.Fatalf("not-ready builds recorded validator-engine collations %+v, want none", collated)
	}
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain:  MetricChainShardchain,
		Result: CollationResultError,
	})
	if collated := stats.BlockStats().Collated.Shard; collated.Error != 1 {
		t.Fatalf("genuine build failures recorded %+v, want one error", collated)
	}
}

// TestRuntimeAdmitsDelegationBehindAPendingSessionWrite pins the difference
// between "consensus progress has been applied to this session" and "production
// may run against the current record right now". publishSessionWrite disarms
// the second on every accepted session mutation until its storage callback
// lands, and the delegated flow reaches CommitDelegation immediately after one:
// LocalSessionBackend.HandleLeaderWindow calls UpdateSession and then
// authorizeUpcomingWindow in the same breath. Refusing there dropped the window
// without a trace — the caller logs the refusal at debug and marks the window
// handled — so the admission reads the applied flag, which a write does not
// touch, and only the launch gate keeps waiting for the callback.
func TestRuntimeAdmitsDelegationBehindAPendingSessionWrite(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(63, 1, 0, time.Now())
	fixture.prepare(t, session, update)
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	managed.progressReady = false
	managed.progressReadyAfterWrite = true
	managed.sessionWritePending = true
	managed.mu.Unlock()

	probe := WindowPreparation{SessionID: session.ID, SourceADNL: fixture.sourceADNL, StartSlot: 1}
	if err = fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatalf("probe refused behind a pending session write: %v", err)
	}
	request := fixture.request(t, session, 1)
	if err = fixture.service.CommitDelegation(context.Background(), request); err != nil {
		t.Fatalf("delegation refused behind a pending session write: %v", err)
	}
	managed.mu.Lock()
	_, authorized := managed.authorizations[request.ID()]
	managed.mu.Unlock()
	if !authorized {
		t.Fatal("accepted delegation was not installed in receiver memory")
	}

	// The condition the gate exists for is untouched: a session serving a
	// descriptor recovered from storage, which has never materialized consensus
	// progress, still refuses.
	managed.mu.Lock()
	managed.progressApplied = false
	managed.progressReady = true
	managed.sessionWritePending = false
	managed.mu.Unlock()
	err = fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 2))
	if !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("delegation before any consensus progress = %v, want ErrAcquisitionNotReady", err)
	}
}
