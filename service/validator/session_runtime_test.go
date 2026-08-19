package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	corestorage "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type runtimeTestJournal struct{}

func (*runtimeTestJournal) Bootstrap() (*simplex.BootstrapState, error) {
	return &simplex.BootstrapState{}, nil
}

func (*runtimeTestJournal) SaveOurVote(_ simplex.Vote, done func(error)) { done(nil) }

func (*runtimeTestJournal) SaveCertificate(_ *simplex.Certificate, done func(error)) { done(nil) }

func (*runtimeTestJournal) SaveFirstNonAnnouncedWindow(_ uint32, done func(error)) { done(nil) }

type runtimeStartupOrder struct {
	mu    sync.Mutex
	steps []string
}

func (o *runtimeStartupOrder) add(step string) {
	o.mu.Lock()
	o.steps = append(o.steps, step)
	o.mu.Unlock()
}

func (o *runtimeStartupOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.steps...)
}

type runtimeStartupOrderJournal struct {
	runtimeTestJournal

	mu       sync.Mutex
	calls    int
	first    uint32
	order    *runtimeStartupOrder
	started  chan struct{}
	startOne sync.Once
}

func (j *runtimeStartupOrderJournal) Bootstrap() (*simplex.BootstrapState, error) {
	j.mu.Lock()
	j.calls++
	call := j.calls
	j.mu.Unlock()
	if call > 1 {
		j.order.add("simplex")
		j.startOne.Do(func() { close(j.started) })
	}

	return &simplex.BootstrapState{FirstNonAnnouncedWindow: j.first}, nil
}

type runtimeTestStorage struct {
	mu sync.Mutex

	// Keyed by the namespace, as the real store keys its rows: a descriptor
	// carries two fields no namespace derivation reads, so a double keyed by the
	// whole SessionStorageID would answer "not found" for rows the store would
	// have found.
	candidates  map[validatorTestNamespace]map[simplex.CandidateID]CandidateRecord
	delegations map[validatorTestNamespace]map[uint32]DelegationAuthorization
	saves       int
	loads       int
	finalized   []simplex.CandidateID
	windows     []LeaderWindowRecord
	saveHook    func(SessionStorageID, CandidateRecord, func(error))
	windowHook  func(SessionStorageID, LeaderWindowRecord, func(error))
	// loadHook runs inside every candidate read, after it has been counted, so a
	// test can hold reads open and observe how many of them overlap.
	loadHook func()
}

func newRuntimeTestStorage() *runtimeTestStorage {
	return &runtimeTestStorage{
		candidates:  make(map[validatorTestNamespace]map[simplex.CandidateID]CandidateRecord),
		delegations: make(map[validatorTestNamespace]map[uint32]DelegationAuthorization),
	}
}

func (*runtimeTestStorage) Journal(SessionStorageID, int) simplex.Journal {
	return &runtimeTestJournal{}
}

func (*runtimeTestStorage) Sessions(context.Context) ([]SessionStorageID, error) {
	return nil, nil
}

func (s *runtimeTestStorage) SaveCandidate(
	session SessionStorageID,
	record CandidateRecord,
	done func(error),
) {
	s.mu.Lock()
	s.saves++
	hook := s.saveHook
	if hook == nil {
		namespace := validatorTestNamespaceOf(session)
		byID := s.candidates[namespace]
		if byID == nil {
			byID = make(map[simplex.CandidateID]CandidateRecord)
			s.candidates[namespace] = byID
		}
		record.Wire = bytes.Clone(record.Wire)
		// Candidates are content-addressed durably, so the stored identity is
		// derived here exactly once and handed back by Candidate.
		record.ContentHash = sha256.Sum256(record.Wire)
		byID[record.ID] = record
	}
	s.mu.Unlock()

	if hook != nil {
		hook(session, record, done)
		return
	}
	done(nil)
}

func (s *runtimeTestStorage) Candidate(
	ctx context.Context,
	session SessionStorageID,
	id simplex.CandidateID,
) (CandidateRecord, error) {
	if err := ctx.Err(); err != nil {
		return CandidateRecord{}, err
	}

	s.mu.Lock()
	s.loads++
	hook := s.loadHook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}

	s.mu.Lock()
	record, exists := s.candidates[validatorTestNamespaceOf(session)][id]
	s.mu.Unlock()
	if !exists {
		return CandidateRecord{}, corestorage.ErrNotFound
	}
	record.Wire = bytes.Clone(record.Wire)

	return record, nil
}

func (s *runtimeTestStorage) MarkFinalized(
	_ SessionStorageID,
	id simplex.CandidateID,
	done func(error),
) {
	s.mu.Lock()
	s.finalized = append(s.finalized, id)
	s.mu.Unlock()
	done(nil)
}

func (s *runtimeTestStorage) RecordLeaderWindow(
	session SessionStorageID,
	window LeaderWindowRecord,
	done func(error),
) {
	s.mu.Lock()
	hook := s.windowHook
	if hook == nil {
		s.windows = append(s.windows, window)
	}
	s.mu.Unlock()

	if hook != nil {
		hook(session, window, done)

		return
	}
	done(nil)
}

func (*runtimeTestStorage) LoadSession(
	_ context.Context,
	session SessionStorageID,
) (StoredSessionState, error) {
	return StoredSessionState{ID: session}, nil
}

func (*runtimeTestStorage) DeleteSession(context.Context, SessionStorageID) error { return nil }

func (*runtimeTestStorage) Status(context.Context) (StorageStatus, error) {
	return StorageStatus{}, nil
}

func (s *runtimeTestStorage) SaveDelegationAuthorization(
	_ context.Context,
	session SessionStorageID,
	authorization DelegationAuthorization,
	done func(error),
) {
	s.mu.Lock()
	namespace := validatorTestNamespaceOf(session)
	byStart := s.delegations[namespace]
	if byStart == nil {
		byStart = make(map[uint32]DelegationAuthorization)
		s.delegations[namespace] = byStart
	}
	stored, exists := byStart[authorization.StartSlot]
	if exists && (stored.Collator != authorization.Collator ||
		!bytes.Equal(stored.Signature, authorization.Signature)) {
		s.mu.Unlock()
		done(ErrDelegationConflict)

		return
	}
	authorization.Signature = bytes.Clone(authorization.Signature)
	byStart[authorization.StartSlot] = authorization
	s.mu.Unlock()
	done(nil)
}

func (s *runtimeTestStorage) DelegationAuthorization(
	ctx context.Context,
	session SessionStorageID,
	start uint32,
) (DelegationAuthorization, error) {
	if err := ctx.Err(); err != nil {
		return DelegationAuthorization{}, err
	}
	s.mu.Lock()
	authorization, exists := s.delegations[validatorTestNamespaceOf(session)][start]
	s.mu.Unlock()
	if !exists {
		return DelegationAuthorization{}, corestorage.ErrNotFound
	}
	authorization.Signature = bytes.Clone(authorization.Signature)

	return authorization, nil
}

func (s *runtimeTestStorage) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.saves
}

func (s *runtimeTestStorage) loadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.loads
}

type runtimeTestBackend struct {
	mu sync.Mutex

	stateRoot  *cell.Cell
	updates    []SessionState
	updateErr  error
	closeErrs  []error
	closeCalls int
	windows    chan LeaderWindow
	window     func(context.Context, LeaderWindow) error
	progresses chan sessionConsensusProgress
	progress   func(context.Context, sessionConsensusProgress) error
	load       func(context.Context, ChainStateRequest) (ChainStateData, error)
	validation func(context.Context, *ChainState, *CandidateArtifact) (CandidateValidation, error)
	// requests observes the internal parent-bound successor handle.
	requests   func(CandidateValidationRequest)
	acceptance func(context.Context, BlockAcceptance) error
}

func (*runtimeTestBackend) ActivateSession(context.Context, SessionStart) error {
	return nil
}

func newRuntimeTestBackend() *runtimeTestBackend {
	return &runtimeTestBackend{
		stateRoot: cell.BeginCell().MustStoreUInt(0x51, 8).EndCell(),
		windows:   make(chan LeaderWindow, 16),
	}
}

func (b *runtimeTestBackend) UpdateSession(_ context.Context, state SessionState) error {
	b.mu.Lock()
	err := b.updateErr
	if err == nil {
		b.updates = append(b.updates, cloneSessionState(state))
	}
	b.mu.Unlock()

	return err
}

func (b *runtimeTestBackend) LoadChainState(
	ctx context.Context,
	request ChainStateRequest,
) (ChainStateData, error) {
	if b.load != nil {
		return b.load(ctx, request)
	}

	tips := make([]ChainTip, len(request.Blocks))
	for i := range request.Blocks {
		tips[i] = ChainTip{ID: request.Blocks[i], State: b.stateRoot}
		if request.Blocks[i].SeqNo != 0 {
			tips[i].BlockBOC = testTipBOCFor(request.Blocks[i])
			tips[i].Block = testTipBlockFor(request.Blocks[i])
		}
	}

	return ChainStateData{Tips: tips}, nil
}

func (b *runtimeTestBackend) ValidateCandidate(
	ctx context.Context,
	request CandidateValidationRequest,
) (CandidateValidation, error) {
	if b.requests != nil {
		b.requests(request)
	}
	if b.validation != nil {
		return b.validation(ctx, request.Parent, request.Artifact)
	}

	return testCandidateValidation(request.Parent, request.Artifact)
}

func (b *runtimeTestBackend) AcceptBlock(ctx context.Context, acceptance BlockAcceptance) error {
	if b.acceptance != nil {
		return b.acceptance(ctx, acceptance)
	}

	return nil
}

func (b *runtimeTestBackend) HandleLeaderWindow(ctx context.Context, window LeaderWindow) error {
	if b.window != nil {
		return b.window(ctx, window)
	}
	b.windows <- window

	return nil
}

func (b *runtimeTestBackend) ObserveConsensusProgress(
	ctx context.Context,
	progress sessionConsensusProgress,
) error {
	if b.progress != nil {
		return b.progress(ctx, progress)
	}
	if b.progresses != nil {
		b.progresses <- progress
	}

	return nil
}

func (*runtimeTestBackend) HandleMisbehavior(context.Context, uint32, simplex.Misbehavior) error {
	return nil
}

func (b *runtimeTestBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	call := b.closeCalls
	b.closeCalls++
	if call < len(b.closeErrs) {
		return b.closeErrs[call]
	}

	return nil
}

func (b *runtimeTestBackend) Retire() error { return b.Close() }

func (b *runtimeTestBackend) updateCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.updates)
}

func (b *runtimeTestBackend) setUpdateError(err error) {
	b.mu.Lock()
	b.updateErr = err
	b.mu.Unlock()
}

func (b *runtimeTestBackend) setCloseErrors(errs ...error) {
	b.mu.Lock()
	b.closeErrs = append([]error(nil), errs...)
	b.mu.Unlock()
}

func (b *runtimeTestBackend) closeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.closeCalls
}

func (b *runtimeTestBackend) latestState() SessionState {
	b.mu.Lock()
	defer b.mu.Unlock()

	return cloneSessionState(b.updates[len(b.updates)-1])
}

type runtimeTestNetwork struct {
	mu sync.Mutex

	started    chan struct{}
	startOne   sync.Once
	receiver   SessionReceiver
	startErr   error
	runErr     error
	request    func(context.Context, CandidateRequest) (CandidateResponse, error)
	broadcast  func(context.Context, simplex.CandidateBroadcast, CandidateArtifact) error
	broadcasts []simplex.CandidateBroadcast
}

type runtimeStartupOrderNetwork struct {
	*runtimeTestNetwork

	order *runtimeStartupOrder
}

func (n *runtimeStartupOrderNetwork) Start(ctx context.Context, receiver SessionReceiver) error {
	if err := n.runtimeTestNetwork.Start(ctx, receiver); err != nil {
		return err
	}
	n.order.add("network")

	return nil
}

func newRuntimeTestNetwork() *runtimeTestNetwork {
	return &runtimeTestNetwork{started: make(chan struct{})}
}

func (*runtimeTestNetwork) BroadcastToAll([]byte) {}

func (*runtimeTestNetwork) BroadcastToValidators([]byte) {}

func (*runtimeTestNetwork) BroadcastToRandom(uint32, []byte) {}

func (n *runtimeTestNetwork) BroadcastCandidate(
	ctx context.Context,
	broadcast simplex.CandidateBroadcast,
	artifact CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.broadcast != nil {
		return n.broadcast(ctx, broadcast, artifact)
	}

	n.mu.Lock()
	n.broadcasts = append(n.broadcasts, simplex.CandidateBroadcast{
		Data:  bytes.Clone(broadcast.Data),
		Extra: bytes.Clone(broadcast.Extra),
	})
	n.mu.Unlock()

	return nil
}

func (n *runtimeTestNetwork) RequestCandidate(
	ctx context.Context,
	request CandidateRequest,
) (CandidateResponse, error) {
	if n.request != nil {
		return n.request(ctx, request)
	}

	return CandidateResponse{}, ErrCandidateUnavailable
}

func (n *runtimeTestNetwork) Start(ctx context.Context, receiver SessionReceiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.startErr != nil {
		return n.startErr
	}

	n.mu.Lock()
	n.receiver = receiver
	n.mu.Unlock()
	n.startOne.Do(func() { close(n.started) })

	return nil
}

func (n *runtimeTestNetwork) Run(ctx context.Context) error {
	if n.runErr != nil {
		return n.runErr
	}

	<-ctx.Done()

	return nil
}

func (n *runtimeTestNetwork) candidateBroadcasts() []simplex.CandidateBroadcast {
	n.mu.Lock()
	defer n.mu.Unlock()

	return append([]simplex.CandidateBroadcast(nil), n.broadcasts...)
}

var _ CandidateProvider = (*runtimeTestNetwork)(nil)
var _ SessionNetwork = (*runtimeTestNetwork)(nil)

type runtimeTestSigner struct {
	key ed25519.PrivateKey
}

func (s runtimeTestSigner) Sign(data []byte) ([]byte, error) {
	return ed25519.Sign(s.key, data), nil
}

func runtimeTestConfig(id byte, journal simplex.Journal) (SessionConfig, ed25519.PrivateKey) {
	seed := bytes.Repeat([]byte{id}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID := simplex.KeyNodeIDShort(publicKey)

	var sessionID, adnlID [32]byte
	sessionID[0] = id
	adnlID[0] = id
	adnlID[31] = 0xa5
	protocol := SessionProtocol{
		Version:              2,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: 4,
	}
	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	storageID := SessionStorageID{
		SessionID:      sessionID,
		Shard:          shard,
		CatchainSeqno:  7,
		LocalADNLID:    adnlID,
		ValidatorIndex: simplex.ObserverIndex,
		Protocol:       protocol,
	}
	var public [32]byte
	copy(public[:], publicKey)

	return SessionConfig{
		SessionID:      sessionID,
		Shard:          shard,
		CatchainSeqno:  storageID.CatchainSeqno,
		Validators:     []groups.Validator{{PublicKey: public, PublicKeyHash: keyID, Weight: 1}},
		OverlayMembers: [][32]byte{adnlID},
		ShardPrefixLen: 0,
		Protocol:       protocol,
		Identity:       SessionIdentity{ADNLID: adnlID},
		StorageID:      storageID,
		Journal:        journal,
	}, privateKey
}

func TestMasterchainRecoveryFinalizationsAreVerifiedAndOrdered(t *testing.T) {
	config, privateKey := runtimeTestConfig(0x45, &runtimeTestJournal{})
	validators := runtimeValidators(config.Validators)
	masterchain := groups.ShardID{Workchain: -1, Shard: math.MinInt64}

	first := runtimeTestCertificate(
		t,
		config.SessionID,
		privateKey,
		simplex.FinalizeVote(simplex.CandidateID{Slot: 2, Hash: [32]byte{0x21}}),
	)
	second := runtimeTestCertificate(
		t,
		config.SessionID,
		privateKey,
		simplex.FinalizeVote(simplex.CandidateID{Slot: 9, Hash: [32]byte{0x91}}),
	)
	got, err := masterchainRecoveryFinalizations(
		masterchain,
		runtimeTestBootstrap(t, config, second, first),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Certificate() != first || got[1].Certificate() != second {
		t.Fatalf("recovery finalizations are not in slot order: %+v", got)
	}
	// The replay set is sealed, and sealed against this session and roster:
	// block acceptance drops its own Ed25519 pass on the strength of that.
	binding, err := simplex.NewCertificateBinding(config.SessionID, validators)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if err = binding.Check(got[i]); err != nil {
			t.Fatalf("recovery finalization %d is not sealed for this session: %v", i, err)
		}
	}

	conflict := runtimeTestCertificate(
		t,
		config.SessionID,
		privateKey,
		simplex.FinalizeVote(simplex.CandidateID{Slot: 9, Hash: [32]byte{0x92}}),
	)
	if _, err = masterchainRecoveryFinalizations(
		masterchain,
		runtimeTestBootstrap(t, config, second, conflict),
	); err == nil {
		t.Fatal("conflicting final certificates for one slot were accepted")
	}

	// Signatures are checked once, when the journal state is validated, so a
	// forged certificate never reaches the replay selection above.
	bad := &simplex.Certificate{
		Vote:       first.Vote,
		Signatures: append([]simplex.VoteSignature(nil), first.Signatures...),
	}
	bad.Signatures[0].Signature = append([]byte(nil), bad.Signatures[0].Signature...)
	bad.Signatures[0].Signature[0] ^= 0xff
	if _, err = simplex.ValidateBootstrap(
		config.SessionID,
		validators,
		&simplex.BootstrapState{Certificates: []*simplex.Certificate{bad}},
	); err == nil {
		t.Fatal("forged recovery final certificate was accepted")
	}
}

func runtimeTestBootstrap(
	t testing.TB,
	config SessionConfig,
	certificates ...*simplex.Certificate,
) simplex.ValidatedBootstrap {
	t.Helper()

	bootstrap, err := simplex.ValidateBootstrap(
		config.SessionID,
		runtimeValidators(config.Validators),
		&simplex.BootstrapState{Certificates: certificates},
	)
	if err != nil {
		t.Fatalf("validate bootstrap: %v", err)
	}

	return bootstrap
}

func runtimeTestCertificate(
	t testing.TB,
	sessionID [32]byte,
	privateKey ed25519.PrivateKey,
	vote simplex.Vote,
) *simplex.Certificate {
	t.Helper()

	return &simplex.Certificate{
		Vote: vote,
		Signatures: []simplex.VoteSignature{{
			ValidatorIndex: 0,
			Signature: ed25519.Sign(
				privateKey,
				simplex.DataToSign(sessionID, simplex.VoteBytes(vote)),
			),
		}},
	}
}

// runtimeTestSeal builds a genuinely verified certificate. simplex.VerifiedCertificate
// has no populated literal form outside its own package by design, so tests
// take exactly the route production takes: sign, then verify.
func runtimeTestSeal(
	t testing.TB,
	config SessionConfig,
	privateKey ed25519.PrivateKey,
	vote simplex.Vote,
) simplex.VerifiedCertificate {
	t.Helper()

	verified, err := simplex.VerifyCertificate(
		config.SessionID,
		runtimeValidators(config.Validators),
		runtimeTestCertificate(t, config.SessionID, privateKey, vote),
	)
	if err != nil {
		t.Fatalf("verify test certificate: %v", err)
	}

	return verified
}

func runtimeTestState() SessionState {
	return SessionState{Params: simplex.DefaultParams()}
}

func runtimeTestStart() SessionStart {
	rootHash := bytes.Repeat([]byte{0x11}, 32)
	fileHash := bytes.Repeat([]byte{0x22}, 32)

	return SessionStart{Genesis: []ton.BlockIDExt{{
		Workchain: 0,
		Shard:     math.MinInt64,
		SeqNo:     0,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}}}
}

func prepareRuntimeTest(
	t *testing.T,
	id byte,
	storage *runtimeTestStorage,
	network *runtimeTestNetwork,
	backend *runtimeTestBackend,
) (*sessionRuntime, ed25519.PrivateKey) {
	t.Helper()

	config, privateKey := runtimeTestConfig(id, &runtimeTestJournal{})
	session, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}

	return session.(*sessionRuntime), privateKey
}

func TestValidateRuntimeConfigUsesSigningKeyADNLFallback(t *testing.T) {
	config, privateKey := runtimeTestConfig(0x41, &runtimeTestJournal{})
	keyID := config.Validators[0].PublicKeyHash
	config.Identity = SessionIdentity{
		ADNLID: keyID,
		Validator: &ValidatorIdentity{
			Index:  0,
			KeyID:  keyID,
			Signer: runtimeTestSigner{key: privateKey},
		},
	}
	config.StorageID.IsValidator = true
	config.StorageID.ValidatorKeyID = keyID
	config.StorageID.LocalADNLID = keyID
	config.StorageID.ValidatorIndex = 0
	config.OverlayMembers = [][32]byte{keyID}
	options := RuntimeOptions{
		Storage: newRuntimeTestStorage(),
		Network: newRuntimeTestNetwork(),
		Backend: newRuntimeTestBackend(),
	}
	if err := validateRuntimeConfig(config, options); err != nil {
		t.Fatalf("signing-key ADNL fallback rejected: %v", err)
	}

	otherADNL := [32]byte{0xff}
	config.Identity.ADNLID = otherADNL
	config.StorageID.LocalADNLID = otherADNL
	if err := validateRuntimeConfig(config, options); err == nil {
		t.Fatal("validator identity with a non-roster ADNL was accepted")
	}
}

func TestSessionRuntimeUpdateBeforeRunAndLifecycle(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	runtime, _ := prepareRuntimeTest(t, 0x31, storage, network, backend)

	updated := runtimeTestState()
	updated.Params.TargetRate = 37 * time.Millisecond
	if err := runtime.Update(context.Background(), updated); err != nil {
		t.Fatalf("pre-run update: %v", err)
	}
	if runtime.candidates.params.TargetRate != updated.Params.TargetRate {
		t.Fatalf("candidate resolver target rate = %v, want %v", runtime.candidates.params.TargetRate, updated.Params.TargetRate)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx, runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}
	if err := runtime.Run(context.Background(), runtimeTestStart()); !errors.Is(err, ErrSessionRuntimeStarted) {
		t.Fatalf("duplicate run error = %v, want ErrSessionRuntimeStarted", err)
	}
	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("run cancellation: %v", err)
	}
	if err := runtime.Run(context.Background(), runtimeTestStart()); !errors.Is(err, ErrSessionRuntimeStarted) {
		t.Fatalf("second run error = %v, want ErrSessionRuntimeStarted", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if backend.updateCount() != 2 {
		t.Fatalf("backend update count = %d, want 2", backend.updateCount())
	}
}

func TestSessionRuntimeStartsNetworkBeforeSimplex(t *testing.T) {
	order := &runtimeStartupOrder{}
	journal := &runtimeStartupOrderJournal{
		first:   3,
		order:   order,
		started: make(chan struct{}),
	}
	config, _ := runtimeTestConfig(0x68, journal)
	storage := newRuntimeTestStorage()
	network := &runtimeStartupOrderNetwork{
		runtimeTestNetwork: newRuntimeTestNetwork(),
		order:              order,
	}
	backend := newRuntimeTestBackend()
	session, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := session.(*sessionRuntime)
	runCtx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx, runtimeTestStart()) }()
	select {
	case <-journal.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Simplex startup was not reached")
	}

	steps := order.snapshot()
	if len(steps) < 2 || steps[0] != "network" || steps[1] != "simplex" {
		cancel()
		t.Fatalf("runtime startup order = %v, want network/simplex", steps)
	}

	cancel()
	select {
	case err = <-runResult:
		if err != nil {
			t.Fatalf("run cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop after cancellation")
	}
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeUpdateReportsTerminalCause(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	runtime, _ := prepareRuntimeTest(t, 0x32, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}

	terminalErr := errors.New("terminal session failure")
	runtime.fail(terminalErr)
	if err := <-runResult; !errors.Is(err, terminalErr) {
		t.Fatalf("run error = %v, want terminal cause", err)
	}
	if err := runtime.Update(context.Background(), runtimeTestState()); !errors.Is(err, terminalErr) {
		t.Fatalf("update error = %v, want terminal cause", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeLeaderWindowTelemetryDoesNotDelayCurrentWindow(t *testing.T) {
	storage := newRuntimeTestStorage()
	telemetryCallbacks := make(chan func(error), 1)
	storage.windowHook = func(_ SessionStorageID, _ LeaderWindowRecord, done func(error)) {
		telemetryCallbacks <- done
	}
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	runtime, _ := prepareRuntimeTest(t, 0x33, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}
	select {
	case <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("initial consensus window was not delivered")
	}
	current := simplex.Window{
		Base:         simplex.Genesis(),
		StartSlot:    100,
		EndSlot:      104,
		ObservedSlot: 100,
		ObservedAt:   time.Now(),
		LocalLeader:  true,
	}
	runtime.HandleWindow(current)
	var telemetryDone func(error)
	select {
	case telemetryDone = <-telemetryCallbacks:
	case <-time.After(time.Second):
		t.Fatal("leader window telemetry was not submitted")
	}
	select {
	case window := <-backend.windows:
		if window.Window.StartSlot != current.StartSlot {
			t.Fatalf("current leader window start = %d, want %d",
				window.Window.StartSlot, current.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked leader window telemetry delayed the current backend window")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- runtime.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime close waited for leader window telemetry")
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}

	callbackReturned := make(chan struct{})
	go func() {
		telemetryDone(nil)
		close(callbackReturned)
	}()
	select {
	case <-callbackReturned:
	case <-time.After(time.Second):
		t.Fatal("late leader window telemetry callback blocked after shutdown")
	}
}

func TestSessionRuntimeHandlesConsensusWindowsInSimplexOrder(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	initial := make(chan simplex.Window, 1)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	backend.progress = func(ctx context.Context, progress sessionConsensusProgress) error {
		switch progress.Window.StartSlot {
		case 0:
			select {
			case initial <- progress.Window:
			default:
			}
		case 100:
			close(firstEntered)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		case 200:
			close(secondEntered)
		}

		return nil
	}
	runtime, _ := prepareRuntimeTest(t, 0x32, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	var baseWindow simplex.Window
	select {
	case baseWindow = <-initial:
	case <-time.After(time.Second):
		t.Fatal("initial consensus window was not observed")
	}

	first := baseWindow
	first.LocalLeader = false
	first.StartSlot = 100
	first.ObservedSlot = 100
	first.EndSlot = 101
	second := first
	second.StartSlot = 200
	second.ObservedSlot = 200
	second.EndSlot = 201

	runtime.HandleWindow(first)
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first consensus window was not handled")
	}
	runtime.HandleWindow(second)
	select {
	case <-secondEntered:
		t.Fatal("second consensus window overtook the blocked first window")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("second consensus window was not handled after the first completed")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeProgressFailureSkipsOnlyCurrentWindow(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "hard producer error", err: errors.New("producer state failed")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := newRuntimeTestStorage()
			network := newRuntimeTestNetwork()
			backend := newRuntimeTestBackend()
			failures := make(chan error, 1)
			failures <- test.err
			observed := make(chan simplex.Window, 2)
			backend.progress = func(_ context.Context, progress sessionConsensusProgress) error {
				observed <- progress.Window
				select {
				case err := <-failures:
					return err
				default:
					return nil
				}
			}
			runtime, _ := prepareRuntimeTest(t, 0x33, storage, network, backend)

			runResult := make(chan error, 1)
			go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
			select {
			case window := <-observed:
				if window.StartSlot != 0 {
					t.Fatalf("failed window start = %d, want 0", window.StartSlot)
				}
			case <-time.After(time.Second):
				t.Fatal("initial consensus progress was not observed")
			}
			select {
			case window := <-backend.windows:
				t.Fatalf("failed producer window was delivered: %+v", window.Window)
			case err := <-runResult:
				t.Fatalf("runtime stopped on producer progress failure: %v", err)
			case <-time.After(50 * time.Millisecond):
			}

			nextState := runtimeTestState()
			nextState.Params.TargetRate += time.Millisecond
			if err := runtime.Update(context.Background(), nextState); err != nil {
				t.Fatalf("update after producer progress failure: %v", err)
			}
			next := simplex.Window{
				Base:         simplex.Genesis(),
				StartSlot:    100,
				EndSlot:      104,
				ObservedSlot: 100,
				ObservedAt:   time.Now(),
			}
			runtime.HandleWindow(next)
			select {
			case window := <-observed:
				if window.StartSlot != next.StartSlot {
					t.Fatalf("retried window start = %d, want %d", window.StartSlot, next.StartSlot)
				}
			case <-time.After(time.Second):
				t.Fatal("later consensus progress was not observed")
			}
			select {
			case window := <-backend.windows:
				if window.Window.StartSlot != next.StartSlot {
					t.Fatalf("later leader window start = %d, want %d", window.Window.StartSlot, next.StartSlot)
				}
			case <-time.After(time.Second):
				t.Fatal("later leader window was not delivered")
			}

			if err := runtime.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-runResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSessionRuntimeRetriesTransientConsensusProgress(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "block state not ready", err: fmt.Errorf("load finalized state: %w", ErrBlockNotReady)},
		{
			name: "local acquisition not ready",
			err:  fmt.Errorf("seed predecessor queue: %w", collator.ErrAcquisitionNotReady),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := newRuntimeTestStorage()
			network := newRuntimeTestNetwork()
			backend := newRuntimeTestBackend()
			attempts := make(chan simplex.Window, 2)
			var count int
			backend.progress = func(_ context.Context, progress sessionConsensusProgress) error {
				attempts <- progress.Window
				count++
				if count == 1 {
					return test.err
				}

				return nil
			}
			runtime, _ := prepareRuntimeTest(t, 0x34, storage, network, backend)

			runResult := make(chan error, 1)
			go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
			for attempt := 0; attempt < 2; attempt++ {
				select {
				case window := <-attempts:
					if window.StartSlot != 0 {
						t.Fatalf("attempt %d window start = %d, want 0", attempt, window.StartSlot)
					}
				case err := <-runResult:
					t.Fatalf("runtime stopped while retrying transient progress: %v", err)
				case <-time.After(time.Second):
					t.Fatalf("transient progress attempt %d was not observed", attempt+1)
				}
			}
			select {
			case window := <-backend.windows:
				if window.Window.StartSlot != 0 {
					t.Fatalf("leader window start = %d, want 0", window.Window.StartSlot)
				}
			case <-time.After(time.Second):
				t.Fatal("leader window was not delivered after transient progress recovered")
			}

			if err := runtime.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-runResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSessionRuntimeCancelsTransientProgressAtNewerWindow(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	initialProgress := make(chan struct{}, 1)
	firstAttempt := make(chan struct{})
	secondProgress := make(chan struct{})
	backend.progress = func(ctx context.Context, progress sessionConsensusProgress) error {
		switch progress.Window.StartSlot {
		case 0:
			initialProgress <- struct{}{}
		case 100:
			close(firstAttempt)
			return fmt.Errorf("wait for local predecessor: %w", collator.ErrAcquisitionNotReady)
		case 104:
			close(secondProgress)
		}

		return nil
	}
	runtime, _ := prepareRuntimeTest(t, 0x35, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-initialProgress:
	case <-time.After(time.Second):
		t.Fatal("initial consensus progress was not observed")
	}
	select {
	case <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("initial leader window was not delivered")
	}

	first := simplex.Window{
		Base:         simplex.Genesis(),
		StartSlot:    100,
		EndSlot:      104,
		ObservedSlot: 100,
		ObservedAt:   time.Now(),
	}
	runtime.HandleWindow(first)
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("first window did not enter transient progress")
	}
	second := first
	second.StartSlot = 104
	second.EndSlot = 108
	second.ObservedSlot = 104
	second.ObservedAt = time.Now()
	runtime.HandleWindow(second)
	select {
	case <-secondProgress:
	case err := <-runResult:
		t.Fatalf("runtime stopped while superseding transient progress: %v", err)
	case <-time.After(time.Second):
		t.Fatal("newer window did not supersede the transient progress wait")
	}
	select {
	case window := <-backend.windows:
		if window.Window.StartSlot != second.StartSlot {
			t.Fatalf("leader window start = %d, want only newer %d", window.Window.StartSlot, second.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("newer leader window was not delivered")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeDoesNotRetryProgressPastWindowDeadline(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	initialProgress := make(chan struct{}, 1)
	progresses := make(chan simplex.Window, 2)
	backend.progress = func(_ context.Context, progress sessionConsensusProgress) error {
		progresses <- progress.Window
		if progress.Window.StartSlot == 0 {
			initialProgress <- struct{}{}
		}

		return nil
	}
	runtime, _ := prepareRuntimeTest(t, 0x36, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-initialProgress:
	case <-time.After(time.Second):
		t.Fatal("initial consensus progress was not observed")
	}
	select {
	case progress := <-progresses:
		if progress.StartSlot != 0 {
			t.Fatalf("initial progress start = %d, want 0", progress.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("initial consensus progress was not delivered")
	}
	select {
	case <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("initial leader window was not delivered")
	}

	stale := simplex.Window{
		Base:         simplex.Genesis(),
		StartSlot:    100,
		EndSlot:      104,
		ObservedSlot: 100,
		ObservedAt:   time.Now().Add(-time.Hour),
	}
	runtime.HandleWindow(stale)
	select {
	case window := <-progresses:
		t.Fatalf("expired progress was observed: %+v", window)
	case window := <-backend.windows:
		t.Fatalf("expired leader window was delivered: %+v", window.Window)
	case err := <-runResult:
		t.Fatalf("runtime stopped on expired progress: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	fresh := stale
	fresh.StartSlot = 104
	fresh.EndSlot = 108
	fresh.ObservedSlot = 104
	fresh.ObservedAt = time.Now()
	runtime.HandleWindow(fresh)
	select {
	case progress := <-progresses:
		if progress.StartSlot != fresh.StartSlot {
			t.Fatalf("progress start = %d, want %d", progress.StartSlot, fresh.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not resume progress after expired window")
	}
	select {
	case window := <-backend.windows:
		if window.Window.StartSlot != fresh.StartSlot {
			t.Fatalf("leader window start = %d, want %d", window.Window.StartSlot, fresh.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh leader window was not delivered")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeLeaderWindowFailureSkipsOnlyCurrentWindow(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	producerErr := errors.New("sign leader delegation failed")
	failures := make(chan error, 1)
	failures <- producerErr
	handled := make(chan LeaderWindow, 2)
	backend.window = func(_ context.Context, window LeaderWindow) error {
		handled <- window
		select {
		case err := <-failures:
			return err
		default:
			return nil
		}
	}
	runtime, _ := prepareRuntimeTest(t, 0x34, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	var failedWindow LeaderWindow
	select {
	case window := <-handled:
		failedWindow = window
		if window.Window.StartSlot != 0 {
			t.Fatalf("failed leader window start = %d, want 0", window.Window.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("initial leader window was not handled")
	}
	select {
	case err := <-runResult:
		t.Fatalf("runtime stopped on leader-window producer failure: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	deadline := time.Now().Add(time.Second)
	for {
		err := failedWindow.Submit(context.Background(), &CandidateArtifact{
			Candidate: simplex.Candidate{ID: simplex.CandidateID{Slot: math.MaxUint32}},
		})
		if errors.Is(err, producerErr) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failed window submitter remained open: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	nextState := runtimeTestState()
	nextState.Params.TargetRate += time.Millisecond
	if err := runtime.Update(context.Background(), nextState); err != nil {
		t.Fatalf("update after leader-window producer failure: %v", err)
	}
	next := simplex.Window{
		Base:         simplex.Genesis(),
		StartSlot:    100,
		EndSlot:      104,
		ObservedSlot: 100,
		ObservedAt:   time.Now(),
		LocalLeader:  true,
	}
	runtime.HandleWindow(next)
	select {
	case window := <-handled:
		if window.Window.StartSlot != next.StartSlot {
			t.Fatalf("later leader window start = %d, want %d", window.Window.StartSlot, next.StartSlot)
		}
	case err := <-runResult:
		t.Fatalf("runtime stopped before later leader window: %v", err)
	case <-time.After(time.Second):
		t.Fatal("later leader window was not delivered")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeWaitsForExactFinalizedStateWithoutStopping(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	resolver.mu.Lock()
	resolver.finalized[artifacts[0].Candidate.ID] = &finalizedState{isDone: true, reconciled: true}
	resolver.mu.Unlock()

	var calls int
	backend := newRuntimeTestBackend()
	backend.load = func(
		_ context.Context,
		request ChainStateRequest,
	) (ChainStateData, error) {
		calls++
		if calls == 1 {
			return ChainStateData{}, ErrBlockNotReady
		}

		tips := make([]ChainTip, len(request.Blocks))
		for i := range request.Blocks {
			tips[i] = ChainTip{
				ID:       request.Blocks[i],
				State:    backend.stateRoot,
				BlockBOC: testTipBOCFor(request.Blocks[i]),
				Block:    testTipBlockFor(request.Blocks[i]),
			}
		}

		return ChainStateData{Tips: tips}, nil
	}
	resolver.backend = backend
	runtime := &sessionRuntime{states: resolver}
	window := simplex.Window{
		Base:      simplex.Parent(artifacts[2].Candidate.ID),
		StartSlot: artifacts[2].Candidate.ID.Slot + 1,
	}
	anchor := artifacts[0].Candidate.Block

	lineage, err := runtime.resolveWindowLineage(context.Background(), window, &anchor)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("exact state load calls = %d, want 2", calls)
	}
	assertArtifactIdentity(t, lineage.Candidates, artifacts)
	if lineage.AppliedAnchor == nil || !sameBlockID(*lineage.AppliedAnchor, anchor) {
		t.Fatalf("applied anchor = %v, want %v", lineage.AppliedAnchor, anchor)
	}
}

func TestSessionRuntimeSkipsWindowBehindLocallyFinalizedState(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	backend.progresses = make(chan sessionConsensusProgress, 4)
	runtime, _ := prepareRuntimeTest(t, 0x52, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}
	select {
	case <-backend.progresses:
	case <-time.After(time.Second):
		t.Fatal("initial consensus progress was not observed")
	}
	select {
	case <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("initial leader window was not delivered")
	}

	state := runtimeTestState()
	ahead := runtimeTestStart().Genesis[0]
	ahead.SeqNo++
	ahead.RootHash = testTipRootHash(0xa1)
	ahead.FileHash = testTipFileHash(0xa1)
	state.FinalizedBlock = &ahead
	if err := runtime.Update(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	runtime.HandleWindow(simplex.Window{
		StartSlot:    100,
		EndSlot:      101,
		ObservedSlot: 100,
		ObservedAt:   time.Now(),
	})
	select {
	case progress := <-backend.progresses:
		t.Fatalf("stale consensus progress was applied: %+v", progress.Window)
	case window := <-backend.windows:
		t.Fatalf("stale leader window was delivered: %+v", window.Window)
	case err := <-runResult:
		t.Fatalf("runtime stopped on stale consensus progress: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	genesis := runtimeTestStart().Genesis[0]
	state.FinalizedBlock = &genesis
	if err := runtime.Update(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	runtime.HandleWindow(simplex.Window{
		StartSlot:    101,
		EndSlot:      102,
		ObservedSlot: 101,
		ObservedAt:   time.Now(),
	})
	select {
	case progress := <-backend.progresses:
		if progress.Window.StartSlot != 101 {
			t.Fatalf("consensus progress start = %d, want 101", progress.Window.StartSlot)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not resume consensus progress")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeFinalizationReleasesResolvedStates(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	accepted := make(chan struct{}, 1)
	backend.acceptance = func(context.Context, BlockAcceptance) error {
		accepted <- struct{}{}

		return nil
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x57, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}

	slot := 4 * runtime.states.stateRetainedSlots()
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, slot, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	runtime.candidates.observeNotarization(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.NotarizeVote(artifact.Candidate.ID)),
	)

	stale := simplex.Parent(simplex.CandidateID{Slot: 0, Hash: [32]byte{0xc0}})
	done := make(chan struct{})
	close(done)
	runtime.states.mu.Lock()
	runtime.states.states[stale] = &stateFlight{done: done}
	runtime.states.mu.Unlock()

	runtime.OnFinalized(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.FinalizeVote(artifact.Candidate.ID)),
	)
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("finalized block was not handed to acceptance")
	}

	runtime.states.mu.Lock()
	retained := runtime.states.states[stale]
	runtime.states.mu.Unlock()
	if retained != nil {
		t.Fatal("finalization did not release the resolved state below its watermark")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeFinalizationReleasesCandidatePayloads(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	accepted := make(chan struct{}, 1)
	backend.acceptance = func(context.Context, BlockAcceptance) error {
		accepted <- struct{}{}

		return nil
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x58, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}

	superseded := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err := runtime.candidates.stage(superseded, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.candidates.store(context.Background(), superseded.Candidate.ID); err != nil {
		t.Fatal(err)
	}

	slot := uint32(4 * candidateCacheRetainedSlots)
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, slot, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x02}); err != nil {
		t.Fatal(err)
	}
	runtime.candidates.observeNotarization(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.NotarizeVote(artifact.Candidate.ID)),
	)
	runtime.OnFinalized(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.FinalizeVote(artifact.Candidate.ID)),
	)
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("finalized block was not handed to acceptance")
	}

	runtime.candidates.mu.Lock()
	released := runtime.candidates.entries[superseded.Candidate.ID]
	finalized := runtime.candidates.entries[artifact.Candidate.ID]
	runtime.candidates.mu.Unlock()
	if released.candidate != nil || released.wire != nil {
		t.Fatal("finalization did not release the candidate payload below its watermark")
	}
	if !released.durable || !released.hasWireHash {
		t.Fatalf("released candidate lost its durable identity: %+v", released)
	}
	if finalized.candidate != artifact {
		t.Fatal("the candidate being finalized was released with its ancestors")
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeValidatesCandidateAfterPayloadRelease(t *testing.T) {
	storage := newRuntimeTestStorage()
	runtime, privateKey := prepareRuntimeTest(
		t,
		0x59,
		storage,
		newRuntimeTestNetwork(),
		newRuntimeTestBackend(),
	)
	defer runtime.Close()
	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	const slot = uint32(6)
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, slot, simplex.Genesis())
	wire, _, err := runtime.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.candidates.stage(artifact, wire); err != nil {
		t.Fatal(err)
	}
	if err = runtime.candidates.store(context.Background(), artifact.Candidate.ID); err != nil {
		t.Fatal(err)
	}
	runtime.candidates.notifyFinalized(slot+candidateCacheRetainedSlots+1, retentionFloorNone)

	if err = runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatalf("validation after the candidate payload was released: %v", err)
	}
	if storage.loadCount() != 1 {
		t.Fatalf("candidate reloads = %d, want exactly one", storage.loadCount())
	}
}

// Readiness waits belong inside the one validation query. If an implementation
// still reports not-ready to the runtime, that query has ended and Simplex
// abstains; the runtime must not restart all semantic stages itself.
func TestSessionRuntimeDoesNotRetryNotReadyValidation(t *testing.T) {
	storage := newRuntimeTestStorage()
	backend := newRuntimeTestBackend()
	attempts := 0
	backend.validation = func(
		_ context.Context,
		_ *ChainState,
		_ *CandidateArtifact,
	) (CandidateValidation, error) {
		attempts++

		return CandidateValidation{}, ErrBlockNotReady
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x5a, storage, newRuntimeTestNetwork(), backend)
	defer runtime.Close()
	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}

	err := runtime.validateCandidate(context.Background(), &artifact.Candidate)
	if !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("validation error = %v, want ErrBlockNotReady", err)
	}
	if attempts != 1 {
		t.Fatalf("validation attempts = %d, want exactly one", attempts)
	}
}

func TestSessionRuntimeAcceptanceFailureDoesNotStopConsensus(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	accepted := make(chan struct{}, 1)
	backend.acceptance = func(context.Context, BlockAcceptance) error {
		accepted <- struct{}{}

		return errors.New("local acceptance unavailable")
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x53, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 1, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	runtime.candidates.observeNotarization(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.NotarizeVote(artifact.Candidate.ID)),
	)
	runtime.OnFinalized(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.FinalizeVote(artifact.Candidate.ID)),
	)
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("finalized block was not handed to acceptance")
	}
	select {
	case err := <-runResult:
		t.Fatalf("runtime stopped after local acceptance failure: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeBackendUpdateFailureKeepsPreviousState(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	runtime, _ := prepareRuntimeTest(t, 0x33, storage, network, backend)
	defer runtime.Close()

	previous := runtimeTestState()
	updated := previous
	updated.Params.TargetRate = 17 * time.Millisecond
	updateErr := errors.New("backend update failed")
	backend.setUpdateError(updateErr)
	if err := runtime.Update(context.Background(), updated); !errors.Is(err, updateErr) {
		t.Fatalf("update error = %v, want backend failure", err)
	}

	runtime.stateMu.RLock()
	runtimeRate := runtime.state.Params.TargetRate
	runtime.stateMu.RUnlock()
	runtime.candidates.mu.Lock()
	resolverRate := runtime.candidates.params.TargetRate
	runtime.candidates.mu.Unlock()
	if runtimeRate != previous.Params.TargetRate || resolverRate != previous.Params.TargetRate {
		t.Fatalf(
			"failed update leaked params: runtime=%v resolver=%v want=%v",
			runtimeRate,
			resolverRate,
			previous.Params.TargetRate,
		)
	}
	if backend.updateCount() != 1 || backend.latestState().Params.TargetRate != previous.Params.TargetRate {
		t.Fatal("atomic backend changed its accepted state on error")
	}
}

func TestSessionRuntimeRetriesBackendCloseAfterFailure(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	closeErr := errors.New("retire failed")
	backend.setCloseErrors(closeErr, nil)
	runtime, _ := prepareRuntimeTest(t, 0x34, storage, network, backend)

	if err := runtime.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("first close error = %v, want %v", err, closeErr)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if backend.closeCount() != 2 {
		t.Fatalf("backend close calls = %d, want 2", backend.closeCount())
	}
}

func TestPrepareSessionRuntimeJoinsBackendCleanupFailure(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	updateErr := errors.New("initial backend update failed")
	closeErr := errors.New("backend cleanup failed")
	backend.setUpdateError(updateErr)
	backend.setCloseErrors(closeErr)
	config, _ := runtimeTestConfig(0x35, &runtimeTestJournal{})

	runtime, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if runtime != nil {
		t.Fatal("failed preparation returned a runtime")
	}
	if !errors.Is(err, updateErr) || !errors.Is(err, closeErr) {
		t.Fatalf("preparation error = %v, want joined update and cleanup failures", err)
	}
	if backend.closeCount() != 1 {
		t.Fatalf("backend cleanup calls = %d, want 1", backend.closeCount())
	}
}

func TestSessionRuntimePropagatesEarlyNetworkFailure(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	networkErr := errors.New("overlay failed")
	network.runErr = networkErr
	backend := newRuntimeTestBackend()
	runtime, _ := prepareRuntimeTest(t, 0x32, storage, network, backend)

	err := runtime.Run(context.Background(), runtimeTestStart())
	if !errors.Is(err, networkErr) || !strings.Contains(err.Error(), "network failed") {
		t.Fatalf("run error = %v, want wrapped network failure", err)
	}
	if closeErr := runtime.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
}

// runtimeBlockingStartNetwork stalls inside SessionNetwork.Start, which is the
// window where the runtime already reports itself as running while the Simplex
// loop goroutine does not exist yet.
type runtimeBlockingStartNetwork struct {
	*runtimeTestNetwork

	entered chan struct{}
	release chan struct{}
	err     error
}

func (n *runtimeBlockingStartNetwork) Start(context.Context, SessionReceiver) error {
	close(n.entered)
	<-n.release

	return n.err
}

func TestSessionRuntimeUpdateBeforeSimplexLoopStarts(t *testing.T) {
	storage := newRuntimeTestStorage()
	startErr := errors.New("private overlay failed to start")
	network := &runtimeBlockingStartNetwork{
		runtimeTestNetwork: newRuntimeTestNetwork(),
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
		err:                startErr,
	}
	backend := newRuntimeTestBackend()
	config, _ := runtimeTestConfig(0x55, &runtimeTestJournal{})
	session, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := session.(*sessionRuntime)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	select {
	case <-network.entered:
	case <-time.After(time.Second):
		t.Fatal("network start was not reached")
	}

	// The session supervisor updates every masterchain block. Posting that
	// update onto the runner queue here would wait for a loop that this run is
	// about to abandon, and it would wait while holding the lifecycle lock that
	// Run and Close both need to finish.
	updated := runtimeTestState()
	updated.Params.TargetRate += time.Millisecond
	updateResult := make(chan error, 1)
	go func() { updateResult <- runtime.Update(context.Background(), updated) }()
	select {
	case err = <-updateResult:
		if err != nil {
			t.Fatalf("update before the simplex loop started: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("update blocked on a simplex loop that was never started")
	}

	close(network.release)
	select {
	case err = <-runResult:
		if !errors.Is(err, startErr) {
			t.Fatalf("run error = %v, want %v", err, startErr)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not return after the network failed to start")
	}
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionPreparerCompositionBindsDistinctNetworks(t *testing.T) {
	storage := newRuntimeTestStorage()
	var mu sync.Mutex
	networks := make(map[[32]byte]*runtimeTestNetwork)
	backends := make(map[[32]byte]*runtimeTestBackend)
	prepare := func(
		ctx context.Context,
		config SessionConfig,
		initial SessionState,
		_ SessionStart,
	) (SessionRuntime, error) {
		network := newRuntimeTestNetwork()
		backend := newRuntimeTestBackend()
		mu.Lock()
		networks[config.SessionID] = network
		backends[config.SessionID] = backend
		mu.Unlock()

		return PrepareSessionRuntime(ctx, config, initial, RuntimeOptions{
			Storage: storage,
			Network: network,
			Backend: backend,
			Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
		})
	}

	configA, _ := runtimeTestConfig(0x41, &runtimeTestJournal{})
	configB, _ := runtimeTestConfig(0x42, &runtimeTestJournal{})
	sessionA, err := prepare(context.Background(), configA, runtimeTestState(), runtimeTestStart())
	if err != nil {
		t.Fatal(err)
	}
	sessionB, err := prepare(context.Background(), configB, runtimeTestState(), runtimeTestStart())
	if err != nil {
		t.Fatal(err)
	}
	if networks[configA.SessionID] == networks[configB.SessionID] {
		t.Fatal("composition reused a session-scoped network")
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	resultA := make(chan error, 1)
	resultB := make(chan error, 1)
	go func() { resultA <- sessionA.Run(ctxA, runtimeTestStart()) }()
	go func() { resultB <- sessionB.Run(ctxB, runtimeTestStart()) }()
	<-networks[configA.SessionID].started
	<-networks[configB.SessionID].started
	cancelA()
	cancelB()
	if err = <-resultA; err != nil {
		t.Fatal(err)
	}
	if err = <-resultB; err != nil {
		t.Fatal(err)
	}
	if err = sessionA.Close(); err != nil {
		t.Fatal(err)
	}
	if err = sessionB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderWindowSubmitterPersistsBeforeIngressAndCloses(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	var broadcastSlot uint32
	var broadcastArtifact CandidateArtifact
	broadcasted := false
	network.broadcast = func(
		_ context.Context,
		broadcast simplex.CandidateBroadcast,
		artifact CandidateArtifact,
	) error {
		if storage.saveCount() != 0 {
			return errors.New("candidate was stored before broadcast")
		}
		extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
		if err != nil {
			return err
		}
		broadcastSlot = extra.Slot
		broadcastArtifact = artifact
		broadcasted = true

		return nil
	}
	storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
		if !broadcasted {
			done(errors.New("candidate reached storage before broadcast"))

			return
		}
		done(nil)
	}
	backend := newRuntimeTestBackend()
	backend.progresses = make(chan sessionConsensusProgress, 1)
	runtime, privateKey := prepareRuntimeTest(t, 0x51, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	<-network.started
	select {
	case progress := <-backend.progresses:
		if progress.Window.StartSlot != 0 || progress.Window.ObservedSlot != 0 {
			t.Fatalf("consensus progress window = %+v", progress.Window)
		}
	case <-time.After(time.Second):
		t.Fatal("local backend did not observe the initial consensus progress")
	}
	var window LeaderWindow
	select {
	case window = <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("leader window was not delivered")
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, window.Window.StartSlot, window.Window.Base)

	wrongLeader := *artifact
	wrongLeader.Candidate = artifact.Candidate
	wrongLeader.Candidate.Leader++
	if err := window.Submit(context.Background(), &wrongLeader); err == nil {
		t.Fatal("wrong leader candidate was accepted")
	}
	if storage.saveCount() != 0 {
		t.Fatal("rejected candidate reached storage")
	}
	if err := window.Submit(context.Background(), artifact); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want 1", storage.saveCount())
	}
	// The local collator can replay an already persisted candidate while
	// retrying a later slot in the same window. Re-delivery of the exact
	// candidate must not turn that retry into an out-of-order proposal.
	if err := window.Submit(context.Background(), artifact); err != nil {
		t.Fatalf("replay accepted candidate: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves after replay = %d, want 1", storage.saveCount())
	}
	if !broadcasted || broadcastSlot != artifact.Candidate.ID.Slot {
		t.Fatalf("candidate broadcast = (%v, %d), want (true, %d)", broadcasted, broadcastSlot, artifact.Candidate.ID.Slot)
	}
	if broadcastArtifact.Candidate.ID != artifact.Candidate.ID ||
		!bytes.Equal(broadcastArtifact.BlockBOC, artifact.BlockBOC) ||
		!bytes.Equal(broadcastArtifact.CollatedData, artifact.CollatedData) {
		t.Fatal("candidate broadcast did not receive the original decoded artifact")
	}
	response, err := runtime.ServeCandidate(context.Background(), simplex.PeerID{0x91}, CandidateRequest{
		SessionID:     runtime.config.SessionID,
		ID:            artifact.Candidate.ID,
		WantCandidate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.CandidateWire) == 0 {
		t.Fatal("stored candidate was not served")
	}

	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-runResult; err != nil {
		t.Fatal(err)
	}
	if err = window.Submit(context.Background(), artifact); !errors.Is(err, context.Canceled) {
		t.Fatalf("submit after close error = %v, want context cancellation", err)
	}
}

func TestLeaderWindowSubmitterContinuesAfterBroadcastFailure(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	broadcastErr := errors.New("private candidate delivery failed")
	network.broadcast = func(
		context.Context,
		simplex.CandidateBroadcast,
		CandidateArtifact,
	) error {
		return broadcastErr
	}
	backend := newRuntimeTestBackend()
	config, privateKey := runtimeTestConfig(0x53, &runtimeTestJournal{})
	var logOutput bytes.Buffer
	logger := zerolog.New(&logOutput)
	session, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits: CandidateLimits{
			MaxBlockBytes:        1 << 20,
			MaxCollatedDataBytes: 1 << 20,
		},
		Logger: &logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := session.(*sessionRuntime)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	<-network.started
	var window LeaderWindow
	select {
	case window = <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("leader window was not delivered")
	}
	artifact := runtimeOrdinaryArtifact(
		t,
		runtime.config,
		privateKey,
		window.Window.StartSlot,
		window.Window.Base,
	)
	if err = window.Submit(context.Background(), artifact); err != nil {
		t.Fatalf("broadcast failure stopped local candidate submission: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want 1", storage.saveCount())
	}
	response, err := runtime.ServeCandidate(context.Background(), simplex.PeerID{0x91}, CandidateRequest{
		SessionID:     runtime.config.SessionID,
		ID:            artifact.Candidate.ID,
		WantCandidate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.CandidateWire) == 0 {
		t.Fatal("candidate was not staged for serving after broadcast failure")
	}
	if !strings.Contains(logOutput.String(), broadcastErr.Error()) {
		t.Fatalf("broadcast failure was not logged: %s", logOutput.String())
	}

	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeValidationWaitsForBackendValidAfter(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	backend.validation = func(
		_ context.Context,
		state *ChainState,
		artifact *CandidateArtifact,
	) (CandidateValidation, error) {
		return CandidateValidation{
			ValidAfter: time.Now().Add(35 * time.Millisecond),
			State:      testValidatedCandidateState(t, state, artifact),
		}, nil
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x52, storage, network, backend)
	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if err := runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 30*time.Millisecond {
		t.Fatalf("validation completed after %v, before backend ValidAfter", elapsed)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimePublishesValidatedStateBeforeFinalization(t *testing.T) {
	storage := newRuntimeTestStorage()
	backend := newRuntimeTestBackend()
	var ordinaryLoads int
	backend.load = func(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
		block := request.Blocks[0]
		if block.SeqNo != 0 {
			ordinaryLoads++

			return ChainStateData{}, ErrBlockNotReady
		}

		return ChainStateData{Tips: []ChainTip{{ID: block, State: backend.stateRoot}}}, nil
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x53, storage, newRuntimeTestNetwork(), backend)
	defer runtime.Close()
	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	certificate := runtimeTestSeal(t, runtime.config, privateKey, simplex.NotarizeVote(artifact.Candidate.ID))
	runtime.candidates.observeNotarization(artifact.Candidate.ID, certificate)
	if err := runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatal(err)
	}
	if err := runtime.states.finalize(
		context.Background(),
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.FinalizeVote(artifact.Candidate.ID)),
	); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resolved, err := runtime.states.resolve(ctx, simplex.Parent(artifact.Candidate.ID))
	if err != nil {
		t.Fatalf("resolve validated parent from live view: %v", err)
	}
	block, err := resolved.State.NormalBlock()
	if err != nil {
		t.Fatal(err)
	}
	if !sameBlockID(block, artifact.Candidate.Block) {
		t.Fatalf("resolved block = %+v, want %+v", block, artifact.Candidate.Block)
	}
	if ordinaryLoads != 0 {
		t.Fatalf("validated parent loaded from ordinary store %d times, want 0", ordinaryLoads)
	}
}

func TestSessionRuntimeValidatedStateCompletesInflightStoreLookup(t *testing.T) {
	storage := newRuntimeTestStorage()
	backend := newRuntimeTestBackend()
	storeLookup := make(chan struct{}, 1)
	backend.load = func(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
		block := request.Blocks[0]
		if block.SeqNo != 0 {
			storeLookup <- struct{}{}

			return ChainStateData{}, ErrBlockNotReady
		}

		return ChainStateData{Tips: []ChainTip{{ID: block, State: backend.stateRoot}}}, nil
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x54, storage, newRuntimeTestNetwork(), backend)
	defer runtime.Close()
	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	runtime.candidates.observeNotarization(
		artifact.Candidate.ID,
		runtimeTestSeal(t, runtime.config, privateKey, simplex.NotarizeVote(artifact.Candidate.ID)),
	)
	runtime.states.mu.Lock()
	runtime.states.finalized[artifact.Candidate.ID] = &finalizedState{isDone: true}
	runtime.states.mu.Unlock()

	resolved := make(chan error, 1)
	go func() {
		_, err := runtime.states.resolve(context.Background(), simplex.Parent(artifact.Candidate.ID))
		resolved <- err
	}()
	select {
	case <-storeLookup:
	case <-time.After(time.Second):
		t.Fatal("finalized parent store lookup did not start")
	}
	if err := runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-resolved:
		if err != nil {
			t.Fatalf("in-flight parent resolution: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("validated live state did not complete the in-flight store lookup")
	}
}

func TestSessionRuntimeLogsCompletedBlockValidation(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	runtime, privateKey := prepareRuntimeTest(t, 0x55, storage, network, backend)
	defer runtime.Close()

	var output bytes.Buffer
	logger := zerolog.New(&output).Level(zerolog.InfoLevel)
	runtime.log = &logger

	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 5, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatal(err)
	}

	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode validation log: %v; output=%q", err, output.String())
	}
	for field, want := range map[string]any{
		"level":          "info",
		"message":        "block validated",
		"slot":           float64(5),
		"window_start":   float64(4),
		"window_end":     float64(8),
		"is_empty":       false,
		"block_seqno":    float64(1),
		"block_bytes":    float64(len(artifact.BlockBOC)),
		"collated_bytes": float64(len(artifact.CollatedData)),
	} {
		if got := event[field]; got != want {
			t.Fatalf("validation log field %q = %v, want %v; event=%v", field, got, want, event)
		}
	}
	if _, exists := event["elapsed"]; !exists {
		t.Fatalf("validation log has no elapsed field: %v", event)
	}
	if event["candidate_hash"] == "" || event["block_root_hash"] == "" || event["block_file_hash"] == "" {
		t.Fatalf("validation log has incomplete candidate identity: %v", event)
	}
}

func TestSessionRuntimeLogsCompletedEmptyCandidateValidation(t *testing.T) {
	storage := newRuntimeTestStorage()
	runtime, _ := prepareRuntimeTest(t, 0x56, storage, newRuntimeTestNetwork(), newRuntimeTestBackend())
	defer runtime.Close()

	var output bytes.Buffer
	logger := zerolog.New(&output).Level(zerolog.InfoLevel)
	runtime.log = &logger

	block := ton.BlockIDExt{
		Workchain: runtime.config.Shard.Workchain,
		Shard:     runtime.config.Shard.Shard,
		SeqNo:     17,
		RootHash:  testTipRootHash(0x61),
		FileHash:  testTipFileHash(0x61),
	}
	stateRoot := cell.BeginCell().MustStoreUInt(0x63, 8).EndCell()
	parentID := simplex.CandidateID{Slot: 4, Hash: [32]byte{0x64}}
	parent := simplex.Parent(parentID)
	flight := &stateFlight{done: make(chan struct{}), result: ResolvedState{State: &ChainState{
		shard: runtime.config.Shard,
		tips:  []ChainTip{{ID: block, State: stateRoot}},
		root:  stateRoot,
	}}}
	close(flight.done)
	runtime.states.states[parent] = flight

	candidate := simplex.Candidate{
		Parent: parent,
		Leader: 0,
		Empty:  true,
		Block:  block,
	}
	candidate.ID = candidate.ComputeID(6)
	artifact := &CandidateArtifact{Candidate: candidate}
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatal(err)
	}

	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode empty validation log: %v; output=%q", err, output.String())
	}
	for field, want := range map[string]any{
		"message":        "block validated",
		"slot":           float64(6),
		"window_start":   float64(4),
		"window_end":     float64(8),
		"is_empty":       true,
		"block_seqno":    float64(17),
		"block_bytes":    float64(0),
		"collated_bytes": float64(0),
	} {
		if got := event[field]; got != want {
			t.Fatalf("empty validation log field %q = %v, want %v; event=%v", field, got, want, event)
		}
	}
}

func TestSessionRuntimeValidationErrorAbstainsWithoutStopping(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	validationErr := errors.New("local candidate validation failed")
	var validationDeadline time.Time
	backend.validation = func(
		ctx context.Context,
		_ *ChainState,
		_ *CandidateArtifact,
	) (CandidateValidation, error) {
		var ok bool
		validationDeadline, ok = ctx.Deadline()
		if !ok {
			return CandidateValidation{}, errors.New("candidate validation has no deadline")
		}

		return CandidateValidation{}, validationErr
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x54, storage, network, backend)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer runtime.Close()
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runCtx, runtimeTestStart()) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("network did not start")
	}

	// Run the session at the test network's own pacing: 200 ms slots with the
	// four slots per leader window runtimeTestConfig sets. That is the exact
	// configuration a window-derived deadline collapsed to 6.4 s on.
	fast := runtimeTestState()
	fast.Params.TargetRate = 200 * time.Millisecond
	if err := runtime.Update(context.Background(), fast); err != nil {
		t.Fatalf("update to the fast slot rate: %v", err)
	}

	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	validationDone := make(chan error, 1)
	runtime.ValidateCandidate(&artifact.Candidate, func(err error) { validationDone <- err })
	select {
	case err := <-validationDone:
		if !errors.Is(err, validationErr) {
			t.Fatalf("validation error = %v, want %v", err, validationErr)
		}
	case <-time.After(time.Second):
		t.Fatal("candidate validation did not complete")
	}
	// The deadline is the flat wall-clock constant and nothing about this
	// session's pacing enters it.
	remaining := time.Until(validationDeadline)
	if remaining < candidateValidationTimeout-time.Second || remaining > candidateValidationTimeout {
		t.Fatalf("candidate validation deadline remaining = %v, want about %v",
			remaining, candidateValidationTimeout)
	}

	select {
	case err := <-runResult:
		t.Fatalf("candidate-local validation error stopped runtime: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	updated := runtimeTestState()
	updated.Params.TargetRate += time.Millisecond
	if err := runtime.Update(context.Background(), updated); err != nil {
		t.Fatalf("update after candidate-local validation error: %v", err)
	}

	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("run cancellation: %v", err)
	}
}

func TestSessionRuntimeValidatesOnNotarizedMasterchainParentState(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	config, privateKey := runtimeTestConfig(0x53, &runtimeTestJournal{})
	config.Shard = groups.ShardID{Workchain: -1, Shard: math.MinInt64}
	config.StorageID.Shard = config.Shard

	applied := ton.BlockIDExt{
		Workchain: -1,
		Shard:     math.MinInt64,
		SeqNo:     164,
		RootHash:  testTipRootHash(0xa1),
		FileHash:  testTipFileHash(0xa1),
	}
	initial := runtimeTestState()
	initial.FinalizedBlock = &applied
	session, err := PrepareSessionRuntime(context.Background(), config, initial, RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := session.(*sessionRuntime)
	defer runtime.Close()

	parentID := simplex.CandidateID{Slot: 2, Hash: [32]byte{0xb1}}
	speculative := ton.BlockIDExt{
		Workchain: -1,
		Shard:     math.MinInt64,
		SeqNo:     applied.SeqNo + 1,
		RootHash:  testTipRootHash(0xb2),
		FileHash:  testTipFileHash(0xb2),
	}
	root := cell.BeginCell().MustStoreUInt(0xb4, 8).EndCell()
	resolved := ResolvedState{State: &ChainState{
		shard: config.Shard,
		tips:  []ChainTip{{ID: speculative, State: root}},
		root:  root,
	}}
	flight := &stateFlight{done: make(chan struct{}), result: resolved}
	close(flight.done)
	runtime.states.states[simplex.Parent(parentID)] = flight

	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 4, simplex.Parent(parentID))
	if err = runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	backend.validation = func(
		_ context.Context,
		state *ChainState,
		artifact *CandidateArtifact,
	) (CandidateValidation, error) {
		block, blockErr := state.NormalBlock()
		if blockErr != nil {
			return CandidateValidation{}, blockErr
		}
		if !block.Equals(&speculative) {
			t.Fatalf("validation parent = %+v, want speculative %+v", block, speculative)
		}

		return CandidateValidation{
			State: testValidatedCandidateState(t, state, artifact),
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = runtime.validateCandidate(ctx, &artifact.Candidate); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRuntimeTargetRateUpdateDoesNotExpireLeaderWindow(t *testing.T) {
	tests := []struct {
		name        string
		initialRate time.Duration
		updatedRate time.Duration
	}{
		{name: "increase", initialRate: 15 * time.Millisecond, updatedRate: 500 * time.Millisecond},
		{name: "decrease", initialRate: 500 * time.Millisecond, updatedRate: 15 * time.Millisecond},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := newRuntimeTestStorage()
			network := newRuntimeTestNetwork()
			backend := newRuntimeTestBackend()
			runtime, privateKey := prepareRuntimeTest(t, byte(0x70+index), storage, network, backend)

			state := runtimeTestState()
			state.Params.TargetRate = test.initialRate
			if err := runtime.Update(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			runResult := make(chan error, 1)
			go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
			<-network.started

			var window LeaderWindow
			select {
			case window = <-backend.windows:
			case <-time.After(time.Second):
				t.Fatal("leader window was not delivered")
			}
			state.Params.TargetRate = test.updatedRate
			if err := runtime.Update(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			time.Sleep(120 * time.Millisecond)

			artifact := runtimeOrdinaryArtifact(
				t,
				runtime.config,
				privateKey,
				window.Window.StartSlot,
				window.Window.Base,
			)
			err := window.Submit(context.Background(), artifact)
			if err != nil {
				t.Fatalf("leader window expired without being superseded: %v", err)
			}

			if err = runtime.Close(); err != nil {
				t.Fatal(err)
			}
			if err = <-runResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSessionRuntimeTreatsActiveBackendCancellationAsFatal(t *testing.T) {
	runtime := &sessionRuntime{fatal: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	runtime.startWork(ctx)
	runtime.fail(context.Canceled)
	select {
	case err := <-runtime.fatal:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("fatal error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active backend cancellation was silently classified as shutdown")
	}

	cancel()
	runtime.fail(context.Canceled)
	select {
	case err := <-runtime.fatal:
		t.Fatalf("shutdown cancellation became fatal: %v", err)
	default:
	}
	runtime.stopWork()
}

func runtimeOrdinaryArtifact(
	t testing.TB,
	config SessionConfig,
	privateKey ed25519.PrivateKey,
	slot uint32,
	parent simplex.ParentID,
) *CandidateArtifact {
	t.Helper()

	return runtimeBlockArtifact(t, config, privateKey, slot, parent, 1, 0x71)
}

func runtimeBlockArtifact(
	t testing.TB,
	config SessionConfig,
	privateKey ed25519.PrivateKey,
	slot uint32,
	parent simplex.ParentID,
	seqNo uint32,
	tag uint64,
) *CandidateArtifact {
	t.Helper()

	// A backend asked for this block as a chain tip has to hand its parsed root
	// back on ChainTip.Block, so the root has to be findable by its hash.
	blockRoot := cell.BeginCell().MustStoreUInt(tag, 8).EndCell()
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	extra := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(time.Now().UnixMilli()), 64).
		EndCell()
	collatedData, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{extra},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	// These exact bytes are what the id's file hash names, so a backend serving
	// this block as a chain tip has to serve them and not a re-serialization.
	registerTestTipBlockBOC(blockRoot, blockBOC)
	fileHash := sha256.Sum256(blockBOC)
	candidate := simplex.Candidate{
		Parent: parent,
		Leader: 0,
		Block: ton.BlockIDExt{
			Workchain: config.Shard.Workchain,
			Shard:     config.Shard.Shard,
			SeqNo:     seqNo,
			RootHash:  blockRoot.Hash(),
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256(collatedData),
	}
	candidate.ID = candidate.ComputeID(slot)
	candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: privateKey},
		config.SessionID,
		candidate.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	return &CandidateArtifact{
		Candidate:    candidate,
		BlockBOC:     blockBOC,
		CollatedData: collatedData,
		// Both file hashes above were taken from these two buffers, which is
		// exactly how both real producers of an artifact get them: the codec
		// derives them from the payload it decompressed, the builder from the
		// buffers it just serialized. A fixture that stood for a produced
		// candidate but withheld the claim would exercise the fallback rather
		// than the path production takes.
		digested: true,
	}
}

// TestLeaderWindowSubmitterDoesNotWaitForTheDurableWrite pins the producer's
// half of the store split: the record is submitted to storage synchronously —
// the save call has already happened when Submit returns — but the fsync is not
// waited for. The wait belongs to the voter, which joins the same write before
// it notarizes (simplex/hardening_test.go TestStoreOverlapsValidation), and the
// reference detaches it here exactly the same way.
func TestLeaderWindowSubmitterDoesNotWaitForTheDurableWrite(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	callbacks := make(chan func(error), 1)
	storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
		callbacks <- done
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x57, storage, network, backend)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	<-network.started
	var window LeaderWindow
	select {
	case window = <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("leader window was not delivered")
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, window.Window.StartSlot, window.Window.Base)

	submitted := make(chan error, 1)
	go func() { submitted <- window.Submit(context.Background(), artifact) }()
	select {
	case err := <-submitted:
		if err != nil {
			t.Fatalf("submit candidate: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the producer is still waiting for the durable write")
	}
	// The submission itself stayed on the producer's goroutine and kept its
	// place in the durable FIFO: the write is outstanding, not deferred.
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want 1", storage.saveCount())
	}
	var done func(error)
	select {
	case done = <-callbacks:
	default:
		t.Fatal("the candidate write was never submitted")
	}

	// A peer asking while the write is outstanding is answered from the staged
	// payload, which is why staging must not move.
	response, err := runtime.ServeCandidate(context.Background(), simplex.PeerID{0x91}, CandidateRequest{
		SessionID:     runtime.config.SessionID,
		ID:            artifact.Candidate.ID,
		WantCandidate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.CandidateWire) == 0 {
		t.Fatal("candidate was not servable while its durable write was outstanding")
	}

	done(nil)
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-runResult; err != nil {
		t.Fatal(err)
	}
}

// TestLeaderWindowSubmitterSurvivesDurableWriteFailure covers the runtime that
// has no voter to report the write to — the standalone and delegated collator
// composition builds its session as an observer, so this producer store is the
// only durable write of its own candidate wire there is. Its failure is logged
// rather than returned: the bytes were already broadcast, the collator marker
// that binds this slot to this candidate is already durable, and retrying the
// window does not repair a disk. That is the reference behaviour, which detaches
// this store outright.
func TestLeaderWindowSubmitterSurvivesDurableWriteFailure(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	saveErr := errors.New("no space left on device")
	storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
		done(saveErr)
	}
	config, privateKey := runtimeTestConfig(0x58, &runtimeTestJournal{})
	if config.Identity.Validator != nil {
		t.Fatal("this test needs the observer composition, which has no voter")
	}
	var logOutput bytes.Buffer
	logger := zerolog.New(&logOutput)
	session, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: backend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
		Logger:  &logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := session.(*sessionRuntime)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	<-network.started
	var window LeaderWindow
	select {
	case window = <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("leader window was not delivered")
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, window.Window.StartSlot, window.Window.Base)
	if err = window.Submit(context.Background(), artifact); err != nil {
		t.Fatalf("a failed durable write stopped the producer: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want 1", storage.saveCount())
	}
	if !strings.Contains(logOutput.String(), saveErr.Error()) {
		t.Fatalf("the failed candidate write was not logged: %s", logOutput.String())
	}

	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-runResult; err != nil {
		t.Fatal(err)
	}
}

// The retention gauges have to describe now, not the last finalization. A
// session that has stopped finalizing keeps accepting candidates and keeps
// walking a lineage per leader window, so both numbers move while it is stuck —
// and a projection refreshed only by OnFinalized holds its pre-standstill
// values for exactly the interval the gauges were added to explain.
func TestConsensusStatsReportRetentionWithoutAFinalization(t *testing.T) {
	runtime, privateKey := prepareRuntimeTest(
		t, 0x73, newRuntimeTestStorage(), newRuntimeTestNetwork(), newRuntimeTestBackend())
	defer runtime.Close()

	if stats := runtime.ConsensusStats(); stats.Retention.RetainedPayloads != 0 ||
		stats.Retention.AnchorKnown {
		t.Fatalf("a session that has done nothing reports %+v", stats.Retention)
	}

	parent := simplex.Genesis()
	var bytesStaged int64
	for slot := range uint32(3) {
		artifact := runtimeBlockArtifact(t, runtime.config, privateKey, slot, parent, slot+1, uint64(slot))
		wire, _, err := runtime.candidates.codec.encodeForBroadcast(artifact)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime.candidates.stage(artifact, wire); err != nil {
			t.Fatal(err)
		}
		bytesStaged += candidatePayloadBytes(artifact, wire)
		parent = simplex.Parent(artifact.Candidate.ID)
	}
	// A leader window walked back to slot 5 while the session was not
	// finalizing, which is what a stalled session spends its time doing.
	runtime.states.mu.Lock()
	runtime.states.noteLineageFloorLocked(5)
	runtime.states.mu.Unlock()

	stats := runtime.ConsensusStats()
	if stats.Retention.RetainedPayloads != 3 {
		t.Fatalf("retained payloads = %d, want the 3 staged without a finalization",
			stats.Retention.RetainedPayloads)
	}
	if stats.Retention.RetainedBytes != bytesStaged {
		t.Fatalf("retained bytes = %d, want %d", stats.Retention.RetainedBytes, bytesStaged)
	}
	if !stats.Retention.AnchorKnown || stats.Retention.AnchorSlot != 5 {
		t.Fatalf("lineage anchor = %d (known=%v), want 5",
			stats.Retention.AnchorSlot, stats.Retention.AnchorKnown)
	}
	if stats.Retention.BudgetBytes != retentionFloorCapBytes {
		t.Fatalf("retention budget = %d, want the shard budget %d",
			stats.Retention.BudgetBytes, retentionFloorCapBytes)
	}
}
