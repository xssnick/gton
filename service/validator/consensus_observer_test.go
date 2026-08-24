package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	corestorage "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type observerTestJournal struct {
	mu sync.Mutex

	bootstrapErrors []error
	bootstrapCalls  int
}

func (j *observerTestJournal) Bootstrap() (*simplex.BootstrapState, error) {
	j.mu.Lock()
	call := j.bootstrapCalls
	j.bootstrapCalls++
	var err error
	if call < len(j.bootstrapErrors) {
		err = j.bootstrapErrors[call]
	}
	j.mu.Unlock()
	if err != nil {
		return nil, err
	}

	return &simplex.BootstrapState{}, nil
}

func (*observerTestJournal) SaveOurVote(_ simplex.Vote, done func(error)) { done(nil) }

func (*observerTestJournal) SaveCertificate(_ *simplex.Certificate, done func(error)) {
	done(nil)
}

func (*observerTestJournal) SaveFirstNonAnnouncedWindow(_ uint32, done func(error)) {
	done(nil)
}

type observerTestStorage struct {
	*runtimeTestStorage
	journal simplex.Journal

	mu       sync.Mutex
	sessions []SessionStorageID
	deleted  []SessionStorageID
}

func (s *observerTestStorage) Journal(SessionStorageID, int) simplex.Journal {
	return s.journal
}

func (s *observerTestStorage) Sessions(ctx context.Context) ([]SessionStorageID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]SessionStorageID(nil), s.sessions...), nil
}

func (s *observerTestStorage) DeleteSession(ctx context.Context, id SessionStorageID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.deleted = append(s.deleted, id)
	for i := range s.sessions {
		if s.sessions[i] == id {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	return nil
}

func (s *observerTestStorage) deletes() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.deleted)
}

type observerTestSessionNetwork struct {
	mu sync.Mutex

	receiver      SessionReceiver
	startErrors   []error
	startCalls    int
	runCalls      int
	runFailures   chan error
	broadcasts    []simplex.CandidateBroadcast
	consensusAll  int
	consensusSome int
}

func newObserverTestSessionNetwork() *observerTestSessionNetwork {
	return &observerTestSessionNetwork{runFailures: make(chan error, 4)}
}

func (n *observerTestSessionNetwork) BroadcastToAll([]byte) {
	n.mu.Lock()
	n.consensusAll++
	n.mu.Unlock()
}

func (n *observerTestSessionNetwork) BroadcastToValidators([]byte) {
	n.mu.Lock()
	n.consensusAll++
	n.mu.Unlock()
}

func (n *observerTestSessionNetwork) BroadcastToRandom(_ uint32, _ []byte) {
	n.mu.Lock()
	n.consensusSome++
	n.mu.Unlock()
}

func (n *observerTestSessionNetwork) BroadcastCandidate(
	ctx context.Context,
	broadcast simplex.CandidateBroadcast,
	_ CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	n.broadcasts = append(n.broadcasts, broadcast)
	n.mu.Unlock()

	return nil
}

func (*observerTestSessionNetwork) RequestCandidate(
	context.Context,
	CandidateRequest,
) (CandidateResponse, error) {
	return CandidateResponse{}, ErrCandidateUnavailable
}

func (n *observerTestSessionNetwork) Start(ctx context.Context, receiver SessionReceiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	call := n.startCalls
	n.startCalls++
	var err error
	if call < len(n.startErrors) {
		err = n.startErrors[call]
	}
	if err == nil {
		n.receiver = receiver
	}
	n.mu.Unlock()

	return err
}

func (n *observerTestSessionNetwork) Run(ctx context.Context) error {
	n.mu.Lock()
	n.runCalls++
	n.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil
	case err := <-n.runFailures:
		return err
	}
}

func (n *observerTestSessionNetwork) receiverSnapshot() SessionReceiver {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.receiver
}

func (n *observerTestSessionNetwork) counts() (start, run, broadcasts int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.startCalls, n.runCalls, len(n.broadcasts)
}

func (n *observerTestSessionNetwork) lastBroadcast() simplex.CandidateBroadcast {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.broadcasts[len(n.broadcasts)-1]
}

type observerTestNetwork struct {
	local   [32]byte
	session *observerTestSessionNetwork

	mu           sync.Mutex
	startErr     error
	closeErrors  []error
	update       func(context.Context, collator.OverlaySession) error
	prepareCalls int
	updateCalls  int
	retireCalls  int
	closeCalls   int
}

func (n *observerTestNetwork) LocalADNLID() [32]byte { return n.local }

func (n *observerTestNetwork) Start(ctx context.Context, _ collator.RemoteHandlers) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	err := n.startErr
	n.mu.Unlock()

	return err
}

func (n *observerTestNetwork) PrepareSession(
	ctx context.Context,
	_ collator.OverlaySession,
) (SessionNetwork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	n.mu.Lock()
	n.prepareCalls++
	n.mu.Unlock()

	return n.session, nil
}

func (n *observerTestNetwork) UpdateSession(
	ctx context.Context,
	session collator.OverlaySession,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	n.updateCalls++
	update := n.update
	n.mu.Unlock()
	if update != nil {
		return update(ctx, session)
	}

	return nil
}

func (n *observerTestNetwork) RetireSession(ctx context.Context, _ [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	n.retireCalls++
	n.mu.Unlock()

	return nil
}

func (n *observerTestNetwork) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	call := n.closeCalls
	n.closeCalls++
	var err error
	if call < len(n.closeErrors) {
		err = n.closeErrors[call]
	}
	n.mu.Unlock()

	return err
}

func (n *observerTestNetwork) counts() (prepare, retire, close int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.prepareCalls, n.retireCalls, n.closeCalls
}

type observerFixture struct {
	observer     *ConsensusObserver
	network      *observerTestNetwork
	sessionNet   *observerTestSessionNetwork
	storage      *observerTestStorage
	descriptor   collator.ConsensusObserverSession
	activation   collator.SessionActivation
	validatorKey ed25519.PrivateKey
	collatorKey  ed25519.PrivateKey
	progress     chan collator.ConsensusProgress
}

type observerUpdateTestRuntime struct {
	update func(context.Context, SessionState) error
}

func (*observerUpdateTestRuntime) Recover(context.Context, SessionStart) error { return nil }

func (*observerUpdateTestRuntime) Run(context.Context, SessionStart) error { return nil }

func (r *observerUpdateTestRuntime) Update(ctx context.Context, state SessionState) error {
	return r.update(ctx, state)
}

func (*observerUpdateTestRuntime) Close() error  { return nil }
func (*observerUpdateTestRuntime) Retire() error { return nil }

func (*observerUpdateTestRuntime) runWithStartup(
	context.Context,
	SessionStart,
	chan<- error,
) error {
	return nil
}

func (*observerUpdateTestRuntime) publishCandidate(context.Context, *CandidateArtifact) error {
	return nil
}

func (*observerUpdateTestRuntime) fail(error) {}

func newObserverFixture(
	t *testing.T,
	journal simplex.Journal,
	progressed func(context.Context, collator.ConsensusProgress) error,
) *observerFixture {
	t.Helper()

	validatorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x41}, ed25519.SeedSize))
	collatorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x51}, ed25519.SeedSize))
	validatorPublic := validatorKey.Public().(ed25519.PublicKey)
	collatorPublic := collatorKey.Public().(ed25519.PublicKey)
	var validatorPublicArray [ed25519.PublicKeySize]byte
	copy(validatorPublicArray[:], validatorPublic)
	validatorADNL := simplex.KeyNodeIDShort(validatorPublic)
	localADNL := simplex.KeyNodeIDShort(collatorPublic)
	validator := groups.Validator{
		PublicKey: validatorPublicArray,
		ADNL:      validatorADNL,
		Weight:    7,
	}
	validatorHash, err := groups.PublicKeyHash(validator.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	validator.PublicKeyHash = validatorHash
	catchainSeqno := uint32(19)
	validatorSetHash, err := groups.ValidatorSetHash(groups.ValidatorSetHashInput{
		CatchainSeqno: catchainSeqno,
		Validators:    []groups.Validator{validator},
	})
	if err != nil {
		t.Fatal(err)
	}

	zeroState := cell.BeginCell().MustStoreUInt(0x71, 8).EndCell()
	zeroBOC := zeroState.ToBOC()
	zeroFileHash := sha256.Sum256(zeroBOC)
	genesis := ton.BlockIDExt{
		Workchain: 0,
		Shard:     -1 << 63,
		SeqNo:     0,
		RootHash:  zeroState.Hash(),
		FileHash:  zeroFileHash[:],
	}
	masterchain := ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     100,
		RootHash:  bytes.Repeat([]byte{0x61}, 32),
		FileHash:  bytes.Repeat([]byte{0x62}, 32),
	}
	sessionID := [32]byte{0x81}
	params := simplex.DefaultParams()
	descriptor := collator.ConsensusObserverSession{
		Overlay: collator.OverlaySession{
			Session: collator.Session{
				ID:                   sessionID,
				Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
				CatchainSeqno:        catchainSeqno,
				ValidatorSetHash:     validatorSetHash,
				ConsensusVersion:     2,
				ProtocolVersion:      3,
				SlotsPerLeaderWindow: 2,
				Validators: []collator.SessionValidator{{
					PublicKey: validatorPublicArray,
					ADNLID:    validatorADNL,
					Weight:    validator.Weight,
				}},
			},
			Role: collator.OverlayRoleCollator,
			CollatorsByValidator: []groups.CollatorRegistryEntry{{
				ValidatorKeyID:  validatorHash,
				CollatorADNLIDs: [][32]byte{localADNL},
			}},
			AllCollators:              [][32]byte{localADNL},
			AllCurrentValidators:      [][32]byte{validatorADNL},
			AllOverlayNodes:           [][32]byte{validatorADNL, localADNL},
			MaxBlockSize:              1 << 20,
			MaxCollatedDataSize:       1 << 20,
			BroadcastMode:             collator.CandidateBroadcastPrivateOverlay,
			ObserversInPrivateOverlay: true,
		},
		Update: collator.SessionUpdate{
			SessionID:        sessionID,
			TargetRate:       params.TargetRate,
			MasterchainBlock: masterchain,
		},
		Params: params,
	}
	activation := collator.SessionActivation{
		SessionID:      sessionID,
		Genesis:        []ton.BlockIDExt{genesis},
		MinMasterchain: masterchain,
	}
	if journal == nil {
		journal = &observerTestJournal{}
	}
	storage := &observerTestStorage{runtimeTestStorage: newRuntimeTestStorage(), journal: journal}
	node := &localBackendTestNode{
		states: map[string]*corestorage.BlockState{
			localBackendTestBlockKey(genesis): {
				Block:         genesis,
				StateRootHash: zeroState.Hash(),
				StateFileHash: zeroFileHash[:],
				Cell:          zeroState,
			},
		},
		stateCells: make(map[string]*cell.Cell),
		blocks:     make(map[string][]byte),
	}
	sessionNet := newObserverTestSessionNetwork()
	network := &observerTestNetwork{local: localADNL, session: sessionNet}
	observer, err := NewConsensusObserver(ConsensusObserverOptions{
		Network: network,
		Storage: storage,
		Node:    node,
		Groups:  localBackendTestGroups{snapshot: &groups.Snapshot{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	progress := make(chan collator.ConsensusProgress, 16)
	if progressed == nil {
		progressed = func(_ context.Context, value collator.ConsensusProgress) error {
			progress <- value

			return nil
		}
	}
	if err = observer.Start(context.Background(), collator.RemoteHandlers{
		Probe: func(context.Context, collator.AuthenticatedQuery, simplex.ConsensusPleaseCollatePrepare) error {
			return nil
		},
		Commit: func(context.Context, collator.AuthenticatedQuery, simplex.ConsensusPleaseCollate) error {
			return nil
		},
	}, collator.ConsensusObserverEvents{
		Progressed: progressed,
		Finalized:  func(context.Context, collator.ConsensusFinalization) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	return &observerFixture{
		observer:     observer,
		network:      network,
		sessionNet:   sessionNet,
		storage:      storage,
		descriptor:   descriptor,
		activation:   activation,
		validatorKey: validatorKey,
		collatorKey:  collatorKey,
		progress:     progress,
	}
}

func TestConsensusObserverRuntimeInputProtocolRoleMatrix(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	t.Cleanup(func() {
		if err := fixture.observer.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	for _, test := range []struct {
		name             string
		protocolVersion  uint8
		role             collator.OverlayRole
		broadcastMode    collator.CandidateBroadcastMode
		privateObservers bool
		want             bool
	}{
		{name: "protocol zero observer", protocolVersion: 0, role: collator.OverlayRoleObserver,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay},
		{name: "protocol zero collator", protocolVersion: 0, role: collator.OverlayRoleCollator,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay},
		{name: "protocol one observer", protocolVersion: 1, role: collator.OverlayRoleObserver,
			broadcastMode: collator.CandidateBroadcastBlockSyncOverlay, want: true},
		{name: "protocol one collator", protocolVersion: 1, role: collator.OverlayRoleCollator,
			broadcastMode: collator.CandidateBroadcastBlockSyncOverlay, want: true},
		{name: "protocol two observer", protocolVersion: 2, role: collator.OverlayRoleObserver,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay, privateObservers: true, want: true},
		{name: "protocol two collator", protocolVersion: 2, role: collator.OverlayRoleCollator,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay, privateObservers: true, want: true},
		{name: "protocol three observer", protocolVersion: 3, role: collator.OverlayRoleObserver,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay, privateObservers: true, want: true},
		{name: "protocol three collator", protocolVersion: 3, role: collator.OverlayRoleCollator,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay, privateObservers: true, want: true},
		{name: "unrepresentable protocol", protocolVersion: simplex.MaxProtocolVersion + 1,
			role: collator.OverlayRoleObserver, broadcastMode: collator.CandidateBroadcastPrivateOverlay},
		{name: "protocol one wrong route", protocolVersion: 1, role: collator.OverlayRoleObserver,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay},
		{name: "protocol two wrong observer policy", protocolVersion: 2, role: collator.OverlayRoleObserver,
			broadcastMode: collator.CandidateBroadcastPrivateOverlay},
	} {
		t.Run(test.name, func(t *testing.T) {
			descriptor := fixture.descriptor
			descriptor.Overlay.Session.ProtocolVersion = test.protocolVersion
			descriptor.Overlay.Role = test.role
			if test.role == collator.OverlayRoleObserver {
				descriptor.Overlay.CollatorsByValidator = nil
				descriptor.Overlay.AllCollators = nil
			}
			descriptor.Overlay.BroadcastMode = test.broadcastMode
			descriptor.Overlay.ObserversInPrivateOverlay = test.privateObservers
			input, err := fixture.observer.runtimeInput(descriptor)
			if (err == nil) != test.want {
				t.Fatalf("runtimeInput error = %v, want success %v", err, test.want)
			}
			if err == nil && !slices.Equal(input.config.AllCollators, descriptor.Overlay.AllCollators) {
				t.Fatalf("all-collator roster = %x, want %x",
					input.config.AllCollators, descriptor.Overlay.AllCollators)
			}
		})
	}
}

func TestConsensusObserverUsesBlockSyncOnlyRuntimeForProtocolOneObserver(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	descriptor := fixture.descriptor
	descriptor.Overlay.Session.ProtocolVersion = 1
	descriptor.Overlay.Role = collator.OverlayRoleObserver
	descriptor.Overlay.CollatorsByValidator = nil
	descriptor.Overlay.AllCollators = nil
	descriptor.Overlay.BroadcastMode = collator.CandidateBroadcastBlockSyncOverlay
	descriptor.Overlay.ObserversInPrivateOverlay = false

	if err := fixture.observer.PrepareSession(context.Background(), descriptor); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(descriptor.Overlay.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	runtime := session.runtime
	_, lightweight := runtime.(*blockSyncObserverRuntime)
	session.mu.Unlock()
	if !lightweight {
		t.Fatalf("protocol-1 observer runtime = %T, want block-sync-only", runtime)
	}
	if err = fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	if _, _, broadcasts := fixture.sessionNet.counts(); broadcasts != 0 {
		t.Fatalf("block-sync observer originated %d candidate broadcasts", broadcasts)
	}
	if err = fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverUpdateReleasesSessionLockForRuntimeProgress(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	t.Cleanup(func() {
		if err := fixture.observer.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.descriptor.Overlay.Session.ID)
	if err != nil {
		t.Fatal(err)
	}

	updateEntered := make(chan struct{})
	releaseUpdate := make(chan struct{})
	networkEntered := make(chan struct{})
	releaseNetwork := make(chan struct{})
	var releaseUpdateOnce, releaseNetworkOnce sync.Once
	releaseRuntimeUpdate := func() { releaseUpdateOnce.Do(func() { close(releaseUpdate) }) }
	releaseNetworkUpdate := func() { releaseNetworkOnce.Do(func() { close(releaseNetwork) }) }
	t.Cleanup(releaseRuntimeUpdate)
	t.Cleanup(releaseNetworkUpdate)
	fixture.network.mu.Lock()
	fixture.network.update = func(ctx context.Context, _ collator.OverlaySession) error {
		close(networkEntered)
		select {
		case <-releaseNetwork:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fixture.network.mu.Unlock()
	runtime := &observerUpdateTestRuntime{
		update: func(ctx context.Context, _ SessionState) error {
			close(updateEntered)
			select {
			case <-releaseUpdate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	session.mu.Lock()
	original := session.runtime
	session.runtime = runtime
	session.phase = observerSessionStarting
	session.mu.Unlock()
	if err = original.Close(); err != nil {
		t.Fatal(err)
	}

	updated := fixture.descriptor
	updated.Params.TargetRate += time.Millisecond
	updated.Update.TargetRate = updated.Params.TargetRate
	updated.Overlay.AllOverlayNodes = slices.Clone(updated.Overlay.AllOverlayNodes)
	updated.Overlay.AllOverlayNodes[0], updated.Overlay.AllOverlayNodes[1] =
		updated.Overlay.AllOverlayNodes[1], updated.Overlay.AllOverlayNodes[0]
	updateResult := make(chan error, 1)
	go func() {
		updateResult <- fixture.observer.UpdateSession(context.Background(), updated)
	}()
	select {
	case <-networkEntered:
	case <-time.After(time.Second):
		t.Fatal("network update did not start")
	}

	observeProgress := func(slot uint32, stage string) {
		t.Helper()

		progressResult := make(chan error, 1)
		go func() {
			progressResult <- fixture.observer.handleProgress(
				context.Background(),
				session,
				sessionConsensusProgress{Window: simplex.Window{StartSlot: slot}},
			)
		}()
		select {
		case progressErr := <-progressResult:
			if progressErr != nil {
				t.Fatalf("progress during %s update: %v", stage, progressErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("progress deadlocked behind the %s update", stage)
		}
	}
	observeProgress(43, "network")
	releaseNetworkUpdate()
	select {
	case <-updateEntered:
	case <-time.After(time.Second):
		releaseRuntimeUpdate()
		<-updateResult
		t.Fatal("runtime update did not start after the network update")
	}
	observeProgress(44, "runtime")

	select {
	case err = <-updateResult:
		t.Fatalf("session update returned before the runtime commit was released: %v", err)
	default:
	}
	releaseRuntimeUpdate()
	select {
	case err = <-updateResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("session update did not finish after the runtime commit")
	}

	session.mu.Lock()
	pending := session.pending
	committed := session.descriptor
	session.mu.Unlock()
	if pending == nil || pending.Window.StartSlot != 44 {
		t.Fatalf("concurrent progress = %+v, want pending window 44", pending)
	}
	if !committed.Equal(updated) {
		t.Fatal("session descriptor was not committed after the runtime update")
	}
}

func TestConsensusObserverRecoversDesiredAndStaleNamespaces(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	input, err := fixture.observer.runtimeInput(fixture.descriptor)
	if err != nil {
		t.Fatal(err)
	}
	desired := input.config.StorageID
	stale := desired
	stale.SessionID[0] ^= 0x7f
	validatorSession := desired
	validatorSession.SessionID[0] ^= 0x3f
	validatorSession.IsValidator = true
	validatorSession.ValidatorKeyID = [32]byte{0x91}
	validatorSession.ValidatorIndex = 0
	foreignObserver := desired
	foreignObserver.SessionID[0] ^= 0x1f
	foreignObserver.LocalADNLID[0] ^= 0xff
	fixture.storage.sessions = []SessionStorageID{
		foreignObserver,
		stale,
		validatorSession,
		desired,
	}

	recovered, err := fixture.observer.RecoverSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 2 || recovered[0] != desired.SessionID || recovered[1] != stale.SessionID {
		t.Fatalf("recovered sessions = %x, want [%x %x]", recovered, desired.SessionID, stale.SessionID)
	}
	replayed, err := fixture.observer.RecoverSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != len(recovered) || replayed[0] != recovered[0] || replayed[1] != recovered[1] {
		t.Fatalf("idempotent recovery = %x, want %x", replayed, recovered)
	}

	if err = fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatalf("prepare recovered desired session: %v", err)
	}
	session, err := fixture.observer.session(desired.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	storageID := session.config.StorageID
	phase := session.phase
	session.mu.Unlock()
	if storageID != desired || phase != observerSessionPrepared {
		t.Fatalf("reopened session = storage:%+v phase:%d", storageID, phase)
	}

	if err = fixture.observer.RetireSession(context.Background(), stale.SessionID); err != nil {
		t.Fatalf("retire stale recovered session: %v", err)
	}
	if err = fixture.observer.RetireSession(context.Background(), desired.SessionID); err != nil {
		t.Fatalf("retire desired recovered session: %v", err)
	}
	prepare, retire, _ := fixture.network.counts()
	if prepare != 1 || retire != 2 || fixture.storage.deletes() != 2 {
		t.Fatalf("recovery lifecycle = prepare:%d retire:%d delete:%d", prepare, retire, fixture.storage.deletes())
	}
	if err = fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverRequiresRecoveryBeforePreparation(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.observer.RecoverSessions(context.Background()); err == nil {
		t.Fatal("recovery after session preparation succeeded")
	}
	if err := fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverPrepareActivateAndAuthenticatedProgress(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	preparedRuntime := session.runtime
	session.mu.Unlock()
	if err = fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if prepare, _, _ := fixture.network.counts(); prepare != 1 {
		t.Fatalf("overlay prepare calls = %d, want 1", prepare)
	}
	if err = fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	activeRuntime := session.runtime
	activeConfig := session.config
	session.mu.Unlock()
	if activeRuntime != preparedRuntime {
		t.Fatal("activation replaced the future session runtime")
	}
	initial := waitObserverProgress(t, fixture.progress, 0)
	if initial.Window.Base != simplex.Genesis() || initial.Base != nil || initial.StartAt.IsZero() {
		t.Fatalf("initial progress = %+v", initial)
	}

	receiver := fixture.sessionNet.receiverSnapshot()
	if receiver == nil {
		t.Fatal("session network did not bind its receiver")
	}
	forged := observerCertificate(
		fixture.activation.SessionID,
		fixture.collatorKey,
		simplex.SkipVote(0),
	)
	receiver.ReceiveConsensusMessage(simplex.PeerID{0xf1}, 0, forged)
	select {
	case progress := <-fixture.progress:
		t.Fatalf("forged certificate advanced progress: %+v", progress)
	case <-time.After(30 * time.Millisecond):
	}
	receiver.ReceiveConsensusMessage(simplex.PeerID{0xa1}, 0, observerCertificate(
		fixture.activation.SessionID,
		fixture.validatorKey,
		simplex.SkipVote(0),
	))
	receiver.ReceiveConsensusMessage(simplex.PeerID{0xa2}, 0, observerCertificate(
		fixture.activation.SessionID,
		fixture.validatorKey,
		simplex.SkipVote(1),
	))
	advanced := waitObserverProgress(t, fixture.progress, 2)
	if advanced.Window.Base != simplex.Genesis() || advanced.Base != nil {
		t.Fatalf("authenticated skip progress = %+v", advanced)
	}

	artifact := runtimeOrdinaryArtifact(
		t,
		activeConfig,
		fixture.validatorKey,
		advanced.Window.StartSlot,
		advanced.Window.Base,
	)
	collatorPublic := fixture.collatorKey.Public().(ed25519.PublicKey)
	artifact.Candidate.Delegation = &simplex.Delegation{CollatorKey: collatorPublic}
	artifact.Candidate.Delegation.Signature, err = simplex.SignDelegation(
		runtimeTestSigner{key: fixture.validatorKey},
		fixture.activation.SessionID,
		advanced.Window.StartSlot,
		simplex.KeyNodeIDShort(collatorPublic),
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact.Candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: fixture.collatorKey},
		fixture.activation.SessionID,
		artifact.Candidate.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.observer.BroadcastCandidate(context.Background(), collator.CandidateArtifact{
		SessionID: fixture.activation.SessionID,
		WindowID: collator.WindowID{
			SessionID: fixture.activation.SessionID,
			StartSlot: advanced.Window.StartSlot,
		},
		Candidate:    artifact.Candidate,
		BlockBOC:     artifact.BlockBOC,
		CollatedData: artifact.CollatedData,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, broadcasts := fixture.sessionNet.counts(); broadcasts != 1 {
		t.Fatalf("candidate broadcasts = %d, want 1", broadcasts)
	}
	extra, err := simplex.ParseBroadcastExtra(fixture.sessionNet.lastBroadcast().Extra)
	if err != nil {
		t.Fatal(err)
	}
	if extra.Delegation == nil || extra.Slot != artifact.Candidate.ID.Slot || fixture.storage.saveCount() != 1 {
		t.Fatalf("delegated candidate was not broadcast and stored: extra=%+v saves=%d", extra, fixture.storage.saveCount())
	}
	// The delegated route reaches its payload through the capsule the collator
	// built, the self route through the same capsule, and a replay through the
	// BOCs. All three must put the same bytes on the overlay, so pin the emitted
	// payload against the route-independent serialization of the same inputs.
	expected, err := simplex.SerializeCandidateForBroadcast(
		artifact.Candidate,
		artifact.BlockBOC,
		artifact.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fixture.sessionNet.lastBroadcast().Data, expected.Data) ||
		!bytes.Equal(fixture.sessionNet.lastBroadcast().Extra, expected.Extra) {
		t.Fatal("delegated broadcast bytes differ from the candidate serialization")
	}

	retireCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = fixture.observer.RetireSession(retireCtx, fixture.activation.SessionID); err != nil {
		t.Fatal(err)
	}
	prepare, retire, _ := fixture.network.counts()
	if prepare != 1 || retire != 1 || fixture.storage.deletes() != 1 {
		t.Fatalf("retirement counts = prepare:%d retire:%d delete:%d", prepare, retire, fixture.storage.deletes())
	}
	if err = fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverRetriesSynchronousStartupFailures(t *testing.T) {
	t.Run("network start", func(t *testing.T) {
		startErr := errors.New("bind private overlay")
		fixture := newObserverFixture(t, nil, nil)
		fixture.sessionNet.startErrors = []error{startErr}
		if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
			t.Fatal(err)
		}
		if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); !errors.Is(err, startErr) {
			t.Fatalf("first activation error = %v, want %v", err, startErr)
		}
		if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
			t.Fatalf("activation retry: %v", err)
		}
		start, run, _ := fixture.sessionNet.counts()
		prepare, _, _ := fixture.network.counts()
		if prepare != 1 || start != 2 || run != 1 {
			t.Fatalf("retry counts = prepare:%d start:%d run:%d", prepare, start, run)
		}
		if err := fixture.observer.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("engine start", func(t *testing.T) {
		engineErr := errors.New("journal recovery failed")
		journal := &observerTestJournal{bootstrapErrors: []error{nil, engineErr}}
		fixture := newObserverFixture(t, journal, nil)
		if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
			t.Fatal(err)
		}
		if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); !errors.Is(err, engineErr) {
			t.Fatalf("first activation error = %v, want %v", err, engineErr)
		}
		if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
			t.Fatalf("activation retry: %v", err)
		}
		if prepare, _, _ := fixture.network.counts(); prepare != 1 {
			t.Fatalf("overlay was prepared %d times, want once", prepare)
		}
		if err := fixture.observer.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsensusObserverRunFailureAfterReadinessIsTerminal(t *testing.T) {
	runErr := errors.New("private overlay receiver failed")
	fixture := newObserverFixture(t, nil, nil)
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	fixture.sessionNet.runFailures <- runErr
	waitObserverPhase(t, fixture.observer, fixture.activation.SessionID, observerSessionFailed)
	if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); !errors.Is(err, runErr) {
		t.Fatalf("terminal activation error = %v, want %v", err, runErr)
	}
	if err := fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverRetireCancelsBlockedProgress(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	fixture := newObserverFixture(t, nil, func(ctx context.Context, _ collator.ConsensusProgress) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()

		return ctx.Err()
	})
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("progress callback did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := fixture.observer.RetireSession(ctx, fixture.activation.SessionID); err != nil {
		t.Fatalf("retire with blocked progress: %v", err)
	}
	if err := fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverRuntimeFailureReleasesStartupFlushWorkers(t *testing.T) {
	// Stands in for the collator controller mutex: reconcileSession holds it
	// across retirement, so a startup flush worker parked on it can only be
	// released by cancelling the run context the worker was launched with.
	controller := make(chan struct{}, 1)
	parked := make(chan struct{})
	fixture := newObserverFixture(t, nil, func(ctx context.Context, progress collator.ConsensusProgress) error {
		if progress.Window.StartSlot != 99 {
			return nil
		}
		close(parked)
		select {
		case controller <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	controller <- struct{}{}

	// Replays the activation launch site: the flush workers run on a context
	// whose only handle is the cancel function handed to watchRuntime, so a
	// runtime that dies on its own must release it there.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	watchDone := make(chan struct{})
	session.mu.Lock()
	runtime := session.runtime
	session.runCancel = cancel
	session.watchDone = watchDone
	session.progressWG.Add(1)
	fixture.observer.workersWG.Add(2)
	session.mu.Unlock()
	go fixture.observer.watchRuntime(session, runtime, cancel, returned, watchDone)
	go fixture.observer.flushProgress(runCtx, session, runtime, collator.ConsensusProgress{
		SessionID: fixture.activation.SessionID,
		Window:    simplex.Window{StartSlot: 99},
	})
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("startup flush worker did not park on the controller mutex")
	}
	returned <- errors.New("session runtime stopped")
	// Retirement must observe the terminated run, exactly as the collator does:
	// by then the session no longer holds a handle to the run context.
	waitObserverPhase(t, fixture.observer, fixture.activation.SessionID, observerSessionFailed)

	ctx, cancelRetire := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRetire()
	if err = fixture.observer.RetireSession(ctx, fixture.activation.SessionID); err != nil {
		t.Fatalf("retire after runtime failure: %v", err)
	}
	if err = fixture.observer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusObserverCloseHonorsContextAndCanRetry(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	fixture := newObserverFixture(t, nil, func(_ context.Context, progress collator.ConsensusProgress) error {
		if progress.Window.StartSlot != 99 {
			return nil
		}
		close(entered)
		<-release

		return nil
	})
	if err := fixture.observer.PrepareSession(context.Background(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if err := fixture.observer.ActivateSession(context.Background(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	runtime := session.runtime
	session.progressWG.Add(1)
	fixture.observer.workersWG.Add(1)
	session.mu.Unlock()
	go fixture.observer.flushProgress(context.Background(), session, runtime, collator.ConsensusProgress{
		SessionID: fixture.activation.SessionID,
		Window:    simplex.Window{StartSlot: 99},
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("test progress worker did not block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	err = fixture.observer.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want deadline", err)
	}
	close(release)
	if err = fixture.observer.Close(context.Background()); err != nil {
		t.Fatalf("close retry: %v", err)
	}
	if _, _, closeCalls := fixture.network.counts(); closeCalls != 1 {
		t.Fatalf("network close calls = %d, want 1 after successful retry", closeCalls)
	}
}

func TestConsensusObserverStartCleanupCanRetry(t *testing.T) {
	startErr := errors.New("open delegated endpoint")
	cleanupErr := errors.New("join delegated endpoint")
	network := &observerTestNetwork{
		local:       [32]byte{0x71},
		session:     newObserverTestSessionNetwork(),
		startErr:    startErr,
		closeErrors: []error{cleanupErr},
	}
	observer, err := NewConsensusObserver(ConsensusObserverOptions{
		Network: network,
		Storage: &observerTestStorage{
			runtimeTestStorage: newRuntimeTestStorage(),
			journal:            &observerTestJournal{},
		},
		Node:   &localBackendTestNode{},
		Groups: localBackendTestGroups{snapshot: &groups.Snapshot{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = observer.Start(context.Background(), collator.RemoteHandlers{
		Probe: func(context.Context, collator.AuthenticatedQuery, simplex.ConsensusPleaseCollatePrepare) error {
			return nil
		},
		Commit: func(context.Context, collator.AuthenticatedQuery, simplex.ConsensusPleaseCollate) error {
			return nil
		},
	}, collator.ConsensusObserverEvents{
		Progressed: func(context.Context, collator.ConsensusProgress) error { return nil },
		Finalized:  func(context.Context, collator.ConsensusFinalization) error { return nil },
	})
	if !errors.Is(err, startErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("start error = %v, want joined start and cleanup errors", err)
	}
	observer.mu.RLock()
	state := observer.state
	observer.mu.RUnlock()
	if state != consensusObserverClosing {
		t.Fatalf("observer state = %d, want retryable closing", state)
	}
	if err = observer.Close(context.Background()); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if _, _, closeCalls := network.counts(); closeCalls != 2 {
		t.Fatalf("network close calls = %d, want 2", closeCalls)
	}
}

func observerCertificate(
	sessionID [32]byte,
	key ed25519.PrivateKey,
	vote simplex.Vote,
) []byte {
	signature := ed25519.Sign(key, simplex.DataToSign(sessionID, simplex.VoteBytes(vote)))
	return (&simplex.Certificate{
		Vote: vote,
		Signatures: []simplex.VoteSignature{{
			ValidatorIndex: 0,
			Signature:      signature,
		}},
	}).Serialize()
}

func waitObserverProgress(
	t *testing.T,
	progress <-chan collator.ConsensusProgress,
	startSlot uint32,
) collator.ConsensusProgress {
	t.Helper()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case value := <-progress:
			if value.Window.StartSlot == startSlot {
				return value
			}
		case <-timer.C:
			t.Fatalf("consensus progress for window %d did not arrive", startSlot)
		}
	}
}

func waitObserverPhase(
	t *testing.T,
	observer *ConsensusObserver,
	id [32]byte,
	want observerSessionPhase,
) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		observer.mu.RLock()
		session := observer.sessions[id]
		observer.mu.RUnlock()
		if session != nil {
			session.mu.Lock()
			phase := session.phase
			session.mu.Unlock()
			if phase == want {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("observer session did not reach phase %d", want)
}
