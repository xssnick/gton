package collator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

func controllerTestMessagePool(t *testing.T) *msgpool.Pool {
	t.Helper()

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)

	return pool
}

func TestReconcileSessionUpdatePreservesFinalizedObservationAndNewerWindow(t *testing.T) {
	current := SessionUpdate{
		SessionID:                 [32]byte{0x11},
		TargetRate:                200 * time.Millisecond,
		MasterchainBlock:          runtimeTestBlockID(-1, -1<<63, 10),
		HasFinalizedBlock:         true,
		FinalizedBlock:            runtimeTestBlockID(0, 1<<62, 7),
		HasCurrentWindow:          true,
		CurrentWindowStart:        4,
		CurrentWindowObservedSlot: 4,
		CurrentWindowStartAt:      time.Unix(100, 0),
	}
	next := SessionUpdate{
		SessionID:                 current.SessionID,
		TargetRate:                current.TargetRate,
		MasterchainBlock:          runtimeTestBlockID(-1, -1<<63, 11),
		HasCurrentWindow:          true,
		CurrentWindowStart:        8,
		CurrentWindowObservedSlot: 8,
		CurrentWindowStartAt:      time.Unix(200, 0),
	}

	got, shouldUpdate, err := reconcileSessionUpdate(current, next)
	if err != nil {
		t.Fatal(err)
	}
	if !shouldUpdate || !got.HasFinalizedBlock || !sameBlockID(got.FinalizedBlock, current.FinalizedBlock) ||
		got.CurrentWindowStart != next.CurrentWindowStart ||
		got.CurrentWindowObservedSlot != next.CurrentWindowObservedSlot ||
		!got.CurrentWindowStartAt.Equal(next.CurrentWindowStartAt) {
		t.Fatalf("reconciled update = %+v, should update = %t", got, shouldUpdate)
	}
	got.FinalizedBlock.RootHash[0]++
	if sameBlockID(got.FinalizedBlock, current.FinalizedBlock) {
		t.Fatal("reconciled finalized block aliases the durable observation")
	}
}

type controllerTestBackend struct {
	mu sync.Mutex

	id         [32]byte
	records    map[[32]byte]SessionRecord
	started    int
	closed     int
	prepared   int
	activated  int
	retired    int
	probes     int
	progresses int
	progressed chan ConsensusProgress
	sessionErr error
	closeErr   error
	closeFails int
}

type controllerEmptyHistory struct{}

type controllerTestAcquisition struct{}

func (controllerTestAcquisition) PublishMasterchainView(
	context.Context,
	*groups.Snapshot,
	*cell.Cell,
	*cell.Cell,
) error {
	return nil
}

func (controllerEmptyHistory) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}

func (controllerEmptyHistory) BlockState(
	context.Context,
	ton.BlockIDExt,
) (*storage.BlockState, error) {
	return nil, storage.ErrNotFound
}

func (controllerEmptyHistory) BlockMeta(
	context.Context,
	ton.BlockIDExt,
) (*storage.BlockMeta, error) {
	return nil, storage.ErrNotFound
}

func newControllerTestBackend(id [32]byte) *controllerTestBackend {
	return &controllerTestBackend{
		id:         id,
		records:    make(map[[32]byte]SessionRecord),
		progressed: make(chan ConsensusProgress, 4),
	}
}

func (b *controllerTestBackend) CollatorID() [32]byte { return b.id }

func (b *controllerTestBackend) Start(context.Context) error {
	b.mu.Lock()
	b.started++
	b.mu.Unlock()

	return nil
}

func (b *controllerTestBackend) Close(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed++
	if b.closeFails > 0 {
		b.closeFails--
		return b.closeErr
	}

	return nil
}

func (b *controllerTestBackend) Session(_ context.Context, id [32]byte) (SessionRecord, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sessionErr != nil {
		return SessionRecord{}, b.sessionErr
	}
	record, exists := b.records[id]
	if !exists {
		return SessionRecord{}, ErrNotFound
	}

	return cloneSessionRecord(record), nil
}

func (b *controllerTestBackend) PrepareSession(
	_ context.Context,
	session Session,
	update SessionUpdate,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepared++
	if current, exists := b.records[session.ID]; exists {
		if current.Session.Equal(session) && current.Update.Equal(update) {
			return nil
		}

		return ErrSessionConflict
	}
	b.records[session.ID] = cloneSessionRecord(SessionRecord{Session: session, Update: update})

	return nil
}

func (b *controllerTestBackend) ActivateSession(_ context.Context, activation SessionActivation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.activated++
	record, exists := b.records[activation.SessionID]
	if !exists {
		return ErrNotFound
	}
	if record.Activation != nil && !record.Activation.Equal(activation) {
		return ErrSessionConflict
	}
	activation = cloneSessionActivation(activation)
	record.Activation = &activation
	b.records[activation.SessionID] = record

	return nil
}

func (b *controllerTestBackend) UpdateSession(_ context.Context, update SessionUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	record, exists := b.records[update.SessionID]
	if !exists {
		return ErrNotFound
	}
	record.Update = cloneSessionUpdate(update)
	b.records[update.SessionID] = record

	return nil
}

func (b *controllerTestBackend) RetireSession(_ context.Context, id [32]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retired++
	delete(b.records, id)

	return nil
}

func (b *controllerTestBackend) Probe(context.Context, WindowPreparation) error {
	b.mu.Lock()
	b.probes++
	b.mu.Unlock()

	return nil
}

func (*controllerTestBackend) CommitDelegation(context.Context, WindowRequest) error { return nil }

func (*controllerTestBackend) Status(context.Context) (Status, error) { return Status{}, nil }

func (b *controllerTestBackend) ApplyConsensusProgress(
	_ context.Context,
	progress ConsensusProgress,
) error {
	b.mu.Lock()
	b.progresses++
	b.mu.Unlock()
	b.progressed <- progress

	return nil
}

func (*controllerTestBackend) ObserveConsensusFinalized(
	context.Context,
	[32]byte,
	ton.BlockIDExt,
) error {
	return nil
}

type controllerTestObserver struct {
	mu sync.Mutex

	id                 [32]byte
	handlers           RemoteHandlers
	events             ConsensusObserverEvents
	preparedSessions   map[[32]byte]ConsensusObserverSession
	prepared           int
	activated          int
	updated            int
	retired            int
	closed             int
	activationFailures int
	emitProgress       bool
	callbackErrors     chan error
	startErr           error
	closeErr           error
	closeFails         int
	recovered          [][32]byte
}

func newControllerTestObserver(id [32]byte) *controllerTestObserver {
	return &controllerTestObserver{
		id:               id,
		preparedSessions: make(map[[32]byte]ConsensusObserverSession),
		callbackErrors:   make(chan error, 4),
	}
}

func (o *controllerTestObserver) LocalADNLID() [32]byte { return o.id }

func (o *controllerTestObserver) Start(
	_ context.Context,
	handlers RemoteHandlers,
	events ConsensusObserverEvents,
) error {
	o.mu.Lock()
	o.handlers = handlers
	o.events = events
	err := o.startErr
	o.mu.Unlock()

	return err
}

func (o *controllerTestObserver) RecoverSessions(context.Context) ([][32]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([][32]byte(nil), o.recovered...), nil
}

func (o *controllerTestObserver) PrepareSession(
	_ context.Context,
	session ConsensusObserverSession,
) error {
	o.mu.Lock()
	o.prepared++
	o.preparedSessions[session.Overlay.Session.ID] = session
	o.mu.Unlock()

	return nil
}

func (o *controllerTestObserver) ActivateSession(
	_ context.Context,
	activation SessionActivation,
) error {
	o.mu.Lock()
	o.activated++
	if o.activationFailures > 0 {
		o.activationFailures--
		o.mu.Unlock()

		return errors.New("observer activation failed")
	}
	session := o.preparedSessions[activation.SessionID].Overlay.Session
	progressed := o.events.Progressed
	emitProgress := o.emitProgress
	o.mu.Unlock()

	if emitProgress {
		progress := ConsensusProgress{
			SessionID: activation.SessionID,
			Window: simplex.Window{
				Base:         simplex.Genesis(),
				ObservedSlot: 0,
				StartSlot:    0,
				EndSlot:      session.SlotsPerLeaderWindow,
				Leader:       0,
				ObservedAt:   time.Now(),
			},
			StartAt: time.Now(),
		}
		go func() {
			o.callbackErrors <- progressed(context.Background(), progress)
		}()
	}

	return nil
}

func (o *controllerTestObserver) UpdateSession(
	_ context.Context,
	session ConsensusObserverSession,
) error {
	o.mu.Lock()
	o.updated++
	o.preparedSessions[session.Overlay.Session.ID] = session
	o.mu.Unlock()

	return nil
}

func (o *controllerTestObserver) RetireSession(_ context.Context, id [32]byte) error {
	o.mu.Lock()
	o.retired++
	delete(o.preparedSessions, id)
	o.mu.Unlock()

	return nil
}

func (o *controllerTestObserver) Close(context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed++
	if o.closeFails > 0 {
		o.closeFails--
		return o.closeErr
	}

	return nil
}

func controllerTestSnapshot(local [32]byte) *groups.Snapshot {
	master := runtimeTestBlockID(-1, -1<<63, 20)
	validator := groups.Validator{
		PublicKey:     [32]byte{0x11},
		PublicKeyHash: [32]byte{0x12},
		ADNL:          [32]byte{0x13},
		Weight:        1,
	}
	consensus := &groups.SimplexConfig{
		Version:              2,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: 2,
	}
	return &groups.Snapshot{
		MasterchainBlock: master,
		Ready:            true,
		LifecycleEnabled: true,
		Config: &groups.Config{
			NewConsensus: groups.NewConsensusConfig{
				Masterchain: consensus,
				Shard:       consensus,
			},
			MaxBlockSize:         1 << 20,
			MaxCollatedDataSize:  2 << 20,
			AllCurrentValidators: [][32]byte{validator.ADNL},
		},
		Active: []groups.Session{{
			ID:               [32]byte{0x21},
			Shard:            groups.ShardID{Workchain: 0, Shard: -1 << 63},
			CatchainSeqno:    7,
			ValidatorSetHash: 8,
			Validators:       []groups.Validator{validator},
			Genesis:          []ton.BlockIDExt{runtimeTestBlockID(0, -1<<63, 19)},
			MinMasterchain:   master,
		}},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID:  validator.PublicKeyHash,
			CollatorADNLIDs: [][32]byte{local},
		}},
		PersistentOverlay: []groups.PersistentOverlayMember{{ADNL: validator.ADNL}},
	}
}

func newControllerTestFixture(t *testing.T) (*Controller, *controllerTestBackend, *controllerTestObserver) {
	t.Helper()
	local := [32]byte{0x31}
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     newRuntimeMemoryStorage(),
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	return controller, backend, observer
}

func TestProjectCollatorSessionsUsesChainRegistryRoles(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	policy := sessionProjectionPolicy{localADNLID: local}

	withoutRegistration := *snapshot
	withoutRegistration.CollatorsByValidator = nil
	projected, err := projectCollatorSessions(&withoutRegistration, policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 0 {
		t.Fatalf("unregistered local collator projected %d sessions", len(projected))
	}

	observerOnly := *snapshot
	observerOnly.CollatorsByValidator = []groups.CollatorRegistryEntry{{
		ValidatorKeyID:  [32]byte{0x41},
		CollatorADNLIDs: [][32]byte{local},
	}}
	projected, err = projectCollatorSessions(&observerOnly, policy)
	if err != nil {
		t.Fatal(err)
	}
	base := projected[snapshot.Active[0].ID]
	if base.overlay.Role != OverlayRoleCollator || len(base.overlay.CollatorsByValidator) != 0 ||
		!slices.Equal(base.overlay.AllCollators, [][32]byte{local}) {
		t.Fatalf("non-roster registration projection = %+v", base.overlay)
	}

	projected, err = projectCollatorSessions(snapshot, policy)
	if err != nil {
		t.Fatal(err)
	}
	base = projected[snapshot.Active[0].ID]
	if base.overlay.Role != OverlayRoleCollator ||
		base.overlay.BroadcastMode != CandidateBroadcastPrivateOverlay ||
		!base.overlay.ObserversInPrivateOverlay || len(base.overlay.CollatorsByValidator) != 1 ||
		!slices.Equal(base.overlay.AllCollators, [][32]byte{local}) ||
		!slices.Equal(base.overlay.AllCurrentValidators, snapshot.Config.AllCurrentValidators) {
		t.Fatalf("roster registration projection = %+v", base.overlay)
	}
}

func TestProjectCollatorSessionsGivesGroupValidatorADNLPrecedence(t *testing.T) {
	snapshot := controllerTestSnapshot([32]byte{0x31})
	validatorADNL := snapshot.Active[0].Validators[0].ADNL
	snapshot.CollatorsByValidator = []groups.CollatorRegistryEntry{{
		ValidatorKeyID:  snapshot.Active[0].Validators[0].PublicKeyHash,
		CollatorADNLIDs: [][32]byte{validatorADNL},
	}}

	projected, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: validatorADNL})
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 0 {
		t.Fatalf("validator ADNL projected %d standalone collator sessions", len(projected))
	}
}

func TestProjectCollatorSessionsSkipsUnavailableObserverIdentity(t *testing.T) {
	legacy := &groups.SimplexConfig{
		Version:              1,
		SlotsPerLeaderWindow: 2,
	}
	for _, test := range []struct {
		name      string
		consensus *groups.SimplexConfig
	}{
		{name: "absent"},
		{name: "legacy", consensus: legacy},
		{name: "protocol zero", consensus: &groups.SimplexConfig{
			Version:              2,
			ProtocolVersion:      0,
			SlotsPerLeaderWindow: 2,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			local := [32]byte{0x31}
			snapshot := controllerTestSnapshot(local)
			shard := snapshot.Active[0]
			master := shard
			master.ID = [32]byte{0x22}
			master.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
			master.Genesis = []ton.BlockIDExt{runtimeTestBlockID(-1, -1<<63, 19)}
			snapshot.Active = []groups.Session{master, shard}
			snapshot.Config.NewConsensus.Masterchain = test.consensus

			projected, err := projectCollatorSessions(
				snapshot,
				sessionProjectionPolicy{localADNLID: local},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(projected) != 1 {
				t.Fatalf("mixed consensus projection has %d sessions, want shard only", len(projected))
			}
			if _, exists := projected[master.ID]; exists {
				t.Fatal("unsupported masterchain observer session was projected")
			}
			if projected[shard.ID].overlay.Role != OverlayRoleCollator {
				t.Fatalf("shard role = %v, want collator", projected[shard.ID].overlay.Role)
			}
		})
	}
}

func TestProjectCollatorSessionsRejectsUnsupportedDelegatedGroup(t *testing.T) {
	for _, test := range []struct {
		name      string
		consensus *groups.SimplexConfig
	}{
		{name: "absent"},
		{name: "legacy", consensus: &groups.SimplexConfig{
			Version:              1,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 2,
		}},
		{name: "unrepresentable protocol", consensus: &groups.SimplexConfig{
			Version:              2,
			ProtocolVersion:      simplex.MaxProtocolVersion + 1,
			SlotsPerLeaderWindow: 2,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			local := [32]byte{0x31}
			snapshot := controllerTestSnapshot(local)
			snapshot.Config.NewConsensus.Shard = test.consensus

			_, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: local})
			if err == nil {
				t.Fatal("unsupported delegated session was accepted")
			}
		})
	}
}

func TestProjectCollatorSessionsProtocolRoleMatrix(t *testing.T) {
	local := [32]byte{0x31}
	for _, test := range []struct {
		name             string
		protocolVersion  uint8
		masterchain      bool
		wantProjected    bool
		wantRole         OverlayRole
		wantBroadcast    CandidateBroadcastMode
		wantPrivatePeers bool
	}{
		{name: "protocol zero collator omitted", protocolVersion: 0},
		{name: "protocol one collator block sync", protocolVersion: 1, wantProjected: true,
			wantRole: OverlayRoleCollator, wantBroadcast: CandidateBroadcastBlockSyncOverlay},
		{name: "protocol two collator private", protocolVersion: 2, wantProjected: true,
			wantRole: OverlayRoleCollator, wantBroadcast: CandidateBroadcastPrivateOverlay, wantPrivatePeers: true},
		{name: "protocol three collator private", protocolVersion: 3, wantProjected: true,
			wantRole: OverlayRoleCollator, wantBroadcast: CandidateBroadcastPrivateOverlay, wantPrivatePeers: true},
		{name: "protocol zero observer omitted", protocolVersion: 0, masterchain: true},
		{name: "protocol one block sync observer", protocolVersion: 1, masterchain: true, wantProjected: true,
			wantRole: OverlayRoleObserver, wantBroadcast: CandidateBroadcastBlockSyncOverlay},
		{name: "protocol two private observer", protocolVersion: 2, masterchain: true, wantProjected: true,
			wantRole: OverlayRoleObserver, wantBroadcast: CandidateBroadcastPrivateOverlay, wantPrivatePeers: true},
		{name: "protocol three private observer", protocolVersion: 3, masterchain: true, wantProjected: true,
			wantRole: OverlayRoleObserver, wantBroadcast: CandidateBroadcastPrivateOverlay, wantPrivatePeers: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := controllerTestSnapshot(local)
			consensus := &groups.SimplexConfig{
				Version:              2,
				ProtocolVersion:      test.protocolVersion,
				SlotsPerLeaderWindow: 2,
			}
			group := snapshot.Active[0]
			if test.masterchain {
				group.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
				group.Genesis = []ton.BlockIDExt{runtimeTestBlockID(-1, -1<<63, 19)}
				snapshot.Config.NewConsensus.Masterchain = consensus
			} else {
				snapshot.Config.NewConsensus.Shard = consensus
			}
			snapshot.Active = []groups.Session{group}

			projected, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: local})
			if err != nil {
				t.Fatal(err)
			}
			got, exists := projected[group.ID]
			if exists != test.wantProjected {
				t.Fatalf("projected = %v, want %v", exists, test.wantProjected)
			}
			if !exists {
				return
			}
			if got.overlay.Role != test.wantRole || got.overlay.BroadcastMode != test.wantBroadcast ||
				got.overlay.ObserversInPrivateOverlay != test.wantPrivatePeers ||
				!slices.Equal(got.overlay.AllCollators, [][32]byte{local}) {
				t.Fatalf("protocol projection = %+v", got.overlay)
			}
		})
	}
}

func TestControllerBootstrapMasterchainLifecycle(t *testing.T) {
	local := [32]byte{0x31}
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     newRuntimeMemoryStorage(),
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.BootstrapMasterchain(
		context.Background(),
		controllerEmptyHistory{},
		nil,
		time.Now(),
	); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("empty bootstrap error = %v, want ErrNoSnapshot", err)
	}
	if err = controller.BootstrapMasterchain(
		context.Background(),
		controllerEmptyHistory{},
		nil,
		time.Now(),
	); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("empty bootstrap retry error = %v, want ErrNoSnapshot", err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = controller.BootstrapMasterchain(
		context.Background(),
		controllerEmptyHistory{},
		nil,
		time.Now(),
	); err == nil {
		t.Fatal("bootstrap after Start succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = controller.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = controller.BootstrapMasterchain(
		context.Background(),
		controllerEmptyHistory{},
		nil,
		time.Now(),
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("bootstrap after Close error = %v, want ErrClosed", err)
	}
}

func TestControllerRetriesFailedStartCleanup(t *testing.T) {
	local := [32]byte{0x31}
	backend := newControllerTestBackend(local)
	backend.closeErr = errors.New("backend close failed")
	backend.closeFails = 1
	observer := newControllerTestObserver(local)
	observer.startErr = errors.New("observer start failed")
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     newRuntimeMemoryStorage(),
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	err = controller.Start(context.Background())
	if !errors.Is(err, observer.startErr) || !errors.Is(err, backend.closeErr) {
		t.Fatalf("Start error = %v, want observer and cleanup failures", err)
	}
	if err = controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	closed := backend.closed
	backend.mu.Unlock()
	if closed != 2 {
		t.Fatalf("backend close calls = %d, want cleanup retry", closed)
	}
	status, err := controller.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Closed {
		t.Fatalf("controller status after cleanup retry = %+v", status)
	}
}

func TestControllerClosesBackendOnlyAfterObserverQuiesces(t *testing.T) {
	controller, backend, observer := newControllerTestFixture(t)
	observer.mu.Lock()
	observer.closeErr = errors.New("observer close timed out")
	observer.closeFails = 1
	observer.mu.Unlock()

	err := controller.Close(context.Background())
	if !errors.Is(err, observer.closeErr) {
		t.Fatalf("first Close error = %v, want observer failure", err)
	}
	backend.mu.Lock()
	backendCloses := backend.closed
	backend.mu.Unlock()
	if backendCloses != 0 {
		t.Fatalf("backend closed %d times before observer quiesced", backendCloses)
	}

	if err = controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backendCloses = backend.closed
	backend.mu.Unlock()
	if backendCloses != 1 {
		t.Fatalf("backend close calls = %d, want 1 after observer retry", backendCloses)
	}
}

func TestControllerCloseHonorsContextWhileLifecycleIsBusy(t *testing.T) {
	local := [32]byte{0x31}
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     newRuntimeMemoryStorage(),
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	controller.applyMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = controller.Close(ctx)
	cancel()
	controller.applyMu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want context deadline", err)
	}

	if err = controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestControllerActivationAdmitsQueuedProgressAfterCommit(t *testing.T) {
	controller, backend, observer := newControllerTestFixture(t)
	observer.mu.Lock()
	observer.emitProgress = true
	observer.mu.Unlock()
	snapshot := controllerTestSnapshot(backend.id)
	if err := controller.reconcileSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-observer.callbackErrors:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued observer progress did not complete")
	}
	select {
	case progress := <-backend.progressed:
		if progress.SessionID != snapshot.Active[0].ID {
			t.Fatalf("progress session = %x", progress.SessionID)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not receive observer progress")
	}
}

func TestControllerRetriesExactObserverActivationWithIngressClosed(t *testing.T) {
	controller, backend, observer := newControllerTestFixture(t)
	observer.mu.Lock()
	observer.activationFailures = 1
	observer.mu.Unlock()
	snapshot := controllerTestSnapshot(backend.id)
	sessionID := snapshot.Active[0].ID

	if err := controller.reconcileSnapshot(context.Background(), snapshot); err == nil {
		t.Fatal("transient observer activation failure was ignored")
	}
	handlers := controller.remoteHandlers()
	err := handlers.Probe(context.Background(), AuthenticatedQuery{
		SessionID:  sessionID,
		SourceADNL: snapshot.Active[0].Validators[0].ADNL,
	}, simplex.ConsensusPleaseCollatePrepare{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("delegation during incomplete activation = %v, want ErrNotFound", err)
	}

	if err = controller.reconcileSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err = handlers.Probe(context.Background(), AuthenticatedQuery{
		SessionID:  sessionID,
		SourceADNL: snapshot.Active[0].Validators[0].ADNL,
	}, simplex.ConsensusPleaseCollatePrepare{}); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	prepared, activated, updated := observer.prepared, observer.activated, observer.updated
	observer.mu.Unlock()
	if prepared != 1 || activated != 2 || updated != 0 {
		t.Fatalf("observer lifecycle prepare=%d activate=%d update=%d, want 1/2/0",
			prepared, activated, updated)
	}
}

func TestControllerKeepsDelegationIngressClosedBeforeStartCompletes(t *testing.T) {
	local := [32]byte{0x31}
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     newRuntimeMemoryStorage(),
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := controllerTestSnapshot(local)
	projection, err := projectCollatorSessions(snapshot, controller.policy)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := snapshot.Active[0].ID
	managed := newControlledSession()
	managed.projection = projection[sessionID]
	managed.hasProjection = true
	managed.reconciled = true
	managed.hasBackend = true
	managed.hasObserver = true
	controller.managed[sessionID] = managed

	err = controller.remoteHandlers().Probe(context.Background(), AuthenticatedQuery{
		SessionID:  sessionID,
		SourceADNL: snapshot.Active[0].Validators[0].ADNL,
	}, simplex.ConsensusPleaseCollatePrepare{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("delegation before controller start error = %v, want ErrNotFound", err)
	}
}

func TestControllerRetiresRecoveredSessionMissingFromSnapshot(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	projected, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: local})
	if err != nil {
		t.Fatal(err)
	}
	want := projected[snapshot.Active[0].ID]
	activation := cloneSessionActivation(*want.activation)
	record := SessionRecord{
		Session:    cloneSession(want.session),
		Activation: &activation,
		Update:     cloneSessionUpdate(want.update),
	}
	storage := newRuntimeMemoryStorage()
	storage.sessions[record.Session.ID] = cloneSessionRecord(record)
	backend := newControllerTestBackend(local)
	backend.records[record.Session.ID] = cloneSessionRecord(record)
	observer := newControllerTestObserver(local)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     storage,
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	withoutRegistration := *snapshot
	withoutRegistration.CollatorsByValidator = nil
	if err = controller.reconcileSnapshot(context.Background(), &withoutRegistration); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	retired := backend.retired
	_, retained := backend.records[record.Session.ID]
	backend.mu.Unlock()
	if retired != 1 || retained {
		t.Fatalf("recovered retirement calls=%d retained=%t, want 1/false", retired, retained)
	}
}

func TestControllerReconcilesRecoveredObserverInventory(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	desiredID := snapshot.Active[0].ID
	staleID := [32]byte{0x99}
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	observer.recovered = [][32]byte{desiredID, staleID}
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     newRuntimeMemoryStorage(),
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	if err = controller.reconcileSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	observer.mu.Lock()
	prepared := observer.prepared
	activated := observer.activated
	updated := observer.updated
	retired := observer.retired
	observer.mu.Unlock()
	if prepared != 1 || activated != 1 || updated != 0 || retired != 1 {
		t.Fatalf(
			"recovered observer lifecycle prepare=%d activate=%d update=%d retire=%d, want 1/1/0/1",
			prepared,
			activated,
			updated,
			retired,
		)
	}

	controller.mu.RLock()
	desired := controller.managed[desiredID]
	_, staleRetained := controller.managed[staleID]
	controller.mu.RUnlock()
	if desired == nil || staleRetained {
		t.Fatalf("recovered inventory desired=%v stale retained=%t", desired != nil, staleRetained)
	}
	desired.mu.Lock()
	reconciled := desired.reconciled
	hasBackend := desired.hasBackend
	hasObserver := desired.hasObserver
	desired.mu.Unlock()
	if !reconciled || !hasBackend || !hasObserver {
		t.Fatalf(
			"desired recovered state reconciled=%t backend=%t observer=%t",
			reconciled,
			hasBackend,
			hasObserver,
		)
	}
}

func TestControllerDoesNotReconcileUnavailableBackendFromStorage(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	snapshot.Future = append([]groups.Session(nil), snapshot.Active...)
	snapshot.Active = nil
	projected, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: local})
	if err != nil {
		t.Fatal(err)
	}
	desiredID := snapshot.Future[0].ID
	want := projected[desiredID]
	record := SessionRecord{
		Session: cloneSession(want.session),
		Update:  cloneSessionUpdate(want.update),
	}
	storage := newRuntimeMemoryStorage()
	storage.sessions[desiredID] = cloneSessionRecord(record)
	backend := newControllerTestBackend(local)
	backend.sessionErr = ErrSessionUnavailable
	observer := newControllerTestObserver(local)
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     storage,
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Messages:    controllerTestMessagePool(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	err = controller.reconcileSnapshot(context.Background(), snapshot)
	if !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("reconcile error = %v, want ErrSessionUnavailable", err)
	}

	controller.mu.RLock()
	managed := controller.managed[desiredID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("recovered backend inventory disappeared after unavailable load")
	}
	managed.mu.Lock()
	reconciled := managed.reconciled
	hasProjection := managed.hasProjection
	managed.mu.Unlock()
	if reconciled || hasProjection {
		t.Fatalf("unavailable backend reconciled=%t projection=%t", reconciled, hasProjection)
	}
	observer.mu.Lock()
	prepared := observer.prepared
	observer.mu.Unlock()
	if prepared != 0 {
		t.Fatalf("observer prepared %d sessions before backend became available", prepared)
	}
}

func TestControllerStatusHonorsContextWhileSessionIsBusy(t *testing.T) {
	controller, backend, _ := newControllerTestFixture(t)
	snapshot := controllerTestSnapshot(backend.id)
	if err := controller.reconcileSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	controller.mu.RLock()
	managed := controller.managed[snapshot.Active[0].ID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("reconciled session is absent")
	}
	managed.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	result := make(chan error, 1)
	go func() {
		_, err := controller.Status(ctx)
		result <- err
	}()

	select {
	case err := <-result:
		managed.mu.Unlock()
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Status error = %v, want context deadline", err)
		}
	case <-time.After(time.Second):
		managed.mu.Unlock()
		cancel()
		<-result
		t.Fatal("Status ignored context while session lock was held")
	}
}
