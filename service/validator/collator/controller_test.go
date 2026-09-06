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

func controllerTestFeed(t *testing.T) *msgpool.Feed {
	t.Helper()

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)

	return msgpool.NewFeed(msgpool.FeedOptions{Pool: pool})
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

	id           [32]byte
	records      map[[32]byte]SessionRecord
	started      int
	closed       int
	prepared     int
	activated    int
	retired      int
	probes       int
	progresses   int
	updates      int
	progressed   chan ConsensusProgress
	sessionErr   error
	updateErr    error
	closeErr     error
	closeFails   int
	speculated   []SpeculativeWindowRequest
	speculateErr error
	notarized    func(groups.ShardID, simplex.CandidateID, time.Time)
}

type controllerEmptyHistory struct{}

type controllerTestAcquisition struct{}

type controllerTestTracker struct {
	snapshot *groups.Snapshot
}

type controllerReplayTracker struct {
	snapshot *groups.Snapshot
	applies  int
}

func (t controllerTestTracker) Bootstrap(
	context.Context,
	groups.MasterchainHistory,
	[]groups.BufferedMasterchainState,
	time.Time,
) ([]groups.Transition, error) {
	return nil, nil
}

func (t controllerTestTracker) Apply(groups.ApplyInput) (groups.ApplyResult, error) {
	return groups.ApplyResult{Snapshot: t.snapshot}, nil
}

func (t controllerTestTracker) Snapshot() (*groups.Snapshot, error) {
	return t.snapshot, nil
}

func (t *controllerReplayTracker) Bootstrap(
	context.Context,
	groups.MasterchainHistory,
	[]groups.BufferedMasterchainState,
	time.Time,
) ([]groups.Transition, error) {
	return nil, nil
}

func (t *controllerReplayTracker) Apply(input groups.ApplyInput) (groups.ApplyResult, error) {
	t.applies++
	switch {
	case input.Block.SeqNo < t.snapshot.MasterchainBlock.SeqNo:
		return groups.ApplyResult{}, groups.ErrStaleMasterchainState
	case input.Block.SeqNo == t.snapshot.MasterchainBlock.SeqNo:
		if !input.Block.Equals(&t.snapshot.MasterchainBlock) {
			return groups.ApplyResult{}, groups.ErrConflictingMasterchainState
		}

		return groups.ApplyResult{Snapshot: t.snapshot}, nil
	default:
		next := *t.snapshot
		next.MasterchainBlock = input.Block
		t.snapshot = &next

		return groups.ApplyResult{Snapshot: t.snapshot}, nil
	}
}

func (t *controllerReplayTracker) Snapshot() (*groups.Snapshot, error) {
	return t.snapshot, nil
}

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
	b.updates++
	if b.updateErr != nil {
		return b.updateErr
	}
	record, exists := b.records[update.SessionID]
	if !exists {
		return ErrNotFound
	}
	record.Update = cloneSessionUpdate(update)
	b.records[update.SessionID] = record

	return nil
}

func TestControllerKeepsReconciledProjectionOnDeferredBackendRefresh(t *testing.T) {
	controller, backend, _ := newControllerTestFixture(t)
	snapshot := controllerTestSnapshot(backend.id)
	if err := controller.reconcileSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	sessionID := snapshot.Active[0].ID
	controller.mu.RLock()
	managed := controller.managed[sessionID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("reconciled session is absent")
	}
	managed.mu.Lock()
	accepted := cloneSessionUpdate(managed.projection.update)
	managed.mu.Unlock()

	next := *snapshot
	next.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, snapshot.MasterchainBlock.SeqNo+1)
	backend.mu.Lock()
	backend.updateErr = ErrSessionUpdateDeferred
	backend.mu.Unlock()
	if err := controller.reconcileSnapshot(context.Background(), &next); err != nil {
		t.Fatalf("deferred refresh escaped controller: %v", err)
	}

	managed.mu.Lock()
	reconciled := managed.reconciled
	projected := cloneSessionUpdate(managed.projection.update)
	managed.mu.Unlock()
	if !reconciled {
		t.Fatal("deferred backend refresh closed reconciled ingress")
	}
	if !projected.Equal(accepted) {
		t.Fatalf("deferred projection was published before backend acceptance: %+v", projected)
	}
	if err := controller.remoteHandlers().Probe(context.Background(), AuthenticatedQuery{
		SessionID:  sessionID,
		SourceADNL: snapshot.Active[0].Validators[0].ADNL,
	}, simplex.ConsensusPleaseCollatePrepare{}); err != nil {
		t.Fatalf("delegation ingress closed by deferred refresh: %v", err)
	}

	backend.mu.Lock()
	backend.updateErr = nil
	backend.mu.Unlock()
	if err := controller.reconcileSnapshot(context.Background(), &next); err != nil {
		t.Fatalf("apply deferred refresh: %v", err)
	}
	managed.mu.Lock()
	projected = cloneSessionUpdate(managed.projection.update)
	managed.mu.Unlock()
	if projected.MasterchainBlock.SeqNo != next.MasterchainBlock.SeqNo {
		t.Fatalf("projection masterchain seqno = %d, want %d", projected.MasterchainBlock.SeqNo, next.MasterchainBlock.SeqNo)
	}
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

func (b *controllerTestBackend) SpeculateWindow(_ context.Context, request SpeculativeWindowRequest) error {
	b.mu.Lock()
	b.speculated = append(b.speculated, request)
	b.mu.Unlock()

	return b.speculateErr
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
	activationErr      error
	activationHook     func(context.Context) error
	emitProgress       bool
	callbackErrors     chan error
	beforeUpdate       func()
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
	ctx context.Context,
	activation SessionActivation,
) error {
	o.mu.Lock()
	o.activated++
	if o.activationFailures > 0 {
		o.activationFailures--
		err := o.activationErr
		if err == nil {
			err = errors.New("observer activation failed")
		}
		o.mu.Unlock()

		return err
	}
	session := o.preparedSessions[activation.SessionID].Overlay.Session
	progressed := o.events.Progressed
	emitProgress := o.emitProgress
	activationHook := o.activationHook
	o.mu.Unlock()
	if activationHook != nil {
		if err := activationHook(ctx); err != nil {
			return err
		}
	}

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
	beforeUpdate := o.beforeUpdate
	o.mu.Unlock()
	if beforeUpdate != nil {
		beforeUpdate()
	}

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
		Feed:        controllerTestFeed(t),
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
		Feed:        controllerTestFeed(t),
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
		Feed:        controllerTestFeed(t),
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
		Feed:        controllerTestFeed(t),
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

func TestControllerDefersProgressAcrossObserverUpdateLockInversion(t *testing.T) {
	controller, backend, observer := newControllerTestFixture(t)
	snapshot := controllerTestSnapshot(backend.id)
	if err := controller.reconcileSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	// Model the observer runtime's lock order exactly: progress is delivered
	// while runtime.lifecycleMu is held, while reconciliation holds managed.mu
	// before Observer.UpdateSession waits for runtime.lifecycleMu.
	var lifecycleMu sync.Mutex
	lifecycleMu.Lock()
	updateStarted := make(chan struct{})
	var updateStartedOnce sync.Once
	observer.mu.Lock()
	observer.beforeUpdate = func() {
		updateStartedOnce.Do(func() { close(updateStarted) })
		lifecycleMu.Lock()
		lifecycleMu.Unlock()
	}
	progressed := observer.events.Progressed
	observer.mu.Unlock()

	next := *snapshot
	next.MasterchainBlock = runtimeTestBlockID(
		-1,
		-1<<63,
		snapshot.MasterchainBlock.SeqNo+1,
	)
	reconciled := make(chan error, 1)
	go func() {
		reconciled <- controller.reconcileSnapshot(context.Background(), &next)
	}()

	select {
	case <-updateStarted:
	case <-time.After(time.Second):
		lifecycleMu.Unlock()
		<-reconciled
		t.Fatal("observer update did not reach the runtime lifecycle lock")
	}

	now := time.Now()
	progress := ConsensusProgress{
		SessionID: snapshot.Active[0].ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      snapshot.Config.NewConsensus.Shard.SlotsPerLeaderWindow,
			Leader:       0,
			ObservedAt:   now,
		},
		StartAt: now,
	}
	progressResult := make(chan error, 1)
	go func() {
		progressResult <- progressed(context.Background(), progress)
	}()

	var progressErr error
	select {
	case progressErr = <-progressResult:
	case <-time.After(time.Second):
		lifecycleMu.Unlock()
		<-reconciled
		<-progressResult
		t.Fatal("progress waited on the controller session lock")
	}
	if !errors.Is(progressErr, ErrSessionUpdateDeferred) {
		lifecycleMu.Unlock()
		<-reconciled
		t.Fatalf("contended progress error = %v, want ErrSessionUpdateDeferred", progressErr)
	}
	backend.mu.Lock()
	progresses := backend.progresses
	backend.mu.Unlock()
	if progresses != 0 {
		lifecycleMu.Unlock()
		<-reconciled
		t.Fatalf("deferred progress reached backend %d times", progresses)
	}

	lifecycleMu.Unlock()
	if err := <-reconciled; err != nil {
		t.Fatalf("reconcile after deferred progress: %v", err)
	}
	if err := progressed(context.Background(), progress); err != nil {
		t.Fatalf("retry progress after reconcile: %v", err)
	}
	backend.mu.Lock()
	progresses = backend.progresses
	backend.mu.Unlock()
	if progresses != 1 {
		t.Fatalf("retried progress reached backend %d times, want 1", progresses)
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

func TestControllerRecoveryReplayGateRetriesCurrentLifecycle(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	snapshot.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 24)
	snapshot.Active[0].MinMasterchain = snapshot.MasterchainBlock
	projected, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: local})
	if err != nil {
		t.Fatal(err)
	}
	sessionID := snapshot.Active[0].ID
	want := projected[sessionID]
	floor := cloneBlockID(snapshot.MasterchainBlock)
	recoveredUpdate := cloneSessionUpdate(want.update)
	record := SessionRecord{
		Session: cloneSession(want.session),
		Update:  recoveredUpdate,
	}
	storage := newRuntimeMemoryStorage()
	storage.sessions[sessionID] = cloneSessionRecord(record)
	tracker := &controllerReplayTracker{snapshot: snapshot}
	backend := newControllerTestBackend(local)
	backend.records[sessionID] = cloneSessionRecord(record)
	observer := newControllerTestObserver(local)
	observer.recovered = [][32]byte{sessionID}
	observer.activationFailures = 1
	observer.activationErr = ErrAcquisitionNotReady
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     storage,
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Feed:        controllerTestFeed(t),
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

	controller.mu.RLock()
	managed := controller.managed[sessionID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("deferred bootstrapped session is absent")
	}
	managed.mu.Lock()
	reconciled := managed.reconciled
	committedBlock := cloneBlockID(managed.projection.update.MasterchainBlock)
	managed.mu.Unlock()
	if reconciled || !sameBlockID(committedBlock, snapshot.MasterchainBlock) {
		t.Fatalf("deferred bootstrap reconciled=%t masterchain=%v", reconciled, committedBlock)
	}
	if controller.recoveryFloor == nil || !sameBlockID(*controller.recoveryFloor, floor) {
		t.Fatalf("deferred bootstrap recovery floor = %v, want %v", controller.recoveryFloor, floor)
	}
	for _, replay := range []ton.BlockIDExt{
		runtimeTestBlockID(-1, -1<<63, floor.SeqNo-2),
		runtimeTestBlockID(-1, -1<<63, snapshot.MasterchainBlock.SeqNo-1),
	} {
		if err = controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
			Block: replay,
			AsOf:  time.Now(),
		}); err != nil {
			t.Fatalf("recovery replay %d: %v", replay.SeqNo, err)
		}
	}
	committed, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if committed != snapshot {
		t.Fatal("older replay replaced the bootstrapped tracker snapshot")
	}
	if tracker.applies != 0 {
		t.Fatalf("recovery replays reached tracker %d times", tracker.applies)
	}
	observer.mu.Lock()
	activations := observer.activated
	observer.mu.Unlock()
	if activations != 1 {
		t.Fatalf("older replay retried observer activation %d times, want no retry", activations)
	}

	fork := cloneBlockID(snapshot.MasterchainBlock)
	fork.FileHash[0]++
	if err = controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
		Block: fork,
		AsOf:  time.Now(),
	}); !errors.Is(err, groups.ErrConflictingMasterchainState) {
		t.Fatalf("same-height fork error = %v, want ErrConflictingMasterchainState", err)
	}
	if tracker.applies != 1 {
		t.Fatalf("same-height fork tracker applies = %d, want 1", tracker.applies)
	}

	if err = controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
		Block: snapshot.MasterchainBlock,
		AsOf:  time.Now(),
	}); err != nil {
		t.Fatalf("exact current replay: %v", err)
	}
	managed.mu.Lock()
	reconciled = managed.reconciled
	committedBlock = cloneBlockID(managed.projection.update.MasterchainBlock)
	managed.mu.Unlock()
	observer.mu.Lock()
	activations = observer.activated
	observer.mu.Unlock()
	if !reconciled || !sameBlockID(committedBlock, snapshot.MasterchainBlock) || activations != 2 {
		t.Fatalf(
			"exact replay reconciled=%t masterchain=%v observer activations=%d",
			reconciled,
			committedBlock,
			activations,
		)
	}
	if controller.recoveryFloor != nil {
		t.Fatalf("successful exact replay retained recovery floor %v", controller.recoveryFloor)
	}

	older := runtimeTestBlockID(-1, -1<<63, snapshot.MasterchainBlock.SeqNo-1)
	if err = controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
		Block: older,
		AsOf:  time.Now(),
	}); !errors.Is(err, groups.ErrStaleMasterchainState) {
		t.Fatalf("stale apply after recovery error = %v, want ErrStaleMasterchainState", err)
	}
}

func TestControllerRecoveryReplayGateKeepsStaleHardAboveFloor(t *testing.T) {
	floor := runtimeTestBlockID(-1, -1<<63, 22)
	current := &groups.Snapshot{
		MasterchainBlock: runtimeTestBlockID(-1, -1<<63, floor.SeqNo+2),
	}
	tracker := &controllerReplayTracker{snapshot: current}
	controller := &Controller{
		tracker:       tracker,
		applyMu:       newCtxMutex(),
		state:         controllerRunning,
		recoveryFloor: &floor,
	}
	floorFork := cloneBlockID(floor)
	floorFork.RootHash[0]++
	err := controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
		Block: floorFork,
		AsOf:  time.Now(),
	})
	if !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("recovery floor fork above floor error = %v, want ErrSessionConflict", err)
	}
	if tracker.applies != 0 {
		t.Fatalf("recovery floor fork above floor reached tracker %d times", tracker.applies)
	}
	older := runtimeTestBlockID(-1, -1<<63, current.MasterchainBlock.SeqNo-1)

	err = controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
		Block: older,
		AsOf:  time.Now(),
	})
	if !errors.Is(err, groups.ErrStaleMasterchainState) {
		t.Fatalf("stale apply above recovery floor error = %v, want ErrStaleMasterchainState", err)
	}
	if tracker.applies != 1 {
		t.Fatalf("stale apply above recovery floor reached tracker %d times, want 1", tracker.applies)
	}
	if controller.recoveryFloor == nil || !sameBlockID(*controller.recoveryFloor, floor) {
		t.Fatalf("stale apply above recovery floor changed floor %v", controller.recoveryFloor)
	}
}

func TestControllerRecoveryReplayGateAdvancesNormallyBeforeFloor(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	tracker := &controllerReplayTracker{snapshot: snapshot}
	floor := runtimeTestBlockID(-1, -1<<63, 24)
	recoveredID := [32]byte{0x71}
	storage := newRuntimeMemoryStorage()
	storage.sessions[recoveredID] = SessionRecord{
		Session: Session{ID: recoveredID},
		Update: SessionUpdate{
			SessionID:        recoveredID,
			MasterchainBlock: floor,
		},
	}
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     storage,
		Observer:    observer,
		Tracker:     tracker,
		Acquisition: controllerTestAcquisition{},
		Feed:        controllerTestFeed(t),
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

	for seqno := snapshot.MasterchainBlock.SeqNo + 1; seqno < floor.SeqNo; seqno++ {
		block := runtimeTestBlockID(-1, -1<<63, seqno)
		if err = controller.ApplyMasterchainState(context.Background(), AppliedMasterchainState{
			Block: block,
			AsOf:  time.Now(),
		}); err != nil {
			t.Fatalf("apply masterchain %d below recovery floor: %v", seqno, err)
		}
	}
	committed, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if committed.MasterchainBlock.SeqNo != floor.SeqNo-1 || tracker.applies != 3 {
		t.Fatalf(
			"pre-floor tracker masterchain=%d applies=%d, want %d/3",
			committed.MasterchainBlock.SeqNo,
			tracker.applies,
			floor.SeqNo-1,
		)
	}
	if controller.recoveryFloor == nil || !sameBlockID(*controller.recoveryFloor, floor) {
		t.Fatalf("pre-floor replay changed recovery floor %v", controller.recoveryFloor)
	}
}

func TestControllerDefersRecoveredObserverActivationUntilExactSnapshotRetry(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	sessionID := snapshot.Active[0].ID
	backend := newControllerTestBackend(local)
	observer := newControllerTestObserver(local)
	observer.recovered = [][32]byte{sessionID}
	observer.activationFailures = 1
	observer.activationErr = ErrAcquisitionNotReady
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
		Feed:        controllerTestFeed(t),
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
	controller.tracker = controllerTestTracker{snapshot: snapshot}
	input := AppliedMasterchainState{Block: snapshot.MasterchainBlock}

	if err = controller.ApplyMasterchainState(context.Background(), input); err != nil {
		t.Fatalf("deferred observer activation blocked masterchain apply: %v", err)
	}
	controller.mu.RLock()
	managed := controller.managed[sessionID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("deferred recovered observer session disappeared")
	}
	managed.mu.Lock()
	reconciled := managed.reconciled
	hasProjection := managed.hasProjection
	hasObserver := managed.hasObserver
	managed.mu.Unlock()
	if reconciled || !hasProjection || !hasObserver {
		t.Fatalf(
			"deferred recovered state reconciled=%t projection=%t observer=%t",
			reconciled,
			hasProjection,
			hasObserver,
		)
	}

	handlers := controller.remoteHandlers()
	query := AuthenticatedQuery{
		SessionID:  sessionID,
		SourceADNL: snapshot.Active[0].Validators[0].ADNL,
	}
	if err = handlers.Probe(
		context.Background(),
		query,
		simplex.ConsensusPleaseCollatePrepare{},
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("probe during deferred activation = %v, want ErrNotFound", err)
	}
	if err = handlers.Commit(
		context.Background(),
		query,
		simplex.ConsensusPleaseCollate{},
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("commit during deferred activation = %v, want ErrNotFound", err)
	}
	if err = controller.handleConsensusProgress(context.Background(), ConsensusProgress{
		SessionID: sessionID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("progress during deferred activation = %v, want ErrNotFound", err)
	}

	if err = controller.ApplyMasterchainState(context.Background(), input); err != nil {
		t.Fatalf("retry exact masterchain snapshot: %v", err)
	}
	managed.mu.Lock()
	reconciled = managed.reconciled
	managed.mu.Unlock()
	if !reconciled {
		t.Fatal("exact snapshot retry did not reconcile recovered observer session")
	}
	if err = handlers.Probe(
		context.Background(),
		query,
		simplex.ConsensusPleaseCollatePrepare{},
	); err != nil {
		t.Fatalf("probe after activation retry: %v", err)
	}

	observer.mu.Lock()
	prepared, activated, updated := observer.prepared, observer.activated, observer.updated
	observer.mu.Unlock()
	if prepared != 1 || activated != 2 || updated != 0 {
		t.Fatalf(
			"recovered observer lifecycle prepare=%d activate=%d update=%d, want 1/2/0",
			prepared,
			activated,
			updated,
		)
	}
}

func TestControllerBoundsRecoveredObserverActivationAndBacksOff(t *testing.T) {
	local := [32]byte{0x31}
	snapshot := controllerTestSnapshot(local)
	snapshot.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 24)
	snapshot.Active[0].MinMasterchain = snapshot.MasterchainBlock
	projected, err := projectCollatorSessions(snapshot, sessionProjectionPolicy{localADNLID: local})
	if err != nil {
		t.Fatal(err)
	}
	sessionID := snapshot.Active[0].ID
	want := projected[sessionID]
	activation := cloneSessionActivation(*want.activation)
	record := SessionRecord{
		Session:    cloneSession(want.session),
		Activation: &activation,
		Update:     cloneSessionUpdate(want.update),
	}
	storage := newRuntimeMemoryStorage()
	storage.sessions[sessionID] = cloneSessionRecord(record)
	backend := newControllerTestBackend(local)
	backend.records[sessionID] = cloneSessionRecord(record)
	observer := newControllerTestObserver(local)
	observer.recovered = [][32]byte{sessionID}
	deadlineObserved := make(chan time.Duration, 1)
	observer.activationHook = func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			deadlineObserved <- 0

			return context.DeadlineExceeded
		}
		deadlineObserved <- time.Until(deadline)

		return context.DeadlineExceeded
	}
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
		Feed:        controllerTestFeed(t),
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
	controller.tracker = controllerTestTracker{snapshot: snapshot}
	input := AppliedMasterchainState{Block: snapshot.MasterchainBlock}

	if err = controller.ApplyMasterchainState(context.Background(), input); err != nil {
		t.Fatalf("bounded recovered activation blocked masterchain apply: %v", err)
	}
	remaining := <-deadlineObserved
	if remaining <= 0 || remaining > recoveredObserverActivationProbeTimeout {
		t.Fatalf("recovered activation deadline remaining = %v", remaining)
	}
	controller.mu.RLock()
	managed := controller.managed[sessionID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("deferred recovered observer session disappeared")
	}
	managed.mu.Lock()
	reconciled := managed.reconciled
	recoveredObserver := managed.recoveredObserver
	retryAt := managed.observerActivationRetryAt
	managed.mu.Unlock()
	if reconciled || !recoveredObserver || !retryAt.After(time.Now()) {
		t.Fatalf(
			"bounded activation reconciled=%t recovered=%t retry_at=%v",
			reconciled,
			recoveredObserver,
			retryAt,
		)
	}
	if controller.recoveryFloor == nil {
		t.Fatal("bounded activation cleared recovery floor")
	}

	if err = controller.ApplyMasterchainState(context.Background(), input); err != nil {
		t.Fatalf("backed-off exact snapshot: %v", err)
	}
	observer.mu.Lock()
	activated := observer.activated
	observer.mu.Unlock()
	if activated != 1 {
		t.Fatalf("observer activated %d times during backoff, want 1", activated)
	}

	managed.mu.Lock()
	managed.observerActivationRetryAt = time.Now().Add(-time.Second)
	managed.mu.Unlock()
	observer.mu.Lock()
	observer.activationHook = nil
	observer.mu.Unlock()
	if err = controller.ApplyMasterchainState(context.Background(), input); err != nil {
		t.Fatalf("activation after backoff: %v", err)
	}
	managed.mu.Lock()
	reconciled = managed.reconciled
	recoveredObserver = managed.recoveredObserver
	managed.mu.Unlock()
	if !reconciled || recoveredObserver {
		t.Fatalf(
			"successful retry reconciled=%t recovered=%t",
			reconciled,
			recoveredObserver,
		)
	}
	if controller.recoveryFloor != nil {
		t.Fatalf("successful retry retained recovery floor %v", controller.recoveryFloor)
	}
}

func TestControllerDefersRecoveredLifecycleUntilNodeCheckpointCatchesUp(t *testing.T) {
	local := [32]byte{0x31}
	active := controllerTestSnapshot(local)
	active.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 24)
	active.Active[0].MinMasterchain = active.MasterchainBlock
	projected, err := projectCollatorSessions(active, sessionProjectionPolicy{localADNLID: local})
	if err != nil {
		t.Fatal(err)
	}
	sessionID := active.Active[0].ID
	want := projected[sessionID]
	activation := cloneSessionActivation(*want.activation)
	record := SessionRecord{
		Session:    cloneSession(want.session),
		Activation: &activation,
		Update:     cloneSessionUpdate(want.update),
	}

	behind := *active
	behind.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 20)
	behind.Future = append([]groups.Session(nil), active.Active...)
	behind.Active = nil

	storage := newRuntimeMemoryStorage()
	storage.sessions[sessionID] = cloneSessionRecord(record)
	backend := newControllerTestBackend(local)
	backend.records[sessionID] = cloneSessionRecord(record)
	observer := newControllerTestObserver(local)
	observer.recovered = [][32]byte{sessionID}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     storage,
		Observer:    observer,
		Tracker:     controllerTestTracker{snapshot: &behind},
		Acquisition: controllerTestAcquisition{},
		Feed:        controllerTestFeed(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatalf("start behind recovered lifecycle floor: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	controller.mu.RLock()
	managed := controller.managed[sessionID]
	controller.mu.RUnlock()
	if managed == nil {
		t.Fatal("recovered session disappeared below lifecycle floor")
	}
	managed.mu.Lock()
	reconciled := managed.reconciled
	hasProjection := managed.hasProjection
	managed.mu.Unlock()
	if reconciled || hasProjection {
		t.Fatalf("behind snapshot reconciled=%t projection=%t", reconciled, hasProjection)
	}
	backend.mu.Lock()
	retired := backend.retired
	backend.mu.Unlock()
	observer.mu.Lock()
	prepared, activated := observer.prepared, observer.activated
	observer.mu.Unlock()
	if retired != 0 || prepared != 0 || activated != 0 {
		t.Fatalf(
			"lifecycle ran below recovery floor: retire=%d prepare=%d activate=%d",
			retired,
			prepared,
			activated,
		)
	}
	if err = controller.remoteHandlers().Probe(context.Background(), AuthenticatedQuery{
		SessionID:  sessionID,
		SourceADNL: active.Active[0].Validators[0].ADNL,
	}, simplex.ConsensusPleaseCollatePrepare{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delegation below recovery floor = %v, want ErrNotFound", err)
	}

	almost := behind
	almost.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 23)
	if err = controller.reconcileSnapshot(context.Background(), &almost); err != nil {
		t.Fatalf("reconcile below recovery floor: %v", err)
	}
	if err = controller.reconcileSnapshot(context.Background(), active); err != nil {
		t.Fatalf("reconcile at recovery floor: %v", err)
	}

	managed.mu.Lock()
	reconciled = managed.reconciled
	hasProjection = managed.hasProjection
	managed.mu.Unlock()
	if !reconciled || !hasProjection {
		t.Fatalf("floor snapshot reconciled=%t projection=%t", reconciled, hasProjection)
	}
	observer.mu.Lock()
	prepared, activated = observer.prepared, observer.activated
	observer.mu.Unlock()
	if prepared != 1 || activated != 1 {
		t.Fatalf("observer lifecycle at floor prepare=%d activate=%d, want 1/1", prepared, activated)
	}
}

func TestControllerDoesNotReplaceAheadRecoveredSessionFromOlderValidatorSet(t *testing.T) {
	local := [32]byte{0x31}
	current := controllerTestSnapshot(local)
	current.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 24)
	current.Active[0].ID = [32]byte{0x42}
	current.Active[0].CatchainSeqno++
	current.Active[0].MinMasterchain = current.MasterchainBlock
	projected, err := projectCollatorSessions(current, sessionProjectionPolicy{localADNLID: local})
	if err != nil {
		t.Fatal(err)
	}
	currentID := current.Active[0].ID
	want := projected[currentID]
	activation := cloneSessionActivation(*want.activation)
	record := SessionRecord{
		Session:    cloneSession(want.session),
		Activation: &activation,
		Update:     cloneSessionUpdate(want.update),
	}

	behind := controllerTestSnapshot(local)
	behind.MasterchainBlock = runtimeTestBlockID(-1, -1<<63, 20)
	behind.Active[0].MinMasterchain = behind.MasterchainBlock
	oldID := behind.Active[0].ID
	if oldID == currentID {
		t.Fatal("test snapshots reused a session id")
	}

	storage := newRuntimeMemoryStorage()
	storage.sessions[currentID] = cloneSessionRecord(record)
	backend := newControllerTestBackend(local)
	backend.records[currentID] = cloneSessionRecord(record)
	observer := newControllerTestObserver(local)
	observer.recovered = [][32]byte{currentID}
	controller, err := NewController(ControllerOptions{
		Backend:     backend,
		Storage:     storage,
		Observer:    observer,
		Tracker:     controllerTestTracker{snapshot: behind},
		Acquisition: controllerTestAcquisition{},
		Feed:        controllerTestFeed(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Start(context.Background()); err != nil {
		t.Fatalf("start behind recovered validator-set transition: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := controller.Close(ctx); closeErr != nil {
			t.Error(closeErr)
		}
	})

	controller.mu.RLock()
	recovered := controller.managed[currentID]
	_, preparedOld := controller.managed[oldID]
	controller.mu.RUnlock()
	if recovered == nil || preparedOld {
		t.Fatalf("behind inventory recovered=%v prepared_old=%t", recovered != nil, preparedOld)
	}
	backend.mu.Lock()
	prepared, retired := backend.prepared, backend.retired
	retained := cloneSessionRecord(backend.records[currentID])
	backend.mu.Unlock()
	observer.mu.Lock()
	observerPrepared, observerRetired := observer.prepared, observer.retired
	observer.mu.Unlock()
	if prepared != 0 || retired != 0 || observerPrepared != 0 || observerRetired != 0 {
		t.Fatalf(
			"older snapshot changed lifecycle: backend prepare=%d retire=%d observer prepare=%d retire=%d",
			prepared,
			retired,
			observerPrepared,
			observerRetired,
		)
	}
	if retained.Activation == nil || !retained.Activation.Equal(activation) {
		t.Fatal("older snapshot replaced recovered activation")
	}

	if err = controller.reconcileSnapshot(context.Background(), current); err != nil {
		t.Fatalf("reconcile recovered validator-set session at floor: %v", err)
	}
	recovered.mu.Lock()
	reconciled := recovered.reconciled
	recovered.mu.Unlock()
	backend.mu.Lock()
	prepared, retired = backend.prepared, backend.retired
	retained = cloneSessionRecord(backend.records[currentID])
	backend.mu.Unlock()
	if !reconciled || prepared != 0 || retired != 0 || retained.Activation == nil ||
		!retained.Activation.Equal(activation) {
		t.Fatalf(
			"floor recovery reconciled=%t prepare=%d retire=%d activation=%v",
			reconciled,
			prepared,
			retired,
			retained.Activation != nil,
		)
	}
}

func TestRecoveredSessionFloorRejectsSameHeightForks(t *testing.T) {
	left := runtimeTestBlockID(-1, -1<<63, 24)
	right := cloneBlockID(left)
	right.FileHash[0]++

	floor, err := recoveredSessionFloor([]SessionRecord{
		{Update: SessionUpdate{MasterchainBlock: left}},
		{Update: SessionUpdate{MasterchainBlock: right}},
	})
	if floor != nil || !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("floor=%v error=%v, want session conflict", floor, err)
	}
}

func TestControllerRejectsRecoveredLifecycleFloorFork(t *testing.T) {
	floor := runtimeTestBlockID(-1, -1<<63, 24)
	controller := &Controller{recoveryFloor: &floor}
	fork := cloneBlockID(floor)
	fork.RootHash[0]++

	ready, err := controller.recoverySnapshotReady(fork)
	if ready || !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("fork ready=%t error=%v, want session conflict", ready, err)
	}
	if controller.recoveryFloor == nil {
		t.Fatal("fork cleared recovery floor")
	}

	newer := runtimeTestBlockID(-1, -1<<63, floor.SeqNo+1)
	ready, err = controller.recoverySnapshotReady(newer)
	if err != nil || !ready || controller.recoveryFloor == nil {
		t.Fatalf("newer checkpoint ready=%t error=%v floor=%v", ready, err, controller.recoveryFloor)
	}
	controller.completeRecoverySnapshot(newer)
	if controller.recoveryFloor != nil {
		t.Fatalf("completed newer checkpoint retained recovery floor %v", controller.recoveryFloor)
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
		Feed:        controllerTestFeed(t),
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
		Feed:        controllerTestFeed(t),
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
		Feed:        controllerTestFeed(t),
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
		Feed:        controllerTestFeed(t),
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

func (b *controllerTestBackend) ObserveConsensusNotarized(shard groups.ShardID, id simplex.CandidateID, at time.Time) {
	if b.notarized != nil {
		b.notarized(shard, id, at)
	}
}
