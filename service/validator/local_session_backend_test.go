package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// localBackendTestNode models the live view the way the real one behaves for the
// two properties the backend depends on: a published state is readable
// immediately and BY REFERENCE, and every publication closes the artifacts
// signal that a waiting read is parked on.
type localBackendTestNode struct {
	mu         sync.Mutex
	states     map[string]*storage.BlockState
	stateCells map[string]*cell.Cell
	blocks     map[string][]byte
	submitted  []p2p.DownloadedBlock
	published  []storage.LiveBlockArtifacts
	blockReads int
	signal     chan struct{}
}

func (n *localBackendTestNode) artifactsSignalLocked() chan struct{} {
	if n.signal == nil {
		n.signal = make(chan struct{})
	}

	return n.signal
}

func (n *localBackendTestNode) BlockArtifactsSignal() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.artifactsSignalLocked()
}

func (n *localBackendTestNode) PublishAcceptedBlockState(artifacts storage.LiveBlockArtifacts) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.published = append(n.published, artifacts)
	key := localBackendTestBlockKey(artifacts.Block)
	if artifacts.State != nil {
		n.states[key] = artifacts.State
		if artifacts.State.Cell != nil {
			n.stateCells[key] = artifacts.State.Cell
		}
	}
	if len(artifacts.BlockData) > 0 {
		n.blocks[key] = artifacts.BlockData
	}
	close(n.artifactsSignalLocked())
	n.signal = make(chan struct{})

	return nil
}

type localBackendTestGroups struct {
	snapshot *groups.Snapshot
}

func (g localBackendTestGroups) Snapshot() (*groups.Snapshot, error) {
	return g.snapshot, nil
}

func (g localBackendTestGroups) Project(
	*groups.Snapshot,
	groups.ApplyInput,
) (*groups.Snapshot, error) {
	return g.snapshot, nil
}

func (g localBackendTestGroups) WaitProject(
	ctx context.Context,
	previous *groups.Snapshot,
	input groups.ApplyInput,
) (*groups.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return g.Project(previous, input)
}

func TestLocalSessionBackendWaitValidationViewResumesOnPublication(t *testing.T) {
	backend := &LocalSessionBackend{
		activation:        &collator.SessionActivation{},
		validationChanged: make(chan struct{}),
	}
	result := make(chan *localValidationView, 1)
	failure := make(chan error, 1)
	go func() {
		view, err := backend.waitValidationView(t.Context())
		if err != nil {
			failure <- err
			return
		}
		result <- view
	}()

	select {
	case <-result:
		t.Fatal("validation view was returned before publication")
	case err := <-failure:
		t.Fatalf("validation view wait failed before publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	backend.controlMu.Lock()
	backend.publishValidationView()
	backend.controlMu.Unlock()

	select {
	case view := <-result:
		if view == nil {
			t.Fatal("published validation view is nil")
		}
	case err := <-failure:
		t.Fatalf("validation view wait after publication: %v", err)
	case <-time.After(time.Second):
		t.Fatal("validation view wait did not resume after publication")
	}
}

func TestLocalSessionBackendWaitValidationViewStopsOnClose(t *testing.T) {
	backend := &LocalSessionBackend{validationChanged: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := backend.waitValidationView(t.Context())
		result <- err
	}()

	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrLocalSessionBackendClosed) {
			t.Fatalf("validation view wait error = %v, want closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("validation view wait did not stop with the backend")
	}
}

func (n *localBackendTestNode) BlockData(_ context.Context, id ton.BlockIDExt) ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.blockReads++
	data, exists := n.blocks[localBackendTestBlockKey(id)]
	if !exists {
		return nil, storage.ErrNotFound
	}

	return data, nil
}

func (*localBackendTestNode) BlockProof(
	context.Context,
	storage.ServedProofKind,
	ton.BlockIDExt,
) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (n *localBackendTestNode) BlockState(_ context.Context, id ton.BlockIDExt) (*storage.BlockState, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	state, exists := n.states[localBackendTestBlockKey(id)]
	if !exists {
		return nil, storage.ErrNotFound
	}

	return state, nil
}

func (n *localBackendTestNode) LoadStateCellTree(
	_ context.Context,
	id ton.BlockIDExt,
	_ []byte,
) (*cell.Cell, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	root, exists := n.stateCells[localBackendTestBlockKey(id)]
	if !exists {
		return nil, storage.ErrNotFound
	}

	return root, nil
}

func (*localBackendTestNode) LookupBlockBySeqNo(
	context.Context,
	storage.BlockSeqRef,
) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (n *localBackendTestNode) SubmitBlockLocally(block p2p.DownloadedBlock) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.submitted = append(n.submitted, block)
}

type localBackendTestCollator struct {
	mu sync.Mutex

	id            [32]byte
	record        *collator.SessionRecord
	sessionErr    error
	probeErr      error
	commitErr     error
	commit        func(context.Context, collator.WindowRequest) error
	selfErr       error
	updateErr     error
	progressErr   error
	retireErr     error
	prepareCalls  []collator.SessionRecord
	activateCalls []collator.SessionActivation
	updateCalls   []collator.SessionUpdate
	probeCalls    []collator.WindowPreparation
	commitCalls   []collator.WindowRequest
	selfCalls     []collator.SelfWindowRequest
	selfDeadlines []time.Time
	retireCalls   [][32]byte
	progressCalls []collator.ConsensusProgress
	updateEntered chan struct{}
	updateRelease <-chan struct{}
}

// localBackendAdvancingTestCollator models the collator's durable monotonic
// session-update fence. It turns a lost producer-local observation into the
// same ErrSessionConflict returned by the production service.
type localBackendAdvancingTestCollator struct {
	*localBackendTestCollator
	update collator.SessionUpdate
}

func (c *localBackendAdvancingTestCollator) UpdateSession(
	ctx context.Context,
	update collator.SessionUpdate,
) error {
	if err := collator.ValidateSessionUpdateAdvance(c.update, update); err != nil {
		return err
	}
	if err := c.localBackendTestCollator.UpdateSession(ctx, update); err != nil {
		return err
	}
	c.update = update

	return nil
}

func (c *localBackendAdvancingTestCollator) ApplyConsensusProgress(
	ctx context.Context,
	progress collator.ConsensusProgress,
) error {
	if err := c.localBackendTestCollator.ApplyConsensusProgress(ctx, progress); err != nil {
		return err
	}

	next := c.update
	next.HasCurrentWindow = true
	next.CurrentWindowStart = progress.Window.StartSlot
	next.CurrentWindowObservedSlot = progress.Window.ObservedSlot
	if c.update.HasCurrentWindow && c.update.CurrentWindowStart == progress.Window.StartSlot {
		next.CurrentWindowStartAt = c.update.CurrentWindowStartAt
	} else if progress.Window.ObservedSlot == progress.Window.StartSlot {
		next.CurrentWindowStartAt = progress.StartAt
	} else {
		next.CurrentWindowStartAt = time.Time{}
	}
	next.CurrentBase = progress.Window.Base
	if err := collator.ValidateSessionUpdateAdvance(c.update, next); err != nil {
		return err
	}
	c.update = next

	return nil
}

func (c *localBackendTestCollator) CollatorID() [32]byte      { return c.id }
func (*localBackendTestCollator) Start(context.Context) error { return nil }
func (*localBackendTestCollator) Close(context.Context) error { return nil }

func (c *localBackendTestCollator) Session(
	context.Context,
	[32]byte,
) (collator.SessionRecord, error) {
	if c.sessionErr != nil {
		return collator.SessionRecord{}, c.sessionErr
	}
	if c.record == nil {
		return collator.SessionRecord{}, collator.ErrNotFound
	}

	return *c.record, nil
}

func (c *localBackendTestCollator) PrepareSession(
	_ context.Context,
	session collator.Session,
	update collator.SessionUpdate,
) error {
	c.prepareCalls = append(c.prepareCalls, collator.SessionRecord{Session: session, Update: update})
	return nil
}

func (c *localBackendTestCollator) UpdateSession(
	ctx context.Context,
	update collator.SessionUpdate,
) error {
	c.updateCalls = append(c.updateCalls, update)
	if c.updateEntered != nil {
		select {
		case c.updateEntered <- struct{}{}:
		default:
		}
	}
	if c.updateRelease != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.updateRelease:
		}
	}
	return c.updateErr
}

func (c *localBackendTestCollator) ActivateSession(
	_ context.Context,
	activation collator.SessionActivation,
) error {
	c.activateCalls = append(c.activateCalls, activation)
	return nil
}

func (c *localBackendTestCollator) RetireSession(_ context.Context, id [32]byte) error {
	c.retireCalls = append(c.retireCalls, id)
	return c.retireErr
}

func (c *localBackendTestCollator) Probe(
	_ context.Context,
	window collator.WindowPreparation,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.probeCalls = append(c.probeCalls, window)
	return c.probeErr
}

func (c *localBackendTestCollator) CommitDelegation(
	ctx context.Context,
	window collator.WindowRequest,
) error {
	c.mu.Lock()
	c.commitCalls = append(c.commitCalls, window)
	commit := c.commit
	err := c.commitErr
	c.mu.Unlock()
	if commit != nil {
		return commit(ctx, window)
	}

	return err
}

func (c *localBackendTestCollator) ActivateSelfWindow(
	ctx context.Context,
	window collator.SelfWindowRequest,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selfCalls = append(c.selfCalls, window)
	deadline, _ := ctx.Deadline()
	c.selfDeadlines = append(c.selfDeadlines, deadline)

	return c.selfErr
}

func (c *localBackendTestCollator) delegationCallSnapshot() (
	[]collator.WindowPreparation,
	[]collator.WindowRequest,
	[]collator.SelfWindowRequest,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]collator.WindowPreparation(nil), c.probeCalls...),
		append([]collator.WindowRequest(nil), c.commitCalls...),
		append([]collator.SelfWindowRequest(nil), c.selfCalls...)
}

func (c *localBackendTestCollator) ApplyConsensusProgress(
	_ context.Context,
	progress collator.ConsensusProgress,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progressCalls = append(c.progressCalls, progress)

	return c.progressErr
}

func (*localBackendTestCollator) ObserveConsensusFinalized(
	context.Context,
	[32]byte,
	ton.BlockIDExt,
) error {
	return nil
}

func (*localBackendTestCollator) Status(context.Context) (collator.Status, error) {
	return collator.Status{}, nil
}

type localBackendTestSigner struct {
	key ed25519.PrivateKey
}

func (s localBackendTestSigner) Sign(data []byte) ([]byte, error) {
	return ed25519.Sign(s.key, data), nil
}

type localBackendRecordingSigner struct {
	key   ed25519.PrivateKey
	calls int
}

func (s *localBackendRecordingSigner) Sign(data []byte) ([]byte, error) {
	s.calls++

	return ed25519.Sign(s.key, data), nil
}

type localBackendInvalidSigner struct {
	calls int
}

func (s *localBackendInvalidSigner) Sign([]byte) ([]byte, error) {
	s.calls++

	return []byte{0x01}, nil
}

type localBackendTestDelegations struct {
	storage *runtimeTestStorage
	save    func(context.Context, SessionStorageID, DelegationAuthorization, func(error))
	load    func(context.Context, SessionStorageID, uint32) (DelegationAuthorization, error)
}

func newLocalBackendTestDelegations() *localBackendTestDelegations {
	return &localBackendTestDelegations{storage: newRuntimeTestStorage()}
}

func (s *localBackendTestDelegations) SaveDelegationAuthorization(
	ctx context.Context,
	session SessionStorageID,
	authorization DelegationAuthorization,
	done func(error),
) {
	if s.save != nil {
		s.save(ctx, session, authorization, done)

		return
	}
	s.storage.SaveDelegationAuthorization(ctx, session, authorization, done)
}

func (s *localBackendTestDelegations) DelegationAuthorization(
	ctx context.Context,
	session SessionStorageID,
	start uint32,
) (DelegationAuthorization, error) {
	if s.load != nil {
		return s.load(ctx, session, start)
	}

	return s.storage.DelegationAuthorization(ctx, session, start)
}

type localBackendPendingDelegationSave struct {
	session       SessionStorageID
	authorization DelegationAuthorization
	done          func(error)
}

type localBackendProductionTestFixture struct {
	backend    *LocalSessionBackend
	producer   *localBackendTestCollator
	config     SessionConfig
	privateKey ed25519.PrivateKey
	signer     *localBackendRecordingSigner
}

func newLocalBackendProductionTestFixture(
	t *testing.T,
	mode collator.ProductionMode,
	delegations DelegationAuthorizationStorage,
) *localBackendProductionTestFixture {
	t.Helper()

	config, _, state := localBackendTestRuntimeInputs(t)
	baseSigner, ok := config.Identity.Validator.Signer.(localBackendTestSigner)
	if !ok {
		t.Fatal("runtime fixture validator signer has unexpected type")
	}
	signer := &localBackendRecordingSigner{key: baseSigner.key}
	config.Identity.Validator.Signer = signer
	producer := &localBackendTestCollator{id: [32]byte{0x96}}
	backend := &LocalSessionBackend{
		config:         config,
		delegations:    delegations,
		collator:       producer,
		productionMode: mode,
		validator:      config.Identity.Validator,
		state:          state,
		update:         localCollatorUpdate(config.SessionID, state),
		closeAfter:     time.Second,
	}
	if mode == collator.ProductionModeSelf {
		backend.self = producer
	}

	return &localBackendProductionTestFixture{
		backend:    backend,
		producer:   producer,
		config:     config,
		privateKey: baseSigner.key,
		signer:     signer,
	}
}

func (f *localBackendProductionTestFixture) window(start, observed uint32) LeaderWindow {
	windowSize := f.config.Protocol.SlotsPerLeaderWindow
	leader := start / windowSize % uint32(len(f.config.Validators))

	return LeaderWindow{
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			StartSlot:    start,
			EndSlot:      start + windowSize,
			ObservedSlot: observed,
			Leader:       leader,
			LocalLeader:  leader == f.config.Identity.Validator.Index,
		},
		StartAt: time.Unix(240, 0),
		Submit:  func(context.Context, *CandidateArtifact) error { return nil },
	}
}

func TestLocalSessionBackendLoadsExactOrdinaryAndZeroStateTips(t *testing.T) {
	ordinaryState := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	ordinaryShard := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	ordinaryBlock := acceptanceTestBlockRoot(t, ordinaryShard, 7, 0x11223344)
	ordinaryBOC := ordinaryBlock.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	ordinary := localBackendTestBlockID(ordinaryShard.Workchain, ordinaryShard.Shard, 2, ordinaryBlock.Hash(), ordinaryBOC)

	zeroState := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	zeroBOC := zeroState.ToBOC()
	zero := localBackendTestBlockID(0, -1<<63, 0, zeroState.Hash(), zeroBOC)
	node := &localBackendTestNode{
		states: map[string]*storage.BlockState{
			localBackendTestBlockKey(ordinary): {
				Block:         ordinary,
				StateRootHash: ordinaryState.Hash(),
			},
			localBackendTestBlockKey(zero): {
				Block:         zero,
				StateRootHash: zeroState.Hash(),
				StateFileHash: zero.FileHash,
				Cell:          zeroState,
			},
		},
		stateCells: map[string]*cell.Cell{localBackendTestBlockKey(ordinary): ordinaryState},
		blocks:     map[string][]byte{localBackendTestBlockKey(ordinary): ordinaryBOC},
	}
	backend := &LocalSessionBackend{node: node}

	ordinaryData, err := backend.LoadChainState(context.Background(), ChainStateRequest{Blocks: []ton.BlockIDExt{ordinary}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ordinaryData.Tips) != 1 || ordinaryData.Tips[0].State != ordinaryState ||
		!bytes.Equal(ordinaryData.Tips[0].BlockBOC, ordinaryBOC) {
		t.Fatalf("ordinary tip = %+v", ordinaryData.Tips)
	}
	if &ordinaryData.Tips[0].BlockBOC[0] != &ordinaryBOC[0] {
		t.Fatal("ordinary block BOC was copied")
	}
	// The loader already decoded and hash-checked this root for its own identity
	// check; handing it over is what spares every validation a second decode.
	if ordinaryData.Tips[0].Block == nil ||
		!bytes.Equal(ordinaryData.Tips[0].Block.Hash(), ordinary.RootHash) {
		t.Fatalf("ordinary tip block root = %+v", ordinaryData.Tips[0].Block)
	}

	zeroData, err := backend.LoadChainState(context.Background(), ChainStateRequest{Blocks: []ton.BlockIDExt{zero}})
	if err != nil {
		t.Fatal(err)
	}
	if len(zeroData.Tips) != 1 || zeroData.Tips[0].State != zeroState || zeroData.Tips[0].BlockBOC != nil {
		t.Fatalf("zero tip = %+v", zeroData.Tips)
	}
	// A zerostate has no block cell at all, and newChainState rejects a tip that
	// claims otherwise.
	if zeroData.Tips[0].Block != nil {
		t.Fatal("zerostate tip carries a block root")
	}
	if node.blockReads != 1 {
		t.Fatalf("ordinary/zero block reads = %d, want only the ordinary block read", node.blockReads)
	}
}

func TestNewLocalSessionBackendSeparatesCloseFromRetirement(t *testing.T) {
	config, start, initial := localBackendTestRuntimeInputs(t)
	producer := &localBackendTestCollator{id: [32]byte{0x81}, sessionErr: collator.ErrNotFound}
	router := NewLocalCandidateRouter()
	node := &localBackendTestNode{
		states:     make(map[string]*storage.BlockState),
		stateCells: make(map[string]*cell.Cell),
		blocks:     make(map[string][]byte),
	}
	backend, err := NewLocalSessionBackend(context.Background(), LocalSessionBackendOptions{
		Config:          config,
		Initial:         initial,
		Node:            node,
		Groups:          localBackendTestGroups{snapshot: &groups.Snapshot{}},
		Storage:         newRuntimeTestStorage(),
		Acquisition:     &collator.LocalAcquisition{},
		Collator:        producer,
		ProductionMode:  collator.ProductionModeSelf,
		CandidateRouter: router,
		CloseTimeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = backend.ActivateSession(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	if _, err = router.Register(
		config.SessionID,
		func(context.Context, collator.CandidateArtifact) error { return nil },
	); !errors.Is(err, ErrCandidateRouteConflict) {
		t.Fatalf("active local candidate route error = %v, want ErrCandidateRouteConflict", err)
	}
	if len(producer.prepareCalls) != 1 || producer.prepareCalls[0].Session.ID != config.SessionID {
		t.Fatalf("prepared collator records = %+v", producer.prepareCalls)
	}
	if producer.prepareCalls[0].Update.TargetRate != initial.Params.TargetRate {
		t.Fatalf(
			"prepared target rate = %v, want %v",
			producer.prepareCalls[0].Update.TargetRate,
			initial.Params.TargetRate,
		)
	}

	if err = backend.Close(); err != nil {
		t.Fatal(err)
	}
	if len(producer.retireCalls) != 0 {
		t.Fatalf("runtime close retired %d collator sessions", len(producer.retireCalls))
	}
	if err = backend.Retire(); err != nil {
		t.Fatal(err)
	}
	if len(producer.retireCalls) != 1 || producer.retireCalls[0] != config.SessionID {
		t.Fatalf("retired sessions = %x, want %x", producer.retireCalls, config.SessionID)
	}
	if err = router.EmitCandidate(context.Background(), collator.CandidateArtifact{
		SessionID: config.SessionID,
		WindowID:  collator.WindowID{SessionID: config.SessionID},
	}); !errors.Is(err, ErrCandidateRouteNotFound) {
		t.Fatalf("candidate route after close = %v, want ErrCandidateRouteNotFound", err)
	}
}

func TestNewLocalSessionBackendAllowsRemoteVotingWithoutCandidateRouter(t *testing.T) {
	backend, producer := newLocalBackendTestRemoteVotingBackend(t)
	if len(producer.prepareCalls) != 1 {
		t.Fatalf("prepared remote collator sessions = %d, want 1", len(producer.prepareCalls))
	}

	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewLocalSessionBackendRejectsInvalidProductionModeRouterCombinations(t *testing.T) {
	t.Run("self without router", func(t *testing.T) {
		options := localBackendTestConstructorOptions(t)
		options.ProductionMode = collator.ProductionModeSelf

		if _, err := NewLocalSessionBackend(context.Background(), options); err == nil {
			t.Fatal("self production without candidate router was accepted")
		}
	})

	t.Run("delegated with router", func(t *testing.T) {
		options := localBackendTestConstructorOptions(t)
		options.ProductionMode = collator.ProductionModeDelegated
		options.Delegations = newRuntimeTestStorage()
		options.CandidateRouter = NewLocalCandidateRouter()

		if _, err := NewLocalSessionBackend(context.Background(), options); err == nil {
			t.Fatal("delegated production with candidate router was accepted")
		}
	})

	t.Run("unspecified mode", func(t *testing.T) {
		options := localBackendTestConstructorOptions(t)

		if _, err := NewLocalSessionBackend(context.Background(), options); err == nil {
			t.Fatal("validator production without an explicit mode was accepted")
		}
	})
}

func TestLocalSessionBackendValidationDoesNotWaitForCollatorUpdate(t *testing.T) {
	backend, producer := newLocalBackendTestRemoteVotingBackend(t)
	defer func() {
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	producer.updateEntered = entered
	producer.updateRelease = release

	next := cloneSessionState(backend.state)
	next.MasterchainBlock.SeqNo++
	updated := make(chan error, 1)
	go func() {
		updated <- backend.UpdateSession(context.Background(), next)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("collator update did not start")
	}
	validated := make(chan error, 1)
	go func() {
		_, err := backend.ValidateCandidate(context.Background(), CandidateValidationRequest{Parent: &ChainState{}, Artifact: &CandidateArtifact{}})
		validated <- err
	}()
	select {
	case err := <-validated:
		if !errors.Is(err, ErrCandidateRejected) {
			t.Fatalf("validation error = %v, want ErrCandidateRejected", err)
		}
	case <-time.After(time.Second):
		t.Fatal("candidate validation waited for the collator update")
	}

	close(release)
	if err := <-updated; err != nil {
		t.Fatal(err)
	}
	view := backend.validation.Load()
	if view == nil || view.update.MasterchainBlock.SeqNo != next.MasterchainBlock.SeqNo {
		t.Fatal("committed collator update was not published to candidate validation")
	}
}

// The predecessor root a validation hands the collator comes from ChainTip.Block,
// which every producer of a tip already parsed — the store loader, the applied
// path and, since the successor of a validated candidate carries its own block
// root, the speculative path too. Nothing re-decodes BlockBOC here any more, so
// a tip whose BOC bytes are unreadable still validates, and a tip without a
// parsed root is a local plumbing fault rather than another decode: skipping the
// block half of predecessor verification silently is the failure this refuses.
func TestLocalSessionBackendValidateCandidateDoesNotDecodePredecessorBOC(t *testing.T) {
	backend, _ := newLocalBackendTestRemoteVotingBackend(t)
	defer func() {
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Three predecessors, so the verdict quotes a count only the acquisition
	// knows: reaching it is what the test is about, and no predecessor of it
	// counts anything.
	state := &ChainState{tips: make([]ChainTip, 3)}
	for i := range state.tips {
		blockRoot := cell.BeginCell().MustStoreUInt(uint64(0x77+i), 8).EndCell()
		state.tips[i] = ChainTip{
			ID: ton.BlockIDExt{
				Workchain: 0,
				Shard:     math.MinInt64,
				SeqNo:     uint32(9 + i),
				RootHash:  blockRoot.Hash(),
				FileHash:  bytes.Repeat([]byte{byte(0x81 + i)}, 32),
			},
			BlockBOC: []byte{0xde, 0xad, 0xbe, 0xef},
			Block:    blockRoot,
			State:    cell.BeginCell().MustStoreUInt(uint64(0x91+i), 8).EndCell(),
		}
	}

	_, err := backend.ValidateCandidate(context.Background(), CandidateValidationRequest{Parent: state, Artifact: &CandidateArtifact{}})
	if err == nil {
		t.Fatal("a zero candidate was validated")
	}
	if strings.Contains(err.Error(), "decode predecessor") {
		t.Fatalf("predecessor BOC was decoded: %v", err)
	}
	if !errors.Is(err, ErrCandidateRejected) || !strings.Contains(err.Error(), "3 predecessors") {
		t.Fatalf("validation error = %v, want the acquisition verdict on 3 predecessors", err)
	}

	// Negative control: the acquisition is only reached because the roots were
	// there. Without one, the call stops before it — loudly, naming the missing
	// parse, and without decoding anything.
	state.tips[0].Block = nil
	_, err = backend.ValidateCandidate(context.Background(), CandidateValidationRequest{Parent: state, Artifact: &CandidateArtifact{}})
	if err == nil || !strings.Contains(err.Error(), "carries no parsed block") {
		t.Fatalf("validation without a parsed predecessor root error = %v, want a plumbing failure", err)
	}
	if strings.Contains(err.Error(), "decode predecessor") || strings.Contains(err.Error(), "3 predecessors") {
		t.Fatalf("a tip without a parsed root was decoded or passed on: %v", err)
	}
	if errors.Is(err, ErrCandidateRejected) {
		t.Fatalf("a missing local parse was classified as a candidate rejection: %v", err)
	}
}

func TestLocalSessionBackendAcceptBlockAfterClose(t *testing.T) {
	backend, _ := newLocalBackendTestRemoteVotingBackend(t)
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	err := backend.AcceptBlock(context.Background(), BlockAcceptance{})
	if !errors.Is(err, ErrLocalSessionBackendClosed) {
		t.Fatalf("AcceptBlock after Close error = %v, want ErrLocalSessionBackendClosed", err)
	}
}

func TestLocalSessionBackendDoesNotReplayFinalizationIntoDeferredCollator(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{
		Workchain: 0,
		Shard:     math.MinInt64,
	})
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.Replay = true
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatal(err)
	}
	view := fixture.view(t)
	observed := 0
	backend := &LocalSessionBackend{
		config: fixture.config,
		state: SessionState{
			MasterchainBlock: view.MasterchainBlock,
			Registered:       view.Registered,
		},
		groups: localBackendTestGroups{snapshot: &groups.Snapshot{
			MasterchainBlock: view.MasterchainBlock,
			Active: []groups.Session{{
				Shard:      fixture.config.Shard,
				Registered: view.Registered,
			}},
		}},
		accepter: accepter,
		finalized: func(context.Context, ton.BlockIDExt) error {
			observed++
			return collator.ErrSessionUnavailable
		},
	}
	backend.collatorDeferred.Store(true)

	if err = backend.AcceptBlock(context.Background(), acceptance); err != nil {
		t.Fatalf("accept replayed block: %v", err)
	}
	if observed != 0 {
		t.Fatalf("deferred collator observed %d replayed finalizations, want 0", observed)
	}
}

func TestLocalSessionBackendAcceptsFromLatestAppliedShardRegistry(t *testing.T) {
	base := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: math.MinInt64})
	registered := acceptanceTestBlockID(base.config.Shard, 9, 0xa1)
	latest := acceptanceTestSuccessorFixture(t, base, 10, registered)
	view := latest.view(t)
	view.Registered[0].Block = registered

	stale := view
	stale.Registered = append([]groups.ShardDescription(nil), view.Registered...)
	stale.Registered[0].Block = acceptanceTestBlockID(base.config.Shard, 1, 0xb1)
	base.config.Identity.Validator = nil

	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := NewLocalSessionBackend(t.Context(), LocalSessionBackendOptions{
		Config:  base.config,
		Initial: SessionState{MasterchainBlock: stale.MasterchainBlock, Registered: stale.Registered},
		Node:    &acceptanceTestNode{},
		Groups: localBackendTestGroups{snapshot: &groups.Snapshot{
			MasterchainBlock: view.MasterchainBlock,
			Active: []groups.Session{{
				Shard:      base.config.Shard,
				Registered: view.Registered,
			}},
		}},
		Storage:   newRuntimeTestStorage(),
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()

	if err = backend.AcceptBlock(t.Context(), latest.acceptance(simplex.VoteFinalize, false)); err != nil {
		t.Fatalf("accept with latest applied shard registry: %v", err)
	}
	if inbox.Len() != 1 {
		t.Fatalf("installed shard tops = %d, want 1", inbox.Len())
	}
}

func TestLocalSessionBackendMapsMissingExactStateToNotReady(t *testing.T) {
	backend := &LocalSessionBackend{node: &localBackendTestNode{states: map[string]*storage.BlockState{}}}
	_, err := backend.LoadChainState(context.Background(), ChainStateRequest{
		Blocks: []ton.BlockIDExt{localBackendTestBlockID(0, -1<<63, 9, bytes.Repeat([]byte{1}, 32), nil)},
	})
	if !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("missing chain state error = %v, want ErrBlockNotReady", err)
	}
}

func TestPrepareLocalCollatorSessionPreparesMissingSession(t *testing.T) {
	producer := &localBackendTestCollator{sessionErr: collator.ErrNotFound}
	session, update := localBackendTestCollatorRecord()

	got, err := prepareLocalCollatorSession(context.Background(), producer, session, update)
	if err != nil {
		t.Fatal(err)
	}
	if len(producer.prepareCalls) != 1 || len(producer.updateCalls) != 0 {
		t.Fatalf("prepare/update calls = %d/%d, want 1/0", len(producer.prepareCalls), len(producer.updateCalls))
	}
	if !got.ready || got.update.TargetRate != update.TargetRate ||
		!sameBlockID(got.update.MasterchainBlock, update.MasterchainBlock) {
		t.Fatalf("prepared update = %+v, want %+v", got, update)
	}
}

func TestPrepareLocalCollatorSessionReopensRetiredGeneration(t *testing.T) {
	producer := &localBackendTestCollator{sessionErr: collator.ErrSessionRetired}
	session, update := localBackendTestCollatorRecord()

	got, err := prepareLocalCollatorSession(context.Background(), producer, session, update)
	if err != nil {
		t.Fatal(err)
	}
	if len(producer.prepareCalls) != 1 || len(producer.updateCalls) != 0 {
		t.Fatalf("prepare/update calls = %d/%d, want 1/0", len(producer.prepareCalls), len(producer.updateCalls))
	}
	if !got.ready || !got.update.Equal(update) {
		t.Fatalf("reopened update = %+v, want %+v", got, update)
	}
}

func TestPrepareLocalCollatorSessionPreservesRecoveredProgress(t *testing.T) {
	producer := &localBackendTestCollator{}
	session, latest := localBackendTestCollatorRecord()
	recovered := latest
	recovered.TargetRate = 50 * time.Millisecond
	recovered.HasFinalizedBlock = true
	recovered.FinalizedBlock = localBackendTestBlockID(
		session.Shard.Workchain,
		session.Shard.Shard,
		7,
		bytes.Repeat([]byte{0x92}, 32),
		nil,
	)
	recovered.HasCurrentWindow = true
	recovered.CurrentWindowStart = 12
	recovered.CurrentWindowStartAt = time.Unix(100, 0)
	recovered.CurrentBase = simplex.Parent(simplex.CandidateID{Slot: 11, Hash: [32]byte{0x91}})
	producer.record = &collator.SessionRecord{Session: session, Update: recovered}

	got, err := prepareLocalCollatorSession(context.Background(), producer, session, latest)
	if err != nil {
		t.Fatal(err)
	}
	if len(producer.prepareCalls) != 0 || len(producer.updateCalls) != 1 {
		t.Fatalf("prepare/update calls = %d/%d, want 0/1", len(producer.prepareCalls), len(producer.updateCalls))
	}
	if !got.ready || got.update.TargetRate != latest.TargetRate || !got.update.HasFinalizedBlock ||
		!sameBlockID(got.update.FinalizedBlock, recovered.FinalizedBlock) || !got.update.HasCurrentWindow ||
		got.update.CurrentWindowStart != 12 ||
		!got.update.CurrentWindowStartAt.Equal(recovered.CurrentWindowStartAt) ||
		got.update.CurrentBase != recovered.CurrentBase {
		t.Fatalf("reconciled update = %+v", got)
	}
}

func TestPrepareLocalCollatorSessionRetriesExactRecoveredUpdate(t *testing.T) {
	retryErr := errors.New("retry durable collator update")
	producer := &localBackendTestCollator{updateErr: retryErr}
	session, update := localBackendTestCollatorRecord()
	producer.record = &collator.SessionRecord{Session: session, Update: update}

	_, err := prepareLocalCollatorSession(context.Background(), producer, session, update)
	if !errors.Is(err, retryErr) {
		t.Fatalf("exact recovered update error = %v, want %v", err, retryErr)
	}
	if len(producer.prepareCalls) != 0 || len(producer.updateCalls) != 1 {
		t.Fatalf("prepare/update calls = %d/%d, want 0/1", len(producer.prepareCalls), len(producer.updateCalls))
	}
}

func TestPrepareLocalCollatorSessionDefersRecoveredMasterchainView(t *testing.T) {
	producer := &localBackendTestCollator{}
	session, recovered := localBackendTestCollatorRecord()
	latest := recovered
	latest.MasterchainBlock.SeqNo--
	latest.MasterchainBlock.RootHash = bytes.Repeat([]byte{0x22}, 32)
	producer.record = &collator.SessionRecord{Session: session, Update: recovered}

	got, err := prepareLocalCollatorSession(context.Background(), producer, session, latest)
	if err != nil {
		t.Fatal(err)
	}
	if got.ready || !got.update.Equal(recovered) {
		t.Fatalf("deferred preparation = %+v, want recovered update", got)
	}
	if len(producer.prepareCalls) != 0 || len(producer.updateCalls) != 0 {
		t.Fatal("ahead recovered session was mutated")
	}
}

func TestLocalSessionBackendActivatesDeferredCollatorAfterChainCatchesUp(t *testing.T) {
	config, start, initial := localBackendTestRuntimeInputs(t)
	recoveredState := cloneSessionState(initial)
	recoveredState.MasterchainBlock.SeqNo += 2
	recoveredState.MasterchainBlock.RootHash = bytes.Repeat([]byte{0x91}, 32)
	recoveredState.MasterchainBlock.FileHash = bytes.Repeat([]byte{0x92}, 32)
	recovered := localCollatorUpdate(config.SessionID, recoveredState)
	producer := &localBackendTestCollator{
		id: [32]byte{0x81},
		record: &collator.SessionRecord{
			Session: localCollatorSession(config),
			Update:  recovered,
		},
	}
	backend, err := NewLocalSessionBackend(context.Background(), LocalSessionBackendOptions{
		Config:         config,
		Initial:        initial,
		Node:           &localBackendTestNode{},
		Groups:         localBackendTestGroups{snapshot: &groups.Snapshot{}},
		Storage:        newRuntimeTestStorage(),
		Acquisition:    &collator.LocalAcquisition{},
		Collator:       producer,
		ProductionMode: collator.ProductionModeDelegated,
		Delegations:    newRuntimeTestStorage(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	if !backend.collatorDeferred.Load() {
		t.Fatal("collator with an ahead recovered view is ready")
	}
	if err = backend.ActivateSession(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	if len(producer.activateCalls) != 0 || backend.validation.Load() != nil {
		t.Fatal("deferred collator was activated before its masterchain view became local")
	}
	validated := make(chan error, 1)
	go func() {
		_, validationErr := backend.ValidateCandidate(
			t.Context(),
			CandidateValidationRequest{Parent: &ChainState{}, Artifact: &CandidateArtifact{}},
		)
		validated <- validationErr
	}()
	select {
	case err = <-validated:
		t.Fatalf("validation returned before the recovered view arrived: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	intermediate := cloneSessionState(initial)
	intermediate.MasterchainBlock.SeqNo++
	intermediate.MasterchainBlock.RootHash = bytes.Repeat([]byte{0x81}, 32)
	intermediate.MasterchainBlock.FileHash = bytes.Repeat([]byte{0x82}, 32)
	if err = backend.UpdateSession(context.Background(), intermediate); err != nil {
		t.Fatal(err)
	}
	if len(producer.updateCalls) != 0 || len(producer.activateCalls) != 0 {
		t.Fatal("collator was mutated while its recovered masterchain view was still ahead")
	}
	select {
	case err = <-validated:
		t.Fatalf("validation returned on an intermediate view: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err = backend.UpdateSession(context.Background(), recoveredState); err != nil {
		t.Fatal(err)
	}
	if backend.collatorDeferred.Load() || len(producer.activateCalls) != 1 {
		t.Fatalf("collator deferred/activation = %v/%d, want false/1", backend.collatorDeferred.Load(), len(producer.activateCalls))
	}
	if len(producer.updateCalls) != 1 {
		t.Fatalf("exact recovered view update attempts = %d, want 1", len(producer.updateCalls))
	}
	select {
	case err = <-validated:
		if !errors.Is(err, ErrCandidateRejected) {
			t.Fatalf("validation after recovered view = %v, want candidate rejection", err)
		}
	case <-time.After(time.Second):
		t.Fatal("validation did not resume after the recovered view arrived")
	}
	view := backend.validation.Load()
	if view == nil || !view.update.Equal(recovered) {
		t.Fatalf("published recovered validation view = %+v", view)
	}
}

func TestPrepareLocalCollatorSessionRejectsRecoveredImmutableConflict(t *testing.T) {
	producer := &localBackendTestCollator{}
	session, update := localBackendTestCollatorRecord()
	conflict := session
	conflict.CatchainSeqno++
	producer.record = &collator.SessionRecord{Session: conflict, Update: update}

	_, err := prepareLocalCollatorSession(context.Background(), producer, session, update)
	if !errors.Is(err, collator.ErrSessionConflict) {
		t.Fatalf("immutable recovery error = %v, want ErrSessionConflict", err)
	}
	if len(producer.prepareCalls) != 0 || len(producer.updateCalls) != 0 {
		t.Fatal("conflicting recovered session was mutated")
	}
}

func TestLocalSessionBackendSelfModeActivatesAndRoutesValidatorCandidate(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var validatorKey [ed25519.PublicKeySize]byte
	copy(validatorKey[:], publicKey)
	signer := &localBackendRecordingSigner{key: privateKey}
	identity := &ValidatorIdentity{Index: 0, Signer: signer}
	config := SessionConfig{
		SessionID:  [32]byte{0x61},
		Validators: []groups.Validator{{PublicKey: validatorKey, ADNL: [32]byte{0x62}, Weight: 1}},
		Protocol:   SessionProtocol{SlotsPerLeaderWindow: 2},
		Identity:   SessionIdentity{ADNLID: [32]byte{0x62}, Validator: identity},
	}
	producer := &localBackendTestCollator{id: [32]byte{0x63}}
	backend := &LocalSessionBackend{
		config:         config,
		collator:       producer,
		productionMode: collator.ProductionModeSelf,
		self:           producer,
		validator:      identity,
		state:          SessionState{Params: simplex.DefaultParams()},
		update:         collator.SessionUpdate{SessionID: config.SessionID},
		closeAfter:     time.Second,
	}
	submitted := make(chan *CandidateArtifact, 1)
	window := LeaderWindow{
		Window: simplex.Window{
			StartSlot:    0,
			EndSlot:      2,
			ObservedSlot: 0,
			Leader:       0,
			LocalLeader:  true,
		},
		StartAt: time.Unix(200, 0),
		Submit: func(_ context.Context, artifact *CandidateArtifact) error {
			submitted <- artifact
			return nil
		},
	}
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err = backend.HandleLeaderWindow(ctx, window); err != nil {
		t.Fatal(err)
	}
	if len(producer.updateCalls) != 1 || len(producer.selfCalls) != 1 {
		t.Fatalf(
			"update/self calls = %d/%d, want 1/1",
			len(producer.updateCalls),
			len(producer.selfCalls),
		)
	}
	if len(producer.probeCalls) != 0 || len(producer.commitCalls) != 0 {
		t.Fatalf(
			"self mode probe/commit calls = %d/%d, want 0/0",
			len(producer.probeCalls),
			len(producer.commitCalls),
		)
	}
	self := producer.selfCalls[0]
	if self.SessionID != config.SessionID || self.StartSlot != window.Window.StartSlot ||
		!self.Deadline.Equal(deadline) || !producer.selfDeadlines[0].Equal(deadline) {
		t.Fatalf("self activation = %+v, context deadline %v, want deadline %v", self, producer.selfDeadlines[0], deadline)
	}
	if self.Signer != signer {
		t.Fatal("self activation did not preserve the validator signer")
	}

	blockBOC := []byte{1, 2}
	collatedData := []byte{3, 4}
	candidateID := simplex.CandidateID{Slot: 0, Hash: [32]byte{0x64}}
	signature, err := simplex.SignCandidate(signer, config.SessionID, candidateID)
	if err != nil {
		t.Fatal(err)
	}
	if err = backend.routeCandidate(context.Background(), collator.CandidateArtifact{
		SessionID:    config.SessionID,
		WindowID:     collator.WindowID{SessionID: config.SessionID, StartSlot: 0},
		Candidate:    simplex.Candidate{ID: candidateID, Signature: signature},
		BlockBOC:     blockBOC,
		CollatedData: collatedData,
	}); err != nil {
		t.Fatal(err)
	}
	artifact := <-submitted
	if &artifact.BlockBOC[0] != &blockBOC[0] || &artifact.CollatedData[0] != &collatedData[0] {
		t.Fatal("leader candidate route copied immutable payloads")
	}
	if artifact.Candidate.Delegation != nil {
		t.Fatal("self candidate acquired delegated authority")
	}
	if !bytes.Equal(artifact.Candidate.Signature, signature) ||
		!simplex.VerifyCandidateSignature(publicKey, config.SessionID, artifact.Candidate.ID, artifact.Candidate.Signature) {
		t.Fatal("self candidate did not preserve its validator signature")
	}

	delegated := collator.CandidateArtifact{
		SessionID: config.SessionID,
		WindowID:  collator.WindowID{SessionID: config.SessionID, StartSlot: 0},
		Candidate: simplex.Candidate{
			ID:         candidateID,
			Signature:  signature,
			Delegation: &simplex.Delegation{},
		},
	}
	if err = backend.routeCandidate(context.Background(), delegated); err == nil {
		t.Fatal("self route accepted delegated candidate")
	}

	// The self route no longer re-verifies a signature the collator verified the
	// instant it made it. What it does assert is the one thing that second
	// verification could establish over the first: that the leader the collator
	// signed for is the validator this backend is.
	wrongLeader := delegated
	wrongLeader.Candidate.Delegation = nil
	wrongLeader.Candidate.Leader = backend.validator.Index + 1
	if err = backend.routeCandidate(context.Background(), wrongLeader); err == nil {
		t.Fatal("self route accepted a candidate that names another leader")
	}
}

// TestSelfCandidateSignatureIsStillCheckedBeforeTheNetwork pins the property
// the removed self-route verification was resting on, on the real path.
//
// routeCandidate no longer verifies the signature of a candidate the in-process
// collator just produced. That is only safe because of what happens after it:
// windowSubmitter.submit does nothing but validate window order before calling
// publishCandidate, and publishCandidate's first act is encodeForBroadcast,
// which verifies the candidate against the codec's own roster and leader
// schedule — before the broadcast, before the resolver stage, before the
// durable write and before Simplex is told anything. So the test drives
// collator → routeCandidate → submit → publishCandidate for real, with a real
// runtime behind it, and asserts a forged candidate leaves no trace at all.
//
// Asserting this against a codec built by hand would pin the codec, not the
// route, and would keep passing if the route ever gained an irreversible step
// in front of the check.
func TestSelfCandidateSignatureIsStillCheckedBeforeTheNetwork(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	runtimeBackend := newRuntimeTestBackend()
	var broadcasts atomic.Int32
	network.broadcast = func(context.Context, simplex.CandidateBroadcast, CandidateArtifact) error {
		broadcasts.Add(1)

		return nil
	}
	// A voting runtime, because the self route only exists on one: the local
	// leader window it opens is what LocalSessionBackend routes into.
	config, privateKey := runtimeTestConfig(0x62, &runtimeTestJournal{})
	keyID := config.Validators[0].PublicKeyHash
	config.Identity = SessionIdentity{
		ADNLID: keyID,
		Validator: &ValidatorIdentity{
			Index:  0,
			KeyID:  keyID,
			Signer: runtimeTestSigner{key: privateKey},
		},
	}
	config.OverlayMembers = [][32]byte{keyID}
	config.StorageID.IsValidator = true
	config.StorageID.ValidatorKeyID = keyID
	config.StorageID.LocalADNLID = keyID
	config.StorageID.ValidatorIndex = 0
	prepared, err := PrepareSessionRuntime(context.Background(), config, runtimeTestState(), RuntimeOptions{
		Storage: storage,
		Network: network,
		Backend: runtimeBackend,
		Limits:  CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := prepared.(*sessionRuntime)

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	<-network.started
	var window LeaderWindow
	select {
	case window = <-runtimeBackend.windows:
	case <-time.After(time.Second):
		t.Fatal("leader window was not delivered")
	}
	if !window.Window.LocalLeader {
		t.Fatal("the window under test is not a local leader window")
	}

	// The backend the in-process collator hands its candidates to, wired to the
	// runtime's own leader window.
	backend := &LocalSessionBackend{
		config:    runtime.config,
		validator: &ValidatorIdentity{Index: window.Window.Leader},
	}
	backend.setWindowRoute(window)

	honest := runtimeOrdinaryArtifact(t, runtime.config, privateKey, window.Window.StartSlot, window.Window.Base)
	routed := collator.CandidateArtifact{
		SessionID: runtime.config.SessionID,
		WindowID: collator.WindowID{
			SessionID: runtime.config.SessionID,
			StartSlot: window.Window.StartSlot,
		},
		Candidate:    honest.Candidate,
		BlockBOC:     honest.BlockBOC,
		CollatedData: honest.CollatedData,
	}

	forged := routed
	forged.Candidate.Signature = bytes.Repeat([]byte{0xff}, ed25519.SignatureSize)
	if err = backend.routeCandidate(context.Background(), forged); err == nil {
		t.Fatal("a forged self candidate was routed to the network")
	}
	if got := broadcasts.Load(); got != 0 {
		t.Fatalf("forged candidate broadcasts = %d, want 0", got)
	}
	if got := storage.saveCount(); got != 0 {
		t.Fatalf("forged candidate durable writes = %d, want 0", got)
	}
	if stats := runtime.candidates.cacheStats(); stats.Entries != 0 {
		t.Fatalf("forged candidate reached the resolver cache: %+v", stats)
	}

	// The same route, the same window, the honest signature: what stopped the
	// candidate above was the signature check and not the fixture.
	if err = backend.routeCandidate(context.Background(), routed); err != nil {
		t.Fatalf("honestly signed self candidate refused: %v", err)
	}
	if got := broadcasts.Load(); got != 1 {
		t.Fatalf("honest candidate broadcasts = %d, want 1", got)
	}

	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-runResult; err != nil {
		t.Fatal(err)
	}
}

func TestLocalSessionBackendPublishesConsensusProgressToInProcessCollator(t *testing.T) {
	producer := &localBackendTestCollator{id: [32]byte{0x71}}
	config, _, initial := localBackendTestRuntimeInputs(t)
	backend := &LocalSessionBackend{
		config:     config,
		collator:   producer,
		progress:   producer,
		validator:  config.Identity.Validator,
		state:      initial,
		update:     localCollatorUpdate(config.SessionID, initial),
		closeAfter: time.Second,
	}
	progress := sessionConsensusProgress{
		FinalizedAnchor: initial.FinalizedBlock,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 4,
			StartSlot:    4,
			EndSlot:      8,
			Leader:       1,
		},
		StartAt: time.Unix(123, 0),
		Candidates: []*CandidateArtifact{{
			Candidate:    simplex.Candidate{ID: simplex.CandidateID{Slot: 3}},
			BlockBOC:     []byte{1, 2},
			CollatedData: []byte{3, 4},
		}},
	}
	if err := backend.ObserveConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	if len(producer.progressCalls) != 1 {
		t.Fatalf("progress calls = %d, want 1", len(producer.progressCalls))
	}
	got := producer.progressCalls[0]
	if got.SessionID != config.SessionID || got.Window != progress.Window || !got.StartAt.Equal(progress.StartAt) ||
		len(got.Candidates) != 1 || (progress.FinalizedAnchor != nil &&
		(got.FinalizedAnchor == nil || !sameBlockID(*got.FinalizedAnchor, *progress.FinalizedAnchor))) {
		t.Fatalf("forwarded progress = %+v", got)
	}
	if got.Candidates[0].WindowID.StartSlot != 2 ||
		&got.Candidates[0].BlockBOC[0] != &progress.Candidates[0].BlockBOC[0] ||
		&got.Candidates[0].CollatedData[0] != &progress.Candidates[0].CollatedData[0] {
		t.Fatal("consensus progress did not preserve the immutable candidate projection")
	}
	if !backend.update.HasCurrentWindow || backend.update.CurrentWindowStart != 4 ||
		backend.update.CurrentWindowObservedSlot != 4 || backend.update.CurrentBase != progress.Window.Base ||
		!backend.update.CurrentWindowStartAt.Equal(progress.StartAt) {
		t.Fatalf("local collator update = %+v", backend.update)
	}
}

func TestLocalSessionBackendLeaderWindowPreservesPreparedProgress(t *testing.T) {
	config, _, state := localBackendTestRuntimeInputs(t)
	session := localCollatorSession(config)
	latest := localCollatorUpdate(config.SessionID, state)
	finalized := localBackendTestBlockID(
		config.Shard.Workchain,
		config.Shard.Shard,
		11,
		bytes.Repeat([]byte{0x76}, 32),
		nil,
	)
	recovered := latest
	recovered.HasFinalizedBlock = true
	recovered.FinalizedBlock = finalized
	base := &localBackendTestCollator{
		id:     [32]byte{0x77},
		record: &collator.SessionRecord{Session: session, Update: recovered},
	}
	producer := &localBackendAdvancingTestCollator{
		localBackendTestCollator: base,
		update:                   recovered,
	}
	prepared, err := prepareLocalCollatorSession(
		context.Background(),
		producer,
		session,
		latest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.ready || !prepared.update.HasFinalizedBlock ||
		!sameBlockID(prepared.update.FinalizedBlock, finalized) {
		t.Fatalf("prepared update lost finalized parent: %+v", prepared)
	}

	backend := &LocalSessionBackend{
		config:         config,
		collator:       producer,
		productionMode: collator.ProductionModeSelf,
		progress:       producer,
		self:           producer,
		validator:      config.Identity.Validator,
		state:          state,
		update:         prepared.update,
		closeAfter:     time.Second,
	}
	startedAt := time.Unix(300, 400)
	window := simplex.Window{
		Base:         simplex.Genesis(),
		ObservedSlot: 0,
		StartSlot:    0,
		EndSlot:      config.Protocol.SlotsPerLeaderWindow,
		Leader:       0,
		LocalLeader:  true,
	}
	if err = backend.ObserveConsensusProgress(context.Background(), sessionConsensusProgress{
		Window:  window,
		StartAt: startedAt,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err = backend.HandleLeaderWindow(ctx, LeaderWindow{
		Window:  window,
		StartAt: startedAt,
		Submit:  func(context.Context, *CandidateArtifact) error { return nil },
	}); err != nil {
		t.Fatalf("leader window after prepared progress: %v", err)
	}

	got := producer.update
	if !got.HasFinalizedBlock || !sameBlockID(got.FinalizedBlock, finalized) {
		t.Fatalf("leader update lost finalized parent: %+v", got)
	}
	if !got.HasCurrentWindow || got.CurrentWindowStart != window.StartSlot ||
		got.CurrentWindowObservedSlot != window.ObservedSlot ||
		!got.CurrentWindowStartAt.Equal(startedAt) || got.CurrentBase != window.Base {
		t.Fatalf("leader update regressed consensus window: %+v", got)
	}
}

func TestLocalSessionBackendPreservesWindowStartAcrossMidWindowProgress(t *testing.T) {
	producer := &localBackendTestCollator{id: [32]byte{0x74}}
	config, _, initial := localBackendTestRuntimeInputs(t)
	backend := &LocalSessionBackend{
		config:     config,
		collator:   producer,
		progress:   producer,
		validator:  config.Identity.Validator,
		state:      initial,
		update:     localCollatorUpdate(config.SessionID, initial),
		closeAfter: time.Second,
	}
	startedAt := time.Unix(123, 456)
	progress := sessionConsensusProgress{
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 4,
			StartSlot:    4,
			EndSlot:      8,
			Leader:       1,
		},
		StartAt: startedAt,
	}
	if err := backend.ObserveConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	stored := backend.update

	progress.Window.ObservedSlot++
	progress.StartAt = time.Time{}
	if err := backend.ObserveConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	if !backend.update.CurrentWindowStartAt.Equal(startedAt) {
		t.Fatalf("mid-window start = %v, want %v", backend.update.CurrentWindowStartAt, startedAt)
	}

	next := cloneSessionState(initial)
	next.MasterchainBlock.SeqNo++
	next.MasterchainBlock.RootHash = bytes.Repeat([]byte{0x75}, 32)
	next.MasterchainBlock.FileHash = bytes.Repeat([]byte{0x76}, 32)
	if err := backend.UpdateSession(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	last := producer.updateCalls[len(producer.updateCalls)-1]
	if !last.CurrentWindowStartAt.Equal(startedAt) {
		t.Fatalf("published mid-window start = %v, want %v", last.CurrentWindowStartAt, startedAt)
	}
	if err := collator.ValidateSessionUpdateAdvance(stored, last); err != nil {
		t.Fatalf("advance durable same-window update: %v", err)
	}
}

func TestLocalSessionBackendRetriesNonterminalConsensusProgress(t *testing.T) {
	progressErr := fmt.Errorf("seed predecessor queue: %w", collator.ErrAcquisitionNotReady)
	producer := &localBackendTestCollator{id: [32]byte{0x72}, progressErr: progressErr}
	config, _, initial := localBackendTestRuntimeInputs(t)
	backend := &LocalSessionBackend{
		config:     config,
		collator:   producer,
		progress:   producer,
		validator:  config.Identity.Validator,
		state:      initial,
		update:     localCollatorUpdate(config.SessionID, initial),
		closeAfter: time.Second,
	}
	progress := sessionConsensusProgress{
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      config.Protocol.SlotsPerLeaderWindow,
			Leader:       0,
		},
		StartAt: time.Now(),
	}
	if err := backend.ObserveConsensusProgress(context.Background(), progress); !errors.Is(err, progressErr) {
		t.Fatalf("first progress error = %v, want %v", err, progressErr)
	}
	if backend.update.HasCurrentWindow || backend.collatorUnavailable {
		t.Fatalf("failed retryable progress changed backend state: %+v", backend.update)
	}
	producer.mu.Lock()
	producer.progressErr = nil
	producer.mu.Unlock()
	if err := backend.ObserveConsensusProgress(context.Background(), progress); err != nil {
		t.Fatalf("retry progress: %v", err)
	}
	if !backend.update.HasCurrentWindow || backend.update.CurrentWindowStart != progress.Window.StartSlot {
		t.Fatalf("retried progress was not committed: %+v", backend.update)
	}
	producer.mu.Lock()
	progressCalls := len(producer.progressCalls)
	producer.mu.Unlock()
	if progressCalls != 2 {
		t.Fatalf("progress calls = %d, want 2", progressCalls)
	}
}

func TestLocalSessionBackendQuarantinesTerminalProducerOnly(t *testing.T) {
	producer := &localBackendTestCollator{
		id:          [32]byte{0x73},
		progressErr: fmt.Errorf("collator generation stopped: %w", collator.ErrSessionUnavailable),
	}
	config, _, initial := localBackendTestRuntimeInputs(t)
	backend := &LocalSessionBackend{
		config:     config,
		collator:   producer,
		progress:   producer,
		validator:  config.Identity.Validator,
		state:      initial,
		update:     localCollatorUpdate(config.SessionID, initial),
		closeAfter: time.Second,
	}
	progress := sessionConsensusProgress{
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      config.Protocol.SlotsPerLeaderWindow,
			Leader:       0,
		},
		StartAt: time.Now(),
	}
	if err := backend.ObserveConsensusProgress(context.Background(), progress); !errors.Is(
		err,
		collator.ErrSessionUnavailable,
	) {
		t.Fatalf("terminal producer progress error = %v", err)
	}
	if !backend.collatorUnavailable {
		t.Fatal("terminal producer was not quarantined")
	}

	next := cloneSessionState(initial)
	next.MasterchainBlock.SeqNo++
	if err := backend.UpdateSession(context.Background(), next); err != nil {
		t.Fatalf("authoritative session update after producer quarantine: %v", err)
	}
	if backend.state.MasterchainBlock.SeqNo != next.MasterchainBlock.SeqNo {
		t.Fatal("producer quarantine blocked authoritative validator state")
	}
	if err := backend.HandleLeaderWindow(context.Background(), LeaderWindow{
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      config.Protocol.SlotsPerLeaderWindow,
			Leader:       0,
			LocalLeader:  true,
		},
		StartAt: time.Now(),
		Submit:  func(context.Context, *CandidateArtifact) error { return nil },
	}); err != nil {
		t.Fatalf("leader window after producer quarantine: %v", err)
	}
	if len(producer.updateCalls) != 0 || len(producer.commitCalls) != 0 {
		t.Fatal("quarantined producer received further production work")
	}
}

func TestLocalSessionBackendDelegatedPersistsNextAuthorizationBeforeCommit(t *testing.T) {
	delegations := newLocalBackendTestDelegations()
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, delegations)
	pending := make(chan localBackendPendingDelegationSave, 1)
	delegations.save = func(
		_ context.Context,
		session SessionStorageID,
		authorization DelegationAuthorization,
		done func(error),
	) {
		pending <- localBackendPendingDelegationSave{
			session:       session,
			authorization: authorization,
			done:          done,
		}
	}

	result := make(chan error, 1)
	go func() {
		result <- fixture.backend.HandleLeaderWindow(context.Background(), fixture.window(0, 0))
	}()

	var save localBackendPendingDelegationSave
	select {
	case save = <-pending:
	case <-time.After(time.Second):
		t.Fatal("delegated authorization was not submitted to storage")
	}
	if len(fixture.producer.commitCalls) != 0 {
		t.Fatal("delegation was committed before its durable callback")
	}
	if len(fixture.producer.selfCalls) != 0 {
		t.Fatal("delegated mode fell back to self activation")
	}
	if save.session != fixture.config.StorageID || save.authorization.StartSlot != 2 ||
		save.authorization.Collator != fixture.producer.id ||
		!simplex.VerifyDelegationSignature(
			fixture.config.Validators[0].PublicKey[:],
			fixture.config.SessionID,
			2,
			fixture.producer.id,
			save.authorization.Signature,
		) {
		t.Fatalf("pending W+1 authorization = %+v", save.authorization)
	}

	delegations.storage.SaveDelegationAuthorization(
		context.Background(),
		save.session,
		save.authorization,
		save.done,
	)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("leader window did not resume after durable authorization")
	}
	if len(fixture.producer.probeCalls) != 1 || fixture.producer.probeCalls[0].StartSlot != 2 {
		t.Fatalf("delegated probes = %+v, want exact W+1", fixture.producer.probeCalls)
	}
	if len(fixture.producer.commitCalls) != 1 {
		t.Fatalf("delegated commits = %d, want 1", len(fixture.producer.commitCalls))
	}
	commit := fixture.producer.commitCalls[0]
	if commit.ID().StartSlot != 2 || commit.SourceADNL != fixture.config.Identity.ADNLID ||
		!bytes.Equal(commit.PleaseCollate.Signature, save.authorization.Signature) {
		t.Fatalf("committed W+1 authorization = %+v", commit)
	}
}

func TestLocalSessionBackendDelegatedDeliveryFailureDoesNotBlockNextWindow(t *testing.T) {
	delegations := newLocalBackendTestDelegations()
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, delegations)
	var attempts atomic.Int32
	fixture.producer.commit = func(context.Context, collator.WindowRequest) error {
		if attempts.Add(1) == 1 {
			return collator.ErrUnavailable
		}

		return nil
	}

	if err := fixture.backend.HandleLeaderWindow(
		context.Background(),
		fixture.window(0, 0),
	); err != nil {
		t.Fatalf("first window after inconclusive W+1 delivery: %v", err)
	}
	if err := fixture.backend.HandleLeaderWindow(
		context.Background(),
		fixture.window(2, 2),
	); err != nil {
		t.Fatalf("next W+1 after prior delivery failure: %v", err)
	}

	_, commits, self := fixture.producer.delegationCallSnapshot()
	if len(commits) != 2 || commits[0].ID().StartSlot != 2 || commits[1].ID().StartSlot != 4 {
		t.Fatalf("delegated deliveries after failure = %+v, want independent W+1 slots 2 and 4", commits)
	}
	for _, start := range []uint32{2, 4} {
		authorization, err := delegations.DelegationAuthorization(
			context.Background(),
			fixture.config.StorageID,
			start,
		)
		if err != nil {
			t.Fatalf("load durable authorization %d: %v", start, err)
		}
		if authorization.StartSlot != start || authorization.Collator != fixture.producer.id {
			t.Fatalf("durable authorization %d = %+v", start, authorization)
		}
	}
	if len(self) != 0 {
		t.Fatal("delegated delivery failure fell back to self production")
	}
}

func TestLocalSessionBackendDelegatedRejectsInvalidSignerOutputBeforeSave(t *testing.T) {
	delegations := newLocalBackendTestDelegations()
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, delegations)
	signer := &localBackendInvalidSigner{}
	fixture.backend.validator.Signer = signer
	saves := 0
	delegations.save = func(
		_ context.Context,
		_ SessionStorageID,
		_ DelegationAuthorization,
		done func(error),
	) {
		saves++
		done(nil)
	}

	err := fixture.backend.HandleLeaderWindow(context.Background(), fixture.window(0, 0))
	if !errors.Is(err, ErrDelegationConflict) {
		t.Fatalf("invalid signer output error = %v, want ErrDelegationConflict", err)
	}
	if signer.calls != 1 {
		t.Fatalf("invalid signer calls = %d, want 1", signer.calls)
	}
	if saves != 0 {
		t.Fatalf("invalid authority durable saves = %d, want 0", saves)
	}
	if len(fixture.producer.commitCalls) != 0 || len(fixture.producer.selfCalls) != 0 {
		t.Fatal("invalid signer output activated delegated or self production")
	}
}

func TestLocalSessionBackendSelfMidWindowDoesNotActivate(t *testing.T) {
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeSelf, nil)
	fixture.backend.update.HasCurrentWindow = true
	fixture.backend.update.CurrentWindowStart = 0

	if err := fixture.backend.HandleLeaderWindow(
		context.Background(),
		fixture.window(0, 1),
	); err != nil {
		t.Fatal(err)
	}
	if len(fixture.producer.selfCalls) != 0 || len(fixture.producer.updateCalls) != 0 ||
		len(fixture.producer.probeCalls) != 0 || len(fixture.producer.commitCalls) != 0 {
		t.Fatalf(
			"mid-window self/update/probe/commit calls = %d/%d/%d/%d, want 0/0/0/0",
			len(fixture.producer.selfCalls),
			len(fixture.producer.updateCalls),
			len(fixture.producer.probeCalls),
			len(fixture.producer.commitCalls),
		)
	}
}

func TestLocalSessionBackendDelegatedCurrentWindowHasNoFallback(t *testing.T) {
	fixture := newLocalBackendProductionTestFixture(
		t,
		collator.ProductionModeDelegated,
		newLocalBackendTestDelegations(),
	)
	fixture.config.Validators = append(fixture.config.Validators, groups.Validator{
		PublicKey: [32]byte{0x91},
		ADNL:      [32]byte{0x92},
		Weight:    1,
	})
	fixture.backend.config = fixture.config

	if err := fixture.backend.HandleLeaderWindow(
		context.Background(),
		fixture.window(0, 0),
	); err != nil {
		t.Fatal(err)
	}
	if len(fixture.producer.updateCalls) != 1 {
		t.Fatalf("current delegated window updates = %d, want 1", len(fixture.producer.updateCalls))
	}
	if len(fixture.producer.probeCalls) != 0 || len(fixture.producer.commitCalls) != 0 ||
		len(fixture.producer.selfCalls) != 0 {
		t.Fatalf(
			"current delegated probe/commit/self calls = %d/%d/%d, want 0/0/0",
			len(fixture.producer.probeCalls),
			len(fixture.producer.commitCalls),
			len(fixture.producer.selfCalls),
		)
	}
}

func TestNewLocalSessionBackendDelegatedConstructorDoesNotPerformDelegationIO(t *testing.T) {
	options := localBackendTestConstructorOptions(t)
	baseSigner, ok := options.Config.Identity.Validator.Signer.(localBackendTestSigner)
	if !ok {
		t.Fatal("runtime fixture validator signer has unexpected type")
	}
	signer := &localBackendRecordingSigner{key: baseSigner.key}
	options.Config.Identity.Validator.Signer = signer
	producer := &localBackendTestCollator{id: [32]byte{0x98}}
	recovered := localCollatorUpdate(options.Config.SessionID, options.Initial)
	recovered.HasCurrentWindow = true
	recovered.CurrentWindowStart = 4
	recovered.CurrentWindowObservedSlot = 4
	recovered.CurrentWindowStartAt = time.Unix(260, 0)
	recovered.CurrentBase = simplex.Genesis()
	producer.record = &collator.SessionRecord{
		Session: localCollatorSession(options.Config),
		Update:  recovered,
	}
	delegations := newLocalBackendTestDelegations()
	loads := 0
	delegations.load = func(
		context.Context,
		SessionStorageID,
		uint32,
	) (DelegationAuthorization, error) {
		loads++

		return DelegationAuthorization{}, storage.ErrNotFound
	}
	saves := 0
	delegations.save = func(
		_ context.Context,
		_ SessionStorageID,
		_ DelegationAuthorization,
		done func(error),
	) {
		saves++
		done(nil)
	}
	options.Collator = producer
	options.ProductionMode = collator.ProductionModeDelegated
	options.Delegations = delegations

	backend, err := NewLocalSessionBackend(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := backend.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	probes, commits, self := producer.delegationCallSnapshot()
	if loads != 0 || saves != 0 || signer.calls != 0 || len(probes) != 0 || len(commits) != 0 || len(self) != 0 {
		t.Fatalf(
			"constructor delegation load/save/sign/probe/commit/self calls = %d/%d/%d/%d/%d/%d, want zero",
			loads,
			saves,
			signer.calls,
			len(probes),
			len(commits),
			len(self),
		)
	}
}

func TestLocalSessionBackendDelegatedStorageFailureNeverActivates(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		delegations := newLocalBackendTestDelegations()
		fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, delegations)
		writeErr := errors.New("write delegation authorization")
		delegations.save = func(
			_ context.Context,
			_ SessionStorageID,
			_ DelegationAuthorization,
			done func(error),
		) {
			done(writeErr)
		}

		err := fixture.backend.HandleLeaderWindow(context.Background(), fixture.window(0, 0))
		if !errors.Is(err, writeErr) {
			t.Fatalf("storage failure = %v, want %v", err, writeErr)
		}
		if len(fixture.producer.commitCalls) != 0 || len(fixture.producer.selfCalls) != 0 {
			t.Fatal("storage failure activated delegated or self production")
		}
	})

	t.Run("unknown outcome", func(t *testing.T) {
		delegations := newLocalBackendTestDelegations()
		fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, delegations)
		pending := make(chan localBackendPendingDelegationSave, 1)
		delegations.save = func(
			_ context.Context,
			session SessionStorageID,
			authorization DelegationAuthorization,
			done func(error),
		) {
			pending <- localBackendPendingDelegationSave{
				session:       session,
				authorization: authorization,
				done:          done,
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			result <- fixture.backend.HandleLeaderWindow(ctx, fixture.window(0, 0))
		}()

		var save localBackendPendingDelegationSave
		select {
		case save = <-pending:
		case <-time.After(time.Second):
			t.Fatal("delegated authorization write did not start")
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("unknown write outcome = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("leader window did not observe cancellation")
		}

		// Complete the unknown write after the caller has already stopped. The
		// late durable result must not continue into either activation path.
		delegations.storage.SaveDelegationAuthorization(
			context.Background(),
			save.session,
			save.authorization,
			save.done,
		)
		if len(fixture.producer.commitCalls) != 0 || len(fixture.producer.selfCalls) != 0 {
			t.Fatal("unknown storage outcome activated delegated or self production")
		}
	})
}

func TestLocalSessionBackendRetiresCollatorOnce(t *testing.T) {
	producer := &localBackendTestCollator{}
	releases := 0
	backend := &LocalSessionBackend{
		config:       SessionConfig{SessionID: [32]byte{0x71}},
		collator:     producer,
		validator:    &ValidatorIdentity{},
		closeAfter:   time.Second,
		releaseRoute: func() { releases++ },
	}

	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if len(producer.retireCalls) != 0 || releases != 1 {
		t.Fatalf("close retire/release calls = %d/%d, want 0/1", len(producer.retireCalls), releases)
	}
	if err := backend.Retire(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Retire(); err != nil {
		t.Fatal(err)
	}
	if len(producer.retireCalls) != 1 || releases != 1 {
		t.Fatalf("retire/release calls = %d/%d, want 1/1", len(producer.retireCalls), releases)
	}
}

func TestClassifyLocalCandidateErrorPreservesTaxonomy(t *testing.T) {
	notReady := fmt.Errorf("wrapped: %w", collator.ErrAcquisitionNotReady)
	if err := classifyLocalCandidateError(notReady); !errors.Is(err, ErrBlockNotReady) || !errors.Is(err, notReady) {
		t.Fatalf("not-ready classification = %v", err)
	}
	rejected := fmt.Errorf("wrapped: %w", collator.ErrInvalidInput)
	if err := classifyLocalCandidateError(rejected); !errors.Is(err, ErrCandidateRejected) || !errors.Is(err, rejected) {
		t.Fatalf("rejection classification = %v", err)
	}
	infrastructure := errors.New("disk failed")
	if err := classifyLocalCandidateError(infrastructure); err != infrastructure {
		t.Fatalf("infrastructure error = %v, want original", err)
	}
}

func localBackendTestCollatorRecord() (collator.Session, collator.SessionUpdate) {
	block := localBackendTestBlockID(-1, -1<<63, 10, bytes.Repeat([]byte{0x21}, 32), nil)
	session := collator.Session{
		ID:                   [32]byte{0x11},
		Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
		CatchainSeqno:        7,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: 2,
		Validators: []collator.SessionValidator{{
			PublicKey: [32]byte{0x41},
			ADNLID:    [32]byte{0x42},
			Weight:    1,
		}},
	}
	return session, collator.SessionUpdate{
		SessionID:        session.ID,
		TargetRate:       100 * time.Millisecond,
		MasterchainBlock: block,
	}
}

func localBackendTestRuntimeInputs(t *testing.T) (SessionConfig, SessionStart, SessionState) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var key [ed25519.PublicKeySize]byte
	copy(key[:], publicKey)
	validator := groups.Validator{PublicKey: key, ADNL: [32]byte{0x82}, Weight: 7}
	catchainSeqno := uint32(9)
	validatorSetHash, err := groups.ValidatorSetHash(groups.ValidatorSetHashInput{
		CatchainSeqno: catchainSeqno,
		Validators:    []groups.Validator{validator},
	})
	if err != nil {
		t.Fatal(err)
	}
	master := localBackendTestBlockID(-1, -1<<63, 20, bytes.Repeat([]byte{0x83}, 32), nil)
	genesis := localBackendTestBlockID(0, -1<<63, 10, bytes.Repeat([]byte{0x84}, 32), nil)
	identity := &ValidatorIdentity{
		Index:  0,
		Signer: localBackendTestSigner{key: privateKey},
	}
	config := SessionConfig{
		SessionID:        [32]byte{0x85},
		Shard:            groups.ShardID{Workchain: 0, Shard: -1 << 63},
		CatchainSeqno:    catchainSeqno,
		ValidatorSetHash: validatorSetHash,
		Validators:       []groups.Validator{validator},
		Protocol: SessionProtocol{
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 2,
		},
		Identity: SessionIdentity{ADNLID: validator.ADNL, Validator: identity},
	}
	return config, SessionStart{
			Genesis:        []ton.BlockIDExt{genesis},
			MinMasterchain: master,
		}, SessionState{
			MasterchainBlock: master,
			Params:           simplex.DefaultParams(),
		}
}

func newLocalBackendTestRemoteVotingBackend(
	t *testing.T,
) (*LocalSessionBackend, *localBackendTestCollator) {
	t.Helper()

	config, start, initial := localBackendTestRuntimeInputs(t)
	producer := &localBackendTestCollator{id: [32]byte{0x91}, sessionErr: collator.ErrNotFound}
	backend, err := NewLocalSessionBackend(context.Background(), LocalSessionBackendOptions{
		Config:  config,
		Initial: initial,
		Node: &localBackendTestNode{
			states:     make(map[string]*storage.BlockState),
			stateCells: make(map[string]*cell.Cell),
			blocks:     make(map[string][]byte),
		},
		Groups:         localBackendTestGroups{snapshot: &groups.Snapshot{}},
		Storage:        newRuntimeTestStorage(),
		Acquisition:    &collator.LocalAcquisition{},
		Collator:       producer,
		ProductionMode: collator.ProductionModeDelegated,
		Delegations:    newRuntimeTestStorage(),
		CloseTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("construct remote voting backend without candidate router: %v", err)
	}
	if err = backend.ActivateSession(context.Background(), start); err != nil {
		t.Fatalf("activate remote voting backend: %v", err)
	}

	return backend, producer
}

func localBackendTestConstructorOptions(t *testing.T) LocalSessionBackendOptions {
	t.Helper()

	config, _, initial := localBackendTestRuntimeInputs(t)

	return LocalSessionBackendOptions{
		Config:  config,
		Initial: initial,
		Node: &localBackendTestNode{
			states:     make(map[string]*storage.BlockState),
			stateCells: make(map[string]*cell.Cell),
			blocks:     make(map[string][]byte),
		},
		Groups:       localBackendTestGroups{snapshot: &groups.Snapshot{}},
		Storage:      newRuntimeTestStorage(),
		Acquisition:  &collator.LocalAcquisition{},
		Collator:     &localBackendTestCollator{id: [32]byte{0x97}, sessionErr: collator.ErrNotFound},
		CloseTimeout: time.Second,
	}
}

func localBackendTestBlockID(
	workchain int32,
	shard int64,
	seqno uint32,
	rootHash []byte,
	fileData []byte,
) ton.BlockIDExt {
	fileHash := sha256.Sum256(fileData)
	if fileData == nil {
		fileHash = sha256.Sum256(rootHash)
	}

	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Clone(rootHash),
		FileHash:  bytes.Clone(fileHash[:]),
	}
}

func localBackendTestBlockKey(id ton.BlockIDExt) string {
	return fmt.Sprintf("%d:%d:%d:%x:%x", id.Workchain, id.Shard, id.SeqNo, id.RootHash, id.FileHash)
}
