package validator

import (
	"context"
	"crypto/ed25519"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// builtSuccessorFixture obtains the sealed successor through the public builder
// and collator emission path. Tests cannot manufacture the token they exercise.
type builtSuccessorFixture struct {
	parent   *ChainState
	artifact *CandidateArtifact
	live     collator.LiveSuccessorState
	expected *cell.Cell
	update   *cell.Cell
}

func newBuiltSuccessorFixture(t *testing.T) builtSuccessorFixture {
	t.Helper()

	config, private := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	request := successorShardRequest(t)
	request.CreatedBy = config.Validators[0].PublicKey
	builder := collator.NewBuilder(tvm.NewTVM(), tlb.GlobalVersion{Version: 1})
	previous, err := builder.BuildShard(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	previousRoot, err := cell.FromBOC(previous.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	queueSize := previous.Stats.OutQueueSize
	request.Previous = collator.PreviousBlock{
		ID: previous.ID, Block: previousRoot, State: previous.State, OutQueueSize: &queueSize,
	}
	request.Header.GenUtime++
	request.Header.GenUtimeMS += 1000
	built, err := builder.BuildShard(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := newChainState(ChainStateRequest{
		Shard: config.Shard, Blocks: []ton.BlockIDExt{previous.ID}, MinMasterchain: request.Masterchain.ID,
	}, ChainStateData{Tips: []ChainTip{{
		ID: previous.ID, Block: previousRoot, BlockBOC: previous.BlockBOC, State: previous.State,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	fixture := builtSuccessorFixture{parent: parent, expected: built.State, update: built.StateUpdate}

	keys := successorSigningKeys{private: private}
	emitted := make(chan collator.CandidateArtifact, 1)
	service, err := collator.NewService(collator.ServiceOptions{
		ProductionMode: collator.ProductionModeSelf,
		Storage:        &successorEmissionStore{},
		Pipeline:       &successorEmissionPipeline{candidate: built, parent: previous.ID},
		Keys:           keys, CollatorKeyID: keys.KeyIDs()[0], AllowAllValidators: true,
		Emit: func(_ context.Context, artifact collator.CandidateArtifact) error {
			emitted <- artifact
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	session := localCollatorSession(config)
	session.SlotsPerLeaderWindow = 1
	update := collator.SessionUpdate{
		SessionID: session.ID, MasterchainBlock: request.Masterchain.ID,
		TargetRate: time.Second, NoEmptyBlocksOnErrTimeout: time.Minute,
	}
	if err = service.PrepareSession(t.Context(), session, update); err != nil {
		t.Fatal(err)
	}
	if err = service.ActivateSession(t.Context(), collator.SessionActivation{
		SessionID: session.ID, Genesis: []ton.BlockIDExt{previous.ID}, MinMasterchain: request.Masterchain.ID,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = service.ApplyConsensusProgress(t.Context(), collator.ConsensusProgress{
		SessionID: session.ID,
		Window:    simplex.Window{EndSlot: 1, ObservedAt: now}, StartAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err = service.ActivateSelfWindow(t.Context(), collator.SelfWindowRequest{
		SessionID: session.ID, Deadline: now.Add(5 * time.Second), Signer: runtimeTestSigner{key: private},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case artifact := <-emitted:
		block, roots := artifact.ValidationRoots()
		fixture.live = artifact.BuiltSuccessor()
		fixture.artifact = &CandidateArtifact{
			Candidate: artifact.Candidate, BlockBOC: artifact.BlockBOC, CollatedData: artifact.CollatedData,
			validationRoots: &candidateValidationRoots{block: block, collated: roots, builtSuccessor: fixture.live},
		}
	case <-time.After(5 * time.Second):
		status, _ := service.Status(t.Context())
		t.Fatalf("collator emitted no successor: %+v", status)
	}
	return fixture
}

func successorShardRequest(t *testing.T) collator.ShardRequest {
	t.Helper()

	boc, err := os.ReadFile("../../../tonutils-go/tlb/testdata/blockchain_config_mainnet.boc")
	if err != nil {
		t.Fatal(err)
	}
	root, err := cell.FromBOC(boc)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := tvm.PrepareBlockchainConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	configAddress, err := (tlb.BlockchainConfig{Root: root}).GetConfigAddress()
	if err != nil {
		t.Fatal(err)
	}
	config, err := collator.PrepareConfig(execution, [32]byte(configAddress))
	if err != nil {
		t.Fatal(err)
	}
	global, err := (tlb.BlockchainConfig{Root: root}).GetGlobalID()
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queueInfo, err := (tlb.OutMsgQueueInfo{OutQueue: queue, ProcInfo: cell.NewDict(96)}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	master := chainStateBlock(math.MinInt64, 7_000_001, 0x22)
	master.Workchain = -1
	stats, err := (tlb.ShardStateStats{MasterRef: &tlb.ExtBlkRef{
		EndLt: 7_000_000_000, SeqNo: master.SeqNo - 1,
		RootHash: master.RootHash, FileHash: master.FileHash,
	}}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	state := tlb.ShardStateUnsplit{
		GlobalID: global.GlobalID, ShardIdent: tlb.ShardIdent{WorkchainID: 0}, Seqno: 100,
		VertSeqno: 2, GenUTime: 1_900_000_000, GenLT: 7_000_002_000, MinRefMCSeqno: master.SeqNo - 1,
		OutMsgQueueInfo: queueInfo, Stats: stats,
	}
	state.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	previous := chainStateBlock(shard.Shard, state.Seqno, 0x33)
	queueSize := uint64(0)
	return collator.ShardRequest{
		Shard: shard, Previous: collator.PreviousBlock{ID: previous, State: stateRoot, OutQueueSize: &queueSize},
		Masterchain: collator.MasterchainContext{
			ID: master, EndLT: uint64(master.SeqNo) * 1000, GenUtime: state.GenUTime,
			VertSeqno: state.VertSeqno, Config: config, OutMsgQueueInfo: queueInfo,
			Groups: &groups.Snapshot{
				MasterchainBlock: master, ConfigRootHash: root.HashKey(), GenUTime: state.GenUTime,
				LastKeyBlockSeqno: master.SeqNo - 1, Ready: true,
				Active: []groups.Session{{Shard: shard, CatchainSeqno: 17, ValidatorSetHash: 0x10203040,
					Registered: []groups.ShardDescription{{Shard: shard, Block: previous}}}},
			},
		},
		Header:   collator.HeaderParams{GenUtime: state.GenUTime + 1, GenUtimeMS: uint64(state.GenUTime+1) * 1000},
		RandSeed: [32]byte{1},
	}
}

// The emitter fixture supplies a single real Builder result to Service. These
// fakes model only its session bookkeeping; no fake constructs a successor token.
type successorEmissionPipeline struct {
	candidate *collator.Candidate
	parent    ton.BlockIDExt
}

func (*successorEmissionPipeline) PrepareSession(context.Context, collator.Session, collator.SessionUpdate) error {
	return nil
}

func (*successorEmissionPipeline) ActivateSession(context.Context, collator.SessionActivation, collator.SessionUpdate) error {
	return nil
}

func (*successorEmissionPipeline) UpdateSession(context.Context, collator.Session, collator.SessionUpdate) error {
	return nil
}

func (*successorEmissionPipeline) AdvanceConsensusBase(context.Context, collator.ConsensusBaseUpdate) error {
	return nil
}

func (p *successorEmissionPipeline) ResolveCandidateState(context.Context, collator.BuildRequest) (collator.CandidateState, error) {
	return collator.CandidateState{Block: p.parent, NextSeqno: p.parent.SeqNo + 1}, nil
}

func (p *successorEmissionPipeline) BuildCandidate(context.Context, collator.BuildRequest) (*collator.Candidate, error) {
	candidate := p.candidate
	p.candidate = nil
	if candidate == nil {
		return nil, collator.ErrCandidateConflict
	}

	return candidate, nil
}

func (*successorEmissionPipeline) RestoreCandidate(context.Context, collator.BuildRequest, collator.CandidateArtifact) error {
	return nil
}

func (*successorEmissionPipeline) CommitCandidate(context.Context, collator.CandidateCommit) error {
	return nil
}

func (*successorEmissionPipeline) SoftTimeout(context.Context, collator.SoftTimeoutRequest) (collator.SoftTimeoutDecision, error) {
	return collator.SoftTimeoutDecision{Action: collator.SoftTimeoutWait}, nil
}

func (*successorEmissionPipeline) RetireSession(context.Context, [32]byte) error { return nil }

type successorEmissionStore struct {
	mu        sync.Mutex
	session   *collator.SessionRecord
	candidate *collator.CandidateRecord
}

func (s *successorEmissionStore) SaveSession(_ context.Context, record collator.SessionRecord, done func(error)) {
	s.mu.Lock()
	s.session = &record
	s.mu.Unlock()
	done(nil)
}

func (s *successorEmissionStore) Session(context.Context, [32]byte) (collator.SessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session == nil {
		return collator.SessionRecord{}, collator.ErrNotFound
	}
	return *s.session, nil
}

func (*successorEmissionStore) Sessions(context.Context) ([]collator.SessionRecord, error) {
	return nil, nil
}

func (*successorEmissionStore) DeleteSession(context.Context, [32]byte) error { return nil }

func (s *successorEmissionStore) SaveCandidate(record collator.CandidateRecord, done func(error)) {
	s.mu.Lock()
	s.candidate = &record
	s.mu.Unlock()
	done(nil)
}

func (s *successorEmissionStore) Candidate(context.Context, collator.WindowID, uint32) (collator.CandidateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.candidate == nil {
		return collator.CandidateRecord{}, collator.ErrNotFound
	}
	return *s.candidate, nil
}

func (*successorEmissionStore) Status(context.Context) (collator.StorageStatus, error) {
	return collator.StorageStatus{}, nil
}

type successorSigningKeys struct{ private ed25519.PrivateKey }

func (k successorSigningKeys) KeyIDs() [][32]byte {
	return [][32]byte{simplex.KeyNodeIDShort(k.private.Public().(ed25519.PublicKey))}
}

func (k successorSigningKeys) PublicKeyFor([32]byte) (ed25519.PublicKey, error) {
	return k.private.Public().(ed25519.PublicKey), nil
}

func (k successorSigningKeys) Sign(_ [32]byte, payload []byte) ([]byte, error) {
	return ed25519.Sign(k.private, payload), nil
}
