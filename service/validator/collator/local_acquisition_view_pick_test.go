package collator

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// viewPickMasterBlock is one hand-assembled masterchain block/state pair.
type viewPickMasterBlock struct {
	id       ton.BlockIDExt
	block    *cell.Cell
	state    *cell.Cell
	genUtime uint32
}

// viewPickChain is the shard side of the stand: four consecutive shard blocks
// and the masterchain block every one of them references. The registries built
// over it name successively newer tops of the same chain.
type viewPickChain struct {
	config     *Config
	shardID    groups.ShardID
	registered [3]PreviousBlock
	previous   PreviousBlock
	anchor     viewPickMasterBlock
}

const (
	// viewPickAnchorSeqno is the masterchain block emptyCandidateRequest stamps
	// into every shard block built over it. The two views under test follow it.
	viewPickAnchorSeqno = 7_000_001
	viewPickShardGen    = 1_900_000_000
)

// newViewPickChain builds the shard blocks the registries will point at. They
// are produced by the real shard builder so the block roots the store hands
// back carry a real header and a valid state update, which is what
// verifyPredecessor demands of every block acquisition reads.
func newViewPickChain(t *testing.T) viewPickChain {
	t.Helper()

	config := loadMainnetConfig(t)
	anchor := viewPickMasterState(t, config, viewPickAnchorSeqno, viewPickShardGen-2, nil, nil)

	request := emptyCandidateRequest(t)
	// The anchor is the masterchain block every shard block below references.
	// Its id has to be one this test can also serve as a real state, because a
	// processed-upto record naming it sends historicalShardEndLT to the store.
	masterchain := request.Masterchain
	snapshot := *masterchain.Groups
	snapshot.MasterchainBlock = cloneBlockID(anchor.id)
	masterchain.ID = cloneBlockID(anchor.id)
	masterchain.Groups = &snapshot
	request.Masterchain = masterchain

	var chain viewPickChain
	chain.config = config
	chain.anchor = anchor
	chain.shardID = request.Shard
	for i := range chain.registered {
		request = advanceCandidateRequest(t, request)
		chain.registered[i] = request.Previous
	}
	request = advanceCandidateRequest(t, request)
	chain.previous = request.Previous

	return chain
}

// viewPickHistoryDict builds the OldMcBlocksInfo dictionary a masterchain state
// at seqno needs: masterPrevBlocksTuple walks sixteen consecutive ancestors and
// sixteen hundred-block steps out of it before any view exists at all.
func viewPickHistoryDict(t *testing.T, seqno uint32, exact []ton.BlockIDExt) *tlb.OldMcBlocksInfoAugDict {
	t.Helper()

	wanted := make(map[uint32]struct{}, 2*masterHistoryTupleLimit)
	for i := uint32(1); i <= masterHistoryTupleLimit && i <= seqno; i++ {
		wanted[seqno-i] = struct{}{}
	}
	for step, at := 0, seqno/100*100; step <= masterHistoryTupleLimit; step++ {
		wanted[at] = struct{}{}
		if at < 100 {
			break
		}
		at -= 100
	}
	delete(wanted, seqno)

	known := make(map[uint32]ton.BlockIDExt, len(exact))
	for i := range exact {
		known[exact[i].SeqNo] = exact[i]
	}

	dict, err := cell.NewAugDict(32, oldMCBlocksAugmentation{})
	if err != nil {
		t.Fatal(err)
	}
	for at := range wanted {
		id, resolved := known[at]
		if !resolved {
			id = testBlockID(masterchainWorkchainID, math.MinInt64, at, byte(at))
		}
		value, cellErr := tlb.ToCell(&tlb.KeyExtBlkRef{
			IsKey: at%100 == 0,
			BlkRef: tlb.ExtBlkRef{
				EndLt:    uint64(at) * logicalTimeAlignment,
				SeqNo:    at,
				RootHash: bytes.Clone(id.RootHash),
				FileHash: bytes.Clone(id.FileHash),
			},
		})
		if cellErr != nil {
			t.Fatal(cellErr)
		}
		key := cell.BeginCell().MustStoreUInt(uint64(at), 32).EndCell()
		inserted, setErr := dict.SetWithMode(key, value, cell.DictSetModeAdd)
		if setErr != nil || !inserted {
			t.Fatalf("store masterchain history block %d: inserted=%t err=%v", at, inserted, setErr)
		}
	}

	return &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: dict}
}

// viewPickMasterState assembles one masterchain block and its state around a
// single registered basechain top. It is the fixture equivalent of
// newMasterBuildFixture at an arbitrary seqno: the pick compares views, so the
// stand needs more than one of them and each has to carry its own registry and
// its own authenticated ancestry.
func viewPickMasterState(
	t *testing.T,
	config *Config,
	seqno uint32,
	genUtime uint32,
	registered *ton.BlockIDExt,
	ancestors []ton.BlockIDExt,
) viewPickMasterBlock {
	t.Helper()

	configRoot := config.execution.Root()
	parsedGroups, err := groups.ParseConfig(configRoot)
	if err != nil {
		t.Fatal(err)
	}
	const catchainSeqno = uint32(17)
	validatorHash, err := groups.MasterchainStateValidatorHash(parsedGroups, catchainSeqno)
	if err != nil {
		t.Fatal(err)
	}
	configAddress, err := (tlb.BlockchainConfig{Root: configRoot}).GetConfigAddress()
	if err != nil || len(configAddress) != 32 {
		t.Fatalf("load configuration address: %v", err)
	}
	accounts := masterBuildAccounts(t, configRoot, configAddress, genUtime, false)
	balance, err := loadAccountsBalance(accounts)
	if err != nil {
		t.Fatal(err)
	}
	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queueRoot, err := (tlb.OutMsgQueueInfo{
		OutQueue: outQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}

	top := testBlockID(0, math.MinInt64, 1, 0x61)
	if registered != nil {
		top = *registered
	}
	shardHashes := masterBuildShardHashes(t, masterBuildShardDescriptor(t, top, 0, genUtime-1))
	lastKey := tlb.ExtBlkRef{
		SeqNo:    0,
		RootHash: bytes.Repeat([]byte{0x01}, 32),
		FileHash: bytes.Repeat([]byte{0x02}, 32),
	}
	info := tlb.McStateExtraBlockInfo{
		ValidatorInfo: tlb.ValidatorInfo{
			ValidatorListHashShort: validatorHash,
			CatchainSeqno:          catchainSeqno,
			NextCCUpdated:          true,
		},
		PrevBlocks:   viewPickHistoryDict(t, seqno, ancestors),
		LastKeyBlock: &lastKey,
	}
	infoRoot, err := info.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	params := tlb.ConfigParams{ConfigAddr: bytes.Clone(configAddress)}
	params.Config.Params = configRoot.AsDict(32)
	extraRoot, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes:  shardHashes,
		ConfigParams: params,
		Info:         infoRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	statsRoot, err := (tlb.ShardStateStats{TotalBalance: balance, Libraries: cell.NewDict(256)}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	state := tlb.ShardStateUnsplit{
		GlobalID:        testConfigGlobalID(t, config),
		ShardIdent:      tlb.ShardIdent{WorkchainID: address.MasterchainID},
		Seqno:           seqno,
		VertSeqno:       2,
		GenUTime:        genUtime,
		GenLT:           uint64(seqno) * logicalTimeAlignment,
		OutMsgQueueInfo: queueRoot,
		Stats:           statsRoot,
		McStateExtra:    extraRoot,
	}
	state.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}

	blockRoot := viewPickMasterBlockRoot(t, state, stateRoot)
	rootHash := blockRoot.HashKey()

	return viewPickMasterBlock{
		id: ton.BlockIDExt{
			Workchain: masterchainWorkchainID,
			Shard:     math.MinInt64,
			SeqNo:     seqno,
			RootHash:  bytes.Clone(rootHash[:]),
			FileHash:  bytes.Repeat([]byte{byte(seqno), 0x5a}, 16),
		},
		block:    blockRoot,
		state:    stateRoot,
		genUtime: genUtime,
	}
}

// viewPickMasterBlockRoot is the minimal masterchain block that satisfies
// verifyPredecessor: its root hash names the block, its header agrees with the
// state, and its state update is a well-formed Merkle update.
func viewPickMasterBlockRoot(t *testing.T, state tlb.ShardStateUnsplit, stateRoot *cell.Cell) *cell.Cell {
	t.Helper()

	var header tlb.BlockHeader
	header.SeqNo = state.Seqno
	header.VertSeqNo = state.VertSeqno
	header.Shard = state.ShardIdent
	header.GenUtime = state.GenUTime
	header.StartLt = state.GenLT - 1_000
	header.EndLt = state.GenLT
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    state.GenLT - 1_000,
		SeqNo:    state.Seqno - 1,
		RootHash: bytes.Repeat([]byte{0x73}, 32),
		FileHash: bytes.Repeat([]byte{0x74}, 32),
	}}
	infoRoot, err := header.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	zero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	flow, err := tlb.ValueFlow{
		FromPrevBlock: zero,
		ToNextBlock:   zero,
		Imported:      zero,
		Exported:      zero,
		FeesCollected: zero,
		FeesImported:  zero,
		Recovered:     zero,
		Created:       zero,
		Minted:        zero,
		Burned:        zero,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	// verifyPredecessor binds the block to the state through the update's
	// destination hash, so the update has to name this exact state root. A
	// self-update is the cheapest one that does.
	update, err := cell.CreateMerkleUpdate(stateRoot, stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	empty := cell.BeginCell().EndCell()
	extra := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(empty).
		MustStoreRef(empty).
		MustStoreRef(empty).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreBoolBit(false).
		EndCell()

	return cell.BeginCell().
		MustStoreUInt(blockTag, 32).
		MustStoreInt(int64(state.GlobalID), 32).
		MustStoreRef(infoRoot).
		MustStoreRef(flow).
		MustStoreRef(update).
		MustStoreRef(extra).
		EndCell()
}

// viewPickStand is one activated shard session pinned to masterchain view N,
// with a second view N+1 ready to be installed behind its back.
type viewPickStand struct {
	chain       viewPickChain
	acquisition *LocalAcquisition
	pool        *msgpool.Pool
	tracker     *groups.Tracker
	incumbent   viewPickMasterBlock
	session     ActivatedSession
	update      SessionUpdate
}

// newViewPickStand activates a shard session against masterchain view N, whose
// registry names the first of the three shard blocks. The build predecessor is
// the last of them, so both views under test list a top strictly behind it —
// the ordinary steady state of a leader window, and the only one in which a
// newer view may legally be adopted mid-window.
func newViewPickStand(t *testing.T) *viewPickStand {
	t.Helper()

	chain := newViewPickChain(t)

	return newViewPickStandOn(t, chain, chain.registered[0].ID)
}

// newViewPickStandOn is newViewPickStand with the incumbent view's registered
// shard top chosen by the caller, so a stand can also be built on a registry
// the shard chain has already run away from.
func newViewPickStandOn(t *testing.T, chain viewPickChain, registeredTop ton.BlockIDExt) *viewPickStand {
	t.Helper()

	incumbent := viewPickMasterState(
		t,
		chain.config,
		viewPickAnchorSeqno+1,
		chain.anchor.genUtime+1,
		&registeredTop,
		[]ton.BlockIDExt{chain.anchor.id},
	)

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := tracker.Apply(groups.ApplyInput{
		Block: incumbent.id,
		Root:  incumbent.state,
		AsOf:  time.Unix(int64(incumbent.genUtime), 0),
	})
	if err != nil {
		t.Fatalf("apply incumbent masterchain state: %v", err)
	}
	if !applied.Snapshot.Ready {
		t.Fatal("incumbent masterchain snapshot is not ready")
	}

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	destination := targetShardIdent(chain.shardID)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	// The committed masterchain out-queue run this node would have ingested by
	// itself. Without it no view has all its neighbour tops committed and the
	// pick would always fall through to the seed-allowed relaxation, which is
	// the one branch that cannot distinguish a preference from an admission.
	anchorRef, err := localSourceRef(chain.anchor.id)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, blockShardIdent(chain.anchor.id), anchorRef, nil, 0); err != nil {
		t.Fatal(err)
	}
	store := &localValidationStore{states: []localValidationState{
		{block: chain.anchor.id, root: chain.anchor.state, data: chain.anchor.block},
		{block: chain.previous.ID, root: chain.previous.State, data: chain.previous.Block},
	}}
	for i := range chain.registered {
		store.states = append(store.states, localValidationState{
			block: chain.registered[i].ID,
			root:  chain.registered[i].State,
			data:  chain.registered[i].Block,
		})
	}
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     store,
		Groups:    &localAcquisitionTestGroups{snapshot: applied.Snapshot},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = acquisition.PublishMasterchainView(
		context.Background(),
		applied.Snapshot,
		incumbent.block,
		incumbent.state,
	); err != nil {
		t.Fatalf("publish incumbent masterchain view: %v", err)
	}

	session := viewPickSession(t, applied.Snapshot, chain.shardID)
	update := viewPickUpdate(t, applied.Snapshot, session, incumbent.id)
	if err = acquisition.PrepareSession(context.Background(), session.Session, update); err != nil {
		t.Fatalf("prepare shard session: %v", err)
	}
	if err = acquisition.ActivateSession(context.Background(), SessionActivation{
		SessionID:      session.ID,
		Genesis:        session.Genesis,
		MinMasterchain: session.MinMasterchain,
	}, update); err != nil {
		t.Fatalf("activate shard session: %v", err)
	}

	stand := &viewPickStand{
		chain:       chain,
		acquisition: acquisition,
		pool:        pool,
		tracker:     tracker,
		incumbent:   incumbent,
		session:     session,
		update:      update,
	}
	stand.commitMasterchainSource(t, incumbent.id)

	return stand
}

// commitMasterchainSource advances the shard's committed view of the
// masterchain out-queue to one masterchain block, the way the applied-block
// feed does on a live node.
func (s *viewPickStand) commitMasterchainSource(t *testing.T, id ton.BlockIDExt) {
	t.Helper()

	ref, err := localSourceRef(id)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.pool.Internals().ApplyBlock(
		targetShardIdent(s.chain.shardID),
		blockShardIdent(id),
		ref,
		&msgpool.InternalsDelta{},
	); err != nil {
		t.Fatalf("commit masterchain source %d: %v", id.SeqNo, err)
	}
}

// successor builds masterchain view N+1 registering top and applies it to the
// same tracker, so the session it carries is the continuation of the activated
// one rather than a fresh session that happens to cover the same shard.
func (s *viewPickStand) successor(t *testing.T, top ton.BlockIDExt) (viewPickMasterBlock, *groups.Snapshot) {
	t.Helper()

	return s.successorOn(t, top, []ton.BlockIDExt{s.chain.anchor.id, s.incumbent.id})
}

func (s *viewPickStand) successorOn(
	t *testing.T,
	top ton.BlockIDExt,
	ancestors []ton.BlockIDExt,
) (viewPickMasterBlock, *groups.Snapshot) {
	t.Helper()

	block := viewPickMasterState(
		t,
		s.chain.config,
		s.incumbent.id.SeqNo+1,
		s.incumbent.genUtime+1,
		&top,
		ancestors,
	)
	applied, err := s.tracker.Apply(groups.ApplyInput{
		Block: block.id,
		Root:  block.state,
		AsOf:  time.Unix(int64(block.genUtime), 0),
	})
	if err != nil {
		t.Fatalf("apply successor masterchain state: %v", err)
	}

	return block, applied.Snapshot
}

// publish installs one masterchain view as the resident one without touching
// the session, which is exactly what a leader window's held production barrier
// does to every routine refresh for the length of the window.
func (s *viewPickStand) publish(t *testing.T, block viewPickMasterBlock, snapshot *groups.Snapshot) {
	t.Helper()

	if err := s.acquisition.PublishMasterchainView(
		context.Background(),
		snapshot,
		block.block,
		block.state,
	); err != nil {
		t.Fatalf("publish successor masterchain view: %v", err)
	}
	s.commitMasterchainSource(t, block.id)
}

// acquire runs one slot of the window that opened at the activated update.
func (s *viewPickStand) acquire(t *testing.T) (ShardRequest, error) {
	t.Helper()

	candidate := simplex.CandidateID{Slot: s.update.CurrentWindowStart, Hash: [32]byte{0x71}}

	return s.acquisition.AcquireShard(context.Background(), BuildRequest{
		Session: s.session,
		Update:  s.update,
		Slot:    s.update.CurrentWindowStart + 1,
		Parent:  simplex.Parent(candidate),
		Previous: &CandidateArtifact{
			SessionID: s.session.ID,
			Candidate: simplex.Candidate{ID: candidate, Block: cloneBlockID(s.chain.previous.ID)},
		},
	})
}

func (s *viewPickStand) fallbacks() int64 {
	return s.acquisition.selectedViewFallbacks.Load()
}

func viewPickSession(t *testing.T, snapshot *groups.Snapshot, target groups.ShardID) ActivatedSession {
	t.Helper()

	var active *groups.Session
	for i := range snapshot.Active {
		if snapshot.Active[i].Shard == target {
			active = &snapshot.Active[i]
			break
		}
	}
	if active == nil {
		t.Fatalf("snapshot has no active session for shard %+v", target)
	}
	validators := make([]SessionValidator, len(active.Validators))
	for i := range active.Validators {
		validators[i] = SessionValidator{
			PublicKey: active.Validators[i].PublicKey,
			ADNLID:    active.Validators[i].ADNL,
			Weight:    active.Validators[i].Weight,
		}
	}

	return ActivatedSession{
		Session: Session{
			ID:                   active.ID,
			Shard:                active.Shard,
			CatchainSeqno:        active.CatchainSeqno,
			ValidatorSetHash:     active.ValidatorSetHash,
			ConsensusVersion:     2,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 4,
			Validators:           validators,
		},
		Genesis:        active.Genesis,
		MinMasterchain: active.MinMasterchain,
	}
}

func viewPickUpdate(
	t *testing.T,
	snapshot *groups.Snapshot,
	session ActivatedSession,
	masterchain ton.BlockIDExt,
) SessionUpdate {
	t.Helper()

	var active *groups.Session
	for i := range snapshot.Active {
		if snapshot.Active[i].ID == session.ID {
			active = &snapshot.Active[i]
			break
		}
	}
	if active == nil {
		t.Fatal("snapshot lost the activated session")
	}
	update := SessionUpdate{
		SessionID:                 session.ID,
		TargetRate:                400 * time.Millisecond,
		MasterchainBlock:          cloneBlockID(masterchain),
		Registered:                append([]groups.ShardDescription(nil), active.Registered...),
		HasCurrentWindow:          true,
		CurrentWindowStart:        session.SlotsPerLeaderWindow,
		CurrentWindowObservedSlot: session.SlotsPerLeaderWindow,
		CurrentWindowStartAt:      time.Unix(viewPickShardGen+60, 0),
	}
	if active.FinalizedBlock != nil {
		update.HasFinalizedBlock = true
		update.FinalizedBlock = cloneBlockID(*active.FinalizedBlock)
	}

	return update
}

// A leader window holds the production barrier for all of its slots, so every
// routine masterchain refresh is deferred and the session keeps pointing at the
// view the window opened on. If the slot then builds against that pinned view,
// the block's master_ref stands still for the whole window and the neighbour
// out-queue tops registered with it stand still too — which is how a shard's
// imported internal messages fell to zero within one window on the stand. This
// pins both halves at once: the newest resident view is the one stamped, and
// the neighbour set moves with it.
func TestAcquireShardStampsNewestResidentMasterchainView(t *testing.T) {
	stand := newViewPickStand(t)

	incumbent, err := stand.acquire(t)
	if err != nil {
		t.Fatalf("acquire against the incumbent view: %v", err)
	}
	if !incumbent.Masterchain.ID.Equals(&stand.incumbent.id) {
		t.Fatalf("incumbent slot stamped %d, want %d", incumbent.Masterchain.ID.SeqNo, stand.incumbent.id.SeqNo)
	}

	// The refresh a held barrier would have deferred: a newer masterchain view
	// registering the next shard top, with no session update behind it.
	successor, snapshot := stand.successor(t, stand.chain.registered[1].ID)
	stand.publish(t, successor, snapshot)

	before := stand.fallbacks()
	result, err := stand.acquire(t)
	if err != nil {
		t.Fatalf("acquire after a deferred masterchain refresh: %v", err)
	}
	if !result.Masterchain.ID.Equals(&successor.id) {
		t.Fatalf(
			"slot stamped masterchain %d, want the resident %d",
			result.Masterchain.ID.SeqNo,
			successor.id.SeqNo,
		)
	}
	if got := stand.fallbacks(); got != before {
		t.Fatalf("selected view fallbacks moved to %d for an admissible newer view", got)
	}

	expected, err := expectedShardNeighbors(result.Masterchain, stand.session.Shard)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Neighbors) != len(expected) {
		t.Fatalf("acquired %d neighbours, want %d", len(result.Neighbors), len(expected))
	}
	for i := range result.Neighbors {
		key := neighborShardKey{
			workchain: result.Neighbors[i].Block.Workchain,
			shard:     result.Neighbors[i].Block.Shard,
		}
		want, listed := expected[key]
		if !listed || !want.Equals(&result.Neighbors[i].Block) {
			t.Fatalf("neighbour %d is %s, which the selected view does not register", i, storage.FormatBlockRef(result.Neighbors[i].Block))
		}
	}

	// Both halves of the change: the block's master_ref advanced, and so did at
	// least one out-queue top it imports from.
	moved := 0
	for i := range result.Neighbors {
		for j := range incumbent.Neighbors {
			if result.Neighbors[i].Shard != incumbent.Neighbors[j].Shard {
				continue
			}
			if !result.Neighbors[i].Block.Equals(&incumbent.Neighbors[j].Block) {
				moved++
			}
		}
	}
	if moved == 0 {
		t.Fatal("every neighbour top stayed where the pinned view left it")
	}
}

// install puts one view in front of the pick without going through
// PublishMasterchainView. It exists for the single case that publish cannot
// stage — a snapshot that is not Ready, which the publish path refuses outright
// — and it is the sharper form of the same question: resident is a freshness
// rule, never an admission one, so the pick has to re-derive every binding for
// itself instead of trusting whatever installed the view.
func (s *viewPickStand) install(t *testing.T, block viewPickMasterBlock, snapshot *groups.Snapshot) {
	t.Helper()

	queueSize, state, err := residentMasterchainPredecessor(block.id, block.block, block.state)
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.acquisition.masterView(PreviousBlock{
		ID:           cloneBlockID(block.id),
		Block:        block.block,
		State:        block.state,
		OutQueueSize: &queueSize,
	}, state, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	s.acquisition.master.mu.Lock()
	s.acquisition.master.demoteLocked(s.acquisition.master.view)
	s.acquisition.master.view = view
	s.acquisition.master.mu.Unlock()
	s.acquisition.blocks.advance()
	s.commitMasterchainSource(t, block.id)
}

// viewPickRotate returns a copy of snapshot with mutate applied to it. The
// tracker publishes immutable snapshots that the incumbent view still points
// at, so a rotation has to be staged on a copy.
func viewPickRotate(snapshot *groups.Snapshot, mutate func(*groups.Snapshot)) *groups.Snapshot {
	rotated := *snapshot
	rotated.Active = append([]groups.Session(nil), snapshot.Active...)
	mutate(&rotated)

	return &rotated
}

func viewPickActiveIndex(t *testing.T, snapshot *groups.Snapshot, id [32]byte) int {
	t.Helper()

	for i := range snapshot.Active {
		if snapshot.Active[i].ID == id {
			return i
		}
	}
	t.Fatal("snapshot has no active entry for the session under test")

	return -1
}

// Every one of these is a routine event on a live chain: a group rotates, a
// catchain seqno advances, a shard is momentarily absent from a snapshot the
// tracker is still reconciling. A per-slot view pick that treated any of them
// as an error would turn the newest masterchain view from an improvement into a
// hazard — the slot would fail, produceWindowWithRetry would spin, and the
// leader window would be lost outright. Stepping down to the view the session
// was updated to costs one slot's worth of freshness and nothing else.
func TestAcquireShardKeepsIncumbentViewWhenTheRosterRotates(t *testing.T) {
	chain := newViewPickChain(t)

	rotations := []struct {
		name   string
		mutate func(t *testing.T, session ActivatedSession, snapshot *groups.Snapshot)
		ready  bool
	}{
		{
			name: "shard absent from the snapshot",
			mutate: func(t *testing.T, session ActivatedSession, snapshot *groups.Snapshot) {
				kept := snapshot.Active[:0:0]
				for i := range snapshot.Active {
					if snapshot.Active[i].Shard != session.Shard {
						kept = append(kept, snapshot.Active[i])
					}
				}
				snapshot.Active = kept
			},
			ready: true,
		},
		{
			name: "catchain seqno advanced",
			mutate: func(t *testing.T, session ActivatedSession, snapshot *groups.Snapshot) {
				at := viewPickActiveIndex(t, snapshot, session.ID)
				snapshot.Active[at].CatchainSeqno++
			},
			ready: true,
		},
		{
			name: "roster entry replaced",
			mutate: func(t *testing.T, session ActivatedSession, snapshot *groups.Snapshot) {
				at := viewPickActiveIndex(t, snapshot, session.ID)
				validators := append([]groups.Validator(nil), snapshot.Active[at].Validators...)
				validators[0].PublicKey[0] ^= 0xff
				snapshot.Active[at].Validators = validators
			},
			ready: true,
		},
		{
			name:   "snapshot not ready",
			mutate: func(*testing.T, ActivatedSession, *groups.Snapshot) {},
			ready:  false,
		},
		{
			name: "two active sessions for the shard",
			mutate: func(t *testing.T, session ActivatedSession, snapshot *groups.Snapshot) {
				at := viewPickActiveIndex(t, snapshot, session.ID)
				duplicate := snapshot.Active[at]
				duplicate.ID[0] ^= 0xff
				snapshot.Active = append(snapshot.Active, duplicate)
			},
			ready: true,
		},
	}

	for _, rotation := range rotations {
		t.Run(rotation.name, func(t *testing.T) {
			stand := newViewPickStandOn(t, chain, chain.registered[0].ID)
			successor, snapshot := stand.successor(t, chain.registered[1].ID)
			rotated := viewPickRotate(snapshot, func(copied *groups.Snapshot) {
				copied.Ready = rotation.ready
				rotation.mutate(t, stand.session, copied)
			})

			before := stand.fallbacks()
			if rotation.ready {
				stand.publish(t, successor, rotated)
			} else {
				stand.install(t, successor, rotated)
			}

			result, err := stand.acquire(t)
			if err != nil {
				t.Fatalf("acquire beside a rotated snapshot: %v", err)
			}
			if !result.Masterchain.ID.Equals(&stand.incumbent.id) {
				t.Fatalf(
					"slot stamped masterchain %d, want the incumbent %d",
					result.Masterchain.ID.SeqNo,
					stand.incumbent.id.SeqNo,
				)
			}
			if got := stand.fallbacks(); got != before+1 {
				t.Fatalf("selected view fallbacks = %d, want %d", got, before+1)
			}
		})
	}
}

// The registry moving ahead of our own head is not corruption: the masterchain
// registers a shard top this node has not built on yet, or registers another
// block at our predecessor's height because a sibling won that slot. Both are
// hard ErrInvalidInput at build time, and a slot that returned one would be
// non-retryable — the window would end there. The pick has to see the same rule
// before it commits to a view, and answer it by stepping down.
func TestAcquireShardRefusesViewListingATopAheadOfThePredecessor(t *testing.T) {
	chain := newViewPickChain(t)

	ahead := cloneBlockID(chain.previous.ID)
	ahead.SeqNo++
	ahead.RootHash = bytes.Repeat([]byte{0x4a}, 32)
	ahead.FileHash = bytes.Repeat([]byte{0x4b}, 32)

	forked := cloneBlockID(chain.previous.ID)
	forked.RootHash = bytes.Repeat([]byte{0x5a}, 32)
	forked.FileHash = bytes.Repeat([]byte{0x5b}, 32)

	for _, listed := range []struct {
		name string
		top  ton.BlockIDExt
	}{
		{name: "one block past the predecessor", top: ahead},
		{name: "another block at the predecessor height", top: forked},
	} {
		t.Run(listed.name, func(t *testing.T) {
			stand := newViewPickStandOn(t, chain, chain.registered[0].ID)
			successor, snapshot := stand.successor(t, listed.top)

			before := stand.fallbacks()
			stand.publish(t, successor, snapshot)

			result, err := stand.acquire(t)
			if errors.Is(err, ErrInvalidInput) {
				t.Fatalf("a refused view failed the slot instead of stepping down: %v", err)
			}
			if err != nil {
				t.Fatalf("acquire beside a registry ahead of the chain: %v", err)
			}
			if !result.Masterchain.ID.Equals(&stand.incumbent.id) {
				t.Fatalf(
					"slot stamped masterchain %d, want the incumbent %d",
					result.Masterchain.ID.SeqNo,
					stand.incumbent.id.SeqNo,
				)
			}
			if got := stand.fallbacks(); got != before+1 {
				t.Fatalf("selected view fallbacks = %d, want %d", got, before+1)
			}
		})
	}
}

// Our own validator resolves a candidate's masterchain reference by requiring
// the session's base masterchain block to be an ancestor of it. A block stamped
// with a view off that branch is one this node would refuse to validate for
// itself. The resident install only refuses to lower the seqno, which proves
// nothing about ancestry, so a newer seqno alone can never be enough — the
// ancestry has to be re-proved for every view the pick considers.
func TestAcquireShardRefusesNonDescendantMasterchainView(t *testing.T) {
	stand := newViewPickStand(t)

	// Same seqno as an ordinary successor, but its recorded history names some
	// other block at the incumbent's height.
	successor, snapshot := stand.successorOn(
		t,
		stand.chain.registered[1].ID,
		[]ton.BlockIDExt{stand.chain.anchor.id},
	)

	before := stand.fallbacks()
	stand.publish(t, successor, snapshot)

	result, err := stand.acquire(t)
	if err != nil {
		t.Fatalf("acquire beside a non-descendant view: %v", err)
	}
	if !result.Masterchain.ID.Equals(&stand.incumbent.id) {
		t.Fatalf(
			"slot stamped masterchain %d, want the incumbent %d",
			result.Masterchain.ID.SeqNo,
			stand.incumbent.id.SeqNo,
		)
	}
	if got := stand.fallbacks(); got != before+1 {
		t.Fatalf("selected view fallbacks = %d, want %d", got, before+1)
	}
}

// The +8 clause of check_prev_block: once the shard chain is eight blocks past
// what every admissible masterchain view registers, no view can carry a real
// block and only the masterchain moving can change that. produceWindowWithRetry
// has no attempt cap and its delay saturates, so a merely retryable refusal
// here spins for the rest of the window and emits nothing at all. The slot has
// to be told to degrade to an empty candidate instead.
func TestAcquireShardUnregisteredChainOfEightDegradesToEmpty(t *testing.T) {
	chain := newViewPickChain(t)

	stale := cloneBlockID(chain.previous.ID)
	stale.SeqNo -= 8
	stale.RootHash = bytes.Repeat([]byte{0x6a}, 32)
	stale.FileHash = bytes.Repeat([]byte{0x6b}, 32)

	stand := newViewPickStandOn(t, chain, stale)
	successor, snapshot := stand.successor(t, stale)
	stand.publish(t, successor, snapshot)

	_, err := stand.acquire(t)
	if !errors.Is(err, errCollationMustBeEmpty) {
		t.Fatalf("acquire error = %v, want errCollationMustBeEmpty", err)
	}
	if errors.Is(err, ErrInvalidInput) {
		t.Fatalf("degradation carried ErrInvalidInput, which fails the window: %v", err)
	}
	if retryableProductionError(err) {
		t.Fatal("a slot that no masterchain view can carry was reported as retryable")
	}
}
