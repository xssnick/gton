package collator

import (
	"github.com/xssnick/gton/service/validator/msgpool"

	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
)

func TestHistoryWeightUsesThreeSixteenBitWindows(t *testing.T) {
	cases := []struct {
		name    string
		history uint64
		want    int
	}{
		{name: "empty", want: -64},
		{name: "below_threshold", history: 0xffff | 0x7f<<16, want: -2},
		{name: "above_threshold", history: 0xffff | 0x1ff<<16, want: 2},
		{name: "exact_threshold_across_windows", history: 0xffff | 0xff<<16, want: 0},
		{name: "oldest_window_is_ignored", history: 1 << 48, want: -64},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := historyWeight(tc.history); got != tc.want {
				t.Fatalf("historyWeight(%#x) = %d, want %d", tc.history, got, tc.want)
			}
		})
	}
}

func TestLoadHistoryQueueBoundaries(t *testing.T) {
	limits := idleLimitStatus()
	cases := []struct {
		name          string
		load          LoadClass
		queueSize     uint64
		wantOverload  uint64
		wantUnderload uint64
	}{
		{name: "underload_merge_limit", load: LoadUnderload, queueSize: mergeMaxQueueSize, wantUnderload: 1},
		{name: "underload_above_merge_limit", load: LoadUnderload, queueSize: mergeMaxQueueSize + 1},
		{name: "queue_forces_overload", load: LoadNormal, queueSize: forceSplitQueueSize, wantOverload: 1},
		{name: "queue_below_force_split", load: LoadNormal, queueSize: forceSplitQueueSize - 1},
		{name: "soft_load_split_limit", load: LoadSoft, queueSize: splitMaxQueueSize, wantOverload: 1},
		{name: "soft_load_above_split_limit", load: LoadSoft, queueSize: splitMaxQueueSize + 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := collation{
				oldStats: tlb.ShardStateStats{
					OverloadHistory:  1 << 4,
					UnderloadHistory: 1 << 5,
				},
				limits:    limits,
				peakLoad:  tc.load,
				queueSize: tc.queueSize,
			}

			overload, underload, wantSplit, wantMerge := c.loadHistory()
			if overload != 1<<5|tc.wantOverload {
				t.Fatalf("overload history = %#x, want %#x", overload, 1<<5|tc.wantOverload)
			}
			if underload != 1<<6|tc.wantUnderload {
				t.Fatalf("underload history = %#x, want %#x", underload, 1<<6|tc.wantUnderload)
			}
			if wantSplit || wantMerge {
				t.Fatalf("unexpected split flags: split=%t merge=%t", wantSplit, wantMerge)
			}
		})
	}
}

func TestBuildEmptyBasechainCandidate(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{} // queues inspected and drained
	builder := testBuilder()

	first, err := builder.BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(first.BlockBOC, second.BlockBOC) {
		t.Fatal("same request produced different block BOCs")
	}
	if !bytes.Equal(first.CollatedData, second.CollatedData) {
		t.Fatal("same request produced different collated data")
	}
	firstBlock := candidateBlock(t, first)
	secondBlock := candidateBlock(t, second)
	if firstBlock.HashKey(0) != secondBlock.HashKey(0) || first.State.HashKey(0) != second.State.HashKey(0) {
		t.Fatal("same request produced different roots")
	}
	canonicalBlockBOC, err := firstBlock.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatalf("serialize canonical block BOC: %v", err)
	}
	if !bytes.Equal(first.BlockBOC, canonicalBlockBOC) {
		t.Fatal("block BOC does not use the canonical mode 31")
	}
	if first.Stats.Transactions != 0 || first.Stats.EndLT != requestStartLT(t, req)+1 || first.Stats.OutQueueSize != 0 {
		t.Fatalf("unexpected stats: %+v", first.Stats)
	}
	if len(first.Externals) != 0 {
		t.Fatalf("unexpected external results: %d", len(first.Externals))
	}

	fileHash := sha256.Sum256(first.BlockBOC)
	if !bytes.Equal(first.ID.RootHash, firstBlock.Hash()) || !bytes.Equal(first.ID.FileHash, fileHash[:]) {
		t.Fatal("candidate ID hashes do not match block data")
	}
	collatedHash := sha256.Sum256(first.CollatedData)
	if first.CollatedFileHash != collatedHash || first.CreatedBy != req.CreatedBy {
		t.Fatal("candidate metadata does not match its payload")
	}
	collatedRoots, err := cell.FromBOCMultiRoot(first.CollatedData)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	if len(collatedRoots) != 1 {
		t.Fatalf("collated roots = %d, want 1", len(collatedRoots))
	}
	collated := collatedRoots[0].MustBeginParse()
	if tag := collated.MustLoadUInt(32); tag != consensusExtraDataTag {
		t.Fatalf("collated data tag = %x", tag)
	}
	if flags := collated.MustLoadUInt(32); flags != 0 {
		t.Fatalf("collated data flags = %x", flags)
	}
	if got := collated.MustLoadUInt(64); got != req.Header.GenUtimeMS {
		t.Fatalf("collated generation time = %d, want %d", got, req.Header.GenUtimeMS)
	}

	var block tlb.Block
	if err = parseExact(&block, firstBlock); err != nil {
		t.Fatalf("decode block: %v", err)
	}
	if block.BlockInfo.SeqNo != req.Previous.ID.SeqNo+1 || block.BlockInfo.EndLt != first.Stats.EndLT {
		t.Fatalf("unexpected block header: %+v", block.BlockInfo)
	}
	if block.BlockInfo.WantSplit || block.BlockInfo.WantMerge {
		t.Fatalf("unexpected split flags: split=%t merge=%t", block.BlockInfo.WantSplit, block.BlockInfo.WantMerge)
	}

	var flow tlb.ValueFlow
	if err = parseExact(&flow, block.ValueFlow); err != nil {
		t.Fatalf("decode value flow: %v", err)
	}
	if !flow.FromPrevBlock.Equals(tlb.CurrencyCollection{}) || !flow.ToNextBlock.Equals(tlb.CurrencyCollection{}) {
		t.Fatalf("empty state changed account balance: from=%s to=%s", flow.FromPrevBlock.Coins, flow.ToNextBlock.Coins)
	}
	if !flow.Created.Equals(flow.FeesCollected) || flow.Created.Coins.Nano().Sign() <= 0 {
		t.Fatalf("unexpected creation fees: created=%s collected=%s", flow.Created.Coins, flow.FeesCollected.Coins)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, first.State); err != nil {
		t.Fatalf("decode next state: %v", err)
	}
	if state.Seqno != req.Previous.ID.SeqNo+1 || state.GenLT != first.Stats.EndLT ||
		state.ShardIdent.WorkchainID != req.Previous.ID.Workchain || int64(state.ShardIdent.GetShardID()) != req.Previous.ID.Shard {
		t.Fatalf("unexpected next state header: %+v", state)
	}
	applied, err := cell.ApplyMerkleUpdate(req.Previous.State, first.StateUpdate)
	if err != nil {
		t.Fatalf("apply state update: %v", err)
	}
	if applied.HashKey(0) != first.State.HashKey(0) {
		t.Fatal("state update does not produce candidate state")
	}

	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		t.Fatalf("decode next state statistics: %v", err)
	}
	if !stats.TotalBalance.Equals(flow.ToNextBlock) || !stats.TotalValidatorFees.Equals(flow.FeesCollected) {
		t.Fatalf("state statistics disagree with value flow: balance=%s fees=%s", stats.TotalBalance.Coins, stats.TotalValidatorFees.Coins)
	}
	if stats.MasterRef == nil || stats.MasterRef.SeqNo != req.Masterchain.ID.SeqNo {
		t.Fatalf("master reference was not advanced: %+v", stats.MasterRef)
	}

	assertEmptyCandidateQueue(t, req, state.OutMsgQueueInfo)
	if state.MinRefMCSeqno != req.Masterchain.ID.SeqNo {
		t.Fatalf("state min referenced masterchain seqno = %d, want %d", state.MinRefMCSeqno, req.Masterchain.ID.SeqNo)
	}
}

func TestReadPreviousPrewarmsLazyBlockStateUpdate(t *testing.T) {
	request := emptyCandidateRequest(t)
	request.Internals = &msgpool.Cut{}
	candidate, err := testBuilder().BuildShard(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	lazyBlock, err := cell.FromBOCWithOptions(candidate.BlockBOC, cell.BOCParseOptions{
		TrustedHashes: true,
		Lazy:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &lazyPreviousStore{
		block: candidate.ID,
		root:  lazyBlock,
		state: candidate.State,
	}
	acquisition := &LocalAcquisition{store: store}
	previous, state, err := acquisition.readPrevious(context.Background(), candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if previous.Block == nil || !bytes.Equal(previous.Block.Hash(), candidate.ID.RootHash) ||
		state.Seqno != candidate.ID.SeqNo || previous.State.HashKey() != candidate.State.HashKey() {
		t.Fatalf("read previous result is inconsistent: previous=%+v state_seqno=%d", previous.ID, state.Seqno)
	}
}

type lazyPreviousStore struct {
	block ton.BlockIDExt
	root  *cell.Cell
	state *cell.Cell
}

func (s *lazyPreviousStore) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !block.Equals(&s.block) {
		return nil, storage.ErrNotFound
	}
	hash := s.state.Hash()
	return &storage.BlockState{
		Block:         cloneBlockID(s.block),
		StateRootHash: hash[:],
		Cell:          s.state,
	}, nil
}

func (s *lazyPreviousStore) LoadStateCellTree(
	ctx context.Context,
	block ton.BlockIDExt,
	_ []byte,
) (*cell.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !block.Equals(&s.block) {
		return nil, storage.ErrNotFound
	}

	return s.state, nil
}

func (s *lazyPreviousStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !block.Equals(&s.block) {
		return nil, storage.ErrNotFound
	}

	return s.root, nil
}

func (s *lazyPreviousStore) WaitBlockArtifacts(ctx context.Context, block ton.BlockIDExt) error {
	_, err := s.BlockState(ctx, block)
	if err == nil && block.SeqNo != 0 {
		_, err = s.BlockRoot(ctx, block)
	}

	return err
}

func TestBuildCandidateEmitsFullCollatedStateProof(t *testing.T) {
	req := advanceCandidateRequest(t, emptyCandidateRequest(t))
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	first, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.CollatedData, second.CollatedData) {
		t.Fatal("full collated data is not deterministic")
	}

	roots, err := cell.FromBOCMultiRoot(first.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 3 {
		t.Fatalf("collated roots = %d, want consensus extra, previous-block proof, and previous-state proof", len(roots))
	}
	if roots[1].GetType() != cell.MerkleProofCellType {
		t.Fatalf("second collated root type = %v, want Merkle proof", roots[1].GetType())
	}
	previousBlock, err := cell.UnwrapProof(roots[1], req.Previous.ID.RootHash)
	if err != nil {
		t.Fatalf("unwrap previous-block proof: %v", err)
	}
	loader := previousBlock.MustBeginParse()
	loader.MustLoadUInt(64)
	if _, err = loader.LoadRefCell(); err != nil {
		t.Fatal(err)
	}
	if _, err = loader.LoadRefCell(); err != nil {
		t.Fatal(err)
	}
	stateUpdate, err := loader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if stateUpdate.MustPeekRef(1).HashKey(0) != req.Previous.State.HashKey(0) {
		t.Fatal("previous-block proof does not bind the supplied previous state")
	}

	state, err := cell.UnwrapProof(roots[2], req.Previous.State.Hash())
	if err != nil {
		t.Fatalf("unwrap previous-state proof: %v", err)
	}
	if state.HashKey() != req.Previous.State.HashKey() {
		t.Fatal("previous-state proof has a different virtual root")
	}
}

func TestBuildCandidateEmitsFullCollatedProofAfterZerostate(t *testing.T) {
	req := emptyCandidateRequest(t)
	rewritePreviousShardState(t, &req, func(state *tlb.ShardStateUnsplit) {
		state.Seqno = 0
	})
	req.Previous.ID.SeqNo = 0
	req.Previous.ID.RootHash = bytes.Clone(req.Previous.State.Hash())
	req.Previous.Block = nil
	req.Masterchain.Groups.Active[0].Registered[0].Block = *req.Previous.ID.Copy()
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 2 {
		t.Fatalf("collated roots = %d, want consensus extra and zerostate proof", len(roots))
	}
	state, err := cell.UnwrapProof(roots[1], req.Previous.State.Hash())
	if err != nil {
		t.Fatalf("unwrap zerostate proof: %v", err)
	}
	if state.HashKey() != req.Previous.State.HashKey() {
		t.Fatal("zerostate proof has a different virtual root")
	}
}

func TestBuildCollatedRootsIncludesOnlyUsedStorageStats(t *testing.T) {
	req := advanceCandidateRequest(t, emptyCandidateRequest(t))
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)
	oldRoot := req.Previous.State
	usage := cell.NewReadSet(oldRoot)
	estimator := newProofSizeEstimator(0)
	usage.SetRecordCallback(estimator.addLoadedCell)
	tracedRoot := usage.Root()
	// Read what prepare always reads: parsePredecessorParts decodes the state
	// and its outbound queue info, and the verifier of the constructed roots
	// decodes the queue info out of the previous-state proof. An unread leaf is
	// a pruned branch in that proof, so a collation assembled by hand has to make
	// the same reads or the verifier meets a boundary where prepare never leaves
	// one.
	var tracedState tlb.ShardStateUnsplit
	if err := parseExact(&tracedState, tracedRoot); err != nil {
		t.Fatal(err)
	}
	var tracedQueue tlb.OutMsgQueueInfo
	if err := parseExact(&tracedQueue, tracedState.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	used := storageStatTestDict(t)
	unused := storageStatTestDict(t)
	usedProofRecord := newAccountStorageProof(used)
	usedProof := usedProofRecord.builder
	if _, err := usedProof.Root().AsDict(256).LoadValueByBytesKey(storageStatTestKeys[0][:]); err != nil {
		t.Fatal(err)
	}
	unusedProofRecord := newAccountStorageProof(unused)
	c := &collation{
		ctx:     context.Background(),
		config:  req.Masterchain.Config,
		header:  tlb.BlockHeader{},
		oldRoot: tracedRoot,
		usage:   usage,
		req:     shardCollationRequest(req),
		lanes: map[[32]byte]*accountLane{
			{1}: {
				initialStorageStat: used,
				storageProof:       usedProof,
				touched:            true,
			},
			// A committed transaction that never reads its storage-stat dictionary
			// must not contribute an all-pruned proof.
			{2}: {
				initialStorageStat: unused,
				storageProof:       unusedProofRecord.builder,
				touched:            true,
			},
		},
		accountStorageProofs: map[cell.Hash]*accountStorageProof{
			used.HashKey():   usedProofRecord,
			unused.HashKey(): unusedProofRecord,
		},
		fullCollated:          true,
		collatedProofEstimate: estimator,
	}
	c.header.GenUtime = req.Header.GenUtime
	c.req.storageStats = AccountStorageStats{
		used.HashKey():   used,
		unused.HashKey(): unused,
	}
	// A second transaction of the same account reads another branch. The
	// incremental estimate and the emitted proof both have to carry that one too.
	c.trackAccountStorageProof(c.lanes[[32]byte{1}])
	if _, err := usedProof.Root().AsDict(256).LoadValueByBytesKey(storageStatTestKeys[4][:]); err != nil {
		t.Fatal(err)
	}
	c.trackAccountStorageProof(c.lanes[[32]byte{1}])
	consensusExtra := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(req.Header.GenUtimeMS, 64).
		EndCell()

	stateUpdate, err := cell.CreateMerkleUpdate(oldRoot, oldRoot)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := c.buildCollatedRoots(consensusExtra, stateUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 4 {
		t.Fatalf("collated roots = %d, want consensus, block/state proofs, and one storage proof", len(roots))
	}
	loader := roots[3].MustBeginParse()
	if tag := loader.MustLoadUInt(32); tag != 0x37c1e3fc {
		t.Fatalf("storage proof tag = %x", tag)
	}
	proof, err := loader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	storage, err := cell.UnwrapProof(proof, used.Hash())
	if err != nil {
		t.Fatalf("unwrap account storage proof: %v", err)
	}
	// Level 0 explicitly: the proof body carries pruned branches, so its top
	// level is not the one the proven dictionary root is addressed by.
	if storage.HashKey(0) != used.HashKey() {
		t.Fatal("account storage proof has a different virtual root")
	}
	// Entry 4 is the one only the second transaction read: the snapshot taken
	// after the first cannot prove it.
	for _, index := range []int{0, 4} {
		if _, err = storage.AsDict(256).LoadValueByBytesKey(storageStatTestKeys[index][:]); err != nil {
			t.Fatalf("read traced account storage entry %d: %v", index, err)
		}
	}
	if _, err = storage.AsDict(256).LoadValueByBytesKey(storageStatTestKeys[2][:]); err == nil {
		t.Fatal("unread account storage branch was included in the emitted proof")
	}
}

func TestFullCollatedPreviousStateProofIncludesAccountsRootExtra(t *testing.T) {
	req := emptyCandidateRequest(t)
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x5a}, 32))
	req.Previous.State = stateWithAccounts(
		t,
		req.Previous.State,
		accountsWithActiveContract(t, addr, req.Header.GenUtime, 100_000_000_000),
	)
	req = advanceCandidateRequest(t, req)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	var proven *cell.Cell
	for _, root := range roots {
		candidateRoot, unwrapErr := cell.UnwrapProof(root, req.Previous.State.Hash())
		if unwrapErr == nil {
			proven = candidateRoot
			break
		}
	}
	if proven == nil {
		t.Fatal("collated data has no previous state proof")
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, proven); err != nil {
		t.Fatal(err)
	}
	if _, err = state.Accounts.ShardAccounts.LoadRootExtra(); err != nil {
		t.Fatalf("load proven predecessor accounts root extra: %v", err)
	}
}

func TestFullCollatedPreviousStateProofCoversDispatchQueueDelta(t *testing.T) {
	req := emptyCandidateRequest(t)
	startLT := requestStartLT(t, req)
	sourceID := [32]byte{0x40}
	lts := make([]uint64, 30)
	for i := range lts {
		lts[i] = startLT - 300 + uint64(i)*10
	}
	dispatch := makeDispatchQueue(t, dispatchFixtureAccount{
		accountID: sourceID,
		bodyInRef: true,
		metadata: &tlb.MsgMetadata{
			Depth:       1,
			Initiator:   address.NewAddress(0, 0, sourceID[:]),
			InitiatorLT: startLT - 100,
		},
		lts: lts,
	})
	req.Previous.State = previousStateWithDispatchQueue(t, req.Previous.State, dispatch)
	req = advanceCandidateRequest(t, req)
	req.Dispatch = DispatchPolicy{
		DeferringEnabled:      true,
		DeferMessagesAfter:    100,
		Phase2MaxTotal:        19,
		Phase2MaxPerInitiator: 100,
	}
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	var proven *cell.Cell
	for _, root := range roots {
		candidateRoot, unwrapErr := cell.UnwrapProof(root, req.Previous.State.Hash())
		if unwrapErr == nil {
			proven = candidateRoot
			break
		}
	}
	if proven == nil {
		t.Fatal("collated data has no previous state proof")
	}

	var previousState tlb.ShardStateUnsplit
	if err = parseExact(&previousState, proven); err != nil {
		t.Fatal(err)
	}
	var previousQueue tlb.OutMsgQueueInfo
	if err = parseExact(&previousQueue, previousState.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	if previousQueue.Extra == nil || previousQueue.Extra.DispatchQueue == nil {
		t.Fatal("proven predecessor has no dispatch queue")
	}
	oldAccount, err := loadAccountDispatchQueue(previousQueue.Extra.DispatchQueue, sourceID)
	if err != nil {
		t.Fatalf("load proven predecessor dispatch account: %v", err)
	}
	for _, lt := range lts[1:21] {
		removed, loadErr := oldAccount.Messages.LoadValue(dispatchLTKey(lt))
		if loadErr != nil {
			t.Fatalf("load removed dispatch entry %d: %v", lt, loadErr)
		}
		var enqueued tlb.EnqueuedMsg
		if loadErr = loadExactSlice(&enqueued, removed); loadErr != nil {
			t.Fatalf("decode removed dispatch entry %d: %v", lt, loadErr)
		}
		var envelope tlb.MsgEnvelope
		if loadErr = parseExact(&envelope, enqueued.Msg); loadErr != nil {
			t.Fatalf("decode removed dispatch envelope %d: %v", lt, loadErr)
		}
		if _, loadErr = tvm.PrepareMessage(envelope.Msg); loadErr != nil {
			t.Fatalf("decode removed dispatch message %d: %v", lt, loadErr)
		}
	}
	if _, _, err = oldAccount.Messages.LoadMax(); err != nil {
		t.Fatalf("load predecessor dispatch maximum: %v", err)
	}
	if _, err = oldAccount.Messages.LoadValue(dispatchLTKey(lts[21])); err != nil {
		t.Fatalf("load candidate dispatch minimum from predecessor proof: %v", err)
	}

	nextQueue := candidateQueueInfo(t, candidate)
	nextAccount, err := loadAccountDispatchQueue(nextQueue.Extra.DispatchQueue, sourceID)
	if err != nil {
		t.Fatalf("load candidate dispatch account: %v", err)
	}
	if key, _, minErr := nextAccount.Messages.LoadMin(); minErr != nil {
		t.Fatalf("load candidate dispatch minimum: %v", minErr)
	} else if got := key.MustBeginParse().MustLoadUInt(64); got != lts[21] {
		t.Fatalf("candidate dispatch minimum = %d, want %d", got, lts[21])
	}
}

func TestPreviousStateProofCoversTransactionDispatchLookup(t *testing.T) {
	req := emptyCandidateRequest(t)
	startLT := requestStartLT(t, req)
	dispatch := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: [32]byte{0x00}, lts: []uint64{startLT - 20}},
		dispatchFixtureAccount{accountID: [32]byte{0xff}, lts: []uint64{startLT - 10}},
	)
	root := previousStateWithDispatchQueue(t, req.Previous.State, dispatch)
	usage := cell.NewReadSet(root)
	traced := usage.Root()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, traced); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	transactionAccount := [32]byte{0x80}
	c := &collation{
		oldDispatchQueue: queue.Extra.DispatchQueue,
		dispatchSources: [2]predecessorDispatchSource{{
			shard: state.ShardIdent,
			queue: queue.Extra.DispatchQueue,
		}},
		dispatchSourceCount: 1,
		dispatchQueue: &tlb.DispatchQueueAugDict{
			AugmentedDictionary: queue.Extra.DispatchQueue.Copy(),
		},
		shard: msgpool.ShardIdent{Workchain: req.Shard.Workchain, Shard: uint64(req.Shard.Shard)},
		lanes: map[[32]byte]*accountLane{
			transactionAccount: {touched: true},
		},
	}
	if err := c.traceDispatchQueueValidationClosure(); err != nil {
		t.Fatal(err)
	}

	proof, err := usage.Proof()
	if err != nil {
		t.Fatal(err)
	}
	proven, err := cell.UnwrapProof(proof, root.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if err = parseExact(&state, proven); err != nil {
		t.Fatal(err)
	}
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	_, err = queue.Extra.DispatchQueue.LoadValue(dispatchAccountKey(transactionAccount))
	if !isMissingKey(err) {
		t.Fatalf("transaction account dispatch lookup = %v, want absent", err)
	}
}

// storageStatTestKeys differ in their top three bits, so the dictionary they
// build is a balanced tree of four two-leaf forks. Reading one key materializes
// its own fork and leaves the other three behind pruned boundaries — the pair
// under a fork always travels together, because a childless leaf is cheaper to
// carry verbatim than to replace with a pruned branch.
var storageStatTestKeys = [][32]byte{
	{0x00}, {0x20}, {0x40}, {0x60}, {0x80}, {0xa0}, {0xc0}, {0xe0},
}

// storageStatTestDict stands in for an account's storage-stat dictionary: the
// shape matters, the contents do not.
func storageStatTestDict(tb testing.TB) *cell.Cell {
	tb.Helper()

	dict := cell.NewDict(256)
	for i := range storageStatTestKeys {
		if err := dict.SetBuilderByBytesKey(storageStatTestKeys[i][:], cell.BeginCell().MustStoreUInt(uint64(i+1), 32)); err != nil {
			tb.Fatal(err)
		}
	}
	root, err := dict.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

func TestTrackAccountStorageProofUsesDictionaryReads(t *testing.T) {
	keys := storageStatTestKeys
	root := storageStatTestDict(t)
	record := newAccountStorageProof(root)
	builder := record.builder
	if _, err := builder.Root().AsDict(256).LoadValueByBytesKey(keys[0][:]); err != nil {
		t.Fatal(err)
	}

	lane := &accountLane{
		initialStorageStat: root,
		storageProof:       builder,
	}
	c := &collation{
		fullCollated:          true,
		collatedProofEstimate: newProofSizeEstimator(0),
		accountStorageProofs:  map[cell.Hash]*accountStorageProof{root.HashKey(): record},
	}
	c.trackAccountStorageProof(lane)
	if c.collatedFixedEstimate == 0 {
		t.Fatal("used storage dictionary charged no collated bytes")
	}

	proof, err := builder.CreateProof()
	if err != nil {
		t.Fatal(err)
	}
	wrapped := wrapAccountStorageProof(proof)
	loader := wrapped.MustBeginParse()
	if tag := loader.MustLoadUInt(32); tag != accountStorageDictProofTag {
		t.Fatalf("account storage proof tag = %x", tag)
	}
	proof, err = loader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	proven, err := cell.UnwrapProof(proof, root.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = proven.AsDict(256).LoadValueByBytesKey(keys[0][:]); err != nil {
		t.Fatalf("read traced account storage entry: %v", err)
	}
	if _, err = proven.AsDict(256).LoadValueByBytesKey(keys[3][:]); err == nil {
		t.Fatal("unread account storage branch was included in usage proof")
	}
	boc, err := wrapped.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.collatedFixedEstimate < uint64(len(boc)) {
		t.Fatalf("storage proof estimate = %d, exact wrapped BOC = %d", c.collatedFixedEstimate, len(boc))
	}
}

func TestTrackAccountStorageProofSkipsUntouchedDictionary(t *testing.T) {
	root := storageStatTestDict(t)
	record := newAccountStorageProof(root)
	lane := &accountLane{
		initialStorageStat: root,
		storageProof:       record.builder,
	}
	c := &collation{
		fullCollated:          true,
		collatedProofEstimate: newProofSizeEstimator(0),
		accountStorageProofs:  map[cell.Hash]*accountStorageProof{root.HashKey(): record},
	}

	c.trackAccountStorageProof(lane)
	if c.collatedFixedEstimate != 0 {
		t.Fatalf("untouched storage dictionary charged %d collated bytes", c.collatedFixedEstimate)
	}
}

func TestBuildValueFlowPreservesAccountsAndAccruesCreationFee(t *testing.T) {
	config := loadMainnetConfig(t)
	accounts := accountsWithBalance(t, 25_000_000_000)
	accountBlocks, err := tlb.NewShardAccountBlocksAugDict()
	if err != nil {
		t.Fatal(err)
	}
	inMessages, err := tlb.NewInMsgDescrAugDict(config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	outMessages, err := tlb.NewOutMsgDescrAugDict(config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	oldBalance := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(25_000_000_000)}
	oldFees := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(777)}
	var header tlb.BlockHeader
	header.Shard = tlb.ShardIdent{WorkchainID: 0}
	c := collation{
		config: config,
		header: header,
		oldStats: tlb.ShardStateStats{
			TotalBalance:       oldBalance,
			TotalValidatorFees: oldFees,
		},
		accounts:      accounts,
		accountBlocks: accountBlocks,
		inMessages:    inMessages,
		outMessages:   outMessages,
	}

	built, err := c.buildValueFlow()
	if err != nil {
		t.Fatal(err)
	}
	var flow tlb.ValueFlow
	if err = parseExact(&flow, built.root); err != nil {
		t.Fatal(err)
	}
	if !built.totalBalance.Equals(oldBalance) || !flow.FromPrevBlock.Equals(oldBalance) || !flow.ToNextBlock.Equals(oldBalance) {
		t.Fatalf("account balance was not preserved: from=%s to=%s total=%s", flow.FromPrevBlock.Coins, flow.ToNextBlock.Coins, built.totalBalance.Coins)
	}
	if !flow.Created.Equals(flow.FeesCollected) || flow.Created.Coins.Nano().Sign() <= 0 {
		t.Fatalf("unexpected creation fee flow: created=%s collected=%s", flow.Created.Coins, flow.FeesCollected.Coins)
	}
	wantValidatorFees, err := oldFees.Add(flow.FeesCollected)
	if err != nil {
		t.Fatal(err)
	}
	if !built.validatorFees.Equals(wantValidatorFees) {
		t.Fatalf("validator fees = %s, want %s", built.validatorFees.Coins, wantValidatorFees.Coins)
	}
}

func TestBuildValueFlowRejectsBasechainBlackholeBurn(t *testing.T) {
	config := loadMainnetConfig(t)
	oldNano := uint64(25_000_000_000)
	burnedNano := uint64(700)
	accounts := accountsWithBalance(t, oldNano-burnedNano)
	accountBlocks, err := tlb.NewShardAccountBlocksAugDict()
	if err != nil {
		t.Fatal(err)
	}
	inMessages, err := tlb.NewInMsgDescrAugDict(config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	outMessages, err := tlb.NewOutMsgDescrAugDict(config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	burned := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(burnedNano)}
	var header tlb.BlockHeader
	header.Shard = tlb.ShardIdent{WorkchainID: 0}
	c := collation{
		config: config,
		header: header,
		oldStats: tlb.ShardStateStats{
			TotalBalance: tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(oldNano)},
		},
		accounts:      accounts,
		accountBlocks: accountBlocks,
		inMessages:    inMessages,
		outMessages:   outMessages,
		burned:        burned,
	}

	if _, err = c.buildValueFlow(); err == nil || !strings.Contains(err.Error(), "blackhole burn") {
		t.Fatalf("blackhole burn error = %v", err)
	}
}

func BenchmarkBuildEmptyBasechainCandidate(b *testing.B) {
	req := emptyCandidateRequest(b)
	builder := testBuilder()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := builder.BuildShard(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

func idleLimitStatus() *blockLimitStatus {
	thresholds := limitThresholds{10_000, 20_000, 30_000, 40_000}
	return newBlockLimitStatus(blockLimits{
		bytes:        thresholds,
		gas:          thresholds,
		ltDelta:      thresholds,
		collatedData: thresholds,
	}, 1, cell.NewReadSet(nil), 0, 0)
}

func TestExistingQueueSizeRejectsNonZeroSizeForEmptyQueue(t *testing.T) {
	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	info := tlb.OutMsgQueueInfo{OutQueue: queue}
	nonZero := uint64(1)
	if _, err = existingQueueSize(&info, &nonZero); err == nil {
		t.Fatal("empty queue with a non-zero supplied size must be rejected")
	}
	if size, err := existingQueueSize(&info, nil); err != nil || size != 0 {
		t.Fatalf("empty queue size = %d, err = %v", size, err)
	}
}

func emptyCandidateRequest(tb testing.TB) ShardRequest {
	tb.Helper()

	config := loadMainnetConfig(tb)
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	queueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: outQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}

	masterRef := testExtBlkRef(7_000_000, 0x11)
	stats, err := (tlb.ShardStateStats{MasterRef: masterRef}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	shard := tlb.ShardIdent{WorkchainID: 0}
	state := tlb.ShardStateUnsplit{
		GlobalID:        testConfigGlobalID(tb, config),
		ShardIdent:      shard,
		Seqno:           100,
		VertSeqno:       2,
		GenUTime:        1_900_000_000,
		GenLT:           masterRef.EndLt + 2_000,
		MinRefMCSeqno:   masterRef.SeqNo,
		OutMsgQueueInfo: queueInfo,
		Stats:           stats,
	}
	state.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}

	previous := testBlockID(0, int64(shard.GetShardID()), state.Seqno, 0x33)
	masterchain := testBlockID(address.MasterchainID, math.MinInt64, masterRef.SeqNo+1, 0x22)

	var seed [32]byte
	var creator [32]byte
	for i := range seed {
		seed[i] = byte(i + 1)
		creator[i] = byte(0x80 + i)
	}

	queueSize := uint64(0)
	return ShardRequest{
		Shard: groups.ShardID{
			Workchain: state.ShardIdent.WorkchainID,
			Shard:     int64(state.ShardIdent.GetShardID()),
		},
		Previous: PreviousBlock{
			ID:           previous,
			State:        stateRoot,
			OutQueueSize: &queueSize,
		},
		Masterchain: MasterchainContext{
			ID:              masterchain,
			EndLT:           uint64(masterchain.SeqNo) * 1_000,
			GenUtime:        state.GenUTime,
			VertSeqno:       state.VertSeqno,
			Config:          config,
			OutMsgQueueInfo: queueInfo,
			Groups: &groups.Snapshot{
				MasterchainBlock:  masterchain,
				ConfigRootHash:    config.execution.Root().HashKey(),
				GenUTime:          state.GenUTime,
				LastKeyBlockSeqno: masterRef.SeqNo,
				Ready:             true,
				Active: []groups.Session{{
					Shard:            groups.ShardID{Workchain: 0, Shard: int64(shard.GetShardID())},
					CatchainSeqno:    17,
					ValidatorSetHash: 0x10203040,
					Registered: []groups.ShardDescription{{
						Shard: groups.ShardID{Workchain: 0, Shard: int64(shard.GetShardID())},
						Block: *previous.Copy(),
					}},
				}},
			},
		},
		Header: HeaderParams{
			GenUtime:   state.GenUTime + 1,
			GenUtimeMS: uint64(state.GenUTime+1) * 1000,
		},
		RandSeed:            seed,
		CreatedBy:           creator,
		MaxExternalAttempts: 256,
	}
}

func testConfigGlobalID(tb testing.TB, config *Config) int32 {
	tb.Helper()
	value, err := (tlb.BlockchainConfig{Root: config.execution.Root()}).GetGlobalID()
	if err != nil {
		tb.Fatal(err)
	}
	return value.GlobalID
}

func requestStartLT(tb testing.TB, req ShardRequest) uint64 {
	tb.Helper()

	state := loadPreviousShardState(tb, req)
	startLT, err := nextBlockStartLT(state.GenLT, req.Masterchain.EndLT)
	if err != nil {
		tb.Fatal(err)
	}
	return startLT
}

func advanceCandidateRequest(tb testing.TB, req ShardRequest) ShardRequest {
	tb.Helper()

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		tb.Fatal(err)
	}
	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		tb.Fatal(err)
	}
	queueSize := candidate.Stats.OutQueueSize
	req.Previous = PreviousBlock{
		ID:           candidate.ID,
		Block:        blockRoot,
		State:        candidate.State,
		OutQueueSize: &queueSize,
	}
	req.Header.GenUtime++
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1_000
	return req
}

func attachFullCollatedTestNeighbors(tb testing.TB, req *ShardRequest) {
	tb.Helper()

	state := loadPreviousShardState(tb, *req)
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		tb.Fatalf("decode previous queue: %v", err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, uint64(state.ShardIdent.GetShardID()))
	if err != nil {
		tb.Fatalf("decode previous processed frontier: %v", err)
	}

	for i := range req.Masterchain.Groups.Active {
		session := &req.Masterchain.Groups.Active[i]
		if session.Shard != req.Shard || len(session.Registered) != 1 || session.Registered[0].Shard != req.Shard {
			continue
		}
		session.Registered[0].Block = *req.Previous.ID.Copy()
	}
	req.Neighbors = []Neighbor{
		{
			Block: req.Previous.ID,
			Shard: msgpool.ShardIdent{
				Workchain: req.Previous.ID.Workchain,
				Shard:     uint64(req.Previous.ID.Shard),
			},
			EndLT:     state.GenLT,
			Processed: records,
		},
		{
			Block: req.Masterchain.ID,
			Shard: msgpool.ShardIdent{
				Workchain: req.Masterchain.ID.Workchain,
				Shard:     uint64(req.Masterchain.ID.Shard),
			},
			EndLT: req.Masterchain.EndLT,
		},
	}
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return state.GenLT }
}

func testBuilder() *Builder {
	return NewBuilder(tvm.NewTVM(), tlb.GlobalVersion{Version: 1})
}

func candidateBlock(tb testing.TB, candidate *Candidate) *cell.Cell {
	tb.Helper()

	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

func testBlockID(workchain int32, shard int64, seqno uint32, fill byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{fill}, 32),
		FileHash:  bytes.Repeat([]byte{fill + 1}, 32),
	}
}

func loadMainnetConfig(tb testing.TB) *Config {
	tb.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("resolve test source path")
	}
	boc, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../../../../tonutils-go/tlb/testdata/blockchain_config_mainnet.boc"))
	if err != nil {
		tb.Fatal(err)
	}
	root, err := cell.FromBOC(boc)
	if err != nil {
		tb.Fatal(err)
	}
	return testPrepareConfig(tb, root)
}

// testPrepareConfig is the test-side mirror of localConfigCache.prepare: it
// captures the footprint of one configuration epoch and parses the tree the
// capture materialized, so every fixture Config reaches master collation the
// way a live one does — including the order the two run in.
func testPrepareConfig(tb testing.TB, root *cell.Cell) *Config {
	tb.Helper()

	resident, footprint := captureConfigFootprint(root)
	if footprint == nil {
		tb.Fatal("configuration footprint was not captured")
	}
	parsed, err := parseMasterConfigEpoch(resident)
	if err != nil {
		tb.Fatal(err)
	}
	parsed.config.footprint = footprint
	return parsed.config
}

func testExtBlkRef(seqno uint32, fill byte) *tlb.ExtBlkRef {
	rootHash := bytes.Repeat([]byte{fill}, 32)
	fileHash := bytes.Repeat([]byte{fill + 1}, 32)
	return &tlb.ExtBlkRef{
		EndLt:    uint64(seqno) * 1_000,
		SeqNo:    seqno,
		RootHash: rootHash,
		FileHash: fileHash,
	}
}

func accountsWithBalance(tb testing.TB, nano uint64) *tlb.ShardAccountsAugDict {
	tb.Helper()

	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x44}, 32))
	account, err := (tlb.AccountState{
		IsValid: true,
		Address: addr,
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: big.NewInt(0),
				BitsUsed:  big.NewInt(0),
			},
			StorageExtra: tlb.StorageExtraNone{},
		},
		AccountStorage: tlb.AccountStorage{
			Status:  tlb.AccountStatusUninit,
			Balance: tlb.FromNanoTONU(nano),
		},
	}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       account,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		tb.Fatal(err)
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(addr.Data(), 256).EndCell()
	if err = accounts.Set(key, shardAccount); err != nil {
		tb.Fatal(err)
	}
	return accounts
}

func assertEmptyCandidateQueue(t *testing.T, req ShardRequest, root *cell.Cell) {
	t.Helper()

	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, root); err != nil {
		t.Fatalf("decode next outbound queue: %v", err)
	}
	if !queue.OutQueue.IsEmpty() {
		t.Fatal("empty collation produced outbound messages")
	}
	// A complete and empty inbound cut drains the queues: the collation claims
	// everything below the masterchain reference lt with an infinity record.
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	want := tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   req.Masterchain.EndLT - 1,
		LastMsgHash: processedInfinityHash,
	}
	if len(records) != 1 || records[0] != want {
		t.Fatalf("processed info records = %+v, want the infinity record %+v", records, want)
	}
}
