package service

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLoadStoredBlockForApplyLoadsProof(t *testing.T) {
	store := openTestPebbleStorage(t)
	downloaded := mustLoadFixtureDownloadedBlock(t)
	proofBOC := []byte{0x70, 0x71, 0x72}

	if _, err := store.SaveStateCheckpointEntries(context.Background(), []tnstore.StateCheckpointBlock{{
		Artifact: &tnstore.ServedBlockFull{
			ID:             downloaded.ID,
			Block:          downloaded.BlockBOC,
			Proof:          proofBOC,
			Meta:           downloaded.Meta,
			IsLink:         downloaded.IsLink,
			MessageEntries: []tnstore.MessageTransactionIndexEntry{},
		},
	}}, tnstore.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save fixture block: %v", err)
	}

	prepared, err := loadStoredBlockForApply(context.Background(), store, downloaded.ID, false)
	if err != nil {
		t.Fatalf("load stored block: %v", err)
	}
	if string(prepared.BlockBOC) != string(downloaded.BlockBOC) {
		t.Fatal("stored block data was not loaded")
	}
	if string(prepared.ProofBOC) != string(proofBOC) {
		t.Fatal("stored proof was not loaded")
	}
	if prepared.IsLink != downloaded.IsLink {
		t.Fatalf("stored proof link flag = %v, want %v", prepared.IsLink, downloaded.IsLink)
	}
}

func TestApplyBlockFromFixture(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, downloaded.Block.MustBeginParse()); err != nil {
		t.Fatalf("load fixture tlb block: %v", err)
	}

	oldStateCell := block.StateUpdate.MustPeekRef(0)
	var oldParsed tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&oldParsed, oldStateCell.MustBeginParse()); err != nil {
		t.Fatalf("parse old state from update: %v", err)
	}

	oldWC, oldShard := tlb.ConvertShardIdentToShard(oldParsed.ShardIdent)
	oldStateHash := oldStateCell.HashKey(0)
	current, err := tnstore.ParseStateCell(&ton.BlockIDExt{
		Workchain: oldWC,
		Shard:     int64(oldShard),
		SeqNo:     oldParsed.Seqno,
	}, oldStateCell, oldStateCell.ToBOCWithOptions(cell.BOCSerializeOptions{}), oldStateHash[:], nil)
	if err != nil {
		t.Fatalf("build current state from old update branch: %v", err)
	}

	prepared, err := prepareDownloadedBlockForApply(*downloaded)
	if err != nil {
		t.Fatalf("prepare block: %v", err)
	}

	applied, err := applyBlockWithPreviousStates([]*tnstore.BlockState{current}, prepared, nil)
	if err != nil {
		t.Fatalf("apply block: %v", err)
	}
	next := applied.Next

	if !next.Block.Equals(&downloaded.ID) {
		t.Fatalf("unexpected next block id %s", tnstore.FormatBlockRef(next.Block))
	}
	if next.Parsed == nil {
		t.Fatal("expected parsed next state")
	}
	if next.Parsed.Seqno != downloaded.ID.SeqNo {
		t.Fatalf("unexpected next state seqno %d", next.Parsed.Seqno)
	}

	newStateCell := block.StateUpdate.MustPeekRef(1)
	newStateHash := newStateCell.HashKey(0)
	if got, want := hex.EncodeToString(next.StateRootHash), hex.EncodeToString(newStateHash[:]); got != want {
		t.Fatalf("unexpected next state root hash %s want %s", got, want)
	}
}

func TestPrepareBlockDataRejectsBlockIDHashMismatches(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)

	tests := []struct {
		name   string
		mutate func(*ton.BlockIDExt)
	}{
		{
			name: "root hash mismatch",
			mutate: func(id *ton.BlockIDExt) {
				id.RootHash = append([]byte(nil), id.RootHash...)
				id.RootHash[0] ^= 0x01
			},
		},
		{
			name: "file hash mismatch",
			mutate: func(id *ton.BlockIDExt) {
				id.FileHash = append([]byte(nil), id.FileHash...)
				id.FileHash[0] ^= 0x01
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := downloaded.ID
			id.RootHash = append([]byte(nil), id.RootHash...)
			id.FileHash = append([]byte(nil), id.FileHash...)
			tt.mutate(&id)

			if _, err := prepareBlockData("fixture block", id, downloaded.BlockBOC); err == nil {
				t.Fatal("block data with mismatched BlockID hash was accepted")
			}
		})
	}
}

func TestApplyStoredMasterchainTransitionDoesNotRequireProof(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	prepared, err := prepareDownloadedBlockForApply(*downloaded)
	if err != nil {
		t.Fatalf("prepare fixture block: %v", err)
	}

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, downloaded.Block.MustBeginParse()); err != nil {
		t.Fatalf("load fixture tlb block: %v", err)
	}

	oldStateCell := block.StateUpdate.MustPeekRef(0)
	var oldParsed tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&oldParsed, oldStateCell.MustBeginParse()); err != nil {
		t.Fatalf("parse old state from update: %v", err)
	}

	oldWC, oldShard := tlb.ConvertShardIdentToShard(oldParsed.ShardIdent)
	oldStateHash := oldStateCell.HashKey(0)
	current, err := tnstore.ParseStateCell(&ton.BlockIDExt{
		Workchain: oldWC,
		Shard:     int64(oldShard),
		SeqNo:     oldParsed.Seqno,
	}, oldStateCell, oldStateCell.ToBOCWithOptions(cell.BOCSerializeOptions{}), oldStateHash[:], nil)
	if err != nil {
		t.Fatalf("build current state from old update branch: %v", err)
	}
	current.Block = prepared.Meta.PrevRefs[0]

	prepared.ProofBOC = nil
	next, _, err := (&Service{}).applyMasterchainTransition(context.Background(), current, prepared, nil, nil, nil)
	if err != nil {
		t.Fatalf("apply stored masterchain transition: %v", err)
	}
	if !next.Block.Equals(&downloaded.ID) {
		t.Fatalf("unexpected next block id %s", tnstore.FormatBlockRef(next.Block))
	}
}

func TestApplyBlockRejectsWrongCurrentState(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)

	current := &tnstore.BlockState{
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     downloaded.ID.SeqNo - 1,
		},
		StateRootHash: bytes32(0x42),
	}

	prepared, err := prepareDownloadedBlockForApply(*downloaded)
	if err != nil {
		t.Fatalf("prepare block: %v", err)
	}

	if _, err := applyBlockWithPreviousStates([]*tnstore.BlockState{current}, prepared, nil); err == nil {
		t.Fatal("expected apply block to reject wrong current state")
	}
}

func TestMerkleUpdateApplyRejectsStateRootHashMismatch(t *testing.T) {
	from := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	to := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	update := mustMerkleUpdateCell(t, from, to)

	window := newStateCellWindowCache(nil)
	_, err := window.applyMerkleUpdate([]*tnstore.BlockState{{
		Block:         testBlockID(-1, topShard, 1),
		StateRootHash: bytes32(0x42),
		Cell:          from,
	}}, update, mustPreparedStateUpdateCells(t, update))
	if err == nil {
		t.Fatal("expected state root hash mismatch to reject merkle update")
	}
}

func TestMerkleUpdateWindowCacheDoesNotLoadOldTreeDuringApply(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	rightLeaf := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	right := cell.BeginCell().MustStoreRef(rightLeaf).EndCell()
	from := cell.BeginCell().
		MustStoreUInt(0x33, 8).
		MustStoreRef(left).
		MustStoreRef(right).
		EndCell()

	nextLeft := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	to := cell.BeginCell().
		MustStoreUInt(0x55, 8).
		MustStoreRef(nextLeft).
		MustStoreRef(right).
		EndCell()
	update := mustMerkleUpdateCell(t, from, mustProofBody(t, to, 0))

	var baseLoads int
	base := func(hash cell.Hash) (*cell.Cell, error) {
		if hash != right.HashKey() {
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
		baseLoads++
		return right, nil
	}

	window := newStateCellWindowCache(base)
	applied, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: from}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply cached merkle update: %v", err)
	}
	got := applied.NextRoot
	if got.HashKey() != to.HashKey() {
		t.Fatalf("applied root hash mismatch: got=%x want=%x", got.HashKey(), to.HashKey())
	}
	if baseLoads != 0 {
		t.Fatalf("base loader calls = %d, want 0", baseLoads)
	}

	loadedRight, err := got.PeekRef(1)
	if err != nil {
		t.Fatalf("load reused right ref: %v", err)
	}
	loadedRightSlice, err := loadedRight.BeginParse()
	if err != nil {
		t.Fatalf("materialize reused right ref: %v", err)
	}
	loadedRight = loadedRightSlice.BaseCell()
	if loadedRight.HashKey() != right.HashKey() {
		t.Fatalf("right ref hash mismatch: got=%x want=%x", loadedRight.HashKey(), right.HashKey())
	}
	if baseLoads != 1 {
		t.Fatalf("base loader calls after loading reused ref = %d, want 1", baseLoads)
	}
}

func TestMerkleUpdateWindowCacheStoresDestinationCellsWithLogicalHashes(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	from := cell.BeginCell().
		MustStoreUInt(0x33, 8).
		MustStoreRef(left).
		MustStoreRef(right).
		EndCell()

	prunedLeft := mustPrunedBranch(t, left)
	to := cell.BeginCell().
		MustStoreUInt(0x44, 8).
		MustStoreRef(prunedLeft).
		MustStoreRef(right).
		EndCell()
	update := mustMerkleUpdateCell(t, from, to)

	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		switch hash {
		case left.HashKey():
			return left, nil
		case right.HashKey():
			return right, nil
		default:
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
	})
	applied, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: from}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply direct merkle update: %v", err)
	}
	got := applied.NextRoot
	want, _, err := cell.ApplyMerkleUpdate(from.Virtualize(0), update)
	if err != nil {
		t.Fatalf("apply reference merkle update: %v", err)
	}
	assertResolvedCellTreeEqual(t, got, want)

	checkpoint := window.beginCheckpoint()
	records := checkpoint.records()
	directRoot := to.Virtualize(0)
	if !hasCellRecord(records, directRoot.HashKey()) {
		t.Fatal("checkpoint cache does not contain destination root")
	}
	if rawRootHash := to.HashKey(); rawRootHash != directRoot.HashKey() && hasCellRecord(records, rawRootHash) {
		t.Fatal("checkpoint cache contains raw high-level destination root")
	}
	if hasCellRecord(records, prunedLeft.HashKey()) {
		t.Fatal("checkpoint cache contains pruned destination boundary")
	}
}

func TestMerkleUpdateWindowCacheCrossChecksVirtualizedDestinationWithApplyMerkleUpdate(t *testing.T) {
	oldLeaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	stableLeaf := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	oldBranch := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(oldLeaf).EndCell()
	fromRoot := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(stableLeaf).
		EndCell()

	prunedOldLeaf := mustPrunedBranch(t, oldLeaf)
	prunedStable := mustPrunedBranch(t, stableLeaf)
	updateFrom := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(prunedStable).
		EndCell()
	updateToBranch := cell.BeginCell().
		MustStoreUInt(0xcc, 8).
		MustStoreRef(prunedOldLeaf).
		EndCell()
	if updateToBranch.Level() == 0 {
		t.Fatal("test update destination branch must have non-zero level")
	}
	logicalToBranch := updateToBranch.Virtualize(0)
	if logicalToBranch.HashKey() == updateToBranch.HashKey() {
		t.Fatal("test update destination branch must have distinct logical and raw hashes")
	}

	updateTo := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(updateToBranch).
		MustStoreRef(prunedStable).
		EndCell()
	directRoot := updateTo.Virtualize(0)
	if directRoot.HashKey() == updateTo.HashKey() {
		t.Fatal("test update destination root must have distinct logical and raw hashes")
	}
	update := mustMerkleUpdateCell(t, updateFrom, updateTo)

	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		switch hash {
		case oldLeaf.HashKey():
			return oldLeaf, nil
		case stableLeaf.HashKey():
			return stableLeaf, nil
		default:
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
	})
	applied, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: fromRoot}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply direct merkle update: %v", err)
	}
	got := applied.NextRoot

	want, _, err := cell.ApplyMerkleUpdate(fromRoot.Virtualize(0), update)
	if err != nil {
		t.Fatalf("apply reference merkle update: %v", err)
	}
	assertResolvedCellTreeEqual(t, got, want)

	checkpoint := window.beginCheckpoint()
	records := checkpoint.records()
	if !hasCellRecord(records, directRoot.HashKey()) {
		t.Fatal("checkpoint cache does not contain logical destination root")
	}
	if !hasCellRecord(records, logicalToBranch.HashKey()) {
		t.Fatal("checkpoint cache does not contain logical destination branch")
	}
	if hasCellRecord(records, updateTo.HashKey()) {
		t.Fatal("checkpoint cache contains raw high-level destination root")
	}
	if hasCellRecord(records, updateToBranch.HashKey()) {
		t.Fatal("checkpoint cache contains raw high-level destination branch")
	}
	if hasCellRecord(records, prunedOldLeaf.HashKey()) || hasCellRecord(records, prunedStable.HashKey()) {
		t.Fatal("checkpoint cache contains pruned destination boundary")
	}
}

func TestMerkleUpdateWindowCacheUsesPreparedDestinationCells(t *testing.T) {
	oldLeaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	stableLeaf := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	oldBranch := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(oldLeaf).EndCell()
	fromRoot := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(stableLeaf).
		EndCell()

	prunedOldLeaf := mustPrunedBranch(t, oldLeaf)
	prunedStable := mustPrunedBranch(t, stableLeaf)
	updateFrom := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(prunedStable).
		EndCell()
	updateToBranch := cell.BeginCell().
		MustStoreUInt(0xcc, 8).
		MustStoreRef(prunedOldLeaf).
		EndCell()
	updateTo := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(updateToBranch).
		MustStoreRef(prunedStable).
		EndCell()
	update := mustMerkleUpdateCell(t, updateFrom, updateTo)
	prepared, err := tnstore.PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatalf("prepare destination cells: %v", err)
	}

	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		switch hash {
		case oldLeaf.HashKey():
			return oldLeaf, nil
		case stableLeaf.HashKey():
			return stableLeaf, nil
		default:
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
	})
	applied, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: fromRoot}}, update, prepared)
	if err != nil {
		t.Fatalf("apply prepared merkle update: %v", err)
	}
	got := applied.NextRoot

	want, _, err := cell.ApplyMerkleUpdate(fromRoot.Virtualize(0), update)
	if err != nil {
		t.Fatalf("apply reference merkle update: %v", err)
	}
	assertResolvedCellTreeEqual(t, got, want)

	checkpoint := window.beginCheckpoint()
	records := checkpoint.records()
	for _, record := range prepared.Records {
		if !hasCellRecord(records, record.Hash) {
			t.Fatalf("checkpoint cache does not contain prepared cell %x", record.Hash[:])
		}
	}
	if hasCellRecord(records, prunedOldLeaf.HashKey()) || hasCellRecord(records, prunedStable.HashKey()) {
		t.Fatal("checkpoint cache contains prepared pruned boundary")
	}
}

func TestMerkleUpdateWindowCacheRejectsPreparedDestinationWithoutRoot(t *testing.T) {
	from := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	to := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	update := mustMerkleUpdateCell(t, from, to)
	prepared, err := tnstore.PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatalf("prepare destination cells: %v", err)
	}
	prepared = removePreparedCellRecord(prepared, to.Virtualize(0).HashKey())

	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		return nil, fmt.Errorf("unexpected base load %x", hash[:])
	})
	_, err = window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: from}}, update, prepared)
	if err == nil || !strings.Contains(err.Error(), "prepared state update cells") {
		t.Fatalf("apply with incomplete prepared cells err = %v, want prepared cells error", err)
	}
}

func TestMerkleUpdateWindowCacheSupportsMergePreviousStates(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	mergeRoot := cell.BeginCell().
		MustStoreUInt(0x5f327da5, 32).
		MustStoreRef(left).
		MustStoreRef(right).
		EndCell()
	to := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	update := mustMerkleUpdateCell(t, mergeRoot, to)

	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		return nil, fmt.Errorf("unexpected base load %x", hash[:])
	})
	applied, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: left}, {Cell: right}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply merge merkle update: %v", err)
	}
	got := applied.NextRoot
	if got.HashKey() != to.HashKey() {
		t.Fatalf("merge destination hash mismatch: got=%x want=%x", got.HashKey(), to.HashKey())
	}
}

func TestStateCellWindowCacheCarriesCellsAcrossMerkleUpdates(t *testing.T) {
	stableLeaf := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	stable := cell.BeginCell().MustStoreRef(stableLeaf).EndCell()
	change0 := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	change1 := cell.BeginCell().MustStoreUInt(0x21, 8).EndCell()
	change2 := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()

	state0 := cell.BeginCell().
		MustStoreUInt(0x30, 8).
		MustStoreRef(stable).
		MustStoreRef(change0).
		EndCell()
	state1 := cell.BeginCell().
		MustStoreUInt(0x31, 8).
		MustStoreRef(stable).
		MustStoreRef(change1).
		EndCell()
	state2 := cell.BeginCell().
		MustStoreUInt(0x32, 8).
		MustStoreRef(stable).
		MustStoreRef(change2).
		EndCell()

	update1 := mustMerkleUpdateCell(t, state0, mustProofBody(t, state1, 1))
	update2 := mustMerkleUpdateCell(t, mustProofBody(t, state1, 1), mustProofBody(t, state2, 1))

	var baseLoads int
	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		if hash != stable.HashKey() {
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
		baseLoads++
		return stable, nil
	})

	applied1, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: state0}}, update1, mustPreparedStateUpdateCells(t, update1))
	if err != nil {
		t.Fatalf("apply first update: %v", err)
	}
	root1 := applied1.NextRoot
	if root1.HashKey() != state1.HashKey() {
		t.Fatalf("first state hash mismatch: got=%x want=%x", root1.HashKey(), state1.HashKey())
	}

	applied2, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: root1}}, update2, mustPreparedStateUpdateCells(t, update2))
	if err != nil {
		t.Fatalf("apply second update: %v", err)
	}
	root2 := applied2.NextRoot
	if root2.HashKey() != state2.HashKey() {
		t.Fatalf("second state hash mismatch: got=%x want=%x", root2.HashKey(), state2.HashKey())
	}

	loadedStable, err := root2.PeekRef(0)
	if err != nil {
		t.Fatalf("load stable ref from second state: %v", err)
	}
	loadedStableSlice, err := loadedStable.BeginParse()
	if err != nil {
		t.Fatalf("materialize stable ref from second state: %v", err)
	}
	loadedStable = loadedStableSlice.BaseCell()
	if loadedStable.HashKey() != stable.HashKey() {
		t.Fatalf("stable ref hash mismatch: got=%x want=%x", loadedStable.HashKey(), stable.HashKey())
	}
	if baseLoads != 1 {
		t.Fatalf("base loader calls = %d, want 1", baseLoads)
	}

	checkpoint := window.beginCheckpoint()
	records := checkpoint.records()
	if !hasCellRecord(records, change1.HashKey()) {
		t.Fatal("checkpoint cache does not contain intermediate changed cell")
	}
	if hasCellRecord(records, stable.HashKey()) {
		t.Fatal("checkpoint cache contains source-only stable cell")
	}
}

func TestArchiveStateCellOverlayCarriesCellsAcrossMerkleUpdates(t *testing.T) {
	stableLeaf := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	stable := cell.BeginCell().MustStoreRef(stableLeaf).EndCell()
	change0 := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	change1 := cell.BeginCell().MustStoreUInt(0x21, 8).EndCell()
	change2 := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()

	state0 := cell.BeginCell().
		MustStoreUInt(0x30, 8).
		MustStoreRef(stable).
		MustStoreRef(change0).
		EndCell()
	state1 := cell.BeginCell().
		MustStoreUInt(0x31, 8).
		MustStoreRef(stable).
		MustStoreRef(change1).
		EndCell()
	state2 := cell.BeginCell().
		MustStoreUInt(0x32, 8).
		MustStoreRef(stable).
		MustStoreRef(change2).
		EndCell()

	update1 := mustMerkleUpdateCell(t, state0, mustProofBody(t, state1, 1))
	update2 := mustMerkleUpdateCell(t, mustProofBody(t, state1, 1), mustProofBody(t, state2, 1))

	var baseLoads int
	overlay := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		if hash != stable.HashKey() {
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
		baseLoads++
		return stable, nil
	})

	applied1, err := overlay.applyPreparedMerkleUpdate([]*tnstore.BlockState{{Cell: state0}}, update1, mustPreparedStateUpdateCells(t, update1), nil, 0)
	if err != nil {
		t.Fatalf("apply first archive update: %v", err)
	}
	root1 := applied1.NextRoot
	if root1.HashKey() != state1.HashKey() {
		t.Fatalf("first state hash mismatch: got=%x want=%x", root1.HashKey(), state1.HashKey())
	}

	applied2, err := overlay.applyPreparedMerkleUpdate([]*tnstore.BlockState{{Cell: root1}}, update2, mustPreparedStateUpdateCells(t, update2), nil, 0)
	if err != nil {
		t.Fatalf("apply second archive update: %v", err)
	}
	root2 := applied2.NextRoot
	if root2.HashKey() != state2.HashKey() {
		t.Fatalf("second state hash mismatch: got=%x want=%x", root2.HashKey(), state2.HashKey())
	}

	loadedStable, err := root2.PeekRef(0)
	if err != nil {
		t.Fatalf("load stable ref from second archive state: %v", err)
	}
	loadedStableSlice, err := loadedStable.BeginParse()
	if err != nil {
		t.Fatalf("materialize stable ref from second archive state: %v", err)
	}
	loadedStable = loadedStableSlice.BaseCell()
	if loadedStable.HashKey() != stable.HashKey() {
		t.Fatalf("stable ref hash mismatch: got=%x want=%x", loadedStable.HashKey(), stable.HashKey())
	}
	if baseLoads != 1 {
		t.Fatalf("base loader calls = %d, want 1", baseLoads)
	}

	checkpoint := overlay.beginCheckpoint()
	records := checkpoint.records()
	if !hasCellRecord(records, change1.HashKey()) {
		t.Fatal("archive checkpoint cache does not contain intermediate changed cell")
	}
	if hasCellRecord(records, stable.HashKey()) {
		t.Fatal("archive checkpoint cache contains source-only stable cell")
	}
}

func TestArchiveStateCellOverlayRetriesPendingCheckpointCells(t *testing.T) {
	state0 := cell.BeginCell().MustStoreUInt(0x10, 8).EndCell()
	state1 := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	state2 := cell.BeginCell().MustStoreUInt(0x12, 8).EndCell()

	overlay := newStateCellWindowCache(nil)
	update1 := mustMerkleUpdateCell(t, state0, state1)
	applied1, err := overlay.applyPreparedMerkleUpdate([]*tnstore.BlockState{{Cell: state0}}, update1, mustPreparedStateUpdateCells(t, update1), nil, 0)
	if err != nil {
		t.Fatalf("apply first archive update: %v", err)
	}
	root1 := applied1.NextRoot
	failedCheckpoint := overlay.beginCheckpoint()
	failedRecords := failedCheckpoint.records()
	if !hasCellRecord(failedRecords, state1.HashKey()) {
		t.Fatal("first archive checkpoint does not contain first state")
	}

	update2 := mustMerkleUpdateCell(t, state1, state2)
	if _, err = overlay.applyPreparedMerkleUpdate([]*tnstore.BlockState{{Cell: root1}}, update2, mustPreparedStateUpdateCells(t, update2), nil, 0); err != nil {
		t.Fatalf("apply second archive update: %v", err)
	}
	nextCheckpoint := overlay.beginCheckpoint()
	records := nextCheckpoint.records()
	if !hasCellRecord(records, state1.HashKey()) {
		t.Fatal("retry archive checkpoint does not include still-pending first state")
	}
	if !hasCellRecord(records, state2.HashKey()) {
		t.Fatal("retry archive checkpoint does not include new active state")
	}

	nextCheckpoint.complete()
	if stale := overlay.beginCheckpoint(); stale != nil {
		t.Fatal("completed archive checkpoint left pending cells behind")
	}
}

func TestServiceStateCellLoaderKeepsArchiveCheckpointCellsVisible(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()

	overlay := newStateCellWindowCache(nil)
	overlay.addPreparedRecords(mustPreparedReachableStateCells(t, root))
	checkpoint := overlay.beginCheckpoint()
	if checkpoint == nil {
		t.Fatal("archive checkpoint is nil")
	}

	svc := &Service{}
	release := svc.retainStateCellLoader(checkpoint.loader())
	loader := svc.stateCellLoader()

	loaded, err := loader(root.HashKey())
	if err != nil {
		t.Fatalf("load pending archive checkpoint cell: %v", err)
	}
	if loaded.HashKey() != root.HashKey() {
		t.Fatalf("loaded hash mismatch: got=%x want=%x", loaded.HashKey(), root.HashKey())
	}

	release()
	if _, err = loader(root.HashKey()); err == nil {
		t.Fatal("released archive checkpoint cell loader still resolves cells")
	}
}

func TestStateCellWindowRetainedLoaderFallsThroughToBaseRefs(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0x43, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	records := removePreparedCellRecord(mustPreparedReachableStateCells(t, root), child.HashKey())

	window := newStateCellWindowCache(nil)
	if err := window.addPreparedRecords(records); err != nil {
		t.Fatalf("remember window records: %v", err)
	}

	baseLoads := 0
	loader := window.retainedLoader(func(hash cell.Hash) (*cell.Cell, error) {
		baseLoads++
		if hash == child.HashKey() {
			return child, nil
		}
		return nil, tnstore.ErrNotFound
	})

	if _, err := loader(child.HashKey()); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("top-level retained loader err=%v, want ErrNotFound", err)
	}
	if baseLoads != 0 {
		t.Fatalf("top-level retained loader used base %d times", baseLoads)
	}

	loadedRoot, err := loader(root.HashKey())
	if err != nil {
		t.Fatalf("load retained root: %v", err)
	}
	loadedChild, err := loadedRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load retained root ref through base: %v", err)
	}
	loadedChild, err = loadedChild.Prewarm()
	if err != nil {
		t.Fatalf("prewarm retained root ref through base: %v", err)
	}
	if loadedChild.HashKey() != child.HashKey() {
		t.Fatalf("loaded child hash mismatch")
	}
	if baseLoads == 0 {
		t.Fatal("retained root ref did not use base loader")
	}
}

func TestStateCellWindowCacheRetriesPendingCheckpointCells(t *testing.T) {
	first := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	second := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()

	window := newStateCellWindowCache(nil)
	if err := window.addPreparedRecords(mustPreparedReachableStateCells(t, first)); err != nil {
		t.Fatalf("add first prepared records: %v", err)
	}
	failedCheckpoint := window.beginCheckpoint()
	failedRecords := failedCheckpoint.records()
	if !hasCellRecord(failedRecords, first.HashKey()) {
		t.Fatal("first checkpoint does not contain first cell")
	}

	if err := window.addPreparedRecords(mustPreparedReachableStateCells(t, second)); err != nil {
		t.Fatalf("add second prepared records: %v", err)
	}
	nextCheckpoint := window.beginCheckpoint()
	records := nextCheckpoint.records()
	if !hasCellRecord(records, first.HashKey()) {
		t.Fatal("retry checkpoint does not include still-pending first cell")
	}
	if !hasCellRecord(records, second.HashKey()) {
		t.Fatal("retry checkpoint does not include new active cell")
	}

	nextCheckpoint.complete()
	if stale := window.beginCheckpoint(); stale != nil {
		t.Fatal("completed checkpoint left pending cells behind")
	}
}

func TestStateCellWindowCacheByteSizeTracksActiveAndPendingCells(t *testing.T) {
	var first, second cell.Hash
	first[0] = 1
	second[0] = 2

	window := newStateCellWindowCache(nil)
	if err := window.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		first: []byte{1, 2, 3},
	})); err != nil {
		t.Fatalf("add first prepared records: %v", err)
	}
	checkpoint := window.beginCheckpoint()
	if err := window.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: []byte{4, 5},
	})); err != nil {
		t.Fatalf("add second prepared records: %v", err)
	}

	if got := window.byteSize(); got != 5 {
		t.Fatalf("window byte size = %d, want 5", got)
	}

	checkpoint.complete()
	if got := window.byteSize(); got != 2 {
		t.Fatalf("window byte size after checkpoint = %d, want 2", got)
	}

	if err := window.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: []byte{6},
	})); err != nil {
		t.Fatalf("replace prepared records: %v", err)
	}
	if got := window.byteSize(); got != 1 {
		t.Fatalf("window byte size after replacement = %d, want 1", got)
	}
}

func TestStateCellWindowLoaderUsesLiveSources(t *testing.T) {
	releasedRoot := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	cache := newStateCellEncodedCache(1)
	if err := cache.stageRecords(mustPreparedReachableStateCells(t, releasedRoot), nil).enqueue(); err != nil {
		t.Fatalf("add source records: %v", err)
	}

	window := newStateCellWindowCache(nil)
	window.mu.Lock()
	window.active = nil
	window.pending = []*stateCellEncodedCache{cache}
	window.mu.Unlock()

	loader := window.loader()

	baseRoot := cell.BeginCell().MustStoreUInt(0x52, 8).EndCell()
	baseCache := newStateCellEncodedCache(1)
	if err := baseCache.stageRecords(mustPreparedReachableStateCells(t, baseRoot), nil).enqueue(); err != nil {
		t.Fatalf("add base records: %v", err)
	}

	window.mu.Lock()
	window.pending = nil
	window.active = newStateCellEncodedCache(1)
	window.base = func(hash cell.Hash) (*cell.Cell, error) {
		return baseCache.loadWith(hash, nil)
	}
	window.mu.Unlock()

	if _, err := loader(releasedRoot.HashKey()); err == nil {
		t.Fatal("loaded released pending cell through old loader")
	}

	loaded, err := loader(baseRoot.HashKey())
	if err != nil {
		t.Fatalf("load cell from live base: %v", err)
	}
	if loaded.HashKey() != baseRoot.HashKey() {
		t.Fatal("loaded live base cell hash mismatch")
	}
}

func TestArchiveStateCellOverlayLoaderReleasesRecordsToBase(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x52, 8).EndCell()

	base := newStateCellWindowCache(nil)
	overlay := newStateCellWindowCache(nil)
	overlay.addPreparedRecords(mustPreparedReachableStateCells(t, root))

	loader := overlay.loader()
	base.adoptRecordsFrom(overlay)
	overlay.releaseRecordsToBase(base.loader())

	if got := overlay.byteSize(); got != 0 {
		t.Fatalf("released overlay byte size = %d, want 0", got)
	}

	loaded, err := loader(root.HashKey())
	if err != nil {
		t.Fatalf("load archive cell through released loader base: %v", err)
	}
	if loaded.HashKey() != root.HashKey() {
		t.Fatal("loaded archive cell hash mismatch")
	}
}

func TestArchiveStateCellOverlayLoaderDropsReleasedRecordsWithoutBase(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x53, 8).EndCell()

	overlay := newStateCellWindowCache(nil)
	overlay.addPreparedRecords(mustPreparedReachableStateCells(t, root))

	loader := overlay.loader()
	overlay.releaseRecordsToBase(nil)

	if got := overlay.byteSize(); got != 0 {
		t.Fatalf("released overlay byte size = %d, want 0", got)
	}
	if _, err := loader(root.HashKey()); err == nil {
		t.Fatal("loaded released archive cell through old loader")
	}
}

func TestArchiveStateCellOverlayByteSizeTracksActiveAndPendingCells(t *testing.T) {
	var first, second cell.Hash
	first[0] = 1
	second[0] = 2

	overlay := newStateCellWindowCache(nil)
	overlay.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		first: []byte{1, 2, 3, 4},
	}))
	checkpoint := overlay.beginCheckpoint()
	overlay.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: []byte{5, 6},
	}))

	if got := overlay.byteSize(); got != 6 {
		t.Fatalf("overlay byte size = %d, want 6", got)
	}

	checkpoint.complete()
	if got := overlay.byteSize(); got != 2 {
		t.Fatalf("overlay byte size after checkpoint = %d, want 2", got)
	}

	overlay.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: []byte{7},
	}))
	if got := overlay.byteSize(); got != 1 {
		t.Fatalf("overlay byte size after replacement = %d, want 1", got)
	}
}

func TestArchiveStateCellOverlayAdoptsWindowCellsOnEmission(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x53, 8).EndCell()
	windowCells := newStateCellWindowCache(nil)
	if err := windowCells.rememberApplied(root, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember window cells: %v", err)
	}

	checkpointCells := newStateCellWindowCache(nil)
	if got := checkpointCells.byteSize(); got != 0 {
		t.Fatalf("checkpoint overlay starts with %d bytes, want 0", got)
	}

	checkpointCells.adoptRecordsFrom(windowCells)
	checkpoint := checkpointCells.beginCheckpoint()
	records := checkpoint.records()
	if !hasCellRecord(records, root.HashKey()) {
		t.Fatal("emitted archive window cells were not adopted into checkpoint overlay")
	}
}

func TestArchiveStateCellOverlayAdoptDoesNotDedupeRecords(t *testing.T) {
	var hash cell.Hash
	hash[0] = 0x54
	records := testStateCellRecords(map[cell.Hash][]byte{
		hash: []byte{1, 2, 3},
	})

	windowCells := newStateCellWindowCache(nil)
	windowCells.addPreparedRecords(records)
	checkpointCells := newStateCellWindowCache(nil)
	checkpointCells.addPreparedRecords(records)

	checkpointCells.adoptRecordsFrom(windowCells)
	checkpoint := checkpointCells.beginCheckpoint()
	if got := countCellRecords(checkpoint.records(), hash); got != 2 {
		t.Fatalf("adopted record count = %d, want 2", got)
	}
}

func TestStateCellWindowLazyLoadMetricsCountHits(t *testing.T) {
	var counters lazyCellLoadCounters
	root := cell.BeginCell().MustStoreUInt(0x91, 8).EndCell()
	window := newStateCellWindowCache(nil, &counters)

	if err := window.addPreparedRecords(mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("add prepared records: %v", err)
	}
	if _, err := window.loader()(root.HashKey()); err != nil {
		t.Fatalf("load cached state cell: %v", err)
	}
	if got := lazyCellLoadMetricCount(counters.snapshot(), tnstore.LazyCellLoadLayerStateWindow); got != 1 {
		t.Fatalf("state window lazy loads = %d, want 1", got)
	}
}

func TestStateCellWindowCacheMemoizesDecodedCells(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0x71, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()

	window := newStateCellWindowCache(nil)
	if err := window.addPreparedRecords(mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("add prepared records: %v", err)
	}

	loader := window.loader()
	first, err := loader(root.HashKey())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	second, err := loader(root.HashKey())
	if err != nil {
		t.Fatalf("reload root: %v", err)
	}
	if second != first {
		t.Fatal("repeated window loads should return the memoized decoded cell")
	}

	// The memoized cell still resolves refs through the window cache.
	loadedChild, err := second.PeekRef(0)
	if err != nil {
		t.Fatalf("peek memoized root ref: %v", err)
	}
	if loadedChild, err = loadedChild.Prewarm(); err != nil {
		t.Fatalf("prewarm memoized root ref: %v", err)
	}
	if loadedChild.HashKey() != child.HashKey() {
		t.Fatal("memoized root ref hash mismatch")
	}

	// Re-adding identical records is deduplicated and keeps the memoized cell.
	if err = window.addPreparedRecords(mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("re-add prepared records: %v", err)
	}
	third, err := loader(root.HashKey())
	if err != nil {
		t.Fatalf("load root after identical re-add: %v", err)
	}
	if third != first {
		t.Fatal("identical record re-add should keep the memoized cell")
	}
}

func TestStateCellEncodedCacheMemoInvalidatedOnRecordReplacement(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x61, 8).EndCell()

	cache := newStateCellEncodedCache(1)
	if err := cache.stageRecords(mustPreparedReachableStateCells(t, root), nil).enqueue(); err != nil {
		t.Fatalf("add records: %v", err)
	}

	first, err := cache.loadWith(root.HashKey(), nil)
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	if again, err := cache.loadWith(root.HashKey(), nil); err != nil || again != first {
		t.Fatalf("repeated load = (%v, %v), want memoized cell", again, err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	idx := cache.index[root.HashKey()]
	if cache.decoded[idx].Load() != first {
		t.Fatal("decoded slot does not hold the memoized cell")
	}
	if cache.setRecordLocked(root.HashKey(), cache.records[idx].Data) {
		t.Fatal("identical record replacement should be a no-op")
	}
	if cache.decoded[idx].Load() != first {
		t.Fatal("identical record replacement should keep the memoized cell")
	}
	if !cache.setRecordLocked(root.HashKey(), []byte{0xde, 0xad}) {
		t.Fatal("record replacement should report a change")
	}
	if cache.decoded[idx].Load() != nil {
		t.Fatal("record replacement should invalidate the memoized cell")
	}
}

func mustProofBody(tb testing.TB, root *cell.Cell, recursiveRefs ...int) *cell.Cell {
	tb.Helper()

	proofBuilder := cell.NewMerkleProofBuilder(root)
	proofRoot := proofBuilder.Root()
	proofSlice, err := proofRoot.BeginParse()
	if err != nil {
		tb.Fatalf("begin proof root: %v", err)
	}
	for _, ref := range recursiveRefs {
		refCell, err := proofSlice.PeekRefCellAt(ref)
		if err != nil {
			tb.Fatalf("proof ref %d: %v", ref, err)
		}
		markFullProofSubtree(tb, refCell)
	}
	proof, err := proofBuilder.CreateProof()
	if err != nil {
		tb.Fatalf("create proof: %v", err)
	}
	body, err := cell.UnwrapProof(proof, root.Hash())
	if err != nil {
		tb.Fatalf("unwrap proof: %v", err)
	}
	return body
}

func markFullProofSubtree(tb testing.TB, root *cell.Cell) {
	tb.Helper()

	loader, err := root.BeginParse()
	if err != nil {
		tb.Fatalf("begin proof subtree: %v", err)
	}
	for i := 0; i < loader.RefsNum(); i++ {
		ref, err := loader.PeekRefCellAt(i)
		if err != nil {
			tb.Fatalf("proof subtree ref %d: %v", i, err)
		}
		markFullProofSubtree(tb, ref)
	}
}

func mustPreparedStateUpdateCells(tb testing.TB, update *cell.Cell) tnstore.StateCellRecords {
	tb.Helper()

	prepared, err := tnstore.PrepareStateUpdateCells(update)
	if err != nil {
		tb.Fatalf("prepare state update cells: %v", err)
	}
	return prepared
}

func mustPreparedReachableStateCells(tb testing.TB, root *cell.Cell) tnstore.StateCellRecords {
	tb.Helper()

	prepared, err := tnstore.PrepareReachableStateCells(root)
	if err != nil {
		tb.Fatalf("prepare reachable state cells: %v", err)
	}
	return prepared
}

func testStateCellRecords(records map[cell.Hash][]byte) tnstore.StateCellRecords {
	encoded := make([]tnstore.EncodedCellRecord, 0, len(records))
	for hash, data := range records {
		encoded = append(encoded, tnstore.EncodedCellRecord{Hash: hash, Data: data})
	}
	return tnstore.NewStateCellRecords(encoded)
}

func removePreparedCellRecord(records tnstore.StateCellRecords, hash cell.Hash) tnstore.StateCellRecords {
	encoded := make([]tnstore.EncodedCellRecord, 0, records.Len())
	for _, record := range records.Records {
		if record.Hash == hash {
			continue
		}
		encoded = append(encoded, record)
	}
	return tnstore.NewStateCellRecords(encoded)
}

func lazyCellLoadMetricCount(metrics []tnstore.LazyCellLoadMetric, layer string) uint64 {
	var count uint64
	for _, metric := range metrics {
		if metric.Layer == layer {
			count += metric.Count
		}
	}
	return count
}

func hasCellRecord(records []tnstore.EncodedCellRecord, hash cell.Hash) bool {
	for _, record := range records {
		if record.Hash == hash {
			return true
		}
	}
	return false
}

func countCellRecords(records []tnstore.EncodedCellRecord, hash cell.Hash) int {
	count := 0
	for _, record := range records {
		if record.Hash == hash {
			count++
		}
	}
	return count
}

func assertResolvedCellTreeEqual(tb testing.TB, got, want *cell.Cell) {
	tb.Helper()
	assertResolvedCellTreeEqualAt(tb, got, want, map[cell.Hash]struct{}{})
}

func assertResolvedCellTreeEqualAt(tb testing.TB, got, want *cell.Cell, seen map[cell.Hash]struct{}) {
	tb.Helper()

	if got == nil || want == nil {
		tb.Fatalf("nil cell comparison got=%t want=%t", got == nil, want == nil)
	}
	gotSlice, err := got.BeginParse()
	if err != nil {
		tb.Fatalf("materialize got cell: %v", err)
	}
	wantSlice, err := want.BeginParse()
	if err != nil {
		tb.Fatalf("materialize want cell: %v", err)
	}
	got = gotSlice.BaseCell()
	want = wantSlice.BaseCell()

	gotHash := got.HashKey(0)
	wantHash := want.HashKey(0)
	if gotHash != wantHash {
		tb.Fatalf("cell hash mismatch: got=%x want=%x", gotHash, wantHash)
	}
	if got.GetType() != want.GetType() {
		tb.Fatalf("cell type mismatch for %x: got=%d want=%d", gotHash, got.GetType(), want.GetType())
	}
	if got.RefsNum() != want.RefsNum() {
		tb.Fatalf("refs count mismatch for %x: got=%d want=%d", gotHash, got.RefsNum(), want.RefsNum())
	}
	if _, ok := seen[gotHash]; ok {
		return
	}
	seen[gotHash] = struct{}{}

	for i := 0; i < int(want.RefsNum()); i++ {
		gotRef, err := got.PeekRef(i)
		if err != nil {
			tb.Fatalf("load got ref %d from %x: %v", i, gotHash, err)
		}
		wantRef, err := want.PeekRef(i)
		if err != nil {
			tb.Fatalf("load want ref %d from %x: %v", i, wantHash, err)
		}
		assertResolvedCellTreeEqualAt(tb, gotRef, wantRef, seen)
	}
}

func mustPrunedBranch(tb testing.TB, hidden *cell.Cell) *cell.Cell {
	tb.Helper()

	hiddenHash := hidden.HashKey(0)
	depth := make([]byte, 2)
	binary.BigEndian.PutUint16(depth, hidden.Depth(0))
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash[:]...)
	prunedData = append(prunedData, depth...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("build pruned branch: %v", err)
	}
	return pruned
}

func mustMerkleUpdateCell(tb testing.TB, left *cell.Cell, right *cell.Cell) *cell.Cell {
	tb.Helper()

	builder := cell.BeginCell()
	if err := builder.StoreUInt(uint64(cell.MerkleUpdateCellType), 8); err != nil {
		tb.Fatalf("store merkle update type: %v", err)
	}
	if err := builder.StoreSlice(left.Hash(0), 256); err != nil {
		tb.Fatalf("store merkle update left hash: %v", err)
	}
	if err := builder.StoreSlice(right.Hash(0), 256); err != nil {
		tb.Fatalf("store merkle update right hash: %v", err)
	}
	if err := builder.StoreUInt(uint64(left.Depth(0)), 16); err != nil {
		tb.Fatalf("store merkle update left depth: %v", err)
	}
	if err := builder.StoreUInt(uint64(right.Depth(0)), 16); err != nil {
		tb.Fatalf("store merkle update right depth: %v", err)
	}
	if err := builder.StoreRef(left); err != nil {
		tb.Fatalf("store merkle update left ref: %v", err)
	}
	if err := builder.StoreRef(right); err != nil {
		tb.Fatalf("store merkle update right ref: %v", err)
	}
	update, err := builder.EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("build merkle update cell: %v", err)
	}
	return update
}

func mustLoadFixtureDownloadedBlock(tb testing.TB) *p2p.DownloadedBlock {
	tb.Helper()

	rawFixture, err := os.ReadFile("testdata/masterchain_block_fixture.json")
	if err != nil {
		tb.Fatalf("read fixture: %v", err)
	}

	var fixture blockFixture
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		tb.Fatalf("decode fixture: %v", err)
	}

	blockData, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		tb.Fatalf("decode block boc base64: %v", err)
	}
	blockCell, err := cell.FromBOC(blockData)
	if err != nil {
		tb.Fatalf("parse block boc: %v", err)
	}

	rootHash, err := hex.DecodeString(fixture.Block.RootHashHex)
	if err != nil {
		tb.Fatalf("decode root hash: %v", err)
	}

	fileHash, err := hex.DecodeString(fixture.Block.FileHashHex)
	if err != nil {
		tb.Fatalf("decode file hash: %v", err)
	}

	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		tb.Fatalf("parse shard: %v", err)
	}

	id := ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}
	parsed, err := tnstore.ParseVerifiedBlockCell(id, blockCell)
	if err != nil {
		tb.Fatalf("parse fixture block: %v", err)
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		tb.Fatalf("build fixture block meta: %v", err)
	}

	return &p2p.DownloadedBlock{
		ID:               id,
		Kind:             "fixture",
		Block:            blockCell,
		BlockBOC:         blockData,
		Meta:             meta,
		StateUpdate:      parsed.StateUpdate,
		VerifiedRootHash: true,
	}
}

func bytes32(fill byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = fill
	}
	return out
}
