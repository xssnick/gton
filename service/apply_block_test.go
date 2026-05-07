package service

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flexserver/service/p2p"
	tnstore "flexserver/service/storage"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestApplyBlockFromFixture(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, downloaded.Block.BeginParse()); err != nil {
		t.Fatalf("load fixture tlb block: %v", err)
	}

	oldStateCell := block.StateUpdate.MustPeekRef(0)
	var oldParsed tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&oldParsed, oldStateCell.BeginParse()); err != nil {
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

	next, err := ApplyBlock(current, *downloaded)
	if err != nil {
		t.Fatalf("apply block: %v", err)
	}

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

	if _, err := ApplyBlock(current, *downloaded); err == nil {
		t.Fatal("expected apply block to reject wrong current state")
	}
}

func TestMerkleUpdateWindowCacheDoesNotLoadOldTreeDuringApply(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
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
	got, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: from}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply cached merkle update: %v", err)
	}
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
	got, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: from}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply direct merkle update: %v", err)
	}
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
	got, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: fromRoot}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply direct merkle update: %v", err)
	}

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
	prepared, err := tnstore.PrepareReachableStateCells(updateTo.Virtualize(0))
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
	got, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: fromRoot}}, update, prepared)
	if err != nil {
		t.Fatalf("apply prepared merkle update: %v", err)
	}

	want, _, err := cell.ApplyMerkleUpdate(fromRoot.Virtualize(0), update)
	if err != nil {
		t.Fatalf("apply reference merkle update: %v", err)
	}
	assertResolvedCellTreeEqual(t, got, want)

	checkpoint := window.beginCheckpoint()
	records := checkpoint.records()
	for hash := range prepared {
		if !hasCellRecord(records, hash) {
			t.Fatalf("checkpoint cache does not contain prepared cell %x", hash[:])
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
	prepared, err := tnstore.PrepareReachableStateCells(to.Virtualize(0))
	if err != nil {
		t.Fatalf("prepare destination cells: %v", err)
	}
	delete(prepared, to.Virtualize(0).HashKey())

	window := newStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		return nil, fmt.Errorf("unexpected base load %x", hash[:])
	})
	_, err = window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: from}}, update, prepared)
	if err == nil || !strings.Contains(err.Error(), "destination root") {
		t.Fatalf("apply with incomplete prepared cells err = %v, want destination root error", err)
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
	got, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: left}, {Cell: right}}, update, mustPreparedStateUpdateCells(t, update))
	if err != nil {
		t.Fatalf("apply merge merkle update: %v", err)
	}
	if got.HashKey() != to.HashKey() {
		t.Fatalf("merge destination hash mismatch: got=%x want=%x", got.HashKey(), to.HashKey())
	}
}

func TestStateCellWindowCacheCarriesCellsAcrossMerkleUpdates(t *testing.T) {
	stable := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
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

	root1, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: state0}}, update1, mustPreparedStateUpdateCells(t, update1))
	if err != nil {
		t.Fatalf("apply first update: %v", err)
	}
	if root1.HashKey() != state1.HashKey() {
		t.Fatalf("first state hash mismatch: got=%x want=%x", root1.HashKey(), state1.HashKey())
	}

	root2, err := window.applyMerkleUpdate([]*tnstore.BlockState{{Cell: root1}}, update2, mustPreparedStateUpdateCells(t, update2))
	if err != nil {
		t.Fatalf("apply second update: %v", err)
	}
	if root2.HashKey() != state2.HashKey() {
		t.Fatalf("second state hash mismatch: got=%x want=%x", root2.HashKey(), state2.HashKey())
	}

	loadedStable, err := root2.PeekRef(0)
	if err != nil {
		t.Fatalf("load stable ref from second state: %v", err)
	}
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
	stable := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
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
	overlay := newArchiveStateCellOverlay(func(hash cell.Hash) (*cell.Cell, error) {
		if hash != stable.HashKey() {
			return nil, fmt.Errorf("unexpected base load %x", hash[:])
		}
		baseLoads++
		return stable, nil
	})

	root1, err := overlay.applyMerkleUpdate([]*tnstore.BlockState{{Cell: state0}}, update1)
	if err != nil {
		t.Fatalf("apply first archive update: %v", err)
	}
	if root1.HashKey() != state1.HashKey() {
		t.Fatalf("first state hash mismatch: got=%x want=%x", root1.HashKey(), state1.HashKey())
	}

	root2, err := overlay.applyMerkleUpdate([]*tnstore.BlockState{{Cell: root1}}, update2)
	if err != nil {
		t.Fatalf("apply second archive update: %v", err)
	}
	if root2.HashKey() != state2.HashKey() {
		t.Fatalf("second state hash mismatch: got=%x want=%x", root2.HashKey(), state2.HashKey())
	}

	loadedStable, err := root2.PeekRef(0)
	if err != nil {
		t.Fatalf("load stable ref from second archive state: %v", err)
	}
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

	overlay := newArchiveStateCellOverlay(nil)
	root1, err := overlay.applyMerkleUpdate([]*tnstore.BlockState{{Cell: state0}}, mustMerkleUpdateCell(t, state0, state1))
	if err != nil {
		t.Fatalf("apply first archive update: %v", err)
	}
	failedCheckpoint := overlay.beginCheckpoint()
	failedRecords := failedCheckpoint.records()
	if !hasCellRecord(failedRecords, state1.HashKey()) {
		t.Fatal("first archive checkpoint does not contain first state")
	}

	if _, err = overlay.applyMerkleUpdate([]*tnstore.BlockState{{Cell: root1}}, mustMerkleUpdateCell(t, state1, state2)); err != nil {
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

func TestStateCellWindowCacheRetriesPendingCheckpointCells(t *testing.T) {
	first := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	second := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()

	window := newStateCellWindowCache(nil)
	if err := window.activeCache().rememberReachable(first); err != nil {
		t.Fatalf("remember first cell: %v", err)
	}
	failedCheckpoint := window.beginCheckpoint()
	failedRecords := failedCheckpoint.records()
	if !hasCellRecord(failedRecords, first.HashKey()) {
		t.Fatal("first checkpoint does not contain first cell")
	}

	if err := window.activeCache().rememberReachable(second); err != nil {
		t.Fatalf("remember second cell: %v", err)
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

func mustProofBody(tb testing.TB, root *cell.Cell, recursiveRefs ...int) *cell.Cell {
	tb.Helper()

	sk := cell.CreateProofSkeleton()
	for _, ref := range recursiveRefs {
		sk.ProofRef(ref).SetRecursive()
	}
	proof, err := root.CreateProof(sk)
	if err != nil {
		tb.Fatalf("create proof: %v", err)
	}
	body, err := cell.UnwrapProof(proof, root.Hash())
	if err != nil {
		tb.Fatalf("unwrap proof: %v", err)
	}
	return body
}

func mustPreparedStateUpdateCells(tb testing.TB, update *cell.Cell) map[cell.Hash][]byte {
	tb.Helper()

	updateTo, err := merkleUpdateToRef(update)
	if err != nil {
		tb.Fatalf("load update_to ref: %v", err)
	}
	prepared, err := tnstore.PrepareReachableStateCells(updateTo.Virtualize(0))
	if err != nil {
		tb.Fatalf("prepare update_to cells: %v", err)
	}
	return prepared
}

func hasCellRecord(records []tnstore.EncodedCellRecord, hash cell.Hash) bool {
	for _, record := range records {
		if record.Hash == hash {
			return true
		}
	}
	return false
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

	return &p2p.DownloadedBlock{
		ID: ton.BlockIDExt{
			Workchain: fixture.Block.Workchain,
			Shard:     int64(shard),
			SeqNo:     fixture.Block.SeqNo,
			RootHash:  rootHash,
			FileHash:  fileHash,
		},
		Block: blockCell,
	}
}

func bytes32(fill byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = fill
	}
	return out
}
