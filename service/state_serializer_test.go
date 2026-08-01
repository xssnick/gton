package service

import (
	"bytes"
	"context"
	"os"
	"testing"

	state2 "github.com/xssnick/gton/service/state"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestStateSerializerFailsOnMissingPrunedBoundary(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	hidden := cell.BeginCell().MustStoreUInt(0x88, 8).EndCell()
	pruned := mustPrunedBranch(t, hidden)
	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(pruned).EndCell()

	record, err := tnstore.CellRecordFromCellMetadata(root, root.GetMetadata())
	if err != nil {
		t.Fatalf("build root record: %v", err)
	}
	if err = store.SaveCells([]*tnstore.CellRecord{record}); err != nil {
		t.Fatalf("save root record: %v", err)
	}

	loader := newLargeBOCStateLoader(ctx, store, 0)
	var rootHash cell.Hash
	copy(rootHash[:], root.Hash())
	var buf bytes.Buffer
	if err = cell.ToLargeBOC(&buf, []cell.Hash{rootHash}, persistentStateBOCOptions(), loader, 1, defaultPersistentStateLargeBOCBatchSize); err == nil {
		t.Fatal("serialized state with missing pruned boundary")
	}
}

func TestPersistentStateSerializerInitializesCursorFromZeroState(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	zero := testBlockID(-1, topShard, 0)
	zeroState := &tnstore.BlockState{
		Block: zero,
		Cell:  cell.BeginCell().EndCell(),
	}
	if err := saveTestStateCheckpoint(ctx, store, []*tnstore.BlockState{zeroState}, &tnstore.CurrentState{
		ShardClientSeqno: 0,
		Masterchain:      *zeroState,
		Shards:           map[tnstore.ShardKey]tnstore.BlockState{},
	}); err != nil {
		t.Fatalf("save current zero state: %v", err)
	}

	svc := &Service{
		storage:         store,
		stateSerializer: newStateSerializer(zerolog.Nop(), store, t.TempDir(), false, 0, false),
	}
	svc.enableAutomaticStateSerialization()
	if err := svc.processPersistentStateSerialization(ctx); err != nil {
		t.Fatalf("process persistent state serialization: %v", err)
	}

	cursor, err := store.PersistentStateSerializerState(ctx)
	if err != nil {
		t.Fatalf("load serializer cursor: %v", err)
	}
	if !cursor.LastBlock.Equals(&zero) {
		t.Fatalf("last block = %s, want %s", tnstore.FormatBlockRef(cursor.LastBlock), tnstore.FormatBlockRef(zero))
	}
	if !cursor.LastWrittenBlock.Equals(&zero) {
		t.Fatalf("last written block = %s, want %s", tnstore.FormatBlockRef(cursor.LastWrittenBlock), tnstore.FormatBlockRef(zero))
	}
	if cursor.LastWrittenBlockUTime != 0 {
		t.Fatalf("last written utime = %d, want 0", cursor.LastWrittenBlockUTime)
	}
}

func TestStateSerializerSerializesPersistedSplitSyntheticRootWithLargeBOC(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	leaf := cell.BeginCell().MustStoreUInt(0x88, 8).EndCell()
	branch := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(leaf).EndCell()
	leafRecord, err := tnstore.CellRecordFromCell(leaf)
	if err != nil {
		t.Fatalf("build leaf record: %v", err)
	}
	branchRecord, err := tnstore.CellRecordFromCell(branch)
	if err != nil {
		t.Fatalf("build branch record: %v", err)
	}
	if err = store.SaveCells([]*tnstore.CellRecord{leafRecord, branchRecord}); err != nil {
		t.Fatalf("save cell records: %v", err)
	}

	lazyBranch, err := tnstore.LazyCellRecord(branchRecord, func(hash cell.Hash) (*cell.Cell, error) {
		if hash == leaf.HashKey() {
			return leaf, nil
		}
		return nil, cell.ErrLazyRefNotFound
	})
	if err != nil {
		t.Fatalf("build lazy branch: %v", err)
	}

	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(lazyBranch).EndCell()
	records, err := splitStateCellRecords(ctx, root)
	if err != nil {
		t.Fatalf("build split cell records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("split cell records = %d, want 2", len(records))
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save split cell records: %v", err)
	}

	loader := newLargeBOCStateLoader(ctx, store, 0)
	rootHash := root.HashKey()
	var got bytes.Buffer
	if err = cell.ToLargeBOC(&got, []cell.Hash{rootHash}, persistentStateBOCOptions(), loader, 0, defaultPersistentStateLargeBOCBatchSize); err != nil {
		t.Fatalf("serialize synthetic root: %v", err)
	}

	want := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(branch).EndCell().
		ToBOCWithOptions(persistentStateBOCOptions())
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatal("synthetic root boc mismatch")
	}

	onePassLoader := newLargeBOCStateLoader(ctx, store, 0)
	var onePass bytes.Buffer
	if err = cell.ToLargeBOCOnePass(&onePass, []cell.Hash{rootHash}, persistentStateBOCOptions(), onePassLoader, 0, defaultPersistentStateLargeBOCBatchSize); err != nil {
		t.Fatalf("one-pass serialize synthetic root: %v", err)
	}
	if !bytes.Equal(onePass.Bytes(), want) {
		t.Fatal("one-pass synthetic root boc mismatch")
	}
}

func TestStateSerializerPersistsRawSplitRefs(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	hidden := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	pruned := mustPrunedBranch(t, hidden)
	childRaw := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(pruned).EndCell()
	childRecord, err := tnstore.CellRecordFromCell(childRaw)
	if err != nil {
		t.Fatalf("build child record: %v", err)
	}
	child, err := tnstore.LazyCellRecord(childRecord, func(hash cell.Hash) (*cell.Cell, error) {
		if hash == pruned.HashKey() {
			return pruned, nil
		}
		return nil, cell.ErrLazyRefNotFound
	})
	if err != nil {
		t.Fatalf("build lazy child: %v", err)
	}
	rawChildHash := child.HashKey()
	effectiveChildHash := child.Virtualize(0).HashKey()
	if effectiveChildHash == rawChildHash {
		t.Fatal("test child does not have a distinct effective ref hash")
	}
	prunedRecord, err := tnstore.CellRecordFromCell(pruned)
	if err != nil {
		t.Fatalf("build pruned record: %v", err)
	}
	if err = store.SaveCells([]*tnstore.CellRecord{prunedRecord}); err != nil {
		t.Fatalf("save pruned record: %v", err)
	}

	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(child).EndCell()
	records, err := splitStateCellRecords(ctx, root)
	if err != nil {
		t.Fatalf("build split cell records: %v", err)
	}
	if !hasCellRecordRecord(records, rawChildHash) {
		t.Fatalf("split records do not contain raw child ref %x", rawChildHash)
	}
	if hasCellRecordRecord(records, effectiveChildHash) {
		t.Fatalf("split records contain effective child ref %x", effectiveChildHash)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save split cell records: %v", err)
	}

	loader := newLargeBOCStateLoader(ctx, store, 0)
	var got bytes.Buffer
	if err = cell.ToLargeBOC(&got, []cell.Hash{root.HashKey()}, persistentStateBOCOptions(), loader, 0, defaultPersistentStateLargeBOCBatchSize); err != nil {
		t.Fatalf("serialize split root with effective refs: %v", err)
	}

	gotRoot, err := cell.FromBOCWithOptions(got.Bytes(), cell.BOCParseOptions{AllowNonZeroLevelRoot: true})
	if err != nil {
		t.Fatalf("parse serialized split root: %v", err)
	}
	if gotRoot.HashKey() != root.HashKey() {
		t.Fatalf("serialized split root hash mismatch: got=%x want=%x", gotRoot.HashKey(), root.HashKey())
	}
}

func TestStateSerializationLogBlockUsesSplitEffectiveShard(t *testing.T) {
	target := stateSerializationTarget{
		block: testPebbleBlockID(0, topShard, 101),
		kind:  "basechain",
	}

	unsplit := stateSerializationLogBlock(target, state2.PersistentStatePart{Kind: state2.PersistentStatePartUnsplit})
	if unsplit.Shard != topShard {
		t.Fatalf("unsplit log shard = %d, want %d", unsplit.Shard, topShard)
	}

	effectiveShard := int64(0x0800000000000000)
	split := stateSerializationLogBlock(target, state2.PersistentStatePart{
		Kind:           state2.PersistentStatePartSplitAccount,
		EffectiveShard: effectiveShard,
	})
	if split.Shard != effectiveShard {
		t.Fatalf("split log shard = %016x, want %016x", uint64(split.Shard), uint64(effectiveShard))
	}
	if split.SeqNo != target.block.SeqNo || split.Workchain != target.block.Workchain {
		t.Fatalf("split log block changed non-shard fields: got=%+v target=%+v", split, target.block)
	}
}

func TestStateSerializationUsesOnePassForManyBasechainSplitAccounts(t *testing.T) {
	target := stateSerializationTarget{
		block: testPebbleBlockID(0, topShard, 101),
		kind:  "basechain",
	}
	parts := []state2.PersistentStatePart{
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitHeader},
	}

	if !useOnePassLargeBOCForStateSerialization(target, parts) {
		t.Fatal("basechain target with four split account parts does not use one-pass large boc")
	}
}

func TestStateSerializationKeepsTwoPhaseBelowSplitThreshold(t *testing.T) {
	target := stateSerializationTarget{
		block: testPebbleBlockID(0, topShard, 101),
		kind:  "basechain",
	}
	parts := []state2.PersistentStatePart{
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitHeader},
	}

	if useOnePassLargeBOCForStateSerialization(target, parts) {
		t.Fatal("basechain target with three split account parts uses one-pass large boc")
	}

	if useOnePassLargeBOCForStateSerialization(target, []state2.PersistentStatePart{{Kind: state2.PersistentStatePartUnsplit}}) {
		t.Fatal("unsplit basechain target uses one-pass large boc")
	}
}

func TestStateSerializationKeepsTwoPhaseForNonBasechainSplits(t *testing.T) {
	target := stateSerializationTarget{
		block: testPebbleBlockID(1, topShard, 101),
		kind:  "shardchain",
	}
	parts := []state2.PersistentStatePart{
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitAccount},
		{Kind: state2.PersistentStatePartSplitHeader},
	}

	if useOnePassLargeBOCForStateSerialization(target, parts) {
		t.Fatal("non-basechain target uses one-pass large boc")
	}
}

func TestStateSerializationForcedOnePassOverridesConditions(t *testing.T) {
	serializer := newStateSerializer(zerolog.Nop(), nil, "", false, 0, true)
	target := stateSerializationTarget{
		block: testPebbleBlockID(1, topShard, 101),
		kind:  "shardchain",
	}
	parts := []state2.PersistentStatePart{{Kind: state2.PersistentStatePartUnsplit}}

	if !serializer.useOnePassLargeBOCForStateSerialization(target, parts) {
		t.Fatal("forced one-pass setting does not use one-pass large boc")
	}
}

func TestStateSerializerTreatsExistingFinalFileAsReady(t *testing.T) {
	store := openTestPebbleStorage(t)
	serializer := newStateSerializer(zerolog.Nop(), store, store.StateFilesDir(), false, 0, false)

	master := testPebbleBlockID(-1, topShard, 100)
	block := testPebbleBlockID(0, topShard, 101)
	if err := os.MkdirAll(serializer.dir, 0o755); err != nil {
		t.Fatalf("create serializer target dir: %v", err)
	}

	path, err := serializer.serializedStatePath(master, block, 0)
	if err != nil {
		t.Fatalf("persistent state path: %v", err)
	}
	if err := os.WriteFile(path, []byte("complete"), 0o644); err != nil {
		t.Fatalf("write final state file: %v", err)
	}
	if err = store.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		Ref: &tnstore.ArtifactRef{
			Path: path,
			Size: int64(len("complete")),
		},
		FileHash:      bytes.Repeat([]byte{1}, 32),
		StateRootHash: bytes.Repeat([]byte{2}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file meta: %v", err)
	}

	file, err := serializer.existingSerializedState(context.Background(), master, stateSerializationTarget{
		block: block,
		kind:  "basechain",
	}, state2.PersistentStatePart{Kind: state2.PersistentStatePartUnsplit})
	if err != nil {
		t.Fatalf("check existing state: %v", err)
	}
	if file.path != path || file.size != int64(len("complete")) {
		t.Fatalf("unexpected existing state file: %#v", file)
	}
}

func hasCellRecordRecord(records []*tnstore.CellRecord, hash cell.Hash) bool {
	for _, record := range records {
		var recordHash cell.Hash
		copy(recordHash[:], record.Hash)
		if recordHash == hash {
			return true
		}
	}
	return false
}
