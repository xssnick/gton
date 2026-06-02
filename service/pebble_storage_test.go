package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"
	"path/filepath"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestPebbleStoragePeerServingRoundTrip(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	prev := testPebbleBlockID(-1, topShard, 1)
	block := testPebbleBlockID(-1, topShard, 2)

	blockData := []byte{0xAA, 0xBB, 0xCC}
	proofData := []byte{0x10, 0x20, 0x30}
	zeroStateData := []byte{0x01, 0x00, 0x02}

	if err := store.SaveArchiveImport(&storage.ServedArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:     block,
			Block:  blockData,
			Proof:  proofData,
			IsLink: false,
		}},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: block}},
	}); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	if err := store.SaveZeroState(prev, zeroStateData, nil); err != nil {
		t.Fatalf("save zero state: %v", err)
	}

	archiveSeqno := int32(101)
	archiveBlock := testPebbleBlockID(-1, topShard, uint32(archiveSeqno))
	archiveData := []byte{0x55, 0x44}
	if err := store.SaveArchiveImport(&storage.ServedArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    archiveBlock,
			Block: archiveData,
			Meta:  &storage.BlockMeta{ID: archiveBlock, GenUTime: 1010, StartLT: 101, EndLT: 102},
		}},
	}); err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	got, err := store.BlockFull(ctx, block)
	if err != nil {
		t.Fatalf("block full: %v", err)
	}
	if !bytes.Equal(got.Block, blockData) || !bytes.Equal(got.Proof, proofData) || got.IsLink {
		t.Fatalf("unexpected block full: %#v", got)
	}

	next, err := store.NextBlockFull(ctx, prev)
	if err != nil {
		t.Fatalf("next block full: %v", err)
	}
	if !next.ID.Equals(&block) {
		t.Fatalf("unexpected next block: got=%v want=%v", next.ID, block)
	}

	zeroState, err := store.ZeroState(ctx, prev)
	if err != nil {
		t.Fatalf("zero state: %v", err)
	}
	if !bytes.Equal(zeroState, zeroStateData) {
		t.Fatalf("unexpected zero state: %x", zeroState)
	}

	archiveID, err := store.ArchiveInfo(ctx, archiveSeqno, -1, topShard)
	if err != nil {
		t.Fatalf("archive info mismatch: err=%v id=%d", err, archiveID)
	}

	archiveHeader, err := store.ArchiveSlice(ctx, archiveID, 0, packfile.HeaderSize)
	var wantMagic [packfile.HeaderSize]byte
	binary.LittleEndian.PutUint32(wantMagic[:], packfile.PackageMagic)
	if err != nil || !bytes.Equal(archiveHeader, wantMagic[:]) {
		t.Fatalf("archive header mismatch: err=%v data=%x", err, archiveHeader)
	}

	archiveOffset := int64(packfile.HeaderSize + packfile.EntryHeaderSize + len(packfile.EntryName(packfile.KindBlock, archiveBlock)))
	archive, err := store.ArchiveSlice(ctx, archiveID, archiveOffset, int32(len(archiveData)))
	if err != nil || !bytes.Equal(archive, archiveData) {
		t.Fatalf("archive slice mismatch: err=%v data=%x", err, archive)
	}
}

func TestPebbleStorageIndexesCellsAndCurrentState(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	blockA := testPebbleBlockID(0, topShard, 10)
	blockB := testPebbleBlockID(0, topShard, 11)

	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:       blockA,
		GenUTime: 100,
		StartLT:  1000,
		EndLT:    1999,
	}); err != nil {
		t.Fatalf("save block meta A: %v", err)
	}
	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:       blockB,
		GenUTime: 120,
		StartLT:  2000,
		EndLT:    2999,
	}); err != nil {
		t.Fatalf("save block meta B: %v", err)
	}

	if got, err := store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 11); err != nil || !got.Equals(&blockB) {
		t.Fatalf("lookup by seqno failed: err=%v got=%v", err, got)
	}
	if got, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 500); err != nil || !got.Equals(&blockA) {
		t.Fatalf("lookup by early lt failed: err=%v got=%v", err, got)
	}
	if got, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 2500); err != nil || !got.Equals(&blockB) {
		t.Fatalf("lookup by lt failed: err=%v got=%v", err, got)
	}
	if _, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 3000); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup future lt error = %v, want ErrNotFound", err)
	}
	if got, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 90); err != nil || !got.Equals(&blockA) {
		t.Fatalf("lookup by early utime failed: err=%v got=%v", err, got)
	}
	if got, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 110); err != nil || !got.Equals(&blockB) {
		t.Fatalf("lookup by utime failed: err=%v got=%v", err, got)
	}
	if _, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 121); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup future utime error = %v, want ErrNotFound", err)
	}

	leaf := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(leaf).MustStoreRef(leaf).EndCell()
	records, err := collectCellRecordsForTest(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save cells: %v", err)
	}
	rootHash := root.HashKey()
	loaded, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load cell: %v", err)
	}
	requireLazyCellBOC(t, loaded, root)

	current := &storage.CurrentState{
		SyncedAt:    time.Now().Round(0),
		Masterchain: storage.BlockState{Block: testPebbleBlockID(-1, topShard, 100), Cell: cell.BeginCell().EndCell()},
		Shards: map[storage.ShardKey]storage.BlockState{
			{Workchain: 0, Shard: topShard}: {Block: blockB, Cell: cell.BeginCell().EndCell()},
		},
	}
	if err = store.SaveCurrentState(ctx, current); err != nil {
		t.Fatalf("save current state: %v", err)
	}
	gotCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("current state: %v", err)
	}
	if !gotCurrent.Masterchain.Block.Equals(&current.Masterchain.Block) {
		t.Fatalf("masterchain block mismatch")
	}
	shard := gotCurrent.Shards[storage.ShardKey{Workchain: 0, Shard: topShard}]
	if !shard.Block.Equals(&blockB) {
		t.Fatalf("shard block mismatch")
	}

}

func TestPebbleStorageLookupBlockByAccountLTUsesShardPrefixPath(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	account := bytes.Repeat([]byte{0x40}, 32)
	shards := storage.AccountShardCandidates(0, account)
	if len(shards) < 3 {
		t.Fatalf("account shard candidates = %d, want at least 3", len(shards))
	}

	topBlock := testPebbleBlockID(0, topShard, 10)
	pathBlock := testPebbleBlockID(0, shards[2], 20)
	siblingBlock := testPebbleBlockID(0, int64(0x2000000000000000), 30)
	for _, meta := range []*storage.BlockMeta{
		{ID: topBlock, StartLT: 1, EndLT: 100},
		{ID: pathBlock, StartLT: 101, EndLT: 200},
		{ID: siblingBlock, StartLT: 101, EndLT: 200},
	} {
		if err := store.SaveBlockMeta(meta); err != nil {
			t.Fatalf("save block meta %s: %v", storage.FormatBlockRef(meta.ID), err)
		}
	}

	got, err := store.LookupBlockByAccountLT(ctx, 0, account, 150)
	if err != nil {
		t.Fatalf("lookup account lt: %v", err)
	}
	if !got.Equals(&pathBlock) {
		t.Fatalf("lookup account block = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(pathBlock))
	}
}

func TestPebbleStorageLookupBlockByAccountLTDoesNotReturnFloorBlock(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	account := bytes.Repeat([]byte{0x40}, 32)
	block := testPebbleBlockID(0, topShard, 10)

	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:      block,
		StartLT: 1,
		EndLT:   100,
	}); err != nil {
		t.Fatalf("save block meta: %v", err)
	}

	_, err := store.LookupBlockByAccountLT(ctx, 0, account, 150)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup floor block error = %v, want ErrNotFound", err)
	}
}

func TestPebbleStorageLookupBlockByAccountLTReturnsLowerBoundOnShardPath(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	account := bytes.Repeat([]byte{0x40}, 32)
	shards := storage.AccountShardCandidates(0, account)
	if len(shards) < 3 {
		t.Fatalf("account shard candidates = %d, want at least 3", len(shards))
	}

	topBlock := testPebbleBlockID(0, topShard, 50)
	pathBlock := testPebbleBlockID(0, shards[2], 20)
	siblingBlock := testPebbleBlockID(0, int64(0x2000000000000000), 10)
	for _, meta := range []*storage.BlockMeta{
		{ID: topBlock, StartLT: 1, EndLT: 500},
		{ID: pathBlock, StartLT: 150, EndLT: 200},
		{ID: siblingBlock, StartLT: 1, EndLT: 200},
	} {
		if err := store.SaveBlockMeta(meta); err != nil {
			t.Fatalf("save block meta %s: %v", storage.FormatBlockRef(meta.ID), err)
		}
	}

	got, err := store.LookupBlockByAccountLT(ctx, 0, account, 125)
	if err != nil {
		t.Fatalf("lookup account lt lower-bound: %v", err)
	}
	if !got.Equals(&pathBlock) {
		t.Fatalf("lookup account lower-bound block = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(pathBlock))
	}
}

func TestPebbleStoragePersistentStateSerializerMetadata(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	last := testPebbleBlockID(-1, topShard, 100)
	written := testPebbleBlockID(-1, topShard, 50)
	cursor := &storage.PersistentStateSerializerState{
		LastBlock:             last,
		LastWrittenBlock:      written,
		LastWrittenBlockUTime: 123456,
	}
	if err := store.SavePersistentStateSerializerState(ctx, cursor); err != nil {
		t.Fatalf("save serializer state: %v", err)
	}

	gotCursor, err := store.PersistentStateSerializerState(ctx)
	if err != nil {
		t.Fatalf("load serializer state: %v", err)
	}
	if !gotCursor.LastBlock.Equals(&last) || !gotCursor.LastWrittenBlock.Equals(&written) || gotCursor.LastWrittenBlockUTime != cursor.LastWrittenBlockUTime {
		t.Fatalf("unexpected serializer cursor: %#v", gotCursor)
	}

	active := &storage.PersistentStateSerializerActive{
		Block:         last,
		StartedAtUnix: 42,
	}
	if err = store.SaveActivePersistentStateSerialization(ctx, active); err != nil {
		t.Fatalf("save active serializer state: %v", err)
	}
	gotActive, err := store.ActivePersistentStateSerialization(ctx)
	if err != nil {
		t.Fatalf("load active serializer state: %v", err)
	}
	if !gotActive.Block.Equals(&active.Block) || gotActive.StartedAtUnix != active.StartedAtUnix {
		t.Fatalf("unexpected active serializer state: %#v", gotActive)
	}
	if err = store.DeleteActivePersistentStateSerialization(ctx); err != nil {
		t.Fatalf("delete active serializer state: %v", err)
	}
	if _, err = store.ActivePersistentStateSerialization(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("active serializer state after delete error = %v, want not found", err)
	}

	expired := &storage.PersistentStateDescription{
		MasterchainBlock: testPebbleBlockID(-1, topShard, 60),
		StartTime:        10,
		EndTime:          20,
	}
	if err = store.SavePersistentStateDescription(ctx, expired); err != nil {
		t.Fatalf("save expired description: %v", err)
	}
	descriptions, err := store.PersistentStateDescriptions(ctx)
	if err != nil {
		t.Fatalf("load descriptions after expired save: %v", err)
	}
	if len(descriptions) != 0 {
		t.Fatalf("expired description was stored: %#v", descriptions)
	}

	desc := &storage.PersistentStateDescription{
		MasterchainBlock: testPebbleBlockID(-1, topShard, 70),
		StartTime:        uint32(time.Now().Unix()),
		EndTime:          uint64(time.Now().Add(time.Hour).Unix()),
		ShardBlocks: []storage.PersistentStateDescriptionShard{{
			Block:      testPebbleBlockID(0, topShard, 71),
			SplitDepth: 4,
		}},
	}
	if err = store.SavePersistentStateDescription(ctx, desc); err != nil {
		t.Fatalf("save description: %v", err)
	}
	descriptions, err = store.PersistentStateDescriptions(ctx)
	if err != nil {
		t.Fatalf("load descriptions: %v", err)
	}
	if len(descriptions) != 1 {
		t.Fatalf("descriptions count = %d, want 1", len(descriptions))
	}
	if !descriptions[0].MasterchainBlock.Equals(&desc.MasterchainBlock) || descriptions[0].ShardBlocks[0].SplitDepth != 4 {
		t.Fatalf("unexpected description: %#v", descriptions[0])
	}
}

func TestPebbleStorageLoadsCellsByRepresentationHash(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	hidden := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	var depth [2]byte
	binary.BigEndian.PutUint16(depth[:], hidden.Depth(0))
	hiddenLevelZeroHash := hidden.HashKey(0)
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenLevelZeroHash[:]...)
	prunedData = append(prunedData, depth[:]...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create pruned cell: %v", err)
	}
	root := cell.BeginCell().MustStoreRef(pruned).EndCell()
	rootHash := root.HashKey()
	rootLevelZeroHash := root.HashKey(0)
	if rootHash == rootLevelZeroHash {
		t.Fatalf("test fixture must have different level-zero and max-level hashes")
	}

	records, err := collectCellRecordsForTest(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	loaded, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load cell by level-zero hash: %v", err)
	}
	requireLazyCellBOC(t, loaded, root)

	fullRoot := cell.BeginCell().MustStoreRef(hidden).EndCell()
	prunedLevelZeroHash := pruned.HashKey(0)
	if prunedLevelZeroHash != hiddenLevelZeroHash {
		t.Fatalf("test fixture must share level-zero hash")
	}
	prunedHash := pruned.HashKey()
	hiddenHash := hidden.HashKey()
	if prunedHash == hiddenHash {
		t.Fatalf("test fixture must have different representation hashes")
	}

	records, err = collectCellRecordsForTest(fullRoot)
	if err != nil {
		t.Fatalf("collect full cell records: %v", err)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save full cells: %v", err)
	}

	fullRootHash := fullRoot.HashKey()
	loadedFull, err := store.LoadCell(ctx, fullRootHash[:])
	if err != nil {
		t.Fatalf("load full root: %v", err)
	}
	requireLazyCellBOC(t, loadedFull, fullRoot)

	loadedPruned, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("reload pruned root: %v", err)
	}
	requireLazyCellBOC(t, loadedPruned, root)
}

func TestPebbleStorageLoadsLazyCells(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	leaf := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	child := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xCC, 8).MustStoreRef(child).EndCell()
	records, err := collectCellRecordsForTest(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	rootHash := root.HashKey()
	loaded, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load lazy root: %v", err)
	}
	if loaded.IsLazy() {
		t.Fatalf("loaded root should have body")
	}
	if loaded.HashKey() != root.HashKey() || loaded.Depth() != root.Depth() {
		t.Fatalf("loaded root metadata mismatch")
	}

	loadedChild, err := loaded.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy child: %v", err)
	}
	loadedChildSlice, err := loadedChild.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy child: %v", err)
	}
	loadedChild = loadedChildSlice.BaseCell()
	if loadedChild.HashKey() != child.HashKey() || loadedChild.Depth() != child.Depth() {
		t.Fatalf("loaded child metadata mismatch")
	}
	if loadedChild.RefsNum() != child.RefsNum() {
		t.Fatalf("loaded child refs mismatch: got=%d want=%d lazy=%v dump=%s", loadedChild.RefsNum(), child.RefsNum(), loadedChild.IsLazy(), loadedChild.Dump(512))
	}

	loadedLeaf, err := loadedChild.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy leaf: %v", err)
	}
	loadedLeafSlice, err := loadedLeaf.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy leaf: %v", err)
	}
	loadedLeaf = loadedLeafSlice.BaseCell()
	if loadedLeaf.HashKey() != leaf.HashKey() || loadedLeaf.Depth() != leaf.Depth() {
		t.Fatalf("loaded leaf metadata mismatch")
	}

	hidden := cell.BeginCell().MustStoreUInt(0xDD, 8).EndCell()
	var depth [2]byte
	binary.BigEndian.PutUint16(depth[:], hidden.Depth(0))
	hiddenHash := hidden.HashKey(0)
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash[:]...)
	prunedData = append(prunedData, depth[:]...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create pruned cell: %v", err)
	}
	prunedRoot := cell.BeginCell().MustStoreRef(pruned).EndCell()
	records, err = collectCellRecordsForTest(prunedRoot)
	if err != nil {
		t.Fatalf("collect pruned cell records: %v", err)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save pruned cells: %v", err)
	}

	prunedRootHash := prunedRoot.HashKey()
	loadedPrunedRoot, err := store.LoadCell(ctx, prunedRootHash[:])
	if err != nil {
		t.Fatalf("load lazy pruned root: %v", err)
	}
	loadedPruned, err := loadedPrunedRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy pruned ref: %v", err)
	}
	loadedPrunedSlice, err := loadedPruned.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy pruned ref: %v", err)
	}
	loadedPruned = loadedPrunedSlice.BaseCell()
	if loadedPruned.HashKey() != pruned.HashKey() || loadedPruned.Depth() != pruned.Depth() {
		t.Fatalf("loaded pruned metadata mismatch")
	}
}

func openTestPebbleStorage(t *testing.T) *pebblestore.Store {
	t.Helper()

	store, err := pebblestore.Open(pebblestore.Options{
		Dir: filepath.Join(t.TempDir(), "storage"),
	})
	if err != nil {
		t.Fatalf("open pebble storage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close pebble storage: %v", err)
		}
	})
	return store
}

func requireLazyCellBOC(t testing.TB, loaded *cell.Cell, want *cell.Cell) {
	t.Helper()

	if loaded.IsLazy() {
		t.Fatalf("expected loaded cell body")
	}

	materialized := materializeLazyCell(t, loaded)
	if !bytes.Equal(materialized.ToBOC(), want.ToBOC()) {
		t.Fatalf("loaded cell mismatch")
	}
}

func materializeLazyCell(t testing.TB, cl *cell.Cell) *cell.Cell {
	t.Helper()

	loader := cl.MustBeginParse()
	cl = loader.BaseCell()
	bits, data, err := loader.RestBits()
	if err != nil {
		t.Fatalf("load cell bits: %v", err)
	}

	builder := cell.BeginCell().MustStoreSlice(data, bits)
	for i := 0; i < int(cl.RefsNum()); i++ {
		ref, err := cl.PeekRef(i)
		if err != nil {
			t.Fatalf("load lazy ref %d: %v", i, err)
		}
		builder.MustStoreRef(materializeLazyCell(t, ref))
	}

	if cl.IsSpecial() {
		special, err := builder.EndCellSpecial(true)
		if err != nil {
			t.Fatalf("end special cell: %v", err)
		}
		return special
	}
	return builder.EndCell()
}

func testPebbleBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := bytes.Repeat([]byte{byte(seqno)}, 32)
	file := bytes.Repeat([]byte{byte(seqno + 1)}, 32)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}
