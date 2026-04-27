package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"flexserver/service/storage"
	"flexserver/service/storage/pebblestore"
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

	if err := store.SaveBlockFull(&storage.ServedBlockFull{
		ID:     block,
		Block:  blockData,
		Proof:  proofData,
		IsLink: false,
	}); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	store.LinkNextBlock(prev, block)
	store.SaveArchiveInfo(int32(block.SeqNo), 777)
	store.SaveArchiveSlice(777, 0, []byte{0x55, 0x44})

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

	archiveID, err := store.ArchiveInfo(ctx, int32(block.SeqNo))
	if err != nil || archiveID != 777 {
		t.Fatalf("archive info mismatch: err=%v id=%d", err, archiveID)
	}

	archive, err := store.ArchiveSlice(ctx, 777, 0, 16)
	if err != nil || !bytes.Equal(archive, []byte{0x55, 0x44}) {
		t.Fatalf("archive slice mismatch: err=%v data=%x", err, archive)
	}
}

func TestPebbleStorageIndexesCellsAndCurrentState(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	blockA := testPebbleBlockID(0, topShard, 10)
	blockB := testPebbleBlockID(0, topShard, 11)

	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:        blockA,
		GenUTime:  100,
		StartLT:   1000,
		EndLT:     1999,
		UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("save block meta A: %v", err)
	}
	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:        blockB,
		GenUTime:  120,
		StartLT:   2000,
		EndLT:     2999,
		UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("save block meta B: %v", err)
	}

	if got, err := store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 11); err != nil || !got.Equals(&blockB) {
		t.Fatalf("lookup by seqno failed: err=%v got=%v", err, got)
	}
	if got, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 2500); err != nil || !got.Equals(&blockB) {
		t.Fatalf("lookup by lt failed: err=%v got=%v", err, got)
	}
	if got, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 110); err != nil || !got.Equals(&blockA) {
		t.Fatalf("lookup by utime failed: err=%v got=%v", err, got)
	}

	leaf := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(leaf).MustStoreRef(leaf).EndCell()
	records, err := storage.CollectCellRecords(root)
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

	entry := storage.AccountTxIndexEntry{
		AccountKey: bytes.Repeat([]byte{0x11}, 32),
		LT:         555,
		Hash:       bytes.Repeat([]byte{0x22}, 32),
		Block:      blockB,
	}
	if err = store.SaveAccountTxIndex(entry); err != nil {
		t.Fatalf("save account tx index: %v", err)
	}
	entries, err := store.ListAccountTx(ctx, entry.AccountKey, 0, 10)
	if err != nil {
		t.Fatalf("list account tx: %v", err)
	}
	if len(entries) != 1 || entries[0].LT != entry.LT || !entries[0].Block.Equals(&blockB) {
		t.Fatalf("unexpected account tx entries: %#v", entries)
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

	records, err := storage.CollectCellRecords(root)
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

	records, err = storage.CollectCellRecords(fullRoot)
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
	records, err := storage.CollectCellRecords(root)
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
	if !loaded.IsLazy() {
		t.Fatalf("expected lazy root")
	}
	if loaded.HashKey() != root.HashKey() || loaded.Depth() != root.Depth() {
		t.Fatalf("loaded root metadata mismatch")
	}

	loadedChild, err := loaded.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy child: %v", err)
	}
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
	records, err = storage.CollectCellRecords(prunedRoot)
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
	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	t.Cleanup(func() {
		cell.LazyLoader = oldLoader
		if err := store.Close(); err != nil {
			t.Fatalf("close pebble storage: %v", err)
		}
	})
	return store
}

func requireLazyCellBOC(t testing.TB, loaded *cell.Cell, want *cell.Cell) {
	t.Helper()

	if !loaded.IsLazy() {
		t.Fatalf("expected lazy loaded cell")
	}

	materialized := materializeLazyCell(t, loaded)
	if !bytes.Equal(materialized.ToBOC(), want.ToBOC()) {
		t.Fatalf("loaded cell mismatch")
	}
}

func materializeLazyCell(t testing.TB, cl *cell.Cell) *cell.Cell {
	t.Helper()

	bits, data, err := cl.BeginParse().RestBits()
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
