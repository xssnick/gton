package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flexserver/service/storage"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCellRecordCodecStoresOrdinaryDescriptor(t *testing.T) {
	ordinary := cell.BeginCell().MustStoreUInt(0xbc, 8).EndCell()
	record, err := storage.CellRecordFromCell(ordinary)
	if err != nil {
		t.Fatalf("cell record from cell: %v", err)
	}
	encoded := encodeCellRecord(record)
	record, err = decodeCellRecord(record.Hash, encoded)
	if err != nil {
		t.Fatalf("decode cell record: %v", err)
	}
	if record.D1&8 != 0 {
		t.Fatalf("ordinary cell was decoded as special")
	}
	if record.D1&7 != 0 {
		t.Fatalf("ordinary cell refs leaked from dirty buffer: %d", record.D1&7)
	}
}

func TestCellRecordCodecStoresRefMerkleMetadata(t *testing.T) {
	hidden := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	var hiddenDepth [2]byte
	binary.BigEndian.PutUint16(hiddenDepth[:], hidden.Depth(0))

	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash[:]...)
	prunedData = append(prunedData, hiddenDepth[:]...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create pruned cell: %v", err)
	}

	root := cell.BeginCell().MustStoreRef(pruned).EndCell()
	record, err := storage.CellRecordFromCell(root)
	if err != nil {
		t.Fatalf("cell record from cell: %v", err)
	}

	encoded := encodeCellRecord(record)
	record, err = decodeCellRecord(record.Hash, encoded)
	if err != nil {
		t.Fatalf("decode cell record: %v", err)
	}
	if len(record.Refs) != 1 {
		t.Fatalf("unexpected refs count %d", len(record.Refs))
	}

	ref := record.Refs[0]
	refMask := pruned.LevelMask()
	if ref.LevelMask != refMask.Mask {
		t.Fatalf("ref level mask mismatch: got=%d want=%d", ref.LevelMask, refMask.Mask)
	}

	hashesCount := storage.CellRefHashesCount(ref.LevelMask)
	if len(ref.Hashes) != hashesCount*32 {
		t.Fatalf("ref hashes size mismatch: got=%d want=%d", len(ref.Hashes), hashesCount*32)
	}
	if len(ref.Depths) != hashesCount*2 {
		t.Fatalf("ref depths size mismatch: got=%d want=%d", len(ref.Depths), hashesCount*2)
	}

	hashPos := 0
	depthPos := 0
	for level := 0; level <= refMask.GetLevel(); level++ {
		if !refMask.IsSignificant(level) {
			continue
		}

		hash := pruned.HashKey(level)
		if !bytes.Equal(ref.Hashes[hashPos:hashPos+32], hash[:]) {
			t.Fatalf("ref hash level %d mismatch", level)
		}
		hashPos += 32

		if got, want := binary.BigEndian.Uint16(ref.Depths[depthPos:depthPos+2]), pruned.Depth(level); got != want {
			t.Fatalf("ref depth level %d mismatch: got=%d want=%d", level, got, want)
		}
		depthPos += 2
	}
}

func TestStateCellDirectEncoderMatchesCellRecordCodec(t *testing.T) {
	hidden := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	var hiddenDepth [2]byte
	binary.BigEndian.PutUint16(hiddenDepth[:], hidden.Depth(0))

	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash[:]...)
	prunedData = append(prunedData, hiddenDepth[:]...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create pruned cell: %v", err)
	}

	leaf := cell.BeginCell().MustStoreUInt(0b101, 3).EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0b10101, 5).
		MustStoreRef(leaf).
		MustStoreRef(pruned).
		EndCell()

	refs := make([]*cell.Cell, int(root.RefsNum()))
	for i := range refs {
		refs[i], err = root.PeekRef(i)
		if err != nil {
			t.Fatalf("peek ref %d: %v", i, err)
		}
	}

	record, err := storage.CellRecordFromCell(root)
	if err != nil {
		t.Fatalf("cell record from cell: %v", err)
	}
	want := encodeCellRecord(record)

	valueLen, d1, d2, err := stateCellEncodedLen(root, refs)
	if err != nil {
		t.Fatalf("state cell encoded len: %v", err)
	}
	got := make([]byte, valueLen)
	encodeStateCellRecordTo(got, root, refs, d1, d2)

	if !bytes.Equal(got, want) {
		t.Fatalf("direct state cell encoding mismatch:\ngot  %x\nwant %x", got, want)
	}
}

func TestSaveStateCellTreeDeduplicatesSharedRefs(t *testing.T) {
	var logOut bytes.Buffer
	logger := zerolog.New(&logOut).Level(zerolog.DebugLevel)

	store, err := Open(Options{
		Dir:    t.TempDir(),
		Logger: &logger,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	leaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(leaf).MustStoreRef(leaf).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     1,
	}

	if err = store.saveStateCellTree(context.Background(), block, root, nil, 2, nil, nil); err != nil {
		t.Fatalf("save state cell tree: %v", err)
	}

	type logEntry struct {
		Message   string `json:"message"`
		Processed int64  `json:"processed_cells"`
		Total     uint64 `json:"total_cells"`
		Progress  string `json:"progress"`
	}

	var final logEntry
	for _, line := range strings.Split(strings.TrimSpace(logOut.String()), "\n") {
		var entry logEntry
		if err = json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse log line %q: %v", line, err)
		}
		if entry.Message == "state cells persisted" {
			final = entry
		}
	}
	if final.Message == "" {
		t.Fatal("missing final persistence log")
	}
	if final.Processed != 2 {
		t.Fatalf("processed duplicate shared ref: got=%d want=2", final.Processed)
	}
	if final.Total != 2 || final.Progress != "100.0%" {
		t.Fatalf("unexpected final progress: total=%d progress=%q", final.Total, final.Progress)
	}
}

func TestImportStateCellTreeSyncsCellsWithoutForcedFlush(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	leaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(leaf).EndCell()
	rootHash := root.HashKey()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	roots, parsedCells, err := cell.FromBOCMultiRootReader(bytes.NewReader(root.ToBOC()))
	if err != nil {
		t.Fatalf("parse state boc: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("unexpected roots count: %d", len(roots))
	}
	root = roots[0]
	rootHash = root.HashKey()

	lazyRoot, err := store.ImportStateCellTree(ctx, block, root, parsedCells, uint64(len(parsedCells)))
	if err != nil {
		t.Fatalf("import state cell tree: %v", err)
	}
	if !lazyRoot.IsLazy() || lazyRoot.HashKey() != rootHash {
		t.Fatalf("lazy root metadata mismatch")
	}

	marker, err := store.getHotCopy(ctx, hotKeyStateCellSync(block))
	if err != nil {
		t.Fatalf("load state cell sync marker: %v", err)
	}
	if want := encodeStateCellSync(rootHash, 2); !bytes.Equal(marker, want) {
		t.Fatalf("state cell sync marker mismatch: got=%x want=%x", marker, want)
	}

	rootLevelHash := root.HashKey(0)
	loadedRoot, loadedCells, err := store.LoadStateCellTree(ctx, block, rootLevelHash[:])
	if err != nil {
		t.Fatalf("load imported state cell tree: %v", err)
	}
	if !loadedRoot.IsLazy() || loadedRoot.HashKey() != rootHash {
		t.Fatalf("loaded lazy root metadata mismatch")
	}
	if loadedCells != 2 {
		t.Fatalf("loaded cells count mismatch: got=%d want=2", loadedCells)
	}

	wrongHash := bytes.Repeat([]byte{0x44}, 32)
	if _, _, err = store.LoadStateCellTree(ctx, block, wrongHash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load imported state cell tree with wrong hash error = %v, want ErrNotFound", err)
	}
	if got := store.hot.Metrics().Flush.Count; got != 0 {
		t.Fatalf("state import forced memtable flush: got flush count %d", got)
	}
	if got := store.hot.Metrics().Ingest.Count; got != 0 {
		t.Fatalf("state import unexpectedly used pebble ingest: got ingest count %d", got)
	}
}

func TestSaveBlockStateAndCurrentStatePersistsOneDurableState(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	root := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	rootHash := root.HashKey(0)
	cellHash := root.HashKey()

	state := &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		StateCellHash: cellHash[:],
		CellsCount:    1,
		Cell:          root,
		DownloadedAt:  time.Now(),
	}
	current := &storage.CurrentState{
		SyncedAt:    time.Now(),
		Masterchain: *state,
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}

	if err = store.SaveBlockStateAndCurrentState(ctx, state, current); err != nil {
		t.Fatalf("save block and current state: %v", err)
	}

	meta, err := store.blockStateMeta(ctx, block)
	if err != nil {
		t.Fatalf("load block state meta: %v", err)
	}
	if !bytes.Equal(meta.StateCellHash, cellHash[:]) {
		t.Fatalf("state cell hash mismatch: got=%x want=%x", meta.StateCellHash, cellHash)
	}

	loadedRoot, cells, err := store.LoadStateCellTree(ctx, block, rootHash[:])
	if err != nil {
		t.Fatalf("load state cell tree marker: %v", err)
	}
	if cells != 1 {
		t.Fatalf("loaded cells = %d, want 1", cells)
	}
	if loadedRoot.HashKey(0) != rootHash {
		t.Fatalf("loaded root hash mismatch: got=%x want=%x", loadedRoot.HashKey(0), rootHash)
	}

	loadedCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current state: %v", err)
	}
	if !loadedCurrent.Masterchain.Block.Equals(&block) {
		t.Fatalf("current masterchain block = %s, want %s", storage.FormatBlockRef(loadedCurrent.Masterchain.Block), storage.FormatBlockRef(block))
	}
}

func TestStateSyncProgressPersistsAndClears(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	shard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     11,
		RootHash:  bytes.Repeat([]byte{3}, 32),
		FileHash:  bytes.Repeat([]byte{4}, 32),
	}

	masterState := blockStateWithSingleCell(master, 0x11)
	shardState := blockStateWithSingleCell(shard, 0x22)
	if err = store.SaveBlockState(ctx, masterState); err != nil {
		t.Fatalf("save master state: %v", err)
	}
	if err = store.SaveBlockState(ctx, shardState); err != nil {
		t.Fatalf("save shard state: %v", err)
	}

	progress := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain:      *masterState,
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard): *shardState,
		},
	}
	if err = store.SaveStateSyncProgress(ctx, progress); err != nil {
		t.Fatalf("save sync progress: %v", err)
	}

	loaded, err := store.StateSyncProgress(ctx)
	if err != nil {
		t.Fatalf("load sync progress: %v", err)
	}
	if !loaded.Masterchain.Block.Equals(&master) {
		t.Fatalf("progress masterchain block = %s, want %s", storage.FormatBlockRef(loaded.Masterchain.Block), storage.FormatBlockRef(master))
	}
	if _, ok := loaded.Shards[storage.ShardKeyFromBlock(shard)]; !ok {
		t.Fatalf("progress shard %s is missing", storage.FormatBlockRef(shard))
	}
	if _, err = store.CurrentState(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state should stay absent, got %v", err)
	}

	if err = store.ClearStateSyncProgress(ctx); err != nil {
		t.Fatalf("clear sync progress: %v", err)
	}
	if _, err = store.StateSyncProgress(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("sync progress after clear = %v, want ErrNotFound", err)
	}
}

func blockStateWithSingleCell(block ton.BlockIDExt, value uint64) *storage.BlockState {
	root := cell.BeginCell().MustStoreUInt(value, 8).EndCell()
	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	return &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		StateCellHash: cellHash[:],
		CellsCount:    1,
		Cell:          root,
		DownloadedAt:  time.Now(),
	}
}

func TestSaveStateCellTreeSkipsReusedLazySubtree(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}

	oldLeft := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	oldRightLeaf := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	oldRight := cell.BeginCell().MustStoreUInt(0x33, 8).MustStoreRef(oldRightLeaf).EndCell()
	oldRoot := cell.BeginCell().MustStoreRef(oldLeft).MustStoreRef(oldRight).EndCell()

	lazyOldRoot, err := store.ImportStateCellTree(ctx, block, oldRoot, nil, 4)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}

	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	defer func() { cell.LazyLoader = oldLoader }()

	oldRightRef, err := lazyOldRoot.PeekRef(1)
	if err != nil {
		t.Fatalf("load old right ref: %v", err)
	}
	if !oldRightRef.IsLazy() {
		t.Fatal("expected imported old right subtree to be lazy")
	}

	newLeft := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	newRoot := cell.BeginCell().MustStoreRef(newLeft).MustStoreRef(oldRightRef).EndCell()

	nextBlock := block
	nextBlock.SeqNo = 2
	oldRightHash := oldRight.HashKey()
	reused := []cell.MerkleUpdateReusedCell{{
		Hash: oldRightHash,
		Cell: oldRightRef,
	}}
	reusedRefs := []cell.MerkleUpdateReusedRef{{
		ParentHash:  newRoot.HashKey(),
		RefIndex:    1,
		LogicalHash: oldRightHash,
		RawCell:     oldRightRef,
	}}
	if err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 2, reused, reusedRefs); err != nil {
		t.Fatalf("save new state: %v", err)
	}

	lazyNewRoot, err := store.loadLazyCell(ctx, newRoot.Hash())
	if err != nil {
		t.Fatalf("load new lazy root: %v", err)
	}
	if lazyNewRoot.HashKey() != newRoot.HashKey() {
		t.Fatalf("lazy new root hash mismatch: got=%x want=%x", lazyNewRoot.Hash(), newRoot.Hash())
	}
	loadedRight, err := lazyNewRoot.PeekRef(1)
	if err != nil {
		t.Fatalf("load reused right subtree: %v", err)
	}
	if loadedRight.HashKey() != oldRightHash {
		t.Fatalf("reused right hash mismatch: got=%x want=%x", loadedRight.Hash(), oldRight.Hash())
	}
}

func TestSaveStateCellTreePersistsReusedSubtreeMissingFromDB(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     2,
	}

	left := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	rightLeaf := cell.BeginCell().MustStoreUInt(0x55, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x66, 8).MustStoreRef(rightLeaf).EndCell()
	root := cell.BeginCell().MustStoreRef(left).MustStoreRef(right).EndCell()

	rightHash := right.HashKey()
	reused := []cell.MerkleUpdateReusedCell{{
		Hash: rightHash,
		Cell: right,
	}}
	reusedRefs := []cell.MerkleUpdateReusedRef{{
		ParentHash:  root.HashKey(),
		RefIndex:    1,
		LogicalHash: rightHash,
		RawCell:     right,
	}}
	if err = store.saveStateCellTree(ctx, block, root, nil, 4, reused, reusedRefs); err != nil {
		t.Fatalf("save state with reused in-memory subtree: %v", err)
	}

	if _, err = store.loadLazyCell(ctx, rightHash[:]); err != nil {
		t.Fatalf("reused subtree was not persisted: %v", err)
	}

	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	defer func() { cell.LazyLoader = oldLoader }()

	lazyRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	loadedRight, err := lazyRoot.PeekRef(1)
	if err != nil {
		t.Fatalf("load reused right subtree: %v", err)
	}
	if loadedRight.HashKey() != rightHash {
		t.Fatalf("reused right hash mismatch: got=%x want=%x", loadedRight.Hash(), right.Hash())
	}
}

func TestSaveStateCellTreePersistsVirtualReusedSubtreeByLogicalHash(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     3,
	}

	hidden := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	depth := make([]byte, 2)
	binary.BigEndian.PutUint16(depth, hidden.Depth(0))
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash[:]...)
	prunedData = append(prunedData, depth...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("build pruned branch: %v", err)
	}

	left := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x66, 8).MustStoreRef(pruned).EndCell()
	if right.Level() == 0 {
		t.Fatal("test right subtree must have a non-zero level")
	}
	virtualRight := right.Virtualize(0)
	root := cell.BeginCell().MustStoreRef(left).MustStoreRef(virtualRight).EndCell()

	logicalHash := virtualRight.HashKey()
	reused := []cell.MerkleUpdateReusedCell{{
		Hash: logicalHash,
		Cell: virtualRight,
	}}
	reusedRefs := []cell.MerkleUpdateReusedRef{{
		ParentHash:  root.HashKey(),
		RefIndex:    1,
		LogicalHash: logicalHash,
		RawCell:     virtualRight,
	}}
	if err = store.saveStateCellTree(ctx, block, root, nil, 4, reused, reusedRefs); err != nil {
		t.Fatalf("save state with virtual reused subtree: %v", err)
	}

	if _, err = store.loadLazyCell(ctx, logicalHash[:]); err != nil {
		t.Fatalf("virtual reused subtree was not persisted by logical hash: %v", err)
	}

	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	defer func() { cell.LazyLoader = oldLoader }()

	lazyRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	loadedRight, err := lazyRoot.PeekRef(1)
	if err != nil {
		t.Fatalf("load virtual reused right subtree: %v", err)
	}
	if loadedRight.HashKey() != logicalHash {
		t.Fatalf("logical right hash mismatch: got=%x want=%x", loadedRight.Hash(), logicalHash)
	}
}

func TestSaveStateCellTreeRejectsMissingReusedLazySubtree(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}

	oldRight := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), nil, 2)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}
	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	defer func() { cell.LazyLoader = oldLoader }()

	lazyRight, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy right ref: %v", err)
	}
	if !lazyRight.IsLazy() {
		t.Fatal("expected lazy right ref")
	}

	rightHash := lazyRight.HashKey()
	if err = store.hot.Delete(hotKeyCell(rightHash[:]), pebble.NoSync); err != nil {
		t.Fatalf("delete reused cell: %v", err)
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	newRoot := cell.BeginCell().MustStoreRef(lazyRight).EndCell()
	reused := []cell.MerkleUpdateReusedCell{{
		Hash: rightHash,
		Cell: lazyRight,
	}}
	reusedRefs := []cell.MerkleUpdateReusedRef{{
		ParentHash:  newRoot.HashKey(),
		RefIndex:    0,
		LogicalHash: rightHash,
		RawCell:     lazyRight,
	}}
	err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 1, reused, reusedRefs)
	if err == nil || !strings.Contains(err.Error(), "reused lazy state cell") {
		t.Fatalf("expected missing reused lazy subtree error, got %v", err)
	}
}

func TestSaveStateCellTreeRejectsMissingLazySubtree(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}

	oldRight := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), nil, 2)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}
	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	defer func() { cell.LazyLoader = oldLoader }()

	lazyRight, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy right ref: %v", err)
	}

	rightHash := lazyRight.HashKey()
	if err = store.hot.Delete(hotKeyCell(rightHash[:]), pebble.NoSync); err != nil {
		t.Fatalf("delete lazy cell: %v", err)
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	newRoot := cell.BeginCell().MustStoreRef(lazyRight).EndCell()
	err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 1, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "lazy state ref") {
		t.Fatalf("expected missing lazy subtree error, got %v", err)
	}
}

type rejectingLazyLoader struct{}

func (rejectingLazyLoader) LoadCell(cell.Hash) (*cell.Cell, error) {
	return nil, errors.New("unexpected lazy load")
}

func TestSaveStateCellTreeSkipsExistingLazyRootWithoutLoading(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}

	leaf := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(leaf).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, root, nil, 2)
	if err != nil {
		t.Fatalf("import state: %v", err)
	}
	if !lazyRoot.IsLazy() {
		t.Fatal("expected imported root to be lazy")
	}

	oldLoader := cell.LazyLoader
	cell.LazyLoader = rejectingLazyLoader{}
	defer func() { cell.LazyLoader = oldLoader }()

	nextBlock := block
	nextBlock.SeqNo = 2
	if err = store.saveStateCellTree(ctx, nextBlock, lazyRoot, nil, 1, nil, nil); err != nil {
		t.Fatalf("save existing lazy root: %v", err)
	}
}

func TestClosedStoreReturnsError(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if _, err = store.ArchiveInfo(context.Background(), 1); err == nil {
		t.Fatal("expected closed store error")
	}
}
