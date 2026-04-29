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

func TestCellRecordCodecCompactsCommonRefs(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(0x01, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x02, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(left).MustStoreRef(right).EndCell()

	record, err := storage.CellRecordFromCell(root)
	if err != nil {
		t.Fatalf("cell record from cell: %v", err)
	}

	encoded := encodeCellRecord(record)
	if encoded[0]&cellRecordCompactRefsFlag == 0 {
		t.Fatalf("compact refs flag is not set")
	}
	if got, want := encoded[0]&^cellRecordCompactRefsFlag, record.D1; got != want {
		t.Fatalf("stored descriptor mismatch after clearing compact flag: got=%d want=%d", got, want)
	}

	bodyLen := int(record.D2/2 + record.D2%2)
	layoutPos := 2 + bodyLen
	if got := encoded[layoutPos]; got != 0 {
		t.Fatalf("common refs slow mask mismatch: got=%d want=0", got)
	}
	if got, want := len(encoded), 2+bodyLen+1+2*(cellRecordHashSize+cellRecordDepthSize); got != want {
		t.Fatalf("compact encoded length mismatch: got=%d want=%d", got, want)
	}

	decoded, err := decodeCellRecord(record.Hash, encoded)
	if err != nil {
		t.Fatalf("decode cell record: %v", err)
	}
	if decoded.D1 != record.D1 {
		t.Fatalf("decoded descriptor mismatch: got=%d want=%d", decoded.D1, record.D1)
	}
	if len(decoded.Refs) != len(record.Refs) {
		t.Fatalf("decoded refs count mismatch: got=%d want=%d", len(decoded.Refs), len(record.Refs))
	}
	for i := range record.Refs {
		if decoded.Refs[i].LevelMask != 0 {
			t.Fatalf("decoded ref %d level mask mismatch: got=%d want=0", i, decoded.Refs[i].LevelMask)
		}
		if !bytes.Equal(decoded.Refs[i].Hashes, record.Refs[i].Hashes) {
			t.Fatalf("decoded ref %d hashes mismatch", i)
		}
		if !bytes.Equal(decoded.Refs[i].Depths, record.Refs[i].Depths) {
			t.Fatalf("decoded ref %d depths mismatch", i)
		}
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

	rootMeta := root.GetMetadata()
	refs := rootMeta.Refs

	valueLen, d1, d2, err := stateCellEncodedLen(root, refs)
	if err != nil {
		t.Fatalf("state cell encoded len: %v", err)
	}
	got := make([]byte, valueLen)
	encodeStateCellRecordTo(got, root, refs, d1, d2)

	record, err := decodeCellRecord(rootMeta.Hash[:], got)
	if err != nil {
		t.Fatalf("decode direct state cell record: %v", err)
	}
	if len(record.Refs) != len(refs) {
		t.Fatalf("direct state refs count mismatch: got=%d want=%d", len(record.Refs), len(refs))
	}
	for i, ref := range refs {
		gotRef := record.Refs[i]
		if gotRef.LevelMask != ref.LevelMask.Mask {
			t.Fatalf("ref %d level mask mismatch: got=%d want=%d", i, gotRef.LevelMask, ref.LevelMask.Mask)
		}
		for j, hash := range ref.Hashes {
			if !bytes.Equal(gotRef.Hashes[j*32:(j+1)*32], hash[:]) {
				t.Fatalf("ref %d hash %d mismatch", i, j)
			}
			depth := binary.BigEndian.Uint16(gotRef.Depths[j*2 : (j+1)*2])
			if depth != ref.Depths[j] {
				t.Fatalf("ref %d depth %d mismatch: got=%d want=%d", i, j, depth, ref.Depths[j])
			}
		}
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

	if _, err = store.saveStateCellTree(context.Background(), block, root, nil, 2, nil, nil); err != nil {
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

func TestImportStateCellTreeFlushesCellsBeforeSyncMarker(t *testing.T) {
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
	if lazyRoot.IsLazy() || lazyRoot.HashKey() != rootHash {
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
	if loadedRoot.IsLazy() || loadedRoot.HashKey() != rootHash {
		t.Fatalf("loaded lazy root metadata mismatch")
	}
	if loadedCells != 2 {
		t.Fatalf("loaded cells count mismatch: got=%d want=2", loadedCells)
	}

	wrongHash := bytes.Repeat([]byte{0x44}, 32)
	if _, _, err = store.LoadStateCellTree(ctx, block, wrongHash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load imported state cell tree with wrong hash error = %v, want ErrNotFound", err)
	}
	cellMetrics := store.cells.metrics()
	if cellMetrics.flushCount == 0 {
		t.Fatal("state import did not flush cell db before sync marker")
	}
	if got := cellMetrics.ingestCount; got != 0 {
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
	if oldRightRef.IsLazy() {
		t.Fatal("expected imported old right subtree to have body")
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
	if _, err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 2, reused, reusedRefs); err != nil {
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
	if _, err = store.saveStateCellTree(ctx, block, root, nil, 4, reused, reusedRefs); err != nil {
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
	if _, err = store.saveStateCellTree(ctx, block, root, nil, 4, reused, reusedRefs); err != nil {
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

func TestSaveStateCellTreeRekeysLazyVirtualSubtreeByLogicalHash(t *testing.T) {
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

	rawRight := cell.BeginCell().MustStoreUInt(0x66, 8).MustStoreRef(pruned).EndCell()
	if rawRight.Level() == 0 {
		t.Fatal("test right subtree must have a non-zero level")
	}
	rawHash := rawRight.HashKey()
	if _, err = store.ImportStateCellTree(ctx, block, rawRight, nil, 2); err != nil {
		t.Fatalf("import raw subtree: %v", err)
	}

	oldLoader := cell.LazyLoader
	cell.LazyLoader = store.LazyCellLoader()
	defer func() { cell.LazyLoader = oldLoader }()

	lazyRawRight, err := store.loadLazyCell(ctx, rawHash[:])
	if err != nil {
		t.Fatalf("load raw lazy subtree: %v", err)
	}
	virtualLazyRight := lazyRawRight.Virtualize(0)
	if virtualLazyRight.IsLazy() || !virtualLazyRight.IsVirtualized() {
		t.Fatal("expected virtual subtree with body")
	}

	mergedBlock := block
	mergedBlock.SeqNo = 4
	root := cell.BeginCell().MustStoreRef(virtualLazyRight).EndCell()
	logicalHash := virtualLazyRight.HashKey()
	if logicalHash == rawHash {
		t.Fatal("test subtree must have distinct logical and raw hashes")
	}

	if _, err = store.saveStateCellTree(ctx, mergedBlock, root, nil, 3, nil, nil); err != nil {
		t.Fatalf("save state with lazy virtual subtree: %v", err)
	}

	if _, err = store.loadLazyCell(ctx, logicalHash[:]); err != nil {
		t.Fatalf("lazy virtual subtree was not persisted by logical hash: %v", err)
	}
}

func TestSaveStateCellTreePersistsMissingReusedLazyDataCell(t *testing.T) {
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

	oldLeaf := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	oldRight := cell.BeginCell().MustStoreUInt(0x33, 8).MustStoreRef(oldLeaf).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), nil, 3)
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
	if lazyRight.IsLazy() {
		t.Fatal("expected right ref body")
	}

	rightHash := lazyRight.HashKey()
	_, shard, err := store.cells.shardForHash(rightHash[:])
	if err != nil {
		t.Fatalf("route reused cell shard: %v", err)
	}
	if err = shard.db.Delete(cellKey(rightHash[:]), pebble.NoSync); err != nil {
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
	if _, err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 2, reused, reusedRefs); err != nil {
		t.Fatalf("save state with missing reused lazy data cell: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, rightHash[:]); err != nil {
		t.Fatalf("reused lazy data cell was not persisted: %v", err)
	}
}

func TestSaveStateCellTreeSkipsMissingVirtualLazyPrunedSubtree(t *testing.T) {
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

	oldLeaf := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	oldRight := cell.BeginCell().MustStoreUInt(0x33, 8).MustStoreRef(oldLeaf).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), nil, 3)
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
	lazyRightMeta := lazyRight.GetMetadata()
	if len(lazyRightMeta.Refs) != 1 {
		t.Fatalf("unexpected lazy right refs: got=%d", len(lazyRightMeta.Refs))
	}
	lazyLeaf := lazyRightMeta.Refs[0]
	if !lazyLeaf.Lazy {
		t.Fatal("expected lazy leaf boundary")
	}

	rightHash := lazyRight.HashKey()
	_, shard, err := store.cells.shardForHash(rightHash[:])
	if err != nil {
		t.Fatalf("route lazy cell shard: %v", err)
	}
	if err = shard.db.Delete(cellKey(rightHash[:]), pebble.NoSync); err != nil {
		t.Fatalf("delete lazy cell: %v", err)
	}
	leafHash := lazyLeaf.Hash
	_, leafShard, err := store.cells.shardForHash(leafHash[:])
	if err != nil {
		t.Fatalf("route lazy leaf shard: %v", err)
	}
	if err = leafShard.db.Delete(cellKey(leafHash[:]), pebble.NoSync); err != nil {
		t.Fatalf("delete lazy leaf: %v", err)
	}
	if exists, err := store.cells.has(leafHash[:]); err != nil {
		t.Fatalf("check deleted lazy leaf: %v", err)
	} else if exists {
		t.Fatal("lazy leaf still exists after delete")
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	newRoot := cell.BeginCell().MustStoreRef(lazyRight).EndCell()
	if _, err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 3, nil, nil); err != nil {
		t.Fatalf("save state with missing virtual lazy pruned subtree: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, leafHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("virtual lazy pruned subtree was restored from boundary: %v", err)
	}
}

func TestSaveStateCellTreePersistsMissingLazyDataCell(t *testing.T) {
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

	oldLeaf := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	oldRight := cell.BeginCell().MustStoreUInt(0x33, 8).MustStoreRef(oldLeaf).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), nil, 3)
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
	if lazyRight.IsLazy() {
		t.Fatal("expected lazy data cell body")
	}

	rightHash := lazyRight.HashKey()
	_, shard, err := store.cells.shardForHash(rightHash[:])
	if err != nil {
		t.Fatalf("route lazy cell shard: %v", err)
	}
	if err = shard.db.Delete(cellKey(rightHash[:]), pebble.NoSync); err != nil {
		t.Fatalf("delete lazy data cell: %v", err)
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	newRoot := cell.BeginCell().MustStoreRef(lazyRight).EndCell()
	if _, err = store.saveStateCellTree(ctx, nextBlock, newRoot, nil, 2, nil, nil); err != nil {
		t.Fatalf("save state with missing lazy data cell: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, rightHash[:]); err != nil {
		t.Fatalf("lazy data cell was not persisted: %v", err)
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
	if lazyRoot.IsLazy() {
		t.Fatal("expected imported root body")
	}

	oldLoader := cell.LazyLoader
	cell.LazyLoader = rejectingLazyLoader{}
	defer func() { cell.LazyLoader = oldLoader }()

	nextBlock := block
	nextBlock.SeqNo = 2
	if _, err = store.saveStateCellTree(ctx, nextBlock, lazyRoot, nil, 1, nil, nil); err != nil {
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

	if _, err = store.ArchiveInfo(context.Background(), 1, -1, int64(-1<<63)); err == nil {
		t.Fatal("expected closed store error")
	}
}
