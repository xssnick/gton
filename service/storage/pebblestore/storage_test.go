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

	if _, err = store.saveStateCellTree(context.Background(), stateCellTreeSave{
		block:      block,
		root:       root,
		totalCells: 2,
	}); err != nil {
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

	roots, parsedCells, err := cell.FromBOCMultiRootReader(bytes.NewReader(root.ToBOC()), cell.BOCParseOptions{})
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

func TestLoadStateCellTreeUsesRootHashWithoutSyncMarker(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	leaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(leaf).EndCell()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     2,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      block,
		root:       root,
		totalCells: 2,
	}); err != nil {
		t.Fatalf("save state cells: %v", err)
	}

	if ok, err := pebbleReaderHas(store.hot, hotKeyStateCellSync(block)); err != nil || ok {
		t.Fatalf("sync marker exists=%t err=%v, want missing", ok, err)
	}

	rootHash := root.HashKey(0)
	loadedRoot, loadedCells, err := store.LoadStateCellTree(ctx, block, rootHash[:])
	if err != nil {
		t.Fatalf("load state by root hash: %v", err)
	}
	if loadedRoot.HashKey() != root.HashKey() {
		t.Fatalf("loaded root hash mismatch")
	}
	if loadedCells != 0 {
		t.Fatalf("loaded cells = %d, want 0 without sync marker", loadedCells)
	}

	wrongHash := bytes.Repeat([]byte{0x44}, 32)
	if _, _, err = store.LoadStateCellTree(ctx, block, wrongHash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load state by wrong root hash err = %v, want ErrNotFound", err)
	}
}

func TestSaveStateCheckpointPersistsOneDurableState(t *testing.T) {
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

	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{state}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	meta, err := store.blockStateMeta(ctx, block)
	if err != nil {
		t.Fatalf("load block state meta: %v", err)
	}
	if !bytes.Equal(meta.StateCellHash, cellHash[:]) {
		t.Fatalf("state cell hash mismatch: got=%x want=%x", meta.StateCellHash, cellHash)
	}
	if ok, err := pebbleReaderHas(store.hot, hotKeyStateCellSync(block)); err != nil || ok {
		t.Fatalf("checkpoint state sync marker exists=%t err=%v, want missing", ok, err)
	}

	loadedRoot, cells, err := store.LoadStateCellTree(ctx, block, rootHash[:])
	if err != nil {
		t.Fatalf("load state cell tree by state meta: %v", err)
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

func TestSeenMasterchainBlockPersistsOnlyNewestHint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	block10 := testMasterBlockID(10, 10)
	block9 := testMasterBlockID(9, 9)

	if _, err = store.SeenMasterchainBlock(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("seen masterchain before save = %v, want ErrNotFound", err)
	}
	if err = store.SaveSeenMasterchainBlock(ctx, block10); err != nil {
		t.Fatalf("save seen masterchain: %v", err)
	}
	if err = store.SaveSeenMasterchainBlock(ctx, block9); err != nil {
		t.Fatalf("save older seen masterchain: %v", err)
	}

	loaded, err := store.SeenMasterchainBlock(ctx)
	if err != nil {
		t.Fatalf("load seen masterchain: %v", err)
	}
	if !loaded.Equals(&block10) {
		t.Fatalf("seen masterchain = %s, want %s", storage.FormatBlockRef(loaded), storage.FormatBlockRef(block10))
	}

	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	loaded, err = store.SeenMasterchainBlock(ctx)
	if err != nil {
		t.Fatalf("load seen masterchain after reopen: %v", err)
	}
	if !loaded.Equals(&block10) {
		t.Fatalf("seen masterchain after reopen = %s, want %s", storage.FormatBlockRef(loaded), storage.FormatBlockRef(block10))
	}
}

func TestVerifiedKeyBlockProgressPersistsOnlyNewestHint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	block20 := testMasterBlockID(20, 20)
	block19 := testMasterBlockID(19, 19)

	if _, err = store.VerifiedKeyBlockProgress(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("verified key progress before save = %v, want ErrNotFound", err)
	}
	if err = store.SaveVerifiedKeyBlockProgress(ctx, block20); err != nil {
		t.Fatalf("save verified key progress: %v", err)
	}
	if err = store.SaveVerifiedKeyBlockProgress(ctx, block19); err != nil {
		t.Fatalf("save older verified key progress: %v", err)
	}

	loaded, err := store.VerifiedKeyBlockProgress(ctx)
	if err != nil {
		t.Fatalf("load verified key progress: %v", err)
	}
	if !loaded.Equals(&block20) {
		t.Fatalf("verified key progress = %s, want %s", storage.FormatBlockRef(loaded), storage.FormatBlockRef(block20))
	}

	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	loaded, err = store.VerifiedKeyBlockProgress(ctx)
	if err != nil {
		t.Fatalf("load verified key progress after reopen: %v", err)
	}
	if !loaded.Equals(&block20) {
		t.Fatalf("verified key progress after reopen = %s, want %s", storage.FormatBlockRef(loaded), storage.FormatBlockRef(block20))
	}
}

func TestRollbackRestoresCurrentAndDeletesFutureMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master10 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	shard10 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x11)
	current10 := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master10.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master10),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard10.Block): storage.BlockStateWithoutCells(shard10),
		},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{master10, shard10}, current10); err != nil {
		t.Fatalf("save target current: %v", err)
	}

	master11 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 11}, 0x20)
	shard11 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 101}, 0x21)
	current11 := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master11.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master11),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard11.Block): storage.BlockStateWithoutCells(shard11),
		},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{master11, shard11}, current11); err != nil {
		t.Fatalf("save future current: %v", err)
	}
	if err = store.LinkNextBlock(master10.Block, master11.Block); err != nil {
		t.Fatalf("link future master: %v", err)
	}
	if err = store.SaveSeenMasterchainBlock(ctx, master11.Block); err != nil {
		t.Fatalf("save seen master: %v", err)
	}
	if err = store.SaveVerifiedKeyBlockProgress(ctx, master11.Block); err != nil {
		t.Fatalf("save verified key progress: %v", err)
	}

	stats, err := store.Rollback(ctx, current10)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if stats.DeletedKeys == 0 {
		t.Fatal("rollback did not delete any metadata")
	}

	loadedCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current after rollback: %v", err)
	}
	if !loadedCurrent.Masterchain.Block.Equals(&master10.Block) {
		t.Fatalf("current master = %s, want %s", storage.FormatBlockRef(loadedCurrent.Masterchain.Block), storage.FormatBlockRef(master10.Block))
	}
	if loadedShard := loadedCurrent.Shards[storage.ShardKeyFromBlock(shard10.Block)]; !loadedShard.Block.Equals(&shard10.Block) {
		t.Fatalf("current shard = %s, want %s", storage.FormatBlockRef(loadedShard.Block), storage.FormatBlockRef(shard10.Block))
	}

	if _, err = store.blockStateMeta(ctx, master10.Block); err != nil {
		t.Fatalf("target master state missing after rollback: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, shard10.Block); err != nil {
		t.Fatalf("target shard state missing after rollback: %v", err)
	}
	if _, err = store.BlockState(ctx, master11.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future master state still exists: %v", err)
	}
	if _, err = store.BlockState(ctx, shard11.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future shard state still exists: %v", err)
	}
	if _, err = store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: int64(-1 << 63)}, master11.Block.SeqNo); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future master seq index still exists: %v", err)
	}
	if _, err = store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: int64(-1 << 63)}, shard11.Block.SeqNo); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future shard seq index still exists: %v", err)
	}
	if _, err = store.NextBlockFull(ctx, master10.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future next block link still exists: %v", err)
	}
	seen, err := store.SeenMasterchainBlock(ctx)
	if err != nil {
		t.Fatalf("load seen master after rollback: %v", err)
	}
	if !seen.Equals(&master10.Block) {
		t.Fatalf("seen master = %s, want %s", storage.FormatBlockRef(seen), storage.FormatBlockRef(master10.Block))
	}
	if _, err = store.VerifiedKeyBlockProgress(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("verified key progress after rollback = %v, want ErrNotFound", err)
	}
}

func testMasterBlockID(seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{seed}, 32),
		FileHash:  bytes.Repeat([]byte{seed + 1}, 32),
	}
}

func TestSaveStateCheckpointPersistsAllAppliedStates(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master10 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	master11 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 11}, 0x11)
	shard100 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x20)
	shard101 := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 101}, 0x21)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master11.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master11),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard101.Block): storage.BlockStateWithoutCells(shard101),
		},
	}

	states := []*storage.BlockState{master10, shard100, master11, shard101}
	if err = store.SaveStateCheckpoint(ctx, states, current); err != nil {
		t.Fatalf("save checkpoint states: %v", err)
	}

	for _, state := range states {
		if _, err = store.blockStateMeta(ctx, state.Block); err != nil {
			t.Fatalf("state meta for %s missing: %v", storage.FormatBlockRef(state.Block), err)
		}
		if _, _, err = store.LoadStateCellTree(ctx, state.Block, state.StateRootHash); err != nil {
			t.Fatalf("state cells for %s missing: %v", storage.FormatBlockRef(state.Block), err)
		}
		if ok, err := pebbleReaderHas(store.hot, hotKeyStateCellSync(state.Block)); err != nil || ok {
			t.Fatalf("checkpoint state sync marker for %s exists=%t err=%v, want missing", storage.FormatBlockRef(state.Block), ok, err)
		}
	}

	loaded, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if !loaded.Masterchain.Block.Equals(&master11.Block) {
		t.Fatalf("current master = %s, want %s", storage.FormatBlockRef(loaded.Masterchain.Block), storage.FormatBlockRef(master11.Block))
	}
}

func blockStateWithSingleCell(block ton.BlockIDExt, value uint64) *storage.BlockState {
	root := cell.BeginCell().MustStoreUInt(value, 8).EndCell()
	return blockStateWithRoot(block, root, 1)
}

func blockStateWithRoot(block ton.BlockIDExt, root *cell.Cell, cellsCount uint64) *storage.BlockState {
	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	if len(block.RootHash) == 0 {
		block.RootHash = bytes.Clone(rootHash[:])
	}
	if len(block.FileHash) == 0 {
		block.FileHash = bytes.Clone(cellHash[:])
	}
	return &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		StateCellHash: cellHash[:],
		CellsCount:    cellsCount,
		Cell:          root,
		DownloadedAt:  time.Now(),
	}
}

func mustCellRecord(tb testing.TB, cl *cell.Cell) *storage.CellRecord {
	tb.Helper()

	record, err := storage.CellRecordFromCell(cl)
	if err != nil {
		tb.Fatalf("cell record from cell: %v", err)
	}
	return record
}

func mustEncodedCellRecord(tb testing.TB, cl *cell.Cell) storage.EncodedCellRecord {
	tb.Helper()

	record := mustCellRecord(tb, cl)
	var hash cell.Hash
	copy(hash[:], record.Hash)
	return storage.EncodedCellRecord{
		Hash: hash,
		Data: encodeCellRecord(record),
	}
}

func testPrunedBranch(tb testing.TB, hidden *cell.Cell) *cell.Cell {
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

func testMerkleUpdateCell(tb testing.TB, from *cell.Cell, to *cell.Cell) *cell.Cell {
	tb.Helper()

	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(from.Hash(0), 256).
		MustStoreSlice(to.Hash(0), 256).
		MustStoreUInt(uint64(from.Depth(0)), 16).
		MustStoreUInt(uint64(to.Depth(0)), 16).
		MustStoreRef(from).
		MustStoreRef(to).
		EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("build merkle update cell: %v", err)
	}
	return update
}

func assertResolvedCellTreeEqual(tb testing.TB, got *cell.Cell, want *cell.Cell) {
	tb.Helper()

	assertResolvedCellTreeEqualAt(tb, got, want, map[cell.Hash]struct{}{})
}

func assertResolvedCellTreeEqualAt(tb testing.TB, got *cell.Cell, want *cell.Cell, seen map[cell.Hash]struct{}) {
	tb.Helper()

	gotHash := got.HashKey(0)
	wantHash := want.HashKey(0)
	if gotHash != wantHash {
		tb.Fatalf("cell hash mismatch: got=%x want=%x", gotHash, wantHash)
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

func assertStateCellExists(tb testing.TB, store *Store, hash cell.Hash, want bool) {
	tb.Helper()

	exists, err := store.cells.has(hash[:])
	if err != nil {
		tb.Fatalf("check state cell %x: %v", hash, err)
	}
	if exists != want {
		tb.Fatalf("state cell %x exists=%t want=%t", hash, exists, want)
	}
}

func TestSaveStateCellTreeStoresPrunedRefAsLazyBoundary(t *testing.T) {
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

	hidden := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	if _, err = store.ImportStateCellTree(ctx, block, hidden, nil, 1); err != nil {
		t.Fatalf("import represented subtree: %v", err)
	}

	pruned := testPrunedBranch(t, hidden)
	prunedHash := pruned.HashKey()
	if prunedHash == hiddenHash {
		t.Fatal("test pruned cell hash must differ from represented hash")
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	root := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(pruned).EndCell()
	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      nextBlock,
		root:       root,
		totalCells: 2,
	}); err != nil {
		t.Fatalf("save state with pruned boundary: %v", err)
	}

	if _, err = store.loadLazyCell(ctx, prunedHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw pruned cell body was persisted: %v", err)
	}

	lazyRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load patch root: %v", err)
	}
	rootMeta := lazyRoot.GetMetadata()
	if len(rootMeta.Refs) != 1 {
		t.Fatalf("unexpected root refs count %d", len(rootMeta.Refs))
	}
	if !rootMeta.Refs[0].Lazy {
		t.Fatal("expected pruned ref to be stored as lazy boundary")
	}
	if rootMeta.Refs[0].Hash != hiddenHash {
		t.Fatalf("boundary hash mismatch: got=%x want=%x", rootMeta.Refs[0].Hash, hiddenHash)
	}

	loadedHidden, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load represented subtree through boundary: %v", err)
	}
	if loadedHidden.HashKey(0) != hiddenHash {
		t.Fatalf("represented subtree hash mismatch: got=%x want=%x", loadedHidden.HashKey(0), hiddenHash)
	}
}

func TestSaveBlockStateSwitchesStateToLazyRoot(t *testing.T) {
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

	hidden := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	if _, err = store.ImportStateCellTree(ctx, block, hidden, nil, 1); err != nil {
		t.Fatalf("import represented subtree: %v", err)
	}

	pruned := testPrunedBranch(t, hidden)
	prunedHash := pruned.HashKey()
	nextBlock := block
	nextBlock.SeqNo = 2
	root := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(pruned).EndCell()
	state := &storage.BlockState{
		Block:      nextBlock,
		Cell:       root,
		CellsCount: 2,
	}
	if err = store.SaveBlockState(ctx, state); err != nil {
		t.Fatalf("save state with pruned boundary: %v", err)
	}

	if state.Cell == root {
		t.Fatal("saved state still points to original proof-shaped root")
	}
	if _, err = store.loadLazyCell(ctx, prunedHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw pruned cell body was persisted: %v", err)
	}
	rootMeta := state.Cell.GetMetadata()
	if len(rootMeta.Refs) != 1 {
		t.Fatalf("unexpected root refs count %d", len(rootMeta.Refs))
	}
	if !rootMeta.Refs[0].Lazy {
		t.Fatal("expected saved root ref to become lazy boundary")
	}
	if rootMeta.Refs[0].Hash != hiddenHash {
		t.Fatalf("boundary hash mismatch: got=%x want=%x", rootMeta.Refs[0].Hash, hiddenHash)
	}
	loadedHidden, err := state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load represented subtree through saved lazy root: %v", err)
	}
	if loadedHidden.HashKey(0) != hiddenHash {
		t.Fatalf("represented subtree hash mismatch: got=%x want=%x", loadedHidden.HashKey(0), hiddenHash)
	}
}

func TestSaveStateCellTreeDirectMerkleUpdateMatchesApplyMerkleUpdate(t *testing.T) {
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

	oldLeaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	stableLeaf := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	oldBranch := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(oldLeaf).EndCell()
	fromRoot := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(stableLeaf).
		EndCell()

	lazyFromRoot, err := store.ImportStateCellTree(ctx, block, fromRoot, nil, 4)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}

	prunedOldLeaf := testPrunedBranch(t, oldLeaf)
	prunedStable := testPrunedBranch(t, stableLeaf)
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
		t.Fatal("test update destination branch must have a non-zero level")
	}
	logicalToBranch := updateToBranch.Virtualize(0)
	if logicalToBranch.HashKey() == updateToBranch.HashKey() {
		t.Fatal("test update destination branch must have distinct logical and raw hashes")
	}

	fullToBranch := cell.BeginCell().
		MustStoreUInt(0xcc, 8).
		MustStoreRef(oldLeaf).
		EndCell()
	updateTo := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(updateToBranch).
		MustStoreRef(prunedStable).
		EndCell()
	fullToRoot := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(fullToBranch).
		MustStoreRef(stableLeaf).
		EndCell()
	update := testMerkleUpdateCell(t, updateFrom, updateTo)

	if err = cell.ValidateMerkleUpdate(update); err != nil {
		t.Fatalf("validate merkle update: %v", err)
	}
	if err = cell.MayApplyMerkleUpdate(lazyFromRoot.Virtualize(0), update); err != nil {
		t.Fatalf("may apply merkle update: %v", err)
	}
	applied, _, err := cell.ApplyMerkleUpdate(lazyFromRoot.Virtualize(0), update)
	if err != nil {
		t.Fatalf("apply merkle update: %v", err)
	}
	assertResolvedCellTreeEqual(t, applied, fullToRoot)

	directRoot := updateTo.Virtualize(0)
	stateRootHash := directRoot.HashKey(0)
	stateCellHash := directRoot.HashKey()
	if stateCellHash != stateRootHash {
		t.Fatalf("direct state root is not level zero: got=%x want=%x", stateCellHash, stateRootHash)
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	nextState := &storage.BlockState{
		Block:         nextBlock,
		StateRootHash: stateRootHash[:],
		StateCellHash: stateCellHash[:],
		CellsCount:    4,
		Cell:          directRoot,
		DownloadedAt:  time.Now(),
	}
	if err = store.SaveBlockState(ctx, nextState); err != nil {
		t.Fatalf("save direct state root: %v", err)
	}

	loaded, _, err := store.LoadStateCellTree(ctx, nextBlock, stateRootHash[:])
	if err != nil {
		t.Fatalf("load direct state root: %v", err)
	}
	if loaded.HashKey() != applied.HashKey() {
		t.Fatalf("loaded root hash mismatch with apply result: got=%x want=%x", loaded.HashKey(), applied.HashKey())
	}
	assertResolvedCellTreeEqual(t, loaded, applied)
	assertResolvedCellTreeEqual(t, loaded, fullToRoot)

	assertStateCellExists(t, store, stateCellHash, true)
	assertStateCellExists(t, store, logicalToBranch.HashKey(), true)
	assertStateCellExists(t, store, updateToBranch.HashKey(), false)
	assertStateCellExists(t, store, prunedOldLeaf.HashKey(), false)
	assertStateCellExists(t, store, prunedStable.HashKey(), false)
	assertStateCellExists(t, store, updateFrom.HashKey(), false)
	assertStateCellExists(t, store, update.HashKey(), false)
	if rawRootHash := updateTo.HashKey(); rawRootHash != stateCellHash {
		assertStateCellExists(t, store, rawRootHash, false)
	}
}

func TestApplyMerkleUpdateCheckpointKeepsIntermediateCells(t *testing.T) {
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

	oldLeaf := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	stableLeaf := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	oldBranch := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(oldLeaf).EndCell()
	fromRoot := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(stableLeaf).
		EndCell()
	lazyFromRoot, err := store.ImportStateCellTree(ctx, block, fromRoot, nil, 4)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}

	prunedStable := testPrunedBranch(t, stableLeaf)
	newLeaf := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	newBranch := cell.BeginCell().MustStoreUInt(0xcc, 8).MustStoreRef(newLeaf).EndCell()
	update1From := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreRef(oldBranch).
		MustStoreRef(prunedStable).
		EndCell()
	update1To := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(newBranch).
		MustStoreRef(prunedStable).
		EndCell()
	fullState1 := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(newBranch).
		MustStoreRef(stableLeaf).
		EndCell()

	update1 := testMerkleUpdateCell(t, update1From, update1To)
	state1, _, err := cell.ApplyMerkleUpdate(lazyFromRoot.Virtualize(0), update1)
	if err != nil {
		t.Fatalf("apply first update: %v", err)
	}
	assertResolvedCellTreeEqual(t, state1, fullState1)

	prunedNewBranch := testPrunedBranch(t, newBranch)
	changedStable := cell.BeginCell().MustStoreUInt(0xdd, 8).EndCell()
	update2From := cell.BeginCell().
		MustStoreUInt(0x20, 8).
		MustStoreRef(prunedNewBranch).
		MustStoreRef(stableLeaf).
		EndCell()
	update2To := cell.BeginCell().
		MustStoreUInt(0x30, 8).
		MustStoreRef(prunedNewBranch).
		MustStoreRef(changedStable).
		EndCell()
	fullState2 := cell.BeginCell().
		MustStoreUInt(0x30, 8).
		MustStoreRef(newBranch).
		MustStoreRef(changedStable).
		EndCell()

	update2 := testMerkleUpdateCell(t, update2From, update2To)
	state2, _, err := cell.ApplyMerkleUpdate(state1, update2)
	if err != nil {
		t.Fatalf("apply second update: %v", err)
	}
	assertResolvedCellTreeEqual(t, state2, fullState2)

	directBlock := block
	directBlock.SeqNo = 2
	directState := blockStateWithRoot(directBlock, update2To.Virtualize(0), 4)
	if err = store.SaveBlockState(ctx, directState); err == nil {
		t.Fatal("direct proof-shaped state with unsaved pruned boundary was accepted")
	}

	nextBlock := block
	nextBlock.SeqNo = 3
	nextState := blockStateWithRoot(nextBlock, state2, 5)
	if err = store.SaveBlockState(ctx, nextState); err != nil {
		t.Fatalf("save merged state: %v", err)
	}

	stateRootHash := state2.HashKey(0)
	loaded, _, err := store.LoadStateCellTree(ctx, nextBlock, stateRootHash[:])
	if err != nil {
		t.Fatalf("load merged state root: %v", err)
	}
	assertResolvedCellTreeEqual(t, loaded, fullState2)
	assertStateCellExists(t, store, newBranch.HashKey(), true)
}

func TestSaveStateCheckpointDoesNotCommitMissingLazyBoundary(t *testing.T) {
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
	initial := blockStateWithSingleCell(block, 0x11)
	initialCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(initial),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{initial}, initialCurrent); err != nil {
		t.Fatalf("save initial current: %v", err)
	}

	hidden := cell.BeginCell().MustStoreUInt(0x99, 8).EndCell()
	pruned := testPrunedBranch(t, hidden)
	badRoot := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(pruned).EndCell()
	badBlock := block
	badBlock.SeqNo = 2
	badState := blockStateWithRoot(badBlock, badRoot, 2)
	badCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: badBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(badState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{badState}, badCurrent); err == nil {
		t.Fatal("checkpoint with missing lazy boundary was accepted")
	}

	loaded, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current after failed checkpoint: %v", err)
	}
	if !loaded.Masterchain.Block.Equals(&initial.Block) {
		t.Fatalf("current advanced after failed checkpoint: got %s want %s", storage.FormatBlockRef(loaded.Masterchain.Block), storage.FormatBlockRef(initial.Block))
	}
	if _, err = store.blockStateMeta(ctx, badBlock); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("bad block state metadata was committed: %v", err)
	}
}

func TestSaveStateCheckpointWithPreparedCellsLoadsLazyRoot(t *testing.T) {
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
	child := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(child).EndCell()
	state := blockStateWithRoot(block, root, 2)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}

	records := []storage.EncodedCellRecord{
		mustEncodedCellRecord(t, root),
		mustEncodedCellRecord(t, child),
	}
	if err = store.SaveStateCheckpointWithCells(ctx, []*storage.BlockState{state}, current, records); err != nil {
		t.Fatalf("save checkpoint with prepared cells: %v", err)
	}

	loaded, _, err := store.LoadStateCellTree(ctx, block, state.StateRootHash)
	if err != nil {
		t.Fatalf("load saved state: %v", err)
	}
	loadedChild, err := loaded.PeekRef(0)
	if err != nil {
		t.Fatalf("load saved child ref: %v", err)
	}
	if loadedChild.HashKey() != child.HashKey() {
		t.Fatalf("loaded child hash mismatch: got=%x want=%x", loadedChild.HashKey(), child.HashKey())
	}
}

func TestSaveStateCheckpointWithPreparedCellsRejectsMissingRoot(t *testing.T) {
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
	child := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(child).EndCell()
	state := blockStateWithRoot(block, root, 2)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}

	if err = store.SaveStateCheckpointWithCells(ctx, []*storage.BlockState{state}, current, []storage.EncodedCellRecord{mustEncodedCellRecord(t, child)}); err == nil {
		t.Fatal("checkpoint with missing prepared root was accepted")
	}
	if _, err = store.blockStateMeta(ctx, block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("bad block state metadata was committed: %v", err)
	}
}

func TestSaveStateCellTreeStoresConcretePrunedParentByLogicalHash(t *testing.T) {
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

	hidden := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	if _, err = store.ImportStateCellTree(ctx, block, hidden, nil, 1); err != nil {
		t.Fatalf("import represented leaf: %v", err)
	}

	pruned := testPrunedBranch(t, hidden)
	rawChild := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(pruned).EndCell()
	if rawChild.Level() == 0 {
		t.Fatal("test child must have a non-zero level")
	}
	logicalChild := rawChild.Virtualize(0)
	if logicalChild.HashKey() == rawChild.HashKey() {
		t.Fatal("test child must have distinct logical and raw hashes")
	}
	root := cell.BeginCell().MustStoreRef(rawChild).EndCell()

	nextBlock := block
	nextBlock.SeqNo = 2
	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      nextBlock,
		root:       root,
		totalCells: 3,
	}); err != nil {
		t.Fatalf("save state with concrete pruned parent: %v", err)
	}

	if _, err = store.loadLazyCell(ctx, rawChild.Hash()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw child body should not be stored under top hash: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, logicalChild.Hash()); err != nil {
		t.Fatalf("logical child body was not persisted: %v", err)
	}

	lazyRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	loadedChild, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load logical child through parent ref: %v", err)
	}
	if loadedChild.HashKey() != logicalChild.HashKey() {
		t.Fatalf("loaded child hash mismatch: got=%x want=%x", loadedChild.HashKey(), logicalChild.HashKey())
	}
	loadedHidden, err := loadedChild.PeekRef(0)
	if err != nil {
		t.Fatalf("load represented leaf through child boundary: %v", err)
	}
	if loadedHidden.HashKey(0) != hidden.HashKey(0) {
		t.Fatalf("represented leaf hash mismatch: got=%x want=%x", loadedHidden.HashKey(0), hidden.HashKey(0))
	}
}

func TestSaveParsedStateCellsBatchKeepsMissingPrunedBoundaryLazy(t *testing.T) {
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

	hidden := cell.BeginCell().MustStoreUInt(0x88, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	pruned := testPrunedBranch(t, hidden)
	prunedHash := pruned.HashKey()
	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(pruned).EndCell()
	parsedCells := []cell.Cell{*root, *pruned}

	lazyRoot, err := store.ImportStateCellTree(ctx, block, root, parsedCells, uint64(len(parsedCells)))
	if err != nil {
		t.Fatalf("import parsed state cells: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, prunedHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw pruned parsed cell body was persisted: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, hiddenHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("represented pruned boundary placeholder was persisted: %v", err)
	}
	if _, err = lazyRoot.PeekRef(0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing represented pruned boundary should stay lazy until resolved: %v", err)
	}
}

func TestSaveParsedStateCellsBatchUsesExistingRepresentedCellForPrunedBoundary(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	baseBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}

	hidden := cell.BeginCell().MustStoreUInt(0x88, 8).EndCell()
	hiddenHash := hidden.HashKey(0)
	if _, err = store.ImportStateCellTree(ctx, baseBlock, hidden, nil, 1); err != nil {
		t.Fatalf("import represented cell: %v", err)
	}

	pruned := testPrunedBranch(t, hidden)
	prunedHash := pruned.HashKey()
	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(pruned).EndCell()
	parsedCells := []cell.Cell{*root, *pruned}

	block := baseBlock
	block.SeqNo = 2
	if _, err = store.ImportStateCellTree(ctx, block, root, parsedCells, uint64(len(parsedCells))); err != nil {
		t.Fatalf("import parsed state cells: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, prunedHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw pruned parsed cell body was persisted: %v", err)
	}

	lazyRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load parsed root: %v", err)
	}
	loadedRef, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load represented ref through parsed parent: %v", err)
	}
	if loadedRef.GetType() == cell.PrunedCellType {
		t.Fatal("materialized pruned boundary overwrote existing represented cell")
	}
	if loadedRef.HashKey(0) != hiddenHash {
		t.Fatalf("represented cell hash mismatch: got=%x want=%x", loadedRef.HashKey(0), hiddenHash)
	}
}

func TestSaveParsedStateCellsBatchStoresConcretePrunedParentByLogicalHash(t *testing.T) {
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

	hidden := cell.BeginCell().MustStoreUInt(0xcc, 8).EndCell()
	pruned := testPrunedBranch(t, hidden)
	rawChild := cell.BeginCell().MustStoreUInt(0xdd, 8).MustStoreRef(pruned).EndCell()
	if rawChild.Level() == 0 {
		t.Fatal("test child must have a non-zero level")
	}
	logicalChild := rawChild.Virtualize(0)
	root := cell.BeginCell().MustStoreRef(rawChild).EndCell()
	parsedCells := []cell.Cell{*root, *rawChild, *pruned, *hidden}

	if _, err = store.ImportStateCellTree(ctx, block, root, parsedCells, uint64(len(parsedCells))); err != nil {
		t.Fatalf("import parsed state cells: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, logicalChild.Hash()); err != nil {
		t.Fatalf("logical child body was not persisted from parsed batch: %v", err)
	}

	lazyRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load parsed root: %v", err)
	}
	loadedChild, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load logical child through parsed parent ref: %v", err)
	}
	if loadedChild.HashKey() != logicalChild.HashKey() {
		t.Fatalf("loaded child hash mismatch: got=%x want=%x", loadedChild.HashKey(), logicalChild.HashKey())
	}
	loadedHidden, err := loadedChild.PeekRef(0)
	if err != nil {
		t.Fatalf("load represented leaf through parsed child boundary: %v", err)
	}
	if loadedHidden.HashKey(0) != hidden.HashKey(0) {
		t.Fatalf("represented leaf hash mismatch: got=%x want=%x", loadedHidden.HashKey(0), hidden.HashKey(0))
	}
}

func TestSaveStateCellTreeRejectsPrunedRoot(t *testing.T) {
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

	hidden := cell.BeginCell().MustStoreUInt(0x99, 8).EndCell()
	pruned := testPrunedBranch(t, hidden)
	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      block,
		root:       pruned,
		totalCells: 1,
	}); err == nil {
		t.Fatal("expected pruned root to be rejected")
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
	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      nextBlock,
		root:       newRoot,
		totalCells: 3,
	}); err == nil {
		t.Fatal("state with missing virtual lazy pruned subtree was accepted")
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
	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      nextBlock,
		root:       newRoot,
		totalCells: 2,
	}); err != nil {
		t.Fatalf("save state with missing lazy data cell: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, rightHash[:]); err != nil {
		t.Fatalf("lazy data cell was not persisted: %v", err)
	}
}

func rejectingLazyLoader(cell.Hash) (*cell.Cell, error) {
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
	if _, err := store.ImportStateCellTree(ctx, block, root, nil, 2); err != nil {
		t.Fatalf("import state: %v", err)
	}

	record, err := store.CellRecord(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load root cell record: %v", err)
	}
	lazyRoot, err := storage.LazyCellRecord(record, rejectingLazyLoader)
	if err != nil {
		t.Fatalf("create lazy root: %v", err)
	}
	if lazyRoot.IsLazy() {
		t.Fatal("expected imported root body")
	}

	nextBlock := block
	nextBlock.SeqNo = 2
	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      nextBlock,
		root:       lazyRoot,
		totalCells: 1,
	}); err != nil {
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
