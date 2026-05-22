package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/xssnick/gton/service/storage"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
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

func TestLargeBOCLoadRecords(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	partial := cell.BeginCell().MustStoreUInt(0b10101, 5).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0xAA, 8).MustStoreRef(partial).EndCell()
	root := cell.BeginCell().MustStoreUInt(0b111, 3).MustStoreRef(shared).MustStoreRef(shared).EndCell()
	records := make([]*storage.CellRecord, 0, 3)
	for _, cl := range []*cell.Cell{partial, shared, root} {
		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			t.Fatalf("cell record: %v", err)
		}
		records = append(records, record)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	byHash := make(map[cell.Hash]*storage.CellRecord, len(records))
	hashes := make([]cell.Hash, 0, len(records))
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		var hash cell.Hash
		copy(hash[:], record.Hash)
		byHash[hash] = record
		hashes = append(hashes, hash)
	}

	metaRecords, err := store.LargeBOCLoadMeta(context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	payloadRecords, err := store.LargeBOCLoadPayload(context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load payload: %v", err)
	}
	cellRecords, err := store.LargeBOCLoadCells(context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load cells: %v", err)
	}
	for i, hash := range hashes {
		record := byHash[hash]
		wantMeta, err := storage.LargeBOCMetaRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("meta from record: %v", err)
		}
		if metaRecords[i] != wantMeta {
			t.Fatalf("meta[%d] mismatch", i)
		}
		if cellRecords[i].Meta != wantMeta {
			t.Fatalf("one-pass meta[%d] mismatch", i)
		}

		wantPayload, err := storage.LargeBOCPayloadRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("payload from record: %v", err)
		}
		if !bytes.Equal(payloadRecords[i].Data, wantPayload.Data) {
			t.Fatalf("payload[%d] mismatch", i)
		}
		if !bytes.Equal(cellRecords[i].Payload.Data, wantPayload.Data) {
			t.Fatalf("one-pass payload[%d] mismatch", i)
		}
	}
}

func TestLargeBOCLoadPayloadAndCellsWithWorkerLocalArenas(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	records := make([]*storage.CellRecord, 0, largeBOCShardReadWorkerMinCell)
	hashes := make([]cell.Hash, 0, largeBOCShardReadWorkerMinCell)
	for nonce := uint64(0); len(records) < largeBOCShardReadWorkerMinCell && nonce < 1_000_000; nonce++ {
		cl := cell.BeginCell().MustStoreUInt(nonce, 64).EndCell()
		hash := cl.HashKey()
		if int(hash[0]>>5) != 0 {
			continue
		}

		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			t.Fatalf("cell record: %v", err)
		}
		records = append(records, record)
		hashes = append(hashes, hash)
	}
	if len(records) != largeBOCShardReadWorkerMinCell {
		t.Fatalf("generated %d records in one shard, want %d", len(records), largeBOCShardReadWorkerMinCell)
	}
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	payloadRecords, err := store.LargeBOCLoadPayload(context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load payload: %v", err)
	}
	cellRecords, err := store.LargeBOCLoadCells(context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load cells: %v", err)
	}

	if len(payloadRecords) != len(records) {
		t.Fatalf("payload records = %d, want %d", len(payloadRecords), len(records))
	}
	if len(cellRecords) != len(records) {
		t.Fatalf("cell records = %d, want %d", len(cellRecords), len(records))
	}
	for i, record := range records {
		wantMeta, err := storage.LargeBOCMetaRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("meta from record %d: %v", i, err)
		}
		if cellRecords[i].Meta != wantMeta {
			t.Fatalf("one-pass meta[%d] mismatch", i)
		}

		wantPayload, err := storage.LargeBOCPayloadRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("payload from record %d: %v", i, err)
		}
		if !bytes.Equal(payloadRecords[i].Data, wantPayload.Data) {
			t.Fatalf("payload[%d] mismatch", i)
		}
		if !bytes.Equal(cellRecords[i].Payload.Data, wantPayload.Data) {
			t.Fatalf("one-pass payload[%d] mismatch", i)
		}
	}
}

func TestHotMetaCodecsAvoidDuplicatedKeys(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     42,
		RootHash:  bytes.Repeat([]byte{0x42}, 32),
		FileHash:  bytes.Repeat([]byte{0x24}, 32),
	}
	prev := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     41,
		RootHash:  bytes.Repeat([]byte{0x41}, 32),
		FileHash:  bytes.Repeat([]byte{0x14}, 32),
	}
	meta := &storage.BlockMeta{
		ID:       block,
		Flags:    storage.BlockMetaHasBlockData,
		GenUTime: 1000,
		StartLT:  2000,
		EndLT:    3000,
		PrevRefs: []ton.BlockIDExt{prev},
	}

	encodedMeta := encodeBlockMeta(meta)
	if bytes.Contains(encodedMeta, encodeBlockID(block)) {
		t.Fatal("block meta payload stores duplicated block id")
	}
	decodedMeta, err := decodeBlockMeta(block, encodedMeta)
	if err != nil {
		t.Fatalf("decode block meta: %v", err)
	}
	if !decodedMeta.ID.Equals(&block) {
		t.Fatalf("decoded block id = %s, want %s", storage.FormatBlockRef(decodedMeta.ID), storage.FormatBlockRef(block))
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Unix(123, 456).UTC(),
		ShardClientSeqno: block.SeqNo,
		Masterchain: storage.BlockState{Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     block.SeqNo,
			RootHash:  bytes.Repeat([]byte{0xaa}, 32),
			FileHash:  bytes.Repeat([]byte{0xbb}, 32),
		}},
		Shards: map[storage.ShardKey]storage.BlockState{
			{Workchain: 123, Shard: 456}: {Block: block},
		},
	}
	encodedCurrent := encodeCurrentState(current)
	oldCurrentLen := 1 + 8 + 4 + 80 + 4 + len(current.Shards)*(12+80)
	wantCurrentLen := oldCurrentLen - len(current.Shards)*12
	if len(encodedCurrent) != wantCurrentLen {
		t.Fatalf("encoded current state size = %d, want %d", len(encodedCurrent), wantCurrentLen)
	}

	decodedCurrent, err := decodeCurrentState(encodedCurrent)
	if err != nil {
		t.Fatalf("decode current state: %v", err)
	}
	if _, ok := decodedCurrent.Shards[storage.ShardKey{Workchain: 123, Shard: 456}]; ok {
		t.Fatal("decoded current state used stale encoded shard key")
	}
	shard, ok := decodedCurrent.Shards[storage.ShardKeyFromBlock(block)]
	if !ok {
		t.Fatalf("decoded current state misses shard key from block %s", storage.FormatBlockRef(block))
	}
	if !shard.Block.Equals(&block) {
		t.Fatalf("decoded shard block = %s, want %s", storage.FormatBlockRef(shard.Block), storage.FormatBlockRef(block))
	}
}

func TestOpenRejectsMetaDBWithoutCellGenerationManifest(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "metadb")
	if err := os.MkdirAll(hotDir, 0o755); err != nil {
		t.Fatalf("create metadb dir: %v", err)
	}

	db, err := pebble.Open(hotDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open raw metadb: %v", err)
	}
	if err = db.Set(hotKeyVerifiedKeyBlockProgress(), []byte{0x01}, pebble.Sync); err != nil {
		t.Fatalf("write raw metadb record: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close raw metadb: %v", err)
	}

	store, err := Open(Options{Dir: dir})
	if err == nil {
		_ = store.Close()
		t.Fatal("opened non-empty metadb without cell generation manifest")
	}
	if !strings.Contains(err.Error(), "cell generation manifest is missing") {
		t.Fatalf("open error = %v, want missing manifest", err)
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
		if entry.Message == "state cells imported into celldb" {
			final = entry
		}
	}
	if final.Message == "" {
		t.Fatal("missing final state cell import log")
	}
	if final.Processed != 2 {
		t.Fatalf("processed duplicate shared ref: got=%d want=2", final.Processed)
	}
	if final.Total != 2 || final.Progress != "100.0%" {
		t.Fatalf("unexpected final progress: total=%d progress=%q", final.Total, final.Progress)
	}
}

func TestImportStateCellTreeFlushesCellsBeforeReturningLazyRoot(t *testing.T) {
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

	lazyRoot, err := store.ImportStateCellTree(ctx, block, root, 0)
	if err != nil {
		t.Fatalf("import state cell tree: %v", err)
	}
	if lazyRoot.IsLazy() || lazyRoot.HashKey() != rootHash {
		t.Fatalf("lazy root metadata mismatch")
	}

	rootLevelHash := root.HashKey(0)
	if _, err = store.LoadStateCellTree(ctx, block, rootLevelHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load imported state without metadata error = %v, want ErrNotFound", err)
	}
	cellMetrics := store.cells.metrics()
	if cellMetrics.flushCount == 0 {
		t.Fatal("state import did not flush cell db before returning lazy root")
	}
	if got := cellMetrics.ingestCount; got != 0 {
		t.Fatalf("state import unexpectedly used pebble ingest: got ingest count %d", got)
	}
}

func TestImportStateBOCViewFromUntrustedBOC(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	shared := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	left := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(shared).EndCell()
	right := cell.BeginCell().MustStoreUInt(0xcc, 8).MustStoreRef(shared).EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0xdd, 8).
		MustStoreRef(left).
		MustStoreRef(right).
		MustStoreRef(shared).
		EndCell()
	rootHash := root.HashKey()

	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	view, err := cell.OpenBOCView(bytes.NewReader(boc), int64(len(boc)), cell.BOCViewOptions{
		TrustedHashes: false,
		RequireIndex:  true,
		ValidateCRC:   true,
	})
	if err != nil {
		t.Fatalf("open boc view: %v", err)
	}

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     2,
		RootHash:  bytes.Repeat([]byte{0x33}, 32),
		FileHash:  bytes.Repeat([]byte{0x44}, 32),
	}
	lazyRoot, err := store.ImportStateBOCView(ctx, block, view)
	if err != nil {
		t.Fatalf("import boc view state cells: %v", err)
	}
	if lazyRoot.IsLazy() || lazyRoot.HashKey() != rootHash {
		t.Fatalf("lazy root metadata mismatch")
	}

	assertResolvedCellTreeEqual(t, lazyRoot, root)
	assertStateCellExists(t, store, root.HashKey(), true)
	assertStateCellExists(t, store, left.HashKey(), true)
	assertStateCellExists(t, store, right.HashKey(), true)
	assertStateCellExists(t, store, shared.HashKey(), true)

	if got := store.cells.metrics().flushCount; got == 0 {
		t.Fatal("lazy state import did not flush cell db before returning lazy root")
	}
}

func TestImportStateBOCViewPreservesPrunedLevels(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	hidden := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	pruned := testPrunedBranch(t, hidden)
	if pruned.Level() == 0 {
		t.Fatal("test pruned branch must have non-zero level")
	}
	branch := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(pruned).EndCell()
	if branch.Level() == 0 {
		t.Fatal("test branch must inherit non-zero level")
	}
	root := cell.BeginCell().MustStoreUInt(0xaa, 8).MustStoreRef(branch).EndCell()

	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	view, err := cell.OpenBOCView(bytes.NewReader(boc), int64(len(boc)), cell.BOCViewOptions{
		TrustedHashes: false,
		RequireIndex:  true,
		ValidateCRC:   true,
	})
	if err != nil {
		t.Fatalf("open boc view: %v", err)
	}

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     3,
		RootHash:  bytes.Repeat([]byte{0x55}, 32),
		FileHash:  bytes.Repeat([]byte{0x66}, 32),
	}
	lazyRoot, err := store.ImportStateBOCView(ctx, block, view)
	if err != nil {
		t.Fatalf("import boc view state cells with pruned branch: %v", err)
	}

	assertStateCellExists(t, store, root.HashKey(), true)
	assertStateCellExists(t, store, branch.HashKey(), true)
	assertStateCellExists(t, store, pruned.HashKey(), true)
	if hidden.HashKey(0) != pruned.HashKey() {
		assertStateCellExists(t, store, hidden.HashKey(0), false)
	}

	loadedBranch, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load branch from imported lazy root: %v", err)
	}
	if loadedBranch.HashKey() != branch.HashKey() || loadedBranch.Level() != branch.Level() {
		t.Fatalf("loaded branch metadata mismatch")
	}
	loadedBranchBody, err := loadedBranch.Prewarm()
	if err != nil {
		t.Fatalf("prewarm imported branch: %v", err)
	}
	loadedPruned, err := loadedBranchBody.PeekRef(0)
	if err != nil {
		t.Fatalf("load pruned ref from imported branch: %v", err)
	}
	if loadedPruned.GetType() != cell.PrunedCellType {
		t.Fatalf("loaded ref type = %d, want pruned", loadedPruned.GetType())
	}
	if loadedPruned.HashKey() != pruned.HashKey() || loadedPruned.Level() != pruned.Level() {
		t.Fatalf("loaded pruned metadata mismatch")
	}
}

func TestImportStateBOCViewPersistsIndexedBOC(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	hidden := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	pruned := testPrunedBranch(t, hidden)
	shared := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	left := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(shared).EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0xcc, 8).
		MustStoreRef(left).
		MustStoreRef(shared).
		MustStoreRef(pruned).
		EndCell()

	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	view, err := cell.OpenBOCView(bytes.NewReader(boc), int64(len(boc)), cell.BOCViewOptions{
		RequireIndex: true,
		ValidateCRC:  true,
	})
	if err != nil {
		t.Fatalf("open boc view: %v", err)
	}

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     4,
		RootHash:  bytes.Repeat([]byte{0x77}, 32),
		FileHash:  bytes.Repeat([]byte{0x88}, 32),
	}
	lazyRoot, err := store.ImportStateBOCView(ctx, block, view)
	if err != nil {
		t.Fatalf("import boc view state cells: %v", err)
	}
	if lazyRoot.IsLazy() || lazyRoot.HashKey() != root.HashKey() {
		t.Fatalf("lazy root metadata mismatch")
	}

	assertResolvedCellTreeEqual(t, lazyRoot, root)
	assertStateCellExists(t, store, root.HashKey(), true)
	assertStateCellExists(t, store, left.HashKey(), true)
	assertStateCellExists(t, store, shared.HashKey(), true)
	assertStateCellExists(t, store, pruned.HashKey(), true)
	if hidden.HashKey(0) != pruned.HashKey() {
		assertStateCellExists(t, store, hidden.HashKey(0), false)
	}
}

func TestLoadStateCellTreeRequiresCommittedStateMeta(t *testing.T) {
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
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	root := cell.BeginCell().MustStoreUInt(0xcc, 8).EndCell()
	rootHash := root.HashKey(0)
	cellHash := root.HashKey()

	if _, err = store.ImportStateCellTree(ctx, block, root, 1); err != nil {
		t.Fatalf("import state cell tree: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, block, rootHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load state before metadata = %v, want ErrNotFound", err)
	}

	if err = store.SaveBlockState(ctx, &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		StateCellHash: cellHash[:],
	}); err != nil {
		t.Fatalf("save block state metadata: %v", err)
	}

	if _, err = store.LoadStateCellTree(ctx, block, rootHash[:]); err != nil {
		t.Fatalf("load state by state meta: %v", err)
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
		Cell:          root,
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
	loadedRoot, err := store.LoadStateCellTree(ctx, block, rootHash[:])
	if err != nil {
		t.Fatalf("load state cell tree by state meta: %v", err)
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

func TestDecodeBlockStateMetaRejectsPayloadWithoutMasterchainRefFlag(t *testing.T) {
	buf := make([]byte, 0, 1+33+33+33)
	buf = append(buf, blockStateMetaVersion)
	buf = appendLenBytes(buf, bytes.Repeat([]byte{0x11}, 32))
	buf = appendLenBytes(buf, bytes.Repeat([]byte{0x22}, 32))
	buf = appendLenBytes(buf, bytes.Repeat([]byte{0x33}, 32))

	_, _, _, _, err := decodeBlockStateMeta(buf)
	if err == nil || !strings.Contains(err.Error(), "masterchain ref flag missing") {
		t.Fatalf("decode block state meta error = %v, want missing masterchain ref flag", err)
	}
}

func TestSaveStateCheckpointStoresShardInclusionMasterRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 20}, 0x20)
	shard.MasterchainRef = &master.Block

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): storage.BlockStateWithoutCells(shard),
		},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{master, shard}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	meta, err := store.BlockMeta(ctx, shard.Block)
	if err != nil {
		t.Fatalf("load shard meta: %v", err)
	}
	if meta.MasterchainRef == nil || !meta.MasterchainRef.Equals(&master.Block) {
		t.Fatalf("masterchain ref = %+v, want %s", meta.MasterchainRef, storage.FormatBlockRef(master.Block))
	}
}

func TestSaveStateCheckpointKeepsExistingShardMasterchainRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 20}, 0x20)
	oldMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     9,
		RootHash:  bytes.Repeat([]byte{0x09}, 32),
		FileHash:  bytes.Repeat([]byte{0x19}, 32),
	}
	shard.MasterchainRef = &master.Block
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: shard.Block, MasterchainRef: &oldMaster}); err != nil {
		t.Fatalf("save old shard meta: %v", err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): storage.BlockStateWithoutCells(shard),
		},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{master, shard}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	meta, err := store.BlockMeta(ctx, shard.Block)
	if err != nil {
		t.Fatalf("load shard meta: %v", err)
	}
	if meta.MasterchainRef == nil || !meta.MasterchainRef.Equals(&oldMaster) {
		t.Fatalf("masterchain ref = %+v, want %s", meta.MasterchainRef, storage.FormatBlockRef(oldMaster))
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

func TestOpenReadOnlyRejectsWrites(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	block10 := testMasterBlockID(10, 10)
	block11 := testMasterBlockID(11, 11)
	if err = store.SaveVerifiedKeyBlockProgress(ctx, block10); err != nil {
		t.Fatalf("save verified key progress: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	defer func() { _ = store.Close() }()

	loaded, err := store.VerifiedKeyBlockProgress(ctx)
	if err != nil {
		t.Fatalf("load verified key progress: %v", err)
	}
	if !loaded.Equals(&block10) {
		t.Fatalf("verified key progress = %s, want %s", storage.FormatBlockRef(loaded), storage.FormatBlockRef(block10))
	}
	if err = store.SaveVerifiedKeyBlockProgress(ctx, block11); !errors.Is(err, pebble.ErrReadOnly) {
		t.Fatalf("save in read-only store = %v, want pebble.ErrReadOnly", err)
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
		if _, err = store.LoadStateCellTree(ctx, state.Block, state.StateRootHash); err != nil {
			t.Fatalf("state cells for %s missing: %v", storage.FormatBlockRef(state.Block), err)
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

func TestSaveStateCheckpointRejectsCurrentShardWithoutStateMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x20)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): storage.BlockStateWithoutCells(shard),
		},
	}

	err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{master}, current)
	if err == nil || !strings.Contains(err.Error(), "current shard state") || !strings.Contains(err.Error(), "metadata is missing") {
		t.Fatalf("save checkpoint error = %v, want missing current shard metadata", err)
	}
	if _, err = store.CurrentState(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current after failed checkpoint = %v, want ErrNotFound", err)
	}
}

func TestSaveStateCheckpointAllowsExistingCurrentShardStateMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x20)
	if err = store.SaveBlockState(ctx, shard); err != nil {
		t.Fatalf("save existing shard state: %v", err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): storage.BlockStateWithoutCells(shard),
		},
	}

	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{master}, current); err != nil {
		t.Fatalf("save checkpoint with existing shard state: %v", err)
	}
	loaded, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	loadedShard := loaded.Shards[storage.ShardKeyFromBlock(shard.Block)]
	if !loadedShard.Block.Equals(&shard.Block) {
		t.Fatalf("current shard was not loaded from existing metadata")
	}
}

func blockStateWithSingleCell(block ton.BlockIDExt, value uint64) *storage.BlockState {
	root := cell.BeginCell().MustStoreUInt(value, 8).EndCell()
	return blockStateWithRoot(block, root)
}

func blockStateWithRoot(block ton.BlockIDExt, root *cell.Cell) *storage.BlockState {
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
		Cell:          root,
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

func TestSaveStateCellTreeStoresPrunedRefAsRawCell(t *testing.T) {
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
	pruned := testPrunedBranch(t, hidden)
	root := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(pruned).EndCell()
	stats, err := store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      block,
		root:       root,
		totalCells: 2,
	})
	if err != nil {
		t.Fatalf("save state tree with raw pruned ref: %v", err)
	}
	if !stats.applied {
		t.Fatal("expected state cells to be persisted")
	}

	assertStateCellExists(t, store, root.HashKey(), true)
	assertStateCellExists(t, store, pruned.HashKey(), true)
	if hidden.HashKey(0) != pruned.HashKey() {
		assertStateCellExists(t, store, hidden.HashKey(0), false)
	}

	if err = store.flushCellDBs(initialCellGenerationID); err != nil {
		t.Fatalf("flush cells: %v", err)
	}
	loadedRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	loadedRef, err := loadedRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load raw pruned ref: %v", err)
	}
	if loadedRef.GetType() != cell.PrunedCellType {
		t.Fatalf("loaded ref type = %d, want pruned", loadedRef.GetType())
	}
	if loadedRef.HashKey() != pruned.HashKey() {
		t.Fatalf("loaded pruned hash mismatch: got=%x want=%x", loadedRef.HashKey(), pruned.HashKey())
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

	child := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	childHash := child.HashKey()
	root := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(child).EndCell()
	state := &storage.BlockState{
		Block: block,
		Cell:  root,
	}
	if err = store.SaveBlockState(ctx, state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	if state.Cell == root {
		t.Fatal("saved state still points to original root")
	}
	rootMeta := state.Cell.GetMetadata()
	if len(rootMeta.Refs) != 1 {
		t.Fatalf("unexpected root refs count %d", len(rootMeta.Refs))
	}
	if !rootMeta.Refs[0].Lazy {
		t.Fatal("expected saved root ref to become lazy boundary")
	}
	if rootMeta.Refs[0].Hash != childHash {
		t.Fatalf("boundary hash mismatch: got=%x want=%x", rootMeta.Refs[0].Hash, childHash)
	}
	loadedChild, err := state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load child through saved lazy root: %v", err)
	}
	if loadedChild.HashKey() != childHash {
		t.Fatalf("child hash mismatch: got=%x want=%x", loadedChild.HashKey(), childHash)
	}
}

func TestPreparedMerkleUpdateCellsMatchApplyMerkleUpdate(t *testing.T) {
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

	lazyFromRoot, err := store.ImportStateCellTree(ctx, block, fromRoot, 4)
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
		Cell:          directRoot,
	}
	preparedCellsByHash, err := storage.PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatalf("prepare state update cells: %v", err)
	}
	preparedCells := make([]storage.EncodedCellRecord, 0, len(preparedCellsByHash))
	for hash, data := range preparedCellsByHash {
		preparedCells = append(preparedCells, storage.EncodedCellRecord{
			Hash: hash,
			Data: data,
		})
	}
	nextCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: nextBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(nextState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpointWithCells(ctx, []*storage.BlockState{nextState}, nextCurrent, preparedCells); err != nil {
		t.Fatalf("save prepared state update target: %v", err)
	}

	loaded, err := store.LoadStateCellTree(ctx, nextState.Block, stateRootHash[:])
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
	lazyFromRoot, err := store.ImportStateCellTree(ctx, block, fromRoot, 4)
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
	directState := blockStateWithRoot(directBlock, update2To.Virtualize(0))
	if err = store.SaveBlockState(ctx, directState); err == nil {
		t.Fatal("direct proof-shaped state with unsaved pruned boundary was accepted")
	}

	nextBlock := block
	nextBlock.SeqNo = 3
	nextState := blockStateWithRoot(nextBlock, state2)
	if err = store.SaveBlockState(ctx, nextState); err != nil {
		t.Fatalf("save merged state: %v", err)
	}

	stateRootHash := state2.HashKey(0)
	loaded, err := store.LoadStateCellTree(ctx, nextState.Block, stateRootHash[:])
	if err != nil {
		t.Fatalf("load merged state root: %v", err)
	}
	assertResolvedCellTreeEqual(t, loaded, fullState2)
	assertStateCellExists(t, store, newBranch.HashKey(), true)
}

func TestSaveStateCheckpointDoesNotCommitProofShapedRoot(t *testing.T) {
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
	badRoot := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(pruned).EndCell().Virtualize(0)
	if !badRoot.IsVirtualized() {
		t.Fatal("test root must be proof-shaped")
	}
	badBlock := block
	badBlock.SeqNo = 2
	badState := blockStateWithRoot(badBlock, badRoot)
	badCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: badBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(badState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{badState}, badCurrent); err == nil {
		t.Fatal("checkpoint with proof-shaped root was accepted")
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
	state := blockStateWithRoot(block, root)
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

	loaded, err := store.LoadStateCellTree(ctx, state.Block, state.StateRootHash)
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

func TestSaveStateCheckpointWithPreparedCellsAllowsExternalBoundary(t *testing.T) {
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
	external := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(external).EndCell()
	nextBlock := block
	nextBlock.SeqNo = 2
	state := blockStateWithRoot(nextBlock, root)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: nextBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}

	records := []storage.EncodedCellRecord{mustEncodedCellRecord(t, root)}
	if err = store.SaveStateCheckpointWithCells(ctx, []*storage.BlockState{state}, current, records); err != nil {
		t.Fatalf("save checkpoint with external boundary: %v", err)
	}

	loaded, err := store.LoadStateCellTree(ctx, state.Block, state.StateRootHash)
	if err != nil {
		t.Fatalf("load saved state root: %v", err)
	}
	if loaded.HashKey() != root.HashKey() {
		t.Fatalf("loaded root mismatch: got=%x want=%x", loaded.HashKey(), root.HashKey())
	}
	loadedMeta := loaded.GetMetadata()
	if len(loadedMeta.Refs) != 1 {
		t.Fatalf("loaded root refs = %d, want 1", len(loadedMeta.Refs))
	}
	if loadedMeta.Refs[0].Hash != external.HashKey() {
		t.Fatalf("loaded external boundary mismatch: got=%x want=%x", loadedMeta.Refs[0].Hash, external.HashKey())
	}
}

func TestSwitchCellGenerationAtomicallyPublishesCandidateState(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	oldRoot := cell.BeginCell().MustStoreUInt(0x10, 8).EndCell()
	oldState := blockStateWithRoot(oldBlock, oldRoot)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if active.ID != initialCellGenerationID {
		t.Fatalf("active generation = %d, want %d", active.ID, initialCellGenerationID)
	}
	if !active.OriginPersistentState.Equals(&oldState.Block) {
		t.Fatalf("active origin = %s, want %s", storage.FormatBlockRef(active.OriginPersistentState), storage.FormatBlockRef(oldState.Block))
	}

	newBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := store.ImportStateCellTreeInGeneration(ctx, generation, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
	newState.CellGeneration = generation
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}

	historicalBlock := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     15,
	}
	historicalRoot := cell.BeginCell().MustStoreUInt(0x15, 8).EndCell()
	lazyHistoricalRoot, err := store.ImportStateCellTreeInGeneration(ctx, generation, historicalBlock, historicalRoot, 1)
	if err != nil {
		t.Fatalf("import candidate historical state: %v", err)
	}
	historicalState := blockStateWithRoot(historicalBlock, historicalRoot)
	historicalState.Cell = lazyHistoricalRoot
	historicalState.CellGeneration = generation

	durableNewState := blockStateWithRoot(newBlock, newRoot)
	durableHistoricalState := blockStateWithRoot(historicalBlock, historicalRoot)
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{durableHistoricalState, durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if oldGeneration != initialCellGenerationID {
		t.Fatalf("old generation = %d, want %d", oldGeneration, initialCellGenerationID)
	}

	active, err = store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation after switch: %v", err)
	}
	if active.ID != generation {
		t.Fatalf("active generation after switch = %d, want %d", active.ID, generation)
	}
	if !active.OriginPersistentState.Equals(&newState.Block) {
		t.Fatalf("active origin after switch = %s, want %s", storage.FormatBlockRef(active.OriginPersistentState), storage.FormatBlockRef(newState.Block))
	}

	loadedCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current after switch: %v", err)
	}
	if !loadedCurrent.Masterchain.Block.Equals(&newState.Block) {
		t.Fatalf("current master after switch = %s, want %s", storage.FormatBlockRef(loadedCurrent.Masterchain.Block), storage.FormatBlockRef(newState.Block))
	}
	loadedRoot, err := store.LoadStateCellTree(ctx, newState.Block, newState.StateRootHash)
	if err != nil {
		t.Fatalf("load candidate state after switch: %v", err)
	}
	if loadedRoot.HashKey() != newRoot.HashKey() {
		t.Fatalf("candidate root hash mismatch: got=%x want=%x", loadedRoot.HashKey(), newRoot.HashKey())
	}
	loadedHistoricalRoot, err := store.LoadStateCellTree(ctx, historicalState.Block, historicalState.StateRootHash)
	if err != nil {
		t.Fatalf("load candidate historical state after switch: %v", err)
	}
	if loadedHistoricalRoot.HashKey() != historicalRoot.HashKey() {
		t.Fatalf("historical root hash mismatch: got=%x want=%x", loadedHistoricalRoot.HashKey(), historicalRoot.HashKey())
	}

	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, oldState.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old state metadata after cleanup err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, historicalState.Block); err != nil {
		t.Fatalf("load candidate historical state meta after old cleanup: %v", err)
	}

	if _, err = store.CellRecordInGeneration(ctx, oldGeneration, oldState.StateCellHash); err == nil {
		t.Fatal("old generation cell still loads after delete")
	}
}

func TestPendingCellGenerationMigrationSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	oldBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	oldState := blockStateWithSingleCell(oldBlock, 0x10)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	origin := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x20}, 32),
		FileHash:  bytes.Repeat([]byte{0x21}, 32),
	}
	generation, err := store.BeginCellGeneration(ctx, origin)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	if generation <= initialCellGenerationID {
		t.Fatalf("candidate generation = %d, want above %d", generation, initialCellGenerationID)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	pending, err := store.PendingCellGenerationMigration(ctx)
	if err != nil {
		t.Fatalf("load pending migration: %v", err)
	}
	if pending.ID != generation {
		t.Fatalf("pending generation = %d, want %d", pending.ID, generation)
	}
	if !pending.OriginPersistentState.Equals(&origin) {
		t.Fatalf("pending origin = %s, want %s", storage.FormatBlockRef(pending.OriginPersistentState), storage.FormatBlockRef(origin))
	}

	reused, err := store.BeginCellGeneration(ctx, origin)
	if err != nil {
		t.Fatalf("reuse pending generation: %v", err)
	}
	if reused != generation {
		t.Fatalf("reused generation = %d, want %d", reused, generation)
	}

	root := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	if _, err = store.ImportStateCellTreeInGeneration(ctx, generation, origin, root, 1); err != nil {
		t.Fatalf("import into reopened pending generation: %v", err)
	}

	otherOrigin := origin
	otherOrigin.SeqNo++
	if _, err = store.BeginCellGeneration(ctx, otherOrigin); err == nil {
		t.Fatal("begin generation with different origin reused pending migration")
	}
}

func TestSwitchCellGenerationRejectsAdvancedCurrent(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	oldState := blockStateWithSingleCell(oldBlock, 0x10)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := store.ImportStateCellTreeInGeneration(ctx, generation, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
	newState.CellGeneration = generation
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, newRoot)
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}

	advancedBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     21,
	}
	advancedState := blockStateWithSingleCell(advancedBlock, 0x21)
	advancedCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: advancedBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(advancedState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{advancedState}, advancedCurrent); err != nil {
		t.Fatalf("save advanced checkpoint: %v", err)
	}

	if _, err = store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent); !errors.Is(err, storage.ErrCurrentStateAdvanced) {
		t.Fatalf("switch advanced current err=%v, want ErrCurrentStateAdvanced", err)
	}
	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if active.ID != initialCellGenerationID {
		t.Fatalf("active generation = %d, want %d", active.ID, initialCellGenerationID)
	}
}

func TestSwitchCellGenerationRejectsMissingCandidateCurrentCellsBeforeCleanup(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	oldState := blockStateWithSingleCell(oldBlock, 0x10)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	newState := blockStateWithSingleCell(newBlock, 0x20)
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newState.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{newState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}

	if _, err = store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent); err == nil {
		t.Fatal("switch succeeded without candidate current cells")
	}
	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if active.ID != initialCellGenerationID {
		t.Fatalf("active generation = %d, want %d", active.ID, initialCellGenerationID)
	}
	if _, err = store.blockStateMeta(ctx, oldState.Block); err != nil {
		t.Fatalf("old state metadata was deleted before rejected switch: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, newState.Block); err != nil {
		t.Fatalf("current state metadata was deleted before rejected switch: %v", err)
	}
}

func TestOpenReconcilesRetiredCellGenerationCleanup(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	oldBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	oldRoot := cell.BeginCell().MustStoreUInt(0x10, 8).EndCell()
	oldState := blockStateWithRoot(oldBlock, oldRoot)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := store.ImportStateCellTreeInGeneration(ctx, generation, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
	newState.CellGeneration = generation
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, newRoot)
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if oldGeneration != initialCellGenerationID {
		t.Fatalf("old generation = %d, want %d", oldGeneration, initialCellGenerationID)
	}
	if _, err = store.PendingCellGenerationMigration(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pending migration after switch err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, oldState.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old state metadata after switch err=%v, want not found", err)
	}
	assertCellGenerationShardDirs(t, dir, oldGeneration, true)

	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation after reopen: %v", err)
	}
	if active.ID != generation {
		t.Fatalf("active generation after reopen = %d, want %d", active.ID, generation)
	}
	if _, err = store.PendingCellGenerationMigration(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pending migration after reopen err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, oldState.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old state metadata after startup cleanup err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, newState.Block); err != nil {
		t.Fatalf("new state metadata after startup cleanup: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, newState.Block, newState.StateRootHash); err != nil {
		t.Fatalf("load new state after startup cleanup: %v", err)
	}
	assertCellGenerationShardDirs(t, dir, oldGeneration, false)
}

func TestActiveLazyRootSurvivesPreviousGenerationDeleteWhenCellExistsInNewGeneration(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	child := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(child).EndCell()
	oldState := blockStateWithRoot(oldBlock, root)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	oldLazyRoot, err := store.LoadStateCellTree(ctx, oldState.Block, oldState.StateRootHash)
	if err != nil {
		t.Fatalf("load old lazy root: %v", err)
	}

	newBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	lazyRoot, err := store.ImportStateCellTreeInGeneration(ctx, generation, newBlock, root, 2)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, root)
	newState.Cell = lazyRoot
	newState.CellGeneration = generation
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, root)
	if err = store.SaveStateCheckpoint(ctx, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}

	loadedChild, err := oldLazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("old active lazy root did not fall through to current generation: %v", err)
	}
	if loadedChild.HashKey() != child.HashKey() {
		t.Fatalf("loaded child hash mismatch: got=%x want=%x", loadedChild.HashKey(), child.HashKey())
	}
}

func assertCellGenerationShardDirs(tb testing.TB, dir string, generation uint64, wantExists bool) {
	tb.Helper()

	for shard := 0; shard < cellDBShardCount; shard++ {
		_, err := os.Stat(cellGenerationShardDir(dir, generation, shard))
		if wantExists {
			if err != nil {
				tb.Fatalf("cell generation %d shard %d dir missing: %v", generation, shard, err)
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			tb.Fatalf("cell generation %d shard %d dir exists or stat failed: %v", generation, shard, err)
		}
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
	state := blockStateWithRoot(block, root)
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

func TestSaveStateCellTreeStoresConcretePrunedParentByRawHash(t *testing.T) {
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
	pruned := testPrunedBranch(t, hidden)
	rawChild := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(pruned).EndCell()
	if rawChild.Level() == 0 {
		t.Fatal("test child must have a non-zero level")
	}
	root := cell.BeginCell().MustStoreRef(rawChild).EndCell()

	if _, err = store.saveStateCellTree(ctx, stateCellTreeSave{
		block:      block,
		root:       root,
		totalCells: 3,
	}); err != nil {
		t.Fatalf("save state tree with raw pruned descendant: %v", err)
	}

	assertStateCellExists(t, store, root.HashKey(), true)
	assertStateCellExists(t, store, rawChild.HashKey(), true)
	if logicalChild := rawChild.Virtualize(0); logicalChild.HashKey() != rawChild.HashKey() {
		assertStateCellExists(t, store, logicalChild.HashKey(), false)
	}
	assertStateCellExists(t, store, pruned.HashKey(), true)
	if hidden.HashKey(0) != pruned.HashKey() {
		assertStateCellExists(t, store, hidden.HashKey(0), false)
	}

	if err = store.flushCellDBs(initialCellGenerationID); err != nil {
		t.Fatalf("flush cells: %v", err)
	}
	loadedRoot, err := store.loadLazyCell(ctx, root.Hash())
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	loadedChild, err := loadedRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load raw child: %v", err)
	}
	if loadedChild.HashKey() != rawChild.HashKey() {
		t.Fatalf("loaded child hash mismatch: got=%x want=%x", loadedChild.HashKey(), rawChild.HashKey())
	}
	loadedChildSlice, err := loadedChild.BeginParse()
	if err != nil {
		t.Fatalf("materialize raw child: %v", err)
	}
	loadedChild = loadedChildSlice.BaseCell()
	loadedPruned, err := loadedChild.PeekRef(0)
	if err != nil {
		t.Fatalf("load raw pruned ref: %v", err)
	}
	if loadedPruned.HashKey() != pruned.HashKey() {
		t.Fatalf("loaded pruned hash mismatch: got=%x want=%x", loadedPruned.HashKey(), pruned.HashKey())
	}
}

func TestImportStateCellTreeStoresRawPrunedCell(t *testing.T) {
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
	pruned := testPrunedBranch(t, hidden)
	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(pruned).EndCell()
	lazyRoot, err := store.ImportStateCellTree(ctx, block, root, 2)
	if err != nil {
		t.Fatalf("import lazy state cells: %v", err)
	}

	assertStateCellExists(t, store, root.HashKey(), true)
	assertStateCellExists(t, store, pruned.HashKey(), true)
	if hidden.HashKey(0) != pruned.HashKey() {
		assertStateCellExists(t, store, hidden.HashKey(0), false)
	}

	loadedRef, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load raw pruned ref: %v", err)
	}
	if loadedRef.GetType() != cell.PrunedCellType {
		t.Fatalf("loaded ref type = %d, want pruned", loadedRef.GetType())
	}
	if loadedRef.HashKey() != pruned.HashKey() {
		t.Fatalf("loaded pruned hash mismatch: got=%x want=%x", loadedRef.HashKey(), pruned.HashKey())
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

func TestSaveStateCellTreeDoesNotValidateMissingLazyBoundary(t *testing.T) {
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
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), 3)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}
	lazyRight, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy right ref: %v", err)
	}
	lazyRightSlice, err := lazyRight.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy right ref: %v", err)
	}
	lazyRight = lazyRightSlice.BaseCell()
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
	if err = shard.db.Delete(rightHash[:], pebble.NoSync); err != nil {
		t.Fatalf("delete lazy cell: %v", err)
	}
	leafHash := lazyLeaf.Hash
	_, leafShard, err := store.cells.shardForHash(leafHash[:])
	if err != nil {
		t.Fatalf("route lazy leaf shard: %v", err)
	}
	if err = leafShard.db.Delete(leafHash[:], pebble.NoSync); err != nil {
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
	}); err != nil {
		t.Fatalf("save state with missing lazy boundary: %v", err)
	}
	if _, err = store.loadLazyCell(ctx, leafHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lazy boundary was unexpectedly restored: %v", err)
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
	lazyRoot, err := store.ImportStateCellTree(ctx, block, cell.BeginCell().MustStoreRef(oldRight).EndCell(), 3)
	if err != nil {
		t.Fatalf("import old state: %v", err)
	}
	lazyRight, err := lazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy right ref: %v", err)
	}
	lazyRightSlice, err := lazyRight.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy right ref: %v", err)
	}
	lazyRight = lazyRightSlice.BaseCell()
	if lazyRight.IsLazy() {
		t.Fatal("expected lazy data cell body")
	}

	rightHash := lazyRight.HashKey()
	_, shard, err := store.cells.shardForHash(rightHash[:])
	if err != nil {
		t.Fatalf("route lazy cell shard: %v", err)
	}
	if err = shard.db.Delete(rightHash[:], pebble.NoSync); err != nil {
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
	if _, err := store.ImportStateCellTree(ctx, block, root, 2); err != nil {
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

func TestImportStateCellTreeSkipsExistingLazyBoundaryWithoutLoading(t *testing.T) {
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
	if _, err := store.ImportStateCellTree(ctx, block, root, 2); err != nil {
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

	nextBlock := block
	nextBlock.SeqNo = 2
	if _, err = store.ImportStateCellTree(ctx, nextBlock, lazyRoot, 1); err != nil {
		t.Fatalf("import existing lazy root: %v", err)
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
