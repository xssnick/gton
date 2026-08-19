package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type lockedTestBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedTestBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedTestBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestCellRecordCodecStoresOrdinaryDescriptor(t *testing.T) {
	ordinary := cell.BeginCell().MustStoreUInt(0xbc, 8).EndCell()
	record, err := storage.CellRecordFromCell(ordinary)
	if err != nil {
		t.Fatalf("cell record from cell: %v", err)
	}
	encoded := storage.EncodeCellRecord(record)
	record, err = storage.DecodeCellRecord(record.Hash, encoded)
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

func TestSaveBlockMetaRejectsIncompleteBlockIDHashes(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	block := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 42}
	err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:      block,
		StartLT: 10,
		EndLT:   20,
	})
	if !errors.Is(err, storage.ErrInvalidBlockIDHashes) {
		t.Fatalf("save block meta error = %v, want invalid block id hashes", err)
	}
}

func TestSaveStateCheckpointEntriesRejectsIncompleteStateBlockIDHashes(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	block := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 43}
	_, err = store.SaveStateCheckpointEntries(context.Background(), []storage.StateCheckpointBlock{{
		State: &storage.BlockState{
			Block:         block,
			StateRootHash: bytes.Repeat([]byte{0x44}, 32),
		},
	}}, storage.StateCellRecords{}, nil)
	if !errors.Is(err, storage.ErrInvalidBlockIDHashes) {
		t.Fatalf("save checkpoint state error = %v, want invalid block id hashes", err)
	}
}

func TestSaveStateCheckpointEntriesRejectsIncompleteArtifactBlockIDHashes(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	block := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 44}
	_, err = store.SaveStateCheckpointEntries(context.Background(), []storage.StateCheckpointBlock{{
		Artifact: &storage.ServedBlockFull{
			ID:    block,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  &storage.BlockMeta{ID: block, StartLT: 10, EndLT: 20},
		},
	}}, storage.StateCellRecords{}, nil)
	if !errors.Is(err, storage.ErrInvalidBlockIDHashes) {
		t.Fatalf("save checkpoint artifact error = %v, want invalid block id hashes", err)
	}
}

func TestSaveStateCheckpointEntriesRejectsIncompleteCurrentBlockIDHashes(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     45,
		RootHash:  bytes.Repeat([]byte{0x45}, 32),
		FileHash:  bytes.Repeat([]byte{0x46}, 32),
	}
	current := &storage.CurrentState{
		SyncedAt: time.Now(),
		Masterchain: storage.BlockState{
			Block: ton.BlockIDExt{Workchain: block.Workchain, Shard: block.Shard, SeqNo: block.SeqNo},
		},
		Shards: map[storage.ShardKey]storage.BlockState{},
	}

	_, err = store.SaveStateCheckpointEntries(context.Background(), []storage.StateCheckpointBlock{{
		Artifact: &storage.ServedBlockFull{
			ID:    block,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 10, EndLT: 20},
		},
	}}, storage.StateCellRecords{}, current)
	if !errors.Is(err, storage.ErrInvalidBlockIDHashes) {
		t.Fatalf("save checkpoint current error = %v, want invalid block id hashes", err)
	}
}

func TestLargeBOCLoadRecords(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

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
	if err = saveActiveTestCells(store, records); err != nil {
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

	metaRecords, err := loadActiveTestLargeBOCMeta(store, context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	payloadRecords, err := loadActiveTestLargeBOCPayload(store, context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load payload: %v", err)
	}
	cellRecords, err := loadActiveTestLargeBOCCells(store, context.Background(), hashes, nil)
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
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

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
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	payloadRecords, err := loadActiveTestLargeBOCPayload(store, context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load payload: %v", err)
	}
	cellRecords, err := loadActiveTestLargeBOCCells(store, context.Background(), hashes, nil)
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

func TestDecodeBlockMetaFlagsMatchesFullDecode(t *testing.T) {
	makeBlock := func(seed byte, seqno uint32) ton.BlockIDExt {
		return ton.BlockIDExt{
			Workchain: 0,
			Shard:     topShard,
			SeqNo:     seqno,
			RootHash:  bytes.Repeat([]byte{seed}, 32),
			FileHash:  bytes.Repeat([]byte{seed ^ 0xff}, 32),
		}
	}
	block := makeBlock(0x42, 42)

	variants := []*storage.BlockMeta{
		{ID: block},
		{ID: block, GenUTime: 1000, StartLT: 2000, EndLT: 3000},
		{
			ID:            block,
			StateRootHash: bytes.Repeat([]byte{0x11}, 32),
			StateFileHash: bytes.Repeat([]byte{0x22}, 32),
			PrevRefs:      []ton.BlockIDExt{makeBlock(0x41, 41)},
		},
		{
			ID:                  block,
			MasterchainRefSeqno: 40,
			PrevRefs:            []ton.BlockIDExt{makeBlock(0x41, 41), makeBlock(0x40, 40)},
			NextRefs:            []ton.BlockIDExt{makeBlock(0x43, 43)},
		},
	}

	allFlags := storage.BlockMetaHasServedFull |
		storage.BlockMetaServedFullIsLink |
		storage.BlockMetaHasBlockData |
		storage.BlockMetaHasProofBlock |
		storage.BlockMetaHasProofBlockLink |
		storage.BlockMetaHasProofKeyBlock |
		storage.BlockMetaHasProofKeyBlockLink |
		storage.BlockMetaHasStateSnapshot |
		storage.BlockMetaIsKeyBlock |
		storage.BlockMetaHasStateCells
	for variantIdx, variant := range variants {
		for flags := storage.BlockMetaFlags(0); ; flags++ {
			meta := variant.Clone()
			meta.Flags = flags
			encoded := encodeBlockMeta(meta)

			got, err := decodeBlockMetaFlags(encoded)
			if err != nil {
				t.Fatalf("variant %d: decode flags %#x: %v", variantIdx, uint32(flags), err)
			}
			if got != flags {
				t.Fatalf("variant %d: decoded flags %#x, want %#x", variantIdx, uint32(got), uint32(flags))
			}
			decoded, err := decodeBlockMeta(meta.ID, encoded)
			if err != nil {
				t.Fatalf("variant %d: decode meta flags=%#x: %v", variantIdx, uint32(flags), err)
			}
			if decoded.Flags != got {
				t.Fatalf("variant %d: full decode flags %#x, flags decode %#x", variantIdx, uint32(decoded.Flags), uint32(got))
			}
			if flags == allFlags {
				break
			}
		}
	}

	// Undefined high bits must round-trip as raw uint32 payload.
	meta := variants[3].Clone()
	meta.Flags = storage.BlockMetaFlags(0x80000000) | storage.BlockMetaHasServedFull
	encoded := encodeBlockMeta(meta)
	if got, err := decodeBlockMetaFlags(encoded); err != nil || got != meta.Flags {
		t.Fatalf("high bit flags decode = %#x, %v, want %#x", uint32(got), err, uint32(meta.Flags))
	}

	// Header validation parity with decodeBlockMeta: payloads truncated below
	// the fixed header and unknown versions must fail in both decoders.
	for length := 0; length < blockMetaMinEncodedLen; length++ {
		if _, err := decodeBlockMeta(meta.ID, encoded[:length]); err == nil {
			t.Fatalf("full decode of %d-byte payload succeeded", length)
		}
		if _, err := decodeBlockMetaFlags(encoded[:length]); err == nil {
			t.Fatalf("flags decode of %d-byte payload succeeded", length)
		}
	}
	badVersion := bytes.Clone(encoded)
	badVersion[0] = blockMetaVersion + 1
	if _, err := decodeBlockMeta(meta.ID, badVersion); err == nil {
		t.Fatal("full decode of wrong version succeeded")
	}
	if _, err := decodeBlockMetaFlags(badVersion); err == nil {
		t.Fatal("flags decode of wrong version succeeded")
	}

	// Flags live in the fixed header: a payload with an intact header but a
	// corrupt tail still yields flags even though the full decode fails.
	truncatedTail := encoded[:blockMetaMinEncodedLen]
	if _, err := decodeBlockMeta(meta.ID, truncatedTail); err == nil {
		t.Fatal("full decode of truncated tail succeeded")
	}
	if got, err := decodeBlockMetaFlags(truncatedTail); err != nil || got != meta.Flags {
		t.Fatalf("truncated tail flags decode = %#x, %v, want %#x", uint32(got), err, uint32(meta.Flags))
	}
}

func TestOpenRejectsMetaDBWithoutVersionRecord(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "metadb")
	if err := os.MkdirAll(hotDir, 0o755); err != nil {
		t.Fatalf("create metadb dir: %v", err)
	}

	db, err := pebble.Open(hotDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open raw metadb: %v", err)
	}
	if err = db.Set(hotKeyStateSyncProgress(), []byte{0x01}, pebble.Sync); err != nil {
		t.Fatalf("write raw metadb record: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close raw metadb: %v", err)
	}

	store, err := Open(Options{Dir: dir})
	if err == nil {
		_ = store.Close()
		t.Fatal("opened non-empty metadb without version record")
	}
	if !strings.Contains(err.Error(), "version record is missing") {
		t.Fatalf("open error = %v, want missing version record", err)
	}
}

func TestOpenRejectsUnsupportedMetaDBVersion(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "metadb")
	if err := os.MkdirAll(hotDir, 0o755); err != nil {
		t.Fatalf("create metadb dir: %v", err)
	}

	db, err := pebble.Open(hotDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open raw metadb: %v", err)
	}
	if err = db.Set(hotKeyMetaDBVersion(), encodeMetaDBVersion(1), pebble.Sync); err != nil {
		t.Fatalf("write raw metadb version: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close raw metadb: %v", err)
	}

	store, err := Open(Options{Dir: dir})
	if err == nil {
		_ = store.Close()
		t.Fatal("opened metadb with older version")
	}
	if !strings.Contains(err.Error(), "unsupported metadb version") {
		t.Fatalf("open error = %v, want unsupported metadb version", err)
	}
}

func TestOpenMigratesMetaDBV2RebuildsHistoryIndexes(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	block := testMasterBlockID(42, 0x42)
	keyBlock := testMasterBlockID(50, 0x50)
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 100, EndLT: 200}); err != nil {
		t.Fatalf("save block meta: %v", err)
	}
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: keyBlock, Flags: storage.BlockMetaIsKeyBlock, GenUTime: 124, StartLT: 201, EndLT: 300}); err != nil {
		t.Fatalf("save key block meta: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	blockRef := storage.BlockSeqRefFromBlock(block)
	stale := testMasterBlockID(77, 0x77)
	staleRef := storage.BlockSeqRefFromBlock(stale)
	db, err := pebble.Open(filepath.Join(dir, "metadb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("open raw metadb: %v", err)
	}
	if err = db.Set(hotKeyMetaDBVersion(), encodeMetaDBVersion(2), pebble.Sync); err != nil {
		t.Fatalf("write v2 metadb version: %v", err)
	}
	if err = db.Delete(hotKeyBlockSeqIndex(blockRef), pebble.Sync); err != nil {
		t.Fatalf("delete seq index: %v", err)
	}
	if err = db.Set(hotKeyBlockLTIndex(blockRef, 200), []byte{}, pebble.Sync); err != nil {
		t.Fatalf("corrupt lt index: %v", err)
	}
	if err = db.Set(hotKeyBlockUTimeIndex(blockRef, 123), []byte{}, pebble.Sync); err != nil {
		t.Fatalf("corrupt utime index: %v", err)
	}
	if err = db.Set(hotKeyBlockSeqIndex(staleRef), encodeBlockIDHashes(stale), pebble.Sync); err != nil {
		t.Fatalf("write stale seq index: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close raw metadb: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	defer func() { _ = store.Close() }()

	assertHistoryIndexValue(t, store, hotKeyBlockSeqIndex(blockRef), block)
	assertHistoryIndexValue(t, store, hotKeyBlockLTIndex(blockRef, 200), block)
	assertHistoryIndexValue(t, store, hotKeyBlockUTimeIndex(blockRef, 123), block)
	assertHistoryIndexValue(t, store, hotKeyKeyBlockSeqIndex(keyBlock.SeqNo), keyBlock)
	if _, err = store.getHotCopy(context.Background(), hotKeyBlockSeqIndex(staleRef)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stale seq index lookup err = %v, want ErrNotFound", err)
	}
	assertStoreMetaDBVersion(t, store, metaDBVersion)
}

func TestOpenReadOnlyRejectsMetaDBV2Migration(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := pebble.Open(filepath.Join(dir, "metadb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("open raw metadb: %v", err)
	}
	if err = db.Set(hotKeyMetaDBVersion(), encodeMetaDBVersion(2), pebble.Sync); err != nil {
		t.Fatalf("write v2 metadb version: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close raw metadb: %v", err)
	}

	store, err = Open(Options{Dir: dir, ReadOnly: true})
	if err == nil {
		_ = store.Close()
		t.Fatal("opened read-only metadb requiring migration")
	}
	if !strings.Contains(err.Error(), "requires writable migration") {
		t.Fatalf("open error = %v, want writable migration error", err)
	}
}

func assertHistoryIndexValue(t *testing.T, store *Store, key []byte, block ton.BlockIDExt) {
	t.Helper()

	raw, err := store.getHotCopy(context.Background(), key)
	if err != nil {
		t.Fatalf("load history index %x: %v", key, err)
	}
	indexed, err := decodeBlockIDFromHashes(block.Workchain, block.Shard, block.SeqNo, raw)
	if err != nil {
		t.Fatalf("decode history index %x: %v", key, err)
	}
	if !indexed.Equals(&block) {
		t.Fatalf("history index %x = %s, want %s", key, storage.FormatBlockRef(indexed), storage.FormatBlockRef(block))
	}
}

func assertStoreMetaDBVersion(t *testing.T, store *Store, want uint32) {
	t.Helper()

	raw, err := store.getHotCopy(context.Background(), hotKeyMetaDBVersion())
	if err != nil {
		t.Fatalf("load metadb version: %v", err)
	}
	version, err := decodeMetaDBVersion(raw)
	if err != nil {
		t.Fatalf("decode metadb version: %v", err)
	}
	if version != want {
		t.Fatalf("metadb version = %d, want %d", version, want)
	}
}

func TestOpenInitializesMetaDBVersion(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := pebble.Open(filepath.Join(dir, "metadb"), &pebble.Options{})
	if err != nil {
		t.Fatalf("open raw metadb: %v", err)
	}
	raw, closer, err := pebbleReaderGet(db, hotKeyMetaDBVersion())
	if err != nil {
		t.Fatalf("load metadb version: %v", err)
	}
	version, err := decodeMetaDBVersion(raw)
	if closeErr := closer.Close(); closeErr != nil {
		t.Fatalf("close version record: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("decode metadb version: %v", err)
	}
	if version != metaDBVersion {
		t.Fatalf("metadb version = %d, want %d", version, metaDBVersion)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close raw metadb: %v", err)
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
	if err = db.Set(hotKeyMetaDBVersion(), encodeMetaDBVersion(metaDBVersion), pebble.Sync); err != nil {
		t.Fatalf("write raw metadb version: %v", err)
	}
	if err = db.Set(hotKeyStateSyncProgress(), []byte{0x01}, pebble.Sync); err != nil {
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

	encoded := storage.EncodeCellRecord(record)
	const compactRefsFlag = 0x10

	if encoded[0]&compactRefsFlag == 0 {
		t.Fatalf("compact refs flag is not set")
	}
	if got, want := encoded[0]&^compactRefsFlag, record.D1; got != want {
		t.Fatalf("stored descriptor mismatch after clearing compact flag: got=%d want=%d", got, want)
	}

	bodyLen := int(record.D2/2 + record.D2%2)
	layoutPos := 2 + bodyLen
	if got := encoded[layoutPos]; got != 0 {
		t.Fatalf("common refs slow mask mismatch: got=%d want=0", got)
	}
	if got, want := len(encoded), 2+bodyLen+1+2*(32+2); got != want {
		t.Fatalf("compact encoded length mismatch: got=%d want=%d", got, want)
	}

	decoded, err := storage.DecodeCellRecord(record.Hash, encoded)
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

	encoded := storage.EncodeCellRecord(record)
	record, err = storage.DecodeCellRecord(record.Hash, encoded)
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

	encoder, err := storage.PrepareCellRecordEncoder(root, refs)
	if err != nil {
		t.Fatalf("prepare direct state encoder: %v", err)
	}
	got := make([]byte, encoder.EncodedLen())
	encoder.EncodeCellTo(got, root, refs)

	record, err := storage.DecodeCellRecord(rootMeta.Hash[:], got)
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

func TestStateCellImportsPreserveReleasedCellRecordBytes(t *testing.T) {
	golden, err := hex.DecodeString(
		"1200008d9fe7317f066deaca4fdb6c313194e5bb5d2269ecf672f1af9fc790a2205991" +
			"000065fde13cf1e4ea4206c293082657037684ee456e40041c816509b63e1b89d3870000",
	)
	if err != nil {
		t.Fatalf("decode golden bytes: %v", err)
	}

	left := cell.BeginCell().MustStoreUInt(0x01, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0x02, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(left).MustStoreRef(right).EndCell()
	hash := root.HashKey()

	encoded, err := storage.PrepareEncodedCellRecordFromCellMetadata(root, root.GetMetadata())
	if err != nil {
		t.Fatalf("prepare encoded record: %v", err)
	}
	if !bytes.Equal(encoded.Data, golden) {
		t.Fatalf("prepared encoded bytes mismatch:\ngot:  %x\nwant: %x", encoded.Data, golden)
	}

	ctx := context.Background()
	block := testMasterBlockID(777, 0x77)

	t.Run("dfs", func(t *testing.T) {
		store, err := Open(Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer func() { _ = store.Close() }()
		if _, err := store.ImportStateCellTree(ctx, block, root, 1); err != nil {
			t.Fatalf("import state cell tree: %v", err)
		}

		raw := loadActiveTestCellRecordBytes(t, store, hash)
		if !bytes.Equal(raw, golden) {
			t.Fatalf("stored dfs bytes mismatch:\ngot:  %x\nwant: %x", raw, golden)
		}
	})

	t.Run("boc-view", func(t *testing.T) {
		store, err := Open(Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer func() { _ = store.Close() }()
		if _, err := store.ImportStateBOCView(ctx, block, testStateBOCView(t, root, nil)); err != nil {
			t.Fatalf("import state boc view: %v", err)
		}

		raw := loadActiveTestCellRecordBytes(t, store, hash)
		if !bytes.Equal(raw, golden) {
			t.Fatalf("stored boc bytes mismatch:\ngot:  %x\nwant: %x", raw, golden)
		}
	})
}

func TestSaveStateCellTreeDeduplicatesSharedRefs(t *testing.T) {
	var logOut lockedTestBuffer
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

	if _, err = saveActiveTestStateCellTree(context.Background(), store, stateCellTreeSave{
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

	if _, err = store.ImportStateCellTree(ctx, block, root, 1); err != nil {
		t.Fatalf("import state cell tree: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, block, rootHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load state before metadata = %v, want ErrNotFound", err)
	}

	if err = saveTestBlockState(ctx, store, &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
	}); err != nil {
		t.Fatalf("save block state metadata: %v", err)
	}

	if _, err = store.LoadStateCellTree(ctx, block, rootHash[:]); err != nil {
		t.Fatalf("load state by state meta: %v", err)
	}
}

func TestStateCheckpointEntriesPersistsOneDurableState(t *testing.T) {
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

	state := &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		Cell:          root,
	}
	current := &storage.CurrentState{
		SyncedAt:    time.Now(),
		Masterchain: *state,
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}

	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{state}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	meta, err := store.blockStateMeta(ctx, block)
	if err != nil {
		t.Fatalf("load block state meta: %v", err)
	}
	if !bytes.Equal(meta.StateRootHash, rootHash[:]) {
		t.Fatalf("state root hash mismatch: got=%x want=%x", meta.StateRootHash, rootHash)
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

func TestStateMetadataPublishRejectsNonLevelZeroStateRoot(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	hidden := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	pruned := testPrunedBranch(t, hidden)
	root := cell.BeginCell().MustStoreUInt(0xbb, 8).MustStoreRef(pruned).EndCell()
	if root.HashKey() == root.HashKey(0) {
		t.Fatal("test root should not be level-0")
	}

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     4,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	rootHash := root.HashKey(0)
	err = saveTestBlockState(context.Background(), store, &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		Cell:          root,
	})
	if err == nil || !strings.Contains(err.Error(), "not a level-0 state root") {
		t.Fatalf("save block state error = %v, want level-0 root rejection", err)
	}
}

func TestStateCheckpointEntriesPublishesBlockArtifactsAfterPackSync(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	prev := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{0x10}, 32),
		FileHash:  bytes.Repeat([]byte{0x11}, 32),
	}
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     11,
		RootHash:  bytes.Repeat([]byte{0x12}, 32),
		FileHash:  bytes.Repeat([]byte{0x13}, 32),
	}
	root := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	rootHash := root.HashKey(0)
	state := &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		Cell:          root,
	}
	current := &storage.CurrentState{
		SyncedAt:    time.Now(),
		Masterchain: *state,
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}
	blockData := []byte{0x30, 0x31, 0x32}
	proofData := []byte{0x40, 0x41, 0x42}

	_, err = store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		State: state,
		Artifact: &storage.ServedBlockFull{
			ID:    block,
			Block: blockData,
			Proof: proofData,
			Meta: &storage.BlockMeta{
				ID:       block,
				GenUTime: 123,
				StartLT:  10,
				EndLT:    20,
				PrevRefs: []ton.BlockIDExt{prev},
			},
		},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: block}},
	}, {
		Artifact: &storage.ServedBlockFull{
			ID:    prev,
			Block: []byte{0x20},
			Proof: []byte{0x21},
			Meta:  &storage.BlockMeta{ID: prev, GenUTime: prev.SeqNo},
		},
	}}, storage.StateCellRecords{}, current)
	if err != nil {
		t.Fatalf("save checkpoint with artifacts: %v", err)
	}

	gotBlock, err := store.BlockData(ctx, block)
	if err != nil {
		t.Fatalf("load checkpoint block data: %v", err)
	}
	if !bytes.Equal(gotBlock, blockData) {
		t.Fatalf("checkpoint block data = %x, want %x", gotBlock, blockData)
	}
	gotProof, err := store.BlockProof(ctx, storage.ServedProofBlock, block)
	if err != nil {
		t.Fatalf("load checkpoint block proof: %v", err)
	}
	if !bytes.Equal(gotProof, proofData) {
		t.Fatalf("checkpoint block proof = %x, want %x", gotProof, proofData)
	}

	raw, err := store.getHotCopy(ctx, hotKeyBlockDataRef(block))
	if err != nil {
		t.Fatalf("load block data ref: %v", err)
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		t.Fatalf("decode block data ref: %v", err)
	}
	refPath, err := store.artifactRefPath(ctx, ref)
	if err != nil {
		t.Fatalf("resolve block data ref path: %v", err)
	}
	stat, err := os.Stat(store.artifactPath(refPath))
	if err != nil {
		t.Fatalf("stat checkpoint artifact pack: %v", err)
	}
	if stat.Size() < ref.Offset+ref.Size {
		t.Fatalf("checkpoint artifact pack size = %d, want at least ref end %d", stat.Size(), ref.Offset+ref.Size)
	}
	meta, err := store.BlockMeta(ctx, block)
	if err != nil {
		t.Fatalf("load checkpoint block meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaHasServedFull) {
		t.Fatalf("checkpoint block meta flags = %v, want served full", meta.Flags)
	}
	if got, err := store.LookupBlockBySeqNo(ctx, storage.BlockSeqRefFromBlock(block)); err != nil || !got.Equals(&block) {
		t.Fatalf("checkpoint lookup by seqno failed: err=%v got=%s", err, storage.FormatBlockRef(got))
	}
	if got, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: block.Workchain, Shard: block.Shard}, 19); err != nil || !got.Equals(&block) {
		t.Fatalf("checkpoint lookup by lt failed: err=%v got=%s", err, storage.FormatBlockRef(got))
	}
	if got, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: block.Workchain, Shard: block.Shard}, 123); err != nil || !got.Equals(&block) {
		t.Fatalf("checkpoint lookup by utime failed: err=%v got=%s", err, storage.FormatBlockRef(got))
	}

	decodedNext, err := readNextBlockLink(ctx, store, prev)
	if err != nil {
		t.Fatalf("load checkpoint next link: %v", err)
	}
	if !decodedNext.Equals(&block) {
		t.Fatalf("checkpoint next link = %s, want %s", storage.FormatBlockRef(decodedNext), storage.FormatBlockRef(block))
	}

	next, err := store.NextBlockFull(ctx, prev)
	if err != nil {
		t.Fatalf("load checkpoint next block: %v", err)
	}
	if !next.ID.Equals(&block) {
		t.Fatalf("checkpoint next block = %s, want %s", storage.FormatBlockRef(next.ID), storage.FormatBlockRef(block))
	}
}

func TestStateCheckpointEntriesRejectsNonZeroStateWithPartialArtifact(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     12,
	}
	state := blockStateWithSingleCell(block, 0x23)
	current := &storage.CurrentState{
		SyncedAt:    time.Now(),
		Masterchain: storage.BlockStateWithoutCells(state),
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}

	_, err = store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		State: state,
		Artifact: &storage.ServedBlockFull{
			ID:    state.Block,
			Block: []byte{0x30, 0x31},
			Meta:  &storage.BlockMeta{ID: state.Block, GenUTime: 123},
		},
	}}, storage.StateCellRecords{}, current)
	if err == nil || !strings.Contains(err.Error(), "artifact has no proof") {
		t.Fatalf("partial checkpoint error = %v, want missing proof", err)
	}
}

func TestStateCheckpointEntriesRejectsNonZeroStateWithoutArtifact(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	state := blockStateWithSingleCell(ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     12,
	}, 0x24)

	_, err = store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		State: state,
	}}, storage.StateCellRecords{}, nil)
	if err == nil || !strings.Contains(err.Error(), "has no full block artifact") {
		t.Fatalf("state-only checkpoint error = %v, want missing artifact", err)
	}
}

func TestStateCheckpointEntriesStoresShardInclusionMasterRef(t *testing.T) {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{master, shard}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	meta, err := store.BlockMeta(ctx, shard.Block)
	if err != nil {
		t.Fatalf("load shard meta: %v", err)
	}
	if meta.MasterchainRefSeqno != master.Block.SeqNo {
		t.Fatalf("masterchain ref seqno = %d, want %d", meta.MasterchainRefSeqno, master.Block.SeqNo)
	}
}

func TestStateCheckpointEntriesOverwritesHeaderMasterRefWithInclusionMaster(t *testing.T) {
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
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: shard.Block, MasterchainRefSeqno: oldMaster.SeqNo}); err != nil {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{master, shard}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	meta, err := store.BlockMeta(ctx, shard.Block)
	if err != nil {
		t.Fatalf("load shard meta: %v", err)
	}
	// This mirrors cppnode's BlockHandle::masterchain_ref_block behavior. The
	// authoritative value is the masterchain block that includes/applies the
	// shard block, so a stale header-derived ref must not survive the checkpoint.
	if meta.MasterchainRefSeqno != master.Block.SeqNo {
		t.Fatalf("masterchain ref seqno = %d, want %d", meta.MasterchainRefSeqno, master.Block.SeqNo)
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
	if err = saveTestBlockState(ctx, store, masterState); err != nil {
		t.Fatalf("save master state: %v", err)
	}
	if err = saveTestBlockState(ctx, store, shardState); err != nil {
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
	block11 := testMasterBlockID(11, 11)
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: block11, GenUTime: 11, StartLT: 11, EndLT: 12}); err != nil {
		t.Fatalf("save block meta: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if _, err = store.BlockMeta(ctx, block11); err != nil {
		t.Fatalf("load block meta in read-only store: %v", err)
	}
	block12 := testMasterBlockID(12, 12)
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: block12, GenUTime: 12, StartLT: 12, EndLT: 13}); !errors.Is(err, pebble.ErrReadOnly) {
		t.Fatalf("save in read-only store = %v, want pebble.ErrReadOnly", err)
	}
}

func TestSaveBlockMetaRejectsEmptyMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testMasterBlockID(20, 20)
	err = store.SaveBlockMeta(&storage.BlockMeta{ID: block})
	if err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("save empty block meta err = %v, want empty meta error", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty block meta lookup err = %v, want ErrNotFound", err)
	}
}

func TestSaveBlockMetaRejectsDirectNextRefs(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testMasterBlockID(20, 20)
	next := testMasterBlockID(21, 21)
	err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:       prev,
		GenUTime: 20,
		NextRefs: []ton.BlockIDExt{next},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot set next refs directly") {
		t.Fatalf("save direct next refs err = %v, want direct next refs error", err)
	}
	if _, err = store.BlockMeta(context.Background(), prev); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("direct next refs meta lookup err = %v, want ErrNotFound", err)
	}
}

func TestSaveBlockMetaRejectsDirectArtifactFlags(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testMasterBlockID(20, 20)
	err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:       block,
		Flags:    storage.BlockMetaHasServedFull,
		GenUTime: 20,
	})
	if err == nil || !strings.Contains(err.Error(), "cannot set artifact flags directly") {
		t.Fatalf("save direct artifact flags err = %v, want direct artifact flags error", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("direct artifact flags meta lookup err = %v, want ErrNotFound", err)
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

func TestStateCheckpointEntriesPersistsAllAppliedStates(t *testing.T) {
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
	if err = saveTestStateCheckpoint(ctx, store, states, current); err != nil {
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

func TestStateCheckpointEntriesRejectsCurrentShardWithoutStateMeta(t *testing.T) {
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

	err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{master}, current)
	if err == nil || !strings.Contains(err.Error(), "current shard state") || !strings.Contains(err.Error(), "metadata is missing") {
		t.Fatalf("save checkpoint error = %v, want missing current shard metadata", err)
	}
	if _, err = store.CurrentState(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current after failed checkpoint = %v, want ErrNotFound", err)
	}
}

func TestStateCheckpointEntriesAllowsExistingCurrentShardStateMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x20)
	if err = saveTestBlockState(ctx, store, shard); err != nil {
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

	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{master}, current); err != nil {
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

func TestStateCheckpointEntriesUpdatesReusedCurrentShardMasterRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldMaster := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	newMaster := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 11}, 0x11)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x20)
	shard.MasterchainRef = &oldMaster.Block
	if err = saveTestBlockState(ctx, store, oldMaster); err != nil {
		t.Fatalf("save old master state: %v", err)
	}
	if err = saveTestBlockState(ctx, store, shard); err != nil {
		t.Fatalf("save existing shard state: %v", err)
	}

	currentShard := storage.BlockStateWithoutCells(shard)
	currentShard.MasterchainRef = &newMaster.Block
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newMaster.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newMaster),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): currentShard,
		},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{newMaster}, current); err != nil {
		t.Fatalf("save checkpoint with reused shard state: %v", err)
	}

	meta, err := store.BlockMeta(ctx, shard.Block)
	if err != nil {
		t.Fatalf("load shard meta: %v", err)
	}
	if meta.MasterchainRefSeqno != newMaster.Block.SeqNo {
		t.Fatalf("shard master ref seqno = %d, want %d", meta.MasterchainRefSeqno, newMaster.Block.SeqNo)
	}
	loaded, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	loadedShard := loaded.Shards[storage.ShardKeyFromBlock(shard.Block)]
	if loadedShard.MasterchainRef == nil || !loadedShard.MasterchainRef.Equals(&newMaster.Block) {
		t.Fatalf("loaded shard master ref = %+v, want %s", loadedShard.MasterchainRef, storage.FormatBlockRef(newMaster.Block))
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
		Cell:          root,
	}
}

func loadActiveTestCellRecordBytes(tb testing.TB, store *Store, hash cell.Hash) []byte {
	tb.Helper()

	cells, err := store.acquireActiveCellStore(context.Background())
	if err != nil {
		tb.Fatalf("acquire active cell store: %v", err)
	}
	defer cells.release()

	raw, err := cells.getCopy(hash[:])
	if err != nil {
		tb.Fatalf("load raw cell record: %v", err)
	}
	return raw
}

func testMasterStateHeaderRoot(block ton.BlockIDExt, outMsgQueueInfo *cell.Cell) (*cell.Cell, *cell.Cell) {
	accounts := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	stats := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	masterExtra := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0x9023afe2, 32).
		MustStoreInt(-239, 32).
		MustStoreUInt(0, 2).
		MustStoreUInt(0, 6).
		MustStoreInt(int64(block.Workchain), 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(uint64(block.SeqNo), 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 32).
		MustStoreRef(outMsgQueueInfo).
		MustStoreBoolBit(false).
		MustStoreRef(accounts).
		MustStoreRef(stats).
		MustStoreBoolBit(true).
		MustStoreRef(masterExtra).
		EndCell()
	return root, masterExtra
}

func saveCellGenerationSwitchProgress(tb testing.TB, store *Store, ctx context.Context, generation uint64, current *storage.CurrentState) {
	tb.Helper()

	if err := store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
		tb.Fatalf("save cell generation switch progress: %v", err)
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
		Data: storage.EncodeCellRecord(record),
	}
}

func mustReachableEncodedRecords(tb testing.TB, root *cell.Cell) []storage.EncodedCellRecord {
	tb.Helper()

	records, err := storage.PrepareReachableStateCells(root)
	if err != nil {
		tb.Fatalf("prepare reachable state cells: %v", err)
	}
	return records.Records
}

func checkpointEntries(state *storage.BlockState) []storage.StateCheckpointBlock {
	return []storage.StateCheckpointBlock{testStateCheckpointEntry(state)}
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
	stats, err := saveActiveTestStateCellTree(ctx, store, stateCellTreeSave{
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
	loadedRoot, err := store.LoadCell(ctx, root.Hash())
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

func TestStateMetadataPublishSwitchesStateToLazyRoot(t *testing.T) {
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
	state := blockStateWithRoot(block, root)
	if err = saveTestBlockState(ctx, store, state); err != nil {
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
	applied, err := cell.ApplyMerkleUpdate(lazyFromRoot.Virtualize(0), update)
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
	nextBlock.RootHash = bytes.Repeat([]byte{0x21}, 32)
	nextBlock.FileHash = bytes.Repeat([]byte{0x22}, 32)
	nextState := &storage.BlockState{
		Block:         nextBlock,
		StateRootHash: stateRootHash[:],
		Cell:          directRoot,
	}
	preparedCells, err := storage.PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatalf("prepare state update cells: %v", err)
	}
	nextCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: nextBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(nextState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if _, err = store.SaveStateCheckpointEntries(ctx, checkpointEntries(nextState), preparedCells, nextCurrent); err != nil {
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
	state1, err := cell.ApplyMerkleUpdate(lazyFromRoot.Virtualize(0), update1)
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
	state2, err := cell.ApplyMerkleUpdate(state1, update2)
	if err != nil {
		t.Fatalf("apply second update: %v", err)
	}
	assertResolvedCellTreeEqual(t, state2, fullState2)

	directBlock := block
	directBlock.SeqNo = 2
	directState := blockStateWithRoot(directBlock, update2To.Virtualize(0))
	if err = saveTestBlockState(ctx, store, directState); err == nil {
		t.Fatal("direct proof-shaped state with unsaved pruned boundary was accepted")
	}

	nextBlock := block
	nextBlock.SeqNo = 3
	nextState := blockStateWithRoot(nextBlock, state2)
	if err = saveTestBlockState(ctx, store, nextState); err != nil {
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

func TestStateCheckpointEntriesDoesNotCommitProofShapedRoot(t *testing.T) {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{initial}, initialCurrent); err != nil {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{badState}, badCurrent); err == nil {
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

func TestStateCheckpointEntriesWithPreparedCellsLoadsLazyRoot(t *testing.T) {
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
	if _, err = store.SaveStateCheckpointEntries(ctx, checkpointEntries(state), storage.NewStateCellRecords(records), current); err != nil {
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

func TestStateCheckpointEntriesPreparedParsedStateSurvivesPreviousGenerationDelete(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldBlock := testMasterBlockID(10, 0x10)
	payload := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	root, masterExtra := testMasterStateHeaderRoot(oldBlock, payload)
	state := blockStateWithRoot(oldBlock, root)
	state.Parsed = &tlb.ShardStateUnsplit{McStateExtra: masterExtra}
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	prepared := storage.NewStateCellRecords(mustReachableEncodedRecords(t, root))
	if _, err = store.SaveStateCheckpointEntries(ctx, checkpointEntries(state), prepared, oldCurrent); err != nil {
		t.Fatalf("save checkpoint with prepared cells: %v", err)
	}
	if state.Parsed == nil || state.Parsed.OutMsgQueueInfo == nil {
		t.Fatal("checkpoint did not replace parsed state with persisted lazy refs")
	}
	lazyPayload := state.Parsed.OutMsgQueueInfo

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, root, 5)
	if err != nil {
		t.Fatalf("copy state into candidate generation: %v", err)
	}
	newState := blockStateWithRoot(newBlock, root)
	newState.Cell = lazyRoot
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, root)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}

	payloadSlice, err := lazyPayload.BeginParse()
	if err != nil {
		t.Fatalf("materialize checkpoint parsed ref from current generation: %v", err)
	}
	value, err := payloadSlice.LoadUInt(8)
	if err != nil {
		t.Fatalf("load checkpoint parsed ref payload: %v", err)
	}
	if value != 0x51 {
		t.Fatalf("checkpoint parsed ref payload = %#x, want %#x", value, uint64(0x51))
	}
}

func TestStateCellPrewriteAllowsCheckpointWithoutCellBatch(t *testing.T) {
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
	child := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x32, 8).MustStoreRef(child).EndCell()
	state := blockStateWithRoot(block, root)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}

	records := storage.NewStateCellRecords([]storage.EncodedCellRecord{
		mustEncodedCellRecord(t, root),
		mustEncodedCellRecord(t, child),
	})
	if err = store.SaveStateCellRecords(ctx, records); err != nil {
		t.Fatalf("prewrite state cells: %v", err)
	}
	if err = store.FlushStateCells(ctx); err != nil {
		t.Fatalf("flush prewritten cells: %v", err)
	}

	rootHash := root.HashKey()
	_, err = store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load prewritten lazy root: %v", err)
	}
	checkpointState := storage.CloneBlockState(state)
	checkpointState.Cell = nil

	timing, err := store.SaveStateCheckpointEntries(ctx, checkpointEntries(checkpointState), storage.StateCellRecords{}, current)
	if err != nil {
		t.Fatalf("save checkpoint metadata for prewritten cells: %v", err)
	}
	if timing.CellsWrite != 0 {
		t.Fatalf("checkpoint cells write duration = %s, want 0 after prewrite", timing.CellsWrite)
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

func TestStateCheckpointEntriesWithPreparedCellsAllowsExternalBoundary(t *testing.T) {
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
	if _, err = store.SaveStateCheckpointEntries(ctx, checkpointEntries(state), storage.NewStateCellRecords(records), current); err != nil {
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

func TestStateCheckpointAllowsOrphanCellsWithoutMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block1 := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}
	root1 := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	state1 := blockStateWithRoot(block1, root1)
	current1 := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block1.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state1),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}

	block2 := block1
	block2.SeqNo = 2
	child2 := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	root2 := cell.BeginCell().MustStoreUInt(0x33, 8).MustStoreRef(child2).EndCell()
	firstCheckpointCells := append(mustReachableEncodedRecords(t, root1), mustReachableEncodedRecords(t, root2)...)
	if _, err = store.SaveStateCheckpointEntries(ctx, checkpointEntries(state1), storage.NewStateCellRecords(firstCheckpointCells), current1); err != nil {
		t.Fatalf("save first checkpoint: %v", err)
	}
	root2Hash := root2.HashKey(0)
	if _, err = store.LoadCell(ctx, root2Hash[:]); err != nil {
		t.Fatalf("orphan root should be allowed in celldb: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, block2); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("orphan cells committed metadata: %v", err)
	}
	loadedCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current after checkpoint: %v", err)
	}
	if !loadedCurrent.Masterchain.Block.Equals(&state1.Block) {
		t.Fatalf("current advanced unexpectedly: got=%s want=%s", storage.FormatBlockRef(loadedCurrent.Masterchain.Block), storage.FormatBlockRef(state1.Block))
	}
}

func TestStateCheckpointEntriesInitializesActiveOriginOnFirstCurrentState(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	state := blockStateWithRoot(block, cell.BeginCell().MustStoreUInt(0x10, 8).EndCell())
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{state}, current); err != nil {
		t.Fatalf("save first current checkpoint: %v", err)
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if !active.OriginPersistentState.Equals(&state.Block) {
		t.Fatalf("active origin = %s, want %s", storage.FormatBlockRef(active.OriginPersistentState), storage.FormatBlockRef(state.Block))
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	active, err = store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation after reopen: %v", err)
	}
	if !active.OriginPersistentState.Equals(&state.Block) {
		t.Fatalf("active origin after reopen = %s, want %s", storage.FormatBlockRef(active.OriginPersistentState), storage.FormatBlockRef(state.Block))
	}
}

func TestStateCheckpointEntriesDoesNotOverwriteActiveOriginWhenCurrentAlreadyExists(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	firstBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
	}
	firstState := blockStateWithRoot(firstBlock, cell.BeginCell().MustStoreUInt(0x10, 8).EndCell())
	firstCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: firstBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(firstState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{firstState}, firstCurrent); err != nil {
		t.Fatalf("save first current checkpoint: %v", err)
	}
	clearActiveCellOriginForTest(t, store)

	nextBlock := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	nextState := blockStateWithRoot(nextBlock, cell.BeginCell().MustStoreUInt(0x20, 8).EndCell())
	nextCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: nextBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(nextState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{nextState}, nextCurrent); err != nil {
		t.Fatalf("save next current checkpoint: %v", err)
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if !isEmptyBlockID(active.OriginPersistentState) {
		t.Fatalf("active origin = %s, want empty", storage.FormatBlockRef(active.OriginPersistentState))
	}
}

func clearActiveCellOriginForTest(t *testing.T, store *Store) {
	t.Helper()

	ctx := context.Background()
	db, err := store.acquireHotDB(ctx)
	if err != nil {
		t.Fatalf("acquire hot db: %v", err)
	}
	defer store.releaseHotDB()

	store.hotWriteMu.Lock()
	defer store.hotWriteMu.Unlock()

	store.mu.Lock()
	manifest := store.manifestLocked()
	manifest.activeOrigin = ton.BlockIDExt{}
	store.activeCellOrigin = ton.BlockIDExt{}
	store.mu.Unlock()

	if err = db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		t.Fatalf("clear active origin manifest: %v", err)
	}
}

func TestSwitchCellGenerationAtomicallySwitchesActiveGeneration(t *testing.T) {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
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

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
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
	lazyHistoricalRoot, err := generationCells.ImportStateCellTree(ctx, historicalBlock, historicalRoot, 1)
	if err != nil {
		t.Fatalf("import candidate historical state: %v", err)
	}
	historicalState := blockStateWithRoot(historicalBlock, historicalRoot)
	historicalState.Cell = lazyHistoricalRoot

	durableNewState := blockStateWithRoot(newBlock, newRoot)
	durableHistoricalState := blockStateWithRoot(historicalBlock, historicalRoot)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableHistoricalState, durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	if _, err = store.DeleteStateMetadataBeforeCellGenerationSwitch(ctx, newState.Block, newCurrent, nil); err != nil {
		t.Fatalf("delete state metadata before switch: %v", err)
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

	if _, err = store.Cells(oldGeneration); err == nil {
		t.Fatal("old generation cell still loads after delete")
	}
}

func TestDeleteStateMetadataBeforeCellGenerationSwitchDeletesStateMetadataBeforeImportedState(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldMaster := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	oldShard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x11)
	oldShard.MasterchainRef = &oldMaster.Block
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldMaster.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldMaster),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(oldShard.Block): storage.BlockStateWithoutCells(oldShard),
		},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldMaster, oldShard}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, newRoot)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	if _, err = store.DeleteStateMetadataBeforeCellGenerationSwitch(ctx, newState.Block, newCurrent, nil); err != nil {
		t.Fatalf("delete state metadata before switch: %v", err)
	}
	if _, err = store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent); err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, oldMaster.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old master state metadata err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, oldShard.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old shard state metadata err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, newState.Block); err != nil {
		t.Fatalf("new state metadata after switch: %v", err)
	}
}

func TestSwitchCellGenerationDoesNotRewriteCurrentStateMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	candidateRoot := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	durableState := blockStateWithRoot(block, candidateRoot)
	durableState.StateFileHash = bytes.Repeat([]byte{0x44}, 32)
	durableCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(durableState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableState}, durableCurrent); err != nil {
		t.Fatalf("save durable checkpoint: %v", err)
	}

	generation, err := store.BeginCellGeneration(ctx, block)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	if err = generationCells.SaveEncoded(ctx, mustReachableEncodedRecords(t, candidateRoot), true); err != nil {
		t.Fatalf("save candidate cells: %v", err)
	}
	candidateState := blockStateWithRoot(block, candidateRoot)
	candidateState.StateFileHash = bytes.Repeat([]byte{0x55}, 32)
	candidateCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(candidateState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, candidateCurrent)

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, block, block, candidateCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}

	meta, err := store.blockStateMeta(ctx, block)
	if err != nil {
		t.Fatalf("load switched metadata: %v", err)
	}
	if !bytes.Equal(meta.StateRootHash, candidateState.StateRootHash) {
		t.Fatalf("state root hash was not switched: got=%x want=%x", meta.StateRootHash, candidateState.StateRootHash)
	}
	if !bytes.Equal(meta.StateFileHash, durableState.StateFileHash) {
		t.Fatalf("state metadata was rewritten during switch: got file_hash=%x want %x", meta.StateFileHash, durableState.StateFileHash)
	}
	loaded, err := store.LoadStateCellTree(ctx, block, candidateState.StateRootHash)
	if err != nil {
		t.Fatalf("load switched state root: %v", err)
	}
	if loaded.HashKey() != candidateRoot.HashKey() {
		t.Fatalf("loaded switched root hash = %x, want %x", loaded.HashKey(), candidateRoot.HashKey())
	}
}

func TestSwitchCellGenerationRejectsDurableRootMismatch(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	durableRoot := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
	durableState := blockStateWithRoot(block, durableRoot)
	durableCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(durableState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableState}, durableCurrent); err != nil {
		t.Fatalf("save durable checkpoint: %v", err)
	}

	generation, err := store.BeginCellGeneration(ctx, block)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	candidateRoot := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	if err = generationCells.SaveEncoded(ctx, mustReachableEncodedRecords(t, candidateRoot), true); err != nil {
		t.Fatalf("save candidate cells: %v", err)
	}
	candidateState := blockStateWithRoot(block, candidateRoot)
	candidateCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(candidateState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, candidateCurrent)

	if _, err = store.SwitchCellGeneration(ctx, generation, block, block, candidateCurrent); err == nil || !strings.Contains(err.Error(), "root hash mismatch") {
		t.Fatalf("switch root mismatch err=%v, want root hash mismatch", err)
	}
	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if active.ID != initialCellGenerationID {
		t.Fatalf("active generation after rejected switch = %d, want %d", active.ID, initialCellGenerationID)
	}
}

func TestSwitchCellGenerationKeepsCurrentShardStateMetadataBeforeOrigin(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldMaster := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	origin := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 20}, 0x20)
	originShard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x30)
	originShard.MasterchainRef = &oldMaster.Block
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: origin.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(origin),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(originShard.Block): storage.BlockStateWithoutCells(originShard),
		},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldMaster, origin, originShard}, current); err != nil {
		t.Fatalf("save durable checkpoint: %v", err)
	}

	generation, err := store.BeginCellGeneration(ctx, origin.Block)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	if _, err = generationCells.ImportStateCellTree(ctx, origin.Block, origin.Cell, 1); err != nil {
		t.Fatalf("import origin state: %v", err)
	}
	if _, err = generationCells.ImportStateCellTree(ctx, originShard.Block, originShard.Cell, 1); err != nil {
		t.Fatalf("import origin shard state: %v", err)
	}
	current.Masterchain = storage.BlockStateWithoutCells(origin)
	current.Shards[storage.ShardKeyFromBlock(originShard.Block)] = storage.BlockStateWithoutCells(originShard)
	saveCellGenerationSwitchProgress(t, store, ctx, generation, current)

	if _, err = store.DeleteStateMetadataBeforeCellGenerationSwitch(ctx, origin.Block, current, nil); err != nil {
		t.Fatalf("delete state metadata before switch: %v", err)
	}
	if _, err = store.SwitchCellGeneration(ctx, generation, origin.Block, origin.Block, current); err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, oldMaster.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old master metadata err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, originShard.Block); err != nil {
		t.Fatalf("origin shard metadata was deleted: %v", err)
	}
}

func TestSwitchCellGenerationKeepsImportedOriginShardStateMetadataBeforeOrigin(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	oldMaster := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}, 0x10)
	origin := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 20}, 0x20)
	staleShard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 90}, 0x30)
	staleShard.MasterchainRef = &oldMaster.Block
	originShard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 100}, 0x40)
	originShard.MasterchainRef = &oldMaster.Block
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: origin.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(origin),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldMaster, staleShard, origin, originShard}, current); err != nil {
		t.Fatalf("save durable checkpoint: %v", err)
	}

	generation, err := store.BeginCellGeneration(ctx, origin.Block)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	if _, err = generationCells.ImportStateCellTree(ctx, origin.Block, origin.Cell, 1); err != nil {
		t.Fatalf("import origin state: %v", err)
	}
	if _, err = generationCells.ImportStateCellTree(ctx, originShard.Block, originShard.Cell, 1); err != nil {
		t.Fatalf("import origin shard state: %v", err)
	}
	current.Masterchain = storage.BlockStateWithoutCells(origin)
	saveCellGenerationSwitchProgress(t, store, ctx, generation, current)

	// The origin persistent state may contain a shard first included by an older masterchain block.
	// Keep that imported metadata even if the shard is no longer current at switch time.
	if _, err = store.DeleteStateMetadataBeforeCellGenerationSwitch(ctx, origin.Block, current, []ton.BlockIDExt{originShard.Block}); err != nil {
		t.Fatalf("delete state metadata before switch: %v", err)
	}
	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, origin.Block, origin.Block, current)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if _, err = store.blockStateMeta(ctx, staleShard.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stale shard metadata err=%v, want not found", err)
	}
	if _, err = store.blockStateMeta(ctx, originShard.Block); err != nil {
		t.Fatalf("origin shard metadata was deleted: %v", err)
	}
	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}
	root, err := store.LoadStateCellTree(ctx, originShard.Block, originShard.StateRootHash)
	if err != nil {
		t.Fatalf("load preserved origin shard state after old generation cleanup: %v", err)
	}
	if root.HashKey() != originShard.Cell.HashKey() {
		t.Fatalf("origin shard root hash mismatch: got=%x want=%x", root.HashKey(), originShard.Cell.HashKey())
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
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
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select reopened pending cell generation: %v", err)
	}

	root := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	if _, err = generationCells.ImportStateCellTree(ctx, origin, root, 1); err != nil {
		t.Fatalf("import into reopened pending generation: %v", err)
	}

	otherOrigin := origin
	otherOrigin.SeqNo++
	if _, err = store.BeginCellGeneration(ctx, otherOrigin); err == nil {
		t.Fatal("begin generation with different origin reused pending migration")
	}
}

func TestCellGenerationMigrationProgressSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
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
	state := blockStateWithSingleCell(origin, 0x33)
	if err = saveTestBlockState(ctx, store, state); err != nil {
		t.Fatalf("save block state: %v", err)
	}
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: origin.SeqNo,
		Masterchain:      *state,
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
		t.Fatalf("save migration progress: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	loaded, err := store.CellGenerationMigrationProgress(ctx, generation)
	if err != nil {
		t.Fatalf("load migration progress: %v", err)
	}
	if !loaded.Masterchain.Block.Equals(&origin) {
		t.Fatalf("progress masterchain = %s, want %s", storage.FormatBlockRef(loaded.Masterchain.Block), storage.FormatBlockRef(origin))
	}
	if err = store.DropPendingCellGeneration(ctx, generation); err != nil {
		t.Fatalf("drop pending generation: %v", err)
	}
	if _, err = store.CellGenerationMigrationProgress(ctx, generation); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("dropped generation progress error = %v, want ErrNotFound", err)
	}
}

func TestCellGenerationMigrationProgressFromStoredCurrentStateResolvesMasterRef(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 30}, 0x30)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 40}, 0x40)
	shard.MasterchainRef = &master.Block
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): storage.BlockStateWithoutCells(shard),
		},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{master, shard}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}

	loadedCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current state: %v", err)
	}
	loadedShard := loadedCurrent.Shards[storage.ShardKeyFromBlock(shard.Block)]
	if loadedShard.MasterchainRef == nil || !loadedShard.MasterchainRef.Equals(&master.Block) {
		t.Fatalf("loaded shard master ref = %+v, want %s", loadedShard.MasterchainRef, storage.FormatBlockRef(master.Block))
	}

	generation, err := store.BeginCellGeneration(ctx, master.Block)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	if err = store.SaveCellGenerationMigrationProgress(ctx, generation, loadedCurrent); err != nil {
		t.Fatalf("save migration progress: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	progress, err := store.CellGenerationMigrationProgress(ctx, generation)
	if err != nil {
		t.Fatalf("load migration progress: %v", err)
	}
	progressShard := progress.Shards[storage.ShardKeyFromBlock(shard.Block)]
	if progressShard.MasterchainRef == nil || !progressShard.MasterchainRef.Equals(&master.Block) {
		t.Fatalf("progress shard master ref = %+v, want %s", progressShard.MasterchainRef, storage.FormatBlockRef(master.Block))
	}
}

func TestCellGenerationMigrationProgressResolvesSeqOnlyMasterRefBeforeEncode(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := blockStateWithSingleCell(ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 31}, 0x31)
	shard := blockStateWithSingleCell(ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 41}, 0x41)
	masterMeta, err := storage.BuildBlockMetaFromState(*master)
	if err != nil {
		t.Fatalf("build master meta: %v", err)
	}
	if err = store.SaveBlockMeta(masterMeta); err != nil {
		t.Fatalf("save master meta: %v", err)
	}

	seqOnlyMasterRef := ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: master.Block.SeqNo}
	shard.MasterchainRef = &seqOnlyMasterRef
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(master),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard.Block): storage.BlockStateWithoutCells(shard),
		},
	}
	generation, err := store.BeginCellGeneration(ctx, master.Block)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	if err = store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
		t.Fatalf("save migration progress: %v", err)
	}

	progress, err := store.CellGenerationMigrationProgress(ctx, generation)
	if err != nil {
		t.Fatalf("load migration progress: %v", err)
	}
	progressShard := progress.Shards[storage.ShardKeyFromBlock(shard.Block)]
	if progressShard.MasterchainRef == nil || !progressShard.MasterchainRef.Equals(&master.Block) {
		t.Fatalf("progress shard master ref = %+v, want %s", progressShard.MasterchainRef, storage.FormatBlockRef(master.Block))
	}
}

func TestDropPendingCellGenerationDetachesStatus(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
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
	heldCells, err := store.acquireCellStore(ctx, generation)
	if err != nil {
		t.Fatalf("hold pending generation ref: %v", err)
	}
	heldCellsReleased := false
	defer func() {
		if !heldCellsReleased {
			heldCells.release()
		}
	}()
	if err = store.DropPendingCellGeneration(ctx, generation); err != nil {
		t.Fatalf("drop pending generation: %v", err)
	}

	if _, err = store.PendingCellGenerationMigration(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pending migration after drop err = %v, want ErrNotFound", err)
	}

	status, err := store.DBStatus(ctx)
	if err != nil {
		t.Fatalf("db status: %v", err)
	}
	if len(status.CellGenerations) != 1 {
		t.Fatalf("cell generations after drop = %d, want only active generation", len(status.CellGenerations))
	}
	if status.CellGenerations[0].ID == generation {
		t.Fatal("dropped pending generation is still visible in db status")
	}

	deadline := time.Now().Add(time.Second)
	for {
		removed := true
		for shard := 0; shard < cellDBShardCount; shard++ {
			if _, err = os.Stat(cellGenerationShardDir(store.dir, generation, shard)); err == nil {
				removed = false
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stat dropped generation shard dir: %v", err)
			}
		}
		if removed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dropped pending generation dirs were not removed while cell store ref is held")
		}
		time.Sleep(10 * time.Millisecond)
	}
	heldCells.release()
	heldCellsReleased = true
}

func TestCellsRejectsZeroGeneration(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	cells, err := store.Cells(0)
	if err == nil {
		t.Fatal("selecting zero cell generation succeeded")
	}
	if cells != nil {
		t.Fatal("selecting zero cell generation returned a handle")
	}
}

func TestCellsDoesNotFallbackToActive(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
	}
	root := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	if _, err = store.ImportStateCellTree(ctx, block, root, 1); err != nil {
		t.Fatalf("import active state root: %v", err)
	}

	generation, err := store.BeginCellGeneration(ctx, testMasterBlockID(21, 0x21))
	if err != nil {
		t.Fatalf("begin candidate generation: %v", err)
	}
	cells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select candidate generation: %v", err)
	}

	rootHash := root.HashKey(0)
	if _, err = cells.Load(ctx, rootHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load active-only cell from candidate err=%v, want ErrNotFound", err)
	}
	if _, err = cells.Loader()(rootHash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lazy-load active-only cell from candidate err=%v, want ErrNotFound", err)
	}
}

func TestLazyCellLoadMetricsCountDecodedCacheAndPebble(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     21,
	}
	root := cell.BeginCell().MustStoreUInt(0x21, 8).EndCell()
	if _, err = store.ImportStateCellTree(ctx, block, root, 1); err != nil {
		t.Fatalf("import state cell tree: %v", err)
	}

	before, err := store.DBStatus(ctx)
	if err != nil {
		t.Fatalf("db status before load: %v", err)
	}
	beforeDecoded := lazyCellLoadMetricCount(before.LazyCellLoads, storage.LazyCellLoadLayerDecodedCache)
	beforeStore := lazyCellStoreReadCount(before.LazyCellLoads)
	store.decodedCells = newDecodedCellCache(decodedCellCacheConfig{
		enabled: true,
		shards:  DefaultDecodedCellCacheShards,
		entries: DefaultDecodedCellCacheEntries,
	})

	rootHash := root.HashKey(0)
	if _, err = store.LoadCell(ctx, rootHash[:]); err != nil {
		t.Fatalf("load lazy cell from pebble: %v", err)
	}
	if _, err = store.LoadCell(ctx, rootHash[:]); err != nil {
		t.Fatalf("load lazy cell from decoded cache: %v", err)
	}

	after, err := store.DBStatus(ctx)
	if err != nil {
		t.Fatalf("db status after load: %v", err)
	}
	// Summed across the three store-read layers rather than pinned to one of
	// them: which band a read lands in depends on how long it took, and a test
	// cannot pin that. What is deterministic — and what this asserts — is that
	// the load fell through to the store exactly once.
	if got := lazyCellStoreReadCount(after.LazyCellLoads) - beforeStore; got != 1 {
		t.Fatalf("store-read lazy loads delta = %d, want 1", got)
	}
	if got := lazyCellLoadMetricCount(after.LazyCellLoads, storage.LazyCellLoadLayerDecodedCache) - beforeDecoded; got != 1 {
		t.Fatalf("decoded cache lazy loads delta = %d, want 1", got)
	}
}

func TestCellGenerationHandleKeepsRequestedGenerationAfterSwitch(t *testing.T) {
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
	oldChild := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	oldRoot := cell.BeginCell().MustStoreUInt(0x12, 8).MustStoreRef(oldChild).EndCell()
	if _, err = store.ImportStateCellTree(ctx, oldBlock, oldRoot, 2); err != nil {
		t.Fatalf("import old active root: %v", err)
	}
	oldCells, err := store.Cells(initialCellGenerationID)
	if err != nil {
		t.Fatalf("select old cell generation: %v", err)
	}

	oldRootHash := oldRoot.HashKey(0)
	oldLazyRoot, err := oldCells.Loader()(oldRootHash)
	if err != nil {
		t.Fatalf("load explicit old generation root: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	newCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select new cell generation: %v", err)
	}
	if err = newCells.SaveEncoded(ctx, mustReachableEncodedRecords(t, newRoot), true); err != nil {
		t.Fatalf("save candidate root cells: %v", err)
	}

	newState := blockStateWithRoot(newBlock, newRoot)
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{newState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)
	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if oldCells.ID() != initialCellGenerationID {
		t.Fatalf("old handle generation after switch = %d, want %d", oldCells.ID(), initialCellGenerationID)
	}
	activeCells, err := store.ActiveCells()
	if err != nil {
		t.Fatalf("select active generation after switch: %v", err)
	}
	if activeCells.ID() != generation {
		t.Fatalf("active handle generation after switch = %d, want %d", activeCells.ID(), generation)
	}
	if _, err = oldCells.Load(ctx, oldRootHash[:]); err != nil {
		t.Fatalf("load old generation root through pinned handle after switch: %v", err)
	}
	oldChildHash := oldChild.HashKey(0)
	if _, err = store.LoadCell(ctx, oldChildHash[:]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load old-only cell through active view err=%v, want ErrNotFound", err)
	}

	loadedChild, err := oldLazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load old generation child after switch: %v", err)
	}
	if loadedChild.HashKey(0) != oldChild.HashKey(0) {
		t.Fatalf("old generation child hash = %x, want %x", loadedChild.HashKey(0), oldChild.HashKey(0))
	}

	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}
	if _, err = oldCells.Load(ctx, oldRootHash[:]); !errors.Is(err, errCellGenerationNotOpen) {
		t.Fatalf("load through cleaned generation handle err=%v, want errCellGenerationNotOpen", err)
	}
	if _, err = oldCells.Loader()(oldRootHash); !errors.Is(err, errCellGenerationNotOpen) {
		t.Fatalf("lazy-load through cleaned generation handle err=%v, want errCellGenerationNotOpen", err)
	}
	if _, err = loadedChild.BeginParse(); !errors.Is(err, errCellGenerationNotOpen) {
		t.Fatalf("materialize child through cleaned generation handle err=%v, want errCellGenerationNotOpen", err)
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, newRoot)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{advancedState}, advancedCurrent); err != nil {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{newState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

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

func TestSwitchCellGenerationRejectsUnflushedCandidateCells(t *testing.T) {
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	if err = generationCells.SaveEncoded(ctx, mustReachableEncodedRecords(t, newRoot), false); err != nil {
		t.Fatalf("save unflushed candidate cells: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newState.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{newState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	if _, err = store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent); err == nil || !strings.Contains(err.Error(), "unflushed cell shards") {
		t.Fatalf("switch unflushed cells err=%v, want unflushed cell shards", err)
	}
	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	if active.ID != initialCellGenerationID {
		t.Fatalf("active generation = %d, want %d", active.ID, initialCellGenerationID)
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	newRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, newRoot, 1)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, newRoot)
	newState.Cell = lazyRoot
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, newRoot)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	if _, err = store.DeleteStateMetadataBeforeCellGenerationSwitch(ctx, newState.Block, newCurrent, nil); err != nil {
		t.Fatalf("delete state metadata before switch: %v", err)
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
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	oldLazyRoot, err := store.LoadStateCellTree(ctx, oldState.Block, oldState.StateRootHash)
	if err != nil {
		t.Fatalf("load old lazy root: %v", err)
	}

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, root, 2)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, root)
	newState.Cell = lazyRoot
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, root)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}

	loadedChild, err := oldLazyRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("get old active lazy root child: %v", err)
	}
	loadedChildSlice, err := loadedChild.BeginParse()
	if err != nil {
		t.Fatalf("materialize old active lazy root child from current generation: %v", err)
	}
	value, err := loadedChildSlice.LoadUInt(8)
	if err != nil {
		t.Fatalf("load old active lazy root child payload: %v", err)
	}
	if value != 0x11 {
		t.Fatalf("loaded child payload = %#x, want %#x", value, uint64(0x11))
	}
}

func TestActiveImportRootsSurvivePreviousGenerationDeleteWhenCellsExistInNewGeneration(t *testing.T) {
	tests := []struct {
		name       string
		importRoot func(*testing.T, context.Context, *Store, ton.BlockIDExt, *cell.Cell) (*cell.Cell, error)
	}{
		{
			name: "cell tree",
			importRoot: func(_ *testing.T, ctx context.Context, store *Store, block ton.BlockIDExt, root *cell.Cell) (*cell.Cell, error) {
				return store.ImportStateCellTree(ctx, block, root, 2)
			},
		},
		{
			name: "boc view",
			importRoot: func(t *testing.T, ctx context.Context, store *Store, block ton.BlockIDExt, root *cell.Cell) (*cell.Cell, error) {
				return store.ImportStateBOCView(ctx, block, testStateBOCView(t, root, nil))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer func() { _ = store.Close() }()

			ctx := context.Background()
			oldBlock := testMasterBlockID(10, 0x10)
			child := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
			root := cell.BeginCell().MustStoreUInt(0x12, 8).MustStoreRef(child).EndCell()
			importedRoot, err := tt.importRoot(t, ctx, store, oldBlock, root)
			if err != nil {
				t.Fatalf("import active state: %v", err)
			}
			importedChild, err := importedRoot.PeekRef(0)
			if err != nil {
				t.Fatalf("get imported active root child: %v", err)
			}

			oldState := blockStateWithRoot(oldBlock, root)
			oldCurrent := &storage.CurrentState{
				SyncedAt:         time.Now(),
				ShardClientSeqno: oldBlock.SeqNo,
				Masterchain:      storage.BlockStateWithoutCells(oldState),
				Shards:           map[storage.ShardKey]storage.BlockState{},
			}
			if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
				t.Fatalf("save old checkpoint: %v", err)
			}

			newBlock := testMasterBlockID(20, 0x20)
			generation, err := store.BeginCellGeneration(ctx, newBlock)
			if err != nil {
				t.Fatalf("begin generation: %v", err)
			}
			generationCells, err := store.Cells(generation)
			if err != nil {
				t.Fatalf("select cell generation: %v", err)
			}
			lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, root, 2)
			if err != nil {
				t.Fatalf("copy state into candidate generation: %v", err)
			}
			newState := blockStateWithRoot(newBlock, root)
			newState.Cell = lazyRoot
			newCurrent := &storage.CurrentState{
				SyncedAt:         time.Now(),
				ShardClientSeqno: newBlock.SeqNo,
				Masterchain:      storage.BlockStateWithoutCells(newState),
				Shards:           map[storage.ShardKey]storage.BlockState{},
			}
			durableNewState := blockStateWithRoot(newBlock, root)
			if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
				t.Fatalf("save durable current before switch: %v", err)
			}
			saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

			oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
			if err != nil {
				t.Fatalf("switch generation: %v", err)
			}
			if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
				t.Fatalf("cleanup old generation: %v", err)
			}

			childSlice, err := importedChild.BeginParse()
			if err != nil {
				t.Fatalf("materialize imported active root child from current generation: %v", err)
			}
			value, err := childSlice.LoadUInt(8)
			if err != nil {
				t.Fatalf("load imported active root child payload: %v", err)
			}
			if value != 0x11 {
				t.Fatalf("imported child payload = %#x, want %#x", value, uint64(0x11))
			}
		})
	}
}

func TestBlockStateLazyRefsSurvivePreviousGenerationDeleteWhenCellsExistInNewGeneration(t *testing.T) {
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
	payload := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	root, _ := testMasterStateHeaderRoot(oldBlock, payload)
	oldState := blockStateWithRoot(oldBlock, root)
	oldCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: oldBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(oldState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{oldState}, oldCurrent); err != nil {
		t.Fatalf("save old checkpoint: %v", err)
	}

	loadedState, err := store.BlockState(ctx, oldState.Block)
	if err != nil {
		t.Fatalf("load old block state: %v", err)
	}
	if loadedState.Parsed == nil || loadedState.Parsed.OutMsgQueueInfo == nil {
		t.Fatal("loaded block state has no out message queue ref")
	}
	lazyPayload := loadedState.Parsed.OutMsgQueueInfo

	newBlock := testMasterBlockID(20, 0x20)
	generation, err := store.BeginCellGeneration(ctx, newBlock)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	lazyRoot, err := generationCells.ImportStateCellTree(ctx, newBlock, root, 5)
	if err != nil {
		t.Fatalf("import candidate state: %v", err)
	}
	newState := blockStateWithRoot(newBlock, root)
	newState.Cell = lazyRoot
	newCurrent := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: newBlock.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(newState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	durableNewState := blockStateWithRoot(newBlock, root)
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{durableNewState}, newCurrent); err != nil {
		t.Fatalf("save durable current before switch: %v", err)
	}
	saveCellGenerationSwitchProgress(t, store, ctx, generation, newCurrent)

	oldGeneration, err := store.SwitchCellGeneration(ctx, generation, newState.Block, newState.Block, newCurrent)
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if err = store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}

	payloadSlice, err := lazyPayload.BeginParse()
	if err != nil {
		t.Fatalf("materialize old block state ref from current generation: %v", err)
	}
	value, err := payloadSlice.LoadUInt(8)
	if err != nil {
		t.Fatalf("load old block state ref payload: %v", err)
	}
	if value != 0x11 {
		t.Fatalf("loaded block state ref payload = %#x, want %#x", value, uint64(0x11))
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

	if _, err = saveActiveTestStateCellTree(ctx, store, stateCellTreeSave{
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
	loadedRoot, err := store.LoadCell(ctx, root.Hash())
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
	if _, err = saveActiveTestStateCellTree(ctx, store, stateCellTreeSave{
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
	if _, err = saveActiveTestStateCellTree(ctx, store, stateCellTreeSave{
		block:      nextBlock,
		root:       newRoot,
		totalCells: 3,
	}); err != nil {
		t.Fatalf("save state with missing lazy boundary: %v", err)
	}
	if _, err = store.LoadCell(ctx, leafHash[:]); !errors.Is(err, storage.ErrNotFound) {
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
	if _, err = saveActiveTestStateCellTree(ctx, store, stateCellTreeSave{
		block:      nextBlock,
		root:       newRoot,
		totalCells: 2,
	}); err != nil {
		t.Fatalf("save state with missing lazy data cell: %v", err)
	}
	if _, err = store.LoadCell(ctx, rightHash[:]); err != nil {
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
	if _, err = saveActiveTestStateCellTree(ctx, store, stateCellTreeSave{
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

// lazyCellStoreReadCount sums the layers that mean "this load reached the cell
// store", whichever band its duration put it in.
func lazyCellStoreReadCount(metrics []storage.LazyCellLoadMetric) uint64 {
	return lazyCellLoadMetricCount(metrics, storage.LazyCellLoadLayerBlockCache) +
		lazyCellLoadMetricCount(metrics, storage.LazyCellLoadLayerPageCache) +
		lazyCellLoadMetricCount(metrics, storage.LazyCellLoadLayerDisk)
}

func lazyCellLoadMetricCount(metrics []storage.LazyCellLoadMetric, layer string) uint64 {
	var count uint64
	for _, metric := range metrics {
		if metric.Layer == layer {
			count += metric.Count
		}
	}
	return count
}

func TestSaveCellRecordsWritesChunksAndDedupes(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	generation, err := store.activeCellGenerationID()
	if err != nil {
		t.Fatalf("active cell generation: %v", err)
	}

	const count = 4096 + 512
	records := make([]storage.EncodedCellRecord, 0, count+1)
	for i := 0; i < count; i++ {
		var hash cell.Hash
		hash[0] = byte(i)
		binary.BigEndian.PutUint64(hash[8:16], uint64(i))
		data := make([]byte, 12)
		binary.BigEndian.PutUint64(data, uint64(i))
		records = append(records, storage.EncodedCellRecord{Hash: hash, Data: data})
	}
	firstData := bytes.Clone(records[0].Data)
	duplicate := records[0]
	duplicate.Data = []byte{0xff}
	records = append(records, duplicate)

	set := storage.NewStateCellRecordChunks([][]storage.EncodedCellRecord{
		records[:count/2],
		records[count/2:],
	}, 0)
	stats, err := store.saveCellRecordSet(ctx, set, true, generation, true)
	if err != nil {
		t.Fatalf("save encoded cells: %v", err)
	}
	if stats.written != count || stats.skipped != 1 || stats.bytes != int64(count*len(firstData)) {
		t.Fatalf("save stats = %+v, want written=%d skipped=1 bytes=%d", stats, count, count*len(firstData))
	}

	seenShards := map[int]struct{}{}
	for i := 0; i < count; i += 61 {
		raw, err := store.getCellCopyFromGeneration(ctx, generation, records[i].Hash[:])
		if err != nil {
			t.Fatalf("read cell %d back: %v", i, err)
		}
		if !bytes.Equal(raw, records[i].Data) {
			t.Fatalf("cell %d data mismatch: got=%x want=%x", i, raw, records[i].Data)
		}
		seenShards[cellShardIndex(records[i].Hash)] = struct{}{}
	}
	if len(seenShards) != cellDBShardCount {
		t.Fatalf("readback covered %d shards, want %d", len(seenShards), cellDBShardCount)
	}
	raw, err := store.getCellCopyFromGeneration(ctx, generation, records[0].Hash[:])
	if err != nil {
		t.Fatalf("read duplicated cell: %v", err)
	}
	if !bytes.Equal(raw, firstData) {
		t.Fatalf("deduplicated cell data = %x, want first value %x", raw, firstData)
	}
}

func TestSaveCellRecordsRejectsEmptyRecord(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	generation, err := store.activeCellGenerationID()
	if err != nil {
		t.Fatalf("active cell generation: %v", err)
	}

	type emptyRecordTestCase struct {
		name  string
		count int
	}
	tests := []emptyRecordTestCase{
		{name: "single-writer", count: 8},
		{name: "sharded", count: stateCellSaveShardedMinRecords},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := make([]storage.EncodedCellRecord, 0, tt.count)
			for i := 0; i < tt.count; i++ {
				var hash cell.Hash
				hash[0] = byte(i)
				binary.BigEndian.PutUint64(hash[8:16], uint64(i))
				record := storage.EncodedCellRecord{Hash: hash, Data: []byte{0x01}}
				if i == tt.count/2 {
					record.Data = nil
				}
				records = append(records, record)
			}

			_, saveErr := store.saveCellRecordSet(ctx, storage.NewStateCellRecords(records), false, generation, false)
			if saveErr == nil {
				t.Fatal("expected empty record error")
			}
			if !strings.Contains(saveErr.Error(), "encoded cell record is empty") {
				t.Fatalf("save empty record err = %v, want root cause", saveErr)
			}
		})
	}
}

func TestSaveCellRecordsHonorsCanceledContext(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	type canceledContextTestCase struct {
		name  string
		count int
		save  func(storage.StateCellRecords) (cellRecordBatchStats, error)
	}
	tests := []canceledContextTestCase{
		{
			name:  "single-writer",
			count: 1,
			save: func(records storage.StateCellRecords) (cellRecordBatchStats, error) {
				return saveCellRecords(ctx, store.cells, records, false)
			},
		},
		{
			name:  "sharded",
			count: stateCellSaveShardedMinRecords,
			save: func(records storage.StateCellRecords) (cellRecordBatchStats, error) {
				return saveCellRecordSetSharded(ctx, store.cells, records, false)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := make([]storage.EncodedCellRecord, tt.count)
			for i := range records {
				binary.BigEndian.PutUint64(records[i].Hash[24:], uint64(i))
				records[i].Data = []byte{0x01}
			}

			stats, err := tt.save(storage.NewStateCellRecords(records))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("save canceled records err = %v, want context.Canceled", err)
			}
			if stats != (cellRecordBatchStats{}) {
				t.Fatalf("save canceled records stats = %+v, want zero", stats)
			}
		})
	}
}
