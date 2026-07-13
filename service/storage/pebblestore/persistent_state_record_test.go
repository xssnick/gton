package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestPersistentStateFileRecordCellsCountCompatibility(t *testing.T) {
	fileHash := bytes.Repeat([]byte{0x11}, 32)
	stateRootHash := bytes.Repeat([]byte{0x22}, 32)

	encoded := encodePersistentStateFileRecord(&storage.PersistentStateFile{
		Ref:           &storage.ArtifactRef{Size: 1234},
		FileHash:      fileHash,
		StateRootHash: stateRootHash,
		CellsCount:    5678,
	})
	record, err := decodePersistentStateFileRecord(encoded)
	if err != nil {
		t.Fatalf("decode current record: %v", err)
	}
	if record.size != 1234 || record.cellsCount != 5678 {
		t.Fatalf("current record size=%d cells=%d", record.size, record.cellsCount)
	}

	legacy := []byte{persistentStateVersionV1}
	legacy = appendLenBytes(legacy, fileHash)
	legacy = appendLenBytes(legacy, stateRootHash)
	legacy = binary.BigEndian.AppendUint64(legacy, 1234)
	record, err = decodePersistentStateFileRecord(legacy)
	if err != nil {
		t.Fatalf("decode v1 record: %v", err)
	}
	if record.size != 1234 || record.cellsCount != 0 {
		t.Fatalf("v1 record size=%d cells=%d, want cells=0", record.size, record.cellsCount)
	}
}

func TestPersistentStateCellsCountHintUsesLatestCompatibleOlderState(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	const shard = int64(-1 << 63)
	oldBlock := persistentStateCountTestBlock(0, shard, 90)
	oldMaster := persistentStateCountTestBlock(-1, shard, 100)
	savePersistentStateCountTestFile(t, store, oldBlock, oldMaster, 0, 90)

	previousBlock := persistentStateCountTestBlock(0, shard, 110)
	previousMaster := persistentStateCountTestBlock(-1, shard, 120)
	savePersistentStateCountTestFile(t, store, previousBlock, previousMaster, 0, 120)

	newerBlock := persistentStateCountTestBlock(0, shard, 150)
	newerMaster := persistentStateCountTestBlock(-1, shard, 160)
	savePersistentStateCountTestFile(t, store, newerBlock, newerMaster, 0, 160)

	currentBlock := persistentStateCountTestBlock(0, shard, 130)
	currentMaster := persistentStateCountTestBlock(-1, shard, 140)
	count, err := store.PersistentStateCellsCountHint(context.Background(), currentBlock, currentMaster, 0)
	if err != nil {
		t.Fatalf("load cells count hint: %v", err)
	}
	if count != 120 {
		t.Fatalf("cells count hint = %d, want latest older 120", count)
	}

	if err = store.DeletePersistentStateFile(context.Background(), previousBlock, previousMaster, 0); err != nil {
		t.Fatalf("delete latest compatible state: %v", err)
	}
	count, err = store.PersistentStateCellsCountHint(context.Background(), currentBlock, currentMaster, 0)
	if err != nil {
		t.Fatalf("load cells count hint after delete: %v", err)
	}
	if count != 90 {
		t.Fatalf("cells count hint after delete = %d, want 90", count)
	}

	savePersistentStateCountTestFile(t, store, oldBlock, oldMaster, 0, 0)
	if _, err = store.PersistentStateCellsCountHint(context.Background(), currentBlock, currentMaster, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("zero-count replacement hint error = %v, want ErrNotFound", err)
	}
}

func TestPersistentStateCellsCountHintSeparatesPartClass(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	const effectiveShard = int64(0x2000000000000000)
	master := persistentStateCountTestBlock(-1, int64(-1<<63), 100)
	accountBlock := persistentStateCountTestBlock(0, int64(-1<<63), 90)
	savePersistentStateCountTestFile(t, store, accountBlock, master, effectiveShard, 90)

	currentMaster := persistentStateCountTestBlock(-1, int64(-1<<63), 120)
	headerBlock := persistentStateCountTestBlock(0, effectiveShard, 110)
	if _, err = store.PersistentStateCellsCountHint(context.Background(), headerBlock, currentMaster, effectiveShard); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("split header matched split account hint: %v", err)
	}
}

func TestPersistentStateCellsCountHintSeparatesSourceShard(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	oldBlock := persistentStateCountTestBlock(0, int64(-1<<63), 90)
	oldMaster := persistentStateCountTestBlock(-1, int64(-1<<63), 100)
	savePersistentStateCountTestFile(t, store, oldBlock, oldMaster, 0, 90)

	currentBlock := persistentStateCountTestBlock(0, int64(1<<62), 110)
	currentMaster := persistentStateCountTestBlock(-1, int64(-1<<63), 120)
	if _, err = store.PersistentStateCellsCountHint(context.Background(), currentBlock, currentMaster, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unsplit state matched another source shard hint: %v", err)
	}
}

func savePersistentStateCountTestFile(t *testing.T, store *Store, block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64, count uint64) {
	t.Helper()

	name, err := storage.PersistentStateFileName(block, master, effectiveShard)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   effectiveShard,
		Ref: &storage.ArtifactRef{
			Path: filepath.Join(store.StateFilesDir(), name),
			Size: 1,
		},
		FileHash:      bytes.Repeat([]byte{0x33}, 32),
		StateRootHash: bytes.Repeat([]byte{0x44}, 32),
		CellsCount:    count,
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
}

func persistentStateCountTestBlock(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno), 0x11}, 16),
		FileHash:  bytes.Repeat([]byte{byte(seqno), 0x22}, 16),
	}
}
