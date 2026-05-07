package pebblestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"flexserver/service/archive/packfile"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSaveArchiveImportPublishesRefOnlyFullBlock(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	blockData := []byte{0x11, 0x22, 0x33}
	proofData := []byte{0x44, 0x55, 0x66}
	packPath, blockPtr, proofPtr := writeRefOnlyTestPack(t, block, blockData, proofData)

	err = store.SaveArchiveImport(&storage.ServedArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:       block,
			BlockRef: &storage.ArtifactRef{Path: packPath, Offset: blockPtr.Offset, Size: blockPtr.Size},
			ProofRef: &storage.ArtifactRef{Path: packPath, Offset: proofPtr.Offset, Size: proofPtr.Size},
			Meta:     &storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 10, EndLT: 20},
		}},
	})
	if err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	gotBlock, err := store.BlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("load block data: %v", err)
	}
	if !bytes.Equal(gotBlock, blockData) {
		t.Fatalf("block data mismatch: got %x want %x", gotBlock, blockData)
	}

	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofBlock, block)
	if err != nil {
		t.Fatalf("load proof: %v", err)
	}
	if !bytes.Equal(gotProof, proofData) {
		t.Fatalf("proof data mismatch: got %x want %x", gotProof, proofData)
	}

	indexed, err := store.LookupBlockBySeqNo(context.Background(), storage.BlockHistoryKey{Workchain: block.Workchain, Shard: block.Shard}, block.SeqNo)
	if err != nil {
		t.Fatalf("lookup block index: %v", err)
	}
	if !indexed.Equals(&block) {
		t.Fatalf("indexed block = %s, want %s", storage.FormatBlockRef(indexed), storage.FormatBlockRef(block))
	}
}

func TestSaveBlockFullStoresOriginalBlockBOC(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	root := cell.BeginCell().
		MustStoreUInt(0xab, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xcd, 8).EndCell()).
		EndCell()
	blockData := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockData)
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     77,
		RootHash:  append([]byte(nil), rootHash[:]...),
		FileHash:  append([]byte(nil), fileHash[:]...),
	}

	err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:    block,
		Block: append([]byte(nil), blockData...),
		Meta:  &storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 10, EndLT: 20},
	})
	if err != nil {
		t.Fatalf("save block full: %v", err)
	}

	got, err := store.BlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("load block data: %v", err)
	}
	if !bytes.Equal(got, blockData) {
		t.Fatalf("block data mismatch: got %x want %x", got, blockData)
	}

	rawRef, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
	if err != nil {
		t.Fatalf("load block artifact ref: %v", err)
	}
	ref, err := decodeArtifactRef(rawRef)
	if err != nil {
		t.Fatalf("decode block artifact ref: %v", err)
	}
	packed, err := store.readArtifactRef(ref)
	if err != nil {
		t.Fatalf("read packed block data: %v", err)
	}
	if !bytes.Equal(packed, blockData) {
		t.Fatalf("packed block data mismatch: got %x want %x", packed, blockData)
	}

	store.artifactMu.Lock()
	_, pending := store.pendingLooseSync[store.loosePackPath()]
	store.artifactMu.Unlock()
	if !pending {
		t.Fatal("loose block pack was not scheduled for checkpoint sync")
	}

	if err = store.syncPendingArtifactFiles(); err != nil {
		t.Fatalf("sync pending artifacts: %v", err)
	}
	store.artifactMu.Lock()
	pendingCount := len(store.pendingLooseSync)
	store.artifactMu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("pending loose sync files = %d, want 0", pendingCount)
	}
}

func TestSaveKeyBlockProofUsesKeyArchivePack(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     345678,
		RootHash:  bytes.Repeat([]byte{0x03}, 32),
		FileHash:  bytes.Repeat([]byte{0x04}, 32),
	}
	proofData := []byte{0x44, 0x55, 0x66}

	if err = store.SaveBlockProof(storage.ServedProofKeyBlock, block, proofData, nil); err != nil {
		t.Fatalf("save key proof: %v", err)
	}

	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofKeyBlock, block)
	if err != nil {
		t.Fatalf("load key proof: %v", err)
	}
	if !bytes.Equal(gotProof, proofData) {
		t.Fatalf("key proof data mismatch: got %x want %x", gotProof, proofData)
	}

	keyPack := filepath.Join(dir, "packs", "key", "key000", "key.archive.200000.pack")
	if _, err = os.Stat(keyPack); err != nil {
		t.Fatalf("key pack was not created: %v", err)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyProofRef(storage.ServedProofKeyBlock, block)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("legacy proof ref error = %v, want ErrNotFound", err)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyKeyProofRef(storage.ServedProofKeyBlock, block)); err != nil {
		t.Fatalf("key proof ref missing: %v", err)
	}
	meta, err := store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaIsKeyBlock) {
		t.Fatal("key proof did not mark block meta as key block")
	}
}

func TestSavePersistentStateFileServesSizeSliceAndMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     77,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     78,
		RootHash:  bytes.Repeat([]byte{0x03}, 32),
		FileHash:  bytes.Repeat([]byte{0x04}, 32),
	}
	data := []byte{1, 2, 3, 4, 5, 6}
	path := filepath.Join(store.StateFilesDir(), "state-test")
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	fileHash := bytes.Repeat([]byte{0x55}, 32)
	stateRootHash := bytes.Repeat([]byte{0x66}, 32)

	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: path, Size: int64(len(data))},
		FileHash:         fileHash,
		StateRootHash:    stateRootHash,
		CellsCount:       9,
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}

	size, err := store.PersistentStateSize(context.Background(), block, master, 0)
	if err != nil {
		t.Fatalf("persistent state size: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("persistent state size = %d, want %d", size, len(data))
	}

	chunk, err := store.PersistentStateSlice(context.Background(), block, master, 0, 2, 3)
	if err != nil {
		t.Fatalf("persistent state slice: %v", err)
	}
	if !bytes.Equal(chunk, data[2:5]) {
		t.Fatalf("persistent state slice = %x, want %x", chunk, data[2:5])
	}

	meta, err := store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatal("persistent state file did not mark block meta")
	}
	if !bytes.Equal(meta.StateFileHash, fileHash) {
		t.Fatalf("state file hash = %x, want %x", meta.StateFileHash, fileHash)
	}
	if !bytes.Equal(meta.StateRootHash, stateRootHash) {
		t.Fatalf("state root hash = %x, want %x", meta.StateRootHash, stateRootHash)
	}
}

func writeRefOnlyTestPack(t *testing.T, block ton.BlockIDExt, blockData []byte, proofData []byte) (string, packfile.Pointer, packfile.Pointer) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("create pack: %v", err)
	}
	defer func() { _ = file.Close() }()

	blockPtr, err := packfile.Append(file, packfile.EntryName(packfile.KindBlock, block), blockData, false)
	if err != nil {
		t.Fatalf("append block: %v", err)
	}
	proofPtr, err := packfile.Append(file, packfile.EntryName(packfile.KindProof, block), proofData, false)
	if err != nil {
		t.Fatalf("append proof: %v", err)
	}
	if err = file.Sync(); err != nil {
		t.Fatalf("sync pack: %v", err)
	}
	return path, blockPtr, proofPtr
}
