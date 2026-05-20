package pebblestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCommitSyncedPackSizesDoesNotWaitForArtifactMu(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	store.artifactMu.Lock()
	defer store.artifactMu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- store.commitSyncedPackSizes([]pendingPackSync{{
			path: filepath.Join(store.dir, "archive", "deadlock-test.pack"),
			size: 123,
		}})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("commit synced pack sizes: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("commit synced pack sizes waited for artifactMu")
	}
}

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

func TestStoredZeroStateBlocks(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if _, err = store.StoredZeroStateBlocks(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty zerostate blocks error = %v, want ErrNotFound", err)
	}

	blockA := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	blockB := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	if err = store.SaveZeroState(blockA, []byte{0x01}, nil); err != nil {
		t.Fatalf("save zerostate A: %v", err)
	}
	if err = store.SaveZeroState(blockB, []byte{0x02}, nil); err != nil {
		t.Fatalf("save zerostate B: %v", err)
	}

	blocks, err := store.StoredZeroStateBlocks(ctx)
	if err != nil {
		t.Fatalf("stored zerostate blocks: %v", err)
	}
	if !containsBlock(blocks, blockA) || !containsBlock(blocks, blockB) {
		t.Fatalf("stored zerostate blocks = %#v, want both test blocks", blocks)
	}
}

func containsBlock(blocks []ton.BlockIDExt, want ton.BlockIDExt) bool {
	for _, block := range blocks {
		if block.Equals(&want) {
			return true
		}
	}
	return false
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
		Workchain: -1,
		Shard:     int64(-1 << 63),
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
	_, pending := store.pendingArchiveSync[store.artifactPath(ref.Path)]
	store.artifactMu.Unlock()
	if !pending {
		t.Fatal("archive block pack was not scheduled for checkpoint sync")
	}

	if err = store.syncPendingArtifactFiles(); err != nil {
		t.Fatalf("sync pending artifacts: %v", err)
	}
	store.artifactMu.Lock()
	pendingCount := len(store.pendingArchiveSync)
	store.artifactMu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("pending archive sync files = %d, want 0", pendingCount)
	}
}

func TestArchivePackageMetadataTracksFirstMasterBlock(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	firstSaved := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     150,
		RootHash:  bytes.Repeat([]byte{0x15}, 32),
		FileHash:  bytes.Repeat([]byte{0x16}, 32),
	}
	earlier := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     120,
		RootHash:  bytes.Repeat([]byte{0x12}, 32),
		FileHash:  bytes.Repeat([]byte{0x13}, 32),
	}
	if err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:    firstSaved,
		Block: []byte{0x01},
		Meta:  &storage.BlockMeta{ID: firstSaved, GenUTime: 1500, StartLT: 15, EndLT: 16},
	}); err != nil {
		t.Fatalf("save first block: %v", err)
	}
	if err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:    earlier,
		Block: []byte{0x02},
		Meta:  &storage.BlockMeta{ID: earlier, GenUTime: 1200, StartLT: 12, EndLT: 13},
	}); err != nil {
		t.Fatalf("save earlier block: %v", err)
	}

	archiveID, err := store.ArchiveInfo(context.Background(), 120, -1, int64(-1<<63))
	if err != nil {
		t.Fatalf("archive info: %v", err)
	}
	raw, err := store.getHotCopy(context.Background(), hotKeyArchivePackage(archiveID))
	if err != nil {
		t.Fatalf("load archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		t.Fatalf("decode archive package meta: %v", err)
	}
	if meta.firstMasterSeq != 120 || meta.firstMasterUTime != 1200 || meta.firstMasterLT != 12 {
		t.Fatalf("unexpected first master metadata seq=%d utime=%d lt=%d", meta.firstMasterSeq, meta.firstMasterUTime, meta.firstMasterLT)
	}
	if meta.startSeq != 100 {
		t.Fatalf("unexpected archive package start seqno %d", meta.startSeq)
	}
	if meta.path == "" || meta.size == 0 {
		t.Fatalf("archive package meta did not record path/size: %+v", meta)
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

	keyPack := filepath.Join(dir, "archive", "packages", "key000", "key.archive.200000.pack")
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

func TestSaveBlockFullReplacesMissingArtifactRefs(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     99,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	missing := encodeArtifactRef(&storage.ArtifactRef{
		Path:   "archive/packages/arch0000/missing.pack",
		Offset: 10,
		Size:   4,
	})
	if err = store.withHotBatch(func(batch *pebble.Batch) error {
		if err := batch.Set(hotKeyBlockDataRef(block), missing, pebble.NoSync); err != nil {
			return err
		}
		return batch.Set(hotKeyStoredProofRef(storage.ServedProofBlock, block), missing, pebble.NoSync)
	}); err != nil {
		t.Fatalf("save stale refs: %v", err)
	}

	if _, err = store.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block data with missing ref error = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockProof(context.Background(), storage.ServedProofBlock, block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block proof with missing ref error = %v, want ErrNotFound", err)
	}

	blockData := []byte{0x11, 0x22, 0x33}
	proofData := []byte{0x44, 0x55, 0x66}
	if err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:    block,
		Block: blockData,
		Proof: proofData,
		Meta:  &storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 10, EndLT: 20},
	}); err != nil {
		t.Fatalf("save block full: %v", err)
	}

	gotBlock, err := store.BlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("load replaced block data: %v", err)
	}
	if !bytes.Equal(gotBlock, blockData) {
		t.Fatalf("replaced block data = %x, want %x", gotBlock, blockData)
	}
	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofBlock, block)
	if err != nil {
		t.Fatalf("load replaced proof: %v", err)
	}
	if !bytes.Equal(gotProof, proofData) {
		t.Fatalf("replaced proof = %x, want %x", gotProof, proofData)
	}
}

func TestSaveArchiveImportReusesExistingRefsAndAppendsMissingBlocks(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block1 := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     100,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	block2 := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     101,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	block1Data := []byte{0x01, 0x02}
	block1Proof := []byte{0x03, 0x04}
	if err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:    block1,
		Block: block1Data,
		Proof: block1Proof,
		Meta:  &storage.BlockMeta{ID: block1, GenUTime: 123, StartLT: 10, EndLT: 20},
	}); err != nil {
		t.Fatalf("save existing block: %v", err)
	}

	oldBlockRefRaw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block1))
	if err != nil {
		t.Fatalf("load existing block ref: %v", err)
	}
	oldBlockRef, err := decodeArtifactRef(oldBlockRefRaw)
	if err != nil {
		t.Fatalf("decode existing block ref: %v", err)
	}
	oldProofRefRaw, err := store.getHotCopy(context.Background(), hotKeyStoredProofRef(storage.ServedProofBlock, block1))
	if err != nil {
		t.Fatalf("load existing proof ref: %v", err)
	}
	oldProofRef, err := decodeArtifactRef(oldProofRefRaw)
	if err != nil {
		t.Fatalf("decode existing proof ref: %v", err)
	}

	block2Data := []byte{0x05, 0x06}
	block2Proof := []byte{0x07, 0x08}
	err = store.SaveArchiveImport(&storage.ServedArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			{
				ID:    block1,
				Block: []byte{0xaa, 0xbb},
				Proof: []byte{0xcc, 0xdd},
				Meta:  &storage.BlockMeta{ID: block1, GenUTime: 123, StartLT: 10, EndLT: 20},
			},
			{
				ID:    block2,
				Block: block2Data,
				Proof: block2Proof,
				Meta:  &storage.BlockMeta{ID: block2, GenUTime: 124, StartLT: 30, EndLT: 40},
			},
		},
	})
	if err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	newBlockRefRaw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block1))
	if err != nil {
		t.Fatalf("load reused block ref: %v", err)
	}
	newBlockRef, err := decodeArtifactRef(newBlockRefRaw)
	if err != nil {
		t.Fatalf("decode reused block ref: %v", err)
	}
	if *newBlockRef != *oldBlockRef {
		t.Fatalf("block ref was replaced: got %+v want %+v", newBlockRef, oldBlockRef)
	}
	newProofRefRaw, err := store.getHotCopy(context.Background(), hotKeyStoredProofRef(storage.ServedProofBlock, block1))
	if err != nil {
		t.Fatalf("load reused proof ref: %v", err)
	}
	newProofRef, err := decodeArtifactRef(newProofRefRaw)
	if err != nil {
		t.Fatalf("decode reused proof ref: %v", err)
	}
	if *newProofRef != *oldProofRef {
		t.Fatalf("proof ref was replaced: got %+v want %+v", newProofRef, oldProofRef)
	}

	gotBlock1, err := store.BlockData(context.Background(), block1)
	if err != nil {
		t.Fatalf("load reused block data: %v", err)
	}
	if !bytes.Equal(gotBlock1, block1Data) {
		t.Fatalf("reused block data = %x, want %x", gotBlock1, block1Data)
	}
	gotBlock2, err := store.BlockData(context.Background(), block2)
	if err != nil {
		t.Fatalf("load appended block data: %v", err)
	}
	if !bytes.Equal(gotBlock2, block2Data) {
		t.Fatalf("appended block data = %x, want %x", gotBlock2, block2Data)
	}
	gotProof2, err := store.BlockProof(context.Background(), storage.ServedProofBlock, block2)
	if err != nil {
		t.Fatalf("load appended proof: %v", err)
	}
	if !bytes.Equal(gotProof2, block2Proof) {
		t.Fatalf("appended proof = %x, want %x", gotProof2, block2Proof)
	}
}

func TestArchiveInfoNormalizesSplitShardPrefix(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	splitShard := int64(-8646911284551352320) // 0x8800000000000000
	packPath, _, _ := writeRefOnlyTestPack(t, ton.BlockIDExt{
		Workchain: 0,
		Shard:     splitShard,
		SeqNo:     42,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}, []byte{1}, []byte{2})
	if _, err = store.SaveArchiveFile(42, 0, splitShard, 0, packPath); err != nil {
		t.Fatalf("save archive file: %v", err)
	}

	exact, err := store.ArchiveInfo(context.Background(), 42, 0, splitShard)
	if err != nil {
		t.Fatalf("exact archive info: %v", err)
	}
	normalized, err := store.ArchiveInfo(context.Background(), 42, 0, int64(-1<<63))
	if err != nil {
		t.Fatalf("normalized archive info: %v", err)
	}
	if normalized != exact {
		t.Fatalf("normalized archive id = %d, want %d", normalized, exact)
	}
}

func TestArchivePackIDSeparatesBasechainFullAndSplitShard(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	fullPath, _, _ := writeRefOnlyTestPack(t, ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     156,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}, []byte{1}, []byte{2})
	if _, err = store.SaveArchiveFile(156, 0, int64(-1<<63), 0, fullPath); err != nil {
		t.Fatalf("save full basechain archive: %v", err)
	}

	splitShard := int64(0x4000000000000000)
	splitPath, _, _ := writeRefOnlyTestPack(t, ton.BlockIDExt{
		Workchain: 0,
		Shard:     splitShard,
		SeqNo:     156,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}, []byte{3}, []byte{4})
	if _, err = store.SaveArchiveFile(156, 0, splitShard, 0, splitPath); err != nil {
		t.Fatalf("save split basechain archive: %v", err)
	}

	fullID, err := store.ArchiveInfo(context.Background(), 156, 0, int64(-1<<63))
	if err != nil {
		t.Fatalf("full archive info: %v", err)
	}
	splitID, err := store.ArchiveInfo(context.Background(), 156, 0, splitShard)
	if err != nil {
		t.Fatalf("split archive info: %v", err)
	}
	if fullID == splitID {
		t.Fatalf("archive ids collided: full=%d split=%d", fullID, splitID)
	}
}

func TestPruneArchivePackagesDeletesOldPackagesAfterBoundary(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	oldA := testArchivePruneBlock(10, 0x10)
	oldB := testArchivePruneBlock(150, 0x20)
	boundary := testArchivePruneBlock(220, 0x30)
	newer := testArchivePruneBlock(320, 0x40)

	savePruneBlock := func(block ton.BlockIDExt, utime uint32) string {
		t.Helper()
		if err := store.SaveBlockFull(&storage.ServedBlockFull{
			ID:    block,
			Block: []byte{byte(block.SeqNo)},
			Meta:  &storage.BlockMeta{ID: block, GenUTime: utime, StartLT: uint64(block.SeqNo), EndLT: uint64(block.SeqNo + 1)},
		}); err != nil {
			t.Fatalf("save block %d: %v", block.SeqNo, err)
		}
		raw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
		if err != nil {
			t.Fatalf("load block ref %d: %v", block.SeqNo, err)
		}
		ref, err := decodeArtifactRef(raw)
		if err != nil {
			t.Fatalf("decode block ref %d: %v", block.SeqNo, err)
		}
		return store.artifactPath(ref.Path)
	}

	oldAPath := savePruneBlock(oldA, 1000)
	oldBPath := savePruneBlock(oldB, 1500)
	boundaryPath := savePruneBlock(boundary, 2000)
	newerPath := savePruneBlock(newer, 3000)
	if err = store.syncPendingArtifactFiles(); err != nil {
		t.Fatalf("sync pending artifacts: %v", err)
	}
	wantDeletedBytes := testFileSize(t, oldAPath) + testFileSize(t, oldBPath)

	stats, err := store.PruneArchivePackages(context.Background(), 2500, 0)
	if err != nil {
		t.Fatalf("prune archive packages: %v", err)
	}
	if stats.DeletedBeforeSeqno != 200 {
		t.Fatalf("deleted before seqno = %d, want 200", stats.DeletedBeforeSeqno)
	}
	if stats.RetainedBoundarySeqno != 200 {
		t.Fatalf("retained boundary seqno = %d, want 200", stats.RetainedBoundarySeqno)
	}
	if stats.DeletedPackages != 2 || stats.DeletedPackageFiles != 2 {
		t.Fatalf("deleted packages/files = %d/%d, want 2/2", stats.DeletedPackages, stats.DeletedPackageFiles)
	}
	if stats.DeletedPackageBytes != wantDeletedBytes {
		t.Fatalf("deleted package bytes = %d, want %d", stats.DeletedPackageBytes, wantDeletedBytes)
	}
	if stats.DeletedBlockMeta != 2 {
		t.Fatalf("deleted block meta = %d, want 2", stats.DeletedBlockMeta)
	}

	for _, block := range []ton.BlockIDExt{oldA, oldB} {
		if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("old block meta %d error = %v, want ErrNotFound", block.SeqNo, err)
		}
		if _, err = store.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("old block data %d error = %v, want ErrNotFound", block.SeqNo, err)
		}
		if _, err = store.ArchiveInfo(context.Background(), int32(block.SeqNo), -1, int64(-1<<63)); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("old archive info %d error = %v, want ErrNotFound", block.SeqNo, err)
		}
	}

	for _, block := range []ton.BlockIDExt{boundary, newer} {
		if _, err = store.BlockMeta(context.Background(), block); err != nil {
			t.Fatalf("kept block meta %d: %v", block.SeqNo, err)
		}
		if _, err = store.BlockData(context.Background(), block); err != nil {
			t.Fatalf("kept block data %d: %v", block.SeqNo, err)
		}
		if _, err = store.ArchiveInfo(context.Background(), int32(block.SeqNo), -1, int64(-1<<63)); err != nil {
			t.Fatalf("kept archive info %d: %v", block.SeqNo, err)
		}
	}

	for _, path := range []string{oldAPath, oldBPath} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("old archive pack %s stat error = %v, want missing", path, err)
		}
	}
	for _, path := range []string{boundaryPath, newerPath} {
		if _, err = os.Stat(path); err != nil {
			t.Fatalf("kept archive pack %s: %v", path, err)
		}
	}
	if err = store.syncPendingArtifactFiles(); err != nil {
		t.Fatalf("sync pending artifacts after prune: %v", err)
	}
}

func TestOpenRemovesArchivePackAfterCommittedMarkerDeleted(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	block := testArchivePruneBlock(42, 0x42)
	if err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:    block,
		Block: []byte{0x42},
		Meta:  &storage.BlockMeta{ID: block, GenUTime: 4200, StartLT: 42, EndLT: 43},
	}); err != nil {
		t.Fatalf("save block: %v", err)
	}
	if err = store.syncPendingArtifactFiles(); err != nil {
		t.Fatalf("sync pending artifacts: %v", err)
	}

	raw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
	if err != nil {
		t.Fatalf("load block ref: %v", err)
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		t.Fatalf("decode block ref: %v", err)
	}
	path := store.artifactPath(ref.Path)
	batch := store.hot.NewBatch()
	if err = batch.Delete(hotKeyPackCommitted(ref.Path), pebble.NoSync); err == nil {
		err = batch.Commit(pebble.Sync)
	}
	if closeErr := batch.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("delete committed marker: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("archive pack stat error = %v, want missing", err)
	}
}

func TestSaveArchiveFileMarksExistingPackPendingBeyondCommittedSize(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	initialPath := writeTestPackEntries(t, map[string][]byte{
		"old": {0x01},
	})
	saved, err := store.SaveArchiveFile(78, -1, int64(-1<<63), 0, initialPath)
	if err != nil {
		t.Fatalf("save initial archive file: %v", err)
	}
	if err = store.syncPendingArtifactFiles(); err != nil {
		t.Fatalf("sync initial archive file: %v", err)
	}

	file, err := os.OpenFile(saved.Path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open stored archive file: %v", err)
	}
	if _, err = packfile.Append(file, "new", []byte{0x02}, false); err != nil {
		_ = file.Close()
		t.Fatalf("append unsynced archive entry: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close stored archive file: %v", err)
	}
	store.artifactMu.Lock()
	store.dirtyArchivePacks[saved.Path] = struct{}{}
	store.artifactMu.Unlock()

	fullPath := writeTestPackEntries(t, map[string][]byte{
		"old": {0x01},
		"new": {0x02},
	})
	merged, err := store.SaveArchiveFile(78, -1, int64(-1<<63), 0, fullPath)
	if err != nil {
		t.Fatalf("save full archive file: %v", err)
	}
	if !merged.ReusedExisting {
		t.Fatalf("archive file reuse = false, want true")
	}

	store.artifactMu.Lock()
	_, pending := store.pendingArchiveSync[saved.Path]
	store.artifactMu.Unlock()
	if !pending {
		t.Fatalf("existing archive pack beyond committed size was not scheduled for checkpoint sync")
	}
}

func testArchivePruneBlock(seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{seed}, 32),
		FileHash:  bytes.Repeat([]byte{seed + 1}, 32),
	}
}

func TestArtifactSlicesReturnNotFoundOnTruncatedFiles(t *testing.T) {
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

	statePath := filepath.Join(store.StateFilesDir(), "state-truncated")
	if err = os.WriteFile(statePath, []byte{1, 2, 3, 4, 5}, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: statePath, Size: 5},
		FileHash:         bytes.Repeat([]byte{0x55}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x66}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
	if err = os.Truncate(statePath, 4); err != nil {
		t.Fatalf("truncate state file: %v", err)
	}
	if _, err = store.PersistentStateSlice(context.Background(), block, master, 0, 0, 5); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("truncated state slice error = %v, want ErrNotFound", err)
	}

	archivePath, _, _ := writeRefOnlyTestPack(t, master, []byte{9, 8, 7}, []byte{6, 5, 4})
	savedArchive, err := store.SaveArchiveFile(78, -1, int64(-1<<63), 0, archivePath)
	if err != nil {
		t.Fatalf("save archive file: %v", err)
	}
	archiveID, err := store.ArchiveInfo(context.Background(), 78, -1, int64(-1<<63))
	if err != nil {
		t.Fatalf("archive info: %v", err)
	}
	stat, err := os.Stat(savedArchive.Path)
	if err != nil {
		t.Fatalf("stat archive file: %v", err)
	}
	if err = os.Truncate(savedArchive.Path, stat.Size()-1); err != nil {
		t.Fatalf("truncate archive file: %v", err)
	}
	if _, err = store.ArchiveSlice(context.Background(), archiveID, 0, int32(stat.Size())); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("truncated archive slice error = %v, want ErrNotFound", err)
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

	if err = store.DeletePersistentStateFile(context.Background(), block, master, 0); err != nil {
		t.Fatalf("delete persistent state file: %v", err)
	}
	if _, err = store.PersistentStateSize(context.Background(), block, master, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state size after delete error = %v, want not found", err)
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("persistent state file after delete error = %v, want not exist", err)
	}
	meta, err = store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta after delete: %v", err)
	}
	if meta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatal("persistent state delete left snapshot flag in block meta")
	}
	if len(meta.StateFileHash) != 0 {
		t.Fatalf("state file hash after delete = %x, want empty", meta.StateFileHash)
	}
}

func TestPruneExpiredPersistentStateFilesKeepsTwoNewestGroups(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	nowUnix := uint64(10_000_000)
	expiredUTime := uint32(1 << 17)

	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master, expiredUTime)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}

	stats, err := store.PruneExpiredPersistentStateFiles(ctx, nowUnix, 2, 1)
	if err != nil {
		t.Fatalf("prune persistent states: %v", err)
	}
	if stats.ScannedFiles != 4 {
		t.Fatalf("scanned files = %d, want 4", stats.ScannedFiles)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 {
		t.Fatalf("deleted = records:%d disk:%d, want records:1 disk:1", stats.DeletedFileRecords, stats.DeletedDiskFiles)
	}
	if stats.DeletedDiskBytes != 1 {
		t.Fatalf("deleted disk bytes = %d, want 1", stats.DeletedDiskBytes)
	}
	assertTestPersistentStatePruned(t, store, masters[0], paths[10])
	assertTestPersistentStatePresent(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])

	stats, err = store.PruneExpiredPersistentStateFiles(ctx, nowUnix, 2, 0)
	if err != nil {
		t.Fatalf("prune persistent states second pass: %v", err)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 {
		t.Fatalf("second pass deleted = records:%d disk:%d, want records:1 disk:1", stats.DeletedFileRecords, stats.DeletedDiskFiles)
	}
	if stats.DeletedDiskBytes != 1 {
		t.Fatalf("second pass deleted disk bytes = %d, want 1", stats.DeletedDiskBytes)
	}
	if stats.RetainedRecentGroups != 2 || stats.OldestRetainedMasterSeqno != 30 {
		t.Fatalf("retained groups = %d oldest = %d, want 2 and 30", stats.RetainedRecentGroups, stats.OldestRetainedMasterSeqno)
	}
	assertTestPersistentStatePruned(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func TestPrunePreviousPersistentStateFilesDeletesLatestOlderGroup(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master, 1<<17)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}

	stats, err := store.PrunePreviousPersistentStateFiles(ctx, 35)
	if err != nil {
		t.Fatalf("prune previous persistent state: %v", err)
	}
	if stats.DeletedMasterSeqno != 30 {
		t.Fatalf("deleted master seqno = %d, want 30", stats.DeletedMasterSeqno)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 || stats.DeletedDiskBytes != 1 {
		t.Fatalf("deleted = records:%d files:%d bytes:%d, want 1/1/1", stats.DeletedFileRecords, stats.DeletedDiskFiles, stats.DeletedDiskBytes)
	}
	assertTestPersistentStatePresent(t, store, masters[0], paths[10])
	assertTestPersistentStatePresent(t, store, masters[1], paths[20])
	assertTestPersistentStatePruned(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func saveTestPersistentStatePruneMasterMeta(t *testing.T, store *Store, master ton.BlockIDExt, genUTime uint32) {
	t.Helper()

	err := store.withHotBatch(func(batch *pebble.Batch) error {
		return store.setMergedBlockMeta(batch, &storage.BlockMeta{
			ID:       master,
			GenUTime: genUTime,
		})
	})
	if err != nil {
		t.Fatalf("save master meta %s: %v", storage.FormatBlockRef(master), err)
	}
}

func saveTestPersistentStatePruneFile(t *testing.T, store *Store, master ton.BlockIDExt) string {
	t.Helper()

	data := []byte{byte(master.SeqNo)}
	path := filepath.Join(store.StateFilesDir(), fmt.Sprintf("state-prune-%d", master.SeqNo))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write persistent state file: %v", err)
	}

	if err := store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            master,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: path, Size: int64(len(data))},
		FileHash:         bytes.Repeat([]byte{byte(master.SeqNo)}, 32),
		StateRootHash:    bytes.Repeat([]byte{byte(master.SeqNo + 1)}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file %s: %v", storage.FormatBlockRef(master), err)
	}
	return path
}

func assertTestPersistentStatePruned(t *testing.T, store *Store, master ton.BlockIDExt, path string) {
	t.Helper()

	if _, err := store.PersistentStateSize(context.Background(), master, master, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state %s size error = %v, want ErrNotFound", storage.FormatBlockRef(master), err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("persistent state file %s stat error = %v, want not exist", path, err)
	}
}

func assertTestPersistentStatePresent(t *testing.T, store *Store, master ton.BlockIDExt, path string) {
	t.Helper()

	if _, err := store.PersistentStateSize(context.Background(), master, master, 0); err != nil {
		t.Fatalf("persistent state %s size: %v", storage.FormatBlockRef(master), err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("persistent state file %s stat: %v", path, err)
	}
}

func testFileSize(t *testing.T, path string) uint64 {
	t.Helper()

	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if stat.Size() <= 0 {
		return 0
	}
	return uint64(stat.Size())
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

func writeTestPackEntries(t *testing.T, entries map[string][]byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("create archive pack: %v", err)
	}

	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if _, err = packfile.Append(file, name, entries[name], true); err != nil {
			_ = file.Close()
			t.Fatalf("append archive entry %s: %v", name, err)
		}
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close archive pack: %v", err)
	}
	return path
}
