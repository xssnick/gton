package pebblestore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"flexserver/service/archive/packfile"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
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
