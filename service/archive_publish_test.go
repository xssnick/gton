package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/archive/packfile"

	"github.com/xssnick/tonutils-go/ton"
)

func TestImportArchiveBlocksRejectsInvalidNewArchive(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	badPath := writeServiceTestCorruptArchivePack(t)
	_, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: 21,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Path:             badPath,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "archive entry magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want entry magic mismatch", err)
	}
}

func TestImportArchiveBlocksDoesNotTouchStoredArchiveOnInvalidDuplicateDownload(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	archiveID := int64(0)
	oldPath, oldPtr := writeServiceTestArchivePack(t, "old", []byte{0x55})
	if _, err := store.SaveArchiveFile(21, -1, topShard, archiveID, oldPath); err != nil {
		t.Fatalf("save old archive: %v", err)
	}

	badPath := writeServiceTestCorruptArchivePack(t)
	_, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: 21,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        archiveID,
		Path:             badPath,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "archive entry magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want entry magic mismatch", err)
	}
	if _, err = os.Stat(badPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate corrupt archive stat error = %v, want removed", err)
	}

	got, err := store.ArchiveSlice(ctx, archiveID, oldPtr.Offset, int32(oldPtr.Size))
	if err != nil {
		t.Fatalf("read old archive slice: %v", err)
	}
	if string(got) != string([]byte{0x55}) {
		t.Fatalf("archive file was replaced before full validation: %x", got)
	}
}

func TestImportArchiveBlocksStoresDownloadedPackRefs(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	block, blockData := readServiceMasterchainBlockFixture(t)
	path := writeServiceTestBlockArchivePack(t, block, blockData)
	imported, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: block.SeqNo,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Path:             path,
	}, 0)
	if err != nil {
		t.Fatalf("import archive blocks: %v", err)
	}
	if len(imported.stored.FullBlocks) != 1 {
		t.Fatalf("stored full blocks = %d, want 1", len(imported.stored.FullBlocks))
	}
	full := imported.stored.FullBlocks[0]
	if full.BlockRef == nil || full.ProofRef == nil {
		t.Fatalf("stored full block refs are missing: block=%+v proof=%+v", full.BlockRef, full.ProofRef)
	}
	if len(full.Block) != 0 || len(full.Proof) != 0 {
		t.Fatalf("stored full block keeps inline data: block=%d proof=%d", len(full.Block), len(full.Proof))
	}

	if err = store.SaveArchiveImport(&imported.stored); err != nil {
		t.Fatalf("save archive import refs: %v", err)
	}
	got, err := store.BlockData(ctx, block)
	if err != nil {
		t.Fatalf("load archived block data: %v", err)
	}
	if !bytes.Equal(got, blockData) {
		t.Fatalf("archived block data mismatch")
	}
}

func TestImportArchiveBlocksReusesExistingStoredPack(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	block, blockData := readServiceMasterchainBlockFixture(t)
	firstPath := writeServiceTestBlockArchivePack(t, block, blockData)
	first, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: block.SeqNo,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Path:             firstPath,
	}, 0)
	if err != nil {
		t.Fatalf("import first archive blocks: %v", err)
	}
	if len(first.stored.FullBlocks) != 1 {
		t.Fatalf("first stored full blocks = %d, want 1", len(first.stored.FullBlocks))
	}

	secondPath := writeServiceTestBlockArchivePackProofFirst(t, block, blockData)
	second, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: block.SeqNo,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Path:             secondPath,
	}, 0)
	if err != nil {
		t.Fatalf("import duplicate archive blocks: %v", err)
	}
	if len(second.stored.FullBlocks) != 1 {
		t.Fatalf("second stored full blocks = %d, want 1", len(second.stored.FullBlocks))
	}
	if _, err = os.Stat(secondPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate downloaded pack stat error = %v, want removed", err)
	}

	firstBlock := first.stored.FullBlocks[0]
	secondBlock := second.stored.FullBlocks[0]
	if firstBlock.BlockRef == nil || firstBlock.ProofRef == nil || secondBlock.BlockRef == nil || secondBlock.ProofRef == nil {
		t.Fatalf("stored refs are missing: first=%+v second=%+v", firstBlock, secondBlock)
	}
	if *secondBlock.BlockRef != *firstBlock.BlockRef {
		t.Fatalf("duplicate block ref = %+v, want existing %+v", secondBlock.BlockRef, firstBlock.BlockRef)
	}
	if *secondBlock.ProofRef != *firstBlock.ProofRef {
		t.Fatalf("duplicate proof ref = %+v, want existing %+v", secondBlock.ProofRef, firstBlock.ProofRef)
	}
}

func TestSaveArchiveFileMergesDownloadedPackIntoExistingPartialPack(t *testing.T) {
	store := openTestPebbleStorage(t)

	oldPath := writeServiceTestArchivePackEntries(t, map[string][]byte{
		"old": {0x01},
	})
	saved, err := store.SaveArchiveFile(21, -1, topShard, 0, oldPath)
	if err != nil {
		t.Fatalf("save partial archive: %v", err)
	}

	fullPath := writeServiceTestArchivePackEntries(t, map[string][]byte{
		"old": {0x02},
		"new": {0x03},
	})
	merged, err := store.SaveArchiveFile(21, -1, topShard, 0, fullPath)
	if err != nil {
		t.Fatalf("merge full archive: %v", err)
	}
	if !merged.ReusedExisting {
		t.Fatalf("merged archive did not report existing pack reuse: %+v", merged)
	}
	if merged.Path != saved.Path {
		t.Fatalf("merged path = %q, want %q", merged.Path, saved.Path)
	}
	if _, err = os.Stat(fullPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("full downloaded pack stat error = %v, want removed", err)
	}

	entries := readServiceTestArchiveEntries(t, saved.Path)
	if !bytes.Equal(entries["old"], []byte{0x01}) {
		t.Fatalf("old entry = %x, want original partial data", entries["old"])
	}
	if !bytes.Equal(entries["new"], []byte{0x03}) {
		t.Fatalf("new entry = %x, want appended full-pack data", entries["new"])
	}
}

func writeServiceTestArchivePack(t *testing.T, name string, data []byte) (string, packfile.Pointer) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("create archive pack: %v", err)
	}
	ptr, err := packfile.Append(file, name, data, true)
	if closeErr := file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("write archive pack: %v", err)
	}
	return path, ptr
}

func writeServiceTestArchivePackEntries(t *testing.T, entries map[string][]byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
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
			t.Fatalf("write archive entry %s: %v", name, err)
		}
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close archive pack: %v", err)
	}
	return path
}

func readServiceTestArchiveEntries(t *testing.T, path string) map[string][]byte {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open archive pack: %v", err)
	}
	defer func() { _ = file.Close() }()

	entries := map[string][]byte{}
	if err = packfile.Read(context.Background(), file, func(entry packfile.Entry) error {
		entries[entry.Name] = append([]byte(nil), entry.Data...)
		return nil
	}); err != nil {
		t.Fatalf("read archive pack: %v", err)
	}
	return entries
}

func writeServiceTestBlockArchivePack(t *testing.T, block ton.BlockIDExt, blockData []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("create archive pack: %v", err)
	}
	if _, err = packfile.Append(file, serviceTestEntryName("block", block), blockData, true); err != nil {
		t.Fatalf("write block entry: %v", err)
	}
	if _, err = packfile.Append(file, serviceTestEntryName("proof", block), []byte{0x01}, true); err != nil {
		t.Fatalf("write proof entry: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close archive pack: %v", err)
	}
	return path
}

func writeServiceTestBlockArchivePackProofFirst(t *testing.T, block ton.BlockIDExt, blockData []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("create archive pack: %v", err)
	}
	if _, err = packfile.Append(file, serviceTestEntryName("proof", block), []byte{0x01}, true); err != nil {
		t.Fatalf("write proof entry: %v", err)
	}
	if _, err = packfile.Append(file, serviceTestEntryName("block", block), blockData, true); err != nil {
		t.Fatalf("write block entry: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close archive pack: %v", err)
	}
	return path
}

func readServiceMasterchainBlockFixture(t *testing.T) (ton.BlockIDExt, []byte) {
	t.Helper()

	rawFixture, err := os.ReadFile("testdata/masterchain_block_fixture.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var fixture blockFixture
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	blockData, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		t.Fatalf("decode block boc base64: %v", err)
	}
	rootHash, err := hex.DecodeString(fixture.Block.RootHashHex)
	if err != nil {
		t.Fatalf("decode root hash: %v", err)
	}
	fileHash, err := hex.DecodeString(fixture.Block.FileHashHex)
	if err != nil {
		t.Fatalf("decode file hash: %v", err)
	}
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		t.Fatalf("parse shard: %v", err)
	}

	return ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}, blockData
}

func serviceTestEntryName(kind string, block ton.BlockIDExt) string {
	return fmt.Sprintf(
		"%s_(%d,%016x,%d):%x:%x",
		kind,
		block.Workchain,
		uint64(block.Shard),
		block.SeqNo,
		block.RootHash,
		block.FileHash,
	)
}

func writeServiceTestCorruptArchivePack(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "bad.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("create bad archive pack: %v", err)
	}
	var writeErr error
	if err = binary.Write(file, binary.LittleEndian, uint32(packfile.PackageMagic)); err != nil {
		writeErr = err
	}
	if writeErr == nil {
		writeErr = binary.Write(file, binary.LittleEndian, uint32(0))
	}
	if writeErr == nil {
		writeErr = binary.Write(file, binary.LittleEndian, uint32(0))
	}
	if closeErr := file.Close(); closeErr != nil && writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		t.Fatalf("write bad archive pack: %v", writeErr)
	}
	return path
}
