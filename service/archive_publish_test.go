package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestImportArchiveBlocksRejectsInvalidNewArchive(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	badPath := writeServiceTestCorruptArchivePack(t)
	badData := readServiceTestFile(t, badPath)
	_, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: 21,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Data:             badData,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "archive entry magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want entry magic mismatch", err)
	}
}

func TestImportArchiveBlocksDoesNotTouchStoredArchiveOnInvalidDuplicateDownload(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     21,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	oldData := []byte{0x55}
	if err := store.SaveArchiveImport(&storage.ServedArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: oldData,
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 2100, StartLT: 21, EndLT: 22},
		}},
	}); err != nil {
		t.Fatalf("save old archive block: %v", err)
	}

	archiveID, err := store.ArchiveInfo(ctx, int32(block.SeqNo), -1, topShard)
	if err != nil {
		t.Fatalf("old archive info: %v", err)
	}
	oldOffset := int64(packfile.HeaderSize + packfile.EntryHeaderSize + len(packfile.EntryName(packfile.KindBlock, block)))

	badPath := writeServiceTestCorruptArchivePack(t)
	badData := readServiceTestFile(t, badPath)
	_, err = svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: 21,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        archiveID,
		Data:             badData,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "archive entry magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want entry magic mismatch", err)
	}

	got, err := store.ArchiveSlice(ctx, archiveID, oldOffset, int32(len(oldData)))
	if err != nil {
		t.Fatalf("read old archive slice: %v", err)
	}
	if !bytes.Equal(got, oldData) {
		t.Fatalf("archive file was replaced before full validation: %x", got)
	}
}

func TestImportArchiveBlocksStoresInlineArchiveData(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	block, blockData := readServiceMasterchainBlockFixture(t)
	path := writeServiceTestBlockArchivePack(t, block, blockData)
	data := readServiceTestFile(t, path)
	_, imported, err := svc.importArchiveBlocksForStorage(ctx, &archive.Downloaded{
		MasterchainSeqno: block.SeqNo,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Data:             data,
	}, 0, nil)
	if err != nil {
		t.Fatalf("import archive blocks: %v", err)
	}
	if len(imported.FullBlocks) != 1 {
		t.Fatalf("stored full blocks = %d, want 1", len(imported.FullBlocks))
	}
	full := imported.FullBlocks[0]
	if !bytes.Equal(full.Block, blockData) || len(full.Proof) == 0 {
		t.Fatalf("stored full block misses inline data: block=%d proof=%d", len(full.Block), len(full.Proof))
	}

	if err = store.SaveArchiveImport(&imported); err != nil {
		t.Fatalf("save archive import data: %v", err)
	}
	got, err := store.BlockData(ctx, block)
	if err != nil {
		t.Fatalf("load archived block data: %v", err)
	}
	if !bytes.Equal(got, blockData) {
		t.Fatalf("archived block data mismatch")
	}
}

func readServiceTestFile(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test file %s: %v", path, err)
	}
	return data
}

func writeServiceTestBlockArchivePack(t *testing.T, block ton.BlockIDExt, blockData []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("create archive pack: %v", err)
	}
	appender, err := packfile.NewAppender(file)
	if err != nil {
		t.Fatalf("create archive appender: %v", err)
	}
	if _, err = appender.Append(serviceTestEntryName("block", block), blockData); err != nil {
		t.Fatalf("write block entry: %v", err)
	}
	if _, err = appender.Append(serviceTestEntryName("proof", block), []byte{0x01}); err != nil {
		t.Fatalf("write proof entry: %v", err)
	}
	if err = file.Sync(); err != nil {
		t.Fatalf("sync archive pack: %v", err)
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
