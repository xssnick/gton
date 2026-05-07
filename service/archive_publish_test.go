package service

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"flexserver/service/archive"
	"flexserver/service/archive/packfile"
)

func TestImportArchiveBlocksPublishesArchiveAfterFullValidation(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	archiveID := int64(777)
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
	})
	if err == nil || !strings.Contains(err.Error(), "archive entry magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want entry magic mismatch", err)
	}

	got, err := store.ArchiveSlice(ctx, archiveID, oldPtr.Offset, int32(oldPtr.Size))
	if err != nil {
		t.Fatalf("read old archive slice: %v", err)
	}
	if string(got) != string([]byte{0x55}) {
		t.Fatalf("archive file was replaced before full validation: %x", got)
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
