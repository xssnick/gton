package p2p

import (
	"encoding/binary"
	"github.com/xssnick/gton/service/archive/packfile"
	"strings"
	"testing"
)

func TestArchivePackDownloadLimitRejectsEndlessFullSlices(t *testing.T) {
	var offset int64
	fullSlices := int(archivePackMaxBytes / archiveSliceSize)
	for i := 0; i < fullSlices; i++ {
		if err := checkArchivePackDownloadSize(offset, archiveSliceSize); err != nil {
			t.Fatalf("slice %d rejected before limit: %v", i, err)
		}
		offset += archiveSliceSize
	}
	if offset != archivePackMaxBytes {
		t.Fatalf("test setup reached offset=%d want=%d", offset, archivePackMaxBytes)
	}

	err := checkArchivePackDownloadSize(offset, archiveSliceSize)
	if err == nil || !strings.Contains(err.Error(), "exceeds max size") {
		t.Fatalf("next full slice error = %v, want max size rejection", err)
	}
}

func TestArchivePackMagicRejectsInvalidFirstSlice(t *testing.T) {
	valid := make([]byte, packfile.HeaderSize)
	binary.LittleEndian.PutUint32(valid, packfile.PackageMagic)
	if err := checkArchivePackMagic(valid); err != nil {
		t.Fatalf("valid archive magic rejected: %v", err)
	}

	if err := checkArchivePackMagic([]byte{1, 2, 3}); err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("short archive magic error = %v, want too short", err)
	}

	invalid := make([]byte, packfile.HeaderSize)
	binary.LittleEndian.PutUint32(invalid, 0xdeadbeef)
	if err := checkArchivePackMagic(invalid); err == nil || !strings.Contains(err.Error(), "magic mismatch") {
		t.Fatalf("invalid archive magic error = %v, want mismatch", err)
	}
}
